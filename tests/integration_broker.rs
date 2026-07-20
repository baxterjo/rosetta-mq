use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;
use tokio::time::timeout;

use rosetta_mq::client::Client;
use rosetta_mq::config::{BrokerConfig, Config, TopicMapping};
use rosetta_mq::decoder::protobuf::ProtobufConfig;
use rosetta_mq::decoder::{DecoderConfig, DecoderRegistryBuilder};
use rosetta_mq::pipeline;
use rosetta_mq::topic::TopicFilter;

const TEST_PORT: u16 = 18883;
const PROTOBUF_TEST_PORT: u16 = 18884;
const PROTO_FIXTURE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/protobuf/device.proto");
// `device.proto` imports `common/status.proto`, a sibling of `protobuf/` -- resolving it needs
// an include path covering both directories, not just device.proto's own parent.
const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn rumqttd_config(port: u16) -> rumqttd::Config {
    let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut v4 = HashMap::new();
    v4.insert(
        "test".to_string(),
        rumqttd::ServerSettings {
            name: "test".to_string(),
            listen,
            tls: None,
            next_connection_delay_ms: 1,
            connections: rumqttd::ConnectionSettings {
                connection_timeout_ms: 5000,
                max_payload_size: 20 * 1024,
                max_inflight_count: 100,
                auth: None,
                external_auth: None,
                dynamic_filters: false,
            },
        },
    );

    rumqttd::Config {
        id: 0,
        router: rumqttd::RouterConfig {
            max_connections: 10,
            max_outgoing_packet_count: 200,
            max_segment_size: 1024 * 1024,
            max_segment_count: 10,
            ..Default::default()
        },
        v4: Some(v4),
        v5: None,
        ws: None,
        cluster: None,
        console: None,
        bridge: None,
        prometheus: None,
        metrics: None,
    }
}

async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "broker did not start listening on {addr} in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Runs entirely in one test process: an embedded rumqttd broker, the rosetta-mq pipeline, and
/// a second "observer" rumqttc client all in-process, over real TCP on a local test port. No
/// external broker or extra terminal/process is needed.
#[tokio::test]
async fn subscribe_decode_republish_end_to_end() {
    // 1. Start the embedded broker on a dedicated OS thread -- `Broker::start` blocks.
    let broker_config = rumqttd_config(TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(TEST_PORT).await;

    // 2. Wire up the rosetta-mq pipeline against the embedded broker, exactly as main.rs does.
    let app_config = Config {
        broker: BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: TEST_PORT,
            client_id: "rosetta-mq-test".to_string(),
        },
        topics: vec![
            TopicMapping {
                topic_filter: "devices/+/raw".to_string(),
                decoder: DecoderConfig::Utf8,
            },
            TopicMapping {
                topic_filter: "sensors/#".to_string(),
                decoder: DecoderConfig::Hexdump,
            },
        ],
    };

    let mut builder = DecoderRegistryBuilder::new();
    for mapping in &app_config.topics {
        let decoder = mapping.decoder.build(Path::new(".")).unwrap();
        let filter = TopicFilter::parse(&mapping.topic_filter).unwrap();
        builder.register(filter, decoder).unwrap();
    }
    let registry = builder.build();

    let conn = Client::connect(&app_config.broker);
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let pipeline_client = conn.client.clone();
    tokio::spawn(pipeline::run(conn.incoming, pipeline_client, registry));

    // 3. A second, independent rumqttc client plays the role of "any existing MQTT client"
    // observing the mirrored topics.
    let mut options = MqttOptions::new("test-observer", "127.0.0.1", TEST_PORT);
    options.set_keep_alive(Duration::from_secs(30));
    let (test_client, mut test_eventloop) = AsyncClient::new(options, 100);

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        loop {
            match test_eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if tx
                        .send((publish.topic, publish.payload.to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    test_client
        .subscribe("devices/42/raw/decoded", QoS::AtLeastOnce)
        .await
        .unwrap();
    test_client
        .subscribe("devices/42/raw/decode_error", QoS::AtLeastOnce)
        .await
        .unwrap();
    // Subscribed with the same '#' breadth as the pipeline's own mapping, so this would also
    // catch a feedback loop (`.../decoded/decoded`, etc.) if the guard below regresses.
    test_client
        .subscribe("sensors/#", QoS::AtLeastOnce)
        .await
        .unwrap();

    // Give the broker time to acknowledge both subscriptions before publishing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 4. Success path: valid UTF-8 round-trips through the Utf8Decoder onto `.../decoded`.
    test_client
        .publish("devices/42/raw", QoS::AtLeastOnce, false, "hello device")
        .await
        .unwrap();

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for decoded message")
        .expect("channel closed");
    assert_eq!(topic, "devices/42/raw/decoded");
    assert_eq!(String::from_utf8(payload).unwrap(), "hello device");

    // 5. Failure path: invalid UTF-8 is never dropped -- it's republished, error-annotated, to
    // the reserved `.../error` topic, regardless of the success-path topic above.
    test_client
        .publish(
            "devices/42/raw",
            QoS::AtLeastOnce,
            false,
            vec![0xffu8, 0xfe],
        )
        .await
        .unwrap();

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for error message")
        .expect("channel closed");
    assert_eq!(topic, "devices/42/raw/decode_error");
    let payload = String::from_utf8(payload).unwrap();
    assert!(payload.starts_with("error: "));
    assert!(payload.contains("raw_hex: fffe"));

    // 6. Regression check: a `#` mapping must not decode its own republished output. Publishing
    // to `sensors/temp` should produce exactly one `sensors/temp/decoded` message -- not a
    // `sensors/temp/decoded/decoded` feedback loop, since `sensors/#` also matches our own
    // output topics.
    test_client
        .publish("sensors/temp", QoS::AtLeastOnce, false, "sensor-payload")
        .await
        .unwrap();

    // We're subscribed via "sensors/#", which also matches the raw topic we just published to,
    // so we first see our own echoed publish before the pipeline's decoded output.
    let (echo_topic, _) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for raw echo")
        .expect("channel closed");
    assert_eq!(echo_topic, "sensors/temp");

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for sensors decoded message")
        .expect("channel closed");
    assert_eq!(topic, "sensors/temp/decoded");
    assert_eq!(
        String::from_utf8(payload).unwrap(),
        format!(
            "{} ({} bytes)",
            hex::encode("sensor-payload"),
            "sensor-payload".len()
        )
    );

    // No further (feedback-loop) message should follow within a short window.
    let third = timeout(Duration::from_millis(500), rx.recv()).await;
    assert!(
        third.is_err(),
        "expected no further messages, but got a feedback-loop message: {third:?}"
    );
}

/// End-to-end coverage for the runtime-schema protobuf decoder: a real broker, a topic mapping
/// pointing at the fixture `.proto`, and real encoded wire bytes -- proving `.proto` compilation,
/// dynamic decode, and JSON republish all work together, not just each in isolation.
#[tokio::test]
async fn protobuf_decoder_end_to_end() {
    let broker_config = rumqttd_config(PROTOBUF_TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(PROTOBUF_TEST_PORT).await;

    let app_config = Config {
        broker: BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: PROTOBUF_TEST_PORT,
            client_id: "rosetta-mq-protobuf-test".to_string(),
        },
        topics: vec![TopicMapping {
            topic_filter: "devices/+/proto".to_string(),
            decoder: DecoderConfig::Protobuf(ProtobufConfig {
                proto_file: PROTO_FIXTURE.to_string(),
                message_type: "device.v1.DeviceReading".to_string(),
                include_paths: vec![FIXTURES_DIR.to_string()],
            }),
        }],
    };

    let mut builder = DecoderRegistryBuilder::new();
    for mapping in &app_config.topics {
        let decoder = mapping.decoder.build(Path::new(".")).unwrap();
        let filter = TopicFilter::parse(&mapping.topic_filter).unwrap();
        builder.register(filter, decoder).unwrap();
    }
    let registry = builder.build();

    let conn = Client::connect(&app_config.broker);
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let pipeline_client = conn.client.clone();
    tokio::spawn(pipeline::run(conn.incoming, pipeline_client, registry));

    let mut options = MqttOptions::new(
        "test-observer-protobuf",
        "127.0.0.1",
        PROTOBUF_TEST_PORT,
    );
    options.set_keep_alive(Duration::from_secs(30));
    let (test_client, mut test_eventloop) = AsyncClient::new(options, 100);

    let (tx, mut rx) = mpsc::channel(10);
    tokio::spawn(async move {
        loop {
            match test_eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if tx
                        .send((publish.topic, publish.payload.to_vec()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    test_client
        .subscribe("devices/42/proto/decoded", QoS::AtLeastOnce)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Encode a real DeviceReading, independent of the decoder under test -- compiling the same
    // fixture a second time here mirrors an external producer that encodes according to the
    // shared schema, rather than reaching into the decoder's private descriptor.
    let file_descriptor_set = protox::compile([PROTO_FIXTURE], [FIXTURES_DIR]).unwrap();
    let pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set).unwrap();
    let descriptor = pool.get_message_by_name("device.v1.DeviceReading").unwrap();
    let mut message = prost_reflect::DynamicMessage::new(descriptor);
    message.set_field_by_name(
        "device_id",
        prost_reflect::Value::String("sensor-42".to_string()),
    );
    message.set_field_by_name("temperature_c", prost_reflect::Value::F64(21.5));
    message.set_field_by_name("online", prost_reflect::Value::Bool(true));
    message.set_field_by_name("status", prost_reflect::Value::EnumNumber(1)); // CONNECTION_STATUS_ONLINE
    let bytes = prost::Message::encode_to_vec(&message);

    test_client
        .publish("devices/42/proto", QoS::AtLeastOnce, false, bytes)
        .await
        .unwrap();

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for decoded protobuf message")
        .expect("channel closed");
    assert_eq!(topic, "devices/42/proto/decoded");

    let json: serde_json::Value = serde_json::from_slice(&payload).unwrap();
    assert_eq!(json["device_id"], "sensor-42");
    assert_eq!(json["temperature_c"], 21.5);
    assert_eq!(json["online"], true);
    assert_eq!(json["status"], "CONNECTION_STATUS_ONLINE");
}
