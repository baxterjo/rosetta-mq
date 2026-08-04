use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS, Transport};
use tokio::sync::mpsc::{self, Sender};
use tokio::time::timeout;

use rosetta_mq::auth::ResolvedAuth;
use rosetta_mq::client::{Client, ConnectionConfig};
use rosetta_mq::config::{Config, EngineConfig, RefOr, TopicMapping};
use rosetta_mq::decoder::protobuf::ProtobufConfig;
use rosetta_mq::decoder::template::TemplateConfig;
use rosetta_mq::decoder::{
    DecodeError, DecodePublish, Decoder, DecoderConfig, DecoderRegistryBuilder,
};
use rosetta_mq::engine;
use rosetta_mq::protocol::{Protocol, WebsocketConfig};
use rosetta_mq::topic::TopicFilter;
use tokio_util::sync::CancellationToken;

const TEST_PORT: u16 = 18883;
const PROTOBUF_TEST_PORT: u16 = 18884;
const WEBSOCKET_TEST_PORT: u16 = 18888;
const CONCURRENCY_TEST_PORT: u16 = 18889;
const SHUTDOWN_TEST_PORT: u16 = 18890;
const TEMPLATE_TEST_PORT: u16 = 18891;
const PROTO_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/protobuf/device.proto"
);
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

/// Same shape as `rumqttd_config`, but listens for websocket connections (`ws`) instead of raw
/// TCP (`v4`) -- rumqttd's websocket listener doesn't validate the request path, so any
/// `WebsocketConfig.path` the client sends is accepted.
fn rumqttd_ws_config(port: u16) -> rumqttd::Config {
    let listen: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    let mut ws = HashMap::new();
    ws.insert(
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
        v4: None,
        v5: None,
        ws: Some(ws),
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

/// Runs entirely in one test process: an embedded rumqttd broker, the rosetta-mq engine, and
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

    // 2. Wire up the rosetta-mq engine against the embedded broker, exactly as main.rs does.
    let app_config = Config {
        connection: ConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: TEST_PORT,
            client_id: "rosetta-mq-test".to_string(),
            auth: None,
            tls: false,
            protocol: Protocol::Mqtt,
            allow_self_signed_certs: false,
        },
        topics: vec![
            TopicMapping {
                topic_filter: "devices/+/raw".to_string(),
                decoder: Some(RefOr::Literal(DecoderConfig::Utf8)),
            },
            TopicMapping {
                topic_filter: "sensors/#".to_string(),
                decoder: Some(RefOr::Literal(DecoderConfig::Hexdump)),
            },
        ],
        decoders: HashMap::new(),
        engine: EngineConfig::default(),
    };

    let registry = build_registry(&app_config, Path::new("."));

    let conn = Client::connect(&app_config.connection, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let engine_client = conn.client.clone();
    tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        app_config.engine.max_concurrent_decodes,
        CancellationToken::new(),
    ));

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
    // Subscribed with the same '#' breadth as the engine's own mapping, so this would also
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
    // so we first see our own echoed publish before the engine's decoded output.
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
        connection: ConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: PROTOBUF_TEST_PORT,
            client_id: "rosetta-mq-protobuf-test".to_string(),
            auth: None,
            tls: false,
            protocol: Protocol::Mqtt,
            allow_self_signed_certs: false,
        },
        topics: vec![TopicMapping {
            topic_filter: "devices/+/proto".to_string(),
            decoder: Some(RefOr::Literal(DecoderConfig::Protobuf(ProtobufConfig {
                proto_file: PROTO_FIXTURE.to_string(),
                message_type: "device.v1.DeviceReading".to_string(),
                include_paths: vec![FIXTURES_DIR.to_string()],
            }))),
        }],
        decoders: HashMap::new(),
        engine: EngineConfig::default(),
    };

    let registry = build_registry(&app_config, Path::new("."));

    let conn = Client::connect(&app_config.connection, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let engine_client = conn.client.clone();
    tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        app_config.engine.max_concurrent_decodes,
        CancellationToken::new(),
    ));

    let mut options = MqttOptions::new("test-observer-protobuf", "127.0.0.1", PROTOBUF_TEST_PORT);
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
    let pool =
        prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set).unwrap();
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

#[tokio::test]
async fn template_decoder_end_to_end() {
    let broker_config = rumqttd_config(TEMPLATE_TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(TEMPLATE_TEST_PORT).await;

    let app_config = Config {
        connection: ConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: TEMPLATE_TEST_PORT,
            client_id: "rosetta-mq-template-test".to_string(),
            auth: None,
            tls: false,
            protocol: Protocol::Mqtt,
            allow_self_signed_certs: false,
        },
        topics: vec![TopicMapping {
            topic_filter: "devices/+/raw".to_string(),
            decoder: Some(RefOr::Literal(DecoderConfig::Template(TemplateConfig {
                template:
                    "{{ payload.device_id }} reads {{ payload.temperature_c }}C on {{ topic }}"
                        .to_string(),
                ..Default::default()
            }))),
        }],
        decoders: HashMap::new(),
        engine: EngineConfig::default(),
    };

    let registry = build_registry(&app_config, Path::new("."));

    let conn = Client::connect(&app_config.connection, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let engine_client = conn.client.clone();
    tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        app_config.engine.max_concurrent_decodes,
        CancellationToken::new(),
    ));

    let mut options = MqttOptions::new("test-observer-template", "127.0.0.1", TEMPLATE_TEST_PORT);
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

    tokio::time::sleep(Duration::from_millis(300)).await;

    test_client
        .publish(
            "devices/42/raw",
            QoS::AtLeastOnce,
            false,
            br#"{"device_id": "sensor-42", "temperature_c": 21.5}"#.to_vec(),
        )
        .await
        .unwrap();

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for decoded template message")
        .expect("channel closed");
    assert_eq!(topic, "devices/42/raw/decoded");
    assert_eq!(payload, b"sensor-42 reads 21.5C on devices/42/raw");
}

/// Proves the websocket transport carries real traffic end-to-end -- same embedded-broker shape
/// as `subscribe_decode_republish_end_to_end`, but both the engine's connection and the
/// observer connection go over `ws://` instead of raw TCP. Doesn't repeat every case from the TCP
/// test; just proves messages round-trip over websocket the same way they do over TCP.
#[tokio::test]
async fn websocket_end_to_end() {
    let broker_config = rumqttd_ws_config(WEBSOCKET_TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(WEBSOCKET_TEST_PORT).await;

    let app_config = Config {
        connection: ConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: WEBSOCKET_TEST_PORT,
            client_id: "rosetta-mq-websocket-test".to_string(),
            auth: None,
            tls: false,
            protocol: Protocol::Ws(WebsocketConfig {
                path: "/mqtt".to_string(),
            }),
            allow_self_signed_certs: false,
        },
        topics: vec![TopicMapping {
            topic_filter: "devices/+/raw".to_string(),
            decoder: Some(RefOr::Literal(DecoderConfig::Utf8)),
        }],
        decoders: HashMap::new(),
        engine: EngineConfig::default(),
    };

    let registry = build_registry(&app_config, Path::new("."));

    let conn = Client::connect(&app_config.connection, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(
        &conn.client,
        app_config.topics.iter().map(|t| t.topic_filter.as_str()),
    )
    .await
    .unwrap();

    let engine_client = conn.client.clone();
    tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        app_config.engine.max_concurrent_decodes,
        CancellationToken::new(),
    ));

    // The broker only has a `ws` listener (no `v4`), so the observer client also connects over
    // websocket.
    let mut options = MqttOptions::new(
        "test-observer-websocket",
        format!("ws://127.0.0.1:{WEBSOCKET_TEST_PORT}/mqtt"),
        WEBSOCKET_TEST_PORT,
    );
    options.set_keep_alive(Duration::from_secs(30));
    options.set_transport(Transport::Ws);
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

    tokio::time::sleep(Duration::from_millis(300)).await;

    test_client
        .publish("devices/42/raw", QoS::AtLeastOnce, false, "hello over ws")
        .await
        .unwrap();

    let (topic, payload) = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for decoded message over websocket")
        .expect("channel closed");
    assert_eq!(topic, "devices/42/raw/decoded");
    assert_eq!(String::from_utf8(payload).unwrap(), "hello over ws");
}

/// Test-only decoder that tracks how many decodes are running concurrently, via an
/// `Arc<AtomicUsize>` counter, and the peak value that counter ever reached, via a second
/// `Arc<AtomicUsize>` shared back with the test. Sleeps for `delay` on every decode using
/// `tokio::time::sleep` (not `std::thread::sleep` -- `decode` is genuinely async) so that messages
/// published back-to-back overlap in time, giving concurrently-running decodes a window in which
/// to actually overlap.
struct SlowDecoder {
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    delay: Duration,
}

#[async_trait]
impl Decoder for SlowDecoder {
    type Error = Infallible;

    fn name(&self) -> &str {
        "slow"
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>> {
        let current = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);

        tx.send(DecodePublish {
            payload: publish.payload.to_vec(),
            ..Default::default()
        })
        .await?;
        Ok(())
    }
}

/// Proves `max_concurrent_decodes` both bounds *and* enables concurrency: with a decoder slow
/// enough that back-to-back messages overlap in time, peak concurrent decodes observed across the
/// run must land at the configured cap -- never above it (the bound), and never at 1 (which would
/// mean messages were still being handled one at a time despite the cap, i.e. no real parallelism).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_concurrent_decoding() {
    let broker_config = rumqttd_config(CONCURRENCY_TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(CONCURRENCY_TEST_PORT).await;

    const CAP: usize = 3;
    const MESSAGE_COUNT: usize = 9;
    let delay = Duration::from_millis(200);

    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut builder = DecoderRegistryBuilder::new();
    builder
        .register(
            TopicFilter::parse("load/#").unwrap(),
            Arc::new(SlowDecoder {
                in_flight: Arc::clone(&in_flight),
                peak: Arc::clone(&peak),
                delay,
            }),
        )
        .unwrap();
    let registry = builder.build();

    let broker_cfg = ConnectionConfig {
        host: "127.0.0.1".to_string(),
        port: CONCURRENCY_TEST_PORT,
        client_id: "rosetta-mq-concurrency-test".to_string(),
        auth: None,
        tls: false,
        protocol: Protocol::Mqtt,
        allow_self_signed_certs: false,
    };

    let conn = Client::connect(&broker_cfg, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(&conn.client, ["load/#"])
        .await
        .unwrap();

    let engine_client = conn.client.clone();
    tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        CAP,
        CancellationToken::new(),
    ));

    let mut options = MqttOptions::new(
        "test-observer-concurrency",
        "127.0.0.1",
        CONCURRENCY_TEST_PORT,
    );
    options.set_keep_alive(Duration::from_secs(30));
    let (test_client, mut test_eventloop) = AsyncClient::new(options, 100);

    let (tx, mut rx) = mpsc::channel(MESSAGE_COUNT);
    tokio::spawn(async move {
        loop {
            match test_eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(_))) => {
                    if tx.send(()).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    test_client
        .subscribe("load/+/decoded", QoS::AtLeastOnce)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    for i in 0..MESSAGE_COUNT {
        test_client
            .publish(format!("load/{i}"), QoS::AtLeastOnce, false, "x")
            .await
            .unwrap();
    }

    for _ in 0..MESSAGE_COUNT {
        timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for decoded message")
            .expect("channel closed");
    }

    let observed_peak = peak.load(Ordering::SeqCst);
    assert!(
        observed_peak <= CAP,
        "peak concurrent decodes ({observed_peak}) exceeded cap ({CAP})"
    );
    assert!(
        observed_peak > 1,
        "peak concurrent decodes was {observed_peak} -- messages were processed sequentially, not concurrently"
    );
}

/// Proves cancellation is a graceful shutdown, not an abort: an in-flight decode/publish must
/// still complete and be republished after `shutdown.cancel()`, and `engine::run` must return
/// once that happens rather than sitting out its full drain timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_drains_in_flight_task_before_returning() {
    let broker_config = rumqttd_config(SHUTDOWN_TEST_PORT);
    std::thread::spawn(move || {
        let mut broker = rumqttd::Broker::new(broker_config);
        let _ = broker.start();
    });
    wait_for_port(SHUTDOWN_TEST_PORT).await;

    // Long enough that the decode is still running when we cancel a moment after publishing, but
    // well under both the 2s we allow `run` to return by and the 5s abandon timeout -- so a
    // passing test proves the in-flight task was drained, not that it got lucky beating the clock.
    let decode_delay = Duration::from_millis(300);
    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));

    let mut builder = DecoderRegistryBuilder::new();
    builder
        .register(
            TopicFilter::parse("shutdown/#").unwrap(),
            Arc::new(SlowDecoder {
                in_flight,
                peak,
                delay: decode_delay,
            }),
        )
        .unwrap();
    let registry = builder.build();

    let broker_cfg = ConnectionConfig {
        host: "127.0.0.1".to_string(),
        port: SHUTDOWN_TEST_PORT,
        client_id: "rosetta-mq-shutdown-test".to_string(),
        auth: None,
        tls: false,
        protocol: Protocol::Mqtt,
        allow_self_signed_certs: false,
    };

    let conn = Client::connect(&broker_cfg, &ResolvedAuth::None).unwrap();
    Client::subscribe_all(&conn.client, ["shutdown/#"])
        .await
        .unwrap();

    let engine_client = conn.client.clone();
    let shutdown = CancellationToken::new();
    let engine_handle = tokio::spawn(engine::run(
        conn.incoming,
        engine_client,
        registry,
        10,
        shutdown.clone(),
    ));

    let mut options = MqttOptions::new("test-observer-shutdown", "127.0.0.1", SHUTDOWN_TEST_PORT);
    options.set_keep_alive(Duration::from_secs(30));
    let (test_client, mut test_eventloop) = AsyncClient::new(options, 100);

    let (tx, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            match test_eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(publish))) => {
                    if tx.send(publish.topic).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    });

    test_client
        .subscribe("shutdown/1/decoded", QoS::AtLeastOnce)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    test_client
        .publish("shutdown/1", QoS::AtLeastOnce, false, "x")
        .await
        .unwrap();
    // Give the decode a moment to actually start, so it's genuinely in-flight when cancelled --
    // not just still sitting in `incoming`.
    tokio::time::sleep(Duration::from_millis(50)).await;

    shutdown.cancel();

    timeout(Duration::from_secs(2), engine_handle)
        .await
        .expect("engine::run did not return promptly after cancellation")
        .expect("engine task panicked");

    let topic = timeout(Duration::from_secs(1), rx.recv())
        .await
        .expect("decoded message was dropped instead of drained on shutdown")
        .expect("channel closed");
    assert_eq!(topic, "shutdown/1/decoded");
}

/// Mirrors the registry-building loop in `main.rs`: skips topics with no decoder assigned,
/// resolves named/inline decoder refs, and registers the rest. Shared across the tests above
/// since each builds its own `Config` by hand rather than loading one from TOML.
fn build_registry(config: &Config, base_dir: &Path) -> rosetta_mq::decoder::DecoderRegistry {
    let mut builder = DecoderRegistryBuilder::new();
    for mapping in &config.topics {
        let Some(decoder_ref) = mapping.decoder.as_ref() else {
            continue;
        };
        let decoder = decoder_ref
            .resolve(&config.decoders)
            .expect("test config decoder ref must resolve")
            .build(base_dir)
            .unwrap();
        let filter = TopicFilter::parse(&mapping.topic_filter).unwrap();
        builder.register(filter, decoder).unwrap();
    }
    builder.build()
}
