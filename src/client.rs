use std::time::Duration;

use bytes::Bytes;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::config::BrokerConfig;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("mqtt connection error: {0}")]
    Connection(#[from] rumqttc::ConnectionError),
}

/// A message received on a subscribed topic.
pub struct IncomingMessage {
    pub topic: String,
    pub payload: Bytes,
}

/// A live connection to an MQTT broker: a client handle for publishing/subscribing, a channel of
/// incoming messages, and the background task driving the underlying connection.
pub struct MqttConnection {
    pub client: AsyncClient,
    pub incoming: mpsc::Receiver<IncomingMessage>,
    pub driver: JoinHandle<Result<(), BrokerError>>,
}

pub struct Client;

impl Client {
    /// Connects to the configured broker and spawns a background task that continuously polls
    /// the connection (required to keep it alive and drive publish acks), forwarding incoming
    /// Publish packets onto a channel. Decoupling the network poll loop from decode/republish
    /// work this way means slow decoding never risks stalling keepalives.
    pub fn connect(config: &BrokerConfig) -> MqttConnection {
        let mut options =
            MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port);
        options.set_keep_alive(Duration::from_secs(30));

        let (client, mut eventloop) = AsyncClient::new(options, 100);
        let (tx, rx) = mpsc::channel(100);

        let driver = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        let message = IncomingMessage {
                            topic: publish.topic,
                            payload: publish.payload,
                        };
                        if tx.send(message).await.is_err() {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => return Err(BrokerError::Connection(e)),
                }
            }
            Ok(())
        });

        MqttConnection {
            client,
            incoming: rx,
            driver,
        }
    }

    /// Subscribes to every configured topic filter.
    pub async fn subscribe_all(
        client: &AsyncClient,
        filters: impl IntoIterator<Item = &str>,
    ) -> Result<(), rumqttc::ClientError> {
        for filter in filters {
            client.subscribe(filter, QoS::AtLeastOnce).await?;
        }
        Ok(())
    }
}
