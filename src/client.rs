use std::time::Duration;

use rumqttc::{
    AsyncClient, Event, MqttOptions, Packet, Publish, QoS, TlsConfiguration, Transport,
};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::ResolvedAuth;
use crate::config::BrokerConfig;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("mqtt connection error: {0}")]
    Connection(#[from] rumqttc::ConnectionError),
}

/// A live connection to an MQTT broker: a client handle for publishing/subscribing, a channel of
/// incoming messages, and the background task driving the underlying connection.
pub struct MqttConnection {
    pub client: AsyncClient,
    pub incoming: mpsc::Receiver<Publish>,
    pub driver: JoinHandle<Result<(), BrokerError>>,
}

/// Builds the `MqttOptions` for `config`, applying `auth` on top -- separated from `connect`
/// itself so the auth wiring can be asserted on directly in tests, without needing to spin up
/// (or poll) the background event loop `connect` spawns.
fn build_options(config: &BrokerConfig, auth: &ResolvedAuth) -> MqttOptions {
    let mut options = MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port);
    options.set_keep_alive(Duration::from_secs(30));

    match auth {
        ResolvedAuth::None => {}
        ResolvedAuth::UserPass { username, password } => {
            options.set_credentials(username.clone(), password.clone());
        }
        ResolvedAuth::Mtls { ca, cert, key } => {
            options.set_transport(Transport::Tls(TlsConfiguration::Simple {
                ca: ca.clone(),
                alpn: None,
                client_auth: Some((cert.clone(), key.clone())),
            }));
        }
    }

    options
}

pub struct Client;

impl Client {
    /// Connects to the configured broker and spawns a background task that continuously polls
    /// the connection (required to keep it alive and drive publish acks), forwarding incoming
    /// Publish packets onto a channel. Decoupling the network poll loop from decode/republish
    /// work this way means slow decoding never risks stalling keepalives.
    ///
    /// `auth` is already resolved (certs read, env vars looked up) by [`crate::auth::AuthConfig::build`]
    /// before this is called, so connecting itself stays infallible and filesystem/env-free.
    pub fn connect(config: &BrokerConfig, auth: &ResolvedAuth) -> MqttConnection {
        let options = build_options(config, auth);

        let (client, mut eventloop) = AsyncClient::new(options, 100);
        let (tx, rx) = mpsc::channel(100);

        let driver = tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if tx.send(publish).await.is_err() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> BrokerConfig {
        BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: 1883,
            client_id: "rosetta-mq-test".to_string(),
            auth: None,
        }
    }

    #[test]
    fn no_auth_leaves_options_unauthenticated() {
        let options = build_options(&test_config(), &ResolvedAuth::None);
        assert!(options.credentials().is_none());
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn userpass_auth_sets_credentials() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let options = build_options(&test_config(), &auth);

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert_eq!(login.password, "supersecret");
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn mtls_auth_sets_tls_transport_with_client_cert() {
        let auth = ResolvedAuth::Mtls {
            ca: b"ca-bytes".to_vec(),
            cert: b"cert-bytes".to_vec(),
            key: b"key-bytes".to_vec(),
        };
        let options = build_options(&test_config(), &auth);

        assert!(options.credentials().is_none());
        match options.transport() {
            Transport::Tls(TlsConfiguration::Simple {
                ca,
                client_auth,
                ..
            }) => {
                assert_eq!(ca, b"ca-bytes");
                assert_eq!(
                    client_auth,
                    Some((b"cert-bytes".to_vec(), b"key-bytes".to_vec()))
                );
            }
            _ => panic!("expected Tls(Simple {{ .. }}) transport"),
        }
    }
}
