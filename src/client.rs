use std::sync::Arc;
use std::time::Duration;

use rumqttc::tokio_rustls::rustls::{
    self,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, Publish, QoS, TlsConfiguration, Transport};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::auth::ResolvedAuth;
use crate::config::BrokerConfig;
use crate::protocol::Protocol;

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("mqtt connection error: {0}")]
    Connection(#[from] rumqttc::ConnectionError),
}

#[derive(Debug, Error)]
pub enum ClientInitError {
    #[error("failed to parse {context} PEM: {source}")]
    PemParse {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },
    #[error("no private key found in client key file")]
    MissingPrivateKey,
    #[error("failed to load platform root certificates: {0:?}")]
    NativeCerts(Vec<rustls_native_certs::Error>),
    #[error("invalid client certificate/key: {0}")]
    InvalidClientCert(#[from] rustls::Error),
}

fn parse_pem_certs(
    pem: &[u8],
    context: &'static str,
) -> Result<Vec<CertificateDer<'static>>, ClientInitError> {
    rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| ClientInitError::PemParse { context, source })
}

fn parse_pem_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, ClientInitError> {
    rustls_pemfile::private_key(&mut &pem[..])
        .map_err(|source| ClientInitError::PemParse {
            context: "client private key",
            source,
        })?
        .ok_or(ClientInitError::MissingPrivateKey)
}

/// A live connection to an MQTT broker: a client handle for publishing/subscribing, a channel of
/// incoming messages, and the background task driving the underlying connection.
pub struct MqttConnection {
    pub client: AsyncClient,
    pub incoming: mpsc::Receiver<Publish>,
    pub driver: JoinHandle<Result<(), BrokerError>>,
}

/// Builds the `MqttOptions` for `config`, applying `tls`/`allow_self_signed_certs`, `protocol`,
/// and `auth` on top.
///
/// `config.tls = false` is ignored if `broker.auth.method = mtls`
fn build_options(
    config: &BrokerConfig,
    auth: &ResolvedAuth,
) -> Result<MqttOptions, ClientInitError> {
    let tls = if !config.tls && matches!(auth, ResolvedAuth::Mtls { .. }) {
        tracing::warn!(
            "[broker.auth] method = \"mtls\" requires TLS; ignoring broker.tls = false and \
             connecting with TLS anyway"
        );
        true
    } else {
        config.tls
    };

    // `protocol = "ws"` takes the full ws(s)://host:port/path URL as the "host" argument --
    // rumqttc parses domain/port back out of it at connect time and ignores the separate numeric
    // port field in that case.
    let mut options = match &config.protocol {
        Protocol::Mqtt => {
            tracing::info!("connecting to mqtt://{}:{}", config.host, config.port);
            MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port)
        }
        Protocol::Ws(ws) => {
            let scheme = if tls { "wss" } else { "ws" };
            let url = format!("{scheme}://{}:{}{}", config.host, config.port, ws.path);
            tracing::info!("connecting to {url}");
            MqttOptions::new(config.client_id.clone(), url, config.port)
        }
    };
    options.set_keep_alive(Duration::from_secs(30));

    if tls {
        let tls_config = build_tls_config(config, auth)?;
        options.set_transport(match &config.protocol {
            Protocol::Mqtt => Transport::Tls(tls_config),
            Protocol::Ws(_) => Transport::Wss(tls_config),
        });
    } else if matches!(&config.protocol, Protocol::Ws(_)) {
        options.set_transport(Transport::Ws);
    }

    if let ResolvedAuth::UserPass { username, password } = auth {
        options.set_credentials(username.clone(), password.clone());
    }

    Ok(options)
}

/// Accepts any server certificate without verification -- used only when
/// `allow_self_signed_certs` is set.
#[derive(Debug)]
struct NoServerCertVerification;

impl ServerCertVerifier for NoServerCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA1,
            SignatureScheme::ECDSA_SHA1_Legacy,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::ED448,
        ]
    }
}

/// Builds the `TlsConfiguration` for an encrypted connection.
fn build_tls_config(
    config: &BrokerConfig,
    auth: &ResolvedAuth,
) -> Result<TlsConfiguration, ClientInitError> {
    // Configure how the server is verified.
    let verifier_builder = if config.allow_self_signed_certs {
        // If `allow_self_signed_certs == true` then put in a custom verifier automatically
        // verifies the server with a no-op.
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoServerCertVerification))
    } else {
        let mut roots = RootCertStore::empty();
        match auth {
            // If auth method is Mtls then the provided CA will be used to verify the server.
            ResolvedAuth::Mtls { ca, .. } => {
                roots.add_parsable_certificates(parse_pem_certs(ca, "CA certificate")?);
            }
            // Otherwise use native certs
            ResolvedAuth::None | ResolvedAuth::UserPass { .. } => {
                let native_certs = rustls_native_certs::load_native_certs();
                if !native_certs.errors.is_empty() {
                    return Err(ClientInitError::NativeCerts(native_certs.errors));
                }
                roots.add_parsable_certificates(native_certs.certs);
            }
        }
        ClientConfig::builder().with_root_certificates(roots)
    };

    // Configure how the client is authenticated.
    let client_config = match auth {
        ResolvedAuth::Mtls { cert, key, .. } => {
            let cert_chain = parse_pem_certs(cert, "client certificate")?;
            let key_der = parse_pem_private_key(key)?;
            verifier_builder.with_client_auth_cert(cert_chain, key_der)?
        }
        // UN/PW auth is handled in rumqttc, not rustls.
        ResolvedAuth::None | ResolvedAuth::UserPass { .. } => {
            verifier_builder.with_no_client_auth()
        }
    };

    Ok(TlsConfiguration::Rustls(Arc::new(client_config)))
}

pub struct Client;

impl Client {
    /// Connects to the configured broker and spawns a background task that continuously polls
    /// the connection (required to keep it alive and drive publish acks), forwarding incoming
    /// Publish packets onto a channel.
    pub fn connect(
        config: &BrokerConfig,
        auth: &ResolvedAuth,
    ) -> Result<MqttConnection, ClientInitError> {
        let options = build_options(config, auth)?;

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

        Ok(MqttConnection {
            client,
            incoming: rx,
            driver,
        })
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
    use crate::protocol::WebsocketConfig;

    fn test_config() -> BrokerConfig {
        BrokerConfig {
            host: "127.0.0.1".to_string(),
            port: 1883,
            client_id: "rosetta-mq-test".to_string(),
            auth: None,
            tls: false,
            protocol: Protocol::Mqtt,
            allow_self_signed_certs: false,
        }
    }

    fn websocket_config() -> BrokerConfig {
        BrokerConfig {
            protocol: Protocol::Ws(WebsocketConfig {
                path: "/mqtt".to_string(),
            }),
            ..test_config()
        }
    }

    #[test]
    fn no_auth_leaves_options_unauthenticated() {
        let options = build_options(&test_config(), &ResolvedAuth::None).unwrap();
        assert!(options.credentials().is_none());
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn userpass_auth_sets_credentials() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let options = build_options(&test_config(), &auth).unwrap();

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert_eq!(login.password, "supersecret");
        assert!(matches!(options.transport(), Transport::Tcp));
    }

    #[test]
    fn mtls_auth_sets_tls_transport_with_client_cert() {
        // build_tls_config parses `ca`/`cert`/`key` as real PEM (it builds a `RootCertStore` from
        // `ca` and a client identity from `cert`/`key` itself now), so this needs real fixture
        // PEM input rather than placeholder bytes.
        let auth = ResolvedAuth::Mtls {
            ca: include_bytes!("../tests/fixtures/tls/ca.pem").to_vec(),
            cert: include_bytes!("../tests/fixtures/tls/client-cert.pem").to_vec(),
            key: include_bytes!("../tests/fixtures/tls/client-key.pem").to_vec(),
        };
        let config = BrokerConfig {
            tls: true,
            ..test_config()
        };
        let options = build_options(&config, &auth).unwrap();

        assert!(options.credentials().is_none());
        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn tls_with_no_auth_uses_native_root_store() {
        let config = BrokerConfig {
            tls: true,
            ..test_config()
        };
        let options = build_options(&config, &ResolvedAuth::None).unwrap();

        assert!(options.credentials().is_none());
        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn tls_with_userpass_uses_native_root_store_and_sets_credentials() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let config = BrokerConfig {
            tls: true,
            ..test_config()
        };
        let options = build_options(&config, &auth).unwrap();

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert_eq!(login.password, "supersecret");
        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn mtls_without_tls_connects_with_tls_anyway() {
        // build_tls_config actually runs here (tls ends up true despite tls: false on the
        // config), so this needs real fixture PEM input, not placeholder bytes.
        let auth = ResolvedAuth::Mtls {
            ca: include_bytes!("../tests/fixtures/tls/ca.pem").to_vec(),
            cert: include_bytes!("../tests/fixtures/tls/client-cert.pem").to_vec(),
            key: include_bytes!("../tests/fixtures/tls/client-key.pem").to_vec(),
        };
        let options = build_options(&test_config(), &auth).unwrap();

        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
        assert!(options.credentials().is_none());
    }

    #[test]
    fn allow_self_signed_certs_skips_verification_with_mtls_client_cert() {
        // Real PEM-encoded self-signed cert + key, and a matching client cert -- the insecure
        // path still parses the client cert/key via rustls-pemfile, so it needs real PEM input,
        // not placeholder bytes.
        let cert_pem = include_bytes!("../tests/fixtures/tls/client-cert.pem").to_vec();
        let key_pem = include_bytes!("../tests/fixtures/tls/client-key.pem").to_vec();
        let auth = ResolvedAuth::Mtls {
            ca: b"unused-when-self-signed-certs-are-allowed".to_vec(),
            cert: cert_pem,
            key: key_pem,
        };
        let config = BrokerConfig {
            tls: true,
            allow_self_signed_certs: true,
            ..test_config()
        };
        let options = build_options(&config, &auth).unwrap();

        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn allow_self_signed_certs_skips_verification_with_userpass() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let config = BrokerConfig {
            tls: true,
            allow_self_signed_certs: true,
            ..test_config()
        };
        let options = build_options(&config, &auth).unwrap();

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert!(matches!(
            options.transport(),
            Transport::Tls(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn websocket_no_tls_uses_ws_transport_and_url_host() {
        let options = build_options(&websocket_config(), &ResolvedAuth::None).unwrap();

        assert!(options.credentials().is_none());
        assert!(matches!(options.transport(), Transport::Ws));
        assert_eq!(
            options.broker_address(),
            ("ws://127.0.0.1:1883/mqtt".to_string(), 1883)
        );
    }

    #[test]
    fn websocket_no_tls_userpass_sets_credentials_and_ws_transport() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let options = build_options(&websocket_config(), &auth).unwrap();

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert_eq!(login.password, "supersecret");
        assert!(matches!(options.transport(), Transport::Ws));
    }

    #[test]
    fn websocket_tls_mtls_sets_wss_transport_with_client_cert() {
        let auth = ResolvedAuth::Mtls {
            ca: include_bytes!("../tests/fixtures/tls/ca.pem").to_vec(),
            cert: include_bytes!("../tests/fixtures/tls/client-cert.pem").to_vec(),
            key: include_bytes!("../tests/fixtures/tls/client-key.pem").to_vec(),
        };
        let config = BrokerConfig {
            tls: true,
            ..websocket_config()
        };
        let options = build_options(&config, &auth).unwrap();

        assert!(options.credentials().is_none());
        assert!(matches!(
            options.transport(),
            Transport::Wss(TlsConfiguration::Rustls(_))
        ));
        assert_eq!(
            options.broker_address(),
            ("wss://127.0.0.1:1883/mqtt".to_string(), 1883)
        );
    }

    #[test]
    fn websocket_tls_userpass_uses_native_root_store_and_sets_credentials() {
        let auth = ResolvedAuth::UserPass {
            username: "device-reader".to_string(),
            password: "supersecret".to_string(),
        };
        let config = BrokerConfig {
            tls: true,
            ..websocket_config()
        };
        let options = build_options(&config, &auth).unwrap();

        let login = options.credentials().expect("credentials to be set");
        assert_eq!(login.username, "device-reader");
        assert_eq!(login.password, "supersecret");
        assert!(matches!(
            options.transport(),
            Transport::Wss(TlsConfiguration::Rustls(_))
        ));
    }

    #[test]
    fn websocket_mtls_without_tls_connects_with_wss_anyway() {
        let auth = ResolvedAuth::Mtls {
            ca: include_bytes!("../tests/fixtures/tls/ca.pem").to_vec(),
            cert: include_bytes!("../tests/fixtures/tls/client-cert.pem").to_vec(),
            key: include_bytes!("../tests/fixtures/tls/client-key.pem").to_vec(),
        };
        let options = build_options(&websocket_config(), &auth).unwrap();

        assert!(matches!(
            options.transport(),
            Transport::Wss(TlsConfiguration::Rustls(_))
        ));
        assert_eq!(
            options.broker_address(),
            ("wss://127.0.0.1:1883/mqtt".to_string(), 1883)
        );
        assert!(options.credentials().is_none());
    }
}
