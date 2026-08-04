use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::auth::AuthConfig;
use crate::decoder::DecoderConfig;
use crate::protocol::Protocol;
use crate::topic::{TopicError, TopicFilter};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid topic_filter {topic_filter:?}: {source}")]
    InvalidTopicFilter {
        topic_filter: String,
        #[source]
        source: TopicError,
    },
    #[error("pipeline.max_concurrent_decodes must be at least 1")]
    InvalidPipelineConfig,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub broker: BrokerConfig,
    #[serde(rename = "topic", default)]
    pub topics: Vec<TopicMapping>,
    #[serde(default)]
    pub pipeline: PipelineConfig,
}

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    /// Maximum number of incoming messages decoded and republished concurrently.
    #[serde(default = "PipelineConfig::default_max_concurrent_decodes")]
    pub max_concurrent_decodes: usize,
}

impl PipelineConfig {
    fn default_max_concurrent_decodes() -> usize {
        100
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_decodes: Self::default_max_concurrent_decodes(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub auth: Option<AuthConfig>,
    /// Use TLS (or WSS when using websockets)
    pub tls: bool,
    /// Which protocol carries the connection. Defaults to `mqtt` (plain TCP) when omitted.
    #[serde(flatten, default)]
    pub protocol: Protocol,
    /// When `tls` is set, accept the broker's certificate without any verification (no root
    /// store, no hostname check) -- for self-hosted/dev brokers using a self-signed cert where
    /// distributing a CA file isn't practical. Has no effect when `tls` is `false`.
    #[serde(default)]
    pub allow_self_signed_certs: bool,
}

#[derive(Debug, Deserialize)]
pub struct TopicMapping {
    pub topic_filter: String,
    #[serde(flatten)]
    pub decoder: DecoderConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::parse(&raw)
    }

    fn parse(raw: &str) -> Result<Self, ConfigError> {
        let mut value: toml::Value = toml::from_str(raw)?;
        // `Protocol`'s `#[serde(flatten)]` can't fall back to its `Default` impl when the
        // `protocol` key is missing entirely -- known serde limitation, flatten + default don't
        // compose for enums (https://github.com/serde-rs/serde/issues/1626) -- so inject the
        // default explicitly before deserializing.
        if let Some(broker) = value.get_mut("broker").and_then(toml::Value::as_table_mut) {
            broker
                .entry("protocol")
                .or_insert_with(|| toml::Value::String("mqtt".to_string()));
        }
        let mut config: Config = value.try_into()?;
        config.validate()?;
        config.normalize();
        Ok(config)
    }

    /// Eagerly validates topic filters at load time rather than at first-message time.
    fn validate(&self) -> Result<(), ConfigError> {
        for mapping in &self.topics {
            TopicFilter::parse(&mapping.topic_filter).map_err(|source| {
                ConfigError::InvalidTopicFilter {
                    topic_filter: mapping.topic_filter.clone(),
                    source,
                }
            })?;
        }
        if self.pipeline.max_concurrent_decodes == 0 {
            return Err(ConfigError::InvalidPipelineConfig);
        }
        Ok(())
    }

    fn normalize(&mut self) {
        if let Protocol::Ws(ws) = &mut self.broker.protocol {
            ws.normalize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
        [broker]
        host = "127.0.0.1"
        port = 1883
        client_id = "rosetta-mq"
        tls = false

        [[topic]]
        topic_filter = "devices/+/raw"
        decoder = "utf8"

        [[topic]]
        topic_filter = "sensors/#"
        decoder = "hexdump"

        [[topic]]
        topic_filter = "devices/+/proto"
        decoder = "protobuf"
        proto_file = "schemas/device.proto"
        message_type = "device.v1.DeviceReading"
    "#;

    #[test]
    fn parses_valid_config() {
        let config = Config::parse(VALID).unwrap();
        assert_eq!(config.broker.host, "127.0.0.1");
        assert_eq!(config.broker.port, 1883);
        assert_eq!(config.broker.client_id, "rosetta-mq");
        assert_eq!(config.topics.len(), 3);
        assert_eq!(config.topics[0].topic_filter, "devices/+/raw");
        assert!(matches!(config.topics[0].decoder, DecoderConfig::Utf8));
        assert!(matches!(config.topics[1].decoder, DecoderConfig::Hexdump));
        match &config.topics[2].decoder {
            DecoderConfig::Protobuf(cfg) => {
                assert_eq!(cfg.proto_file, "schemas/device.proto");
                assert_eq!(cfg.message_type, "device.v1.DeviceReading");
                assert!(cfg.include_paths.is_empty());
            }
            other => panic!("expected protobuf decoder config, got {other:?}"),
        }
    }

    #[test]
    fn parses_template_mapping_with_multiline_toml_string() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = "template"
            template = """
            topic: {{ topic }}
            device: {{ payload.device_id }}
            """
        "#,
        )
        .unwrap();

        match &config.topics[0].decoder {
            DecoderConfig::Template(cfg) => {
                assert!(cfg.template.contains("topic: {{ topic }}"));
                assert!(cfg.template.contains("device: {{ payload.device_id }}"));
            }
            other => panic!("expected template decoder config, got {other:?}"),
        }
    }

    #[test]
    fn rejects_template_mapping_missing_required_field() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = "template"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn rejects_protobuf_mapping_missing_required_field() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/proto"
            decoder = "protobuf"
            proto_file = "schemas/device.proto"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn defaults_to_none_when_broker_auth_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert!(config.broker.auth.is_none());
    }

    #[test]
    fn defaults_to_mqtt_protocol_when_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert!(matches!(
            config.broker.protocol,
            crate::protocol::Protocol::Mqtt
        ));
    }

    #[test]
    fn parses_ws_protocol_with_path() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false
            protocol = "ws"
            path = "/mqtt"
        "#,
        )
        .unwrap();

        match config.broker.protocol {
            crate::protocol::Protocol::Ws(ws) => assert_eq!(ws.path, "/mqtt"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_ws_path_missing_leading_slash() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false
            protocol = "ws"
            path = "mqtt"
        "#,
        )
        .unwrap();

        match config.broker.protocol {
            crate::protocol::Protocol::Ws(ws) => assert_eq!(ws.path, "/mqtt"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn parses_broker_mtls_auth() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true

            [broker.auth]
            method = "mtls"
            ca_file = "ca.pem"
            cert_file = "client.pem"
            key_file = "client.key"
        "#,
        )
        .unwrap();

        match config.broker.auth {
            Some(AuthConfig::Mtls(cfg)) => {
                assert_eq!(cfg.ca_file, Path::new("ca.pem"));
                assert_eq!(cfg.cert_file, Path::new("client.pem"));
                assert_eq!(cfg.key_file, Path::new("client.key"));
            }
            other => panic!("expected mtls auth config, got {other:?}"),
        }
    }

    #[test]
    fn parses_broker_userpass_auth() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [broker.auth]
            method = "userpass"
            username = "device-reader"
            password = { env = "MQTT_PASSWORD" }
        "#,
        )
        .unwrap();

        match config.broker.auth {
            Some(AuthConfig::UserPass(cfg)) => {
                assert_eq!(cfg.username, "device-reader");
                assert!(
                    matches!(cfg.password, crate::auth::PasswordSource::Env { env } if env == "MQTT_PASSWORD")
                );
            }
            other => panic!("expected userpass auth config, got {other:?}"),
        }
    }

    #[test]
    fn defaults_to_empty_topics_when_omitted() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false
        "#,
        )
        .unwrap();
        assert!(config.topics.is_empty());
    }

    #[test]
    fn rejects_invalid_topic_filter_at_load_time() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "a/#/b"
            decoder = "utf8"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::InvalidTopicFilter { .. })));
    }

    #[test]
    fn parses_tls_true() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true
        "#,
        )
        .unwrap();
        assert!(config.broker.tls);
    }

    #[test]
    fn rejects_broker_missing_tls() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn allow_self_signed_certs_defaults_to_false() {
        let config = Config::parse(VALID).unwrap();
        assert!(!config.broker.allow_self_signed_certs);
    }

    #[test]
    fn parses_allow_self_signed_certs_true() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true
            allow_self_signed_certs = true
        "#,
        )
        .unwrap();
        assert!(config.broker.allow_self_signed_certs);
    }

    #[test]
    fn defaults_max_concurrent_decodes_when_pipeline_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert_eq!(config.pipeline.max_concurrent_decodes, 100);
    }

    #[test]
    fn parses_pipeline_max_concurrent_decodes() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [pipeline]
            max_concurrent_decodes = 5
        "#,
        )
        .unwrap();
        assert_eq!(config.pipeline.max_concurrent_decodes, 5);
    }

    #[test]
    fn rejects_zero_max_concurrent_decodes() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [pipeline]
            max_concurrent_decodes = 0
        "#,
        );
        assert!(matches!(err, Err(ConfigError::InvalidPipelineConfig)));
    }

    #[test]
    fn rejects_malformed_toml() {
        assert!(matches!(
            Config::parse("not valid toml === "),
            Err(ConfigError::Parse(_))
        ));
    }
}
