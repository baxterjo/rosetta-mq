use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::auth::AuthConfig;
use crate::decoder::DecoderConfig;
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
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub broker: BrokerConfig,
    #[serde(rename = "topic", default)]
    pub topics: Vec<TopicMapping>,
}

#[derive(Debug, Deserialize)]
pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    /// Absent when the broker requires no authentication -- serde defaults a missing
    /// `Option<T>` field to `None`, so existing configs without a `[broker.auth]` table keep
    /// parsing unchanged.
    pub auth: Option<AuthConfig>,
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
        let config: Config = toml::from_str(raw)?;
        config.validate()?;
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
        Ok(())
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
    fn rejects_protobuf_mapping_missing_required_field() {
        let err = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"

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
    fn parses_broker_mtls_auth() {
        let config = Config::parse(
            r#"
            [broker]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"

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

            [[topic]]
            topic_filter = "a/#/b"
            decoder = "utf8"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::InvalidTopicFilter { .. })));
    }

    #[test]
    fn rejects_malformed_toml() {
        assert!(matches!(Config::parse("not valid toml === "), Err(ConfigError::Parse(_))));
    }
}
