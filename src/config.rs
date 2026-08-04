use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::client::ConnectionConfig;
use crate::decoder::DecoderConfig;
use crate::protocol::Protocol;
use crate::topic::{TopicError, TopicFilter};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub connection: ConnectionConfig,
    #[serde(rename = "topic", default)]
    pub topics: Vec<TopicMapping>,
    /// Named, reusable decoder definitions (`[decoder.NAME]` in TOML), referenced from `[[topic]]`
    /// blocks via `RefOr::Ref`.
    #[serde(rename = "decoder", default)]
    pub decoders: HashMap<String, DecoderConfig>,
    #[serde(default)]
    pub engine: EngineConfig,
}

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
    #[error("engine.max_concurrent_decodes must be at least 1")]
    InvalidEngineConfig,
    #[error("topic_filter {topic_filter:?} references unknown decoder {decoder_ref:?}")]
    UnknownDecoderRef {
        topic_filter: String,
        decoder_ref: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// Maximum number of incoming messages decoded and republished concurrently.
    #[serde(default = "EngineConfig::default_max_concurrent_decodes")]
    pub max_concurrent_decodes: usize,
}

impl EngineConfig {
    fn default_max_concurrent_decodes() -> usize {
        100
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_decodes: Self::default_max_concurrent_decodes(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct TopicMapping {
    pub topic_filter: String,
    /// `None` subscribes to `topic_filter` without decoding or republishing anything -- a pure
    /// pass-through for visibility. `Some` decodes matches, either via a named reference into
    /// [`Config::decoders`] (`RefOr::Ref`) or an inline literal (`RefOr::Literal`).
    pub decoder: Option<RefOr<DecoderConfig>>,
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
        if let Some(connection) = value
            .get_mut("connection")
            .and_then(toml::Value::as_table_mut)
        {
            connection
                .entry("protocol")
                .or_insert_with(|| toml::Value::String("mqtt".to_string()));
        }
        let mut config: Config = value.try_into()?;
        config.validate()?;
        config.normalize();
        Ok(config)
    }

    /// Eagerly validates topic filters and decoder references at load time rather than at
    /// first-message time.
    fn validate(&self) -> Result<(), ConfigError> {
        for mapping in &self.topics {
            TopicFilter::parse(&mapping.topic_filter).map_err(|source| {
                ConfigError::InvalidTopicFilter {
                    topic_filter: mapping.topic_filter.clone(),
                    source,
                }
            })?;
            if let Some(RefOr::Ref(name)) = &mapping.decoder {
                if !self.decoders.contains_key(name) {
                    return Err(ConfigError::UnknownDecoderRef {
                        topic_filter: mapping.topic_filter.clone(),
                        decoder_ref: name.clone(),
                    });
                }
            }
        }
        if self.engine.max_concurrent_decodes == 0 {
            return Err(ConfigError::InvalidEngineConfig);
        }
        Ok(())
    }

    fn normalize(&mut self) {
        if let Protocol::Ws(ws) = &mut self.connection.protocol {
            ws.normalize();
        }
    }
}

/// Either a name referencing a shared definition elsewhere in the config (`Ref`), or the
/// definition written out inline (`Literal`). Used for `TopicMapping.decoder` so a topic can
/// either reuse a named `[decoder.NAME]` table or define a one-off decoder directly on itself.
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RefOr<T> {
    Ref(String),
    Literal(T),
}

impl<T> RefOr<T>
where
    T: Clone,
{
    pub fn resolve(&self, source: &HashMap<String, T>) -> Option<T> {
        match self {
            RefOr::Ref(s) => source.get(s).cloned(),
            RefOr::Literal(i) => Some(i).cloned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthConfig;

    const VALID: &str = r#"
        [connection]
        host = "127.0.0.1"
        port = 1883
        client_id = "rosetta-mq"
        tls = false

        [decoder.utf8]
        decoder = "utf8"

        [decoder.hex]
        decoder = "hexdump"

        [decoder.proto]
        decoder = "protobuf"
        proto_file = "schemas/device.proto"
        message_type = "device.v1.DeviceReading"

        [[topic]]
        topic_filter = "devices/+/raw"
        decoder = "utf8"

        [[topic]]
        topic_filter = "sensors/#"
        decoder = "hex"

        [[topic]]
        topic_filter = "devices/+/proto"
        decoder = "proto"
    "#;

    #[test]
    fn parses_valid_config() {
        let config = Config::parse(VALID).unwrap();
        assert_eq!(config.connection.host, "127.0.0.1");
        assert_eq!(config.connection.port, 1883);
        assert_eq!(config.connection.client_id, "rosetta-mq");
        assert_eq!(config.topics.len(), 3);
        assert_eq!(config.topics[0].topic_filter, "devices/+/raw");
        assert!(matches!(
            &config.topics[0].decoder,
            Some(RefOr::Ref(name)) if name == "utf8"
        ));
        assert!(matches!(
            config.decoders.get("utf8"),
            Some(DecoderConfig::Utf8)
        ));
        assert!(matches!(
            config.decoders.get("hex"),
            Some(DecoderConfig::Hexdump)
        ));
        match config.decoders.get("proto") {
            Some(DecoderConfig::Protobuf(cfg)) => {
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
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [decoder.json_template]
            decoder = "template"
            template = """
            topic: {{ topic }}
            device: {{ payload.device_id }}
            """

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = "json_template"
        "#,
        )
        .unwrap();

        match config.decoders.get("json_template") {
            Some(DecoderConfig::Template(cfg)) => {
                assert!(cfg.template.contains("topic: {{ topic }}"));
                assert!(cfg.template.contains("device: {{ payload.device_id }}"));
                assert!(matches!(
                    cfg.undefined_behavior,
                    crate::decoder::template::UndefinedBehavior::Strict
                ));
            }
            other => panic!("expected template decoder config, got {other:?}"),
        }
    }

    #[test]
    fn parses_template_mapping_undefined_behavior_override() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [decoder.json_template]
            decoder = "template"
            template = "{{ topic }}"
            undefined_behavior = "lenient"

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = "json_template"
        "#,
        )
        .unwrap();

        match config.decoders.get("json_template") {
            Some(DecoderConfig::Template(cfg)) => {
                assert!(matches!(
                    cfg.undefined_behavior,
                    crate::decoder::template::UndefinedBehavior::Lenient
                ));
            }
            other => panic!("expected template decoder config, got {other:?}"),
        }
    }

    #[test]
    fn rejects_template_mapping_missing_required_field() {
        let err = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [decoder.json_template]
            decoder = "template"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn rejects_protobuf_mapping_missing_required_field() {
        let err = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [decoder.proto]
            decoder = "protobuf"
            proto_file = "schemas/device.proto"
        "#,
        );
        assert!(matches!(err, Err(ConfigError::Parse(_))));
    }

    #[test]
    fn topic_without_decoder_parses_to_none_and_is_subscribe_only() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/status"
        "#,
        )
        .unwrap();

        assert!(config.topics[0].decoder.is_none());
    }

    #[test]
    fn rejects_reference_to_unknown_decoder() {
        let err = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = "does_not_exist"
        "#,
        );
        assert!(matches!(
            err,
            Err(ConfigError::UnknownDecoderRef { topic_filter, decoder_ref })
                if topic_filter == "devices/+/raw" && decoder_ref == "does_not_exist"
        ));
    }

    #[test]
    fn parses_inline_literal_decoder_on_topic() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [[topic]]
            topic_filter = "devices/+/raw"
            decoder = { decoder = "utf8" }
        "#,
        )
        .unwrap();

        assert!(matches!(
            config.topics[0].decoder,
            Some(RefOr::Literal(DecoderConfig::Utf8))
        ));
    }

    #[test]
    fn defaults_to_none_when_connection_auth_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert!(config.connection.auth.is_none());
    }

    #[test]
    fn defaults_to_mqtt_protocol_when_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert!(matches!(
            config.connection.protocol,
            crate::protocol::Protocol::Mqtt
        ));
    }

    #[test]
    fn parses_ws_protocol_with_path() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false
            protocol = "ws"
            path = "/mqtt"
        "#,
        )
        .unwrap();

        match config.connection.protocol {
            crate::protocol::Protocol::Ws(ws) => assert_eq!(ws.path, "/mqtt"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn normalizes_ws_path_missing_leading_slash() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false
            protocol = "ws"
            path = "mqtt"
        "#,
        )
        .unwrap();

        match config.connection.protocol {
            crate::protocol::Protocol::Ws(ws) => assert_eq!(ws.path, "/mqtt"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn parses_connection_mtls_auth() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true

            [connection.auth]
            method = "mtls"
            ca_file = "ca.pem"
            cert_file = "client.pem"
            key_file = "client.key"
        "#,
        )
        .unwrap();

        match config.connection.auth {
            Some(AuthConfig::Mtls(cfg)) => {
                assert_eq!(cfg.ca_file, Path::new("ca.pem"));
                assert_eq!(cfg.cert_file, Path::new("client.pem"));
                assert_eq!(cfg.key_file, Path::new("client.key"));
            }
            other => panic!("expected mtls auth config, got {other:?}"),
        }
    }

    #[test]
    fn parses_connection_userpass_auth() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [connection.auth]
            method = "userpass"
            username = "device-reader"
            password = { env = "MQTT_PASSWORD" }
        "#,
        )
        .unwrap();

        match config.connection.auth {
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
            [connection]
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
            [connection]
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
            [connection]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true
        "#,
        )
        .unwrap();
        assert!(config.connection.tls);
    }

    #[test]
    fn rejects_connection_missing_tls() {
        let err = Config::parse(
            r#"
            [connection]
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
        assert!(!config.connection.allow_self_signed_certs);
    }

    #[test]
    fn parses_allow_self_signed_certs_true() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 8883
            client_id = "x"
            tls = true
            allow_self_signed_certs = true
        "#,
        )
        .unwrap();
        assert!(config.connection.allow_self_signed_certs);
    }

    #[test]
    fn defaults_max_concurrent_decodes_when_engine_omitted() {
        let config = Config::parse(VALID).unwrap();
        assert_eq!(config.engine.max_concurrent_decodes, 100);
    }

    #[test]
    fn parses_engine_max_concurrent_decodes() {
        let config = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [engine]
            max_concurrent_decodes = 5
        "#,
        )
        .unwrap();
        assert_eq!(config.engine.max_concurrent_decodes, 5);
    }

    #[test]
    fn rejects_zero_max_concurrent_decodes() {
        let err = Config::parse(
            r#"
            [connection]
            host = "127.0.0.1"
            port = 1883
            client_id = "x"
            tls = false

            [engine]
            max_concurrent_decodes = 0
        "#,
        );
        assert!(matches!(err, Err(ConfigError::InvalidEngineConfig)));
    }

    #[test]
    fn rejects_malformed_toml() {
        assert!(matches!(
            Config::parse("not valid toml === "),
            Err(ConfigError::Parse(_))
        ));
    }
}
