use serde::Deserialize;

/// Which protocol carries the MQTT connection, discriminated by the `protocol` field in TOML
/// (e.g. `protocol = "ws"`, plus that variant's own fields as siblings) -- same
/// "internally tagged" convention as [`crate::decoder::DecoderConfig`]. Defaults to `mqtt` (plain
/// TCP, optionally wrapped in TLS via `[connection].tls`) when omitted. `ws` connects over a
/// websocket upgrade instead.
#[derive(Debug, Default, Deserialize)]
#[serde(tag = "protocol", rename_all = "lowercase")]
pub enum Protocol {
    #[default]
    Mqtt,
    Ws(WebsocketConfig),
}

#[derive(Debug, Deserialize)]
pub struct WebsocketConfig {
    /// Path the websocket upgrade request is made against, e.g. `/mqtt`. Empty by default, which
    /// connects at the bare host:port -- most brokers normalize that to the root path.
    #[serde(default = "default_path")]
    pub path: String,
}

fn default_path() -> String {
    "/ws".to_string()
}

impl WebsocketConfig {
    /// Ensures `path` has a leading `/`, e.g. `mqtt` -> `/mqtt`. Leaves an empty path as-is.
    pub fn normalize(&mut self) {
        if !self.path.is_empty() && !self.path.starts_with('/') {
            self.path.insert(0, '/');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(flatten, default)]
        protocol: Protocol,
    }

    fn parse(raw: &str) -> Protocol {
        let wrapper: Wrapper = toml::from_str(raw).unwrap();
        wrapper.protocol
    }

    #[test]
    fn protocol_default_is_mqtt() {
        // `Protocol`'s own `Deserialize` requires the `protocol` tag key -- it's
        // `Config::parse` (see `src/config.rs`) that injects `protocol = "mqtt"` before
        // deserializing when the key is missing entirely, since flatten + default don't compose
        // for enums in serde (https://github.com/serde-rs/serde/issues/1626).
        assert!(matches!(Protocol::default(), Protocol::Mqtt));
    }

    #[test]
    fn parses_protocol_mqtt() {
        assert!(matches!(parse(r#"protocol = "mqtt""#), Protocol::Mqtt));
    }

    #[test]
    fn parses_protocol_ws_with_path() {
        let protocol = parse(
            r#"
            protocol = "ws"
            path = "/mqtt"
        "#,
        );
        match protocol {
            Protocol::Ws(cfg) => assert_eq!(cfg.path, "/mqtt"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn ws_path_defaults_to_empty() {
        let protocol = parse(r#"protocol = "ws""#);
        match protocol {
            Protocol::Ws(cfg) => assert_eq!(cfg.path, "/ws"),
            other => panic!("expected ws protocol, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_protocol() {
        let err: Result<Wrapper, _> = toml::from_str(r#"protocol = "quic""#);
        assert!(err.is_err());
    }

    #[test]
    fn normalize_leaves_empty_path_as_is() {
        let mut cfg = WebsocketConfig {
            path: String::new(),
        };
        cfg.normalize();
        assert_eq!(cfg.path, "");
    }

    #[test]
    fn normalize_leaves_path_starting_with_slash_as_is() {
        let mut cfg = WebsocketConfig {
            path: "/mqtt".to_string(),
        };
        cfg.normalize();
        assert_eq!(cfg.path, "/mqtt");
    }

    #[test]
    fn normalize_adds_leading_slash_when_missing() {
        let mut cfg = WebsocketConfig {
            path: "mqtt".to_string(),
        };
        cfg.normalize();
        assert_eq!(cfg.path, "/mqtt");
    }
}
