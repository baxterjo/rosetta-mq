use async_trait::async_trait;
use minijinja::{Environment, Value, context};
use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::decoder::{DecodeError, DecodePublish, Decoder};

/// There's exactly one template per decoder instance, registered under this name -- never
/// user-facing, just an internal key into the `minijinja::Environment`.
const TEMPLATE_NAME: &str = "message";

/// Mirrors [`minijinja::UndefinedBehavior`] for TOML config. See [`minijinja::UndefinedBehavior`]'s own docs for
/// exact per-variant semantics (printing/iteration/attribute-access/truthiness).
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UndefinedBehavior {
    Lenient,
    Chainable,
    SemiStrict,
    #[default]
    Strict,
}

impl From<UndefinedBehavior> for minijinja::UndefinedBehavior {
    fn from(value: UndefinedBehavior) -> Self {
        match value {
            UndefinedBehavior::Lenient => minijinja::UndefinedBehavior::Lenient,
            UndefinedBehavior::Chainable => minijinja::UndefinedBehavior::Chainable,
            UndefinedBehavior::SemiStrict => minijinja::UndefinedBehavior::SemiStrict,
            UndefinedBehavior::Strict => minijinja::UndefinedBehavior::Strict,
        }
    }
}

/// Per-topic config for the template decoder: the Jinja2-compatible template text itself, written
/// directly in the TOML config.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TemplateConfig {
    pub template: String,
    #[serde(default)]
    pub undefined_behavior: UndefinedBehavior,
}

#[derive(Debug, Error)]
pub enum TemplateDecoderError {
    #[error("invalid template syntax: {0}")]
    Parse(#[source] minijinja::Error),
    #[error("template render failed: {0}")]
    Render(#[source] minijinja::Error),
}

/// Renders an incoming publish through a user-authored Jinja2-compatible template (via
/// [`minijinja`]). The template gets the whole `Publish` packet as context (`topic`, `qos`,
/// `retain`, `dup`, `pkid`), plus `payload`: parsed and indexable (`payload.field`, `payload[0]`,
/// ...) when the payload is JSON, otherwise a plain string, or a hex string when the payload
/// isn't even valid UTF-8. Undefined references (a missing JSON field, indexing into a non-JSON
/// payload, a typo'd variable name) are a hard render error rather than blank output by default
/// -- configurable per topic via [`TemplateConfig::undefined_behavior`].
#[derive(Debug)]
pub struct TemplateDecoder {
    env: Environment<'static>,
}

impl TemplateDecoder {
    pub fn from_config(cfg: &TemplateConfig) -> Result<Self, TemplateDecoderError> {
        let mut env = Environment::new();
        env.set_undefined_behavior(cfg.undefined_behavior.into());
        env.add_template_owned(TEMPLATE_NAME, cfg.template.clone())
            .map_err(TemplateDecoderError::Parse)?;
        Ok(Self { env })
    }
}

/// Builds the `payload` context value: parsed JSON (indexable) when the payload is valid UTF-8
/// and valid JSON, a plain string when it's valid UTF-8 but not JSON, or a hex string when it
/// isn't even valid UTF-8 -- so `payload` is always renderable, but only indexable when the
/// payload is actually structured.
fn payload_value(payload: &[u8]) -> Value {
    match std::str::from_utf8(payload) {
        Ok(text) => match serde_json::from_str::<serde_json::Value>(text) {
            Ok(json) => Value::from_serialize(&json),
            Err(_) => Value::from(text),
        },
        Err(_) => Value::from(hex::encode(payload)),
    }
}

#[async_trait]
impl Decoder for TemplateDecoder {
    type Error = TemplateDecoderError;

    fn name(&self) -> &str {
        "template"
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>> {
        let template = self
            .env
            .get_template(TEMPLATE_NAME)
            .expect("registered in from_config");

        let ctx = context! {
            topic => publish.topic.as_str(),
            qos => format!("{:?}", publish.qos),
            retain => publish.retain,
            dup => publish.dup,
            pkid => publish.pkid,
            payload => payload_value(&publish.payload),
        };

        let rendered = template
            .render(ctx)
            .map_err(|e| DecodeError::Decode(TemplateDecoderError::Render(e)))?;

        tx.send(DecodePublish {
            payload: rendered.into_bytes(),
            ..Default::default()
        })
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::QoS;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn indexes_json_payload_fields() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ payload.device_id }} is {{ payload.temperature_c }}C".to_string(),
            ..Default::default()
        })
        .unwrap();
        let publish = Publish::new(
            "devices/42/raw",
            QoS::AtLeastOnce,
            br#"{"device_id": "sensor-42", "temperature_c": 21.5}"#.to_vec(),
        );

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"sensor-42 is 21.5C");
    }

    #[tokio::test]
    async fn renders_publish_packet_fields() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ topic }} {{ qos }} {{ retain }} {{ dup }} {{ pkid }}".to_string(),
            ..Default::default()
        })
        .unwrap();
        let mut publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hi".to_vec());
        publish.retain = true;
        publish.dup = true;
        publish.pkid = 7;

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"devices/42/raw AtLeastOnce true true 7");
    }

    #[tokio::test]
    async fn treats_plain_text_payload_as_string() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "raw: {{ payload | upper }}".to_string(),
            ..Default::default()
        })
        .unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hello device".to_vec());

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"raw: HELLO DEVICE");
    }

    #[tokio::test]
    async fn renders_non_utf8_payload_as_hex() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ payload }}".to_string(),
            ..Default::default()
        })
        .unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0x00, 0x10]);

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"ff0010");
    }

    #[tokio::test]
    async fn indexing_non_json_payload_is_a_decode_failure() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ payload.device_id }}".to_string(),
            ..Default::default()
        })
        .unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"not json".to_vec());

        let (tx, _rx) = mpsc::channel(1);
        let err = decoder.decode(&publish, tx).await.unwrap_err();
        assert!(matches!(
            err,
            DecodeError::Decode(TemplateDecoderError::Render(_))
        ));
    }

    #[tokio::test]
    async fn undefined_variable_is_a_decode_failure() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ does_not_exist }}".to_string(),
            ..Default::default()
        })
        .unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hi".to_vec());

        let (tx, _rx) = mpsc::channel(1);
        let err = decoder.decode(&publish, tx).await.unwrap_err();
        assert!(matches!(
            err,
            DecodeError::Decode(TemplateDecoderError::Render(_))
        ));
    }

    #[tokio::test]
    async fn lenient_undefined_behavior_renders_blank_instead_of_failing() {
        let decoder = TemplateDecoder::from_config(&TemplateConfig {
            template: "[{{ does_not_exist }}]".to_string(),
            undefined_behavior: UndefinedBehavior::Lenient,
        })
        .unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hi".to_vec());

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"[]");
    }

    #[test]
    fn fails_to_construct_with_invalid_template_syntax() {
        let err = TemplateDecoder::from_config(&TemplateConfig {
            template: "{{ unclosed".to_string(),
            ..Default::default()
        })
        .unwrap_err();
        assert!(matches!(err, TemplateDecoderError::Parse(_)));
    }
}
