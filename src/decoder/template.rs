use async_trait::async_trait;
use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::decoder::context::{self, CompiledTemplate};
use crate::decoder::{DecodeError, DecodePublish, Decoder};

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
/// directly in the TOML config. `template` compiles as soon as it's deserialized (see
/// [`CompiledTemplate`]), so a syntax error fails at config-load time, not on the first message.
#[derive(Debug, Clone, Deserialize)]
pub struct TemplateConfig {
    pub template: CompiledTemplate,
    #[serde(default)]
    pub undefined_behavior: UndefinedBehavior,
}

#[derive(Debug, Error)]
pub enum TemplateDecoderError {
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
    template: CompiledTemplate,
}

impl TemplateDecoder {
    pub fn from_config(cfg: &TemplateConfig) -> Self {
        let mut template = cfg.template.clone();
        template.set_undefined_behavior(cfg.undefined_behavior.into());
        Self { template }
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
        let rendered = self
            .template
            .render(context::publish_context(publish))
            .map_err(|e| DecodeError::Decode(TemplateDecoderError::Render(e)))?;

        tx.send(DecodePublish {
            payload: rendered.into_bytes(),
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

    fn config(template: &str) -> TemplateConfig {
        TemplateConfig {
            template: CompiledTemplate::new(template).unwrap(),
            undefined_behavior: UndefinedBehavior::default(),
        }
    }

    #[tokio::test]
    async fn indexes_json_payload_fields() {
        let decoder =
            TemplateDecoder::from_config(&config("{{ payload.device_id }} is {{ payload.temperature_c }}C"));
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
        let decoder = TemplateDecoder::from_config(&config(
            "{{ topic }} {{ qos }} {{ retain }} {{ dup }} {{ pkid }}",
        ));
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
        let decoder = TemplateDecoder::from_config(&config("raw: {{ payload | upper }}"));
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hello device".to_vec());

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"raw: HELLO DEVICE");
    }

    #[tokio::test]
    async fn renders_non_utf8_payload_as_hex() {
        let decoder = TemplateDecoder::from_config(&config("{{ payload }}"));
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0x00, 0x10]);

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"ff0010");
    }

    #[tokio::test]
    async fn indexing_non_json_payload_is_a_decode_failure() {
        let decoder = TemplateDecoder::from_config(&config("{{ payload.device_id }}"));
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
        let decoder = TemplateDecoder::from_config(&config("{{ does_not_exist }}"));
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
            template: CompiledTemplate::new("[{{ does_not_exist }}]").unwrap(),
            undefined_behavior: UndefinedBehavior::Lenient,
        });
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hi".to_vec());

        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"[]");
    }
}
