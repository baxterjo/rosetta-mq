use minijinja::{Environment, Value, context};
use rumqttc::Publish;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Never user-facing, just an internal key into each [`CompiledTemplate`]'s own
/// `minijinja::Environment` -- every instance holds exactly one template, so there's no collision
/// risk between instances.
const TEMPLATE_NAME: &str = "message";

/// A Jinja2-compatible template (via [`minijinja`]) that's compiled at construction time --
/// either through [`CompiledTemplate::new`] or, via the [`Deserialize`] impl below, as soon as
/// its source text is parsed out of the config. This is what lets a bad template in the config
/// fail at config-load time rather than on the first message that happens to hit it.
#[derive(Debug, Clone)]
pub struct CompiledTemplate {
    // Kept alongside `env` for `Serialize`/`Debug` -- minijinja doesn't hand a template's source
    // text back out cheaply once compiled.
    source: String,
    env: Environment<'static>,
}

impl CompiledTemplate {
    pub fn new(source: impl Into<String>) -> Result<Self, minijinja::Error> {
        let source = source.into();
        let mut env = Environment::new();
        env.add_template_owned(TEMPLATE_NAME, source.clone())?;
        Ok(Self { source, env })
    }

    pub fn render(&self, ctx: impl Serialize) -> Result<String, minijinja::Error> {
        self.env
            .get_template(TEMPLATE_NAME)
            .expect("compiled in `new`")
            .render(ctx)
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn set_undefined_behavior(&mut self, behavior: minijinja::UndefinedBehavior) {
        self.env.set_undefined_behavior(behavior);
    }
}

impl<'de> Deserialize<'de> for CompiledTemplate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl Serialize for CompiledTemplate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.source)
    }
}

/// Builds the template-rendering context shared by the `template` decoder and by templated
/// output topics: the whole incoming `Publish` packet (`topic`, `qos`, `retain`, `dup`, `pkid`),
/// plus `payload` -- see [`payload_value`].
pub(crate) fn publish_context(publish: &Publish) -> Value {
    context! {
        topic => publish.topic.as_str(),
        qos => format!("{:?}", publish.qos),
        retain => publish.retain,
        dup => publish.dup,
        pkid => publish.pkid,
        payload => payload_value(&publish.payload),
    }
}

/// Converts a raw payload into a template-friendly value: parsed JSON (indexable) when it's valid
/// UTF-8 and valid JSON, a plain string when it's valid UTF-8 but not JSON, or a hex string when
/// it isn't even valid UTF-8 -- so the value is always renderable, but only indexable when the
/// payload is actually structured.
pub(crate) fn payload_value(payload: &[u8]) -> Value {
    match std::str::from_utf8(payload) {
        Ok(text) => match serde_json::from_str::<serde_json::Value>(text) {
            Ok(json) => Value::from_serialize(&json),
            Err(_) => Value::from(text),
        },
        Err(_) => Value::from(hex::encode(payload)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_valid_template_source() {
        let template = CompiledTemplate::new("{{ topic }}").unwrap();
        let publish = Publish::new("devices/42/raw", rumqttc::QoS::AtLeastOnce, b"hi".to_vec());
        assert_eq!(
            template.render(publish_context(&publish)).unwrap(),
            "devices/42/raw"
        );
    }

    #[test]
    fn fails_to_construct_with_invalid_template_syntax() {
        assert!(CompiledTemplate::new("{{ unclosed").is_err());
    }
}
