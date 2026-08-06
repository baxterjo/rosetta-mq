use minijinja::{context, Environment, Value};
use rumqttc::Publish;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Internal key for indexing a template within a decoder's environment. There is only ever one
/// template per environment so the same one is used every time.
const TEMPLATE_NAME: &str = "message";

/// A newtype struct around a [`minijinja`] template. This struct and the (de)serialize impls below
/// allow invalid templates to be flagged at init time.
#[derive(Debug, Clone)]
pub struct CompiledTemplate {
    // Kept alongside `env` for `Serialize`/`Debug`. [`minijinja`] doesn't hand a template's source
    // text back out cheaply once compiled.
    source: String,
    env: Environment<'static>,
}

impl CompiledTemplate {
    /// Generate a new template from an input string.
    pub fn new(source: impl Into<String>) -> Result<Self, minijinja::Error> {
        let source = source.into();
        let mut env = Environment::new();
        env.add_template_owned(TEMPLATE_NAME, source.clone())?;
        Ok(Self { source, env })
    }

    /// Render a template from the given context.
    pub fn render(&self, ctx: impl Serialize) -> Result<String, minijinja::Error> {
        self.env
            .get_template(TEMPLATE_NAME)
            .expect("compiled in `new`")
            .render(ctx)
    }

    /// Get the string that is the source of the template.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// Sets the undefined behavior for the template.
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

/// Creates a template context from an incoming [`rumqttc::Publish`] struct.
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

/// Converts a raw payload into a template-friendly value:
/// - Parsed JSON (indexable) when it's valid UTF-8 and valid JSON
/// - A plain string when it's valid UTF-8 but not JSON
/// - A hex string when it isn't even valid UTF-8.
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
