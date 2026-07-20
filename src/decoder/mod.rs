use std::path::Path;
use std::sync::Arc;

use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;

use crate::topic::TopicFilter;

pub mod builtin;
pub mod protobuf;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("{0}")]
    Message(String),
}

/// Decodes an incoming MQTT publish into a human-readable string. Implementations get the whole
/// [`Publish`] packet (topic, QoS, retain, payload, ...), not just the payload bytes, since some
/// decoders may need more than the raw payload to decode correctly. Implementations should be
/// cheap to share across messages (registered once, invoked per message).
pub trait Decoder: Send + Sync {
    fn name(&self) -> &str;
    fn decode(&self, publish: &Publish) -> Result<String, DecodeError>;
}

/// Per-topic decoder configuration, discriminated by the `decoder` field in TOML (e.g.
/// `decoder = "protobuf"`, plus that variant's own fields as siblings at the same level -- see
/// [`protobuf::ProtobufConfig`]). Lives here rather than in `config.rs` because it's
/// decoder-specific domain knowledge, the same way `config.rs` already depends on
/// [`crate::topic::TopicFilter`] rather than redefining topic-filter parsing itself.
#[derive(Debug, Deserialize)]
#[serde(tag = "decoder")]
pub enum DecoderConfig {
    #[serde(rename = "hexdump")]
    Hexdump,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "protobuf")]
    Protobuf(protobuf::ProtobufConfig),
}

impl DecoderConfig {
    /// Constructs the decoder this config describes. Fallible and I/O-bound for schema-based
    /// decoders (e.g. compiling a `.proto` file), so this runs once at registry-build time, not
    /// per message. `base_dir` resolves any relative paths in decoder-specific config (e.g.
    /// `proto_file`) against the config file's directory rather than the process's CWD.
    pub fn build(&self, base_dir: &Path) -> Result<Arc<dyn Decoder>, BuildDecoderError> {
        match self {
            DecoderConfig::Hexdump => Ok(Arc::new(builtin::HexDumpDecoder)),
            DecoderConfig::Utf8 => Ok(Arc::new(builtin::Utf8Decoder)),
            DecoderConfig::Protobuf(cfg) => Ok(Arc::new(protobuf::ProtobufDecoder::from_config(
                cfg, base_dir,
            )?)),
        }
    }
}

#[derive(Debug, Error)]
pub enum BuildDecoderError {
    #[error(transparent)]
    Protobuf(#[from] protobuf::ProtobufDecoderError),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("duplicate topic filter {0:?}")]
    DuplicateFilter(String),
}

struct RegistryEntry {
    filter: TopicFilter,
    decoder: Arc<dyn Decoder>,
}

/// Builds a [`DecoderRegistry`] by registering one decoder per topic filter.
#[derive(Default)]
pub struct DecoderRegistryBuilder {
    entries: Vec<RegistryEntry>,
}

impl DecoderRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        filter: TopicFilter,
        decoder: Arc<dyn Decoder>,
    ) -> Result<(), RegistryError> {
        if self
            .entries
            .iter()
            .any(|e| e.filter.as_str() == filter.as_str())
        {
            return Err(RegistryError::DuplicateFilter(filter.as_str().to_string()));
        }
        self.entries.push(RegistryEntry { filter, decoder });
        Ok(())
    }

    pub fn build(self) -> DecoderRegistry {
        DecoderRegistry {
            entries: self.entries,
        }
    }
}

/// Resolves an incoming concrete topic to the decoder registered for the most specific matching
/// filter ("best match wins" — an exact-match filter beats a wildcard filter that also matches).
pub struct DecoderRegistry {
    entries: Vec<RegistryEntry>,
}

impl DecoderRegistry {
    pub fn resolve(&self, topic: &str) -> Option<&dyn Decoder> {
        self.entries
            .iter()
            .filter(|e| e.filter.matches(topic))
            .max_by_key(|e| &e.filter)
            .map(|e| e.decoder.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::builtin::{HexDumpDecoder, Utf8Decoder};

    #[test]
    fn resolves_most_specific_match() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
            )
            .unwrap();
        builder
            .register(
                TopicFilter::parse("devices/42/raw").unwrap(),
                Arc::new(HexDumpDecoder),
            )
            .unwrap();
        let registry = builder.build();

        assert_eq!(
            registry.resolve("devices/42/raw").unwrap().name(),
            "hexdump"
        );
        assert_eq!(registry.resolve("devices/99/raw").unwrap().name(), "utf8");
    }

    #[test]
    fn resolve_returns_none_when_no_filter_matches() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
            )
            .unwrap();
        let registry = builder.build();

        assert!(registry.resolve("sensors/1").is_none());
    }

    #[test]
    fn register_rejects_duplicate_filter_strings() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
            )
            .unwrap();

        let err = builder.register(
            TopicFilter::parse("devices/+/raw").unwrap(),
            Arc::new(HexDumpDecoder),
        );
        assert_eq!(
            err,
            Err(RegistryError::DuplicateFilter("devices/+/raw".to_string()))
        );
    }
}
