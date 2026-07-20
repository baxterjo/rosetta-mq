use std::sync::Arc;

use thiserror::Error;

use crate::topic::TopicFilter;

pub mod builtin;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("{0}")]
    Message(String),
}

/// Decodes a raw MQTT payload into a human-readable string. Implementations should be cheap to
/// share across messages (registered once, invoked per message).
pub trait Decoder: Send + Sync {
    fn name(&self) -> &str;
    fn decode(&self, payload: &[u8]) -> Result<String, DecodeError>;
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
        if self.entries.iter().any(|e| e.filter.as_str() == filter.as_str()) {
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
        let mut best: Option<&RegistryEntry> = None;
        for entry in &self.entries {
            if !entry.filter.matches(topic) {
                continue;
            }
            let is_better = match best {
                None => true,
                Some(current) => entry.filter > current.filter,
            };
            if is_better {
                best = Some(entry);
            }
        }
        best.map(|entry| entry.decoder.as_ref())
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
            .register(TopicFilter::parse("devices/+/raw").unwrap(), Arc::new(Utf8Decoder))
            .unwrap();
        builder
            .register(
                TopicFilter::parse("devices/42/raw").unwrap(),
                Arc::new(HexDumpDecoder),
            )
            .unwrap();
        let registry = builder.build();

        assert_eq!(registry.resolve("devices/42/raw").unwrap().name(), "hexdump");
        assert_eq!(registry.resolve("devices/99/raw").unwrap().name(), "utf8");
    }

    #[test]
    fn resolve_returns_none_when_no_filter_matches() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(TopicFilter::parse("devices/+/raw").unwrap(), Arc::new(Utf8Decoder))
            .unwrap();
        let registry = builder.build();

        assert!(registry.resolve("sensors/1").is_none());
    }

    #[test]
    fn register_rejects_duplicate_filter_strings() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(TopicFilter::parse("devices/+/raw").unwrap(), Arc::new(Utf8Decoder))
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
