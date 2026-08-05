use std::sync::Arc;

use thiserror::Error;

use crate::topic::TopicFilter;

use super::ErasedDecoder;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("duplicate topic filter {0:?}")]
    DuplicateFilter(String),
}

struct RegistryEntry {
    filter: TopicFilter,
    decoder: Arc<dyn ErasedDecoder>,
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
        decoder: Arc<dyn ErasedDecoder>,
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
    pub fn resolve(&self, topic: &str) -> Option<Arc<dyn ErasedDecoder>> {
        self.entries
            .iter()
            .filter(|e| e.filter.matches(topic))
            .max_by_key(|e| &e.filter)
            .map(|e| Arc::clone(&e.decoder))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::{hexdump::HexDumpDecoder, utf8::Utf8Decoder};

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
