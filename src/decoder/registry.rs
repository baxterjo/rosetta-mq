use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::topic::TopicFilter;

use super::{ErasedDecoder, OutputBehavior};

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("duplicate topic filter {0:?}")]
    DuplicateFilter(String),
}

/// Everything resolved for one `[[topic]]` mapping: the decoder itself, where its success/error
/// output goes, and the state needed to recognize the entry's own published output when it comes
/// back in as a new incoming message (see `mark_published`/`consume_echo`). This state is scoped
/// to a single entry (one topic filter/decoder pairing), not shared registry-wide, so that
/// intentionally chaining decoders through the broker (one decoder's output topic feeding
/// another's input filter) is never mistaken for a feedback loop -- only an entry's *own* past
/// output is ever suppressed.
pub struct RegistryEntry {
    filter: TopicFilter,
    pub decoder: Arc<dyn ErasedDecoder>,
    pub success_output: OutputBehavior,
    pub error_output: OutputBehavior,
    // Counts topics this entry has published to and hasn't yet seen echoed back, keyed by the
    // resolved topic string. A counter rather than a single flag so back-to-back publishes to the
    // same topic (e.g. a wildcard filter whose outputs collide, or a `Literal` topic spec) don't
    // under- or over-suppress.
    pending_echoes: Mutex<HashMap<String, u32>>,
}

impl RegistryEntry {
    /// Records that this entry just published to `topic` as its own output, so the next incoming
    /// message on that exact topic is recognized as an echo rather than new input. Call only
    /// after the publish itself succeeds -- marking on a failed publish would leave a phantom
    /// pending count that could swallow a real, unrelated future message on that topic.
    pub fn mark_published(&self, topic: &str) {
        let mut pending = self.pending_echoes.lock().unwrap();
        *pending.entry(topic.to_string()).or_insert(0) += 1;
    }

    /// If `topic` has a pending self-published count, consumes one and returns `true` (this
    /// message is our own echo, not new input); otherwise returns `false`.
    pub fn consume_echo(&self, topic: &str) -> bool {
        let mut pending = self.pending_echoes.lock().unwrap();
        let Some(count) = pending.get_mut(topic) else {
            return false;
        };
        *count -= 1;
        if *count == 0 {
            pending.remove(topic);
        }
        true
    }
}

/// Builds a [`DecoderRegistry`] by registering one decoder per topic filter.
#[derive(Default)]
pub struct DecoderRegistryBuilder {
    entries: Vec<Arc<RegistryEntry>>,
}

impl DecoderRegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        filter: TopicFilter,
        decoder: Arc<dyn ErasedDecoder>,
        success_output: OutputBehavior,
        error_output: OutputBehavior,
    ) -> Result<(), RegistryError> {
        if self
            .entries
            .iter()
            .any(|e| e.filter.as_str() == filter.as_str())
        {
            return Err(RegistryError::DuplicateFilter(filter.as_str().to_string()));
        }
        self.entries.push(Arc::new(RegistryEntry {
            filter,
            decoder,
            success_output,
            error_output,
            pending_echoes: Mutex::new(HashMap::new()),
        }));
        Ok(())
    }

    pub fn build(self) -> DecoderRegistry {
        DecoderRegistry {
            entries: self.entries,
        }
    }
}

/// Resolves an incoming concrete topic to the entry registered for the most specific matching
/// filter ("best match wins" — an exact-match filter beats a wildcard filter that also matches).
pub struct DecoderRegistry {
    entries: Vec<Arc<RegistryEntry>>,
}

impl DecoderRegistry {
    /// Returns the *same* `Arc<RegistryEntry>` on repeated calls for the same filter -- callers
    /// rely on this identity to accumulate `pending_echoes` state across messages.
    pub fn resolve(&self, topic: &str) -> Option<Arc<RegistryEntry>> {
        self.entries
            .iter()
            .filter(|e| e.filter.matches(topic))
            .max_by_key(|e| &e.filter)
            .map(Arc::clone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decoder::{hexdump::HexDumpDecoder, utf8::Utf8Decoder};

    fn quiet() -> OutputBehavior {
        OutputBehavior::Quiet
    }

    #[test]
    fn resolves_most_specific_match() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        builder
            .register(
                TopicFilter::parse("devices/42/raw").unwrap(),
                Arc::new(HexDumpDecoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        assert_eq!(
            registry.resolve("devices/42/raw").unwrap().decoder.name(),
            "hexdump"
        );
        assert_eq!(
            registry.resolve("devices/99/raw").unwrap().decoder.name(),
            "utf8"
        );
    }

    #[test]
    fn resolve_returns_none_when_no_filter_matches() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
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
                quiet(),
                quiet(),
            )
            .unwrap();

        let err = builder.register(
            TopicFilter::parse("devices/+/raw").unwrap(),
            Arc::new(HexDumpDecoder),
            quiet(),
            quiet(),
        );
        assert_eq!(
            err,
            Err(RegistryError::DuplicateFilter("devices/+/raw".to_string()))
        );
    }

    #[test]
    fn resolve_returns_same_entry_across_calls() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        let first = registry.resolve("devices/42/raw").unwrap();
        first.mark_published("devices/42/raw/decoded");
        let second = registry.resolve("devices/42/raw").unwrap();
        assert!(second.consume_echo("devices/42/raw/decoded"));
    }

    #[test]
    fn consume_echo_is_false_without_a_prior_mark() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        let entry = registry.resolve("devices/42/raw").unwrap();
        assert!(!entry.consume_echo("devices/42/raw/decoded"));
    }
}
