use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::topic::TopicFilter;

use super::{ErasedDecoder, OutputBehavior};

/// TTL for a pending echo. This is used to ensure RegistryEntry.pending_echos does not grow
/// forever when messages are missed.
const PENDING_ECHO_TTL: Duration = Duration::from_secs(30);

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RegistryError {
    #[error("duplicate topic filter {0:?}")]
    DuplicateFilter(String),
}

/// All required state for a single topic/decoder pairing.
pub struct RegistryEntry {
    filter: TopicFilter,
    pub name: String,
    pub decoder: Arc<dyn ErasedDecoder>,
    pub success_output: OutputBehavior,
    pub error_output: OutputBehavior,
    // Keeps track of previously sent messages on a specific topic to avoid feedback loops
    // (echoes).
    pending_echoes: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl RegistryEntry {
    /// Records that this entry just published to `topic` as its own output, so the next incoming
    /// message on that exact topic is recognized as an echo rather than new input.
    ///
    /// A no-op unless `topic` still matches this entry's own `topic_filter`
    pub fn mark_published(&self, topic: &str) {
        if !self.filter.matches(topic) {
            return;
        }
        let mut pending = self
            .pending_echoes
            .lock()
            .expect("poisoned mutex should panic");
        prune_expired(&mut pending);
        pending
            .entry(topic.to_string())
            .or_default()
            .push_back(Instant::now());
    }

    /// If `topic` has a pending self-published mark, consumes the oldest one and returns `true`
    /// (this message is our own echo, not new input); otherwise returns `false`.
    pub fn consume_echo(&self, topic: &str) -> bool {
        let mut pending = self
            .pending_echoes
            .lock()
            .expect("poisoned mutex should panic");
        prune_expired(&mut pending);
        let Some(marks) = pending.get_mut(topic) else {
            return false;
        };
        let consumed = marks.pop_front().is_some();
        if marks.is_empty() {
            pending.remove(topic);
        }
        consumed
    }
}

/// Drops any pending mark older than [`PENDING_ECHO_TTL`], and any topic left with no marks at
/// all.
fn prune_expired(pending: &mut HashMap<String, VecDeque<Instant>>) {
    let now = Instant::now();
    pending.retain(|_, marks| {
        while matches!(marks.front(), Some(mark) if now.saturating_duration_since(*mark) > PENDING_ECHO_TTL)
        {
            marks.pop_front();
        }
        !marks.is_empty()
    });
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
        name: String,
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
            name,
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
                "utf8_topics".to_string(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        builder
            .register(
                TopicFilter::parse("devices/42/raw").unwrap(),
                "device_42_hex".to_string(),
                Arc::new(HexDumpDecoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        assert_eq!(
            registry.resolve("devices/42/raw").unwrap().name,
            "device_42_hex"
        );
        assert_eq!(
            registry.resolve("devices/99/raw").unwrap().name,
            "utf8_topics"
        );
    }

    #[test]
    fn resolve_returns_none_when_no_filter_matches() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                "utf8_topics".to_string(),
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
                "utf8_topics".to_string(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();

        let err = builder.register(
            TopicFilter::parse("devices/+/raw").unwrap(),
            "device_42_hex".to_string(),
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
                "utf8_topics".to_string(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        // "devices/99/raw" still matches "devices/+/raw" -- mark_published only tracks topics
        // that could resolve back to this same entry, so the mark/consume pair below only works
        // if `first` and `second` really are the same underlying entry.
        let first = registry.resolve("devices/42/raw").unwrap();
        first.mark_published("devices/99/raw");
        let second = registry.resolve("devices/42/raw").unwrap();
        assert!(second.consume_echo("devices/99/raw"));
    }

    #[test]
    fn mark_published_ignores_topics_outside_the_entrys_own_filter() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                "utf8_topics".to_string(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        let entry = registry.resolve("devices/42/raw").unwrap();
        // The default `/decoded` success suffix, for instance -- doesn't match "devices/+/raw",
        // so this entry could never be asked to consume it back; marking it should be a no-op.
        entry.mark_published("devices/42/raw/decoded");
        assert!(!entry.consume_echo("devices/42/raw/decoded"));
    }

    #[test]
    fn consume_echo_is_false_without_a_prior_mark() {
        let mut builder = DecoderRegistryBuilder::new();
        builder
            .register(
                TopicFilter::parse("devices/+/raw").unwrap(),
                "utf8_topics".to_string(),
                Arc::new(Utf8Decoder),
                quiet(),
                quiet(),
            )
            .unwrap();
        let registry = builder.build();

        let entry = registry.resolve("devices/42/raw").unwrap();
        assert!(!entry.consume_echo("devices/42/raw/decoded"));
    }

    #[test]
    fn prune_expired_drops_stale_marks_and_keeps_live_ones() {
        // Instant has no public constructor for an arbitrary past value, but subtracting a
        // Duration from a real `now()` is exact and needs no actual waiting -- so this is
        // deterministic, not a sleep-and-hope test.
        let mut pending = HashMap::new();
        pending.insert(
            "stale/topic".to_string(),
            VecDeque::from([Instant::now() - (PENDING_ECHO_TTL + Duration::from_secs(1))]),
        );
        pending.insert("live/topic".to_string(), VecDeque::from([Instant::now()]));

        prune_expired(&mut pending);

        assert!(!pending.contains_key("stale/topic"));
        assert!(pending.contains_key("live/topic"));
    }

    #[test]
    fn prune_expired_drops_only_the_stale_marks_within_a_topic() {
        let mut pending = HashMap::new();
        pending.insert(
            "topic".to_string(),
            VecDeque::from([
                Instant::now() - (PENDING_ECHO_TTL + Duration::from_secs(1)),
                Instant::now(),
            ]),
        );

        prune_expired(&mut pending);

        assert_eq!(pending.get("topic").unwrap().len(), 1);
    }
}
