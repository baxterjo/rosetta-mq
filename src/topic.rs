use std::cmp::Ordering;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TopicError {
    #[error("topic filter must not be empty")]
    Empty,
    #[error("'#' wildcard must be the last segment in filter {0:?}")]
    HashNotLast(String),
    #[error("wildcard segment {0:?} in filter {1:?} must not mix '+'/'#' with other characters")]
    InvalidWildcardSegment(String, String),
}

/// A parsed MQTT topic filter (e.g. `devices/+/raw`, `sensors/#`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFilter {
    raw: String,
    specificity: Vec<u8>,
}

impl TopicFilter {
    pub fn parse(raw: &str) -> Result<Self, TopicError> {
        if raw.is_empty() {
            return Err(TopicError::Empty);
        }

        let parts: Vec<&str> = raw.split('/').collect();
        let last = parts.len() - 1;
        let mut specificity = Vec::with_capacity(parts.len());

        for (i, part) in parts.iter().enumerate() {
            let rank = match *part {
                "#" if i == last => 0,
                "#" => return Err(TopicError::HashNotLast(raw.to_string())),
                "+" => 1,
                _ if part.contains('#') || part.contains('+') => {
                    return Err(TopicError::InvalidWildcardSegment(
                        part.to_string(),
                        raw.to_string(),
                    ));
                }
                _ => 2,
            };
            specificity.push(rank);
        }

        Ok(TopicFilter {
            raw: raw.to_string(),
            specificity,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Whether this filter matches a concrete (non-wildcard) topic.
    pub fn matches(&self, topic: &str) -> bool {
        rumqttc::mqttbytes::matches(topic, &self.raw)
    }

    /// Per-segment specificity vector (literal=2, `+`=1, `#`=0), calculated once at parse time.
    pub fn specificity(&self) -> &[u8] {
        &self.specificity
    }
}

/// Orders filters by specificity only — lexicographic comparison of the per-segment vector,
/// where an earlier, more specific segment (literal > `+` > `#`) decides ties on later segments.
/// Only meaningful when comparing filters that matched the same concrete topic; two filters with
/// different `raw` text can compare as `Equal` here even though they aren't `==`.
impl PartialOrd for TopicFilter {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopicFilter {
    fn cmp(&self, other: &Self) -> Ordering {
        self.specificity.cmp(&other.specificity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_exact_literal() {
        let f = TopicFilter::parse("devices/42/raw").unwrap();
        assert!(f.matches("devices/42/raw"));
        assert!(!f.matches("devices/43/raw"));
        assert!(!f.matches("devices/42/raw/extra"));
    }

    #[test]
    fn matches_single_level_wildcard() {
        let f = TopicFilter::parse("devices/+/raw").unwrap();
        assert!(f.matches("devices/42/raw"));
        assert!(f.matches("devices/anything/raw"));
        assert!(!f.matches("devices/42/43/raw"));
        assert!(!f.matches("devices/raw"));
    }

    #[test]
    fn matches_multi_level_wildcard() {
        let f = TopicFilter::parse("sensors/#").unwrap();
        assert!(f.matches("sensors"));
        assert!(f.matches("sensors/a"));
        assert!(f.matches("sensors/a/b/c"));
        assert!(!f.matches("other/a"));
    }

    #[test]
    fn matches_bare_hash() {
        let f = TopicFilter::parse("#").unwrap();
        assert!(f.matches("anything/at/all"));
    }

    #[test]
    fn rejects_hash_not_last() {
        assert_eq!(
            TopicFilter::parse("a/#/b"),
            Err(TopicError::HashNotLast("a/#/b".to_string()))
        );
    }

    #[test]
    fn rejects_mixed_wildcard_segment() {
        assert!(matches!(
            TopicFilter::parse("a/b#/c"),
            Err(TopicError::InvalidWildcardSegment(_, _))
        ));
        assert!(matches!(
            TopicFilter::parse("a/+b/c"),
            Err(TopicError::InvalidWildcardSegment(_, _))
        ));
    }

    #[test]
    fn rejects_empty_filter() {
        assert_eq!(TopicFilter::parse(""), Err(TopicError::Empty));
    }

    #[test]
    fn specificity_ranks_exact_over_plus_over_hash() {
        let exact = TopicFilter::parse("devices/42/raw").unwrap();
        let plus = TopicFilter::parse("devices/+/raw").unwrap();
        let hash = TopicFilter::parse("devices/#").unwrap();

        assert!(exact > plus);
        assert!(plus > hash);
    }

    #[test]
    fn specificity_bare_hash_loses_to_prefixed_hash() {
        let bare = TopicFilter::parse("#").unwrap();
        let prefixed = TopicFilter::parse("a/#").unwrap();
        assert!(prefixed > bare);
    }
}
