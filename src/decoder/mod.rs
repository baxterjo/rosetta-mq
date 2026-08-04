use std::sync::Arc;
use std::{error::Error, path::Path};

use rumqttc::{AsyncClient, ClientError, Publish, QoS};
use serde::Deserialize;
use thiserror::Error;

use crate::topic::TopicFilter;
use async_trait::async_trait;
use tokio::sync::mpsc::{Sender, error::SendError};

pub mod hexdump;
pub mod protobuf;
pub mod template;
pub mod utf8;

#[derive(Debug, Error)]
pub enum DecodeError<E: Error> {
    #[error(transparent)]
    Chanel(#[from] SendError<DecodePublish>),
    #[error(transparent)]
    Decode(E),
}

impl<E: Error> DecodeError<E> {
    fn map_decode<F: Error>(self, f: impl FnOnce(E) -> F) -> DecodeError<F> {
        match self {
            DecodeError::Chanel(e) => DecodeError::Chanel(e),
            DecodeError::Decode(e) => DecodeError::Decode(f(e)),
        }
    }
}

impl<E: Error + Send + Sync + 'static> DecodeError<E> {
    /// Boxes the decoder-specific error so it can travel through a type-erased [`Decoder`]
    /// trait object, which can't carry `Self::Error` as-is.
    fn erase(self) -> DecodeError<BoxedDecodeError> {
        self.map_decode(|e| BoxedDecodeError(Box::new(e)))
    }
}

/// Type-erased decoder error, used at the [`Decoder`] trait-object boundary in place of the
/// concrete `Self::Error` a specific decoder impl would otherwise carry. `Box<dyn Error>` doesn't
/// implement `Error` itself (only `Box<E: Error>` does), so this newtype forwards `Display` and
/// `source()` by hand.
#[derive(Debug)]
pub struct BoxedDecodeError(Box<dyn Error + Send + Sync>);

impl std::fmt::Display for BoxedDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Error for BoxedDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.0.source()
    }
}

/// Decoders will emit this to a channel whenever they want to publish a new message.
///
/// Many decoders will only emit one of these, but some may emit a stream.
///
/// The optional fields will be resolved
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DecodePublish {
    pub topic: Option<String>,
    pub qos: Option<QoS>,
    pub retain: Option<bool>,
    pub payload: Vec<u8>,
}

impl DecodePublish {
    /// Resolves missing publish arguments using the values already present in `incoming` and
    /// publishes using `client`.
    pub async fn publish(self, incoming: Publish, client: &AsyncClient) -> Result<(), ClientError> {
        self.resolve(incoming).publish(client).await
    }

    fn resolve(self, incoming: Publish) -> ResolvedDecodePublish {
        ResolvedDecodePublish {
            topic: self.topic.unwrap_or(format!("{}/decoded", incoming.topic)),
            qos: self.qos.unwrap_or(incoming.qos),
            retain: self.retain.unwrap_or(incoming.retain),
            payload: self.payload.into(),
        }
    }
}

pub struct ResolvedDecodePublish {
    pub topic: String,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Vec<u8>,
}

impl ResolvedDecodePublish {
    async fn publish(self, client: &AsyncClient) -> Result<(), ClientError> {
        client
            .publish(self.topic, self.qos, self.retain, self.payload)
            .await
    }
}

/// Decodes an incoming MQTT publish into a human-readable string. Implementations get the whole
/// [`Publish`] packet (topic, QoS, retain, payload, ...), not just the payload bytes, since some
/// decoders may need more than the raw payload to decode correctly. Implementations should be
/// cheap to share across messages (registered once, invoked per message).
#[async_trait]
pub trait Decoder {
    /// Error emitted if decoder fails to decode the message.
    type Error: Error;
    /// Name of the decoder.
    fn name(&self) -> &str;
    /// Decode the incoming publish message.
    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>>;
}

/// Object-safe counterpart to [`Decoder`], used everywhere a decoder needs to be stored or
/// passed as `dyn` (e.g. the registry below). `Decoder` itself can't be made into a trait object
/// because `Self::Error` varies per implementation; this trait erases that error into
/// [`BoxedDecodeError`] instead. Implemented for every `Decoder` via the blanket impl below --
/// decoder authors implement `Decoder`, never this trait directly.
#[async_trait]
pub trait ErasedDecoder: Send + Sync {
    fn name(&self) -> &str;
    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<BoxedDecodeError>>;
}

#[async_trait]
impl<T> ErasedDecoder for T
where
    T: Decoder + Send + Sync,
    T::Error: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        Decoder::name(self)
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<BoxedDecodeError>> {
        Decoder::decode(self, publish, tx)
            .await
            .map_err(DecodeError::erase)
    }
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
    #[serde(rename = "template")]
    Template(template::TemplateConfig),
}

impl DecoderConfig {
    /// Constructs the decoder this config describes. Fallible and I/O-bound for schema-based
    /// decoders (e.g. compiling a `.proto` file), so this runs once at registry-build time, not
    /// per message. `base_dir` resolves any relative paths in decoder-specific config (e.g.
    /// `proto_file`) against the config file's directory rather than the process's CWD.
    pub fn build(&self, base_dir: &Path) -> Result<Arc<dyn ErasedDecoder>, BuildDecoderError> {
        match self {
            DecoderConfig::Hexdump => Ok(Arc::new(hexdump::HexDumpDecoder)),
            DecoderConfig::Utf8 => Ok(Arc::new(utf8::Utf8Decoder)),
            DecoderConfig::Protobuf(cfg) => Ok(Arc::new(protobuf::ProtobufDecoder::from_config(
                cfg, base_dir,
            )?)),
            DecoderConfig::Template(cfg) => {
                Ok(Arc::new(template::TemplateDecoder::from_config(cfg)?))
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum BuildDecoderError {
    #[error(transparent)]
    Protobuf(#[from] protobuf::ProtobufDecoderError),
    #[error(transparent)]
    Template(#[from] template::TemplateDecoderError),
}

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
