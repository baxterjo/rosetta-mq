use std::collections::HashMap;
use std::sync::Arc;
use std::{error::Error, path::Path};

use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;

use async_trait::async_trait;
use tokio::sync::mpsc::{error::SendError, Sender};

pub mod context;
pub mod hexdump;
pub mod protobuf;
pub mod registry;
pub mod template;
pub mod utf8;

pub use registry::{DecoderRegistry, DecoderRegistryBuilder, RegistryEntry, RegistryError};

use context::CompiledTemplate;
use crate::config::RefOr;

//  _____ ____  _   _ ______ _____ _____
// / ____/ __ \| \ | |  ____|_   _/ ____|
//| |   | |  | |  \| | |__    | || |  __
//| |   | |  | | . ` |  __|   | || | |_ |
//| |___| |__| | |\  | |     _| || |__| |
// \_____\____/|_| \_|_|    |_____\_____|

/// Always deserializes successfully on its own -- `success_output`/`error_output` have defaults
/// and `decoder` is `Option` -- so it's safe to flatten directly onto `TopicMapping` without the
/// `flatten` + `Option<Struct>` footgun that comes from wrapping the whole thing in `Option`
/// instead (a deserialize failure partway through a flattened `Option<T>` silently becomes `None`
/// rather than propagating, since serde/toml can't tell "absent" apart from "present but
/// invalid" once the struct itself is optional -- see the regression test below). `decoder` being
/// `None` is what actually means "no decoder assigned, subscribe-only".
#[derive(Debug, Clone, Deserialize)]
pub struct DecoderConfig {
    /// What should happen when the decoder succeeds?
    #[serde(default = "default_success_behavior")]
    pub success_output: OutputBehavior,
    #[serde(default = "default_error_behavior")]
    pub error_output: OutputBehavior,
    pub decoder: Option<RefOr<DecoderKind>>,
}

/// The result of [`DecoderConfig::build`]: a ready-to-use decoder plus its two output behaviors,
/// already resolved from config -- both are ready to use as-is at message time, since only a
/// codec (e.g. a `.proto` schema) needs a build step; output routing doesn't.
pub struct BuiltDecoder {
    pub decoder: Arc<dyn ErasedDecoder>,
    pub success_output: OutputBehavior,
    pub error_output: OutputBehavior,
}

impl DecoderConfig {
    /// Resolves the inner `decoder` ref against `decoders` and builds the codec it names (see
    /// [`DecoderKind::build`]), bundling the result with this mapping's output behaviors. Only
    /// meaningful to call when `self.decoder` is `Some` (i.e. a decoder is actually configured for
    /// this topic) -- callers check that first, the same way they already skip subscribe-only
    /// topics before doing anything else with them. The ref is assumed already validated (see
    /// `Config::validate`), so an unresolvable name here is a bug, not a user-facing error.
    pub fn build(
        &self,
        base_dir: &Path,
        decoders: &HashMap<String, DecoderKind>,
    ) -> Result<BuiltDecoder, BuildDecoderError> {
        let kind = self
            .decoder
            .as_ref()
            .expect("only called when a decoder is configured")
            .resolve(decoders)
            .expect("decoder reference validated at config load");
        Ok(BuiltDecoder {
            decoder: kind.build(base_dir)?,
            success_output: self.success_output.clone(),
            error_output: self.error_output.clone(),
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputBehavior {
    /// Publish the decoder output using the provided args.
    Publish(PublishArgs),
    /// Emit the decoder output to stdout
    StdOut,
    /// Emit the decoder output to stderr
    StdErr,
    /// Run the decoder, but do not emit the decoder output anywhere.
    Quiet,
}

/// The default `success_output`: publish to `{topic}/decoded`. Public so code that constructs a
/// `DecoderConfig` directly (rather than via TOML, where `#[serde(default = ...)]` already
/// applies this) can reuse the same default -- e.g. tests.
pub fn default_success_behavior() -> OutputBehavior {
    OutputBehavior::Publish(PublishArgs {
        topic: TopicSpec::Suffix("/decoded".to_string()),
        qos: InheritOr::Literal(QoS::AtMostOnce),
        retain: InheritOr::Literal(false),
    })
}

/// The default `error_output`: publish to `{topic}/decode_error`. See
/// [`default_success_behavior`].
pub fn default_error_behavior() -> OutputBehavior {
    OutputBehavior::Publish(PublishArgs {
        topic: TopicSpec::Suffix("/decode_error".to_string()),
        qos: InheritOr::Literal(QoS::AtMostOnce),
        retain: InheritOr::Literal(false),
    })
}

#[derive(Debug, Clone, Deserialize)]
/// Publish arguments for decoder output.
pub struct PublishArgs {
    /// Topic spec for decoder output publish.
    pub topic: TopicSpec,
    /// Optional QoS, defaults to the lowest QoS to conserve network resources.
    ///
    /// Use `qos = inherit` to inherit QoS from input message.
    #[serde(default)]
    pub qos: InheritOr<QoS>,
    /// Optional retain, defaults to false to conserve broker resources.
    ///
    /// Use `retain = inherit` to inherit retain from input message.
    #[serde(default)]
    pub retain: InheritOr<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopicSpec {
    /// Always publish to this topic from this decoder.
    Literal(String),
    /// Add a prefix to the original topic for this decoder.
    Prefix(String),
    /// Add a suffix to the original topic for this decoder.
    Suffix(String),
    /// Generate the topic from a given template string. The template will be given all the context
    /// that the `template` decoder is given as well as the decoded output.
    Template(CompiledTemplate),
}

impl TopicSpec {
    /// Resolves this spec against the incoming message and the decoder's output payload for this
    /// message. `Prefix`/`Suffix` concatenate directly against `incoming.topic` with no inserted
    /// separator -- the configured string is expected to carry its own `/`, same as the default
    /// `"/decoded"` suffix does.
    pub fn resolve(
        &self,
        incoming: &Publish,
        output_payload: &[u8],
    ) -> Result<String, minijinja::Error> {
        Ok(match self {
            TopicSpec::Literal(topic) => topic.clone(),
            TopicSpec::Prefix(prefix) => format!("{prefix}{}", incoming.topic),
            TopicSpec::Suffix(suffix) => format!("{}{suffix}", incoming.topic),
            TopicSpec::Template(template) => {
                let ctx = context::publish_context(incoming);
                template.render(minijinja::context! {
                    output => context::payload_value(output_payload),
                    ..ctx
                })?
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Deserialize, Default)]
pub enum QoS {
    #[default]
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl From<QoS> for rumqttc::QoS {
    fn from(value: QoS) -> Self {
        match value {
            QoS::AtMostOnce => Self::AtMostOnce,
            QoS::AtLeastOnce => Self::AtLeastOnce,
            QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

impl From<rumqttc::QoS> for QoS {
    fn from(value: rumqttc::QoS) -> Self {
        match value {
            rumqttc::QoS::AtMostOnce => Self::AtMostOnce,
            rumqttc::QoS::AtLeastOnce => Self::AtLeastOnce,
            rumqttc::QoS::ExactlyOnce => Self::ExactlyOnce,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum InheritOr<T> {
    Inherit,
    Literal(T),
}

impl<T: Default> Default for InheritOr<T> {
    fn default() -> Self {
        InheritOr::Literal(T::default())
    }
}

impl<T> InheritOr<T> {
    /// Resolves to `inherited` for `Inherit`, or the configured value for `Literal`.
    pub fn resolve(self, inherited: T) -> T {
        match self {
            InheritOr::Inherit => inherited,
            InheritOr::Literal(value) => value,
        }
    }
}

/// Per-topic decoder configuration, discriminated by the `decoder` field in TOML (e.g.
/// `decoder = "protobuf"`, plus that variant's own fields as siblings at the same level -- see
/// [`protobuf::ProtobufConfig`]). Lives here rather than in `config.rs` because it's
/// decoder-specific domain knowledge, the same way `config.rs` already depends on
/// [`crate::topic::TopicFilter`] rather than redefining topic-filter parsing itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind")]
pub enum DecoderKind {
    #[serde(rename = "hexdump")]
    Hexdump,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "protobuf")]
    Protobuf(protobuf::ProtobufConfig),
    #[serde(rename = "template")]
    Template(template::TemplateConfig),
}

impl DecoderKind {
    /// Constructs the decoder this config describes. Fallible and I/O-bound for schema-based
    /// decoders (e.g. compiling a `.proto` file), so this runs once at registry-build time, not
    /// per message. `base_dir` resolves any relative paths in decoder-specific config (e.g.
    /// `proto_file`) against the config file's directory rather than the process's CWD.
    pub fn build(&self, base_dir: &Path) -> Result<Arc<dyn ErasedDecoder>, BuildDecoderError> {
        match self {
            DecoderKind::Hexdump => Ok(Arc::new(hexdump::HexDumpDecoder)),
            DecoderKind::Utf8 => Ok(Arc::new(utf8::Utf8Decoder)),
            DecoderKind::Protobuf(cfg) => Ok(Arc::new(protobuf::ProtobufDecoder::from_config(
                cfg, base_dir,
            )?)),
            DecoderKind::Template(cfg) => Ok(Arc::new(template::TemplateDecoder::from_config(cfg))),
        }
    }
}

// ______ _____  _____   ____  _____   _____
//|  ____|  __ \|  __ \ / __ \|  __ \ / ____|
//| |__  | |__) | |__) | |  | | |__) | (___
//|  __| |  _  /|  _  /| |  | |  _  / \___ \
//| |____| | \ \| | \ \| |__| | | \ \ ____) |
//|______|_|  \_\_|  \_\\____/|_|  \_\_____/

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

#[derive(Debug, Error)]
pub enum BuildDecoderError {
    #[error(transparent)]
    Protobuf(#[from] protobuf::ProtobufDecoderError),
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

// _______ _____            _____ _______ _____            _   _ _____
//|__   __|  __ \     /\   |_   _|__   __/ ____|     /\   | \ | |  __ \
//   | |  | |__) |   /  \    | |    | | | (___      /  \  |  \| | |  | |
//   | |  |  _  /   / /\ \   | |    | |  \___ \    / /\ \ | . ` | |  | |
//   | |  | | \ \  / ____ \ _| |_   | |  ____) |  / ____ \| |\  | |__| |
//   |_|  |_|  \_\/_/    \_\_____|  |_| |_____/  /_/    \_\_| \_|_____/
//
//
//  _____ _______ _____  _    _  _____ _______ _____
// / ____|__   __|  __ \| |  | |/ ____|__   __/ ____|
//| (___    | |  | |__) | |  | | |       | | | (___
// \___ \   | |  |  _  /| |  | | |       | |  \___ \
// ____) |  | |  | | \ \| |__| | |____   | |  ____) |
//|_____/   |_|  |_|  \_\\____/ \_____|  |_| |_____/

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
/// passed as `dyn` (e.g. [`registry::DecoderRegistry`]). `Decoder` itself can't be made into a trait object
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

/// Decoders will emit this to a channel whenever they want to publish a new message.
///
/// Many decoders will only emit one of these, but some may emit a stream. Where (and whether)
/// each emitted payload actually gets published is entirely config-driven (see
/// [`OutputBehavior`]/[`PublishArgs`]) -- decoders themselves never choose their own output topic,
/// QoS, or retain flag.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DecodePublish {
    pub payload: Vec<u8>,
}
