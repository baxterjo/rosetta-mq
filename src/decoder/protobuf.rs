use std::path::{Path, PathBuf};

use async_trait::async_trait;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

use crate::decoder::{DecodeError, DecodePublish, Decoder};
use crate::util::resolve_path;

/// Per-topic config for the protobuf decoder: which `.proto` file to compile at runtime, which
/// message type within it to decode payloads as, and any extra include paths the schema's
/// `import`s need to resolve. `proto_file`/`include_paths` are resolved relative to the config
/// file's directory (see `ProtobufDecoder::from_config`), not the process's current directory.
#[derive(Debug, Clone, Deserialize)]
pub struct ProtobufConfig {
    pub proto_file: String,
    pub message_type: String,
    #[serde(default)]
    pub include_paths: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ProtobufDecoderError {
    #[error("failed to compile {proto_file:?}: {source}")]
    Compile {
        proto_file: PathBuf,
        #[source]
        source: protox::Error,
    },
    #[error("failed to build descriptor pool: {0}")]
    Descriptor(#[from] prost_reflect::DescriptorError),
    #[error("failed to decode protobuf payload against schema: {0}")]
    Decode(#[from] protox::prost::DecodeError),
    #[error("Failed to serialize into JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("message type {0:?} not found in compiled schema")]
    UnknownMessageType(String),
}

/// Decodes protobuf wire-format payloads into JSON using a `.proto` schema compiled at runtime
/// (via `protox`, a pure-Rust `protoc` replacement -- no `protoc` binary required at build or run
/// time). Two caveats inherent to the wire format, not bugs in this decoder: (1) protobuf's wire
/// format isn't self-validating against a specific schema, so a payload encoded with a different
/// but wire-compatible message can decode "successfully" into plausible-but-wrong JSON; (2)
/// fields present in the payload but absent from the loaded schema decode into the message's
/// unknown fields and are invisible in the JSON output below.
#[derive(Debug)]
pub struct ProtobufDecoder {
    descriptor: MessageDescriptor,
}

impl ProtobufDecoder {
    pub fn from_config(
        cfg: &ProtobufConfig,
        base_dir: &Path,
    ) -> Result<Self, ProtobufDecoderError> {
        let proto_file = resolve_path(base_dir, &cfg.proto_file);

        // `proto_file`'s own directory is always searchable (protox requires the root file to
        // reside under one of the given include paths, same as `protoc`) -- `include_paths` are
        // *extra* paths on top of that, for resolving `import`s that live elsewhere, not a
        // replacement for it.
        let mut include_paths: Vec<PathBuf> =
            vec![proto_file.parent().unwrap_or(base_dir).to_path_buf()];
        include_paths.extend(cfg.include_paths.iter().map(|p| resolve_path(base_dir, p)));

        let file_descriptor_set =
            protox::compile([&proto_file], &include_paths).map_err(|source| {
                ProtobufDecoderError::Compile {
                    proto_file: proto_file.clone(),
                    source,
                }
            })?;

        let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)?;
        let descriptor = pool
            .get_message_by_name(&cfg.message_type)
            .ok_or_else(|| ProtobufDecoderError::UnknownMessageType(cfg.message_type.clone()))?;

        Ok(Self { descriptor })
    }
}

#[async_trait]
impl Decoder for ProtobufDecoder {
    type Error = ProtobufDecoderError;

    fn name(&self) -> &str {
        "protobuf"
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>> {
        let message = DynamicMessage::decode(self.descriptor.clone(), publish.payload.clone())
            .map_err(|e| DecodeError::Decode(e.into()))?;

        // `use_proto_field_name(true)` so JSON keys match the .proto source (snake_case) the
        // user wrote, rather than prost-reflect's default lowerCamelCase -- this is a debugging
        // tool where the schema and decoded output are read side by side.
        let mut buf = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut buf);
        message
            .serialize_with_options(
                &mut serializer,
                &SerializeOptions::new().use_proto_field_name(true),
            )
            .map_err(|e| DecodeError::Decode(e.into()))?;

        tx.send(DecodePublish {
            payload: buf,
            ..Default::default()
        })
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prost::Message as _;
    use prost_reflect::Value;
    use rumqttc::QoS;
    use tokio::sync::mpsc;

    use super::*;

    const FIXTURE: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/protobuf/device.proto"
    );
    // `device.proto` imports `common/status.proto`, which lives in a sibling directory
    // (`tests/fixtures/common/`), not under `device.proto`'s own directory -- so resolving it
    // requires an explicit include path covering both, not just the default single-directory one.
    const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

    fn test_config() -> ProtobufConfig {
        ProtobufConfig {
            proto_file: FIXTURE.to_string(),
            message_type: "device.v1.DeviceReading".to_string(),
            include_paths: vec![FIXTURES_DIR.to_string()],
        }
    }

    #[tokio::test]
    async fn decodes_valid_wire_bytes_to_json() {
        let decoder = ProtobufDecoder::from_config(&test_config(), Path::new(".")).unwrap();

        let mut message = DynamicMessage::new(decoder.descriptor.clone());
        message.set_field_by_name("device_id", Value::String("sensor-42".to_string()));
        message.set_field_by_name("temperature_c", Value::F64(21.5));
        message.set_field_by_name("online", Value::Bool(true));
        message.set_field_by_name(
            "status",
            Value::EnumNumber(1), // CONNECTION_STATUS_ONLINE
        );
        let bytes = message.encode_to_vec();

        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, bytes);
        let (tx, mut rx) = mpsc::channel(1);
        decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();

        let value: serde_json::Value = serde_json::from_slice(&decoded.payload).unwrap();
        assert_eq!(value["device_id"], "sensor-42");
        assert_eq!(value["temperature_c"], 21.5);
        assert_eq!(value["online"], true);
        assert_eq!(value["status"], "CONNECTION_STATUS_ONLINE");
    }

    #[test]
    fn resolves_relative_proto_file_against_base_dir_not_cwd() {
        // `device.proto` doesn't exist relative to the crate root (the test process's CWD) --
        // only under `tests/fixtures/protobuf/`. This only succeeds if resolution actually uses
        // `base_dir`, not `std::env::current_dir()`.
        let base_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/protobuf");
        let cfg = ProtobufConfig {
            proto_file: "device.proto".to_string(),
            message_type: "device.v1.DeviceReading".to_string(),
            // Relative to `base_dir` (tests/fixtures/protobuf/), so this resolves to
            // tests/fixtures/ -- the import "common/status.proto" is joined against this,
            // landing on tests/fixtures/common/status.proto, where it actually lives.
            include_paths: vec!["..".to_string()],
        };

        ProtobufDecoder::from_config(&cfg, &base_dir).unwrap();
    }

    #[test]
    fn fails_to_compile_without_include_paths_for_import() {
        // Without an explicit `include_paths`, the default (device.proto's own parent
        // directory) doesn't cover the sibling `tests/fixtures/common/status.proto` it imports,
        // so compilation must fail -- proving `include_paths` is actually load-bearing here, not
        // just accepted-and-ignored.
        let cfg = ProtobufConfig {
            proto_file: FIXTURE.to_string(),
            message_type: "device.v1.DeviceReading".to_string(),
            include_paths: Vec::new(),
        };
        let err = ProtobufDecoder::from_config(&cfg, Path::new(".")).unwrap_err();
        assert!(matches!(err, ProtobufDecoderError::Compile { .. }));
    }

    #[test]
    fn fails_to_construct_with_missing_proto_file() {
        let cfg = ProtobufConfig {
            proto_file: "does/not/exist.proto".to_string(),
            message_type: "device.v1.DeviceReading".to_string(),
            include_paths: Vec::new(),
        };
        let err = ProtobufDecoder::from_config(&cfg, Path::new(".")).unwrap_err();
        assert!(matches!(err, ProtobufDecoderError::Compile { .. }));
    }

    #[test]
    fn fails_to_construct_with_unknown_message_type() {
        let mut cfg = test_config();
        cfg.message_type = "device.v1.Nonexistent".to_string();
        let err = ProtobufDecoder::from_config(&cfg, Path::new(".")).unwrap_err();
        assert!(matches!(err, ProtobufDecoderError::UnknownMessageType(_)));
    }

    #[tokio::test]
    async fn decode_fails_on_malformed_wire_bytes() {
        let decoder = ProtobufDecoder::from_config(&test_config(), Path::new(".")).unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0xff, 0xff]);
        let (tx, _rx) = mpsc::channel(1);
        assert!(decoder.decode(&publish, tx).await.is_err());
    }
}
