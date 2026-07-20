use std::path::{Path, PathBuf};

use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use rumqttc::Publish;
use serde::Deserialize;
use thiserror::Error;

use crate::decoder::{DecodeError, Decoder};

/// Per-topic config for the protobuf decoder: which `.proto` file to compile at runtime, which
/// message type within it to decode payloads as, and any extra include paths the schema's
/// `import`s need to resolve. `proto_file`/`include_paths` are resolved relative to the config
/// file's directory (see `ProtobufDecoder::from_config`), not the process's current directory.
#[derive(Debug, Deserialize)]
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
    message_type: String,
    descriptor: MessageDescriptor,
}

impl ProtobufDecoder {
    pub fn from_config(
        cfg: &ProtobufConfig,
        base_dir: &Path,
    ) -> Result<Self, ProtobufDecoderError> {
        let proto_file = resolve(base_dir, &cfg.proto_file);

        let include_paths: Vec<PathBuf> = if cfg.include_paths.is_empty() {
            vec![proto_file.parent().unwrap_or(base_dir).to_path_buf()]
        } else {
            cfg.include_paths
                .iter()
                .map(|p| resolve(base_dir, p))
                .collect()
        };

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

        Ok(Self {
            message_type: cfg.message_type.clone(),
            descriptor,
        })
    }
}

fn resolve(base_dir: &Path, path: &str) -> PathBuf {
    let path = Path::new(path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

impl Decoder for ProtobufDecoder {
    fn name(&self) -> &str {
        "protobuf"
    }

    fn decode(&self, publish: &Publish) -> Result<String, DecodeError> {
        let message = DynamicMessage::decode(self.descriptor.clone(), publish.payload.clone())
            .map_err(|e| {
                DecodeError::Message(format!(
                    "protobuf decode failed for message type {:?}: {e}",
                    self.message_type
                ))
            })?;

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
            .map_err(|e| {
                DecodeError::Message(format!("failed to serialize decoded message as JSON: {e}"))
            })?;

        Ok(String::from_utf8(buf).expect("serde_json output is valid UTF-8"))
    }
}

#[cfg(test)]
mod tests {
    use prost::Message as _;
    use prost_reflect::Value;
    use rumqttc::QoS;

    use super::*;

    const FIXTURE: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/protobuf/device.proto");

    fn test_config() -> ProtobufConfig {
        ProtobufConfig {
            proto_file: FIXTURE.to_string(),
            message_type: "device.v1.DeviceReading".to_string(),
            include_paths: Vec::new(),
        }
    }

    #[test]
    fn decodes_valid_wire_bytes_to_json() {
        let decoder = ProtobufDecoder::from_config(&test_config(), Path::new(".")).unwrap();

        let mut message = DynamicMessage::new(decoder.descriptor.clone());
        message.set_field_by_name("device_id", Value::String("sensor-42".to_string()));
        message.set_field_by_name("temperature_c", Value::F64(21.5));
        message.set_field_by_name("online", Value::Bool(true));
        let bytes = message.encode_to_vec();

        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, bytes);
        let json = decoder.decode(&publish).unwrap();

        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["device_id"], "sensor-42");
        assert_eq!(value["temperature_c"], 21.5);
        assert_eq!(value["online"], true);
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
            include_paths: Vec::new(),
        };

        ProtobufDecoder::from_config(&cfg, &base_dir).unwrap();
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

    #[test]
    fn decode_fails_on_malformed_wire_bytes() {
        let decoder = ProtobufDecoder::from_config(&test_config(), Path::new(".")).unwrap();
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0xff, 0xff]);
        assert!(decoder.decode(&publish).is_err());
    }
}
