//! Encodes a sample `device.v1.DeviceReading` message (the same schema used by
//! `tests/fixtures/protobuf/device.proto` and the `protobuf` mapping in `rosetta-mq.toml`) to
//! raw protobuf wire bytes on stdout, for manually exercising the protobuf decoder without
//! needing `protoc` installed:
//!
//!   cargo run --quiet --example encode_device_reading | mosquitto_pub -t sensors/proto/reading -s

use std::io::Write;

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

const PROTO_FILE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/protobuf/device.proto");
// `device.proto` imports `common/status.proto`, a sibling of `protobuf/` -- the include path
// needs to cover both directories, not just device.proto's own parent.
const FIXTURES_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures");

fn main() {
    let file_descriptor_set = protox::compile([PROTO_FILE], [FIXTURES_DIR])
        .expect("compiling tests/fixtures/protobuf/device.proto");
    let pool = DescriptorPool::from_file_descriptor_set(file_descriptor_set)
        .expect("building descriptor pool");
    let descriptor = pool
        .get_message_by_name("device.v1.DeviceReading")
        .expect("device.v1.DeviceReading not found in schema");

    let mut message = DynamicMessage::new(descriptor);
    message.set_field_by_name("device_id", Value::String("sensor-42".to_string()));
    message.set_field_by_name("temperature_c", Value::F64(21.5));
    message.set_field_by_name("online", Value::Bool(true));
    message.set_field_by_name("status", Value::EnumNumber(1)); // CONNECTION_STATUS_ONLINE

    std::io::stdout()
        .write_all(&message.encode_to_vec())
        .expect("writing encoded bytes to stdout");
}
