# Manual end-to-end testing helpers for rosetta-mq -- run each in its own terminal:
#
#   just broker      # 1: local mosquitto broker
#   just run         # 2: rosetta-mq against rosetta-mq.toml
#   just watch       # 3: see every raw/decoded/decode_error message go by
#   just pub-utf8 / just pub-hex / just pub-proto / just pub-invalid   # 4: publish test messages

# Start a local mosquitto broker on 127.0.0.1:1883 (anonymous access, foreground).
broker:
    mosquitto -c {{justfile_directory()}}/dev/mosquitto.conf -v

# Run rosetta-mq against the example config.
run:
    cargo run --bin rosetta-mq -- --config {{justfile_directory()}}/tests/fixtures/config/rosetta-mq.toml --log-level debug

# Watch every message on the broker -- raw input alongside its decoded/decode_error output.
watch:
    mosquitto_sub -h 127.0.0.1 -v -t '#'

# Publish a UTF-8 text message -- exercises the `utf8` decoder on devices/+/raw.
pub-utf8 message="hello device":
    mosquitto_pub -h 127.0.0.1 -t devices/42/raw -m "{{message}}"

# Publish a text payload -- exercises the `hexdump` decoder on sensors/#.
pub-hex message="sensor-payload":
    mosquitto_pub -h 127.0.0.1 -t sensors/temp -m "{{message}}"

# Publish a real protobuf-encoded DeviceReading -- exercises the `protobuf` decoder on
# sensors/proto/#. Encoded via the `encode_device_reading` example, so no `protoc` is needed.
pub-proto:
    cargo run --quiet --example encode_device_reading | mosquitto_pub -h 127.0.0.1 -t sensors/proto/reading -s

# Publish invalid UTF-8 -- exercises the never-drop-silently error-annotated republish path.
pub-invalid:
    printf '\xff\xfe' | mosquitto_pub -h 127.0.0.1 -t devices/42/raw -s
