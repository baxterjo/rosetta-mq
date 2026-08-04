# Manual end-to-end testing helpers for rosetta-mq -- run each in its own terminal:
#
#   just broker      # 1: local mosquitto broker
#   just run         # 2: rosetta-mq against rosetta-mq.toml
#   just watch       # 3: see every raw/decoded/decode_error message go by
#   just pub / just pub-proto / just pub-invalid   # 4: publish test messages

# Start a local mosquitto broker on 127.0.0.1:1883 (anonymous access, foreground).
broker:
    mosquitto -c {{ justfile_directory() }}/dev/mosquitto.conf -v

# Run rosetta-mq against the example config.
run:
    cargo run --bin rosetta-mq -- --config {{ justfile_directory() }}/tests/fixtures/config/rosetta-mq.toml --log-level debug

# Run tests using cargo-nextest
test:
    cargo nextest run

# Run coverage report
coverage:
    cargo llvm-cov --html --open nextest

# Watch every message on the broker -- raw input alongside its decoded/decode_error output.
watch:
    mosquitto_sub -h 127.0.0.1 -v -t '#'

# Publish a message, defaults to a JSON formatted string - exerdecoder on devices/42.
[arg("message", long, short)]
[arg("topic", long, short)]
pub topic="devices/42" message='{"device_id": "sensor-42", "temperature_c": 21.5}':
    mosquitto_pub -h 127.0.0.1 -t "{{ topic }}" -m '{{ message }}'

# Publish a real protobuf-encoded DeviceReading -- exercises the `protobuf` decoder on
# sensors/proto/#. Encoded via the `encode_device_reading` example, so no `protoc` is needed.
pub-proto:
    cargo run --quiet --example encode_device_reading | mosquitto_pub -h 127.0.0.1 -t sensors/proto/reading -s

# Publish invalid UTF-8 -- exercises the never-drop-silently error-annotated republish path.
pub-invalid:
    printf '\xff\xfe' | mosquitto_pub -h 127.0.0.1 -t devices/42/raw -s
