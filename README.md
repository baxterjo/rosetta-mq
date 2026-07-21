# rosetta-mq

A general purpose MQTT decode tool for debugging MQTT based applications.

`rosetta-mq` subscribes to a broker, decodes message payloads that arrive in
various encoded formats (UTF-8 text, hex/binary, Protobuf, ...), and
republishes a human-readable version of each message back to the same broker
on a mirrored topic (e.g. `devices/42/raw` -> `devices/42/decoded`). Point it
at a broker and topic filter, and readable payloads start showing up on a
parallel topic that any existing MQTT client (MQTT Explorer, MQTTX,
`mosquitto_sub`, etc.) can subscribe to.

## Installation

```sh
cargo install rosetta-mq
```

Requires a recent stable Rust toolchain. This installs the `rosetta-mq`
binary; no other runtime dependencies are needed (the Protobuf decoder
compiles `.proto` schemas itself at startup, so a `protoc` binary is not
required).

## Configuration

`rosetta-mq` reads a TOML config file — `rosetta-mq.toml` in the current
directory by default, or any path passed via `--config`/`-c`. The file has
two parts: a `[broker]` table, and one or more `[[topic]]` blocks mapping a
topic filter to a decoder.

```toml
[broker]
host = "127.0.0.1"
port = 1883
client_id = "rosetta-mq"

[[topic]]
topic_filter = "devices/+/raw"
decoder = "utf8"

[[topic]]
topic_filter = "sensors/#"
decoder = "hexdump"

[[topic]]
topic_filter = "sensors/proto/#"
decoder = "protobuf"
proto_file = "schemas/device.proto"
message_type = "device.v1.DeviceReading"
# Only needed if device.proto imports another .proto that isn't a sibling of
# proto_file itself -- extra directories to search when resolving imports.
include_paths = ["schemas/common"]
```

### `[broker]`

| Field       | Description                                  |
|-------------|-----------------------------------------------|
| `host`      | Broker hostname or IP address.                |
| `port`      | Broker port (e.g. `1883`).                    |
| `client_id` | MQTT client ID used for this connection.      |

### `[[topic]]`

Each block matches incoming messages against `topic_filter` (a standard MQTT
topic filter — supports `+` and `#` wildcards) and decodes matches with the
decoder named by `decoder`, plus that decoder's own fields as siblings.

If more than one `[[topic]]` block matches the same incoming topic, the most
specific one wins — an exact-match filter is preferred over a wildcard filter
that also matches (e.g. `devices/42/raw` beats `devices/+/raw`).

Built-in decoders:

| `decoder`    | Extra fields                                                                 | Description                                      |
|--------------|-------------------------------------------------------------------------------|---------------------------------------------------|
| `"utf8"`     | —                                                                              | Decodes the payload as UTF-8 text.                |
| `"hexdump"`  | —                                                                              | Renders the raw payload bytes as hex.             |
| `"protobuf"` | `proto_file`, `message_type`, `include_paths` (optional)                     | Decodes a Protobuf payload to JSON using a schema.|

For `"protobuf"`:
- `proto_file` — path to the `.proto` file defining the message, resolved
  relative to the config file's own directory.
- `message_type` — fully-qualified message type to decode the payload as
  (e.g. `device.v1.DeviceReading`).
- `include_paths` — optional list of extra directories to search when
  resolving `import`s in the schema, also resolved relative to the config
  file's directory. `proto_file`'s own directory is always searched; this is
  only needed for imports that live elsewhere.

## Usage

```sh
rosetta-mq --config rosetta-mq.toml
```

| Flag             | Default          | Description                                                        |
|-------------------|------------------|----------------------------------------------------------------------|
| `--config`, `-c`  | `rosetta-mq.toml`| Path to the TOML config file.                                       |
| `--log-level`     | `info`           | Log level, passed through as a `tracing` env-filter (e.g. `debug`, `rosetta_mq=debug`). |

Once running, `rosetta-mq` subscribes to every `topic_filter` in the config
and, for each incoming message, republishes to a mirrored topic on the same
broker:

- `{topic}/decoded` — the decoded, human-readable payload, on success.
- `{topic}/decode_error` — an error message plus the raw payload as hex, if
  no decoder matched or decoding failed. A message is never silently dropped.

For example, a message on `devices/42/raw` produces either
`devices/42/decoded` or `devices/42/decode_error`. Stop `rosetta-mq` at any
time with `Ctrl+C`.

### Trying it locally

The repo's `Justfile` has recipes for exercising the whole pipeline against a
local broker without any external MQTT client:

```sh
just broker      # start a local mosquitto broker
just run         # run rosetta-mq against the example config
just watch       # in another terminal, watch every raw/decoded/decode_error message
just pub-utf8    # publish a message that exercises the utf8 decoder
just pub-hex      # ...the hexdump decoder
just pub-proto    # ...the protobuf decoder
just pub-invalid  # publish invalid input to see the decode_error path
```

See the `Justfile` for details on each recipe.
