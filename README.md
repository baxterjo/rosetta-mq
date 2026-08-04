# rosetta-mq

A general purpose MQTT decode tool for debugging MQTT based applications.

`rosetta-mq` subscribes to a broker, decodes message payloads that arrive in
various encoded formats (UTF-8 text, hex/binary, Protobuf, ...), and
republishes a human-readable version of each message back to the same broker
on a mirrored topic (e.g. `devices/42` -> `devices/42/decoded`). Point it
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
three parts: a `[connection]` table, zero or more named `[decoder.NAME]`
decoder definitions, and one or more `[[topic]]` blocks mapping a topic
filter to a decoder — or to nothing at all, for a subscribe-only topic.

```toml
[connection]
host = "127.0.0.1"
port = 1883
client_id = "rosetta-mq"
tls = false

[decoder.utf8]
decoder = "utf8"

[decoder.hex]
decoder = "hexdump"

[decoder.device_proto]
decoder = "protobuf"
proto_file = "schemas/device.proto"
message_type = "device.v1.DeviceReading"
# Only needed if device.proto imports another .proto that isn't a sibling of
# proto_file itself -- extra directories to search when resolving imports.
include_paths = ["schemas/common"]

[decoder.json_template]
decoder = "template"
template = """
{{ topic }}: {{ payload.device_id }} is {{ payload.temperature_c }}C
"""

[[topic]]
topic_filter = "devices/+/raw"
decoder = "utf8"

[[topic]]
topic_filter = "sensors/#"
decoder = "hex"

[[topic]]
topic_filter = "sensors/proto/#"
decoder = "device_proto"

[[topic]]
topic_filter = "devices/+/json"
decoder = "json_template"

[[topic]]
topic_filter = "devices/+/status"
# No `decoder` -- rosetta-mq subscribes so any other MQTT client can see this
# traffic, but doesn't decode or republish it.
```

### `[connection]`

| Field                     | Description                                  |
|---------------------------|-----------------------------------------------|
| `host`                    | Broker hostname or IP address.                |
| `port`                    | Broker port (e.g. `1883`).                    |
| `client_id`               | MQTT client ID used for this connection.      |
| `tls`                     | **Required.** `true`/`false` — whether the connection is encrypted. |
| `allow_self_signed_certs` | Optional, defaults to `false`. When `tls` is `true`, accept the broker's certificate with no verification at all (no root store, no hostname check) — for self-hosted/dev brokers using a self-signed cert where distributing a CA file isn't practical. |
| `auth`                    | Optional `[connection.auth]` table (see below). Omit entirely for an unauthenticated connection. |
| `protocol`                | Optional, defaults to `"mqtt"`. `"mqtt"` connects over plain TCP/TLS; `"ws"` connects over a websocket upgrade and takes an additional `path` field — see below. |


### `[connection.auth]`

Two mutually exclusive auth methods, picked by `method`. Only one may be
configured per broker.

```toml
[connection.auth]
method = "mtls"
ca_file = "certs/ca.pem"
cert_file = "certs/client.pem"
key_file = "certs/client.key"
```

(`tls = true` must also be set on `[connection]` — see above.)

```toml
[connection.auth]
method = "userpass"
username = "device-reader"
password = { env = "MQTT_PASSWORD" } # or a literal string: password = "supersecret"
```

| `method`    | Fields                                    | Description                                                        |
|-------------|--------------------------------------------|---------------------------------------------------------------------|
| `"mtls"`    | `ca_file`, `cert_file`, `key_file`         | Mutual TLS: CA cert, client cert, and client private key (PEM), each resolved relative to the config file's directory. |
| `"userpass"`| `username`, `password`                    | Username/password auth. `password` is either a literal string, or `{ env = "VAR_NAME" }` to read it from an environment variable instead of storing it in the config file. |

### `protocol = "ws"`

Set `protocol = "ws"` when the broker only exposes MQTT over a websocket
upgrade — common for browser-facing and cloud-hosted brokers. `ws`-vs-`wss`
follows the same `tls` flag already documented above for plain TCP. `path`
is only meaningful (and only allowed) alongside `protocol = "ws"`.

```toml
[connection]
protocol = "ws"
# Optional; "/ws" by default. Set explicitly for brokers that mount MQTT
# websocket traffic at a specific path, e.g. "/mqtt" or "/ws".
path = "/mqtt"
```

| Field  | Description                                                        |
|--------|---------------------------------------------------------------------|
| `path` | Optional, defaults to `/ws`. Path the websocket upgrade request is made against. Only valid when `protocol = "ws"`. |

### `[[topic]]`

Each block matches incoming messages against `topic_filter` (a standard MQTT
topic filter — supports `+` and `#` wildcards).

`decoder` is optional. Omit it entirely and `rosetta-mq` still subscribes to
`topic_filter` — any other MQTT client can observe that raw traffic — but
never republishes anything for it. When present, `decoder` is either:
- a string naming a `[decoder.NAME]` table defined elsewhere in the config
  (see below), for a decoder shared across multiple topics; or
- an inline table literal carrying the decoder's own fields directly, for a
  one-off decoder not worth naming, e.g.
  `decoder = { decoder = "utf8" }`.

If more than one `[[topic]]` block matches the same incoming topic, the most
specific one wins — an exact-match filter is preferred over a wildcard filter
that also matches (e.g. `devices/42` beats `devices/#`).

### `[decoder.NAME]`

Each named table defines one reusable decoder, referenced by name (`NAME`)
from any number of `[[topic]]` blocks via `decoder = "NAME"`. Every table
needs its own `decoder` field naming which built-in decoder type it is, plus
that type's own fields as siblings — same shape as the inline literal form
described above, just written once and referenced by name instead of
repeated per topic.

Built-in decoders:

| `decoder`    | Extra fields                                                                 | Description                                      |
|--------------|-------------------------------------------------------------------------------|---------------------------------------------------|
| `"utf8"`     | —                                                                              | Decodes the payload as UTF-8 text. (Mostly used for testing)                |
| `"hexdump"`  | —                                                                              | Renders the raw payload bytes as hex.             |
| `"protobuf"` | `proto_file`, `message_type`, `include_paths` (optional)                     | Decodes a Protobuf payload to JSON using a schema.|
| `"template"` | `template`, `undefined_behavior` (optional)                                   | Renders the message through a user-authored Jinja2-compatible template.|

For `"protobuf"`:
- `proto_file` — path to the `.proto` file defining the message, resolved
  relative to the config file's own directory.
- `message_type` — fully-qualified message type to decode the payload as
  (e.g. `device.v1.DeviceReading`).
- `include_paths` — optional list of extra directories to search when
  resolving `import`s in the schema, also resolved relative to the config
  file's directory. `proto_file`'s own directory is always searched; this is
  only needed for imports that live elsewhere.

For `"template"`:
- `template` — the template text itself, written inline in the config
  (typically as a TOML triple-quoted string). Syntax is Jinja2-compatible,
  via the [`minijinja`](https://docs.rs/minijinja) engine.
- The template has access to every field of the incoming MQTT packet:
  `topic`, `qos`, `retain`, `dup`, and `pkid`.
- `payload` is also available and adapts to the payload's content: if it's
  valid JSON, `payload` is the parsed value and can be indexed
  (`{{ payload.device_id }}`, `{{ payload[0] }}`, ...); if it's valid UTF-8
  text but not JSON, `payload` is that text as a plain string; otherwise
  `payload` is the raw bytes hex-encoded as a string.
- `undefined_behavior` — optional, defaults to `"strict"`. Controls what
  happens when a template references something undefined — a missing JSON
  key, indexing into a payload that isn't JSON, a typo'd variable name. Where
  the table below says "decode failure", that's treated like any other
  decode failure: republished (to `.../decode_error`, with the render error
  and the raw payload as hex) rather than silently dropped.
  | Value              | Behavior                                                            |
  |--------------------|----------------------------------------------------------------------|
  | `"strict"`         | Any use of an undefined value (printing, iterating, attribute access, truthiness) is a decode failure. |
  | `"semi_strict"`    | Like `"strict"`, but checking an undefined value for truthiness (e.g. `{% if maybe_field %}`) is allowed instead of failing. |
  | `"chainable"`      | Attribute access on an undefined value returns another undefined value instead of failing, so a chain like `{{ payload.a.b }}` fails only when printed/iterated, not at the first missing link. |
  | `"lenient"`        | Undefined values print as an empty string and iterate as empty — matches Jinja2's own default behavior. |

### `[pipeline]`

```toml
[pipeline]
# Optional; defaults to 100. Maximum number of incoming messages decoded and
# republished concurrently. Once this many are in flight, the pipeline stops
# pulling new messages until one finishes.
max_concurrent_decodes = 100
```

| Field                    | Description                                                        |
|--------------------------|----------------------------------------------------------------------|
| `max_concurrent_decodes` | Optional, defaults to `100`. Must be at least `1`. Caps how many messages are decoded and republished concurrently. |

## Usage

```sh
rosetta-mq --config rosetta-mq.toml
```

| Flag             | Default          | Description                                                        |
|-------------------|------------------|----------------------------------------------------------------------|
| `--config`, `-c`  | `rosetta-mq.toml`| Path to the TOML config file.                                       |
| `--log-level`     | `info`           | Log level, passed through as a `tracing` env-filter (e.g. `debug`, `rosetta_mq=debug`). |

Once running, `rosetta-mq` subscribes to every `topic_filter` in the config.
For topics with a `decoder` assigned, each incoming message is republished to
a mirrored topic on the same broker:

- `{topic}/decoded` — the decoded, human-readable payload, on success.
- `{topic}/decode_error` — an error message plus the raw payload as hex, if
  decoding failed. A message is never silently dropped.

For example, a message on `devices/42` produces either
`devices/42/decoded` or `devices/42/decode_error`. Topics with no `decoder`
assigned are subscribed but not mirrored at all — the raw traffic is visible
to any other MQTT client, and that's it. Stop `rosetta-mq` at any time with
`Ctrl+C`.

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
