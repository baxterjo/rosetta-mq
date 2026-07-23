# rosetta-mq

IF `AGENTS-local.md` EXISTS IN THIS DIRECTORY, READ IT BEFORE READING THIS FILE.

## What this is

`rosetta-mq` is a standalone CLI tool for developers debugging IoT systems over MQTT.
It subscribes to a broker, decodes message payloads that arrive in various encoded
formats (JSON, Protobuf, MessagePack, CBOR, hex/binary, etc.), and republishes a
human-readable version of each decoded message back to the same broker on a mirrored
topic (e.g. `devices/42/raw` -> `devices/42/decoded`).

The goal is to make opaque, encoded IoT traffic legible in real time without requiring
developers to write custom decode scripts or attach a debugger to the device — just
point the tool at a broker and topic filter, and readable payloads start showing up
on a parallel topic that any existing MQTT client (MQTT Explorer, MQTTX, mosquitto_sub,
etc.) can subscribe to.


## Core architecture

1. **Subscriber** — connects to a broker, subscribes to a configurable topic filter
   (e.g. `devices/+/raw`).
2. **Decoder registry** — pluggable decoders keyed by topic pattern with a "best match wins" strategy. Meaning matches with wildcards do not run if there is a decoder with an exact match. MVP decoder will be protobuf. But application must be designed in a way that decoders can be easily added. Decoder registry should be publically available as a library if other applications want to embed the capability.
4. **Republisher** — publishes the decoded payload (typically as readable JSON) to a
   mirrored/derived topic on the same broker.
5. **Config** — a TOML config file defines topic mappings and which
   decoder/schema applies to which topics. Avoid hardcoding topic logic.

## Language & deployment

- Rust is the current direction: no GC, small static binaries, strong ecosystem for
  the target codecs, and an MQTT client via `rumqttc`.
- Must cross-compile to common IoT-gateway targets (e.g. `armv7-unknown-linux-*`,
  `aarch64-unknown-linux-*`) in addition to standard desktop/server targets.

## Resolved design decisions

- **Schema handling**: v1 requires the user to supply a schema (e.g. a `.proto` file
  or FileDescriptorSet) per topic for Protobuf decoding. No heuristic/best-effort
  fallback in v1 — revisit if unschematized traffic becomes a common ask.
- **Directionality**: strictly one-way (encoded -> readable). No support for encoding
  readable input back to raw formats / injecting test messages in v1.
- **Multi-broker support**: one broker connection per instance. Run multiple instances
  (one config each) if multiple brokers need to be observed simultaneously.
- **Decode failures**: never drop a message silently. If no decoder can successfully
  decode a payload (missing schema match, corrupt bytes, mismatched schema, etc.),
  still republish to the decoded topic with an error-annotated payload (e.g.
  `{"error": "...", "raw_hex": "..."}`) so the mirrored topic always reflects one
  message per input message.

These are v1 decisions, not permanent constraints — future work may revisit them
(e.g. adding heuristic decoding, round-trip encode, or multi-broker support), but
don't build toward those speculatively unless asked.

## Non-goals

- Not a general-purpose MQTT broker or GUI client — there are good tools for that
  already (MQTTX, MQTT Explorer). Stay focused on the decode/republish pipeline.
- Not a security/pentesting tool (that's ZANCUDO's niche) — don't build MITM/TLS-
  interception features unless explicitly requested later.
- Not trying to replace broker-native rule engines (e.g. EMQX) — the value is being
  broker-agnostic and dependency-free.

## Working conventions for this repo

- Prefer small, composable modules — especially around the decoder registry, since
  new codecs will be added frequently.
- Code comments and doc comments should never reference the past state of the codebase. They are only to document the current state.
- When running tests, always use cargo-nextest.
-= Any changes to config must come with a corresponding change to the README.



