use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Publish};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::decoder::{DecoderRegistry, OutputBehavior, RegistryEntry};

/// Bounded so a misbehaving decoder that streams many messages applies backpressure instead of
/// growing memory unbounded; small since decoded messages are drained concurrently with decode
/// (see `decode_and_publish` below), not buffered up-front.
const DECODE_CHANNEL_CAPACITY: usize = 8;

/// How long shutdown waits for in-flight decode/publish tasks to finish on their own before
/// abandoning whatever's left -- so a user hitting Ctrl+C isn't stuck waiting on a decode that's
/// hung (e.g. a wedged child process).
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

//  _____ ____  _   _ ______ _____ _____
// / ____/ __ \| \ | |  ____|_   _/ ____|
//| |   | |  | |  \| | |__    | || |  __
//| |   | |  | | . ` |  __|   | || | |_ |
//| |___| |__| | |\  | |     _| || |__| |
// \_____\____/|_| \_|_|    |_____\_____|

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    /// Maximum number of incoming messages decoded and republished concurrently.
    #[serde(default = "EngineConfig::default_max_concurrent_decodes")]
    pub max_concurrent_decodes: usize,
}

impl EngineConfig {
    fn default_max_concurrent_decodes() -> usize {
        100
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrent_decodes: Self::default_max_concurrent_decodes(),
        }
    }
}

/// Decode/republish loop: for each incoming message, resolves a registry entry for its topic and
/// spawns a task that drains whatever the decoder publishes, routing successful output through
/// the entry's `success_output` and any terminal decode error through its `error_output` (see
/// `emit`). A topic with no decoder configured at all is a silent no-op by design (subscribed for
/// visibility, never mirrored) — see `handle_incoming`. A failed publish is logged rather than
/// fatal — one bad publish must not kill the rest of the subscriber loop.
///
/// Before spawning, `handle_incoming` checks whether the incoming message is this same entry's
/// own previously published output (see `RegistryEntry::consume_echo`) and skips it if so —
/// replaces the old blanket "topic ends in /decoded or /decode_error" check, which can't work
/// once output topics are arbitrary and config-driven. Scoping that check to the entry rather
/// than the whole registry is what lets decoders be intentionally chained through the broker (one
/// decoder's output topic feeding another's input filter) without it being mistaken for a
/// feedback loop.
///
/// Up to `max_concurrent_decodes` messages are decoded and republished concurrently, so a slow or
/// long-streaming decode doesn't stall messages behind it. Once that many are in flight, intake
/// pauses until one finishes.
///
/// Cancelling `shutdown` stops intake immediately and gives outstanding tasks up to
/// `SHUTDOWN_DRAIN_TIMEOUT` to finish before `run` returns, rather than the caller simply dropping
/// this future (which would abort every in-flight decode/publish mid-flight).
pub async fn run(
    mut incoming: mpsc::Receiver<Publish>,
    client: AsyncClient,
    registry: DecoderRegistry,
    max_concurrent_decodes: usize,
    shutdown: CancellationToken,
) {
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            // Only offered while there's room in the concurrency budget, so a full `tasks` set
            // applies backpressure on `incoming` (and transitively on the broker connection's own
            // channel) rather than accepting a message it has nowhere to run.
            message = incoming.recv(), if tasks.len() < max_concurrent_decodes => {
                if handle_incoming(message, &registry, &client, &mut tasks).is_break() {
                    break;
                }
            }
            // Reaps completed tasks.
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                reap(result);
            }
            () = shutdown.cancelled() => {
                break;
            }
        }
    }

    // The loop above ends for one of two reasons: `incoming` closed, or `shutdown` was
    // cancelled. Either way, give outstanding tasks a bounded window to finish on their own
    // before abandoning them -- `tasks` aborts anything still running when it's dropped at the
    // end of this function.
    if timeout(SHUTDOWN_DRAIN_TIMEOUT, drain(&mut tasks))
        .await
        .is_err()
    {
        tracing::warn!(
            remaining = tasks.len(),
            "timed out waiting for in-flight decode tasks to finish; abandoning the rest"
        );
    }
}

async fn drain(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.join_next().await {
        reap(result);
    }
}

/// Handles one item off `incoming`: `Break` means the channel closed and `run`'s loop should
/// stop; `Continue` covers everything else (own-echo skip, no-decoder-configured skip, or a
/// spawned task).
fn handle_incoming(
    message: Option<Publish>,
    registry: &DecoderRegistry,
    client: &AsyncClient,
    tasks: &mut JoinSet<()>,
) -> ControlFlow<()> {
    let Some(message) = message else {
        return ControlFlow::Break(());
    };

    let Some(entry) = registry.resolve(&message.topic) else {
        // No decoder was configured for this topic -- an intentional, expected state (a
        // subscribe-only topic), not a problem, so this doesn't warrant a warning.
        tracing::debug!(
            topic = %message.topic,
            "no decoder configured for topic; message received, not republished"
        );
        return ControlFlow::Continue(());
    };

    if entry.consume_echo(&message.topic) {
        tracing::debug!(
            topic = %message.topic,
            decoder = %entry.name,
            "own previously published output; not treated as new input"
        );
        return ControlFlow::Continue(());
    }

    let client = client.clone();
    tasks.spawn(decode_and_publish(entry, message, client));
    ControlFlow::Continue(())
}

fn reap(result: Result<(), JoinError>) {
    if let Err(err) = result {
        tracing::error!(error = %err, "decode task panicked");
    }
}

async fn decode_and_publish(entry: Arc<RegistryEntry>, message: Publish, client: AsyncClient) {
    let (tx, mut rx) = mpsc::channel(DECODE_CHANNEL_CAPACITY);
    // Decode and drain concurrently: the decoder holds `tx` and may block on a full channel
    // mid-decode (e.g. streaming many messages), so the channel must be drained while decode is
    // still running rather than after it completes.
    let decode = entry.decoder.decode(&message, tx);
    let drain = async {
        while let Some(decoded) = rx.recv().await {
            emit(&entry, &entry.success_output, decoded.payload, &message, &client).await;
        }
    };
    let (result, ()) = tokio::join!(decode, drain);

    if let Err(err) = result {
        let payload =
            format!("error: {err}\nraw_hex: {}", hex::encode(&message.payload)).into_bytes();
        emit(&entry, &entry.error_output, payload, &message, &client).await;
    }
}

/// Routes one decoded (or error) payload according to `behavior`. Only `OutputBehavior::Publish`
/// touches the broker, and only a successful publish marks the topic on `entry` (see
/// `RegistryEntry::mark_published`) -- a failed render or failed publish is logged and the
/// message is dropped, the same tolerance the rest of the engine already applies to a failed
/// publish; it must not take down the subscriber loop.
async fn emit(
    entry: &RegistryEntry,
    behavior: &OutputBehavior,
    payload: Vec<u8>,
    incoming: &Publish,
    client: &AsyncClient,
) {
    match behavior {
        OutputBehavior::Publish(args) => {
            let topic = match args.topic.resolve(incoming, &payload) {
                Ok(topic) => topic,
                Err(err) => {
                    tracing::error!(
                        decoder = %entry.name,
                        error = %err,
                        "failed to render output topic template"
                    );
                    return;
                }
            };
            let qos = args.qos.clone().resolve(incoming.qos.into()).into();
            let retain = args.retain.clone().resolve(incoming.retain);
            match client.publish(topic.clone(), qos, retain, payload).await {
                Ok(()) => entry.mark_published(&topic),
                Err(err) => {
                    tracing::error!(
                        decoder = %entry.name,
                        topic = %topic,
                        error = %err,
                        "failed to publish decoded message"
                    )
                }
            }
        }
        OutputBehavior::StdOut => println!("{}", String::from_utf8_lossy(&payload)),
        OutputBehavior::StdErr => eprintln!("{}", String::from_utf8_lossy(&payload)),
        OutputBehavior::Quiet => {}
    }
}
