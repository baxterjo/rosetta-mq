use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::Duration;

use rumqttc::{AsyncClient, Publish};
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinSet};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::decoder::{DecodePublish, DecoderRegistry, ErasedDecoder};

/// Bounded so a misbehaving decoder that streams many messages applies backpressure instead of
/// growing memory unbounded; small since decoded messages are drained concurrently with decode
/// (see `decode_and_publish` below), not buffered up-front.
const DECODE_CHANNEL_CAPACITY: usize = 8;

/// How long shutdown waits for in-flight decode/publish tasks to finish on their own before
/// abandoning whatever's left -- so a user hitting Ctrl+C isn't stuck waiting on a decode that's
/// hung (e.g. a wedged child process).
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Decode/republish loop: for each incoming message, resolves a decoder for its topic and spawns
/// a task that drains whatever it publishes to `{topic}/decoded` (a decoder may emit zero, one,
/// or a stream of messages). A decoder that fails republishes an error message plus the raw
/// payload as hex to `{topic}/decode_error` instead, so a topic with a decoder assigned always
/// gets one output message per input message. A topic with no decoder configured at all is a
/// silent no-op by design (subscribed for visibility, never mirrored) — see `handle_incoming`. A
/// failed publish is logged rather than fatal — one bad publish must not kill the rest of the
/// subscriber loop.
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
/// stop; `Continue` covers everything else (own-topic skip, no-decoder-configured skip, or a
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

    // A `#` (or otherwise broad) topic_filter can match our own `.../decoded` and
    // `.../decode_error` output topics, which would otherwise cause the engine to decode its
    // own republished messages forever. Never treat our own output as new input.
    if message.topic.ends_with("/decoded") || message.topic.ends_with("/decode_error") {
        return ControlFlow::Continue(());
    }

    let Some(decoder) = registry.resolve(&message.topic) else {
        // No decoder was configured for this topic -- an intentional, expected state (a
        // subscribe-only topic), not a problem, so this doesn't warrant a warning.
        tracing::debug!(
            topic = %message.topic,
            "no decoder configured for topic; message received, not republished"
        );
        return ControlFlow::Continue(());
    };

    let client = client.clone();
    tasks.spawn(decode_and_publish(decoder, message, client));
    ControlFlow::Continue(())
}

fn reap(result: Result<(), JoinError>) {
    if let Err(err) = result {
        tracing::error!(error = %err, "decode task panicked");
    }
}

async fn decode_and_publish(
    decoder: Arc<dyn ErasedDecoder>,
    message: Publish,
    client: AsyncClient,
) {
    let (tx, mut rx) = mpsc::channel(DECODE_CHANNEL_CAPACITY);
    // Decode and drain concurrently: the decoder holds `tx` and may block on a full channel
    // mid-decode (e.g. streaming many messages), so the channel must be drained while decode is
    // still running rather than after it completes.
    let decode = decoder.decode(&message, tx);
    let drain = async {
        while let Some(decoded) = rx.recv().await {
            publish(&client, decoded, &message).await;
        }
    };
    let (result, ()) = tokio::join!(decode, drain);

    if let Err(err) = result {
        let error_payload = DecodePublish {
            topic: Some(format!("{}/decode_error", message.topic)),
            payload: format!("error: {err}\nraw_hex: {}", hex::encode(&message.payload))
                .into_bytes(),
            ..Default::default()
        };
        publish(&client, error_payload, &message).await;
    }
}

async fn publish(client: &AsyncClient, decoded: DecodePublish, incoming: &Publish) {
    if let Err(err) = decoded.publish(incoming.clone(), client).await {
        tracing::error!(topic = %incoming.topic, error = %err, "failed to publish decoded message");
    }
}
