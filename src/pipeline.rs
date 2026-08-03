use rumqttc::{AsyncClient, Publish};
use tokio::sync::mpsc;

use crate::decoder::{DecodePublish, DecoderRegistry};

/// Bounded so a misbehaving decoder that streams many messages applies backpressure instead of
/// growing memory unbounded; small since decoded messages are drained concurrently with decode
/// (see `run` below), not buffered up-front.
const DECODE_CHANNEL_CAPACITY: usize = 8;

/// Decode/republish loop: for each incoming message, resolves a decoder for its topic and drains
/// whatever it publishes to `{topic}/decoded` (a decoder may emit zero, one, or a stream of
/// messages). A decoder that fails, or has no match, republishes an error message plus the raw
/// payload as hex to `{topic}/decode_error` instead, so the mirrored topics always reflect one
/// message per input message. A failed publish is logged rather than fatal — one bad publish
/// must not kill the rest of the subscriber loop.
pub async fn run(
    mut incoming: mpsc::Receiver<Publish>,
    client: AsyncClient,
    registry: DecoderRegistry,
) {
    while let Some(message) = incoming.recv().await {
        // A `#` (or otherwise broad) topic_filter can match our own `.../decoded` and
        // `.../decode_error` output topics, which would otherwise cause the pipeline to decode
        // its own republished messages forever. Never treat our own output as new input.
        if message.topic.ends_with("/decoded") || message.topic.ends_with("/decode_error") {
            continue;
        }

        let Some(decoder) = registry.resolve(&message.topic) else {
            tracing::warn!(topic = %message.topic, "no decoder registered for topic");
            continue;
        };

        let (tx, mut rx) = mpsc::channel(DECODE_CHANNEL_CAPACITY);
        // Decode and drain concurrently: the decoder holds `tx` and may block on a full channel
        // mid-decode (e.g. streaming many messages), so the channel must be drained while decode
        // is still running rather than after it completes.
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
}

async fn publish(client: &AsyncClient, decoded: DecodePublish, incoming: &Publish) {
    if let Err(err) = decoded.publish(incoming.clone(), client).await {
        tracing::error!(topic = %incoming.topic, error = %err, "failed to publish decoded message");
    }
}
