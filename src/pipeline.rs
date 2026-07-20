use rumqttc::{AsyncClient, QoS};
use tokio::sync::mpsc;

use crate::client::IncomingMessage;
use crate::decoder::DecoderRegistry;

/// Decode/republish loop: for each incoming message, resolves a decoder for its topic and
/// publishes either the decoded text to `{topic}/decoded` or an error-annotated payload to
/// `{topic}/error`. Never drops a message silently, and a failed publish is logged rather than
/// fatal — one bad publish must not kill the rest of the subscriber loop.
pub async fn run(
    mut incoming: mpsc::Receiver<IncomingMessage>,
    client: AsyncClient,
    registry: DecoderRegistry,
) {
    while let Some(message) = incoming.recv().await {
        // A `#` (or otherwise broad) topic_filter can match our own `.../decoded` and
        // `.../error` output topics, which would otherwise cause the pipeline to decode its own
        // republished messages forever. Never treat our own output as new input.
        if message.topic.ends_with("/decoded") || message.topic.ends_with("/decode_error") {
            continue;
        }

        let Some(decoder) = registry.resolve(&message.topic) else {
            tracing::warn!(topic = %message.topic, "no decoder registered for topic");
            continue;
        };

        let (output_topic, payload) = match decoder.decode(&message.payload) {
            Ok(decoded) => (format!("{}/decoded", message.topic), decoded),
            Err(err) => (
                format!("{}/decode_error", message.topic),
                format!("error: {err}\nraw_hex: {}", hex::encode(&message.payload)),
            ),
        };

        if let Err(err) = client
            .publish(output_topic.clone(), QoS::AtLeastOnce, false, payload)
            .await
        {
            tracing::error!(topic = %output_topic, error = %err, "failed to publish");
        }
    }
}
