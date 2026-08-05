use std::convert::Infallible;

use async_trait::async_trait;
use rumqttc::Publish;
use tokio::sync::mpsc::Sender;

use crate::decoder::{DecodeError, DecodePublish, Decoder};

/// Always succeeds; renders the payload as a hex dump. Proves the registry/engine wiring
/// without any real decoding logic.
pub struct HexDumpDecoder;

#[async_trait]
impl Decoder for HexDumpDecoder {
    type Error = Infallible;
    fn name(&self) -> &str {
        "hexdump"
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>> {
        tx.send(DecodePublish {
            payload: format!(
                "{} ({} bytes)",
                hex::encode(&publish.payload),
                publish.payload.len()
            )
            .into(),
            ..Default::default()
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::QoS;
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn hexdump_always_succeeds() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, [0xDE, 0xAD, 0xBE, 0xEF]);
        let (tx, mut rx) = mpsc::channel(1);
        HexDumpDecoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"deadbeef (4 bytes)");
    }
}
