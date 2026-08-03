use std::str::Utf8Error;

use async_trait::async_trait;
use rumqttc::Publish;
use tokio::sync::mpsc::Sender;

use crate::decoder::{DecodeError, DecodePublish, Decoder};

/// Interprets the payload as UTF-8 text. Fails on invalid UTF-8, which exercises the
/// decode-failure / error-annotated-republish path without needing a structured format.
pub struct Utf8Decoder;

#[async_trait]
impl Decoder for Utf8Decoder {
    type Error = Utf8Error;
    fn name(&self) -> &str {
        "utf8"
    }

    async fn decode(
        &self,
        publish: &Publish,
        tx: Sender<DecodePublish>,
    ) -> Result<(), DecodeError<Self::Error>> {
        let payload = std::str::from_utf8(&publish.payload)
            .map_err(DecodeError::Decode)?
            .into();
        tx.send(DecodePublish {
            payload,
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
    async fn utf8_decodes_valid_text() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hello device".to_vec());
        let (tx, mut rx) = mpsc::channel(1);
        Utf8Decoder.decode(&publish, tx).await.unwrap();
        let decoded = rx.recv().await.unwrap();
        assert_eq!(decoded.payload, b"hello device");
    }

    #[tokio::test]
    async fn utf8_fails_on_invalid_bytes() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0xfe]);
        let (tx, _rx) = mpsc::channel(1);
        assert!(Utf8Decoder.decode(&publish, tx).await.is_err());
    }
}
