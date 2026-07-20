use rumqttc::Publish;

use crate::decoder::{DecodeError, Decoder};

/// Interprets the payload as UTF-8 text. Fails on invalid UTF-8, which exercises the
/// decode-failure / error-annotated-republish path without needing a structured format.
pub struct Utf8Decoder;

impl Decoder for Utf8Decoder {
    fn name(&self) -> &str {
        "utf8"
    }

    fn decode(&self, publish: &Publish) -> Result<String, DecodeError> {
        std::str::from_utf8(&publish.payload)
            .map(|s| s.to_string())
            .map_err(|e| DecodeError::Message(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::QoS;

    use super::*;

    #[test]
    fn utf8_decodes_valid_text() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, b"hello device".to_vec());
        assert_eq!(Utf8Decoder.decode(&publish).unwrap(), "hello device");
    }

    #[test]
    fn utf8_fails_on_invalid_bytes() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, vec![0xff, 0xfe]);
        assert!(Utf8Decoder.decode(&publish).is_err());
    }
}
