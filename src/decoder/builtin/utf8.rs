use crate::decoder::{DecodeError, Decoder};

/// Interprets the payload as UTF-8 text. Fails on invalid UTF-8, which exercises the
/// decode-failure / error-annotated-republish path without needing a structured format.
pub struct Utf8Decoder;

impl Decoder for Utf8Decoder {
    fn name(&self) -> &str {
        "utf8"
    }

    fn decode(&self, payload: &[u8]) -> Result<String, DecodeError> {
        std::str::from_utf8(payload)
            .map(|s| s.to_string())
            .map_err(|e| DecodeError::Message(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_decodes_valid_text() {
        assert_eq!(Utf8Decoder.decode(b"hello device").unwrap(), "hello device");
    }

    #[test]
    fn utf8_fails_on_invalid_bytes() {
        assert!(Utf8Decoder.decode(&[0xff, 0xfe]).is_err());
    }
}
