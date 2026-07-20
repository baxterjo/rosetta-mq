use crate::decoder::{DecodeError, Decoder};

/// Always succeeds; renders the payload as a hex dump. Proves the registry/pipeline wiring
/// without any real decoding logic.
pub struct HexDumpDecoder;

impl Decoder for HexDumpDecoder {
    fn name(&self) -> &str {
        "hexdump"
    }

    fn decode(&self, payload: &[u8]) -> Result<String, DecodeError> {
        Ok(format!(
            "{} ({} bytes)",
            hex::encode(payload),
            payload.len()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hexdump_always_succeeds() {
        let out = HexDumpDecoder.decode(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        assert_eq!(out, "deadbeef (4 bytes)");
    }
}
