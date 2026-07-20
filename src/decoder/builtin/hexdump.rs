use rumqttc::Publish;

use crate::decoder::{DecodeError, Decoder};

/// Always succeeds; renders the payload as a hex dump. Proves the registry/pipeline wiring
/// without any real decoding logic.
pub struct HexDumpDecoder;

impl Decoder for HexDumpDecoder {
    fn name(&self) -> &str {
        "hexdump"
    }

    fn decode(&self, publish: &Publish) -> Result<String, DecodeError> {
        Ok(format!(
            "{} ({} bytes)",
            hex::encode(&publish.payload),
            publish.payload.len()
        ))
    }
}

#[cfg(test)]
mod tests {
    use rumqttc::QoS;

    use super::*;

    #[test]
    fn hexdump_always_succeeds() {
        let publish = Publish::new("devices/42/raw", QoS::AtLeastOnce, [0xDE, 0xAD, 0xBE, 0xEF]);
        let out = HexDumpDecoder.decode(&publish).unwrap();
        assert_eq!(out, "deadbeef (4 bytes)");
    }
}
