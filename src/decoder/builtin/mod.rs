use std::sync::Arc;

use super::Decoder;

mod hexdump;
mod utf8;

pub use hexdump::HexDumpDecoder;
pub use utf8::Utf8Decoder;

/// Temporary string -> decoder lookup for builtin decoders. This is where protobuf/schema-based
/// resolution plugs in later.
pub fn by_name(name: &str) -> Option<Arc<dyn Decoder>> {
    match name {
        "hexdump" => Some(Arc::new(HexDumpDecoder)),
        "utf8" => Some(Arc::new(Utf8Decoder)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn by_name_resolves_builtins_and_none_for_unknown() {
        assert!(by_name("hexdump").is_some());
        assert!(by_name("utf8").is_some());
        assert!(by_name("nonexistent").is_none());
    }
}
