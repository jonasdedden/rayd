//! `bincode` encode / decode helpers for plasma wire frames.
//!
//! Frames on the UDS are `[u32 LE length][bincode body]`. Frame bodies are
//! always `Request` or `Response` from `crate::wire`.

use bincode::config::{Configuration, Fixint, Limit, LittleEndian, NoLimit};
use bincode::{decode_from_slice, encode_to_vec, Decode, Encode};

/// Maximum frame body size we accept. 8 MiB is plenty for control messages —
/// all bulk data goes through the mmap, never through the socket.
pub const MAX_FRAME_BYTES: u64 = 8 * 1024 * 1024;

/// Bincode configuration: little-endian, fixed-int encoding (matches `to_le_bytes`).
type Cfg = Configuration<LittleEndian, Fixint, NoLimit>;

const fn config() -> Cfg {
    bincode::config::standard()
        .with_little_endian()
        .with_fixed_int_encoding()
}

const fn limited_config() -> Configuration<LittleEndian, Fixint, Limit<{ MAX_FRAME_BYTES as usize }>>
{
    bincode::config::standard()
        .with_little_endian()
        .with_fixed_int_encoding()
        .with_limit::<{ MAX_FRAME_BYTES as usize }>()
}

/// Encode a request/response body to a vector.
pub fn encode<T: Encode>(value: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    encode_to_vec(value, config())
}

/// Decode a request/response body from bytes.
pub fn decode<T: Decode<()>>(bytes: &[u8]) -> Result<T, bincode::error::DecodeError> {
    let (value, _read) = decode_from_slice(bytes, limited_config())?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{AddressBlob, CreateRequest, Request, Response, SlotInfo};

    fn round_trip<T: Encode + Decode<()> + std::fmt::Debug>(value: &T) -> T {
        let encoded = encode(value).expect("encode");
        decode::<T>(&encoded).expect("decode")
    }

    #[test]
    fn create_request_round_trips() {
        let req = Request::Create(CreateRequest {
            object_id: [7u8; 28],
            metadata_size: 4,
            data_size: 1024,
            owner: AddressBlob::new("h".into(), 9, [0xab; 16]),
        });
        let _decoded: Request = round_trip(&req);
    }

    #[test]
    fn slot_info_round_trips_via_response() {
        let resp = Response::Got(SlotInfo {
            arena_id: 0,
            arena_capacity: 1 << 20,
            metadata_offset: 0,
            metadata_size: 2,
            data_offset: 16,
            data_size: 100,
            sealed: true,
        });
        let _decoded: Response = round_trip(&resp);
    }

    #[test]
    fn list_response_with_many_ids_round_trips() {
        let ids: Vec<[u8; 28]> = (0u8..16u8).map(|i| [i; 28]).collect();
        let _decoded: Response = round_trip(&Response::Listed { object_ids: ids });
    }
}
