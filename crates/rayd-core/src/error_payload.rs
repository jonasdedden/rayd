//! Wire format for the `data` buffer of failed `RayObject`s.
//!
//! Read alongside `crate::error_info::ErrorInfo`: `ErrorPayload` is the
//! on-the-wire form; `ErrorInfo` is the user-facing summary projected from
//! it without unpickling the user exception.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

/// Encoding/decoding errors for `ErrorPayload`.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ErrorPayloadCodecError {
    /// Buffer ended before the expected number of bytes.
    #[error("error payload truncated: needed {needed} more bytes")]
    Truncated {
        /// How many additional bytes the parser needed.
        needed: usize,
    },
    /// A length prefix would require more memory than the buffer holds.
    #[error("error payload length prefix {claimed} exceeds remaining {remaining}")]
    LengthOverflow {
        /// The claimed length value.
        claimed: u64,
        /// Bytes remaining in the buffer.
        remaining: usize,
    },
    /// String section was not valid UTF-8.
    #[error("error payload string section was not valid UTF-8")]
    InvalidUtf8,
    /// Boolean flag byte was neither 0 nor 1.
    #[error("error payload flag byte was {0}, expected 0 or 1")]
    InvalidFlag(u8),
}

/// On-the-wire form of an error stored in a `RayObject`'s data buffer.
///
/// Wire encoding (manual; small enough that adding `serde` is overkill):
/// ```text
/// [u32 LE: message_len]
/// [message bytes (UTF-8)]
/// [u8 flag: traceback present?]
///   if 1:
///     [u32 LE: traceback_len]
///     [traceback bytes (UTF-8)]
/// [u16 LE: raw_code]
/// [u8 flag: pickled exception present?]
///   if 1:
///     [u32 LE: pickled_len]
///     [pickled bytes]
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorPayload {
    /// Human-readable message, always present.
    pub message: String,
    /// Formatted traceback. Present for `TaskException`; otherwise `None`.
    pub traceback: Option<String>,
    /// Granular error code mirroring the Ray-style `ErrorType` integer.
    pub raw_code: u16,
    /// Pickled Python exception. Present for `TaskException`; otherwise
    /// `None`. Decoded only by `ObjectRef::exception()`, never by
    /// `peek_error()` — this keeps cheap state checks out of the
    /// pickle path.
    pub pickled_python_exception: Option<Bytes>,
}

impl ErrorPayload {
    /// Construct a payload with just a message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            traceback: None,
            raw_code: 0,
            pickled_python_exception: None,
        }
    }

    /// Set the traceback (typically only meaningful for `TaskException`).
    #[must_use]
    pub fn with_traceback(mut self, tb: impl Into<String>) -> Self {
        self.traceback = Some(tb.into());
        self
    }

    /// Set the granular `raw_code`.
    #[must_use]
    pub const fn with_raw_code(mut self, raw_code: u16) -> Self {
        self.raw_code = raw_code;
        self
    }

    /// Attach a pickled Python exception.
    #[must_use]
    pub fn with_pickled_exception(mut self, pickled: Bytes) -> Self {
        self.pickled_python_exception = Some(pickled);
        self
    }

    /// Encode into a fresh `Bytes`.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let cap = 4
            + self.message.len()
            + 1
            + self.traceback.as_deref().map_or(0, |t| 4 + t.len())
            + 2
            + 1
            + self
                .pickled_python_exception
                .as_ref()
                .map_or(0, |b| 4 + b.len());
        let mut buf = BytesMut::with_capacity(cap);
        buf.put_u32_le(u32::try_from(self.message.len()).expect("message under 4GiB"));
        buf.put_slice(self.message.as_bytes());
        match &self.traceback {
            None => buf.put_u8(0),
            Some(tb) => {
                buf.put_u8(1);
                buf.put_u32_le(u32::try_from(tb.len()).expect("traceback under 4GiB"));
                buf.put_slice(tb.as_bytes());
            }
        }
        buf.put_u16_le(self.raw_code);
        match &self.pickled_python_exception {
            None => buf.put_u8(0),
            Some(blob) => {
                buf.put_u8(1);
                buf.put_u32_le(u32::try_from(blob.len()).expect("pickled exception under 4GiB"));
                buf.put_slice(blob);
            }
        }
        buf.freeze()
    }

    /// Decode a wire payload.
    pub fn decode(mut input: &[u8]) -> Result<Self, ErrorPayloadCodecError> {
        let buf = &mut input;
        let message = read_string(buf)?;
        let traceback = if read_flag(buf)? {
            Some(read_string(buf)?)
        } else {
            None
        };
        let raw_code = read_u16_le(buf)?;
        let pickled_python_exception = if read_flag(buf)? {
            Some(read_bytes(buf)?)
        } else {
            None
        };
        Ok(Self {
            message,
            traceback,
            raw_code,
            pickled_python_exception,
        })
    }
}

fn ensure(buf: &[u8], needed: usize) -> Result<(), ErrorPayloadCodecError> {
    if buf.len() < needed {
        Err(ErrorPayloadCodecError::Truncated {
            needed: needed - buf.len(),
        })
    } else {
        Ok(())
    }
}

fn read_u16_le(buf: &mut &[u8]) -> Result<u16, ErrorPayloadCodecError> {
    ensure(buf, 2)?;
    Ok(buf.get_u16_le())
}

fn read_u32_le(buf: &mut &[u8]) -> Result<u32, ErrorPayloadCodecError> {
    ensure(buf, 4)?;
    Ok(buf.get_u32_le())
}

fn read_flag(buf: &mut &[u8]) -> Result<bool, ErrorPayloadCodecError> {
    ensure(buf, 1)?;
    let v = buf.get_u8();
    match v {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ErrorPayloadCodecError::InvalidFlag(other)),
    }
}

fn read_length_prefixed<'a>(buf: &mut &'a [u8]) -> Result<&'a [u8], ErrorPayloadCodecError> {
    let len = read_u32_le(buf)? as usize;
    if buf.len() < len {
        return Err(ErrorPayloadCodecError::LengthOverflow {
            claimed: len as u64,
            remaining: buf.len(),
        });
    }
    let (head, tail) = buf.split_at(len);
    *buf = tail;
    Ok(head)
}

fn read_string(buf: &mut &[u8]) -> Result<String, ErrorPayloadCodecError> {
    let bytes = read_length_prefixed(buf)?;
    core::str::from_utf8(bytes)
        .map(ToOwned::to_owned)
        .map_err(|_| ErrorPayloadCodecError::InvalidUtf8)
}

fn read_bytes(buf: &mut &[u8]) -> Result<Bytes, ErrorPayloadCodecError> {
    let bytes = read_length_prefixed(buf)?;
    Ok(Bytes::copy_from_slice(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_minimal() {
        let p = ErrorPayload::new("boom");
        let encoded = p.encode();
        let decoded = ErrorPayload::decode(&encoded).expect("decode");
        assert_eq!(decoded, p);
    }

    #[test]
    fn round_trip_full() {
        let p = ErrorPayload::new("user error")
            .with_traceback("Traceback...\n  ...")
            .with_raw_code(3)
            .with_pickled_exception(Bytes::from_static(b"\x80\x05pickled"));
        let encoded = p.encode();
        let decoded = ErrorPayload::decode(&encoded).expect("decode");
        assert_eq!(decoded, p);
    }

    #[test]
    fn truncated_input_errors() {
        assert!(matches!(
            ErrorPayload::decode(&[]),
            Err(ErrorPayloadCodecError::Truncated { .. })
        ));
        assert!(matches!(
            ErrorPayload::decode(&[1, 0, 0, 0]),
            Err(ErrorPayloadCodecError::LengthOverflow { .. })
        ));
    }

    #[test]
    fn invalid_flag_errors() {
        let bad = {
            let mut p = ErrorPayload::new("x").encode().to_vec();
            // First flag byte sits right after the message (4 + 1 = 5).
            p[5] = 7;
            p
        };
        assert!(matches!(
            ErrorPayload::decode(&bad),
            Err(ErrorPayloadCodecError::InvalidFlag(7))
        ));
    }

    #[test]
    fn message_carries_through() {
        let p = ErrorPayload::new("こんにちは");
        let decoded = ErrorPayload::decode(&p.encode()).expect("decode");
        assert_eq!(decoded.message, "こんにちは");
    }
}
