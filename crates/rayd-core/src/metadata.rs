//! Typed metadata that travels alongside every stored value.
//!
//! Replaces Ray's stringly-typed metadata bytes (`b"RAW"`, `b"PYTHON"`,
//! `b"3"` for `TASK_EXECUTION_EXCEPTION`, etc.) with a discriminated enum.
//! The wire encoding is documented inline so the format is reviewable and
//! versionable.

use thiserror::Error;

/// Lifecycle state of an `ObjectRef` as observed from the holder's worker.
///
/// A snapshot. `READY_LOCAL` and `FAILED` are terminal once observed; the
/// other two may transition. See `docs/design/05-state-and-error-api.md`
/// for the full guarantees.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum RefState {
    /// The value is not yet present in any store we know about.
    Pending,
    /// The value is materialized on the local node (memory store or local plasma).
    ReadyLocal,
    /// The value is materialized somewhere in the cluster but not on this node;
    /// `get()` will trigger a `Pull`.
    ReadyRemote,
    /// The value is an error sentinel; `get()` will raise.
    Failed,
}

impl RefState {
    /// Whether the state is one of `ReadyLocal | ReadyRemote | Failed`.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::ReadyLocal | Self::ReadyRemote | Self::Failed)
    }

    /// Whether the state is `Failed`.
    #[must_use]
    pub const fn is_failed(self) -> bool {
        matches!(self, Self::Failed)
    }
}

/// User-meaningful error category. Coarser than Ray's `ErrorType` enum so the
/// public API is easier to pattern-match against; the granular code is kept
/// in `ErrorInfo::raw_code`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum ErrorCategory {
    /// User code raised a Python exception.
    TaskException = 1,
    /// Worker process died (segfault, OS kill, raylet death).
    WorkerDied = 2,
    /// Actor died and could not be restarted.
    ActorDied = 3,
    /// The owning worker died; the value is unreconstructable.
    OwnerDied = 4,
    /// Task was cancelled explicitly.
    TaskCancelled = 5,
    /// Object was lost from plasma; lineage may or may not recover.
    ObjectLost = 6,
    /// Object lost and lineage reconstruction exhausted.
    ObjectUnreconstructable = 7,
    /// Object exists somewhere but couldn't be pulled in time.
    FetchTimeout = 8,
    /// Worker startup or runtime-env materialization failed.
    RuntimeEnvFailed = 9,
    /// Task could not be scheduled (no feasible node, placement-group removed, ...).
    Unschedulable = 10,
    /// Out of memory or out of disk on the executing node.
    OutOfMemory = 11,
}

impl ErrorCategory {
    /// Discriminator byte used in the metadata wire encoding.
    #[must_use]
    pub const fn as_byte(self) -> u8 {
        self as u8
    }

    /// Decode from the wire-format byte.
    pub const fn from_byte(b: u8) -> Result<Self, MetadataDecodeError> {
        Ok(match b {
            1 => Self::TaskException,
            2 => Self::WorkerDied,
            3 => Self::ActorDied,
            4 => Self::OwnerDied,
            5 => Self::TaskCancelled,
            6 => Self::ObjectLost,
            7 => Self::ObjectUnreconstructable,
            8 => Self::FetchTimeout,
            9 => Self::RuntimeEnvFailed,
            10 => Self::Unschedulable,
            11 => Self::OutOfMemory,
            other => return Err(MetadataDecodeError::UnknownErrorCategory(other)),
        })
    }
}

/// Typed metadata header that prefixes every stored value.
///
/// Wire encoding (1 to 4 bytes):
/// ```text
/// [discriminator: 1 byte]
///   1 (Pickle5):     [has_nested_refs flag: 1 byte] = 2 bytes total
///   2 (Raw):         empty                          = 1 byte total
///   3 (ActorHandle): empty                          = 1 byte total
///   16 (Error):      [category: 1 byte]
///                    [raw_code: 2 bytes LE]         = 4 bytes total
/// ```
///
/// The discriminator deliberately leaves a gap (4..15) for additional
/// success-tag variants without colliding with `Error`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Metadata {
    /// Successful: pickled Python object (cloudpickle/pickle5).
    Pickle5 {
        /// Whether the data buffer contains nested `ObjectRef`s that need
        /// refcount propagation.
        has_nested_refs: bool,
    },
    /// Successful: raw bytes (e.g. `rayd.put(b"...")`).
    Raw,
    /// Successful: pickled actor handle reducer payload.
    ActorHandle,
    /// Failure sentinel.
    Error {
        /// Coarse user-facing category.
        category: ErrorCategory,
        /// Granular code; usually mirrors a Ray-style `ErrorType` int for
        /// observability without bloating the surface API.
        raw_code: u16,
    },
}

const DISCRIMINATOR_PICKLE5: u8 = 1;
const DISCRIMINATOR_RAW: u8 = 2;
const DISCRIMINATOR_ACTOR_HANDLE: u8 = 3;
const DISCRIMINATOR_ERROR: u8 = 16;

/// Errors decoding a metadata wire payload.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum MetadataDecodeError {
    /// The buffer was empty.
    #[error("metadata buffer is empty")]
    Empty,
    /// Discriminator byte didn't match a known variant.
    #[error("unknown metadata discriminator: {0}")]
    UnknownDiscriminator(u8),
    /// Buffer ended before the variant's expected payload was complete.
    #[error("metadata buffer truncated: needed {expected} bytes, got {got}")]
    Truncated {
        /// Total byte count the variant requires.
        expected: usize,
        /// Bytes actually present.
        got: usize,
    },
    /// `ErrorCategory` discriminator was out of range.
    #[error("unknown error category byte: {0}")]
    UnknownErrorCategory(u8),
}

impl Metadata {
    /// Wire-encode the metadata header into a fresh `Vec<u8>`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            Self::Pickle5 { has_nested_refs } => {
                vec![DISCRIMINATOR_PICKLE5, u8::from(has_nested_refs)]
            }
            Self::Raw => vec![DISCRIMINATOR_RAW],
            Self::ActorHandle => vec![DISCRIMINATOR_ACTOR_HANDLE],
            Self::Error { category, raw_code } => {
                let bytes = raw_code.to_le_bytes();
                vec![DISCRIMINATOR_ERROR, category.as_byte(), bytes[0], bytes[1]]
            }
        }
    }

    /// Decode a wire-encoded metadata header.
    pub fn decode(buf: &[u8]) -> Result<Self, MetadataDecodeError> {
        let &disc = buf.first().ok_or(MetadataDecodeError::Empty)?;
        match disc {
            DISCRIMINATOR_PICKLE5 => {
                if buf.len() < 2 {
                    return Err(MetadataDecodeError::Truncated {
                        expected: 2,
                        got: buf.len(),
                    });
                }
                Ok(Self::Pickle5 {
                    has_nested_refs: buf[1] != 0,
                })
            }
            DISCRIMINATOR_RAW => Ok(Self::Raw),
            DISCRIMINATOR_ACTOR_HANDLE => Ok(Self::ActorHandle),
            DISCRIMINATOR_ERROR => {
                if buf.len() < 4 {
                    return Err(MetadataDecodeError::Truncated {
                        expected: 4,
                        got: buf.len(),
                    });
                }
                let category = ErrorCategory::from_byte(buf[1])?;
                let raw_code = u16::from_le_bytes([buf[2], buf[3]]);
                Ok(Self::Error { category, raw_code })
            }
            other => Err(MetadataDecodeError::UnknownDiscriminator(other)),
        }
    }

    /// Whether this metadata is an `Error` variant.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Project to `ErrorCategory` if this is an `Error` variant.
    #[must_use]
    pub const fn error_category(&self) -> Option<ErrorCategory> {
        if let Self::Error { category, .. } = self {
            Some(*category)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pickle5_round_trip() {
        for has_refs in [false, true] {
            let m = Metadata::Pickle5 {
                has_nested_refs: has_refs,
            };
            let bytes = m.encode();
            assert_eq!(bytes.len(), 2);
            assert_eq!(Metadata::decode(&bytes).expect("decode"), m);
        }
    }

    #[test]
    fn raw_round_trip() {
        let bytes = Metadata::Raw.encode();
        assert_eq!(bytes, vec![2]);
        assert_eq!(Metadata::decode(&bytes).expect("decode"), Metadata::Raw);
    }

    #[test]
    fn actor_handle_round_trip() {
        let bytes = Metadata::ActorHandle.encode();
        assert_eq!(bytes, vec![3]);
        assert_eq!(
            Metadata::decode(&bytes).expect("decode"),
            Metadata::ActorHandle
        );
    }

    #[test]
    fn error_round_trip() {
        let m = Metadata::Error {
            category: ErrorCategory::TaskException,
            raw_code: 0x1234,
        };
        let bytes = m.encode();
        assert_eq!(bytes.len(), 4);
        assert_eq!(Metadata::decode(&bytes).expect("decode"), m);
    }

    #[test]
    fn decode_empty_fails() {
        assert_eq!(Metadata::decode(&[]), Err(MetadataDecodeError::Empty));
    }

    #[test]
    fn decode_unknown_discriminator() {
        assert_eq!(
            Metadata::decode(&[99]),
            Err(MetadataDecodeError::UnknownDiscriminator(99))
        );
    }

    #[test]
    fn decode_truncated_error() {
        assert_eq!(
            Metadata::decode(&[16, 1]),
            Err(MetadataDecodeError::Truncated {
                expected: 4,
                got: 2
            })
        );
    }

    #[test]
    fn ref_state_helpers() {
        assert!(RefState::ReadyLocal.is_ready());
        assert!(RefState::Failed.is_ready());
        assert!(RefState::Failed.is_failed());
        assert!(!RefState::Pending.is_ready());
    }
}
