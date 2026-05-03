//! Strongly-typed identifiers used throughout the runtime.
//!
//! Each identifier is a fixed-width byte array wrapped in a newtype so the
//! compiler refuses to mix them up. Sizes match the design doc:
//! `JobId` = 16 bytes, `TaskId` = 24 bytes (16 job + 8 counter),
//! `ObjectId` = 28 bytes (24 task + 4 return-index), `WorkerId` = `ActorId` = 16 bytes.

use core::fmt;

use rand::RngCore;
use thiserror::Error;

/// Returned when an identifier's hex representation has the wrong length or
/// contains non-hex characters.
#[derive(Debug, Error, PartialEq)]
pub enum IdParseError {
    /// The decoded byte length did not match the expected size.
    #[error("expected {expected} bytes, got {got}")]
    WrongLength {
        /// Expected number of bytes for this identifier kind.
        expected: usize,
        /// Number of bytes actually decoded.
        got: usize,
    },
    /// Hex decoding failed.
    #[error(transparent)]
    Hex(#[from] hex::FromHexError),
}

macro_rules! define_id {
    ($name:ident, $size:expr, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name([u8; $size]);

        impl $name {
            #[doc = concat!("The byte length of a `", stringify!($name), "`.")]
            pub const SIZE: usize = $size;

            /// Construct an id from a fixed-width byte array.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; $size]) -> Self {
                Self(bytes)
            }

            /// View the id's bytes.
            #[must_use]
            pub const fn as_bytes(&self) -> &[u8; $size] {
                &self.0
            }

            /// Encode the id as a lowercase hex string.
            #[must_use]
            pub fn hex(&self) -> String {
                hex::encode(self.0)
            }

            /// Decode a hex string into an id of this kind.
            pub fn from_hex(s: &str) -> Result<Self, IdParseError> {
                let bytes = hex::decode(s)?;
                if bytes.len() != $size {
                    return Err(IdParseError::WrongLength {
                        expected: $size,
                        got: bytes.len(),
                    });
                }
                let mut arr = [0u8; $size];
                arr.copy_from_slice(&bytes);
                Ok(Self(arr))
            }

            /// Generate a fresh random id using the OS RNG.
            #[must_use]
            pub fn random() -> Self {
                let mut bytes = [0u8; $size];
                rand::rng().fill_bytes(&mut bytes);
                Self(bytes)
            }

            /// The all-zero ("nil") id; used as a sentinel for "unset".
            #[must_use]
            pub const fn nil() -> Self {
                Self([0u8; $size])
            }

            /// Whether this id equals the all-zero sentinel.
            #[must_use]
            pub fn is_nil(&self) -> bool {
                self.0 == [0u8; $size]
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.hex())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.hex())
            }
        }
    };
}

define_id!(
    JobId,
    16,
    "Identifier for a Ray-style job (one driver = one job)."
);
define_id!(
    TaskId,
    24,
    "Identifier for a single task submission attempt."
);
define_id!(
    WorkerId,
    16,
    "Identifier for a worker process within the cluster."
);
define_id!(ActorId, 16, "Identifier for a stateful actor.");

/// Identifier for an object stored in the distributed object store.
///
/// Unlike the other ids, an `ObjectId` is *deterministically* derived from
/// the parent `TaskId` plus a 4-byte return index. This lets the submitter
/// hand callers ids before the task has run.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId([u8; 28]);

impl ObjectId {
    /// The byte length of an `ObjectId`.
    pub const SIZE: usize = 28;

    /// Construct an `ObjectId` from raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 28]) -> Self {
        Self(bytes)
    }

    /// Build an `ObjectId` deterministically from a parent task id and a
    /// 0-based return index.
    #[must_use]
    pub fn for_return(task: &TaskId, return_index: u32) -> Self {
        let mut bytes = [0u8; 28];
        bytes[..24].copy_from_slice(task.as_bytes());
        bytes[24..].copy_from_slice(&return_index.to_le_bytes());
        Self(bytes)
    }

    /// View the id's bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 28] {
        &self.0
    }

    /// Recover the parent task id from this `ObjectId`.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        let mut buf = [0u8; 24];
        buf.copy_from_slice(&self.0[..24]);
        TaskId::from_bytes(buf)
    }

    /// Recover the parent job id (the first 16 bytes of the task id).
    #[must_use]
    pub fn job_id(&self) -> JobId {
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&self.0[..16]);
        JobId::from_bytes(buf)
    }

    /// Recover the return index encoded in the last 4 bytes.
    #[must_use]
    pub fn return_index(&self) -> u32 {
        let mut buf = [0u8; 4];
        buf.copy_from_slice(&self.0[24..]);
        u32::from_le_bytes(buf)
    }

    /// Encode the id as a lowercase hex string.
    #[must_use]
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Decode a hex string into an `ObjectId`.
    pub fn from_hex(s: &str) -> Result<Self, IdParseError> {
        let bytes = hex::decode(s)?;
        if bytes.len() != Self::SIZE {
            return Err(IdParseError::WrongLength {
                expected: Self::SIZE,
                got: bytes.len(),
            });
        }
        let mut arr = [0u8; 28];
        arr.copy_from_slice(&bytes);
        Ok(Self(arr))
    }

    /// The all-zero sentinel.
    #[must_use]
    pub const fn nil() -> Self {
        Self([0u8; 28])
    }

    /// Whether this id is the nil sentinel.
    #[must_use]
    pub fn is_nil(&self) -> bool {
        self.0 == [0u8; 28]
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.hex())
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn job_id_hex_round_trip() {
        let id = JobId::random();
        let parsed = JobId::from_hex(&id.hex()).expect("valid hex");
        assert_eq!(id, parsed);
    }

    #[test]
    fn object_id_from_task_round_trip() {
        let task = TaskId::random();
        let object = ObjectId::for_return(&task, 7);
        assert_eq!(object.task_id(), task);
        assert_eq!(object.return_index(), 7);
        assert_eq!(object.job_id().as_bytes(), &task.as_bytes()[..16]);
    }

    #[test]
    fn object_id_hex_length_is_56() {
        let id = ObjectId::for_return(&TaskId::random(), 0);
        assert_eq!(id.hex().len(), 56);
    }

    #[test]
    fn nil_ids_round_trip() {
        assert!(JobId::nil().is_nil());
        assert!(TaskId::nil().is_nil());
        assert!(ObjectId::nil().is_nil());
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        assert!(matches!(
            JobId::from_hex("ff"),
            Err(IdParseError::WrongLength {
                expected: 16,
                got: 1
            })
        ));
    }
}
