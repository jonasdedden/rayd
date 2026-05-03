//! `RayObject`: the in-memory unit a worker stores for every `ObjectRef`.
//!
//! Keeps `data` and `metadata` as separate fields so callers can read the
//! metadata (state, error category) without touching the data buffer. This
//! is the foundation of the cheap state-inspection API documented in
//! `docs/design/05-state-and-error-api.md`.

use bytes::Bytes;

use crate::metadata::Metadata;
use crate::object_ref::ObjectRef;

/// A stored value: small typed metadata header, opaque data buffer, and the
/// list of `ObjectRef`s contained in `data` (for refcount propagation).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RayObject {
    /// Type tag / error sentinel.
    pub metadata: Metadata,
    /// Opaque payload (pickled Python object, raw bytes, or an encoded
    /// `ErrorPayload` for failed values).
    pub data: Bytes,
    /// `ObjectRef`s embedded in `data`. Empty in Phase 1 (we don't yet
    /// scan pickled payloads for nested refs); reserved for the
    /// reference-counting work in Phase 4.
    pub nested_refs: Vec<ObjectRef>,
}

impl RayObject {
    /// Construct a new `RayObject` with no nested refs.
    #[must_use]
    pub fn new(metadata: Metadata, data: Bytes) -> Self {
        Self {
            metadata,
            data,
            nested_refs: Vec::new(),
        }
    }

    /// Whether this object's metadata is an `Error` variant.
    #[must_use]
    pub const fn is_error(&self) -> bool {
        self.metadata.is_error()
    }

    /// Total bytes occupied by metadata + data (used for budget accounting).
    #[must_use]
    pub fn size_bytes(&self) -> usize {
        // `Metadata::encode` allocates; we know the wire size is 1–4 bytes
        // and avoid the allocation by computing it directly.
        let metadata_size = match self.metadata {
            Metadata::Pickle5 { .. } => 2,
            Metadata::Raw | Metadata::ActorHandle => 1,
            Metadata::Error { .. } => 4,
        };
        metadata_size + self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{ErrorCategory, Metadata};

    #[test]
    fn raw_round_trip() {
        let obj = RayObject::new(Metadata::Raw, Bytes::from_static(b"hello"));
        assert!(!obj.is_error());
        assert_eq!(obj.size_bytes(), 1 + 5);
    }

    #[test]
    fn error_object() {
        let obj = RayObject::new(
            Metadata::Error {
                category: ErrorCategory::TaskException,
                raw_code: 3,
            },
            Bytes::from_static(b"err-payload"),
        );
        assert!(obj.is_error());
        assert_eq!(
            obj.metadata.error_category(),
            Some(ErrorCategory::TaskException)
        );
    }
}
