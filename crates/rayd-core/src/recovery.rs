//! Pluggable recovery hook for objects that have left local plasma.
//!
//! `CoreWorker::resolve_entry` consults a registered `ObjectRecoverer`
//! when plasma reports `NotFound` for an object the local store still
//! believes lives there. The recoverer returns the bytes (e.g. from a
//! spill backend, a peer node, or a persistent cache), the worker
//! reseals into plasma, and the resolve continues transparently.
//!
//! The trait lives in `rayd-core` to avoid a cycle with `rayd-raylet`,
//! where the canonical impl (`LocalObjectManager`) lives.

use bytes::Bytes;
use thiserror::Error;

use crate::id::ObjectId;

/// Bytes returned from a successful recovery. Mirrors what plasma
/// stores: an encoded metadata header plus the payload.
#[derive(Debug, Clone)]
pub struct RecoveredObject {
    /// Encoded metadata bytes (the same form plasma round-trips).
    pub metadata: Bytes,
    /// Object payload.
    pub data: Bytes,
}

/// Errors a recoverer can surface. The "no entry" case is encoded as
/// `Ok(None)` rather than an error variant so callers can pattern-
/// match cleanly.
#[derive(Debug, Error)]
pub enum RecoveryError {
    /// I/O or decoding failure underneath the recoverer (corrupt
    /// spill file, transient disk error, etc.). Caller should
    /// surface this rather than retry.
    #[error("recoverer: {0}")]
    Other(String),
}

/// Pluggable recovery hook. `LocalObjectManager` implements it from
/// the rayd-raylet crate; tests can substitute their own impl.
pub trait ObjectRecoverer: Send + Sync + std::fmt::Debug {
    /// Try to recover bytes for `id`. `Ok(None)` means the recoverer
    /// has no record of this object — the caller should treat the
    /// situation as "not present" and fall through. `Err(_)` is a
    /// terminal failure (corrupt backing store, etc.).
    fn recover(&self, id: ObjectId) -> Result<Option<RecoveredObject>, RecoveryError>;

    /// Stash bytes for later recovery. Used by the spill-on-pressure
    /// path to hand cold objects to the backing store before the
    /// caller deletes them from plasma. Idempotent on the same id.
    fn store(&self, id: ObjectId, metadata: Bytes, data: Bytes) -> Result<(), RecoveryError>;
}
