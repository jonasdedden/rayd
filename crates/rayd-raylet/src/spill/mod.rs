//! Object-spill backend abstraction (Phase 6.1).
//!
//! When the local plasma store fills past a threshold, the raylet
//! spills cold objects to a `SpillBackend`. The default implementation
//! writes to a directory on local disk (`LocalFsBackend`); future
//! work may add an S3-backed variant behind a feature flag.
//!
//! A `SpillUrl` identifies a spilled object so the raylet can locate
//! and restore it later. The url is opaque to the rest of rayd —
//! callers just round-trip it through their bookkeeping.

use bytes::Bytes;
use thiserror::Error;

mod local_fs;

pub use local_fs::LocalFsBackend;

/// 28-byte rayd object id, mirrored here so the spill module doesn't
/// depend on `rayd-core`'s `ObjectId` type. Callers convert at the
/// boundary.
pub type ObjectIdBytes = [u8; 28];

/// Opaque locator returned by `SpillBackend::spill`. The string is
/// backend-specific (e.g. `file:///path/to/dir/<hex>.spill` for
/// `LocalFsBackend`). Round-trip it as an opaque token.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpillUrl(pub String);

impl SpillUrl {
    /// Borrow the underlying string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SpillUrl {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Bytes returned from a spill restore: metadata + data, mirroring
/// what plasma stores.
#[derive(Debug, Clone)]
pub struct RestoredObject {
    /// Encoded `Metadata` bytes (same bytes plasma stores as the
    /// metadata header of the sealed object).
    pub metadata: Bytes,
    /// The object's payload.
    pub data: Bytes,
}

/// Errors observable across any spill backend.
#[derive(Debug, Error)]
pub enum SpillError {
    /// The url didn't refer to anything the backend knows about.
    /// Surfaced when restoring or removing a stale entry.
    #[error("spill url not found: {url}")]
    NotFound {
        /// The url that wasn't resolvable.
        url: String,
    },
    /// Underlying I/O failure (disk full, permission denied, …).
    #[error("spill I/O: {0}")]
    Io(#[from] std::io::Error),
    /// Backend rejected the request because the on-disk format was
    /// corrupt (e.g. truncated header). The raylet should treat this
    /// like `NotFound` from a recovery standpoint — the spilled
    /// bytes are unrecoverable.
    #[error("spill corruption at {url}: {reason}")]
    Corrupt {
        /// The url whose contents couldn't be parsed.
        url: String,
        /// Human-readable detail.
        reason: String,
    },
}

/// Pluggable spill backend.
///
/// Implementations are sync because the workspace's spill caller
/// (Phase 6.2's `LocalObjectManager`) wraps backend calls in
/// `tokio::task::spawn_blocking`. Keeping the trait sync simplifies
/// per-call testing and avoids dragging `async_trait` into the
/// dependency graph for an I/O-bound code path that never benefits
/// from inline async polling.
pub trait SpillBackend: Send + Sync + std::fmt::Debug {
    /// Spill an object's `(metadata, data)` to the backend. Returns a
    /// url the backend understands. Idempotent on the same `object_id`
    /// — re-spilling overwrites.
    fn spill(
        &self,
        object_id: ObjectIdBytes,
        metadata: Bytes,
        data: Bytes,
    ) -> Result<SpillUrl, SpillError>;

    /// Restore an object's `(metadata, data)` from the backend. Returns
    /// `NotFound` if `url` is unknown.
    fn restore(&self, url: &SpillUrl) -> Result<RestoredObject, SpillError>;

    /// Remove a spilled object. Idempotent: removing an already-removed
    /// url is a successful no-op (the goal state is "not present").
    fn remove(&self, url: &SpillUrl) -> Result<(), SpillError>;
}
