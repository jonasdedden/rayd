//! Error types for the plasma server and client.

use std::io;

use thiserror::Error;

/// Server-side error kinds reported back to the client over the wire.
#[derive(bincode::Encode, bincode::Decode, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerErrorKind {
    /// Object not present in the store.
    NotFound,
    /// An object with this id already exists (and is unsealed).
    AlreadyExists,
    /// The arena is out of space for a new allocation.
    OutOfMemory,
    /// Operation requires the object to be sealed.
    NotSealed,
    /// Operation cannot be performed on a sealed object.
    AlreadySealed,
    /// Generic internal failure (a server bug).
    Internal,
}

/// Errors observable by callers of `PlasmaClient` and `PlasmaServer`.
#[derive(Debug, Error)]
pub enum PlasmaError {
    /// I/O error talking to the UDS or with mmap operations.
    #[error("plasma I/O: {0}")]
    Io(#[from] io::Error),
    /// `bincode` encoding/decoding error.
    #[error("plasma codec: {0}")]
    Encode(#[from] bincode::error::EncodeError),
    /// `bincode` decoding error (separate variant for thiserror).
    #[error("plasma codec: {0}")]
    Decode(#[from] bincode::error::DecodeError),
    /// `nix` syscall error.
    #[error("plasma syscall: {0}")]
    Nix(#[from] nix::errno::Errno),
    /// The server returned an error.
    #[error("plasma server error: {kind:?}: {message}")]
    Server {
        /// The categorical reason.
        kind: ServerErrorKind,
        /// A human-readable message.
        message: String,
    },
    /// Server replied with the wrong kind of response.
    #[error("plasma protocol mismatch: {0}")]
    Protocol(String),
    /// `mmap` of an arena failed.
    #[error("plasma mmap: {0}")]
    Mmap(String),
    /// The server replied OK but didn't include an expected file descriptor.
    #[error("plasma: missing file descriptor in server reply")]
    MissingFd,
    /// Metrics endpoint setup failed.
    #[error("plasma metrics: {0}")]
    Metrics(#[from] crate::metrics::MetricsStartError),
}
