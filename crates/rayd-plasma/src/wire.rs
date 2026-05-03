//! Wire-format messages exchanged between `PlasmaClient` and `PlasmaServer`.
//!
//! Encoded with `bincode` v2 derive macros. Frames go over a UDS as a
//! 4-byte little-endian length prefix followed by the encoded body. On
//! `Create` and `Get` replies, the server attaches the arena memfd via
//! `SCM_RIGHTS` ancillary data; see `crate::scm`.

use bincode::{Decode, Encode};

use crate::error::ServerErrorKind;

/// Compact wire-form of `rayd_core::Address` (host/port/`worker_id`).
#[derive(Encode, Decode, Debug, Clone, PartialEq, Eq)]
pub struct AddressBlob {
    /// IP literal or hostname.
    pub host: String,
    /// TCP port for the worker's gRPC server (zero is a placeholder).
    pub port: u16,
    /// 16-byte worker id.
    pub worker_id: [u8; 16],
}

impl AddressBlob {
    /// Build a fresh `AddressBlob` from raw fields.
    pub fn new(host: String, port: u16, worker_id: [u8; 16]) -> Self {
        Self {
            host,
            port,
            worker_id,
        }
    }

    /// All-zero placeholder address.
    #[must_use]
    pub fn nil() -> Self {
        Self {
            host: String::new(),
            port: 0,
            worker_id: [0u8; 16],
        }
    }
}

/// Request from a client to the plasma server.
#[derive(Encode, Decode, Debug, Clone)]
pub enum Request {
    /// Allocate space for a new object; returns offsets and the arena fd.
    Create(CreateRequest),
    /// Mark a previously-created object immutable.
    Seal {
        /// The 28-byte id of the object to seal.
        object_id: [u8; 28],
    },
    /// Look up an object's location; returns offsets and (re-)sends the fd.
    Get {
        /// The id to look up.
        object_id: [u8; 28],
    },
    /// Cheap: report whether an object is present, plus its metadata header.
    Contains {
        /// The id to check.
        object_id: [u8; 28],
    },
    /// Decrement the per-client reference count.
    Release {
        /// The id to release.
        object_id: [u8; 28],
    },
    /// Remove the object outright (bypasses refcount; for explicit free).
    Delete {
        /// The id to remove.
        object_id: [u8; 28],
    },
    /// Diagnostic: list every present object id.
    List,
    /// Politely close the connection.
    Disconnect,
}

/// Body of a `Create` request.
#[derive(Encode, Decode, Debug, Clone)]
pub struct CreateRequest {
    /// 28-byte object id derived from `(TaskId, return_index)`.
    pub object_id: [u8; 28],
    /// Number of bytes the metadata header will occupy.
    pub metadata_size: u64,
    /// Number of bytes the data payload will occupy.
    pub data_size: u64,
    /// Owner of the object (the worker that submitted the producing task).
    pub owner: AddressBlob,
}

/// Server response. Successful `Created` and `Got` replies are accompanied
/// by the arena memfd via `SCM_RIGHTS`.
#[derive(Encode, Decode, Debug, Clone)]
pub enum Response {
    /// Plain success.
    Ok,
    /// Reply to `Create`. The memfd is attached.
    Created(SlotInfo),
    /// Reply to `Get`. The memfd is attached.
    Got(SlotInfo),
    /// Reply to `Contains`.
    Contains {
        /// Whether the object is present (sealed or not).
        present: bool,
        /// Encoded `Metadata` header — present iff `present` is true and the
        /// object has been sealed.
        metadata: Option<Vec<u8>>,
        /// Whether the object is sealed (only meaningful if `present`).
        sealed: bool,
    },
    /// Reply to `List`.
    Listed {
        /// All ids the server currently knows about.
        object_ids: Vec<[u8; 28]>,
    },
    /// Server-side error.
    Error {
        /// Categorical reason.
        kind: ServerErrorKind,
        /// Human-readable message.
        message: String,
    },
}

/// Location of an object inside an arena. Both `Created` and `Got` carry
/// this; the offsets are byte-aligned within the arena's mmap.
#[derive(Encode, Decode, Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotInfo {
    /// Identifier of the arena (matches the arena's id, not its memfd).
    pub arena_id: u64,
    /// Total bytes mapped by the arena.
    pub arena_capacity: u64,
    /// Byte offset within the arena where the metadata buffer begins.
    pub metadata_offset: u64,
    /// Number of bytes reserved for metadata.
    pub metadata_size: u64,
    /// Byte offset where the data buffer begins.
    pub data_offset: u64,
    /// Number of bytes reserved for data.
    pub data_size: u64,
    /// Whether the object is sealed (immutable).
    pub sealed: bool,
}
