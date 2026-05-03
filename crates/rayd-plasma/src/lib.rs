//! `rayd-plasma`: shared-memory object store.
//!
//! Phase 2 implementation. The server allocates an mmap-backed `memfd` once;
//! every connecting client receives the fd via `SCM_RIGHTS` and mmaps it
//! locally. After that one handoff, all reads and writes are direct memory
//! accesses with no further IPC.
//!
//! The protocol is a length-prefixed `bincode`-encoded request/response.
//! The fd handoff happens on `Create` and `Get` replies.

// `mmap`-based shared memory inherently requires `unsafe`. The unsafe blocks
// are localized to `arena.rs` (`MmapMut::map_mut`) and `client.rs`
// (`MmapMut::map_*` for client-side mappings). Each one is documented inline.
#![allow(unsafe_code)]

pub mod arena;
pub mod client;
pub mod codec;
pub mod error;
pub mod metrics;
mod scm;
pub mod server;
pub mod wire;

pub use arena::Arena;
pub use client::{PlasmaClient, ReadHandle, WriteHandle};
pub use error::{PlasmaError, ServerErrorKind};
pub use metrics::{Metrics, MetricsServerHandle, MetricsStartError};
pub use server::{PlasmaServer, ServerHandle};
pub use wire::AddressBlob;

/// The default arena capacity used by `PlasmaServer::start_default`. 128 MiB.
pub const DEFAULT_ARENA_BYTES: u64 = 128 * 1024 * 1024;

/// 16-byte alignment for object payloads inside the arena.
pub const OBJECT_ALIGN: u64 = 16;
