//! `PlasmaClient`: connects to a `PlasmaServer` over UDS, mmaps the arena
//! once, and exposes safe slice handles for reads and writes.
//!
//! Workflow:
//! - `connect(path)` opens the socket.
//! - `create(id, metadata_size, data_size, owner)` issues a Create RPC and
//!   returns a `WriteHandle` whose `metadata` and `data` slices live inside
//!   the mmap region. Caller fills them, then calls `seal`.
//! - `get(id)` issues a Get RPC and returns a `ReadHandle` over the same
//!   region. Slices are valid as long as the handle is.

use std::collections::HashMap;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Arc;

use memmap2::{MmapMut, MmapOptions};
use tracing::trace;

use crate::codec::{decode, encode};
use crate::error::PlasmaError;
use crate::scm::{recv_frame, send_frame};
use crate::wire::{CreateRequest, Request, Response, SlotInfo};

/// Per-client cache of mmap'd arenas, keyed by arena id.
#[derive(Default)]
struct ArenaCache {
    arenas: HashMap<u64, Arc<MmapMut>>,
}

/// Connection to a `PlasmaServer`. Single-threaded: every method takes
/// `&mut self` so the underlying socket isn't shared. To use from multiple
/// threads, wrap an `Arc<Mutex<PlasmaClient>>`.
pub struct PlasmaClient {
    stream: UnixStream,
    cache: ArenaCache,
}

impl std::fmt::Debug for PlasmaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlasmaClient")
            .field("arenas_cached", &self.cache.arenas.len())
            .finish()
    }
}

impl PlasmaClient {
    /// Open a connection to the server at `path`.
    pub fn connect(path: impl AsRef<Path>) -> Result<Self, PlasmaError> {
        let stream = UnixStream::connect(path.as_ref())?;
        Ok(Self {
            stream,
            cache: ArenaCache::default(),
        })
    }

    /// Issue a `Create` RPC and return a `WriteHandle` over the freshly
    /// allocated metadata + data buffers.
    pub fn create(
        &mut self,
        object_id: [u8; 28],
        metadata: &[u8],
        data: &[u8],
        owner: crate::wire::AddressBlob,
    ) -> Result<WriteHandle, PlasmaError> {
        let req = Request::Create(CreateRequest {
            object_id,
            metadata_size: metadata.len() as u64,
            data_size: data.len() as u64,
            owner,
        });
        let body = encode(&req)?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, fds) = recv_frame(&self.stream)?;
        let resp: Response = decode(&resp_body)?;

        let info = match resp {
            Response::Created(info) => info,
            Response::Error { kind, message } => return Err(PlasmaError::Server { kind, message }),
            other => {
                return Err(PlasmaError::Protocol(format!(
                    "unexpected reply to Create: {other:?}"
                )));
            }
        };

        let mmap = self.ensure_arena_mapped(&info, fds)?;
        let mut handle = WriteHandle {
            object_id,
            info,
            mmap,
        };
        handle.write_metadata(metadata);
        handle.write_data(data);
        Ok(handle)
    }

    /// Helper: do `create + seal` in one call. The most common path for
    /// "I have these bytes, store them and finalize" use cases.
    pub fn create_and_seal(
        &mut self,
        object_id: [u8; 28],
        metadata: &[u8],
        data: &[u8],
        owner: crate::wire::AddressBlob,
    ) -> Result<(), PlasmaError> {
        let _handle = self.create(object_id, metadata, data, owner)?;
        self.seal(object_id)
    }

    /// Mark an object immutable. After this, `get` will return it.
    pub fn seal(&mut self, object_id: [u8; 28]) -> Result<(), PlasmaError> {
        let body = encode(&Request::Seal { object_id })?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, _fds) = recv_frame(&self.stream)?;
        match decode::<Response>(&resp_body)? {
            Response::Ok => Ok(()),
            Response::Error { kind, message } => Err(PlasmaError::Server { kind, message }),
            other => Err(PlasmaError::Protocol(format!(
                "unexpected reply to Seal: {other:?}"
            ))),
        }
    }

    /// Fetch an object's location and return a read-only handle into the
    /// mmap'd arena. The handle's slices are valid until it is dropped.
    pub fn get(&mut self, object_id: [u8; 28]) -> Result<ReadHandle, PlasmaError> {
        let body = encode(&Request::Get { object_id })?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, fds) = recv_frame(&self.stream)?;
        let resp: Response = decode(&resp_body)?;

        let info = match resp {
            Response::Got(info) => info,
            Response::Error { kind, message } => return Err(PlasmaError::Server { kind, message }),
            other => {
                return Err(PlasmaError::Protocol(format!(
                    "unexpected reply to Get: {other:?}"
                )));
            }
        };

        let mmap = self.ensure_arena_mapped(&info, fds)?;
        Ok(ReadHandle {
            object_id,
            info,
            mmap,
        })
    }

    /// Cheap probe: returns `(present, sealed, metadata_bytes)`. The
    /// metadata copy travels over the socket, not via mmap, because it's
    /// small (≤ a few bytes) and reading it via mmap would still require an
    /// arena fd handoff.
    pub fn contains(&mut self, object_id: [u8; 28]) -> Result<ContainsResult, PlasmaError> {
        let body = encode(&Request::Contains { object_id })?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, _fds) = recv_frame(&self.stream)?;
        match decode::<Response>(&resp_body)? {
            Response::Contains {
                present,
                sealed,
                metadata,
            } => Ok(ContainsResult {
                present,
                sealed,
                metadata,
            }),
            Response::Error { kind, message } => Err(PlasmaError::Server { kind, message }),
            other => Err(PlasmaError::Protocol(format!(
                "unexpected reply to Contains: {other:?}"
            ))),
        }
    }

    /// Release a reference. (Phase 2: also accepts `Delete` semantics; see
    /// note in `delete`.)
    pub fn release(&mut self, object_id: [u8; 28]) -> Result<(), PlasmaError> {
        self.simple_command(Request::Release { object_id })
    }

    /// Delete an object outright. Phase 2: this is the only way to remove
    /// objects, since per-client refcounting hasn't landed yet.
    pub fn delete(&mut self, object_id: [u8; 28]) -> Result<(), PlasmaError> {
        self.simple_command(Request::Delete { object_id })
    }

    /// Diagnostic: list all object ids the server knows about.
    pub fn list(&mut self) -> Result<Vec<[u8; 28]>, PlasmaError> {
        let body = encode(&Request::List)?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, _fds) = recv_frame(&self.stream)?;
        match decode::<Response>(&resp_body)? {
            Response::Listed { object_ids } => Ok(object_ids),
            Response::Error { kind, message } => Err(PlasmaError::Server { kind, message }),
            other => Err(PlasmaError::Protocol(format!(
                "unexpected reply to List: {other:?}"
            ))),
        }
    }

    fn simple_command(&self, req: Request) -> Result<(), PlasmaError> {
        let body = encode(&req)?;
        send_frame(&self.stream, &body, None)?;
        let (resp_body, _fds) = recv_frame(&self.stream)?;
        match decode::<Response>(&resp_body)? {
            Response::Ok => Ok(()),
            Response::Error { kind, message } => Err(PlasmaError::Server { kind, message }),
            other => Err(PlasmaError::Protocol(format!(
                "unexpected reply: {other:?}"
            ))),
        }
    }

    fn ensure_arena_mapped(
        &mut self,
        info: &SlotInfo,
        fds: Vec<OwnedFd>,
    ) -> Result<Arc<MmapMut>, PlasmaError> {
        if let Some(mmap) = self.cache.arenas.get(&info.arena_id) {
            return Ok(Arc::clone(mmap));
        }
        let fd = fds.into_iter().next().ok_or(PlasmaError::MissingFd)?;
        // SAFETY: the kernel just gave us this fd via SCM_RIGHTS and it
        // points to a memfd of the size the server told us. Our mmap stays
        // valid as long as we hold either `fd` or any aliasing mapping;
        // we keep the Arc in `cache` so the mapping outlives all handles.
        let mmap = unsafe {
            MmapOptions::new()
                .len(
                    usize::try_from(info.arena_capacity)
                        .map_err(|_| PlasmaError::Mmap("arena capacity too large".into()))?,
                )
                .map_mut(&fd.as_fd())
        }
        .map_err(|e| PlasmaError::Mmap(e.to_string()))?;
        let mmap = Arc::new(mmap);
        // Drop the OwnedFd: the mmap retains its own kernel reference.
        drop(fd);
        self.cache.arenas.insert(info.arena_id, Arc::clone(&mmap));
        trace!(
            arena_id = info.arena_id,
            capacity = info.arena_capacity,
            "mmapped arena"
        );
        Ok(mmap)
    }

    /// Send a polite disconnect; the server's connection thread exits cleanly.
    pub fn disconnect(&mut self) -> Result<(), PlasmaError> {
        let body = encode(&Request::Disconnect)?;
        send_frame(&self.stream, &body, None)
    }
}

/// Reply payload from `contains`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainsResult {
    /// Whether the object is present.
    pub present: bool,
    /// Whether the object is sealed.
    pub sealed: bool,
    /// Encoded metadata header bytes (only populated if present + sealed).
    pub metadata: Option<Vec<u8>>,
}

/// Mutable handle returned by `create`. Drop without `seal` to abandon.
pub struct WriteHandle {
    object_id: [u8; 28],
    info: SlotInfo,
    mmap: Arc<MmapMut>,
}

impl std::fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteHandle")
            .field("object_id", &hex::encode(self.object_id))
            .field("info", &self.info)
            .finish()
    }
}

impl WriteHandle {
    /// The 28-byte id this handle writes.
    #[must_use]
    pub fn object_id(&self) -> [u8; 28] {
        self.object_id
    }

    /// Allocation info reported by the server.
    #[must_use]
    pub fn info(&self) -> &SlotInfo {
        &self.info
    }

    /// Copy `bytes` into the metadata slot.
    pub fn write_metadata(&mut self, bytes: &[u8]) {
        let dst = self.metadata_mut_slice();
        let copy_len = bytes.len().min(dst.len());
        dst[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    /// Copy `bytes` into the data slot.
    pub fn write_data(&mut self, bytes: &[u8]) {
        let dst = self.data_mut_slice();
        let copy_len = bytes.len().min(dst.len());
        dst[..copy_len].copy_from_slice(&bytes[..copy_len]);
    }

    /// Direct mutable access to the metadata slot (for advanced fill
    /// strategies, e.g. pickle out-of-band buffers landing here).
    pub fn metadata_mut_slice(&mut self) -> &mut [u8] {
        let off = self.info.metadata_offset as usize;
        let end = off + self.info.metadata_size as usize;
        // SAFETY: We hold the only `WriteHandle` for this slot; the server
        // promised `metadata_offset..metadata_offset+metadata_size` is
        // exclusively ours until we Seal. The `Arc<MmapMut>` is shared but
        // we project a disjoint slice via raw pointer arithmetic.
        unsafe { mmap_slice_mut(&self.mmap, off, end - off) }
    }

    /// Direct mutable access to the data slot.
    pub fn data_mut_slice(&mut self) -> &mut [u8] {
        let off = self.info.data_offset as usize;
        let end = off + self.info.data_size as usize;
        // SAFETY: see `metadata_mut_slice`.
        unsafe { mmap_slice_mut(&self.mmap, off, end - off) }
    }
}

/// Read-only handle returned by `get`.
pub struct ReadHandle {
    object_id: [u8; 28],
    info: SlotInfo,
    mmap: Arc<MmapMut>,
}

impl std::fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHandle")
            .field("object_id", &hex::encode(self.object_id))
            .field("info", &self.info)
            .finish()
    }
}

impl ReadHandle {
    /// The 28-byte id this handle reads.
    #[must_use]
    pub fn object_id(&self) -> [u8; 28] {
        self.object_id
    }

    /// Allocation info as reported by the server.
    #[must_use]
    pub fn info(&self) -> &SlotInfo {
        &self.info
    }

    /// View the metadata slot. Slice lifetime is bound to `self`.
    pub fn metadata(&self) -> &[u8] {
        let off = self.info.metadata_offset as usize;
        let end = off + self.info.metadata_size as usize;
        &self.mmap[off..end]
    }

    /// View the data slot. Slice lifetime is bound to `self`.
    pub fn data(&self) -> &[u8] {
        let off = self.info.data_offset as usize;
        let end = off + self.info.data_size as usize;
        &self.mmap[off..end]
    }
}

/// Project a `&mut [u8]` into an `Arc<MmapMut>` via a raw pointer.
///
/// The caller is responsible for ensuring no other slice into the same byte
/// range exists for the duration of the returned reference. Within rayd
/// this is enforced by the protocol: only one `WriteHandle` exists per
/// object at a time, and it owns disjoint metadata + data ranges.
unsafe fn mmap_slice_mut(mmap: &Arc<MmapMut>, offset: usize, len: usize) -> &'static mut [u8] {
    let ptr = mmap.as_ptr().cast::<u8>().wrapping_add(offset).cast_mut();
    // SAFETY: caller upholds the disjointness invariant.
    unsafe { std::slice::from_raw_parts_mut(ptr, len) }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;
    use crate::error::ServerErrorKind;
    use crate::server::{PlasmaServer, ServerHandle};
    use crate::wire::AddressBlob;

    fn fresh_server() -> (TempDir, ServerHandle, PathBuf) {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        let handle = PlasmaServer::start(path.clone(), 1 << 20).expect("start");
        (dir, handle, path)
    }

    #[test]
    fn create_seal_get_round_trip() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let id = [42u8; 28];
        client
            .create_and_seal(id, b"\x01", b"hello world", AddressBlob::nil())
            .expect("create+seal");
        let h = client.get(id).expect("get");
        assert_eq!(h.metadata(), b"\x01");
        assert_eq!(h.data(), b"hello world");
    }

    #[test]
    fn contains_reports_presence_and_metadata() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let id = [9u8; 28];

        let absent = client.contains(id).expect("contains");
        assert!(!absent.present);

        client
            .create_and_seal(id, b"\x02\x01", b"x", AddressBlob::nil())
            .expect("create+seal");

        let present = client.contains(id).expect("contains");
        assert!(present.present);
        assert!(present.sealed);
        assert_eq!(present.metadata.as_deref(), Some(&b"\x02\x01"[..]));
    }

    #[test]
    fn get_before_seal_errors() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let id = [3u8; 28];
        let _h = client
            .create(id, b"\x01", b"abc", AddressBlob::nil())
            .expect("create");
        match client.get(id) {
            Err(PlasmaError::Server {
                kind: ServerErrorKind::NotSealed,
                ..
            }) => {}
            other => panic!("expected NotSealed, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_create_errors() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let id = [4u8; 28];
        let _h = client
            .create(id, b"\x01", b"a", AddressBlob::nil())
            .expect("first create");
        match client.create(id, b"\x01", b"b", AddressBlob::nil()) {
            Err(PlasmaError::Server {
                kind: ServerErrorKind::AlreadyExists,
                ..
            }) => {}
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn list_reports_known_ids() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let a = [5u8; 28];
        let b = [6u8; 28];
        client
            .create_and_seal(a, b"\x01", b"a", AddressBlob::nil())
            .expect("a");
        client
            .create_and_seal(b, b"\x01", b"b", AddressBlob::nil())
            .expect("b");
        let mut listed = client.list().expect("list");
        listed.sort_unstable();
        let mut want = vec![a, b];
        want.sort_unstable();
        assert_eq!(listed, want);
    }

    #[test]
    fn delete_removes_object() {
        let (_dir, _server, path) = fresh_server();
        let mut client = PlasmaClient::connect(&path).expect("connect");
        let id = [7u8; 28];
        client
            .create_and_seal(id, b"\x01", b"v", AddressBlob::nil())
            .expect("create+seal");
        client.delete(id).expect("delete");
        let r = client.contains(id).expect("contains");
        assert!(!r.present);
    }

    #[test]
    fn out_of_memory_returns_typed_error() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        let _server = PlasmaServer::start(path.clone(), 4096).expect("start");
        let mut client = PlasmaClient::connect(&path).expect("connect");
        match client.create([0u8; 28], b"\x01", &vec![0u8; 1 << 20], AddressBlob::nil()) {
            Err(PlasmaError::Server {
                kind: ServerErrorKind::OutOfMemory,
                ..
            }) => {}
            other => panic!("expected OutOfMemory, got {other:?}"),
        }
    }

    #[test]
    fn many_clients_share_arena_and_data() {
        // Two independent connections see each other's writes through the
        // same backing memfd: that's what makes plasma plasma.
        let (_dir, _server, path) = fresh_server();
        let mut writer = PlasmaClient::connect(&path).expect("connect writer");
        let mut reader = PlasmaClient::connect(&path).expect("connect reader");

        let id = [11u8; 28];
        let payload = vec![0xABu8; 64 * 1024];
        writer
            .create_and_seal(id, b"\x01", &payload, AddressBlob::nil())
            .expect("create+seal");

        let h = reader.get(id).expect("get");
        assert_eq!(h.data().len(), payload.len());
        assert_eq!(h.data(), payload.as_slice());
    }
}
