//! `PlasmaServer`: accepts UDS connections, allocates objects in a single
//! arena, and replies with `SCM_RIGHTS`-attached memfds for direct mmap
//! access on the client side.
//!
//! Phase 2 is intentionally simple: one arena, no eviction, no spilling, no
//! per-client mmap caching (the memfd is sent on every Get reply; the
//! client may cache it locally to avoid re-mapping). Concurrent connections
//! are handled with one OS thread per client.

use std::collections::HashMap;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use parking_lot::Mutex;
use tracing::{debug, info, warn};

use crate::arena::Arena;
use crate::codec::{decode, encode};
use crate::error::{PlasmaError, ServerErrorKind};
use crate::metrics::{start_metrics_server, Metrics, MetricsServerHandle};
use crate::scm::{recv_frame, send_frame};
use crate::wire::{CreateRequest, Request, Response, SlotInfo};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
struct ObjectEntry {
    metadata_offset: u64,
    metadata_size: u64,
    data_offset: u64,
    data_size: u64,
    sealed: bool,
}

#[derive(Debug, Default)]
struct ObjectTable {
    objects: HashMap<[u8; 28], ObjectEntry>,
}

/// A handle to a running plasma server. Drop it (or call `shutdown`) to
/// stop the accept loop and remove the UDS socket file.
#[derive(Debug)]
pub struct ServerHandle {
    socket_path: PathBuf,
    accept_thread: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    arena: Arc<Arena>,
    metrics: Option<MetricsServerHandle>,
}

impl ServerHandle {
    /// Path of the UDS socket clients should connect to.
    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }

    /// Approximate bytes used by allocated objects.
    pub fn used_bytes(&self) -> u64 {
        self.arena.used()
    }

    /// Total arena capacity.
    pub fn capacity(&self) -> u64 {
        self.arena.capacity()
    }

    /// Stop the accept loop and remove the socket file. Idempotent.
    pub fn shutdown(&mut self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }
        // Stop the metrics endpoint first so its responder isn't
        // racing the table teardown.
        if let Some(mut m) = self.metrics.take() {
            m.shutdown();
        }
        // Connect to ourselves so `accept` returns, allowing the loop to
        // observe `shutdown`.
        let _ = UnixStream::connect(&self.socket_path);
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }

    /// Address of the Prometheus `/metrics` endpoint, when enabled.
    #[must_use]
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics.as_ref().map(MetricsServerHandle::local_addr)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Plasma server that runs an accept loop in a dedicated thread.
#[derive(Debug)]
pub struct PlasmaServer;

impl PlasmaServer {
    /// Bind a UDS at `socket_path` and start the accept loop. Returns a
    /// handle owning the running thread and the arena. Metrics are
    /// disabled — to opt in see `start_with_metrics`.
    pub fn start(socket_path: PathBuf, capacity: u64) -> Result<ServerHandle, PlasmaError> {
        Self::start_with_metrics(socket_path, capacity, None)
    }

    /// Like `start`, plus an optional Prometheus `/metrics` HTTP
    /// endpoint at `metrics_bind`. When `Some(addr)`, counters are
    /// updated from the request handlers and exposed via a tiny
    /// hand-rolled HTTP server (see `metrics.rs`). When `None`, no
    /// counters are touched and no extra port is bound.
    pub fn start_with_metrics(
        socket_path: PathBuf,
        capacity: u64,
        metrics_bind: Option<SocketAddr>,
    ) -> Result<ServerHandle, PlasmaError> {
        // Make sure stale socket files don't break the bind.
        let _ = std::fs::remove_file(&socket_path);

        let metrics = match metrics_bind {
            Some(_) => {
                let m = Metrics::new()?;
                m.arena_bytes_total
                    .set(i64::try_from(capacity).unwrap_or(i64::MAX));
                Some(m)
            }
            None => None,
        };

        let listener = UnixListener::bind(&socket_path)?;
        let arena = Arc::new(
            Arena::create(0, capacity, "rayd_plasma")
                .map_err(|e| PlasmaError::Mmap(e.to_string()))?,
        );
        let table = Arc::new(Mutex::new(ObjectTable::default()));
        let shutdown = Arc::new(AtomicBool::new(false));

        let arena_for_thread = Arc::clone(&arena);
        let shutdown_for_thread = Arc::clone(&shutdown);
        let socket_path_clone = socket_path.clone();
        let metrics_for_thread = metrics.clone();

        let accept_thread = thread::Builder::new()
            .name(format!("plasma-accept@{}", socket_path.display()))
            .spawn(move || {
                accept_loop(
                    &listener,
                    &arena_for_thread,
                    &table,
                    &shutdown_for_thread,
                    metrics_for_thread.as_ref(),
                );
                let _ = std::fs::remove_file(&socket_path_clone);
            })?;

        let metrics_handle = match (metrics_bind, metrics) {
            (Some(addr), Some(m)) => Some(start_metrics_server(addr, m)?),
            _ => None,
        };

        info!(
            ?socket_path,
            capacity,
            metrics_addr = ?metrics_handle.as_ref().map(MetricsServerHandle::local_addr),
            "plasma server started"
        );

        Ok(ServerHandle {
            socket_path,
            accept_thread: Some(accept_thread),
            shutdown,
            arena,
            metrics: metrics_handle,
        })
    }
}

fn accept_loop(
    listener: &UnixListener,
    arena: &Arc<Arena>,
    table: &Arc<Mutex<ObjectTable>>,
    shutdown: &Arc<AtomicBool>,
    metrics: Option<&Metrics>,
) {
    for stream in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "plasma accept failed");
                continue;
            }
        };
        let arena = Arc::clone(arena);
        let table = Arc::clone(table);
        let metrics_for_conn = metrics.cloned();
        let _ = thread::Builder::new()
            .name("plasma-conn".into())
            .spawn(move || {
                if let Err(e) = handle_connection(stream, arena, table, metrics_for_conn) {
                    debug!(error = %e, "plasma connection ended with error");
                }
            });
    }
}

fn handle_connection(
    stream: UnixStream,
    arena: Arc<Arena>,
    table: Arc<Mutex<ObjectTable>>,
    metrics: Option<Metrics>,
) -> Result<(), PlasmaError> {
    loop {
        let (body, _fds) = match recv_frame(&stream) {
            Ok(b) => b,
            Err(PlasmaError::Io(e)) if matches!(e.kind(), std::io::ErrorKind::UnexpectedEof) => {
                return Ok(());
            }
            Err(PlasmaError::Protocol(_)) => return Ok(()),
            Err(e) => return Err(e),
        };

        let request: Request = match decode(&body) {
            Ok(r) => r,
            Err(e) => {
                let response = Response::Error {
                    kind: ServerErrorKind::Internal,
                    message: format!("malformed request: {e}"),
                };
                send_response(&stream, &response, None)?;
                continue;
            }
        };

        match request {
            Request::Disconnect => return Ok(()),
            Request::Create(req) => {
                handle_create(&stream, &arena, &table, req, metrics.as_ref())?;
            }
            Request::Seal { object_id } => handle_seal(&stream, &table, object_id)?,
            Request::Get { object_id } => {
                handle_get(&stream, &arena, &table, object_id, metrics.as_ref())?;
            }
            Request::Contains { object_id } => {
                handle_contains(&stream, &arena, &table, object_id)?;
            }
            Request::Release { object_id } | Request::Delete { object_id } => {
                handle_delete(&stream, &arena, &table, object_id, metrics.as_ref())?;
            }
            Request::List => handle_list(&stream, &table)?,
        }
    }
}

fn send_response(
    stream: &UnixStream,
    response: &Response,
    fd: Option<std::os::fd::BorrowedFd<'_>>,
) -> Result<(), PlasmaError> {
    let body = encode(response)?;
    send_frame(stream, &body, fd)
}

fn err_response(kind: ServerErrorKind, message: impl Into<String>) -> Response {
    Response::Error {
        kind,
        message: message.into(),
    }
}

fn handle_create(
    stream: &UnixStream,
    arena: &Arc<Arena>,
    table: &Arc<Mutex<ObjectTable>>,
    req: CreateRequest,
    metrics: Option<&Metrics>,
) -> Result<(), PlasmaError> {
    if let Some(m) = metrics {
        m.create_total.inc();
    }
    {
        let table_guard = table.lock();
        if table_guard.objects.contains_key(&req.object_id) {
            let r = err_response(ServerErrorKind::AlreadyExists, "object already created");
            send_response(stream, &r, None)?;
            return Ok(());
        }
    }

    let metadata_size = req.metadata_size;
    let data_size = req.data_size;
    let metadata_offset = match arena.alloc(metadata_size) {
        Ok(o) => o,
        Err(e) => {
            let r = err_response(ServerErrorKind::OutOfMemory, e.to_string());
            send_response(stream, &r, None)?;
            return Ok(());
        }
    };
    let data_offset = match arena.alloc(data_size) {
        Ok(o) => o,
        Err(e) => {
            let r = err_response(ServerErrorKind::OutOfMemory, e.to_string());
            send_response(stream, &r, None)?;
            return Ok(());
        }
    };

    {
        let mut table_guard = table.lock();
        table_guard.objects.insert(
            req.object_id,
            ObjectEntry {
                metadata_offset,
                metadata_size,
                data_offset,
                data_size,
                sealed: false,
            },
        );
        if let Some(m) = metrics {
            m.objects_total
                .set(i64::try_from(table_guard.objects.len()).unwrap_or(i64::MAX));
        }
    }
    if let Some(m) = metrics {
        m.arena_bytes_used
            .set(i64::try_from(arena.used()).unwrap_or(i64::MAX));
    }

    let info = SlotInfo {
        arena_id: arena.id(),
        arena_capacity: arena.capacity(),
        metadata_offset,
        metadata_size,
        data_offset,
        data_size,
        sealed: false,
    };
    send_response(
        stream,
        &Response::Created(info),
        Some(arena.as_borrowed_fd()),
    )?;
    Ok(())
}

fn handle_seal(
    stream: &UnixStream,
    table: &Arc<Mutex<ObjectTable>>,
    object_id: [u8; 28],
) -> Result<(), PlasmaError> {
    let response = {
        let mut guard = table.lock();
        match guard.objects.get_mut(&object_id) {
            None => err_response(ServerErrorKind::NotFound, "object not present"),
            Some(entry) if entry.sealed => {
                err_response(ServerErrorKind::AlreadySealed, "object already sealed")
            }
            Some(entry) => {
                entry.sealed = true;
                Response::Ok
            }
        }
    };
    send_response(stream, &response, None)?;
    Ok(())
}

fn handle_get(
    stream: &UnixStream,
    arena: &Arc<Arena>,
    table: &Arc<Mutex<ObjectTable>>,
    object_id: [u8; 28],
    metrics: Option<&Metrics>,
) -> Result<(), PlasmaError> {
    if let Some(m) = metrics {
        m.get_total.inc();
    }
    let entry = table.lock().objects.get(&object_id).cloned();
    match entry {
        None => send_response(
            stream,
            &err_response(ServerErrorKind::NotFound, "not found"),
            None,
        ),
        Some(entry) if !entry.sealed => send_response(
            stream,
            &err_response(ServerErrorKind::NotSealed, "object not yet sealed"),
            None,
        ),
        Some(entry) => {
            let info = SlotInfo {
                arena_id: arena.id(),
                arena_capacity: arena.capacity(),
                metadata_offset: entry.metadata_offset,
                metadata_size: entry.metadata_size,
                data_offset: entry.data_offset,
                data_size: entry.data_size,
                sealed: true,
            };
            send_response(stream, &Response::Got(info), Some(arena.as_borrowed_fd()))
        }
    }
}

fn handle_contains(
    stream: &UnixStream,
    arena: &Arc<Arena>,
    table: &Arc<Mutex<ObjectTable>>,
    object_id: [u8; 28],
) -> Result<(), PlasmaError> {
    let entry = table.lock().objects.get(&object_id).cloned();
    let response = match entry {
        None => Response::Contains {
            present: false,
            metadata: None,
            sealed: false,
        },
        Some(entry) => {
            let metadata = if entry.sealed {
                Some(arena.read_copy(entry.metadata_offset, entry.metadata_size))
            } else {
                None
            };
            Response::Contains {
                present: true,
                metadata,
                sealed: entry.sealed,
            }
        }
    };
    send_response(stream, &response, None)
}

fn handle_delete(
    stream: &UnixStream,
    arena: &Arc<Arena>,
    table: &Arc<Mutex<ObjectTable>>,
    object_id: [u8; 28],
    metrics: Option<&Metrics>,
) -> Result<(), PlasmaError> {
    if let Some(m) = metrics {
        m.delete_total.inc();
    }
    let new_count = {
        let mut guard = table.lock();
        guard.objects.remove(&object_id);
        guard.objects.len()
    };
    if let Some(m) = metrics {
        m.objects_total
            .set(i64::try_from(new_count).unwrap_or(i64::MAX));
        // Note: arena.used() doesn't shrink on delete in Phase 2 (no
        // free-list yet — proper arena reuse waits on Phase 6).
        // Setting it anyway keeps the metric internally consistent
        // for the inverse: a future arena that does reclaim will
        // automatically reflect that here without changing this code.
        m.arena_bytes_used
            .set(i64::try_from(arena.used()).unwrap_or(i64::MAX));
    }
    send_response(stream, &Response::Ok, None)
}

fn handle_list(stream: &UnixStream, table: &Arc<Mutex<ObjectTable>>) -> Result<(), PlasmaError> {
    let ids: Vec<[u8; 28]> = table.lock().objects.keys().copied().collect();
    send_response(stream, &Response::Listed { object_ids: ids }, None)
}

#[cfg(unix)]
#[allow(unused)] // surfaced via tests in client.rs to exercise round-trips.
pub(crate) fn _server_module_compiles() {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn server_starts_and_stops() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        let mut handle = PlasmaServer::start(path.clone(), 4096).expect("start");
        assert!(path.exists());
        handle.shutdown();
        assert!(!path.exists());
    }

    #[test]
    fn drop_cleans_up_socket() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        {
            let _handle = PlasmaServer::start(path.clone(), 4096).expect("start");
        }
        assert!(!path.exists());
    }

    #[test]
    fn capacity_is_reported() {
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        let handle = PlasmaServer::start(path, 16384).expect("start");
        assert_eq!(handle.capacity(), 16384);
        assert_eq!(handle.used_bytes(), 0);
    }

    #[test]
    fn metrics_endpoint_counts_create_get_delete() {
        use crate::{AddressBlob, PlasmaClient};
        use std::io::{Read as _, Write as _};
        use std::net::TcpStream;
        use std::time::Duration;

        // Bind plasma + a metrics endpoint on a kernel-assigned port.
        let dir = TempDir::new().expect("tempdir");
        let path = dir.path().join("plasma.sock");
        let metrics_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut handle = PlasmaServer::start_with_metrics(path.clone(), 65536, Some(metrics_addr))
            .expect("start_with_metrics");
        let metrics_addr = handle.metrics_addr().expect("metrics enabled");

        // Drive a few client RPCs: 2 create+seal, 1 get, 1 delete.
        let mut client = PlasmaClient::connect(&path).expect("client connect");
        let oid_a: [u8; 28] = [0xA1; 28];
        let oid_b: [u8; 28] = [0xB2; 28];
        client
            .create_and_seal(oid_a, b"meta", b"the-payload-aaaa", AddressBlob::nil())
            .expect("create+seal a");
        client
            .create_and_seal(oid_b, b"meta", b"the-payload-bbbb", AddressBlob::nil())
            .expect("create+seal b");

        let _read = client.get(oid_a).expect("get a");
        client.delete(oid_a).expect("delete a");

        // Scrape /metrics with raw HTTP to avoid pulling in a client.
        let mut stream =
            TcpStream::connect_timeout(&metrics_addr, Duration::from_millis(500)).expect("dial");
        stream
            .write_all(
                format!(
                    "GET /metrics HTTP/1.0\r\nHost: {metrics_addr}\r\nConnection: close\r\n\r\n",
                )
                .as_bytes(),
            )
            .expect("send GET");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).expect("read response");
        let response = String::from_utf8_lossy(&buf).into_owned();
        let body = response
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_owned())
            .unwrap_or(response);

        assert!(
            body.contains("rayd_plasma_create_total 2"),
            "body = {body:?}"
        );
        assert!(body.contains("rayd_plasma_get_total 1"), "body = {body:?}");
        assert!(
            body.contains("rayd_plasma_delete_total 1"),
            "body = {body:?}"
        );
        // 1 object remaining after the delete.
        assert!(
            body.contains("rayd_plasma_objects_total 1"),
            "body = {body:?}"
        );
        // Static at startup; should mirror what we passed.
        assert!(
            body.contains("rayd_plasma_arena_bytes_total 65536"),
            "body = {body:?}"
        );

        handle.shutdown();
    }
}
