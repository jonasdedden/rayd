//! Driver ↔ worker dispatch protocol.
//!
//! Frames travel over a Unix domain socket as `[u32 LE length][body]`.
//! Each body is a Python pickle of a `dict` with at minimum a `"kind"`
//! key. We use pickle (rather than msgpack or bincode) because:
//!
//! - Both sides already have access to it: Rust via `crate::serialize`
//!   (which calls `CPython`'s `_pickle` through `PyO3`), Python via stdlib.
//! - `bytes` is first-class — no base64 / type-tag gymnastics.
//! - No extra runtime dependency on the worker.
//!
//! Per-task callable / args / kwargs are serialised separately with
//! **cloudpickle** so closures and test-module functions round-trip
//! correctly. The dict carries those payloads as raw `bytes` fields.
//!
//! ## Message shapes
//!
//! Worker → driver:
//! ```text
//! {"kind": "worker_ready",
//!  "worker_id": <16 raw bytes>,
//!  "pid":       <int>}
//!
//! {"kind": "task_complete",
//!  "task_id": <24 raw bytes>,
//!  "returns": [
//!      {"object_id": <28 raw bytes>,
//!       "metadata":  <small bytes>,
//!       "data_size": <int>},
//!      ...
//!  ]}
//! ```
//!
//! Driver → worker:
//! ```text
//! {"kind": "dispatch_task",
//!  "task_id":      <24 raw bytes>,
//!  "num_returns":  <int>,
//!  "callable":     <cloudpickled callable>,
//!  "args":         <cloudpickled args tuple>,
//!  "kwargs":       <cloudpickled kwargs dict, or None>}
//!
//! {"kind": "shutdown"}
//! ```

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

/// Maximum frame body the codec accepts. Hard cap; tasks with bigger args
/// would route their bulk data through plasma in a future phase.
pub(crate) const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

/// Send `[u32 LE length][body]` over the stream.
pub(crate) fn send_frame(stream: &UnixStream, body: &[u8]) -> std::io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("dispatch frame too large: {} bytes", body.len()),
        )
    })?;
    let mut s = stream;
    s.write_all(&len.to_le_bytes())?;
    s.write_all(body)?;
    Ok(())
}

/// Read a length-prefixed frame body. Returns `None` on graceful EOF
/// (peer closed) so the dispatcher can distinguish that from real errors.
pub(crate) fn recv_frame(stream: &UnixStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    let mut s = stream;
    match s.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("dispatch frame too large: {len} bytes"),
        ));
    }
    let mut body = vec![0u8; len];
    s.read_exact(&mut body)?;
    Ok(Some(body))
}
