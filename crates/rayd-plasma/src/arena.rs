//! Arena allocator backed by an anonymous file + `mmap`.
//!
//! Phase 2 design: a single arena per server, bump-allocated. Eviction
//! and multi-arena pools land in later phases.
//!
//! Cross-platform anonymous-fd story:
//! - **Linux**: `memfd_create(MFD_CLOEXEC)` returns a fresh anonymous
//!   fd backed by tmpfs. The fd has no filesystem name; it's passed
//!   to peer processes via `SCM_RIGHTS` and they can mmap it directly.
//! - **macOS** (and other non-Linux unix): `memfd_create` doesn't exist.
//!   We use the POSIX `shm_open` + immediate `shm_unlink` idiom: open a
//!   uniquely-named segment with `O_CREAT|O_EXCL|O_RDWR|O_CLOEXEC`,
//!   then unlink the name right away. The fd remains valid and can be
//!   passed via `SCM_RIGHTS` exactly like a memfd; the name is gone
//!   so concurrent processes don't collide and `/dev/shm` doesn't
//!   leak entries on crash.

use std::ffi::CString;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};

use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;
use thiserror::Error;

#[cfg(any(target_os = "linux", target_os = "android"))]
use nix::sys::memfd::{memfd_create, MemFdCreateFlag};

#[cfg(not(any(target_os = "linux", target_os = "android")))]
use nix::{
    fcntl::OFlag,
    sys::{
        mman::{shm_open, shm_unlink},
        stat::Mode,
    },
};

use crate::OBJECT_ALIGN;

/// Errors specific to arena allocation. Transparent into [`crate::PlasmaError`].
#[derive(Debug, Error)]
pub enum ArenaError {
    /// Underlying syscall failure (`memfd_create`, `ftruncate`).
    #[error("arena syscall: {0}")]
    Nix(#[from] nix::errno::Errno),
    /// Standard I/O error.
    #[error("arena io: {0}")]
    Io(#[from] std::io::Error),
    /// Bump pointer would exceed arena capacity.
    #[error("arena out of memory: requested {requested} bytes, only {remaining} remaining")]
    OutOfMemory {
        /// Bytes asked for.
        requested: u64,
        /// Bytes remaining at the time of the request.
        remaining: u64,
    },
    /// Arena name (for `memfd_create`) contained an interior NUL byte.
    #[error("arena name contained NUL: {0}")]
    InvalidName(String),
}

/// Create an anonymous fd suitable for `mmap` + `SCM_RIGHTS` handoff.
///
/// On Linux this is a one-shot `memfd_create`. On macOS we use the
/// POSIX-shm + immediate-unlink idiom — same end result, the name
/// just briefly appears in the shm namespace before we delete it.
#[cfg(any(target_os = "linux", target_os = "android"))]
fn create_anon_fd(cname: &std::ffi::CStr, _id: u64) -> Result<OwnedFd, ArenaError> {
    Ok(memfd_create(cname, MemFdCreateFlag::MFD_CLOEXEC)?)
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn create_anon_fd(_cname: &std::ffi::CStr, _id: u64) -> Result<OwnedFd, ArenaError> {
    use std::sync::atomic::{AtomicU64, Ordering};

    // POSIX shm names are filesystem-visible and shared across the
    // host. macOS additionally caps `PSHMNAMLEN` at 31 chars
    // (including the leading `/` and null terminator), so the format
    // below is deliberately compact. `pid` (≤8 hex chars) plus a
    // process-local counter (≤16 hex chars) gives system-wide
    // uniqueness in 4 + 8 + 1 + 16 = 29 chars, comfortably under
    // the limit even at the pathological end of the counter range.
    // The caller's `name` is informational only on Linux's memfd —
    // dropped here.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    let unique = format!("/rd-{pid:x}-{counter:x}");
    let cunique =
        CString::new(unique.clone()).map_err(|_| ArenaError::InvalidName(unique.clone()))?;

    let fd = shm_open(
        cunique.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )?;
    // Best-effort: unlink the name so the segment becomes anonymous
    // (the open fd keeps the storage alive). If unlink fails the
    // segment still works for this process — the only consequence is
    // a stale entry in the shm namespace until the host reboots, so
    // we log and proceed rather than failing the open.
    if let Err(e) = shm_unlink(cunique.as_c_str()) {
        tracing::warn!(
            error = %e,
            name = %unique,
            "rayd-plasma: shm_unlink after shm_open failed; segment will leak by name"
        );
    }
    Ok(fd)
}

/// One mmap-backed memfd region. Cheap to clone via `Arc` because the arena
/// itself is `!Clone`; users hold an `Arc<Arena>`.
#[derive(Debug)]
pub struct Arena {
    id: u64,
    fd: OwnedFd,
    mmap: Mutex<MmapMut>,
    capacity: u64,
    bump: Mutex<u64>,
}

impl Arena {
    /// Create a fresh memfd-backed arena of `capacity` bytes named `name`.
    ///
    /// The memfd is sized via `ftruncate` and then mmapped read/write,
    /// shared. The returned `Arena` owns the memfd; cloning the underlying
    /// fd happens on `as_borrowed` and during `SCM_RIGHTS` transfers.
    pub fn create(id: u64, capacity: u64, name: &str) -> Result<Self, ArenaError> {
        let cname = CString::new(name).map_err(|_| ArenaError::InvalidName(name.to_owned()))?;
        let fd: OwnedFd = create_anon_fd(&cname, id)?;

        // Cast capacity to off_t (i64 on Linux/macOS).
        let off =
            i64::try_from(capacity).map_err(|_| ArenaError::Nix(nix::errno::Errno::EINVAL))?;
        nix::unistd::ftruncate(fd.as_fd(), off)?;

        let mmap = unsafe {
            MmapOptions::new()
                .len(
                    usize::try_from(capacity)
                        .map_err(|_| ArenaError::Nix(nix::errno::Errno::EINVAL))?,
                )
                .map_mut(&fd)?
        };

        Ok(Self {
            id,
            fd,
            mmap: Mutex::new(mmap),
            capacity,
            bump: Mutex::new(0),
        })
    }

    /// The arena id (server-assigned; distinct from the memfd's number).
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// Total bytes mapped by this arena.
    #[must_use]
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Bytes already handed out by the bump pointer.
    #[must_use]
    pub fn used(&self) -> u64 {
        *self.bump.lock()
    }

    /// Bytes remaining for new allocations.
    #[must_use]
    pub fn remaining(&self) -> u64 {
        self.capacity.saturating_sub(self.used())
    }

    /// A borrowed view of the underlying memfd, suitable for `SCM_RIGHTS`.
    pub fn as_borrowed_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    /// Raw fd accessor for diagnostics. Prefer `as_borrowed_fd` for safety.
    #[must_use]
    pub fn raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }

    /// Allocate `size` bytes from the bump pointer at `OBJECT_ALIGN` alignment.
    ///
    /// Returns the byte offset within the arena. The caller is responsible
    /// for actually filling the region (via mmap on either side).
    pub fn alloc(&self, size: u64) -> Result<u64, ArenaError> {
        let mut bump = self.bump.lock();
        let aligned = (*bump + OBJECT_ALIGN - 1) & !(OBJECT_ALIGN - 1);
        let end = aligned.checked_add(size).ok_or(ArenaError::OutOfMemory {
            requested: size,
            remaining: 0,
        })?;
        if end > self.capacity {
            return Err(ArenaError::OutOfMemory {
                requested: size,
                remaining: self.capacity.saturating_sub(*bump),
            });
        }
        *bump = end;
        Ok(aligned)
    }

    /// Read a slice at `offset..offset+len` from the arena's mmap.
    ///
    /// Used by the server to project the metadata header out of an arena
    /// slot for `Contains` replies. Avoid calling for large slices; large
    /// reads should use the client's mmap of the same memfd.
    #[must_use]
    pub fn read_copy(&self, offset: u64, len: u64) -> Vec<u8> {
        let off = offset as usize;
        let len = len as usize;
        let guard = self.mmap.lock();
        guard[off..off + len].to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_advances_bump_with_alignment() {
        let arena = Arena::create(0, 1024, "rayd_test_arena").expect("create");
        let a = arena.alloc(7).expect("alloc 7");
        let b = arena.alloc(8).expect("alloc 8");
        // First alloc starts at 0.
        assert_eq!(a, 0);
        // Next alloc is 16-byte aligned (after 7 bytes -> next aligned is 16).
        assert_eq!(b, 16);
        assert_eq!(arena.used(), 24);
    }

    #[test]
    fn alloc_fails_when_exhausted() {
        let arena = Arena::create(1, 64, "rayd_test_small").expect("create");
        let _ = arena.alloc(64).expect("alloc 64");
        assert!(matches!(
            arena.alloc(1),
            Err(ArenaError::OutOfMemory { .. })
        ));
    }

    #[test]
    fn capacity_and_remaining_track_bump() {
        let arena = Arena::create(2, 256, "rayd_test_capacity").expect("create");
        assert_eq!(arena.capacity(), 256);
        assert_eq!(arena.remaining(), 256);
        let _ = arena.alloc(100).expect("alloc 100");
        // Bump only pads on the *next* alloc (it's leading-edge alignment), so
        // a single 100-byte alloc consumes exactly 100 bytes.
        assert_eq!(arena.remaining(), 256 - 100);
        let _ = arena.alloc(8).expect("alloc 8");
        // Now the pre-pad to 112 happens before the 8-byte allocation.
        assert_eq!(arena.remaining(), 256 - 120);
    }

    #[test]
    fn read_copy_returns_initial_zeros() {
        let arena = Arena::create(3, 64, "rayd_test_read").expect("create");
        let bytes = arena.read_copy(0, 16);
        assert_eq!(bytes, vec![0u8; 16]);
    }
}
