//! Per-raylet object manager (Phase 6.2 skeleton).
//!
//! Owns the spill backend and the in-memory `oid → SpillUrl`
//! directory. The manager is the single chokepoint for "this object
//! left plasma": when the raylet decides to evict, it calls
//! [`LocalObjectManager::spill`]; when a `Pull`/local-`Get` misses
//! plasma but the manager knows about the oid, it calls
//! [`LocalObjectManager::restore`]; when the owner-side refcount
//! reaches zero, it calls [`LocalObjectManager::forget`].
//!
//! Phase 6.2 ships only the directory + the spill/restore/forget
//! plumbing on top of an injected [`SpillBackend`]. Spill-on-pressure
//! (the policy that decides *when* to evict) and the restore-on-get
//! integration land in subsequent phases.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use rayd_core::{ObjectId, ObjectRecoverer, RecoveredObject, RecoveryError};

use crate::spill::{ObjectIdBytes, RestoredObject, SpillBackend, SpillError, SpillUrl};

/// In-memory directory + spill-backend coordinator.
pub struct LocalObjectManager {
    backend: Arc<dyn SpillBackend>,
    /// `oid → SpillUrl` for objects currently spilled. Held under a
    /// short-lived mutex; backend I/O happens *outside* the lock so
    /// a slow disk doesn't stall lookup.
    directory: Mutex<HashMap<ObjectIdBytes, SpillUrl>>,
}

impl std::fmt::Debug for LocalObjectManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalObjectManager")
            .field("backend", &self.backend)
            .field("spilled_count", &self.directory.lock().len())
            .finish()
    }
}

impl LocalObjectManager {
    /// Construct a manager backed by `backend`. The directory starts
    /// empty — callers can rebuild from the backend by re-spilling
    /// known objects, or by enumerating the backend (left as a future
    /// extension).
    #[must_use]
    pub fn new(backend: Arc<dyn SpillBackend>) -> Self {
        Self {
            backend,
            directory: Mutex::new(HashMap::new()),
        }
    }

    /// Number of objects currently tracked as spilled.
    #[must_use]
    pub fn spilled_count(&self) -> usize {
        self.directory.lock().len()
    }

    /// `true` if `object_id` has a spill entry.
    #[must_use]
    pub fn is_spilled(&self, object_id: ObjectIdBytes) -> bool {
        self.directory.lock().contains_key(&object_id)
    }

    /// Look up the spill url for `object_id`, if any.
    #[must_use]
    pub fn spill_url(&self, object_id: ObjectIdBytes) -> Option<SpillUrl> {
        self.directory.lock().get(&object_id).cloned()
    }

    /// Spill `object_id`'s `(metadata, data)` to the backend and
    /// record the resulting url in the directory. Idempotent on the
    /// same `object_id`: re-spilling overwrites both the backend
    /// bytes and the directory entry.
    ///
    /// Errors propagate from the backend; on error the directory is
    /// left untouched so a retry can restore the previous mapping.
    pub fn spill(
        &self,
        object_id: ObjectIdBytes,
        metadata: Bytes,
        data: Bytes,
    ) -> Result<SpillUrl, SpillError> {
        let url = self.backend.spill(object_id, metadata, data)?;
        self.directory.lock().insert(object_id, url.clone());
        Ok(url)
    }

    /// Restore `object_id` from the backend. Returns `Ok(None)` if
    /// the manager has no spill entry for it (caller should look
    /// elsewhere — e.g. live plasma, peer node, lineage).
    ///
    /// On a transient backend error the directory entry is preserved
    /// so a retry can succeed. On `SpillError::NotFound`/`Corrupt`
    /// the entry is dropped — re-trying the same url won't help, and
    /// keeping the stale mapping would falsely advertise the object
    /// as recoverable.
    pub fn restore(
        &self,
        object_id: ObjectIdBytes,
    ) -> Result<Option<RestoredObject>, SpillError> {
        let Some(url) = self.spill_url(object_id) else {
            return Ok(None);
        };
        match self.backend.restore(&url) {
            Ok(restored) => Ok(Some(restored)),
            Err(e @ (SpillError::NotFound { .. } | SpillError::Corrupt { .. })) => {
                // The url is unrecoverable — purge it so a later
                // caller doesn't keep retrying the same dead path.
                self.directory.lock().remove(&object_id);
                Err(e)
            }
            Err(other) => Err(other),
        }
    }

    /// Drop the spill entry for `object_id` and remove the backend
    /// bytes. Idempotent — calling on an unknown `object_id` is a
    /// successful no-op.
    pub fn forget(&self, object_id: ObjectIdBytes) -> Result<(), SpillError> {
        let url = self.directory.lock().remove(&object_id);
        if let Some(url) = url {
            self.backend.remove(&url)?;
        }
        Ok(())
    }
}

/// Bridge `LocalObjectManager` into `rayd_core`'s recovery hook so the
/// driver-side `CoreWorker::resolve_entry` can transparently restore
/// an object that's been evicted out of plasma.
impl ObjectRecoverer for LocalObjectManager {
    fn recover(&self, id: ObjectId) -> Result<Option<RecoveredObject>, RecoveryError> {
        match self.restore(*id.as_bytes()) {
            Ok(Some(RestoredObject { metadata, data })) => {
                Ok(Some(RecoveredObject { metadata, data }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(RecoveryError::Other(format!("spill restore: {e}"))),
        }
    }

    fn store(
        &self,
        id: ObjectId,
        metadata: Bytes,
        data: Bytes,
    ) -> Result<(), RecoveryError> {
        match self.spill(*id.as_bytes(), metadata, data) {
            Ok(_url) => Ok(()),
            Err(e) => Err(RecoveryError::Other(format!("spill store: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spill::LocalFsBackend;
    use tempfile::TempDir;

    fn obj_id(seed: u8) -> ObjectIdBytes {
        let mut id = [0u8; 28];
        id[0] = seed;
        id
    }

    fn fixture() -> (TempDir, LocalObjectManager) {
        let dir = TempDir::new().unwrap();
        let backend = Arc::new(LocalFsBackend::new(dir.path()).unwrap());
        let manager = LocalObjectManager::new(backend);
        (dir, manager)
    }

    #[test]
    fn empty_manager_reports_nothing_spilled() {
        let (_dir, mgr) = fixture();
        assert_eq!(mgr.spilled_count(), 0);
        assert!(!mgr.is_spilled(obj_id(1)));
        assert!(mgr.spill_url(obj_id(1)).is_none());
    }

    #[test]
    fn spill_records_url_and_restores_round_trips() {
        let (_dir, mgr) = fixture();
        let metadata = Bytes::from_static(b"meta");
        let data = Bytes::from_static(b"the payload");

        let url = mgr.spill(obj_id(2), metadata.clone(), data.clone()).unwrap();
        assert_eq!(mgr.spilled_count(), 1);
        assert!(mgr.is_spilled(obj_id(2)));
        assert_eq!(mgr.spill_url(obj_id(2)).as_ref(), Some(&url));

        let restored = mgr.restore(obj_id(2)).unwrap().unwrap();
        assert_eq!(restored.metadata, metadata);
        assert_eq!(restored.data, data);
    }

    #[test]
    fn restore_unknown_object_is_none_not_error() {
        let (_dir, mgr) = fixture();
        assert!(mgr.restore(obj_id(3)).unwrap().is_none());
    }

    #[test]
    fn forget_drops_directory_entry_and_backend_file() {
        let (_dir, mgr) = fixture();
        mgr.spill(obj_id(4), Bytes::from_static(b"m"), Bytes::from_static(b"d")).unwrap();
        assert!(mgr.is_spilled(obj_id(4)));

        mgr.forget(obj_id(4)).unwrap();
        assert!(!mgr.is_spilled(obj_id(4)));
        assert_eq!(mgr.spilled_count(), 0);
        // Subsequent restore is a clean None.
        assert!(mgr.restore(obj_id(4)).unwrap().is_none());
    }

    #[test]
    fn forget_unknown_is_idempotent() {
        let (_dir, mgr) = fixture();
        mgr.forget(obj_id(5)).expect("forgetting an unknown oid is a no-op");
    }

    #[test]
    fn re_spill_overwrites_directory_entry() {
        let (_dir, mgr) = fixture();
        let url1 = mgr
            .spill(obj_id(6), Bytes::from_static(b"m1"), Bytes::from_static(b"d1"))
            .unwrap();
        let url2 = mgr
            .spill(obj_id(6), Bytes::from_static(b"m2"), Bytes::from_static(b"d2"))
            .unwrap();
        // `LocalFsBackend` reuses paths per oid, so the urls match.
        assert_eq!(url1, url2);
        let restored = mgr.restore(obj_id(6)).unwrap().unwrap();
        assert_eq!(restored.data, Bytes::from_static(b"d2"));
    }

    /// Two threads racing to re-spill the same id must both succeed,
    /// and the resulting on-disk + directory state must be one of the
    /// two payloads — never a half-written or mismatched mix. The
    /// backend's atomic-rename + `last writer wins` directory insert
    /// is what makes this safe; this test pins that invariant.
    #[test]
    fn concurrent_re_spill_of_same_oid_is_safe() {
        use std::thread;

        let (_dir, mgr) = fixture();
        let mgr = Arc::new(mgr);

        let payload_a = Bytes::from(vec![0xAAu8; 4096]);
        let payload_b = Bytes::from(vec![0xBBu8; 4096]);
        let id = obj_id(20);

        // Hammer the same oid from two threads concurrently. Each
        // iteration tries to overwrite whatever's there; whichever
        // thread is latest wins both the directory entry and the
        // backend file.
        let handles: Vec<_> = (0..2)
            .map(|i| {
                let mgr = Arc::clone(&mgr);
                let payload = if i == 0 {
                    payload_a.clone()
                } else {
                    payload_b.clone()
                };
                thread::spawn(move || {
                    for _ in 0..100 {
                        mgr.spill(id, Bytes::from_static(b"m"), payload.clone())
                            .expect("spill");
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(mgr.spilled_count(), 1, "single id stays single");
        let restored = mgr.restore(id).unwrap().unwrap();
        assert!(
            restored.data == payload_a || restored.data == payload_b,
            "restored payload must be one of the two writers' inputs (no torn write)",
        );
    }

    #[test]
    fn multiple_objects_round_trip_independently() {
        let (_dir, mgr) = fixture();
        for i in 0..5u8 {
            let data = Bytes::from(vec![i; 16]);
            mgr.spill(obj_id(10 + i), Bytes::from_static(b"m"), data).unwrap();
        }
        assert_eq!(mgr.spilled_count(), 5);
        for i in 0..5u8 {
            let restored = mgr.restore(obj_id(10 + i)).unwrap().unwrap();
            assert_eq!(restored.data, Bytes::from(vec![i; 16]));
        }
    }

    /// If the on-disk file vanished out from under us (manual cleanup,
    /// disk corruption, …) the manager surfaces the error AND drops
    /// its stale entry so the caller doesn't keep hitting the same
    /// dead url.
    #[test]
    fn restore_purges_stale_entry_on_not_found() {
        let dir = TempDir::new().unwrap();
        let backend = Arc::new(LocalFsBackend::new(dir.path()).unwrap());
        let mgr = LocalObjectManager::new(backend.clone());

        let url = mgr
            .spill(obj_id(7), Bytes::from_static(b"m"), Bytes::from_static(b"d"))
            .unwrap();
        // Yank the file out from under the manager.
        backend.remove(&url).unwrap();
        // The directory still says it's spilled at this point —
        // restore() detects the gap and self-heals.
        assert!(mgr.is_spilled(obj_id(7)));
        let err = mgr.restore(obj_id(7)).unwrap_err();
        assert!(matches!(err, SpillError::NotFound { .. }), "got {err:?}");
        assert!(!mgr.is_spilled(obj_id(7)));
    }
}
