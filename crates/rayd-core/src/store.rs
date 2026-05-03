//! `MemoryStore`: the per-worker local index of objects.
//!
//! After Phase 2 the store holds two kinds of entries:
//!
//! - **Inline**: the full `RayObject` lives in process memory. Used for
//!   small payloads, exception sentinels, and freshly-pickled control
//!   results.
//! - **Plasma**: only the metadata header lives here; the data buffer is
//!   in the shared-memory plasma store, accessed via `rayd-plasma`.
//!
//! Either kind is enough for cheap state inspection — the metadata is on
//! the local index for both. Data fetches for plasma entries route through
//! the plasma client (handled by `CoreWorker::resolve`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::id::ObjectId;
use crate::metadata::{Metadata, RefState};
use crate::ray_object::RayObject;

/// Outcome of a `wait()` call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WaitOutcome {
    /// Ids that are present in the store.
    pub ready: Vec<ObjectId>,
    /// Ids that were not yet present when the wait returned.
    pub not_ready: Vec<ObjectId>,
}

/// Local pointer to a plasma-resident object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlasmaIndex {
    /// The object's metadata header (a few bytes).
    pub metadata: Metadata,
    /// Length of the data buffer in plasma.
    pub data_size: u64,
}

/// Either an inline `RayObject` or a pointer to plasma.
#[derive(Clone, Debug)]
pub enum StoredEntry {
    /// Full object held in process memory.
    Inline(Arc<RayObject>),
    /// Plasma-resident; only the metadata header is local.
    Plasma(PlasmaIndex),
}

impl StoredEntry {
    /// Read the entry's metadata (cheap; no copies).
    #[must_use]
    pub fn metadata(&self) -> Metadata {
        match self {
            Self::Inline(obj) => obj.metadata,
            Self::Plasma(idx) => idx.metadata,
        }
    }

    /// Whether the entry's metadata is an `Error` variant.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.metadata().is_error()
    }

    /// Map to a `RefState`. Always `ReadyLocal` or `Failed`; never `Pending`
    /// (the entry by definition exists).
    #[must_use]
    pub fn ref_state(&self) -> RefState {
        if self.is_error() {
            RefState::Failed
        } else {
            RefState::ReadyLocal
        }
    }

    /// Local-memory size in bytes (excludes plasma payload).
    #[must_use]
    pub fn local_bytes(&self) -> usize {
        match self {
            Self::Inline(obj) => obj.size_bytes(),
            // Plasma entries only carry their index; the data lives elsewhere.
            Self::Plasma(_) => size_of::<PlasmaIndex>(),
        }
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<ObjectId, StoredEntry>,
    bytes: u64,
}

/// Thread-safe in-process index mapping `ObjectId → StoredEntry`.
///
/// Writers (worker threads landing task results) call `put_inline` /
/// `put_plasma`; readers (the public API surface) call `get_*`,
/// `state_*`, or `wait`. A process-wide condition variable wakes all
/// blocked readers on every put; per-id signaling is a future
/// optimization.
#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
    notify: Condvar,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.lock();
        f.debug_struct("MemoryStore")
            .field("entries", &guard.entries.len())
            .field("local_bytes", &guard.bytes)
            .finish()
    }
}

impl MemoryStore {
    /// Create a fresh empty store wrapped in an `Arc`.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Insert (or replace) an inline object. Wakes blocked waiters.
    pub fn put_inline(&self, id: ObjectId, object: RayObject) {
        let entry = StoredEntry::Inline(Arc::new(object));
        self.insert(id, entry);
    }

    /// Register a plasma-resident object. Metadata + size live locally;
    /// the bulk data is in plasma.
    pub fn put_plasma(&self, id: ObjectId, index: PlasmaIndex) {
        self.insert(id, StoredEntry::Plasma(index));
    }

    fn insert(&self, id: ObjectId, entry: StoredEntry) {
        {
            let mut guard = self.inner.lock();
            let added = entry.local_bytes() as u64;
            if let Some(prev) = guard.entries.insert(id, entry) {
                guard.bytes = guard.bytes.saturating_sub(prev.local_bytes() as u64);
            }
            guard.bytes = guard.bytes.saturating_add(added);
        }
        self.notify.notify_all();
    }

    /// Non-blocking lookup of the full local entry (inline or plasma).
    #[must_use]
    pub fn get_entry(&self, id: &ObjectId) -> Option<StoredEntry> {
        self.inner.lock().entries.get(id).cloned()
    }

    /// Non-blocking inline lookup. Returns `None` for plasma-resident
    /// objects too — callers that want to handle plasma should use
    /// `get_entry`.
    #[must_use]
    pub fn get_inline(&self, id: &ObjectId) -> Option<Arc<RayObject>> {
        match self.inner.lock().entries.get(id) {
            Some(StoredEntry::Inline(obj)) => Some(Arc::clone(obj)),
            _ => None,
        }
    }

    /// Whether an id is present in the store (either kind).
    #[must_use]
    pub fn contains(&self, id: &ObjectId) -> bool {
        self.inner.lock().entries.contains_key(id)
    }

    /// State for a single id. Cheap; no data deserialization.
    #[must_use]
    pub fn state_of(&self, id: &ObjectId) -> RefState {
        self.inner
            .lock()
            .entries
            .get(id)
            .map_or(RefState::Pending, StoredEntry::ref_state)
    }

    /// Batched state lookup; one mutex acquisition for all ids.
    #[must_use]
    pub fn state_snapshot(&self, ids: &[ObjectId]) -> HashMap<ObjectId, RefState> {
        let guard = self.inner.lock();
        ids.iter()
            .map(|id| {
                let state = guard
                    .entries
                    .get(id)
                    .map_or(RefState::Pending, StoredEntry::ref_state);
                (*id, state)
            })
            .collect()
    }

    /// Block until the entry is present, then return it. Times out if
    /// `timeout` elapses before any entry lands.
    #[must_use]
    pub fn get_entry_blocking(
        &self,
        id: &ObjectId,
        timeout: Option<Duration>,
    ) -> Option<StoredEntry> {
        let mut guard = self.inner.lock();
        let deadline = timeout.map(|t| Instant::now() + t);
        loop {
            if let Some(entry) = guard.entries.get(id) {
                return Some(entry.clone());
            }
            match deadline {
                None => self.notify.wait(&mut guard),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return None;
                    }
                    let _ = self.notify.wait_for(&mut guard, d - now);
                }
            }
        }
    }

    /// Wait for at least `num_returns` of the supplied ids to enter the
    /// store (either kind). Returns the partition into ready/not-ready.
    #[must_use]
    pub fn wait(
        &self,
        ids: &[ObjectId],
        num_returns: usize,
        timeout: Option<Duration>,
    ) -> WaitOutcome {
        let target = num_returns.min(ids.len());
        let mut guard = self.inner.lock();
        let deadline = timeout.map(|t| Instant::now() + t);

        loop {
            let ready_count = ids
                .iter()
                .filter(|id| guard.entries.contains_key(id))
                .count();
            if ready_count >= target {
                break;
            }
            match deadline {
                None => self.notify.wait(&mut guard),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        break;
                    }
                    let _ = self.notify.wait_for(&mut guard, d - now);
                }
            }
        }

        let mut ready = Vec::with_capacity(target);
        let mut not_ready = Vec::with_capacity(ids.len().saturating_sub(target));
        for id in ids {
            if guard.entries.contains_key(id) {
                ready.push(*id);
            } else {
                not_ready.push(*id);
            }
        }
        WaitOutcome { ready, not_ready }
    }

    /// Remove a set of ids.
    pub fn delete(&self, ids: &[ObjectId]) {
        let mut guard = self.inner.lock();
        for id in ids {
            if let Some(prev) = guard.entries.remove(id) {
                guard.bytes = guard.bytes.saturating_sub(prev.local_bytes() as u64);
            }
        }
    }

    /// Number of entries in the store.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().entries.len()
    }

    /// Whether the store is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.lock().entries.is_empty()
    }

    /// Approximate local-memory bytes used (does NOT count plasma data).
    #[must_use]
    pub fn used_memory(&self) -> u64 {
        self.inner.lock().bytes
    }

    /// Snapshot of every plasma-resident entry. Returned as a `Vec`
    /// rather than an iterator so the lock isn't held across the
    /// caller's iteration. Used by the spill-on-pressure policy to
    /// pick eviction victims.
    #[must_use]
    pub fn plasma_entries(&self) -> Vec<(ObjectId, PlasmaIndex)> {
        let guard = self.inner.lock();
        guard
            .entries
            .iter()
            .filter_map(|(id, entry)| match entry {
                StoredEntry::Plasma(idx) => Some((*id, *idx)),
                StoredEntry::Inline(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use bytes::Bytes;

    use super::*;
    use crate::id::TaskId;
    use crate::metadata::{ErrorCategory, Metadata};

    fn ok_obj(payload: &[u8]) -> RayObject {
        RayObject::new(
            Metadata::Pickle5 {
                has_nested_refs: false,
            },
            Bytes::copy_from_slice(payload),
        )
    }

    fn err_obj() -> RayObject {
        RayObject::new(
            Metadata::Error {
                category: ErrorCategory::TaskException,
                raw_code: 3,
            },
            Bytes::from_static(b""),
        )
    }

    fn random_id() -> ObjectId {
        ObjectId::for_return(&TaskId::random(), 0)
    }

    #[test]
    fn inline_round_trip() {
        let store = MemoryStore::new();
        let id = random_id();
        store.put_inline(id, ok_obj(b"hello"));
        let entry = store.get_entry(&id).expect("present");
        match entry {
            StoredEntry::Inline(obj) => assert_eq!(obj.data.as_ref(), b"hello"),
            StoredEntry::Plasma(_) => panic!("expected inline"),
        }
        // get_inline returns it too.
        let obj = store.get_inline(&id).expect("inline");
        assert_eq!(obj.data.as_ref(), b"hello");
    }

    #[test]
    fn plasma_index_does_not_appear_as_inline() {
        let store = MemoryStore::new();
        let id = random_id();
        store.put_plasma(
            id,
            PlasmaIndex {
                metadata: Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                data_size: 10_000_000,
            },
        );
        assert!(store.contains(&id));
        // get_inline returns None for plasma-resident.
        assert!(store.get_inline(&id).is_none());
        // get_entry returns the Plasma variant.
        let entry = store.get_entry(&id).expect("present");
        assert!(matches!(entry, StoredEntry::Plasma(_)));
    }

    #[test]
    fn state_distinguishes_ready_failed_and_pending() {
        let store = MemoryStore::new();
        let ok_id = random_id();
        let err_id = random_id();
        let plasma_err_id = random_id();
        let plasma_ok_id = random_id();

        store.put_inline(ok_id, ok_obj(b"v"));
        store.put_inline(err_id, err_obj());
        store.put_plasma(
            plasma_err_id,
            PlasmaIndex {
                metadata: Metadata::Error {
                    category: ErrorCategory::WorkerDied,
                    raw_code: 0,
                },
                data_size: 0,
            },
        );
        store.put_plasma(
            plasma_ok_id,
            PlasmaIndex {
                metadata: Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                data_size: 10_000,
            },
        );

        assert_eq!(store.state_of(&ok_id), RefState::ReadyLocal);
        assert_eq!(store.state_of(&err_id), RefState::Failed);
        assert_eq!(store.state_of(&plasma_err_id), RefState::Failed);
        assert_eq!(store.state_of(&plasma_ok_id), RefState::ReadyLocal);
        assert_eq!(store.state_of(&random_id()), RefState::Pending);
    }

    #[test]
    fn get_entry_blocking_wakes_on_put() {
        let store = MemoryStore::new();
        let id = random_id();

        let s2 = Arc::clone(&store);
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            s2.put_inline(id, ok_obj(b"late"));
        });

        let entry = store
            .get_entry_blocking(&id, Some(Duration::from_secs(2)))
            .expect("must arrive");
        match entry {
            StoredEntry::Inline(obj) => assert_eq!(obj.data.as_ref(), b"late"),
            StoredEntry::Plasma(_) => panic!("expected inline"),
        }
        handle.join().expect("worker join");
    }

    #[test]
    fn get_entry_blocking_times_out() {
        let store = MemoryStore::new();
        let id = random_id();
        let res = store.get_entry_blocking(&id, Some(Duration::from_millis(20)));
        assert!(res.is_none());
    }

    #[test]
    fn wait_returns_partition() {
        let store = MemoryStore::new();
        let a = random_id();
        let b = random_id();
        let c = random_id();
        store.put_inline(a, ok_obj(b"a"));
        store.put_plasma(
            b,
            PlasmaIndex {
                metadata: Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                data_size: 1024,
            },
        );

        let out = store.wait(&[a, b, c], 2, Some(Duration::from_millis(50)));
        assert_eq!(out.ready.len(), 2);
        assert_eq!(out.not_ready, vec![c]);
    }

    #[test]
    fn wait_times_out_when_threshold_unreachable() {
        let store = MemoryStore::new();
        let a = random_id();
        let b = random_id();
        let out = store.wait(&[a, b], 2, Some(Duration::from_millis(20)));
        assert_eq!(out.ready, Vec::<ObjectId>::new());
        assert_eq!(out.not_ready.len(), 2);
    }

    #[test]
    fn delete_releases_bytes() {
        let store = MemoryStore::new();
        let id = random_id();
        store.put_inline(id, ok_obj(b"sizable"));
        assert!(store.used_memory() > 0);
        store.delete(&[id]);
        assert_eq!(store.used_memory(), 0);
        assert!(!store.contains(&id));
    }

    #[test]
    fn replace_updates_byte_count() {
        let store = MemoryStore::new();
        let id = random_id();
        store.put_inline(id, ok_obj(b"short"));
        let before = store.used_memory();
        store.put_inline(id, ok_obj(b"a much longer payload"));
        assert!(store.used_memory() > before);
    }
}
