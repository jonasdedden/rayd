//! `CoreWorker`: per-process root that owns the local index, an optional
//! plasma client, and the deterministic task-id minter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use rayd_plasma::{wire::AddressBlob, PlasmaClient, PlasmaError};
use thiserror::Error;

use crate::id::{JobId, ObjectId, TaskId, WorkerId};
use crate::metadata::Metadata;
use crate::object_ref::Address;
use crate::ray_object::RayObject;
use crate::recovery::ObjectRecoverer;
use crate::ref_counter::RefCounter;
use crate::store::{MemoryStore, PlasmaIndex, StoredEntry};

/// Default size threshold above which a value is routed through plasma.
/// 100 KiB matches Ray's `max_direct_call_object_size`.
pub const INLINE_THRESHOLD_BYTES: usize = 100 * 1024;

/// Default plasma-pressure budget the worker tracks before triggering
/// spill-on-pressure. 1 GiB matches a sensible default for a single-
/// node test box; production deployments override via env vars.
pub const DEFAULT_SPILL_BUDGET_BYTES: u64 = 1024 * 1024 * 1024;

/// Default fraction of the budget that triggers eviction.
pub const DEFAULT_SPILL_THRESHOLD: f64 = 0.75;

/// Per-worker spill-on-pressure config.
#[derive(Debug, Clone, Copy)]
pub struct SpillPolicy {
    /// Target plasma budget in bytes. The worker spills cold objects
    /// when its tracked usage rises above
    /// `(budget_bytes as f64 * threshold) as u64`.
    pub budget_bytes: u64,
    /// Fraction in `(0.0, 1.0]`. Out-of-range values are clamped at
    /// `set_spill_policy` time.
    pub threshold: f64,
}

impl Default for SpillPolicy {
    fn default() -> Self {
        Self {
            budget_bytes: DEFAULT_SPILL_BUDGET_BYTES,
            threshold: DEFAULT_SPILL_THRESHOLD,
        }
    }
}

/// Errors a `CoreWorker` can return on store/plasma operations.
#[derive(Debug, Error)]
pub enum CoreError {
    /// Underlying plasma operation failed.
    #[error("plasma: {0}")]
    Plasma(#[from] PlasmaError),
    /// Caller asked plasma to resolve an id but the worker has no plasma
    /// client configured. (Phase 1 mode.)
    #[error("object {object_id} is plasma-resident but no plasma client is connected")]
    PlasmaUnavailable {
        /// The hex of the requested id.
        object_id: String,
    },
    /// A registered `ObjectRecoverer` was consulted on plasma miss but
    /// reported a terminal failure (corrupt spill file, transient I/O
    /// that didn't self-clear, etc.).
    #[error("recovery failed for {object_id}: {reason}")]
    Recovery {
        /// The hex of the object that couldn't be recovered.
        object_id: String,
        /// Human-readable failure detail.
        reason: String,
    },
}

/// Result returned by `CoreWorker::resolve`. Carries the metadata header
/// and a (potentially-shared) `Bytes` for the data.
#[derive(Clone, Debug)]
pub struct ResolvedObject {
    /// Typed metadata header.
    pub metadata: Metadata,
    /// The data buffer; `Bytes::clone` is cheap (refcounted).
    pub data: Bytes,
    /// Currently always empty; populated once Phase 4 lands ref propagation.
    pub nested_refs: Vec<crate::ObjectRef>,
}

/// Callback invoked by the worker when it frees an object's local
/// state (memory store + plasma) after every pin has cleared.
///
/// Used by the rayd-py glue to drive owner-self-deregister at the
/// raylet's directory: once the local plasma copy is gone, the
/// raylet should also stop advertising us as a holder. See
/// `CoreWorker::set_free_callback`.
pub type FreeCallback = Arc<dyn Fn(ObjectId) + Send + Sync>;

/// The per-process root.
pub struct CoreWorker {
    job_id: JobId,
    worker_id: WorkerId,
    address: Address,
    store: Arc<MemoryStore>,
    plasma: Mutex<Option<PlasmaClient>>,
    refs: Arc<RefCounter>,
    free_callback: Mutex<Option<FreeCallback>>,
    /// Optional recovery hook consulted on plasma `NotFound` for an
    /// object the local store still believes lives in plasma.
    /// `LocalObjectManager` from rayd-raylet is the canonical impl.
    recoverer: Mutex<Option<Arc<dyn ObjectRecoverer>>>,
    /// Spill-on-pressure config. Updated by `set_spill_policy`;
    /// consulted on every `seal_value_to_plasma`.
    spill_policy: Mutex<SpillPolicy>,
    inline_threshold: usize,
    next_task_counter: AtomicU64,
}

impl std::fmt::Debug for CoreWorker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreWorker")
            .field("job_id", &self.job_id)
            .field("worker_id", &self.worker_id)
            .field("address", &self.address)
            .field("store", &self.store)
            .field("inline_threshold", &self.inline_threshold)
            .field("has_free_callback", &self.free_callback.lock().is_some())
            .finish_non_exhaustive()
    }
}

impl CoreWorker {
    /// Build a worker with no plasma client (Phase 1-style; large objects
    /// also live in the in-process memory store).
    #[must_use]
    pub fn new(job_id: JobId, worker_id: WorkerId, address: Address) -> Arc<Self> {
        Arc::new(Self {
            job_id,
            worker_id,
            address,
            store: MemoryStore::new(),
            plasma: Mutex::new(None),
            refs: Arc::new(RefCounter::new()),
            free_callback: Mutex::new(None),
            recoverer: Mutex::new(None),
            spill_policy: Mutex::new(SpillPolicy::default()),
            inline_threshold: INLINE_THRESHOLD_BYTES,
            next_task_counter: AtomicU64::new(0),
        })
    }

    /// Install (or replace) the callback fired after a successful
    /// `free_unpinned`. Used by the rayd-py glue so the raylet's
    /// directory can drop the owner's own self-entry alongside the
    /// in-store + plasma cleanup.
    pub fn set_free_callback(&self, callback: FreeCallback) {
        *self.free_callback.lock() = Some(callback);
    }

    /// Install (or replace) the recovery hook. `resolve_entry`
    /// consults it on plasma `NotFound` for an object the local
    /// store still believes lives in plasma — typically because the
    /// raylet evicted the bytes to a spill backend after the seal.
    pub fn set_recoverer(&self, recoverer: Arc<dyn ObjectRecoverer>) {
        *self.recoverer.lock() = Some(recoverer);
    }

    /// Configure spill-on-pressure. `threshold` is clamped to
    /// `(0.0, 1.0]`. Calling with `budget_bytes = u64::MAX` and
    /// `threshold = 1.0` effectively disables the policy.
    pub fn set_spill_policy(&self, budget_bytes: u64, threshold: f64) {
        let clamped = threshold.clamp(f64::MIN_POSITIVE, 1.0);
        *self.spill_policy.lock() = SpillPolicy {
            budget_bytes,
            threshold: clamped,
        };
    }

    /// Local-mode constructor: random ids, placeholder address, no plasma.
    #[must_use]
    pub fn new_local() -> Arc<Self> {
        let worker_id = WorkerId::random();
        let job_id = JobId::random();
        let address = Address::new("localhost", 0, worker_id);
        Self::new(job_id, worker_id, address)
    }

    /// Local-mode constructor that also connects to a plasma server at
    /// `plasma_socket`. Use this when your runtime starts a `PlasmaServer`.
    pub fn new_local_with_plasma<P: AsRef<std::path::Path>>(
        plasma_socket: P,
    ) -> Result<Arc<Self>, PlasmaError> {
        let worker = Self::new_local();
        let client = PlasmaClient::connect(plasma_socket)?;
        *worker.plasma.lock() = Some(client);
        Ok(worker)
    }

    /// The store backing this worker.
    #[must_use]
    pub fn store(&self) -> &Arc<MemoryStore> {
        &self.store
    }

    /// The job this worker belongs to.
    #[must_use]
    pub const fn job_id(&self) -> &JobId {
        &self.job_id
    }

    /// The worker id.
    #[must_use]
    pub const fn worker_id(&self) -> &WorkerId {
        &self.worker_id
    }

    /// Owner address embedded in `ObjectRef`s produced here.
    #[must_use]
    pub const fn address(&self) -> &Address {
        &self.address
    }

    /// Whether this worker has a plasma client connected.
    pub fn has_plasma(&self) -> bool {
        self.plasma.lock().is_some()
    }

    /// The current inline/plasma threshold in bytes.
    #[must_use]
    pub const fn inline_threshold(&self) -> usize {
        self.inline_threshold
    }

    /// Mint a fresh `TaskId` derived from `job_id` plus a monotonic counter.
    #[must_use]
    pub fn next_task_id(&self) -> TaskId {
        let counter = self.next_task_counter.fetch_add(1, Ordering::Relaxed) + 1;
        let mut bytes = [0u8; TaskId::SIZE];
        bytes[..16].copy_from_slice(self.job_id.as_bytes());
        bytes[16..].copy_from_slice(&counter.to_le_bytes());
        TaskId::from_bytes(bytes)
    }

    /// Always-plasma variant of `seal_value`. Used by worker subprocesses
    /// that need their results to land in the shared plasma store
    /// regardless of size, so the driver can ingest them via a `PlasmaIndex`.
    /// Returns the data buffer length the worker actually wrote.
    pub fn seal_value_to_plasma(
        &self,
        id: ObjectId,
        metadata: Metadata,
        data: Bytes,
    ) -> Result<u64, CoreError> {
        let mut guard = self.plasma.lock();
        let client = guard.as_mut().ok_or_else(|| CoreError::PlasmaUnavailable {
            object_id: id.hex(),
        })?;
        let address_blob = self.address_blob();
        client.create_and_seal(*id.as_bytes(), &metadata.encode(), &data, address_blob)?;
        let data_size = data.len() as u64;
        drop(guard);
        // The driver records its own `PlasmaIndex` from the dispatch
        // response; the worker's local store entry is harmless overhead.
        self.store.put_plasma(
            id,
            PlasmaIndex {
                metadata,
                data_size,
            },
        );
        self.refs.add_owned(id);
        // Spill-on-pressure: if the local store now exceeds the
        // configured threshold, evict cold objects via the recoverer.
        // No-op when no recoverer is registered or under threshold.
        self.maybe_spill_for_pressure();
        Ok(data_size)
    }

    /// Store a successful or failed value under `id`. Routes to plasma when
    /// `data.len() > inline_threshold` and a plasma client is connected.
    pub fn seal_value(
        &self,
        id: ObjectId,
        metadata: Metadata,
        data: Bytes,
    ) -> Result<(), CoreError> {
        let plasma_eligible = data.len() > self.inline_threshold;
        if plasma_eligible {
            let mut guard = self.plasma.lock();
            if let Some(client) = guard.as_mut() {
                let address_blob = self.address_blob();
                client.create_and_seal(*id.as_bytes(), &metadata.encode(), &data, address_blob)?;
                drop(guard);
                self.store.put_plasma(
                    id,
                    PlasmaIndex {
                        metadata,
                        data_size: data.len() as u64,
                    },
                );
                self.refs.add_owned(id);
                return Ok(());
            }
            // No plasma client — fall through to inline storage.
        }
        self.store.put_inline(id, RayObject::new(metadata, data));
        self.refs.add_owned(id);
        Ok(())
    }

    /// The owner-side reference counter. Lives for the worker's
    /// lifetime; cloning the `Arc` is cheap.
    #[must_use]
    pub fn refs(&self) -> &Arc<RefCounter> {
        &self.refs
    }

    /// Owner-side `ObjectRef` clone hook: bump the local count.
    /// Returns the new local count. No-op (returns 0) when the id
    /// isn't tracked — that means the ref is for an object we
    /// don't own (e.g. one we borrowed from a peer).
    pub fn inc_local_ref(&self, id: ObjectId) -> u64 {
        self.refs.inc_local(id)
    }

    /// Owner-side `ObjectRef` drop hook: decrement the local count.
    /// When that pushes the entry to fully unpinned (no borrowers,
    /// no submit-deps), free the object from local plasma + the
    /// memory store. Errors freeing from plasma are returned but
    /// the in-memory state is always cleaned regardless.
    pub fn dec_local_ref(&self, id: ObjectId) -> Result<(), CoreError> {
        if !self.refs.dec_local(id) {
            return Ok(());
        }
        self.free_unpinned(id)
    }

    /// Record that a peer worker now holds a replica of `id` (called
    /// when the peer's raylet routes a `RegisterObject` to us as the
    /// owner). Bumps the borrower set; pin survives as long as any
    /// borrower has a copy.
    pub fn add_borrower_pin(&self, id: ObjectId, borrower: WorkerId) {
        self.refs.add_borrower(id, borrower);
    }

    /// Counterpart of `add_borrower_pin`: a peer worker dropped its
    /// last `ObjectRef`. If that clears the last pin (no local refs,
    /// no other borrowers, no submit-deps), free the object.
    pub fn remove_borrower_pin(&self, id: ObjectId, borrower: WorkerId) -> Result<(), CoreError> {
        if !self.refs.remove_borrower(id, borrower) {
            return Ok(());
        }
        self.free_unpinned(id)
    }

    /// Spill `id` out of plasma into the registered recoverer.
    ///
    /// Reads the bytes from local plasma, hands them to the
    /// recoverer's `store` hook, then deletes the plasma copy. The
    /// `MemoryStore` index entry is preserved so subsequent
    /// `resolve_entry` calls still see `StoredEntry::Plasma` and
    /// trigger the recover-and-reseal path.
    ///
    /// Returns `Ok(true)` if a spill happened, `Ok(false)` if the
    /// object wasn't in plasma to begin with (idempotent), or an
    /// error if no recoverer is registered or any step failed.
    pub fn spill_to_recoverer(&self, id: ObjectId) -> Result<bool, CoreError> {
        let recoverer = self.recoverer.lock().clone();
        let Some(recoverer) = recoverer else {
            return Err(CoreError::Recovery {
                object_id: id.hex(),
                reason: "no recoverer registered".to_owned(),
            });
        };

        // 1) Read bytes out of plasma. None → nothing to spill.
        let (metadata, data) = {
            let mut guard = self.plasma.lock();
            let Some(client) = guard.as_mut() else {
                return Err(CoreError::PlasmaUnavailable {
                    object_id: id.hex(),
                });
            };
            match client.get(*id.as_bytes()) {
                Ok(handle) => {
                    let metadata = Bytes::copy_from_slice(handle.metadata());
                    let data = Bytes::copy_from_slice(handle.data());
                    (metadata, data)
                }
                Err(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::NotFound,
                    ..
                }) => return Ok(false),
                Err(other) => return Err(other.into()),
            }
        };

        // 2) Hand to the recoverer. Translate a recovery failure
        //    into a CoreError::Recovery so the caller can surface
        //    it; the plasma copy is still intact at this point.
        if let Err(e) = recoverer.store(id, metadata, data) {
            return Err(CoreError::Recovery {
                object_id: id.hex(),
                reason: e.to_string(),
            });
        }

        // 3) Delete from plasma. Leave the local store index alone
        //    so later resolves trigger recover-and-reseal. NotFound
        //    is benign (raced with another evict).
        let mut guard = self.plasma.lock();
        if let Some(client) = guard.as_mut() {
            match client.delete(*id.as_bytes()) {
                Ok(())
                | Err(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::NotFound,
                    ..
                }) => {}
                Err(other) => return Err(other.into()),
            }
        }
        Ok(true)
    }

    /// Spill cold objects until plasma usage drops back under the
    /// configured threshold. Returns the number of successful spills.
    ///
    /// Walks the local store's plasma entries in arbitrary order
    /// (`HashMap` iteration), spilling via `spill_to_recoverer` and
    /// decrementing an estimated remaining-bytes counter. Stops as
    /// soon as the estimate drops below `threshold * budget`.
    ///
    /// No-op when no recoverer is registered, when usage is already
    /// under threshold, or when no plasma entries exist. Errors from
    /// individual spill attempts are swallowed — a failed eviction
    /// just means the next seal will try again.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation,
    )]
    pub fn maybe_spill_for_pressure(&self) -> usize {
        let policy = *self.spill_policy.lock();
        // 1.0 ratio + u64::MAX budget effectively disables. Avoid
        // doing the work in that case. Casts are bounded:
        // `policy.threshold` is clamped to (0, 1] in `set_spill_policy`,
        // so the product stays within `[0, budget_bytes]` and fits in a u64.
        let threshold_bytes = (policy.budget_bytes as f64 * policy.threshold) as u64;
        if threshold_bytes == u64::MAX {
            return 0;
        }
        let entries = self.store.plasma_entries();
        let mut current: u64 = entries.iter().map(|(_, idx)| idx.data_size).sum();
        if current <= threshold_bytes {
            return 0;
        }
        let mut spilled = 0;
        for (id, idx) in entries {
            if current <= threshold_bytes {
                break;
            }
            match self.spill_to_recoverer(id) {
                Ok(true) => {
                    current = current.saturating_sub(idx.data_size);
                    spilled += 1;
                }
                Ok(false) => {
                    // Already gone from plasma — somebody beat us.
                    // Subtract since the bytes are presumed freed.
                    current = current.saturating_sub(idx.data_size);
                }
                Err(_) => {
                    // Skip transient failures; the next seal will retry.
                }
            }
        }
        spilled
    }

    /// Public twin of `free_unpinned` for tests / lineage:
    /// drop the local memory store entry AND remove from plasma,
    /// without touching the reference counter or firing the
    /// free-callback. Used to simulate object loss between gets.
    pub fn evict_local(&self, id: ObjectId) -> Result<(), CoreError> {
        self.store.delete(&[id]);
        let mut guard = self.plasma.lock();
        if let Some(client) = guard.as_mut() {
            match client.delete(*id.as_bytes()) {
                Ok(())
                | Err(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::NotFound,
                    ..
                }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    /// Free `id` from the local memory store and (if present) plasma.
    /// Used by the unpin path; safe to call independently when a
    /// caller has externally decided the object is dead.
    fn free_unpinned(&self, id: ObjectId) -> Result<(), CoreError> {
        // Drop the local index first so concurrent readers see "gone"
        // before plasma starts unmapping.
        self.store.delete(&[id]);
        let mut guard = self.plasma.lock();
        if let Some(client) = guard.as_mut() {
            // PlasmaError::Server { kind: NotFound, .. } is fine — we
            // may be cleaning up an inline-only object whose seal
            // never reached plasma.
            match client.delete(*id.as_bytes()) {
                Ok(())
                | Err(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::NotFound,
                    ..
                }) => {}
                Err(e) => return Err(e.into()),
            }
        }
        // Drop the plasma lock before invoking the callback to avoid
        // surprising re-entrancy. Clone the Arc so the callback can
        // outlive a concurrent `set_free_callback` swap.
        drop(guard);
        let callback = self.free_callback.lock().clone();
        if let Some(cb) = callback {
            cb(id);
        }
        Ok(())
    }

    /// Block until `id` resolves, then return the metadata + data.
    pub fn resolve_blocking(
        &self,
        id: &ObjectId,
        timeout: Option<Duration>,
    ) -> Result<Option<ResolvedObject>, CoreError> {
        let Some(entry) = self.store.get_entry_blocking(id, timeout) else {
            return Ok(None);
        };
        Ok(Some(self.resolve_entry(id, &entry)?))
    }

    /// Non-blocking resolve.
    pub fn resolve_now(&self, id: &ObjectId) -> Result<Option<ResolvedObject>, CoreError> {
        let Some(entry) = self.store.get_entry(id) else {
            return Ok(None);
        };
        Ok(Some(self.resolve_entry(id, &entry)?))
    }

    fn resolve_entry(
        &self,
        id: &ObjectId,
        entry: &StoredEntry,
    ) -> Result<ResolvedObject, CoreError> {
        match entry {
            StoredEntry::Inline(obj) => Ok(ResolvedObject {
                metadata: obj.metadata,
                data: obj.data.clone(),
                nested_refs: obj.nested_refs.clone(),
            }),
            StoredEntry::Plasma(idx) => {
                // Fast path: plasma still has the bytes.
                {
                    let mut guard = self.plasma.lock();
                    let Some(client) = guard.as_mut() else {
                        return Err(CoreError::PlasmaUnavailable {
                            object_id: id.hex(),
                        });
                    };
                    match client.get(*id.as_bytes()) {
                        Ok(handle) => {
                            let data = Bytes::copy_from_slice(handle.data());
                            return Ok(ResolvedObject {
                                metadata: idx.metadata,
                                data,
                                nested_refs: Vec::new(),
                            });
                        }
                        Err(PlasmaError::Server {
                            kind: rayd_plasma::ServerErrorKind::NotFound,
                            ..
                        }) => {
                            // fall through to the recoverer
                        }
                        Err(other) => return Err(other.into()),
                    }
                }

                // Plasma miss. The local store still has the index
                // entry, so somebody (most likely the spill-on-pressure
                // policy) evicted the bytes out from under us. Try
                // the registered recoverer.
                self.recover_and_reseal(id, idx)
            }
        }
    }

    /// Helper for `resolve_entry`: consult the recoverer, reseal into
    /// plasma, return the resolved object. Splits out so the
    /// happy-path read in `resolve_entry` stays compact.
    ///
    /// We trust that a successful `create_and_seal` (treating
    /// `AlreadyExists` as benign) means the bytes are now in plasma,
    /// and we return the recovered bytes directly without re-opening
    /// a `ReadHandle` to verify. The raylet's `plasma_get_with_restore`
    /// re-opens the handle as a backstop on the cross-node `Pull`
    /// path, so a stale-view bug there would surface as a miss rather
    /// than a corruption.
    fn recover_and_reseal(
        &self,
        id: &ObjectId,
        idx: &PlasmaIndex,
    ) -> Result<ResolvedObject, CoreError> {
        let recoverer = self.recoverer.lock().clone();
        let Some(recoverer) = recoverer else {
            // No recoverer configured — surface the plasma miss as-is.
            return Err(CoreError::Plasma(PlasmaError::Server {
                kind: rayd_plasma::ServerErrorKind::NotFound,
                message: format!(
                    "object {} not in plasma and no recoverer registered",
                    id.hex()
                ),
            }));
        };
        let bytes = match recoverer.recover(*id) {
            Ok(Some(r)) => r,
            Ok(None) => {
                return Err(CoreError::Plasma(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::NotFound,
                    message: format!(
                        "object {} not in plasma and not held by any recoverer",
                        id.hex()
                    ),
                }));
            }
            Err(crate::recovery::RecoveryError::Other(msg)) => {
                return Err(CoreError::Recovery {
                    object_id: id.hex(),
                    reason: msg,
                });
            }
        };

        // Reseal into plasma. `AlreadyExists` is benign — somebody
        // raced and beat us; the bytes are present either way.
        {
            let address_blob = self.address_blob();
            let mut guard = self.plasma.lock();
            let Some(client) = guard.as_mut() else {
                return Err(CoreError::PlasmaUnavailable {
                    object_id: id.hex(),
                });
            };
            match client.create_and_seal(
                *id.as_bytes(),
                &bytes.metadata,
                &bytes.data,
                address_blob,
            ) {
                Ok(())
                | Err(PlasmaError::Server {
                    kind: rayd_plasma::ServerErrorKind::AlreadyExists,
                    ..
                }) => {}
                Err(other) => return Err(other.into()),
            }
        }

        Ok(ResolvedObject {
            metadata: idx.metadata,
            data: bytes.data,
            nested_refs: Vec::new(),
        })
    }

    fn address_blob(&self) -> AddressBlob {
        AddressBlob::new(
            self.address.host.clone(),
            self.address.port,
            *self.address.worker_id.as_bytes(),
        )
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rayd_plasma::PlasmaServer;
    use tempfile::TempDir;

    use super::*;
    use crate::metadata::ErrorCategory;

    #[test]
    fn task_ids_are_monotonic_and_share_job_prefix() {
        let worker = CoreWorker::new_local();
        let a = worker.next_task_id();
        let b = worker.next_task_id();
        let c = worker.next_task_id();

        assert_eq!(&a.as_bytes()[..16], worker.job_id().as_bytes());
        assert_eq!(&b.as_bytes()[..16], worker.job_id().as_bytes());
        assert_eq!(&c.as_bytes()[..16], worker.job_id().as_bytes());

        assert_eq!(
            u64::from_le_bytes(a.as_bytes()[16..].try_into().expect("8 bytes")),
            1
        );
        assert_eq!(
            u64::from_le_bytes(b.as_bytes()[16..].try_into().expect("8 bytes")),
            2
        );
        assert_eq!(
            u64::from_le_bytes(c.as_bytes()[16..].try_into().expect("8 bytes")),
            3
        );
    }

    #[test]
    fn store_starts_empty() {
        let worker = CoreWorker::new_local();
        assert!(worker.store().is_empty());
    }

    #[test]
    fn seal_below_threshold_goes_inline_when_plasma_present() {
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("plasma.sock");
        let _server = PlasmaServer::start(socket.clone(), 1 << 20).unwrap();
        let worker = CoreWorker::new_local_with_plasma(&socket).unwrap();

        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from_static(b"small"),
            )
            .unwrap();
        // Inline path: get_inline succeeds.
        assert!(worker.store().get_inline(&id).is_some());
    }

    #[test]
    fn seal_above_threshold_routes_to_plasma() {
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("plasma.sock");
        let _server = PlasmaServer::start(socket.clone(), 1 << 22).unwrap();
        let worker = CoreWorker::new_local_with_plasma(&socket).unwrap();

        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        let big = vec![0xABu8; INLINE_THRESHOLD_BYTES * 2];
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from(big.clone()),
            )
            .unwrap();
        // Plasma path: get_inline returns None, but get_entry returns Plasma.
        assert!(worker.store().get_inline(&id).is_none());
        let entry = worker.store().get_entry(&id).expect("present");
        assert!(matches!(entry, StoredEntry::Plasma(_)));

        // resolve_blocking fetches from plasma and returns the bytes.
        let resolved = worker
            .resolve_blocking(&id, Some(Duration::from_secs(1)))
            .unwrap()
            .expect("resolved");
        assert_eq!(resolved.data.as_ref(), big.as_slice());
    }

    #[test]
    fn no_plasma_falls_back_to_inline_for_large() {
        let worker = CoreWorker::new_local();
        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        let big = vec![1u8; INLINE_THRESHOLD_BYTES * 2];
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from(big),
            )
            .unwrap();
        // Without a plasma client we keep it inline regardless of size.
        assert!(worker.store().get_inline(&id).is_some());
    }

    #[test]
    fn error_object_round_trips_via_plasma() {
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("plasma.sock");
        let _server = PlasmaServer::start(socket.clone(), 1 << 22).unwrap();
        let worker = CoreWorker::new_local_with_plasma(&socket).unwrap();

        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        let payload = vec![0u8; INLINE_THRESHOLD_BYTES * 2];
        worker
            .seal_value(
                id,
                Metadata::Error {
                    category: ErrorCategory::TaskException,
                    raw_code: 7,
                },
                Bytes::from(payload.clone()),
            )
            .unwrap();
        assert_eq!(
            worker.store().state_of(&id),
            crate::metadata::RefState::Failed
        );
        let resolved = worker
            .resolve_blocking(&id, Some(Duration::from_secs(1)))
            .unwrap()
            .expect("resolved");
        match resolved.metadata {
            Metadata::Error { category, raw_code } => {
                assert_eq!(category, ErrorCategory::TaskException);
                assert_eq!(raw_code, 7);
            }
            _ => panic!("expected Error metadata"),
        }
        assert_eq!(resolved.data.as_ref(), payload.as_slice());
    }

    // ── Phase 4.2: ref-counted lifecycle ──────────────────────────────

    #[test]
    fn seal_value_registers_owned_entry_in_ref_counter() {
        let worker = CoreWorker::new_local();
        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from_static(b"x"),
            )
            .unwrap();
        let entry = worker.refs().snapshot(id).expect("tracked");
        assert_eq!(entry.local_count, 1);
    }

    #[test]
    fn dec_local_ref_unpins_inline_object_and_clears_store() {
        let worker = CoreWorker::new_local();
        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from_static(b"x"),
            )
            .unwrap();
        assert!(worker.store().get_entry(&id).is_some());
        worker.dec_local_ref(id).unwrap();
        assert!(worker.store().get_entry(&id).is_none());
        assert!(worker.refs().snapshot(id).is_none());
    }

    #[test]
    fn dec_local_ref_frees_plasma_object() {
        let dir = TempDir::new().unwrap();
        let socket = dir.path().join("plasma.sock");
        let _server = PlasmaServer::start(socket.clone(), 1 << 22).unwrap();
        let worker = CoreWorker::new_local_with_plasma(&socket).unwrap();

        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        let big = vec![0xABu8; INLINE_THRESHOLD_BYTES * 2];
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from(big),
            )
            .unwrap();

        // Confirm it's in plasma.
        let mut probe = PlasmaClient::connect(&socket).unwrap();
        let contains_before = probe.contains(*id.as_bytes()).unwrap();
        assert!(contains_before.present);

        worker.dec_local_ref(id).unwrap();

        let contains_after = probe.contains(*id.as_bytes()).unwrap();
        assert!(!contains_after.present);
        assert!(worker.refs().snapshot(id).is_none());
    }

    #[test]
    fn clone_inc_then_two_decs_to_free() {
        let worker = CoreWorker::new_local();
        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from_static(b"x"),
            )
            .unwrap();
        assert_eq!(worker.inc_local_ref(id), 2); // simulated clone
                                                 // First drop: still 1 outstanding ref → object stays.
        worker.dec_local_ref(id).unwrap();
        assert!(worker.store().get_entry(&id).is_some());
        // Second drop: now unpinned.
        worker.dec_local_ref(id).unwrap();
        assert!(worker.store().get_entry(&id).is_none());
    }

    #[test]
    fn dec_local_ref_does_not_free_while_borrower_holds() {
        let worker = CoreWorker::new_local();
        let id = ObjectId::for_return(&worker.next_task_id(), 0);
        worker
            .seal_value(
                id,
                Metadata::Pickle5 {
                    has_nested_refs: false,
                },
                Bytes::from_static(b"x"),
            )
            .unwrap();
        let borrower = WorkerId::random();
        worker.refs().add_borrower(id, borrower);

        worker.dec_local_ref(id).unwrap();
        // Borrower still holds — object survives.
        assert!(worker.store().get_entry(&id).is_some());
        assert!(worker.refs().snapshot(id).is_some());

        worker.refs().remove_borrower(id, borrower);
        // After borrower drops, the entry is unpinned but `dec_local_ref`
        // is the path that does the free; remove_borrower returns true
        // (unpinned) but doesn't itself wipe the store. For now, the
        // owner notices on the next dec/snapshot — we'll wire this up
        // when the borrower-side RPC lands in 4.3.
        // (Test invariant: the entry is gone from the ref counter, even
        // if the store still has the bytes.)
        assert!(worker.refs().snapshot(id).is_none());
    }
}
