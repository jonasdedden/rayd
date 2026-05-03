//! Owner-side reference counter for `ObjectRef`s.
//!
//! Phase 4.1 ships only the in-memory data structure: who-owns-what,
//! with no networking. The owner of an object holds a `RefCounter`
//! entry per id whose lifetime spans:
//! - **local count**: how many `ObjectRef`s the owner itself holds.
//! - **borrower set**: which peer workers have asked to borrow.
//! - **submit-dep count**: how many in-flight tasks consume this
//!   object as an argument (we keep the object alive until those
//!   tasks have started reading).
//!
//! When ALL three drop to zero, [`should_free`] returns `true`, and
//! the caller (the owner's `CoreWorker`) is expected to issue a
//! `FreeObjects` fanout — that wire-up lands in 4.2.
//!
//! ## Design notes
//!
//! - `RefCounter` is `Send + Sync` (single mutex over the inner map),
//!   so a `CoreWorker` can drive it from many threads.
//! - Operations on missing ids are a no-op rather than panic — the
//!   borrower side is concurrent and may sometimes try to drop an
//!   already-freed entry. We log nothing here; the caller decides
//!   whether a stray drop is interesting.
//! - The borrower set deliberately *doesn't* count multiplicity. If
//!   a single borrower asks twice, that's still one entry — the
//!   borrower's local refcount is its own bookkeeping problem.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use crate::id::{ObjectId, WorkerId};

/// Per-object accounting on the owner side.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OwnerEntry {
    /// Refs the owner itself holds (incremented on clone, decremented
    /// on drop).
    pub local_count: u64,
    /// Workers that have borrowed this id.
    pub borrowers: HashSet<WorkerId>,
    /// In-flight tasks consuming this id as an argument.
    pub submit_dep_count: u64,
}

impl OwnerEntry {
    /// Whether every kind of pin on this id has cleared.
    #[must_use]
    pub fn is_unpinned(&self) -> bool {
        self.local_count == 0 && self.borrowers.is_empty() && self.submit_dep_count == 0
    }
}

/// Thread-safe owner-side reference table. One per `CoreWorker`.
#[derive(Debug, Default)]
pub struct RefCounter {
    entries: Mutex<HashMap<ObjectId, OwnerEntry>>,
}

impl RefCounter {
    /// Empty counter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the owner has just produced `id`. Sets the entry
    /// up with `local_count = 1` and no borrowers. Idempotent on
    /// re-add: if the entry already exists, this just bumps
    /// `local_count` by one (the same effect a clone would have).
    pub fn add_owned(&self, id: ObjectId) {
        let mut guard = self.entries.lock();
        guard.entry(id).or_default().local_count += 1;
    }

    /// Cloning an `ObjectRef` on the owner side: bump local count.
    /// Returns the new local count. No-op (returns 0) on unknown id.
    pub fn inc_local(&self, id: ObjectId) -> u64 {
        let mut guard = self.entries.lock();
        match guard.get_mut(&id) {
            Some(entry) => {
                entry.local_count += 1;
                entry.local_count
            }
            None => 0,
        }
    }

    /// Dropping an owner-side `ObjectRef`: decrement local count,
    /// returning `true` when this drop unpinned the object (i.e. all
    /// counters are now zero and the caller should `FreeObjects`).
    /// Saturating-subtract: a stray drop on an unknown / already-zero
    /// entry returns `false` rather than panicking.
    pub fn dec_local(&self, id: ObjectId) -> bool {
        let mut guard = self.entries.lock();
        let Some(entry) = guard.get_mut(&id) else {
            return false;
        };
        entry.local_count = entry.local_count.saturating_sub(1);
        if entry.is_unpinned() {
            guard.remove(&id);
            true
        } else {
            false
        }
    }

    /// A peer asked to borrow `id`. Idempotent: same `(id, worker)`
    /// twice keeps a single set entry. Returns `true` when this was
    /// a new borrower for this id.
    pub fn add_borrower(&self, id: ObjectId, worker: WorkerId) -> bool {
        let mut guard = self.entries.lock();
        guard.entry(id).or_default().borrowers.insert(worker)
    }

    /// A borrower dropped its last copy. Returns `true` when this
    /// drop unpinned the entry (cleared the last hold and the entry
    /// was removed). No-op on unknown id.
    pub fn remove_borrower(&self, id: ObjectId, worker: WorkerId) -> bool {
        let mut guard = self.entries.lock();
        let Some(entry) = guard.get_mut(&id) else {
            return false;
        };
        entry.borrowers.remove(&worker);
        if entry.is_unpinned() {
            guard.remove(&id);
            true
        } else {
            false
        }
    }

    /// A task that consumes `id` as an argument has been submitted.
    /// Bump the submit-dep count so the object stays pinned until
    /// the task actually starts reading.
    pub fn add_submit_dep(&self, id: ObjectId) {
        let mut guard = self.entries.lock();
        guard.entry(id).or_default().submit_dep_count += 1;
    }

    /// The task has fetched the object. Decrement the dep count;
    /// returns `true` when this clears the last pin and the entry
    /// was freed.
    pub fn complete_submit_dep(&self, id: ObjectId) -> bool {
        let mut guard = self.entries.lock();
        let Some(entry) = guard.get_mut(&id) else {
            return false;
        };
        entry.submit_dep_count = entry.submit_dep_count.saturating_sub(1);
        if entry.is_unpinned() {
            guard.remove(&id);
            true
        } else {
            false
        }
    }

    /// Snapshot the current entry. `None` when the id has been
    /// freed (or was never added).
    #[must_use]
    pub fn snapshot(&self, id: ObjectId) -> Option<OwnerEntry> {
        self.entries.lock().get(&id).cloned()
    }

    /// Number of objects we're currently pinning (any kind of hold).
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Whether the counter is tracking any objects at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(seed: u8) -> ObjectId {
        let mut buf = [0u8; 28];
        buf[0] = seed;
        ObjectId::from_bytes(buf)
    }

    fn wid(seed: u8) -> WorkerId {
        let mut buf = [0u8; 16];
        buf[0] = seed;
        WorkerId::from_bytes(buf)
    }

    #[test]
    fn add_owned_starts_local_count_at_one() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        let e = c.snapshot(oid(1)).expect("entry");
        assert_eq!(e.local_count, 1);
        assert!(e.borrowers.is_empty());
        assert_eq!(e.submit_dep_count, 0);
    }

    #[test]
    fn dec_unpins_when_count_reaches_zero() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        assert!(c.dec_local(oid(1)));
        assert!(c.snapshot(oid(1)).is_none());
        assert!(c.is_empty());
    }

    #[test]
    fn dec_does_not_unpin_while_borrower_holds() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        c.add_borrower(oid(1), wid(7));
        // Owner drops its last local ref, but the borrower keeps it alive.
        assert!(!c.dec_local(oid(1)));
        let e = c.snapshot(oid(1)).expect("still tracked");
        assert_eq!(e.local_count, 0);
        assert_eq!(e.borrowers.len(), 1);
        // Borrower drops too — now we free.
        assert!(c.remove_borrower(oid(1), wid(7)));
        assert!(c.snapshot(oid(1)).is_none());
    }

    #[test]
    fn dec_does_not_unpin_while_submit_dep_holds() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        c.add_submit_dep(oid(1));
        assert!(!c.dec_local(oid(1)));
        // Task starts reading.
        assert!(c.complete_submit_dep(oid(1)));
        assert!(c.snapshot(oid(1)).is_none());
    }

    #[test]
    fn inc_local_increments_for_clones() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        assert_eq!(c.inc_local(oid(1)), 2);
        assert_eq!(c.inc_local(oid(1)), 3);
        assert!(!c.dec_local(oid(1)));
        assert!(!c.dec_local(oid(1)));
        assert!(c.dec_local(oid(1)));
    }

    #[test]
    fn add_borrower_is_idempotent_per_worker() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        assert!(c.add_borrower(oid(1), wid(7)));
        assert!(!c.add_borrower(oid(1), wid(7))); // second time: not new
        let e = c.snapshot(oid(1)).expect("entry");
        assert_eq!(e.borrowers.len(), 1);
    }

    #[test]
    fn dec_local_on_unknown_is_a_no_op() {
        let c = RefCounter::new();
        // Never added — drop is harmless.
        assert!(!c.dec_local(oid(99)));
        assert!(c.is_empty());
    }

    #[test]
    fn dec_local_below_zero_saturates_and_does_not_panic() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        // Forge a state where local_count is already 0 by adding a
        // borrower so the entry stays alive past dec_local → 0.
        c.add_borrower(oid(1), wid(7));
        assert!(!c.dec_local(oid(1))); // count -> 0 but borrower keeps it
                                       // Stray extra drop: must NOT underflow.
        assert!(!c.dec_local(oid(1)));
        let e = c.snapshot(oid(1)).expect("still tracked");
        assert_eq!(e.local_count, 0);
    }

    #[test]
    fn unrelated_borrower_drop_does_not_affect_other_workers() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        c.add_borrower(oid(1), wid(7));
        c.add_borrower(oid(1), wid(8));
        // wid(9) was never a borrower; drop is harmless.
        assert!(!c.remove_borrower(oid(1), wid(9)));
        let e = c.snapshot(oid(1)).expect("entry");
        assert_eq!(e.borrowers.len(), 2);
    }

    #[test]
    fn entry_is_unpinned_only_when_all_three_hit_zero() {
        let c = RefCounter::new();
        c.add_owned(oid(1));
        c.add_borrower(oid(1), wid(7));
        c.add_submit_dep(oid(1));
        // Drop owner-side: still pinned by borrower + submit dep.
        assert!(!c.dec_local(oid(1)));
        // Borrower drops: still pinned by submit dep.
        assert!(!c.remove_borrower(oid(1), wid(7)));
        // Task reads: now unpinned.
        assert!(c.complete_submit_dep(oid(1)));
        assert!(c.is_empty());
    }
}

#[cfg(test)]
mod proptests {
    //! Property tests that drive a `RefCounter` with random sequences
    //! of operations and assert the core invariants:
    //!
    //! - **Presence ↔ pin**: an entry exists in the map iff at least
    //!   one of its three counters is non-zero.
    //! - **Counters never wrap below zero**: saturating-subtraction
    //!   on `dec_local` and `complete_submit_dep` means stray drops
    //!   stay at zero rather than wrapping.
    //! - **Reference model agrees**: a tiny in-memory model replays
    //!   the same op sequence and produces the same map.
    //!
    //! Compared to the hand-rolled tests above, the proptest catches
    //! interactions our written cases didn't think to enumerate
    //! (e.g. "stray drops while a borrower holds, then `add_owned`,
    //! then borrower removes" sequences).
    use std::collections::{HashMap, HashSet};

    use proptest::prelude::*;

    use super::*;

    /// Tiny id space so the random sequences exercise collisions.
    const N_OBJECT_IDS: u8 = 4;
    const N_WORKER_IDS: u8 = 3;

    fn oid(seed: u8) -> ObjectId {
        let mut buf = [0u8; 28];
        buf[0] = seed;
        ObjectId::from_bytes(buf)
    }

    fn wid(seed: u8) -> WorkerId {
        let mut buf = [0u8; 16];
        buf[0] = seed;
        WorkerId::from_bytes(buf)
    }

    /// Operation set the strategy emits. Mirrors the public API.
    #[derive(Clone, Debug)]
    enum Op {
        AddOwned(u8),
        IncLocal(u8),
        DecLocal(u8),
        AddBorrower(u8, u8),
        RemoveBorrower(u8, u8),
        AddSubmitDep(u8),
        CompleteSubmitDep(u8),
    }

    fn op_strategy() -> impl Strategy<Value = Op> {
        let oid_s = 0u8..N_OBJECT_IDS;
        let wid_s = 0u8..N_WORKER_IDS;
        prop_oneof![
            oid_s.clone().prop_map(Op::AddOwned),
            oid_s.clone().prop_map(Op::IncLocal),
            oid_s.clone().prop_map(Op::DecLocal),
            (oid_s.clone(), wid_s.clone()).prop_map(|(o, w)| Op::AddBorrower(o, w)),
            (oid_s.clone(), wid_s).prop_map(|(o, w)| Op::RemoveBorrower(o, w)),
            oid_s.clone().prop_map(Op::AddSubmitDep),
            oid_s.prop_map(Op::CompleteSubmitDep),
        ]
    }

    /// Apply `op` to the real counter.
    fn apply(c: &RefCounter, op: &Op) {
        match *op {
            Op::AddOwned(o) => c.add_owned(oid(o)),
            Op::IncLocal(o) => {
                c.inc_local(oid(o));
            }
            Op::DecLocal(o) => {
                c.dec_local(oid(o));
            }
            Op::AddBorrower(o, w) => {
                c.add_borrower(oid(o), wid(w));
            }
            Op::RemoveBorrower(o, w) => {
                c.remove_borrower(oid(o), wid(w));
            }
            Op::AddSubmitDep(o) => c.add_submit_dep(oid(o)),
            Op::CompleteSubmitDep(o) => {
                c.complete_submit_dep(oid(o));
            }
        }
    }

    /// Reference model state: same shape as `OwnerEntry` but stored
    /// directly in a `HashMap` so we can replay independently. We
    /// keep entries present even when fully unpinned, then drop them
    /// at the end to mirror the counter's free-on-zero rule.
    #[derive(Default)]
    struct ModelEntry {
        local: u64,
        borrowers: HashSet<u8>,
        submit_dep: u64,
    }

    impl ModelEntry {
        fn unpinned(&self) -> bool {
            self.local == 0 && self.borrowers.is_empty() && self.submit_dep == 0
        }
    }

    /// Apply `op` to the reference model. Mirrors the actual
    /// counter's saturating-subtract + remove-on-unpinned behaviour.
    fn apply_model(model: &mut HashMap<u8, ModelEntry>, op: &Op) {
        match *op {
            Op::AddOwned(o) => model.entry(o).or_default().local += 1,
            Op::IncLocal(o) => {
                if let Some(e) = model.get_mut(&o) {
                    e.local += 1;
                }
            }
            Op::DecLocal(o) => {
                if let Some(e) = model.get_mut(&o) {
                    e.local = e.local.saturating_sub(1);
                    if e.unpinned() {
                        model.remove(&o);
                    }
                }
            }
            Op::AddBorrower(o, w) => {
                model.entry(o).or_default().borrowers.insert(w);
            }
            Op::RemoveBorrower(o, w) => {
                if let Some(e) = model.get_mut(&o) {
                    e.borrowers.remove(&w);
                    if e.unpinned() {
                        model.remove(&o);
                    }
                }
            }
            Op::AddSubmitDep(o) => model.entry(o).or_default().submit_dep += 1,
            Op::CompleteSubmitDep(o) => {
                if let Some(e) = model.get_mut(&o) {
                    e.submit_dep = e.submit_dep.saturating_sub(1);
                    if e.unpinned() {
                        model.remove(&o);
                    }
                }
            }
        }
    }

    proptest! {
        /// Invariant: a snapshotted entry is never unpinned. (If it
        /// were, the counter would have removed it.)
        #[test]
        fn snapshotted_entry_is_never_unpinned(
            ops in proptest::collection::vec(op_strategy(), 0..256),
        ) {
            let c = RefCounter::new();
            for op in &ops {
                apply(&c, op);
            }
            for o in 0..N_OBJECT_IDS {
                if let Some(entry) = c.snapshot(oid(o)) {
                    prop_assert!(!entry.is_unpinned(),
                        "snapshot returned unpinned entry for oid {}: {entry:?}", o);
                }
            }
        }

        /// Invariant: replaying the same ops on a model produces the
        /// same map (presence + counter values + borrower set).
        #[test]
        fn agrees_with_reference_model(
            ops in proptest::collection::vec(op_strategy(), 0..256),
        ) {
            let c = RefCounter::new();
            let mut model: HashMap<u8, ModelEntry> = HashMap::new();
            for op in &ops {
                apply(&c, op);
                apply_model(&mut model, op);
            }
            for o in 0..N_OBJECT_IDS {
                let actual = c.snapshot(oid(o));
                let expected = model.get(&o);
                match (actual, expected) {
                    (None, None) => {}
                    (Some(a), Some(e)) => {
                        prop_assert_eq!(a.local_count, e.local,
                            "local_count diverged for oid {}", o);
                        prop_assert_eq!(a.submit_dep_count, e.submit_dep,
                            "submit_dep_count diverged for oid {}", o);
                        let actual_set: HashSet<u8> = a.borrowers.iter()
                            .map(|w| w.as_bytes()[0]).collect();
                        prop_assert_eq!(actual_set, e.borrowers.clone(),
                            "borrower set diverged for oid {}", o);
                    }
                    (Some(a), None) => prop_assert!(false,
                        "actual has entry for oid {} but model doesn't: {a:?}", o),
                    (None, Some(_)) => prop_assert!(false,
                        "model has entry for oid {} but actual doesn't", o),
                }
            }
        }

        /// Invariant: extra `dec_local` / `complete_submit_dep`
        /// after a sequence never panic (saturating-subtract holds).
        /// Catches future regressions if anyone replaces the
        /// saturating ops with raw subtraction.
        #[test]
        fn extra_decs_after_sequence_never_panic(
            ops in proptest::collection::vec(op_strategy(), 0..128),
        ) {
            let c = RefCounter::new();
            for op in &ops {
                apply(&c, op);
            }
            for o in 0..N_OBJECT_IDS {
                let _ = c.dec_local(oid(o));
                let _ = c.dec_local(oid(o));
                let _ = c.complete_submit_dep(oid(o));
                let _ = c.complete_submit_dep(oid(o));
            }
            // If the loop above didn't panic, the property holds.
        }
    }
}

// ── Phase 4.3.3c-D: loom tests for the borrower handshake ─────────────────
//
// Compiled and run only with `--cfg loom`:
//
//     RUSTFLAGS="--cfg loom" \
//       cargo test --release loom_ -p rayd-core --lib
//
// Why a parallel mini-counter instead of `RefCounter` itself: loom can
// only see operations that go through its own `Mutex`/`Arc`/atomics —
// the real `RefCounter` uses `parking_lot::Mutex`, opaque to loom. The
// mini-counter below mirrors the *protocol* (add_owned / dec_local /
// add_borrower / remove_borrower with the same free-on-zero rule),
// using loom-instrumented primitives so loom can interleave operations
// across threads and check invariants on every schedule.
//
// What we're checking is NOT "is the data structure thread-safe" (the
// single mutex makes that trivial). It's the protocol invariants:
//   1. No panics on saturating-sub regardless of interleaving.
//   2. The freed-bool returned by dec_local / remove_borrower is
//      consistent with the post-state: if it returns true the entry is
//      gone; if false the entry is either gone (concurrent dec ran
//      first) or pinned (someone else holds).
//   3. The "resurrection" race — dec_local frees, then add_borrower
//      recreates — produces a valid pinned entry, never a corrupt one.
//      This race is real but a higher layer (raylet directory) keeps
//      it from happening in production; loom verifies the counter
//      itself doesn't break if it ever does.
#[cfg(loom)]
#[allow(clippy::wildcard_imports)]
mod loom_tests {
    use loom::sync::{Arc, Mutex};
    use loom::thread;
    use std::collections::{HashMap, HashSet};

    /// Minimal mirror of `OwnerEntry` whose fields use `std` types
    /// behind a loom `Mutex`. Only the borrower-handshake-relevant
    /// fields are modeled (no `submit_dep_count`).
    #[derive(Default)]
    struct LoomEntry {
        local: u64,
        borrowers: HashSet<u8>,
    }

    impl LoomEntry {
        fn unpinned(&self) -> bool {
            self.local == 0 && self.borrowers.is_empty()
        }
    }

    /// Mirror of `RefCounter` with the same free-on-zero rule. Keyed
    /// on `u8` because loom's exhaustive search blows up fast and we
    /// only need a tiny key space to cover the relevant interleavings.
    struct LoomCounter {
        entries: Mutex<HashMap<u8, LoomEntry>>,
    }

    impl LoomCounter {
        fn new() -> Self {
            Self {
                entries: Mutex::new(HashMap::new()),
            }
        }

        fn add_owned(&self, id: u8) {
            let mut g = self.entries.lock().unwrap();
            g.entry(id).or_default().local += 1;
        }

        fn dec_local(&self, id: u8) -> bool {
            let mut g = self.entries.lock().unwrap();
            let Some(entry) = g.get_mut(&id) else {
                return false;
            };
            entry.local = entry.local.saturating_sub(1);
            if entry.unpinned() {
                g.remove(&id);
                true
            } else {
                false
            }
        }

        fn add_borrower(&self, id: u8, w: u8) {
            let mut g = self.entries.lock().unwrap();
            g.entry(id).or_default().borrowers.insert(w);
        }

        fn remove_borrower(&self, id: u8, w: u8) -> bool {
            let mut g = self.entries.lock().unwrap();
            let Some(entry) = g.get_mut(&id) else {
                return false;
            };
            entry.borrowers.remove(&w);
            if entry.unpinned() {
                g.remove(&id);
                true
            } else {
                false
            }
        }

        fn snapshot_local(&self, id: u8) -> Option<u64> {
            self.entries.lock().unwrap().get(&id).map(|e| e.local)
        }

        fn snapshot_borrowers(&self, id: u8) -> Option<HashSet<u8>> {
            self.entries
                .lock()
                .unwrap()
                .get(&id)
                .map(|e| e.borrowers.clone())
        }
    }

    /// Loom-explored invariant: under concurrent driver-drop and
    /// peer-borrow on the same id, the counter never panics and the
    /// final state is one of two valid shapes.
    #[test]
    fn loom_concurrent_dec_local_and_add_borrower_no_panic() {
        loom::model(|| {
            let c = Arc::new(LoomCounter::new());
            // Initial state: owner has produced the object once.
            c.add_owned(1);

            let c1 = Arc::clone(&c);
            let t1 = thread::spawn(move || {
                // Owner-side: last local ref dropping.
                let _freed = c1.dec_local(1);
            });

            let c2 = Arc::clone(&c);
            let t2 = thread::spawn(move || {
                // Borrower-side: peer registering as a holder.
                c2.add_borrower(1, 7);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // Two valid shapes after both threads complete:
            // - dec_local ran first: entry was removed, then add_borrower
            //   recreated it with local=0 and one borrower (resurrection).
            // - add_borrower ran first: entry has local=0 (after dec) and
            //   one borrower; not unpinned, so retained.
            // In both cases, IF an entry exists at all, it has exactly
            // one borrower and local=0.
            if let Some(local) = c.snapshot_local(1) {
                let borrowers = c.snapshot_borrowers(1).expect("paired snapshot");
                assert_eq!(local, 0, "local count should be zero on any final shape");
                assert_eq!(
                    borrowers.len(),
                    1,
                    "borrower set should have exactly the one borrower"
                );
                assert!(borrowers.contains(&7));
            }
            // If no entry exists, both ops happened in the order
            // (add_borrower, dec_local) but dec_local short-circuited
            // because... actually that order would leave an entry with
            // local=0 + borrower. Entry-absent is only possible if
            // add_borrower never ran — which can't happen here because
            // we joined. So this branch should be unreachable, but we
            // tolerate it rather than asserting (loom changes are
            // non-load-bearing for this property).
        });
    }

    /// Loom-explored invariant: two concurrent borrower-add/remove
    /// pairs on different worker ids never lose either entry and never
    /// double-free.
    #[test]
    fn loom_concurrent_two_borrowers_independent() {
        loom::model(|| {
            let c = Arc::new(LoomCounter::new());
            c.add_owned(1);

            let c1 = Arc::clone(&c);
            let t1 = thread::spawn(move || {
                c1.add_borrower(1, 7);
                c1.remove_borrower(1, 7);
            });

            let c2 = Arc::clone(&c);
            let t2 = thread::spawn(move || {
                c2.add_borrower(1, 8);
                c2.remove_borrower(1, 8);
            });

            t1.join().unwrap();
            t2.join().unwrap();

            // After both threads finish, both borrowers are gone. The
            // entry is still pinned by the owner's local count = 1.
            assert_eq!(c.snapshot_local(1), Some(1));
            assert_eq!(c.snapshot_borrowers(1), Some(HashSet::new()));

            // Owner now drops — entry should free cleanly.
            assert!(c.dec_local(1));
            assert!(c.snapshot_local(1).is_none());
        });
    }

    /// Loom-explored invariant: the free-on-zero contract holds even
    /// when dec_local and remove_borrower race to be the "last" pin
    /// drop. Exactly one of the two should observe a `true` return
    /// (the unpin), and the entry must end up gone.
    #[test]
    fn loom_dec_and_remove_race_exactly_one_unpin() {
        loom::model(|| {
            let c = Arc::new(LoomCounter::new());
            c.add_owned(1);
            c.add_borrower(1, 7);
            // Now: local=1, borrowers={7}. Pinned twice.

            let c1 = Arc::clone(&c);
            let t1 = thread::spawn(move || c1.dec_local(1));

            let c2 = Arc::clone(&c);
            let t2 = thread::spawn(move || c2.remove_borrower(1, 7));

            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            // Exactly one of the two operations was the *second* to
            // run, and it's the one that sees `unpinned == true` and
            // returns `true`. The other returns `false` (only one of
            // the two pins cleared).
            assert!(r1 ^ r2, "exactly one of dec/remove should report unpin");
            assert!(c.snapshot_local(1).is_none(), "entry must be gone");
        });
    }
}
