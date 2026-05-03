//! In-memory record of submitted tasks for lineage reconstruction.
//!
//! Phase 4.4 MVP: when `submit_task` queues a `DispatchJob`, we also
//! stash the (cloudpickled callable, args, kwargs) blobs keyed by
//! every output `ObjectId` the task produces. If the result is later
//! lost (peer-evicted, owner-side `free_unpinned` cleared it,
//! whatever), `try_resubmit` rebuilds the same `DispatchJob` and
//! queues it again — same `task_id`, so the worker writes back to
//! the same plasma id, and any `ObjectRef` already in flight resolves
//! when the second attempt seals.
//!
//! Each record carries a small retry budget. Once exhausted, future
//! `try_resubmit` calls return `None` and the caller (`rayd.get`)
//! surfaces the failure (Phase 4.5 will route this through
//! `ErrorCategory::ObjectUnreconstructable`).
//!
//! ## Concurrency
//!
//! One `parking_lot::Mutex<HashMap>` for the whole table. Records
//! are small (a few hundred bytes of pickle + counters), so the
//! contention story matches `RefCounter` — fine for the per-driver
//! use case, sharded later if it ever shows up in a profile.

use std::collections::HashMap;

use parking_lot::Mutex;
use rayd_core::{ObjectId, TaskId};

use crate::dispatcher::DispatchJob;

/// Default retry budget for a freshly recorded task.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Per-task lineage info. Cloned (cheap on the blob `Arc`s) when
/// `try_resubmit` mints a fresh `DispatchJob`.
#[derive(Clone, Debug)]
pub(crate) struct TaskRecord {
    pub(crate) task_id: TaskId,
    pub(crate) num_returns: u32,
    pub(crate) callable_blob: std::sync::Arc<Vec<u8>>,
    pub(crate) args_blob: std::sync::Arc<Vec<u8>>,
    pub(crate) kwargs_blob: Option<std::sync::Arc<Vec<u8>>>,
    pub(crate) retries_remaining: u32,
    /// `true` once the dispatcher has observed a successful
    /// completion for this task. Until then the task is still in
    /// flight and we MUST NOT resubmit (else the worker double-seals
    /// and plasma errors with `AlreadyExists`). Set by
    /// `mark_completed`.
    pub(crate) was_completed: bool,
}

/// Outcome of a `lineage_status` query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineageStatus {
    /// We never recorded a task that produces this id.
    NotRecorded,
    /// Task is recorded but hasn't sealed once yet — still in flight.
    NotYetCompleted,
    /// Task has completed at least once and budget remains; a
    /// `try_resubmit` would succeed.
    ReadyToResubmit,
    /// Task has completed at least once but the retry budget is
    /// gone — `rayd.get` should surface this as
    /// `ObjectUnreconstructable`.
    BudgetExhausted,
}

/// Tracker for resubmittable tasks. Lives for the worker's lifetime.
#[derive(Debug, Default)]
pub(crate) struct TaskManager {
    by_object: Mutex<HashMap<ObjectId, TaskRecord>>,
}

impl TaskManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record one entry per output `object_id`, all sharing the same
    /// underlying `TaskRecord` (so a re-submit on any of the outputs
    /// rebuilds the whole task).
    pub(crate) fn record(
        &self,
        outputs: &[ObjectId],
        task_id: TaskId,
        num_returns: u32,
        callable_blob: Vec<u8>,
        args_blob: Vec<u8>,
        kwargs_blob: Option<Vec<u8>>,
    ) {
        let record = TaskRecord {
            task_id,
            num_returns,
            callable_blob: std::sync::Arc::new(callable_blob),
            args_blob: std::sync::Arc::new(args_blob),
            kwargs_blob: kwargs_blob.map(std::sync::Arc::new),
            retries_remaining: DEFAULT_MAX_RETRIES,
            was_completed: false,
        };
        let mut guard = self.by_object.lock();
        for oid in outputs {
            guard.insert(*oid, record.clone());
        }
    }

    /// Mark every record with `task_id` as having sealed at least
    /// once. Called by the dispatcher's completion handler so
    /// future `try_resubmit` calls know the task is past the in-
    /// flight phase.
    pub(crate) fn mark_completed(&self, task_id: TaskId) {
        let mut guard = self.by_object.lock();
        for r in guard.values_mut() {
            if r.task_id == task_id {
                r.was_completed = true;
            }
        }
    }

    /// Lineage classification for an object id. Used by `rayd.get`'s
    /// auto-resubmit path to choose between "wait", "resubmit", and
    /// "raise `ObjectUnreconstructable`".
    pub(crate) fn lineage_status(&self, object_id: ObjectId) -> LineageStatus {
        let guard = self.by_object.lock();
        let Some(record) = guard.get(&object_id) else {
            return LineageStatus::NotRecorded;
        };
        if !record.was_completed {
            return LineageStatus::NotYetCompleted;
        }
        if record.retries_remaining == 0 {
            return LineageStatus::BudgetExhausted;
        }
        LineageStatus::ReadyToResubmit
    }

    /// If we've recorded a task that produces `object_id`, the task
    /// has completed at least once, and the retry budget is non-zero,
    /// decrement the budget and return a fresh `DispatchJob` ready
    /// for the dispatcher to queue. Otherwise `None`.
    ///
    /// Decrements the budget AND resets `was_completed` to `false`
    /// for ALL of the task's outputs (they share a record). The
    /// reset means a concurrent caller (e.g. `rayd.get`'s auto-
    /// resubmit path) sees `NotYetCompleted` and waits for the new
    /// attempt to seal instead of double-submitting.
    pub(crate) fn try_resubmit(&self, object_id: ObjectId) -> Option<DispatchJob> {
        let mut guard = self.by_object.lock();
        let record = guard.get(&object_id)?;
        if !record.was_completed {
            return None;
        }
        if record.retries_remaining == 0 {
            return None;
        }
        let job = DispatchJob {
            task_id: record.task_id,
            num_returns: record.num_returns,
            callable_blob: (*record.callable_blob).clone(),
            args_blob: (*record.args_blob).clone(),
            kwargs_blob: record.kwargs_blob.as_ref().map(|b| (**b).clone()),
        };
        let task_id = record.task_id;
        let oids: Vec<ObjectId> = guard
            .iter()
            .filter_map(|(oid, r)| (r.task_id == task_id).then_some(*oid))
            .collect();
        for oid in oids {
            if let Some(r) = guard.get_mut(&oid) {
                r.retries_remaining = r.retries_remaining.saturating_sub(1);
                r.was_completed = false;
            }
        }
        Some(job)
    }

    /// How many distinct task outputs are currently tracked.
    /// Test/diagnostic accessor.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_object.lock().len()
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

    fn tid(seed: u8) -> TaskId {
        let mut buf = [0u8; 24];
        buf[0] = seed;
        TaskId::from_bytes(buf)
    }

    #[test]
    fn record_inserts_one_entry_per_output() {
        let m = TaskManager::new();
        m.record(
            &[oid(1), oid(2)],
            tid(7),
            2,
            b"call".to_vec(),
            b"args".to_vec(),
            None,
        );
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn try_resubmit_blocks_until_completion_seen() {
        let m = TaskManager::new();
        m.record(
            &[oid(1)],
            tid(7),
            1,
            b"call".to_vec(),
            b"args".to_vec(),
            None,
        );
        // Before completion is observed, try_resubmit MUST return
        // None — we don't want to double-submit an in-flight task.
        assert!(m.try_resubmit(oid(1)).is_none());
        assert_eq!(m.lineage_status(oid(1)), LineageStatus::NotYetCompleted);

        // Each resubmit cycle: dispatcher observes completion, then
        // we evict, then try_resubmit. After try_resubmit fires it
        // resets was_completed to false (the new attempt is now in
        // flight) — so we mark_completed again before the next
        // iteration to model the dispatcher's callback.
        for _ in 0..DEFAULT_MAX_RETRIES {
            m.mark_completed(tid(7));
            assert!(m.try_resubmit(oid(1)).is_some());
        }
        // Next mark_completed leaves the record completed, but the
        // budget is gone; try_resubmit must return None.
        m.mark_completed(tid(7));
        assert!(m.try_resubmit(oid(1)).is_none());
        assert_eq!(m.lineage_status(oid(1)), LineageStatus::BudgetExhausted);
    }

    #[test]
    fn unknown_object_returns_not_recorded() {
        let m = TaskManager::new();
        assert!(m.try_resubmit(oid(99)).is_none());
        assert_eq!(m.lineage_status(oid(99)), LineageStatus::NotRecorded);
    }

    #[test]
    fn shared_task_decrements_all_outputs_together() {
        let m = TaskManager::new();
        m.record(
            &[oid(1), oid(2)],
            tid(7),
            2,
            b"call".to_vec(),
            b"args".to_vec(),
            None,
        );
        for _ in 0..DEFAULT_MAX_RETRIES {
            m.mark_completed(tid(7));
            assert!(m.try_resubmit(oid(1)).is_some());
        }
        m.mark_completed(tid(7));
        assert!(m.try_resubmit(oid(2)).is_none());
        assert_eq!(m.lineage_status(oid(2)), LineageStatus::BudgetExhausted);
    }

    #[test]
    fn mark_completed_propagates_to_all_outputs_of_same_task() {
        let m = TaskManager::new();
        m.record(
            &[oid(1), oid(2)],
            tid(7),
            2,
            b"call".to_vec(),
            b"args".to_vec(),
            None,
        );
        m.mark_completed(tid(7));
        assert_eq!(m.lineage_status(oid(1)), LineageStatus::ReadyToResubmit);
        assert_eq!(m.lineage_status(oid(2)), LineageStatus::ReadyToResubmit);
    }
}
