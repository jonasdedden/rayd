//! `OwnerSink` impl that routes raylet borrower-events to the local
//! `CoreWorker`'s `RefCounter`.
//!
//! Construction-time wiring: the driver builds one of these around
//! the same `CoreWorker` it uses for `put` / `get`, and hands an
//! `Arc<dyn OwnerSink>` to the `Raylet`. The raylet's
//! `RegisterObject` and `WaitForRefRemoved` handlers then drive the
//! owner-side reference counter, so cross-process drops actually
//! free local plasma when all pins clear.

use std::sync::Arc;

use rayd_core::{CoreWorker, ObjectId, WorkerId};
use rayd_raylet::OwnerSink;
use tracing::warn;

/// Bridges raylet RPCs to the owner-side `CoreWorker`.
pub(crate) struct WorkerOwnerSink {
    worker: Arc<CoreWorker>,
}

impl WorkerOwnerSink {
    pub(crate) fn new(worker: Arc<CoreWorker>) -> Self {
        Self { worker }
    }
}

impl std::fmt::Debug for WorkerOwnerSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerOwnerSink").finish_non_exhaustive()
    }
}

impl OwnerSink for WorkerOwnerSink {
    fn add_borrower(&self, object_id: [u8; 28], borrower_node_id: [u8; 16]) {
        // We use the borrower's node id as its identity in the
        // refcount table. With one worker per node in the current
        // model that's unambiguous; if multiple workers per node
        // ever land we'll thread the worker id through the proto.
        let oid = ObjectId::from_bytes(object_id);
        let borrower = WorkerId::from_bytes(borrower_node_id);
        self.worker.add_borrower_pin(oid, borrower);
    }

    fn remove_borrower(&self, object_id: [u8; 28], borrower_node_id: [u8; 16]) {
        let oid = ObjectId::from_bytes(object_id);
        let borrower = WorkerId::from_bytes(borrower_node_id);
        if let Err(e) = self.worker.remove_borrower_pin(oid, borrower) {
            warn!(error = %e, "rayd-py: free after borrower removed failed");
        }
    }
}
