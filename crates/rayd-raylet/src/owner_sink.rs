//! Owner-side hook the raylet calls when peer borrowing changes.
//!
//! The raylet doesn't itself own a `CoreWorker`, so it can't directly
//! update the owner-side reference counter. Instead, the driver-side
//! glue (e.g. `rayd-py`) builds a `Raylet` with an `Arc<dyn OwnerSink>`
//! that the raylet's gRPC handlers invoke whenever a peer registers
//! or releases a replica.
//!
//! Phase 4.3 implementation hooks:
//! - `RegisterObject` ⇒ `add_borrower(object_id, node_id)`
//! - `WaitForRefRemoved` ⇒ `remove_borrower(object_id, node_id)`
//!
//! Both calls are idempotent. The sink is responsible for any extra
//! bookkeeping (e.g. freeing the local plasma object once all pins
//! clear).

/// Side-effects fired by the raylet when peer borrowing state changes.
pub trait OwnerSink: Send + Sync + std::fmt::Debug {
    /// A peer raylet has reported it now holds a replica of
    /// `object_id`. Idempotent.
    fn add_borrower(&self, object_id: [u8; 28], borrower_node_id: [u8; 16]);

    /// A peer raylet has reported its last `ObjectRef` for `object_id`
    /// just dropped. Idempotent — duplicate calls are harmless.
    fn remove_borrower(&self, object_id: [u8; 28], borrower_node_id: [u8; 16]);

    /// The GCS reports `dead_node_id` is no longer alive (Phase 4.3.3c).
    /// Implementors should treat any object whose owner is `dead_node_id`
    /// as `OwnerDied`, and drop borrower book entries that named it
    /// (so a stale peer can't keep an object pinned indefinitely).
    /// Default impl is empty: pre-4.3.3c sinks can opt in incrementally.
    fn on_owner_died(&self, dead_node_id: [u8; 16]) {
        let _ = dead_node_id;
    }
}
