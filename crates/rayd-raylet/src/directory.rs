//! In-memory `ObjectId → {node_ids}` directory.
//!
//! Each raylet keeps one of these. Phase 3.4b's directory is populated
//! manually via the `RegisterObject` RPC; Phase 3.4c will wire driver
//! `put()` calls so the owner registers itself automatically.
//!
//! Single `parking_lot::Mutex<HashMap>` is fine for the small per-node
//! traffic we expect (read-heavy, low fan-out). A sharded map is a
//! later optimization if the directory ever becomes hot.

use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

/// 28-byte plasma object id; reused from `rayd-plasma`/`rayd-core`.
pub(crate) type ObjectId = [u8; 28];
/// 16-byte node id assigned by the GCS.
pub(crate) type NodeId = [u8; 16];

#[derive(Debug, Default)]
pub(crate) struct ObjectDirectory {
    inner: Mutex<HashMap<ObjectId, HashSet<NodeId>>>,
}

impl ObjectDirectory {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Record that `node_id` holds a replica of `object_id`. Idempotent.
    pub(crate) fn register(&self, object_id: ObjectId, node_id: NodeId) {
        self.inner
            .lock()
            .entry(object_id)
            .or_default()
            .insert(node_id);
    }

    /// Remove `node_id` from the holder set for `object_id`. Drops the
    /// entry entirely when the set becomes empty. Idempotent.
    pub(crate) fn remove(&self, object_id: ObjectId, node_id: NodeId) {
        let mut guard = self.inner.lock();
        let drop_entry = match guard.get_mut(&object_id) {
            None => return,
            Some(set) => {
                set.remove(&node_id);
                set.is_empty()
            }
        };
        if drop_entry {
            guard.remove(&object_id);
        }
    }

    /// Return the set of holders for `object_id`. Empty if unknown.
    pub(crate) fn locations(&self, object_id: &ObjectId) -> Vec<NodeId> {
        self.inner
            .lock()
            .get(object_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Number of distinct `object_id`s currently in the directory.
    /// Used by the metrics gauge.
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_is_idempotent() {
        let dir = ObjectDirectory::new();
        let oid = [1u8; 28];
        let nid = [2u8; 16];
        dir.register(oid, nid);
        dir.register(oid, nid);
        assert_eq!(dir.locations(&oid), vec![nid]);
    }

    #[test]
    fn unknown_object_returns_empty() {
        let dir = ObjectDirectory::new();
        let oid = [9u8; 28];
        assert!(dir.locations(&oid).is_empty());
    }

    #[test]
    fn multiple_holders_are_returned() {
        let dir = ObjectDirectory::new();
        let oid = [1u8; 28];
        dir.register(oid, [10u8; 16]);
        dir.register(oid, [11u8; 16]);
        let mut got = dir.locations(&oid);
        got.sort_unstable();
        assert_eq!(got, vec![[10u8; 16], [11u8; 16]]);
    }
}
