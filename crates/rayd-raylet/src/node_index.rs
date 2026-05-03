//! Live cache of cluster node state, fed by the GCS `WatchNodes` stream.
//!
//! Phase 4.3.3c. The raylet keeps one of these per process; the
//! `WatchNodes` subscriber writes into it, and other raylet code reads
//! it for:
//! - peer-address resolution (avoiding a `list_nodes()` round-trip per
//!   raylet→raylet RPC),
//! - the directed `Evict` fanout target list (only `Alive` peers),
//! - eager `OwnerDied` propagation via `OwnerSink::on_owner_died`.
//!
//! The map is `RwLock<HashMap>` because reads dominate (every
//! Pull/Push consults it) and writes only happen on cluster topology
//! changes, which are rare relative to the data plane.

use std::collections::HashMap;
use std::net::SocketAddr;

use parking_lot::RwLock;

use rayd_gcs::{NodeInfo, NodeStatus};

/// Per-node cached state.
#[derive(Clone, Debug)]
pub(crate) struct NodeEntry {
    pub(crate) status: NodeStatus,
    /// Wired in Phase 4.3.3c-C (directed Evict fanout) — kept now so
    /// the cache type matches the eventual consumer without a churny
    /// rename later.
    #[allow(dead_code)]
    pub(crate) addr: SocketAddr,
}

/// In-memory snapshot of `NodeId → (status, addr)` driven by the
/// streaming `WatchNodes` subscription. Exposes the typed read surface
/// the raylet code actually needs; updates are gated through
/// `apply_event` so callers can't bypass the status-transition logic.
#[derive(Debug, Default)]
pub(crate) struct NodeIndex {
    inner: RwLock<HashMap<[u8; 16], NodeEntry>>,
}

impl NodeIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Apply an inbound `NodeInfo` event. Returns `Some(prev_status)`
    /// when the entry's status actually changed (so the subscriber can
    /// decide whether to emit `OwnerSink::on_owner_died`); returns
    /// `None` for first-sight inserts and no-op updates.
    pub(crate) fn apply_event(&self, info: &NodeInfo) -> StatusTransition {
        let Some(addr) = parse_node_address(info) else {
            return StatusTransition::Skipped;
        };
        let new_status =
            NodeStatus::try_from(info.status).unwrap_or(NodeStatus::Unspecified);
        let Some(node_id) = info.address.as_ref().map(|a| a.node_id.as_slice()) else {
            return StatusTransition::Skipped;
        };
        if node_id.len() != 16 {
            return StatusTransition::Skipped;
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(node_id);

        let mut guard = self.inner.write();
        let prev = guard.insert(
            id,
            NodeEntry {
                status: new_status,
                addr,
            },
        );
        match prev {
            None => StatusTransition::Inserted(new_status),
            Some(prev) if prev.status == new_status => StatusTransition::Unchanged,
            Some(prev) => StatusTransition::Changed {
                from: prev.status,
                to: new_status,
            },
        }
    }

    /// Resolve a node id to its raylet socket addr (independent of
    /// status — callers gate on liveness themselves). Used by the
    /// Phase 4.3.3c-C directed Evict fanout.
    #[allow(dead_code)]
    pub(crate) fn address_of(&self, node_id: &[u8; 16]) -> Option<SocketAddr> {
        self.inner.read().get(node_id).map(|e| e.addr)
    }

    /// Status snapshot for `node_id`. `None` if unknown — caller
    /// should fall back to a synchronous `list_nodes()` RPC.
    pub(crate) fn status_of(&self, node_id: &[u8; 16]) -> Option<NodeStatus> {
        self.inner.read().get(node_id).map(|e| e.status)
    }

    /// Entry count — for tests / debug.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.read().len()
    }
}

/// What `apply_event` did. `Changed { from: Alive, to: Dead }` is the
/// signal that drives the owner-died fanout in the subscriber loop.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatusTransition {
    /// First time we've seen this node.
    Inserted(NodeStatus),
    /// Status moved from `from` to `to`.
    Changed {
        from: NodeStatus,
        to: NodeStatus,
    },
    /// Re-broadcast or duplicate — status identical.
    Unchanged,
    /// Malformed event (missing address, bad `node_id` length). Caller
    /// should log + ignore; not a hard failure since the GCS may emit
    /// future versions of the schema we don't know yet.
    Skipped,
}

fn parse_node_address(info: &NodeInfo) -> Option<SocketAddr> {
    let addr = info.address.as_ref()?;
    let host = addr.host.as_str();
    let port = u16::try_from(addr.port).ok()?;
    format!("{host}:{port}").parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rayd_gcs::NodeAddress;

    fn info(node_id: [u8; 16], status: NodeStatus, host: &str) -> NodeInfo {
        NodeInfo {
            address: Some(NodeAddress {
                host: host.to_owned(),
                port: 60001,
                node_id: node_id.to_vec(),
                plasma_socket: String::new(),
            }),
            resources: None,
            status: status as i32,
            registered_at_unix_ms: 0,
            last_heartbeat_unix_ms: 0,
        }
    }

    #[test]
    fn first_seen_is_inserted_then_unchanged_then_changed() {
        let idx = NodeIndex::new();
        let nid = [42u8; 16];
        assert!(matches!(
            idx.apply_event(&info(nid, NodeStatus::Alive, "10.0.0.1")),
            StatusTransition::Inserted(NodeStatus::Alive)
        ));
        assert_eq!(
            idx.apply_event(&info(nid, NodeStatus::Alive, "10.0.0.1")),
            StatusTransition::Unchanged
        );
        assert_eq!(
            idx.apply_event(&info(nid, NodeStatus::Dead, "10.0.0.1")),
            StatusTransition::Changed {
                from: NodeStatus::Alive,
                to: NodeStatus::Dead,
            }
        );
    }

    #[test]
    fn malformed_node_address_is_skipped() {
        let idx = NodeIndex::new();
        let mut bad = info([1u8; 16], NodeStatus::Alive, "10.0.0.2");
        bad.address = None;
        assert_eq!(idx.apply_event(&bad), StatusTransition::Skipped);
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn address_of_returns_socket_addr_after_apply() {
        let idx = NodeIndex::new();
        let nid = [7u8; 16];
        idx.apply_event(&info(nid, NodeStatus::Alive, "127.0.0.1"));
        assert_eq!(
            idx.address_of(&nid),
            Some("127.0.0.1:60001".parse().unwrap())
        );
        assert_eq!(idx.status_of(&nid), Some(NodeStatus::Alive));
    }
}
