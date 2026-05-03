//! `ObjectRef` and `Address`: the wire-level identity of stored values.
//!
//! As discussed in `docs/analysis/02-ownership-and-references.md`, an
//! `ObjectRef` carries both the object's id *and* the address of its owner.
//! Carrying the owner address inline lets any holder talk directly to the
//! owner for refcount and location queries without a global directory.

use core::fmt;

use crate::id::{ObjectId, WorkerId};

/// Address of a worker (or any rayd component that runs a `CoreWorkerService`).
///
/// Mirrors Ray's `rpc::Address` proto; kept as a plain Rust struct here so the
/// crate stays free of a protobuf dependency at this layer.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Address {
    /// Hostname or IP literal.
    pub host: String,
    /// TCP port for the worker's gRPC server.
    pub port: u16,
    /// The worker's id; `WorkerId::nil()` is allowed for placeholder addresses
    /// (e.g. before the worker has registered).
    pub worker_id: WorkerId,
}

impl Address {
    /// Construct a fully-specified address.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16, worker_id: WorkerId) -> Self {
        Self {
            host: host.into(),
            port,
            worker_id,
        }
    }

    /// Whether this address has a non-nil worker id.
    #[must_use]
    pub fn is_resolved(&self) -> bool {
        !self.worker_id.is_nil()
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}@{}", self.host, self.port, self.worker_id)
    }
}

/// A reference to a stored value plus the address of its owner.
///
/// Cloning is cheap (the embedded `Address` is the only allocation), and
/// `ObjectRef` is `Send + Sync` so it can travel across threads freely. The
/// `PyO3` wrapper in `rayd-py` exposes this type to Python with the same
/// semantics.
///
/// `owner_node_id` is the 16-byte GCS node id of the owner-raylet.
/// `Some` when the producing driver was attached to a GCS (so peers can
/// dial that raylet for `Pull`); `None` for in-process / single-machine
/// uses where there's nothing to dial.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ObjectRef {
    object_id: ObjectId,
    owner: Address,
    owner_node_id: Option<[u8; 16]>,
}

impl ObjectRef {
    /// Construct a new `ObjectRef` with no owner-node-id stamp.
    #[must_use]
    pub const fn new(object_id: ObjectId, owner: Address) -> Self {
        Self {
            object_id,
            owner,
            owner_node_id: None,
        }
    }

    /// Builder method: stamp the owner's node id into a fresh `ObjectRef`.
    #[must_use]
    pub fn with_owner_node_id(mut self, node_id: [u8; 16]) -> Self {
        self.owner_node_id = Some(node_id);
        self
    }

    /// The id of the referenced object.
    #[must_use]
    pub const fn object_id(&self) -> &ObjectId {
        &self.object_id
    }

    /// The address of the owner worker.
    #[must_use]
    pub const fn owner(&self) -> &Address {
        &self.owner
    }

    /// The 16-byte GCS node id of the owner-raylet, when known.
    #[must_use]
    pub const fn owner_node_id(&self) -> Option<[u8; 16]> {
        self.owner_node_id
    }
}

impl fmt::Display for ObjectRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectRef({} owned-by {})", self.object_id, self.owner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::TaskId;

    #[test]
    fn ref_carries_owner() {
        let object = ObjectId::for_return(&TaskId::random(), 0);
        let owner = Address::new("10.0.0.1", 60123, WorkerId::random());
        let r = ObjectRef::new(object, owner.clone());
        assert_eq!(r.object_id(), &object);
        assert_eq!(r.owner(), &owner);
    }

    #[test]
    fn unresolved_address() {
        let owner = Address::new("placeholder", 0, WorkerId::nil());
        assert!(!owner.is_resolved());
    }
}
