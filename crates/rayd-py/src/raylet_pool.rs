//! Per-process pool of tonic `Channel`s keyed by raylet `SocketAddr`.
//!
//! The driver issues a steady trickle of small RPCs against the same
//! few raylets (its own, plus a handful of peers): `pull`,
//! `register_object`, `get_object_locations`. Each `Channel::connect`
//! costs a TCP + HTTP/2 handshake (~ms on loopback, more on a LAN);
//! reusing the same channel turns subsequent calls into "send a frame
//! on an open H2 stream", which is essentially free.
//!
//! Tonic's `Channel` is `Clone` and intentionally cheap to share —
//! cloning bumps a refcount and threads a request through the same
//! HTTP/2 multiplexer. We wrap it in a `Mutex<HashMap>` for the
//! double-locked-cache pattern.

use std::collections::HashMap;
use std::net::SocketAddr;

use parking_lot::Mutex;
use rayd_raylet::{Channel, ObjectTransportClient, ObjectTransportClientError, RpcCode};
use tracing::debug;

/// A small refcount-friendly cache of `addr → Channel`. Concurrent
/// callers racing to fetch the same uncached address can each open a
/// channel; the second insert just overwrites — no correctness issue,
/// only a one-time waste that the cache hit skips on subsequent calls.
#[derive(Debug, Default)]
pub(crate) struct RayletConnPool {
    channels: Mutex<HashMap<SocketAddr, Channel>>,
}

impl RayletConnPool {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return a `Channel` to `addr`, opening a new one if the cache
    /// misses. The returned `Channel` is a cheap refcount clone.
    pub(crate) async fn get_or_connect(
        &self,
        addr: SocketAddr,
    ) -> Result<Channel, ObjectTransportClientError> {
        let cached = self.channels.lock().get(&addr).cloned();
        if let Some(ch) = cached {
            return Ok(ch);
        }
        let ch = ObjectTransportClient::build_channel(addr).await?;
        self.channels.lock().insert(addr, ch.clone());
        Ok(ch)
    }

    /// Build a typed client over a pooled channel. Convenience for
    /// the call-site pattern of "fetch channel, wrap, issue RPC".
    pub(crate) async fn client(
        &self,
        addr: SocketAddr,
    ) -> Result<ObjectTransportClient, ObjectTransportClientError> {
        let channel = self.get_or_connect(addr).await?;
        Ok(ObjectTransportClient::from_channel(channel))
    }

    /// Drop the cached channel for `addr`. Called after an RPC fails
    /// with a "server gone" error so the next call rebuilds against
    /// the new TCP listener instead of repeatedly hitting a dead
    /// connection.
    pub(crate) fn evict(&self, addr: SocketAddr) {
        if self.channels.lock().remove(&addr).is_some() {
            debug!(%addr, "rayd-py: evicted dead raylet channel");
        }
    }
}

/// Whether the error indicates a transport-level failure that should
/// cause the cached channel to be dropped (so the next call rebuilds).
///
/// `Unavailable` is what tonic surfaces on a closed connection or a
/// server that's not listening anymore; bare `Connect` errors come
/// before we've cached anything, so they don't need eviction.
pub(crate) fn should_evict(err: &ObjectTransportClientError) -> bool {
    matches!(
        err,
        ObjectTransportClientError::Rpc(status) if status.code() == RpcCode::Unavailable,
    )
}
