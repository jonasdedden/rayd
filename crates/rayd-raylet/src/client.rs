//! Typed wrapper around the tonic-generated `ObjectTransport` client.

use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tokio_stream::StreamExt;
pub use tonic::transport::Channel;
pub use tonic::Code as RpcCode;

use crate::proto::object_transport_client::ObjectTransportClient as InnerClient;
use crate::proto::pull_chunk::Kind as PullKind;
use crate::proto::push_frame::Kind as PushKind;
use crate::proto::{
    EvictRequest, GetObjectLocationsRequest, PullRequest, PushFrame, PushHeader,
    RegisterObjectRequest, WaitForRefRemovedRequest,
};

/// Client-side errors talking to a raylet.
#[derive(Debug, Error)]
pub enum ObjectTransportClientError {
    /// Channel connect / dial failure.
    #[error("connect: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// RPC returned an error status.
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    /// Server returned a malformed payload.
    #[error("malformed reply: {0}")]
    Malformed(String),
    /// Server's data-size header didn't match how many data bytes arrived.
    #[error("pull short read: expected {expected} bytes, got {got}")]
    ShortRead {
        /// Bytes the server promised in the metadata header.
        expected: u64,
        /// Bytes actually received before EOF.
        got: u64,
    },
}

/// Set of node ids known to hold replicas of a given object id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ObjectLocations {
    /// 16-byte node ids.
    pub node_ids: Vec<[u8; 16]>,
}

/// Result of a successful `Pull`: reassembled object body + metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulledObject {
    /// User-defined metadata bytes (mirrors plasma's metadata field).
    pub metadata: Vec<u8>,
    /// Reassembled object body.
    pub data: Vec<u8>,
}

/// Connection-typed wrapper around the tonic-generated client.
#[derive(Debug, Clone)]
pub struct ObjectTransportClient {
    inner: InnerClient<Channel>,
}

impl ObjectTransportClient {
    /// Connect to a raylet at `addr`. Convenience for one-shot calls;
    /// callers that issue many RPCs against the same raylet should
    /// build a `Channel` once (e.g. via a connection pool) and use
    /// [`from_channel`].
    pub async fn connect(addr: SocketAddr) -> Result<Self, ObjectTransportClientError> {
        let channel = build_channel(addr).await?;
        Ok(Self::from_channel(channel))
    }

    /// Wrap an existing tonic `Channel`. Use this with a per-process
    /// pool that caches `addr → Channel`, so repeated RPCs against
    /// the same raylet reuse the underlying HTTP/2 connection instead
    /// of paying for a fresh TCP + H2 handshake every call.
    #[must_use]
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: InnerClient::new(channel),
        }
    }

    /// Open a fresh `Channel` to a raylet at `addr`. Exposed so a
    /// pool can build channels on demand without re-implementing the
    /// URI / timeout boilerplate.
    pub async fn build_channel(addr: SocketAddr) -> Result<Channel, ObjectTransportClientError> {
        build_channel(addr).await
    }

    /// Stream-fetch `object_id` from this raylet and reassemble it.
    /// Returns `Status::NotFound` if the object isn't in local plasma.
    pub async fn pull(
        &mut self,
        object_id: Vec<u8>,
    ) -> Result<PulledObject, ObjectTransportClientError> {
        let mut stream = self
            .inner
            .pull(PullRequest { object_id })
            .await?
            .into_inner();

        // Server contract: first frame is `Meta`, rest are `Data`.
        let first = stream.next().await.ok_or_else(|| {
            ObjectTransportClientError::Malformed("pull stream closed before metadata".into())
        })??;
        let meta = match first.kind {
            Some(PullKind::Meta(m)) => m,
            other => {
                return Err(ObjectTransportClientError::Malformed(format!(
                    "first pull frame must be metadata, got {other:?}"
                )));
            }
        };

        let mut data = Vec::with_capacity(usize::try_from(meta.data_size).unwrap_or(0));
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            match frame.kind {
                Some(PullKind::Data(bytes)) => data.extend_from_slice(&bytes),
                Some(PullKind::Meta(_)) => {
                    return Err(ObjectTransportClientError::Malformed(
                        "received second metadata frame mid-stream".into(),
                    ));
                }
                None => {
                    return Err(ObjectTransportClientError::Malformed(
                        "received empty pull frame".into(),
                    ));
                }
            }
        }

        if (data.len() as u64) != meta.data_size {
            return Err(ObjectTransportClientError::ShortRead {
                expected: meta.data_size,
                got: data.len() as u64,
            });
        }

        Ok(PulledObject {
            metadata: meta.metadata,
            data,
        })
    }

    /// Stream-push `(metadata, data)` into this raylet's local plasma
    /// under `object_id`, then return when the seal completes. Bytes
    /// are framed in 64 KiB chunks. Idempotent: if the object is
    /// already in the raylet's plasma the seal is treated as success.
    pub async fn push(
        &mut self,
        object_id: Vec<u8>,
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<(), ObjectTransportClientError> {
        const CHUNK: usize = 64 * 1024;
        let data_size = data.len() as u64;
        let header_frame = PushFrame {
            kind: Some(PushKind::Header(PushHeader {
                object_id,
                metadata,
                data_size,
            })),
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<PushFrame>(8);
        // Producer task: header, then data chunks. We keep this off
        // the calling future so the gRPC half can poll the receiver
        // concurrently with our writes.
        tokio::spawn(async move {
            if tx.send(header_frame).await.is_err() {
                return;
            }
            for chunk in data.chunks(CHUNK) {
                let frame = PushFrame {
                    kind: Some(PushKind::Data(chunk.to_vec())),
                };
                if tx.send(frame).await.is_err() {
                    return;
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        self.inner.push(stream).await?;
        Ok(())
    }

    /// Tell this raylet that `node_id` now holds a replica of `object_id`.
    pub async fn register_object(
        &mut self,
        object_id: Vec<u8>,
        node_id: [u8; 16],
    ) -> Result<(), ObjectTransportClientError> {
        let req = RegisterObjectRequest {
            object_id,
            node_id: node_id.to_vec(),
        };
        self.inner.register_object(req).await?;
        Ok(())
    }

    /// Tell this raylet that `node_id`'s last `ObjectRef` for
    /// `object_id` just dropped. Idempotent on the server side.
    pub async fn wait_for_ref_removed(
        &mut self,
        object_id: Vec<u8>,
        node_id: [u8; 16],
    ) -> Result<(), ObjectTransportClientError> {
        let req = WaitForRefRemovedRequest {
            object_id,
            node_id: node_id.to_vec(),
        };
        self.inner.wait_for_ref_removed(req).await?;
        Ok(())
    }

    /// Tell this raylet to drop the local plasma copy + spill record
    /// for each `object_id` (Phase 4.3.3c — directed Evict fanout).
    /// Idempotent: a peer that has already evicted will quietly succeed.
    pub async fn evict(
        &mut self,
        object_ids: Vec<Vec<u8>>,
    ) -> Result<(), ObjectTransportClientError> {
        let req = EvictRequest { object_ids };
        self.inner.evict(req).await?;
        Ok(())
    }

    /// Ask this raylet (when it's the owner) which nodes hold the object.
    pub async fn get_object_locations(
        &mut self,
        object_id: Vec<u8>,
    ) -> Result<ObjectLocations, ObjectTransportClientError> {
        let req = GetObjectLocationsRequest { object_id };
        let reply = self.inner.get_object_locations(req).await?.into_inner();
        let mut node_ids = Vec::with_capacity(reply.node_ids.len());
        for raw in reply.node_ids {
            if raw.len() != 16 {
                return Err(ObjectTransportClientError::Malformed(format!(
                    "node_id must be 16 bytes, got {}",
                    raw.len()
                )));
            }
            let mut buf = [0u8; 16];
            buf.copy_from_slice(&raw);
            node_ids.push(buf);
        }
        Ok(ObjectLocations { node_ids })
    }
}

async fn build_channel(addr: SocketAddr) -> Result<Channel, ObjectTransportClientError> {
    let url = format!("http://{addr}");
    let channel = Channel::from_shared(url)
        .map_err(|e| ObjectTransportClientError::Malformed(format!("invalid uri: {e}")))?
        .connect_timeout(Duration::from_secs(5))
        .connect()
        .await?;
    Ok(channel)
}
