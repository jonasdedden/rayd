//! Thin async client wrapper around the tonic-generated GCS clients.
//!
//! Hides the proto types' raw shape from callers (e.g. converting
//! `Vec<u8>` node/job ids into typed `[u8; 16]`). One `GcsClient` carries
//! one connection that multiplexes both the `NodeRegistry` and
//! `JobRegistry` services.

use std::net::SocketAddr;
use std::time::Duration;

use thiserror::Error;
use tonic::transport::Channel;

use crate::actor_proto::actor_registry_client::ActorRegistryClient;
use crate::actor_proto::{
    ActorInfo, GetActorRequest, ListActorsRequest, RegisterActorRequest, UnregisterActorRequest,
};
use crate::job_proto::job_registry_client::JobRegistryClient;
use crate::job_proto::{AddJobRequest, JobInfo, ListJobsRequest, MarkJobFinishedRequest};
use crate::proto::node_registry_client::NodeRegistryClient;
use crate::proto::{
    DrainRequest, HeartbeatRequest, ListRequest, NodeEvent, NodeInfo, RegisterRequest, Resources,
    WatchNodesRequest,
};

/// Client-side errors talking to the GCS.
#[derive(Debug, Error)]
pub enum GcsClientError {
    /// Channel connect / dial failure.
    #[error("connect: {0}")]
    Connect(#[from] tonic::transport::Error),
    /// RPC returned an error status.
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    /// Server returned a malformed payload (e.g. wrong-length `node_id`).
    #[error("malformed reply: {0}")]
    Malformed(String),
}

/// Connection-typed wrapper around the generated clients. Holds the
/// three generated clients sharing one underlying `Channel`.
#[derive(Debug, Clone)]
pub struct GcsClient {
    nodes: NodeRegistryClient<Channel>,
    jobs: JobRegistryClient<Channel>,
    actors: ActorRegistryClient<Channel>,
}

impl GcsClient {
    /// Connect to a GCS at `addr`. Used in tests and the per-node
    /// daemon's startup path.
    pub async fn connect(addr: SocketAddr) -> Result<Self, GcsClientError> {
        let url = format!("http://{addr}");
        let channel = Channel::from_shared(url)
            .map_err(|e| GcsClientError::Malformed(format!("invalid uri: {e}")))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self {
            nodes: NodeRegistryClient::new(channel.clone()),
            jobs: JobRegistryClient::new(channel.clone()),
            actors: ActorRegistryClient::new(channel),
        })
    }

    /// Register this node. Returns `(node_id, cluster_session_id)`.
    pub async fn register(
        &mut self,
        host: impl Into<String>,
        port: u16,
        plasma_socket: impl Into<String>,
        resources: Resources,
    ) -> Result<RegisterOutcome, GcsClientError> {
        let req = RegisterRequest {
            host: host.into(),
            port: u32::from(port),
            plasma_socket: plasma_socket.into(),
            resources: Some(resources),
        };
        let reply = self.nodes.register(req).await?.into_inner();
        let node_id = parse_id(&reply.node_id, "node_id")?;
        let cluster_session_id = parse_id(&reply.cluster_session_id, "cluster_session_id")?;
        Ok(RegisterOutcome {
            node_id,
            cluster_session_id,
        })
    }

    /// Mark a node as draining.
    pub async fn drain(&mut self, node_id: [u8; 16]) -> Result<(), GcsClientError> {
        self.nodes
            .drain(DrainRequest {
                node_id: node_id.to_vec(),
            })
            .await?;
        Ok(())
    }

    /// Snapshot all nodes the GCS currently knows about.
    pub async fn list(&mut self) -> Result<Vec<NodeInfo>, GcsClientError> {
        let reply = self.nodes.list(ListRequest {}).await?.into_inner();
        Ok(reply.nodes)
    }

    /// Send a liveness ping.
    pub async fn heartbeat(&mut self, node_id: [u8; 16]) -> Result<(), GcsClientError> {
        self.nodes
            .heartbeat(HeartbeatRequest {
                node_id: node_id.to_vec(),
            })
            .await?;
        Ok(())
    }

    /// Open a server-streaming subscription to node-state changes.
    /// Pass `last_seen_sequence = 0` for a fresh snapshot followed by
    /// the live tail; pass the highest sequence previously observed
    /// to resume after a transient disconnect. The server returns
    /// `OUT_OF_RANGE` if the resume point is older than its replay
    /// buffer — callers should retry with `0` in that case.
    pub async fn watch_nodes(
        &mut self,
        last_seen_sequence: u64,
    ) -> Result<tonic::Streaming<NodeEvent>, GcsClientError> {
        let req = WatchNodesRequest { last_seen_sequence };
        let resp = self.nodes.watch_nodes(req).await?;
        Ok(resp.into_inner())
    }

    // ── JobRegistry ─────────────────────────────────────────────────────

    /// Register a new job. Returns the server-assigned `job_id`. Pass
    /// `node_id = None` for jobs not associated with a particular node.
    pub async fn add_job(
        &mut self,
        driver_host: impl Into<String>,
        driver_pid: u32,
        node_id: Option<[u8; 16]>,
    ) -> Result<[u8; 16], GcsClientError> {
        let req = AddJobRequest {
            driver_host: driver_host.into(),
            driver_pid,
            node_id: node_id.map(|n| n.to_vec()).unwrap_or_default(),
        };
        let reply = self.jobs.add_job(req).await?.into_inner();
        parse_id(&reply.job_id, "job_id")
    }

    /// Mark a job as finished. Pass an empty `failure_message` for graceful
    /// finish; non-empty surfaces the job as `FAILED`.
    pub async fn mark_job_finished(
        &mut self,
        job_id: [u8; 16],
        failure_message: impl Into<String>,
    ) -> Result<(), GcsClientError> {
        self.jobs
            .mark_job_finished(MarkJobFinishedRequest {
                job_id: job_id.to_vec(),
                failure_message: failure_message.into(),
            })
            .await?;
        Ok(())
    }

    /// Snapshot all jobs the GCS currently knows about.
    pub async fn list_jobs(&mut self) -> Result<Vec<JobInfo>, GcsClientError> {
        let reply = self.jobs.list(ListJobsRequest {}).await?.into_inner();
        Ok(reply.jobs)
    }

    // ── ActorRegistry ───────────────────────────────────────────────────

    /// Register a named actor in the GCS directory. Returns
    /// `Status::AlreadyExists` if `name` is taken by a different
    /// `actor_id`; idempotent on a re-register with the same id.
    ///
    /// `driver_actor_host`/`driver_actor_port` advertise the owner
    /// driver's actor-RPC TCP listener. Pass empty/`0` when the owner
    /// runs without a listener (e.g. unit tests).
    pub async fn register_actor(
        &mut self,
        name: impl Into<String>,
        actor_id: [u8; 16],
        owner_node_id: Option<[u8; 16]>,
        owner_pid: u32,
        driver_actor_host: impl Into<String>,
        driver_actor_port: u16,
    ) -> Result<(), GcsClientError> {
        let req = RegisterActorRequest {
            name: name.into(),
            actor_id: actor_id.to_vec(),
            owner_node_id: owner_node_id.map(|n| n.to_vec()).unwrap_or_default(),
            owner_pid,
            driver_actor_host: driver_actor_host.into(),
            driver_actor_port: u32::from(driver_actor_port),
        };
        self.actors.register_actor(req).await?;
        Ok(())
    }

    /// Look up a named actor. `None` if no actor with that name exists.
    pub async fn get_actor(
        &mut self,
        name: impl Into<String>,
    ) -> Result<Option<ActorInfo>, GcsClientError> {
        let req = GetActorRequest { name: name.into() };
        match self.actors.get_actor(req).await {
            Ok(reply) => Ok(reply.into_inner().actor),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    /// Remove a named-actor entry. The caller's `actor_id` must match
    /// the registered entry — prevents stale handles from clobbering a
    /// freshly-registered name.
    pub async fn unregister_actor(
        &mut self,
        name: impl Into<String>,
        actor_id: [u8; 16],
    ) -> Result<(), GcsClientError> {
        let req = UnregisterActorRequest {
            name: name.into(),
            actor_id: actor_id.to_vec(),
        };
        self.actors.unregister_actor(req).await?;
        Ok(())
    }

    /// Snapshot all named actors the GCS currently tracks. Mostly for
    /// tests & tooling; production callers should use `get_actor`.
    pub async fn list_actors(&mut self) -> Result<Vec<ActorInfo>, GcsClientError> {
        let reply = self.actors.list(ListActorsRequest {}).await?.into_inner();
        Ok(reply.actors)
    }
}

/// Successful return from `register`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisterOutcome {
    /// Server-assigned node id.
    pub node_id: [u8; 16],
    /// Identifies the GCS instance; changes on restart.
    pub cluster_session_id: [u8; 16],
}

fn parse_id(bytes: &[u8], name: &str) -> Result<[u8; 16], GcsClientError> {
    if bytes.len() != 16 {
        return Err(GcsClientError::Malformed(format!(
            "{name} must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(buf)
}
