//! `rayd-gcs`: Global Control Service.
//!
//! Phase 3.3 ships the `NodeRegistry` gRPC service; Phase 3.3b adds
//! `JobRegistry`. Both are served on the same gRPC channel by
//! [`GcsServer::start`]. State is in-memory and single-process; durable
//! state and HA come in later phases.
//!
//! ```no_run
//! use rayd_gcs::{GcsServer, GcsServerHandle};
//! use std::net::SocketAddr;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let addr: SocketAddr = "127.0.0.1:0".parse()?;
//! let handle: GcsServerHandle = GcsServer::start(addr).await?;
//! println!("listening on {}", handle.local_addr());
//! handle.shutdown().await;
//! # Ok(())
//! # }
//! ```

#![allow(unsafe_code)]

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::all,
    missing_docs,
    unreachable_pub,
    missing_debug_implementations,
    rust_2018_idioms,
    unused_qualifications,
    clippy::doc_markdown,
    clippy::derive_partial_eq_without_eq,
    clippy::large_enum_variant
)]
mod proto {
    tonic::include_proto!("rayd.gcs.node_info.v1");
}

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::all,
    missing_docs,
    unreachable_pub,
    missing_debug_implementations,
    rust_2018_idioms,
    unused_qualifications,
    clippy::doc_markdown,
    clippy::derive_partial_eq_without_eq,
    clippy::large_enum_variant
)]
mod job_proto {
    tonic::include_proto!("rayd.gcs.job_info.v1");
}

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::all,
    missing_docs,
    unreachable_pub,
    missing_debug_implementations,
    rust_2018_idioms,
    unused_qualifications,
    clippy::doc_markdown,
    clippy::derive_partial_eq_without_eq,
    clippy::large_enum_variant
)]
mod actor_proto {
    tonic::include_proto!("rayd.gcs.actor_info.v1");
}

mod actor_service;
mod client;
mod job_service;
mod metrics;
mod server;
mod service;

pub use actor_proto::{
    ActorInfo, GetActorReply, GetActorRequest, ListActorsReply, ListActorsRequest,
    RegisterActorReply, RegisterActorRequest, UnregisterActorReply, UnregisterActorRequest,
};
pub use client::{GcsClient, GcsClientError, RegisterOutcome};
pub use job_proto::{
    AddJobReply, AddJobRequest, JobInfo, JobStatus, ListJobsReply, ListJobsRequest,
    MarkJobFinishedReply, MarkJobFinishedRequest,
};
pub use metrics::{Metrics, MetricsServerHandle, MetricsStartError};
pub use proto::{
    DrainReply, DrainRequest, HeartbeatReply, HeartbeatRequest, ListReply, ListRequest,
    NodeAddress, NodeEvent, NodeInfo, NodeStatus, RegisterReply, RegisterRequest, Resources,
    WatchNodesRequest,
};
pub use server::{GcsServer, GcsServerConfig, GcsServerHandle, GcsServerStartError};
