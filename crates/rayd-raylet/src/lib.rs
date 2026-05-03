//! `rayd-raylet`: per-node daemon for rayd.
//!
//! A raylet is the fixed counterpart of a driver: one per host. It
//! registers itself with the GCS as a node, sends heartbeats, and
//! serves the `ObjectTransport` gRPC for cross-node object moves and
//! ownership-directory queries.
//!
//! Phase 3.4a (this crate) ships:
//! - `ObjectTransport` proto contract.
//! - A raylet binary skeleton that registers + heartbeats with the GCS
//!   and serves the proto with `Unimplemented` stubs.
//! - A `Raylet::start` API that's reusable from tests and from the
//!   `rayd start --address=<gcs>` CLI subcommand.
//!
//! Phase 3.4b/c will fill in the Pull/Push streaming and the
//! `GetObjectLocations` answer.
//!
//! ```no_run
//! use rayd_raylet::{Raylet, RayletConfig};
//! use std::net::SocketAddr;
//! use std::path::PathBuf;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let config = RayletConfig {
//!     gcs_address: "127.0.0.1:60000".parse()?,
//!     bind: "127.0.0.1:0".parse()?,
//!     plasma_socket: PathBuf::from("/tmp/rayd-raylet.sock"),
//!     advertise_host: "127.0.0.1".to_string(),
//!     ..RayletConfig::defaults()
//! };
//! let handle = Raylet::start(config).await?;
//! println!("raylet listening on {}", handle.local_addr());
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
    tonic::include_proto!("rayd.raylet.object_transport.v1");
}

mod client;
mod directory;
mod lifecycle;
mod metrics;
mod node_index;
mod object_manager;
mod owner_sink;
mod service;
mod spill;
mod watch_nodes;

pub use client::{
    Channel, ObjectLocations, ObjectTransportClient, ObjectTransportClientError, PulledObject,
    RpcCode,
};
pub use lifecycle::{Raylet, RayletConfig, RayletHandle, RayletStartError};
pub use metrics::{Metrics, MetricsServerHandle, MetricsStartError};
pub use object_manager::LocalObjectManager;
pub use owner_sink::OwnerSink;
pub use spill::{LocalFsBackend, ObjectIdBytes, RestoredObject, SpillBackend, SpillError, SpillUrl};
pub use proto::{
    GetObjectLocationsReply, GetObjectLocationsRequest, ObjectMetadata, PullChunk, PullRequest,
    PushFrame, PushHeader, PushReply, RegisterObjectReply, RegisterObjectRequest,
    WaitForRefRemovedReply, WaitForRefRemovedRequest,
};
