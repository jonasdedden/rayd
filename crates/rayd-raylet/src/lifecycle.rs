//! Raylet lifecycle: register with GCS, serve `ObjectTransport`, heartbeat.
//!
//! `Raylet::start` brings up:
//! 1. A `tonic::Server` serving `ObjectTransport` on `config.bind`.
//! 2. A `GcsClient` connection to `config.gcs_address`.
//! 3. A `Register` RPC that mints this raylet's node id.
//! 4. A heartbeat task that pings the GCS until `RayletHandle::shutdown`.
//!
//! `RayletHandle::shutdown` reverses the order: cancels the heartbeat
//! task, drains the node, and stops the gRPC server.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rayd_gcs::{GcsClient, GcsClientError, Resources};
use rayd_plasma::{PlasmaClient, PlasmaError};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::{debug, info, warn};

use crate::directory::ObjectDirectory;
use crate::metrics::{start_metrics_server, Metrics, MetricsServerHandle, MetricsStartError};
use crate::node_index::NodeIndex;
use crate::object_manager::LocalObjectManager;
use crate::owner_sink::OwnerSink;
use crate::proto::object_transport_server::ObjectTransportServer;
use crate::service::ObjectTransportService;
use crate::watch_nodes::run_watch_nodes;

/// Errors observable while standing up a raylet.
#[derive(Debug, Error)]
pub enum RayletStartError {
    /// The raylet's gRPC bind step failed.
    #[error("bind {addr} failed: {source}")]
    Bind {
        /// The address we attempted to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Could not connect to the GCS.
    #[error("gcs: {0}")]
    Gcs(#[from] GcsClientError),
    /// Could not connect to the local plasma server.
    #[error("plasma: {0}")]
    Plasma(#[from] PlasmaError),
    /// Tonic transport setup failure (constructing the `Server` future).
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Metrics endpoint setup failed.
    #[error("metrics: {0}")]
    Metrics(#[from] MetricsStartError),
}

/// All knobs needed to bring up a raylet. `defaults()` fills in the parts
/// that are usually fine; callers must always supply the GCS address and
/// the host name they want this raylet advertised under.
#[derive(Clone)]
pub struct RayletConfig {
    /// `host:port` of the cluster's GCS.
    pub gcs_address: SocketAddr,
    /// Where the raylet's own gRPC server binds. `:0` lets the OS pick.
    pub bind: SocketAddr,
    /// Hostname the GCS records for this raylet — peer raylets dial this
    /// to send `Pull`/`Push`. Usually a routable IP, not "localhost".
    pub advertise_host: String,
    /// Path of the local plasma UDS. Recorded in the `NodeInfo` so peers
    /// know where to seal data fetched from this raylet (Phase 3.4b/c).
    pub plasma_socket: PathBuf,
    /// Resources this raylet contributes to the cluster.
    pub resources: Resources,
    /// How often the heartbeat task pings the GCS.
    pub heartbeat_interval: Duration,
    /// Optional owner-side hook the raylet calls on `RegisterObject`
    /// and `WaitForRefRemoved`, so the driver-side `RefCounter` can
    /// track peer borrowers and free objects once everyone clears.
    /// `None` means the raylet runs as a passive transport with no
    /// distributed refcount (equivalent to pre-Phase-4.3 behaviour).
    pub owner_sink: Option<Arc<dyn OwnerSink>>,
    /// Optional spill manager. When present, the raylet's `Pull`
    /// handler consults it on plasma-miss and transparently restores
    /// the object from the backing `SpillBackend`. `None` disables
    /// spilling entirely (equivalent to pre-Phase-6 behaviour).
    pub object_manager: Option<Arc<LocalObjectManager>>,
    /// Optional bind address for the Prometheus `/metrics` HTTP
    /// endpoint. `None` disables metrics entirely.
    pub metrics_bind: Option<SocketAddr>,
}

impl std::fmt::Debug for RayletConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RayletConfig")
            .field("gcs_address", &self.gcs_address)
            .field("bind", &self.bind)
            .field("advertise_host", &self.advertise_host)
            .field("plasma_socket", &self.plasma_socket)
            .field("resources", &self.resources)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("has_owner_sink", &self.owner_sink.is_some())
            .field("has_object_manager", &self.object_manager.is_some())
            .field("metrics_bind", &self.metrics_bind)
            .finish()
    }
}

impl RayletConfig {
    /// Sensible defaults for the rarely-changed knobs. Caller still has
    /// to set `gcs_address`, `bind`, `advertise_host`, `plasma_socket`.
    #[must_use]
    pub fn defaults() -> Self {
        Self {
            gcs_address: "127.0.0.1:60000".parse().expect("default gcs addr"),
            bind: "0.0.0.0:0".parse().expect("default bind"),
            advertise_host: String::from("127.0.0.1"),
            plasma_socket: PathBuf::from("/tmp/rayd-raylet.sock"),
            resources: Resources {
                num_cpus: 1,
                num_gpus: 0,
                memory_bytes: 0,
            },
            heartbeat_interval: Duration::from_secs(2),
            owner_sink: None,
            object_manager: None,
            metrics_bind: None,
        }
    }
}

/// Construction-only marker; `Raylet::start` returns the running handle.
#[derive(Debug)]
pub struct Raylet;

impl Raylet {
    /// Bind, register with the GCS, start heartbeats, and serve until the
    /// returned handle's `shutdown()` is awaited (or the handle is dropped).
    #[allow(clippy::too_many_lines)]
    pub async fn start(config: RayletConfig) -> Result<RayletHandle, RayletStartError> {
        let listener =
            TcpListener::bind(config.bind)
                .await
                .map_err(|e| RayletStartError::Bind {
                    addr: config.bind,
                    source: e,
                })?;
        let local_addr = listener.local_addr().map_err(|e| RayletStartError::Bind {
            addr: config.bind,
            source: e,
        })?;

        // Open the local plasma connection up front. We want a hard
        // failure if the socket isn't reachable rather than a confusing
        // half-up state with an `Unavailable` Pull RPC.
        let plasma_client = PlasmaClient::connect(&config.plasma_socket)?;
        let plasma = Arc::new(Mutex::new(plasma_client));
        let directory = Arc::new(ObjectDirectory::new());

        let metrics = match config.metrics_bind {
            Some(_) => Some(Metrics::new()?),
            None => None,
        };

        let svc = ObjectTransportService::new(
            Arc::clone(&plasma),
            Arc::clone(&directory),
            config.owner_sink.clone(),
            config.object_manager.clone(),
            metrics.clone(),
        );

        // Standard `grpc.health.v1.Health` service. Mark our
        // `ObjectTransport` service as `SERVING`; the empty-key ""
        // overall slot also reports `SERVING`. Probes use this to
        // verify the raylet's gRPC layer is up before sending real
        // RPCs.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<ObjectTransportServer<ObjectTransportService>>()
            .await;

        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel::<()>();
        let server_future = Server::builder()
            .add_service(health_service)
            .add_service(ObjectTransportServer::new(svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = server_shutdown_rx.await;
                },
            );
        let server_join = tokio::spawn(async move {
            if let Err(e) = server_future.await {
                warn!(error = %e, "rayd-raylet: gRPC server exited with error");
            }
        });

        let mut gcs = GcsClient::connect(config.gcs_address).await?;
        let plasma_socket_str = config.plasma_socket.to_string_lossy().into_owned();
        let outcome = gcs
            .register(
                config.advertise_host.clone(),
                local_addr.port(),
                plasma_socket_str,
                config.resources,
            )
            .await?;

        info!(
            node_id = ?hex_prefix(&outcome.node_id),
            local_addr = %local_addr,
            gcs_address = %config.gcs_address,
            "rayd-raylet: registered with GCS"
        );

        let (hb_shutdown_tx, hb_shutdown_rx) = oneshot::channel::<()>();
        let heartbeat_join = tokio::spawn(run_heartbeat(
            gcs.clone(),
            outcome.node_id,
            config.heartbeat_interval,
            hb_shutdown_rx,
        ));

        // Phase 4.3.3c: subscribe to GCS node events. Drives the local
        // NodeIndex (peer-address cache) and forwards Alive→Dead
        // transitions to the OwnerSink so borrowers can surface
        // OwnerDied without polling list_nodes.
        let node_index = Arc::new(NodeIndex::new());
        let (watch_shutdown_tx, watch_shutdown_rx) = oneshot::channel::<()>();
        let watch_join = tokio::spawn(run_watch_nodes(
            gcs.clone(),
            Arc::clone(&node_index),
            config.owner_sink.clone(),
            watch_shutdown_rx,
        ));

        let metrics_handle = if let (Some(addr), Some(m)) = (config.metrics_bind, metrics.clone()) {
            Some(start_metrics_server(addr, m).await?)
        } else {
            None
        };

        Ok(RayletHandle {
            local_addr,
            node_id: outcome.node_id,
            cluster_session_id: outcome.cluster_session_id,
            gcs,
            directory,
            node_index,
            metrics_bag: metrics,
            server_shutdown_tx: Some(server_shutdown_tx),
            server_join: Some(server_join),
            heartbeat_shutdown_tx: Some(hb_shutdown_tx),
            heartbeat_join: Some(heartbeat_join),
            watch_shutdown_tx: Some(watch_shutdown_tx),
            watch_join: Some(watch_join),
            metrics: metrics_handle,
        })
    }
}

/// Owns a running raylet. Drop or call `shutdown()` to stop it.
#[derive(Debug)]
pub struct RayletHandle {
    local_addr: SocketAddr,
    node_id: [u8; 16],
    cluster_session_id: [u8; 16],
    gcs: GcsClient,
    directory: Arc<ObjectDirectory>,
    /// Phase 4.3.3c: live cache of cluster node state, fed by the
    /// `WatchNodes` subscription. Read by `node_status` for the
    /// driver-side owner-liveness gate; the subscriber writes into
    /// it on every event.
    node_index: Arc<NodeIndex>,
    /// Metric handles cloned out of the bag built during `start`.
    /// `None` when metrics aren't enabled (`metrics_bind = None`).
    /// Used by `node_status` to bump the lookup-outcome counter
    /// without re-checking whether the HTTP endpoint is up.
    metrics_bag: Option<Metrics>,
    metrics: Option<MetricsServerHandle>,
    server_shutdown_tx: Option<oneshot::Sender<()>>,
    server_join: Option<JoinHandle<()>>,
    heartbeat_shutdown_tx: Option<oneshot::Sender<()>>,
    heartbeat_join: Option<JoinHandle<()>>,
    watch_shutdown_tx: Option<oneshot::Sender<()>>,
    watch_join: Option<JoinHandle<()>>,
}

impl RayletHandle {
    /// The address the gRPC server actually bound to.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 16-byte node id assigned by the GCS at registration.
    #[must_use]
    pub fn node_id(&self) -> [u8; 16] {
        self.node_id
    }

    /// 16-byte cluster session id assigned by the GCS we connected to.
    #[must_use]
    pub fn cluster_session_id(&self) -> [u8; 16] {
        self.cluster_session_id
    }

    /// Add this raylet's own node id to the directory entry for
    /// `object_id`. Used by the driver after a `put()` so peers can
    /// dial us for `Pull`. Skips the owner-side borrower book —
    /// that's only for tracking PEER borrowers; self-registration
    /// must NOT count as a peer pin or the object would never free.
    pub fn register_self(&self, object_id: [u8; 28]) {
        self.directory.register(object_id, self.node_id);
    }

    /// Look up the cached status of `node_id` from the local
    /// `WatchNodes`-driven node index. Returns `None` when the
    /// subscriber hasn't yet observed this node — callers should
    /// fall back to a `list_nodes()` RPC for a fresh answer. The
    /// index is updated on every Register/Drain/sweep flip so under
    /// steady state this is the freshest signal available short of
    /// a synchronous RPC. Phase 4.3.3c.
    #[must_use]
    pub fn node_status(&self, node_id: &[u8; 16]) -> Option<rayd_gcs::NodeStatus> {
        let result = self.node_index.status_of(node_id);
        if let Some(metrics) = &self.metrics_bag {
            let outcome = if result.is_some() { "hit" } else { "miss" };
            metrics
                .node_status_lookups_total
                .with_label_values(&[outcome])
                .inc();
        }
        result
    }

    /// Remove this raylet's own node id from the directory entry for
    /// `object_id`. Called by the driver glue (rayd-py) after the
    /// owner's `CoreWorker` frees the local plasma copy, so peers
    /// stop seeing us as a holder. Idempotent and lock-free.
    pub fn deregister_self(&self, object_id: [u8; 28]) {
        self.directory.remove(object_id, self.node_id);
    }

    /// The address of the Prometheus `/metrics` endpoint, when enabled.
    #[must_use]
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics.as_ref().map(MetricsServerHandle::local_addr)
    }

    /// Stop heartbeats, drain the node, stop the gRPC server.
    /// Best-effort: errors at any step are logged and swallowed.
    pub async fn shutdown(mut self) {
        // Stop the metrics endpoint first so its handler doesn't
        // race with the directory teardown that follows.
        if let Some(m) = self.metrics.take() {
            m.shutdown().await;
        }
        // Stop the WatchNodes subscriber early — we don't need any
        // more cluster-state events while we're tearing down, and the
        // pending stream blocks the GcsClient channel.
        if let Some(tx) = self.watch_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.watch_join.take() {
            let _ = handle.await;
        }
        // Stop heartbeats first so the loop isn't racing the drain RPC.
        if let Some(tx) = self.heartbeat_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.heartbeat_join.take() {
            let _ = handle.await;
        }
        if let Err(e) = self.gcs.drain(self.node_id).await {
            warn!(error = %e, "rayd-raylet: drain rpc failed at shutdown");
        }
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_join.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for RayletHandle {
    fn drop(&mut self) {
        // Best-effort signal; without async we can't await joins or send
        // the drain RPC. Prefer `shutdown().await` for clean teardown.
        if let Some(tx) = self.heartbeat_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.watch_shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.server_shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn run_heartbeat(
    mut client: GcsClient,
    node_id: [u8; 16],
    interval_dur: Duration,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut interval = tokio::time::interval(interval_dur);
    // Skip the immediate tick — register set last_heartbeat already.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            _ = interval.tick() => {
                if let Err(e) = client.heartbeat(node_id).await {
                    debug!(error = %e, "rayd-raylet: heartbeat rpc failed (will retry)");
                }
            }
        }
    }
}

fn hex_prefix(id: &[u8; 16]) -> String {
    id.iter().take(4).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
