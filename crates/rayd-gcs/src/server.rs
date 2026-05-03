//! gRPC server for the GCS.
//!
//! `GcsServer::start` binds the requested address, spawns the tonic
//! server on the current tokio runtime, and returns a `GcsServerHandle`
//! whose `Drop` (or explicit `shutdown().await`) tears the server down.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Server;
use tracing::info;

use crate::actor_proto::actor_registry_server::ActorRegistryServer;
use crate::actor_service::{ActorRegistryService, ActorsRegistry};
use crate::job_proto::job_registry_server::JobRegistryServer;
use crate::job_service::{JobRegistryService, JobsRegistry};
use crate::metrics::{start_metrics_server, Metrics, MetricsServerHandle};
use crate::proto::node_registry_server::NodeRegistryServer;
use crate::service::{NodeRegistryService, Registry};

/// Tunables for `GcsServer::start_with_config`. Defaults give 10 s for a
/// node to miss its first heartbeat before being marked `Dead`, with the
/// sweeper running every second.
#[derive(Debug, Clone, Copy)]
pub struct GcsServerConfig {
    /// How long since `last_heartbeat_unix_ms` before an `Alive` node is
    /// flipped to `Dead`. Zero disables expiry entirely (useful for tests
    /// that don't want any background liveness work).
    pub heartbeat_timeout: Duration,
    /// How often the sweeper runs. Capped at `heartbeat_timeout`.
    pub sweep_interval: Duration,
    /// Optional bind address for the Prometheus `/metrics` HTTP
    /// endpoint. `None` disables metrics entirely (no counters
    /// updated, no extra port bound).
    pub metrics_bind: Option<SocketAddr>,
}

impl Default for GcsServerConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(10),
            sweep_interval: Duration::from_secs(1),
            metrics_bind: None,
        }
    }
}

/// Errors observable when starting the GCS server.
#[derive(Debug, Error)]
pub enum GcsServerStartError {
    /// Could not bind the requested address.
    #[error("bind {addr} failed: {source}")]
    Bind {
        /// The address we attempted to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Tonic transport setup failure.
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Metrics endpoint setup failed (bind error or duplicate
    /// metric name in the registry).
    #[error("metrics: {0}")]
    Metrics(#[from] crate::metrics::MetricsStartError),
}

/// Server entry point. Construct via `start()`.
#[derive(Debug)]
pub struct GcsServer;

impl GcsServer {
    /// Bind `addr` and serve both registries with default config.
    pub async fn start(addr: SocketAddr) -> Result<GcsServerHandle, GcsServerStartError> {
        Self::start_with_config(addr, GcsServerConfig::default()).await
    }

    /// Bind `addr` and serve both registries. Spawns a sweeper task that
    /// expires stale `Alive` nodes per `config`. Both background tasks
    /// (the gRPC server and the sweeper) shut down on
    /// `GcsServerHandle::shutdown` or `Drop`.
    pub async fn start_with_config(
        addr: SocketAddr,
        config: GcsServerConfig,
    ) -> Result<GcsServerHandle, GcsServerStartError> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| GcsServerStartError::Bind { addr, source: e })?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| GcsServerStartError::Bind { addr, source: e })?;

        // Build metrics first (if requested) so service constructors
        // can take the bag and increment counters as RPCs land.
        let metrics = match config.metrics_bind {
            Some(_) => Some(Metrics::new()?),
            None => None,
        };

        let registry = Registry::new();
        let jobs_registry = JobsRegistry::new();
        let actors_registry = ActorsRegistry::new();
        let node_svc = NodeRegistryService::new(Arc::clone(&registry), metrics.clone());
        let job_svc = JobRegistryService::new(Arc::clone(&jobs_registry), metrics.clone());
        let actor_svc = ActorRegistryService::new(Arc::clone(&actors_registry), metrics.clone());

        // Standard `grpc.health.v1.Health` service. Mark each
        // tonic-generated service we serve as `SERVING` for the
        // duration of the GCS lifetime; the empty key "" reports
        // overall server health, which is what most probes check.
        // Dropping the reporter on shutdown auto-flips status to
        // `NOT_SERVING` so a probe in flight sees the failure
        // promptly.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NodeRegistryServer<NodeRegistryService>>()
            .await;
        health_reporter
            .set_serving::<JobRegistryServer<JobRegistryService>>()
            .await;
        health_reporter
            .set_serving::<ActorRegistryServer<ActorRegistryService>>()
            .await;

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let server_future = Server::builder()
            .add_service(health_service)
            .add_service(NodeRegistryServer::new(node_svc))
            .add_service(JobRegistryServer::new(job_svc))
            .add_service(ActorRegistryServer::new(actor_svc))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async move {
                    let _ = shutdown_rx.await;
                },
            );

        let join_handle = tokio::spawn(async move {
            if let Err(e) = server_future.await {
                tracing::warn!(error = %e, "rayd-gcs: server exited with error");
            }
        });

        let sweeper_handle = if config.heartbeat_timeout.is_zero() {
            None
        } else {
            let (sweeper_tx, sweeper_rx) = oneshot::channel::<()>();
            let registry_for_sweeper = Arc::clone(&registry);
            let metrics_for_sweeper = metrics.clone();
            let join = tokio::spawn(run_sweeper(
                registry_for_sweeper,
                config,
                sweeper_rx,
                metrics_for_sweeper,
            ));
            Some(SweeperHandle {
                shutdown_tx: Some(sweeper_tx),
                join_handle: Some(join),
            })
        };

        let metrics_handle = if let (Some(addr), Some(m)) = (config.metrics_bind, metrics.clone()) {
            Some(start_metrics_server(addr, m).await?)
        } else {
            None
        };

        info!(
            %local_addr,
            heartbeat_timeout_ms = u64::try_from(config.heartbeat_timeout.as_millis())
                .unwrap_or(u64::MAX),
            metrics_addr = ?metrics_handle.as_ref().map(MetricsServerHandle::local_addr),
            "rayd-gcs: NodeRegistry + JobRegistry + ActorRegistry listening",
        );
        Ok(GcsServerHandle {
            local_addr,
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(join_handle),
            sweeper: sweeper_handle,
            registry,
            jobs_registry,
            actors_registry,
            metrics: metrics_handle,
        })
    }
}

/// Background task: every `config.sweep_interval`, mark `Alive` nodes
/// with stale heartbeats as `Dead`. Exits cleanly when `shutdown_rx` fires.
async fn run_sweeper(
    registry: Arc<Registry>,
    config: GcsServerConfig,
    mut shutdown_rx: oneshot::Receiver<()>,
    metrics: Option<Metrics>,
) {
    let interval_dur = config.sweep_interval.min(config.heartbeat_timeout);
    let mut interval = tokio::time::interval(interval_dur);
    // Skip the first immediate tick — there's nothing stale yet at startup.
    interval.tick().await;
    loop {
        tokio::select! {
            _ = &mut shutdown_rx => break,
            _ = interval.tick() => {
                let now_ms = unix_ms();
                let timeout_ms = u64::try_from(config.heartbeat_timeout.as_millis())
                    .unwrap_or(u64::MAX);
                let deadline_ms = now_ms.saturating_sub(timeout_ms);
                let _flipped = registry.expire_stale(deadline_ms, metrics.as_ref());
            }
        }
    }
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Owns a running GCS server. Call `shutdown().await` to stop it cleanly,
/// or just drop the handle (which triggers the same path).
#[derive(Debug)]
pub struct GcsServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
    sweeper: Option<SweeperHandle>,
    registry: Arc<Registry>,
    jobs_registry: Arc<JobsRegistry>,
    actors_registry: Arc<ActorsRegistry>,
    metrics: Option<MetricsServerHandle>,
}

#[derive(Debug)]
struct SweeperHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl GcsServerHandle {
    /// The address actually bound (informative when caller passed `:0`).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop the server and await its task. Idempotent.
    pub async fn shutdown(mut self) {
        self.shutdown_inner().await;
    }

    /// Number of nodes currently registered.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.registry.len()
    }

    /// Number of jobs the GCS knows about (running + finished).
    #[must_use]
    pub fn job_count(&self) -> usize {
        self.jobs_registry.len()
    }

    /// Number of named actors currently registered.
    #[must_use]
    pub fn actor_count(&self) -> usize {
        self.actors_registry.len()
    }

    /// Address of the Prometheus `/metrics` endpoint, when enabled.
    #[must_use]
    pub fn metrics_addr(&self) -> Option<SocketAddr> {
        self.metrics.as_ref().map(MetricsServerHandle::local_addr)
    }

    async fn shutdown_inner(&mut self) {
        // Stop the metrics endpoint up front so its handler doesn't
        // race with the registry teardown that follows.
        if let Some(m) = self.metrics.take() {
            m.shutdown().await;
        }
        // Tell the sweeper first so it stops touching the registry, then
        // tear the gRPC server down.
        if let Some(mut sweeper) = self.sweeper.take() {
            if let Some(tx) = sweeper.shutdown_tx.take() {
                let _ = tx.send(());
            }
            if let Some(handle) = sweeper.join_handle.take() {
                let _ = handle.await;
            }
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for GcsServerHandle {
    fn drop(&mut self) {
        // Best-effort signal; the spawned tasks will exit on their own.
        // We can't `await` here, so the join may take longer than
        // explicit `shutdown().await`. For test cleanliness, prefer the
        // explicit form.
        if let Some(sweeper) = self.sweeper.as_mut() {
            if let Some(tx) = sweeper.shutdown_tx.take() {
                let _ = tx.send(());
            }
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// Pull `tokio_stream` only for the `TcpListenerStream` wrapper used above.
// Adding it to deps explicitly so cargo resolves it.
#[allow(unused_imports)]
use tokio_stream as _tokio_stream_dep;
