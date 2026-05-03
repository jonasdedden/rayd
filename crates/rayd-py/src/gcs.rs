//! Driver-side GCS + raylet binding.
//!
//! When `RAYD_GCS_ADDRESS` is set on `init()`, the runtime spins up:
//! 1. A tokio runtime shared by the GCS client and the local raylet.
//! 2. A `Raylet` bound to a loopback port: it registers this driver
//!    with the GCS (so peers can dial us back for `Pull`) and keeps
//!    the heartbeat alive.
//! 3. A `GcsClient` connection that the driver uses for `add_job` /
//!    `list_*` and the eventual ownership-directory queries.
//!
//! `shutdown()` reverses the order: `mark_job_finished` first (so the
//! GCS records a graceful exit before the node disappears), then
//! `Raylet::shutdown` (drain + stop gRPC), then drop the runtime.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rayd_gcs::{ActorInfo, GcsClient, GcsClientError, JobInfo, NodeInfo, Resources};
use rayd_raylet::{
    LocalFsBackend, LocalObjectManager, ObjectTransportClientError, OwnerSink, PulledObject,
    Raylet, RayletConfig, RayletHandle, RayletStartError, SpillError,
};

use crate::raylet_pool::{should_evict, RayletConnPool};
use thiserror::Error;
use tokio::runtime::Runtime;
use tracing::info;

/// Driver-side raylet heartbeat interval. The default GCS sweeper
/// expires nodes after 10 s, so 2 s gives 5x headroom against
/// transient stalls. Override via `RAYD_HEARTBEAT_INTERVAL_MS`.
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const HEARTBEAT_INTERVAL_ENV: &str = "RAYD_HEARTBEAT_INTERVAL_MS";

/// When set to `host:port`, the driver-attached raylet exposes its
/// Prometheus `/metrics` endpoint on that address. Useful for local
/// development with Grafana — the dev cluster setup wires this so
/// the dashboard's raylet-side panels (`NodeIndex` hit ratio, Pull/Push
/// rates, directory size, ...) get real data without needing to run
/// a separate `rayd start --address` worker.
const RAYLET_METRICS_BIND_ENV: &str = "RAYD_RAYLET_METRICS_BIND";

fn heartbeat_interval() -> Duration {
    std::env::var(HEARTBEAT_INTERVAL_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map_or(DEFAULT_HEARTBEAT_INTERVAL, Duration::from_millis)
}

/// Parse `RAYD_RAYLET_METRICS_BIND` into a `SocketAddr`, returning
/// `None` when unset or unparseable. A bad value warns once but
/// doesn't fail driver startup — metrics are observability, not a
/// hard dependency.
fn raylet_metrics_bind() -> Option<SocketAddr> {
    let raw = std::env::var(RAYLET_METRICS_BIND_ENV).ok()?;
    match raw.parse() {
        Ok(addr) => Some(addr),
        Err(e) => {
            tracing::warn!(
                error = %e,
                value = %raw,
                "rayd-py: ignoring malformed {RAYLET_METRICS_BIND_ENV}"
            );
            None
        }
    }
}

/// Errors observable while standing up the GCS binding.
#[derive(Debug, Error)]
pub(crate) enum GcsBindingError {
    #[error("invalid RAYD_GCS_ADDRESS: {0}")]
    InvalidAddress(String),
    #[error("tokio runtime: {0}")]
    Runtime(#[source] std::io::Error),
    #[error("gcs: {0}")]
    Gcs(#[from] GcsClientError),
    #[error("raylet: {0}")]
    Raylet(#[from] RayletStartError),
    #[error("spill: {0}")]
    Spill(#[from] SpillError),
    #[error("spill tempdir: {0}")]
    SpillTempDir(#[source] std::io::Error),
}

/// Errors observable when issuing a remote `Pull` or directory call.
#[derive(Debug, Error)]
pub(crate) enum PullError {
    #[error("transport: {0}")]
    Transport(#[from] ObjectTransportClientError),
}

/// Owns the per-session GCS connection, the local raylet, and the ids
/// the GCS assigned at registration.
pub(crate) struct GcsBinding {
    runtime: Arc<Runtime>,
    client: Mutex<GcsClient>,
    raylet: Mutex<Option<RayletHandle>>,
    raylet_addr: SocketAddr,
    raylet_pool: Arc<RayletConnPool>,
    node_id: [u8; 16],
    job_id: [u8; 16],
    cluster_session_id: [u8; 16],
    driver_host: String,
    /// Per-session spill manager. Reachable from tests + the future
    /// spill-on-pressure policy. The raylet's `Pull` handler holds an
    /// `Arc` to the same instance.
    object_manager: Arc<LocalObjectManager>,
    /// Tempdir backing the spill manager's `LocalFsBackend`. Dropped
    /// at session shutdown so spilled files don't outlive the driver.
    _spill_dir: tempfile::TempDir,
}

impl std::fmt::Debug for GcsBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsBinding")
            .field("node_id_prefix", &hex_prefix(&self.node_id))
            .field("job_id_prefix", &hex_prefix(&self.job_id))
            .field("driver_host", &self.driver_host)
            .field("raylet_addr", &self.raylet_addr)
            .finish()
    }
}

impl GcsBinding {
    /// Connect to the GCS at `RAYD_GCS_ADDRESS`, start a local raylet
    /// (which registers as a node), and add a job under that node.
    pub(crate) fn connect_and_register(
        gcs_address: &str,
        plasma_socket: &Path,
        owner_sink: Arc<dyn OwnerSink>,
    ) -> Result<Self, GcsBindingError> {
        let addr: SocketAddr = gcs_address
            .parse()
            .map_err(|e| GcsBindingError::InvalidAddress(format!("{gcs_address}: {e}")))?;

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("rayd-gcs-rt")
            .build()
            .map_err(GcsBindingError::Runtime)?;
        let runtime = Arc::new(runtime);

        let driver_host = gethostname::gethostname()
            .into_string()
            .unwrap_or_else(|_| "localhost".to_owned());
        let driver_pid = std::process::id();

        // Spill manager — backs the raylet's `Pull` restore-on-miss
        // path with a per-session `LocalFsBackend`. Tempdir-rooted so
        // spilled files are cleaned up when the session shuts down.
        // No automatic eviction policy yet (Phase 6.5); the manager
        // here is purely a recovery substrate.
        let spill_dir = tempfile::Builder::new()
            .prefix("rayd-spill-")
            .tempdir()
            .map_err(GcsBindingError::SpillTempDir)?;
        let spill_backend = Arc::new(LocalFsBackend::new(spill_dir.path())?);
        let object_manager = Arc::new(LocalObjectManager::new(spill_backend));

        // 1) Local raylet — binds on all interfaces so peers reaching
        //    us via the advertised hostname (which may resolve to a
        //    non-127.0.0.1 loopback like Debian's 127.0.1.1, or to a
        //    LAN IP) can dial through. Registers with the GCS and
        //    runs the heartbeat task internally.
        let raylet_config = RayletConfig {
            gcs_address: addr,
            bind: "0.0.0.0:0".parse().expect("0.0.0.0:0"),
            advertise_host: driver_host.clone(),
            plasma_socket: plasma_socket.to_path_buf(),
            resources: default_resources(),
            heartbeat_interval: heartbeat_interval(),
            owner_sink: Some(owner_sink),
            object_manager: Some(Arc::clone(&object_manager)),
            metrics_bind: raylet_metrics_bind(),
        };
        let raylet_handle = runtime.block_on(Raylet::start(raylet_config))?;
        let raylet_addr = raylet_handle.local_addr();
        let node_id = raylet_handle.node_id();
        let cluster_session_id = raylet_handle.cluster_session_id();

        // 2) GCS client for job tracking + future directory queries.
        let mut client = runtime.block_on(GcsClient::connect(addr))?;
        let job_id =
            runtime.block_on(client.add_job(driver_host.clone(), driver_pid, Some(node_id)))?;

        info!(
            node_id = ?hex_prefix(&node_id),
            job_id = ?hex_prefix(&job_id),
            raylet_addr = %raylet_addr,
            "rayd-py: registered with GCS at {addr}"
        );

        Ok(Self {
            runtime,
            client: Mutex::new(client),
            raylet: Mutex::new(Some(raylet_handle)),
            raylet_addr,
            raylet_pool: Arc::new(RayletConnPool::new()),
            node_id,
            job_id,
            cluster_session_id,
            driver_host,
            object_manager,
            _spill_dir: spill_dir,
        })
    }

    /// Borrow the per-session spill manager. Reachable from tests
    /// and from the future spill-on-pressure policy via
    /// `runtime::with_gcs(|b| b.object_manager())`. Dead-code-allowed
    /// while the Python-side helpers that drive it haven't landed
    /// yet (Phase 6.5).
    #[allow(dead_code)]
    pub(crate) fn object_manager(&self) -> &Arc<LocalObjectManager> {
        &self.object_manager
    }

    /// Borrow the binding's tokio runtime handle. Used by rayd-py's
    /// init path to enter the runtime context before bringing up
    /// pieces that need a current runtime — notably the OTLP span
    /// exporter, whose tonic gRPC client requires a running reactor.
    pub(crate) fn runtime_handle(&self) -> &tokio::runtime::Handle {
        self.runtime.handle()
    }

    pub(crate) fn node_id(&self) -> [u8; 16] {
        self.node_id
    }

    pub(crate) fn job_id(&self) -> [u8; 16] {
        self.job_id
    }

    pub(crate) fn cluster_session_id(&self) -> [u8; 16] {
        self.cluster_session_id
    }

    pub(crate) fn raylet_addr(&self) -> SocketAddr {
        self.raylet_addr
    }

    #[allow(dead_code)] // exposed for future diagnostic pyfunctions
    pub(crate) fn driver_host(&self) -> &str {
        &self.driver_host
    }

    /// Tell the local raylet to drop our self-entry from the directory
    /// for `object_id`. Called from the `CoreWorker`'s free-callback
    /// after a local unpin, so peers stop seeing us as a holder.
    pub(crate) fn deregister_self(&self, object_id: [u8; 28]) {
        if let Some(handle) = self.raylet.lock().as_ref() {
            handle.deregister_self(object_id);
        }
    }

    pub(crate) fn list_nodes(&self) -> Result<Vec<NodeInfo>, GcsClientError> {
        let mut client = self.client.lock();
        self.runtime.block_on(client.list())
    }

    /// Fast push-driven liveness check (Phase 4.3.3c). Reads the local
    /// raylet's `WatchNodes`-driven node index. `None` means the
    /// subscriber hasn't observed `node_id` yet (or no raylet is
    /// attached) — caller should fall back to a `list_nodes()` RPC.
    pub(crate) fn node_status(&self, node_id: [u8; 16]) -> Option<rayd_gcs::NodeStatus> {
        self.raylet
            .lock()
            .as_ref()
            .and_then(|r| r.node_status(&node_id))
    }

    pub(crate) fn list_jobs(&self) -> Result<Vec<JobInfo>, GcsClientError> {
        let mut client = self.client.lock();
        self.runtime.block_on(client.list_jobs())
    }

    /// Register a named actor with the GCS. Owner is this driver.
    ///
    /// `driver_actor_host`/`driver_actor_port` advertise this driver's
    /// actor-RPC TCP listener. Empty/`0` means "no listener" — peers
    /// that look the actor up by name will get back an entry that
    /// can't be invoked cross-driver.
    pub(crate) fn register_actor(
        &self,
        name: &str,
        actor_id: [u8; 16],
        driver_actor_host: &str,
        driver_actor_port: u16,
    ) -> Result<(), GcsClientError> {
        let mut client = self.client.lock();
        self.runtime.block_on(client.register_actor(
            name.to_owned(),
            actor_id,
            Some(self.node_id),
            std::process::id(),
            driver_actor_host.to_owned(),
            driver_actor_port,
        ))
    }

    /// Look up a named actor in the GCS. `Ok(None)` if not registered.
    pub(crate) fn get_actor(&self, name: &str) -> Result<Option<ActorInfo>, GcsClientError> {
        let mut client = self.client.lock();
        self.runtime.block_on(client.get_actor(name.to_owned()))
    }

    /// Remove a named-actor entry. Caller's `actor_id` must match the
    /// registered entry.
    pub(crate) fn unregister_actor(
        &self,
        name: &str,
        actor_id: [u8; 16],
    ) -> Result<(), GcsClientError> {
        let mut client = self.client.lock();
        self.runtime
            .block_on(client.unregister_actor(name.to_owned(), actor_id))
    }

    pub(crate) fn list_actors(&self) -> Result<Vec<ActorInfo>, GcsClientError> {
        let mut client = self.client.lock();
        self.runtime.block_on(client.list_actors())
    }

    /// Register self as a holder of `object_id` at the local raylet's
    /// directory. Used after a `put()` so peers can dial us for `Pull`.
    /// Crucially does NOT route through the gRPC handler (which would
    /// also add us to our OWN `RefCounter.borrowers` via the sink and
    /// pin the object forever) — touches the directory directly.
    pub(crate) fn register_self_local(&self, object_id: [u8; 28]) {
        if let Some(handle) = self.raylet.lock().as_ref() {
            handle.register_self(object_id);
        }
    }

    /// Register an arbitrary `(object_id, holder_node_id)` at the LOCAL
    /// raylet's directory. Goes through the gRPC handler — same path
    /// peers take, so the `OwnerSink` also fires. Use
    /// `register_self_local` instead when registering ourselves.
    pub(crate) fn register_object_local(
        &self,
        object_id: [u8; 28],
        holder_node_id: [u8; 16],
    ) -> Result<(), PullError> {
        self.register_object_at(self.raylet_addr, object_id, holder_node_id)
    }

    /// Ask the LOCAL raylet which nodes hold `object_id`. Empty when
    /// the directory has no entry — that's not an error.
    pub(crate) fn get_object_locations_local(
        &self,
        object_id: [u8; 28],
    ) -> Result<Vec<[u8; 16]>, PullError> {
        self.get_object_locations_at(self.raylet_addr, object_id)
    }

    /// Push `(metadata, data)` into a (possibly remote) raylet's
    /// local plasma under `object_id`. Returns when the seal completes.
    /// Idempotent on `AlreadyExists`. Reuses the pool's cached channel
    /// and evicts on `Unavailable`.
    pub(crate) fn push_to(
        &self,
        addr: SocketAddr,
        object_id: [u8; 28],
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> Result<(), PullError> {
        let pool = Arc::clone(&self.raylet_pool);
        self.runtime.block_on(async move {
            let mut client = pool.client(addr).await?;
            match client.push(object_id.to_vec(), metadata, data).await {
                Ok(()) => Ok(()),
                Err(e) => {
                    if should_evict(&e) {
                        pool.evict(addr);
                    }
                    Err(e.into())
                }
            }
        })
    }

    /// Pull `object_id` from a (possibly remote) raylet at `addr`.
    /// Returns `(metadata, data)`. Reuses the pool's cached channel
    /// and evicts it on `Unavailable` so the next call rebuilds.
    pub(crate) fn pull_from(
        &self,
        addr: SocketAddr,
        object_id: [u8; 28],
    ) -> Result<PulledObject, PullError> {
        let pool = Arc::clone(&self.raylet_pool);
        self.runtime.block_on(async move {
            let mut client = pool.client(addr).await?;
            match client.pull(object_id.to_vec()).await {
                Ok(p) => Ok(p),
                Err(e) => {
                    if should_evict(&e) {
                        pool.evict(addr);
                    }
                    Err(e.into())
                }
            }
        })
    }

    /// Ask a (possibly remote) raylet at `addr` which nodes hold
    /// `object_id`. Used by the cross-node fetch path to query the
    /// owner-raylet's directory. Reuses the pool's cached channel
    /// and evicts on `Unavailable`.
    pub(crate) fn get_object_locations_at(
        &self,
        addr: SocketAddr,
        object_id: [u8; 28],
    ) -> Result<Vec<[u8; 16]>, PullError> {
        let pool = Arc::clone(&self.raylet_pool);
        self.runtime.block_on(async move {
            let mut client = pool.client(addr).await?;
            match client.get_object_locations(object_id.to_vec()).await {
                Ok(locs) => Ok(locs.node_ids),
                Err(e) => {
                    if should_evict(&e) {
                        pool.evict(addr);
                    }
                    Err(e.into())
                }
            }
        })
    }

    /// Notify the owner-raylet identified by `owner_node_id` that this
    /// driver no longer holds a ref to `object_id`. Resolves the
    /// owner's raylet address via the GCS, then dispatches the RPC.
    /// Best-effort: errors are logged and swallowed (drop semantics).
    pub(crate) fn notify_owner_of_drop(&self, owner_node_id: [u8; 16], object_id: [u8; 28]) {
        // Local lookup: find the owner-raylet's address from list_nodes.
        let nodes = match self.list_nodes() {
            Ok(ns) => ns,
            Err(e) => {
                tracing::debug!(error = %e, "rayd-py: list_nodes for drop notify failed");
                return;
            }
        };
        let Some(addr) = raylet_addr_for(&nodes, owner_node_id) else {
            // Owner already gone from GCS — nothing to notify.
            return;
        };
        self.notify_ref_removed_at(addr, object_id, self.node_id);
    }

    /// Tell a (possibly remote) raylet at `addr` that `borrower_node_id`'s
    /// last `ObjectRef` for `object_id` just dropped. Best-effort: a
    /// transport error is logged but not propagated, since this fires
    /// from `Drop` and there's nobody to bubble the error up to.
    pub(crate) fn notify_ref_removed_at(
        &self,
        addr: SocketAddr,
        object_id: [u8; 28],
        borrower_node_id: [u8; 16],
    ) {
        let pool = Arc::clone(&self.raylet_pool);
        let result: Result<(), PullError> = self.runtime.block_on(async move {
            let mut client = pool.client(addr).await?;
            match client
                .wait_for_ref_removed(object_id.to_vec(), borrower_node_id)
                .await
            {
                Ok(()) => Ok(()),
                Err(e) => {
                    if should_evict(&e) {
                        pool.evict(addr);
                    }
                    Err(e.into())
                }
            }
        });
        if let Err(e) = result {
            tracing::warn!(error = %e, %addr, "rayd-py: WaitForRefRemoved RPC failed");
        }
    }

    /// Tell a (possibly remote) raylet at `addr` that `holder_node_id`
    /// holds `object_id`. Used by the cross-node fetch path to notify
    /// the owner-raylet that a new replica was sealed locally. Reuses
    /// the pool's cached channel and evicts on `Unavailable`.
    pub(crate) fn register_object_at(
        &self,
        addr: SocketAddr,
        object_id: [u8; 28],
        holder_node_id: [u8; 16],
    ) -> Result<(), PullError> {
        let pool = Arc::clone(&self.raylet_pool);
        self.runtime.block_on(async move {
            let mut client = pool.client(addr).await?;
            match client
                .register_object(object_id.to_vec(), holder_node_id)
                .await
            {
                Ok(()) => Ok(()),
                Err(e) => {
                    if should_evict(&e) {
                        pool.evict(addr);
                    }
                    Err(e.into())
                }
            }
        })
    }

    /// Mark the job finished, then drain + stop the raylet, then drop
    /// the runtime. Best effort: errors are logged and swallowed.
    pub(crate) fn shutdown(self) {
        let Self {
            runtime,
            client,
            raylet,
            job_id,
            ..
        } = self;
        let mut client = client.into_inner();
        let raylet_handle = raylet.into_inner();
        runtime.block_on(async move {
            if let Err(e) = client.mark_job_finished(job_id, "").await {
                tracing::warn!(error = %e, "rayd-py: mark_job_finished failed at shutdown");
            }
            if let Some(handle) = raylet_handle {
                handle.shutdown().await;
            }
        });
        // `runtime: Arc<Runtime>` dropped here; tokio shuts down its
        // worker threads. We're outside the runtime so the drop is safe.
        drop(runtime);
    }
}

/// Resolve `node_id` to a `SocketAddr` from a GCS `list_nodes` snapshot.
/// Hostname → IP resolution mirrors what `_native.fetch_object` does.
fn raylet_addr_for(nodes: &[NodeInfo], node_id: [u8; 16]) -> Option<SocketAddr> {
    use std::net::ToSocketAddrs;
    for n in nodes {
        let addr = n.address.as_ref()?;
        if addr.node_id.len() != 16 || addr.node_id.as_slice() != node_id.as_slice() {
            continue;
        }
        let port = u16::try_from(addr.port).ok()?;
        let resolved = (addr.host.as_str(), port).to_socket_addrs().ok()?.next()?;
        return Some(resolved);
    }
    None
}

fn default_resources() -> Resources {
    Resources {
        // Use the host's logical CPU count as a placeholder. Per-task
        // resource accounting lands when the scheduler does.
        num_cpus: u32::try_from(num_logical_cpus()).unwrap_or(1),
        num_gpus: 0,
        memory_bytes: 0,
    }
}

fn num_logical_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZero::get)
        .unwrap_or(1)
}

fn hex_prefix(id: &[u8; 16]) -> String {
    id.iter().take(4).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
