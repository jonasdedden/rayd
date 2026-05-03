//! Driver-side Prometheus metrics + `/metrics` HTTP server.
//!
//! Phase 7.4d. Same axum + prometheus shape as `rayd-gcs::metrics`,
//! but hosted by the rayd Python driver's runtime via `PyO3`. Builds
//! a dedicated single-thread tokio runtime so the metrics endpoint
//! is independent of whether GCS is attached or not — driver
//! observability shouldn't require a cluster.
//!
//! Counter bump points are scattered across `lib.rs` (put/get/
//! `get_settled`/`submit_task`) and `dispatcher.rs` (the completion-frame
//! handler). To keep the bump call sites cheap and threading-free,
//! the bag is stashed in a process-global `RwLock<Option<Arc<DriverMetrics>>>`
//! that the bumpers consult via `current()`. The lock (vs. a `OnceLock`)
//! lets `rayd.shutdown()` clear the slot so a subsequent `rayd.init()`
//! installs a fresh registry — without that, bumps would orphan into
//! a stale counter set the new HTTP server never serves.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use parking_lot::{Mutex, RwLock};
use prometheus::{
    core::Desc, proto, Encoder as _, IntCounter, IntGauge, Registry as PromRegistry, TextEncoder,
};
use rayd_core::CoreWorker;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// Env var that, when set, opens the `/metrics` HTTP endpoint at
/// the given `host:port`. Unset = no metrics.
pub(crate) const METRICS_BIND_ENV: &str = "RAYD_METRICS_BIND";

/// Errors observable while bringing up the metrics endpoint.
#[derive(Debug, Error)]
pub(crate) enum MetricsStartError {
    /// Bind failed.
    #[error("metrics bind {addr} failed: {source}")]
    Bind {
        /// The address we attempted to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Failed to register a metric.
    #[error("metrics register: {0}")]
    Register(#[from] prometheus::Error),
    /// Couldn't build the dedicated tokio runtime.
    #[error("metrics runtime: {0}")]
    Runtime(#[source] std::io::Error),
}

/// Driver-side metric handles. Cloned cheaply via internal Arcs so
/// counter bumps in hot paths are an atomic increment.
#[derive(Clone, Debug)]
pub(crate) struct DriverMetrics {
    /// Cumulative count of `submit_task` calls accepted.
    pub tasks_submitted_total: IntCounter,
    /// Cumulative count of dispatcher completion frames observed
    /// for tasks that returned a value.
    pub tasks_completed_total: IntCounter,
    /// Cumulative count of dispatcher completion frames observed
    /// for tasks that returned an error.
    pub tasks_failed_total: IntCounter,
    /// Cumulative count of `rayd.put(value)` calls.
    pub puts_total: IntCounter,
    /// Cumulative count of `rayd.get`/`rayd.get_settled` calls.
    pub gets_total: IntCounter,
    registry: Arc<PromRegistry>,
}

impl DriverMetrics {
    fn new(worker: &Arc<CoreWorker>) -> Result<Self, MetricsStartError> {
        let registry = PromRegistry::new();
        let tasks_submitted_total = IntCounter::new(
            "rayd_driver_tasks_submitted_total",
            "Cumulative count of submit_task calls accepted.",
        )?;
        let tasks_completed_total = IntCounter::new(
            "rayd_driver_tasks_completed_total",
            "Cumulative count of dispatcher completion frames for tasks that returned a value.",
        )?;
        let tasks_failed_total = IntCounter::new(
            "rayd_driver_tasks_failed_total",
            "Cumulative count of dispatcher completion frames for tasks that returned an error.",
        )?;
        let puts_total = IntCounter::new(
            "rayd_driver_puts_total",
            "Cumulative count of rayd.put calls.",
        )?;
        let gets_total = IntCounter::new(
            "rayd_driver_gets_total",
            "Cumulative count of rayd.get / rayd.get_settled calls.",
        )?;
        registry.register(Box::new(tasks_submitted_total.clone()))?;
        registry.register(Box::new(tasks_completed_total.clone()))?;
        registry.register(Box::new(tasks_failed_total.clone()))?;
        registry.register(Box::new(puts_total.clone()))?;
        registry.register(Box::new(gets_total.clone()))?;
        // `rayd_driver_refs_alive` is a Collector-impl gauge whose
        // value is read from the live RefCounter at scrape time. No
        // manual updates needed — keeps ref-drop call sites free of
        // metric bookkeeping.
        registry.register(Box::new(RefsAliveCollector::new(Arc::clone(worker))))?;
        Ok(Self {
            tasks_submitted_total,
            tasks_completed_total,
            tasks_failed_total,
            puts_total,
            gets_total,
            registry: Arc::new(registry),
        })
    }

    /// Encode the current metric state as Prometheus text.
    pub(crate) fn encode_text(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf)?;
        String::from_utf8(buf).map_err(|e| {
            prometheus::Error::Msg(format!("metrics encoder produced non-UTF-8: {e}"))
        })
    }
}

/// Process-global slot for the bump call sites to read. `RwLock`
/// (not `OnceLock`) so that `rayd.shutdown()` followed by `rayd.init()`
/// installs a fresh registry — important for tests and notebook
/// re-runs where a stale registry would silently swallow bumps that
/// the new HTTP server never serves.
static METRICS: RwLock<Option<Arc<DriverMetrics>>> = RwLock::new(None);

/// Read the global bag if metrics are enabled. `None` means metrics
/// are off and the bumper should be a no-op.
#[must_use]
pub(crate) fn current() -> Option<Arc<DriverMetrics>> {
    METRICS.read().clone()
}

/// Custom collector that reads the live `RefCounter`'s size at scrape
/// time. Avoids having every ref-drop call site bump a counter.
#[derive(Debug)]
struct RefsAliveCollector {
    worker: Arc<CoreWorker>,
    desc: Desc,
}

impl RefsAliveCollector {
    fn new(worker: Arc<CoreWorker>) -> Self {
        let desc = Desc::new(
            "rayd_driver_refs_alive".to_owned(),
            "Number of distinct ObjectIds the local RefCounter currently tracks.".to_owned(),
            Vec::new(),
            std::collections::HashMap::new(),
        )
        .expect("static name + help is valid");
        Self { worker, desc }
    }
}

impl prometheus::core::Collector for RefsAliveCollector {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let len = i64::try_from(self.worker.refs().len()).unwrap_or(i64::MAX);
        let gauge = IntGauge::new(self.desc.fq_name.clone(), self.desc.help.clone())
            .expect("name + help validated in `new`");
        gauge.set(len);
        gauge.collect()
    }
}

/// Owns a running metrics HTTP server. Drop or call `shutdown()` to stop it.
#[derive(Debug)]
pub(crate) struct MetricsServerHandle {
    // The runtime must outlive the join_handle. Held in a Mutex so
    // shutdown can move it out and drop it after the join.
    runtime: Arc<Mutex<Option<Runtime>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsServerHandle {
    /// Stop the server, await its task, and shut down the dedicated
    /// runtime. Idempotent.
    pub(crate) fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let runtime = self.runtime.lock().take();
        if let (Some(handle), Some(rt)) = (self.join_handle.take(), runtime) {
            // We need to join inside the runtime context. block_on
            // here is fine since we're being called from synchronous
            // code (the Session drop path).
            let _ = rt.block_on(handle);
            // `rt` drops here; tokio shuts down its worker thread.
            drop(rt);
        }
        // Clear the global so a subsequent `init_if_enabled` installs
        // a fresh registry rather than orphaning bumps into a stale
        // counter set that no `/metrics` endpoint serves.
        *METRICS.write() = None;
    }
}

impl Drop for MetricsServerHandle {
    fn drop(&mut self) {
        // Best-effort signal; without owning self we can't await the
        // join. Prefer explicit `shutdown()` for clean teardown.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Bring up the driver-side `/metrics` endpoint if `RAYD_METRICS_BIND`
/// is set in the environment. Builds a dedicated single-thread tokio
/// runtime, registers the metrics in the process-global slot, and
/// starts an axum scraper.
///
/// Returns:
/// - `Ok(Some(handle))` when metrics were enabled and the endpoint
///   bound successfully.
/// - `Ok(None)` when the env var is unset (the common case for users
///   who haven't opted in).
/// - `Err(_)` when the env var was set but setup failed (bind error,
///   malformed addr, runtime build failure, ...).
pub(crate) fn init_if_enabled(
    worker: &Arc<CoreWorker>,
) -> Result<Option<MetricsServerHandle>, MetricsStartError> {
    let Ok(bind_str) = std::env::var(METRICS_BIND_ENV) else {
        return Ok(None);
    };
    let addr: SocketAddr = bind_str.parse().map_err(|e| MetricsStartError::Bind {
        addr: "0.0.0.0:0".parse().expect("static"),
        source: std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {METRICS_BIND_ENV}={bind_str:?}: {e}"),
        ),
    })?;

    let metrics = DriverMetrics::new(worker)?;
    *METRICS.write() = Some(Arc::new(metrics.clone()));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(1)
        .thread_name("rayd-driver-metrics")
        .build()
        .map_err(MetricsStartError::Runtime)?;

    let listener = runtime
        .block_on(TcpListener::bind(addr))
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;

    let app = Router::new()
        .route("/metrics", get(scrape_handler))
        .with_state(metrics);

    info!(addr = %local_addr, "rayd-py: driver metrics server listening on /metrics");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join_handle = runtime.spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
        if let Err(e) = server.await {
            warn!(error = %e, "rayd-py: driver metrics server exited with error");
        }
    });

    Ok(Some(MetricsServerHandle {
        runtime: Arc::new(Mutex::new(Some(runtime))),
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    }))
}

async fn scrape_handler(State(metrics): State<DriverMetrics>) -> impl IntoResponse {
    match metrics.encode_text() {
        Ok(body) => (StatusCode::OK, [("Content-Type", "text/plain; version=0.0.4")], body),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("Content-Type", "text/plain")],
            format!("encode failed: {e}"),
        ),
    }
}
