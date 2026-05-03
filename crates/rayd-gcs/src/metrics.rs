//! GCS Prometheus metrics + `/metrics` HTTP server.
//!
//! Phase 7.4 ships a small set of counters and gauges keyed by the
//! GCS's own state — node/job/actor counts, plus call-rate counters
//! for the noisy RPCs (`Register`, `Heartbeat`). Production
//! deployments scrape this endpoint with a Prometheus server (or a
//! compatible agent like the `OTel` collector).
//!
//! Metric naming follows the standard Prometheus conventions:
//! `rayd_gcs_<subject>_<unit_or_total>`. All metrics live in a custom
//! `Registry` so the global default registry isn't polluted — keeps
//! us isolated from any host process that might also use
//! `prometheus::default_registry()`.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use prometheus::{Encoder as _, IntCounter, IntGauge, Registry as PromRegistry, TextEncoder};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

/// Errors observable while bringing up the metrics endpoint.
#[derive(Debug, Error)]
pub enum MetricsStartError {
    /// Bind failed.
    #[error("metrics bind {addr} failed: {source}")]
    Bind {
        /// The address we attempted to bind.
        addr: SocketAddr,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// Failed to register a metric (duplicate name, malformed labels, …).
    #[error("metrics register: {0}")]
    Register(#[from] prometheus::Error),
}

/// Bag of metric handles, one per cardinality slot.
///
/// `Arc`-share into the gRPC service handlers so RPC entry points can
/// `.inc()` a counter without touching the encoder. Encoding happens
/// lazily when the `/metrics` endpoint is scraped.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Number of `Register` RPCs accepted (cumulative).
    pub register_node_total: IntCounter,
    /// Number of `Heartbeat` RPCs accepted (cumulative).
    pub heartbeat_received_total: IntCounter,
    /// Currently `Alive`-status nodes in the directory.
    pub nodes_alive: IntGauge,
    /// All nodes the GCS has seen this session, regardless of status.
    pub nodes_total: IntGauge,
    /// Jobs in `JOB_STATUS_RUNNING` state.
    pub jobs_running: IntGauge,
    /// Actors registered in the named-actor directory.
    pub actors_total: IntGauge,
    /// Cumulative count of `NodeEvent`s broadcast to `WatchNodes`
    /// subscribers (Phase 4.3.3c). Compare against subscriber-side
    /// receive counters to spot back-pressure or dropped events.
    pub watch_events_published_total: IntCounter,
    registry: Arc<PromRegistry>,
}

impl Metrics {
    /// Build the metrics bag and register every handle on a fresh
    /// `Registry`. Call once per `GcsServer` instance.
    pub fn new() -> Result<Self, MetricsStartError> {
        let registry = PromRegistry::new();
        let register_node_total = IntCounter::new(
            "rayd_gcs_register_node_total",
            "Cumulative count of Register RPCs accepted.",
        )?;
        let heartbeat_received_total = IntCounter::new(
            "rayd_gcs_heartbeat_received_total",
            "Cumulative count of Heartbeat RPCs accepted.",
        )?;
        let nodes_alive = IntGauge::new(
            "rayd_gcs_nodes_alive",
            "Number of nodes currently in the Alive state.",
        )?;
        let nodes_total = IntGauge::new(
            "rayd_gcs_nodes_total",
            "All nodes the GCS has seen this session, regardless of status.",
        )?;
        let jobs_running = IntGauge::new(
            "rayd_gcs_jobs_running",
            "Jobs currently in the Running state.",
        )?;
        let actors_total = IntGauge::new(
            "rayd_gcs_actors_total",
            "Named actors registered in the directory.",
        )?;
        let watch_events_published_total = IntCounter::new(
            "rayd_gcs_watch_events_published_total",
            "Cumulative count of NodeEvents broadcast to WatchNodes subscribers.",
        )?;
        registry.register(Box::new(register_node_total.clone()))?;
        registry.register(Box::new(heartbeat_received_total.clone()))?;
        registry.register(Box::new(nodes_alive.clone()))?;
        registry.register(Box::new(nodes_total.clone()))?;
        registry.register(Box::new(jobs_running.clone()))?;
        registry.register(Box::new(actors_total.clone()))?;
        registry.register(Box::new(watch_events_published_total.clone()))?;
        Ok(Self {
            register_node_total,
            heartbeat_received_total,
            nodes_alive,
            nodes_total,
            jobs_running,
            actors_total,
            watch_events_published_total,
            registry: Arc::new(registry),
        })
    }

    /// Encode the current metric state in the Prometheus text format.
    /// Used by the `/metrics` HTTP handler. Visible for tests.
    pub fn encode_text(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf)?;
        String::from_utf8(buf)
            .map_err(|e| prometheus::Error::Msg(format!("metrics encoder produced non-UTF-8: {e}")))
    }
}

/// Owns a running metrics HTTP server. Drop or call `shutdown()` to stop it.
#[derive(Debug)]
pub struct MetricsServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsServerHandle {
    /// The address actually bound (informative when caller passed `:0`).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop the server and await its task. Idempotent.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for MetricsServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Bring up the `/metrics` HTTP endpoint on `addr`. The returned
/// handle owns the bound port; drop or call `shutdown()` to stop.
pub(crate) async fn start_metrics_server(
    addr: SocketAddr,
    metrics: Metrics,
) -> Result<MetricsServerHandle, MetricsStartError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;

    let app = Router::new()
        .route("/metrics", get(scrape_handler))
        .with_state(metrics);

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let join_handle = tokio::spawn(async move {
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
        if let Err(e) = server.await {
            warn!(error = %e, "rayd-gcs: metrics server exited with error");
        }
    });
    Ok(MetricsServerHandle {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    })
}

async fn scrape_handler(State(metrics): State<Metrics>) -> impl IntoResponse {
    match metrics.encode_text() {
        Ok(body) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; version=0.0.4")],
            body,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("Content-Type", "text/plain")],
            format!("encode failed: {e}"),
        ),
    }
}
