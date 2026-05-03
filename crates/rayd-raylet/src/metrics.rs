//! Raylet Prometheus metrics + `/metrics` HTTP server.
//!
//! Phase 7.4b — same shape as `rayd-gcs::metrics`. Per-node daemon
//! reports its activity (pulls, pushes, directory size, spill
//! restore events) so a Prometheus scraper can graph cluster-wide
//! object-store traffic.
//!
//! All metrics live in a custom `Registry` to keep us isolated from
//! anything else in the host process that might use the global
//! default. Naming follows `rayd_raylet_<subject>_<unit_or_total>`.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use prometheus::{
    opts, Encoder as _, IntCounter, IntCounterVec, IntGauge, Registry as PromRegistry, TextEncoder,
};
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
    /// Failed to register a metric.
    #[error("metrics register: {0}")]
    Register(#[from] prometheus::Error),
}

/// Bag of metric handles, one per cardinality slot. Cloned into the
/// gRPC service handlers so RPC entry points can `.inc()` without
/// touching the encoder. Encoding happens lazily on `/metrics` scrape.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// `Pull` RPCs accepted.
    pub pull_total: IntCounter,
    /// `Push` RPCs accepted.
    pub push_total: IntCounter,
    /// `RegisterObject` RPCs accepted.
    pub register_object_total: IntCounter,
    /// `GetObjectLocations` RPCs accepted.
    pub get_object_locations_total: IntCounter,
    /// Successful restores from the spill backend on a `Pull` miss.
    pub spill_restore_total: IntCounter,
    /// Number of `(object_id → node_ids)` entries currently in the
    /// in-memory directory.
    pub directory_entries: IntGauge,
    /// Phase 4.3.3c: outcome of `RayletHandle::node_status` lookups
    /// against the live `WatchNodes`-driven node index. Labels:
    /// `outcome="hit"` (cache had the node), `outcome="miss"` (the
    /// cache hadn't observed it yet — caller falls back to a
    /// synchronous `list_nodes()` RPC). The hit ratio reflects how
    /// well the push-driven liveness gate is working; a falling ratio
    /// means subscribers are missing events or starting cold.
    pub node_status_lookups_total: IntCounterVec,
    registry: Arc<PromRegistry>,
}

impl Metrics {
    /// Build the metrics bag and register every handle. Call once
    /// per `Raylet` instance.
    pub fn new() -> Result<Self, MetricsStartError> {
        let registry = PromRegistry::new();
        let pull_total = IntCounter::new(
            "rayd_raylet_pull_total",
            "Cumulative count of Pull RPCs accepted.",
        )?;
        let push_total = IntCounter::new(
            "rayd_raylet_push_total",
            "Cumulative count of Push RPCs accepted.",
        )?;
        let register_object_total = IntCounter::new(
            "rayd_raylet_register_object_total",
            "Cumulative count of RegisterObject RPCs accepted.",
        )?;
        let get_object_locations_total = IntCounter::new(
            "rayd_raylet_get_object_locations_total",
            "Cumulative count of GetObjectLocations RPCs accepted.",
        )?;
        let spill_restore_total = IntCounter::new(
            "rayd_raylet_spill_restore_total",
            "Successful spill-backend restores triggered by a Pull miss.",
        )?;
        let directory_entries = IntGauge::new(
            "rayd_raylet_directory_entries",
            "Number of (object_id → node_ids) entries in the directory.",
        )?;
        let node_status_lookups_total = IntCounterVec::new(
            opts!(
                "rayd_node_index_status_lookups_total",
                "WatchNodes-driven NodeIndex lookups by outcome (hit / miss)."
            ),
            &["outcome"],
        )?;
        // Pre-instantiate both label values so the hit-ratio query is
        // safe even before the first lookup of a given outcome.
        let _ = node_status_lookups_total.with_label_values(&["hit"]);
        let _ = node_status_lookups_total.with_label_values(&["miss"]);
        registry.register(Box::new(pull_total.clone()))?;
        registry.register(Box::new(push_total.clone()))?;
        registry.register(Box::new(register_object_total.clone()))?;
        registry.register(Box::new(get_object_locations_total.clone()))?;
        registry.register(Box::new(spill_restore_total.clone()))?;
        registry.register(Box::new(directory_entries.clone()))?;
        registry.register(Box::new(node_status_lookups_total.clone()))?;
        Ok(Self {
            pull_total,
            push_total,
            register_object_total,
            get_object_locations_total,
            spill_restore_total,
            directory_entries,
            node_status_lookups_total,
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
        String::from_utf8(buf).map_err(|e| {
            prometheus::Error::Msg(format!("metrics encoder produced non-UTF-8: {e}"))
        })
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
    /// Address actually bound (informative when caller passed `:0`).
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

/// Bring up the `/metrics` HTTP endpoint on `addr`.
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
            warn!(error = %e, "rayd-raylet: metrics server exited with error");
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
        Ok(body) => (StatusCode::OK, [("Content-Type", "text/plain; version=0.0.4")], body),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [("Content-Type", "text/plain")],
            format!("encode failed: {e}"),
        ),
    }
}
