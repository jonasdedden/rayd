//! Plasma server Prometheus metrics + tiny `/metrics` HTTP server.
//!
//! Hand-rolled HTTP/1.0 responder rather than axum so the plasma
//! crate stays free of tokio. The whole loop lives in a dedicated
//! `std::thread`, accepts one connection at a time, parses just enough
//! of the request to verify it asks for `/metrics`, writes the
//! prometheus text body, and closes. Sufficient for a scraper that
//! polls every 15 s.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use prometheus::{Encoder as _, IntCounter, IntGauge, Registry as PromRegistry, TextEncoder};
use thiserror::Error;
use tracing::{debug, warn};

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
    /// Failed to register a metric (duplicate name, malformed labels, ...).
    #[error("metrics register: {0}")]
    Register(#[from] prometheus::Error),
}

/// Bag of metric handles. Cloned into the plasma server's request
/// handlers so create/get/delete can `.inc()` without touching the
/// encoder. Encoding happens lazily on `/metrics` scrape.
#[derive(Clone, Debug)]
pub struct Metrics {
    /// Total arena capacity in bytes (set once at server startup).
    pub arena_bytes_total: IntGauge,
    /// Currently allocated arena bytes.
    pub arena_bytes_used: IntGauge,
    /// Number of objects currently in the table (sealed or unsealed).
    pub objects_total: IntGauge,
    /// Cumulative `Create` requests served.
    pub create_total: IntCounter,
    /// Cumulative `Get` requests served.
    pub get_total: IntCounter,
    /// Cumulative `Delete` requests served.
    pub delete_total: IntCounter,
    registry: Arc<PromRegistry>,
}

impl Metrics {
    /// Build the metrics bag and register every handle. Call once
    /// per `PlasmaServer` instance.
    pub fn new() -> Result<Self, MetricsStartError> {
        let registry = PromRegistry::new();
        let arena_bytes_total = IntGauge::new(
            "rayd_plasma_arena_bytes_total",
            "Total bytes of the plasma server's mmap'd arena.",
        )?;
        let arena_bytes_used = IntGauge::new(
            "rayd_plasma_arena_bytes_used",
            "Bytes currently allocated to objects (sealed or unsealed).",
        )?;
        let objects_total = IntGauge::new(
            "rayd_plasma_objects_total",
            "Number of objects currently in the plasma object table.",
        )?;
        let create_total = IntCounter::new(
            "rayd_plasma_create_total",
            "Cumulative count of Create RPCs served.",
        )?;
        let get_total = IntCounter::new(
            "rayd_plasma_get_total",
            "Cumulative count of Get RPCs served.",
        )?;
        let delete_total = IntCounter::new(
            "rayd_plasma_delete_total",
            "Cumulative count of Delete RPCs served.",
        )?;
        registry.register(Box::new(arena_bytes_total.clone()))?;
        registry.register(Box::new(arena_bytes_used.clone()))?;
        registry.register(Box::new(objects_total.clone()))?;
        registry.register(Box::new(create_total.clone()))?;
        registry.register(Box::new(get_total.clone()))?;
        registry.register(Box::new(delete_total.clone()))?;
        Ok(Self {
            arena_bytes_total,
            arena_bytes_used,
            objects_total,
            create_total,
            get_total,
            delete_total,
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

/// Owns a running metrics HTTP server thread.
#[derive(Debug)]
pub struct MetricsServerHandle {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    join_handle: Option<JoinHandle<()>>,
}

impl MetricsServerHandle {
    /// Address actually bound (informative when caller passed `:0`).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stop the server and join its thread. Idempotent.
    pub fn shutdown(&mut self) {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return;
        }
        // Poke ourselves with a connection so the accept loop wakes
        // and observes the shutdown flag.
        let _ = std::net::TcpStream::connect_timeout(&self.local_addr, Duration::from_millis(200));
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for MetricsServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Bring up the `/metrics` HTTP endpoint on `addr`. The returned
/// handle owns the bound port and the responder thread; drop or
/// call `shutdown()` to stop.
pub fn start_metrics_server(
    addr: SocketAddr,
    metrics: Metrics,
) -> Result<MetricsServerHandle, MetricsStartError> {
    let listener =
        TcpListener::bind(addr).map_err(|e| MetricsStartError::Bind { addr, source: e })?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;
    // Short timeout so the accept loop can poll the shutdown flag.
    listener
        .set_nonblocking(false)
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_for_thread = Arc::clone(&shutdown);
    let metrics_for_thread = metrics;

    let join_handle = thread::Builder::new()
        .name(format!("plasma-metrics@{local_addr}"))
        .spawn(move || run_metrics_loop(&listener, &metrics_for_thread, &shutdown_for_thread))
        .map_err(|e| MetricsStartError::Bind { addr, source: e })?;

    Ok(MetricsServerHandle {
        local_addr,
        shutdown,
        join_handle: Some(join_handle),
    })
}

fn run_metrics_loop(listener: &TcpListener, metrics: &Metrics, shutdown: &Arc<AtomicBool>) {
    for accept in listener.incoming() {
        if shutdown.load(Ordering::SeqCst) {
            break;
        }
        let mut stream = match accept {
            Ok(s) => s,
            Err(e) => {
                debug!(error = %e, "plasma metrics: accept failed");
                continue;
            }
        };
        // Best-effort 1-second timeouts so a stuck client can't
        // pin the accept thread.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(1)));
        let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

        if let Err(e) = serve_one(&mut stream, metrics) {
            debug!(error = %e, "plasma metrics: serve failed");
        }
    }
}

fn serve_one(stream: &mut std::net::TcpStream, metrics: &Metrics) -> Result<(), std::io::Error> {
    // Read request bytes until we see "\r\n\r\n" or hit a small cap.
    // We don't need to parse the full request — just confirm it asks
    // for `/metrics` on GET.
    let mut buf = [0u8; 1024];
    let mut total = 0;
    loop {
        if total == buf.len() {
            // Request line is suspiciously long — bail.
            break;
        }
        let n = stream.read(&mut buf[total..])?;
        if n == 0 {
            break;
        }
        total += n;
        if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let head = std::str::from_utf8(&buf[..total]).unwrap_or("");
    let first_line = head.lines().next().unwrap_or("");
    let want_metrics = first_line.starts_with("GET /metrics");

    if !want_metrics {
        let body = b"not found";
        let response = format!(
            "HTTP/1.0 404 Not Found\r\n\
             Content-Type: text/plain\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n",
            body.len(),
        );
        stream.write_all(response.as_bytes())?;
        stream.write_all(body)?;
        return Ok(());
    }

    let body = match metrics.encode_text() {
        Ok(b) => b,
        Err(e) => {
            warn!(error = %e, "plasma metrics: encode failed");
            let body = b"encode failed";
            let response = format!(
                "HTTP/1.0 500 Internal Server Error\r\n\
                 Content-Type: text/plain\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n",
                body.len(),
            );
            stream.write_all(response.as_bytes())?;
            stream.write_all(body)?;
            return Ok(());
        }
    };
    let response = format!(
        "HTTP/1.0 200 OK\r\n\
         Content-Type: text/plain; version=0.0.4\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len(),
    );
    stream.write_all(response.as_bytes())?;
    stream.write_all(body.as_bytes())?;
    Ok(())
}
