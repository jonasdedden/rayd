//! Python `logging` bridge for Rust `tracing` events.
//!
//! When `RAYD_LOG_FORWARD=1` is set at runtime, `runtime::install`
//! registers an `EventHandler` (via `rayd_core::set_event_handler`)
//! that forwards each tracing event into Python's `logging` module:
//!
//! ```text
//! tracing::info!(target: "rayd_gcs", "node registered host=localhost")
//!   → logging.getLogger("rayd").log(20, "rayd_gcs: node registered host=localhost")
//! ```
//!
//! Trade-offs:
//! - Acquires the GIL once per event. The `OnceLock`-cached logger
//!   reference avoids re-importing `logging` on every call, but the
//!   GIL acquisition itself is unavoidable.
//! - Off by default. Users who want their existing Python logging
//!   handlers (file, JSON, syslog, …) to see rayd's diagnostics
//!   opt in via the env var.
//! - The bridge is additive — the stderr fmt layer still emits, so
//!   users who set up `logging` filtering also see plain stderr
//!   output unless they tune `RAYD_LOG`.

use std::sync::OnceLock;

use pyo3::prelude::*;
use rayd_core::EventHandler;
use tracing::Level;

/// Env var that, when set to `1`, switches the bridge on.
pub(crate) const FORWARD_ENV: &str = "RAYD_LOG_FORWARD";

/// Logger name in Python's `logging` namespace. Users hook a handler
/// onto `logging.getLogger("rayd")` to capture our events.
const PY_LOGGER_NAME: &str = "rayd";

/// Cached `logging.getLogger("rayd")` reference. Set once on the
/// first event so we don't reimport `logging` on every call. We
/// store it via `PyO3`'s `Py<PyAny>` so the cached value crosses
/// `Python::attach` boundaries.
static PY_LOGGER: OnceLock<Py<PyAny>> = OnceLock::new();

/// Map a `tracing::Level` to Python `logging` integer levels.
///
/// `tracing` has 5 levels (TRACE/DEBUG/INFO/WARN/ERROR); Python
/// `logging` has 5 named levels (DEBUG/INFO/WARNING/ERROR/CRITICAL)
/// at well-known integer values. tracing TRACE collapses to DEBUG
/// here — most users don't configure a finer-grained DEBUG level.
const fn level_to_python(level: Level) -> i32 {
    match level {
        Level::TRACE | Level::DEBUG => 10, // logging.DEBUG
        Level::INFO => 20,                 // logging.INFO
        Level::WARN => 30,                 // logging.WARNING
        Level::ERROR => 40,                // logging.ERROR
    }
}

/// Implementation of `EventHandler` that dispatches to
/// `logging.getLogger("rayd").log(level, msg)`. Constructible only
/// via `register_if_enabled`.
#[derive(Debug)]
struct PythonLogBridge;

impl EventHandler for PythonLogBridge {
    fn handle(&self, level: Level, target: &str, message: &str) {
        let level_int = level_to_python(level);
        let formatted = format!("{target}: {message}");
        // Acquire the GIL and dispatch. Errors during dispatch (e.g.
        // a misbehaving handler raising) are swallowed — losing one
        // log record is preferable to taking down the dispatcher.
        Python::attach(|py| {
            let logger = match get_or_init_logger(py) {
                Ok(l) => l,
                Err(_) => return,
            };
            let _ = logger.bind(py).call_method1("log", (level_int, formatted));
        });
    }
}

fn get_or_init_logger(py: Python<'_>) -> PyResult<&'static Py<PyAny>> {
    if let Some(logger) = PY_LOGGER.get() {
        return Ok(logger);
    }
    let logging = py.import("logging")?;
    let logger = logging.call_method1("getLogger", (PY_LOGGER_NAME,))?;
    // Race-safe: if two threads call this simultaneously, both build a
    // logger; whichever one's `set` succeeds wins. The other's logger
    // is dropped harmlessly. Both are equivalent (Python's logging
    // returns the same singleton for a given name).
    let _ = PY_LOGGER.set(logger.unbind());
    Ok(PY_LOGGER.get().expect("PY_LOGGER set above"))
}

/// Register the bridge with `rayd-core` if `RAYD_LOG_FORWARD=1`.
/// No-op otherwise. Idempotent — `rayd_core::set_event_handler` is
/// `OnceLock::set`, so the first registration wins.
///
/// Call before `init_default_subscriber` so the bridge is in place
/// when the first event fires.
pub(crate) fn register_if_enabled() {
    if std::env::var(FORWARD_ENV).as_deref() != Ok("1") {
        return;
    }
    rayd_core::set_event_handler(std::sync::Arc::new(PythonLogBridge));
}
