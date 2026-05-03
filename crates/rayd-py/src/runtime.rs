//! Process-wide handle to the active rayd session.
//!
//! `init()` brings up:
//! 1. A temp dir for our UDS sockets (only when we auto-spawn the plasma server).
//! 2. A `PlasmaServer` — auto-spawned, OR external if `RAYD_PLASMA_SOCKET`
//!    points at an existing socket.
//! 3. A `CoreWorker` connected to that plasma server.
//! 4. A `Dispatcher` that spawns `RAYD_NUM_WORKERS` (default 4) Python
//!    worker subprocesses for task execution.
//!
//! `shutdown()` reverses the lifecycle in the right order: dispatcher
//! drains and joins workers (with the GIL released), then plasma server +
//! temp dir clean themselves up via `Drop`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pyo3::exceptions::PyRuntimeError;
use pyo3::PyResult;
use pyo3::Python;
use rayd_core::{CoreWorker, ObjectId};
use rayd_plasma::{PlasmaServer, ServerHandle, DEFAULT_ARENA_BYTES};
use tempfile::TempDir;

use crate::dispatcher::{Dispatcher, DEFAULT_WORKERS};
use crate::gcs::GcsBinding;
use crate::task_manager::TaskManager;

/// Environment variable that, when set, points `init()` at an existing
/// plasma server instead of auto-spawning one.
const PLASMA_SOCKET_ENV: &str = "RAYD_PLASMA_SOCKET";

/// Optional override for the number of worker subprocesses.
const NUM_WORKERS_ENV: &str = "RAYD_NUM_WORKERS";

/// Suppresses dispatcher startup (worker subprocess spawning) when set.
/// Used by the worker entry point itself, which calls `rayd.init()` to
/// connect to plasma but must not recursively spawn its own pool.
const NO_DISPATCH_ENV: &str = "RAYD_NO_DISPATCH";

/// When set, points the runtime at a `rayd-gcs` `NodeRegistry` /
/// `JobRegistry` server. The driver registers as both a node and a job
/// during `init()`, drains/finishes during `shutdown()`.
const GCS_ADDRESS_ENV: &str = "RAYD_GCS_ADDRESS";

/// Plasma budget for spill-on-pressure (bytes). `CoreWorker` evicts
/// cold objects when its tracked plasma usage rises above
/// `(budget * RAYD_SPILL_THRESHOLD)`.
const SPILL_BUDGET_ENV: &str = "RAYD_SPILL_BUDGET_BYTES";

/// Threshold ratio in `(0, 1]`. Default 0.75 matches Ray's behaviour.
const SPILL_THRESHOLD_ENV: &str = "RAYD_SPILL_THRESHOLD";

/// Global session state held during `init()`-bracketed lifetime.
struct Session {
    worker: Arc<CoreWorker>,
    dispatcher: Option<Arc<Dispatcher>>,
    gcs: Option<GcsBinding>,
    tasks: Arc<TaskManager>,
    /// Path to the plasma UDS the runtime is connected to. Exposed
    /// to Python (via `_plasma_socket_path`) so per-actor subprocesses
    /// can be spawned pointing at the same store.
    plasma_socket: PathBuf,
    /// `Some` when we auto-spawned a server; `None` when we connected to
    /// an external one.
    _server: Option<ServerHandle>,
    /// `Some` matching `_server`; drop removes the temp dir.
    _temp_dir: Option<TempDir>,
    /// Driver-side Prometheus `/metrics` endpoint when
    /// `RAYD_METRICS_BIND` is set. Held here so shutdown can stop it
    /// before the rest of the session tears down.
    metrics: Option<crate::driver_metrics::MetricsServerHandle>,
}

static SESSION: RwLock<Option<Session>> = RwLock::new(None);

/// Bring up a fresh runtime. Idempotent — returns `false` if a session
/// is already installed.
#[allow(clippy::too_many_lines)]
pub(crate) fn install() -> PyResult<bool> {
    {
        let guard = SESSION.read();
        if guard.is_some() {
            return Ok(false);
        }
    }

    // The tracing subscriber install is *deferred* until after the
    // GCS binding's tokio runtime is up — see the matching call near
    // the end of `install`. Reason: the OTLP span exporter (when
    // `OTEL_EXPORTER_OTLP_ENDPOINT` is set) requires a current tokio
    // runtime; calling init before any runtime exists either panics
    // deep inside hyper or surfaces a fallback warning. We accept
    // that very-early plasma/dispatcher tracing events are dropped
    // — the load-bearing events all happen after init lands.
    let (plasma_socket, temp_dir, server) = resolve_plasma_socket()?;

    let worker = CoreWorker::new_local_with_plasma(&plasma_socket)
        .map_err(|e| PyRuntimeError::new_err(format!("failed to connect plasma client: {e}")))?;

    // Apply spill-on-pressure config from env vars (uses defaults
    // when unset). The recoverer that does the actual work is wired
    // later — until then `maybe_spill_for_pressure` is a no-op even
    // if the threshold trips.
    let (budget_bytes, threshold) = parse_spill_policy();
    worker.set_spill_policy(budget_bytes, threshold);

    // Build the TaskManager up front so the dispatcher can wire its
    // completion callback to it. Same Arc gets stashed in `Session`.
    let tasks = Arc::new(TaskManager::new());

    let dispatcher = if std::env::var(NO_DISPATCH_ENV).is_ok() {
        None
    } else {
        let session_dir = temp_dir.as_ref().map_or_else(
            || {
                // External plasma server: place the dispatch socket next
                // to the plasma socket so the lifetime story stays simple.
                plasma_socket
                    .parent()
                    .map_or_else(|| PathBuf::from("/tmp"), Path::to_path_buf)
            },
            |d| d.path().to_path_buf(),
        );
        let dispatch_socket = session_dir.join("dispatch.sock");
        let num_workers = parse_num_workers();
        let d = Dispatcher::start(
            Arc::clone(&worker),
            dispatch_socket,
            &plasma_socket,
            num_workers,
            Some(Arc::clone(&tasks)),
        )
        .map_err(|e| PyRuntimeError::new_err(format!("failed to start dispatcher: {e}")))?;
        // Block briefly until workers register, so the first submit_task
        // doesn't lose tasks in a race.
        if !d.wait_for_workers(Duration::from_secs(5)) {
            return Err(PyRuntimeError::new_err(format!(
                "rayd dispatcher: only some of {num_workers} worker subprocesses \
                 connected back within 5s"
            )));
        }
        Some(d)
    };

    // Optionally connect to a remote GCS and register this driver.
    let gcs = if let Ok(addr) = std::env::var(GCS_ADDRESS_ENV) {
        let sink: Arc<dyn rayd_raylet::OwnerSink> = Arc::new(
            crate::owner_sink_impl::WorkerOwnerSink::new(Arc::clone(&worker)),
        );
        match GcsBinding::connect_and_register(&addr, &plasma_socket, sink) {
            Ok(binding) => {
                // After the raylet is up, install the free-callback so
                // every owner-side unpin also drops us from the local
                // raylet's directory AND removes any spill record for
                // the same object. Closure goes through `with_gcs` so
                // it survives even if the binding identity changes
                // across re-init (idempotent — current value used at
                // call time).
                worker.set_free_callback(Arc::new(|object_id: ObjectId| {
                    let _ = with_gcs(|b| {
                        b.deregister_self(*object_id.as_bytes());
                        // Best-effort: drop the spill entry so the
                        // on-disk file doesn't leak past the ref's
                        // last drop. Errors are logged inside `forget`
                        // and don't stall the unpin flow.
                        if let Err(e) = b.object_manager().forget(*object_id.as_bytes()) {
                            tracing::warn!(
                                error = %e,
                                "rayd-py: spill forget failed during free"
                            );
                        }
                    });
                }));
                // Wire the spill manager as the recovery hook so
                // local `rayd.get` can transparently restore an
                // object that's been evicted out of plasma. Two
                // steps so the unsizing coercion `Arc<Manager> →
                // Arc<dyn Recoverer>` happens on the binding.
                let manager = Arc::clone(binding.object_manager());
                let recoverer: Arc<dyn rayd_core::ObjectRecoverer> = manager;
                worker.set_recoverer(recoverer);
                Some(binding)
            }
            Err(e) => {
                return Err(PyRuntimeError::new_err(format!(
                    "RAYD_GCS_ADDRESS={addr} but registration failed: {e}"
                )));
            }
        }
    } else {
        None
    };

    // Now that the GCS binding (if any) is up and its tokio runtime
    // is running, install the tracing subscriber. When a runtime
    // exists we enter its context first so the OTLP exporter can
    // spawn its tonic gRPC client cleanly.
    //
    // The Python `logging` bridge — if enabled via
    // `RAYD_LOG_FORWARD=1` — is registered BEFORE init so the very
    // first events flowing through the subscriber's `DispatchLayer`
    // are forwarded into Python's `logging` module.
    crate::python_log::register_if_enabled();
    if let Some(binding) = &gcs {
        let _guard = binding.runtime_handle().enter();
        rayd_core::init_default_subscriber();
    } else {
        rayd_core::init_default_subscriber();
    }

    // First event emitted *after* the subscriber is installed.
    // Useful as a "init succeeded" diagnostic AND as a deterministic
    // first event for the Python-logging bridge tests.
    tracing::info!(
        gcs_attached = gcs.is_some(),
        dispatcher_attached = dispatcher.is_some(),
        "rayd-py: init complete"
    );

    // Bring up the driver-side `/metrics` endpoint after the worker
    // is ready so the `refs_alive` collector can read live state.
    // Runs in its own dedicated tokio runtime — independent of GCS.
    let metrics = crate::driver_metrics::init_if_enabled(&worker)
        .map_err(|e| PyRuntimeError::new_err(format!("driver metrics: {e}")))?;

    let mut guard = SESSION.write();
    if guard.is_some() {
        return Ok(false);
    }
    *guard = Some(Session {
        worker,
        dispatcher,
        gcs,
        tasks,
        plasma_socket,
        _server: server,
        _temp_dir: temp_dir,
        metrics,
    });
    Ok(true)
}

/// Path to the active session's plasma UDS, if a session is installed.
pub(crate) fn current_plasma_socket() -> Option<PathBuf> {
    SESSION.read().as_ref().map(|s| s.plasma_socket.clone())
}

/// Snapshot of the session's `TaskManager`. `None` when no session
/// is installed.
pub(crate) fn current_tasks() -> Option<Arc<TaskManager>> {
    SESSION.read().as_ref().map(|s| Arc::clone(&s.tasks))
}

/// Tear down the session.
///
/// Dispatcher first (drains workers), then GCS (drain node + finish job),
/// then the plasma server + temp dir drop in that lexical order. The whole
/// teardown runs with the GIL released so worker threads can grab it.
pub(crate) fn uninstall(py: Python<'_>) -> bool {
    let Some(session) = SESSION.write().take() else {
        return false;
    };
    let Session {
        worker,
        dispatcher,
        gcs,
        tasks,
        plasma_socket: _,
        _server: server,
        _temp_dir: temp_dir,
        metrics,
    } = session;
    py.detach(move || {
        // Stop the metrics endpoint first so its scrape handler
        // can't race the worker / refcount teardown.
        if let Some(m) = metrics {
            m.shutdown();
        }
        if let Some(d) = dispatcher {
            d.shutdown();
        }
        if let Some(binding) = gcs {
            binding.shutdown();
        }
        drop(tasks);
        drop(server);
        drop(temp_dir);
        drop(worker);
    });
    true
}

pub(crate) fn is_initialized() -> bool {
    SESSION.read().is_some()
}

pub(crate) fn current() -> Option<Arc<CoreWorker>> {
    SESSION.read().as_ref().map(|s| Arc::clone(&s.worker))
}

pub(crate) fn require() -> PyResult<Arc<CoreWorker>> {
    current()
        .ok_or_else(|| PyRuntimeError::new_err("rayd is not initialized; call rayd.init() first"))
}

pub(crate) fn require_dispatcher() -> PyResult<Arc<Dispatcher>> {
    SESSION
        .read()
        .as_ref()
        .and_then(|s| s.dispatcher.clone())
        .ok_or_else(|| {
            PyRuntimeError::new_err(
                "rayd is not initialized (or dispatcher disabled); \
                 call rayd.init() first",
            )
        })
}

/// Run `f` against the active GCS binding, if any.
///
/// Returns `Ok(None)` when this session has no GCS connection
/// (`RAYD_GCS_ADDRESS` was unset). Returns `Err` only when the runtime
/// itself isn't initialised.
pub(crate) fn with_gcs<F, R>(f: F) -> PyResult<Option<R>>
where
    F: FnOnce(&GcsBinding) -> R,
{
    let guard = SESSION.read();
    let session = guard.as_ref().ok_or_else(|| {
        PyRuntimeError::new_err("rayd is not initialized; call rayd.init() first")
    })?;
    Ok(session.gcs.as_ref().map(f))
}

/// Decide where to find / how to start the plasma server.
fn resolve_plasma_socket() -> PyResult<(PathBuf, Option<TempDir>, Option<ServerHandle>)> {
    if let Ok(path) = std::env::var(PLASMA_SOCKET_ENV) {
        let socket = PathBuf::from(path);
        if !socket.exists() {
            return Err(PyRuntimeError::new_err(format!(
                "RAYD_PLASMA_SOCKET points at {} which doesn't exist; \
                 start a plasma server first (`rayd plasma-server <path>`)",
                socket.display()
            )));
        }
        return Ok((socket, None, None));
    }

    let temp_dir = TempDir::with_prefix("rayd-")
        .map_err(|e| PyRuntimeError::new_err(format!("failed to create rayd temp dir: {e}")))?;
    let socket = temp_dir.path().join("plasma.sock");
    let server = PlasmaServer::start(socket.clone(), DEFAULT_ARENA_BYTES)
        .map_err(|e| PyRuntimeError::new_err(format!("failed to start plasma server: {e}")))?;
    Ok((socket, Some(temp_dir), Some(server)))
}

fn parse_num_workers() -> usize {
    match std::env::var(NUM_WORKERS_ENV) {
        Ok(s) => s
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_WORKERS),
        Err(_) => DEFAULT_WORKERS,
    }
}

/// Read spill-on-pressure config from env. Falls back to the
/// `CoreWorker` defaults when unset or unparseable.
fn parse_spill_policy() -> (u64, f64) {
    let budget = std::env::var(SPILL_BUDGET_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(rayd_core::DEFAULT_SPILL_BUDGET_BYTES);
    let threshold = std::env::var(SPILL_THRESHOLD_ENV)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|t| t.is_finite() && *t > 0.0 && *t <= 1.0)
        .unwrap_or(rayd_core::DEFAULT_SPILL_THRESHOLD);
    (budget, threshold)
}

use std::path::Path;
