//! Driver-side worker dispatcher.
//!
//! Spawns N `python -m rayd._worker` subprocesses, accepts their connections
//! on a Unix domain socket, and routes queued tasks to idle workers. When a
//! `task_complete` frame arrives, the dispatcher reads the per-return
//! metadata and registers a `PlasmaIndex` entry in the driver's
//! `MemoryStore` — the result data already lives in the shared plasma
//! server.
//!
//! Replaces Phase 3.1's in-process `ThreadPool`. Tasks now run in real
//! subprocess workers, free of the GIL contention that bounded Phase 1+2.
//!
//! Phase 3.2 keeps things simple:
//! - Tasks queued in the driver in submission order.
//! - Each idle worker picks up the next queued task; round-robin emerges
//!   naturally from the locks.
//! - Results always go through plasma (no inline bypass yet).
//! - Workers are spawned at `init()` and torn down at `shutdown()`.

use std::collections::VecDeque;
use std::fs;
use std::io::ErrorKind;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use rayd_core::{CoreWorker, Metadata, ObjectId, PlasmaIndex, TaskId};
use tracing::{debug, warn};

use crate::serialize;
use crate::wire::{recv_frame, send_frame};

/// Default workers count. Override at session install time.
pub(crate) const DEFAULT_WORKERS: usize = 4;

/// Pending task held in the dispatcher's queue.
pub(crate) struct DispatchJob {
    pub(crate) task_id: TaskId,
    pub(crate) num_returns: u32,
    /// cloudpickle bytes of the user callable.
    pub(crate) callable_blob: Vec<u8>,
    /// cloudpickle bytes of the args tuple.
    pub(crate) args_blob: Vec<u8>,
    /// cloudpickle bytes of the kwargs dict; `None` if no kwargs.
    pub(crate) kwargs_blob: Option<Vec<u8>>,
}

/// Owns the listener thread, child workers, and the queue.
pub(crate) struct Dispatcher {
    queue: Mutex<VecDeque<DispatchJob>>,
    queue_cv: Condvar,
    shutdown: AtomicBool,
    workers: Mutex<Vec<JoinHandle<()>>>,
    listener_thread: Mutex<Option<JoinHandle<()>>>,
    children: Mutex<Vec<Child>>,
    socket_path: PathBuf,
    listener: Option<UnixListener>,
    worker: Arc<CoreWorker>,
    /// Optional lineage tracker. When set, every task completion
    /// observed by `handle_completion` fires `mark_completed` so
    /// auto-resubmit can distinguish "in flight" from "lost".
    tasks: Option<Arc<crate::task_manager::TaskManager>>,
    /// Number of workers expected to register before `wait_for_workers`
    /// returns. Used so the first `submit_task` doesn't lose tasks.
    expected_workers: usize,
    /// Counter of `worker_ready` greetings received so far. Pairs with
    /// `registered_cv` (NOT `queue_cv`; `parking_lot`'s `Condvar` requires
    /// a single companion mutex per condvar instance).
    registered: Mutex<usize>,
    registered_cv: Condvar,
}

impl Dispatcher {
    /// Spawn the dispatcher: bind UDS, fork N workers, accept their
    /// `worker_ready` registrations.
    pub(crate) fn start(
        worker: Arc<CoreWorker>,
        socket_path: PathBuf,
        plasma_socket: &PathBuf,
        num_workers: usize,
        tasks: Option<Arc<crate::task_manager::TaskManager>>,
    ) -> std::io::Result<Arc<Self>> {
        let _ = fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)?;

        let dispatcher = Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            queue_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            workers: Mutex::new(Vec::with_capacity(num_workers)),
            listener_thread: Mutex::new(None),
            children: Mutex::new(Vec::with_capacity(num_workers)),
            socket_path: socket_path.clone(),
            listener: Some(listener),
            worker,
            tasks,
            expected_workers: num_workers,
            registered: Mutex::new(0),
            registered_cv: Condvar::new(),
        });

        // Fork workers BEFORE spawning the accept loop so the workers'
        // outbound `connect()` always finds a bound listener.
        spawn_children(&dispatcher, plasma_socket, num_workers)?;

        // Accept loop runs on a dedicated thread; per-worker handler threads
        // get spawned out of it.
        let accept_dispatcher = Arc::clone(&dispatcher);
        let listener = dispatcher
            .listener
            .as_ref()
            .expect("listener bound")
            .try_clone()?;
        let listener_thread = std::thread::Builder::new()
            .name(format!("rayd-dispatch-accept@{}", socket_path.display()))
            .spawn(move || accept_loop(&accept_dispatcher, listener))?;
        *dispatcher.listener_thread.lock() = Some(listener_thread);

        Ok(dispatcher)
    }

    /// Enqueue a task. Silently dropped after shutdown.
    pub(crate) fn submit(&self, job: DispatchJob) {
        if self.shutdown.load(Ordering::SeqCst) {
            return;
        }
        self.queue.lock().push_back(job);
        self.queue_cv.notify_one();
    }

    pub(crate) fn pending(&self) -> usize {
        self.queue.lock().len()
    }

    /// Block until the dispatcher has confirmed `expected_workers`
    /// `worker_ready` messages, or `timeout` elapses.
    pub(crate) fn wait_for_workers(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut guard = self.registered.lock();
        while *guard < self.expected_workers {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let _ = self.registered_cv.wait_for(&mut guard, deadline - now);
        }
        true
    }

    /// Drain queued tasks, signal workers to exit, join everything.
    /// Caller must NOT hold the GIL.
    pub(crate) fn shutdown(&self) {
        // Same lock-then-store pattern as `pool.rs`: prevents the missed
        // wakeup race against any worker handler thread that's about to
        // wait on the queue.
        {
            let _g = self.queue.lock();
            self.shutdown.store(true, Ordering::SeqCst);
        }
        self.queue_cv.notify_all();

        // Worker handler threads will see the shutdown flag and exit;
        // they also send a `shutdown` frame to their child or close the
        // socket so the worker subprocess terminates.
        let mut handlers = self.workers.lock();
        for handle in handlers.drain(..) {
            if handle.join().is_err() {
                warn!("rayd dispatcher: worker handler panicked");
            }
        }
        drop(handlers);

        // Stopping the accept loop: connect to ourselves so accept() returns,
        // OR just remove the socket and let it bail. We do both.
        let _ = UnixStream::connect(&self.socket_path);
        let listener_handle = self.listener_thread.lock().take();
        if let Some(handle) = listener_handle {
            let _ = handle.join();
        }
        let _ = fs::remove_file(&self.socket_path);

        // Wait for child processes. They should have exited when their
        // sockets closed; if not, kill after a short grace period.
        let mut children = self.children.lock();
        for child in children.iter_mut() {
            let _ = wait_with_timeout(child, Duration::from_secs(5));
        }
        children.clear();
    }
}

fn spawn_children(
    dispatcher: &Arc<Dispatcher>,
    plasma_socket: &PathBuf,
    num_workers: usize,
) -> std::io::Result<()> {
    let interpreter = current_python_interpreter();
    for i in 0..num_workers {
        let mut cmd = Command::new(&interpreter);
        cmd.args(["-m", "rayd._worker", "--dispatch-socket"])
            .arg(&dispatcher.socket_path)
            .arg("--plasma-socket")
            .arg(plasma_socket);
        cmd.stdin(Stdio::null());
        // Driver-only env vars: workers must not inherit them. The
        // `/metrics` endpoint is a driver-side aggregator; if every
        // worker tried to bind the same port the second one onward
        // would crash with EADDRINUSE.
        cmd.env_remove(crate::driver_metrics::METRICS_BIND_ENV);
        // Inherit stdout/stderr so worker tracebacks surface during dev.
        // Tests can redirect by setting RAYD_WORKER_QUIET=1 (Phase 3.2b).
        let child = cmd.spawn().map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to spawn rayd._worker #{i}: {e}"))
        })?;
        dispatcher.children.lock().push(child);
    }
    Ok(())
}

fn current_python_interpreter() -> PathBuf {
    // Acquire the GIL just long enough to read sys.executable.
    Python::attach(|py| -> PyResult<PathBuf> {
        let sys = py.import("sys")?;
        let executable: String = sys.getattr("executable")?.extract()?;
        Ok(PathBuf::from(executable))
    })
    .unwrap_or_else(|_| PathBuf::from("python3"))
}

fn accept_loop(dispatcher: &Arc<Dispatcher>, listener: UnixListener) {
    for stream in listener.incoming() {
        if dispatcher.shutdown.load(Ordering::SeqCst) {
            break;
        }
        let stream = match stream {
            Ok(s) => s,
            Err(e) if e.kind() == ErrorKind::WouldBlock => continue,
            Err(e) => {
                warn!(error = %e, "rayd dispatcher: accept failed");
                continue;
            }
        };
        let dispatcher_for_thread = Arc::clone(dispatcher);
        let handle = std::thread::Builder::new()
            .name("rayd-dispatch-conn".into())
            .spawn(move || {
                if let Err(e) = handle_connection(&dispatcher_for_thread, stream) {
                    debug!(error = %e, "rayd dispatcher: connection ended with error");
                }
            });
        match handle {
            Ok(h) => dispatcher.workers.lock().push(h),
            Err(e) => warn!(error = %e, "rayd dispatcher: failed to spawn handler thread"),
        }
    }
}

fn handle_connection(dispatcher: &Arc<Dispatcher>, stream: UnixStream) -> std::io::Result<()> {
    // First frame must be a `worker_ready` greeting. We don't actually
    // verify its contents — just count and move on.
    match recv_frame(&stream)? {
        None => return Ok(()),
        Some(frame) => {
            // Acknowledge the registration. Phase 3.2b can decode + log
            // worker_id / pid for diagnostics.
            let _ = frame;
            let mut reg = dispatcher.registered.lock();
            *reg += 1;
            dispatcher.registered_cv.notify_all();
        }
    }

    // Main loop: pick a job, send it, wait for completion.
    while let Some(job) = next_job(dispatcher) {
        let frame =
            Python::attach(|py| -> PyResult<Vec<u8>> { build_dispatch_task_frame(py, &job) });
        let frame = match frame {
            Ok(f) => f,
            Err(e) => {
                warn!(error = %e, "failed to build dispatch_task frame");
                // We never gave the job to a worker; surface a runtime error
                // by not delivering any result. The driver-side ObjectRefs
                // remain Pending; a future improvement can synthesise an
                // error.
                continue;
            }
        };

        if let Err(e) = send_frame(&stream, &frame) {
            warn!(error = %e, "rayd dispatcher: failed to send dispatch_task");
            // Re-enqueue the job so another worker can pick it up.
            dispatcher.queue.lock().push_front(job);
            dispatcher.queue_cv.notify_one();
            return Err(e);
        }

        // Wait for `task_complete` (or the connection closing).
        let completion = match recv_frame(&stream)? {
            None => {
                debug!("rayd dispatcher: worker disconnected mid-task");
                return Ok(());
            }
            Some(body) => body,
        };
        if let Err(e) = handle_completion(&dispatcher.worker, &job, &completion) {
            warn!(error = %e, "rayd dispatcher: failed to ingest task_complete");
        } else if let Some(tasks) = &dispatcher.tasks {
            // Successful completion → mark the lineage record so
            // auto-resubmit knows the task is past the in-flight
            // phase. Skipped on parse errors (above) since we don't
            // know which task actually completed.
            tasks.mark_completed(job.task_id);
        }
    }

    // Best-effort polite shutdown frame. The peer may have closed already.
    let shutdown_frame = Python::attach(|py| -> PyResult<Vec<u8>> {
        let dict = PyDict::new(py);
        dict.set_item("kind", "shutdown")?;
        let bound: Bound<'_, PyAny> = dict.into_any();
        Ok(serialize::dumps(py, &bound)?.to_vec())
    });
    if let Ok(frame) = shutdown_frame {
        let _ = send_frame(&stream, &frame);
    }
    Ok(())
}

fn next_job(dispatcher: &Dispatcher) -> Option<DispatchJob> {
    let mut queue = dispatcher.queue.lock();
    loop {
        if let Some(job) = queue.pop_front() {
            return Some(job);
        }
        if dispatcher.shutdown.load(Ordering::SeqCst) {
            return None;
        }
        dispatcher.queue_cv.wait(&mut queue);
    }
}

fn build_dispatch_task_frame(py: Python<'_>, job: &DispatchJob) -> PyResult<Vec<u8>> {
    let dict = PyDict::new(py);
    dict.set_item("kind", "dispatch_task")?;
    dict.set_item("task_id", PyBytes::new(py, job.task_id.as_bytes()))?;
    dict.set_item("num_returns", job.num_returns)?;
    dict.set_item("callable", PyBytes::new(py, &job.callable_blob))?;
    dict.set_item("args", PyBytes::new(py, &job.args_blob))?;
    match &job.kwargs_blob {
        Some(b) => dict.set_item("kwargs", PyBytes::new(py, b))?,
        None => dict.set_item("kwargs", py.None())?,
    }
    let bound: Bound<'_, PyAny> = dict.into_any();
    Ok(serialize::dumps(py, &bound)?.to_vec())
}

fn handle_completion(worker: &Arc<CoreWorker>, job: &DispatchJob, frame: &[u8]) -> PyResult<()> {
    Python::attach(|py| -> PyResult<()> {
        let decoded = serialize::loads(py, frame)?;
        let dict = decoded.bind(py).cast::<PyDict>()?;
        let kind: String = dict.get_item("kind")?.unwrap().extract()?;
        if kind != "task_complete" {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "expected task_complete, got {kind}"
            )));
        }
        let returns_obj = dict
            .get_item("returns")?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing 'returns'"))?;
        let returns = returns_obj.cast::<PyList>()?;
        for entry in returns.iter() {
            let entry_dict = entry.cast::<PyDict>()?;
            let object_id_bytes: Vec<u8> = entry_dict
                .get_item("object_id")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing 'object_id'"))?
                .extract()?;
            let metadata_bytes: Vec<u8> = entry_dict
                .get_item("metadata")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing 'metadata'"))?
                .extract()?;
            let data_size: u64 = entry_dict
                .get_item("data_size")?
                .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("missing 'data_size'"))?
                .extract()?;
            if object_id_bytes.len() != ObjectId::SIZE {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "object_id wrong length: {}",
                    object_id_bytes.len()
                )));
            }
            let mut id_buf = [0u8; ObjectId::SIZE];
            id_buf.copy_from_slice(&object_id_bytes);
            let id = ObjectId::from_bytes(id_buf);
            let metadata = Metadata::decode(&metadata_bytes).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("could not decode metadata: {e}"))
            })?;
            // Bump driver-side metrics. The `Error` metadata variant
            // is the worker-set "this task raised" marker; everything
            // else (Pickle5, Inline) means a value-bearing return.
            if let Some(m) = crate::driver_metrics::current() {
                if metadata.is_error() {
                    m.tasks_failed_total.inc();
                } else {
                    m.tasks_completed_total.inc();
                }
            }
            worker.store().put_plasma(
                id,
                PlasmaIndex {
                    metadata,
                    data_size,
                },
            );
        }
        // Bind `job.task_id` so we can use it in the task_id sanity check.
        let _ = job;
        Ok(())
    })
}

fn wait_with_timeout(child: &mut Child, timeout: Duration) -> std::io::Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
