//! `PyO3` bindings for rayd.
//!
//! This crate is the **only** Python-visible entry point. Implementation
//! details live in `rayd-core`; this module is the typed surface.
//!
//! Stub generation uses `PyO3`'s native `experimental-inspect`: the
//! `#[pymodule]` *inline module* below records introspection metadata that
//! `crates/rayd-py/src/bin/stub_gen.rs` reads via `pyo3-introspection`.

#![forbid(unsafe_code)]

mod dispatcher;
mod driver_metrics;
mod gcs;
mod owner_sink_impl;
mod python_log;
mod raylet_pool;
mod runtime;
mod serialize;
mod task_manager;
mod wire;

use pyo3::prelude::*;

/// The version reported on `rayd._native.__version__`.
pub const CORE_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `rayd._native`: the `PyO3` extension module.
///
/// Declared with the *inline module* form (`pub mod`, not `fn`) so the
/// `experimental-inspect` feature can record introspection metadata for
/// every item below.
#[pymodule]
pub mod _native {
    use std::time::Duration;

    use pyo3::exceptions::{PyKeyError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
    use rayd_core::{
        Address, CoreWorker, ErrorCategory, ErrorInfo, ErrorPayload, Metadata, ObjectId, ObjectRef,
        RefState, WorkerId,
    };

    use crate::dispatcher::DispatchJob;
    use crate::runtime;
    use crate::serialize;

    /// Module-level version string (mirrors the cargo package version).
    #[pymodule_export]
    #[allow(non_upper_case_globals)] // matches Python's `__version__` convention.
    pub const __version__: &str = super::CORE_VERSION;

    // ──────────────────────────────────────────────────────────────────
    // ObjectId
    // ──────────────────────────────────────────────────────────────────

    /// 28-byte identifier of an object in the distributed store.
    ///
    /// Equivalent to Ray's `ObjectID`: deterministically derived from the
    /// parent task id plus a 4-byte return index, so callers can predict
    /// ids before the producing task runs.
    #[pyclass(
        name = "ObjectId",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyObjectId {
        pub(crate) inner: ObjectId,
    }

    #[pymethods]
    impl PyObjectId {
        /// Construct from raw bytes (must be exactly 28).
        #[new]
        fn new(bytes: Vec<u8>) -> PyResult<Self> {
            if bytes.len() != ObjectId::SIZE {
                return Err(PyValueError::new_err(format!(
                    "ObjectId requires {} bytes, got {}",
                    ObjectId::SIZE,
                    bytes.len()
                )));
            }
            let mut buf = [0u8; ObjectId::SIZE];
            buf.copy_from_slice(&bytes);
            Ok(Self {
                inner: ObjectId::from_bytes(buf),
            })
        }

        /// Build an id from the parent task's bytes (24) and a return index.
        #[staticmethod]
        fn for_return(task_bytes: Vec<u8>, return_index: u32) -> PyResult<Self> {
            if task_bytes.len() != rayd_core::TaskId::SIZE {
                return Err(PyValueError::new_err(format!(
                    "task id requires {} bytes, got {}",
                    rayd_core::TaskId::SIZE,
                    task_bytes.len()
                )));
            }
            let mut buf = [0u8; rayd_core::TaskId::SIZE];
            buf.copy_from_slice(&task_bytes);
            let task = rayd_core::TaskId::from_bytes(buf);
            Ok(Self {
                inner: ObjectId::for_return(&task, return_index),
            })
        }

        /// Generate a fresh random id.
        #[staticmethod]
        fn random() -> Self {
            let task = rayd_core::TaskId::random();
            Self {
                inner: ObjectId::for_return(&task, 0),
            }
        }

        /// The all-zero sentinel id.
        #[staticmethod]
        fn nil() -> Self {
            Self {
                inner: ObjectId::nil(),
            }
        }

        /// Lowercase hex (56 characters).
        #[getter]
        fn hex(&self) -> String {
            self.inner.hex()
        }

        /// Raw 28-byte representation.
        fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, self.inner.as_bytes())
        }

        /// 0-based return index encoded in the last 4 bytes.
        #[getter]
        fn return_index(&self) -> u32 {
            self.inner.return_index()
        }

        /// Whether this id equals the all-zero sentinel.
        fn is_nil(&self) -> bool {
            self.inner.is_nil()
        }

        fn __repr__(&self) -> String {
            format!("ObjectId({})", self.inner.hex())
        }

        fn __str__(&self) -> String {
            self.inner.hex()
        }

        /// Pickle protocol: emit `(ObjectId, (raw_bytes,))`.
        fn __reduce__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
            let cls = py.get_type::<Self>().into_any();
            let bytes = PyBytes::new(py, self.inner.as_bytes()).into_any();
            let args = PyTuple::new(py, [bytes])?;
            Ok((cls, args))
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Address
    // ──────────────────────────────────────────────────────────────────

    /// Address of a worker process. Carries host, port, and the worker's id.
    #[pyclass(
        name = "Address",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyAddress {
        pub(crate) inner: Address,
    }

    #[pymethods]
    impl PyAddress {
        /// Construct from `host`, `port`, and a 16-byte worker id.
        #[new]
        fn new(host: String, port: u16, worker_id_bytes: Vec<u8>) -> PyResult<Self> {
            if worker_id_bytes.len() != WorkerId::SIZE {
                return Err(PyValueError::new_err(format!(
                    "worker id requires {} bytes, got {}",
                    WorkerId::SIZE,
                    worker_id_bytes.len()
                )));
            }
            let mut buf = [0u8; WorkerId::SIZE];
            buf.copy_from_slice(&worker_id_bytes);
            Ok(Self {
                inner: Address::new(host, port, WorkerId::from_bytes(buf)),
            })
        }

        /// Placeholder address for "not yet resolved" cases.
        #[staticmethod]
        fn nil() -> Self {
            Self {
                inner: Address::new(String::new(), 0, WorkerId::nil()),
            }
        }

        #[getter]
        fn host(&self) -> &str {
            &self.inner.host
        }

        #[getter]
        fn port(&self) -> u16 {
            self.inner.port
        }

        #[getter]
        fn worker_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, self.inner.worker_id.as_bytes())
        }

        /// Whether this address carries a non-nil worker id.
        fn is_resolved(&self) -> bool {
            self.inner.is_resolved()
        }

        fn __repr__(&self) -> String {
            format!(
                "Address(host={:?}, port={}, worker_id={})",
                self.inner.host, self.inner.port, self.inner.worker_id
            )
        }

        fn __str__(&self) -> String {
            format!("{}", self.inner)
        }

        /// Pickle protocol: emit `(Address, (host, port, worker_id_bytes))`.
        fn __reduce__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
            let cls = py.get_type::<Self>().into_any();
            let host = self.inner.host.clone().into_pyobject(py)?.into_any();
            let port = self.inner.port.into_pyobject(py)?.into_any();
            let worker_id = PyBytes::new(py, self.inner.worker_id.as_bytes()).into_any();
            let args = PyTuple::new(py, [host, port, worker_id])?;
            Ok((cls, args))
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // RefState
    // ──────────────────────────────────────────────────────────────────

    /// Lifecycle state of an `ObjectRef` as observed from the holder's worker.
    #[pyclass(
        name = "RefState",
        module = "rayd._native",
        eq,
        eq_int,
        hash,
        frozen,
        from_py_object
    )]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum PyRefState {
        /// Not yet present in any store we know about.
        Pending,
        /// Materialized on the local node.
        ReadyLocal,
        /// Materialized somewhere else in the cluster (Phase 3+).
        ReadyRemote,
        /// An error sentinel; `get()` will raise.
        Failed,
    }

    impl PyRefState {
        pub(crate) const fn from_core(state: RefState) -> Self {
            match state {
                RefState::Pending => Self::Pending,
                RefState::ReadyLocal => Self::ReadyLocal,
                RefState::ReadyRemote => Self::ReadyRemote,
                RefState::Failed => Self::Failed,
            }
        }

        pub(crate) const fn to_core(self) -> RefState {
            match self {
                Self::Pending => RefState::Pending,
                Self::ReadyLocal => RefState::ReadyLocal,
                Self::ReadyRemote => RefState::ReadyRemote,
                Self::Failed => RefState::Failed,
            }
        }
    }

    #[pymethods]
    impl PyRefState {
        /// Whether the state is `READY_LOCAL`, `READY_REMOTE`, or `FAILED`.
        fn is_ready(&self) -> bool {
            self.to_core().is_ready()
        }

        /// Whether the state is `FAILED`.
        fn is_failed(&self) -> bool {
            self.to_core().is_failed()
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // ErrorCategory
    // ──────────────────────────────────────────────────────────────────

    /// Coarse user-facing error category. The granular `raw_code` lives on
    /// `ErrorInfo`.
    #[pyclass(
        name = "ErrorCategory",
        module = "rayd._native",
        eq,
        eq_int,
        hash,
        frozen,
        from_py_object
    )]
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum PyErrorCategory {
        /// User code raised a Python exception.
        TaskException,
        /// Worker process died.
        WorkerDied,
        /// Actor died and could not be restarted.
        ActorDied,
        /// The owning worker died; the value is unreconstructable.
        OwnerDied,
        /// Task was cancelled explicitly.
        TaskCancelled,
        /// Object was lost from plasma.
        ObjectLost,
        /// Object lost and lineage reconstruction exhausted.
        ObjectUnreconstructable,
        /// Object exists somewhere but couldn't be pulled in time.
        FetchTimeout,
        /// Worker startup or runtime-env materialization failed.
        RuntimeEnvFailed,
        /// Task could not be scheduled.
        Unschedulable,
        /// Out of memory or out of disk on the executing node.
        OutOfMemory,
    }

    impl PyErrorCategory {
        pub(crate) const fn from_core(category: ErrorCategory) -> Self {
            match category {
                ErrorCategory::TaskException => Self::TaskException,
                ErrorCategory::WorkerDied => Self::WorkerDied,
                ErrorCategory::ActorDied => Self::ActorDied,
                ErrorCategory::OwnerDied => Self::OwnerDied,
                ErrorCategory::TaskCancelled => Self::TaskCancelled,
                ErrorCategory::ObjectLost => Self::ObjectLost,
                ErrorCategory::ObjectUnreconstructable => Self::ObjectUnreconstructable,
                ErrorCategory::FetchTimeout => Self::FetchTimeout,
                ErrorCategory::RuntimeEnvFailed => Self::RuntimeEnvFailed,
                ErrorCategory::Unschedulable => Self::Unschedulable,
                ErrorCategory::OutOfMemory => Self::OutOfMemory,
            }
        }

        pub(crate) const fn to_core(self) -> ErrorCategory {
            match self {
                Self::TaskException => ErrorCategory::TaskException,
                Self::WorkerDied => ErrorCategory::WorkerDied,
                Self::ActorDied => ErrorCategory::ActorDied,
                Self::OwnerDied => ErrorCategory::OwnerDied,
                Self::TaskCancelled => ErrorCategory::TaskCancelled,
                Self::ObjectLost => ErrorCategory::ObjectLost,
                Self::ObjectUnreconstructable => ErrorCategory::ObjectUnreconstructable,
                Self::FetchTimeout => ErrorCategory::FetchTimeout,
                Self::RuntimeEnvFailed => ErrorCategory::RuntimeEnvFailed,
                Self::Unschedulable => ErrorCategory::Unschedulable,
                Self::OutOfMemory => ErrorCategory::OutOfMemory,
            }
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // ErrorInfo
    // ──────────────────────────────────────────────────────────────────

    /// Information about a failed `ObjectRef` recoverable without unpickling.
    #[pyclass(
        name = "ErrorInfo",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyErrorInfo {
        pub(crate) inner: ErrorInfo,
    }

    impl PyErrorInfo {
        pub(crate) const fn from_inner(inner: ErrorInfo) -> Self {
            Self { inner }
        }
    }

    #[pymethods]
    impl PyErrorInfo {
        /// Construct an `ErrorInfo`. `traceback` is meaningful only for
        /// `TaskException`; for other categories pass `None`.
        #[new]
        #[pyo3(signature = (category, message, traceback=None, raw_code=0))]
        fn new(
            category: PyErrorCategory,
            message: String,
            traceback: Option<String>,
            raw_code: u16,
        ) -> Self {
            let mut info = ErrorInfo::new(category.to_core(), message).with_raw_code(raw_code);
            if let Some(tb) = traceback {
                info = info.with_traceback(tb);
            }
            Self { inner: info }
        }

        #[getter]
        fn category(&self) -> PyErrorCategory {
            PyErrorCategory::from_core(self.inner.category)
        }

        #[getter]
        fn message(&self) -> &str {
            &self.inner.message
        }

        #[getter]
        fn traceback(&self) -> Option<&str> {
            self.inner.traceback.as_deref()
        }

        #[getter]
        fn raw_code(&self) -> u16 {
            self.inner.raw_code
        }

        fn __repr__(&self) -> String {
            format!(
                "ErrorInfo(category={:?}, message={:?}, raw_code={})",
                self.inner.category, self.inner.message, self.inner.raw_code
            )
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // ObjectRef
    // ──────────────────────────────────────────────────────────────────

    /// Reference to a value in the distributed object store.
    #[pyclass(
        name = "ObjectRef",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Debug)]
    pub struct PyObjectRef {
        pub(crate) inner: ObjectRef,
        /// Phase 4.3 marker: only the *first* `PyObjectRef` for a given
        /// id (the one minted by `__new__` or `from_inner`) is responsible
        /// for the matching `dec_local_ref` on `Drop`. Rust-side clones
        /// (e.g. `extract_ref`'s `borrow().clone()`) carry `owns_count =
        /// false` so they don't double-decrement.
        pub(crate) owns_count: bool,
    }

    impl PyObjectRef {
        pub(crate) const fn from_inner(inner: ObjectRef) -> Self {
            Self {
                inner,
                owns_count: true,
            }
        }
    }

    impl Clone for PyObjectRef {
        fn clone(&self) -> Self {
            // Rust-side clones never own the refcount — only the
            // pyclass instance that backs a live Python object does.
            Self {
                inner: self.inner.clone(),
                owns_count: false,
            }
        }
    }

    impl PartialEq for PyObjectRef {
        fn eq(&self, other: &Self) -> bool {
            self.inner == other.inner
        }
    }

    impl Eq for PyObjectRef {}

    impl std::hash::Hash for PyObjectRef {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Drop for PyObjectRef {
        fn drop(&mut self) {
            if !self.owns_count {
                return;
            }
            // Skip if the runtime isn't initialised (e.g. ref outlived
            // `rayd.shutdown()` because Python kept it alive). The
            // worker has already torn down its store.
            let Some(worker) = runtime::current() else {
                return;
            };
            let oid = *self.inner.object_id();
            let _ = worker.dec_local_ref(oid);

            // Cross-node: if this ref was minted by another driver
            // (its `owner_node_id` differs from ours), tell the owner-
            // raylet that we no longer hold a copy. Best-effort —
            // errors are logged inside, drop can't propagate.
            let Some(owner_nid) = self.inner.owner_node_id() else {
                return;
            };
            let local_nid_opt = runtime::with_gcs(super::gcs::GcsBinding::node_id)
                .ok()
                .flatten();
            if let Some(local_nid) = local_nid_opt {
                if local_nid != owner_nid {
                    let oid_bytes = *oid.as_bytes();
                    let _ = runtime::with_gcs(|binding| {
                        binding.notify_owner_of_drop(owner_nid, oid_bytes);
                    });
                }
            }
        }
    }

    #[pymethods]
    impl PyObjectRef {
        /// Construct an `ObjectRef` from an id and an owner address,
        /// optionally stamping the owner-raylet's 16-byte node id (so
        /// peers can fetch the object via `Pull` after this ref is
        /// shipped to another process).
        #[new]
        #[pyo3(signature = (object_id, owner, owner_node_id=None))]
        fn new(
            object_id: PyObjectId,
            owner: PyAddress,
            owner_node_id: Option<Vec<u8>>,
        ) -> PyResult<Self> {
            let mut inner = ObjectRef::new(object_id.inner, owner.inner);
            if let Some(bytes) = owner_node_id {
                if bytes.len() != 16 {
                    return Err(PyValueError::new_err(format!(
                        "owner_node_id must be 16 bytes, got {}",
                        bytes.len()
                    )));
                }
                let mut buf = [0u8; 16];
                buf.copy_from_slice(&bytes);
                inner = inner.with_owner_node_id(buf);
            }
            Ok(Self {
                inner,
                owns_count: true,
            })
        }

        /// The id of the referenced object.
        #[getter]
        fn object_id(&self) -> PyObjectId {
            PyObjectId {
                inner: *self.inner.object_id(),
            }
        }

        /// The address of the owner worker.
        #[getter]
        fn owner(&self) -> PyAddress {
            PyAddress {
                inner: self.inner.owner().clone(),
            }
        }

        /// 16-byte GCS node id of the owner-raylet, or `None` when this
        /// ref wasn't created under a GCS-attached driver.
        #[getter]
        fn owner_node_id<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
            self.inner.owner_node_id().map(|nid| PyBytes::new(py, &nid))
        }

        /// Pickle protocol: emit `(ObjectRef, (object_id, owner, owner_node_id))`
        /// so the ref survives a round-trip through `pickle.dumps` and can
        /// travel between processes attached to the same GCS.
        fn __reduce__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyTuple>)> {
            let cls = py.get_type::<Self>().into_any();
            let object_id = PyObjectId {
                inner: *self.inner.object_id(),
            }
            .into_pyobject(py)?
            .into_any();
            let owner = PyAddress {
                inner: self.inner.owner().clone(),
            }
            .into_pyobject(py)?
            .into_any();
            let owner_nid: Bound<'py, PyAny> = match self.inner.owner_node_id() {
                Some(nid) => PyBytes::new(py, &nid).into_any(),
                None => py.None().into_bound(py),
            };
            let args = PyTuple::new(py, [object_id, owner, owner_nid])?;
            Ok((cls, args))
        }

        /// Lowercase hex of the underlying `ObjectId`.
        #[getter]
        fn hex(&self) -> String {
            self.inner.object_id().hex()
        }

        /// Snapshot of the ref's lifecycle state. Cheap: reads metadata only.
        ///
        /// Returns `Pending` when no runtime is initialized, so the call
        /// degrades gracefully before `init()`. Returns `ReadyRemote`
        /// when the ref carries an `owner_node_id` that's NOT this node
        /// — i.e. we know the bytes live on a peer raylet but haven't
        /// pulled them yet. After `rayd.get` (or `_native.fetch_object`)
        /// seals locally, this flips to `ReadyLocal`.
        fn state(&self) -> PyRefState {
            let Some(worker) = runtime::current() else {
                return PyRefState::Pending;
            };
            let core_state = worker.store().state_of(self.inner.object_id());
            let py_state = PyRefState::from_core(core_state);
            if !matches!(py_state, PyRefState::Pending) {
                return py_state;
            }
            // Pending locally; check if the ref points at a remote
            // owner-raylet — if so, surface that as `ReadyRemote`.
            let Some(owner_nid) = self.inner.owner_node_id() else {
                return PyRefState::Pending;
            };
            let local_nid = runtime::with_gcs(super::gcs::GcsBinding::node_id)
                .ok()
                .flatten();
            if local_nid.is_some_and(|local| local != owner_nid) {
                PyRefState::ReadyRemote
            } else {
                PyRefState::Pending
            }
        }

        /// Returns the error info for failed refs without unpickling the
        /// user-supplied exception. `None` for pending or successful refs.
        fn peek_error(&self) -> Option<PyErrorInfo> {
            let worker = runtime::current()?;
            // Fast path: only fetch the data buffer if the local index's
            // metadata says this is an error. For inline entries that's a
            // single hash-map lookup; for plasma we still need a fetch
            // because the ErrorPayload lives in the data buffer.
            let resolved = worker.resolve_now(self.inner.object_id()).ok().flatten()?;
            let Metadata::Error { category, raw_code } = resolved.metadata else {
                return None;
            };
            let payload = ErrorPayload::decode(&resolved.data).ok()?;
            let mut info = ErrorInfo::new(category, payload.message).with_raw_code(raw_code);
            if let Some(tb) = payload.traceback {
                info = info.with_traceback(tb);
            }
            Some(PyErrorInfo::from_inner(info))
        }

        /// Returns the original Python exception. Heavier than `peek_error`:
        /// unpickles the user payload. `None` if pending or successful, or
        /// if the exception wasn't picklable.
        fn exception(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
            let Some(worker) = runtime::current() else {
                return Ok(None);
            };
            let Some(resolved) = worker.resolve_now(self.inner.object_id()).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("resolve failed: {e}"))
            })?
            else {
                return Ok(None);
            };
            if !resolved.metadata.is_error() {
                return Ok(None);
            }
            let payload = ErrorPayload::decode(&resolved.data).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("corrupted error payload: {e}"))
            })?;
            match payload.pickled_python_exception {
                Some(blob) => Ok(Some(serialize::loads(py, &blob)?)),
                None => Ok(None),
            }
        }

        /// Convenience: whether `state()` is one of the ready states.
        fn is_ready(&self) -> bool {
            self.state().is_ready()
        }

        /// Convenience: whether `state() == Failed`.
        fn is_failed(&self) -> bool {
            self.state().is_failed()
        }

        fn __repr__(&self) -> String {
            format!("ObjectRef({})", self.inner.object_id().hex())
        }
    }

    // ──────────────────────────────────────────────────────────────────
    // Top-level lifecycle
    // ──────────────────────────────────────────────────────────────────

    /// Initialize the rayd runtime. Idempotent: calling twice is a no-op.
    ///
    /// `address` is reserved for connecting to an existing head node and
    /// is currently ignored (Phase 1 is single-process).
    #[pyfunction]
    #[pyo3(signature = (address=None))]
    pub fn init(address: Option<String>) -> PyResult<()> {
        let _ = address;
        runtime::install()?;
        Ok(())
    }

    /// Tear down the rayd runtime.
    ///
    /// Drains the worker thread pool with the GIL released so any task
    /// currently mid-flight can finish without deadlocking on the
    /// interpreter, then drops the plasma server and temp dir.
    #[pyfunction]
    pub fn shutdown(py: Python<'_>) -> PyResult<()> {
        runtime::uninstall(py);
        Ok(())
    }

    /// Whether `init()` has been called more recently than `shutdown()`.
    #[pyfunction]
    pub fn is_initialized() -> bool {
        runtime::is_initialized()
    }

    // ──────────────────────────────────────────────────────────────────
    // Top-level put / get / get_settled / state / wait
    // ──────────────────────────────────────────────────────────────────

    /// Pickle `value` and store it under a fresh deterministic id. Returns
    /// the resulting `ObjectRef`.
    ///
    /// Routes to plasma when the pickled buffer exceeds the inline
    /// threshold; ALSO forces the plasma path (and registers the
    /// object at the local raylet's directory) when GCS is configured,
    /// so peers can pull it via `fetch_object`.
    #[pyfunction]
    pub fn put(py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<PyObjectRef> {
        if let Some(m) = crate::driver_metrics::current() {
            m.puts_total.inc();
        }
        let worker = runtime::require()?;
        let task_id = worker.next_task_id();
        let id = ObjectId::for_return(&task_id, 0);
        let pickled = serialize::dumps(py, &value)?;
        let metadata = Metadata::Pickle5 {
            has_nested_refs: false,
        };

        // When a GCS is attached, force the plasma path so a peer can
        // dial our raylet and pull this object regardless of size.
        let local_node_id = runtime::with_gcs(super::gcs::GcsBinding::node_id)?;
        if let Some(node_id) = local_node_id {
            worker
                .seal_value_to_plasma(id, metadata, pickled)
                .map_err(|e| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!("seal failed: {e}"))
                })?;
            // Register self as a holder so peers can dial us. This
            // bypasses the gRPC + OwnerSink path on purpose — see
            // `GcsBinding::register_self_local`'s docstring.
            runtime::with_gcs(|binding| {
                binding.register_self_local(*id.as_bytes());
            })?;
            let _ = node_id; // node_id no longer needed in this branch.
        } else {
            worker.seal_value(id, metadata, pickled).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("seal failed: {e}"))
            })?;
        }

        let mut inner = ObjectRef::new(id, worker.address().clone());
        if let Some(nid) = local_node_id {
            inner = inner.with_owner_node_id(nid);
        }
        Ok(PyObjectRef::from_inner(inner))
    }

    /// Block until each ref is resolved, then return the values. Raises on
    /// the first failure encountered. For partial-success semantics, use
    /// `get_settled`.
    ///
    /// `refs` may be a single `ObjectRef` or a list of them.
    #[pyfunction]
    #[pyo3(signature = (refs, timeout=None))]
    pub fn get(
        py: Python<'_>,
        refs: Bound<'_, PyAny>,
        timeout: Option<f64>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(m) = crate::driver_metrics::current() {
            m.gets_total.inc();
        }
        let worker = runtime::require()?;
        let timeout = parse_timeout(timeout)?;

        // Single-ref shorthand.
        if refs.cast::<PyObjectRef>().is_ok() {
            let single = extract_ref(&refs)?;
            let value = get_one(py, &worker, &single, timeout)?;
            return Ok(value);
        }

        let list = refs.cast::<PyList>().map_err(|_| {
            PyValueError::new_err("rayd.get expects an ObjectRef or a list of ObjectRefs")
        })?;
        let py_list = PyList::empty(py);
        for item in list.iter() {
            let r = extract_ref(&item)?;
            let value = get_one(py, &worker, &r, timeout)?;
            py_list.append(value)?;
        }
        Ok(py_list.unbind().into_any())
    }

    fn get_one(
        py: Python<'_>,
        worker: &CoreWorker,
        r: &PyObjectRef,
        timeout: Option<Duration>,
    ) -> PyResult<Py<PyAny>> {
        let resolved = py
            .detach(|| worker.resolve_blocking(r.inner.object_id(), timeout))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("resolve: {e}")))?
            .ok_or_else(|| {
                pyo3::exceptions::PyTimeoutError::new_err(format!(
                    "rayd.get timed out waiting for ObjectRef({})",
                    r.inner.object_id()
                ))
            })?;
        match resolved.metadata {
            Metadata::Error { category, raw_code } => Err(raise_from_error_object(
                py,
                category,
                raw_code,
                &resolved.data,
            )),
            Metadata::Pickle5 { .. } => serialize::loads(py, &resolved.data),
            Metadata::Raw => Ok(PyBytes::new(py, &resolved.data).unbind().into_any()),
            Metadata::ActorHandle => Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "actor handle deserialization lands in Phase 5",
            )),
        }
    }

    /// Like `get`, but returns one entry per ref without raising on
    /// individual failures. The result is a list whose entries are:
    ///
    /// - the value, on success;
    /// - `RaydError` (or a subclass) wrapped via `ErrorInfo`, on failure;
    /// - the special sentinel `Pending`, when the ref hadn't resolved by
    ///   the supplied `timeout`.
    ///
    /// Concretely each entry is a 2-tuple `(kind, payload)` where `kind`
    /// is `"ok" | "err" | "pending"`. The Python facade in
    /// `python/rayd/__init__.py` rewraps these as `Ok`/`Err`/`Pending`
    /// dataclasses for ergonomics.
    #[pyfunction]
    #[pyo3(signature = (refs, timeout=None))]
    pub fn get_settled(
        py: Python<'_>,
        refs: Vec<PyObjectRef>,
        timeout: Option<f64>,
    ) -> PyResult<Vec<(String, Py<PyAny>)>> {
        if let Some(m) = crate::driver_metrics::current() {
            m.gets_total.inc();
        }
        let worker = runtime::require()?;
        let timeout = parse_timeout(timeout)?;

        let mut out: Vec<(String, Py<PyAny>)> = Vec::with_capacity(refs.len());
        for r in &refs {
            let resolved = py
                .detach(|| worker.resolve_blocking(r.inner.object_id(), timeout))
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("resolve: {e}")))?;
            let entry: (String, Py<PyAny>) = match resolved {
                None => ("pending".to_owned(), py.None()),
                Some(obj) => match obj.metadata {
                    Metadata::Error { category, raw_code } => {
                        let info = error_info_from_object(category, raw_code, &obj.data)?;
                        (
                            "err".to_owned(),
                            Py::new(py, PyErrorInfo::from_inner(info))?.into_any(),
                        )
                    }
                    Metadata::Pickle5 { .. } => ("ok".to_owned(), serialize::loads(py, &obj.data)?),
                    Metadata::Raw => (
                        "ok".to_owned(),
                        PyBytes::new(py, &obj.data).into_any().unbind(),
                    ),
                    Metadata::ActorHandle => {
                        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                            "actor handle deserialization lands in Phase 5",
                        ));
                    }
                },
            };
            out.push(entry);
        }
        Ok(out)
    }

    /// Snapshot per-ref state. One mutex acquisition for the whole batch;
    /// no payload deserialization.
    ///
    /// Returns a list of `(ref, state)` pairs rather than a dict so
    /// `PyO3`'s `experimental-inspect` can emit a precise
    /// `list[tuple[ObjectRef, RefState]]` type hint without relying
    /// on `Py<PyDict>` (which erases to bare `dict`). The Python
    /// facade rewraps via `dict(...)`.
    #[pyfunction]
    pub fn state(refs: Vec<PyObjectRef>) -> PyResult<Vec<(PyObjectRef, PyRefState)>> {
        let worker = runtime::require()?;
        let ids: Vec<ObjectId> = refs.iter().map(|r| *r.inner.object_id()).collect();
        let snap = worker.store().state_snapshot(&ids);
        Ok(refs
            .into_iter()
            .map(|r| {
                let s = snap
                    .get(r.inner.object_id())
                    .copied()
                    .unwrap_or(RefState::Pending);
                (r, PyRefState::from_core(s))
            })
            .collect())
    }

    /// Wait for at least `num_returns` of `refs` to enter a terminal state.
    /// Returns `(ready, not_ready)` lists.
    #[pyfunction]
    #[pyo3(signature = (refs, num_returns=1, timeout=None))]
    pub fn wait(
        py: Python<'_>,
        refs: Vec<PyObjectRef>,
        num_returns: usize,
        timeout: Option<f64>,
    ) -> PyResult<(Vec<PyObjectRef>, Vec<PyObjectRef>)> {
        let worker = runtime::require()?;
        let timeout = parse_timeout(timeout)?;
        let ids: Vec<ObjectId> = refs.iter().map(|r| *r.inner.object_id()).collect();

        let outcome = py.detach(|| worker.store().wait(&ids, num_returns, timeout));

        let mut ready = Vec::new();
        let mut not_ready = Vec::new();
        for r in refs {
            if outcome.ready.contains(r.inner.object_id()) {
                ready.push(r);
            } else {
                not_ready.push(r);
            }
        }
        Ok((ready, not_ready))
    }

    /// Wait variant that returns a snapshot of states instead of a
    /// `(ready, not_ready)` split. Matches `state()` in shape but blocks
    /// for `timeout` to give pending refs a chance to land.
    #[pyfunction]
    #[pyo3(signature = (refs, timeout=None))]
    pub fn wait_with_states(
        py: Python<'_>,
        refs: Vec<PyObjectRef>,
        timeout: Option<f64>,
    ) -> PyResult<Vec<(PyObjectRef, PyRefState)>> {
        let worker = runtime::require()?;
        let timeout = parse_timeout(timeout)?;
        let ids: Vec<ObjectId> = refs.iter().map(|r| *r.inner.object_id()).collect();

        // Wait for all refs to settle (or until timeout).
        py.detach(|| worker.store().wait(&ids, ids.len(), timeout));
        let snap = worker.store().state_snapshot(&ids);

        Ok(refs
            .into_iter()
            .map(|r| {
                let s = snap
                    .get(r.inner.object_id())
                    .copied()
                    .unwrap_or(RefState::Pending);
                (r, PyRefState::from_core(s))
            })
            .collect())
    }

    /// Submit a callable for asynchronous execution. Returns a list of
    /// `ObjectRef`s — one per return value. With `num_returns == 1` the
    /// list has length 1; the Python facade unwraps to a single ref.
    ///
    /// The callable is held by reference and invoked on a worker thread;
    /// it may run before, during, or after this call returns.
    #[pyfunction]
    #[pyo3(signature = (callable, args, kwargs=None, num_returns=1))]
    pub fn submit_task(
        py: Python<'_>,
        callable: Py<PyAny>,
        args: Py<PyTuple>,
        kwargs: Option<Py<PyDict>>,
        num_returns: u32,
    ) -> PyResult<Vec<PyObjectRef>> {
        if let Some(m) = crate::driver_metrics::current() {
            m.tasks_submitted_total.inc();
        }
        if num_returns == 0 {
            return Err(PyValueError::new_err("num_returns must be >= 1"));
        }
        let worker = runtime::require()?;
        let dispatcher = runtime::require_dispatcher()?;
        let task_id = worker.next_task_id();
        let object_ids: Vec<ObjectId> = (0..num_returns)
            .map(|i| ObjectId::for_return(&task_id, i))
            .collect();

        // Cloudpickle the callable + args + kwargs while we hold the GIL,
        // then queue the resulting bytes for an idle worker to run.
        let callable_blob = serialize::cloudpickle_dumps(py, callable.bind(py))?.to_vec();
        let args_blob =
            serialize::cloudpickle_dumps(py, &args.bind(py).clone().into_any())?.to_vec();
        let kwargs_blob = match kwargs {
            None => None,
            Some(k) => {
                Some(serialize::cloudpickle_dumps(py, &k.bind(py).clone().into_any())?.to_vec())
            }
        };

        // Record the task for lineage reconstruction BEFORE we hand
        // off to the dispatcher — once the worker subprocess seals,
        // a peer can already start to evict, and we need the record
        // available for any subsequent `try_resubmit_for_lineage`.
        if let Some(tasks) = runtime::current_tasks() {
            tasks.record(
                &object_ids,
                task_id,
                num_returns,
                callable_blob.clone(),
                args_blob.clone(),
                kwargs_blob.clone(),
            );
        }

        dispatcher.submit(DispatchJob {
            task_id,
            num_returns,
            callable_blob,
            args_blob,
            kwargs_blob,
        });

        let _ = py;
        Ok(object_ids
            .into_iter()
            .map(|id| PyObjectRef::from_inner(ObjectRef::new(id, worker.address().clone())))
            .collect())
    }

    /// Free a list of refs from the local store. (No-op if not present.)
    #[pyfunction]
    pub fn free(refs: Vec<PyObjectRef>) -> PyResult<()> {
        let worker = runtime::require()?;
        let ids: Vec<ObjectId> = refs.iter().map(|r| *r.inner.object_id()).collect();
        worker.store().delete(&ids);
        Ok(())
    }

    /// Test/diagnostic helper: how many tasks are currently waiting in the
    /// dispatcher's queue. Production code doesn't need this; it's exposed
    /// so pytest can assert the queue drained at shutdown.
    #[pyfunction]
    pub fn _pool_pending() -> PyResult<usize> {
        let dispatcher = runtime::require_dispatcher()?;
        Ok(dispatcher.pending())
    }

    /// Test/diagnostic helper: forcibly remove `object_id` from the
    /// local memory store AND from plasma, simulating object loss.
    ///
    /// Does NOT touch the reference counter (so `dec_local_ref`
    /// won't fire on a missing entry) and does NOT invoke the
    /// free-callback. Used by lineage-reconstruction tests so a
    /// subsequent `try_resubmit_for_lineage` can re-seal at the
    /// same id without hitting plasma's `AlreadyExists`.
    #[pyfunction]
    pub fn _evict_local(object_id: Vec<u8>) -> PyResult<()> {
        let oid = parse_object_id_28(&object_id)?;
        let worker = runtime::require()?;
        let id = ObjectId::from_bytes(oid);
        worker
            .evict_local(id)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("evict_local: {e}")))
    }

    /// Diagnostic: report whether the per-session spill manager has a
    /// record for `object_id`. Returns `False` when no GCS connection
    /// is attached. Test-only API.
    #[pyfunction]
    pub fn _is_spilled(object_id: Vec<u8>) -> PyResult<bool> {
        let oid = parse_object_id_28(&object_id)?;
        Ok(runtime::with_gcs(|b| b.object_manager().is_spilled(oid))?.unwrap_or(false))
    }

    /// Spill `object_id` out of plasma into the per-session spill manager.
    ///
    /// Reads the bytes from local plasma, hands them to the
    /// recoverer, then deletes the plasma copy. The local store
    /// index entry is preserved so a subsequent `get` triggers
    /// recover-and-reseal transparently.
    ///
    /// Returns `True` only when this call moved bytes from plasma into
    /// the spill backend. `False` covers all the "no work needed"
    /// cases — the object isn't in plasma right now: it may have been
    /// spilled+evicted by a previous call (idempotent re-spill), it
    /// may have been an inline-only seal that never reached plasma,
    /// or another concurrent caller raced ahead. To check the
    /// resulting on-disk state regardless of who did the work, use
    /// `_is_spilled(object_id)`.
    ///
    /// Raises `RuntimeError` if no recoverer is registered (i.e.
    /// running without a GCS) or any step failed.
    ///
    /// Today this is a manual trigger callable from tests. The
    /// automatic spill-on-pressure policy was wired in Phase 6.7.
    #[pyfunction]
    pub fn _spill_object(object_id: Vec<u8>) -> PyResult<bool> {
        let oid = parse_object_id_28(&object_id)?;
        let worker = runtime::require()?;
        let id = ObjectId::from_bytes(oid);
        worker
            .spill_to_recoverer(id)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("spill_object: {e}")))
    }

    /// Lineage-reconstruction hook: requeue a recorded task.
    ///
    /// If we recorded a task that produced `object_id`, the task has
    /// completed at least once, and its retry budget is non-zero,
    /// queue a fresh dispatch with the same `task_id` so the worker
    /// writes back to the same plasma slot. Returns `True` when a
    /// resubmit fired; `False` when no record / not yet completed /
    /// budget exhausted.
    #[pyfunction]
    pub fn try_resubmit_for_lineage(object_id: Vec<u8>) -> PyResult<bool> {
        let oid = parse_object_id_28(&object_id)?;
        let id = ObjectId::from_bytes(oid);
        let Some(tasks) = runtime::current_tasks() else {
            return Ok(false);
        };
        let dispatcher = runtime::require_dispatcher()?;
        match tasks.try_resubmit(id) {
            Some(job) => {
                dispatcher.submit(job);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Classify the lineage state of `object_id`. Used by the Python
    /// `rayd.get` auto-resubmit path. Returns one of:
    ///   - `"not_recorded"`: no task on file produces this id (or
    ///     no runtime is initialised).
    ///   - `"not_yet_completed"`: task is recorded but hasn't sealed
    ///     once yet — caller should wait, NOT resubmit.
    ///   - `"ready"`: completed at least once and budget remains;
    ///     `try_resubmit_for_lineage` will succeed.
    ///   - `"exhausted"`: completed but the retry budget is gone;
    ///     the object can't be reconstructed.
    #[pyfunction]
    pub fn _lineage_status_str(object_id: Vec<u8>) -> PyResult<&'static str> {
        use crate::task_manager::LineageStatus;
        let oid = parse_object_id_28(&object_id)?;
        let id = ObjectId::from_bytes(oid);
        let Some(tasks) = runtime::current_tasks() else {
            return Ok("not_recorded");
        };
        Ok(match tasks.lineage_status(id) {
            LineageStatus::NotRecorded => "not_recorded",
            LineageStatus::NotYetCompleted => "not_yet_completed",
            LineageStatus::ReadyToResubmit => "ready",
            LineageStatus::BudgetExhausted => "exhausted",
        })
    }

    /// Path to the active session's plasma UDS.
    ///
    /// Used by the actor-subprocess machinery to spawn child workers
    /// pointing at the same plasma store the driver opened. Errors
    /// when no session is installed.
    #[pyfunction]
    pub fn _plasma_socket_path() -> PyResult<String> {
        runtime::current_plasma_socket()
            .map(|p| p.to_string_lossy().into_owned())
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "rayd is not initialized; call rayd.init() first",
                )
            })
    }

    /// Mint a fresh `ObjectRef` for an actor method's result.
    ///
    /// Uses the same `task_id` allocator as `submit_task`, so actor
    /// results share the deterministic `(task_id, return_index)`
    /// scheme. Caller is expected to seal data at the returned id via
    /// `_native._worker_seal` before any blocking read.
    ///
    /// `owner_node_id` (16 bytes) stamps the ref's distributed owner.
    /// Pass `None` for same-driver actors (the ref is owned by this
    /// node). Pass a remote node id for cross-driver actors so the
    /// caller's `rayd.get` triggers the cross-node fetch path against
    /// the actor-driver's raylet directory.
    #[pyfunction]
    #[pyo3(signature = (owner_node_id=None))]
    pub fn _mint_actor_result_ref(owner_node_id: Option<Vec<u8>>) -> PyResult<PyObjectRef> {
        let worker = runtime::require()?;
        let task_id = worker.next_task_id();
        let id = ObjectId::for_return(&task_id, 0);
        let mut inner = ObjectRef::new(id, worker.address().clone());
        if let Some(bytes) = owner_node_id {
            let nid = parse_node_id_16(&bytes)?;
            inner = inner.with_owner_node_id(nid);
        }
        Ok(PyObjectRef::from_inner(inner))
    }

    /// Driver-side hook for foreign-process plasma seals.
    ///
    /// Records that `object_id` has been sealed in shared plasma by
    /// another process (e.g. a per-actor worker subprocess). Updates
    /// the local `MemoryStore`'s `PlasmaIndex` so `rayd.get` resolves
    /// through plasma; bumps the owner-side refcount so `Drop` later
    /// cleans up. Idempotent on the store (replaces any prior entry);
    /// the refcount add is per call, so callers must invoke this
    /// exactly once per seal observation.
    ///
    /// When a GCS is attached, also registers this driver as a holder
    /// of `object_id` at the local raylet's directory — so peers can
    /// `Pull` actor results across nodes. Mirrors what `put()` does
    /// for the same reason. No-op when running without a GCS.
    #[pyfunction]
    pub fn _record_plasma_seal(
        object_id: Vec<u8>,
        metadata: Vec<u8>,
        data_size: u64,
    ) -> PyResult<()> {
        let oid = parse_object_id_28(&object_id)?;
        let id = ObjectId::from_bytes(oid);
        let metadata_value = Metadata::decode(&metadata).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("invalid metadata: {e}"))
        })?;
        let worker = runtime::require()?;
        worker.store().put_plasma(
            id,
            rayd_core::PlasmaIndex {
                metadata: metadata_value,
                data_size,
            },
        );
        worker.refs().add_owned(id);
        // Register at the local raylet so cross-node `Pull` succeeds.
        // Same direct-directory path `put()` uses to avoid the
        // OwnerSink self-pin (see `register_self_local`'s docstring).
        runtime::with_gcs(|binding| {
            binding.register_self_local(*id.as_bytes());
        })?;
        Ok(())
    }

    /// Worker-subprocess hook: write a result directly into shared plasma
    /// under the supplied object id. Called from `python -m rayd._worker`.
    /// Driver-side Python should never invoke this.
    #[pyfunction]
    pub fn _worker_seal(object_id: Vec<u8>, metadata: Vec<u8>, data: Vec<u8>) -> PyResult<u64> {
        if object_id.len() != ObjectId::SIZE {
            return Err(PyValueError::new_err(format!(
                "object_id must be {} bytes, got {}",
                ObjectId::SIZE,
                object_id.len()
            )));
        }
        let mut buf = [0u8; ObjectId::SIZE];
        buf.copy_from_slice(&object_id);
        let id = ObjectId::from_bytes(buf);

        let metadata_value = Metadata::decode(&metadata).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("invalid metadata: {e}"))
        })?;

        let worker = runtime::require()?;
        let written = worker
            .seal_value_to_plasma(id, metadata_value, bytes::Bytes::from(data))
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("seal failed: {e}")))?;
        // Register at the local raylet's directory so peers can `Pull`
        // this object cross-node. No-op when no GCS is attached (the
        // actor subprocess clears `RAYD_GCS_ADDRESS` before init), so
        // this only matters when the driver itself seals — e.g.
        // `_seal_actor_died` on the crash-mid-call path.
        runtime::with_gcs(|binding| {
            binding.register_self_local(*id.as_bytes());
        })?;
        Ok(written)
    }

    // ──────────────────────────────────────────────────────────────────
    // GCS pyclasses (PyResources / PyNodeInfo / PyJobInfo)
    // ──────────────────────────────────────────────────────────────────

    /// Resource counts a node advertises to the GCS.
    #[pyclass(
        name = "Resources",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyResources {
        num_cpus: u32,
        num_gpus: u32,
        memory_bytes: u64,
    }

    #[pymethods]
    impl PyResources {
        #[new]
        #[pyo3(signature = (num_cpus = 0, num_gpus = 0, memory_bytes = 0))]
        #[allow(clippy::similar_names)] // names mirror the Python API
        fn new(num_cpus: u32, num_gpus: u32, memory_bytes: u64) -> Self {
            Self {
                num_cpus,
                num_gpus,
                memory_bytes,
            }
        }
        #[getter]
        fn num_cpus(&self) -> u32 {
            self.num_cpus
        }
        #[getter]
        fn num_gpus(&self) -> u32 {
            self.num_gpus
        }
        #[getter]
        fn memory_bytes(&self) -> u64 {
            self.memory_bytes
        }
        fn __repr__(&self) -> String {
            format!(
                "Resources(num_cpus={}, num_gpus={}, memory_bytes={})",
                self.num_cpus, self.num_gpus, self.memory_bytes
            )
        }
    }

    /// Snapshot view of one node, returned by `list_nodes()`.
    #[pyclass(
        name = "NodeInfo",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyNodeInfo {
        host: String,
        port: u32,
        node_id: Vec<u8>,
        plasma_socket: String,
        resources: PyResources,
        status: String,
        registered_at_unix_ms: u64,
        last_heartbeat_unix_ms: u64,
    }

    #[pymethods]
    impl PyNodeInfo {
        #[getter]
        fn host(&self) -> &str {
            &self.host
        }
        #[getter]
        fn port(&self) -> u32 {
            self.port
        }
        #[getter]
        fn node_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, &self.node_id)
        }
        #[getter]
        fn plasma_socket(&self) -> &str {
            &self.plasma_socket
        }
        #[getter]
        fn resources(&self) -> PyResources {
            self.resources.clone()
        }
        /// One of `"alive" | "draining" | "dead" | "unspecified"`.
        #[getter]
        fn status(&self) -> &str {
            &self.status
        }
        #[getter]
        fn registered_at_unix_ms(&self) -> u64 {
            self.registered_at_unix_ms
        }
        #[getter]
        fn last_heartbeat_unix_ms(&self) -> u64 {
            self.last_heartbeat_unix_ms
        }
        fn __repr__(&self) -> String {
            format!(
                "NodeInfo(host={:?}, port={}, status={:?}, resources={})",
                self.host,
                self.port,
                self.status,
                self.resources.__repr__(),
            )
        }
    }

    /// Snapshot view of one job, returned by `list_jobs()`.
    #[pyclass(
        name = "JobInfo",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyJobInfo {
        job_id: Vec<u8>,
        driver_host: String,
        driver_pid: u32,
        node_id: Vec<u8>,
        status: String,
        registered_at_unix_ms: u64,
        finished_at_unix_ms: u64,
    }

    #[pymethods]
    impl PyJobInfo {
        #[getter]
        fn job_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, &self.job_id)
        }
        #[getter]
        fn driver_host(&self) -> &str {
            &self.driver_host
        }
        #[getter]
        fn driver_pid(&self) -> u32 {
            self.driver_pid
        }
        /// 16-byte node id this job's driver is attached to. Empty bytes
        /// when the job isn't linked to a registered node.
        #[getter]
        fn node_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, &self.node_id)
        }
        /// One of `"running" | "finished" | "failed" | "unspecified"`.
        #[getter]
        fn status(&self) -> &str {
            &self.status
        }
        #[getter]
        fn registered_at_unix_ms(&self) -> u64 {
            self.registered_at_unix_ms
        }
        #[getter]
        fn finished_at_unix_ms(&self) -> u64 {
            self.finished_at_unix_ms
        }
        fn __repr__(&self) -> String {
            format!(
                "JobInfo(driver_host={:?}, driver_pid={}, status={:?})",
                self.driver_host, self.driver_pid, self.status
            )
        }
    }

    /// Snapshot view of one named actor, returned by `list_actors()` and
    /// `_lookup_named_actor()`.
    #[pyclass(
        name = "ActorInfo",
        module = "rayd._native",
        frozen,
        eq,
        hash,
        from_py_object
    )]
    #[derive(Clone, Debug, PartialEq, Eq, Hash)]
    pub struct PyActorInfo {
        name: String,
        actor_id: Vec<u8>,
        owner_node_id: Vec<u8>,
        owner_pid: u32,
        registered_at_unix_ms: u64,
        driver_actor_host: String,
        driver_actor_port: u32,
    }

    #[pymethods]
    impl PyActorInfo {
        #[getter]
        fn name(&self) -> &str {
            &self.name
        }
        /// 16-byte driver-minted actor id.
        #[getter]
        fn actor_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, &self.actor_id)
        }
        /// 16-byte node id of the driver that owns the actor. Empty when
        /// the owner driver registered without an associated node.
        #[getter]
        fn owner_node_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
            PyBytes::new(py, &self.owner_node_id)
        }
        #[getter]
        fn owner_pid(&self) -> u32 {
            self.owner_pid
        }
        #[getter]
        fn registered_at_unix_ms(&self) -> u64 {
            self.registered_at_unix_ms
        }
        /// Host of the owner driver's actor-RPC TCP listener. Empty
        /// when the owner runs without one.
        #[getter]
        fn driver_actor_host(&self) -> &str {
            &self.driver_actor_host
        }
        /// Port of the owner driver's actor-RPC TCP listener. Zero
        /// alongside an empty host means "no listener".
        #[getter]
        fn driver_actor_port(&self) -> u32 {
            self.driver_actor_port
        }
        fn __repr__(&self) -> String {
            format!(
                "ActorInfo(name={:?}, owner_pid={})",
                self.name, self.owner_pid
            )
        }
    }

    fn convert_actor(info: rayd_gcs::ActorInfo) -> PyActorInfo {
        PyActorInfo {
            name: info.name,
            actor_id: info.actor_id,
            owner_node_id: info.owner_node_id,
            owner_pid: info.owner_pid,
            registered_at_unix_ms: info.registered_at_unix_ms,
            driver_actor_host: info.driver_actor_host,
            driver_actor_port: info.driver_actor_port,
        }
    }

    fn node_status_str(code: i32) -> &'static str {
        match code {
            1 => "alive",
            2 => "draining",
            3 => "dead",
            _ => "unspecified",
        }
    }

    fn job_status_str(code: i32) -> &'static str {
        match code {
            1 => "running",
            2 => "finished",
            3 => "failed",
            _ => "unspecified",
        }
    }

    fn convert_node(info: rayd_gcs::NodeInfo) -> PyNodeInfo {
        let address = info.address.unwrap_or_default();
        let resources = info.resources.unwrap_or_default();
        PyNodeInfo {
            host: address.host,
            port: address.port,
            node_id: address.node_id,
            plasma_socket: address.plasma_socket,
            resources: PyResources {
                num_cpus: resources.num_cpus,
                num_gpus: resources.num_gpus,
                memory_bytes: resources.memory_bytes,
            },
            status: node_status_str(info.status).to_owned(),
            registered_at_unix_ms: info.registered_at_unix_ms,
            last_heartbeat_unix_ms: info.last_heartbeat_unix_ms,
        }
    }

    fn convert_job(info: rayd_gcs::JobInfo) -> PyJobInfo {
        PyJobInfo {
            job_id: info.job_id,
            driver_host: info.driver_host,
            driver_pid: info.driver_pid,
            node_id: info.node_id,
            status: job_status_str(info.status).to_owned(),
            registered_at_unix_ms: info.registered_at_unix_ms,
            finished_at_unix_ms: info.finished_at_unix_ms,
        }
    }

    /// Fast push-driven liveness lookup (Phase 4.3.3c).
    ///
    /// Returns the locally-cached status of `node_id` ("alive" /
    /// "draining" / "dead") sourced from the raylet's `WatchNodes`
    /// subscription. `None` means the subscriber hasn't observed this
    /// node yet — caller should fall back to `list_nodes()` for an
    /// authoritative answer.
    #[pyfunction]
    pub fn node_status_local(node_id: &[u8]) -> PyResult<Option<String>> {
        if node_id.len() != 16 {
            return Err(PyValueError::new_err(format!(
                "node_id must be 16 bytes, got {}",
                node_id.len()
            )));
        }
        let mut buf = [0u8; 16];
        buf.copy_from_slice(node_id);
        // `with_gcs` returns Some(_) only when a GCS is attached; the
        // inner Option is None when the subscriber hasn't observed
        // this node yet. Either way we surface as Python `None`.
        let status = runtime::with_gcs(|b| b.node_status(buf))
            .ok()
            .flatten()
            .flatten();
        Ok(status.map(|s| node_status_str(s as i32).to_owned()))
    }

    /// Snapshot all nodes the GCS knows about.
    ///
    /// Raises `RuntimeError` if `RAYD_GCS_ADDRESS` was not set on `init()`,
    /// since there's no GCS to query.
    #[pyfunction]
    pub fn list_nodes() -> PyResult<Vec<PyNodeInfo>> {
        let nodes = runtime::with_gcs(super::gcs::GcsBinding::list_nodes)?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("gcs list_nodes: {e}"))
            })?;
        Ok(nodes.into_iter().map(convert_node).collect())
    }

    /// Snapshot all jobs the GCS knows about (running + finished).
    ///
    /// Raises `RuntimeError` if `RAYD_GCS_ADDRESS` was not set on `init()`.
    #[pyfunction]
    pub fn list_jobs() -> PyResult<Vec<PyJobInfo>> {
        let jobs = runtime::with_gcs(super::gcs::GcsBinding::list_jobs)?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("gcs list_jobs: {e}"))
            })?;
        Ok(jobs.into_iter().map(convert_job).collect())
    }

    /// Register a named actor in the GCS directory.
    ///
    /// `driver_actor_host`/`driver_actor_port` advertise the owner
    /// driver's actor-RPC TCP listener — pass `("", 0)` when no
    /// listener has been started.
    ///
    /// Raises `RuntimeError` if there's no GCS connection.
    /// Raises `ValueError` if `actor_id` is not 16 bytes.
    /// Raises `RuntimeError` (wrapping `Status::AlreadyExists`) if the
    /// name is taken by a different actor.
    #[pyfunction]
    #[pyo3(signature = (name, actor_id, driver_actor_host="", driver_actor_port=0))]
    pub fn _register_named_actor(
        name: String,
        actor_id: Vec<u8>,
        driver_actor_host: &str,
        driver_actor_port: u16,
    ) -> PyResult<()> {
        let aid = parse_node_id_16(&actor_id)?;
        runtime::with_gcs(|binding| {
            binding.register_actor(&name, aid, driver_actor_host, driver_actor_port)
        })?
        .ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
            )
        })?
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("register_actor: {e}")))
    }

    /// Look up a named actor in the GCS directory.
    ///
    /// Returns `None` if no actor with that name is registered. Raises
    /// `RuntimeError` if there's no GCS connection.
    #[pyfunction]
    pub fn _lookup_named_actor(name: String) -> PyResult<Option<PyActorInfo>> {
        let info = runtime::with_gcs(|binding| binding.get_actor(&name))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("get_actor: {e}")))?;
        Ok(info.map(convert_actor))
    }

    /// Remove a named-actor entry from the GCS directory.
    ///
    /// Caller's `actor_id` must match the registered entry — prevents
    /// stale handles from clobbering a freshly-registered name.
    /// Raises `RuntimeError` if the name is unknown.
    #[pyfunction]
    pub fn _unregister_named_actor(name: String, actor_id: Vec<u8>) -> PyResult<()> {
        let aid = parse_node_id_16(&actor_id)?;
        runtime::with_gcs(|binding| binding.unregister_actor(&name, aid))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("unregister_actor: {e}"))
            })
    }

    /// Snapshot all named actors the GCS knows about. Mostly for tests
    /// & tooling; production callers should use `_lookup_named_actor`.
    #[pyfunction]
    pub fn list_actors() -> PyResult<Vec<PyActorInfo>> {
        let actors = runtime::with_gcs(super::gcs::GcsBinding::list_actors)?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("gcs list_actors: {e}"))
            })?;
        Ok(actors.into_iter().map(convert_actor).collect())
    }

    /// 16-byte cluster session id assigned by the GCS we connected to.
    /// Returns `None` when `RAYD_GCS_ADDRESS` was not set.
    #[pyfunction]
    pub fn cluster_session_id(py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let id = runtime::with_gcs(super::gcs::GcsBinding::cluster_session_id)?;
        Ok(id.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// 16-byte node id this driver was assigned by the GCS. `None` when
    /// no GCS connection.
    #[pyfunction]
    pub fn node_id(py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let id = runtime::with_gcs(super::gcs::GcsBinding::node_id)?;
        Ok(id.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// 16-byte job id this driver was assigned by the GCS. `None` when
    /// no GCS connection.
    #[pyfunction]
    pub fn job_id(py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let id = runtime::with_gcs(super::gcs::GcsBinding::job_id)?;
        Ok(id.map(|bytes| PyBytes::new(py, &bytes).unbind()))
    }

    /// Address `host:port` of the raylet this driver started.
    /// `None` when `RAYD_GCS_ADDRESS` was not set.
    #[pyfunction]
    pub fn local_raylet_address() -> PyResult<Option<(String, u16)>> {
        let addr = runtime::with_gcs(super::gcs::GcsBinding::raylet_addr)?;
        Ok(addr.map(|a| (a.ip().to_string(), a.port())))
    }

    /// Register a holder of `object_id` at the LOCAL raylet's directory.
    ///
    /// Pass this driver's own `node_id` after a `put()` so peers know
    /// who to pull from. 28-byte `object_id`, 16-byte `holder_node_id`.
    #[pyfunction]
    pub fn register_object(object_id: Vec<u8>, holder_node_id: Vec<u8>) -> PyResult<()> {
        let oid = parse_object_id_28(&object_id)?;
        let nid = parse_node_id_16(&holder_node_id)?;
        let result = runtime::with_gcs(|binding| binding.register_object_local(oid, nid))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?;
        result
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("register_object: {e}")))
    }

    /// Ask the LOCAL raylet which nodes hold `object_id`. Returns the
    /// list of 16-byte `node_id`s (empty when no replicas are known —
    /// not an error).
    #[pyfunction]
    pub fn get_object_locations(object_id: Vec<u8>) -> PyResult<Vec<Vec<u8>>> {
        let oid = parse_object_id_28(&object_id)?;
        let ids = runtime::with_gcs(|binding| binding.get_object_locations_local(oid))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("get_object_locations: {e}"))
            })?;
        // `Vec<Vec<u8>>` introspects to `list[bytes]` because PyO3 maps
        // `Vec<u8>` to `bytes` (not `list[int]`) — exactly the runtime
        // surface, no fix_stubs override needed.
        Ok(ids.into_iter().map(|n| n.to_vec()).collect())
    }

    /// Pull `object_id` from a (possibly remote) raylet at `host:port`.
    /// Returns `(metadata, data)` as bytes pairs.
    #[pyfunction]
    pub fn pull_object(
        py: Python<'_>,
        host: String,
        port: u16,
        object_id: Vec<u8>,
    ) -> PyResult<(Py<PyBytes>, Py<PyBytes>)> {
        let oid = parse_object_id_28(&object_id)?;
        let addr: std::net::SocketAddr = format!("{host}:{port}").parse().map_err(|e| {
            PyValueError::new_err(format!("invalid raylet address {host}:{port}: {e}"))
        })?;
        let pulled = runtime::with_gcs(|binding| binding.pull_from(addr, oid))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("pull_object: {e}")))?;
        Ok((
            PyBytes::new(py, &pulled.metadata).unbind(),
            PyBytes::new(py, &pulled.data).unbind(),
        ))
    }

    /// Push `(metadata, data)` into the raylet at `host:port`'s plasma
    /// under `object_id`. Returns once the seal completes. Idempotent
    /// — pushing an id the target already has is a no-op success.
    ///
    /// Caller is responsible for any directory bookkeeping (e.g.
    /// notifying the owner-raylet via `register_object`); `Push`
    /// itself is just "shove these bytes into your plasma".
    #[pyfunction]
    pub fn push_object(
        host: String,
        port: u16,
        object_id: Vec<u8>,
        metadata: Vec<u8>,
        data: Vec<u8>,
    ) -> PyResult<()> {
        let oid = parse_object_id_28(&object_id)?;
        let addr: std::net::SocketAddr = format!("{host}:{port}").parse().map_err(|e| {
            PyValueError::new_err(format!("invalid raylet address {host}:{port}: {e}"))
        })?;
        runtime::with_gcs(|binding| binding.push_to(addr, oid, metadata, data))?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("push_object: {e}")))
    }

    /// Fetch `object_id` into local plasma by:
    ///   1. asking the owner-raylet for replica locations,
    ///   2. picking a holder (preferring one that's not us),
    ///   3. pulling from the holder's raylet,
    ///   4. sealing the bytes into local plasma,
    ///   5. registering this driver as a new replica at the owner.
    ///
    /// Idempotent: if the object is already in local plasma, the seal
    /// step is treated as a no-op success.
    ///
    /// Raises `RuntimeError` when there's no GCS connection, when no
    /// raylet hosts the object, or on transport failures.
    #[pyfunction]
    pub fn fetch_object(object_id: Vec<u8>, owner_node_id: Vec<u8>) -> PyResult<()> {
        use rayd_core::Metadata;
        let oid = parse_object_id_28(&object_id)?;
        let owner_nid = parse_node_id_16(&owner_node_id)?;

        let worker = runtime::require()?;

        // 1) Resolve owner_node_id → raylet host:port via GCS list_nodes.
        let nodes = runtime::with_gcs(super::gcs::GcsBinding::list_nodes)?
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(
                    "no GCS connection (set RAYD_GCS_ADDRESS before rayd.init())",
                )
            })?
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("list_nodes: {e}")))?;
        let owner_addr = raylet_addr_for(&nodes, owner_nid).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "owner node {} not found in GCS",
                short_hex(&owner_nid)
            ))
        })?;

        // 2) Ask the owner for replica locations.
        let locations = runtime::with_gcs(|b| b.get_object_locations_at(owner_addr, oid))?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("no GCS connection"))?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("get_object_locations: {e}"))
            })?;

        // 3) Pick a holder. Prefer a node that isn't us; fall back to
        //    any holder (e.g. the single-process self-fetch case).
        let local_nid = runtime::with_gcs(super::gcs::GcsBinding::node_id)?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("no GCS connection"))?;
        let holder_nid = locations
            .iter()
            .find(|nid| **nid != local_nid)
            .copied()
            .or_else(|| locations.first().copied())
            .ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "no holder registered for object {}",
                    short_hex_28(&oid)
                ))
            })?;
        let holder_addr = raylet_addr_for(&nodes, holder_nid).ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "holder node {} not found in GCS",
                short_hex(&holder_nid)
            ))
        })?;

        // 4) Pull from the holder.
        let pulled = runtime::with_gcs(|b| b.pull_from(holder_addr, oid))?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("no GCS connection"))?
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("pull: {e}")))?;

        // 5) Seal into local plasma. AlreadyExists is fine — somebody
        //    (maybe a previous fetch in this process) beat us to it.
        let metadata = Metadata::decode(&pulled.metadata).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("decode metadata: {e}"))
        })?;
        let id = ObjectId::from_bytes(oid);
        match worker.seal_value_to_plasma(id, metadata, bytes::Bytes::from(pulled.data)) {
            Ok(_)
            | Err(rayd_core::core_worker::CoreError::Plasma(rayd_plasma::PlasmaError::Server {
                kind: rayd_plasma::ServerErrorKind::AlreadyExists,
                ..
            })) => {}
            Err(e) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "seal_into_plasma: {e}"
                )));
            }
        }

        // 6) Notify the owner that we're now a replica too.
        runtime::with_gcs(|b| b.register_object_at(owner_addr, oid, local_nid))?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("no GCS connection"))?
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("register at owner: {e}"))
            })?;

        Ok(())
    }

    fn raylet_addr_for(
        nodes: &[rayd_gcs::NodeInfo],
        node_id: [u8; 16],
    ) -> Option<std::net::SocketAddr> {
        use std::net::ToSocketAddrs;
        for n in nodes {
            let Some(addr) = n.address.as_ref() else {
                continue;
            };
            if addr.node_id.len() != 16 || addr.node_id.as_slice() != node_id.as_slice() {
                continue;
            }
            let port = u16::try_from(addr.port).ok()?;
            // `addr.host` may be a hostname (e.g. gethostname()) or a
            // bare IP. Resolve via the OS so peer dialing works in
            // both cases.
            let resolved = (addr.host.as_str(), port).to_socket_addrs().ok()?.next()?;
            return Some(resolved);
        }
        None
    }

    fn short_hex(id: &[u8; 16]) -> String {
        id.iter().take(4).fold(String::new(), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    fn short_hex_28(id: &[u8; 28]) -> String {
        id.iter().take(4).fold(String::new(), |mut acc, b| {
            use std::fmt::Write as _;
            let _ = write!(acc, "{b:02x}");
            acc
        })
    }

    fn parse_object_id_28(bytes: &[u8]) -> PyResult<[u8; 28]> {
        if bytes.len() != 28 {
            return Err(PyValueError::new_err(format!(
                "object_id requires 28 bytes, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 28];
        buf.copy_from_slice(bytes);
        Ok(buf)
    }

    fn parse_node_id_16(bytes: &[u8]) -> PyResult<[u8; 16]> {
        if bytes.len() != 16 {
            return Err(PyValueError::new_err(format!(
                "node_id requires 16 bytes, got {}",
                bytes.len()
            )));
        }
        let mut buf = [0u8; 16];
        buf.copy_from_slice(bytes);
        Ok(buf)
    }

    // ──────────────────────────────────────────────────────────────────
    // Helpers (visible only inside the inline module)
    // ──────────────────────────────────────────────────────────────────

    fn parse_timeout(seconds: Option<f64>) -> PyResult<Option<Duration>> {
        match seconds {
            None => Ok(None),
            Some(s) if s.is_finite() && s >= 0.0 => Ok(Some(Duration::from_secs_f64(s))),
            Some(s) => Err(PyValueError::new_err(format!("invalid timeout: {s}"))),
        }
    }

    /// Extract a `PyObjectRef` from an arbitrary Python value.
    ///
    /// Pyo3's auto-generated `FromPyObject` for `frozen + Clone + from_py_object`
    /// pyclasses returns `PyClassGuardError` instead of `PyErr`. We sidestep
    /// the conversion by going through `downcast` + `borrow().clone()`.
    /// Single-element extractor for `get()` which accepts either an
    /// `ObjectRef` or a list. Other functions take `Vec<PyObjectRef>`
    /// directly so they don't need this helper.
    fn extract_ref(value: &Bound<'_, PyAny>) -> PyResult<PyObjectRef> {
        let bound = value.cast::<PyObjectRef>()?;
        Ok(bound.borrow().clone())
    }

    fn error_info_from_object(
        category: ErrorCategory,
        raw_code: u16,
        data: &[u8],
    ) -> PyResult<ErrorInfo> {
        let payload = ErrorPayload::decode(data).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("corrupted error payload: {e}"))
        })?;
        let mut info = ErrorInfo::new(category, payload.message).with_raw_code(raw_code);
        if let Some(tb) = payload.traceback {
            info = info.with_traceback(tb);
        }
        Ok(info)
    }

    fn raise_from_error_object(
        py: Python<'_>,
        category: ErrorCategory,
        _raw_code: u16,
        data: &[u8],
    ) -> PyErr {
        // Best-effort: rehydrate the original Python exception when we have
        // a pickled copy, so user code can `except SpecificError`. Falls
        // back to a generic `RuntimeError` carrying the message + traceback.
        match ErrorPayload::decode(data) {
            Ok(payload) => {
                if let Some(blob) = &payload.pickled_python_exception {
                    if let Ok(value) = serialize::loads(py, blob) {
                        return PyErr::from_value(value.bind(py).clone());
                    }
                }
                let msg = format!(
                    "rayd task failed: category={category:?}; {}",
                    payload.message,
                );
                pyo3::exceptions::PyRuntimeError::new_err(msg)
            }
            Err(e) => {
                pyo3::exceptions::PyRuntimeError::new_err(format!("corrupted error payload: {e}"))
            }
        }
    }

    // Convince clippy that PyKeyError is used (we re-export it via PyResult
    // mapping in a future revision). Keeps the unused-import lint quiet.
    #[allow(dead_code)]
    fn _silence_unused_imports() {
        let _ = PyKeyError::new_err::<&str>("");
    }
}
