from collections.abc import Mapping, Sequence
from typing import Final, final

__version__: Final[str]

@final
class ActorInfo:
    """Snapshot view of one named actor, returned by `list_actors()` and
    `_lookup_named_actor()`.
    """
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __repr__(self, /) -> str: ...
    @property
    def actor_id(self, /) -> bytes:
        """16-byte driver-minted actor id."""
        ...
    @property
    def driver_actor_host(self, /) -> str:
        """Host of the owner driver's actor-RPC TCP listener. Empty
        when the owner runs without one.
        """
        ...
    @property
    def driver_actor_port(self, /) -> int:
        """Port of the owner driver's actor-RPC TCP listener. Zero
        alongside an empty host means "no listener".
        """
        ...
    @property
    def name(self, /) -> str: ...
    @property
    def owner_node_id(self, /) -> bytes:
        """16-byte node id of the driver that owns the actor. Empty when
        the owner driver registered without an associated node.
        """
        ...
    @property
    def owner_pid(self, /) -> int: ...
    @property
    def registered_at_unix_ms(self, /) -> int: ...

@final
class Address:
    """Address of a worker process. Carries host, port, and the worker's id."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __new__(cls, /, host: str, port: int, worker_id_bytes: Sequence[int]) -> Address: ...
    def __reduce__(self, /) -> tuple[object, tuple[object, ...]]: ...
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...
    @property
    def host(self, /) -> str: ...
    def is_resolved(self, /) -> bool:
        """Whether this address carries a non-nil worker id."""
        ...
    @staticmethod
    def nil() -> Address:
        """Placeholder address for "not yet resolved" cases."""
        ...
    @property
    def port(self, /) -> int: ...
    @property
    def worker_id(self, /) -> bytes: ...

@final
class ErrorCategory:
    """Coarse user-facing error category. The granular `raw_code` lives on
    `ErrorInfo`.
    """

    ActorDied: Final[ErrorCategory]
    FetchTimeout: Final[ErrorCategory]
    ObjectLost: Final[ErrorCategory]
    ObjectUnreconstructable: Final[ErrorCategory]
    OutOfMemory: Final[ErrorCategory]
    OwnerDied: Final[ErrorCategory]
    RuntimeEnvFailed: Final[ErrorCategory]
    TaskCancelled: Final[ErrorCategory]
    TaskException: Final[ErrorCategory]
    Unschedulable: Final[ErrorCategory]
    WorkerDied: Final[ErrorCategory]
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __repr__(self, /) -> str: ...

@final
class ErrorInfo:
    """Information about a failed `ObjectRef` recoverable without unpickling."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __new__(
        cls,
        /,
        category: ErrorCategory,
        message: str,
        traceback: str | None = None,
        raw_code: int = 0,
    ) -> ErrorInfo: ...
    def __repr__(self, /) -> str: ...
    @property
    def category(self, /) -> ErrorCategory: ...
    @property
    def message(self, /) -> str: ...
    @property
    def raw_code(self, /) -> int: ...
    @property
    def traceback(self, /) -> str | None: ...

@final
class JobInfo:
    """Snapshot view of one job, returned by `list_jobs()`."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __repr__(self, /) -> str: ...
    @property
    def driver_host(self, /) -> str: ...
    @property
    def driver_pid(self, /) -> int: ...
    @property
    def finished_at_unix_ms(self, /) -> int: ...
    @property
    def job_id(self, /) -> bytes: ...
    @property
    def node_id(self, /) -> bytes:
        """16-byte node id this job's driver is attached to. Empty bytes
        when the job isn't linked to a registered node.
        """
        ...
    @property
    def registered_at_unix_ms(self, /) -> int: ...
    @property
    def status(self, /) -> str:
        """One of `"running" | "finished" | "failed" | "unspecified"`."""
        ...

@final
class NodeInfo:
    """Snapshot view of one node, returned by `list_nodes()`."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __repr__(self, /) -> str: ...
    @property
    def host(self, /) -> str: ...
    @property
    def last_heartbeat_unix_ms(self, /) -> int: ...
    @property
    def node_id(self, /) -> bytes: ...
    @property
    def plasma_socket(self, /) -> str: ...
    @property
    def port(self, /) -> int: ...
    @property
    def registered_at_unix_ms(self, /) -> int: ...
    @property
    def resources(self, /) -> Resources: ...
    @property
    def status(self, /) -> str:
        """One of `"alive" | "draining" | "dead" | "unspecified"`."""
        ...

@final
class ObjectId:
    """28-byte identifier of an object in the distributed store.

    Equivalent to Ray's `ObjectID`: deterministically derived from the
    parent task id plus a 4-byte return index, so callers can predict
    ids before the producing task runs.
    """
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __new__(cls, /, bytes: Sequence[int]) -> ObjectId: ...
    def __reduce__(self, /) -> tuple[object, tuple[object, ...]]: ...
    def __repr__(self, /) -> str: ...
    def __str__(self, /) -> str: ...
    @staticmethod
    def for_return(task_bytes: Sequence[int], return_index: int) -> ObjectId:
        """Build an id from the parent task's bytes (24) and a return index."""
        ...
    @property
    def hex(self, /) -> str:
        """Lowercase hex (56 characters)."""
        ...
    def is_nil(self, /) -> bool:
        """Whether this id equals the all-zero sentinel."""
        ...
    @staticmethod
    def nil() -> ObjectId:
        """The all-zero sentinel id."""
        ...
    @staticmethod
    def random() -> ObjectId:
        """Generate a fresh random id."""
        ...
    @property
    def return_index(self, /) -> int:
        """0-based return index encoded in the last 4 bytes."""
        ...
    def to_bytes(self, /) -> bytes:
        """Raw 28-byte representation."""
        ...

@final
class ObjectRef:
    """Reference to a value in the distributed object store."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __new__(
        cls, /, object_id: ObjectId, owner: Address, owner_node_id: Sequence[int] | None = None
    ) -> ObjectRef: ...
    def __reduce__(self, /) -> tuple[object, tuple[object, ...]]: ...
    def __repr__(self, /) -> str: ...
    def exception(self, /) -> object | None:
        """Returns the original Python exception. Heavier than `peek_error`:
        unpickles the user payload. `None` if pending or successful, or
        if the exception wasn't picklable.
        """
        ...
    @property
    def hex(self, /) -> str:
        """Lowercase hex of the underlying `ObjectId`."""
        ...
    def is_failed(self, /) -> bool:
        """Convenience: whether `state() == Failed`."""
        ...
    def is_ready(self, /) -> bool:
        """Convenience: whether `state()` is one of the ready states."""
        ...
    @property
    def object_id(self, /) -> ObjectId:
        """The id of the referenced object."""
        ...
    @property
    def owner(self, /) -> Address:
        """The address of the owner worker."""
        ...
    @property
    def owner_node_id(self, /) -> bytes | None:
        """16-byte GCS node id of the owner-raylet, or `None` when this
        ref wasn't created under a GCS-attached driver.
        """
        ...
    def peek_error(self, /) -> ErrorInfo | None:
        """Returns the error info for failed refs without unpickling the
        user-supplied exception. `None` for pending or successful refs.
        """
        ...
    def state(self, /) -> RefState:
        """Snapshot of the ref's lifecycle state. Cheap: reads metadata only.

        Returns `Pending` when no runtime is initialized, so the call
        degrades gracefully before `init()`. Returns `ReadyRemote`
        when the ref carries an `owner_node_id` that's NOT this node
        — i.e. we know the bytes live on a peer raylet but haven't
        pulled them yet. After `rayd.get` (or `_native.fetch_object`)
        seals locally, this flips to `ReadyLocal`.
        """
        ...

@final
class RefState:
    """Lifecycle state of an `ObjectRef` as observed from the holder's worker."""

    Failed: Final[RefState]
    Pending: Final[RefState]
    ReadyLocal: Final[RefState]
    ReadyRemote: Final[RefState]
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __int__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __repr__(self, /) -> str: ...
    def is_failed(self, /) -> bool:
        """Whether the state is `FAILED`."""
        ...
    def is_ready(self, /) -> bool:
        """Whether the state is `READY_LOCAL`, `READY_REMOTE`, or `FAILED`."""
        ...

@final
class Resources:
    """Resource counts a node advertises to the GCS."""
    def __eq__(self, value: object, /) -> bool: ...
    def __hash__(self, /) -> int: ...
    def __ne__(self, value: object, /) -> bool: ...
    def __new__(
        cls, /, num_cpus: int = 0, num_gpus: int = 0, memory_bytes: int = 0
    ) -> Resources: ...
    def __repr__(self, /) -> str: ...
    @property
    def memory_bytes(self, /) -> int: ...
    @property
    def num_cpus(self, /) -> int: ...
    @property
    def num_gpus(self, /) -> int: ...

def _evict_local(object_id: Sequence[int]) -> None: ...
def _is_spilled(object_id: Sequence[int]) -> bool: ...
def _lineage_status_str(object_id: Sequence[int]) -> str: ...
def _lookup_named_actor(name: str) -> ActorInfo | None: ...
def _mint_actor_result_ref(owner_node_id: Sequence[int] | None = None) -> ObjectRef: ...
def _plasma_socket_path() -> str: ...
def _pool_pending() -> int: ...
def _record_plasma_seal(
    object_id: Sequence[int], metadata: Sequence[int], data_size: int
) -> None: ...
def _register_named_actor(
    name: str, actor_id: Sequence[int], driver_actor_host: str = "", driver_actor_port: int = 0
) -> None: ...
def _spill_object(object_id: Sequence[int]) -> bool: ...
def _unregister_named_actor(name: str, actor_id: Sequence[int]) -> None: ...
def _worker_seal(object_id: Sequence[int], metadata: Sequence[int], data: Sequence[int]) -> int: ...
def cluster_session_id() -> bytes | None:
    """16-byte cluster session id assigned by the GCS we connected to.
    Returns `None` when `RAYD_GCS_ADDRESS` was not set.
    """
    ...

def fetch_object(object_id: Sequence[int], owner_node_id: Sequence[int]) -> None:
    """Fetch `object_id` into local plasma by:
      1. asking the owner-raylet for replica locations,
      2. picking a holder (preferring one that's not us),
      3. pulling from the holder's raylet,
      4. sealing the bytes into local plasma,
      5. registering this driver as a new replica at the owner.

    Idempotent: if the object is already in local plasma, the seal
    step is treated as a no-op success.

    Raises `RuntimeError` when there's no GCS connection, when no
    raylet hosts the object, or on transport failures.
    """
    ...

def free(refs: Sequence[ObjectRef]) -> None:
    """Free a list of refs from the local store. (No-op if not present.)"""
    ...

def get(refs: object, timeout: float | None = None) -> object:
    """Block until each ref is resolved, then return the values. Raises on
    the first failure encountered. For partial-success semantics, use
    `get_settled`.

    `refs` may be a single `ObjectRef` or a list of them.
    """
    ...

def get_object_locations(object_id: Sequence[int]) -> list[bytes]:
    """Ask the LOCAL raylet which nodes hold `object_id`. Returns the
    list of 16-byte `node_id`s (empty when no replicas are known —
    not an error).
    """
    ...

def get_settled(
    refs: Sequence[ObjectRef], timeout: float | None = None
) -> list[tuple[str, object]]:
    """Like `get`, but returns one entry per ref without raising on
    individual failures. The result is a list whose entries are:

    - the value, on success;
    - `RaydError` (or a subclass) wrapped via `ErrorInfo`, on failure;
    - the special sentinel `Pending`, when the ref hadn't resolved by
      the supplied `timeout`.

    Concretely each entry is a 2-tuple `(kind, payload)` where `kind`
    is `"ok" | "err" | "pending"`. The Python facade in
    `python/rayd/__init__.py` rewraps these as `Ok`/`Err`/`Pending`
    dataclasses for ergonomics.
    """
    ...

def init(address: str | None = None) -> None:
    """Initialize the rayd runtime. Idempotent: calling twice is a no-op.

    `address` is reserved for connecting to an existing head node and
    is currently ignored (Phase 1 is single-process).
    """
    ...

def is_initialized() -> bool:
    """Whether `init()` has been called more recently than `shutdown()`."""
    ...

def job_id() -> bytes | None:
    """16-byte job id this driver was assigned by the GCS. `None` when
    no GCS connection.
    """
    ...

def list_actors() -> list[ActorInfo]:
    """Snapshot all named actors the GCS knows about. Mostly for tests
    & tooling; production callers should use `_lookup_named_actor`.
    """
    ...

def list_jobs() -> list[JobInfo]:
    """Snapshot all jobs the GCS knows about (running + finished).

    Raises `RuntimeError` if `RAYD_GCS_ADDRESS` was not set on `init()`.
    """
    ...

def list_nodes() -> list[NodeInfo]:
    """Snapshot all nodes the GCS knows about.

    Raises `RuntimeError` if `RAYD_GCS_ADDRESS` was not set on `init()`,
    since there's no GCS to query.
    """
    ...

def local_raylet_address() -> tuple[str, int] | None:
    """Address `host:port` of the raylet this driver started.
    `None` when `RAYD_GCS_ADDRESS` was not set.
    """
    ...

def node_id() -> bytes | None:
    """16-byte node id this driver was assigned by the GCS. `None` when
    no GCS connection.
    """
    ...

def node_status_local(node_id: bytes) -> str | None:
    """Fast push-driven liveness lookup (Phase 4.3.3c).

    Returns the locally-cached status of `node_id` ("alive" /
    "draining" / "dead") sourced from the raylet's `WatchNodes`
    subscription. `None` means the subscriber hasn't observed this
    node yet — caller should fall back to `list_nodes()` for an
    authoritative answer.
    """
    ...

def pull_object(host: str, port: int, object_id: Sequence[int]) -> tuple[bytes, bytes]:
    """Pull `object_id` from a (possibly remote) raylet at `host:port`.
    Returns `(metadata, data)` as bytes pairs.
    """
    ...

def push_object(
    host: str, port: int, object_id: Sequence[int], metadata: Sequence[int], data: Sequence[int]
) -> None:
    """Push `(metadata, data)` into the raylet at `host:port`'s plasma
    under `object_id`. Returns once the seal completes. Idempotent
    — pushing an id the target already has is a no-op success.

    Caller is responsible for any directory bookkeeping (e.g.
    notifying the owner-raylet via `register_object`); `Push`
    itself is just "shove these bytes into your plasma".
    """
    ...

def put(value: object) -> ObjectRef:
    """Pickle `value` and store it under a fresh deterministic id. Returns
    the resulting `ObjectRef`.

    Routes to plasma when the pickled buffer exceeds the inline
    threshold; ALSO forces the plasma path (and registers the
    object at the local raylet's directory) when GCS is configured,
    so peers can pull it via `fetch_object`.
    """
    ...

def register_object(object_id: Sequence[int], holder_node_id: Sequence[int]) -> None:
    """Register a holder of `object_id` at the LOCAL raylet's directory.

    Pass this driver's own `node_id` after a `put()` so peers know
    who to pull from. 28-byte `object_id`, 16-byte `holder_node_id`.
    """
    ...

def shutdown() -> None:
    """Tear down the rayd runtime.

    Drains the worker thread pool with the GIL released so any task
    currently mid-flight can finish without deadlocking on the
    interpreter, then drops the plasma server and temp dir.
    """
    ...

def state(refs: Sequence[ObjectRef]) -> list[tuple[ObjectRef, RefState]]:
    """Snapshot per-ref state. One mutex acquisition for the whole batch;
    no payload deserialization.

    Returns a list of `(ref, state)` pairs rather than a dict so
    `PyO3`'s `experimental-inspect` can emit a precise
    `list[tuple[ObjectRef, RefState]]` type hint without relying
    on `Py<PyDict>` (which erases to bare `dict`). The Python
    facade rewraps via `dict(...)`.
    """
    ...

def submit_task(
    callable: object,
    args: tuple[object, ...],
    kwargs: Mapping[str, object] | None = None,
    num_returns: int = 1,
) -> list[ObjectRef]:
    """Submit a callable for asynchronous execution. Returns a list of
    `ObjectRef`s — one per return value. With `num_returns == 1` the
    list has length 1; the Python facade unwraps to a single ref.

    The callable is held by reference and invoked on a worker thread;
    it may run before, during, or after this call returns.
    """
    ...

def try_resubmit_for_lineage(object_id: Sequence[int]) -> bool:
    """Lineage-reconstruction hook: requeue a recorded task.

    If we recorded a task that produced `object_id`, the task has
    completed at least once, and its retry budget is non-zero,
    queue a fresh dispatch with the same `task_id` so the worker
    writes back to the same plasma slot. Returns `True` when a
    resubmit fired; `False` when no record / not yet completed /
    budget exhausted.
    """
    ...

def wait(
    refs: Sequence[ObjectRef], num_returns: int = 1, timeout: float | None = None
) -> tuple[list[ObjectRef], list[ObjectRef]]:
    """Wait for at least `num_returns` of `refs` to enter a terminal state.
    Returns `(ready, not_ready)` lists.
    """
    ...

def wait_with_states(
    refs: Sequence[ObjectRef], timeout: float | None = None
) -> list[tuple[ObjectRef, RefState]]:
    """Wait variant that returns a snapshot of states instead of a
    `(ready, not_ready)` split. Matches `state()` in shape but blocks
    for `timeout` to give pending refs a chance to land.
    """
    ...

__all__ = [
    "__version__",
    "ActorInfo",
    "Address",
    "ErrorCategory",
    "ErrorInfo",
    "JobInfo",
    "NodeInfo",
    "ObjectId",
    "ObjectRef",
    "RefState",
    "Resources",
    "_evict_local",
    "_is_spilled",
    "_lineage_status_str",
    "_lookup_named_actor",
    "_mint_actor_result_ref",
    "_plasma_socket_path",
    "_pool_pending",
    "_record_plasma_seal",
    "_register_named_actor",
    "_spill_object",
    "_unregister_named_actor",
    "_worker_seal",
    "cluster_session_id",
    "fetch_object",
    "free",
    "get",
    "get_object_locations",
    "get_settled",
    "init",
    "is_initialized",
    "job_id",
    "list_actors",
    "list_jobs",
    "list_nodes",
    "local_raylet_address",
    "node_id",
    "node_status_local",
    "pull_object",
    "push_object",
    "put",
    "register_object",
    "shutdown",
    "state",
    "submit_task",
    "try_resubmit_for_lineage",
    "wait",
    "wait_with_states",
]
