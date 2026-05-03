"""rayd: a Rust+PyO3 reimplementation of Ray Core's tasks, actors, and object store.

Phase 1 surface: the typed Python facade over `rayd._native`. The native module
returns broad types (`object`, `list[object]`, ...) at the FFI boundary; this
file narrows them into typed wrappers.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, final

from rayd import _native
from rayd._actor import ActorClass, ActorHandle, get_actor
from rayd._native import (
    Address,
    ErrorCategory,
    ErrorInfo,
    ObjectId,
    ObjectRef,
    RefState,
    __version__,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


# ── Distributed-runtime exceptions ─────────────────────────────────────


class OwnerDiedError(RuntimeError):
    """Raised when a remote `ObjectRef`'s owner-raylet is no longer alive.

    Phase 4.3.3 surfaces this via the GCS's node-liveness signal: if
    `rayd.get` would otherwise dispatch a `Pull` for a ref whose
    owner_node_id is `Draining` or `Dead` (or absent from the GCS),
    we fail fast with this exception instead of a transport error
    from the doomed RPC.
    """


class ObjectUnreconstructableError(RuntimeError):
    """Raised when a locally-lost object's task can no longer be replayed.

    Phase 4.4b: `rayd.get` consults the lineage manager when a
    local-owner ref's state is `Pending`. If the producing task has
    completed at least once but its retry budget has been exhausted,
    we surface this exception (mapping to Ray's
    `OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED`).
    """


class ActorDiedError(RuntimeError):
    """Raised when an actor's subprocess died and its method couldn't run.

    Phase 5.3: When the per-actor worker subprocess crashes, the
    driver seals every in-flight method call's `ObjectRef` with this
    error so `rayd.get` raises rather than hanging. After the actor's
    `max_restarts` budget is exhausted, every future
    `actor.method.remote(...)` call also raises this error.
    """


# ── Result hierarchy ────────────────────────────────────────────────────


@final
@dataclass(frozen=True, slots=True)
class Pending:
    """Sentinel: the ref had not resolved by the supplied timeout."""


@final
@dataclass(frozen=True, slots=True)
class Ok[T_co]:
    """A successful result carrying the unpickled value."""

    value: T_co


@final
@dataclass(frozen=True, slots=True)
class Err:
    """A failed result carrying the lightweight `ErrorInfo` (no exception unpickle)."""

    info: ErrorInfo


type Result[T_co] = Ok[T_co] | Err | Pending


# ── @rayd.remote ────────────────────────────────────────────────────────


@final
class RemoteFunction[**P, R]:
    """Wrap a user callable so it can be submitted via `.remote(...)`.

    Returned by the `@rayd.remote` decorator. The original callable is also
    available via `__wrapped__` for the rare case where a user wants to call
    it locally (e.g. inside a unit test).
    """

    def __init__(self, fn: Callable[P, R], *, num_returns: int = 1) -> None:
        if num_returns < 1:
            msg = f"num_returns must be >= 1, got {num_returns}"
            raise ValueError(msg)
        self._fn = fn
        self._num_returns = num_returns

    @property
    def __wrapped__(self) -> Callable[P, R]:
        return self._fn

    @property
    def num_returns(self) -> int:
        return self._num_returns

    def options(self, *, num_returns: int = 1) -> RemoteFunction[P, R]:
        """Return a copy of this remote function with overridden options."""
        return RemoteFunction(self._fn, num_returns=num_returns)

    def remote(self, *args: P.args, **kwargs: P.kwargs) -> ObjectRef:
        """Submit the wrapped callable; return its first `ObjectRef`."""
        refs = _native.submit_task(self._fn, args, kwargs, self._num_returns)
        if not refs:
            msg = f"submit_task returned no refs: {refs!r}"
            raise RuntimeError(msg)
        return refs[0]


def remote[**P, R](fn: Callable[P, R]) -> RemoteFunction[P, R]:
    """Wrap `fn` in a `RemoteFunction`; submit via `.remote(...)`.

    For stateful classes, use `@rayd.actor` instead — the two
    decorators are kept separate so the static return types are
    unambiguous (every class is also `Callable`, so a single
    overloaded decorator can't be precisely typed).
    """
    return RemoteFunction(fn)


def actor[R](cls: type[R], *, max_restarts: int = 3) -> ActorClass[R]:
    """Wrap a class so `MyClass.remote(*args)` creates an actor.

    Calling `.remote(*args, **kwargs)` spawns a per-actor subprocess,
    instantiates the class there, and returns an `ActorHandle`. Methods
    are invoked via `handle.method.remote(*args)` and run FIFO in
    that subprocess. Method exceptions surface through `rayd.get` as
    the original Python exception.

    `max_restarts` is the budget for restarting the subprocess after
    it crashes mid-method. Default `3`. After the budget is exhausted
    every future call raises `rayd.ActorDiedError`. Pending in-flight
    calls at the moment of a crash are sealed with `ActorDiedError`
    so `rayd.get` raises rather than hangs.

    **Typing caveat**: mypy doesn't propagate class-decorator return
    types through `@`-syntax (a known limitation — see e.g.
    python/mypy#3135). For statically-typed code, use the
    assignment form instead:

        class _MyActor:
            def __init__(self, x: int) -> None: ...

        MyActor = rayd.actor(_MyActor)  # MyActor: ActorClass[_MyActor]

    The decorator form `@rayd.actor` works at runtime but mypy will
    leave `MyActor` typed as `type[_MyActor]`.
    """
    return ActorClass(cls, max_restarts=max_restarts)


# ── Lifecycle ───────────────────────────────────────────────────────────


def init(address: str | None = None) -> None:
    """Initialize the rayd runtime. Idempotent."""
    _native.init(address)
    # Start the per-driver actor-RPC TCP server when a GCS is attached
    # so peers can dial us for cross-driver actor invocation. No-op
    # without a GCS — there's no way for a peer to discover us anyway.
    if _native.node_id() is not None:
        from rayd._actor_rpc import _ensure_rpc_server  # noqa: PLC0415

        _ensure_rpc_server()


def shutdown() -> None:
    """Tear down the rayd runtime."""
    # Stop the actor-RPC server BEFORE the native runtime tears down
    # plasma — otherwise a late incoming frame could touch a freed
    # worker.
    from rayd._actor_rpc import _shutdown_rpc_server  # noqa: PLC0415

    _shutdown_rpc_server()
    _native.shutdown()


def is_initialized() -> bool:
    """Report whether `init()` has been called more recently than `shutdown()`."""
    return _native.is_initialized()


# ── Object store API ────────────────────────────────────────────────────


def put(value: object) -> ObjectRef:
    """Pickle `value` and store it; return its `ObjectRef`."""
    return _native.put(value)


def get(refs: ObjectRef | Sequence[ObjectRef], timeout: float | None = None) -> object:
    """Block until `refs` resolve. Raises on first failure.

    For refs whose owner-raylet is on a remote node, this fetches the
    object via `Pull` first (locating the holder through the GCS, sealing
    the bytes into local plasma, and notifying the owner of the new
    replica). Use `get_settled` instead when you need partial-success
    semantics.
    """
    _ensure_local_for_remote_refs(refs)
    return _native.get(refs, timeout)


def _ensure_local_for_remote_refs(refs: ObjectRef | Sequence[ObjectRef]) -> None:
    """Prepare refs for `_native.get`.

    Three paths per ref:
    - **Remote owner** (owner_node_id ≠ local node): liveness gate,
      then dispatch `fetch_object` (or raise `OwnerDiedError`).
    - **Local owner, Pending state**: consult the lineage manager.
      If completed-but-lost and budget remains → auto-resubmit. If
      completed but budget exhausted → raise
      `ObjectUnreconstructableError`. If still in flight → fall
      through and let `_native.get` block on the original.
    - **Local owner, ready state**: nothing to do.
    """
    if not _native.is_initialized():
        return
    local_nid = _native.node_id()
    # Distinguish single-ref from any sequence by testing for ObjectRef
    # rather than `isinstance(refs, list)` — the parameter accepts any
    # `Sequence[ObjectRef]` (tuples, generators-realised-as-tuples, ...),
    # not just lists, so an isinstance-on-list check would mis-route.
    targets: Sequence[ObjectRef] = [refs] if isinstance(refs, ObjectRef) else refs
    nodes_cache: list[_native.NodeInfo] | None = None
    for ref in targets:
        owner_nid = ref.owner_node_id
        if local_nid is not None and owner_nid is not None and owner_nid != local_nid:
            nodes_cache = _resolve_remote_ref(ref, owner_nid, nodes_cache)
        else:
            _maybe_resubmit_local_lineage(ref)


def _resolve_remote_ref(
    ref: ObjectRef,
    owner_nid: bytes,
    nodes_cache: list[_native.NodeInfo] | None,
) -> list[_native.NodeInfo]:
    """Liveness gate + fetch for one remote-owner ref.

    Returns the (possibly newly-populated) `list_nodes` cache so the
    caller can reuse it across refs in the same batch.

    Phase 4.3.3c: try the local push-driven NodeIndex first
    (`_native.node_status_local`). Only fall back to a synchronous
    `list_nodes()` RPC when the index hasn't seen this node yet — at
    steady state every remote-owner ref skips the RPC and uses the
    sub-second-fresh cached status.
    """
    fast = _native.node_status_local(owner_nid)
    if fast is not None:
        if fast != "alive":
            msg = f"owner of ObjectRef({ref.hex}) is {fast}; cannot fetch"
            raise OwnerDiedError(msg)
        _fetch_with_retries(ref.object_id.to_bytes(), owner_nid)
        return nodes_cache if nodes_cache is not None else []

    if nodes_cache is None:
        nodes_cache = list(_native.list_nodes())
    owner_status = next(
        (n.status for n in nodes_cache if bytes(n.node_id) == owner_nid),
        None,
    )
    if owner_status != "alive":
        msg = f"owner of ObjectRef({ref.hex}) is {owner_status or 'absent from GCS'}; cannot fetch"
        raise OwnerDiedError(msg)
    _fetch_with_retries(ref.object_id.to_bytes(), owner_nid)
    return nodes_cache


# Total wall-clock budget across the retry loop. The seal on the owner
# side (after an actor method returns OR after the actor's reader
# observes a crash) typically lands within tens of milliseconds; this
# budget is a generous safety net.
_FETCH_RETRY_TOTAL_S = 5.0
_FETCH_RETRY_SLEEP_S = 0.05


def _fetch_with_retries(oid_bytes: bytes, owner_nid: bytes) -> None:
    """Retry `fetch_object` while it returns "no holder registered".

    The owner-side seal is asynchronous (it lands when the actor
    method returns OR when the owner's reader thread observes a
    crash). A caller's fetch can race that seal — if the directory is
    still empty when we ask, we retry briefly. Other failures
    (transport, owner unreachable) propagate immediately.
    """
    deadline = time.monotonic() + _FETCH_RETRY_TOTAL_S
    last_err: RuntimeError = RuntimeError("fetch_object never ran")
    while True:
        try:
            _native.fetch_object(oid_bytes, owner_nid)
        except RuntimeError as e:
            if "no holder registered" not in str(e):
                raise
            last_err = e
        else:
            return
        if time.monotonic() >= deadline:
            raise last_err
        time.sleep(_FETCH_RETRY_SLEEP_S)


def _maybe_resubmit_local_lineage(ref: ObjectRef) -> None:
    """Auto-resubmit hook for local-owner refs that are Pending."""
    if ref.state() != _native.RefState.Pending:
        return
    oid = ref.object_id.to_bytes()
    status = _native._lineage_status_str(oid)  # noqa: SLF001
    if status == "ready":
        _native.try_resubmit_for_lineage(oid)
    elif status == "exhausted":
        msg = f"object {ref.hex} cannot be reconstructed: task lineage exhausted retry budget"
        raise ObjectUnreconstructableError(msg)
    # `not_recorded` / `not_yet_completed`: let `_native.get`
    # block on the existing attempt; nothing to do here.


def get_settled(
    refs: Sequence[ObjectRef],
    timeout: float | None = None,
) -> list[Result[object]]:
    """Resolve every ref into a typed `Result` without raising on failure.

    Returns one `Ok(value)`, `Err(info)`, or `Pending` per input ref.
    """
    _ensure_local_for_remote_refs(refs)
    raw = _native.get_settled(refs, timeout)
    out: list[Result[object]] = []
    for kind, payload in raw:
        if kind == "ok":
            out.append(Ok(value=payload))
        elif kind == "err":
            if not isinstance(payload, ErrorInfo):
                msg = f"expected ErrorInfo, got {type(payload).__name__}"
                raise TypeError(msg)
            out.append(Err(info=payload))
        elif kind == "pending":
            out.append(Pending())
        else:
            msg = f"unknown kind from get_settled: {kind!r}"
            raise RuntimeError(msg)
    return out


def state(refs: Sequence[ObjectRef]) -> dict[ObjectRef, RefState]:
    """Snapshot every ref's lifecycle state. Cheap; no deserialization."""
    return dict(_native.state(refs))


def wait(
    refs: Sequence[ObjectRef],
    num_returns: int = 1,
    timeout: float | None = None,
) -> tuple[list[ObjectRef], list[ObjectRef]]:
    """Wait for `num_returns` refs to settle; return `(ready, not_ready)`."""
    ready, not_ready = _native.wait(refs, num_returns, timeout)
    return ready, not_ready


def wait_with_states(
    refs: Sequence[ObjectRef],
    timeout: float | None = None,
) -> dict[ObjectRef, RefState]:
    """Block up to `timeout` for refs to settle; return per-ref states."""
    return dict(_native.wait_with_states(refs, timeout))


def free(refs: Sequence[ObjectRef]) -> None:
    """Free a list of refs from the local store."""
    _native.free(refs)


__all__ = [
    "ActorClass",
    "ActorDiedError",
    "ActorHandle",
    "Address",
    "Err",
    "ErrorCategory",
    "ErrorInfo",
    "ObjectId",
    "ObjectRef",
    "ObjectUnreconstructableError",
    "Ok",
    "OwnerDiedError",
    "Pending",
    "RefState",
    "RemoteFunction",
    "Result",
    "__version__",
    "actor",
    "free",
    "get",
    "get_actor",
    "get_settled",
    "init",
    "is_initialized",
    "put",
    "remote",
    "shutdown",
    "state",
    "wait",
    "wait_with_states",
]
