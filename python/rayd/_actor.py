"""Phase 5.1-5.3: per-actor worker subprocess + restart on crash.

A `@rayd.actor` decorator on a class produces an `ActorClass`. Calling
`ActorClass.remote(*args, **kwargs)` cloudpickles the class plus its
constructor args, spawns a dedicated `python -m rayd._actor_worker`
subprocess (one per actor), and returns an `ActorHandle`. Method
calls go through `handle.method.remote(*args)` and are routed to that
subprocess via a UDS; results land in shared plasma and resolve the
returned `ObjectRef`.

Subprocess isolation means CPU-bound or GIL-grabbing actor methods
don't block the driver. A reader thread on the driver consumes
`actor_call_complete` frames and updates the local `MemoryStore`'s
`PlasmaIndex` so `rayd.get` resolves through plasma.

When the subprocess dies mid-method (a method that raises
`SystemExit`, segfaults, etc.), the driver:

1. Seals every in-flight method call's `ObjectRef` with an
   `ActorDiedError` so callers' `rayd.get` raises rather than hangs.
2. If the actor's `max_restarts` budget remains, spawns a fresh
   subprocess with the same class + ctor args and continues
   accepting calls. Otherwise marks the actor dead and rejects
   future submits with `ActorDiedError`.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, final

import cloudpickle  # type: ignore[import-untyped]

from rayd import _native

if TYPE_CHECKING:
    from rayd._native import ObjectRef

_HANDSHAKE_TIMEOUT_S = 5.0
_TERMINATE_TIMEOUT_S = 5.0
_SUBPROCESS_REAP_TIMEOUT_S = 2.0
_REMOTE_DIAL_TIMEOUT_S = 5.0
_ACTOR_ID_SIZE = 16
_NODE_ID_SIZE = 16


# ── Process-wide actor registry ────────────────────────────────────────
#
# Maps a per-actor 16-byte id to the live `_ActorSubprocess`. Used by
# `ActorHandle.__reduce__` so a pickled handle can be unpickled into a
# fresh `ActorHandle` that talks to the same subprocess (within the
# same driver process). Cross-driver method calls need a network RPC
# (today the per-actor UDS is local-only) — queued for 5.4c.

_ACTOR_REGISTRY: dict[bytes, _ActorSubprocess] = {}
_ACTOR_REGISTRY_LOCK = threading.Lock()


def _register_actor(actor_id: bytes, runner: _ActorSubprocess) -> None:
    with _ACTOR_REGISTRY_LOCK:
        _ACTOR_REGISTRY[actor_id] = runner


def _unregister_actor(actor_id: bytes) -> None:
    with _ACTOR_REGISTRY_LOCK:
        _ACTOR_REGISTRY.pop(actor_id, None)


def _lookup_actor(actor_id: bytes) -> _ActorSubprocess:
    runner = _lookup_actor_optional(actor_id)
    if runner is None:
        msg = (
            f"actor {actor_id.hex()} is not registered in this driver "
            "(unpickled across processes? cross-driver actors land in 5.4c)"
        )
        raise LookupError(msg)
    return runner


def _lookup_actor_optional(actor_id: bytes) -> _ActorSubprocess | None:
    """Non-raising lookup. Returns `None` for unknown ids."""
    with _ACTOR_REGISTRY_LOCK:
        return _ACTOR_REGISTRY.get(actor_id)


def _rebuild_actor_handle(
    actor_id: bytes,
    owner_node_id: bytes = b"",
    host: str = "",
    port: int = 0,
) -> ActorHandle | _RemoteActorHandle:
    """Pickle factory for `ActorHandle` / `_RemoteActorHandle`.

    Resolution order:
      1. If the actor is in *this* driver's registry → `ActorHandle`
         wrapping the live subprocess.
      2. Else if the embedded `owner_node_id` differs from this
         driver's node id and a `(host, port)` is present →
         `_RemoteActorHandle` that dials the owner driver's listener.
      3. Otherwise raise `LookupError` — either the actor used to
         live here and has been terminated, or the pickle predates
         the cross-driver wire (no embedded address).
    """
    runner = _lookup_actor_optional(actor_id)
    if runner is not None:
        return ActorHandle(runner)
    # Decide remote vs. stale-local.
    local_nid: bytes | None = _native.node_id() if _native.is_initialized() else None
    is_remote = (
        bool(owner_node_id)
        and local_nid is not None
        and bytes(local_nid) != owner_node_id
        and bool(host)
        and port > 0
    )
    if is_remote:
        return _RemoteActorHandle(actor_id, owner_node_id, host, port)
    msg = (
        f"actor {actor_id.hex()} is not registered in this driver "
        "(terminated, or unpickled in a process that never owned it)"
    )
    raise LookupError(msg)


def _register_module_for_pickle_by_value(cls: type) -> None:
    """Tell cloudpickle to serialise `cls`'s defining module by value.

    Without this, cloudpickle stores the class as `module.Class`; the
    actor subprocess tries to `import module` and fails when the
    user's class lives in (e.g.) a pytest test module that isn't on
    the subprocess's `sys.path`. Pickling by value embeds the class
    body in the blob so the subprocess doesn't need source access.

    Skips `__main__`, `builtins`, and modules that aren't reachable
    via `sys.modules` — those either don't need it or can't be
    registered.
    """
    mod_name = getattr(cls, "__module__", None)
    if not mod_name or mod_name in {"__main__", "builtins"}:
        return
    mod = sys.modules.get(mod_name)
    if mod is None:
        return
    with contextlib.suppress(Exception):
        cloudpickle.register_pickle_by_value(mod)


@final
class _ActorSubprocess:
    """Owns one actor's subprocess + the UDS connection + reader thread.

    Restarts the subprocess on crash, up to `max_restarts` times.
    """

    def __init__(
        self,
        cls: type,
        args: tuple[object, ...],
        kwargs: dict[str, object] | None,
        max_restarts: int,
        name: str | None = None,
    ) -> None:
        self._cls = cls
        self._ctor_args = args
        self._ctor_kwargs = kwargs
        self._max_restarts = max_restarts
        self._restarts_used = 0
        # Stable identity used by `ActorHandle.__reduce__` so a
        # pickled handle round-trips back to the same subprocess.
        self._actor_id: bytes = uuid.uuid4().bytes
        # Name registered in the GCS, or None for unnamed actors.
        # Cleared on terminate so we don't try to unregister twice.
        self._registered_name: str | None = None
        _register_actor(self._actor_id, self)
        if name is not None:
            # Reserve the name in the GCS BEFORE the subprocess spawn so
            # a clash surfaces synchronously to the caller (and we don't
            # leak a subprocess we then have to tear down). Advertise
            # this driver's actor-RPC listener address so peers can
            # invoke us cross-driver.
            from rayd._actor_rpc import _driver_actor_rpc_address  # noqa: PLC0415

            addr = _driver_actor_rpc_address()
            host, port = addr if addr is not None else ("", 0)
            _native._register_named_actor(name, self._actor_id, host, port)  # noqa: SLF001
            self._registered_name = name
        # `_state_lock` protects in_flight, terminated/dead, and the
        # current sock/proc/reader pointers (which respawn replaces).
        # Always held briefly — never across blocking I/O.
        self._state_lock = threading.Lock()
        # `_send_lock` serializes outgoing frames so concurrent
        # submits don't interleave bytes on the wire. SEPARATE from
        # `_state_lock` so a slow `send()` (buffer full while the
        # subprocess is busy) doesn't block the reader from consuming
        # completions.
        self._send_lock = threading.Lock()
        self._terminated = False
        self._dead = False
        self._in_flight: set[bytes] = set()
        self._tempdir = tempfile.TemporaryDirectory(prefix="rayd-actor-")
        self._socket_seq = 0  # monotonic per-respawn socket name suffix

        # Lazy-import wire helpers to avoid the import-cycle warning
        # the in-process MVP hit.
        from rayd._worker import _decode, _encode, _recv_frame, _send_frame  # noqa: PLC0415

        self._encode = _encode
        self._decode = _decode
        self._recv_frame = _recv_frame
        self._send_frame = _send_frame

        _register_module_for_pickle_by_value(cls)
        try:
            self._spawn_subprocess()
        except BaseException:
            # Roll back the GCS reservation so the name is reusable.
            if self._registered_name is not None:
                with contextlib.suppress(Exception):
                    _native._unregister_named_actor(  # noqa: SLF001
                        self._registered_name,
                        self._actor_id,
                    )
                self._registered_name = None
            _unregister_actor(self._actor_id)
            raise

    def _spawn_subprocess(self) -> None:
        """Fork a fresh subprocess, hand it `actor_spawn`, start the reader."""
        # Use a per-respawn socket name so we don't trip over a stale
        # bind from the previous incarnation.
        self._socket_seq += 1
        socket_path = Path(self._tempdir.name) / f"actor-{self._socket_seq}.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(socket_path))
        listener.listen(1)
        listener.settimeout(_HANDSHAKE_TIMEOUT_S)

        plasma_socket = _native._plasma_socket_path()  # noqa: SLF001
        self._proc = subprocess.Popen(  # noqa: S603  trusted: own python module
            [
                sys.executable,
                "-m",
                "rayd._actor_worker",
                f"--actor-socket={socket_path}",
                f"--plasma-socket={plasma_socket}",
            ],
        )
        try:
            conn, _addr = listener.accept()
        except TimeoutError as e:
            self._proc.kill()
            self._proc.wait()
            msg = f"actor subprocess did not connect within {_HANDSHAKE_TIMEOUT_S}s"
            raise RuntimeError(msg) from e
        finally:
            listener.close()
        self._sock = conn

        # Greet exchange.
        ready = self._recv_frame(self._sock)
        if ready is None:
            msg = "actor subprocess closed before sending actor_ready"
            raise RuntimeError(msg)
        ready_msg = self._decode(ready)
        if ready_msg.get("kind") != "actor_ready":
            msg = f"actor subprocess: expected actor_ready, got {ready_msg.get('kind')!r}"
            raise RuntimeError(msg)
        pid_raw = ready_msg["pid"]
        if not isinstance(pid_raw, int):
            msg = f"actor_ready: pid must be int, got {type(pid_raw).__name__}"
            raise TypeError(msg)
        self._actor_pid: int = pid_raw

        spawn_frame = self._encode(
            {
                "kind": "actor_spawn",
                "class": cloudpickle.dumps(self._cls),
                "args": cloudpickle.dumps(self._ctor_args),
                "kwargs": (cloudpickle.dumps(self._ctor_kwargs) if self._ctor_kwargs else None),
            }
        )
        self._send_frame(self._sock, spawn_frame)

        # Each spawn gets its own reader thread; the old one (if
        # any) returned when its socket EOF'd.
        self._reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self._reader_thread.start()

    def _read_loop(self) -> None:
        # Capture references to our local socket / current state in
        # case respawn replaces them while we're mid-loop.
        sock = self._sock
        while True:
            try:
                frame = self._recv_frame(sock)
            except (ConnectionError, OSError):
                self._on_socket_closed(sock)
                return
            if frame is None:
                self._on_socket_closed(sock)
                return
            msg = self._decode(frame)
            kind = msg.get("kind")
            if kind == "actor_call_complete":
                oid = msg["result_oid"]
                metadata = msg["metadata"]
                data_size = msg["data_size"]
                if (
                    not isinstance(oid, bytes)
                    or not isinstance(metadata, bytes)
                    or not isinstance(data_size, int)
                ):
                    err_msg = "actor_call_complete: malformed frame"
                    raise TypeError(err_msg)
                with self._state_lock:
                    self._in_flight.discard(oid)
                _native._record_plasma_seal(oid, metadata, data_size)  # noqa: SLF001
            else:
                sys.stderr.write(
                    f"rayd._actor: unexpected frame from subprocess: {kind!r}\n",
                )

    def _on_socket_closed(self, sock: socket.socket) -> None:
        """Reader thread saw EOF/error.

        Decide whether it's a clean terminate or a crash; seal
        in-flight refs as errors, reap the dead subprocess, and
        restart if budget remains. The old `sock` is closed here so
        its fd is released eagerly (otherwise it sits open until the
        reader-thread frame is GC'd, which pytest --strict treats as
        an unraisable warning).
        """
        try:
            with self._state_lock:
                if self._terminated:
                    return
                if sock is not self._sock:
                    # We're an old reader thread whose socket was
                    # already replaced by a more recent respawn.
                    return
                # Crash path: seal in-flight refs as errors.
                in_flight = list(self._in_flight)
                self._in_flight.clear()
                for oid in in_flight:
                    self._seal_actor_died(oid)

                # Reap the dead subprocess.
                try:
                    self._proc.wait(timeout=_SUBPROCESS_REAP_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    self._proc.kill()
                    self._proc.wait()

                if self._restarts_used >= self._max_restarts:
                    self._dead = True
                    self._unregister_name_from_gcs()
                    _unregister_actor(self._actor_id)
                    return
                self._restarts_used += 1
                try:
                    self._spawn_subprocess()
                except Exception:  # noqa: BLE001
                    # Couldn't bring up a replacement: mark dead so
                    # future submits surface ActorDiedError instead
                    # of hanging.
                    self._dead = True
                    self._unregister_name_from_gcs()
                    _unregister_actor(self._actor_id)
        finally:
            # Always close the old socket fd before this reader exits.
            with contextlib.suppress(OSError):
                sock.close()

    def _seal_actor_died(self, oid_bytes: bytes) -> None:
        # Use the public Python class so the unpickled value carries
        # the same type the user can `except` against.
        from rayd import ActorDiedError  # noqa: PLC0415
        from rayd._worker import _build_error_payload, _encode_error_metadata  # noqa: PLC0415

        err = ActorDiedError("actor subprocess died mid-call")
        err_data = _build_error_payload(err)
        err_meta = _encode_error_metadata()
        _native._worker_seal(oid_bytes, err_meta, err_data)  # noqa: SLF001

    @property
    def pid(self) -> int:
        """OS pid of the actor subprocess (for diagnostics + tests)."""
        return self._actor_pid

    @property
    def restarts_used(self) -> int:
        return self._restarts_used

    def submit(
        self,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object] | None,
    ) -> ObjectRef:
        """Send a method-call frame.

        Returns the `ObjectRef` whose seal the subprocess fires after
        running. Raises `ActorDiedError` if the actor has exhausted
        its restart budget.
        """
        ref = _native._mint_actor_result_ref()  # noqa: SLF001
        self.dispatch_call_with_oid(
            method_name,
            cloudpickle.dumps(args),
            cloudpickle.dumps(kwargs) if kwargs else None,
            ref.object_id.to_bytes(),
        )
        return ref

    def dispatch_call_with_oid(
        self,
        method_name: str,
        args_blob: bytes,
        kwargs_blob: bytes | None,
        oid_bytes: bytes,
    ) -> None:
        """Send an `actor_call` frame using a caller-supplied result oid.

        Used by both `submit` (after minting locally) and the cross-
        driver RPC path (where the calling driver mints the oid against
        the actor-driver's node id and forwards the pickled blobs as-is).

        Raises `RuntimeError` if the actor has been terminated and
        `ActorDiedError` if the actor has died past its restart budget.
        Tracks the oid in `_in_flight` so a subsequent crash seals
        it as an error instead of leaking.
        """
        from rayd import ActorDiedError  # noqa: PLC0415

        # State lock: guard against terminated/dead, snapshot the
        # current socket. We DON'T send under this lock because
        # send() can block on a full socket buffer; that would stall
        # the reader thread (which also takes _state_lock to discard
        # completed oids).
        with self._state_lock:
            if self._terminated:
                msg = "actor has been terminated"
                raise RuntimeError(msg)
            if self._dead:
                msg = "actor has died and exhausted its restart budget"
                raise ActorDiedError(msg)
            self._in_flight.add(oid_bytes)
            sock = self._sock

        frame = self._encode(
            {
                "kind": "actor_call",
                "method": method_name,
                "args": args_blob,
                "kwargs": kwargs_blob,
                "result_oid": oid_bytes,
            }
        )
        # Send lock: serializes outgoing frames so two callers don't
        # interleave bytes on the wire. Separate from the state lock
        # so the reader can keep consuming while we block on send.
        with self._send_lock:
            self._send_frame(sock, frame)

    def terminate(self, timeout: float | None = _TERMINATE_TIMEOUT_S) -> None:
        """Tell the subprocess to drain + exit, then join the reader."""
        with self._state_lock:
            if self._terminated:
                return
            self._terminated = True
            sock = self._sock
            reader = self._reader_thread
            proc = self._proc

        # Send the shutdown frame under the send lock (NOT the state
        # lock) so we don't stall the reader if the subprocess is
        # mid-method.
        with self._send_lock, contextlib.suppress(OSError):
            self._send_frame(sock, self._encode({"kind": "actor_shutdown"}))
        with contextlib.suppress(OSError):
            sock.shutdown(socket.SHUT_WR)

        reader.join(timeout=timeout)
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        with contextlib.suppress(OSError):
            sock.close()
        with contextlib.suppress(OSError):
            self._tempdir.cleanup()
        self._unregister_name_from_gcs()
        _unregister_actor(self._actor_id)

    def _unregister_name_from_gcs(self) -> None:
        """Best-effort: remove our GCS name entry. Idempotent."""
        name = self._registered_name
        if name is None:
            return
        self._registered_name = None
        with contextlib.suppress(Exception):
            _native._unregister_named_actor(name, self._actor_id)  # noqa: SLF001


@final
class _BoundActorMethod:
    """`actor.method_name` returns one of these. `.remote()` queues a call."""

    def __init__(self, runner: _ActorSubprocess, name: str) -> None:
        self._runner = runner
        self._name = name

    def remote(self, *args: object, **kwargs: object) -> ObjectRef:
        return self._runner.submit(self._name, args, kwargs or None)


@final
class _RemoteBoundMethod:
    """`remote_handle.method.remote(...)` — dispatches via the owner's TCP."""

    def __init__(self, handle: _RemoteActorHandle, name: str) -> None:
        self._handle = handle
        self._name = name

    def remote(self, *args: object, **kwargs: object) -> ObjectRef:
        return self._handle._submit(self._name, args, kwargs or None)  # noqa: SLF001


@final
class _RemoteActorHandle:
    """Cross-driver handle. Dials the owner driver's actor-RPC listener.

    Looks the same as `ActorHandle` from a user perspective: attribute
    access yields a method object whose `.remote()` returns an
    `ObjectRef`. The ref is owned by the actor's driver, so
    `rayd.get(ref)` triggers the existing cross-node fetch path.
    """

    _actor_id: bytes
    _owner_node_id: bytes
    _addr: tuple[str, int]

    def __init__(
        self,
        actor_id: bytes,
        owner_node_id: bytes,
        host: str,
        port: int,
    ) -> None:
        if len(actor_id) != _ACTOR_ID_SIZE:
            msg = f"actor_id must be {_ACTOR_ID_SIZE} bytes, got {len(actor_id)}"
            raise ValueError(msg)
        if len(owner_node_id) != _NODE_ID_SIZE:
            msg = f"owner_node_id must be {_NODE_ID_SIZE} bytes, got {len(owner_node_id)}"
            raise ValueError(msg)
        if not host or port == 0:
            msg = (
                "actor's owner driver did not advertise an RPC address "
                "(it likely registered without an actor-RPC listener — "
                "check that `rayd.init` brought the listener up there)"
            )
            raise RuntimeError(msg)
        self._actor_id = actor_id
        self._owner_node_id = owner_node_id
        self._addr = (host, int(port))

    def __getattr__(self, name: str) -> _RemoteBoundMethod:
        if name.startswith("_"):
            raise AttributeError(name)
        return _RemoteBoundMethod(self, name)

    def _submit(
        self,
        method_name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object] | None,
    ) -> ObjectRef:
        # Lazy-import the wire helpers (same idiom `_ActorSubprocess`
        # uses) — keeps the module-import cost down for callers that
        # never invoke a remote actor.
        from rayd._worker import _decode, _encode, _recv_frame, _send_frame  # noqa: PLC0415

        # Mint a ref OWNED by the actor's driver. Caller's `rayd.get`
        # will see owner_node_id != local and hit the cross-node fetch
        # path against the owner-raylet's directory.
        ref = _native._mint_actor_result_ref(self._owner_node_id)  # noqa: SLF001
        oid_bytes = ref.object_id.to_bytes()

        frame = _encode(
            {
                "kind": "actor_invoke",
                "actor_id": self._actor_id,
                "method": method_name,
                "args": cloudpickle.dumps(args),
                "kwargs": cloudpickle.dumps(kwargs) if kwargs else None,
                "result_oid": oid_bytes,
            },
        )
        try:
            with socket.create_connection(
                self._addr,
                timeout=_REMOTE_DIAL_TIMEOUT_S,
            ) as sock:
                # `create_connection`'s timeout only covers the connect
                # step. Apply the same deadline to recv so a stuck
                # owner-driver RPC thread (deadlock, GIL contention,
                # …) surfaces as a timeout instead of a silent hang.
                sock.settimeout(_REMOTE_DIAL_TIMEOUT_S)
                _send_frame(sock, frame)
                reply_frame = _recv_frame(sock)
        except OSError as e:
            # The most useful failure to distinguish here is a dead
            # owner driver. Consult the GCS: if its node is
            # `Dead`/`Draining`/absent, surface `OwnerDiedError` so
            # user code uses the same exception as for remote refs
            # whose owner is gone. Anything else propagates as-is.
            self._raise_owner_died_or_propagate(e)
            raise  # only reached if owner is still alive (transient)
        if reply_frame is None:
            msg = (
                f"actor RPC server at {self._addr[0]}:{self._addr[1]} "
                "closed the connection without acking actor_invoke"
            )
            raise RuntimeError(msg)
        reply = _decode(reply_frame)
        kind = reply.get("kind")
        if kind == "actor_invoke_ack":
            return ref
        if kind == "actor_invoke_reject":
            reason = reply.get("reason")
            # The owner driver tells us the actor isn't there. Most
            # commonly this means it terminated or died past its
            # restart budget — surface ActorDiedError so user code
            # can `except` it the same way as a same-driver crash.
            from rayd import ActorDiedError  # noqa: PLC0415

            if reason == "unknown_actor_id":
                msg = (
                    f"actor {self._actor_id.hex()} is no longer alive on its "
                    "owner driver (terminated or crashed past max_restarts)"
                )
                raise ActorDiedError(msg)
            msg = f"owner driver rejected actor_invoke: {reason!r}"
            raise RuntimeError(msg)
        msg = f"unexpected reply from actor RPC server: kind={kind!r}"
        raise RuntimeError(msg)

    def __reduce__(self) -> tuple[object, tuple[bytes, bytes, str, int]]:
        """Pickle protocol: emit `(rebuild, (actor_id, owner_node_id, host, port))`.

        A remote handle pickles back to the same 4-tuple as
        `ActorHandle` so the receiving side's `_rebuild_actor_handle`
        can route it to either local (if it happens to be the owner
        driver) or remote (anywhere else).
        """
        return (
            _rebuild_actor_handle,
            (self._actor_id, self._owner_node_id, self._addr[0], int(self._addr[1])),
        )

    def _raise_owner_died_or_propagate(self, original: OSError) -> None:
        """Convert a TCP-connect failure into `OwnerDiedError` if appropriate.

        Looks up the owner's GCS status. If dead/draining/absent,
        raise `OwnerDiedError` chained from `original`. Otherwise
        return so the caller propagates the original OSError.
        """
        from rayd import OwnerDiedError  # noqa: PLC0415

        try:
            nodes = _native.list_nodes()
        except RuntimeError:
            # No GCS attached on this driver — leave OSError to
            # surface as-is; the user's setup is already broken.
            return
        owner_status: str | None = None
        for n in nodes:
            if not isinstance(n, _native.NodeInfo):
                continue
            if bytes(n.node_id) == self._owner_node_id:
                owner_status = n.status
                break
        if owner_status == "alive":
            return
        msg = (
            f"actor {self._actor_id.hex()}'s owner driver is "
            f"{owner_status or 'absent from GCS'}; cannot dispatch"
        )
        raise OwnerDiedError(msg) from original


@final
class ActorHandle:
    """Public facade. Attribute access yields `_BoundActorMethod`s."""

    def __init__(self, runner: _ActorSubprocess) -> None:
        # Avoid going through __getattr__ for our own internal name.
        object.__setattr__(self, "_runner", runner)

    def __getattr__(self, name: str) -> _BoundActorMethod:
        if name.startswith("_") or name in {"terminate", "pid", "restarts_used"}:
            raise AttributeError(name)
        runner = object.__getattribute__(self, "_runner")
        return _BoundActorMethod(runner, name)

    @property
    def pid(self) -> int:
        """OS pid of the actor's worker subprocess (live one if restarted)."""
        runner: _ActorSubprocess = object.__getattribute__(self, "_runner")
        return runner.pid

    @property
    def restarts_used(self) -> int:
        """How many times this actor's subprocess has been restarted."""
        runner: _ActorSubprocess = object.__getattribute__(self, "_runner")
        return runner.restarts_used

    def terminate(self, timeout: float | None = _TERMINATE_TIMEOUT_S) -> None:
        """Drain the actor's queue, ask the subprocess to exit, join."""
        runner: _ActorSubprocess = object.__getattribute__(self, "_runner")
        runner.terminate(timeout=timeout)

    def __reduce__(self) -> tuple[object, tuple[bytes, bytes, str, int]]:
        """Pickle protocol: emit `(rebuild, (actor_id, owner_node_id, host, port))`.

        Within the same driver process, unpickling looks up the live
        `_ActorSubprocess` in the registry and returns a fresh
        `ActorHandle` wrapping it. Both handles share state — calling
        `terminate` on one stops the underlying subprocess for both.

        Across drivers (e.g. shipped via `rayd.put`/`rayd.get` or
        cloudpickled into a peer's RPC frame), unpickling rehydrates
        as a `_RemoteActorHandle` that dials our embedded RPC address.
        Empty `host`/zero `port` means we have no listener (no GCS
        attached) — peers that try to reconstruct will get
        `LookupError` from the same-driver fallback path.
        """
        runner: _ActorSubprocess = object.__getattribute__(self, "_runner")
        actor_id: bytes = runner._actor_id  # noqa: SLF001
        from rayd._actor_rpc import _driver_actor_rpc_address  # noqa: PLC0415

        addr = _driver_actor_rpc_address()
        host, port = addr if addr is not None else ("", 0)
        local_nid = _native.node_id()
        owner_nid = bytes(local_nid) if local_nid is not None else b""
        return (_rebuild_actor_handle, (actor_id, owner_nid, host, port))


@final
class ActorClass[T]:
    """Returned by `@rayd.actor` on a class. `.remote()` instantiates."""

    def __init__(
        self,
        cls: type[T],
        *,
        max_restarts: int = 3,
        name: str | None = None,
    ) -> None:
        self._cls = cls
        self._max_restarts = max_restarts
        self._name = name

    @property
    def __wrapped__(self) -> type[T]:
        return self._cls

    def options(
        self,
        *,
        max_restarts: int | None = None,
        name: str | None = None,
    ) -> ActorClass[T]:
        """Return a copy with overridden options.

        Pass `name="my-actor"` to register the actor under a unique
        cluster-wide name on `.remote()`. Other drivers can then look
        it up via `rayd.get_actor("my-actor")`.

        Re-registering an already-taken name raises `RuntimeError`. The
        name is freed when the actor is terminated (or its subprocess
        crashes past `max_restarts`).
        """
        return ActorClass(
            self._cls,
            max_restarts=self._max_restarts if max_restarts is None else max_restarts,
            name=self._name if name is None else name,
        )

    def remote(self, *args: object, **kwargs: object) -> ActorHandle:
        return ActorHandle(
            _ActorSubprocess(
                self._cls,
                args,
                kwargs or None,
                self._max_restarts,
                name=self._name,
            ),
        )


def remote_class[T](cls: type[T]) -> ActorClass[T]:
    """`rayd.actor(cls)` — surfaced through the `@rayd.actor` decorator."""
    return ActorClass(cls)


def get_actor(name: str) -> ActorHandle | _RemoteActorHandle:
    """Look up a named actor in the GCS and return a handle for it.

    The actor must have been created with
    `MyActor.options(name=...).remote(...)` somewhere in the cluster.

    Returns an `ActorHandle` when the actor is owned by this driver
    (the same handle a fresh `.remote()` would have produced), or a
    `_RemoteActorHandle` when the owner is a different driver. Both
    expose the same `handle.method.remote(...)` surface; the remote
    variant dispatches via the owner driver's actor-RPC listener.

    Raises `ValueError` if no actor with `name` exists; `RuntimeError`
    if the owner is remote but didn't advertise an RPC address (e.g.
    a unit-test driver that skipped starting the listener).
    """
    info = _native._lookup_named_actor(name)  # noqa: SLF001
    if info is None:
        msg = f"no actor named {name!r}"
        raise ValueError(msg)
    actor_id = bytes(info.actor_id)
    runner = _ACTOR_REGISTRY.get(actor_id)
    if runner is not None:
        return ActorHandle(runner)
    return _RemoteActorHandle(
        actor_id=actor_id,
        owner_node_id=bytes(info.owner_node_id),
        host=info.driver_actor_host,
        port=info.driver_actor_port,
    )


__all__ = [
    "ActorClass",
    "ActorHandle",
    "get_actor",
    "remote_class",
]
