"""Phase 5.4c: per-driver TCP server for cross-driver actor invocation.

Each driver opens a localhost TCP listener on `rayd.init` (when a GCS
is attached). Other drivers dial it to send `actor_invoke` frames
addressed by 16-byte `actor_id`. The server looks the actor up in the
driver-local registry, asks the actor's `_ActorSubprocess` to run the
method with the caller-supplied `result_oid`, and replies with a small
acknowledgment. The result bytes are sealed into the actor-driver's
plasma; the calling driver's `rayd.get` pulls them via the existing
cross-node fetch path.

Wire (driver → driver, length-framed via `_worker._send_frame`):

  client → server:
    `actor_invoke` { actor_id, method, args, kwargs?, result_oid }

  server → client:
    `actor_invoke_ack`    {}                         — call queued
    `actor_invoke_reject` { reason }                 — terminal failure

Architectural note (Phase 5.4f decision): this listener is intentional
duplication relative to the raylet's gRPC `ObjectTransport`. We
considered migrating actor invocation onto an `InvokeActor` RPC on
the raylet so each node would carry one transport, but rejected it:

1. `_ActorSubprocess.dispatch_call_with_oid` lives in Python — it
   owns the cloudpickle dump, the `_in_flight` set, and the per-actor
   UDS to the worker subprocess. A Rust gRPC handler would have to
   marshal back into Python anyway, just via a callback channel
   instead of a socket. The dispatch surface doesn't actually move.
2. Actor results MUST be observed by the owner driver's reader
   thread, because that's where `_record_plasma_seal` registers the
   seal at the owner's local raylet directory (which is what makes
   subsequent cross-node `Pull` succeed). Bypassing the owner driver
   would break the directory invariant.
3. The migration would add a Rust↔Python callback channel for ~no
   observable benefit; the architectural duplication shifts location
   instead of disappearing.

Revisit if/when actor management itself moves into Rust (a much larger
project — cloudpickle is Python-only, so this is unlikely to happen).
"""

from __future__ import annotations

import contextlib
import socket
import sys
import threading
from typing import TYPE_CHECKING, final

if TYPE_CHECKING:
    from collections.abc import Callable


_DRIVER_RPC_SERVER: _DriverActorRpcServer | None = None
_DRIVER_RPC_SERVER_LOCK = threading.Lock()
_ACCEPT_TIMEOUT_S = 0.5
_JOIN_TIMEOUT_S = 2.0


@final
class _DriverActorRpcServer:
    """One TCP listener per driver, multiplexes for all owned actors."""

    def __init__(self) -> None:
        from rayd._worker import _decode, _encode, _recv_frame, _send_frame  # noqa: PLC0415

        self._encode: Callable[[dict[str, object]], bytes] = _encode
        self._decode: Callable[[bytes], dict[str, object]] = _decode
        self._recv_frame: Callable[[socket.socket], bytes | None] = _recv_frame
        self._send_frame: Callable[[socket.socket, bytes], None] = _send_frame
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._workers_lock = threading.Lock()
        self._addr: tuple[str, int] | None = None

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", 0))
        sock.listen(64)
        # Short timeout so the accept loop can poll the stop flag without
        # waiting on a final blocking accept.
        sock.settimeout(_ACCEPT_TIMEOUT_S)
        host, port = sock.getsockname()[:2]
        self._listener = sock
        self._addr = (str(host), int(port))
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name="rayd-actor-rpc-accept",
            daemon=True,
        )
        self._accept_thread.start()

    @property
    def address(self) -> tuple[str, int]:
        if self._addr is None:
            msg = "_DriverActorRpcServer.start() must be called before .address"
            raise RuntimeError(msg)
        return self._addr

    def stop(self) -> None:
        self._stop.set()
        sock = self._listener
        self._listener = None
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.close()
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=_JOIN_TIMEOUT_S)
        with self._workers_lock:
            workers = list(self._workers)
            self._workers.clear()
        for w in workers:
            w.join(timeout=_JOIN_TIMEOUT_S)

    def _accept_loop(self) -> None:
        sock = self._listener
        if sock is None:
            return
        while not self._stop.is_set():
            try:
                conn, _addr = sock.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            t = threading.Thread(
                target=self._serve_connection,
                args=(conn,),
                name="rayd-actor-rpc-conn",
                daemon=True,
            )
            with self._workers_lock:
                # Drop joined workers so the list doesn't grow forever.
                self._workers = [w for w in self._workers if w.is_alive()]
                self._workers.append(t)
            t.start()

    def _serve_connection(self, conn: socket.socket) -> None:
        try:
            while not self._stop.is_set():
                try:
                    frame = self._recv_frame(conn)
                except (ConnectionError, OSError):
                    return
                if frame is None:
                    return
                msg = self._decode(frame)
                kind = msg.get("kind")
                if kind == "actor_invoke":
                    self._handle_invoke(conn, msg)
                else:
                    sys.stderr.write(
                        f"rayd._actor_rpc: unknown frame kind {kind!r}\n",
                    )
                    return
        finally:
            with contextlib.suppress(OSError):
                conn.close()

    def _handle_invoke(self, conn: socket.socket, msg: dict[str, object]) -> None:
        from rayd._actor import _lookup_actor_optional  # noqa: PLC0415

        actor_id = msg.get("actor_id")
        method = msg.get("method")
        args_blob = msg.get("args")
        kwargs_blob = msg.get("kwargs")
        result_oid = msg.get("result_oid")
        if (
            not isinstance(actor_id, bytes)
            or not isinstance(method, str)
            or not isinstance(args_blob, bytes)
            or not isinstance(result_oid, bytes)
            or (kwargs_blob is not None and not isinstance(kwargs_blob, bytes))
        ):
            self._reply(conn, {"kind": "actor_invoke_reject", "reason": "malformed"})
            return

        runner = _lookup_actor_optional(actor_id)
        if runner is None:
            self._reply(
                conn,
                {"kind": "actor_invoke_reject", "reason": "unknown_actor_id"},
            )
            return

        try:
            runner.dispatch_call_with_oid(method, args_blob, kwargs_blob, result_oid)
        except Exception as exc:  # noqa: BLE001
            self._reply(
                conn,
                {"kind": "actor_invoke_reject", "reason": str(exc)},
            )
            return
        self._reply(conn, {"kind": "actor_invoke_ack"})

    def _reply(self, conn: socket.socket, message: dict[str, object]) -> None:
        with contextlib.suppress(OSError):
            self._send_frame(conn, self._encode(message))


def _ensure_rpc_server() -> _DriverActorRpcServer:
    """Start the singleton on first use; idempotent."""
    global _DRIVER_RPC_SERVER  # noqa: PLW0603
    with _DRIVER_RPC_SERVER_LOCK:
        if _DRIVER_RPC_SERVER is None:
            server = _DriverActorRpcServer()
            server.start()
            _DRIVER_RPC_SERVER = server
        return _DRIVER_RPC_SERVER


def _shutdown_rpc_server() -> None:
    """Stop and tear down the singleton; idempotent."""
    global _DRIVER_RPC_SERVER  # noqa: PLW0603
    with _DRIVER_RPC_SERVER_LOCK:
        server = _DRIVER_RPC_SERVER
        _DRIVER_RPC_SERVER = None
    if server is not None:
        server.stop()


def _driver_actor_rpc_address() -> tuple[str, int] | None:
    """Return the server's bound address, or `None` when not started."""
    with _DRIVER_RPC_SERVER_LOCK:
        server = _DRIVER_RPC_SERVER
    if server is None:
        return None
    return server.address


__all__ = [
    "_driver_actor_rpc_address",
    "_ensure_rpc_server",
    "_shutdown_rpc_server",
]
