"""Phase 5.4c tests: per-driver actor-RPC TCP server.

Exercises the TCP listener `rayd.init` brings up when a GCS is attached.
For 5.4c the calling driver dials this listener with `actor_invoke`
frames; for these tests we do the round-trip locally (no second driver)
to lock in the wire format and the dispatch path.
"""

from __future__ import annotations

import contextlib
import re
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import cloudpickle  # type: ignore[import-untyped]
import pytest

import rayd
from rayd import _native
from rayd._actor_rpc import _driver_actor_rpc_address
from rayd._worker import _decode, _encode, _recv_frame, _send_frame

if TYPE_CHECKING:
    from collections.abc import Generator


_BIND_TIMEOUT_S = 5.0


def _rayd_cli() -> Path:
    target = Path(__file__).resolve().parents[3] / "target" / "debug" / "rayd"
    if not target.exists():
        pytest.skip(f"rayd-cli binary not built at {target}; run `cargo build -p rayd-cli`")
    return target


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.contextmanager
def _spawn_gcs() -> Generator[str]:
    cli = _rayd_cli()
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    with subprocess.Popen(  # noqa: S603
        [str(cli), "gcs", "--bind", addr],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            deadline = time.monotonic() + _BIND_TIMEOUT_S
            bound = False
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    msg = f"rayd gcs exited prematurely (rc={proc.returncode})"
                    raise RuntimeError(msg)
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.05):
                        bound = True
                        break
                except OSError:
                    time.sleep(0.02)
            if not bound:
                msg = f"rayd gcs failed to bind {addr} within {_BIND_TIMEOUT_S}s"
                raise RuntimeError(msg)
            yield addr
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


@pytest.fixture
def gcs_server() -> Generator[str]:
    with _spawn_gcs() as addr:
        yield addr


@pytest.fixture
def _runtime_with_gcs(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None]:
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    rayd.init()
    try:
        yield
    finally:
        rayd.shutdown()


class _CounterImpl:
    def __init__(self, start: int = 0) -> None:
        self.x = start

    def increment(self) -> int:
        self.x += 1
        return self.x

    def add(self, delta: int) -> int:
        self.x += delta
        return self.x


_Counter = rayd.actor(_CounterImpl)


# ── lifecycle ──────────────────────────────────────────────────────────


def test_no_rpc_server_without_gcs() -> None:
    """No GCS → no peer can find us → no listener needed."""
    rayd.init()
    try:
        assert _driver_actor_rpc_address() is None
    finally:
        rayd.shutdown()


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_rpc_server_address_is_loopback_with_nonzero_port() -> None:
    addr = _driver_actor_rpc_address()
    assert addr is not None
    host, port = addr
    assert host == "127.0.0.1"
    assert 1024 <= port <= 65535


def _try_dial(addr: tuple[str, int]) -> None:
    with socket.create_connection(addr, timeout=0.5) as s:
        s.sendall(b"\x00" * 4)
        s.recv(1)


def test_rpc_server_stops_on_shutdown(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`rayd.shutdown()` closes the listener; the address goes away."""
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    rayd.init()
    addr = _driver_actor_rpc_address()
    assert addr is not None
    rayd.shutdown()
    assert _driver_actor_rpc_address() is None
    # Port should also be no longer bound: the connection either fails
    # to dial or the kernel returns immediately on read.
    with pytest.raises(OSError, match=re.compile(r"refused|reset|broken", re.IGNORECASE)):
        _try_dial(addr)


# ── actor_invoke round-trip ────────────────────────────────────────────


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_actor_invoke_dispatches_to_local_actor() -> None:
    """An `actor_invoke` frame routes to the actor and seals the result."""
    handle = _Counter.options(name="rpc-counter").remote(0)
    try:
        runner = object.__getattribute__(handle, "_runner")
        actor_id = runner._actor_id  # noqa: SLF001

        addr = _driver_actor_rpc_address()
        assert addr is not None

        # Caller mints a result oid the same way `submit` does. No
        # owner_node_id stamp needed for this same-driver test.
        ref = _native._mint_actor_result_ref()  # noqa: SLF001
        result_oid = ref.object_id.to_bytes()
        payload = _encode(
            {
                "kind": "actor_invoke",
                "actor_id": actor_id,
                "method": "increment",
                "args": cloudpickle.dumps(()),
                "kwargs": None,
                "result_oid": result_oid,
            },
        )
        with socket.create_connection(addr, timeout=2.0) as sock:
            _send_frame(sock, payload)
            reply_frame = _recv_frame(sock)
        assert reply_frame is not None
        reply = _decode(reply_frame)
        assert reply["kind"] == "actor_invoke_ack"

        # The actor's reader-thread observes `actor_call_complete` and
        # records the seal in plasma. Block on the ref.
        assert rayd.get(ref, timeout=5.0) == 1
    finally:
        handle.terminate()


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_actor_invoke_rejects_unknown_actor_id() -> None:
    addr = _driver_actor_rpc_address()
    assert addr is not None
    fake_actor_id = b"\x00" * 16
    fake_oid = b"\x00" * 28
    payload = _encode(
        {
            "kind": "actor_invoke",
            "actor_id": fake_actor_id,
            "method": "increment",
            "args": cloudpickle.dumps(()),
            "kwargs": None,
            "result_oid": fake_oid,
        },
    )
    with socket.create_connection(addr, timeout=2.0) as sock:
        _send_frame(sock, payload)
        reply_frame = _recv_frame(sock)
    assert reply_frame is not None
    reply = _decode(reply_frame)
    assert reply["kind"] == "actor_invoke_reject"
    assert reply["reason"] == "unknown_actor_id"


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_actor_invoke_rejects_malformed_frame() -> None:
    addr = _driver_actor_rpc_address()
    assert addr is not None
    # Missing `actor_id` field → server replies with reject "malformed".
    payload = _encode(
        {
            "kind": "actor_invoke",
            "method": "increment",
            "args": cloudpickle.dumps(()),
            "kwargs": None,
            "result_oid": b"\x00" * 28,
        },
    )
    with socket.create_connection(addr, timeout=2.0) as sock:
        _send_frame(sock, payload)
        reply_frame = _recv_frame(sock)
    assert reply_frame is not None
    reply = _decode(reply_frame)
    assert reply["kind"] == "actor_invoke_reject"
    assert reply["reason"] == "malformed"


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_actor_invoke_passes_args_and_kwargs() -> None:
    handle = _Counter.options(name="rpc-args-counter").remote(100)
    try:
        runner = object.__getattribute__(handle, "_runner")
        actor_id = runner._actor_id  # noqa: SLF001

        addr = _driver_actor_rpc_address()
        assert addr is not None

        ref = _native._mint_actor_result_ref()  # noqa: SLF001
        payload = _encode(
            {
                "kind": "actor_invoke",
                "actor_id": actor_id,
                "method": "add",
                "args": cloudpickle.dumps(()),
                "kwargs": cloudpickle.dumps({"delta": 7}),
                "result_oid": ref.object_id.to_bytes(),
            },
        )
        with socket.create_connection(addr, timeout=2.0) as sock:
            _send_frame(sock, payload)
            assert _recv_frame(sock) is not None  # ack
        assert rayd.get(ref, timeout=5.0) == 107
    finally:
        handle.terminate()
