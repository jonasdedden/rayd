"""Phase 5.4b tests: GCS-backed named-actor directory + `rayd.get_actor`.

`MyActor.options(name="x").remote(...)` registers the actor in the GCS;
`rayd.get_actor("x")` looks it up. Cross-driver method calls are 5.4c —
`get_actor` for an actor owned by a different driver raises a clear
RuntimeError pointing at that phase.
"""

from __future__ import annotations

import contextlib
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd
from rayd import _native

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
def _spawn_gcs(*extra_args: str) -> Generator[str]:
    cli = _rayd_cli()
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    with subprocess.Popen(  # noqa: S603
        [str(cli), "gcs", "--bind", addr, *extra_args],
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
def _runtime(
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

    def get(self) -> int:
        return self.x


_Counter = rayd.actor(_CounterImpl)


# ── happy path ─────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_runtime")
def test_named_actor_registered_in_gcs() -> None:
    """`.options(name=…)` registers the actor under that name in the GCS."""
    handle = _Counter.options(name="counter-1").remote(42)
    try:
        infos = _native.list_actors()
        assert len(infos) == 1
        info = infos[0]
        assert isinstance(info, _native.ActorInfo)
        assert info.name == "counter-1"
        assert len(info.actor_id) == 16
        # Owner_node_id is this driver's node id (we connected to a GCS
        # so it's set).
        local_nid = _native.node_id()
        assert local_nid is not None
        assert bytes(info.owner_node_id) == bytes(local_nid)
        # The driver advertised its actor-RPC listener address so peers
        # could dial us for cross-driver invocation.
        assert info.driver_actor_host == "127.0.0.1"
        assert info.driver_actor_port > 0
    finally:
        handle.terminate()


@pytest.mark.usefixtures("_runtime")
def test_get_actor_returns_working_handle() -> None:
    """`rayd.get_actor(name)` returns a handle that can dispatch calls."""
    original = _Counter.options(name="counter-2").remote(0)
    try:
        looked_up = rayd.get_actor("counter-2")
        assert isinstance(looked_up, rayd.ActorHandle)
        # Both handles drive the same subprocess.
        assert looked_up.pid == original.pid
        assert rayd.get(looked_up.increment.remote()) == 1
        assert rayd.get(original.increment.remote()) == 2
        assert rayd.get(looked_up.get.remote()) == 2
    finally:
        original.terminate()


@pytest.mark.usefixtures("_runtime")
def test_terminate_unregisters_name() -> None:
    """After `terminate()`, the GCS no longer lists the name and lookup raises."""
    handle = _Counter.options(name="counter-3").remote(0)
    handle.terminate()
    assert _native.list_actors() == []
    with pytest.raises(ValueError, match="no actor named"):
        rayd.get_actor("counter-3")


@pytest.mark.usefixtures("_runtime")
def test_name_can_be_reused_after_terminate() -> None:
    """A name freed by terminate is available for a new actor."""
    first = _Counter.options(name="reusable").remote(0)
    first.terminate()
    second = _Counter.options(name="reusable").remote(100)
    try:
        assert rayd.get(second.get.remote()) == 100
    finally:
        second.terminate()


# ── failure modes ─────────────────────────────────────────────────────


@pytest.mark.usefixtures("_runtime")
def test_duplicate_name_raises_at_remote() -> None:
    """A second `.options(name=…).remote()` for the same name fails fast."""
    first = _Counter.options(name="duplicate").remote(0)
    try:
        with pytest.raises(RuntimeError, match="already"):
            _Counter.options(name="duplicate").remote(99)
    finally:
        first.terminate()


@pytest.mark.usefixtures("_runtime")
def test_get_actor_unknown_name_raises() -> None:
    with pytest.raises(ValueError, match="no actor named"):
        rayd.get_actor("does-not-exist")


# ── Phase 5.4d: ActorHandle pickling round-trip ────────────────────────


@pytest.mark.usefixtures("_runtime")
def test_actor_handle_round_trips_through_put_get() -> None:
    """`rayd.put(handle)` → `rayd.get(ref)` returns a working `ActorHandle`."""
    import pickle  # noqa: PLC0415

    handle = _Counter.options(name="put-counter").remote(0)
    try:
        # First confirm direct pickle still works (regression for the
        # 5.4a same-driver round-trip behavior).
        twin = pickle.loads(pickle.dumps(handle))  # noqa: S301
        assert isinstance(twin, rayd.ActorHandle)
        assert twin.pid == handle.pid

        # Now via the object store.
        ref = rayd.put(handle)
        rehydrated = rayd.get(ref)
        assert isinstance(rehydrated, rayd.ActorHandle)
        assert rehydrated.pid == handle.pid
        # State observed through `rehydrated` reflects the same actor.
        assert rayd.get(rehydrated.increment.remote()) == 1
    finally:
        handle.terminate()


# ── Phase 5.4d: remote-actor failure modes ─────────────────────────────


def test_remote_actor_crash_mid_call_raises_actor_died(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remote actor that hard-exits mid-call surfaces `ActorDiedError`.

    Owner driver's reader thread seals every in-flight oid as
    `ActorDied` and registers the seal at its raylet directory. The
    caller's `rayd.get` fetches via the cross-node path and re-raises.
    Any `.method.remote()` call after the budget is exhausted is also
    `ActorDiedError` (we map the owner's `unknown_actor_id` reject
    to that exception).
    """
    script = r"""
import os, sys, time
import rayd

class _Bomb:
    def crash(self) -> int:
        os._exit(1)
    def alive(self) -> int:
        return 1

A = rayd.actor(_Bomb, max_restarts=0)
rayd.init()
handle = A.options(name="bomb").remote()
print("READY", flush=True)
try:
    time.sleep(30)
except KeyboardInterrupt:
    pass
finally:
    handle.terminate()
    rayd.shutdown()
"""
    env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_server}
    with subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            assert proc.stdout is not None
            deadline = time.monotonic() + 10.0
            ready = False
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    if proc.poll() is not None:
                        stderr = proc.stderr.read() if proc.stderr else ""
                        msg = f"child exited early (rc={proc.returncode}): {stderr}"
                        raise RuntimeError(msg)
                    continue
                if line.strip() == "READY":
                    ready = True
                    break
            assert ready

            monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
            rayd.init()
            try:
                handle = rayd.get_actor("bomb")
                ref = handle.crash.remote()
                # The actor hard-exits → owner seals ActorDiedError →
                # cross-node fetch retrieves it → rayd.get re-raises.
                with pytest.raises(rayd.ActorDiedError):
                    rayd.get(ref, timeout=10.0)
                # Subsequent calls hit the owner's RPC and the actor
                # is no longer in its registry → reject with
                # `unknown_actor_id` → mapped to ActorDiedError.
                with pytest.raises(rayd.ActorDiedError, match="no longer alive"):
                    handle.alive.remote()
            finally:
                rayd.shutdown()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def test_remote_actor_terminate_then_invoke_raises_actor_died(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A remote-handle call after the actor's clean terminate raises `ActorDiedError`.

    The child creates the actor, signals READY, waits for the parent
    to acknowledge it via a sentinel call, then terminates the actor
    cleanly. The parent's next call hits the owner driver's RPC, gets
    `unknown_actor_id`, and surfaces `ActorDiedError`.
    """
    script = r"""
import sys, time
import rayd

class _Impl:
    def alive(self) -> int:
        return 1

A = rayd.actor(_Impl)
rayd.init()
handle = A.options(name="ephemeral").remote()
print("READY", flush=True)
# Read one byte from stdin as the parent's "go ahead and terminate" signal.
sys.stdin.read(1)
handle.terminate()
print("TERMINATED", flush=True)
try:
    time.sleep(30)
except KeyboardInterrupt:
    pass
finally:
    rayd.shutdown()
"""
    env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_server}
    with subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            assert proc.stdout is not None
            assert proc.stdin is not None
            deadline = time.monotonic() + 10.0
            ready = False
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    if proc.poll() is not None:
                        stderr = proc.stderr.read() if proc.stderr else ""
                        msg = f"child exited early (rc={proc.returncode}): {stderr}"
                        raise RuntimeError(msg)
                    continue
                if line.strip() == "READY":
                    ready = True
                    break
            assert ready

            monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
            rayd.init()
            try:
                handle = rayd.get_actor("ephemeral")
                # First call works while the actor is still alive.
                assert rayd.get(handle.alive.remote(), timeout=10.0) == 1
                # Tell child to terminate and wait for confirmation.
                proc.stdin.write("x")
                proc.stdin.flush()
                proc.stdin.close()
                deadline = time.monotonic() + 5.0
                terminated = False
                while time.monotonic() < deadline:
                    line = proc.stdout.readline()
                    if line.strip() == "TERMINATED":
                        terminated = True
                        break
                assert terminated, "child didn't confirm clean terminate"
                # Next .method.remote() must surface ActorDiedError —
                # the owner driver replies actor_invoke_reject with
                # `unknown_actor_id` and we map it to that exception.
                with pytest.raises(rayd.ActorDiedError, match="no longer alive"):
                    handle.alive.remote()
            finally:
                rayd.shutdown()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def test_actor_handle_pickle_rehydrates_remote_in_other_driver(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pickled handle unpickles as a `_RemoteActorHandle` on a peer.

    Cross-driver via cloudpickle: the child driver creates the actor,
    prints its pickled handle as a base64 blob, and stays alive. The
    parent decodes the blob and invokes a method through the
    rehydrated handle.
    """
    import base64  # noqa: PLC0415

    script = r"""
import base64, pickle, sys, time
import rayd

class _Impl:
    def __init__(self) -> None:
        self.x = 0
    def add(self, delta: int) -> int:
        self.x += delta
        return self.x

A = rayd.actor(_Impl)
rayd.init()
handle = A.options(name="picklable").remote()
blob = pickle.dumps(handle)
print("BLOB:" + base64.b64encode(blob).decode("ascii"), flush=True)
try:
    time.sleep(30)
except KeyboardInterrupt:
    pass
finally:
    handle.terminate()
    rayd.shutdown()
"""
    env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_server}
    with subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            assert proc.stdout is not None
            blob: bytes | None = None
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    if proc.poll() is not None:
                        stderr = proc.stderr.read() if proc.stderr else ""
                        msg = f"child exited early (rc={proc.returncode}): {stderr}"
                        raise RuntimeError(msg)
                    continue
                if line.startswith("BLOB:"):
                    blob = base64.b64decode(line[len("BLOB:") :].strip())
                    break
            assert blob is not None, "child driver didn't emit a pickled handle"

            monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
            rayd.init()
            try:
                import pickle  # noqa: PLC0415

                handle = pickle.loads(blob)  # noqa: S301
                # Cross-driver unpickling lands as the remote variant.
                assert not isinstance(handle, rayd.ActorHandle)
                ref = handle.add.remote(11)
                assert rayd.get(ref, timeout=10.0) == 11
                ref2 = handle.add.remote(31)
                assert rayd.get(ref2, timeout=10.0) == 42
            finally:
                rayd.shutdown()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def test_cross_driver_get_actor_round_trips_via_rpc(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parent driver invokes a method on an actor owned by a child driver.

    The child registers `foreign` and stays alive. The parent connects
    to the same GCS, calls `rayd.get_actor("foreign")` (which returns
    a `_RemoteActorHandle`), invokes a method, and `rayd.get`s the
    result via the cross-node fetch path against the child's raylet.
    """
    script = r"""
import sys, time
import rayd

class _Impl:
    def __init__(self, start: int = 0) -> None:
        self.x = start
    def add(self, delta: int) -> int:
        self.x += delta
        return self.x

A = rayd.actor(_Impl)

rayd.init()
handle = A.options(name="foreign").remote(100)
print("READY", flush=True)
try:
    # Stay alive while the parent test invokes us.
    time.sleep(30)
except KeyboardInterrupt:
    pass
finally:
    handle.terminate()
    rayd.shutdown()
"""
    env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_server}
    with subprocess.Popen(  # noqa: S603
        [sys.executable, "-c", script],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            assert proc.stdout is not None
            deadline = time.monotonic() + 10.0
            ready = False
            while time.monotonic() < deadline:
                line = proc.stdout.readline()
                if not line:
                    if proc.poll() is not None:
                        stderr = proc.stderr.read() if proc.stderr else ""
                        msg = f"child exited early (rc={proc.returncode}): {stderr}"
                        raise RuntimeError(msg)
                    continue
                if line.strip() == "READY":
                    ready = True
                    break
            assert ready, "child driver didn't reach READY"

            monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
            rayd.init()
            try:
                handle = rayd.get_actor("foreign")
                # Cross-driver lookup returns the remote handle variant.
                assert not isinstance(handle, rayd.ActorHandle)
                ref = handle.add.remote(7)
                # rayd.get triggers cross-node fetch from the child's plasma.
                assert rayd.get(ref, timeout=10.0) == 107
                # And again to confirm the actor maintained state across calls.
                ref2 = handle.add.remote(3)
                assert rayd.get(ref2, timeout=10.0) == 110
            finally:
                rayd.shutdown()
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


# ── Phase 5.4e: owner-driver-died maps to OwnerDiedError ───────────────


def test_remote_actor_invoke_after_owner_killed_raises_owner_died(  # noqa: PLR0915
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A killed-owner remote handle surfaces `OwnerDiedError`.

    Uses a short GCS heartbeat timeout so the sweeper marks the dead
    node within seconds. The parent SIGKILLs the child, waits for the
    GCS to flip the node to `Dead`, then dispatches a method — the
    TCP connect fails, we consult the GCS, and re-raise as
    `OwnerDiedError` (rather than a raw `ConnectionRefusedError`).
    """
    with _spawn_gcs("--heartbeat-timeout-ms", "1000") as gcs_addr:
        script = r"""
import sys, time
import rayd

class _Impl:
    def alive(self) -> int:
        return 1

A = rayd.actor(_Impl)
rayd.init()
handle = A.options(name="orphan").remote()
print("READY", flush=True)
try:
    time.sleep(60)
except KeyboardInterrupt:
    pass
finally:
    handle.terminate()
    rayd.shutdown()
"""
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", script],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as proc:
            try:
                assert proc.stdout is not None
                deadline = time.monotonic() + 10.0
                ready = False
                while time.monotonic() < deadline:
                    line = proc.stdout.readline()
                    if not line:
                        if proc.poll() is not None:
                            stderr = proc.stderr.read() if proc.stderr else ""
                            msg = f"child exited (rc={proc.returncode}): {stderr}"
                            raise RuntimeError(msg)
                        continue
                    if line.strip() == "READY":
                        ready = True
                        break
                assert ready

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    handle = rayd.get_actor("orphan")
                    # We're testing the cross-driver path; assert that.
                    assert not isinstance(handle, rayd.ActorHandle)
                    # Confirm the cross-driver path is healthy first.
                    assert rayd.get(handle.alive.remote(), timeout=10.0) == 1

                    # SIGKILL the child driver — no clean shutdown,
                    # the GCS will only learn it's dead via the
                    # heartbeat timeout.
                    proc.kill()
                    proc.wait(timeout=5.0)

                    # Wait for the GCS sweeper to mark the node dead.
                    # Heartbeat timeout = 1s; sweep interval = 1s.
                    # Allow up to 5s wall-clock for the flip to land.
                    owner_nid = bytes(handle._owner_node_id)  # noqa: SLF001
                    deadline = time.monotonic() + 5.0
                    flipped = False
                    while time.monotonic() < deadline:
                        for n in _native.list_nodes():
                            if (
                                isinstance(n, _native.NodeInfo)
                                and bytes(n.node_id) == owner_nid
                                and n.status != "alive"
                            ):
                                flipped = True
                                break
                        if flipped:
                            break
                        time.sleep(0.1)
                    assert flipped, "GCS never marked the dead owner node"

                    with pytest.raises(rayd.OwnerDiedError, match="owner driver"):
                        handle.alive.remote()
                finally:
                    rayd.shutdown()
            finally:
                with contextlib.suppress(ProcessLookupError):
                    proc.terminate()
                try:
                    proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
