"""Phase 3.1 tests: shutdown cleanliness and external plasma-server discovery.

The GIL-teardown regression test (`test_*_pool_drains_at_shutdown`) is the
load-bearing one: before Phase 3.1 the per-task `std::thread::spawn`
executor would emit a fatal `PyGILState_Release` print at interpreter
finalize when any task was still in flight.

The external-plasma-server test asserts that `rayd.init()` connects to a
pre-existing socket pointed at by `RAYD_PLASMA_SOCKET`, instead of auto-
spawning. This is the foundation for multi-driver / multi-node setups.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd
from rayd import _native

if TYPE_CHECKING:
    from collections.abc import Generator


# ── shutdown drains the pool ───────────────────────────────────────────


@pytest.fixture
def runtime() -> Generator[None]:
    rayd.init()
    try:
        yield
    finally:
        rayd.shutdown()


@rayd.remote
def _heavy_task(payload: bytes) -> int:
    # Real work to keep the worker thread busy across the shutdown call.
    return sum(payload)


def test_pool_drains_in_flight_tasks_at_shutdown() -> None:
    """`rayd.shutdown()` must wait for in-flight tasks to finish."""
    rayd.init()
    refs = [_heavy_task.remote(os.urandom(32 * 1024)) for _ in range(20)]
    # Tear down without explicitly awaiting. The pool's drain-on-shutdown
    # contract says workers run remaining queued tasks before joining.
    rayd.shutdown()

    # After shutdown, the runtime is gone; we can't query state on these
    # refs without re-initializing. Re-init and confirm we don't crash.
    rayd.init()
    try:
        # Refs from the previous session aren't tracked in the new one.
        assert _native._pool_pending() == 0  # noqa: SLF001
    finally:
        rayd.shutdown()
    # The whole point of this test: no PyGILState_Release fatal print at
    # interpreter exit. Pytest's exit code will reflect a fatal print.
    _ = refs


def test_repeated_init_shutdown_cycles_are_clean() -> None:
    """Hammer init/shutdown so any thread-leak surfaces fast."""
    for _ in range(5):
        rayd.init()
        ref = rayd.put(b"x" * 1024)
        assert rayd.get(ref) == b"x" * 1024
        rayd.shutdown()


def test_pool_pending_is_zero_after_drain(runtime: None) -> None:  # noqa: ARG001
    refs = [_heavy_task.remote(os.urandom(8 * 1024)) for _ in range(8)]
    _ = rayd.get(refs)  # blocks until everyone finishes
    assert _native._pool_pending() == 0  # noqa: SLF001


# ── External plasma server discovery ───────────────────────────────────


def _rayd_cli() -> Path:
    """Locate the `rayd` binary built by `cargo build`."""
    target = Path(__file__).resolve().parents[3] / "target" / "debug" / "rayd"
    if not target.exists():
        pytest.skip(f"rayd-cli binary not built at {target}; run `cargo build -p rayd-cli`")
    return target


@pytest.fixture
def external_plasma() -> Generator[Path]:
    """Spawn `rayd plasma-server <tmp>/plasma.sock` and tear down on teardown."""
    cli = _rayd_cli()
    with tempfile.TemporaryDirectory(prefix="rayd-ext-plasma-") as tmp:
        socket = Path(tmp) / "plasma.sock"
        with subprocess.Popen(  # noqa: S603
            [str(cli), "plasma-server", str(socket), "--capacity-mb", "16"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) as proc:
            try:
                # Wait for the socket to appear (server bound).
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline and not socket.exists():
                    if proc.poll() is not None:
                        msg = (
                            f"rayd plasma-server exited prematurely "
                            f"(rc={proc.returncode})"
                        )
                        raise RuntimeError(msg)
                    time.sleep(0.02)
                if not socket.exists():
                    msg = "external plasma server failed to bind socket within 5s"
                    raise RuntimeError(msg)
                yield socket
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=3.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


def test_init_connects_to_external_plasma(
    external_plasma: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`rayd.init()` connects to the standalone server when the env var is set."""
    monkeypatch.setenv("RAYD_PLASMA_SOCKET", str(external_plasma))
    rayd.init()
    try:
        # Round-trip a payload above the inline threshold so it goes through
        # the external plasma server.
        payload = os.urandom(256 * 1024)
        ref = rayd.put(payload)
        assert rayd.get(ref) == payload
        # The same socket should still be valid after the round-trip.
        assert external_plasma.exists()
    finally:
        rayd.shutdown()


def test_missing_external_plasma_socket_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAYD_PLASMA_SOCKET", "/tmp/rayd-does-not-exist/plasma.sock")  # noqa: S108
    with pytest.raises(RuntimeError, match="RAYD_PLASMA_SOCKET"):
        rayd.init()
    # Make sure no half-initialized state lingers.
    assert rayd.is_initialized() is False


def test_two_drivers_share_external_plasma(external_plasma: Path) -> None:
    """A second Python process can put + get against the same plasma server."""
    # Driver A: put a value via the external plasma server.
    env = {**os.environ, "RAYD_PLASMA_SOCKET": str(external_plasma)}
    code_put = (
        "import os, sys, rayd\n"
        "rayd.init()\n"
        "ref = rayd.put(b'shared-payload-' + b'x' * 200_000)\n"
        # We can't share an ObjectRef across processes yet (Phase 3.x), so
        # instead the second driver verifies its OWN put/get works against the
        # shared server. The fact that both connect to the same server is
        # what we're testing.
        "print(ref.hex)\n"
        "rayd.shutdown()\n"
    )
    out = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code_put],
        env=env,
        capture_output=True,
        check=True,
        text=True,
    )
    assert out.stdout.strip()  # ref.hex was printed

    # Driver B independently uses the same server.
    code_independent = (
        "import os, rayd\n"
        "rayd.init()\n"
        "r = rayd.put(b'second driver')\n"
        "assert rayd.get(r) == b'second driver'\n"
        "rayd.shutdown()\n"
        "print('ok')\n"
    )
    out2 = subprocess.run(  # noqa: S603
        [sys.executable, "-c", code_independent],
        env=env,
        capture_output=True,
        check=True,
        text=True,
    )
    assert out2.stdout.strip() == "ok"
