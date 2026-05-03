"""Regression tests for the rayd.shutdown() plasma leak.

The bug: `runtime.rs::uninstall` used to drop the `CoreWorker` Arc
without first walking the `RefCounter` to free each tracked plasma
object. Any Python `ObjectRef` still alive at shutdown time (e.g. in
the caller's local frame after a Ctrl-C) had its underlying plasma
copy stranded — visible to a Prometheus scrape as a non-zero
`rayd_plasma_objects_total` long after the driver exited. The fix
adds `CoreWorker::free_all_local()` and calls it from `uninstall`
before the dispatcher / GCS / worker teardown.

These tests spawn a real `rayd plasma-server` with metrics enabled,
seal objects through a driver, then assert that `rayd.shutdown()`
takes the plasma server's object count back to zero — even with refs
still in scope.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd

if TYPE_CHECKING:
    from collections.abc import Generator


_BIND_TIMEOUT_S = 5.0
_INLINE_THRESHOLD = 100 * 1024  # bytes ≥ this go to plasma, not the inline store


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
def _spawn_plasma(socket_path: Path, metrics_addr: str) -> Generator[None]:
    """Run `rayd plasma-server` with a metrics endpoint until exit."""
    cli = _rayd_cli()
    with subprocess.Popen(  # noqa: S603
        [
            str(cli),
            "plasma-server",
            str(socket_path),
            "--capacity-mb",
            "64",
            "--metrics-bind",
            metrics_addr,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    ) as proc:
        try:
            host, port_str = metrics_addr.split(":")
            port = int(port_str)
            deadline = time.monotonic() + _BIND_TIMEOUT_S
            ready = False
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    msg = f"rayd plasma-server exited (rc={proc.returncode})"
                    raise RuntimeError(msg)
                try:
                    with socket.create_connection((host, port), timeout=0.05):
                        ready = True
                        break
                except OSError:
                    time.sleep(0.02)
            if not ready:
                msg = f"plasma metrics endpoint {metrics_addr} not ready in {_BIND_TIMEOUT_S}s"
                raise RuntimeError(msg)
            yield
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def _scrape_objects_total(metrics_addr: str) -> int:
    body = urllib.request.urlopen(f"http://{metrics_addr}/metrics", timeout=2.0).read().decode()
    for line in body.splitlines():
        if line.startswith("rayd_plasma_objects_total"):
            return int(line.split()[1])
    msg = f"rayd_plasma_objects_total not found in scrape:\n{body}"
    raise AssertionError(msg)


def test_shutdown_frees_plasma_even_with_live_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact scenario the user hit: refs alive at shutdown shouldn't leak.

    Mirrors `dashboards/dev/workload.py` after a Ctrl-C: the local
    `refs` list is still in scope when `rayd.shutdown()` runs, so
    `__del__` can't have fired yet. Pre-fix this left N plasma
    objects stranded in the server.
    """
    sock = tmp_path / "plasma.sock"
    metrics_addr = f"127.0.0.1:{_free_port()}"
    monkeypatch.setenv("RAYD_PLASMA_SOCKET", str(sock))

    with _spawn_plasma(sock, metrics_addr):
        rayd.init()
        # 15 plasma-resident objects (each above the inline threshold).
        refs = [
            rayd.put(b"\x00" * (_INLINE_THRESHOLD + 1024 + i)) for i in range(15)
        ]
        # Settle metrics.
        time.sleep(0.1)
        before = _scrape_objects_total(metrics_addr)
        assert before == 15, f"expected 15 plasma objects, got {before}"

        # Shutdown WITHOUT releasing the refs first — this is what
        # caused the leak. `refs` is still bound in this local scope.
        rayd.shutdown()
        # Re-binding `refs` to keep mypy / static-analysis happy that
        # we used the value, AND so a future maintainer doesn't think
        # this line is dead code. The list is intentionally still
        # alive at the shutdown call above.
        assert len(refs) == 15

        # Give the plasma server a beat to process the deletes.
        time.sleep(0.2)
        after = _scrape_objects_total(metrics_addr)
        assert after == 0, f"plasma leaked {after} objects across rayd.shutdown()"


def test_shutdown_then_reinit_starts_with_clean_plasma(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second `rayd.init()` against the same plasma server sees an
    empty arena. Catches the case where shutdown frees memory-store
    entries but leaves plasma copies (which would surface as a hidden
    ramp-up over the lifetime of the plasma server).
    """
    sock = tmp_path / "plasma.sock"
    metrics_addr = f"127.0.0.1:{_free_port()}"
    monkeypatch.setenv("RAYD_PLASMA_SOCKET", str(sock))

    with _spawn_plasma(sock, metrics_addr):
        for cycle in range(3):
            rayd.init()
            _ = [rayd.put(b"\x00" * (_INLINE_THRESHOLD + 1024)) for _ in range(5)]
            time.sleep(0.05)
            mid = _scrape_objects_total(metrics_addr)
            assert mid == 5, f"cycle {cycle}: expected 5 objects mid-run, got {mid}"
            rayd.shutdown()
            time.sleep(0.1)
            end = _scrape_objects_total(metrics_addr)
            assert end == 0, f"cycle {cycle}: leaked {end} objects after shutdown"
