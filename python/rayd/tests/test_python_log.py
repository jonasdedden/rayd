"""Phase 7.3 tests: Rust `tracing` events forwarded into Python `logging`.

When `RAYD_LOG_FORWARD=1` is set before `rayd.init()`, Rust events
emitted via `tracing::info!`/`warn!`/etc surface as records on
`logging.getLogger("rayd")`. Users hook a handler onto that logger
to capture rayd's diagnostics in their existing pipeline.

Test strategy: install a `logging.handlers.MemoryHandler`-style
in-memory handler, drive `rayd.init()` with the env var on, and
assert at least one record arrived. We use a real GCS so events
actually fire (the registration log lines are reliable).
"""

from __future__ import annotations

import contextlib
import logging
import socket
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd

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


class _CapturingHandler(logging.Handler):
    """Stash records in a list so the test can assert on them."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@contextlib.contextmanager
def _attach_capturing_handler() -> Generator[_CapturingHandler]:
    """Attach a capturing handler to the `rayd` logger for the test's lifetime."""
    handler = _CapturingHandler()
    logger = logging.getLogger("rayd")
    prev_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)


def test_log_forward_default_off_does_not_emit_to_python_logging(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the env var, no records should land in the Python logger."""
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    monkeypatch.delenv("RAYD_LOG_FORWARD", raising=False)
    with _attach_capturing_handler() as handler:
        rayd.init()
        try:
            # Trigger something rayd-side that would produce a log line.
            _ = rayd.put(b"warm-up")
        finally:
            rayd.shutdown()
    assert handler.records == [], (
        "with RAYD_LOG_FORWARD unset, the Python logger must not receive any rayd records"
    )


def test_log_forward_enabled_routes_rust_events_to_python_logging(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With the env var, the Python logger captures Rust events."""
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    monkeypatch.setenv("RAYD_LOG_FORWARD", "1")
    # Force at-least-info level so reg events (which are info) surface.
    monkeypatch.setenv("RAYD_LOG", "rayd=info")
    with _attach_capturing_handler() as handler:
        rayd.init()
        try:
            _ = rayd.put(b"warm-up")
        finally:
            rayd.shutdown()
    # The driver registration path emits `info!` events via
    # `rayd-py::registered with GCS` and similar. With a GCS attached
    # and forwarding on, we expect at least one record.
    assert handler.records, "expected at least one Rust event forwarded to Python logging"
    # Records should have the right name and level.
    assert all(r.name == "rayd" for r in handler.records)
    levels_seen = {r.levelno for r in handler.records}
    assert any(lvl >= logging.INFO for lvl in levels_seen), (
        f"expected at least one INFO+ record, got levels {sorted(levels_seen)}"
    )
