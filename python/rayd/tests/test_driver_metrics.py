"""Phase 7.4d tests: driver-side `/metrics` endpoint.

When `RAYD_METRICS_BIND=host:port` is set before `rayd.init()`, the
Python driver hosts a Prometheus text-format `/metrics` endpoint.
The bumpers in `lib.rs` (put/get/submit) and `dispatcher.rs`
(completion frames) increment counters in a process-global slot.

Test strategy: bind to a free port, drive a few rayd operations,
then HTTP GET `/metrics` and assert the counters reflect the work.
The `rayd_driver_refs_alive` gauge is collected at scrape time
from the live `RefCounter`.
"""

from __future__ import annotations

import socket
import urllib.request
from typing import TYPE_CHECKING
from urllib.error import URLError

import pytest

import rayd

if TYPE_CHECKING:
    from collections.abc import Generator


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def metrics_addr(monkeypatch: pytest.MonkeyPatch) -> Generator[str]:
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    monkeypatch.setenv("RAYD_METRICS_BIND", addr)
    rayd.init()
    try:
        yield addr
    finally:
        rayd.shutdown()


def _scrape(addr: str) -> str:
    with urllib.request.urlopen(f"http://{addr}/metrics", timeout=5.0) as resp:
        assert resp.status == 200
        body: bytes = resp.read()
        return body.decode("utf-8")


def _counter_value(body: str, name: str) -> float:
    """Parse a single-value (no labels) counter or gauge from prom text."""
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        metric, _, val = line.partition(" ")
        if metric == name:
            return float(val)
    msg = f"metric {name!r} not found in scrape:\n{body}"
    raise AssertionError(msg)


def test_metrics_endpoint_unset_disables_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without `RAYD_METRICS_BIND`, the server is not started."""
    monkeypatch.delenv("RAYD_METRICS_BIND", raising=False)
    port = _free_port()
    rayd.init()
    try:
        with pytest.raises(URLError):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=0.5)
    finally:
        rayd.shutdown()


def test_metrics_endpoint_serves_prometheus_text(metrics_addr: str) -> None:
    body = _scrape(metrics_addr)
    # The five counters and the refs gauge should all be registered
    # (zero-valued is fine — `# HELP` lines confirm registration).
    for name in (
        "rayd_driver_tasks_submitted_total",
        "rayd_driver_tasks_completed_total",
        "rayd_driver_tasks_failed_total",
        "rayd_driver_puts_total",
        "rayd_driver_gets_total",
        "rayd_driver_refs_alive",
    ):
        assert f"# HELP {name}" in body, f"missing HELP for {name}"


def test_puts_total_increments_on_put(metrics_addr: str) -> None:
    before = _counter_value(_scrape(metrics_addr), "rayd_driver_puts_total")
    _ = rayd.put(b"hello")
    _ = rayd.put(b"world")
    after = _counter_value(_scrape(metrics_addr), "rayd_driver_puts_total")
    assert after - before == 2, f"expected 2 puts, got {after - before}"


def test_gets_total_increments_on_get(metrics_addr: str) -> None:
    ref = rayd.put(b"payload")
    before = _counter_value(_scrape(metrics_addr), "rayd_driver_gets_total")
    _ = rayd.get(ref)
    _ = rayd.get(ref)
    after = _counter_value(_scrape(metrics_addr), "rayd_driver_gets_total")
    assert after - before == 2, f"expected 2 gets, got {after - before}"


def test_refs_alive_reflects_live_refcounter(metrics_addr: str) -> None:
    base = _counter_value(_scrape(metrics_addr), "rayd_driver_refs_alive")
    refs = [rayd.put(i) for i in range(5)]
    after = _counter_value(_scrape(metrics_addr), "rayd_driver_refs_alive")
    assert after - base == 5, f"expected refs_alive +5, got {after - base}"
    del refs
    # Ref drop is synchronous through the Python __del__ -> Rust path.
    final = _counter_value(_scrape(metrics_addr), "rayd_driver_refs_alive")
    assert final == base, f"expected refs_alive to return to {base}, got {final}"


@rayd.remote
def _double(x: int) -> int:
    return x * 2


def test_tasks_submitted_and_completed_increment(metrics_addr: str) -> None:
    sub_before = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_submitted_total")
    com_before = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_completed_total")
    refs = [_double.remote(i) for i in range(3)]
    values = rayd.get(refs)
    assert values == [0, 2, 4]
    sub_after = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_submitted_total")
    com_after = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_completed_total")
    assert sub_after - sub_before == 3, f"expected 3 submits, got {sub_after - sub_before}"
    assert com_after - com_before == 3, f"expected 3 completions, got {com_after - com_before}"


@rayd.remote
def _boom() -> int:
    msg = "intentional"
    raise RuntimeError(msg)


def test_tasks_failed_increments_on_error(metrics_addr: str) -> None:
    failed_before = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_failed_total")
    ref = _boom.remote()
    with pytest.raises(Exception):  # noqa: B017, PT011
        rayd.get(ref)
    failed_after = _counter_value(_scrape(metrics_addr), "rayd_driver_tasks_failed_total")
    assert failed_after - failed_before == 1, (
        f"expected 1 failure, got {failed_after - failed_before}"
    )
