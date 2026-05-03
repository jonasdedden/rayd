"""Phase 3.2 tests: tasks really run in subprocess workers, with parallelism.

These complement test_tasks.py (which tested the API surface). The point
here is to prove that:

1. Task callables execute in worker PIDs distinct from the driver.
2. Multiple workers run concurrently, so wall-clock for N parallel
   sleep-bound tasks is ~one sleep, not N sleeps.
3. Errors propagate correctly across the process boundary, including the
   cloudpickled exception so `ObjectRef.exception()` returns the right type.
4. cloudpickle's ability to ship test-module-defined functions (closures,
   functions defined inside test bodies) actually works.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import pytest

import rayd

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def _runtime() -> Generator[None]:
    rayd.init()
    try:
        yield
    finally:
        rayd.shutdown()


# ── 1. Tasks run in workers, not the driver ───────────────────────────


@rayd.remote
def _capture_pid() -> int:
    return os.getpid()


def test_task_runs_in_a_subprocess_pid() -> None:
    driver_pid = os.getpid()
    ref = _capture_pid.remote()
    worker_pid = rayd.get(ref)
    assert isinstance(worker_pid, int)
    assert worker_pid != driver_pid


def test_many_tasks_distribute_across_distinct_pids() -> None:
    driver_pid = os.getpid()
    refs = [_capture_pid.remote() for _ in range(16)]
    pids = rayd.get(refs)
    assert isinstance(pids, list)
    assert all(p != driver_pid for p in pids)
    # We have 4 default workers, so 16 tasks must spread across at least 2
    # distinct pids (load-balancing means we expect close to all 4).
    distinct = {p for p in pids if isinstance(p, int)}
    assert len(distinct) >= 2


# ── 2. Real parallelism (no GIL bottleneck) ────────────────────────────


@rayd.remote
def _sleeper(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


def test_n_sleeping_tasks_run_in_parallel() -> None:
    """With 4 workers, 4 * 0.5s sleeps must finish in well under 4 * 0.5 == 2s."""
    n = 4
    nap = 0.5
    start = time.monotonic()
    refs = [_sleeper.remote(nap) for _ in range(n)]
    rayd.get(refs)
    elapsed = time.monotonic() - start
    # If tasks were serialized, elapsed >= 4 * 0.5 = 2.0.
    # With 4 parallel workers, expected ~0.5s + dispatch overhead. We allow
    # generous slack to keep the test stable on busy CI machines.
    assert elapsed < 1.5, f"4 parallel 0.5s sleeps took {elapsed:.2f}s — looks serialised"


# ── 3. Error propagation across process boundary ──────────────────────


@rayd.remote
def _raise_value_error_subprocess(message: str) -> None:
    raise ValueError(message)


def test_error_propagates_across_process_boundary() -> None:
    ref = _raise_value_error_subprocess.remote("from-the-worker")
    rayd.wait_with_states([ref], timeout=2.0)
    err = ref.peek_error()
    assert err is not None
    assert err.category == rayd.ErrorCategory.TaskException
    assert "from-the-worker" in err.message


def test_exception_is_unpickled_with_correct_type() -> None:
    ref = _raise_value_error_subprocess.remote("specific")
    rayd.wait_with_states([ref], timeout=2.0)
    exc = ref.exception()
    # cloudpickle round-trips the original ValueError type.
    assert isinstance(exc, ValueError)
    assert "specific" in str(exc)


@rayd.remote
def _raise_keyerror() -> None:
    msg = "missing-key"
    raise KeyError(msg)


def test_get_raises_specific_exception_type() -> None:
    ref = _raise_keyerror.remote()
    with pytest.raises(KeyError):
        rayd.get(ref)


@rayd.remote
def _maybe_fail(i: int) -> int:
    if i == 5:
        msg = "five is forbidden (subprocess)"
        raise ValueError(msg)
    return i * 2


def test_get_settled_partial_failure_through_subprocesses() -> None:
    """Partial-failure semantics still hold when tasks run in subprocesses."""
    refs = [_maybe_fail.remote(i) for i in range(10)]
    results = rayd.get_settled(refs)
    assert len(results) == 10
    successes = [r for r in results if isinstance(r, rayd.Ok)]
    failures = [r for r in results if isinstance(r, rayd.Err)]
    assert len(successes) == 9
    assert len(failures) == 1
    assert failures[0].info.category == rayd.ErrorCategory.TaskException
    assert "five is forbidden (subprocess)" in failures[0].info.message


# ── 4. cloudpickle handles test-module functions ──────────────────────


def test_function_defined_inside_test_body_pickles_via_cloudpickle() -> None:
    """Verify cloudpickle can ship a function defined inside the test body.

    Stdlib `pickle` can't pickle a function defined inside another
    function's scope; cloudpickle can. The dispatcher uses cloudpickle
    precisely so test-module-defined remote functions round-trip.
    """

    @rayd.remote
    def _local_only(x: int) -> int:
        # closes over no locals beyond `x`, so cloudpickle's job is easy
        return x + 100

    ref = _local_only.remote(7)
    assert rayd.get(ref) == 107


def test_closure_over_local_runs_in_worker() -> None:
    multiplier = 13

    @rayd.remote
    def _scaled(x: int) -> int:
        return x * multiplier

    refs = [_scaled.remote(i) for i in range(5)]
    values = rayd.get(refs)
    assert values == [i * 13 for i in range(5)]


# ── 5. Returning large payloads from a worker ─────────────────────────


@rayd.remote
def _produce_blob(size: int) -> bytes:
    return b"\xab" * size


def test_large_subprocess_result_round_trips_via_plasma() -> None:
    payload = rayd.get(_produce_blob.remote(2 * 1024 * 1024))
    assert isinstance(payload, bytes)
    assert len(payload) == 2 * 1024 * 1024
    assert payload[:8] == b"\xab" * 8
