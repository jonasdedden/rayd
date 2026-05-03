"""Phase 1 tests: tasks run, results round-trip, partial failures don't poison batches.

The headline is `test_get_settled_with_partial_failure`: it documents the
behavior that motivates this whole project.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest

import rayd
from rayd import _native

if TYPE_CHECKING:
    from collections.abc import Generator


@pytest.fixture(autouse=True)
def _runtime() -> Generator[None]:
    rayd.init()
    try:
        yield
    finally:
        rayd.shutdown()


# ── put / get ─────────────────────────────────────────────────────────────


def test_put_get_round_trip_int() -> None:
    ref = rayd.put(42)
    assert isinstance(ref, rayd.ObjectRef)
    assert rayd.get(ref) == 42


def test_put_get_round_trip_complex() -> None:
    payload = {"a": [1, 2, 3], "b": {"nested": True}}
    ref = rayd.put(payload)
    assert rayd.get(ref) == payload


def test_put_then_state_is_ready_local() -> None:
    ref = rayd.put("hello")
    assert ref.state() == rayd.RefState.ReadyLocal
    assert ref.is_ready() is True
    assert ref.is_failed() is False


# ── @rayd.remote ───────────────────────────────────────────────────────────


@rayd.remote
def _square(x: int) -> int:
    return x * x


def test_remote_simple_returns_ref() -> None:
    ref = _square.remote(7)
    assert isinstance(ref, rayd.ObjectRef)
    assert rayd.get(ref) == 49


def test_remote_concurrent_submissions() -> None:
    refs = [_square.remote(i) for i in range(20)]
    values = rayd.get(refs)
    assert isinstance(values, list)
    assert values == [i * i for i in range(20)]


def test_remote_keeps_original_callable() -> None:
    assert _square.__wrapped__(6) == 36


# ── failure handling: the headline ────────────────────────────────────────


@rayd.remote
def _maybe_fail(i: int) -> int:
    if i == 5:
        msg = "five is forbidden"
        raise ValueError(msg)
    return i * 2


def test_get_raises_on_first_failure_for_compat() -> None:
    """Plain `rayd.get` mirrors `ray.get`: raises on first failure."""
    refs = [_maybe_fail.remote(i) for i in range(10)]
    with pytest.raises(ValueError, match="five is forbidden"):
        rayd.get(refs)


def test_get_settled_with_partial_failure() -> None:
    """The headline: `get_settled` returns one Result per ref, no raise."""
    refs = [_maybe_fail.remote(i) for i in range(10)]
    results = rayd.get_settled(refs)
    assert len(results) == 10

    successes_raw = [r.value for r in results if isinstance(r, rayd.Ok)]
    failures = [r.info for r in results if isinstance(r, rayd.Err)]
    assert len(successes_raw) == 9
    assert len(failures) == 1

    # Narrow: every success in this test is an int.
    successes: list[int] = []
    for v in successes_raw:
        assert isinstance(v, int)
        successes.append(v)
    assert sorted(successes) == [0, 2, 4, 6, 8, 12, 14, 16, 18]

    err_info = failures[0]
    assert err_info.category == rayd.ErrorCategory.TaskException
    assert "five is forbidden" in err_info.message
    assert err_info.traceback is not None
    assert "raise ValueError" in err_info.traceback


@rayd.remote
def _slow(i: int) -> int:
    time.sleep(0.5)
    return i


def test_get_settled_with_pending_timeout() -> None:
    """A timeout shorter than the task's runtime yields `Pending` entries."""
    refs = [_slow.remote(i) for i in range(3)]
    results = rayd.get_settled(refs, timeout=0.05)
    assert all(isinstance(r, rayd.Pending) for r in results)


@rayd.remote
def _slow_value() -> str:
    time.sleep(0.1)
    return "done"


def test_objectref_state_transitions_pending_to_ready() -> None:
    """A ref starts pending and becomes ready after its task completes."""
    ref = _slow_value.remote()
    initial = ref.state()
    assert initial in (rayd.RefState.Pending, rayd.RefState.ReadyLocal)
    assert rayd.get(ref) == "done"
    assert ref.state() == rayd.RefState.ReadyLocal


@rayd.remote
def _bang() -> int:
    msg = "kaboom"
    raise RuntimeError(msg)


def test_objectref_state_failed_after_exception() -> None:
    ref = _bang.remote()
    states = rayd.wait_with_states([ref], timeout=2.0)
    assert states[ref] == rayd.RefState.Failed
    assert ref.is_failed() is True


# ── peek_error / exception ────────────────────────────────────────────────


@rayd.remote
def _raise_value_error() -> None:
    msg = "lightweight"
    raise ValueError(msg)


def test_peek_error_does_not_unpickle() -> None:
    ref = _raise_value_error.remote()
    rayd.wait_with_states([ref], timeout=2.0)

    info = ref.peek_error()
    assert info is not None
    assert info.category == rayd.ErrorCategory.TaskException
    assert "lightweight" in info.message
    assert info.traceback is not None


@rayd.remote
def _raise_specific() -> None:
    msg = "specific"
    raise KeyError(msg)


def test_exception_unpickles_original_class() -> None:
    ref = _raise_specific.remote()
    rayd.wait_with_states([ref], timeout=2.0)

    exc = ref.exception()
    assert isinstance(exc, KeyError)


# ── state / wait ──────────────────────────────────────────────────────────


@rayd.remote
def _ok(i: int) -> int:
    return i


@rayd.remote
def _bad() -> None:
    msg = "fail"
    raise ValueError(msg)


def test_state_distinguishes_three_categories() -> None:
    ok_ref = _ok.remote(1)
    bad_ref = _bad.remote()
    rayd.wait_with_states([ok_ref, bad_ref], timeout=2.0)

    pending_ref = rayd.put(0)
    rayd.free([pending_ref])

    snap = rayd.state([ok_ref, bad_ref, pending_ref])
    assert snap[ok_ref] == rayd.RefState.ReadyLocal
    assert snap[bad_ref] == rayd.RefState.Failed
    assert snap[pending_ref] == rayd.RefState.Pending


@rayd.remote
def _slow_partition(i: int) -> int:
    time.sleep(0.3)
    return i


@rayd.remote
def _fast_partition(i: int) -> int:
    return i


def test_wait_partitions_ready_and_not_ready() -> None:
    fast_refs = [_fast_partition.remote(i) for i in range(3)]
    slow_refs = [_slow_partition.remote(i) for i in range(2)]
    refs = fast_refs + slow_refs

    ready, not_ready = rayd.wait(refs, num_returns=3, timeout=1.0)
    assert len(ready) == 3
    assert len(not_ready) == 2
    for r in fast_refs:
        assert r in ready


# ── num_returns > 1 ───────────────────────────────────────────────────────


@rayd.remote
def _split(x: int) -> tuple[int, int, int]:
    return x, x * x, x * x * x


def test_multi_return_via_options() -> None:
    ref = _split.options(num_returns=3).remote(3)
    # `.remote()` exposes only the first ref by convention. The full list
    # comes from the lower-level `_native.submit_task`.
    assert isinstance(ref, rayd.ObjectRef)


def test_multi_return_via_native_submit_task() -> None:
    refs = _native.submit_task(_split.__wrapped__, (4,), None, 3)
    assert len(refs) == 3
    typed = [r for r in refs if isinstance(r, rayd.ObjectRef)]
    assert len(typed) == 3
    values = rayd.get(typed)
    assert values == [4, 16, 64]


# ── Phase 4.4: lineage reconstruction ──────────────────────────────────


@rayd.remote
def _payload_task(seed: int) -> bytes:
    # Deterministic — same args → same bytes — so a re-run of the
    # same task produces an identical result. > inline threshold so
    # it lands in plasma where eviction is observable.
    return bytes((seed + i) & 0xFF for i in range(150_000))


def test_lineage_reconstruction_after_local_eviction() -> None:
    """A task that's been recorded can be replayed after local loss.

    `submit_task` records the task. After a successful `get`, force-
    evict the local store entry; a `try_resubmit_for_lineage` then
    re-queues the task with the same `task_id`, the worker writes
    back to the same plasma slot, and a follow-up `get` blocks on
    the new attempt and succeeds with the (deterministically
    identical) bytes.
    """
    ref = _payload_task.remote(7)
    first = rayd.get(ref)
    assert isinstance(first, bytes)
    assert len(first) == 150_000

    # Simulate object loss: drop the local store entry so the next
    # `get` would otherwise block forever.
    object_id = ref.object_id.to_bytes()
    _native._evict_local(object_id)  # noqa: SLF001
    assert ref.state() == rayd.RefState.Pending

    # Explicit lineage hook re-queues the task.
    assert _native.try_resubmit_for_lineage(object_id) is True
    second = rayd.get(ref)
    assert second == first


def test_try_resubmit_returns_false_for_unknown_object() -> None:
    """`try_resubmit_for_lineage` is a no-op for unrecorded ids."""
    fresh = rayd.ObjectId.random()
    assert _native.try_resubmit_for_lineage(fresh.to_bytes()) is False


def test_lineage_budget_eventually_exhausts() -> None:
    """Repeatedly evicting + resubmitting drains the retry budget."""
    ref = _payload_task.remote(11)
    rayd.get(ref)
    object_id = ref.object_id.to_bytes()

    # Default budget is 3. Burn through it by repeatedly evicting
    # and resubmitting; after each, `get` lets the result re-seal.
    for _ in range(3):
        _native._evict_local(object_id)  # noqa: SLF001
        assert _native.try_resubmit_for_lineage(object_id) is True
        rayd.get(ref)

    _native._evict_local(object_id)  # noqa: SLF001
    assert _native.try_resubmit_for_lineage(object_id) is False


def test_rayd_get_auto_resubmits_lost_object() -> None:
    """`rayd.get` transparently replays a task whose result was lost.

    No explicit `try_resubmit_for_lineage` call: the auto-resubmit
    path inside `rayd.get` notices `state() == Pending`, consults
    the lineage manager, finds the task completed with budget
    remaining, and re-queues. The blocking `_native.get` then
    waits for the new attempt to seal.
    """
    ref = _payload_task.remote(13)
    first = rayd.get(ref)
    assert isinstance(first, bytes)
    _native._evict_local(ref.object_id.to_bytes())  # noqa: SLF001
    second = rayd.get(ref)
    assert second == first


def test_rayd_get_raises_object_unreconstructable_after_budget() -> None:
    """Once the retry budget is gone, `rayd.get` surfaces a typed error."""
    ref = _payload_task.remote(17)
    rayd.get(ref)
    oid = ref.object_id.to_bytes()
    # Burn the entire default budget (3 retries).
    for _ in range(3):
        _native._evict_local(oid)  # noqa: SLF001
        assert _native.try_resubmit_for_lineage(oid) is True
        rayd.get(ref)
    # One more eviction → no remaining budget. `rayd.get` now raises.
    _native._evict_local(oid)  # noqa: SLF001
    with pytest.raises(rayd.ObjectUnreconstructableError, match="lineage exhausted"):
        rayd.get(ref)
