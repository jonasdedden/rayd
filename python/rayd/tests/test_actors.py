"""Phase 5.1 + 5.2 tests: actor MVP and subprocess isolation.

`rayd.actor(cls)` produces an `ActorClass`. `MyClass.remote(*args)`
spawns a per-actor subprocess; method calls dispatch FIFO over a
UDS and seal results into shared plasma.
"""

from __future__ import annotations

import os
import pickle
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


class _CounterImpl:
    def __init__(self, start: int = 0) -> None:
        self.x = start

    def increment(self) -> int:
        self.x += 1
        return self.x

    def add(self, delta: int) -> int:
        self.x += delta
        return self.x

    def get(self) -> int:
        return self.x

    def get_pid(self) -> int:
        return os.getpid()

    def boom(self) -> None:
        msg = "user-raised"
        raise ValueError(msg)


# Use the assignment form (rather than `@rayd.actor`) so mypy
# tracks the resulting `ActorClass[_CounterImpl]` precisely. See
# `rayd.actor`'s docstring for the typing caveat.
_Counter = rayd.actor(_CounterImpl)


def test_remote_class_returns_actor_class() -> None:
    assert isinstance(_Counter, rayd.ActorClass)


def test_actor_remote_returns_handle() -> None:
    handle = _Counter.remote(0)
    assert isinstance(handle, rayd.ActorHandle)
    handle.terminate()


def test_actor_method_calls_run_in_order() -> None:
    """1k sequential increments preserve order; final state matches."""
    handle = _Counter.remote(0)
    try:
        n = 1000
        refs = [handle.increment.remote() for _ in range(n)]
        values = [rayd.get(r) for r in refs]
        assert values == list(range(1, n + 1))
        assert rayd.get(handle.get.remote()) == n
    finally:
        handle.terminate()


def test_actor_method_passes_args_and_kwargs() -> None:
    handle = _Counter.remote(100)
    try:
        ref1 = handle.add.remote(5)
        ref2 = handle.add.remote(delta=10)
        assert rayd.get(ref1) == 105
        assert rayd.get(ref2) == 115
    finally:
        handle.terminate()


def test_actor_method_exception_surfaces_through_get() -> None:
    """Method exceptions seal an Error result; `rayd.get` re-raises."""
    handle = _Counter.remote(0)
    try:
        ref = handle.boom.remote()
        with pytest.raises(ValueError, match="user-raised"):
            rayd.get(ref)
    finally:
        handle.terminate()


def test_actor_state_is_isolated_per_instance() -> None:
    """Each `.remote()` produces a fresh actor with its own state."""
    a = _Counter.remote(0)
    b = _Counter.remote(100)
    try:
        a_refs = [a.increment.remote() for _ in range(3)]
        b_refs = [b.increment.remote() for _ in range(2)]
        assert [rayd.get(r) for r in a_refs] == [1, 2, 3]
        assert [rayd.get(r) for r in b_refs] == [101, 102]
    finally:
        a.terminate()
        b.terminate()


def test_terminate_is_idempotent() -> None:
    handle = _Counter.remote(0)
    handle.terminate()
    handle.terminate()  # should be a no-op


# ── Phase 5.2: subprocess isolation ────────────────────────────────────


def test_actor_runs_in_separate_process() -> None:
    """The actor's PID differs from the driver's, proving subprocess isolation."""
    handle = _Counter.remote(0)
    try:
        actor_pid = handle.pid
        assert actor_pid != os.getpid()
        # Method-reported PID should match the handle's reported PID.
        reported_pid = rayd.get(handle.get_pid.remote())
        assert reported_pid == actor_pid
    finally:
        handle.terminate()


def test_distinct_actors_get_distinct_subprocesses() -> None:
    """Two `.remote()` calls produce two subprocesses with different PIDs."""
    a = _Counter.remote(0)
    b = _Counter.remote(0)
    try:
        assert a.pid != b.pid
        assert a.pid != os.getpid()
        assert b.pid != os.getpid()
    finally:
        a.terminate()
        b.terminate()


# ── Phase 5.3: actor restart on subprocess crash ───────────────────────


class _FlakyImpl:
    def __init__(self) -> None:
        self.x = 0

    def increment(self) -> int:
        self.x += 1
        return self.x

    def hard_exit(self) -> None:
        # Hard exit (no SystemExit traversal) so the subprocess
        # terminates without sealing a result. Mimics a segfault /
        # OOM kill — the in-flight method never produces a result.
        os._exit(1)


_Flaky = rayd.actor(_FlakyImpl)
_FlakyOnce = rayd.actor(_FlakyImpl, max_restarts=1)
_FlakyZero = rayd.actor(_FlakyImpl, max_restarts=0)


def test_actor_subprocess_crash_seals_in_flight_call_with_actor_died() -> None:
    """A method that hard-exits the subprocess seals its ref as ActorDied."""
    handle = _FlakyZero.remote()
    try:
        ref = handle.hard_exit.remote()
        with pytest.raises(rayd.ActorDiedError, match="died mid-call"):
            rayd.get(ref)
    finally:
        handle.terminate()


def test_actor_restarts_after_crash_when_budget_remains() -> None:
    """With max_restarts=1, a crash respawns the subprocess."""
    handle = _FlakyOnce.remote()
    try:
        original_pid = handle.pid
        # Crash the subprocess.
        ref = handle.hard_exit.remote()
        with pytest.raises(rayd.ActorDiedError):
            rayd.get(ref)
        # Wait for the respawn to bring up a NEW pid. `restarts_used`
        # is incremented before the new subprocess is fully spawned,
        # so we can't use it as the readiness signal.
        deadline = time.monotonic() + 5.0
        new_pid = original_pid
        while time.monotonic() < deadline:
            new_pid = handle.pid
            if new_pid != original_pid:
                break
            time.sleep(0.05)
        assert new_pid != original_pid, f"actor did not respawn: pid still {original_pid}"
        assert handle.restarts_used == 1
        # Fresh state — increment from 0.
        assert rayd.get(handle.increment.remote()) == 1
    finally:
        handle.terminate()


def test_actor_dies_when_max_restarts_exhausted() -> None:
    """max_restarts=0 → first crash marks the actor dead; future calls fail."""
    handle = _FlakyZero.remote()
    try:
        ref = handle.hard_exit.remote()
        with pytest.raises(rayd.ActorDiedError):
            rayd.get(ref)
        # Wait for the crash handler to finish marking dead.
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            try:
                handle.increment.remote()
            except rayd.ActorDiedError:
                break
            time.sleep(0.05)
        with pytest.raises(rayd.ActorDiedError, match="exhausted"):
            handle.increment.remote()
    finally:
        handle.terminate()


# ── Phase 5.4a: ActorHandle pickling ───────────────────────────────────


def test_actor_handle_round_trips_through_pickle() -> None:
    """`pickle.dumps(handle)` + `pickle.loads(blob)` returns a working handle.

    Within one driver process both handles wrap the SAME live
    subprocess — verified by sharing state across the pair.
    """
    handle = _Counter.remote(0)
    try:
        # Bump state on the original handle.
        rayd.get(handle.increment.remote())
        rayd.get(handle.increment.remote())

        blob = pickle.dumps(handle)
        twin = pickle.loads(blob)  # noqa: S301
        assert isinstance(twin, rayd.ActorHandle)
        # Both handles point at the same subprocess.
        assert twin.pid == handle.pid

        # State observed through `twin` reflects calls made via `handle`.
        assert rayd.get(twin.get.remote()) == 2

        # Calls through `twin` also affect `handle`'s view.
        rayd.get(twin.increment.remote())
        assert rayd.get(handle.get.remote()) == 3
    finally:
        handle.terminate()


def test_unpickling_handle_after_terminate_raises_lookup_error() -> None:
    """Once the underlying actor is gone, unpickling raises `LookupError`."""
    handle = _Counter.remote(0)
    blob = pickle.dumps(handle)
    handle.terminate()
    with pytest.raises(LookupError, match="not registered"):
        pickle.loads(blob)  # noqa: S301


# ── Phase 5.4c groundwork: owner_node_id stamping ──────────────────────


def test_mint_actor_result_ref_default_has_no_owner_node_id() -> None:
    """`_mint_actor_result_ref()` keeps the same-driver default unchanged."""
    ref = _native._mint_actor_result_ref()  # noqa: SLF001
    # Same-driver actor refs leave owner_node_id unset; this is what
    # `rayd.get` interprets as "owner is local".
    assert ref.owner_node_id is None


def test_mint_actor_result_ref_stamps_owner_node_id() -> None:
    """Passing `owner_node_id=…` stamps the bytes onto the ref."""
    nid = bytes(range(16))
    ref = _native._mint_actor_result_ref(nid)  # noqa: SLF001
    assert ref.owner_node_id is not None
    assert bytes(ref.owner_node_id) == nid


def test_mint_actor_result_ref_rejects_wrong_length_owner_node_id() -> None:
    with pytest.raises(ValueError, match="node_id requires 16 bytes"):
        _native._mint_actor_result_ref(b"\x00" * 15)  # noqa: SLF001
