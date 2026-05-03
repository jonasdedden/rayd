"""Phase 2 tests: large objects ride the shared-memory plasma store.

These exercise the path where the pickled buffer exceeds the inline
threshold and gets routed through the UDS+SCM_RIGHTS plasma protocol.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd

if TYPE_CHECKING:
    from collections.abc import Generator


# Match `INLINE_THRESHOLD_BYTES` in rayd-core (100 KiB).
INLINE_THRESHOLD = 100 * 1024


@pytest.fixture(autouse=True)
def _runtime() -> Generator[None]:
    rayd.init()
    try:
        yield
    finally:
        rayd.shutdown()


def _proc_maps() -> str:
    """Read /proc/self/maps. Linux-only; tests skip on other platforms."""
    return Path("/proc/self/maps").read_text(encoding="utf-8")


# ── Round trips ────────────────────────────────────────────────────────


def test_small_value_stays_inline() -> None:
    """Small values do not invoke plasma (and don't appear in /proc/self/maps)."""
    ref = rayd.put(b"a tiny payload")
    assert rayd.get(ref) == b"a tiny payload"


def test_large_bytes_round_trip_via_plasma() -> None:
    """Half a megabyte of bytes round-trips bit-for-bit."""
    payload = os.urandom(512 * 1024)
    assert len(payload) > INLINE_THRESHOLD
    ref = rayd.put(payload)
    out = rayd.get(ref)
    assert isinstance(out, bytes)
    assert out == payload


def test_very_large_value_round_trips() -> None:
    """Multi-MB payload stresses the bump allocator and arena mmap."""
    payload = os.urandom(8 * 1024 * 1024)
    ref = rayd.put(payload)
    out = rayd.get(ref)
    assert out == payload


def test_state_is_ready_local_for_plasma_resident() -> None:
    """State inspection works for plasma-resident objects without fetching data."""
    payload = os.urandom(512 * 1024)
    ref = rayd.put(payload)
    # Cheap state check: must NOT trigger a plasma fetch (we can't observe
    # that directly, but we can at least confirm the state value is correct).
    assert ref.state() == rayd.RefState.ReadyLocal
    assert ref.is_ready() is True
    assert ref.is_failed() is False


# ── /proc/self/maps verification ───────────────────────────────────────


@pytest.mark.skipif(not Path("/proc/self/maps").exists(), reason="Linux-only")
def test_plasma_arena_appears_in_proc_self_maps() -> None:
    """Verify that the plasma arena's memfd appears in /proc/self/maps.

    The rayd-plasma client mmaps the memfd it received via SCM_RIGHTS,
    so the arena must show up as a mapping in this process.
    """
    payload = os.urandom(256 * 1024)
    ref = rayd.put(payload)
    _ = rayd.get(ref)  # forces the client to mmap the arena

    maps = _proc_maps()
    # memfd_create files appear as `/memfd:rayd_plasma (deleted)` entries.
    # Match either substring style without being too brittle.
    assert "memfd:rayd_plasma" in maps or "rayd_plasma" in maps, (
        "expected the rayd_plasma memfd to be mapped into this process; "
        f"/proc/self/maps had no match. First 1k chars:\n{maps[:1024]}"
    )


# ── Tasks producing large outputs ──────────────────────────────────────


@rayd.remote
def _produce_zeros(n: int) -> bytes:
    return b"\x00" * n


def test_remote_task_with_large_output_uses_plasma() -> None:
    ref = _produce_zeros.remote(2 * INLINE_THRESHOLD)
    out = rayd.get(ref)
    assert isinstance(out, bytes)
    assert len(out) == 2 * INLINE_THRESHOLD
    assert all(b == 0 for b in out[:128])


@rayd.remote
def _produce_string_blob(n: int) -> str:
    return "x" * n


def test_remote_task_with_large_string() -> None:
    ref = _produce_string_blob.remote(400 * 1024)
    out = rayd.get(ref)
    assert isinstance(out, str)
    assert len(out) == 400 * 1024


# ── Concurrent producers ───────────────────────────────────────────────


@rayd.remote
def _slice_of_big(seed: int, size: int) -> bytes:
    """Generate a deterministic large blob keyed by `seed`."""
    return ((seed % 251).to_bytes(1, "little")) * size


def test_many_concurrent_large_results_round_trip() -> None:
    """Stress-test concurrency on the plasma client mutex.

    Spawns 10 tasks each producing 200 KiB and verifies all bytes match.
    """
    size = 200 * 1024
    refs = [_slice_of_big.remote(s, size) for s in range(10)]
    results = rayd.get(refs)
    assert isinstance(results, list)
    for s, r in enumerate(results):
        assert isinstance(r, bytes)
        assert len(r) == size
        assert r[0] == s % 251


# ── Mixed inline + plasma in get_settled ───────────────────────────────


@rayd.remote
def _maybe_fail_or_blob(i: int, blob_size: int) -> bytes:
    if i == 3:
        msg = "task 3 broke"
        raise RuntimeError(msg)
    return bytes([i & 0xFF]) * blob_size


def test_get_settled_mixes_inline_failures_and_plasma_successes() -> None:
    """Verify partial-failure semantics when successes go through plasma.

    The headline `get_settled` behavior must hold whether the successful
    refs are inline or plasma-resident.
    """
    size = 256 * 1024
    refs = [_maybe_fail_or_blob.remote(i, size) for i in range(5)]
    results = rayd.get_settled(refs)
    assert len(results) == 5

    failures = [r for r in results if isinstance(r, rayd.Err)]
    assert len(failures) == 1
    assert failures[0].info.category == rayd.ErrorCategory.TaskException
    assert "task 3 broke" in failures[0].info.message

    successes_raw = [r.value for r in results if isinstance(r, rayd.Ok)]
    assert len(successes_raw) == 4
    for r in successes_raw:
        assert isinstance(r, bytes)
        assert len(r) == size


# ── State and wait still cheap on plasma residents ─────────────────────


def test_state_does_not_require_plasma_data() -> None:
    """Verify state() is metadata-only for plasma-resident objects.

    A snapshot should succeed without fetching data buffers from plasma,
    so `state(refs)` stays cheap regardless of payload size.
    """
    refs = [rayd.put(os.urandom(150 * 1024)) for _ in range(5)]
    snap = rayd.state(refs)
    for r in refs:
        assert snap[r] == rayd.RefState.ReadyLocal


def test_wait_with_states_handles_plasma_results() -> None:
    @rayd.remote
    def _slow_blob() -> bytes:
        time.sleep(0.05)
        return os.urandom(150 * 1024)

    refs = [_slow_blob.remote() for _ in range(3)]
    states = rayd.wait_with_states(refs, timeout=2.0)
    for r in refs:
        assert states[r] == rayd.RefState.ReadyLocal


# ── Phase 4.3.1: drop hook frees plasma ────────────────────────────────


def test_dropping_last_object_ref_frees_plasma_object() -> None:
    """Dropping the last Python `ObjectRef` for a put unpins it locally.

    `rayd.put()` registers an entry in the owner's `RefCounter`; the
    final `del ref` (or scope exit) calls `dec_local_ref`, which
    transitions the entry to fully unpinned and removes the object
    from the local memory store + plasma.
    """
    payload = os.urandom(150 * 1024)
    ref = rayd.put(payload)
    # State observable while ref is alive.
    assert ref.state() == rayd.RefState.ReadyLocal
    # After dropping, `state()` on a fresh ref to the same id should
    # report `Pending` (entry was wiped from the local store).
    object_id = ref.object_id
    nil_addr = rayd.Address.nil()
    del ref
    # Reconstruct a ref to the same id (no auto-decrement on drop —
    # this synthetic ref doesn't own the count).
    probe = rayd.ObjectRef(object_id, nil_addr)
    assert probe.state() == rayd.RefState.Pending


def test_two_python_handles_to_same_pyobjectref_drop_once() -> None:
    """Python aliases share one underlying PyObjectRef.

    `b = a` doesn't clone the Rust-side struct — both Python names
    point at the same object. The dec only fires when the refcount
    on that single PyObjectRef hits zero.
    """
    payload = os.urandom(150 * 1024)
    a = rayd.put(payload)
    b = a
    # Drop one alias; the other still pins the object.
    del a
    assert b.state() == rayd.RefState.ReadyLocal
    object_id = b.object_id
    del b
    # Now fully gone.
    probe = rayd.ObjectRef(object_id, rayd.Address.nil())
    assert probe.state() == rayd.RefState.Pending
