"""Phase 6.5 tests: spill + restore on local `rayd.get`.

When a sealed object is evicted out of plasma but the local memory
store still has its index entry, `rayd.get` consults the registered
recoverer (the per-session `LocalObjectManager`), restores the bytes
from the spill backend, reseals into plasma, and returns the value.

The driver-side glue spins up a `LocalObjectManager` rooted in a
tempdir whenever a GCS is attached (Phase 6.4), and Phase 6.5 wires
it as the `CoreWorker`'s recovery hook. The `_native._spill_object`
helper drives the eviction manually for these tests; an automatic
spill-on-pressure policy lands in Phase 6.6.
"""

from __future__ import annotations

import contextlib
import socket
import subprocess
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


# ── happy path ─────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_get_after_spill_restores_transparently() -> None:
    """`rayd.put` → `_spill_object` → `rayd.get` returns the value."""
    payload = list(range(2_000))  # large enough to land in plasma
    ref = rayd.put(payload)

    # Spill the object out of plasma. Local store index entry stays;
    # plasma copy gets removed.
    spilled = _native._spill_object(ref.object_id.to_bytes())  # noqa: SLF001
    assert spilled is True

    # `rayd.get` must consult the recoverer, restore from spill, and
    # return the original value transparently.
    assert rayd.get(ref) == payload


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_double_spill_is_idempotent() -> None:
    """Spilling an already-spilled object is a successful no-op.

    First spill moves the bytes to disk and deletes from plasma.
    Second spill finds no plasma entry and returns False.
    """
    ref = rayd.put(list(range(3_000)))
    assert _native._spill_object(ref.object_id.to_bytes()) is True  # noqa: SLF001
    assert _native._spill_object(ref.object_id.to_bytes()) is False  # noqa: SLF001
    # Get still works — recoverer has the bytes from the first spill.
    assert rayd.get(ref) == list(range(3_000))


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_get_after_spill_then_repeated_get_works() -> None:
    """After the first `get` reseals, subsequent gets read from plasma.

    Confirms the recover-and-reseal path leaves plasma populated so
    repeated reads don't hammer the spill backend.
    """
    payload = list(range(5_000))
    ref = rayd.put(payload)
    _native._spill_object(ref.object_id.to_bytes())  # noqa: SLF001

    # First get — restores via spill.
    assert rayd.get(ref) == payload
    # Second get — should hit plasma directly. (We can't directly
    # observe the plasma cache hit without exposing more native
    # internals; this test mostly proves the second get doesn't
    # regress.)
    assert rayd.get(ref) == payload


# ── Phase 6.6: spill cleanup on free ───────────────────────────────────


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_drop_last_ref_removes_spill_entry() -> None:
    """Once the last `ObjectRef` drops, the spill record is gone too.

    The free-callback fires when the local refcount hits zero; it
    deregisters at the raylet directory AND calls `manager.forget`
    so the on-disk spill file doesn't leak.
    """
    payload = list(range(2_000))
    ref = rayd.put(payload)
    oid_bytes = ref.object_id.to_bytes()

    _native._spill_object(oid_bytes)  # noqa: SLF001
    assert _native._is_spilled(oid_bytes) is True  # noqa: SLF001

    # Drop the last ref. The free-callback fires synchronously inside
    # `dec_local_ref`, so the spill record is gone by the time `del`
    # returns.
    del ref
    assert _native._is_spilled(oid_bytes) is False  # noqa: SLF001


# ── Phase 6.7: automatic spill-on-pressure ─────────────────────────────


def test_seal_above_threshold_triggers_automatic_spill(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A near-zero budget makes every `put` trip eviction.

    With `RAYD_SPILL_BUDGET_BYTES=1` and `RAYD_SPILL_THRESHOLD=1.0`,
    the worker's threshold is 1 byte — any plasma seal exceeds it
    and `maybe_spill_for_pressure` evicts the just-sealed object.
    The ref is still readable via `rayd.get` thanks to
    restore-on-local-Get.
    """
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    monkeypatch.setenv("RAYD_SPILL_BUDGET_BYTES", "1")
    monkeypatch.setenv("RAYD_SPILL_THRESHOLD", "1.0")
    rayd.init()
    try:
        payload = list(range(2_000))
        ref = rayd.put(payload)
        # Eviction ran inside `seal_value_to_plasma`; the just-put
        # object is now in spill, not in plasma.
        assert _native._is_spilled(ref.object_id.to_bytes()) is True  # noqa: SLF001
        # Restore-on-local-Get brings it back transparently.
        assert rayd.get(ref) == payload
    finally:
        rayd.shutdown()


def test_eviction_keeps_user_visible_refs_alive(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Multiple puts under aggressive eviction stay individually readable."""
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    monkeypatch.setenv("RAYD_SPILL_BUDGET_BYTES", "1")
    monkeypatch.setenv("RAYD_SPILL_THRESHOLD", "1.0")
    rayd.init()
    try:
        refs = [rayd.put(list(range(i, i + 500))) for i in range(0, 5_000, 500)]
        # Each should be readable, even though every seal triggered
        # eviction of any prior plasma resident.
        for i, ref in enumerate(refs):
            expected = list(range(i * 500, i * 500 + 500))
            assert rayd.get(ref) == expected
    finally:
        rayd.shutdown()


def test_default_policy_does_not_evict_under_normal_load(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without a tiny budget, normal-sized puts don't trigger eviction.

    Default `RAYD_SPILL_BUDGET_BYTES = 1 GiB` and threshold 0.75 mean
    the test's small payloads stay comfortably below threshold and
    the spill manager never sees them.
    """
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    rayd.init()
    try:
        ref = rayd.put(list(range(1_000)))
        assert _native._is_spilled(ref.object_id.to_bytes()) is False  # noqa: SLF001
    finally:
        rayd.shutdown()


@pytest.mark.usefixtures("_runtime_with_gcs")
def test_drop_unspilled_ref_is_a_no_op_on_spill_directory() -> None:
    """Free path's `forget` is idempotent — a never-spilled ref drops cleanly."""
    ref = rayd.put(list(range(1_000)))
    oid_bytes = ref.object_id.to_bytes()
    assert _native._is_spilled(oid_bytes) is False  # noqa: SLF001

    del ref
    # Still false; the absent entry stayed absent.
    assert _native._is_spilled(oid_bytes) is False  # noqa: SLF001


# ── failure modes ─────────────────────────────────────────────────────


def test_spill_object_without_gcs_errors() -> None:
    """No GCS attached → no recoverer registered → spill errors clearly."""
    rayd.init()
    try:
        ref = rayd.put(b"some data")
        with pytest.raises(RuntimeError, match="no recoverer"):
            _native._spill_object(ref.object_id.to_bytes())  # noqa: SLF001
    finally:
        rayd.shutdown()
