"""Phase 0 smoke tests: minimum viable surface area boots and round-trips."""

from __future__ import annotations

import pytest

import rayd


def test_init_shutdown_idempotent() -> None:
    assert rayd.is_initialized() is False
    rayd.init()
    assert rayd.is_initialized() is True
    rayd.shutdown()
    assert rayd.is_initialized() is False


def test_init_accepts_address_kwarg() -> None:
    rayd.init(address="rayd://placeholder:60123")
    try:
        assert rayd.is_initialized() is True
    finally:
        rayd.shutdown()


def test_object_id_random_round_trips_through_bytes() -> None:
    id1 = rayd.ObjectId.random()
    bytes_ = id1.to_bytes()
    assert len(bytes_) == 28
    id2 = rayd.ObjectId(bytes_)
    assert id1 == id2
    assert id1.hex == id2.hex
    assert hash(id1) == hash(id2)


def test_object_id_for_return_is_deterministic() -> None:
    task_bytes = bytes(range(24))
    a = rayd.ObjectId.for_return(task_bytes, 0)
    b = rayd.ObjectId.for_return(task_bytes, 0)
    assert a == b
    assert a.return_index == 0
    c = rayd.ObjectId.for_return(task_bytes, 1)
    assert c.return_index == 1
    assert a != c


def test_object_id_rejects_wrong_size_bytes() -> None:
    with pytest.raises(ValueError, match="ObjectId requires 28 bytes"):
        rayd.ObjectId(b"too short")


def test_address_roundtrip() -> None:
    worker_id = bytes(range(16))
    addr = rayd.Address("10.0.0.1", 60123, worker_id)
    assert addr.host == "10.0.0.1"
    assert addr.port == 60123
    assert addr.worker_id == worker_id
    assert addr.is_resolved() is True
    nil = rayd.Address.nil()
    assert nil.is_resolved() is False


def test_object_ref_carries_owner_and_state_is_pending() -> None:
    obj_id = rayd.ObjectId.random()
    addr = rayd.Address("h", 1, bytes(16))
    ref = rayd.ObjectRef(obj_id, addr)
    assert ref.object_id == obj_id
    assert ref.owner == addr
    assert ref.state() == rayd.RefState.Pending
    assert ref.peek_error() is None
    assert ref.is_ready() is False
    assert ref.is_failed() is False
    assert ref.hex == obj_id.hex


def test_ref_state_helpers() -> None:
    assert rayd.RefState.ReadyLocal.is_ready() is True
    assert rayd.RefState.Failed.is_ready() is True
    assert rayd.RefState.Failed.is_failed() is True
    assert rayd.RefState.Pending.is_ready() is False


def test_error_info_basic() -> None:
    info = rayd.ErrorInfo(
        rayd.ErrorCategory.TaskException,
        "boom",
        traceback="Traceback (most recent call last):\n  ...",
        raw_code=3,
    )
    assert info.category == rayd.ErrorCategory.TaskException
    assert info.message == "boom"
    assert info.traceback is not None
    assert info.raw_code == 3
