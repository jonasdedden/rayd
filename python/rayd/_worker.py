"""rayd worker subprocess entry point.

Spawned by the driver as `python -m rayd._worker --dispatch-socket=...
--plasma-socket=...`. Connects to the dispatch UDS, registers itself, then
loops on dispatched tasks. Results land in the shared plasma store; the
driver-side `Dispatcher` records the corresponding `PlasmaIndex` entries
when it sees the matching `task_complete` reply.

The wire protocol matches `crates/rayd-py/src/wire.rs`: length-prefixed
pickled `dict` frames, with task callable / args carried as cloudpickle
bytes.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import pickle
import socket
import struct
import sys
import traceback
import uuid
from typing import TYPE_CHECKING

import cloudpickle  # type: ignore[import-untyped]

import rayd

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Maximum frame size — must match `MAX_FRAME_BYTES` in wire.rs.
_MAX_FRAME_BYTES = 64 * 1024 * 1024


# ── Wire framing ──────────────────────────────────────────────────────


def _send_frame(sock: socket.socket, body: bytes) -> None:
    """Send `[u32 LE length][body]` over the dispatch socket."""
    if len(body) > _MAX_FRAME_BYTES:
        msg = f"dispatch frame too large: {len(body)} bytes"
        raise ValueError(msg)
    header = struct.pack("<I", len(body))
    sock.sendall(header + body)


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly `n` bytes; return None on graceful EOF."""
    chunks: list[bytes] = []
    remaining = n
    while remaining > 0:
        chunk = sock.recv(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _recv_frame(sock: socket.socket) -> bytes | None:
    """Read a length-prefixed frame body. Returns None on EOF (driver gone)."""
    header = _recv_exact(sock, 4)
    if header is None:
        return None
    (length,) = struct.unpack("<I", header)
    if length > _MAX_FRAME_BYTES:
        msg = f"dispatch frame too large: {length} bytes"
        raise ValueError(msg)
    body = _recv_exact(sock, length)
    if body is None:
        msg = "EOF mid-body during recv"
        raise ConnectionError(msg)
    return body


def _encode(message: Mapping[str, object]) -> bytes:
    return pickle.dumps(message, protocol=5)


def _decode(frame: bytes) -> dict[str, object]:
    obj = pickle.loads(frame)  # noqa: S301  (frames come from our own driver)
    if not isinstance(obj, dict):
        msg = f"dispatch frame not a dict: {type(obj).__name__}"
        raise TypeError(msg)
    return obj


# ── Task execution ────────────────────────────────────────────────────


def _build_error_payload(exc: BaseException) -> bytes:
    """Encode an `ErrorPayload` for an exception raised by a user task.

    Matches the wire format expected by `rayd_core::ErrorPayload::decode`:

        [u32 LE: msg_len][msg bytes]
        [u8 flag: traceback present]
          [u32 LE: tb_len][tb bytes]
        [u16 LE: raw_code]
        [u8 flag: pickled exception present]
          [u32 LE: pickled_len][pickled bytes]
    """
    message = repr(exc).encode("utf-8")
    tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)).encode("utf-8")
    try:
        pickled = cloudpickle.dumps(exc)
    except Exception:  # noqa: BLE001
        pickled = None

    parts: list[bytes] = []
    parts.append(struct.pack("<I", len(message)))
    parts.append(message)
    parts.append(struct.pack("<B", 1))
    parts.append(struct.pack("<I", len(tb_str)))
    parts.append(tb_str)
    parts.append(struct.pack("<H", 0))  # raw_code unspecified
    if pickled is None:
        parts.append(struct.pack("<B", 0))
    else:
        parts.append(struct.pack("<B", 1))
        parts.append(struct.pack("<I", len(pickled)))
        parts.append(pickled)
    return b"".join(parts)


# Metadata wire constants — must match `rayd_core::metadata`.
_META_DISCRIMINATOR_PICKLE5 = 1
_META_DISCRIMINATOR_ERROR = 16
_ERROR_CATEGORY_TASK_EXCEPTION = 1


def _encode_pickle5_metadata(*, has_nested_refs: bool) -> bytes:
    return struct.pack("<BB", _META_DISCRIMINATOR_PICKLE5, 1 if has_nested_refs else 0)


def _encode_error_metadata(
    category: int = _ERROR_CATEGORY_TASK_EXCEPTION,
    raw_code: int = 0,
) -> bytes:
    return struct.pack("<BBH", _META_DISCRIMINATOR_ERROR, category, raw_code)


def _store_via_native_seal(object_id_bytes: bytes, metadata: bytes, data: bytes) -> int:
    """Write a result directly into the shared plasma store.

    Routes through the worker's own `CoreWorker.seal_value_to_plasma` via
    the `_native._worker_seal` shim. Returns the bytes written
    (== `len(data)`); the driver records the matching `PlasmaIndex` when it
    sees the corresponding `task_complete` reply.
    """
    rayd._native._worker_seal(object_id_bytes, metadata, data)  # noqa: SLF001
    return len(data)


def _spread_error(
    object_ids: Sequence[bytes],
    exc: BaseException,
    returns: list[dict[str, object]],
) -> None:
    """Record `exc` as the result of every id in `object_ids`."""
    err_data = _build_error_payload(exc)
    err_meta = _encode_error_metadata()
    for oid in object_ids:
        size = _store_via_native_seal(oid, err_meta, err_data)
        returns.append({"object_id": oid, "metadata": err_meta, "data_size": size})


def _execute_task(message: Mapping[str, object]) -> dict[str, object]:
    """Run a `dispatch_task` message; return the matching `task_complete`."""
    task_id = message["task_id"]
    if not isinstance(task_id, bytes):
        msg = "task_id must be bytes"
        raise TypeError(msg)
    num_returns_obj = message["num_returns"]
    if not isinstance(num_returns_obj, int):
        msg = "num_returns must be int"
        raise TypeError(msg)
    num_returns = num_returns_obj
    callable_blob = message["callable"]
    args_blob = message["args"]
    kwargs_blob = message.get("kwargs")
    if not isinstance(callable_blob, bytes) or not isinstance(args_blob, bytes):
        msg = "dispatch_task callable/args must be bytes"
        raise TypeError(msg)
    if kwargs_blob is not None and not isinstance(kwargs_blob, bytes):
        msg = "dispatch_task kwargs must be bytes or None"
        raise TypeError(msg)

    object_ids: list[bytes] = []
    for i in range(num_returns):
        # ObjectId layout: [24-byte task id][4-byte LE return index]
        oid = task_id + struct.pack("<I", i)
        object_ids.append(oid)

    fn = cloudpickle.loads(callable_blob)
    args = cloudpickle.loads(args_blob)
    kwargs = cloudpickle.loads(kwargs_blob) if kwargs_blob else {}

    returns: list[dict[str, object]] = []
    try:
        result = fn(*args, **kwargs)
    except BaseException as exc:  # noqa: BLE001
        _spread_error(object_ids, exc, returns)
        return {"kind": "task_complete", "task_id": task_id, "returns": returns}

    if num_returns == 1:
        payload = cloudpickle.dumps(result)
        meta = _encode_pickle5_metadata(has_nested_refs=False)
        size = _store_via_native_seal(object_ids[0], meta, payload)
        returns.append({"object_id": object_ids[0], "metadata": meta, "data_size": size})
        return {"kind": "task_complete", "task_id": task_id, "returns": returns}

    if not isinstance(result, tuple) or len(result) != num_returns:
        type_err = TypeError(
            f"task with num_returns={num_returns} must return a tuple "
            f"of that length, got {type(result).__name__}"
        )
        _spread_error(object_ids, type_err, returns)
        return {"kind": "task_complete", "task_id": task_id, "returns": returns}

    for oid, item in zip(object_ids, result, strict=True):
        payload = cloudpickle.dumps(item)
        meta = _encode_pickle5_metadata(has_nested_refs=False)
        size = _store_via_native_seal(oid, meta, payload)
        returns.append({"object_id": oid, "metadata": meta, "data_size": size})

    return {"kind": "task_complete", "task_id": task_id, "returns": returns}


# ── Main loop ─────────────────────────────────────────────────────────


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="rayd._worker")
    parser.add_argument("--dispatch-socket", required=True)
    parser.add_argument("--plasma-socket", required=True)
    args = parser.parse_args(argv[1:])

    # The worker connects to the SAME plasma server the driver uses, and
    # MUST NOT recursively spawn its own dispatcher OR register itself
    # with the GCS as a separate node/job — the driver did that on behalf
    # of the whole worker pool.
    os.environ["RAYD_PLASMA_SOCKET"] = args.plasma_socket
    os.environ["RAYD_NO_DISPATCH"] = "1"
    os.environ.pop("RAYD_GCS_ADDRESS", None)
    # Worker subprocesses don't run their own OTLP exporter; spans
    # they emit (e.g. via plasma seal calls) flow through the driver's
    # subscriber via the existing tracing call sites the driver wraps.
    # Clearing the env stops the subscriber from logging a spurious
    # "no runtime, OTLP disabled" warning on every worker spawn.
    os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
    rayd.init()

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.dispatch_socket)

    # Greet the dispatcher with a stable random worker_id.
    worker_id = uuid.uuid4().bytes  # 16 bytes
    _send_frame(sock, _encode({"kind": "worker_ready", "worker_id": worker_id, "pid": os.getpid()}))

    try:
        while True:
            try:
                frame = _recv_frame(sock)
            except (ConnectionError, OSError):
                break
            if frame is None:
                break
            message = _decode(frame)
            kind = message.get("kind")
            if kind == "shutdown":
                break
            if kind == "dispatch_task":
                completion = _execute_task(message)
                _send_frame(sock, _encode(completion))
            else:
                # Unknown message — best effort: log and ignore.
                sys.stderr.write(f"rayd._worker: unknown message kind {kind!r}\n")
    finally:
        with contextlib.suppress(OSError):
            sock.close()
        rayd.shutdown()

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
