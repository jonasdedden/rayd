"""Per-actor worker subprocess entry point.

Spawned by the driver as `python -m rayd._actor_worker
--actor-socket=... --plasma-socket=...`. Connects to the actor UDS,
greets the driver with `actor_ready`, then loops:

  driver → worker:
    `actor_spawn`     { class_blob, args_blob, kwargs_blob }
    `actor_call`      { method, args_blob, kwargs_blob, result_oid }
    `actor_shutdown`

  worker → driver:
    `actor_ready`     { pid }
    `actor_call_complete` { result_oid, metadata, data_size }

Method results are sealed into shared plasma via the existing
`_native._worker_seal` shim; the driver records the matching
`PlasmaIndex` entry on its side via `_native._record_plasma_seal`
once it sees the completion frame.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import socket
import sys

import cloudpickle  # type: ignore[import-untyped]

import rayd
from rayd import _native
from rayd._worker import (
    _build_error_payload,
    _decode,
    _encode,
    _encode_error_metadata,
    _encode_pickle5_metadata,
    _recv_frame,
    _send_frame,
)


def _spawn_instance(message: dict[str, object]) -> object:
    cls_blob = message["class"]
    args_blob = message["args"]
    kwargs_blob = message.get("kwargs")
    if not isinstance(cls_blob, bytes) or not isinstance(args_blob, bytes):
        msg = "actor_spawn: class/args must be bytes"
        raise TypeError(msg)
    if kwargs_blob is not None and not isinstance(kwargs_blob, bytes):
        msg = "actor_spawn: kwargs must be bytes or None"
        raise TypeError(msg)
    cls = cloudpickle.loads(cls_blob)
    args = cloudpickle.loads(args_blob)
    kwargs = cloudpickle.loads(kwargs_blob) if kwargs_blob else {}
    return cls(*args, **kwargs)


def _execute_call(
    instance: object,
    message: dict[str, object],
) -> dict[str, object]:
    method_name = message["method"]
    args_blob = message["args"]
    kwargs_blob = message.get("kwargs")
    result_oid = message["result_oid"]
    if (
        not isinstance(method_name, str)
        or not isinstance(args_blob, bytes)
        or not isinstance(result_oid, bytes)
    ):
        msg = "actor_call: malformed message"
        raise TypeError(msg)
    if kwargs_blob is not None and not isinstance(kwargs_blob, bytes):
        msg = "actor_call: kwargs must be bytes or None"
        raise TypeError(msg)
    args = cloudpickle.loads(args_blob)
    kwargs = cloudpickle.loads(kwargs_blob) if kwargs_blob else {}

    try:
        method = getattr(instance, method_name)
        result = method(*args, **kwargs)
        payload = cloudpickle.dumps(result)
        meta = _encode_pickle5_metadata(has_nested_refs=False)
    except BaseException as exc:  # noqa: BLE001
        payload = _build_error_payload(exc)
        meta = _encode_error_metadata()

    _native._worker_seal(result_oid, meta, payload)  # noqa: SLF001
    return {
        "kind": "actor_call_complete",
        "result_oid": result_oid,
        "metadata": meta,
        "data_size": len(payload),
    }


def main(argv: list[str]) -> int:  # pragma: no cover  (subprocess entry)
    parser = argparse.ArgumentParser(prog="rayd._actor_worker")
    parser.add_argument("--actor-socket", required=True)
    parser.add_argument("--plasma-socket", required=True)
    args = parser.parse_args(argv[1:])

    # Same env-flags as `_worker.py`: connect to driver's plasma, no
    # nested dispatcher, no GCS registration (the driver owns all of
    # that on the actor's behalf), no OTLP exporter (the actor
    # subprocess has no tokio runtime; spans flow through driver-side
    # call sites instead).
    os.environ["RAYD_PLASMA_SOCKET"] = args.plasma_socket
    os.environ["RAYD_NO_DISPATCH"] = "1"
    os.environ.pop("RAYD_GCS_ADDRESS", None)
    os.environ.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
    rayd.init()

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(args.actor_socket)
    _send_frame(sock, _encode({"kind": "actor_ready", "pid": os.getpid()}))

    instance: object | None = None
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
            if kind == "actor_shutdown":
                break
            if kind == "actor_spawn":
                instance = _spawn_instance(message)
                continue
            if kind == "actor_call":
                if instance is None:
                    msg = "actor_call before actor_spawn"
                    raise RuntimeError(msg)
                completion = _execute_call(instance, message)
                _send_frame(sock, _encode(completion))
                continue
            sys.stderr.write(f"rayd._actor_worker: unknown kind {kind!r}\n")
    finally:
        with contextlib.suppress(OSError):
            sock.close()
        rayd.shutdown()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv))
