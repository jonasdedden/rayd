"""Phase 3.3b tests: driver registration against a real `rayd gcs` server.

Each test stands up the gcs subprocess on a free port, points
`RAYD_GCS_ADDRESS` at it, drives `rayd.init()`, and asserts on the
`NodeInfo`/`JobInfo` snapshots the GCS reports back. After `rayd.shutdown()`,
the GCS should observe the node as `Draining` and the job as `Finished`.
"""

from __future__ import annotations

import contextlib
import os
import pickle
import re
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import rayd
from rayd import _native

if TYPE_CHECKING:
    from collections.abc import Generator


_LISTEN_RE = re.compile(r"listening on (\S+)")
_BIND_TIMEOUT_S = 5.0


def _rayd_cli() -> Path:
    target = Path(__file__).resolve().parents[3] / "target" / "debug" / "rayd"
    if not target.exists():
        pytest.skip(f"rayd-cli binary not built at {target}; run `cargo build -p rayd-cli`")
    return target


def _free_port() -> int:
    """Ask the kernel for a free TCP port. Race-prone but good enough for tests."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.contextmanager
def _spawn_gcs(*extra_args: str) -> Generator[str]:
    """Spawn `rayd gcs --bind 127.0.0.1:<port> <extra_args...>` and yield `host:port`."""
    cli = _rayd_cli()
    port = _free_port()
    addr = f"127.0.0.1:{port}"
    with subprocess.Popen(  # noqa: S603
        [str(cli), "gcs", "--bind", addr, *extra_args],
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
    """Spawn `rayd gcs` with default heartbeat timeout (10 s)."""
    with _spawn_gcs() as addr:
        yield addr


def test_listen_regex_matches_real_output() -> None:
    """Sanity check that the bind-line regex parses the CLI's output.

    The fixture above probes via TCP rather than parsing stderr, but the
    regex is still kept in sync with the CLI's user-visible format.
    """
    sample = "rayd gcs: NodeRegistry listening on 127.0.0.1:60123 (ctrl-c to stop)"
    m = _LISTEN_RE.search(sample)
    assert m is not None
    assert m.group(1) == "127.0.0.1:60123"


# ── happy path ─────────────────────────────────────────────────────────


def test_driver_registers_node_and_job(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)
    rayd.init()
    try:
        nid = _native.node_id()
        jid = _native.job_id()
        sid = _native.cluster_session_id()
        assert nid is not None
        assert jid is not None
        assert sid is not None
        assert len(nid) == 16
        assert len(jid) == 16
        assert len(sid) == 16
        nodes = _native.list_nodes()
        assert len(nodes) == 1
        node = nodes[0]
        assert isinstance(node, _native.NodeInfo)
        assert node.node_id == nid
        assert node.status == "alive"

        jobs = _native.list_jobs()
        assert len(jobs) == 1
        job = jobs[0]
        assert isinstance(job, _native.JobInfo)
        assert job.job_id == jid
        assert job.driver_pid == os.getpid()
        assert job.node_id == nid
        assert job.status == "running"
        assert job.finished_at_unix_ms == 0
    finally:
        rayd.shutdown()


def test_shutdown_drains_node_and_finishes_job(
    gcs_server: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify post-shutdown state of a previous driver via a fresh driver.

    After `rayd.shutdown()`, a second driver attaching to the same GCS
    should see the previous node as `draining` and the previous job as
    `finished`.
    """
    monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_server)

    rayd.init()
    first_nid = _native.node_id()
    first_jid = _native.job_id()
    rayd.shutdown()

    # Bring up a second driver under the same GCS and confirm it observes the
    # first one as drained/finished.
    rayd.init()
    try:
        nodes_by_id: dict[bytes, _native.NodeInfo] = {}
        for raw in _native.list_nodes():
            assert isinstance(raw, _native.NodeInfo)
            nodes_by_id[bytes(raw.node_id)] = raw
        jobs_by_id: dict[bytes, _native.JobInfo] = {}
        for raw in _native.list_jobs():
            assert isinstance(raw, _native.JobInfo)
            jobs_by_id[bytes(raw.job_id)] = raw
        assert first_nid is not None
        assert first_jid is not None
        first = nodes_by_id.get(bytes(first_nid))
        assert first is not None, "first driver's node missing from GCS"
        assert first.status == "draining"
        prev_job = jobs_by_id.get(bytes(first_jid))
        assert prev_job is not None, "first driver's job missing from GCS"
        assert prev_job.status == "finished"
        assert prev_job.finished_at_unix_ms > 0
    finally:
        rayd.shutdown()


# ── failure modes ─────────────────────────────────────────────────────


def test_init_fails_when_gcs_address_is_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Use the /dev/null of TCP: a port nothing is listening on. _free_port
    # picks a port and lets it close, so we just reuse one that won't be
    # bound by the gcs subprocess.
    port = _free_port()
    monkeypatch.setenv("RAYD_GCS_ADDRESS", f"127.0.0.1:{port}")
    with pytest.raises(RuntimeError, match="RAYD_GCS_ADDRESS"):
        rayd.init()
    assert rayd.is_initialized() is False


def test_init_fails_on_malformed_gcs_address(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAYD_GCS_ADDRESS", "not-a-socket-addr")
    with pytest.raises(RuntimeError, match="RAYD_GCS_ADDRESS"):
        rayd.init()
    assert rayd.is_initialized() is False


def test_gcs_accessors_return_none_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Confirm accessors degrade gracefully when no GCS is configured.

    When `RAYD_GCS_ADDRESS` is unset, init succeeds but the GCS-binding
    accessors all report `None` and `list_*` raise.
    """
    monkeypatch.delenv("RAYD_GCS_ADDRESS", raising=False)
    rayd.init()
    try:
        assert _native.node_id() is None
        assert _native.job_id() is None
        assert _native.cluster_session_id() is None
        with pytest.raises(RuntimeError, match="no GCS connection"):
            _native.list_nodes()
        with pytest.raises(RuntimeError, match="no GCS connection"):
            _native.list_jobs()
    finally:
        rayd.shutdown()


# ── heartbeats ────────────────────────────────────────────────────────


def _node_status(nid: bytes) -> str | None:
    for n in _native.list_nodes():
        assert isinstance(n, _native.NodeInfo)
        if bytes(n.node_id) == nid:
            return n.status
    return None


def test_driver_heartbeat_keeps_node_alive(monkeypatch: pytest.MonkeyPatch) -> None:
    """Driver heartbeats keep the node `alive` across multiple sweeper cycles.

    With the GCS sweeper expiring nodes after 600 ms and the driver
    heartbeating every 100 ms, the node should stay `alive` even after
    several sweep ticks would have flipped a non-heartbeating node.
    """
    # Sweeper expires after 600 ms; driver heartbeats every 100 ms.
    with _spawn_gcs("--heartbeat-timeout-ms", "600") as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        monkeypatch.setenv("RAYD_HEARTBEAT_INTERVAL_MS", "100")
        rayd.init()
        try:
            nid = _native.node_id()
            assert nid is not None
            # Sleep through ~3x the timeout. Heartbeats must keep us alive.
            time.sleep(2.0)
            status = _node_status(bytes(nid))
            assert status == "alive", f"expected alive, got {status!r}"
        finally:
            rayd.shutdown()


# ── multi-node (head + raylet) ────────────────────────────────────────


@contextlib.contextmanager
def _spawn_raylet(gcs_addr: str) -> Generator[None]:
    """Spawn `rayd start --address=<gcs_addr>` and wait for it to register."""
    cli = _rayd_cli()
    tmp = tempfile.TemporaryDirectory(prefix="rayd-raylet-test-")
    plasma_socket = Path(tmp.name) / "plasma.sock"
    with (
        tmp,
        subprocess.Popen(  # noqa: S603
            [
                str(cli),
                "start",
                f"--address={gcs_addr}",
                "--raylet-bind",
                "127.0.0.1:0",
                "--advertise-host",
                "127.0.0.1",
                "--plasma-socket",
                str(plasma_socket),
                "--plasma-capacity-mb",
                "16",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ) as proc,
    ):
        try:
            # No external readiness signal yet; sleep a beat for register.
            time.sleep(0.5)
            if proc.poll() is not None:
                msg = f"rayd start --address exited (rc={proc.returncode})"
                raise RuntimeError(msg)
            yield
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3.0)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def test_driver_sees_raylet_in_node_list(monkeypatch: pytest.MonkeyPatch) -> None:
    """A driver attached to a head GCS sees both itself and a raylet.

    Spins up a `rayd gcs` head and a `rayd start --address=<head>`
    raylet in separate processes, then verifies that a driver attaching
    to the same head sees both nodes in `list_nodes()`, all `alive`.
    """
    with _spawn_gcs() as addr, _spawn_raylet(addr):
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            driver_nid = _native.node_id()
            assert driver_nid is not None
            nodes: list[_native.NodeInfo] = []
            for n in _native.list_nodes():
                assert isinstance(n, _native.NodeInfo)
                nodes.append(n)
            # Should include at least the driver and the raylet.
            assert len(nodes) >= 2
            ids = {bytes(n.node_id) for n in nodes}
            assert bytes(driver_nid) in ids
            # Driver + raylet both heartbeat, so every entry is alive.
            for n in nodes:
                assert n.status == "alive", f"node {n!r} not alive"
        finally:
            rayd.shutdown()


# ── 3.4c: local-raylet Pull through PyO3 ──────────────────────────────


def test_driver_node_info_carries_raylet_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """The driver's `NodeInfo.port` is the local raylet's bound TCP port.

    Phase 3.4c starts a per-driver `Raylet` inside `rayd.init()`. The
    `NodeInfo` advertised to peers must therefore have a non-zero port.
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            local = _native.local_raylet_address()
            assert local is not None
            _, port = local
            assert port > 0

            nid = _native.node_id()
            assert nid is not None

            entry: _native.NodeInfo | None = None
            for n in _native.list_nodes():
                assert isinstance(n, _native.NodeInfo)
                if bytes(n.node_id) == bytes(nid):
                    entry = n
                    break
            assert entry is not None
            assert entry.port == port
        finally:
            rayd.shutdown()


def test_pull_object_round_trips_via_local_raylet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pull a put-into-plasma object back through the local raylet's gRPC.

    `rayd.put` only goes to plasma for payloads above the 100 KiB inline
    threshold; we use 200 KiB to make sure this exercises the gRPC +
    plasma path, not the in-process MemoryStore.
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            payload = b"\xab" * 200_000  # > 100 KiB inline threshold
            ref = rayd.put(payload)
            oid = ref.object_id.to_bytes()
            nid = _native.node_id()
            assert nid is not None

            _native.register_object(oid, nid)
            locs = _native.get_object_locations(oid)
            assert any(isinstance(item, bytes) and item == bytes(nid) for item in locs)

            local = _native.local_raylet_address()
            assert local is not None
            host, raylet_port = local
            metadata, data = _native.pull_object(host, raylet_port, oid)

            # The bytes we pulled are the pickled form rayd uses for
            # plasma storage; unpickling recovers the original payload.
            assert pickle.loads(data) == payload  # noqa: S301
            # Raylet metadata travels alongside; rayd encodes a 2-byte
            # tag, but the contents are an internal detail.
            assert isinstance(metadata, bytes)
        finally:
            rayd.shutdown()


def test_fetch_object_self_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    """Single-process sanity: a driver can `fetch_object` its own put.

    `put` registers itself as a holder; `fetch_object` then locates,
    pulls, and seals. Sealing is idempotent (AlreadyExists is treated
    as success), so this exercises every step except the across-the-
    network half — that's the cross-process test below.
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            payload = b"\xcd" * 50
            ref = rayd.put(payload)  # auto-registers at local raylet
            oid = ref.object_id.to_bytes()
            nid = _native.node_id()
            assert nid is not None

            _native.fetch_object(oid, nid)

            # Local plasma already had it; fetch is a no-op success.
            # Re-pull to confirm.
            local = _native.local_raylet_address()
            assert local is not None
            host, port = local
            _, data = _native.pull_object(host, port, oid)
            assert pickle.loads(data) == payload  # noqa: S301
        finally:
            rayd.shutdown()


def test_fetch_object_pulls_from_peer_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two driver processes share one GCS; B fetches what A put.

    Driver A puts a payload, prints `(object_id_hex, owner_node_id_hex)`,
    then waits on stdin. Driver B reads those, calls `fetch_object`,
    pulls the bytes through A's raylet, and verifies. Then B signals
    A to exit.
    """
    sentinel_payload = bytes(range(256)) * 1024  # 256 KiB, > inline threshold
    with _spawn_gcs() as gcs_addr:
        producer_code = (
            "import os, sys, rayd\n"
            "from rayd import _native\n"
            "rayd.init()\n"
            "ref = rayd.put(bytes(range(256)) * 1024)\n"
            "nid = _native.node_id()\n"
            "assert nid is not None\n"
            "print(ref.object_id.to_bytes().hex(), nid.hex(), flush=True)\n"
            "sys.stdin.readline()\n"  # wait for consumer to signal done
            "rayd.shutdown()\n"
        )
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        # Strip any per-test heartbeat-interval override from the
        # outer test process; producer needs the production default.
        env.pop("RAYD_HEARTBEAT_INTERVAL_MS", None)

        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", producer_code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as producer:
            try:
                assert producer.stdout is not None
                assert producer.stdin is not None
                line = producer.stdout.readline()
                if not line:
                    err = producer.stderr.read() if producer.stderr else ""
                    msg = f"producer exited before printing ids: {err}"
                    raise RuntimeError(msg)
                oid_hex, owner_nid_hex = line.strip().split()
                oid = bytes.fromhex(oid_hex)
                owner_nid = bytes.fromhex(owner_nid_hex)

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    _native.fetch_object(oid, owner_nid)
                    # The bytes are now in the consumer's local plasma.
                    # Pull from its own raylet to verify.
                    local = _native.local_raylet_address()
                    assert local is not None
                    host, port = local
                    _, data = _native.pull_object(host, port, oid)
                    assert pickle.loads(data) == sentinel_payload  # noqa: S301
                finally:
                    rayd.shutdown()
            finally:
                # Tell producer to exit cleanly.
                if producer.stdin is not None and not producer.stdin.closed:
                    try:
                        producer.stdin.write("done\n")
                        producer.stdin.flush()
                        producer.stdin.close()
                    except (BrokenPipeError, OSError):
                        pass
                try:
                    producer.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()


def test_remote_ref_reports_ready_remote_then_ready_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ref pickled from another driver reports `ReadyRemote` until it's fetched.

    Constructed manually — we just need a ref whose `owner_node_id`
    differs from this driver's node id. Without any RPC, the local
    driver should already be able to surface this state from the ref's
    embedded metadata. After `_native.fetch_object` would normally
    fire, the seal would flip the state to `ReadyLocal` (covered
    elsewhere — here we focus on the pre-fetch state).
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            local_nid = _native.node_id()
            assert local_nid is not None
            # Forge a ref claiming a different owner node.
            other_nid = bytes(b ^ 0xFF for b in local_nid)
            oid = _native.ObjectId.random()
            owner_addr = _native.Address("peer.example", 60100, b"\x00" * 16)
            ref = _native.ObjectRef(oid, owner_addr, other_nid)

            # State should be ReadyRemote — we've never seen the bytes
            # locally, but the ref tells us where to look.
            assert ref.state() == _native.RefState.ReadyRemote
            # `is_ready()` is True (ReadyRemote counts as "resolvable
            # without blocking on a producer"); only ReadyLocal means
            # the bytes are sitting in our own plasma.
            assert ref.is_ready()
            assert ref.state() != _native.RefState.ReadyLocal
        finally:
            rayd.shutdown()


def test_local_unfetched_ref_stays_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ref claiming OUR node as owner stays Pending until put-resolved."""
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            local_nid = _native.node_id()
            assert local_nid is not None
            oid = _native.ObjectId.random()
            owner_addr = _native.Address("peer.example", 60100, b"\x00" * 16)
            ref = _native.ObjectRef(oid, owner_addr, local_nid)

            # owner_node_id == local — no remote source, no local data.
            assert ref.state() == _native.RefState.Pending
        finally:
            rayd.shutdown()


def test_remote_ref_state_flips_to_ready_local_after_get(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cross-process: state goes ReadyRemote → ReadyLocal once `rayd.get` fetches."""
    expected_payload = bytes(range(256)) * 1024  # 256 KiB
    with _spawn_gcs() as gcs_addr:
        producer_code = (
            "import pickle, sys, rayd\n"
            "rayd.init()\n"
            "ref = rayd.put(bytes(range(256)) * 1024)\n"
            "sys.stdout.write(pickle.dumps(ref).hex() + '\\n')\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()\n"
            "rayd.shutdown()\n"
        )
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        env.pop("RAYD_HEARTBEAT_INTERVAL_MS", None)
        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", producer_code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as producer:
            try:
                assert producer.stdout is not None
                assert producer.stdin is not None
                line = producer.stdout.readline()
                if not line:
                    err = producer.stderr.read() if producer.stderr else ""
                    msg = f"producer exited: {err}"
                    raise RuntimeError(msg)
                ref = pickle.loads(bytes.fromhex(line.strip()))  # noqa: S301
                assert isinstance(ref, _native.ObjectRef)

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    assert ref.state() == _native.RefState.ReadyRemote
                    value = rayd.get(ref)
                    assert value == expected_payload
                    assert ref.state() == _native.RefState.ReadyLocal
                finally:
                    rayd.shutdown()
            finally:
                if producer.stdin is not None and not producer.stdin.closed:
                    try:
                        producer.stdin.write("done\n")
                        producer.stdin.flush()
                        producer.stdin.close()
                    except (BrokenPipeError, OSError):
                        pass
                try:
                    producer.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()


def test_object_ref_round_trips_through_pickle() -> None:
    """`pickle.dumps(ref)` + `pickle.loads(...)` preserves identity.

    Phase 3.4d adds `__reduce__` to `ObjectId`, `Address`, and
    `ObjectRef` so a peer can ship a ref through the standard pickle
    protocol (e.g. over a multiprocessing queue) without a custom
    serializer.
    """
    oid = _native.ObjectId.random()
    addr = _native.Address("peer.example", 60001, b"\x33" * 16)
    nid = b"\x44" * 16
    ref = _native.ObjectRef(oid, addr, nid)

    rehydrated = pickle.loads(pickle.dumps(ref))  # noqa: S301
    assert rehydrated == ref
    assert rehydrated.object_id == oid
    assert rehydrated.owner_node_id == nid


def test_rayd_get_auto_fetches_remote_ref_through_pickle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`rayd.get(remote_ref)` transparently pulls cross-process bytes.

    Producer process A puts a payload, prints the pickled `ObjectRef`
    as hex on stdout. Consumer (the test) unpickles, calls `rayd.get`,
    and the auto-fetch path locates → pulls → seals → registers, all
    without an explicit `_native.fetch_object` call.
    """
    expected_payload = bytes(range(256)) * 1024  # 256 KiB
    with _spawn_gcs() as gcs_addr:
        producer_code = (
            "import os, pickle, sys, rayd\n"
            "rayd.init()\n"
            "ref = rayd.put(bytes(range(256)) * 1024)\n"
            "blob = pickle.dumps(ref)\n"
            "sys.stdout.write(blob.hex() + '\\n')\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()\n"  # wait for consumer to finish
            "rayd.shutdown()\n"
        )
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        env.pop("RAYD_HEARTBEAT_INTERVAL_MS", None)

        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", producer_code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as producer:
            try:
                assert producer.stdout is not None
                assert producer.stdin is not None
                line = producer.stdout.readline()
                if not line:
                    err = producer.stderr.read() if producer.stderr else ""
                    msg = f"producer exited before printing: {err}"
                    raise RuntimeError(msg)
                ref_blob = bytes.fromhex(line.strip())
                ref = pickle.loads(ref_blob)  # noqa: S301
                assert isinstance(ref, _native.ObjectRef)
                assert ref.owner_node_id is not None

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    # No manual fetch_object! `rayd.get` does the dance
                    # because the ref's owner_node_id != our node_id.
                    value = rayd.get(ref)
                    assert value == expected_payload
                finally:
                    rayd.shutdown()
            finally:
                if producer.stdin is not None and not producer.stdin.closed:
                    try:
                        producer.stdin.write("done\n")
                        producer.stdin.flush()
                        producer.stdin.close()
                    except (BrokenPipeError, OSError):
                        pass
                try:
                    producer.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()


def test_push_object_round_trips_via_local_raylet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_native.push_object` writes bytes; `_native.pull_object` reads them back.

    The Push wire was already covered at the tonic level in the Rust
    crate tests. This is the Python-surface check: same process, same
    raylet, push then pull, bytes equal.
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            local = _native.local_raylet_address()
            assert local is not None
            host, port = local
            payload = b"\x99" * 50_000
            metadata = b"meta-bytes"
            object_id = _native.ObjectId.random().to_bytes()

            _native.push_object(host, port, object_id, metadata, payload)
            got_meta, got_data = _native.pull_object(host, port, object_id)
            assert got_meta == metadata
            assert got_data == payload

            # Idempotency: a second push of the same id is a no-op
            # success (plasma's AlreadyExists is treated as fine).
            _native.push_object(host, port, object_id, metadata, payload)
        finally:
            rayd.shutdown()


def test_owner_self_deregisters_on_local_free(monkeypatch: pytest.MonkeyPatch) -> None:
    """Phase 4.3.3a: dropping the owner's local ref clears its directory entry.

    `rayd.put` registers the owner as a holder. Dropping the last
    `ObjectRef` calls `dec_local_ref` → `free_unpinned` → the
    free-callback fires `RayletHandle::deregister_self`, so peers
    stop seeing this driver as a holder.
    """
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            ref = rayd.put(b"\x99" * 200_000)
            oid = ref.object_id.to_bytes()
            nid = _native.node_id()
            assert nid is not None

            # Right after put: directory should list us.
            before = _native.get_object_locations(oid)
            assert any(isinstance(x, bytes) and x == bytes(nid) for x in before)

            del ref

            # Free callback should have cleared the self-entry.
            after = _native.get_object_locations(oid)
            assert after == []
        finally:
            rayd.shutdown()


def test_get_raises_owner_died_when_owner_has_drained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Phase 4.3.3b: rayd.get on a remote ref whose owner is gone fails fast.

    Producer A puts an object, prints the pickled ref, then exits
    cleanly. `rayd.shutdown` drains its node in the GCS so its
    status flips to `draining`. The consumer's `rayd.get` checks
    owner liveness via `list_nodes` before fetching and raises
    `OwnerDiedError` instead of dispatching a doomed Pull.
    """
    with _spawn_gcs() as gcs_addr:
        producer_code = (
            "import os, pickle, sys, rayd\n"
            "rayd.init()\n"
            "ref = rayd.put(bytes(range(256)) * 1024)\n"
            "sys.stdout.write(pickle.dumps(ref).hex() + '\\n')\n"
            "sys.stdout.flush()\n"
            # Wait for consumer to read the blob so we don't race
            # the GCS sweeper before they have the ref.
            "sys.stdin.readline()\n"
            "rayd.shutdown()\n"
        )
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        env.pop("RAYD_HEARTBEAT_INTERVAL_MS", None)
        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", producer_code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as producer:
            try:
                assert producer.stdout is not None
                assert producer.stdin is not None
                ref = pickle.loads(  # noqa: S301
                    bytes.fromhex(producer.stdout.readline().strip())
                )
                assert isinstance(ref, _native.ObjectRef)

                # Tell producer to drain + exit.
                producer.stdin.write("ack\n")
                producer.stdin.flush()
                producer.wait(timeout=5.0)

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    # Wait until the GCS reflects the producer as
                    # not-alive. (Drain RPC is synchronous — should
                    # be visible immediately, but allow a tick.)
                    deadline = time.monotonic() + 3.0
                    owner_status: str | None = None
                    while time.monotonic() < deadline:
                        nodes = list(_native.list_nodes())
                        for n in nodes:
                            assert isinstance(n, _native.NodeInfo)
                            assert ref.owner_node_id is not None
                            if bytes(n.node_id) == ref.owner_node_id:
                                owner_status = n.status
                                break
                        if owner_status != "alive":
                            break
                        time.sleep(0.05)
                    assert owner_status != "alive", (
                        f"producer's owner status didn't degrade: {owner_status}"
                    )

                    # rayd.get should now raise OwnerDiedError, not
                    # a transport timeout from a doomed Pull.
                    with pytest.raises(rayd.OwnerDiedError, match="owner of ObjectRef"):
                        rayd.get(ref)
                finally:
                    rayd.shutdown()
            finally:
                if producer.stdin is not None and not producer.stdin.closed:
                    with contextlib.suppress(OSError):
                        producer.stdin.close()
                if producer.poll() is None:
                    producer.kill()
                    producer.wait()


def test_borrower_drop_notifies_owner_and_frees_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end Phase 4.3.2 + 4.3.3a: peer drop frees the owner's plasma.

    Producer A puts an object then drops its own ref (so the owner's
    local count clears but the borrower keeps the object pinned).
    Consumer B fetches, then drops. Once B's `WaitForRefRemoved`
    arrives at A, the borrower set empties → A's `free_unpinned`
    fires → plasma + the directory entry are gone.
    """
    expected_payload = bytes(range(256)) * 1024  # 256 KiB
    with _spawn_gcs() as gcs_addr:
        producer_code = (
            "import os, pickle, sys, rayd, time\n"
            "from rayd import _native\n"
            "rayd.init()\n"
            "ref = rayd.put(bytes(range(256)) * 1024)\n"
            "oid = ref.object_id.to_bytes()\n"
            "blob = pickle.dumps(ref)\n"
            "sys.stdout.write(blob.hex() + '\\n')\n"
            "sys.stdout.flush()\n"
            # Wait for consumer to fetch.
            "sys.stdin.readline()\n"
            # Owner drops local ref. Object stays pinned by borrower.
            "del ref\n"
            "sys.stdout.write('owner-dropped\\n')\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()\n"
            # Borrower has now dropped too. Poll until the directory
            # entry is fully gone (our self-entry was already cleared
            # when we dropped owner-side, the borrower's WaitForRefRemoved
            # clears their entry → both gone → empty list).
            "deadline = time.monotonic() + 3.0\n"
            "while time.monotonic() < deadline:\n"
            "    locs = _native.get_object_locations(oid)\n"
            "    if not locs:\n"
            "        break\n"
            "    time.sleep(0.05)\n"
            "final = _native.get_object_locations(oid)\n"
            "sys.stdout.write(f'final_count={len(final)}\\n')\n"
            "sys.stdout.flush()\n"
            "rayd.shutdown()\n"
        )
        env = {**os.environ, "RAYD_GCS_ADDRESS": gcs_addr}
        env.pop("RAYD_HEARTBEAT_INTERVAL_MS", None)

        with subprocess.Popen(  # noqa: S603
            [sys.executable, "-c", producer_code],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        ) as producer:
            try:
                assert producer.stdout is not None
                assert producer.stdin is not None
                ref_blob = bytes.fromhex(producer.stdout.readline().strip())
                ref = pickle.loads(ref_blob)  # noqa: S301
                assert isinstance(ref, _native.ObjectRef)

                monkeypatch.setenv("RAYD_GCS_ADDRESS", gcs_addr)
                rayd.init()
                try:
                    # Fetch (registers consumer as a borrower at owner).
                    value = rayd.get(ref)
                    assert value == expected_payload

                    # Tell producer to drop its local ref.
                    producer.stdin.write("consumer-fetched\n")
                    producer.stdin.flush()
                    line = producer.stdout.readline().strip()
                    assert line == "owner-dropped"

                    # Now drop the consumer's borrower ref → fires
                    # WaitForRefRemoved at the owner.
                    del ref

                    # Tell producer to poll the directory.
                    producer.stdin.write("consumer-dropped\n")
                    producer.stdin.flush()
                    final = producer.stdout.readline().strip()
                    # Owner self-deregistered when it dropped, then
                    # the consumer's drop cleared the borrower side.
                    # Directory should be fully empty.
                    assert final == "final_count=0", f"expected directory cleared, got {final!r}"
                finally:
                    rayd.shutdown()
            finally:
                if producer.stdin is not None and not producer.stdin.closed:
                    with contextlib.suppress(OSError):
                        producer.stdin.close()
                try:
                    producer.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    producer.kill()
                    producer.wait()


def test_pull_object_unknown_id_returns_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _spawn_gcs() as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        rayd.init()
        try:
            local = _native.local_raylet_address()
            assert local is not None
            host, port = local
            with pytest.raises(RuntimeError, match=r"NotFound|object not present"):
                _native.pull_object(host, port, b"\x99" * 28)
        finally:
            rayd.shutdown()


def test_node_expires_to_dead_when_driver_stops_heartbeating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A node whose driver stops heartbeating eventually transitions to `dead`.

    We can't kill the heartbeat task from Python, so we simulate the stall
    by raising the heartbeat interval well above the GCS timeout — the
    first heartbeat tick just doesn't arrive in time and the sweeper
    flips the node to `dead`.
    """
    # Sweeper expires after 250 ms; driver heartbeat is so slow it never fires.
    with _spawn_gcs("--heartbeat-timeout-ms", "250") as addr:
        monkeypatch.setenv("RAYD_GCS_ADDRESS", addr)
        monkeypatch.setenv("RAYD_HEARTBEAT_INTERVAL_MS", "60000")
        rayd.init()
        try:
            nid = _native.node_id()
            assert nid is not None
            # Wait beyond the timeout + one sweeper interval.
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline:
                if _node_status(bytes(nid)) == "dead":
                    break
                time.sleep(0.1)
            status = _node_status(bytes(nid))
            assert status == "dead", f"expected dead, got {status!r}"
        finally:
            rayd.shutdown()
