//! End-to-end tests for the raylet.
//!
//! Phase 3.4a covered registration + heartbeats. Phase 3.4b adds the
//! plasma-backed `Pull` and the in-memory directory: every test below
//! first spins up a plasma server, then a raylet pointing at it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use rayd_gcs::{
    GcsClient, GcsServer, GcsServerConfig, GcsServerHandle, NodeStatus, Resources as GcsResources,
};
use rayd_plasma::{
    AddressBlob, PlasmaClient, PlasmaServer, ServerHandle as PlasmaHandle, DEFAULT_ARENA_BYTES,
};
use rayd_raylet::{
    LocalFsBackend, LocalObjectManager, ObjectTransportClient, Raylet, RayletConfig, RayletHandle,
};
use tempfile::TempDir;

fn loopback_zero() -> SocketAddr {
    "127.0.0.1:0".parse().expect("loopback addr")
}

fn fast_expiry_config() -> GcsServerConfig {
    GcsServerConfig {
        heartbeat_timeout: Duration::from_millis(300),
        sweep_interval: Duration::from_millis(50),
        metrics_bind: None,
    }
}

/// One-shot harness used by every test below.
///
/// Owns the GCS, a fresh plasma server (with its temp dir), and the
/// raylet attached to both. Drop is a best-effort teardown; tests that
/// care about clean shutdown call the explicit helpers first.
struct Harness {
    plasma_socket: PathBuf,
    _plasma_server: PlasmaHandle,
    _temp_dir: TempDir,
    gcs: GcsServerHandle,
    raylet: Option<RayletHandle>,
}

async fn build_harness() -> Harness {
    let temp_dir = TempDir::with_prefix("rayd-raylet-test-").expect("tempdir");
    let plasma_socket = temp_dir.path().join("plasma.sock");
    let plasma_server =
        PlasmaServer::start(plasma_socket.clone(), DEFAULT_ARENA_BYTES).expect("start plasma");

    let gcs = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let config = RayletConfig {
        gcs_address: gcs.local_addr(),
        bind: loopback_zero(),
        advertise_host: "127.0.0.1".to_string(),
        plasma_socket: plasma_socket.clone(),
        resources: GcsResources {
            num_cpus: 4,
            num_gpus: 0,
            memory_bytes: 0,
        },
        heartbeat_interval: Duration::from_millis(50),
        owner_sink: None,
        object_manager: None,
        metrics_bind: None,
    };
    let raylet = Raylet::start(config).await.expect("start raylet");
    Harness {
        plasma_socket,
        _plasma_server: plasma_server,
        _temp_dir: temp_dir,
        gcs,
        raylet: Some(raylet),
    }
}

impl Harness {
    fn raylet(&self) -> &RayletHandle {
        self.raylet.as_ref().expect("raylet running")
    }

    fn plasma_client(&self) -> PlasmaClient {
        PlasmaClient::connect(&self.plasma_socket).expect("connect plasma client")
    }

    /// Seal `(metadata, data)` into the harness's plasma store under
    /// `object_id`. The owner address is a nil placeholder — irrelevant
    /// for the raylet's `Pull` path, which only reads what's local.
    fn seal_object(&self, object_id: [u8; 28], metadata: &[u8], data: &[u8]) {
        let mut client = self.plasma_client();
        client
            .create_and_seal(object_id, metadata, data, AddressBlob::nil())
            .expect("create_and_seal");
    }

    async fn shutdown(mut self) {
        if let Some(r) = self.raylet.take() {
            r.shutdown().await;
        }
        self.gcs.shutdown().await;
    }
}

// ── 3.4a coverage (kept) ───────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raylet_registers_and_heartbeats() {
    let h = build_harness().await;
    let mut gcs_client = GcsClient::connect(h.gcs.local_addr())
        .await
        .expect("connect");

    let nodes = gcs_client.list().await.expect("list");
    let entry = nodes
        .iter()
        .find(|n| {
            n.address
                .as_ref()
                .is_some_and(|a| a.node_id == h.raylet().node_id())
        })
        .expect("raylet visible");
    assert_eq!(NodeStatus::try_from(entry.status), Ok(NodeStatus::Alive));

    tokio::time::sleep(Duration::from_millis(800)).await;
    let nodes = gcs_client.list().await.expect("list-2");
    let entry = nodes
        .iter()
        .find(|n| {
            n.address
                .as_ref()
                .is_some_and(|a| a.node_id == h.raylet().node_id())
        })
        .expect("raylet visible after sleep");
    assert_eq!(NodeStatus::try_from(entry.status), Ok(NodeStatus::Alive));

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_raylets_share_one_gcs() {
    let h1 = build_harness().await;
    // Second raylet needs its own plasma; reuse the harness builder.
    let h2 = build_harness_against(&h1).await;

    assert_ne!(h1.raylet().node_id(), h2.raylet().node_id());
    assert_eq!(
        h1.raylet().cluster_session_id(),
        h2.raylet().cluster_session_id()
    );

    let mut gcs_client = GcsClient::connect(h1.gcs.local_addr())
        .await
        .expect("connect");
    let nodes = gcs_client.list().await.expect("list");
    let registered_ids: std::collections::HashSet<[u8; 16]> = nodes
        .iter()
        .filter_map(|n| {
            let addr = n.address.as_ref()?;
            let mut id = [0u8; 16];
            if addr.node_id.len() == 16 {
                id.copy_from_slice(&addr.node_id);
                Some(id)
            } else {
                None
            }
        })
        .collect();
    assert!(registered_ids.contains(&h1.raylet().node_id()));
    assert!(registered_ids.contains(&h2.raylet().node_id()));

    h2.shutdown_keep_gcs().await;
    h1.shutdown().await;
}

/// Variant of `build_harness` that attaches a second raylet to an
/// already-running GCS, so two raylets in one test share a head.
async fn build_harness_against(other: &Harness) -> Harness {
    let temp_dir = TempDir::with_prefix("rayd-raylet-test-").expect("tempdir");
    let plasma_socket = temp_dir.path().join("plasma.sock");
    let plasma_server =
        PlasmaServer::start(plasma_socket.clone(), DEFAULT_ARENA_BYTES).expect("start plasma");
    let config = RayletConfig {
        gcs_address: other.gcs.local_addr(),
        bind: loopback_zero(),
        advertise_host: "127.0.0.1".to_string(),
        plasma_socket: plasma_socket.clone(),
        resources: GcsResources {
            num_cpus: 4,
            num_gpus: 0,
            memory_bytes: 0,
        },
        heartbeat_interval: Duration::from_millis(50),
        owner_sink: None,
        object_manager: None,
        metrics_bind: None,
    };
    let raylet = Raylet::start(config).await.expect("start raylet 2");
    // For the two-raylets test we don't have a second gcs to manage;
    // re-use the dummy GcsServer trick by not running one. Instead we
    // borrow `other`'s — see `shutdown_keep_gcs` below.
    // Build a no-op GCS server so the Harness invariant holds; we
    // shut it down immediately.
    let dummy_gcs = GcsServer::start(loopback_zero()).await.expect("dummy gcs");
    Harness {
        plasma_socket,
        _plasma_server: plasma_server,
        _temp_dir: temp_dir,
        gcs: dummy_gcs,
        raylet: Some(raylet),
    }
}

impl Harness {
    /// Shut down everything except the GCS (used by `two_raylets_share_one_gcs`
    /// where the second harness only owns a placeholder GCS).
    async fn shutdown_keep_gcs(mut self) {
        if let Some(r) = self.raylet.take() {
            r.shutdown().await;
        }
        self.gcs.shutdown().await;
    }
}

// ── 3.4b: Pull + RegisterObject + GetObjectLocations ───────────────────

const SAMPLE_OID: [u8; 28] = [0x42; 28];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_round_trips_a_sealed_object() {
    let h = build_harness().await;
    let payload = vec![0xABu8; 200_000]; // > one chunk
    let metadata = b"sample-metadata".to_vec();
    h.seal_object(SAMPLE_OID, &metadata, &payload);

    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    let pulled = client.pull(SAMPLE_OID.to_vec()).await.expect("pull");
    assert_eq!(pulled.metadata, metadata);
    assert_eq!(pulled.data, payload);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_unknown_object_returns_not_found() {
    let h = build_harness().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    match client.pull([0x77u8; 28].to_vec()).await {
        Err(rayd_raylet::ObjectTransportClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_handles_zero_byte_object() {
    let h = build_harness().await;
    h.seal_object(SAMPLE_OID, b"", b"");
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    let pulled = client.pull(SAMPLE_OID.to_vec()).await.expect("pull");
    assert!(pulled.metadata.is_empty());
    assert!(pulled.data.is_empty());
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_then_get_object_locations_round_trip() {
    let h = build_harness().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");

    // No registrations yet → empty list.
    let empty = client
        .get_object_locations(SAMPLE_OID.to_vec())
        .await
        .expect("loc");
    assert!(empty.node_ids.is_empty());

    // Register two distinct holders.
    client
        .register_object(SAMPLE_OID.to_vec(), [1u8; 16])
        .await
        .expect("register-1");
    client
        .register_object(SAMPLE_OID.to_vec(), [2u8; 16])
        .await
        .expect("register-2");
    // Idempotent.
    client
        .register_object(SAMPLE_OID.to_vec(), [1u8; 16])
        .await
        .expect("register-1-again");

    let mut locs = client
        .get_object_locations(SAMPLE_OID.to_vec())
        .await
        .expect("loc-after");
    locs.node_ids.sort_unstable();
    assert_eq!(locs.node_ids, vec![[1u8; 16], [2u8; 16]]);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_seals_into_local_plasma_and_pull_returns_same_bytes() {
    let h = build_harness().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");

    let payload = vec![0xCDu8; 200_000];
    let metadata = b"pushed-meta".to_vec();
    client
        .push(SAMPLE_OID.to_vec(), metadata.clone(), payload.clone())
        .await
        .expect("push");

    // Pull from the same raylet to confirm the seal landed.
    let pulled = client.pull(SAMPLE_OID.to_vec()).await.expect("pull");
    assert_eq!(pulled.metadata, metadata);
    assert_eq!(pulled.data, payload);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_is_idempotent_when_object_already_sealed() {
    let h = build_harness().await;
    let payload = b"already-here".to_vec();
    let metadata = b"m".to_vec();
    h.seal_object(SAMPLE_OID, &metadata, &payload);

    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    // Second push of the same id with possibly-different bytes still
    // succeeds — plasma's AlreadyExists is treated as "we already
    // have it, no work needed."
    client
        .push(SAMPLE_OID.to_vec(), metadata.clone(), payload.clone())
        .await
        .expect("push duplicate");

    let pulled = client.pull(SAMPLE_OID.to_vec()).await.expect("pull");
    assert_eq!(pulled.data, payload);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn push_handles_zero_byte_object() {
    let h = build_harness().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    client
        .push(SAMPLE_OID.to_vec(), b"".to_vec(), b"".to_vec())
        .await
        .expect("push empty");
    let pulled = client.pull(SAMPLE_OID.to_vec()).await.expect("pull");
    assert!(pulled.metadata.is_empty());
    assert!(pulled.data.is_empty());
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn register_object_rejects_wrong_byte_lengths() {
    let h = build_harness().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");

    // Object id is required to be 28 bytes.
    match client.register_object(vec![0u8; 5], [1u8; 16]).await {
        Err(rayd_raylet::ObjectTransportClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
        other => panic!("expected InvalidArgument for short object_id, got {other:?}"),
    }

    h.shutdown().await;
}

// ── 6.3: spill-aware Pull ──────────────────────────────────────────────

/// Variant of `build_harness` that attaches a `LocalObjectManager` rooted
/// in the harness's temp dir. Returns the harness AND the manager so a
/// test can pre-spill objects before issuing Pull RPCs.
async fn build_harness_with_spill() -> (Harness, Arc<LocalObjectManager>) {
    let temp_dir = TempDir::with_prefix("rayd-raylet-spill-test-").expect("tempdir");
    let plasma_socket = temp_dir.path().join("plasma.sock");
    let plasma_server =
        PlasmaServer::start(plasma_socket.clone(), DEFAULT_ARENA_BYTES).expect("start plasma");

    let gcs = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");

    let spill_root = temp_dir.path().join("spill");
    let backend = Arc::new(LocalFsBackend::new(spill_root).expect("spill backend"));
    let manager = Arc::new(LocalObjectManager::new(backend));

    let config = RayletConfig {
        gcs_address: gcs.local_addr(),
        bind: loopback_zero(),
        advertise_host: "127.0.0.1".to_string(),
        plasma_socket: plasma_socket.clone(),
        resources: GcsResources {
            num_cpus: 4,
            num_gpus: 0,
            memory_bytes: 0,
        },
        heartbeat_interval: Duration::from_millis(50),
        owner_sink: None,
        object_manager: Some(Arc::clone(&manager)),
        metrics_bind: None,
    };
    let raylet = Raylet::start(config).await.expect("start raylet");
    let harness = Harness {
        plasma_socket,
        _plasma_server: plasma_server,
        _temp_dir: temp_dir,
        gcs,
        raylet: Some(raylet),
    };
    (harness, manager)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_restores_spilled_object_when_plasma_misses() {
    let (h, manager) = build_harness_with_spill().await;
    let oid: [u8; 28] = [0x55; 28];
    let metadata = b"spilled-meta".to_vec();
    let payload = vec![0xCDu8; 200_000]; // > one Pull chunk

    // Pre-spill the object — never touches plasma. The raylet's Pull
    // handler must consult the manager on plasma-miss and restore.
    manager
        .spill(
            oid,
            Bytes::from(metadata.clone()),
            Bytes::from(payload.clone()),
        )
        .expect("spill");
    assert!(manager.is_spilled(oid));

    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    let pulled = client.pull(oid.to_vec()).await.expect("pull");
    assert_eq!(pulled.metadata, metadata);
    assert_eq!(pulled.data, payload);

    // Restore reseals into plasma — verify a second Pull (same client,
    // unchanged manager) still returns the same bytes. Confirms the
    // restore path was idempotent and the resealed object stuck.
    let pulled2 = client.pull(oid.to_vec()).await.expect("pull again");
    assert_eq!(pulled2.data, payload);

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_restores_after_evicting_from_plasma() {
    // End-to-end: seal an object → spill it (read from plasma, copy
    // to backend, then delete from plasma) → Pull RPC must restore
    // transparently. Mirrors the spill-on-pressure flow even though
    // the eviction trigger here is manual.
    let (h, manager) = build_harness_with_spill().await;
    let oid: [u8; 28] = [0xA1; 28];
    let metadata = b"evict-test-meta".to_vec();
    let payload = vec![0xEFu8; 50_000];

    h.seal_object(oid, &metadata, &payload);

    // Read out of plasma + spill via the manager + delete from plasma.
    {
        let mut client = h.plasma_client();
        let handle = client.get(oid).expect("get from plasma");
        let meta_bytes = handle.metadata().to_vec();
        let data_bytes = handle.data().to_vec();
        drop(handle);
        manager
            .spill(oid, Bytes::from(meta_bytes), Bytes::from(data_bytes))
            .expect("spill");
        client.delete(oid).expect("delete from plasma");
    }

    // Confirm plasma really doesn't have it anymore — direct get
    // should fail.
    {
        let mut client = h.plasma_client();
        let err = client.get(oid).unwrap_err();
        assert!(matches!(
            err,
            rayd_plasma::PlasmaError::Server {
                kind: rayd_plasma::ServerErrorKind::NotFound,
                ..
            },
        ));
    }

    // Now Pull via the raylet — the spill-aware handler must restore.
    let mut grpc = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    let pulled = grpc.pull(oid.to_vec()).await.expect("pull after evict");
    assert_eq!(pulled.metadata, metadata);
    assert_eq!(pulled.data, payload);

    // After the restore, plasma has the bytes again. A direct plasma
    // get should now succeed.
    {
        let mut client = h.plasma_client();
        let handle = client
            .get(oid)
            .expect("plasma should have it after restore");
        assert_eq!(handle.data(), &payload[..]);
    }

    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pull_unknown_object_with_spill_manager_still_returns_not_found() {
    // Manager is configured but knows nothing about this oid — should
    // surface NotFound instead of falling into a confusing internal
    // error path.
    let (h, _manager) = build_harness_with_spill().await;
    let mut client = ObjectTransportClient::connect(h.raylet().local_addr())
        .await
        .expect("connect");
    match client.pull([0x99u8; 28].to_vec()).await {
        Err(rayd_raylet::ObjectTransportClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
    h.shutdown().await;
}

// ── 7.4b: Prometheus /metrics ──────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raylet_metrics_endpoint_counts_pulls_and_directory() {
    // Spawn a raylet with metrics on a kernel-assigned port. Drive
    // a couple of RPCs (Pull on a known oid + RegisterObject) and
    // confirm the counter / gauge surfaces in the scrape.
    let temp_dir = TempDir::with_prefix("rayd-raylet-metrics-test-").expect("tempdir");
    let plasma_socket = temp_dir.path().join("plasma.sock");
    let plasma_server =
        PlasmaServer::start(plasma_socket.clone(), DEFAULT_ARENA_BYTES).expect("start plasma");

    let gcs = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let config = RayletConfig {
        gcs_address: gcs.local_addr(),
        bind: loopback_zero(),
        advertise_host: "127.0.0.1".to_string(),
        plasma_socket: plasma_socket.clone(),
        resources: GcsResources {
            num_cpus: 1,
            num_gpus: 0,
            memory_bytes: 0,
        },
        heartbeat_interval: Duration::from_millis(50),
        owner_sink: None,
        object_manager: None,
        metrics_bind: Some(loopback_zero()),
    };
    let raylet = Raylet::start(config).await.expect("start raylet");
    let metrics_addr = raylet.metrics_addr().expect("metrics enabled");

    // Drive 2 RegisterObject + 1 Pull (will return NotFound since
    // we never seal anything; the counter still increments).
    let mut client = ObjectTransportClient::connect(raylet.local_addr())
        .await
        .expect("connect");
    client
        .register_object(vec![0xA1u8; 28], [1u8; 16])
        .await
        .expect("register 1");
    client
        .register_object(vec![0xA2u8; 28], [1u8; 16])
        .await
        .expect("register 2");
    let _ = client.pull([0xA1u8; 28].to_vec()).await; // miss → NotFound

    let body = scrape_metrics(metrics_addr).await;
    assert!(body.contains("rayd_raylet_pull_total 1"), "body = {body:?}");
    assert!(
        body.contains("rayd_raylet_register_object_total 2"),
        "body = {body:?}"
    );
    assert!(
        body.contains("rayd_raylet_directory_entries 2"),
        "body = {body:?}"
    );

    raylet.shutdown().await;
    drop(plasma_server);
    drop(temp_dir);
    gcs.shutdown().await;
}

// ── 4.3.3c-F: NodeIndex lookup metric -------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raylet_metrics_record_node_status_lookup_outcomes() {
    let temp_dir = TempDir::with_prefix("rayd-raylet-fastpath-").expect("tempdir");
    let plasma_socket = temp_dir.path().join("plasma.sock");
    let plasma_server =
        PlasmaServer::start(plasma_socket.clone(), DEFAULT_ARENA_BYTES).expect("start plasma");

    let gcs = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let config = RayletConfig {
        gcs_address: gcs.local_addr(),
        bind: loopback_zero(),
        advertise_host: "127.0.0.1".to_string(),
        plasma_socket: plasma_socket.clone(),
        resources: GcsResources {
            num_cpus: 1,
            num_gpus: 0,
            memory_bytes: 0,
        },
        heartbeat_interval: Duration::from_millis(50),
        owner_sink: None,
        object_manager: None,
        metrics_bind: Some(loopback_zero()),
    };
    let raylet = Raylet::start(config).await.expect("start raylet");
    let metrics_addr = raylet.metrics_addr().expect("metrics enabled");

    // Wait until the WatchNodes subscriber has populated the index
    // with this raylet's own self-registration. Polling the lookup
    // method itself doubles as the test's "hit" assertion.
    let own_id = raylet.node_id();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while raylet.node_status(&own_id).is_none() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        raylet.node_status(&own_id).is_some(),
        "subscriber should have populated the cache for this raylet's own id within 2s"
    );

    // Generate guaranteed misses by querying ids that were never
    // registered.
    let _ = raylet.node_status(&[0xFFu8; 16]);
    let _ = raylet.node_status(&[0xFEu8; 16]);

    let body = scrape_metrics(metrics_addr).await;
    assert!(
        body.contains(r#"rayd_node_index_status_lookups_total{outcome="hit"}"#),
        "metric label 'hit' missing from scrape:\n{body}"
    );
    assert!(
        body.contains(r#"rayd_node_index_status_lookups_total{outcome="miss"}"#),
        "metric label 'miss' missing from scrape:\n{body}"
    );

    raylet.shutdown().await;
    drop(plasma_server);
    drop(temp_dir);
    gcs.shutdown().await;
}

async fn scrape_metrics(addr: SocketAddr) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr).await.expect("connect /metrics");
    let request = format!("GET /metrics HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send GET");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let response = String::from_utf8_lossy(&buf).into_owned();
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or(response)
}

// ── 7.5: Health RPC ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raylet_health_check_reports_serving() {
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::ServingStatus;

    let h = build_harness().await;
    let addr = h.raylet().local_addr();
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect");
    let mut client = HealthClient::new(channel);
    let reply = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("check overall")
        .into_inner();
    assert_eq!(reply.status, ServingStatus::Serving as i32);
    h.shutdown().await;
}
