//! End-to-end test for the GCS: spin up the server, run two clients
//! against it, exercise Register / Heartbeat / Drain / List.

use std::net::SocketAddr;
use std::time::Duration;

use rayd_gcs::{GcsClient, GcsServer, GcsServerConfig, JobStatus, NodeStatus, Resources};

/// `127.0.0.1:0` lets the OS assign a free port; we read the actual addr
/// from the handle so two parallel test runs don't collide.
fn loopback_zero() -> SocketAddr {
    "127.0.0.1:0".parse().expect("loopback addr")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_list_drain_round_trip() {
    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let addr = server.local_addr();

    let mut client_a = GcsClient::connect(addr).await.expect("connect a");
    let mut client_b = GcsClient::connect(addr).await.expect("connect b");

    let resources = Resources {
        num_cpus: 8,
        num_gpus: 0,
        memory_bytes: 16 * 1024 * 1024 * 1024,
    };

    // Register two nodes.
    let outcome_a = client_a
        .register("10.0.0.1", 60001, "/tmp/rayd/a.sock", resources)
        .await
        .expect("register a");
    let outcome_b = client_b
        .register("10.0.0.2", 60002, "/tmp/rayd/b.sock", resources)
        .await
        .expect("register b");

    assert_ne!(outcome_a.node_id, outcome_b.node_id);
    // Both registrations under the same GCS instance share the cluster session id.
    assert_eq!(outcome_a.cluster_session_id, outcome_b.cluster_session_id);

    // Snapshot lists both nodes.
    let nodes = client_a.list().await.expect("list");
    assert_eq!(nodes.len(), 2);

    let mut hosts: Vec<String> = nodes
        .iter()
        .filter_map(|n| n.address.as_ref())
        .map(|a| a.host.clone())
        .collect();
    hosts.sort();
    assert_eq!(hosts, vec!["10.0.0.1", "10.0.0.2"]);

    // Heartbeat updates last_heartbeat_unix_ms; it should be present and >0.
    client_a
        .heartbeat(outcome_a.node_id)
        .await
        .expect("heartbeat a");

    // Drain node A.
    client_a.drain(outcome_a.node_id).await.expect("drain a");

    // List again: A should be Draining, B still Alive.
    let nodes = client_a.list().await.expect("list-2");
    let by_host: std::collections::HashMap<String, NodeStatus> = nodes
        .into_iter()
        .filter_map(|n| {
            let address = n.address.as_ref()?;
            let host = address.host.clone();
            let status = NodeStatus::try_from(n.status).ok()?;
            Some((host, status))
        })
        .collect();
    assert_eq!(by_host.get("10.0.0.1"), Some(&NodeStatus::Draining));
    assert_eq!(by_host.get("10.0.0.2"), Some(&NodeStatus::Alive));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_unknown_node_returns_not_found() {
    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    let unknown = [0xAB; 16];
    match client.drain(unknown).await {
        Err(rayd_gcs::GcsClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
        other => panic!("expected NotFound, got {other:?}"),
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_list_finish_job_round_trip() {
    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");

    // First register a node so we can link a job to it.
    let node = client
        .register(
            "10.0.0.5",
            60010,
            "/tmp/rayd/node.sock",
            Resources {
                num_cpus: 4,
                num_gpus: 0,
                memory_bytes: 1 << 30,
            },
        )
        .await
        .expect("register node");

    let job_a = client
        .add_job("driver-host-a", 4242, Some(node.node_id))
        .await
        .expect("add a");
    let job_b = client
        .add_job("driver-host-b", 4243, None)
        .await
        .expect("add b");
    assert_ne!(job_a, job_b);

    let jobs = client.list_jobs().await.expect("list_jobs");
    assert_eq!(jobs.len(), 2);
    for job in &jobs {
        assert_eq!(job.status, JobStatus::Running as i32);
        assert_eq!(job.finished_at_unix_ms, 0);
    }

    // Mark a graceful finish.
    client.mark_job_finished(job_a, "").await.expect("finish a");
    // Mark a failure on b.
    client
        .mark_job_finished(job_b, "boom")
        .await
        .expect("finish b");

    let after = client.list_jobs().await.expect("list_jobs after finish");
    let by_pid: std::collections::HashMap<u32, &rayd_gcs::JobInfo> =
        after.iter().map(|j| (j.driver_pid, j)).collect();
    assert_eq!(
        by_pid.get(&4242).map(|j| j.status),
        Some(JobStatus::Finished as i32)
    );
    assert_eq!(
        by_pid.get(&4243).map(|j| j.status),
        Some(JobStatus::Failed as i32)
    );
    assert_ne!(by_pid.get(&4242).unwrap().finished_at_unix_ms, 0);

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_job_rejects_empty_driver_host() {
    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    match client.add_job("", 1, None).await {
        Err(rayd_gcs::GcsClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    server.shutdown().await;
}

// ── Heartbeat / expiry sweeper ─────────────────────────────────────────

fn fast_expiry_config() -> GcsServerConfig {
    GcsServerConfig {
        heartbeat_timeout: Duration::from_millis(150),
        sweep_interval: Duration::from_millis(50),
        metrics_bind: None,
    }
}

async fn register_a_node(client: &mut GcsClient) -> [u8; 16] {
    let outcome = client
        .register(
            "10.0.0.7",
            60099,
            "/tmp/rayd/exp.sock",
            Resources {
                num_cpus: 1,
                num_gpus: 0,
                memory_bytes: 1,
            },
        )
        .await
        .expect("register");
    outcome.node_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missed_heartbeats_mark_node_dead() {
    let server = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    let node_id = register_a_node(&mut client).await;

    // Wait long enough for the sweeper to flip Alive → Dead.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let nodes = client.list().await.expect("list");
    let entry = nodes
        .iter()
        .find(|n| n.address.as_ref().is_some_and(|a| a.node_id == node_id))
        .expect("registered node visible");
    assert_eq!(NodeStatus::try_from(entry.status), Ok(NodeStatus::Dead));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_heartbeats_keep_node_alive() {
    let server = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    let node_id = register_a_node(&mut client).await;

    // Heartbeat every 50 ms for ~3x the timeout. The node must stay Alive.
    for _ in 0..9 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.heartbeat(node_id).await.expect("heartbeat");
    }

    let nodes = client.list().await.expect("list");
    let entry = nodes
        .iter()
        .find(|n| n.address.as_ref().is_some_and(|a| a.node_id == node_id))
        .expect("registered node visible");
    assert_eq!(NodeStatus::try_from(entry.status), Ok(NodeStatus::Alive));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drained_node_is_not_resurrected_as_dead() {
    // Drain is a deliberate state — the sweeper must not re-flip a Draining
    // node to Dead just because heartbeats stopped after drain.
    let server = GcsServer::start_with_config(loopback_zero(), fast_expiry_config())
        .await
        .expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    let node_id = register_a_node(&mut client).await;
    client.drain(node_id).await.expect("drain");

    tokio::time::sleep(Duration::from_millis(400)).await;

    let nodes = client.list().await.expect("list");
    let entry = nodes
        .iter()
        .find(|n| n.address.as_ref().is_some_and(|a| a.node_id == node_id))
        .expect("registered node visible");
    assert_eq!(NodeStatus::try_from(entry.status), Ok(NodeStatus::Draining));

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_rejects_empty_host() {
    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");
    let resources = Resources {
        num_cpus: 1,
        num_gpus: 0,
        memory_bytes: 1,
    };
    match client.register("", 12345, "/tmp/x.sock", resources).await {
        Err(rayd_gcs::GcsClientError::Rpc(status)) => {
            assert_eq!(status.code(), tonic::Code::InvalidArgument);
        }
        other => panic!("expected InvalidArgument, got {other:?}"),
    }
    server.shutdown().await;
}

// ── 7.5: Health RPC ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_reports_serving() {
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::ServingStatus;

    let server = GcsServer::start(loopback_zero()).await.expect("start gcs");
    let addr = server.local_addr();

    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("connect");
    let mut client = HealthClient::new(channel);

    // Empty service name → overall server health.
    let reply = client
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("check overall")
        .into_inner();
    assert_eq!(reply.status, ServingStatus::Serving as i32);

    // Per-service slot for the NodeRegistry. Same answer.
    let reply = client
        .check(HealthCheckRequest {
            service: "rayd.gcs.node_info.v1.NodeRegistry".to_owned(),
        })
        .await
        .expect("check NodeRegistry")
        .into_inner();
    assert_eq!(reply.status, ServingStatus::Serving as i32);

    server.shutdown().await;
}

// ── 7.4: Prometheus /metrics ───────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_serves_register_and_heartbeat_counters() {
    // Spawn GCS with metrics enabled on a kernel-assigned port. Two
    // metrics: register_node_total and heartbeat_received_total.
    // After registering and heartbeating, scrape /metrics and verify
    // the counters reflect the work.
    let config = GcsServerConfig {
        heartbeat_timeout: Duration::from_secs(10),
        sweep_interval: Duration::from_secs(1),
        metrics_bind: Some(loopback_zero()),
    };
    let server = GcsServer::start_with_config(loopback_zero(), config)
        .await
        .expect("start gcs");
    let metrics_addr = server.metrics_addr().expect("metrics enabled");

    let mut client = GcsClient::connect(server.local_addr())
        .await
        .expect("connect");

    let resources = Resources {
        num_cpus: 1,
        num_gpus: 0,
        memory_bytes: 0,
    };
    let outcome = client
        .register("10.0.0.1", 60100, "/tmp/rayd-metrics-test.sock", resources)
        .await
        .expect("register");
    // Drive a few heartbeats so the counter has more than one tick.
    for _ in 0..3 {
        client.heartbeat(outcome.node_id).await.expect("heartbeat");
    }

    // Scrape /metrics. Use raw tokio + a one-shot HTTP request rather
    // than dragging in a heavyweight client crate.
    let body = scrape_metrics(metrics_addr).await;
    assert!(body.contains("rayd_gcs_register_node_total 1"), "body = {body:?}");
    assert!(body.contains("rayd_gcs_heartbeat_received_total 3"), "body = {body:?}");
    assert!(body.contains("rayd_gcs_nodes_alive 1"), "body = {body:?}");
    assert!(body.contains("rayd_gcs_nodes_total 1"), "body = {body:?}");
    // Phase 4.3.3c-F: pubsub events published to WatchNodes
    // subscribers. One event per Register; heartbeats don't publish.
    assert!(
        body.contains("rayd_gcs_watch_events_published_total 1"),
        "body = {body:?}"
    );

    server.shutdown().await;
}

// ── Phase 4.3.3c: WatchNodes streaming pubsub ─────────────────────────────

/// `heartbeat_timeout = 0` disables the sweeper so the test's view of
/// the published event sequence matches what the test triggers (no
/// background `Dead` events from missed heartbeats).
fn no_expiry_config() -> GcsServerConfig {
    GcsServerConfig {
        heartbeat_timeout: Duration::from_secs(0),
        sweep_interval: Duration::from_secs(60),
        metrics_bind: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_nodes_emits_snapshot_then_live_events() {
    use futures_util::StreamExt as _;

    let server = GcsServer::start_with_config(loopback_zero(), no_expiry_config())
        .await
        .expect("start gcs");
    let addr = server.local_addr();

    let mut writer = GcsClient::connect(addr).await.expect("connect writer");
    let mut watcher = GcsClient::connect(addr).await.expect("connect watcher");

    // Pre-register one node so the snapshot has something to deliver.
    let outcome = register_a_node(&mut writer).await;

    let mut stream = watcher.watch_nodes(0).await.expect("watch_nodes open");

    // Snapshot: exactly one event for the pre-registered node, sequence 0.
    let snap = stream
        .next()
        .await
        .expect("snapshot event present")
        .expect("snapshot ok");
    assert_eq!(snap.sequence, 0);
    let snap_node = snap.node.expect("snapshot has node");
    assert_eq!(snap_node.address.expect("addr").node_id, outcome.to_vec());

    // Now register a second node and observe the live event.
    let _ = register_a_node(&mut writer).await;
    let live = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("event arrived in time")
        .expect("event present")
        .expect("event ok");
    assert!(live.sequence >= 1, "live event sequence is {}", live.sequence);
    assert!(live.node.is_some());

    // Drop the stream BEFORE shutdown — server graceful shutdown waits
    // for all in-flight RPCs to finish, and a server-streaming RPC
    // doesn't end until either the client cancels or the broadcast tx
    // drops. We cancel here so shutdown doesn't hang.
    drop(stream);
    drop(watcher);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_nodes_publishes_drain_and_dead_transitions() {
    use futures_util::StreamExt as _;

    let config = GcsServerConfig {
        // Tight expiry so the missed-heartbeat flip happens during
        // the test. The sweeper only flips Alive→Dead, so the test
        // separately registers a "drain target" (gets Draining via
        // drain RPC) and a "death target" (gets Dead by missing
        // heartbeats).
        heartbeat_timeout: Duration::from_millis(150),
        sweep_interval: Duration::from_millis(50),
        metrics_bind: None,
    };

    let server = GcsServer::start_with_config(loopback_zero(), config)
        .await
        .expect("start gcs");
    let addr = server.local_addr();

    let mut writer = GcsClient::connect(addr).await.expect("connect writer");
    let mut watcher = GcsClient::connect(addr).await.expect("connect watcher");

    // One node will be drained; another will go Dead via missed
    // heartbeats. Drain alone never produces a Dead transition (the
    // sweeper deliberately skips Draining nodes — see service.rs).
    let drain_target = register_a_node(&mut writer).await;
    let _death_target = register_a_node(&mut writer).await;

    let mut stream = watcher.watch_nodes(0).await.expect("watch_nodes open");

    // Drain one node and assert the corresponding event arrives with
    // status Draining. The OTHER node will time out on its own.
    writer.drain(drain_target).await.expect("drain");

    let mut saw_draining = false;
    let mut saw_dead = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !(saw_draining && saw_dead) && tokio::time::Instant::now() < deadline {
        let next = tokio::time::timeout_at(deadline, stream.next()).await;
        let Ok(Some(Ok(event))) = next else { break };
        let Some(node) = event.node else { continue };
        let status = NodeStatus::try_from(node.status).expect("status enum");
        match status {
            NodeStatus::Draining => saw_draining = true,
            NodeStatus::Dead => saw_dead = true,
            _ => {}
        }
    }
    assert!(saw_draining, "did not observe Draining event");
    assert!(saw_dead, "did not observe Dead event from sweeper");

    drop(stream);
    drop(watcher);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_nodes_resume_with_last_seen_skips_stale_events() {
    use futures_util::StreamExt as _;

    // Disable expiry so background sweeper events don't leak into the
    // test's expected sequence space.
    let server = GcsServer::start_with_config(loopback_zero(), no_expiry_config())
        .await
        .expect("start gcs");
    let addr = server.local_addr();

    let mut writer = GcsClient::connect(addr).await.expect("connect writer");

    // Generate events 1..=3 BEFORE the watcher subscribes.
    let _ = register_a_node(&mut writer).await;
    let _ = register_a_node(&mut writer).await;
    let _ = register_a_node(&mut writer).await;

    let mut watcher = GcsClient::connect(addr).await.expect("connect watcher");

    // Subscribe with last_seen=2 — should ONLY receive sequences > 2,
    // i.e. just one event (sequence 3). No snapshot replay because
    // last_seen != 0.
    let mut stream = watcher.watch_nodes(2).await.expect("watch_nodes resume");
    let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .expect("event in time")
        .expect("event present")
        .expect("event ok");
    assert_eq!(event.sequence, 3);

    // Pending: nothing else should be queued (the buffer only had 1..=3).
    // Use a short timeout to confirm the next read times out.
    let next = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(next.is_err(), "expected no more events; got {next:?}");

    drop(stream);
    drop(watcher);
    server.shutdown().await;
}

async fn scrape_metrics(addr: SocketAddr) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr).await.expect("connect /metrics");
    let request = format!(
        "GET /metrics HTTP/1.0\r\nHost: {addr}\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send GET");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let response = String::from_utf8_lossy(&buf).into_owned();
    // Strip the HTTP status + headers; keep the body after the first
    // "\r\n\r\n".
    response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or(response)
}
