//! `NodeInfo` service implementation.
//!
//! Phase 3.3: in-memory `HashMap` of `node_id → NodeInfo`, guarded by a
//! `parking_lot::Mutex`. No persistence; restarting the GCS clears all
//! state and produces a fresh `cluster_session_id` so callers can detect
//! the bounce and re-register.

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_core::Stream;
use parking_lot::Mutex;
use rand::RngCore;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::metrics::Metrics;
use crate::proto::node_registry_server::NodeRegistry as NodeRegistrySvc;
use crate::proto::{
    DrainReply, DrainRequest, HeartbeatReply, HeartbeatRequest, ListReply, ListRequest,
    NodeAddress, NodeEvent, NodeInfo, NodeStatus, RegisterReply, RegisterRequest,
    WatchNodesRequest,
};

/// Capacity of the per-Registry replay buffer. Subscribers reconnecting
/// with a `last_seen_sequence` older than the oldest entry get
/// `OUT_OF_RANGE` and must restart from a fresh snapshot. ~1k events
/// covers seconds of churn even on a busy cluster.
const REPLAY_BUFFER_CAPACITY: usize = 1024;

/// Capacity of the live broadcast channel. Slow subscribers that lag
/// past this lose events and the stream surfaces a `Lagged` error;
/// our handler turns that into `OUT_OF_RANGE` so the client reconnects.
const BROADCAST_CHANNEL_CAPACITY: usize = 256;

/// In-memory registry shared between the gRPC server and tests.
#[derive(Debug)]
pub(crate) struct Registry {
    nodes: Mutex<HashMap<[u8; 16], NodeInfo>>,
    cluster_session_id: [u8; 16],
    /// Broadcast channel for live `NodeEvent`s. New `WatchNodes`
    /// subscribers subscribe to this after their snapshot replay.
    event_tx: broadcast::Sender<NodeEvent>,
    /// Ring buffer of recent events for resume-after-disconnect.
    /// Bounded by `REPLAY_BUFFER_CAPACITY`; older events drop off
    /// and resume requests below the floor get `OUT_OF_RANGE`.
    replay_buffer: Mutex<VecDeque<NodeEvent>>,
    /// Monotonic sequence assigned per published event. Resets to 0
    /// only when the GCS process restarts (which also rotates the
    /// `cluster_session_id`).
    next_sequence: AtomicU64,
}

impl Registry {
    pub(crate) fn new() -> Arc<Self> {
        let mut session_id = [0u8; 16];
        rand::rng().fill_bytes(&mut session_id);
        let (event_tx, _) = broadcast::channel(BROADCAST_CHANNEL_CAPACITY);
        Arc::new(Self {
            nodes: Mutex::new(HashMap::new()),
            cluster_session_id: session_id,
            event_tx,
            replay_buffer: Mutex::new(VecDeque::with_capacity(REPLAY_BUFFER_CAPACITY)),
            next_sequence: AtomicU64::new(1),
        })
    }

    pub(crate) fn cluster_session_id(&self) -> [u8; 16] {
        self.cluster_session_id
    }

    pub(crate) fn snapshot(&self) -> Vec<NodeInfo> {
        self.nodes.lock().values().cloned().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.lock().len()
    }

    /// Build a `NodeEvent` with a fresh sequence, push it into the
    /// replay buffer, and broadcast to live subscribers. `send` errors
    /// are silently ignored — they only occur when there are zero
    /// subscribers, which is fine.
    fn publish(&self, node: NodeInfo) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let event = NodeEvent {
            sequence,
            node: Some(node),
        };
        {
            let mut buf = self.replay_buffer.lock();
            if buf.len() == REPLAY_BUFFER_CAPACITY {
                buf.pop_front();
            }
            buf.push_back(event.clone());
        }
        let _ = self.event_tx.send(event);
    }

    /// Lowest sequence still in the replay buffer (or `None` when the
    /// buffer is empty — startup state).
    fn oldest_buffered_sequence(&self) -> Option<u64> {
        self.replay_buffer.lock().front().map(|e| e.sequence)
    }

    /// Snapshot of buffered events strictly after `after_sequence`.
    /// Allocates — only used at subscribe time, not on the hot path.
    fn buffered_after(&self, after_sequence: u64) -> Vec<NodeEvent> {
        self.replay_buffer
            .lock()
            .iter()
            .filter(|e| e.sequence > after_sequence)
            .cloned()
            .collect()
    }

    /// Mark every `Alive` node whose last heartbeat is older than
    /// `deadline_ms` as `Dead`. Returns the number of nodes flipped.
    /// `Draining` and already-`Dead` nodes are left alone — drain is a
    /// deliberate state and we don't second-guess it from the sweeper.
    /// When `metrics` is `Some`, decrements `nodes_alive` by the
    /// flipped count.
    pub(crate) fn expire_stale(&self, deadline_ms: u64, metrics: Option<&Metrics>) -> usize {
        let mut flipped_nodes: Vec<NodeInfo> = Vec::new();
        {
            let mut guard = self.nodes.lock();
            for (id, info) in guard.iter_mut() {
                if info.status == NodeStatus::Alive as i32
                    && info.last_heartbeat_unix_ms < deadline_ms
                {
                    info.status = NodeStatus::Dead as i32;
                    warn!(
                        node_id = ?hex_short(id),
                        last_heartbeat_unix_ms = info.last_heartbeat_unix_ms,
                        "rayd-gcs: node missed heartbeat deadline, marking dead"
                    );
                    flipped_nodes.push(info.clone());
                }
            }
        }
        if let Some(m) = metrics {
            m.nodes_alive
                .sub(i64::try_from(flipped_nodes.len()).unwrap_or(i64::MAX));
            m.watch_events_published_total
                .inc_by(flipped_nodes.len() as u64);
        }
        // Publish AFTER releasing the registry lock so subscribers
        // never observe a publish that disagrees with `snapshot()`.
        let flipped = flipped_nodes.len();
        for node in flipped_nodes {
            self.publish(node);
        }
        flipped
    }
}

/// gRPC adapter that wires `Registry` into the tonic-generated trait.
#[derive(Debug, Clone)]
pub(crate) struct NodeRegistryService {
    registry: Arc<Registry>,
    metrics: Option<Metrics>,
}

impl NodeRegistryService {
    pub(crate) fn new(registry: Arc<Registry>, metrics: Option<Metrics>) -> Self {
        Self { registry, metrics }
    }

    /// Build a one-shot snapshot of currently-known nodes as `NodeEvent`s
    /// stamped with `(next_sequence-1, next_sequence-2, ...)` so a fresh
    /// subscriber can prime its local cache. Sequences here are reused
    /// from the `next_sequence` counter WITHOUT incrementing it (the
    /// snapshot is a read view, not a publish).
    fn snapshot_as_events(&self) -> Vec<NodeEvent> {
        // Use sequence 0 for snapshot entries — they're below any real
        // event, so subsequent events with sequence >= 1 always sort
        // after them. Subscribers should track the highest sequence
        // they've seen and pass it on reconnect; sequence 0 is fine to
        // pass back since the gate is `> last_seen`.
        self.registry
            .snapshot()
            .into_iter()
            .map(|node| NodeEvent {
                sequence: 0,
                node: Some(node),
            })
            .collect()
    }
}

#[tonic::async_trait]
impl NodeRegistrySvc for NodeRegistryService {
    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterReply>, Status> {
        let req = request.into_inner();
        if req.host.is_empty() {
            return Err(Status::invalid_argument("host must not be empty"));
        }

        let mut node_id = [0u8; 16];
        rand::rng().fill_bytes(&mut node_id);
        let now_ms = unix_ms();

        let info = NodeInfo {
            address: Some(NodeAddress {
                host: req.host,
                port: req.port,
                node_id: node_id.to_vec(),
                plasma_socket: req.plasma_socket,
            }),
            resources: req.resources,
            status: NodeStatus::Alive as i32,
            registered_at_unix_ms: now_ms,
            last_heartbeat_unix_ms: now_ms,
        };

        info!(
            node_id = ?hex_short(&node_id),
            host = info.address.as_ref().map_or("", |a| a.host.as_str()),
            "rayd-gcs: node registered"
        );
        self.registry.nodes.lock().insert(node_id, info.clone());
        if let Some(m) = &self.metrics {
            m.register_node_total.inc();
            m.nodes_alive.inc();
            m.nodes_total.inc();
            m.watch_events_published_total.inc();
        }
        self.registry.publish(info);

        Ok(Response::new(RegisterReply {
            node_id: node_id.to_vec(),
            cluster_session_id: self.registry.cluster_session_id().to_vec(),
        }))
    }

    async fn drain(&self, request: Request<DrainRequest>) -> Result<Response<DrainReply>, Status> {
        let req = request.into_inner();
        let node_id = parse_node_id(&req.node_id)?;
        let (was_alive, snapshot) = {
            let mut guard = self.registry.nodes.lock();
            match guard.get_mut(&node_id) {
                None => return Err(Status::not_found("unknown node_id")),
                Some(info) => {
                    let was_alive = info.status == NodeStatus::Alive as i32;
                    info.status = NodeStatus::Draining as i32;
                    debug!(node_id = ?hex_short(&node_id), "rayd-gcs: node draining");
                    (was_alive, info.clone())
                }
            }
        };
        if was_alive {
            if let Some(m) = &self.metrics {
                m.nodes_alive.dec();
            }
        }
        if let Some(m) = &self.metrics {
            m.watch_events_published_total.inc();
        }
        self.registry.publish(snapshot);
        Ok(Response::new(DrainReply {}))
    }

    async fn list(&self, _request: Request<ListRequest>) -> Result<Response<ListReply>, Status> {
        Ok(Response::new(ListReply {
            nodes: self.registry.snapshot(),
        }))
    }

    type WatchNodesStream = Pin<Box<dyn Stream<Item = Result<NodeEvent, Status>> + Send + 'static>>;

    async fn watch_nodes(
        &self,
        request: Request<WatchNodesRequest>,
    ) -> Result<Response<Self::WatchNodesStream>, Status> {
        let req = request.into_inner();
        // Subscribe BEFORE building the catch-up batch so any
        // concurrent publish lands on the broadcast tx and is delivered
        // by the live tail rather than dropped in the gap. Subsequent
        // de-dup against the catch-up batch is by sequence number.
        let live = self.registry.event_tx.subscribe();

        // Catch-up batch:
        //   last_seen == 0 → snapshot all currently-known nodes,
        //                    sequence-stamped from `next_sequence` (NOT
        //                    incremented — these are read-only views).
        //   last_seen >  0 → replay buffer entries strictly after it,
        //                    or OUT_OF_RANGE if too old.
        let catchup = if req.last_seen_sequence == 0 {
            self.snapshot_as_events()
        } else {
            match self.registry.oldest_buffered_sequence() {
                Some(oldest) if req.last_seen_sequence + 1 < oldest => {
                    return Err(Status::out_of_range(format!(
                        "last_seen_sequence {} is older than the buffer floor {oldest}",
                        req.last_seen_sequence,
                    )));
                }
                _ => self.registry.buffered_after(req.last_seen_sequence),
            }
        };
        let highest_catchup_seq = catchup.last().map_or(0, |e| e.sequence);

        let live_stream = BroadcastStream::new(live).filter_map(move |item| match item {
            Ok(event) if event.sequence > highest_catchup_seq => Some(Ok(event)),
            // Already covered by the catch-up batch — drop to avoid dups.
            Ok(_) => None,
            // Subscriber lagged past the broadcast capacity. Surface as
            // OUT_OF_RANGE so the client reconnects with last_seen=0.
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(_)) => {
                Some(Err(Status::out_of_range(
                    "subscriber lagged past broadcast capacity; reconnect with last_seen=0",
                )))
            }
        });

        let combined = tokio_stream::iter(catchup.into_iter().map(Ok)).chain(live_stream);
        Ok(Response::new(Box::pin(combined) as Self::WatchNodesStream))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatReply>, Status> {
        let req = request.into_inner();
        let node_id = parse_node_id(&req.node_id)?;
        let mut guard = self.registry.nodes.lock();
        match guard.get_mut(&node_id) {
            None => return Err(Status::not_found("unknown node_id")),
            Some(info) => {
                info.last_heartbeat_unix_ms = unix_ms();
            }
        }
        drop(guard);
        if let Some(m) = &self.metrics {
            m.heartbeat_received_total.inc();
        }
        Ok(Response::new(HeartbeatReply {}))
    }
}

fn parse_node_id(bytes: &[u8]) -> Result<[u8; 16], Status> {
    if bytes.len() != 16 {
        return Err(Status::invalid_argument(format!(
            "node_id must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(buf)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn hex_short(bytes: &[u8; 16]) -> String {
    bytes.iter().take(4).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
