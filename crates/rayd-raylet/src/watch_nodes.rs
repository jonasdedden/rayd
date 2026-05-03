//! Long-lived raylet task that subscribes to the GCS `WatchNodes`
//! stream, applies events to the local `NodeIndex`, and surfaces
//! `Alive → Dead` transitions to the `OwnerSink`.
//!
//! Phase 4.3.3c. The loop is structured around three failure modes:
//! - Transient stream error (connection blip) → reconnect with backoff.
//! - `OUT_OF_RANGE` on resume → drop the resume token and re-subscribe
//!   with `last_seen=0` to pick up a fresh snapshot.
//! - Shutdown signal → break cleanly.

use std::sync::Arc;
use std::time::Duration;

use rayd_gcs::{GcsClient, NodeStatus};
use tokio::sync::oneshot;
use tokio_stream::StreamExt as _;
use tonic::Code;
use tracing::{debug, info, warn};

use crate::node_index::{NodeIndex, StatusTransition};
use crate::owner_sink::OwnerSink;

/// Initial backoff after a stream error. Doubled on each consecutive
/// failure up to `MAX_BACKOFF`. Reset on every successful event.
const INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Cap on backoff between reconnect attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Drive the `WatchNodes` subscription until `shutdown_rx` fires.
///
/// `node_index` is updated in place. `owner_sink`, when present,
/// receives `on_owner_died` calls for every Alive→Dead transition.
pub(crate) async fn run_watch_nodes(
    mut client: GcsClient,
    node_index: Arc<NodeIndex>,
    owner_sink: Option<Arc<dyn OwnerSink>>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    let mut last_seen_sequence: u64 = 0;
    let mut backoff = INITIAL_BACKOFF;
    loop {
        // Open the stream. On connect failure, sleep with backoff then
        // try again — the GCS may still be coming up at startup.
        let stream = match client.watch_nodes(last_seen_sequence).await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, last_seen_sequence,
                    "rayd-raylet: WatchNodes subscribe failed; retrying");
                if sleep_with_shutdown(&mut shutdown_rx, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        info!(last_seen_sequence, "rayd-raylet: WatchNodes stream open");

        // Drive events until either the stream errors or shutdown fires.
        let stream_outcome = consume_stream(
            stream,
            &node_index,
            owner_sink.as_ref(),
            &mut last_seen_sequence,
            &mut shutdown_rx,
        )
        .await;
        match stream_outcome {
            StreamOutcome::Shutdown => return,
            StreamOutcome::ResetResume => {
                // The server told us our resume token is too old.
                // Drop it and re-subscribe from a fresh snapshot.
                last_seen_sequence = 0;
                backoff = INITIAL_BACKOFF;
            }
            StreamOutcome::TransientError => {
                if sleep_with_shutdown(&mut shutdown_rx, backoff).await {
                    return;
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
            StreamOutcome::CleanReset => {
                // Stream ended without error — server probably bounced.
                // Reconnect immediately and keep the resume token.
                backoff = INITIAL_BACKOFF;
            }
        }
    }
}

enum StreamOutcome {
    Shutdown,
    ResetResume,
    TransientError,
    CleanReset,
}

async fn consume_stream(
    mut stream: tonic::Streaming<rayd_gcs::NodeEvent>,
    node_index: &NodeIndex,
    owner_sink: Option<&Arc<dyn OwnerSink>>,
    last_seen_sequence: &mut u64,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> StreamOutcome {
    loop {
        tokio::select! {
            _ = &mut *shutdown_rx => return StreamOutcome::Shutdown,
            next = stream.next() => match next {
                None => return StreamOutcome::CleanReset,
                Some(Err(status)) => {
                    if status.code() == Code::OutOfRange {
                        warn!(error = %status,
                            "rayd-raylet: WatchNodes resume rejected; resetting");
                        return StreamOutcome::ResetResume;
                    }
                    warn!(error = %status,
                        "rayd-raylet: WatchNodes stream error; will reconnect");
                    return StreamOutcome::TransientError;
                }
                Some(Ok(event)) => {
                    if event.sequence > *last_seen_sequence {
                        *last_seen_sequence = event.sequence;
                    }
                    let Some(node) = event.node else { continue };
                    let transition = node_index.apply_event(&node);
                    let dead_node_id = match transition {
                        StatusTransition::Inserted(NodeStatus::Dead)
                        | StatusTransition::Changed { to: NodeStatus::Dead, .. } => {
                            node.address.as_ref().and_then(node_id_from_addr)
                        }
                        _ => None,
                    };
                    if let (Some(node_id), Some(sink)) = (dead_node_id, owner_sink) {
                        debug!(node_id = ?hex_short(&node_id),
                            "rayd-raylet: forwarding owner-died to OwnerSink");
                        sink.on_owner_died(node_id);
                    }
                }
            }
        }
    }
}

/// Either await `dur` or break out of the wait when `shutdown_rx`
/// fires. Returns `true` when shutdown was observed (caller should
/// exit), `false` on natural sleep completion.
async fn sleep_with_shutdown(shutdown_rx: &mut oneshot::Receiver<()>, dur: Duration) -> bool {
    tokio::select! {
        _ = &mut *shutdown_rx => true,
        () = tokio::time::sleep(dur) => false,
    }
}

fn node_id_from_addr(addr: &rayd_gcs::NodeAddress) -> Option<[u8; 16]> {
    if addr.node_id.len() != 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(&addr.node_id);
    Some(buf)
}

fn hex_short(bytes: &[u8; 16]) -> String {
    bytes.iter().take(4).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
