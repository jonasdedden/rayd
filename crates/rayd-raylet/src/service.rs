//! `ObjectTransport` server implementation.
//!
//! All four RPCs are wired:
//! - `Pull`: copies the object out of local plasma, then streams an
//!   `ObjectMetadata` frame followed by 64 KiB `Data` frames.
//! - `Push`: receives a `Header` frame and a stream of `Data` frames,
//!   accumulates the bytes, and seals them into local plasma under
//!   the header's `object_id`. Idempotent on `AlreadyExists`.
//! - `RegisterObject`: idempotently records `(object_id, node_id)` in
//!   the in-memory directory.
//! - `GetObjectLocations`: returns the directory's current view.

use std::sync::Arc;

use parking_lot::Mutex;
use rayd_plasma::{AddressBlob, PlasmaClient, PlasmaError, ReadHandle, ServerErrorKind};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::directory::{NodeId, ObjectDirectory, ObjectId};
use crate::metrics::Metrics;
use crate::object_manager::LocalObjectManager;
use crate::owner_sink::OwnerSink;
use crate::proto::object_transport_server::ObjectTransport as ObjectTransportSvc;
use crate::proto::pull_chunk::Kind as PullKind;
use crate::proto::push_frame::Kind as PushKind;
use crate::proto::{
    EvictReply, EvictRequest, GetObjectLocationsReply, GetObjectLocationsRequest, ObjectMetadata,
    PullChunk, PullRequest, PushFrame, PushReply, RegisterObjectReply, RegisterObjectRequest,
    WaitForRefRemovedReply, WaitForRefRemovedRequest,
};

/// Pull streams data in 64 KiB chunks. Picked to match the typical
/// L1 cache line × 1024; tunable later if we measure differently.
pub(crate) const PULL_CHUNK_SIZE: usize = 64 * 1024;

/// Buffer 8 chunks (~512 KiB) ahead of the wire so a slow link doesn't
/// stall the plasma read or vice-versa.
const PULL_CHANNEL_DEPTH: usize = 8;

/// Owns the per-raylet shared state: the plasma client used to read
/// objects, the in-memory replica directory, an optional owner-side
/// sink that receives borrower add/remove notifications, and an
/// optional `LocalObjectManager` consulted on plasma-miss for spilled
/// restores.
#[derive(Clone)]
pub(crate) struct ObjectTransportService {
    plasma: Arc<Mutex<PlasmaClient>>,
    directory: Arc<ObjectDirectory>,
    owner_sink: Option<Arc<dyn OwnerSink>>,
    object_manager: Option<Arc<LocalObjectManager>>,
    metrics: Option<Metrics>,
}

impl std::fmt::Debug for ObjectTransportService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectTransportService")
            .field("directory", &self.directory)
            .field("has_owner_sink", &self.owner_sink.is_some())
            .field("has_object_manager", &self.object_manager.is_some())
            .field("has_metrics", &self.metrics.is_some())
            .finish_non_exhaustive()
    }
}

impl ObjectTransportService {
    pub(crate) fn new(
        plasma: Arc<Mutex<PlasmaClient>>,
        directory: Arc<ObjectDirectory>,
        owner_sink: Option<Arc<dyn OwnerSink>>,
        object_manager: Option<Arc<LocalObjectManager>>,
        metrics: Option<Metrics>,
    ) -> Self {
        Self {
            plasma,
            directory,
            owner_sink,
            object_manager,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl ObjectTransportSvc for ObjectTransportService {
    type PullStream = ReceiverStream<Result<PullChunk, Status>>;

    async fn pull(
        &self,
        request: Request<PullRequest>,
    ) -> Result<Response<Self::PullStream>, Status> {
        let req = request.into_inner();
        let object_id = parse_object_id(&req.object_id)?;
        if let Some(m) = &self.metrics {
            m.pull_total.inc();
        }

        // Plasma I/O is blocking; do it on a blocking task so we don't
        // stall the tonic worker thread. We open the read handle and
        // copy the (small) metadata buffer up front so we can stamp
        // data_size into the meta frame, then move the handle into the
        // streaming task. Chunks are sliced out of the mmap one by one
        // — no full-object Vec<u8> copy.
        //
        // On plasma miss, consult the spill manager (if configured):
        // if the object was spilled to disk, restore it back into
        // plasma transparently and proceed with the normal Pull flow.
        let plasma = Arc::clone(&self.plasma);
        let manager = self.object_manager.clone();
        let metrics = self.metrics.clone();
        let (handle, metadata) = tokio::task::spawn_blocking(move || -> Result<_, Status> {
            plasma_get_with_restore(&plasma, manager.as_deref(), metrics.as_ref(), object_id)
        })
        .await
        .map_err(|e| Status::internal(format!("plasma blocking task panicked: {e}")))??;

        let data_size = handle.data().len();

        let (tx, rx) = mpsc::channel::<Result<PullChunk, Status>>(PULL_CHANNEL_DEPTH);
        tokio::spawn(async move {
            let meta_frame = PullChunk {
                kind: Some(PullKind::Meta(ObjectMetadata {
                    metadata,
                    data_size: data_size as u64,
                })),
            };
            if tx.send(Ok(meta_frame)).await.is_err() {
                return;
            }
            // Slice the mmap one chunk at a time. The borrow lasts only
            // long enough to copy into the per-chunk Vec<u8>, then is
            // released before the await. Tonic needs owned bytes for
            // the wire frame, but the per-chunk copy is the only one —
            // no upfront full-buffer allocation.
            let mut start = 0usize;
            while start < data_size {
                let end = (start + PULL_CHUNK_SIZE).min(data_size);
                let chunk = handle.data()[start..end].to_vec();
                let frame = PullChunk {
                    kind: Some(PullKind::Data(chunk)),
                };
                if tx.send(Ok(frame)).await.is_err() {
                    // Caller closed the stream early; drop the handle
                    // so the plasma mmap refcount goes back down.
                    return;
                }
                start = end;
            }
            drop(handle);
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn push(
        &self,
        request: Request<Streaming<PushFrame>>,
    ) -> Result<Response<PushReply>, Status> {
        if let Some(m) = &self.metrics {
            m.push_total.inc();
        }
        let mut stream = request.into_inner();

        // Caller contract: first frame is `Header`, rest are `Data`.
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("push stream closed before header"))??;
        let header = match first.kind {
            Some(PushKind::Header(h)) => h,
            Some(PushKind::Data(_)) => {
                return Err(Status::invalid_argument(
                    "first push frame must be a Header, got Data",
                ));
            }
            None => return Err(Status::invalid_argument("first push frame is empty")),
        };
        let object_id = parse_object_id(&header.object_id)?;
        let data_size = usize::try_from(header.data_size).map_err(|_| {
            Status::invalid_argument(format!(
                "data_size {} doesn't fit in usize",
                header.data_size
            ))
        })?;

        let mut data: Vec<u8> = Vec::with_capacity(data_size);
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            match frame.kind {
                Some(PushKind::Data(bytes)) => {
                    if data.len() + bytes.len() > data_size {
                        return Err(Status::invalid_argument(format!(
                            "push data overran header.data_size: header said {data_size}, \
                             received {} so far + {} more",
                            data.len(),
                            bytes.len()
                        )));
                    }
                    data.extend_from_slice(&bytes);
                }
                Some(PushKind::Header(_)) => {
                    return Err(Status::invalid_argument(
                        "received second header frame mid-stream",
                    ));
                }
                None => {
                    return Err(Status::invalid_argument("received empty push frame"));
                }
            }
        }
        if data.len() != data_size {
            return Err(Status::invalid_argument(format!(
                "push short read: header promised {data_size} bytes, got {}",
                data.len()
            )));
        }

        // Seal into local plasma. AlreadyExists is treated as success
        // (somebody — maybe a previous Push or fetch — beat us to it).
        let plasma = Arc::clone(&self.plasma);
        let metadata = header.metadata;
        tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let mut client = plasma.lock();
            match client.create_and_seal(object_id, &metadata, &data, AddressBlob::nil()) {
                Ok(())
                | Err(PlasmaError::Server {
                    kind: ServerErrorKind::AlreadyExists,
                    ..
                }) => Ok(()),
                Err(e) => {
                    warn!(error = %e, "rayd-raylet: push seal failed");
                    Err(Status::internal(format!("plasma: {e}")))
                }
            }
        })
        .await
        .map_err(|e| Status::internal(format!("push blocking task panicked: {e}")))??;

        Ok(Response::new(PushReply {}))
    }

    async fn register_object(
        &self,
        request: Request<RegisterObjectRequest>,
    ) -> Result<Response<RegisterObjectReply>, Status> {
        let req = request.into_inner();
        let object_id = parse_object_id(&req.object_id)?;
        let node_id = parse_node_id(&req.node_id)?;
        self.directory.register(object_id, node_id);
        if let Some(sink) = &self.owner_sink {
            sink.add_borrower(object_id, node_id);
        }
        if let Some(m) = &self.metrics {
            m.register_object_total.inc();
            m.directory_entries
                .set(i64::try_from(self.directory.len()).unwrap_or(i64::MAX));
        }
        Ok(Response::new(RegisterObjectReply {}))
    }

    async fn wait_for_ref_removed(
        &self,
        request: Request<WaitForRefRemovedRequest>,
    ) -> Result<Response<WaitForRefRemovedReply>, Status> {
        let req = request.into_inner();
        let object_id = parse_object_id(&req.object_id)?;
        let node_id = parse_node_id(&req.node_id)?;
        // The directory lists "where can a peer Pull from" — once a
        // peer drops, it can't be pulled from anymore. Remove first
        // so a concurrent GetObjectLocations doesn't return a stale
        // holder.
        self.directory.remove(object_id, node_id);
        if let Some(sink) = &self.owner_sink {
            sink.remove_borrower(object_id, node_id);
        }
        Ok(Response::new(WaitForRefRemovedReply {}))
    }

    async fn evict(&self, request: Request<EvictRequest>) -> Result<Response<EvictReply>, Status> {
        let req = request.into_inner();
        let mut object_ids = Vec::with_capacity(req.object_ids.len());
        for raw in &req.object_ids {
            object_ids.push(parse_object_id(raw)?);
        }
        // Drop from local plasma + spill record (if any). Each step is
        // idempotent — duplicate evicts surface as `NotFound` from both
        // backends, which we swallow. Errors other than `NotFound`
        // log but do NOT propagate: an Evict whose downstream cleanup
        // partially fails still leaves the directory entry to be GC'd
        // by the borrower's own ref-drop path.
        let plasma = Arc::clone(&self.plasma);
        let manager = self.object_manager.clone();
        let directory = Arc::clone(&self.directory);
        let target_ids = object_ids.clone();
        tokio::task::spawn_blocking(move || {
            let mut client = plasma.lock();
            for oid in &target_ids {
                match client.delete(*oid) {
                    Ok(())
                    | Err(PlasmaError::Server {
                        kind: ServerErrorKind::NotFound,
                        ..
                    }) => {}
                    Err(e) => warn!(error = %e, "rayd-raylet: evict plasma delete failed"),
                }
                if let Some(m) = &manager {
                    if let Err(e) = m.forget(*oid) {
                        warn!(error = %e, "rayd-raylet: evict spill forget failed");
                    }
                }
            }
            // The directory entry tracks "where can I Pull from"; if the
            // local plasma copy is gone, the entry is stale. Drop it
            // best-effort — we don't know our own node id here, so we
            // can't selectively remove just the self-entry. The
            // directory's invariant survives stale-on-the-wire fetches
            // (peer replies NotFound and the puller retries).
            //
            // Phase 4.3.3c keeps this as a no-op on directory; the
            // borrower-initiated `WaitForRefRemoved` is what cleans
            // the directory entry on the OWNER side. Evict is a HINT
            // to peers; the directory authority remains the owner.
            let _ = directory;
        })
        .await
        .map_err(|e| Status::internal(format!("evict blocking task panicked: {e}")))?;

        Ok(Response::new(EvictReply {}))
    }

    async fn get_object_locations(
        &self,
        request: Request<GetObjectLocationsRequest>,
    ) -> Result<Response<GetObjectLocationsReply>, Status> {
        if let Some(m) = &self.metrics {
            m.get_object_locations_total.inc();
        }
        let req = request.into_inner();
        let object_id = parse_object_id(&req.object_id)?;
        let node_ids = self
            .directory
            .locations(&object_id)
            .into_iter()
            .map(|id| id.to_vec())
            .collect();
        Ok(Response::new(GetObjectLocationsReply { node_ids }))
    }
}

/// Open a plasma read handle for `object_id`, restoring from the spill
/// manager (if configured) when plasma misses. Returns the handle plus
/// a copy of the metadata buffer so the streaming task can stamp the
/// data size into the leading frame without re-locking plasma.
fn plasma_get_with_restore(
    plasma: &Arc<Mutex<PlasmaClient>>,
    manager: Option<&LocalObjectManager>,
    metrics: Option<&Metrics>,
    object_id: ObjectId,
) -> Result<(ReadHandle, Vec<u8>), Status> {
    // Fast path: object lives in plasma already.
    {
        let mut client = plasma.lock();
        match client.get(object_id) {
            Ok(handle) => {
                let metadata = handle.metadata().to_vec();
                return Ok((handle, metadata));
            }
            Err(PlasmaError::Server {
                kind: ServerErrorKind::NotFound,
                ..
            }) => {
                // fall through to spill check
            }
            Err(other) => return Err(plasma_to_status(other)),
        }
    }

    // Plasma miss. Consult the spill manager.
    let Some(manager) = manager else {
        return Err(Status::not_found("object not present in plasma"));
    };
    let restored = match manager.restore(object_id) {
        Ok(Some(r)) => {
            if let Some(m) = metrics {
                m.spill_restore_total.inc();
            }
            r
        }
        Ok(None) => {
            return Err(Status::not_found("object not present in plasma or spill"));
        }
        Err(e) => {
            warn!(error = %e, "rayd-raylet: spill restore failed");
            return Err(Status::internal(format!("spill restore: {e}")));
        }
    };

    // Seal the restored bytes back into plasma. `AlreadyExists` is
    // benign — another caller raced and beat us; either way the
    // object is now present and the next get() will succeed.
    {
        let mut client = plasma.lock();
        match client.create_and_seal(
            object_id,
            &restored.metadata,
            &restored.data,
            AddressBlob::nil(),
        ) {
            Ok(())
            | Err(PlasmaError::Server {
                kind: ServerErrorKind::AlreadyExists,
                ..
            }) => {}
            Err(other) => return Err(plasma_to_status(other)),
        }
    }

    // Re-open the read handle. Should hit on this attempt.
    let mut client = plasma.lock();
    let handle = client.get(object_id).map_err(plasma_to_status)?;
    let metadata = handle.metadata().to_vec();
    Ok((handle, metadata))
}

fn parse_object_id(bytes: &[u8]) -> Result<ObjectId, Status> {
    if bytes.len() != 28 {
        return Err(Status::invalid_argument(format!(
            "object_id must be 28 bytes, got {}",
            bytes.len()
        )));
    }
    let mut buf = [0u8; 28];
    buf.copy_from_slice(bytes);
    Ok(buf)
}

fn parse_node_id(bytes: &[u8]) -> Result<NodeId, Status> {
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

fn plasma_to_status(err: PlasmaError) -> Status {
    match err {
        PlasmaError::Server {
            kind: ServerErrorKind::NotFound,
            ..
        } => Status::not_found("object not present in plasma"),
        PlasmaError::Server {
            kind: ServerErrorKind::NotSealed,
            ..
        } => Status::failed_precondition("object exists but is unsealed"),
        other => {
            warn!(error = %other, "rayd-raylet: plasma read failed");
            Status::internal(format!("plasma: {other}"))
        }
    }
}
