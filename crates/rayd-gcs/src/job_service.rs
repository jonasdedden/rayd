//! `JobRegistry` service implementation.
//!
//! Mirrors `service.rs` (the node registry): in-memory `HashMap`,
//! 16-byte server-assigned ids, no persistence. Fresh ids are minted on
//! `AddJob`; subsequent `MarkJobFinished` and `List` echo them back.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use rand::RngCore;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::job_proto::job_registry_server::JobRegistry as JobRegistrySvc;
use crate::job_proto::{
    AddJobReply, AddJobRequest, JobInfo, JobStatus, ListJobsReply, ListJobsRequest,
    MarkJobFinishedReply, MarkJobFinishedRequest,
};
use crate::metrics::Metrics;

#[derive(Debug)]
pub(crate) struct JobsRegistry {
    jobs: Mutex<HashMap<[u8; 16], JobInfoEntry>>,
}

/// Server-side entry. Distinct from the wire `JobInfo` so we can carry the
/// optional `failure_message` without it leaking into every snapshot reply.
#[derive(Debug, Clone)]
struct JobInfoEntry {
    info: JobInfo,
    failure_message: Option<String>,
}

impl JobsRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn snapshot(&self) -> Vec<JobInfo> {
        self.jobs.lock().values().map(|e| e.info.clone()).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.jobs.lock().len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JobRegistryService {
    registry: Arc<JobsRegistry>,
    metrics: Option<Metrics>,
}

impl JobRegistryService {
    pub(crate) fn new(registry: Arc<JobsRegistry>, metrics: Option<Metrics>) -> Self {
        Self { registry, metrics }
    }
}

#[tonic::async_trait]
impl JobRegistrySvc for JobRegistryService {
    async fn add_job(
        &self,
        request: Request<AddJobRequest>,
    ) -> Result<Response<AddJobReply>, Status> {
        let req = request.into_inner();
        if req.driver_host.is_empty() {
            return Err(Status::invalid_argument("driver_host must not be empty"));
        }
        // node_id is optional. Empty-vec means "not associated with a node".
        if !req.node_id.is_empty() && req.node_id.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "node_id must be 16 bytes (or empty), got {}",
                req.node_id.len()
            )));
        }

        let mut job_id = [0u8; 16];
        rand::rng().fill_bytes(&mut job_id);
        let now_ms = unix_ms();

        let info = JobInfo {
            job_id: job_id.to_vec(),
            driver_host: req.driver_host,
            driver_pid: req.driver_pid,
            node_id: req.node_id,
            status: JobStatus::Running as i32,
            registered_at_unix_ms: now_ms,
            finished_at_unix_ms: 0,
        };

        info!(
            job_id = ?hex_short(&job_id),
            driver_pid = info.driver_pid,
            "rayd-gcs: job added"
        );
        self.registry.jobs.lock().insert(
            job_id,
            JobInfoEntry {
                info,
                failure_message: None,
            },
        );
        if let Some(m) = &self.metrics {
            m.jobs_running.inc();
        }

        Ok(Response::new(AddJobReply {
            job_id: job_id.to_vec(),
        }))
    }

    async fn mark_job_finished(
        &self,
        request: Request<MarkJobFinishedRequest>,
    ) -> Result<Response<MarkJobFinishedReply>, Status> {
        let req = request.into_inner();
        let job_id = parse_job_id(&req.job_id)?;
        let now_ms = unix_ms();

        let was_running;
        let mut guard = self.registry.jobs.lock();
        match guard.get_mut(&job_id) {
            None => return Err(Status::not_found("unknown job_id")),
            Some(entry) => {
                was_running = entry.info.status == JobStatus::Running as i32;
                entry.info.finished_at_unix_ms = now_ms;
                entry.info.status = if req.failure_message.is_empty() {
                    JobStatus::Finished as i32
                } else {
                    JobStatus::Failed as i32
                };
                if !req.failure_message.is_empty() {
                    entry.failure_message = Some(req.failure_message);
                }
                debug!(
                    job_id = ?hex_short(&job_id),
                    failed = entry.info.status == JobStatus::Failed as i32,
                    "rayd-gcs: job finished"
                );
            }
        }
        drop(guard);
        if was_running {
            if let Some(m) = &self.metrics {
                m.jobs_running.dec();
            }
        }
        Ok(Response::new(MarkJobFinishedReply {}))
    }

    async fn list(
        &self,
        _request: Request<ListJobsRequest>,
    ) -> Result<Response<ListJobsReply>, Status> {
        Ok(Response::new(ListJobsReply {
            jobs: self.registry.snapshot(),
        }))
    }
}

fn parse_job_id(bytes: &[u8]) -> Result<[u8; 16], Status> {
    if bytes.len() != 16 {
        return Err(Status::invalid_argument(format!(
            "job_id must be 16 bytes, got {}",
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
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn hex_short(bytes: &[u8; 16]) -> String {
    bytes.iter().take(4).fold(String::new(), |mut acc, b| {
        use std::fmt::Write as _;
        let _ = write!(acc, "{b:02x}");
        acc
    })
}
