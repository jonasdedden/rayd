//! `ActorRegistry` service implementation.
//!
//! Phase 5.4b: in-memory `name → ActorInfo` map. Names are unique;
//! re-registering the same name with a different `actor_id` returns
//! `AlreadyExists`. `UnregisterActor` rejects the call if the caller's
//! `actor_id` does not match the recorded entry — prevents a stale
//! handle from blowing away a freshly-registered name.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use tracing::{debug, info};

use crate::actor_proto::actor_registry_server::ActorRegistry as ActorRegistrySvc;
use crate::actor_proto::{
    ActorInfo, GetActorReply, GetActorRequest, ListActorsReply, ListActorsRequest,
    RegisterActorReply, RegisterActorRequest, UnregisterActorReply, UnregisterActorRequest,
};
use crate::metrics::Metrics;

#[derive(Debug)]
pub(crate) struct ActorsRegistry {
    actors: Mutex<HashMap<String, ActorInfo>>,
}

impl ActorsRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            actors: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn snapshot(&self) -> Vec<ActorInfo> {
        self.actors.lock().values().cloned().collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.actors.lock().len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ActorRegistryService {
    registry: Arc<ActorsRegistry>,
    metrics: Option<Metrics>,
}

impl ActorRegistryService {
    pub(crate) fn new(registry: Arc<ActorsRegistry>, metrics: Option<Metrics>) -> Self {
        Self { registry, metrics }
    }
}

#[tonic::async_trait]
impl ActorRegistrySvc for ActorRegistryService {
    async fn register_actor(
        &self,
        request: Request<RegisterActorRequest>,
    ) -> Result<Response<RegisterActorReply>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name must not be empty"));
        }
        let actor_id = parse_id(&req.actor_id, "actor_id")?;
        // owner_node_id is optional — drivers without an associated node
        // (rare; mostly tests) pass empty. Otherwise must be 16 bytes.
        if !req.owner_node_id.is_empty() && req.owner_node_id.len() != 16 {
            return Err(Status::invalid_argument(format!(
                "owner_node_id must be 16 bytes (or empty), got {}",
                req.owner_node_id.len()
            )));
        }
        let now_ms = unix_ms();
        let info = ActorInfo {
            name: req.name.clone(),
            actor_id: actor_id.to_vec(),
            owner_node_id: req.owner_node_id,
            owner_pid: req.owner_pid,
            registered_at_unix_ms: now_ms,
            driver_actor_host: req.driver_actor_host,
            driver_actor_port: req.driver_actor_port,
        };

        let mut guard = self.registry.actors.lock();
        if let Some(existing) = guard.get(&req.name) {
            if existing.actor_id == info.actor_id {
                // Same id re-registering: idempotent.
                return Ok(Response::new(RegisterActorReply {}));
            }
            return Err(Status::already_exists(format!(
                "actor name {:?} is already registered",
                req.name
            )));
        }
        info!(
            name = %req.name,
            actor_id = ?hex_short(&actor_id),
            owner_pid = info.owner_pid,
            "rayd-gcs: actor registered"
        );
        guard.insert(req.name, info);
        if let Some(m) = &self.metrics {
            m.actors_total.inc();
        }
        Ok(Response::new(RegisterActorReply {}))
    }

    async fn get_actor(
        &self,
        request: Request<GetActorRequest>,
    ) -> Result<Response<GetActorReply>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("name must not be empty"));
        }
        let guard = self.registry.actors.lock();
        match guard.get(&req.name) {
            None => Err(Status::not_found(format!(
                "no actor named {:?}",
                req.name
            ))),
            Some(info) => Ok(Response::new(GetActorReply {
                actor: Some(info.clone()),
            })),
        }
    }

    async fn unregister_actor(
        &self,
        request: Request<UnregisterActorRequest>,
    ) -> Result<Response<UnregisterActorReply>, Status> {
        let req = request.into_inner();
        let actor_id = parse_id(&req.actor_id, "actor_id")?;
        let mut guard = self.registry.actors.lock();
        match guard.get(&req.name) {
            None => return Err(Status::not_found("unknown actor name")),
            Some(existing) => {
                if existing.actor_id != actor_id {
                    return Err(Status::failed_precondition(
                        "actor_id does not match registered entry",
                    ));
                }
            }
        }
        guard.remove(&req.name);
        debug!(
            name = %req.name,
            actor_id = ?hex_short(&actor_id),
            "rayd-gcs: actor unregistered"
        );
        if let Some(m) = &self.metrics {
            m.actors_total.dec();
        }
        Ok(Response::new(UnregisterActorReply {}))
    }

    async fn list(
        &self,
        _request: Request<ListActorsRequest>,
    ) -> Result<Response<ListActorsReply>, Status> {
        Ok(Response::new(ListActorsReply {
            actors: self.registry.snapshot(),
        }))
    }
}

fn parse_id(bytes: &[u8], name: &str) -> Result<[u8; 16], Status> {
    if bytes.len() != 16 {
        return Err(Status::invalid_argument(format!(
            "{name} must be 16 bytes, got {}",
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
