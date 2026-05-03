//! Core domain types for the rayd distributed runtime.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod core_worker;
pub mod error_info;
pub mod error_payload;
pub mod id;
pub mod log;
pub mod metadata;
pub mod object_ref;
pub mod ray_object;
pub mod recovery;
pub mod ref_counter;
pub mod store;

pub use core_worker::{
    CoreWorker, FreeCallback, SpillPolicy, DEFAULT_SPILL_BUDGET_BYTES, DEFAULT_SPILL_THRESHOLD,
};
pub use error_info::{ErrorInfo, RAW_CODE_UNSPECIFIED};
pub use error_payload::{ErrorPayload, ErrorPayloadCodecError};
pub use id::{ActorId, JobId, ObjectId, TaskId, WorkerId};
pub use log::{
    init_default_subscriber, set_event_handler, EventHandler, DEFAULT_LOG_FILTER, LOG_FILTER_ENV,
};
pub use metadata::{ErrorCategory, Metadata, RefState};
pub use object_ref::{Address, ObjectRef};
pub use ray_object::RayObject;
pub use recovery::{ObjectRecoverer, RecoveredObject, RecoveryError};
pub use ref_counter::{OwnerEntry, RefCounter};
pub use store::{MemoryStore, PlasmaIndex, StoredEntry, WaitOutcome};
