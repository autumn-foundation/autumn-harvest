//! Durable workflow orchestration engine for the Autumn web framework.

pub mod builder;
pub mod cache;
pub mod context;
pub mod error;
pub mod event;
pub mod executor;
pub mod info;
pub mod policy;
pub mod pool;
pub mod prelude;
pub mod replay;
pub mod types;

#[cfg(feature = "db")]
pub mod dlq;
#[cfg(feature = "db")]
pub mod heartbeat;
#[cfg(feature = "db")]
pub mod models;
#[cfg(feature = "db")]
pub mod notify;
#[cfg(feature = "db")]
pub mod queue;
#[cfg(feature = "db")]
#[allow(clippy::wildcard_imports)]
pub mod schema;
#[cfg(feature = "db")]
pub mod store;
#[cfg(feature = "db")]
pub mod timeout;
#[cfg(feature = "db")]
pub mod worker;

pub use builder::{HarvestBuilder, WorkerConfig};
pub use cache::{CachedWorkflowState, WorkflowCache};
pub use context::{ActivityContext, WorkflowCommand, WorkflowContext};
pub use error::{HarvestError, HarvestResult, TimeoutType};
pub use event::WorkflowEvent;
pub use executor::{WorkflowOutcome, run_workflow};
pub use info::{ActivityHandlerFn, ActivityInfo, WorkflowHandlerFn, WorkflowInfo};
pub use policy::{RetryPolicy, Schedule, TaskStatus, TriggerRule};
pub use pool::{HarvestPoolConfig, compute_pool_sizes};
pub use replay::{HistoryMatch, HistoryMatcher};
pub use types::{ActivityExecId, ExecutionId, TimerId, WorkerId, WorkflowId};

#[cfg(feature = "db")]
pub use store::EventHistory;

// Allow macro-generated code to use ::autumn_harvest::serde_json
pub use serde_json;

/// Parse a human-readable duration string like `"5m"`, `"30s"`, `"1h"`.
///
/// Used by macro-generated code — not intended for direct use.
#[doc(hidden)]
#[must_use]
pub fn task_duration(s: &str) -> Option<std::time::Duration> {
    autumn_web::task::parse_duration(s)
}
