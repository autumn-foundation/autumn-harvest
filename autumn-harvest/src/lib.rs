//! Durable workflow orchestration engine core.

pub mod builder;
pub mod cache;
pub mod context;
pub mod dag;
pub mod error;
pub mod event;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod execution;
pub mod executor;
pub mod info;
pub mod policy;
pub mod pool;
pub mod prelude;
pub mod query;
pub mod replay;
pub mod types;

#[cfg(feature = "db")]
#[doc(hidden)]
pub mod dlq;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod heartbeat;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod models;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod notify;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod queue;
#[cfg(feature = "db")]
pub mod scheduler;
#[cfg(feature = "db")]
#[allow(clippy::wildcard_imports)]
#[doc(hidden)]
pub mod schema;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod signal;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod store;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod timeout;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod worker;

pub use builder::{BuiltHarvest, HarvestBuilder, WorkerConfig};
pub use cache::{CachedWorkflowState, WorkflowCache};
pub use context::{ActivityContext, WorkflowCommand, WorkflowContext};
pub use dag::{DagBuildError, DagBuilder, DagDefinition, DagTask, DagTaskRef};
pub use error::{HarvestError, HarvestResult, TimeoutType};
pub use event::WorkflowEvent;
#[cfg(feature = "db")]
pub use execution::{
    StartWorkflowParams, StartedWorkflowExecution, start_or_load_workflow_execution,
};
pub use executor::{WorkflowOutcome, run_workflow};
pub use info::{ActivityHandlerFn, ActivityInfo, DagInfo, WorkflowHandlerFn, WorkflowInfo};
pub use policy::{RetryPolicy, Schedule, TaskStatus, TriggerRule};
pub use pool::{HarvestPoolConfig, compute_pool_sizes};
pub use query::QueryRegistry;
pub use replay::{HistoryMatch, HistoryMatcher};
#[cfg(feature = "db")]
pub use scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerRuntime, compile_dag_catalog,
    register_schedules, tick_once, trigger_dag,
};
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
    let mut total_secs = 0u64;
    let mut current_num = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current_num.push(ch);
        } else if ch.is_ascii_alphabetic() {
            let num: u64 = current_num.parse().ok()?;
            current_num.clear();
            match ch {
                's' => total_secs += num,
                'm' => total_secs += num * 60,
                'h' => total_secs += num * 3600,
                'd' => total_secs += num * 86400,
                _ => return None,
            }
        } else if ch != ' ' {
            return None;
        }
    }

    if !current_num.is_empty() || total_secs == 0 {
        return None;
    }

    Some(std::time::Duration::from_secs(total_secs))
}

#[cfg(test)]
mod tests {
    use super::task_duration;
    use std::time::Duration;

    #[test]
    fn task_duration_parses_compound_values() {
        assert_eq!(task_duration("1h 30m"), Some(Duration::from_secs(5_400)));
        assert_eq!(task_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(task_duration("2d"), Some(Duration::from_secs(172_800)));
    }

    #[test]
    fn task_duration_rejects_invalid_values() {
        assert_eq!(task_duration(""), None);
        assert_eq!(task_duration("5"), None);
        assert_eq!(task_duration("5x"), None);
    }
}
