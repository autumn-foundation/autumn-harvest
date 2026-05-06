//! Durable workflow orchestration engine core.

/// Embedded migrations for the harvest engine schema.
///
/// Downstream crates (such as `autumn-harvest-plugin`) should consume this
/// const instead of invoking `diesel_migrations::embed_migrations!()` against
/// this crate's path. Proc-macro path arguments are resolved at the *consumer's*
/// compile-time location, which doesn't survive `cargo publish` because the
/// upstream crate's directory layout isn't available in the downstream's
/// packaged tarball. Centralising the macro invocation here keeps the path
/// resolution local to this crate, where the `migrations/` directory always
/// ships alongside.
#[cfg(feature = "db")]
pub const MIGRATIONS: diesel_migrations::EmbeddedMigrations =
    diesel_migrations::embed_migrations!();

/// History analyzer and linter.
pub mod analyzer;
/// Audit trail for management API mutations (issue #158).
#[cfg(feature = "db")]
pub mod audit;
/// Batch operations for fleet-wide workflow cancel/terminate/signal (issue #102).
pub mod batch;
pub mod builder;
pub mod cache;
pub mod context;
pub mod critical_path;
pub mod dag;
/// Export format types for Directed Acyclic Graphs (DAGs) representing workflows.
pub mod dag_export;
pub mod dag_linter;
#[cfg(any(test, feature = "testing"))]
pub mod dag_simulator;
pub mod diagnostic;
pub mod error;
pub mod event;
#[cfg(feature = "db")]
#[doc(hidden)]
pub mod execution;
pub mod executor;
#[cfg(feature = "db")]
pub mod external_task;
pub mod history_export;
pub mod info;
/// `metrics` crate adapter for [`telemetry::MetricsRecorder`].
///
/// Bridges every `record_*` call to the global [`metrics`] registry so
/// applications using `metrics-exporter-prometheus` or another compatible
/// backend get harvest engine metrics without any extra glue code.
///
/// Enabled by the `metrics-rs` cargo feature.
#[cfg(feature = "metrics-rs")]
pub mod metrics_rs_adapter;
pub mod payload_codec;
pub mod policy;
pub mod pool;
pub mod prelude;
/// Types and definitions for querying workflow state and metadata.
pub mod query;
pub mod replay;
#[cfg(feature = "db")]
pub mod reset;
pub mod retention;
pub mod saga;
pub mod shard;
pub mod simulator;
/// OpenTelemetry integration: trace-context propagation and metrics.
pub mod telemetry;
#[cfg(any(test, feature = "testing"))]
pub mod test_generator;
/// Replay test harness for verifying workflow determinism pre-deploy.
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod types;
pub mod update;

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
#[cfg(feature = "db")]
pub mod workers;

pub use analyzer::{
    AnalyzerRule, AnalyzerWarning, ExcessiveRetriesRule, HistoryAnalyzer, LargePayloadRule,
    SuspiciousTimerRule,
};
pub use builder::{BuiltHarvest, HarvestBuilder, HarvestBuilderError, WorkerConfig};
pub use cache::{CachedWorkflowState, WorkflowCache};
pub use context::{ActivityContext, WorkflowCommand, WorkflowContext};
pub use critical_path::{CriticalPathAnalyzer, CriticalPathResult};
pub use dag::{DagBuildError, DagBuilder, DagDefinition, DagTask, DagTaskRef};
pub use dag_export::{export_dot, export_mermaid};
pub use dag_linter::{
    DagLinter, DagRule, DagWarning, ExcessiveParallelismRule, MissingRetryPolicyRule,
    MissingTimeoutRule,
};
#[cfg(any(test, feature = "testing"))]
pub use dag_simulator::{DagSimulator, DagSimulatorResult};
pub use diagnostic::{DiagnosticReport, SimulatorResultExt};
pub use error::{HarvestError, HarvestResult, TimeoutType};
pub use event::WorkflowEvent;
#[cfg(feature = "db")]
pub use execution::{
    CancelledWorkflowExecution, StartWorkflowParams, StartedWorkflowExecution,
    cancel_workflow_execution, start_or_load_workflow_execution, terminate_workflow_execution,
};
pub use executor::{WorkflowOutcome, run_workflow};
pub use history_export::export_mermaid_sequence;
pub use info::{ActivityHandlerFn, ActivityInfo, DagInfo, WorkflowHandlerFn, WorkflowInfo};
pub use payload_codec::{CodecError, IdentityCodec, PayloadCodec, PayloadCodecs};
pub use policy::validate_schedule;
pub use policy::{RetryPolicy, Schedule, TaskStatus, TriggerRule, WorkflowSchedule};
pub use pool::{HarvestPoolConfig, compute_pool_sizes};
pub use query::QueryRegistry;
pub use replay::{HistoryMatch, HistoryMatcher};
#[cfg(feature = "db")]
pub use reset::{
    ResetInvalidPoint, ResetPlan, ResetResult, ResetSignalReapplyPolicy, ResetUnresolvedSideEffect,
    WorkflowResetError, WorkflowResetRequest, preview_workflow_reset, reset_workflow_execution,
    validate_reset_point,
};
pub use retention::RetentionConfig;
#[cfg(feature = "db")]
pub use retention::{RetentionMonitor, RetentionRuntime, RetentionStatus, RetentionTickResult};
pub use saga::Saga;
#[cfg(feature = "db")]
pub use scheduler::{
    DagCatalog, RegisteredDag, SchedulerMonitor, SchedulerRuntime, compile_dag_catalog,
    register_schedules, register_workflow_schedules, tick_once, trigger_dag,
};
pub use shard::ShardRouter;
#[cfg(feature = "db")]
pub use shard::ShardedDbPool;
pub use simulator::{SimulatorResult, WorkflowSimulator};
pub use telemetry::{
    ActivityStatus, MetricsRecorder, NoOpMetrics, NoOpPropagator, TelemetryConfig,
    TelemetryConfigBuilder, TraceContextCarrier, TraceContextPropagator, WorkflowStatus,
};
#[cfg(any(test, feature = "testing"))]
pub use test_generator::TestHarnessGenerator;
#[cfg(any(test, feature = "testing"))]
pub use testing::{
    HistorySnapshot, NonDeterminismKind, ReplayReport, ReplayStatus, WorkflowReplayer,
};
pub use types::{
    ActivityExecId, ExecutionId, ExternalActivityToken, ShardId, TimerId, UpdateId, WorkerId,
    WorkflowId, WorkflowIdReusePolicy,
};
pub use update::UpdateRegistry;

#[cfg(feature = "db")]
pub use store::EventHistory;

#[cfg(feature = "db")]
pub use models::{AuditRecord, NewAuditRecord};

#[cfg(feature = "db")]
pub use queue::ConcurrencyKeyStats;

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
            if current_num == "0" {
                current_num.clear();
            }
            if current_num.len() > 20 {
                return None;
            }
            current_num.push(ch);
        } else if ch.is_ascii_alphabetic() {
            let num: u64 = current_num.parse().ok()?;
            current_num.clear();
            match ch {
                's' => total_secs = total_secs.checked_add(num)?,
                'm' => total_secs = total_secs.checked_add(num.checked_mul(60)?)?,
                'h' => total_secs = total_secs.checked_add(num.checked_mul(3600)?)?,
                'd' => total_secs = total_secs.checked_add(num.checked_mul(86400)?)?,
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

    #[cfg(feature = "db")]
    #[test]
    fn embedded_migrations_include_one_harvest_initial() {
        use diesel::migration::MigrationSource;
        use diesel::pg::Pg;

        let migrations =
            <diesel_migrations::EmbeddedMigrations as MigrationSource<Pg>>::migrations(
                &super::MIGRATIONS,
            )
            .expect("embedded migrations should load");
        let harvest_initial_count = migrations
            .iter()
            .filter(|migration| migration.name().to_string().ends_with("_harvest_initial"))
            .count();

        assert_eq!(harvest_initial_count, 1);
    }

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

    #[test]
    fn task_duration_rejects_overflow() {
        assert_eq!(task_duration("18446744073709551615d"), None); // u64::MAX
        assert_eq!(task_duration("18446744073709551615h"), None); // u64::MAX
        assert_eq!(task_duration("18446744073709551615m"), None); // u64::MAX
        assert_eq!(task_duration("18446744073709551614s 2s"), None); // Add overflow
    }

    #[test]
    fn havoc_task_duration_oom_prevention() {
        // Attack: Provide an excessively long string of digits.
        // The implementation rejects it early instead of boundedly growing `current_num`.
        let massive_input = "1".repeat(100);
        let result = task_duration(&massive_input);
        assert_eq!(result, None);
    }

    #[test]
    fn task_duration_allows_zero_padded_values() {
        let zero_padded = format!("{}1s", "0".repeat(50));
        assert_eq!(task_duration(&zero_padded), Some(Duration::from_secs(1)));
        // 0s is rejected globally by task_duration, so we test None
        assert_eq!(task_duration("000000000000s"), None);
        assert_eq!(task_duration("0000010m"), Some(Duration::from_secs(600)));
    }
}
