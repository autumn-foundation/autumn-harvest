//! Fluent API for registering workflows, activities, and configuring the worker.

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::time::Duration;

use crate::context::SharedStateMap;
use crate::info::{ActivityInfo, DagInfo, WorkflowInfo};
use crate::retention::RetentionConfig;
use crate::telemetry::TelemetryConfig;
use crate::types::ShardId;

/// Fluent builder for configuring the autumn-harvest engine.
///
/// In a full Autumn app, this is consumed by `HarvestPlugin` from the
/// `autumn-harvest-plugin` crate. In tests or standalone use, call
/// `.build()` directly.
///
/// ## Examples
///
/// ```rust
/// use autumn_harvest::builder::{HarvestBuilder, WorkerConfig};
///
/// struct DatabasePool;
///
/// let built = HarvestBuilder::new()
///     .workflows(vec![]) // usually from workflows![]
///     .activities(vec![]) // usually from activities![]
///     .dags(vec![]) // usually from dags![]
///     .worker(WorkerConfig::default())
///     .state(DatabasePool)
///     .build();
///
/// assert_eq!(built.workflow_count(), 0);
/// assert!(built.state::<DatabasePool>().is_some());
/// ```
#[derive(Default)]
pub struct HarvestBuilder {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Option<TelemetryConfig>,
    retention: RetentionConfig,
}

impl std::fmt::Debug for HarvestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestBuilder")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry_configured", &self.telemetry.is_some())
            .field("retention", &self.retention)
            .finish()
    }
}

/// Built harvest registration set produced by [`HarvestBuilder::build`].
pub struct BuiltHarvest {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Arc<TelemetryConfig>,
    retention: RetentionConfig,
}

impl std::fmt::Debug for BuiltHarvest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltHarvest")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
            .field("retention", &self.retention)
            .finish()
    }
}

/// Builder-time configuration errors.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum HarvestBuilderError {
    /// Retention configuration validation failed.
    #[error("invalid retention configuration: {0}")]
    InvalidRetention(String),

    /// Two activities sharing a `concurrency_key` declare different
    /// `max_concurrent` values. There is no silent precedence rule — the
    /// operator must pick one value and apply it consistently.
    ///
    /// `activities` lists each `(activity_name, max_concurrent)` pair that
    /// contributed to the conflict.
    #[error(
        "concurrency_key '{key}' has conflicting max_concurrent values across activities: {activities:?}"
    )]
    ConcurrencyKeyMismatch {
        /// The shared concurrency key.
        key: String,
        /// Each `(activity_name, max_concurrent)` pair with a conflicting value.
        activities: Vec<(String, u32)>,
    },

    /// An activity declares a `concurrency_key` but no `max_concurrent` cap.
    /// Without a cap the key is written to the queue row but the saturation
    /// predicate (`HAVING COUNT(*) >= NULL`) never fires, silently bypassing
    /// the intended shared budget.
    #[error(
        "activity '{activity}' sets concurrency_key = \"{key}\" but has no max_concurrent; \
         either add max_concurrent or remove the concurrency_key"
    )]
    ConcurrencyKeyWithoutCap {
        /// The activity name.
        activity: String,
        /// The orphaned concurrency key.
        key: String,
    },
}

impl BuiltHarvest {
    /// Number of registered workflows.
    #[must_use]
    pub const fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub const fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAGs.
    #[must_use]
    pub const fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Access typed shared state registered on the builder.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }

    /// Worker configuration carried through the build step.
    #[must_use]
    pub const fn worker_config(&self) -> &WorkerConfig {
        &self.worker_config
    }

    /// Registered DAG metadata.
    #[must_use]
    pub fn dags(&self) -> &[DagInfo] {
        &self.dags
    }

    /// Telemetry configuration (spans propagator + metrics recorder).
    #[must_use]
    pub const fn telemetry(&self) -> &Arc<TelemetryConfig> {
        &self.telemetry
    }

    /// Retention janitor configuration.
    #[must_use]
    pub const fn retention(&self) -> &RetentionConfig {
        &self.retention
    }

    /// Convert the built harvest registration into worker-ready parts.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts(self) -> (crate::worker::HandlerRegistry, Vec<DagInfo>, WorkerConfig) {
        (
            crate::worker::HandlerRegistry::with_state_and_telemetry(
                self.workflows,
                self.activities,
                Arc::new(self.state),
                self.telemetry,
            ),
            self.dags,
            self.worker_config,
        )
    }

    /// Convert the built harvest registration into worker-ready parts while
    /// injecting additional typed runtime state.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts_with_extra_state(
        mut self,
        extra_state: SharedStateMap,
    ) -> (crate::worker::HandlerRegistry, Vec<DagInfo>, WorkerConfig) {
        self.state.extend(extra_state);
        (
            crate::worker::HandlerRegistry::with_state_and_telemetry(
                self.workflows,
                self.activities,
                Arc::new(self.state),
                self.telemetry,
            ),
            self.dags,
            self.worker_config,
        )
    }
}

impl HarvestBuilder {
    /// Create a new empty builder.
    ///
    /// This starts the fluent configuration chain for registering definitions
    /// and options before finalizing them into a [`BuiltHarvest`] or worker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register workflow definitions (output of `workflows![]` macro).
    ///
    /// The runtime uses these definitions to route executions to the correct
    /// handler functions.
    #[must_use]
    pub fn workflows(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        self.workflows.extend(workflows);
        self
    }

    /// Register activity definitions (output of `activities![]` macro).
    ///
    /// The runtime maps activity tasks to these definitions for execution.
    #[must_use]
    pub fn activities(mut self, activities: Vec<ActivityInfo>) -> Self {
        self.activities.extend(activities);
        self
    }

    /// Register DAG definitions (output of `dags![]` macro).
    ///
    /// DAGs define graphs of steps that run according to a schedule.
    #[must_use]
    pub fn dags(mut self, dags: Vec<DagInfo>) -> Self {
        self.dags.extend(dags);
        self
    }

    /// Configure the worker (concurrency, queues, timeouts).
    ///
    /// See [`WorkerConfig`] for details on adjusting poll behavior.
    #[must_use]
    pub fn worker(mut self, config: WorkerConfig) -> Self {
        self.worker_config = config;
        self
    }

    /// Register typed shared state visible to workflow and activity handlers.
    ///
    /// State injected here can be retrieved in your handlers by calling
    /// `ctx.state::<T>()`. It is useful for sharing database connection pools,
    /// email clients, or configuration structs across tasks.
    ///
    /// Registering the same type more than once replaces the previous value.
    #[must_use]
    pub fn state<T: Any + Send + Sync>(mut self, value: T) -> Self {
        self.state.insert(TypeId::of::<T>(), Box::new(value));
        self
    }

    /// Install a [`TelemetryConfig`] so the worker captures trace context at
    /// enqueue, reinstates it on claim, and emits workflow / activity / timer
    /// metrics through the supplied recorder.
    ///
    /// When unset, the runtime uses safe no-op defaults — telemetry is opt-in.
    #[must_use]
    pub fn telemetry(mut self, telemetry: TelemetryConfig) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Configure retention janitor behavior for completed workflow history.
    #[must_use]
    pub const fn retention(mut self, retention: RetentionConfig) -> Self {
        self.retention = retention;
        self
    }

    /// Number of registered workflows (used in tests and diagnostics).
    #[must_use]
    pub const fn workflow_count(&self) -> usize {
        self.workflows.len()
    }

    /// Number of registered activities.
    #[must_use]
    pub const fn activity_count(&self) -> usize {
        self.activities.len()
    }

    /// Number of registered DAG definitions.
    #[must_use]
    pub const fn dag_count(&self) -> usize {
        self.dags.len()
    }

    /// Finalize the builder into a reusable harvest registration set.
    ///
    /// # Panics
    ///
    /// Panics when retention settings are invalid. Prefer [`Self::try_build`]
    /// if you want startup errors instead.
    #[must_use]
    pub fn build(self) -> BuiltHarvest {
        self.try_build()
            .expect("HarvestBuilder::build failed validation")
    }

    /// Finalize the builder into a reusable harvest registration set.
    ///
    /// # Errors
    ///
    /// Returns [`HarvestBuilderError`] when retention settings are invalid or
    /// when activities sharing a `concurrency_key` declare different
    /// `max_concurrent` values.
    pub fn try_build(self) -> Result<BuiltHarvest, HarvestBuilderError> {
        self.retention
            .validate()
            .map_err(HarvestBuilderError::InvalidRetention)?;

        validate_concurrency_keys(&self.activities)?;

        Ok(BuiltHarvest {
            workflows: self.workflows,
            activities: self.activities,
            dags: self.dags,
            worker_config: self.worker_config,
            state: self.state,
            telemetry: Arc::new(self.telemetry.unwrap_or_default()),
            retention: self.retention,
        })
    }
}

/// Entry in the concurrency-key deduplication map.
struct ConcurrencyKeyEntry {
    first_cap: u32,
    contributors: Vec<(String, u32)>,
}

/// Verify that all activities sharing a `concurrency_key` agree on
/// `max_concurrent`. Fails fast with [`HarvestBuilderError::ConcurrencyKeyMismatch`]
/// if any disagreement is found.
fn validate_concurrency_keys(
    activities: &[crate::info::ActivityInfo],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashMap;

    let mut seen: HashMap<&str, ConcurrencyKeyEntry> = HashMap::new();

    for activity in activities {
        // concurrency_key without max_concurrent silently bypasses the cap — reject it.
        if let (Some(key), None) = (activity.concurrency_key, activity.max_concurrent) {
            return Err(HarvestBuilderError::ConcurrencyKeyWithoutCap {
                activity: activity.name.to_string(),
                key: key.to_string(),
            });
        }

        let (Some(key), Some(cap)) = (activity.concurrency_key, activity.max_concurrent) else {
            continue;
        };
        let entry = seen.entry(key).or_insert_with(|| ConcurrencyKeyEntry {
            first_cap: cap,
            contributors: Vec::new(),
        });
        entry.contributors.push((activity.name.to_string(), cap));

        if entry.first_cap != cap {
            return Err(HarvestBuilderError::ConcurrencyKeyMismatch {
                key: key.to_string(),
                activities: entry.contributors.clone(),
            });
        }
    }

    Ok(())
}

/// Worker concurrency and queue configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queues this worker polls. Defaults to `["default"]`.
    pub queues: Vec<String>,
    /// Optional Postgres URL for LISTEN/NOTIFY wakeups.
    pub notification_database_url: Option<String>,
    /// Maximum concurrent workflow executions on this worker.
    pub max_concurrent_workflows: usize,
    /// Maximum concurrent activity executions on this worker.
    pub max_concurrent_activities: usize,
    /// Graceful shutdown timeout.
    pub shutdown_timeout: Duration,
    /// Maximum cached in-memory workflow states (LRU eviction).
    pub workflow_cache_size: usize,
    /// How long to offer sticky tasks to the sticky worker before fallback.
    pub sticky_timeout: Duration,
    /// Grace period for an activity to finish cooperatively after its workflow
    /// is cancelled before the worker hard-aborts the handler task. Cancellation
    /// is cooperative -- activities should poll [`crate::context::ActivityContext::is_cancelled`]
    /// or call [`crate::context::ActivityContext::heartbeat`], but an uncooperative handler must
    /// not block a worker slot indefinitely.
    pub cancellation_grace_period: Duration,
    /// Shards this worker is responsible for polling.
    ///
    /// Defaults to `[ShardId::new(0)]`, matching the single-shard deployment
    /// shape. Multi-shard operators typically run one worker process per
    /// shard with `shard_assignments = vec![that_shard]`, but the field is
    /// a `Vec` so future per-process multi-shard workers can list all shards
    /// they should poll without changing the config surface.
    pub shard_assignments: Vec<ShardId>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            queues: vec!["default".to_string()],
            notification_database_url: None,
            max_concurrent_workflows: 20,
            max_concurrent_activities: 50,
            shutdown_timeout: Duration::from_secs(30),
            workflow_cache_size: 1000,
            sticky_timeout: Duration::from_secs(5),
            cancellation_grace_period: Duration::from_secs(5),
            shard_assignments: vec![ShardId::new(0)],
        }
    }
}

impl WorkerConfig {
    /// Replace the queue list.
    ///
    /// # Panics
    ///
    /// Panics if any of the provided queue names are empty strings.
    #[must_use]
    pub fn with_queues<'a>(mut self, queues: impl IntoIterator<Item = &'a str>) -> Self {
        self.queues = queues
            .into_iter()
            .map(|q| {
                assert!(!q.is_empty(), "queue name cannot be empty");
                q.to_owned()
            })
            .collect();
        self
    }

    /// Enable LISTEN/NOTIFY wakeups using a dedicated Postgres connection.
    #[must_use]
    pub fn with_notification_database_url(mut self, database_url: impl Into<String>) -> Self {
        self.notification_database_url = Some(database_url.into());
        self
    }

    /// Override the cancellation grace period.
    ///
    /// After a workflow is cancelled, any running activity gets this long to
    /// notice cooperative cancellation (via [`crate::context::ActivityContext::is_cancelled`]
    /// or [`crate::context::ActivityContext::heartbeat`]) and unwind cleanly. If it is still
    /// running at the end of the grace period the worker aborts the handler
    /// task and marks the activity as cancelled.
    #[must_use]
    pub const fn with_cancellation_grace_period(mut self, grace_period: Duration) -> Self {
        self.cancellation_grace_period = grace_period;
        self
    }

    /// Assign which shards this worker is responsible for.
    ///
    /// Empty assignments default back to `[ShardId::new(0)]` to preserve the
    /// single-shard behaviour.
    #[must_use]
    pub fn with_shard_assignments(mut self, shards: impl IntoIterator<Item = ShardId>) -> Self {
        let shards: Vec<ShardId> = shards.into_iter().collect();
        self.shard_assignments = if shards.is_empty() {
            vec![ShardId::new(0)]
        } else {
            shards
        };
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;
    use crate::info::{DagInfo, WorkflowInfo};
    use crate::policy::Schedule;

    fn fake_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            name: "test",
            module: "test",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn fake_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
        }
    }

    #[test]
    fn harvest_builder_collects_workflows() {
        let builder = HarvestBuilder::new().workflows(vec![fake_workflow_info()]);
        assert_eq!(builder.workflow_count(), 1);
    }

    #[test]
    fn worker_config_default_queues() {
        let config = WorkerConfig::default();
        assert!(config.queues.contains(&"default".to_string()));
        assert!(config.notification_database_url.is_none());
    }

    #[test]
    fn worker_config_builder_adds_queues() {
        let config = WorkerConfig::default().with_queues(["email-workers", "etl"]);
        assert!(config.queues.contains(&"email-workers".to_string()));
    }

    #[test]
    fn worker_config_with_empty_queues_clears_list() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }

    #[test]
    fn worker_config_builder_sets_notification_database_url() {
        let config =
            WorkerConfig::default().with_notification_database_url("postgres://localhost/test");
        assert_eq!(
            config.notification_database_url.as_deref(),
            Some("postgres://localhost/test")
        );
    }

    #[test]
    fn harvest_builder_collects_dags() {
        let builder = HarvestBuilder::new().dags(vec![fake_dag_info()]);
        assert_eq!(builder.dag_count(), 1);
    }

    #[test]
    fn harvest_builder_build_registers_shared_state() {
        let built = HarvestBuilder::new().state(String::from("hello")).build();

        assert_eq!(built.workflow_count(), 0);
        assert_eq!(built.activity_count(), 0);
        assert_eq!(built.dag_count(), 0);
        assert_eq!(built.state::<String>(), Some(&String::from("hello")));
        assert!(built.state::<u64>().is_none());
    }

    #[test]
    fn harvest_builder_build_defaults_telemetry_to_noop() {
        let built = HarvestBuilder::new().build();
        // Default is a safe no-op: capturing yields nothing.
        assert!(built.telemetry().capture_trace_context().is_none());
    }

    #[test]
    fn harvest_builder_telemetry_override_is_propagated() {
        use crate::telemetry::{TelemetryConfig, TraceContextCarrier, TraceContextPropagator};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct StubProp {
            captured: AtomicUsize,
        }
        impl TraceContextPropagator for StubProp {
            fn capture(&self) -> Option<TraceContextCarrier> {
                self.captured.fetch_add(1, Ordering::SeqCst);
                Some(TraceContextCarrier::from_traceparent("00-f00-b44-01"))
            }
            fn install(&self, _carrier: &TraceContextCarrier) -> Box<dyn Any + Send> {
                Box::new(())
            }
        }

        let prop = std::sync::Arc::new(StubProp::default());
        let built = HarvestBuilder::new()
            .telemetry(TelemetryConfig::builder().propagator(prop.clone()).build())
            .build();

        assert_eq!(prop.captured.load(Ordering::SeqCst), 0);
        let carrier = built.telemetry().capture_trace_context().unwrap();
        assert_eq!(carrier.traceparent.as_deref(), Some("00-f00-b44-01"));
        assert_eq!(prop.captured.load(Ordering::SeqCst), 1);
    }

    #[cfg(feature = "db")]
    #[test]
    fn built_harvest_into_worker_parts_preserves_shared_state() {
        let built = HarvestBuilder::new()
            .workflows(vec![fake_workflow_info()])
            .activities(vec![ActivityInfo {
                name: "test_activity",
                module: "test",
                default_retry_policy: None,
                default_start_to_close: None,
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_queue: None,
                max_concurrent: None,
                concurrency_key: None,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }])
            .state(String::from("haunted"))
            .build();

        let (registry, _dags, worker_config) = built.into_worker_parts();

        assert_eq!(registry.state::<String>(), Some(&String::from("haunted")));
        assert!(worker_config.queues.contains(&"default".to_string()));
    }

    #[test]
    #[should_panic(expected = "queue name cannot be empty")]
    fn worker_config_with_empty_queue_name_panics() {
        let _config = WorkerConfig::default().with_queues(["", "default"]);
    }

    #[test]
    fn worker_config_with_empty_iterator_clears_queues() {
        let config = WorkerConfig::default().with_queues(Vec::<&str>::new());
        assert!(config.queues.is_empty());
    }

    fn make_activity(name: &'static str, max_concurrent: Option<u32>, key: Option<&'static str>) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "test",
            default_retry_policy: None,
            default_start_to_close: None,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            max_concurrent,
            concurrency_key: key,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    #[test]
    fn builder_accepts_matching_concurrency_key_caps() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), Some("stripe")),
                make_activity("act_b", Some(5), Some("stripe")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_mismatched_concurrency_key_caps() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), Some("stripe")),
                make_activity("act_b", Some(10), Some("stripe")),
            ])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ConcurrencyKeyMismatch { ref key, .. } if key == "stripe"
        ));
        assert!(err.to_string().contains("stripe"));
    }

    #[test]
    fn builder_accepts_activities_without_concurrency_key() {
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", None, None),
                make_activity("act_b", Some(3), Some("sendgrid")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_concurrency_key_without_cap() {
        // concurrency_key set but max_concurrent omitted — the cap predicate
        // would silently never fire (HAVING COUNT(*) >= NULL is always unknown).
        let result = HarvestBuilder::new()
            .activities(vec![make_activity("act_a", None, Some("stripe"))])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ConcurrencyKeyWithoutCap { ref activity, ref key }
                if activity == "act_a" && key == "stripe"
        ));
        assert!(err.to_string().contains("act_a"));
        assert!(err.to_string().contains("stripe"));
    }
}
