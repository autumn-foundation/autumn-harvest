//! Fluent API for registering workflows, activities, and configuring the worker.

use std::any::{Any, TypeId};
use std::sync::Arc;
use std::time::Duration;

use crate::context::{SharedStateMap, WorkflowHistoryPolicy};
use crate::info::{ActivityInfo, DagInfo, WorkflowInfo};
use crate::payload_codec::{PayloadCodec, PayloadCodecs};
use crate::policy::WorkflowSchedule;
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
    workflow_schedules: Vec<WorkflowSchedule>,
    auto_registered_dag_workflows: Vec<String>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Option<TelemetryConfig>,
    retention: RetentionConfig,
    payload_codecs: PayloadCodecs,
    history_policy: WorkflowHistoryPolicy,
}

impl std::fmt::Debug for HarvestBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HarvestBuilder")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("workflow_schedule_count", &self.workflow_schedules.len())
            .field(
                "auto_registered_dag_workflow_count",
                &self.auto_registered_dag_workflows.len(),
            )
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry_configured", &self.telemetry.is_some())
            .field("retention", &self.retention)
            .field("payload_codecs", &"configured")
            .field("history_policy", &self.history_policy)
            .finish()
    }
}

/// Built harvest registration set produced by [`HarvestBuilder::build`].
pub struct BuiltHarvest {
    workflows: Vec<WorkflowInfo>,
    activities: Vec<ActivityInfo>,
    dags: Vec<DagInfo>,
    workflow_schedules: Vec<WorkflowSchedule>,
    worker_config: WorkerConfig,
    state: SharedStateMap,
    telemetry: Arc<TelemetryConfig>,
    retention: RetentionConfig,
    payload_codecs: PayloadCodecs,
    history_policy: WorkflowHistoryPolicy,
}

impl std::fmt::Debug for BuiltHarvest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BuiltHarvest")
            .field("workflow_count", &self.workflows.len())
            .field("activity_count", &self.activities.len())
            .field("dag_count", &self.dags.len())
            .field("workflow_schedule_count", &self.workflow_schedules.len())
            .field("worker_config", &self.worker_config)
            .field("state_count", &self.state.len())
            .field("telemetry", &self.telemetry)
            .field("retention", &self.retention)
            .field("payload_codecs", &"configured")
            .field("history_policy", &self.history_policy)
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
    /// predicate (`(SELECT COUNT(*) ...) < NULL`) is always null/unknown,
    /// silently bypassing the intended shared budget.
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

    /// An activity declares `max_concurrent = 0`, which makes the saturation
    /// check `(SELECT COUNT(*) ...) < 0` always false, permanently deferring
    /// every task for this activity.
    #[error(
        "activity '{activity}' has max_concurrent = 0; use max_concurrent >= 1 \
         or omit max_concurrent entirely to disable the cap"
    )]
    ZeroConcurrencyCap {
        /// The activity name.
        activity: String,
    },

    /// A [`WorkflowSchedule`] names a workflow that was not registered via
    /// `workflows![]`. The schedule is rejected at build time so the operator
    /// sees a clear error rather than silent no-ops at scheduler tick time.
    ///
    /// `workflow_name` is the name that was not found. `registered` lists every
    /// workflow name that was actually registered on this builder.
    #[error(
        "workflow_schedule references unknown workflow '{workflow_name}'; \
         registered workflows: {registered:?}"
    )]
    UnknownWorkflowSchedule {
        /// The unrecognised workflow name in the schedule.
        workflow_name: String,
        /// All workflow names currently registered on the builder.
        registered: Vec<String>,
    },

    /// A local activity declares a `start_to_close` that exceeds the worker's
    /// `max_local_activity_start_to_close` cap. Local activities run inline on
    /// the workflow worker and must not block it indefinitely.
    #[error(
        "local activity '{activity}' start_to_close ({actual:?}) exceeds the worker cap \
         ({cap:?}); lower start_to_close or raise WorkerConfig::max_local_activity_start_to_close"
    )]
    LocalActivityStartToCloseExceedsCap {
        /// The local activity name.
        activity: String,
        /// The declared `start_to_close` on the activity.
        actual: Duration,
        /// The configured worker cap.
        cap: Duration,
    },

    /// A [`WorkflowSchedule`] contains an invalid schedule value (malformed cron
    /// expression, zero-length interval, etc.). Caught at build time so the
    /// operator sees a clear error rather than silently-inert or wedging schedules.
    #[error("workflow_schedule for '{workflow_name}' has an invalid schedule: {reason}")]
    InvalidWorkflowSchedule {
        /// The workflow name whose schedule is invalid.
        workflow_name: String,
        /// Human-readable reason the schedule was rejected.
        reason: String,
    },

    /// A normal workflow registration reused the name of a DAG that is
    /// auto-registered as a workflow for unified DAG execution.
    #[error(
        "workflow name '{name}' collides with an auto-registered DAG workflow; \
         register workflows and DAGs with distinct names"
    )]
    DagWorkflowNameCollision {
        /// The shared workflow/DAG name.
        name: String,
    },

    /// A DAG references an activity registered as local-only. Local activities
    /// run inline on the workflow worker and cannot be scheduled through the
    /// DAG activity queue lowering.
    #[error(
        "DAG '{dag}' references local activity '{activity}'; local activities cannot be used in DAG definitions"
    )]
    LocalActivityInDag {
        /// DAG containing the local activity task.
        dag: String,
        /// Local activity referenced by the DAG.
        activity: String,
    },

    /// A [`WorkerConfig`] field has an invalid value.
    #[error("invalid worker configuration: {0}")]
    InvalidWorkerConfig(String),
}

impl BuiltHarvest {
    #[must_use]
    pub const fn payload_codecs(&self) -> &PayloadCodecs {
        &self.payload_codecs
    }

    /// History-size guardrails applied to workflow contexts and workers.
    #[must_use]
    pub const fn history_policy(&self) -> WorkflowHistoryPolicy {
        self.history_policy
    }

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

    /// Number of registered workflow schedules.
    #[must_use]
    pub const fn workflow_schedule_count(&self) -> usize {
        self.workflow_schedules.len()
    }

    /// Registered workflow schedules.
    #[must_use]
    pub fn workflow_schedules(&self) -> &[WorkflowSchedule] {
        &self.workflow_schedules
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

    /// Override the audit log retention window after the build step.
    ///
    /// Use this to apply a runtime-configured value (e.g. from `HarvestApiState`)
    /// without rebuilding the entire harvest configuration.
    pub const fn set_audit_retention_days(&mut self, days: i64) {
        self.retention.audit_retention_days = days;
    }

    /// Convert the built harvest registration into worker-ready parts.
    #[cfg(feature = "db")]
    #[must_use]
    pub fn into_worker_parts(
        self,
    ) -> (
        crate::worker::HandlerRegistry,
        Vec<DagInfo>,
        Vec<WorkflowSchedule>,
        WorkerConfig,
    ) {
        (
            crate::worker::HandlerRegistry::with_state_and_telemetry(
                self.workflows,
                self.activities,
                Arc::new(self.state),
                self.telemetry,
            )
            .with_history_policy(self.history_policy),
            self.dags,
            self.workflow_schedules,
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
    ) -> (
        crate::worker::HandlerRegistry,
        Vec<DagInfo>,
        Vec<WorkflowSchedule>,
        WorkerConfig,
    ) {
        self.state.extend(extra_state);
        (
            crate::worker::HandlerRegistry::with_state_and_telemetry(
                self.workflows,
                self.activities,
                Arc::new(self.state),
                self.telemetry,
            )
            .with_history_policy(self.history_policy),
            self.dags,
            self.workflow_schedules,
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
    ///
    /// When the `unified-dag-execution` feature is enabled every DAG whose
    /// `workflow_handler` is populated (i.e. produced by the `#[dag]` macro
    /// with that feature on) is also auto-registered as a [`WorkflowInfo`] and,
    /// if it carries a schedule attribute, as a [`WorkflowSchedule`]. This
    /// wires unified DAGs into the standard workflow execution and scheduler
    /// paths without requiring separate `.workflow_schedule(...)` calls.
    #[must_use]
    pub fn dags(mut self, dags: Vec<DagInfo>) -> Self {
        for dag in dags {
            #[cfg(feature = "unified-dag-execution")]
            {
                if let Some(workflow_info) = dag.as_workflow_info() {
                    self.auto_registered_dag_workflows
                        .push(workflow_info.name.to_string());
                    self.workflows.push(workflow_info);
                }
                if let Some(workflow_schedule) = dag.as_workflow_schedule() {
                    self.workflow_schedules.push(workflow_schedule);
                }
            }
            self.dags.push(dag);
        }
        self
    }

    /// Register a per-workflow cron/interval schedule.
    ///
    /// The referenced `workflow_name` must appear in a prior (or subsequent)
    /// `.workflows(workflows![...])` call. [`Self::try_build`] validates this
    /// and returns [`HarvestBuilderError::UnknownWorkflowSchedule`] if the
    /// workflow is missing.
    ///
    /// ## Examples
    ///
    /// ```rust
    /// use autumn_harvest::builder::{HarvestBuilder, HarvestBuilderError};
    /// use autumn_harvest::policy::{Schedule, WorkflowSchedule};
    ///
    /// // Referencing an unregistered workflow name is caught at try_build time.
    /// let result = HarvestBuilder::new()
    ///     .workflow_schedule(
    ///         WorkflowSchedule::new("daily_billing_report", Schedule::Cron("0 3 * * *".to_string()))
    ///     )
    ///     .try_build();
    /// assert!(matches!(result, Err(HarvestBuilderError::UnknownWorkflowSchedule { .. })));
    /// ```
    #[must_use]
    pub fn workflow_schedule(mut self, schedule: WorkflowSchedule) -> Self {
        self.workflow_schedules.push(schedule);
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
    pub fn payload_codec(mut self, codec: impl PayloadCodec + 'static) -> Self {
        self.payload_codecs.set_default(Arc::new(codec));
        self
    }

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

    /// Override the soft history-size threshold used by
    /// [`crate::context::WorkflowContext::should_continue_as_new`].
    #[must_use]
    pub const fn history_continue_as_new_threshold(mut self, threshold: u64) -> Self {
        self.history_policy = self
            .history_policy
            .with_continue_as_new_threshold(threshold);
        self
    }

    /// Configure an opt-in hard cap for workflow history event counts.
    #[must_use]
    pub const fn history_event_hard_cap(mut self, cap: u64) -> Self {
        self.history_policy = self.history_policy.with_event_hard_cap(cap);
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

    /// Number of registered workflow schedules.
    #[must_use]
    pub const fn workflow_schedule_count(&self) -> usize {
        self.workflow_schedules.len()
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
    /// Returns [`HarvestBuilderError`] when retention settings are invalid,
    /// when activities sharing a `concurrency_key` declare different
    /// `max_concurrent` values, or when a [`WorkflowSchedule`] references a
    /// workflow name not registered on this builder.
    pub fn try_build(self) -> Result<BuiltHarvest, HarvestBuilderError> {
        self.retention
            .validate()
            .map_err(HarvestBuilderError::InvalidRetention)?;

        if self.worker_config.worker_heartbeat_interval.is_zero() {
            return Err(HarvestBuilderError::InvalidWorkerConfig(
                "worker_heartbeat_interval must be greater than zero".to_string(),
            ));
        }

        validate_concurrency_keys(&self.activities)?;
        validate_dag_workflow_name_collisions(
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;
        validate_workflow_schedules(
            &self.workflow_schedules,
            &self.workflows,
            &self.auto_registered_dag_workflows,
        )?;
        validate_local_activity_timeouts(
            &self.activities,
            self.worker_config.max_local_activity_start_to_close,
        )?;
        validate_dags_do_not_use_local_activities(&self.dags, &self.activities)?;

        Ok(BuiltHarvest {
            workflows: self.workflows,
            activities: self.activities,
            dags: self.dags,
            workflow_schedules: self.workflow_schedules,
            worker_config: self.worker_config,
            state: self.state,
            telemetry: Arc::new(self.telemetry.unwrap_or_default()),
            retention: self.retention,
            payload_codecs: self.payload_codecs.clone(),
            history_policy: self.history_policy,
        })
    }
}

fn validate_dags_do_not_use_local_activities(
    dags: &[DagInfo],
    activities: &[ActivityInfo],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashSet;

    let local_activities = activities
        .iter()
        .filter(|activity| activity.is_local)
        .map(|activity| activity.name)
        .collect::<HashSet<_>>();
    if local_activities.is_empty() || dags.is_empty() {
        return Ok(());
    }

    for dag in dags {
        let Ok(definition) = dag.build_definition() else {
            continue;
        };
        for task in definition.tasks() {
            if local_activities.contains(task.activity_name.as_str()) {
                return Err(HarvestBuilderError::LocalActivityInDag {
                    dag: dag.name.to_string(),
                    activity: task.activity_name.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Verify that unified DAG auto-registration does not overwrite or get
/// overwritten by a normal workflow with the same name.
fn validate_dag_workflow_name_collisions(
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    use std::collections::HashMap;

    if auto_registered_dag_workflows.is_empty() {
        return Ok(());
    }

    let mut auto_counts: HashMap<&str, usize> = HashMap::new();
    for name in auto_registered_dag_workflows {
        *auto_counts.entry(name.as_str()).or_default() += 1;
    }

    let mut workflow_counts: HashMap<&str, usize> = HashMap::new();
    for workflow in workflows {
        *workflow_counts.entry(workflow.name).or_default() += 1;
    }

    for (name, auto_count) in auto_counts {
        if workflow_counts.get(name).copied().unwrap_or_default() > auto_count {
            return Err(HarvestBuilderError::DagWorkflowNameCollision {
                name: name.to_string(),
            });
        }
    }

    Ok(())
}

/// Verify that every [`WorkflowSchedule`] references a workflow name that is
/// actually registered on the builder. Fails fast with
/// [`HarvestBuilderError::UnknownWorkflowSchedule`] on the first mismatch.
fn validate_workflow_schedules(
    schedules: &[WorkflowSchedule],
    workflows: &[crate::info::WorkflowInfo],
    auto_registered_dag_workflows: &[String],
) -> Result<(), HarvestBuilderError> {
    if schedules.is_empty() {
        return Ok(());
    }
    let registered: Vec<String> = workflows.iter().map(|w| w.name.to_string()).collect();
    for schedule in schedules {
        if !registered.contains(&schedule.workflow_name) {
            return Err(HarvestBuilderError::UnknownWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                registered,
            });
        }
        if schedule.dag_name.is_none()
            && auto_registered_dag_workflows
                .iter()
                .any(|dag_name| dag_name == &schedule.workflow_name)
        {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                reason: "workflow schedule targets an auto-registered DAG workflow; use the DAG schedule registration instead".to_string(),
            });
        }
        // Reject zero-length intervals (would cause infinite loops in due_run_plan
        // with catchup=true) and invalid cron expressions (would silently never fire).
        if let crate::policy::Schedule::Interval(dur) = &schedule.schedule {
            if dur.is_zero() {
                return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                    workflow_name: schedule.workflow_name.clone(),
                    reason: "interval must be at least 1 second".to_string(),
                });
            }
        } else if let Err(reason) = crate::policy::validate_schedule(&schedule.schedule) {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                reason,
            });
        }
        if let Err(reason) = crate::policy::validate_jitter(&schedule.schedule, schedule.jitter) {
            return Err(HarvestBuilderError::InvalidWorkflowSchedule {
                workflow_name: schedule.workflow_name.clone(),
                reason,
            });
        }
    }
    Ok(())
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
        // max_concurrent = 0 makes the cap predicate always-true, permanently
        // deferring every task for that activity. Reject at build time.
        if activity.max_concurrent == Some(0) {
            return Err(HarvestBuilderError::ZeroConcurrencyCap {
                activity: activity.name.to_string(),
            });
        }

        // concurrency_key without max_concurrent silently bypasses the cap — reject it.
        if let (Some(key), None) = (activity.concurrency_key, activity.max_concurrent) {
            return Err(HarvestBuilderError::ConcurrencyKeyWithoutCap {
                activity: activity.name.to_string(),
                key: key.to_string(),
            });
        }

        // Activities with max_concurrent but no explicit concurrency_key use the
        // activity name as the effective key at runtime (persist_scheduled_activity
        // defaults it). Include them in the cross-activity cap consistency check.
        let Some(cap) = activity.max_concurrent else {
            continue;
        };
        let effective_key: &str = activity.concurrency_key.unwrap_or(activity.name);
        let entry = seen
            .entry(effective_key)
            .or_insert_with(|| ConcurrencyKeyEntry {
                first_cap: cap,
                contributors: Vec::new(),
            });
        entry.contributors.push((activity.name.to_string(), cap));

        if entry.first_cap != cap {
            return Err(HarvestBuilderError::ConcurrencyKeyMismatch {
                key: effective_key.to_string(),
                activities: entry.contributors.clone(),
            });
        }
    }

    Ok(())
}

/// Reject local activities whose `default_start_to_close` exceeds the worker
/// cap. Failing early gives operators a clear error instead of a runtime surprise.
fn validate_local_activity_timeouts(
    activities: &[crate::info::ActivityInfo],
    cap: Duration,
) -> Result<(), HarvestBuilderError> {
    for activity in activities {
        if !activity.is_local {
            continue;
        }
        if activity.default_start_to_close.is_some_and(|stc| stc > cap) {
            return Err(HarvestBuilderError::LocalActivityStartToCloseExceedsCap {
                activity: activity.name.to_string(),
                actual: activity.default_start_to_close.unwrap(),
                cap,
            });
        }
    }
    Ok(())
}

/// Configuration for sticky cross-worker routing (issue #235).
///
/// Sticky routing keeps follow-up tasks for a workflow execution on the worker
/// that already has that execution's event history in its in-process LRU cache,
/// reducing cold event-history reloads from Postgres.
///
/// Sticky routing is **off by default**. Enable it via
/// [`WorkerConfig::with_sticky_routing`].
///
/// ## Trade-offs
///
/// | Parameter | Short TTL | Long TTL |
/// |-----------|-----------|----------|
/// | Cache hit rate | Lower (sticky window may expire before follow-up arrives) | Higher |
/// | Failover latency | Fast (expired window → any eligible worker claims) | Slower |
/// | Load distribution | Better (sticky windows expire quickly) | Skewed toward hot workers |
///
/// A 5–30 second `lease_ttl` is a reasonable starting point for most
/// deployments. See `docs/sticky-routing.md` for the full operator guide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StickyRoutingConfig {
    /// How long to prefer the owning worker for follow-up tasks after a
    /// workflow suspends.
    ///
    /// The task queue will offer tasks whose workflow has an active,
    /// unexpired sticky lease to the owning worker before any other eligible
    /// worker can claim them. Once the window elapses the task becomes
    /// claimable by any eligible worker (safe failover).
    pub lease_ttl: Duration,
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
    /// Hard cap on `start_to_close` for local activities.
    ///
    /// Local activities run inline on the workflow worker task. An unbounded
    /// timeout would block the worker indefinitely. Defaults to **60 seconds**.
    /// Any local activity registered with `start_to_close > cap` is rejected
    /// at builder `try_build()` time.
    pub max_local_activity_start_to_close: Duration,
    /// How often the worker upserts its liveness row in `harvest_workers`.
    /// Defaults to **5 seconds**. The API classifies a worker as stale after
    /// `2 × worker_heartbeat_interval` without a heartbeat.
    pub worker_heartbeat_interval: Duration,
    /// Immutable build identifier for this worker binary (issue #171).
    ///
    /// Set to a stable per-build token (Git SHA, semver tag, CI job ID, etc.)
    /// to enable build-aware task routing. Empty string = legacy behaviour
    /// where the worker can claim any task regardless of `required_build_id`.
    pub build_id: String,
    /// Optional human-readable deployment name for operator observability
    /// (issue #171), e.g. `"prod-blue"` or `"canary"`.
    pub deployment_name: Option<String>,
    /// Per-query execution timeout (issue #234).
    ///
    /// When a query handler takes longer than this to complete, the engine
    /// terminates the handler and returns [`HarvestError::QueryTimedOut`] to
    /// the caller. Defaults to **5 seconds**.
    pub query_timeout: Duration,
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
            sticky_timeout: Duration::ZERO,
            cancellation_grace_period: Duration::from_secs(5),
            shard_assignments: vec![ShardId::new(0)],
            max_local_activity_start_to_close: Duration::from_secs(60),
            worker_heartbeat_interval: Duration::from_secs(5),
            build_id: String::new(),
            deployment_name: None,
            query_timeout: Duration::from_secs(5),
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

    /// Override the worker heartbeat interval (default 5 s).
    ///
    /// The management API classifies a worker as stale after
    /// `2 × worker_heartbeat_interval` without a heartbeat write.
    #[must_use]
    pub const fn with_worker_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.worker_heartbeat_interval = interval;
        self
    }

    /// Set the immutable build identifier for this worker (issue #171).
    ///
    /// Use a stable per-build token — a Git SHA, semver tag, or CI job ID.
    /// Workers without a build ID (the default empty string) behave as legacy
    /// workers and can claim any task regardless of build routing policy.
    #[must_use]
    pub fn with_build_id(mut self, build_id: impl Into<String>) -> Self {
        self.build_id = build_id.into();
        self
    }

    /// Set an optional human-readable deployment name (issue #171).
    ///
    /// For operator observability only — e.g. `"prod-blue"`, `"canary"`.
    /// Harvest does not use the deployment name for routing decisions.
    #[must_use]
    pub fn with_deployment_name(mut self, name: impl Into<String>) -> Self {
        self.deployment_name = Some(name.into());
        self
    }

    /// Override the per-query execution timeout (default 5 s, issue #234).
    ///
    /// When a query handler takes longer than this to complete, the engine
    /// terminates the handler and returns [`crate::error::HarvestError::QueryTimedOut`]
    /// to the caller.
    #[must_use]
    pub const fn with_query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Enable sticky cross-worker routing (issue #235).
    ///
    /// Sticky routing is **off by default**. When enabled, each time a workflow
    /// suspends the task queue records a soft affinity lease pointing at the
    /// current worker. Subsequent tasks for that execution are offered to the
    /// owning worker first so its in-process LRU cache stays warm, reducing
    /// full event-history reloads from Postgres.
    ///
    /// When the lease expires (after `config.lease_ttl`) the task becomes
    /// claimable by any eligible worker — sticky routing never blocks progress.
    /// Note: worker drain or unhealthy status does **not** trigger early lease
    /// expiry; only the TTL controls when other workers can claim the task.
    ///
    /// See `docs/sticky-routing.md` for the full operator guide including
    /// the lease-TTL trade-off and interaction with shard assignments and
    /// build-id routing.
    ///
    /// # Example
    ///
    /// ```rust
    /// use autumn_harvest::builder::{StickyRoutingConfig, WorkerConfig};
    /// use std::time::Duration;
    ///
    /// let config = WorkerConfig::default()
    ///     .with_sticky_routing(StickyRoutingConfig {
    ///         lease_ttl: Duration::from_secs(10),
    ///     });
    /// ```
    #[must_use]
    pub const fn with_sticky_routing(mut self, config: StickyRoutingConfig) -> Self {
        self.sticky_timeout = config.lease_ttl;
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
            workflow_handler: None,

            jitter: ::std::time::Duration::ZERO,
        }
    }

    #[cfg(feature = "unified-dag-execution")]
    fn fake_unified_dag_info() -> DagInfo {
        fn build(_dag: &mut DagBuilder) {}

        DagInfo {
            name: "daily_etl",
            module: "test",
            schedule: Some(Schedule::Manual),
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("default"),
            builder: build,
            workflow_handler: Some(|_ctx, input| Box::pin(async move { Ok(input) })),
            jitter: ::std::time::Duration::ZERO,
        }
    }

    #[test]
    fn harvest_builder_collects_workflows() {
        let builder = HarvestBuilder::new().workflows(vec![fake_workflow_info()]);
        assert_eq!(builder.workflow_count(), 1);
    }

    #[test]
    fn worker_heartbeat_interval_defaults_to_5s() {
        assert_eq!(
            WorkerConfig::default().worker_heartbeat_interval,
            Duration::from_secs(5)
        );
    }

    #[test]
    fn worker_heartbeat_interval_zero_is_rejected() {
        let result = HarvestBuilder::new()
            .worker(WorkerConfig::default().with_worker_heartbeat_interval(Duration::ZERO))
            .try_build();
        assert!(
            matches!(result, Err(HarvestBuilderError::InvalidWorkerConfig(_))),
            "expected InvalidWorkerConfig but got {result:?}"
        );
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

    #[cfg(feature = "unified-dag-execution")]
    #[test]
    fn harvest_builder_rejects_workflow_schedule_targeting_auto_registered_dag_name() {
        let result = HarvestBuilder::new()
            .dags(vec![fake_unified_dag_info()])
            .workflow_schedule(WorkflowSchedule::new(
                "daily_etl",
                Schedule::Interval(Duration::from_secs(60)),
            ))
            .try_build();

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::InvalidWorkflowSchedule {
                ref workflow_name,
                ..
            } if workflow_name == "daily_etl"
        ));
        assert!(
            err.to_string().contains("auto-registered DAG"),
            "error should explain the DAG/workflow schedule collision: {err}"
        );
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
    fn harvest_builder_defaults_history_guardrails() {
        let built = HarvestBuilder::new().build();
        let policy = built.history_policy();

        assert_eq!(policy.continue_as_new_threshold(), 10_000);
        assert_eq!(policy.event_hard_cap(), None);
    }

    #[test]
    fn harvest_builder_accepts_history_guardrail_overrides() {
        let built = HarvestBuilder::new()
            .history_continue_as_new_threshold(128)
            .history_event_hard_cap(256)
            .build();
        let policy = built.history_policy();

        assert_eq!(policy.continue_as_new_threshold(), 128);
        assert_eq!(policy.event_hard_cap(), Some(256));
    }

    #[cfg(feature = "db")]
    #[test]
    fn harvest_builder_passes_history_policy_to_worker_registry() {
        let built = HarvestBuilder::new()
            .history_continue_as_new_threshold(9)
            .history_event_hard_cap(11)
            .build();

        let (registry, _dags, _workflow_schedules, _worker_config) = built.into_worker_parts();

        assert_eq!(registry.history_policy().continue_as_new_threshold(), 9);
        assert_eq!(registry.history_policy().event_hard_cap(), Some(11));
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
                is_local: false,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }])
            .state(String::from("haunted"))
            .build();

        let (registry, _dags, _workflow_schedules, worker_config) = built.into_worker_parts();

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

    fn make_activity(
        name: &'static str,
        max_concurrent: Option<u32>,
        key: Option<&'static str>,
    ) -> ActivityInfo {
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
            is_local: false,
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        }
    }

    fn make_local_activity(name: &'static str, start_to_close: Option<Duration>) -> ActivityInfo {
        ActivityInfo {
            name,
            module: "test",
            default_retry_policy: None,
            default_start_to_close: start_to_close,
            default_heartbeat_timeout: None,
            default_schedule_to_start: None,
            default_queue: None,
            max_concurrent: None,
            concurrency_key: None,
            is_local: true,
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
        // would silently never fire (NULL cap bypasses the saturation check).
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

    #[test]
    fn builder_rejects_implicit_key_cap_mismatch_with_explicit_key() {
        // act_a uses max_concurrent=5 with no key (implicit key = "act_a").
        // act_b explicitly declares key="act_a" with a different cap.
        // Both would resolve to the same effective key at runtime, so caps must agree.
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), None),
                make_activity("act_b", Some(10), Some("act_a")),
            ])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(err, HarvestBuilderError::ConcurrencyKeyMismatch { ref key, .. } if key == "act_a"),
            "expected ConcurrencyKeyMismatch for key 'act_a', got: {err}"
        );
    }

    #[test]
    fn builder_accepts_implicit_key_matching_explicit_key_same_cap() {
        // act_a: implicit key = "act_a", cap = 5
        // act_b: explicit key = "act_a", cap = 5 → same effective key and same cap → ok
        let result = HarvestBuilder::new()
            .activities(vec![
                make_activity("act_a", Some(5), None),
                make_activity("act_b", Some(5), Some("act_a")),
            ])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_zero_concurrency_cap() {
        // max_concurrent = 0 makes the COUNT check always fail (0 running < 0 is
        // never true), permanently deferring every task for this activity.
        let result = HarvestBuilder::new()
            .activities(vec![make_activity("act_a", Some(0), Some("stripe"))])
            .try_build();
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            HarvestBuilderError::ZeroConcurrencyCap { ref activity }
                if activity == "act_a"
        ));
        assert!(err.to_string().contains("act_a"));
    }

    // ── Local activity cap tests ──────────────────────────────────────────

    #[test]
    fn worker_config_max_local_activity_start_to_close_defaults_to_60s() {
        let config = WorkerConfig::default();
        assert_eq!(
            config.max_local_activity_start_to_close,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn builder_accepts_local_activity_within_cap() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "compute_hash",
                Some(Duration::from_secs(30)),
            )])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_accepts_local_activity_with_no_start_to_close() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity("compute_hash", None)])
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn builder_rejects_local_activity_exceeding_cap() {
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "slow_local",
                Some(Duration::from_secs(120)),
            )])
            .try_build();
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                HarvestBuilderError::LocalActivityStartToCloseExceedsCap {
                    ref activity, ..
                } if activity == "slow_local"
            ),
            "expected LocalActivityStartToCloseExceedsCap, got {err}"
        );
        assert!(err.to_string().contains("slow_local"));
    }

    #[test]
    fn builder_rejects_local_activity_exactly_at_cap_boundary_when_exceeded() {
        // Exactly 60s is fine; 61s should fail.
        let at_cap = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "edge_case",
                Some(Duration::from_secs(60)),
            )])
            .try_build();
        assert!(at_cap.is_ok());

        let over_cap = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "edge_case",
                Some(Duration::from_secs(61)),
            )])
            .try_build();
        assert!(over_cap.is_err());
    }

    #[test]
    fn builder_accepts_custom_cap_that_fits_activity() {
        let worker = WorkerConfig {
            max_local_activity_start_to_close: Duration::from_secs(120),
            ..WorkerConfig::default()
        };
        let result = HarvestBuilder::new()
            .activities(vec![make_local_activity(
                "slow_local",
                Some(Duration::from_secs(90)),
            )])
            .worker(worker)
            .try_build();
        assert!(result.is_ok());
    }

    #[test]
    fn regular_activity_is_not_subject_to_local_cap() {
        // A regular activity with start_to_close > 60s should not be rejected
        // by the local activity cap validator.
        let result = HarvestBuilder::new()
            .activities(vec![ActivityInfo {
                name: "long_running",
                module: "test",
                default_retry_policy: None,
                default_start_to_close: Some(Duration::from_secs(300)),
                default_heartbeat_timeout: None,
                default_schedule_to_start: None,
                default_queue: None,
                max_concurrent: None,
                concurrency_key: None,
                is_local: false,
                handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            }])
            .try_build();
        assert!(result.is_ok());
    }
}
