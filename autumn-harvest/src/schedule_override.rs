//! Operator overrides for code-declared schedules (issues #2385 and #2386).
//!
//! A DAG declares its schedule in code, through [`DagInfo`](crate::info::DagInfo).
//! A workflow schedule declares it through `WorkerBuilder::workflow_schedule`.
//! The scheduler converges each `harvest_schedules` row to the declared value
//! on every tick. So a direct edit of the row lasts one tick, and a change in
//! code needs a restart.
//!
//! An override is a persisted operator value for one or more schedule fields.
//! It lives in `harvest_schedule_overrides`, on the shard that holds the
//! schedule row. A DAG override is keyed by the DAG name, with `kind = 'dag'`.
//! A workflow override is keyed by the workflow name, with `kind = 'workflow'`.
//! The precedence is fixed:
//!
//! > override, then code.
//!
//! A field that the override does not set inherits the code value.
//!
//! ## Where an override applies
//!
//! The scheduler loads every override of a shard once per pass. It applies
//! them to the DAG catalog and to the lowered workflow schedules before
//! registration. The reconciler then converges each row to the effective
//! value. Its convergence logic does not know that overrides exist.
//!
//! Both registration passes write the same DAG row. So both receive the same
//! effective values, or they would rewrite each other on every tick.
//!
//! ## Failure mode
//!
//! A pass that cannot read the overrides skips its shard. A fallback to the
//! code value would recompute `next_run_at` for one tick, and the next tick
//! would recompute it again. See [`load_overrides`].
//!
//! ## What a workflow override can change
//!
//! A workflow override uses the field set of the #771 patch,
//! [`WorkflowSchedulePatch`], except `input`. The input is a payload. It is
//! stored through the payload codecs, and a codec key rotation rewrites it.
//! An override of it would need the same treatment, so it is out of scope.

use std::borrow::Cow;
use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};

use crate::error::{HarvestError, HarvestResult};
use crate::policy::{
    CatchupPolicy, OverlapPolicy, RetryPolicy, Schedule, SkipPolicy, WorkflowSchedule,
};
use crate::scheduler::{DagCatalog, RegisteredDag, WorkflowSchedulePatch};

/// The `kind` value of a DAG override row.
pub const KIND_DAG: &str = "dag";

/// The `kind` value of a workflow schedule override row (issue #2386).
pub const KIND_WORKFLOW: &str = "workflow";

/// The schedule fields that an override can set.
///
/// `None` means "inherit the code value". An override with every field
/// `None` is not stored.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScheduleOverrideFields {
    /// The schedule cadence. `Some(Schedule::Manual)` stops automatic fires.
    pub schedule: Option<Schedule>,
    /// The legacy catchup flag.
    pub catchup: Option<bool>,
    /// The maximum number of concurrent runs.
    pub max_active_runs: Option<u32>,
    /// The fire jitter window.
    pub jitter: Option<Duration>,
    /// The overlap policy.
    pub overlap_policy: Option<OverlapPolicy>,
    /// The buffer cap under [`OverlapPolicy::BufferAll`].
    pub buffer_all_max: Option<u32>,
}

impl ScheduleOverrideFields {
    /// Whether the override sets no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.schedule.is_none()
            && self.catchup.is_none()
            && self.max_active_runs.is_none()
            && self.jitter.is_none()
            && self.overlap_policy.is_none()
            && self.buffer_all_max.is_none()
    }

    /// Apply this override to one field set of a DAG, in place.
    ///
    /// The code value stays where the override sets nothing.
    fn apply(&self, dag: &mut RegisteredDag) {
        if let Some(schedule) = &self.schedule {
            dag.schedule = Some(schedule.clone());
        }
        if let Some(catchup) = self.catchup {
            dag.catchup = catchup;
        }
        if let Some(max_active_runs) = self.max_active_runs {
            dag.max_active_runs = max_active_runs;
        }
        if let Some(jitter) = self.jitter {
            dag.jitter = jitter;
        }
        if let Some(overlap_policy) = self.overlap_policy {
            dag.overlap_policy = overlap_policy;
        }
        if let Some(buffer_all_max) = self.buffer_all_max {
            dag.buffer_all_max = buffer_all_max;
        }
    }
}

/// A partial change to a stored override.
///
/// Each field is tri-state. The outer `None` keeps the stored value.
/// `Some(None)` returns the field to the code value. `Some(Some(v))` sets
/// `v`.
#[derive(Debug, Clone, Default)]
pub struct ScheduleOverridePatch {
    /// The schedule cadence.
    pub schedule: Option<Option<Schedule>>,
    /// The legacy catchup flag.
    pub catchup: Option<Option<bool>>,
    /// The maximum number of concurrent runs.
    pub max_active_runs: Option<Option<u32>>,
    /// The fire jitter window.
    pub jitter: Option<Option<Duration>>,
    /// The overlap policy.
    pub overlap_policy: Option<Option<OverlapPolicy>>,
    /// The buffer cap under [`OverlapPolicy::BufferAll`].
    pub buffer_all_max: Option<Option<u32>>,
}

impl ScheduleOverridePatch {
    /// Whether the patch changes no field.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.schedule.is_none()
            && self.catchup.is_none()
            && self.max_active_runs.is_none()
            && self.jitter.is_none()
            && self.overlap_policy.is_none()
            && self.buffer_all_max.is_none()
    }

    /// Merge this patch over the stored fields.
    #[must_use]
    pub fn merge(&self, stored: &ScheduleOverrideFields) -> ScheduleOverrideFields {
        fn pick<T: Clone>(patch: Option<&Option<T>>, stored: Option<&T>) -> Option<T> {
            patch.map_or_else(|| stored.cloned(), Clone::clone)
        }
        ScheduleOverrideFields {
            schedule: pick(self.schedule.as_ref(), stored.schedule.as_ref()),
            catchup: pick(self.catchup.as_ref(), stored.catchup.as_ref()),
            max_active_runs: pick(
                self.max_active_runs.as_ref(),
                stored.max_active_runs.as_ref(),
            ),
            jitter: pick(self.jitter.as_ref(), stored.jitter.as_ref()),
            overlap_policy: pick(self.overlap_policy.as_ref(), stored.overlap_policy.as_ref()),
            buffer_all_max: pick(self.buffer_all_max.as_ref(), stored.buffer_all_max.as_ref()),
        }
    }
}

/// One stored DAG override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DagScheduleOverride {
    /// The DAG name.
    pub dag_name: String,
    /// The fields that the override sets.
    pub fields: ScheduleOverrideFields,
    /// The operator's stated reason, if any.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override was first stored.
    pub created_at: DateTime<Utc>,
    /// When the override last changed.
    pub updated_at: DateTime<Utc>,
}

/// One stored override of a code-declared workflow schedule (issue #2386).
///
/// `fields` uses the #771 patch shape. A field that is `None` inherits the
/// code value. Its `input` is always `None`, because an override does not
/// change the input.
#[derive(Debug, Clone)]
pub struct WorkflowScheduleOverride {
    /// The workflow name of the schedule.
    pub workflow_name: String,
    /// The fields that the override sets.
    pub fields: WorkflowSchedulePatch,
    /// The operator's stated reason, if any.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override was first stored.
    pub created_at: DateTime<Utc>,
    /// When the override last changed.
    pub updated_at: DateTime<Utc>,
}

/// Whether a workflow override sets no field.
#[must_use]
pub const fn workflow_fields_are_empty(fields: &WorkflowSchedulePatch) -> bool {
    fields.schedule.is_none()
        && fields.input.is_none()
        && fields.queue_name.is_none()
        && fields.catchup.is_none()
        && fields.max_active_runs.is_none()
        && fields.jitter.is_none()
        && fields.overlap_policy.is_none()
        && fields.buffer_all_max.is_none()
        && fields.calendar.is_none()
        && fields.skip_policy.is_none()
        && fields.consecutive_failure_limit.is_none()
        && fields.end_at.is_none()
        && fields.max_runs.is_none()
        && fields.catchup_policy.is_none()
        && fields.retry_policy.is_none()
}

/// Merge `patch` over the `stored` fields of a workflow override.
///
/// A field that the patch sets wins. A field that it leaves out keeps the
/// stored value. The merge never carries an input.
#[must_use]
pub fn merge_workflow_fields(
    patch: &WorkflowSchedulePatch,
    stored: &WorkflowSchedulePatch,
) -> WorkflowSchedulePatch {
    fn pick<T: Clone>(patch: Option<&T>, stored: Option<&T>) -> Option<T> {
        patch.or(stored).cloned()
    }
    WorkflowSchedulePatch {
        schedule: pick(patch.schedule.as_ref(), stored.schedule.as_ref()),
        input: None,
        queue_name: pick(patch.queue_name.as_ref(), stored.queue_name.as_ref()),
        catchup: pick(patch.catchup.as_ref(), stored.catchup.as_ref()),
        max_active_runs: pick(
            patch.max_active_runs.as_ref(),
            stored.max_active_runs.as_ref(),
        ),
        jitter: pick(patch.jitter.as_ref(), stored.jitter.as_ref()),
        overlap_policy: pick(
            patch.overlap_policy.as_ref(),
            stored.overlap_policy.as_ref(),
        ),
        buffer_all_max: pick(
            patch.buffer_all_max.as_ref(),
            stored.buffer_all_max.as_ref(),
        ),
        calendar: pick(patch.calendar.as_ref(), stored.calendar.as_ref()),
        skip_policy: pick(patch.skip_policy.as_ref(), stored.skip_policy.as_ref()),
        consecutive_failure_limit: pick(
            patch.consecutive_failure_limit.as_ref(),
            stored.consecutive_failure_limit.as_ref(),
        ),
        end_at: pick(patch.end_at.as_ref(), stored.end_at.as_ref()),
        max_runs: pick(patch.max_runs.as_ref(), stored.max_runs.as_ref()),
        catchup_policy: pick(
            patch.catchup_policy.as_ref(),
            stored.catchup_policy.as_ref(),
        ),
        retry_policy: pick(patch.retry_policy.as_ref(), stored.retry_policy.as_ref()),
    }
}

/// The code value of a workflow schedule with `fields` applied.
///
/// The field rules match the #771 merge. A `max_runs` of zero means no
/// budget, as on the create path.
#[must_use]
pub fn effective_workflow_schedule(
    code: &WorkflowSchedule,
    fields: &WorkflowSchedulePatch,
) -> WorkflowSchedule {
    let mut ws = code.clone();
    if let Some(schedule) = &fields.schedule {
        ws.schedule = schedule.clone();
    }
    if let Some(queue_name) = &fields.queue_name {
        ws.queue_name.clone_from(queue_name);
    }
    if let Some(catchup) = fields.catchup {
        ws.catchup = catchup;
    }
    if let Some(max_active_runs) = fields.max_active_runs {
        ws.max_active_runs = max_active_runs;
    }
    if let Some(jitter) = fields.jitter {
        ws.jitter = jitter;
    }
    if let Some(overlap_policy) = fields.overlap_policy {
        ws.overlap_policy = overlap_policy;
    }
    if let Some(buffer_all_max) = fields.buffer_all_max {
        ws.buffer_all_max = buffer_all_max;
    }
    if let Some(calendar) = &fields.calendar {
        ws.calendar.clone_from(calendar);
    }
    if let Some(skip_policy) = fields.skip_policy {
        ws.skip_policy = skip_policy;
    }
    if let Some(limit) = fields.consecutive_failure_limit {
        ws.consecutive_failure_limit = limit;
    }
    if let Some(end_at) = fields.end_at {
        ws.end_at = end_at;
    }
    if let Some(max_runs) = fields.max_runs {
        ws.max_runs = max_runs.filter(|&n| n > 0);
    }
    if let Some(catchup_policy) = fields.catchup_policy {
        ws.catchup_policy = catchup_policy;
    }
    if let Some(retry_policy) = &fields.retry_policy {
        ws.retry_policy.clone_from(retry_policy);
    }
    ws
}

/// Check the effective value of a workflow schedule under a candidate
/// override.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when the fields carry an input, for an
/// invalid cadence, or for a jitter that the effective cadence cannot hold.
pub fn validate_effective_workflow(
    code: &WorkflowSchedule,
    fields: &WorkflowSchedulePatch,
) -> HarvestResult<WorkflowSchedule> {
    if fields.input.is_some() {
        return Err(HarvestError::Config(format!(
            "schedule '{}' is declared in code; an override cannot change its input. \
             Change the input in code",
            code.workflow_name
        )));
    }
    let effective = effective_workflow_schedule(code, fields);
    crate::policy::validate_schedule(&effective.schedule).map_err(HarvestError::Config)?;
    crate::policy::validate_jitter(&effective.schedule, effective.jitter)
        .map_err(HarvestError::Config)?;
    Ok(effective)
}

/// The stored form of the workflow-only override fields.
///
/// An absent key inherits the code value. A JSON `null` on a nullable field
/// sets it to NULL, so the double `Option` needs `present` to read it.
#[expect(
    clippy::option_option,
    reason = "the stored form keeps an absent key apart from a JSON null"
)]
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkflowFieldsJson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    queue_name: Option<String>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    calendar: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skip_policy: Option<SkipPolicy>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    consecutive_failure_limit: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    end_at: Option<Option<DateTime<Utc>>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    max_runs: Option<Option<u32>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    catchup_policy: Option<Option<CatchupPolicy>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    retry_policy: Option<Option<RetryPolicy>>,
}

/// Read a key that is present as `Some`, so that a JSON `null` is
/// `Some(None)` and an absent key stays `None`.
#[expect(
    clippy::option_option,
    reason = "a present key with a JSON null is Some(None)"
)]
fn present<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

impl WorkflowFieldsJson {
    fn from_fields(fields: &WorkflowSchedulePatch) -> Self {
        Self {
            queue_name: fields.queue_name.clone(),
            calendar: fields.calendar.clone(),
            skip_policy: fields.skip_policy,
            consecutive_failure_limit: fields.consecutive_failure_limit,
            end_at: fields.end_at,
            max_runs: fields.max_runs,
            catchup_policy: fields.catchup_policy,
            retry_policy: fields.retry_policy.clone(),
        }
    }

    fn into_fields(self, common: ScheduleOverrideFields) -> WorkflowSchedulePatch {
        WorkflowSchedulePatch {
            schedule: common.schedule,
            input: None,
            queue_name: self.queue_name,
            catchup: common.catchup,
            max_active_runs: common.max_active_runs,
            jitter: common.jitter,
            overlap_policy: common.overlap_policy,
            buffer_all_max: common.buffer_all_max,
            calendar: self.calendar,
            skip_policy: self.skip_policy,
            consecutive_failure_limit: self.consecutive_failure_limit,
            end_at: self.end_at,
            max_runs: self.max_runs,
            catchup_policy: self.catchup_policy,
            retry_policy: self.retry_policy,
        }
    }
}

/// The fields of a workflow override that the shared columns hold.
fn common_fields(fields: &WorkflowSchedulePatch) -> ScheduleOverrideFields {
    ScheduleOverrideFields {
        schedule: fields.schedule.clone(),
        catchup: fields.catchup,
        max_active_runs: fields.max_active_runs,
        jitter: fields.jitter,
        overlap_policy: fields.overlap_policy,
        buffer_all_max: fields.buffer_all_max,
    }
}

/// The JSON view of a stored override, for the management API.
///
/// Each field is the override value, or `null` where the code value applies.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ScheduleOverrideView {
    /// The override cadence, in the stored `schedule_expr` form.
    pub schedule_expr: Option<String>,
    /// The override catchup flag.
    pub catchup: Option<bool>,
    /// The override run cap.
    pub max_active_runs: Option<u32>,
    /// The override jitter, in whole seconds.
    pub jitter_secs: Option<u64>,
    /// The override overlap policy.
    pub overlap_policy: Option<&'static str>,
    /// The override buffer cap.
    pub buffer_all_max: Option<u32>,
    /// The operator's stated reason.
    pub reason: Option<String>,
    /// The actor that last changed the override.
    pub updated_by: String,
    /// When the override last changed.
    pub updated_at: DateTime<Utc>,
}

impl From<&DagScheduleOverride> for ScheduleOverrideView {
    fn from(value: &DagScheduleOverride) -> Self {
        let fields = &value.fields;
        Self {
            schedule_expr: fields.schedule.as_ref().map(schedule_to_storage),
            catchup: fields.catchup,
            max_active_runs: fields.max_active_runs,
            jitter_secs: fields.jitter.map(|jitter| jitter.as_secs()),
            overlap_policy: fields.overlap_policy.map(OverlapPolicy::as_str),
            buffer_all_max: fields.buffer_all_max,
            reason: value.reason.clone(),
            updated_by: value.updated_by.clone(),
            updated_at: value.updated_at,
        }
    }
}

/// The overrides of one shard, keyed by DAG name and by workflow name.
#[derive(Debug, Clone, Default)]
pub struct ScheduleOverrides {
    dags: HashMap<String, DagScheduleOverride>,
    workflows: HashMap<String, WorkflowScheduleOverride>,
}

impl ScheduleOverrides {
    /// A set with no override.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Build a set from stored overrides.
    #[must_use]
    pub fn from_dag_overrides(overrides: impl IntoIterator<Item = DagScheduleOverride>) -> Self {
        Self {
            dags: overrides
                .into_iter()
                .map(|value| (value.dag_name.clone(), value))
                .collect(),
            workflows: HashMap::new(),
        }
    }

    /// Add workflow schedule overrides to the set (issue #2386).
    #[must_use]
    pub fn with_workflow_overrides(
        mut self,
        overrides: impl IntoIterator<Item = WorkflowScheduleOverride>,
    ) -> Self {
        self.workflows.extend(
            overrides
                .into_iter()
                .map(|value| (value.workflow_name.clone(), value)),
        );
        self
    }

    /// Whether the set holds no override.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dags.is_empty() && self.workflows.is_empty()
    }

    /// The override of one code-declared workflow schedule, if any.
    #[must_use]
    pub fn for_workflow(&self, workflow_name: &str) -> Option<&WorkflowScheduleOverride> {
        self.workflows.get(workflow_name)
    }

    /// The override of one DAG, if any.
    #[must_use]
    pub fn for_dag(&self, dag_name: &str) -> Option<&DagScheduleOverride> {
        self.dags.get(dag_name)
    }

    /// The effective value of one DAG: its code value with its override.
    #[must_use]
    pub fn apply_to_dag<'a>(&self, dag: &'a RegisteredDag) -> Cow<'a, RegisteredDag> {
        self.for_dag(&dag.name).map_or(Cow::Borrowed(dag), |value| {
            Cow::Owned(effective_dag(dag, &value.fields))
        })
    }

    /// The effective DAG catalog.
    ///
    /// A set with no override borrows the catalog, so a pass with no
    /// override clones nothing.
    #[must_use]
    pub fn apply_to_catalog<'a>(&self, catalog: &'a DagCatalog) -> Cow<'a, DagCatalog> {
        if !catalog.keys().any(|name| self.dags.contains_key(name)) {
            return Cow::Borrowed(catalog);
        }
        let mut effective = catalog.clone();
        for dag in effective.values_mut() {
            if let Some(value) = self.dags.get(&dag.name) {
                value.fields.apply(dag);
            }
        }
        Cow::Owned(effective)
    }

    /// The effective workflow schedules.
    ///
    /// A unified DAG lowers to a [`WorkflowSchedule`] with `dag_name` set.
    /// That entry takes the same override as the catalog entry. A unified
    /// DAG that declares no schedule has no entry. When its override sets a
    /// schedule, this adds one, so the override can schedule it.
    ///
    /// An entry with no `dag_name` is a code-declared workflow schedule. It
    /// takes the workflow override of its workflow name (issue #2386).
    #[must_use]
    pub fn apply_to_workflow_schedules<'a>(
        &self,
        schedules: &'a [WorkflowSchedule],
        catalog: &DagCatalog,
    ) -> Cow<'a, [WorkflowSchedule]> {
        if self.is_empty() {
            return Cow::Borrowed(schedules);
        }
        let mut effective = schedules.to_vec();
        for schedule in &mut effective {
            let Some(dag_name) = schedule.dag_name.as_deref() else {
                if let Some(value) = self.workflows.get(&schedule.workflow_name) {
                    *schedule = effective_workflow_schedule(schedule, &value.fields);
                }
                continue;
            };
            let (Some(value), Some(dag)) = (self.dags.get(dag_name), catalog.get(dag_name)) else {
                continue;
            };
            apply_to_lowered_schedule(schedule, &effective_dag(dag, &value.fields));
        }
        let mut added: Vec<&RegisteredDag> = catalog
            .values()
            .filter(|dag| dag.is_unified && dag.schedule.is_none())
            .filter(|dag| {
                self.dags
                    .get(&dag.name)
                    .is_some_and(|value| value.fields.schedule.is_some())
            })
            .filter(|dag| {
                !schedules
                    .iter()
                    .any(|schedule| schedule.dag_name.as_deref() == Some(dag.name.as_str()))
            })
            .collect();
        // A stable order keeps registration deterministic across ticks.
        added.sort_by(|a, b| a.name.cmp(&b.name));
        for dag in added {
            if let Some(value) = self.dags.get(&dag.name) {
                effective.push(lowered_schedule(&effective_dag(dag, &value.fields)));
            }
        }
        Cow::Owned(effective)
    }
}

/// The code value of `dag` with `fields` applied.
#[must_use]
pub fn effective_dag(dag: &RegisteredDag, fields: &ScheduleOverrideFields) -> RegisteredDag {
    let mut effective = dag.clone();
    fields.apply(&mut effective);
    effective
}

/// Copy the effective DAG fields onto its lowered workflow schedule.
///
/// The field set matches [`DagInfo::as_workflow_schedule`](crate::info::DagInfo::as_workflow_schedule).
fn apply_to_lowered_schedule(schedule: &mut WorkflowSchedule, dag: &RegisteredDag) {
    if let Some(cadence) = &dag.schedule {
        schedule.schedule = cadence.clone();
    }
    schedule.catchup = dag.catchup;
    schedule.max_active_runs = dag.max_active_runs;
    schedule.jitter = dag.jitter;
    schedule.overlap_policy = dag.overlap_policy;
    schedule.buffer_all_max = dag.buffer_all_max;
}

/// The lowered workflow schedule of a unified DAG.
///
/// This mirrors [`DagInfo::as_workflow_schedule`](crate::info::DagInfo::as_workflow_schedule)
/// for a DAG that declared no schedule in code.
fn lowered_schedule(dag: &RegisteredDag) -> WorkflowSchedule {
    WorkflowSchedule {
        workflow_name: dag.name.clone(),
        dag_name: Some(dag.name.clone()),
        schedule: dag.schedule.clone().unwrap_or(Schedule::Manual),
        input: serde_json::Value::Null,
        catchup: dag.catchup,
        max_active_runs: dag.max_active_runs,
        paused: false,
        queue_name: dag
            .default_queue
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        jitter: dag.jitter,
        overlap_policy: dag.overlap_policy,
        buffer_all_max: dag.buffer_all_max,
        execution_timeout: None,
        chain_execution_timeout: None,
        calendar: None,
        skip_policy: crate::policy::SkipPolicy::Skip,
        consecutive_failure_limit: None,
        end_at: None,
        max_runs: None,
        catchup_policy: None,
        retry_policy: None,
        all_writable_shards: false,
    }
}

/// Check the effective value of a DAG under a candidate override.
///
/// The scheduler validates the same rules at registration. A check here
/// rejects a bad override before it is stored.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] for an invalid cadence, or for a jitter
/// that the effective cadence cannot hold.
pub fn validate_effective(
    dag: &RegisteredDag,
    fields: &ScheduleOverrideFields,
) -> HarvestResult<RegisteredDag> {
    let effective = effective_dag(dag, fields);
    if let Some(schedule) = &effective.schedule {
        crate::policy::validate_schedule(schedule).map_err(HarvestError::Config)?;
        crate::policy::validate_jitter(schedule, effective.jitter).map_err(HarvestError::Config)?;
    }
    if fields.schedule.is_some() && !dag.is_unified {
        return Err(HarvestError::Config(format!(
            "dag '{}' runs on the classic execution path; a schedule override needs \
             unified DAG execution",
            dag.name
        )));
    }
    Ok(effective)
}

/// The stored form of a schedule, as in `harvest_schedules.schedule_expr`.
#[must_use]
pub fn schedule_to_storage(schedule: &Schedule) -> String {
    match schedule {
        Schedule::Cron(expr) => format!("cron:{expr}"),
        Schedule::CronInTimezone { expr, tz } => format!("cron_tz:{tz}:{expr}"),
        Schedule::Interval(interval) => format!("interval:{}", interval.as_secs()),
        Schedule::Manual => "manual".to_string(),
    }
}

/// Parse the stored form of a schedule.
///
/// Returns `None` for a value that no writer of this module stores.
#[must_use]
pub fn schedule_from_storage(stored: &str) -> Option<Schedule> {
    if stored == "manual" {
        return Some(Schedule::Manual);
    }
    crate::scheduler::parse_schedule_from_expr_pub(stored)
}

#[derive(Debug, Clone, Queryable, Selectable)]
#[diesel(table_name = crate::schema::harvest_schedule_overrides)]
#[diesel(check_for_backend(diesel::pg::Pg))]
struct OverrideRow {
    kind: String,
    name: String,
    schedule_expr: Option<String>,
    catchup: Option<bool>,
    max_active_runs: Option<i32>,
    jitter_secs: Option<i64>,
    overlap_policy: Option<String>,
    buffer_all_max: Option<i32>,
    reason: Option<String>,
    updated_by: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    workflow_fields: serde_json::Value,
}

/// A decoded override row of either kind.
enum DecodedOverride {
    Dag(DagScheduleOverride),
    Workflow(WorkflowScheduleOverride),
}

impl OverrideRow {
    /// A config error for a stored value that does not decode.
    fn corrupt(&self, field: &str, value: &dyn std::fmt::Debug) -> HarvestError {
        let (target, route) = if self.kind == KIND_WORKFLOW {
            ("workflow schedule", "PATCH /schedules/{id}")
        } else {
            ("dag", "PATCH /dags/{dag_name}")
        };
        HarvestError::Config(format!(
            "schedule override for {target} '{}' holds an unreadable {field} ({value:?}); \
             set or clear it through {route}",
            self.name
        ))
    }

    /// Decode a stored row of either kind.
    fn decode_any(self) -> HarvestResult<DecodedOverride> {
        match self.kind.as_str() {
            KIND_DAG => self.decode().map(DecodedOverride::Dag),
            KIND_WORKFLOW => self.decode_workflow().map(DecodedOverride::Workflow),
            other => Err(self.corrupt("kind", &other)),
        }
    }

    /// Decode a stored workflow row (issue #2386).
    fn decode_workflow(self) -> HarvestResult<WorkflowScheduleOverride> {
        let extra: WorkflowFieldsJson = serde_json::from_value(self.workflow_fields.clone())
            .map_err(|error| self.corrupt("workflow_fields", &error.to_string()))?;
        let common = self.decode_common()?;
        Ok(WorkflowScheduleOverride {
            workflow_name: self.name,
            fields: extra.into_fields(common),
            reason: self.reason,
            updated_by: self.updated_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }

    /// Decode a stored DAG row.
    ///
    /// A value that does not decode is an error, not a silent inherit. An
    /// inherit would move the schedule back to its code value with no sign.
    fn decode(self) -> HarvestResult<DagScheduleOverride> {
        let fields = self.decode_common()?;
        Ok(DagScheduleOverride {
            dag_name: self.name,
            fields,
            reason: self.reason,
            updated_by: self.updated_by,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }

    /// Decode the shared columns of a row.
    fn decode_common(&self) -> HarvestResult<ScheduleOverrideFields> {
        let corrupt = |field: &str, value: &dyn std::fmt::Debug| self.corrupt(field, value);
        let schedule = match self.schedule_expr.as_deref() {
            Some(stored) => Some(
                schedule_from_storage(stored).ok_or_else(|| corrupt("schedule_expr", &stored))?,
            ),
            None => None,
        };
        let overlap_policy = match self.overlap_policy.as_deref() {
            Some(stored) => Some(
                OverlapPolicy::from_user_input(stored)
                    .map_err(|_| corrupt("overlap_policy", &stored))?,
            ),
            None => None,
        };
        let to_u32 = |field: &str, value: Option<i32>| -> HarvestResult<Option<u32>> {
            value
                .map(|n| u32::try_from(n).map_err(|_| corrupt(field, &n)))
                .transpose()
        };
        let jitter = self
            .jitter_secs
            .map(|n| {
                u64::try_from(n)
                    .map(Duration::from_secs)
                    .map_err(|_| corrupt("jitter_secs", &n))
            })
            .transpose()?;
        Ok(ScheduleOverrideFields {
            schedule,
            catchup: self.catchup,
            max_active_runs: to_u32("max_active_runs", self.max_active_runs)?,
            jitter,
            overlap_policy,
            buffer_all_max: to_u32("buffer_all_max", self.buffer_all_max)?,
        })
    }
}

/// Load every override stored on this shard, of both kinds.
///
/// The scheduler calls this once per shard pass. The table holds one row per
/// overridden schedule, so the read is small.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when the read fails, and
/// [`HarvestError::Config`] when a stored row does not decode. The caller
/// skips the pass on either error. It never falls back to code values.
pub async fn load_overrides(conn: &mut AsyncPgConnection) -> HarvestResult<ScheduleOverrides> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let rows: Vec<OverrideRow> = dsl::harvest_schedule_overrides
        .select(OverrideRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    let mut dags = Vec::new();
    let mut workflows = Vec::new();
    for row in rows {
        match row.decode_any()? {
            DecodedOverride::Dag(value) => dags.push(value),
            DecodedOverride::Workflow(value) => workflows.push(value),
        }
    }
    Ok(ScheduleOverrides::from_dag_overrides(dags).with_workflow_overrides(workflows))
}

/// Load the override of one DAG, if any.
///
/// # Errors
///
/// As [`load_dag_overrides`].
pub async fn get_dag_override(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> HarvestResult<Option<DagScheduleOverride>> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let row: Option<OverrideRow> = dsl::harvest_schedule_overrides
        .filter(dsl::kind.eq(KIND_DAG))
        .filter(dsl::name.eq(dag_name))
        .select(OverrideRow::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    row.map(OverrideRow::decode).transpose()
}

/// Lock and load the override of one DAG, for a read-modify-write.
async fn lock_dag_override(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> HarvestResult<Option<DagScheduleOverride>> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let row: Option<OverrideRow> = dsl::harvest_schedule_overrides
        .filter(dsl::kind.eq(KIND_DAG))
        .filter(dsl::name.eq(dag_name))
        .select(OverrideRow::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    row.map(OverrideRow::decode).transpose()
}

/// Write the merged fields of one DAG override, or delete the row when the
/// merge sets no field.
async fn store_dag_override(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
    fields: &ScheduleOverrideFields,
    actor: &str,
    reason: Option<&str>,
) -> HarvestResult<()> {
    if fields.is_empty() {
        delete_override(conn, KIND_DAG, dag_name).await?;
        return Ok(());
    }
    let row = StoredOverride {
        kind: KIND_DAG,
        name: dag_name,
        fields,
        workflow_fields: serde_json::json!({}),
        actor,
        reason,
    };
    upsert_override(conn, &row).await
}

/// One override row to write.
struct StoredOverride<'a> {
    kind: &'a str,
    name: &'a str,
    fields: &'a ScheduleOverrideFields,
    workflow_fields: serde_json::Value,
    actor: &'a str,
    reason: Option<&'a str>,
}

/// Delete one override row. Returns whether a row was deleted.
async fn delete_override(
    conn: &mut AsyncPgConnection,
    kind: &str,
    name: &str,
) -> HarvestResult<bool> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let deleted = diesel::delete(
        dsl::harvest_schedule_overrides
            .filter(dsl::kind.eq(kind))
            .filter(dsl::name.eq(name)),
    )
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(deleted > 0)
}

/// Insert or replace one override row.
async fn upsert_override(
    conn: &mut AsyncPgConnection,
    row: &StoredOverride<'_>,
) -> HarvestResult<()> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let fields = row.fields;
    let (actor, reason) = (row.actor, row.reason);
    let now = Utc::now();
    let schedule_expr = fields.schedule.as_ref().map(schedule_to_storage);
    let max_active_runs = fields.max_active_runs.map(saturating_i32);
    let jitter_secs = fields
        .jitter
        .map(|jitter| i64::try_from(jitter.as_secs()).unwrap_or(i64::MAX));
    let overlap_policy = fields.overlap_policy.map(OverlapPolicy::as_str);
    let buffer_all_max = fields.buffer_all_max.map(saturating_i32);
    diesel::insert_into(dsl::harvest_schedule_overrides)
        .values((
            dsl::kind.eq(row.kind),
            dsl::name.eq(row.name),
            dsl::schedule_expr.eq(schedule_expr.as_deref()),
            dsl::catchup.eq(fields.catchup),
            dsl::max_active_runs.eq(max_active_runs),
            dsl::jitter_secs.eq(jitter_secs),
            dsl::overlap_policy.eq(overlap_policy),
            dsl::buffer_all_max.eq(buffer_all_max),
            dsl::reason.eq(reason),
            dsl::updated_by.eq(actor),
            dsl::created_at.eq(now),
            dsl::updated_at.eq(now),
            dsl::workflow_fields.eq(&row.workflow_fields),
        ))
        .on_conflict((dsl::kind, dsl::name))
        .do_update()
        .set((
            dsl::schedule_expr.eq(schedule_expr.as_deref()),
            dsl::catchup.eq(fields.catchup),
            dsl::max_active_runs.eq(max_active_runs),
            dsl::jitter_secs.eq(jitter_secs),
            dsl::overlap_policy.eq(overlap_policy),
            dsl::buffer_all_max.eq(buffer_all_max),
            dsl::reason.eq(reason),
            dsl::updated_by.eq(actor),
            dsl::updated_at.eq(now),
            dsl::workflow_fields.eq(&row.workflow_fields),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

fn saturating_i32(value: u32) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

/// The result of [`update_dag_override`].
#[derive(Debug, Clone)]
pub struct DagOverrideUpdate {
    /// The stored override after the change. `None` when no field is left.
    pub stored: Option<DagScheduleOverride>,
    /// The effective DAG: its code value with the stored override.
    pub effective: RegisteredDag,
}

/// Apply a patch to the override of one DAG.
///
/// The merged override is validated against the DAG's code value before any
/// write. A rejected patch writes nothing. A merge that sets no field deletes
/// the row, so the DAG returns to its code value.
///
/// The caller then converges the schedule row with
/// [`ensure_dag_schedule`](crate::scheduler::ensure_dag_schedule) on
/// [`DagOverrideUpdate::effective`]. The next scheduler tick does the same,
/// so a failed convergence is repaired within one tick.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when the merged override is invalid, and
/// [`HarvestError::Database`] when a read or write fails.
pub async fn update_dag_override(
    conn: &mut AsyncPgConnection,
    dag: &RegisteredDag,
    patch: &ScheduleOverridePatch,
    actor: &str,
    reason: Option<&str>,
) -> HarvestResult<DagOverrideUpdate> {
    use diesel_async::AsyncConnection;

    let dag_name = dag.name.clone();
    Box::pin(conn.transaction(async |conn| {
        // FOR UPDATE serializes two concurrent patches of the same DAG. Without
        // it, each would merge over the same stored row and one change would
        // be lost. A first override has no row to lock. The insert conflict
        // on the primary key then decides, and the later writer wins whole.
        let stored = lock_dag_override(conn, &dag_name).await?;
        let base = stored.map(|value| value.fields).unwrap_or_default();
        let merged = patch.merge(&base);
        let effective = validate_effective(dag, &merged)?;
        store_dag_override(conn, &dag_name, &merged, actor, reason).await?;
        let stored = get_dag_override(conn, &dag_name).await?;
        Ok(DagOverrideUpdate { stored, effective })
    }))
    .await
}

/// Delete the override of one DAG. Returns whether a row was deleted.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when the delete fails.
pub async fn clear_dag_override(
    conn: &mut AsyncPgConnection,
    dag_name: &str,
) -> HarvestResult<bool> {
    delete_override(conn, KIND_DAG, dag_name).await
}

/// Load the override of one code-declared workflow schedule, if any
/// (issue #2386).
///
/// # Errors
///
/// As [`load_overrides`].
pub async fn get_workflow_override(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
) -> HarvestResult<Option<WorkflowScheduleOverride>> {
    select_workflow_override(conn, workflow_name, false).await
}

/// Lock and load the override of one workflow schedule, for a
/// read-modify-write.
pub(crate) async fn lock_workflow_override(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
) -> HarvestResult<Option<WorkflowScheduleOverride>> {
    select_workflow_override(conn, workflow_name, true).await
}

async fn select_workflow_override(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    for_update: bool,
) -> HarvestResult<Option<WorkflowScheduleOverride>> {
    use crate::schema::harvest_schedule_overrides::dsl;

    let query = dsl::harvest_schedule_overrides
        .filter(dsl::kind.eq(KIND_WORKFLOW))
        .filter(dsl::name.eq(workflow_name))
        .select(OverrideRow::as_select());
    let row: Option<OverrideRow> = if for_update {
        query.for_update().first(conn).await
    } else {
        query.first(conn).await
    }
    .optional()
    .map_err(crate::error::database_error)?;
    row.map(OverrideRow::decode_workflow).transpose()
}

/// Write the merged fields of one workflow override, or delete the row when
/// the merge sets no field.
pub(crate) async fn store_workflow_override(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    fields: &WorkflowSchedulePatch,
    actor: &str,
    reason: Option<&str>,
) -> HarvestResult<()> {
    if workflow_fields_are_empty(fields) {
        delete_override(conn, KIND_WORKFLOW, workflow_name).await?;
        return Ok(());
    }
    let workflow_fields = serde_json::to_value(WorkflowFieldsJson::from_fields(fields))
        .map_err(|error| HarvestError::Config(format!("workflow override fields: {error}")))?;
    let common = common_fields(fields);
    let row = StoredOverride {
        kind: KIND_WORKFLOW,
        name: workflow_name,
        fields: &common,
        workflow_fields,
        actor,
        reason,
    };
    upsert_override(conn, &row).await
}

/// The effective value of one DAG, read from its shard.
///
/// API paths that converge a DAG row call this first. A path that converges
/// with the code value would undo the override until the next tick.
///
/// # Errors
///
/// As [`get_dag_override`].
pub async fn effective_dag_from_db(
    conn: &mut AsyncPgConnection,
    dag: &RegisteredDag,
) -> HarvestResult<RegisteredDag> {
    Ok(get_dag_override(conn, &dag.name)
        .await?
        .map_or_else(|| dag.clone(), |value| effective_dag(dag, &value.fields)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagBuilder;

    fn dag(name: &str, schedule: Option<Schedule>) -> RegisteredDag {
        RegisteredDag {
            name: name.to_string(),
            module: "tests".to_string(),
            schedule,
            catchup: false,
            max_active_runs: 1,
            default_queue: Some("etl".to_string()),
            is_unified: true,
            definition: DagBuilder::new()
                .build()
                .expect("an empty DAG is a valid graph"),
            jitter: Duration::from_secs(10),
            overlap_policy: OverlapPolicy::Skip,
            buffer_all_max: 100,
            owner: None,
            runbook_url: None,
            severity: None,
        }
    }

    const fn hourly() -> Schedule {
        Schedule::Interval(Duration::from_secs(3600))
    }

    const fn minutely() -> Schedule {
        Schedule::Interval(Duration::from_secs(60))
    }

    fn stored(dag_name: &str, fields: ScheduleOverrideFields) -> DagScheduleOverride {
        DagScheduleOverride {
            dag_name: dag_name.to_string(),
            fields,
            reason: None,
            updated_by: "ops".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn lowered(dag: &RegisteredDag) -> WorkflowSchedule {
        lowered_schedule(dag)
    }

    #[test]
    fn an_override_wins_over_code_and_unset_fields_inherit() {
        let code = dag("etl", Some(hourly()));
        let fields = ScheduleOverrideFields {
            schedule: Some(minutely()),
            max_active_runs: Some(4),
            ..ScheduleOverrideFields::default()
        };
        let effective = effective_dag(&code, &fields);
        assert_eq!(effective.schedule, Some(minutely()));
        assert_eq!(effective.max_active_runs, 4);
        assert_eq!(effective.jitter, code.jitter);
        assert_eq!(effective.catchup, code.catchup);
        assert_eq!(effective.overlap_policy, code.overlap_policy);
    }

    #[test]
    fn a_set_with_no_override_borrows_the_catalog() {
        let mut catalog = DagCatalog::new();
        catalog.insert("etl".to_string(), dag("etl", Some(hourly())));
        let overrides = ScheduleOverrides::from_dag_overrides([stored(
            "other",
            ScheduleOverrideFields {
                catchup: Some(true),
                ..ScheduleOverrideFields::default()
            },
        )]);
        assert!(matches!(
            overrides.apply_to_catalog(&catalog),
            Cow::Borrowed(_)
        ));
        let schedules = vec![lowered(&catalog["etl"])];
        assert!(matches!(
            ScheduleOverrides::none().apply_to_workflow_schedules(&schedules, &catalog),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn catalog_and_lowered_schedule_take_the_same_values() {
        let mut catalog = DagCatalog::new();
        catalog.insert("etl".to_string(), dag("etl", Some(hourly())));
        let schedules = vec![lowered(&catalog["etl"])];
        let overrides = ScheduleOverrides::from_dag_overrides([stored(
            "etl",
            ScheduleOverrideFields {
                schedule: Some(minutely()),
                jitter: Some(Duration::from_secs(5)),
                overlap_policy: Some(OverlapPolicy::BufferAll),
                buffer_all_max: Some(7),
                catchup: Some(true),
                max_active_runs: Some(3),
            },
        )]);
        let effective_catalog = overrides.apply_to_catalog(&catalog);
        let effective_schedules = overrides.apply_to_workflow_schedules(&schedules, &catalog);
        let dag = &effective_catalog["etl"];
        let ws = &effective_schedules[0];
        assert_eq!(dag.schedule.as_ref(), Some(&ws.schedule));
        assert_eq!(dag.jitter, ws.jitter);
        assert_eq!(dag.overlap_policy, ws.overlap_policy);
        assert_eq!(dag.buffer_all_max, ws.buffer_all_max);
        assert_eq!(dag.catchup, ws.catchup);
        assert_eq!(dag.max_active_runs, ws.max_active_runs);
        assert_eq!(ws.schedule, minutely());
        // The queue is not overridable and stays the declared default queue.
        assert_eq!(ws.queue_name, "etl");
    }

    #[test]
    fn a_schedule_override_adds_a_lowered_schedule_for_an_unscheduled_dag() {
        let mut catalog = DagCatalog::new();
        catalog.insert("adhoc".to_string(), dag("adhoc", None));
        let overrides = ScheduleOverrides::from_dag_overrides([stored(
            "adhoc",
            ScheduleOverrideFields {
                schedule: Some(minutely()),
                ..ScheduleOverrideFields::default()
            },
        )]);
        let effective = overrides.apply_to_workflow_schedules(&[], &catalog);
        assert_eq!(effective.len(), 1);
        assert_eq!(effective[0].dag_name.as_deref(), Some("adhoc"));
        assert_eq!(effective[0].workflow_name, "adhoc");
        assert_eq!(effective[0].schedule, minutely());
    }

    #[test]
    fn a_non_schedule_override_adds_no_schedule_for_an_unscheduled_dag() {
        let mut catalog = DagCatalog::new();
        catalog.insert("adhoc".to_string(), dag("adhoc", None));
        let overrides = ScheduleOverrides::from_dag_overrides([stored(
            "adhoc",
            ScheduleOverrideFields {
                max_active_runs: Some(2),
                ..ScheduleOverrideFields::default()
            },
        )]);
        assert!(
            overrides
                .apply_to_workflow_schedules(&[], &catalog)
                .is_empty()
        );
    }

    #[test]
    fn patch_merge_sets_clears_and_keeps() {
        let base = ScheduleOverrideFields {
            schedule: Some(minutely()),
            catchup: Some(true),
            ..ScheduleOverrideFields::default()
        };
        let patch = ScheduleOverridePatch {
            catchup: Some(None),
            max_active_runs: Some(Some(5)),
            ..ScheduleOverridePatch::default()
        };
        let merged = patch.merge(&base);
        assert_eq!(merged.schedule, Some(minutely()));
        assert_eq!(merged.catchup, None);
        assert_eq!(merged.max_active_runs, Some(5));
    }

    #[test]
    fn a_patch_that_clears_every_field_leaves_an_empty_override() {
        let base = ScheduleOverrideFields {
            schedule: Some(minutely()),
            ..ScheduleOverrideFields::default()
        };
        let patch = ScheduleOverridePatch {
            schedule: Some(None),
            ..ScheduleOverridePatch::default()
        };
        assert!(patch.merge(&base).is_empty());
    }

    #[test]
    fn validation_rejects_a_zero_interval() {
        let code = dag("etl", Some(hourly()));
        let fields = ScheduleOverrideFields {
            schedule: Some(Schedule::Interval(Duration::ZERO)),
            ..ScheduleOverrideFields::default()
        };
        assert!(validate_effective(&code, &fields).is_err());
    }

    #[test]
    fn validation_rejects_a_cadence_that_cannot_hold_the_code_jitter() {
        // The code jitter is 10 s. A 5 s interval cannot hold it.
        let code = dag("etl", Some(hourly()));
        let fields = ScheduleOverrideFields {
            schedule: Some(Schedule::Interval(Duration::from_secs(5))),
            ..ScheduleOverrideFields::default()
        };
        let error = validate_effective(&code, &fields).expect_err("jitter too wide");
        assert!(error.to_string().contains("jitter"), "{error}");
        let with_jitter = ScheduleOverrideFields {
            jitter: Some(Duration::ZERO),
            ..fields
        };
        assert!(validate_effective(&code, &with_jitter).is_ok());
    }

    #[test]
    fn validation_rejects_a_schedule_override_on_a_classic_dag() {
        let mut code = dag("etl", Some(hourly()));
        code.is_unified = false;
        let fields = ScheduleOverrideFields {
            schedule: Some(minutely()),
            ..ScheduleOverrideFields::default()
        };
        assert!(validate_effective(&code, &fields).is_err());
    }

    fn workflow_override(name: &str, fields: WorkflowSchedulePatch) -> WorkflowScheduleOverride {
        WorkflowScheduleOverride {
            workflow_name: name.to_string(),
            fields,
            reason: None,
            updated_by: "ops".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn a_workflow_override_applies_to_its_code_declared_schedule_only() {
        let catalog = DagCatalog::new();
        let schedules = vec![
            WorkflowSchedule::new("nightly", hourly()),
            WorkflowSchedule::new("weekly", hourly()),
        ];
        let overrides = ScheduleOverrides::none().with_workflow_overrides([workflow_override(
            "nightly",
            WorkflowSchedulePatch {
                schedule: Some(minutely()),
                queue_name: Some("urgent".to_string()),
                ..WorkflowSchedulePatch::default()
            },
        )]);
        let effective = overrides.apply_to_workflow_schedules(&schedules, &catalog);
        assert_eq!(effective[0].schedule, minutely());
        assert_eq!(effective[0].queue_name, "urgent");
        assert_eq!(effective[0].max_active_runs, schedules[0].max_active_runs);
        assert_eq!(
            effective[1].schedule,
            hourly(),
            "another schedule keeps code"
        );
    }

    #[test]
    fn a_workflow_override_skips_a_lowered_dag_schedule_of_the_same_name() {
        let mut catalog = DagCatalog::new();
        catalog.insert("etl".to_string(), dag("etl", Some(hourly())));
        let schedules = vec![lowered(&catalog["etl"])];
        let overrides = ScheduleOverrides::none().with_workflow_overrides([workflow_override(
            "etl",
            WorkflowSchedulePatch {
                schedule: Some(minutely()),
                ..WorkflowSchedulePatch::default()
            },
        )]);
        let effective = overrides.apply_to_workflow_schedules(&schedules, &catalog);
        assert_eq!(effective[0].schedule, hourly());
    }

    #[test]
    fn workflow_merge_keeps_stored_fields_and_drops_input() {
        let stored = WorkflowSchedulePatch {
            schedule: Some(minutely()),
            calendar: Some(Some("holidays".to_string())),
            ..WorkflowSchedulePatch::default()
        };
        let patch = WorkflowSchedulePatch {
            calendar: Some(None),
            max_runs: Some(Some(5)),
            input: Some(serde_json::json!({"env": "B"})),
            ..WorkflowSchedulePatch::default()
        };
        let merged = merge_workflow_fields(&patch, &stored);
        assert_eq!(merged.schedule, Some(minutely()));
        assert_eq!(merged.calendar, Some(None));
        assert_eq!(merged.max_runs, Some(Some(5)));
        assert!(merged.input.is_none());
        assert!(workflow_fields_are_empty(&WorkflowSchedulePatch::default()));
        assert!(!workflow_fields_are_empty(&merged));
    }

    #[test]
    fn a_zero_run_budget_means_no_budget() {
        let mut code = WorkflowSchedule::new("nightly", hourly());
        code.max_runs = Some(3);
        let fields = WorkflowSchedulePatch {
            max_runs: Some(Some(0)),
            ..WorkflowSchedulePatch::default()
        };
        assert_eq!(effective_workflow_schedule(&code, &fields).max_runs, None);
    }

    #[test]
    fn workflow_validation_rejects_an_input_and_a_bad_cadence() {
        let code = WorkflowSchedule::new("nightly", hourly());
        let with_input = WorkflowSchedulePatch {
            input: Some(serde_json::Value::Null),
            ..WorkflowSchedulePatch::default()
        };
        assert!(validate_effective_workflow(&code, &with_input).is_err());
        let zero = WorkflowSchedulePatch {
            schedule: Some(Schedule::Interval(Duration::ZERO)),
            ..WorkflowSchedulePatch::default()
        };
        assert!(validate_effective_workflow(&code, &zero).is_err());
        assert!(validate_effective_workflow(&code, &WorkflowSchedulePatch::default()).is_ok());
    }

    #[test]
    fn workflow_fields_json_keeps_absent_and_null_apart() {
        let fields = WorkflowSchedulePatch {
            calendar: Some(None),
            end_at: Some(Some(Utc::now())),
            skip_policy: Some(SkipPolicy::RunNextBusinessDay),
            catchup_policy: Some(Some(CatchupPolicy::Window(Duration::from_secs(600)))),
            retry_policy: Some(None),
            ..WorkflowSchedulePatch::default()
        };
        let stored =
            serde_json::to_value(WorkflowFieldsJson::from_fields(&fields)).expect("serialize");
        let object = stored.as_object().expect("an object");
        assert!(object["calendar"].is_null());
        assert!(
            !object.contains_key("max_runs"),
            "an absent field is not stored"
        );
        let back: WorkflowFieldsJson = serde_json::from_value(stored).expect("deserialize");
        let back = back.into_fields(ScheduleOverrideFields::default());
        assert_eq!(back.calendar, Some(None));
        assert_eq!(back.max_runs, None);
        assert_eq!(back.end_at, fields.end_at);
        assert_eq!(back.skip_policy, fields.skip_policy);
        assert_eq!(back.catchup_policy, fields.catchup_policy);
        assert!(matches!(back.retry_policy, Some(None)));
        assert!(
            serde_json::from_value::<WorkflowFieldsJson>(serde_json::json!({"queu_name": "x"}))
                .is_err(),
            "an unknown key is a corrupt row, not a silent inherit"
        );
    }

    #[test]
    fn storage_form_round_trips() {
        for schedule in [
            Schedule::Cron("0 2 * * *".to_string()),
            Schedule::CronInTimezone {
                expr: "0 9 * * 1-5".to_string(),
                tz: "America/Chicago".to_string(),
            },
            minutely(),
            Schedule::Manual,
        ] {
            let stored = schedule_to_storage(&schedule);
            assert_eq!(schedule_from_storage(&stored), Some(schedule), "{stored}");
        }
        assert_eq!(schedule_from_storage("bogus"), None);
    }
}
