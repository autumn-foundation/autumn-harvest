//! The partial update of a workflow schedule, and its override rules.
//!
//! The #771 schedule edit and the #2386 schedule override both use
//! [`WorkflowSchedulePatch`]. The type and its merge rules have no database
//! code, so both backends share them. The SQLite backend applies an override
//! with the same rules as Postgres (issue #2390).

use chrono::{DateTime, Utc};
use std::time::Duration;

use crate::error::{HarvestError, HarvestResult};
use crate::policy::{OverlapPolicy, Schedule, WorkflowSchedule};

/// Partial, id-keyed update for an existing workflow schedule (issue #771).
///
/// Every field is optional: `None` leaves the stored value unchanged.
/// Nullable columns use a double-`Option` — the outer `None` means
/// "unchanged", `Some(None)` means "clear to NULL", `Some(Some(v))` sets `v`.
///
/// The schedule's workflow type is deliberately absent: it is not editable
/// (changing it is semantically a different schedule).
#[derive(Debug, Clone, Default)]
pub struct WorkflowSchedulePatch {
    /// New schedule cadence. Changing it recomputes `next_run_at` anchored
    /// at now — elapsed slots are never retroactively fired.
    pub schedule: Option<Schedule>,
    /// New JSON input passed to every scheduled run.
    pub input: Option<serde_json::Value>,
    /// New task queue name for dispatched runs.
    pub queue_name: Option<String>,
    /// New legacy catchup bool. Ignored when a `catchup_policy` is stored or
    /// provided (the policy takes precedence, issue #484).
    pub catchup: Option<bool>,
    /// New maximum number of concurrently running scheduled executions.
    pub max_active_runs: Option<u32>,
    /// New jitter window (issue #240).
    pub jitter: Option<Duration>,
    /// New overlap policy (issue #241).
    pub overlap_policy: Option<OverlapPolicy>,
    /// New maximum buffered slots under `BufferAll` (issue #241).
    pub buffer_all_max: Option<u32>,
    /// New calendar association (issue #337). `Some(None)` detaches.
    pub calendar: Option<Option<String>>,
    /// New calendar skip policy (issue #337).
    pub skip_policy: Option<crate::policy::SkipPolicy>,
    /// New auto-pause threshold (issue #360). `Some(None)` disables.
    pub consecutive_failure_limit: Option<Option<u32>>,
    /// New absolute cutoff (issue #478). `Some(None)` removes the cutoff.
    pub end_at: Option<Option<DateTime<Utc>>>,
    /// New total run budget (issue #478). `Some(None)` (or `Some(Some(0))`)
    /// removes the budget.
    pub max_runs: Option<Option<u32>>,
    /// New bounded catchup policy (issue #484). `Some(None)` clears the
    /// policy columns, falling back to the legacy `catchup` bool.
    pub catchup_policy: Option<Option<crate::policy::CatchupPolicy>>,
    /// New schedule-level retry policy (issue #523). `Some(None)` clears.
    pub retry_policy: Option<Option<crate::policy::RetryPolicy>>,
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
