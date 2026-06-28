//! Workflow execution persistence helpers.
//!
//! The public start helper in this module gives callers idempotent workflow
//! start semantics scoped to `(workflow_name, workflow_id)`.

use chrono::Utc;
use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use scoped_futures::ScopedFutureExt;
use uuid::Uuid;

use crate::build_routing;
use crate::completion_trigger::DeferredTriggerStart;
use crate::error::{HarvestError, HarvestResult, database_error};
use crate::event::WorkflowEvent;
use crate::models::{NewHarvestSignal, NewWorkflowExecution, WorkflowExecution};
use crate::queue::{self, EnqueueParams, TaskType};
use crate::schema::{harvest_signals, harvest_workflow_executions};
use crate::store;
use crate::telemetry::TraceContextCarrier;
use crate::types::{ExecutionId, ParentClosePolicy, Priority, WorkflowIdReusePolicy};

/// Parameters for starting a workflow execution.
///
/// `exec_id` is the workflow's routing key: its UUID carries the target
/// [`crate::types::ShardId`] in its first two bytes (see
/// [`ExecutionId::new_for_shard`]). In multi-shard deployments the caller
/// picks the shard via [`crate::ShardRouter`] and mints the id with
/// [`ExecutionId::new_for_shard`] before calling this helper. Single-shard
/// deployments can pass `ExecutionId::new_for_shard(ShardId::new(0))` or, for
/// tests and non-production code, the sentinel-producing `ExecutionId::new()`.
#[derive(Debug, Clone)]
pub struct StartWorkflowParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    /// How to handle a duplicate `(workflow_name, workflow_id)` collision.
    /// Defaults to [`WorkflowIdReusePolicy::AllowDuplicate`].
    pub reuse_policy: WorkflowIdReusePolicy,
    /// W3C trace context captured at the call site (e.g., from the HTTP handler's
    /// `harvest.workflow.schedule` span) and stored on the task row so the worker
    /// can stitch the trace across the queue boundary (ADR-0001 §3).
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout` (issue #243).
    ///
    /// When `Some`, the effective timeout is `execution_timeout.min(ceiling)`.
    /// `None` means no ceiling is enforced.  Typically populated from
    /// `BuiltHarvest::max_workflow_execution_timeout` by the plugin layer.
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Pre-resolved concurrency group key for this workflow run (issue #247).
    ///
    /// Callers resolve the key expression from `WorkflowInfo.concurrency.key_expr`
    /// against the input payload via [`crate::concurrency::resolve_concurrency_key`]
    /// before constructing `StartWorkflowParams`. When `None`, no per-key cap is
    /// applied and only the worker-level semaphore limits concurrency.
    pub concurrency_key: Option<String>,
    /// Maximum number of RUNNING workflow tasks allowed for [`Self::concurrency_key`].
    /// Required whenever `concurrency_key` is `Some`; ignored when it is `None`.
    pub concurrency_limit: Option<u32>,
    /// Within-queue claim priority for this workflow execution (issue #249).
    ///
    /// Stored on the task queue row; does not affect the event history or
    /// replay determinism. Defaults to [`Priority::Normal`] so pre-upgrade
    /// callers that do not set this field are unaffected.
    pub priority: Priority,
    /// Maximum allowed byte size for the workflow input payload (issue #252).
    ///
    /// Enforced only on the fresh-insert path: duplicate collisions resolve
    /// against the existing execution without touching the input. Zero means
    /// uncapped (the default for callers that do not configure a cap).
    pub max_workflow_input_bytes: u64,
    /// Optional timestamp to start the workflow at (issue #322).
    pub start_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional duration delay before starting the workflow (issue #322).
    pub delay: Option<chrono::Duration>,
    /// Server-side ceiling on start delay (issue #322).
    pub max_workflow_start_delay: Option<chrono::Duration>,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    /// Ambient string key-value context propagated to all activities and child
    /// workflows without threading through function signatures (issue #481).
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget for this workflow run (issue #487).
    ///
    /// When set, `sla_deadline_at = started_at + sla` is persisted.  A scanner
    /// detects breach (`now > sla_deadline_at` while RUNNING/SUSPENDED) and
    /// emits `harvest.workflow.sla_breached` exactly once — without altering the
    /// run's lifecycle.
    ///
    /// If `sla > effective_timeout` (the hard deadline), `sla` is clamped down
    /// to `effective_timeout` at start time.  `None` = no SLA enforced.
    pub sla: Option<chrono::Duration>,
    /// The `harvest_schedules.id` that triggered this execution (issue #488).
    /// `None` for manually-started (non-scheduled) workflows. When `Some`, the
    /// start path resolves the prior COMPLETED output and most-recent terminal
    /// error for this schedule and freezes them into the `WorkflowStarted` event.
    pub schedule_id: Option<uuid::Uuid>,
    /// The logical schedule slot this run fires for (issue #488). Carryover selects the
    /// previous fire by this slot (not completion time), so out-of-order completions
    /// (overlap / catch-up / backfill) can't roll an incremental cursor backward.
    /// `None` for manual starts and any non-scheduled call site.
    pub scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    /// Attempt number for this execution in the retry chain (issue #523). 1 = first attempt.
    /// Callers starting a fresh workflow pass `1`. The retry hook passes `workflow_attempt + 1`.
    pub workflow_attempt: u32,
    /// Effective retry policy frozen at start time (issue #523).
    ///
    /// Precedence: per-start override > schedule-default > workflow-type default, then clamped
    /// by `max_workflow_attempts_ceiling`. `None` = no auto-retry for this execution.
    pub workflow_retry_policy: Option<crate::policy::RetryPolicy>,
    /// ID of the prior failed execution this run retries (issue #523). `None` for first attempt.
    pub retry_of_exec_id: Option<uuid::Uuid>,
    /// Server-side ceiling on `retry_policy.max_attempts` (issue #523).
    ///
    /// When `Some(n)`, the effective max attempts = `min(policy.max_attempts, n)`.
    /// Typically sourced from `BuiltHarvest::max_workflow_attempts`.
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// Dispatch origin of this execution (issue #534).
    ///
    /// One of [`ORIGIN_SCHEDULED`], [`ORIGIN_BACKFILL`], or [`ORIGIN_MANUAL_TRIGGER`]
    /// for schedule-attributed runs (set alongside `schedule_id`); `None` for every
    /// non-scheduled call site. Persisted as metadata only — it never affects replay,
    /// carryover, or shard routing, and is surfaced solely by the per-schedule
    /// run-history endpoint to distinguish normal cadence from backfill/manual fires.
    pub origin: Option<&'a str>,
}

/// Origin marker for a normal scheduler-tick fire (issue #534).
pub const ORIGIN_SCHEDULED: &str = "scheduled";
/// Origin marker for a run created by a schedule backfill (issue #534).
pub const ORIGIN_BACKFILL: &str = "backfill";
/// Origin marker for an ad-hoc operator `trigger-now` fire of a schedule (issue #534).
pub const ORIGIN_MANUAL_TRIGGER: &str = "manual_trigger";

impl StartWorkflowParams<'_> {
    /// Shard derived from the encoded `exec_id`, used to populate the row's
    /// `shard_id` column. Returns `0` when the caller passed an unencoded id
    /// (tests / legacy call sites), matching the pre-sharding default.
    #[must_use]
    pub fn shard_id(&self) -> i32 {
        let shard = self.exec_id.shard();
        if shard.is_unencoded() {
            0
        } else {
            shard.as_i32()
        }
    }
}

/// Result of an idempotent workflow start attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartedWorkflowExecution {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub created: bool,
}

impl StartedWorkflowExecution {
    fn from_row(execution: WorkflowExecution, created: bool) -> Self {
        Self {
            exec_id: ExecutionId::from_uuid(execution.id),
            workflow_name: execution.workflow_name,
            workflow_id: execution.workflow_id,
            state: execution.state,
            created,
        }
    }
}

/// Result of a workflow cancellation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelledWorkflowExecution {
    /// Cancelled workflow execution ID.
    pub exec_id: ExecutionId,
    /// Final execution state.
    pub state: String,
    /// Stored cancellation reason.
    pub reason: String,
    /// `true` when this request performed the terminal transition.
    pub newly_cancelled: bool,
    /// Number of pending/running task rows failed by this request.
    pub failed_task_count: usize,
    /// Workflow type name — used by callers that want to emit per-workflow
    /// metrics without re-querying the execution row.
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
    /// The state the execution was in before this transition.
    pub prior_state: String,
}

impl CancelledWorkflowExecution {
    fn idempotent(exec_id: ExecutionId, execution: WorkflowExecution) -> Self {
        Self {
            exec_id,
            state: execution.state.clone(),
            reason: execution
                .error
                .unwrap_or_else(|| "workflow already cancelled".to_string()),
            newly_cancelled: false,
            failed_task_count: 0,
            workflow_name: execution.workflow_name,
            queue_name: execution.queue_name,
            prior_state: execution.state,
        }
    }

    fn newly_cancelled(
        exec_id: ExecutionId,
        final_state: &str,
        reason: String,
        failed_task_count: usize,
        workflow_name: String,
        queue_name: String,
        prior_state: String,
    ) -> Self {
        Self {
            exec_id,
            state: final_state.to_string(),
            reason,
            newly_cancelled: true,
            failed_task_count,
            workflow_name,
            queue_name,
            prior_state,
        }
    }
}

/// Start a workflow execution or load the existing one, returning both the result
/// and any deferred completion-trigger starts **without spawning them**.
///
/// This is the low-level primitive for callers that run the start inside a
/// larger outer transaction: the `DeferredTriggerStart`s must only be spawned
/// *after* that outer transaction commits, otherwise trigger workflows could
/// start for a start that later rolls back (issue #499 debounce scanner).
///
/// The plain [`start_or_load_workflow_execution`] wrapper spawns them itself for
/// the common standalone case.
///
/// ## Policy behaviour
///
/// | Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
/// |-------------|------------------|-------------------|---------------------------|----------------------|
/// | none | create | create | create | create |
/// | RUNNING | return existing | `Err(AlreadyExists)` | return existing | cancel + start fresh |
/// | COMPLETED | return existing | `Err(AlreadyExists)` | return existing | start fresh |
/// | FAILED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
/// | CANCELLED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
///
/// `in_outer_transaction` tells the function whether the caller is running it
/// inside the caller's own transaction. It matters only for the
/// `TerminateIfRunning` pre-check cancellation, which commits (Transaction 1)
/// before the replacement start (Transaction 2): if the replacement start then
/// fails, the prior run's completion-trigger / parent-close follow-up starts
/// must still be spawned for a **top-level** caller (`false`), but must be
/// **suppressed** for a caller inside an outer transaction (`true`) whose
/// rollback reverts that cancellation. On success the follow-ups are always
/// returned for the caller to spawn after its outer commit.
///
/// `reject_fresh_if_debounced`, when `true`, makes the function return
/// [`HarvestError::DebounceFreshStart`] (rolling the transaction back) if the
/// reuse-policy decision under the `FOR UPDATE` lock would create a **fresh**
/// execution (insert, or a `replace_execution` seal+insert). An attach /
/// return-existing decision proceeds normally. This is the atomic gate the
/// debounce-aware HTTP entry points use to allow attach/idempotent calls while
/// routing or rejecting true fresh starts without a TOCTOU (issue #499).
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::DebounceFreshStart`] when a fresh start is gated by
///   `reject_fresh_if_debounced`.
/// - [`HarvestError::Database`] for insert/query failures.
/// - Propagates queue/event-store failures from the start transaction.
#[allow(clippy::too_many_lines)]
pub async fn start_or_load_workflow_execution_collect(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
    in_outer_transaction: bool,
    reject_fresh_if_debounced: bool,
) -> HarvestResult<(StartedWorkflowExecution, Vec<DeferredTriggerStart>)> {
    let exec_id = request.exec_id;
    let shard_id_value = request.shard_id();

    // Validate delayed start parameters (issue #322)
    if request.start_at.is_some() && request.delay.is_some() {
        return Err(HarvestError::Config(
            "Cannot specify both start_at and delay".to_string(),
        ));
    }

    let max_delay = request
        .max_workflow_start_delay
        .unwrap_or_else(|| chrono::Duration::days(365));

    if let Some(d) = request.delay {
        if d < chrono::Duration::zero() {
            return Err(HarvestError::Config(
                "Start delay cannot be negative".to_string(),
            ));
        }
        if d > max_delay {
            return Err(HarvestError::Config(format!(
                "Requested delay ({d:?}) exceeds maximum permitted delay ({max_delay:?})",
            )));
        }
    }

    let now = Utc::now();

    if let Some(sa) = request.start_at {
        let max_start_at = now + max_delay;
        if sa > max_start_at {
            return Err(HarvestError::Config(format!(
                "Requested start_at ({sa:?}) exceeds maximum permitted delay ({max_start_at:?})",
            )));
        }
    }

    let target_start_time = if let Some(d) = request.delay {
        now + d
    } else if let Some(sa) = request.start_at {
        sa
    } else {
        now
    };

    // Look up the active build policy for this queue. If a policy exists, new
    // executions are stamped with its build_id so workers can enforce routing.
    // Resolved *before* the TerminateIfRunning pre-check so a build-policy DB
    // error returns without leaving a committed pre-check cancellation whose
    // follow-up starts would be lost.
    let policy = build_routing::get_build_policy(conn, request.queue_name).await?;
    let assigned_build = policy.map(|p| p.build_id);

    // For TerminateIfRunning: if there is an existing RUNNING execution, cancel
    // it (Transaction 1) before the start transaction below (Transaction 2). A
    // crash between the two leaves the prior workflow CANCELLED with no new run;
    // retrying with the same policy starts fresh on the next attempt because the
    // CANCELLED row is treated as "start fresh" by TerminateIfRunning.
    // Deferred follow-up starts produced by the TerminateIfRunning pre-check's
    // cancellation (the prior run's completion-trigger / parent-close cascade).
    // Collected here via the no-spawn cancel variant rather than spawned inline:
    // this collect function may itself run inside a caller's transaction (e.g. the
    // debounce scanner's fire transaction), where the spawning wrapper
    // `cancel_workflow_execution` would launch follow-ups that a later rollback of
    // that outer transaction could orphan. They are appended to the returned
    // deferred list so the caller spawns them only after its outer commit.
    let mut pre_check_deferred: Vec<DeferredTriggerStart> = Vec::new();
    if request.reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning
        // Skip the pre-check cancellation when we're going to reject this fresh
        // start for a debounced workflow — otherwise we'd cancel the prior run
        // (Transaction 1) and then reject, leaving it cancelled with no successor.
        && !reject_fresh_if_debounced
        && let Some(existing) =
            try_load_by_key(conn, request.workflow_name, request.workflow_id).await?
        && matches!(existing.state.as_str(), "RUNNING" | "PAUSED")
    {
        let existing_exec_id = ExecutionId::from_uuid(existing.id);
        // Ignore Config errors: the execution may have transitioned to a terminal
        // state between the pre-check and the cancel lock. In that race the prior
        // run is already done, so we just continue to the start transaction below.
        match cancel_workflow_execution_collect(
            conn,
            existing_exec_id,
            "terminated to start new execution",
        )
        .await
        {
            Ok((_, mut deferred)) => pre_check_deferred.append(&mut deferred),
            Err(HarvestError::Config(_)) => {}
            Err(e) => return Err(e),
        }
    }

    // Apply the server-side ceiling (if any) before computing the deadline.
    // The effective timeout is the minimum of the per-call value and the
    // operator-configured ceiling; this prevents callers from requesting
    // arbitrarily long SLA windows even when they supply an explicit timeout.
    let effective_timeout = match (
        request.execution_timeout,
        request.max_execution_timeout_ceiling,
    ) {
        (Some(t), Some(ceiling)) => Some(t.min(ceiling)),
        (other, _) => other,
    };

    // Compute deadline_at relative to target_start_time (issue #322).
    let deadline_at = effective_timeout.map(|d| target_start_time + d);

    // Compute effective SLA — clamp down to the hard timeout when sla > deadline
    // (issue #487): the hard timeout fires first so the soft signal can never fire.
    // A non-positive SLA budget (<= 0) is treated as "no SLA": persisting an
    // `sla_deadline_at` at or before `started_at` would flag the run as breached
    // on the very next scan, which is never a meaningful budget.
    let effective_sla = match (request.sla, effective_timeout) {
        (Some(sla), _) if sla <= chrono::Duration::zero() => None,
        (Some(sla), Some(hard)) => Some(sla.min(hard)),
        (Some(sla), None) => Some(sla),
        (None, _) => None,
    };
    let sla_deadline_at = effective_sla.map(|d| target_start_time + d);

    let row = NewWorkflowExecution {
        id: exec_id.as_uuid(),
        workflow_name: request.workflow_name,
        workflow_id: request.workflow_id,
        run_id: Uuid::new_v4(),
        shard_id: shard_id_value,
        input: request.input.clone(),
        parent_id: request.parent_id,
        queue_name: request.queue_name,
        execution_timeout: effective_timeout,
        deadline_at,
        sla: effective_sla,
        sla_deadline_at,
        memo: request.memo.clone(),
        search_attrs: request.search_attrs.clone(),
        assigned_build_id: assigned_build.clone(),
        parent_close_policy: None, // root or awaited child; detached uses worker path
        owner: request.owner,
        runbook_url: request.runbook_url,
        severity: request.severity,
        context_headers: request
            .context_headers
            .as_ref()
            .map(|h| serde_json::to_value(h).unwrap_or(serde_json::Value::Null)),
        schedule_id: request.schedule_id,
        scheduled_for: request.scheduled_for,
        workflow_attempt: request.workflow_attempt.cast_signed(),
        workflow_retry_policy: request.workflow_retry_policy.as_ref().map(|p| {
            // Clamp max_attempts to ceiling before persisting.
            let mut p = p.clone();
            if let Some(ceiling) = request.max_workflow_attempts_ceiling {
                p.max_attempts = p.max_attempts.min(ceiling);
            }
            serde_json::to_value(&p).unwrap_or(serde_json::Value::Null)
        }),
        retry_of_exec_id: request.retry_of_exec_id,
        origin: request.origin,
    };
    let mut enqueue = EnqueueParams::new(
        request.queue_name.to_owned(),
        TaskType::Workflow,
        request.input.clone(),
    );
    enqueue.workflow_exec_id = Some(exec_id.as_uuid());
    enqueue.required_build_id = assigned_build.clone();
    // ADR-0001 §3: store the caller's trace context so the worker can restore it.
    enqueue.trace_context.clone_from(&request.trace_context);
    enqueue.concurrency_key.clone_from(&request.concurrency_key);
    enqueue.max_concurrent = request.concurrency_limit;
    enqueue.priority = request.priority.as_i32();
    if request.delay.is_some_and(|d| d > chrono::Duration::zero()) || request.start_at.is_some() {
        enqueue.scheduled_at = target_start_time;
    }

    let main_result = conn
        .transaction::<(StartedWorkflowExecution, Vec<DeferredTriggerStart>), HarvestError, _>(
            |conn| {
                let row = row;
                let enqueue = enqueue.clone();
                let request = request.clone();
                async move {
                    // `on_conflict_do_nothing()` (no explicit target) lets Postgres
                    // arbitrate against the partial unique index installed by the
                    // continue-as-new migration, which only enforces uniqueness on
                    // rows whose state is not sealed (`CONTINUED_AS_NEW` or
                    // `TERMINATED`). A previously sealed continue-as-new chain or reset
                    // source therefore does not block reusing the same
                    // (workflow_name, workflow_id).
                    let inserted = diesel::insert_into(harvest_workflow_executions::table)
                        .values(&row)
                        .on_conflict_do_nothing()
                        .returning(WorkflowExecution::as_returning())
                        .get_result(conn)
                        .await
                        .optional()
                        .map_err(database_error)?;

                    if let Some(execution) = inserted {
                        // Atomic debounce gate: this INSERT is a fresh start. For a
                        // debounced workflow that must go through debounce admission,
                        // roll it back and signal the caller (no TOCTOU — decided here
                        // under the inserted row, not via an unlocked pre-scan).
                        if reject_fresh_if_debounced {
                            return Err(HarvestError::DebounceFreshStart {
                                workflow_name: request.workflow_name.to_string(),
                                workflow_id: request.workflow_id.to_string(),
                            });
                        }
                        if request.start_at.is_some_and(|sa| sa < now) {
                            return Err(HarvestError::Config(
                                "Requested start_at is in the past".to_string(),
                            ));
                        }
                        // Enforce the input cap only on the fresh-insert path. Duplicates
                        // never reach here so the reuse-policy outcome is unaffected.
                        if request.max_workflow_input_bytes > 0 {
                            let observed =
                                serde_json::to_string(&request.input).map_or(0, |s| s.len() as u64);
                            if observed > request.max_workflow_input_bytes {
                                return Err(crate::error::HarvestError::PayloadTooLarge {
                                    kind: crate::error::PayloadKind::WorkflowInput,
                                    observed_bytes: observed,
                                    cap_bytes: request.max_workflow_input_bytes,
                                    workflow_type: request.workflow_name.to_string(),
                                    activity_name: None,
                                });
                            }
                        }
                        // Resolve last-completion-result carryover (issue #488).
                        // Runs inside the same transaction on the same shard-local
                        // connection so the read is consistent with the just-inserted
                        // row. Excludes the new row (`id != exec_id`) as a safety
                        // guard — the new row has state RUNNING, not COMPLETED, so it
                        // can never match, but the explicit exclusion is defensive.
                        let (carryover_result, carryover_error) =
                            if let Some(sched_id) = request.schedule_id {
                                resolve_carryover(
                                    conn,
                                    sched_id,
                                    exec_id.as_uuid(),
                                    request.scheduled_for,
                                )
                                .await?
                            } else {
                                (None, None)
                            };
                        let started_event = WorkflowEvent::WorkflowStarted {
                            input: request.input.clone(),
                            timestamp: target_start_time,
                            last_completion_result: carryover_result,
                            last_error: carryover_error,
                            scheduled_time: request.scheduled_for,
                        };
                        store::append_events(conn, exec_id, &[started_event], 0).await?;
                        queue::enqueue(conn, &enqueue).await?;
                        return Ok((
                            StartedWorkflowExecution::from_row(execution, true),
                            Vec::new(),
                        ));
                    }

                    // INSERT was a no-op: a prior execution exists. Lock the row to
                    // prevent concurrent state changes while we decide what to do.
                    let existing = load_workflow_execution_by_key_for_update(
                        conn,
                        request.workflow_name,
                        request.workflow_id,
                    )
                    .await?;

                    match request.reuse_policy {
                        WorkflowIdReusePolicy::AllowDuplicate => Ok((
                            StartedWorkflowExecution::from_row(existing, false),
                            Vec::new(),
                        )),

                        WorkflowIdReusePolicy::RejectDuplicate => {
                            Err(HarvestError::AlreadyExists {
                                existing_exec_id: ExecutionId::from_uuid(existing.id),
                                existing_state: existing.state,
                            })
                        }

                        WorkflowIdReusePolicy::AllowDuplicateFailedOnly => {
                            match existing.state.as_str() {
                                "FAILED" | "CANCELLED" => {
                                    // Replacing a terminal prior is a fresh start.
                                    if reject_fresh_if_debounced {
                                        return Err(HarvestError::DebounceFreshStart {
                                            workflow_name: request.workflow_name.to_string(),
                                            workflow_id: request.workflow_id.to_string(),
                                        });
                                    }
                                    // Only these two explicitly abnormal states start fresh.
                                    replace_execution(
                                        conn, existing, &row, &enqueue, exec_id, &request, now,
                                    )
                                    .await
                                }
                                _ => {
                                    // RUNNING, COMPLETED, TIMED_OUT, or any other state:
                                    // return the existing execution unchanged.
                                    Ok((
                                        StartedWorkflowExecution::from_row(existing, false),
                                        Vec::new(),
                                    ))
                                }
                            }
                        }

                        WorkflowIdReusePolicy::TerminateIfRunning => {
                            // TerminateIfRunning always starts fresh (cancel + replace).
                            // Gate it before cancelling anything so a debounced workflow
                            // is routed to admission instead.
                            if reject_fresh_if_debounced {
                                return Err(HarvestError::DebounceFreshStart {
                                    workflow_name: request.workflow_name.to_string(),
                                    workflow_id: request.workflow_id.to_string(),
                                });
                            }
                            // The pre-check above cancelled any active prior execution
                            // (Transaction 1). By the time we reach this point the prior
                            // execution's state is CANCELLED, FAILED, COMPLETED, or —
                            // under extreme concurrency — still active (RUNNING/PAUSED).
                            // All cases start fresh; for the still-active race we inline
                            // the cancel here so the new start is not silently blocked
                            // and the prior run's parked task is failed (PAUSED is active
                            // and occupies the uniqueness slot, so it must be cancelled
                            // before replace_execution seals it; issue #383).
                            let mut deferred =
                                if matches!(existing.state.as_str(), "RUNNING" | "PAUSED") {
                                    inline_cancel(conn, ExecutionId::from_uuid(existing.id)).await?
                                } else {
                                    Vec::new()
                                };
                            let (started_wf, mut extra_deferred) = replace_execution(
                                conn, existing, &row, &enqueue, exec_id, &request, now,
                            )
                            .await?;
                            deferred.append(&mut extra_deferred);
                            Ok((started_wf, deferred))
                        }
                    }
                }
                .scope_boxed()
            },
        )
        .await;

    match main_result {
        Ok((cancel_result, mut deferred_starts)) => {
            // Spawn order: the pre-check cancellation's follow-ups first, then the
            // start's own deferred follow-ups — all returned for the caller to spawn
            // after its outer commit.
            pre_check_deferred.append(&mut deferred_starts);
            Ok((cancel_result, pre_check_deferred))
        }
        Err(e) => {
            // The replacement start failed. A TerminateIfRunning pre-check cancel may
            // already be durable (Transaction 1 committed): for a top-level caller we
            // spawn its follow-ups now, since the cancellation is permanent even
            // though no new run started. For a caller inside an outer transaction the
            // cancellation will be rolled back with that transaction, so we suppress
            // the spawn and let the caller's rollback revert everything.
            if !in_outer_transaction {
                for start in pre_check_deferred {
                    start.spawn();
                }
            }
            Err(e)
        }
    }
}

/// Start a workflow execution or load the existing one, applying the caller's
/// [`WorkflowIdReusePolicy`] when a duplicate `(workflow_name, workflow_id)`
/// collision occurs.
///
/// Thin wrapper around [`start_or_load_workflow_execution_collect`] that spawns
/// any deferred completion-trigger starts before returning.
///
/// ## Policy behaviour
///
/// | Prior state | `AllowDuplicate` | `RejectDuplicate` | `AllowDuplicateFailedOnly` | `TerminateIfRunning` |
/// |-------------|------------------|-------------------|---------------------------|----------------------|
/// | none | create | create | create | create |
/// | RUNNING | return existing | `Err(AlreadyExists)` | return existing | cancel + start fresh |
/// | COMPLETED | return existing | `Err(AlreadyExists)` | return existing | start fresh |
/// | FAILED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
/// | CANCELLED | return existing | `Err(AlreadyExists)` | start fresh | start fresh |
///
/// For `TerminateIfRunning` + RUNNING the cancel is performed in a separate
/// transaction (Transaction 1) before the start transaction (Transaction 2). A
/// failure between the two leaves the prior workflow CANCELLED with no new run
/// started; the caller can retry with the same policy to get a fresh run.
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::Database`] for insert/query failures.
/// - Propagates queue/event-store failures from the start transaction.
pub async fn start_or_load_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
) -> HarvestResult<StartedWorkflowExecution> {
    // Top-level caller (`in_outer_transaction = false`): if a TerminateIfRunning
    // pre-check cancellation commits and the replacement start then fails, the
    // collect fn spawns the cancellation's follow-ups itself before returning Err.
    let (result, deferred_starts) =
        start_or_load_workflow_execution_collect(conn, request, false, false).await?;
    for start in deferred_starts {
        start.spawn();
    }
    Ok(result)
}

/// Transition `existing` to `CONTINUED_AS_NEW` (releasing the partial unique
/// index slot) then insert `new_row` as a fresh execution with its own
/// `WorkflowStarted` event and task queue entry.
async fn replace_execution(
    conn: &mut AsyncPgConnection,
    existing: WorkflowExecution,
    new_row: &NewWorkflowExecution<'_>,
    enqueue: &EnqueueParams,
    new_exec_id: ExecutionId,
    request: &StartWorkflowParams<'_>,
    now: chrono::DateTime<Utc>,
) -> HarvestResult<(StartedWorkflowExecution, Vec<DeferredTriggerStart>)> {
    if request.start_at.is_some_and(|sa| sa < now) {
        return Err(HarvestError::Config(
            "Requested start_at is in the past".to_string(),
        ));
    }

    // Seal the prior execution row as CONTINUED_AS_NEW. This removes it from
    // the partial unique index scope (WHERE state NOT IN sealed states),
    // allowing the new row to be inserted without violating the constraint.
    diesel::update(harvest_workflow_executions::table.find(existing.id))
        .set((
            harvest_workflow_executions::state.eq("CONTINUED_AS_NEW"),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

    let new_execution = diesel::insert_into(harvest_workflow_executions::table)
        .values(new_row)
        .returning(WorkflowExecution::as_returning())
        .get_result(conn)
        .await
        .map_err(database_error)?;

    if request.max_workflow_input_bytes > 0 {
        let observed = serde_json::to_string(&request.input).map_or(0, |s| s.len() as u64);
        if observed > request.max_workflow_input_bytes {
            return Err(crate::error::HarvestError::PayloadTooLarge {
                kind: crate::error::PayloadKind::WorkflowInput,
                observed_bytes: observed,
                cap_bytes: request.max_workflow_input_bytes,
                workflow_type: request.workflow_name.to_string(),
                activity_name: None,
            });
        }
    }
    let start_timestamp = if request.delay.is_some_and(|d| d > chrono::Duration::zero())
        || request.start_at.is_some()
    {
        enqueue.scheduled_at
    } else {
        Utc::now()
    };
    // Resolve scheduled carryover on the replacement path too (issue #488): a reuse
    // policy that replaces a prior row (e.g. AllowDuplicateFailedOnly retrying a failed
    // scheduled slot, or TerminateIfRunning) still carries schedule_id/scheduled_for, so
    // the rerun (and any continue-as-new fork from it) must see the previous fire's
    // carryover rather than behaving like a first scheduled run.
    let (carryover_result, carryover_error) = if let Some(sched_id) = request.schedule_id {
        resolve_carryover(conn, sched_id, new_exec_id.as_uuid(), request.scheduled_for).await?
    } else {
        (None, None)
    };
    let started_event = WorkflowEvent::WorkflowStarted {
        input: request.input.clone(),
        timestamp: start_timestamp,
        last_completion_result: carryover_result,
        last_error: carryover_error,
        scheduled_time: request.scheduled_for,
    };
    store::append_events(conn, new_exec_id, &[started_event], 0).await?;
    queue::enqueue(conn, enqueue).await?;

    Ok((
        StartedWorkflowExecution::from_row(new_execution, true),
        Vec::new(),
    ))
}

/// Inline cancellation for the `TerminateIfRunning` race condition where a
/// RUNNING row appears inside the start transaction despite the pre-check.
/// Appends a `WorkflowCancelled` event, transitions to CANCELLED, and fails
/// open tasks — all within the caller's transaction.
async fn inline_cancel(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<DeferredTriggerStart>> {
    let reason = "terminated to start new execution";
    let history = store::load_history(conn, exec_id).await?;
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowCancelled {
            reason: reason.to_string(),
        }],
        history.next_event_id,
    )
    .await?;
    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason)),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused prior run is sealed (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    queue::fail_open_tasks_for_execution(conn, exec_id, &format!("workflow cancelled: {reason}"))
        .await?;
    let mut deferred = Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Cancelled,
        None,
    )
    .await?;
    deferred.extend(triggers);
    Ok(deferred)
}

/// Cancel a running workflow execution.
///
/// Cancellation is a durable terminal transition: this appends a
/// `WorkflowCancelled` event, marks the execution `CANCELLED`, and fails every
/// pending or running task associated with the execution. Repeating the same
/// operation against an already-cancelled execution is idempotent and does not
/// append another event.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist,
/// [`HarvestError::Config`] when the execution is already terminal for another
/// reason, and [`HarvestError::Database`] for persistence failures.
/// Notify an **awaited** parent that one of its children reached a terminal
/// state out-of-band (operator cancel/terminate), waking the parked parent.
///
/// When a parent blocks on `spawn_child_workflow().await`, the child's terminal
/// transition is normally propagated to the parent by the worker
/// (`wake_parent_for_child_failure`). Operator cancel/terminate seal the child
/// directly, bypassing the worker, so without this the parent parks forever.
///
/// Only **awaited** children are handled here (`parent_id` set,
/// `parent_close_policy` NULL); **detached** children (policy set) are the
/// `apply_parent_close_cascade` parent→child path's responsibility and are
/// skipped. There is no `ChildWorkflowCancelled` event variant, so a cancel and
/// a terminate both surface to the parent as `ChildWorkflowFailed` (the
/// child-await resolves `Err`) — matching the worker failure path and adding no
/// new event variant. Awaited children are co-located on the parent's shard
/// (the worker append-on-child-conn path relies on the same invariant), so this
/// append on the child's connection targets the correct shard.
async fn notify_awaited_parent_of_child_terminal(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
    execution: &WorkflowExecution,
    error: String,
) -> HarvestResult<()> {
    if let Some(parent_uuid) = execution.parent_id
        && execution.parent_close_policy.is_none()
    {
        let parent_exec_id = ExecutionId::from_uuid(parent_uuid);
        // Lock the parent row and skip the wake if it is already terminal (or
        // gone): an awaited child can outlive its parent (e.g. the parent hit its
        // execution timeout while the child kept running), and appending a
        // command-consumable `ChildWorkflowFailed` after the parent's own terminal
        // event would add replay-visible history past closure. The `FOR UPDATE`
        // lock serializes against a concurrent parent termination — which holds
        // the same row lock — so we either observe the parent terminal here and
        // skip, or append before it seals and the parent consumes the event.
        let parent_state: Option<String> = harvest_workflow_executions::table
            .find(parent_exec_id.as_uuid())
            .select(harvest_workflow_executions::state)
            .for_update()
            .first(conn)
            .await
            .optional()
            .map_err(database_error)?;
        if matches!(parent_state, Some(state) if !crate::erase::is_terminal_state(&state)) {
            store::append_single_event(
                conn,
                parent_exec_id,
                WorkflowEvent::ChildWorkflowFailed {
                    child_id: child_exec_id,
                    error,
                },
            )
            .await?;
            queue::wake_workflow_task(conn, parent_exec_id).await?;
        }
    }
    Ok(())
}

/// Cancel a running workflow execution, returning the deferred completion-trigger
/// starts to the caller **without spawning them** (and without recording the
/// terminal metric).
///
/// This is the building block for callers that run the cancellation inside a
/// larger outer transaction (the external-cancel inline persist and outbox
/// paths): the `DeferredTriggerStart`s must only be spawned *after* that outer
/// transaction commits, otherwise trigger workflows could start for a
/// cancellation that later rolls back (issue #492). The plain
/// [`cancel_workflow_execution`] wrapper spawns them and records the metric
/// itself for the common standalone case.
///
/// # Errors
///
/// Same as [`cancel_workflow_execution`].
#[allow(clippy::too_many_lines)]
pub async fn cancel_workflow_execution_collect(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<(CancelledWorkflowExecution, Vec<DeferredTriggerStart>)> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow cancellation requested".to_string()
    } else {
        reason.to_string()
    };

    conn.transaction::<(CancelledWorkflowExecution, Vec<DeferredTriggerStart>), HarvestError, _>(
        |conn| {
            async move {
                let execution = harvest_workflow_executions::table
                    .find(exec_id.as_uuid())
                    .select(WorkflowExecution::as_select())
                    .for_update()
                    .first(conn)
                    .await
                    .optional()
                    .map_err(database_error)?
                    .ok_or_else(|| {
                        HarvestError::NotFound(format!("workflow execution {exec_id}"))
                    })?;

                // Cancellation beats pause (issue #383): a PAUSED execution
                // is cancellable just like a RUNNING one; the transition to
                // CANCELLED clears the pending pause record below.
                let prior_state = execution.state.clone();
                match execution.state.as_str() {
                    "RUNNING" | "PAUSED" => {}
                    "CANCELLED" => {
                        return Ok((
                            CancelledWorkflowExecution::idempotent(exec_id, execution),
                            Vec::new(),
                        ));
                    }
                    state => {
                        return Err(HarvestError::Config(format!(
                            "workflow execution {exec_id} is already terminal ({state})"
                        )));
                    }
                }

                let deleted_pending = diesel::delete(
                    crate::schema::harvest_task_queue::table
                        .filter(
                            crate::schema::harvest_task_queue::workflow_exec_id
                                .eq(Some(exec_id.as_uuid())),
                        )
                        .filter(crate::schema::harvest_task_queue::task_type.eq("workflow"))
                        .filter(crate::schema::harvest_task_queue::state.eq("PENDING"))
                        .filter(crate::schema::harvest_task_queue::scheduled_at.gt(Utc::now())),
                )
                .execute(conn)
                .await
                .map_err(database_error)?;

                let history = store::load_history(conn, exec_id).await?;
                store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::WorkflowCancelled {
                        reason: reason.clone(),
                    }],
                    history.next_event_id,
                )
                .await?;

                let completed_at = Utc::now();
                // Mirror resume_workflow_execution: if this execution was PAUSED,
                // push sla_deadline_at forward by the pause span so the SLA scanner
                // does not record a false breach for time spent paused before cancel
                // (issue #383 × #487). Only extend a deadline that was still ahead
                // when the pause began — a deadline already elapsed while RUNNING
                // stays in the past so its breach is still observed by the scanner.
                let new_sla_deadline_at = if prior_state == "PAUSED" {
                    execution
                        .sla_deadline_at
                        .map(|d| match execution.paused_at {
                            Some(p) if d > p => {
                                d + (completed_at - p).max(chrono::Duration::zero())
                            }
                            _ => d,
                        })
                } else {
                    execution.sla_deadline_at
                };
                let updated =
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
                        .set((
                            harvest_workflow_executions::state.eq("CANCELLED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(reason.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(completed_at)),
                            harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                            // Cancellation wins: clear the pending pause record.
                            harvest_workflow_executions::paused_at
                                .eq(None::<chrono::DateTime<Utc>>),
                            harvest_workflow_executions::pause_reason.eq(None::<String>),
                            harvest_workflow_executions::pause_actor.eq(None::<String>),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;

                if updated == 0 {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is no longer running"
                    )));
                }

                let failed_task_count = queue::fail_open_tasks_for_execution(
                    conn,
                    exec_id,
                    &format!("workflow cancelled: {reason}"),
                )
                .await?;

                let total_failed_or_deleted = deleted_pending + failed_task_count;
                // Wake a parent blocked on this child's await (#787): cancelling
                // an awaited child out-of-band must surface to the parent.
                notify_awaited_parent_of_child_terminal(
                    conn,
                    exec_id,
                    &execution,
                    format!("child workflow cancelled: {reason}"),
                )
                .await?;
                let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                    conn,
                    exec_id,
                    crate::completion_trigger::TerminalState::Cancelled,
                    None,
                )
                .await?;
                deferred.extend(triggers);

                Ok((
                    CancelledWorkflowExecution::newly_cancelled(
                        exec_id,
                        "CANCELLED",
                        reason,
                        total_failed_or_deleted,
                        execution.workflow_name.clone(),
                        execution.queue_name.clone(),
                        prior_state,
                    ),
                    deferred,
                ))
            }
            .scope_boxed()
        },
    )
    .await
}

/// Cancel a running workflow execution.
///
/// Cancellation is a durable terminal transition: this appends a
/// `WorkflowCancelled` event, marks the execution `CANCELLED`, and fails every
/// pending or running task associated with the execution. Repeating the same
/// operation against an already-cancelled execution is idempotent and does not
/// append another event.
///
/// Completion-trigger / parent-close-cascade follow-up starts are spawned after
/// the cancellation transaction commits, and the terminal metric is recorded.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist,
/// [`HarvestError::Config`] when the execution is already terminal for another
/// reason, and [`HarvestError::Database`] for persistence failures.
pub async fn cancel_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<CancelledWorkflowExecution> {
    let (cancel_result, deferred_starts) =
        cancel_workflow_execution_collect(conn, exec_id, reason).await?;

    for start in deferred_starts {
        start.spawn();
    }

    if cancel_result.newly_cancelled {
        metrics.record_workflow_terminal(
            &cancel_result.workflow_name,
            &cancel_result.queue_name,
            crate::telemetry::WorkflowStatus::Cancelled,
        );
    }

    Ok(cancel_result)
}

/// Maximum length of an operator-supplied pause reason (issue #383).
pub const MAX_PAUSE_REASON_LEN: usize = 500;

/// Result of a workflow pause request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PausedWorkflowExecution {
    /// Paused workflow execution ID.
    pub exec_id: ExecutionId,
    /// Execution state after the request (always `"PAUSED"`).
    pub state: String,
    /// Stored pause reason, if any.
    pub reason: Option<String>,
    /// Actor that requested the pause.
    pub actor: String,
    /// `true` when this request performed the `RUNNING → PAUSED` transition;
    /// `false` when the execution was already paused (idempotent).
    pub newly_paused: bool,
    /// Workflow type name (for per-workflow metrics without a re-query).
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
}

/// Result of a workflow resume request.
#[derive(Debug, Clone, PartialEq)]
pub struct ResumedWorkflowExecution {
    /// Resumed workflow execution ID.
    pub exec_id: ExecutionId,
    /// Execution state after the request (always `"RUNNING"`).
    pub state: String,
    /// Actor that requested the resume.
    pub actor: String,
    /// Wall-clock seconds the execution spent paused.
    pub pause_duration_secs: f64,
    /// Workflow type name (for per-workflow metrics without a re-query).
    pub workflow_name: String,
    /// Task queue the execution was dispatched on.
    pub queue_name: String,
}

/// Returns `true` when a pause that started at `paused_at` has exceeded the
/// bounded-pause ceiling `max` as of `now` (issue #383).
///
/// Pure helper used by the auto-resume scanner so the expiry decision can be
/// unit-tested without a database. A non-positive `max` is treated as "expire
/// immediately" so a misconfigured zero ceiling does not strand a paused
/// execution forever.
#[must_use]
pub fn pause_timeout_exceeded(
    paused_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
    max: std::time::Duration,
) -> bool {
    // An overflowing ceiling (absurdly large) effectively never expires → false.
    chrono::Duration::from_std(max).is_ok_and(|max| now - paused_at >= max)
}

/// Pause a running workflow execution (issue #383).
///
/// Pausing is a durable, **non-terminal** transition: it appends a
/// [`WorkflowEvent::WorkflowExecutionPaused`] event, marks the execution
/// `PAUSED`, and records the pause audit metadata. The executor enforces the
/// pause at the claim layer — a workflow task belonging to a `PAUSED` execution
/// is never claimed, so no new commands are dispatched. In-flight activities
/// continue to completion; their results are recorded normally and remain
/// queued behind the pause until [`resume_workflow_execution`].
///
/// Repeating the request against an already-paused execution is idempotent and
/// does not append a second event.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
/// - [`HarvestError::Config`] when the execution is already terminal (→ 409),
///   or the reason exceeds [`MAX_PAUSE_REASON_LEN`] (→ 400).
/// - [`HarvestError::Database`] for persistence failures.
pub async fn pause_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: Option<&str>,
    actor: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<PausedWorkflowExecution> {
    let actor = if actor.trim().is_empty() {
        "anonymous".to_string()
    } else {
        actor.trim().to_string()
    };
    let reason = reason.map(str::trim).filter(|r| !r.is_empty());
    if let Some(r) = reason
        && r.chars().count() > MAX_PAUSE_REASON_LEN
    {
        return Err(HarvestError::Config(format!(
            "pause reason exceeds {MAX_PAUSE_REASON_LEN} characters"
        )));
    }
    let reason = reason.map(ToOwned::to_owned);

    let paused_at = Utc::now();
    let result = conn
        .transaction::<PausedWorkflowExecution, HarvestError, _>(|conn| {
            let reason = reason.clone();
            let actor = actor.clone();
            async move {
                let execution = harvest_workflow_executions::table
                    .find(exec_id.as_uuid())
                    .select(WorkflowExecution::as_select())
                    .for_update()
                    .first(conn)
                    .await
                    .optional()
                    .map_err(database_error)?
                    .ok_or_else(|| {
                        HarvestError::NotFound(format!("workflow execution {exec_id}"))
                    })?;

                match execution.state.as_str() {
                    "RUNNING" => {}
                    "PAUSED" => {
                        return Ok(PausedWorkflowExecution {
                            exec_id,
                            state: "PAUSED".to_string(),
                            reason: execution.pause_reason,
                            actor: execution.pause_actor.unwrap_or(actor),
                            newly_paused: false,
                            workflow_name: execution.workflow_name,
                            queue_name: execution.queue_name,
                        });
                    }
                    state => {
                        return Err(HarvestError::Config(format!(
                            "workflow execution {exec_id} is already terminal ({state})"
                        )));
                    }
                }

                let history = store::load_history(conn, exec_id).await?;
                store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::WorkflowExecutionPaused {
                        paused_at,
                        reason: reason.clone(),
                        actor: actor.clone(),
                    }],
                    history.next_event_id,
                )
                .await?;

                let updated =
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .filter(harvest_workflow_executions::state.eq("RUNNING"))
                        .set((
                            harvest_workflow_executions::state.eq("PAUSED"),
                            harvest_workflow_executions::paused_at.eq(Some(paused_at)),
                            harvest_workflow_executions::pause_reason.eq(reason.clone()),
                            harvest_workflow_executions::pause_actor.eq(Some(actor.clone())),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;

                if updated == 0 {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is no longer running"
                    )));
                }

                Ok(PausedWorkflowExecution {
                    exec_id,
                    state: "PAUSED".to_string(),
                    reason,
                    actor,
                    newly_paused: true,
                    workflow_name: execution.workflow_name,
                    queue_name: execution.queue_name,
                })
            }
            .scope_boxed()
        })
        .await?;

    if result.newly_paused {
        metrics.record_workflow_paused(&result.workflow_name, &result.queue_name);
    }

    Ok(result)
}

/// Resume a paused workflow execution (issue #383).
///
/// Appends a [`WorkflowEvent::WorkflowExecutionResumed`] event, transitions the
/// execution back to `RUNNING`, clears the pause audit metadata, and wakes the
/// parked workflow task so the workflow advances on its next decision attempt.
/// Timers whose fire time elapsed while paused fire immediately in their
/// original order on the next decision; signals queued during the pause are
/// delivered in order.
///
/// # Errors
///
/// - [`HarvestError::NotFound`] when the execution does not exist (→ 404).
/// - [`HarvestError::Config`] when the execution is not in the `PAUSED` state
///   (→ 409).
/// - [`HarvestError::Database`] for persistence failures.
pub async fn resume_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    actor: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<ResumedWorkflowExecution> {
    let actor = if actor.trim().is_empty() {
        "anonymous".to_string()
    } else {
        actor.trim().to_string()
    };

    let resumed_at = Utc::now();
    let result = conn
        .transaction::<ResumedWorkflowExecution, HarvestError, _>(|conn| {
            let actor = actor.clone();
            async move {
                let execution = harvest_workflow_executions::table
                    .find(exec_id.as_uuid())
                    .select(WorkflowExecution::as_select())
                    .for_update()
                    .first(conn)
                    .await
                    .optional()
                    .map_err(database_error)?
                    .ok_or_else(|| {
                        HarvestError::NotFound(format!("workflow execution {exec_id}"))
                    })?;

                if execution.state != "PAUSED" {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is not paused (state: {})",
                        execution.state
                    )));
                }

                // Clamp the pause span to a non-negative duration so a clock skew
                // that puts `paused_at` ahead of `resumed_at` neither reports a
                // negative pause nor rewinds the deadline.
                let pause_span = execution
                    .paused_at
                    .map(|p| resumed_at - p)
                    .filter(|span| *span > chrono::Duration::zero())
                    .unwrap_or_else(chrono::Duration::zero);
                let pause_duration_secs = pause_span.to_std().map_or(0.0, |d| d.as_secs_f64());

                // Pause suspends the SLA clock (issue #383 × #243): push the
                // absolute execution deadline forward by the time spent paused so
                // paused wall-clock does not count against the workflow's
                // `execution_timeout`. `None` (no deadline) stays `None`.
                let new_deadline_at = execution.deadline_at.map(|d| d + pause_span);
                // Also push the soft SLA deadline forward (issue #487): a workflow
                // paused mid-flight should not breach its SLA while paused — BUT
                // only suspend a deadline that was still ahead when the pause
                // began. A deadline already passed before the pause stays in the
                // past so the breach (which occurred while RUNNING) is still
                // observed by the scanner on the next tick after resume, rather
                // than being silently pushed into the future.
                let new_sla_deadline_at =
                    execution
                        .sla_deadline_at
                        .map(|d| match execution.paused_at {
                            Some(p) if d > p => d + pause_span,
                            _ => d,
                        });

                let history = store::load_history(conn, exec_id).await?;
                store::append_events(
                    conn,
                    exec_id,
                    &[WorkflowEvent::WorkflowExecutionResumed {
                        resumed_at,
                        actor: actor.clone(),
                    }],
                    history.next_event_id,
                )
                .await?;

                let updated =
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .filter(harvest_workflow_executions::state.eq("PAUSED"))
                        .set((
                            harvest_workflow_executions::state.eq("RUNNING"),
                            harvest_workflow_executions::paused_at
                                .eq(None::<chrono::DateTime<Utc>>),
                            harvest_workflow_executions::pause_reason.eq(None::<String>),
                            harvest_workflow_executions::pause_actor.eq(None::<String>),
                            harvest_workflow_executions::deadline_at.eq(new_deadline_at),
                            harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;

                if updated == 0 {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is no longer paused"
                    )));
                }

                // Re-arm the executor: wake the parked workflow task so the
                // workflow advances on its next decision attempt. Any timer that
                // fired while paused, or signal queued during the pause, is
                // processed when the woken task is claimed.
                queue::wake_workflow_task(conn, exec_id).await?;

                Ok(ResumedWorkflowExecution {
                    exec_id,
                    state: "RUNNING".to_string(),
                    actor,
                    pause_duration_secs,
                    workflow_name: execution.workflow_name,
                    queue_name: execution.queue_name,
                })
            }
            .scope_boxed()
        })
        .await?;

    metrics.record_workflow_pause_duration(
        &result.workflow_name,
        &result.queue_name,
        result.pause_duration_secs,
    );

    Ok(result)
}

/// Reactivate a `FAILED` workflow execution so a redriven dead-letter task can
/// resume from existing history (issue #510).
///
/// This is the load-bearing differentiator of redrive over replay: every DLQ
/// write path seals the owning execution `FAILED` (and appends a terminal
/// `WorkflowFailed` event) at quarantine time, so a redrive must reopen the run
/// before re-enqueuing. It:
///
/// 1. appends a [`WorkflowEvent::WorkflowRedriven`] event **after** the
///    superseded terminal `WorkflowFailed` (append-only — no existing event is
///    rewritten, removed, or reordered), and
/// 2. transitions the execution `FAILED → RUNNING`, clearing the recorded
///    failure (`error`/`output`/`completed_at`).
///
/// The matcher marks the `WorkflowRedriven` event and the `WorkflowFailed` it
/// supersedes as transparent, so the re-enqueued task replays existing history
/// and re-issues the failed step live (see [`crate::replay::HistoryMatcher`]).
///
/// Runs on the caller's connection so it **joins the caller's transaction**;
/// the caller is responsible for re-enqueuing the workflow task. The execution
/// row should already be locked `FOR UPDATE` by the caller and confirmed
/// `FAILED`; the guards here are defensive and roll the transaction back if the
/// state has changed.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] if the execution does not exist,
/// [`HarvestError::Config`] if it is not `FAILED`, or [`HarvestError::Database`]
/// on persistence failure.
pub async fn reactivate_failed_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    dead_letter_id: Uuid,
    reason: Option<&str>,
) -> HarvestResult<()> {
    let execution = harvest_workflow_executions::table
        .find(exec_id.as_uuid())
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

    if execution.state != "FAILED" {
        return Err(HarvestError::Config(format!(
            "cannot redrive: workflow execution {exec_id} is {} (only FAILED is reactivatable)",
            execution.state
        )));
    }

    // Re-anchor the hard deadline and soft SLA deadline from now so the timeout
    // and SLA scanners see a fresh window rather than the stale past deadlines
    // that were set when the execution first started. Without this, a FAILED
    // execution with a non-NULL `deadline_at` in the past would be immediately
    // re-killed by `enforce_workflow_execution_timeouts` on the next scan tick.
    let now = Utc::now();
    let new_deadline_at = execution.execution_timeout.map(|d| now + d);
    let new_sla_deadline_at = execution.sla.map(|d| now + d);

    let history = store::load_history(conn, exec_id).await?;
    store::append_events(
        conn,
        exec_id,
        &[WorkflowEvent::WorkflowRedriven {
            redriven_at: now,
            dead_letter_id,
            reason: reason.map(str::to_string),
        }],
        history.next_event_id,
    )
    .await?;

    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq("FAILED"))
        .set((
            harvest_workflow_executions::state.eq("RUNNING"),
            harvest_workflow_executions::error.eq(None::<String>),
            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
            harvest_workflow_executions::completed_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
            harvest_workflow_executions::deadline_at.eq(new_deadline_at),
            harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
            harvest_workflow_executions::sla_breached.eq(false),
            harvest_workflow_executions::sla_breached_at.eq(None::<chrono::DateTime<Utc>>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

    if updated == 0 {
        // The FOR UPDATE-locked row changed state out from under us — roll the
        // transaction back so the appended WorkflowRedriven event is discarded.
        return Err(HarvestError::Config(format!(
            "workflow execution {exec_id} is no longer FAILED"
        )));
    }

    Ok(())
}

/// Auto-resume executions that have been paused longer than `max_pause_duration`
/// (issue #383, bounded pause).
///
/// Scans `PAUSED` executions whose `paused_at` exceeds the ceiling and resumes
/// each with `actor = "auto-resume(timeout)"`. This prevents orphaned-pause
/// backlogs when an operator pauses during an incident and forgets to resume.
///
/// Returns the number of executions auto-resumed.
///
/// # Errors
///
/// Returns the first database or persistence error encountered. Per-execution
/// races (an execution resumed or cancelled concurrently) are skipped, not
/// treated as fatal.
pub async fn auto_resume_expired_pauses(
    conn: &mut AsyncPgConnection,
    max_pause_duration: std::time::Duration,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    let now = Utc::now();
    // Ceiling too large to represent: nothing can exceed it.
    let Ok(max) = chrono::Duration::from_std(max_pause_duration) else {
        return Ok(0);
    };
    let cutoff = now - max;

    let expired: Vec<ExecutionId> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::state.eq("PAUSED"))
        .filter(harvest_workflow_executions::paused_at.is_not_null())
        .filter(harvest_workflow_executions::paused_at.le(Some(cutoff)))
        .select(harvest_workflow_executions::id)
        .load::<Uuid>(conn)
        .await
        .map_err(database_error)?
        .into_iter()
        .map(ExecutionId::from_uuid)
        .collect();

    let mut resumed = 0;
    for exec_id in expired {
        match resume_workflow_execution(conn, exec_id, "auto-resume(timeout)", metrics).await {
            Ok(_) => {
                resumed += 1;
                tracing::warn!(
                    exec_id = %exec_id,
                    "auto-resumed workflow execution after exceeding max pause duration"
                );
            }
            // The execution was resumed or cancelled between the scan and the
            // claim; not a fatal condition for the sweep.
            Err(HarvestError::Config(_) | HarvestError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
    }

    Ok(resumed)
}

/// Count running detached children that would append a parent cascade event.
pub(crate) async fn parent_close_cascade_event_count(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<u64> {
    let policies: Vec<Option<String>> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        // Must mirror apply_parent_close_cascade's RUNNING|PAUSED selection so the
        // history-cap preflight count matches the events actually appended (#383).
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .select(harvest_workflow_executions::parent_close_policy)
        .load::<Option<String>>(conn)
        .await
        .map_err(database_error)?;

    policies.into_iter().try_fold(0_u64, |count, policy_opt| {
        let policy = policy_opt
            .expect("filtered by is_not_null")
            .parse::<ParentClosePolicy>()
            .map_err(HarvestError::Config)?;
        Ok(count + u64::from(policy != ParentClosePolicy::Abandon))
    })
}

/// Apply parent-close cascade to all active detached children of `parent_exec_id`.
///
/// Queries children with `parent_close_policy IS NOT NULL AND state IN
/// ('RUNNING','PAUSED')` — a paused child is still active (issue #383).
/// - Abandon: no-op
/// - `RequestCancel`: appends `WorkflowCancelled`, transitions to CANCELLED, fails tasks
/// - `Terminate`: appends `WorkflowFailed`, transitions to FAILED, fails tasks
///
/// Appends a `ChildWorkflowCascadeApplied` event to the parent history for each
/// non-Abandon action. Idempotent: acts only on RUNNING/PAUSED children.
pub(crate) async fn apply_parent_close_cascade(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<Vec<DeferredTriggerStart>> {
    use crate::store;

    // PAUSED is a non-terminal active state (issue #383): a paused child is
    // still an active child, so the parent-close cascade must reach it too —
    // otherwise it could be resumed after the parent closed despite a
    // RequestCancel/Terminate policy.
    let running_children: Vec<(Uuid, Option<String>)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<(Uuid, Option<String>)>(conn)
        .await
        .map_err(database_error)?;

    let mut deferred = Vec::new();

    for (child_uuid, policy_opt) in running_children {
        let child_exec_id = ExecutionId::from_uuid(child_uuid);
        let policy_str = policy_opt.expect("filtered by is_not_null");
        let policy = policy_str
            .parse::<ParentClosePolicy>()
            .map_err(HarvestError::Config)?;

        let (action, mut child_deferred) = match policy {
            ParentClosePolicy::Abandon => (None, Vec::new()),
            ParentClosePolicy::RequestCancel => {
                let (success, d) =
                    cascade_cancel_detached_child(conn, child_exec_id, "parent closed").await?;
                (success.then_some("request_cancel"), d)
            }
            ParentClosePolicy::Terminate => {
                let (success, d) =
                    cascade_terminate_detached_child(conn, child_exec_id, "ParentClosed").await?;
                (success.then_some("terminate"), d)
            }
        };

        let Some(action_str) = action else {
            continue;
        };

        deferred.append(&mut child_deferred);

        store::append_single_event(
            conn,
            parent_exec_id,
            crate::event::WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id: child_exec_id,
                policy,
                action: action_str.to_string(),
            },
        )
        .await?;
    }

    Ok(deferred)
}

async fn cascade_cancel_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<(bool, Vec<DeferredTriggerStart>)> {
    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused child is made terminal so
            // it doesn't appear "terminal and still paused" in APIs/UI (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok((false, Vec::new()));
    }
    store::append_single_event(
        conn,
        exec_id,
        WorkflowEvent::WorkflowCancelled {
            reason: reason.to_string(),
        },
    )
    .await?;
    queue::fail_open_tasks_for_execution(
        conn,
        exec_id,
        &format!("workflow cancelled by parent close: {reason}"),
    )
    .await?;
    let mut deferred = Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Cancelled,
        None,
    )
    .await?;
    deferred.extend(triggers);
    Ok((true, deferred))
}

async fn cascade_terminate_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<(bool, Vec<DeferredTriggerStart>)> {
    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
            // Clear active-pause metadata when a paused child is made terminal so
            // it doesn't appear "terminal and still paused" in APIs/UI (#383).
            harvest_workflow_executions::paused_at.eq(None::<chrono::DateTime<Utc>>),
            harvest_workflow_executions::pause_reason.eq(None::<String>),
            harvest_workflow_executions::pause_actor.eq(None::<String>),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok((false, Vec::new()));
    }
    store::append_single_event(
        conn,
        exec_id,
        WorkflowEvent::WorkflowFailed {
            error: reason.to_string(),
        },
    )
    .await?;
    queue::fail_open_tasks_for_execution(
        conn,
        exec_id,
        &format!("workflow terminated by parent close: {reason}"),
    )
    .await?;
    let mut deferred = Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
        conn,
        exec_id,
        crate::completion_trigger::TerminalState::Failed,
        None,
    )
    .await?;
    deferred.extend(triggers);
    Ok((true, deferred))
}

/// Hard-finalize a workflow execution to `TERMINATED` regardless of its
/// current live state.
///
/// `cancel_workflow_execution` is the graceful path: it requires the
/// execution to be `RUNNING`/`PAUSED` and the workflow body must observe
/// the cancellation cooperatively (`is_cancelled`/`check_cancellation`).
/// `terminate_workflow_execution` is the forceful operator escape hatch —
/// it seals a live run (`RUNNING`/`SUSPENDED`/`PAUSED`) in the
/// `TERMINATED` state unilaterally, surfacing to result-awaiting callers as
/// [`HarvestError::Terminated`] (distinct from a cooperative `CANCELLED`
/// and from a `FAILED`). Open task rows are still failed so workers don't
/// keep chewing on a torn-down execution.
///
/// This emits a [`WorkflowEvent::WorkflowCancelled`] (no new event variant —
/// the append-only contract is intact) and records the supplied reason on
/// the row. It is **idempotent against any already-terminal state**
/// (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/
/// `TERMINATED`): the call is a non-mutating no-op that appends no second
/// terminal transition.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist
/// and [`HarvestError::Database`] for persistence failures.
#[allow(clippy::too_many_lines)]
pub async fn terminate_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<CancelledWorkflowExecution> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow termination requested".to_string()
    } else {
        reason.to_string()
    };

    let (cancel_result, deferred_starts) = conn
        .transaction::<(CancelledWorkflowExecution, Vec<DeferredTriggerStart>), HarvestError, _>(
            |conn| {
                async move {
                    let execution = harvest_workflow_executions::table
                        .find(exec_id.as_uuid())
                        .select(WorkflowExecution::as_select())
                        .for_update()
                        .first(conn)
                        .await
                        .optional()
                        .map_err(database_error)?
                        .ok_or_else(|| {
                            HarvestError::NotFound(format!("workflow execution {exec_id}"))
                        })?;

                    // Idempotent no-op against any already-terminal state
                    // (issue #504, AC #7): never append a duplicate terminal
                    // transition. `idempotent` returns the existing state with
                    // `newly_cancelled = false`.
                    if crate::erase::is_terminal_state(&execution.state) {
                        return Ok((
                            CancelledWorkflowExecution::idempotent(exec_id, execution),
                            Vec::new(),
                        ));
                    }

                    let history = store::load_history(conn, exec_id).await?;
                    store::append_events(
                        conn,
                        exec_id,
                        &[WorkflowEvent::WorkflowCancelled {
                            reason: reason.clone(),
                        }],
                        history.next_event_id,
                    )
                    .await?;

                    let completed_at = Utc::now();
                    // Mirror cancel/resume: if this execution was PAUSED, push
                    // sla_deadline_at forward by the pause span so the SLA scanner
                    // does not record a false breach for time spent paused before
                    // terminate (issue #383 × #487). The scanner judges terminal rows
                    // by `sla_deadline_at < COALESCE(completed_at, NOW())`, so leaving
                    // a stale deadline that elapsed during the pause would count a
                    // suspended-clock run as breached. Only extend a deadline that was
                    // still ahead when the pause began — a deadline already elapsed
                    // while RUNNING stays in the past so its breach is still observed.
                    let new_sla_deadline_at = if execution.state == "PAUSED" {
                        execution
                            .sla_deadline_at
                            .map(|d| match execution.paused_at {
                                Some(p) if d > p => {
                                    d + (completed_at - p).max(chrono::Duration::zero())
                                }
                                _ => d,
                            })
                    } else {
                        execution.sla_deadline_at
                    };

                    // No state-precondition filter: operator override force-writes
                    // the live run to the sealed TERMINATED state.
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .set((
                            harvest_workflow_executions::state.eq("TERMINATED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(reason.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(completed_at)),
                            harvest_workflow_executions::sla_deadline_at.eq(new_sla_deadline_at),
                            // Clear active-pause metadata when terminating a paused
                            // run so it doesn't appear terminal-and-paused (#383).
                            harvest_workflow_executions::paused_at
                                .eq(None::<chrono::DateTime<Utc>>),
                            harvest_workflow_executions::pause_reason.eq(None::<String>),
                            harvest_workflow_executions::pause_actor.eq(None::<String>),
                        ))
                        .execute(conn)
                        .await
                        .map_err(database_error)?;

                    let failed_task_count = queue::fail_open_tasks_for_execution(
                        conn,
                        exec_id,
                        &format!("workflow terminated: {reason}"),
                    )
                    .await?;
                    // Wake a parent blocked on this child's await (#787):
                    // force-terminating an awaited child out-of-band must surface
                    // to the parent so it does not park forever.
                    notify_awaited_parent_of_child_terminal(
                        conn,
                        exec_id,
                        &execution,
                        format!("child workflow terminated: {reason}"),
                    )
                    .await?;
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    // Force-terminate fires `Terminated` completion triggers, NOT
                    // `Cancelled` — a force-kill is distinct from a cooperative
                    // cancellation downstream (issue #504). Operators opt into
                    // terminate cascades by registering `terminal_states:
                    // ["Terminated"]`.
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Terminated,
                        None,
                    )
                    .await?;
                    deferred.extend(triggers);

                    let prior_state = execution.state.clone();
                    Ok((
                        CancelledWorkflowExecution::newly_cancelled(
                            exec_id,
                            "TERMINATED",
                            reason,
                            failed_task_count,
                            execution.workflow_name.clone(),
                            execution.queue_name.clone(),
                            prior_state,
                        ),
                        deferred,
                    ))
                }
                .scope_boxed()
            },
        )
        .await?;

    for start in deferred_starts {
        start.spawn();
    }

    // Only emit the Terminated metric when the execution was live (RUNNING,
    // SUSPENDED, or PAUSED — all non-terminal active states; issue #383 routes
    // paused scheduled runs here via TerminateOther). If the prior state was
    // already terminal (FAILED, TIMED_OUT, COMPLETED), that outcome was already
    // counted — emitting Terminated again would inflate the SLO denominator for
    // operator cleanup actions.
    if cancel_result.newly_cancelled
        && matches!(
            cancel_result.prior_state.as_str(),
            "RUNNING" | "SUSPENDED" | "PAUSED"
        )
    {
        metrics.record_workflow_terminal(
            &cancel_result.workflow_name,
            &cancel_result.queue_name,
            crate::telemetry::WorkflowStatus::Terminated,
        );
    }

    Ok(cancel_result)
}

/// Non-locking lookup used for the `TerminateIfRunning` pre-check outside any
/// transaction. Returns `None` if no active execution exists.
pub async fn try_load_by_key(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

// ─────────────────────────────────────────────────────────────────────────────
// SignalWithStart (issue #244)
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters for the atomic `signal_with_start` primitive.
///
/// Combines the inputs of [`StartWorkflowParams`] with the signal name,
/// payload, and optional idempotency key, so a single shard-local transaction
/// can either start a fresh execution and stage the signal for its first
/// dispatch, or attach the signal to an existing live execution.
#[derive(Debug, Clone)]
pub struct SignalWithStartParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout`. Forwarded to
    /// [`StartWorkflowParams::max_execution_timeout_ceiling`].
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Pre-resolved concurrency group key. Forwarded to
    /// [`StartWorkflowParams::concurrency_key`].
    pub concurrency_key: Option<String>,
    /// Per-key concurrency cap. Forwarded to
    /// [`StartWorkflowParams::concurrency_limit`].
    pub concurrency_limit: Option<u32>,
    pub signal_name: &'a str,
    pub signal_payload: serde_json::Value,
    /// Optional dedup key. When present, repeated calls with the same
    /// `(workflow_exec_id, idempotency_key)` deliver the signal exactly once.
    /// Backed by a partial unique index on `harvest_signals`; the `NULL` case
    /// preserves the pre-existing `send_signal` behaviour.
    pub idempotency_key: Option<String>,
    /// Payload cap for `start_input` (bytes). Enforced only on the fresh-start
    /// path — attach paths ignore this field. Zero means no cap.
    pub max_workflow_input_bytes: u64,
    /// Payload cap for `signal_payload` (bytes). Zero means no cap.
    pub max_signal_payload_bytes: u64,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget forwarded to [`StartWorkflowParams::sla`] (issue #487).
    pub sla: Option<chrono::Duration>,
    /// When `true`, reject (with [`HarvestError::DebounceFreshStart`]) any call
    /// that would create a **fresh** execution rather than attach to a live
    /// (RUNNING/PAUSED) prior. Set by the HTTP handler for a debounced workflow
    /// so an attach/idempotent call is preserved while a fresh start is rejected
    /// — decided atomically under this call's lock (issue #499).
    pub reject_fresh_if_debounced: bool,
    /// Effective workflow-level retry policy (issue #523). Forwarded to
    /// [`StartWorkflowParams::workflow_retry_policy`] on fresh starts.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// Server-side ceiling on retry attempts. Forwarded to
    /// [`StartWorkflowParams::max_workflow_attempts_ceiling`].
    pub max_workflow_attempts_ceiling: Option<u32>,
}

/// Result of a [`signal_with_start_workflow_execution`] call.
///
/// `started_fresh` distinguishes a freshly inserted run from one attached to
/// an existing live execution. `signal_delivered` reports whether the signal
/// row was actually queued: it is `false` when the prior execution is in a
/// terminal state (no signal can land) or when the idempotency key matched a
/// row that was already enqueued.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignalWithStartOutcome {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_fresh: bool,
    pub signal_delivered: bool,
}

/// Atomically start a workflow if no live run for `(workflow_name, workflow_id)`
/// exists (subject to `reuse_policy`), or signal the existing run otherwise.
///
/// ## Outcome matrix (4 reuse policies × prior execution state)
///
/// | Prior state          | `AllowDuplicate`            | `RejectDuplicate`     | `AllowDuplicateFailedOnly`   | `TerminateIfRunning`         |
/// |----------------------|-----------------------------|-----------------------|------------------------------|------------------------------|
/// | none                 | start + signal              | start + signal        | start + signal               | start + signal               |
/// | RUNNING / SUSPENDED  | signal existing             | `Err(AlreadyExists)`  | signal existing              | cancel + start + signal      |
/// | COMPLETED            | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | FAILED               | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | CANCELLED            | start fresh + signal        | `Err(AlreadyExists)`  | start fresh + signal         | start fresh + signal         |
/// | TERMINATED           | start fresh + signal        | start fresh + signal  | start fresh + signal         | start fresh + signal         |
///
/// "Suspended" workflows are observable to the engine as `RUNNING` — they are
/// running executions whose handler is awaiting external input — so they
/// behave identically to `RUNNING` in this matrix.
///
/// `TERMINATED` is the *sealed* state set by the reset path (`reset.rs`): the
/// row is released from the partial unique index over
/// `(workflow_name, workflow_id) WHERE state NOT IN ('CONTINUED_AS_NEW',
/// 'TERMINATED')`. A `TERMINATED` row is treated as if the `workflow_id` were
/// free, including under `RejectDuplicate`. This matches the broader
/// [`start_or_load_workflow_execution`] semantics; the reset operator
/// explicitly opted the prior row out of the uniqueness scope.
///
/// Note: `AllowDuplicate` and `AllowDuplicateFailedOnly` diverge from the
/// standalone [`start_or_load_workflow_execution`] behaviour for terminal
/// priors: the standalone start returns the existing terminal row, while
/// signal-with-start escalates to a fresh start so the signal can land. This
/// keeps the spec's "no signal silently dropped" invariant intact.
/// `RejectDuplicate` and `TerminateIfRunning` keep their original semantics.
///
/// ## Event ordering
///
/// On a **fresh start**, only `WorkflowStarted` is appended in this call. The
/// signal is staged as a pending `harvest_signals` row (with the supplied
/// idempotency key) and the worker's existing `ingest_pending_signals` path
/// promotes it to a `SignalReceived` event *before* the workflow function is
/// dispatched on its first tick. No new `WorkflowEvent` variant is needed.
///
/// On an **attach**, the signal row is queued in the same transaction and the
/// running workflow's task is woken; the existing signal-delivery path picks
/// it up at the next dispatch boundary.
///
/// On a **cancel + start** (`TerminateIfRunning` + RUNNING prior), the prior
/// execution receives a `WorkflowCancelled` event and is moved to `CANCELLED`,
/// then the fresh start + signal lands — all inside this function's outer
/// transaction. Diesel-async demotes the inner `conn.transaction(..)` blocks
/// in `cancel_workflow_execution` and `start_or_load_workflow_execution` to
/// savepoints under the outer one, so a crash mid-flight rolls back the
/// cancellation as well: the prior workflow stays RUNNING and the caller can
/// retry from a clean state. (This is a strictly safer guarantee than the
/// standalone `start_or_load_workflow_execution` two-transaction shape, which
/// can leave a CANCELLED orphan on a crash; the wrapping transaction here
/// turns that into an all-or-nothing operation.)
///
/// Check a payload value against a byte cap, returning `PayloadTooLarge` when exceeded.
/// Zero cap means uncapped (no check performed).
fn check_sws_payload_cap(
    value: &serde_json::Value,
    kind: crate::error::PayloadKind,
    cap: u64,
    workflow_type: &str,
) -> HarvestResult<()> {
    if cap == 0 {
        return Ok(());
    }
    let observed = serde_json::to_string(value).map_or(0, |s| s.len() as u64);
    if observed > cap {
        return Err(crate::error::HarvestError::PayloadTooLarge {
            kind,
            observed_bytes: observed,
            cap_bytes: cap,
            workflow_type: workflow_type.to_string(),
            activity_name: None,
        });
    }
    Ok(())
}

/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - Propagates queue/event-store failures from the start transaction.
#[allow(clippy::too_many_lines)] // orchestrates idempotency, cap checks, start, TOCTOU retry, and signal atomically
pub async fn signal_with_start_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: SignalWithStartParams<'_>,
) -> HarvestResult<SignalWithStartOutcome> {
    // Single outer transaction: pre-cancel + start (or attach) + signal insert commit
    // atomically. Inner conn.transaction calls become savepoints under this wrapper.
    conn.transaction::<SignalWithStartOutcome, HarvestError, _>(|conn| {
        let request = request;
        async move {
            // Cross-execution dedupe: scope by (workflow_name, workflow_id, key)
            // so escalation/reset paths on a new exec_id don't re-queue the signal.
            if let Some(key) = request.idempotency_key.as_deref()
                && let Some(prior) = lookup_idempotent_signal_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    key,
                )
                .await?
            {
                return Ok(SignalWithStartOutcome {
                    exec_id: ExecutionId::from_uuid(prior.id),
                    workflow_name: prior.workflow_name,
                    workflow_id: prior.workflow_id,
                    state: prior.state,
                    started_fresh: false,
                    signal_delivered: false,
                });
            }

            // Upgrade AllowDuplicate / AllowDuplicateFailedOnly to TerminateIfRunning
            // when the prior run is terminal so the signal always lands on a live
            // execution ("no signal silently dropped" invariant from issue #244).
            // For a debounced workflow, skip the upgrade: a terminal prior must not
            // be escalated to a fresh start here — the reject check below routes it
            // to debounce admission instead.
            let effective_policy = if request.reject_fresh_if_debounced {
                request.reuse_policy
            } else {
                resolve_effective_signal_with_start_policy(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    request.reuse_policy,
                )
                .await?
            };

            let build_start_request =
                |exec_id: ExecutionId, policy: WorkflowIdReusePolicy| StartWorkflowParams {
                    workflow_name: request.workflow_name,
                    workflow_id: request.workflow_id,
                    exec_id,
                    input: request.input.clone(),
                    parent_id: request.parent_id,
                    queue_name: request.queue_name,
                    execution_timeout: request.execution_timeout,
                    memo: request.memo.clone(),
                    search_attrs: request.search_attrs.clone(),
                    reuse_policy: policy,
                    trace_context: request.trace_context.clone(),
                    max_execution_timeout_ceiling: request.max_execution_timeout_ceiling,
                    concurrency_key: request.concurrency_key.clone(),
                    concurrency_limit: request.concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: 0,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner: request.owner,
                    runbook_url: request.runbook_url,
                    severity: request.severity,
                    context_headers: request.context_headers.clone(),
                    sla: request.sla,
                    schedule_id: None,
                    scheduled_for: None,
                    workflow_attempt: 1,
                    workflow_retry_policy: request
                        .workflow_retry_policy
                        .clone()
                        .and_then(|v| serde_json::from_value(v).ok()),
                    retry_of_exec_id: None,
                    max_workflow_attempts_ceiling: request.max_workflow_attempts_ceiling,
                    origin: None,
                };

            // For a debounced workflow, route the start through the no-spawn collect
            // path with reject_fresh: a fresh start (including a TerminateIfRunning
            // cancel+replace) returns DebounceFreshStart and rolls back WITHOUT
            // cancelling a prior or spawning completion-trigger/parent-close
            // follow-ups (issue #499). An attach returns the existing live run;
            // its deferred list is empty and spawned defensively.
            let started = if request.reject_fresh_if_debounced {
                let (s, deferred) = start_or_load_workflow_execution_collect(
                    conn,
                    build_start_request(request.exec_id, effective_policy),
                    true,
                    true,
                )
                .await?;
                for d in deferred {
                    d.spawn();
                }
                s
            } else {
                start_or_load_workflow_execution(
                    conn,
                    build_start_request(request.exec_id, effective_policy),
                )
                .await?
            };

            // On fresh start only: enforce workflow input cap (tx rollback on error).
            if started.created {
                check_sws_payload_cap(
                    &request.input,
                    crate::error::PayloadKind::WorkflowInput,
                    request.max_workflow_input_bytes,
                    request.workflow_name,
                )?;
            }

            // TOCTOU guard: if a concurrent transaction completed the run between
            // the policy resolver's lock and our start, the start helper returns
            // a terminal row. Escalate to TerminateIfRunning so the signal always
            // lands on a live execution rather than being silently dropped.
            // PAUSED is a non-terminal active state (issue #383): treat it like
            // RUNNING here so a signal-with-start attaches to (and buffers the
            // signal for) the paused run instead of cancelling and replacing it.
            let started = if !matches!(started.state.as_str(), "RUNNING" | "PAUSED")
                // For a debounced workflow, never escalate a terminal prior to a
                // fresh start here — that fresh start must go through debounce
                // admission. The reject check below catches the non-live outcome.
                && !request.reject_fresh_if_debounced
                && matches!(
                    request.reuse_policy,
                    WorkflowIdReusePolicy::AllowDuplicate
                        | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
                ) {
                let fresh_exec_id = ExecutionId::new_for_shard(started.exec_id.shard());
                let fresh = start_or_load_workflow_execution(
                    conn,
                    build_start_request(fresh_exec_id, WorkflowIdReusePolicy::TerminateIfRunning),
                )
                .await?;
                if fresh.created {
                    check_sws_payload_cap(
                        &request.input,
                        crate::error::PayloadKind::WorkflowInput,
                        request.max_workflow_input_bytes,
                        request.workflow_name,
                    )?;
                }
                fresh
            } else {
                started
            };

            // Atomic debounce gate (issue #499): under this transaction's lock, a
            // debounced workflow may only *attach* to a live (RUNNING/PAUSED) prior.
            // Any other outcome — a fresh insert (`created`) or a non-live prior the
            // signal can't land on — would be a fresh start, so reject it and let the
            // caller route to debounce admission. Rolls back any fresh insert above.
            if request.reject_fresh_if_debounced
                && (started.created || !matches!(started.state.as_str(), "RUNNING" | "PAUSED"))
            {
                return Err(HarvestError::DebounceFreshStart {
                    workflow_name: request.workflow_name.to_string(),
                    workflow_id: request.workflow_id.to_string(),
                });
            }

            // Check signal payload cap here — after start/attach/AlreadyExists
            // resolution — so RejectDuplicate conflicts surface as 409 AlreadyExists
            // rather than 413 PayloadTooLarge when the payload happens to be oversized.
            // PAUSED counts as live: the signal will be staged and delivered on resume.
            if matches!(started.state.as_str(), "RUNNING" | "PAUSED") {
                check_sws_payload_cap(
                    &request.signal_payload,
                    crate::error::PayloadKind::SignalPayload,
                    request.max_signal_payload_bytes,
                    request.workflow_name,
                )?;
            }

            let signal_delivered = if matches!(started.state.as_str(), "RUNNING" | "PAUSED") {
                stage_signal_with_idempotency(
                    conn,
                    started.exec_id,
                    request.signal_name,
                    request.signal_payload,
                    request.idempotency_key.as_deref(),
                )
                .await?
            } else {
                false
            };

            Ok(SignalWithStartOutcome {
                exec_id: started.exec_id,
                workflow_name: started.workflow_name,
                workflow_id: started.workflow_id,
                state: started.state,
                started_fresh: started.created,
                signal_delivered,
            })
        }
        .scope_boxed()
    })
    .await
}

/// Pick the policy `start_or_load_workflow_execution` is invoked with, given
/// the caller's requested policy and the current prior-run state. For
/// signal-with-start, `AllowDuplicate` and `AllowDuplicateFailedOnly` are
/// upgraded to `TerminateIfRunning` whenever the prior run is non-RUNNING so
/// that the spec's "no signal silently dropped" invariant holds on terminal
/// priors. `RejectDuplicate` and `TerminateIfRunning` are returned unchanged.
async fn resolve_effective_signal_with_start_policy(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    requested: WorkflowIdReusePolicy,
) -> HarvestResult<WorkflowIdReusePolicy> {
    if !matches!(
        requested,
        WorkflowIdReusePolicy::AllowDuplicate | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
    ) {
        return Ok(requested);
    }
    // Take the row lock here so the observed state persists through
    // `start_or_load_workflow_execution`'s own `FOR UPDATE` lookup below.
    // Without this, a workflow that transitions RUNNING -> terminal between
    // the resolver's read and the start path's lock could let the
    // spec-prohibited "attach to terminal, drop signal" outcome re-emerge.
    // Both calls share the same connection / outer transaction, so the lock
    // taken here is held through the start path and released only on outer
    // commit or rollback.
    let Some(existing) =
        try_load_active_execution_for_update(conn, workflow_name, workflow_id).await?
    else {
        return Ok(requested);
    };
    if matches!(existing.state.as_str(), "RUNNING" | "PAUSED") {
        // PAUSED is a non-terminal active state (issue #383): keep the requested
        // policy so the start path attaches to the existing run and the signal is
        // queued (buffered for delivery on resume), matching direct send_signal.
        // Only a truly terminal prior is upgraded below.
        Ok(requested)
    } else {
        // Non-RUNNING prior under a non-rejecting policy: upgrade so the
        // start transaction takes the `replace_execution` path (seal prior,
        // insert fresh, append WorkflowStarted) and the signal can land.
        Ok(WorkflowIdReusePolicy::TerminateIfRunning)
    }
}

/// Locking variant of [`try_load_by_key`] used by
/// [`signal_with_start_workflow_execution`]'s resolver. Returns `None` when
/// no active execution exists. Acquires `FOR UPDATE` so the caller's outer
/// transaction holds the row lock until commit, preventing a RUNNING ->
/// terminal race between the resolver decision and the start path's own
/// `FOR UPDATE` lookup.
async fn try_load_active_execution_for_update(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Cross-execution idempotency dedupe for `signal_with_start`.
///
/// Returns the most recent workflow execution of `(workflow_name, workflow_id)`
/// that has a `harvest_signals` row with this `idempotency_key`. The per-shard
/// partial unique index on `(workflow_exec_id, idempotency_key)` only enforces
/// uniqueness within one execution; this query scopes the dedupe to the
/// logical workflow so a webhook retry that arrives after the prior signal
/// drove its execution to a terminal state is recognised as a duplicate and
/// short-circuited before any fresh start / replacement happens.
async fn lookup_idempotent_signal_dedupe(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    idempotency_key: &str,
) -> HarvestResult<Option<WorkflowExecution>> {
    use diesel::JoinOnDsl;

    harvest_signals::table
        .inner_join(
            harvest_workflow_executions::table
                .on(harvest_signals::workflow_exec_id.eq(harvest_workflow_executions::id)),
        )
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_signals::idempotency_key.eq(idempotency_key))
        .order_by(harvest_signals::received_at.desc())
        .select(WorkflowExecution::as_select())
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

/// Insert a signal row, returning `false` when the idempotency key collides
/// with an already-staged signal for the same execution.
async fn stage_signal_with_idempotency(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<bool> {
    let row = NewHarvestSignal {
        workflow_exec_id: exec_id.as_uuid(),
        signal_name,
        payload,
        idempotency_key,
    };

    let inserted = diesel::insert_into(harvest_signals::table)
        .values(&row)
        .on_conflict_do_nothing()
        .execute(conn)
        .await
        .map_err(database_error)?;

    if inserted == 0 {
        // Idempotency-key collision — the prior insert already queued an
        // equivalent signal. This is the dedup happy path.
        return Ok(false);
    }

    queue::wake_workflow_task(conn, exec_id).await?;
    Ok(true)
}

/// Locking lookup used inside the start transaction when a policy decision may
/// modify or replace the existing row.
async fn load_workflow_execution_by_key_for_update(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> HarvestResult<WorkflowExecution> {
    harvest_workflow_executions::table
        .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
        .filter(harvest_workflow_executions::workflow_id.eq(workflow_id))
        .filter(harvest_workflow_executions::state.ne_all(["CONTINUED_AS_NEW", "TERMINATED"]))
        .select(WorkflowExecution::as_select())
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(database_error)?
        .ok_or_else(|| {
            HarvestError::NotFound(format!("workflow execution {workflow_name}/{workflow_id}"))
        })
}

// ─────────────────────────────────────────────────────────────────────────────
// UpdateWithStart (issue #479)
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters for the atomic `update_with_start` primitive.
///
/// Combines the inputs of [`StartWorkflowParams`] with the update name,
/// arguments, and optional idempotency key so a single shard-local transaction
/// can either start a fresh execution and admit the update for its first
/// dispatch, or attach the update to an existing live execution.
#[derive(Debug, Clone)]
pub struct UpdateWithStartParams<'a> {
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub exec_id: ExecutionId,
    pub input: serde_json::Value,
    pub parent_id: Option<Uuid>,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub reuse_policy: WorkflowIdReusePolicy,
    pub trace_context: Option<TraceContextCarrier>,
    /// Server-side ceiling applied to `execution_timeout`.
    pub max_execution_timeout_ceiling: Option<chrono::Duration>,
    /// Pre-resolved concurrency group key.
    pub concurrency_key: Option<String>,
    /// Per-key concurrency cap.
    pub concurrency_limit: Option<u32>,
    /// Pre-generated update ID. When `idempotency_key` is `Some`, callers
    /// should derive this deterministically (e.g. `UUIDv5`) so the dedup lookup
    /// matches prior admitted updates.
    pub update_id: crate::types::UpdateId,
    /// The name of the update handler to invoke.
    pub update_name: String,
    /// JSON-serialised update arguments.
    pub update_args: serde_json::Value,
    /// Optional dedup key, scoped to `(workflow_name, workflow_id)`. A retry
    /// with the same key returns the previous outcome without re-admitting.
    pub idempotency_key: Option<String>,
    /// Payload cap for `input` (bytes). Enforced only on the fresh-start path.
    pub max_workflow_input_bytes: u64,
    pub owner: Option<&'a str>,
    pub runbook_url: Option<&'a str>,
    pub severity: Option<&'a str>,
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    /// Soft SLA budget forwarded to [`StartWorkflowParams::sla`] (issue #487).
    pub sla: Option<chrono::Duration>,
    /// Effective workflow-level retry policy (issue #523). Forwarded to
    /// [`StartWorkflowParams::workflow_retry_policy`] on fresh starts.
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// Server-side ceiling on retry attempts. Forwarded to
    /// [`StartWorkflowParams::max_workflow_attempts_ceiling`].
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// When `true`, reject (with [`HarvestError::DebounceFreshStart`]) any call
    /// that would create a **fresh** execution rather than attach to a live
    /// (RUNNING/PAUSED) prior. Set by the HTTP handler for a debounced workflow
    /// so an attach/idempotent call is preserved while a fresh start is rejected
    /// — decided atomically under this call's lock (issue #499).
    pub reject_fresh_if_debounced: bool,
}

/// Result of an [`update_with_start_workflow_execution`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateWithStartOutcome {
    pub exec_id: ExecutionId,
    pub workflow_name: String,
    pub workflow_id: String,
    pub state: String,
    pub started_fresh: bool,
    /// The update ID the caller can poll to retrieve the typed result.
    pub update_id: crate::types::UpdateId,
    /// `false` on an idempotency-key cache hit (update was already admitted).
    pub update_admitted: bool,
}

/// Atomically start or attach to a workflow and admit one update.
///
/// Applies the same reuse-policy matrix as `signal_with_start_workflow_execution`
/// but admits exactly one update instead of a signal.
///
/// ## Outcome matrix (mirrors signal-with-start except PAUSED rejects updates)
///
/// | Prior state         | `AllowDuplicate`       | `RejectDuplicate`   | `AllowDupFailedOnly`   | `TerminateIfRunning`      |
/// |---------------------|------------------------|---------------------|------------------------|---------------------------|
/// | none                | start + admit          | start + admit       | start + admit          | start + admit             |
/// | RUNNING             | admit to existing      | `Err(AlreadyExists)`| admit to existing      | cancel + start + admit    |
/// | PAUSED              | `Err(WorkflowPaused)`  | `Err(AlreadyExists)`| `Err(WorkflowPaused)`  | cancel + start + admit    |
/// | COMPLETED/FAILED    | start fresh + admit    | `Err(AlreadyExists)`| start fresh + admit    | start fresh + admit       |
/// | CANCELLED           | start fresh + admit    | `Err(AlreadyExists)`| start fresh + admit    | start fresh + admit       |
/// | TERMINATED          | start fresh + admit    | start fresh + admit | start fresh + admit    | start fresh + admit       |
///
/// ## Event ordering
///
/// On a **fresh start** `WorkflowStarted` is appended and then
/// `UpdateAdmitted` is appended in the same outer transaction. The worker
/// picks up the already-admitted update before first dispatch.
///
/// On an **attach**, `UpdateAdmitted` is appended and the workflow task is
/// woken — both inside the outer transaction.
///
/// ## Idempotency
///
/// When `idempotency_key` is `Some`, the call checks `harvest_events` for an
/// existing `UpdateAdmitted` event with the same `update_id` scoped to
/// `(workflow_name, workflow_id)`. A match returns the prior outcome without
/// re-starting or re-admitting.
///
/// # Errors
///
/// - [`HarvestError::AlreadyExists`] when `RejectDuplicate` rejects.
/// - [`HarvestError::WorkflowPaused`] when attaching to a PAUSED execution.
/// - Propagates queue/event-store failures from the start/admit transactions.
#[allow(clippy::too_many_lines)]
pub async fn update_with_start_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: UpdateWithStartParams<'_>,
) -> HarvestResult<UpdateWithStartOutcome> {
    conn.transaction::<UpdateWithStartOutcome, HarvestError, _>(|conn| {
        let request = request;
        async move {
            // Cross-execution idempotency dedupe scoped to (workflow_name, workflow_id).
            // When an idempotency key is provided we look up by the supplied update_id
            // (callers should derive it deterministically from the key, e.g. UUIDv5).
            if request.idempotency_key.is_some()
                && let Some(prior) = lookup_idempotent_update_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    &request.update_id,
                )
                .await?
            {
                return Ok(UpdateWithStartOutcome {
                    exec_id: prior.exec_id,
                    workflow_name: prior.workflow_name,
                    workflow_id: prior.workflow_id,
                    state: prior.state,
                    started_fresh: false,
                    update_id: request.update_id,
                    update_admitted: false,
                });
            }

            // Upgrade AllowDuplicate / AllowDuplicateFailedOnly to TerminateIfRunning
            // when the prior run is terminal so the update always lands on a live
            // execution (mirrors the signal-with-start "no signal dropped" invariant).
            // For a debounced workflow, skip the upgrade: we must not escalate a
            // terminal prior to a fresh start here — the reject check below routes it
            // to debounce admission instead.
            let effective_policy = if request.reject_fresh_if_debounced {
                request.reuse_policy
            } else {
                resolve_effective_signal_with_start_policy(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    request.reuse_policy,
                )
                .await?
            };

            let build_start_request =
                |exec_id: ExecutionId, policy: WorkflowIdReusePolicy| StartWorkflowParams {
                    workflow_name: request.workflow_name,
                    workflow_id: request.workflow_id,
                    exec_id,
                    input: request.input.clone(),
                    parent_id: request.parent_id,
                    queue_name: request.queue_name,
                    execution_timeout: request.execution_timeout,
                    memo: request.memo.clone(),
                    search_attrs: request.search_attrs.clone(),
                    reuse_policy: policy,
                    trace_context: request.trace_context.clone(),
                    max_execution_timeout_ceiling: request.max_execution_timeout_ceiling,
                    concurrency_key: request.concurrency_key.clone(),
                    concurrency_limit: request.concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes: 0,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner: request.owner,
                    runbook_url: request.runbook_url,
                    severity: request.severity,
                    context_headers: request.context_headers.clone(),
                    sla: request.sla,
                    schedule_id: None,
                    scheduled_for: None,
                    workflow_attempt: 1,
                    workflow_retry_policy: request
                        .workflow_retry_policy
                        .clone()
                        .and_then(|v| serde_json::from_value(v).ok()),
                    retry_of_exec_id: None,
                    max_workflow_attempts_ceiling: request.max_workflow_attempts_ceiling,
                    origin: None,
                };

            // Debounced workflow: route through the no-spawn collect path with
            // reject_fresh so a fresh start (incl. TerminateIfRunning cancel+replace)
            // rolls back via DebounceFreshStart without cancelling/spawning before
            // the rejection (issue #499). Attach returns the existing live run.
            let started = if request.reject_fresh_if_debounced {
                let (s, deferred) = start_or_load_workflow_execution_collect(
                    conn,
                    build_start_request(request.exec_id, effective_policy),
                    true,
                    true,
                )
                .await?;
                for d in deferred {
                    d.spawn();
                }
                s
            } else {
                start_or_load_workflow_execution(
                    conn,
                    build_start_request(request.exec_id, effective_policy),
                )
                .await?
            };

            // Enforce workflow input cap on fresh start.
            if started.created {
                check_sws_payload_cap(
                    &request.input,
                    crate::error::PayloadKind::WorkflowInput,
                    request.max_workflow_input_bytes,
                    request.workflow_name,
                )?;
            }

            // TOCTOU guard: if a concurrent transaction completed the run between
            // the policy resolver's lock and our start, escalate so the update lands.
            // SUSPENDED is treated as RUNNING here (not a real DB state today, but
            // defensive). PAUSED is a non-terminal active state; the update will be
            // rejected by admit_update_event below (WorkflowPaused), rolling back.
            let started = if !matches!(started.state.as_str(), "RUNNING" | "SUSPENDED" | "PAUSED")
                // Debounced workflow: never escalate a terminal prior to a fresh
                // start here — route it to debounce admission via the check below.
                && !request.reject_fresh_if_debounced
                && matches!(
                    request.reuse_policy,
                    WorkflowIdReusePolicy::AllowDuplicate
                        | WorkflowIdReusePolicy::AllowDuplicateFailedOnly
                ) {
                let fresh_exec_id = ExecutionId::new_for_shard(started.exec_id.shard());
                let fresh = start_or_load_workflow_execution(
                    conn,
                    build_start_request(fresh_exec_id, WorkflowIdReusePolicy::TerminateIfRunning),
                )
                .await?;
                if fresh.created {
                    check_sws_payload_cap(
                        &request.input,
                        crate::error::PayloadKind::WorkflowInput,
                        request.max_workflow_input_bytes,
                        request.workflow_name,
                    )?;
                }
                fresh
            } else {
                started
            };

            // Atomic debounce gate (issue #499): a debounced workflow may only
            // *attach* to a live (RUNNING/SUSPENDED/PAUSED) prior. A fresh insert
            // (`created`) or a non-live prior would be a fresh start — reject and let
            // the caller route to debounce admission. Rolls back any fresh insert.
            if request.reject_fresh_if_debounced
                && (started.created
                    || !matches!(started.state.as_str(), "RUNNING" | "SUSPENDED" | "PAUSED"))
            {
                return Err(HarvestError::DebounceFreshStart {
                    workflow_name: request.workflow_name.to_string(),
                    workflow_id: request.workflow_id.to_string(),
                });
            }

            // Post-lock idempotency re-check: two concurrent calls with the same
            // idempotency_key may both pass the early dedupe query (which runs before
            // the execution row lock is acquired). After the lock is held, any prior
            // admission committed by a racing transaction is now visible — re-check so
            // the loser returns the cached outcome rather than admitting a second time.
            if request.idempotency_key.is_some()
                && let Some(prior) = lookup_idempotent_update_dedupe(
                    conn,
                    request.workflow_name,
                    request.workflow_id,
                    &request.update_id,
                )
                .await?
            {
                return Ok(UpdateWithStartOutcome {
                    exec_id: prior.exec_id,
                    workflow_name: prior.workflow_name,
                    workflow_id: prior.workflow_id,
                    state: prior.state,
                    started_fresh: false,
                    update_id: request.update_id,
                    update_admitted: false,
                });
            }

            // Admit the update against the resolved execution.
            //
            // `admit_update_event` acquires a FOR UPDATE row lock and rejects:
            //   - PAUSED   → HarvestError::WorkflowPaused (rolls back entire tx)
            //   - non-RUNNING → HarvestError::UpdateRejected
            //
            // On fresh start the execution is RUNNING so admission succeeds.
            // The admitted update is part of the same outer transaction as the
            // WorkflowStarted event, so a crash never leaves a half-started
            // execution with no admitted update.
            store::admit_update_event(
                conn,
                started.exec_id,
                request.update_id,
                request.update_name.clone(),
                request.update_args.clone(),
            )
            .await?;

            // Wake the workflow task. For fresh starts, `start_or_load_workflow_execution`
            // already inserted a task queue row; wake_workflow_task is idempotent
            // (it updates the wakeup timestamp) and harmless to call again.
            queue::wake_workflow_task(conn, started.exec_id).await?;

            Ok(UpdateWithStartOutcome {
                exec_id: started.exec_id,
                workflow_name: started.workflow_name,
                workflow_id: started.workflow_id,
                state: started.state,
                started_fresh: started.created,
                update_id: request.update_id,
                update_admitted: true,
            })
        }
        .scope_boxed()
    })
    .await
}

/// Minimal row returned by the idempotency dedupe query.
struct UpdateDedupeRow {
    exec_id: ExecutionId,
    workflow_name: String,
    workflow_id: String,
    state: String,
}

/// Cross-execution idempotency dedupe for `update_with_start`.
///
/// Searches `harvest_events` for an `UpdateAdmitted` event with the given
/// `update_id` across all executions of `(workflow_name, workflow_id)`.
/// Returns the owning execution if found, so a retried call can short-circuit
/// without re-starting or re-admitting.
///
/// The lookup uses JSON operators on `event_data` (Postgres JSONB). This is a
/// cold-path read (retries only) so index coverage is not critical.
async fn lookup_idempotent_update_dedupe(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    update_id: &crate::types::UpdateId,
) -> HarvestResult<Option<UpdateDedupeRow>> {
    use diesel::sql_query;
    use diesel::sql_types::Text;

    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        state: String,
    }

    let row: Option<Row> = sql_query(
        "SELECT wf.id, wf.workflow_name, wf.workflow_id, wf.state \
         FROM harvest_events e \
         JOIN harvest_workflow_executions wf ON e.workflow_exec_id = wf.id \
         WHERE wf.workflow_name = $1 \
           AND wf.workflow_id = $2 \
           AND e.event_data->>'type' = 'UpdateAdmitted' \
           AND e.event_data->'data'->>'update_id' = $3 \
         ORDER BY e.event_id DESC \
         LIMIT 1",
    )
    .bind::<Text, _>(workflow_name)
    .bind::<Text, _>(workflow_id)
    .bind::<Text, _>(update_id.to_string())
    .get_result(conn)
    .await
    .optional()
    .map_err(database_error)?;

    Ok(row.map(|r| UpdateDedupeRow {
        exec_id: ExecutionId::from_uuid(r.id),
        workflow_name: r.workflow_name,
        workflow_id: r.workflow_id,
        state: r.state,
    }))
}

/// Resolve the last-completion-result and last-error carryover for a schedule (issue #488).
///
/// Queries are shard-local, run on the same `conn` inside the start transaction,
/// and exclude the just-inserted execution by `current_exec_id`.
///
/// Selection is by the **scheduled slot** (`scheduled_for`), not completion time:
/// Selection is by the **scheduled slot** (`scheduled_for`), not completion time:
/// the carryover source is the COMPLETED fire with the **greatest `scheduled_for`
/// strictly before this run's own slot** (`current_scheduled_for`) — i.e. the
/// previous logical fire. Bounding the lookup to earlier slots means a backfill or
/// trigger-now run that starts an *older* logical slot after a newer slot already
/// completed sees the cursor as of its own slot, never a future fire's output; and
/// an older slot finishing late can never roll a newer run's cursor backward (the bug
/// a `completed_at` ordering would have). Rows without a slot (`scheduled_for IS NULL`)
/// are excluded; post-migration every scheduled run carries a slot. When the current
/// run itself has no slot (`current_scheduled_for == None`, defensive — scheduled
/// starts always set it) no carryover is resolved.
///
/// Within a slot, ties are broken by `completed_at DESC, id DESC` so that when the
/// same slot was run more than once (a `TERMINATED` row is released from the active
/// uniqueness index and the slot can be re-run), the **latest** attempt of that slot
/// wins rather than an arbitrary older terminated row.
///
/// Returns `(last_completion_result, last_error)` where:
/// - `last_completion_result` = `output` of the highest earlier-slot COMPLETED fire.
/// - `last_error` = `error` of the highest earlier-slot terminal fire if it was
///   `FAILED`/`TIMED_OUT`; `None` if that fire `COMPLETED`/`CANCELLED`/`TERMINATED`.
async fn resolve_carryover(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    current_exec_id: uuid::Uuid,
    current_scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
) -> HarvestResult<(Option<serde_json::Value>, Option<String>)> {
    use crate::schema::harvest_workflow_executions::dsl;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    // No slot on the current run → can't bound to earlier slots; resolve nothing.
    let Some(current_slot) = current_scheduled_for else {
        return Ok((None, None));
    };

    // Highest earlier-slot COMPLETED fire for this schedule.
    let last_completion_result: Option<serde_json::Value> = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .filter(dsl::state.eq("COMPLETED"))
        .filter(dsl::completed_at.is_not_null())
        .filter(dsl::scheduled_for.lt(current_slot))
        .filter(dsl::id.ne(current_exec_id))
        .order((
            dsl::scheduled_for.desc(),
            dsl::completed_at.desc(),
            dsl::id.desc(),
        ))
        .limit(1)
        .select(dsl::output)
        .get_result::<Option<serde_json::Value>>(conn)
        .await
        .optional()
        .map_err(database_error)?
        .flatten();

    // Highest earlier-slot terminal fire for this schedule, across *all* terminal states
    // (COMPLETED, FAILED, TIMED_OUT, CANCELLED, TERMINATED). Surfacing an error only
    // when it is FAILED/TIMED_OUT means a more recent CANCELLED/TERMINATED fire (e.g.
    // via OverlapPolicy::CancelOther / TerminateOther) masks an older failure.
    let last_terminal: Option<(String, Option<String>)> = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .filter(dsl::state.eq_any([
            "COMPLETED",
            "FAILED",
            "TIMED_OUT",
            "CANCELLED",
            "TERMINATED",
        ]))
        .filter(dsl::completed_at.is_not_null())
        .filter(dsl::scheduled_for.lt(current_slot))
        .filter(dsl::id.ne(current_exec_id))
        .order((
            dsl::scheduled_for.desc(),
            dsl::completed_at.desc(),
            dsl::id.desc(),
        ))
        .limit(1)
        .select((dsl::state, dsl::error))
        .get_result::<(String, Option<String>)>(conn)
        .await
        .optional()
        .map_err(database_error)?;

    let last_error = last_terminal.and_then(|(state, error)| {
        if state == "FAILED" || state == "TIMED_OUT" {
            error
        } else {
            None
        }
    });

    Ok((last_completion_result, last_error))
}

/// One row of the per-schedule run-history listing (issue #534).
///
/// Each row is a single workflow execution that a schedule launched (attributed
/// via [`StartWorkflowParams::schedule_id`]), with the columns an operator needs
/// to triage a flaky cron: the originating slot, its timing, its terminal `state`,
/// and the dispatch [`origin`](StartWorkflowParams::origin) marker that keeps a
/// backfill storm or manual fire from masquerading as normal cadence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRunRow {
    /// Execution id of the run.
    pub execution_id: Uuid,
    /// Logical schedule slot this run fired for (`scheduled_for`). `None` for a
    /// `manual_trigger` fire, which is attributed to the schedule but carries no slot.
    pub nominal_fire_time: Option<chrono::DateTime<Utc>>,
    /// When the run started.
    pub started_at: chrono::DateTime<Utc>,
    /// When the run reached a terminal state, or `None` while still active.
    pub completed_at: Option<chrono::DateTime<Utc>>,
    /// Current execution state (`RUNNING`/`COMPLETED`/`FAILED`/`TIMED_OUT`/…).
    pub state: String,
    /// Dispatch origin: `scheduled` / `backfill` / `manual_trigger`. `None` only for
    /// pre-migration rows whose origin could not be reconstructed.
    pub origin: Option<String>,
}

/// Filters + keyset cursor for [`list_schedule_runs`] (issue #534).
///
/// Mirrors the #514 execution-list conventions: a `state` filter, optional time
/// bounds on `started_at`, and a `(started_at, id)` keyset cursor for stable
/// newest-first pagination that merges cleanly across shards.
#[derive(Debug, Clone, Default)]
pub struct ScheduleRunQuery {
    /// Restrict to these terminal/active states. Empty = all states.
    pub states: Vec<String>,
    /// Restrict to these origins (`scheduled`/`backfill`/`manual_trigger`). Empty = all.
    pub origins: Vec<String>,
    /// Lower bound (inclusive) on `started_at`.
    pub since: Option<chrono::DateTime<Utc>>,
    /// Upper bound (exclusive) on `started_at`.
    pub until: Option<chrono::DateTime<Utc>>,
    /// Keyset cursor: return only rows strictly before `(started_at, id)` in the
    /// `started_at DESC, id DESC` ordering.
    pub cursor: Option<(chrono::DateTime<Utc>, Uuid)>,
    /// Maximum rows to return. The caller typically passes `limit + 1` to detect
    /// whether a further page exists.
    pub limit: i64,
}

/// List the executions a schedule launched, newest-first (issue #534).
///
/// Shard-local: a schedule's runs may be spread across shards, so the plugin layer
/// fans this out across `iter_shards()` and merges the per-shard results with a
/// keyset merge. Ordered `started_at DESC, id DESC` so the cursor is stable and the
/// cross-shard merge is well-defined.
pub async fn list_schedule_runs(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    query: &ScheduleRunQuery,
) -> HarvestResult<Vec<ScheduleRunRow>> {
    use crate::schema::harvest_workflow_executions::dsl;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let mut q = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .into_boxed();

    if !query.states.is_empty() {
        q = q.filter(dsl::state.eq_any(query.states.clone()));
    }
    if !query.origins.is_empty() {
        q = q.filter(dsl::origin.eq_any(query.origins.clone()));
    }
    if let Some(since) = query.since {
        q = q.filter(dsl::started_at.ge(since));
    }
    if let Some(until) = query.until {
        q = q.filter(dsl::started_at.lt(until));
    }
    if let Some((cursor_ts, cursor_id)) = query.cursor {
        // Keyset: (started_at, id) < (cursor_ts, cursor_id) under the DESC ordering.
        q = q.filter(
            dsl::started_at
                .lt(cursor_ts)
                .or(dsl::started_at.eq(cursor_ts).and(dsl::id.lt(cursor_id))),
        );
    }

    let rows = q
        .order((dsl::started_at.desc(), dsl::id.desc()))
        .limit(query.limit.max(0) + 1)
        .select((
            dsl::id,
            dsl::scheduled_for,
            dsl::started_at,
            dsl::completed_at,
            dsl::state,
            dsl::origin,
        ))
        .load::<(
            Uuid,
            Option<chrono::DateTime<Utc>>,
            chrono::DateTime<Utc>,
            Option<chrono::DateTime<Utc>>,
            String,
            Option<String>,
        )>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(
            |(execution_id, nominal_fire_time, started_at, completed_at, state, origin)| {
                ScheduleRunRow {
                    execution_id,
                    nominal_fire_time,
                    started_at,
                    completed_at,
                    state,
                    origin,
                }
            },
        )
        .collect())
}

/// A `(state, count)` pair from the per-schedule cadence summary (issue #534).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRunStateCount {
    /// Execution state.
    pub state: String,
    /// Number of `scheduled`-origin runs in this state within the window.
    pub count: i64,
}

/// Count a schedule's **scheduled-origin** runs by terminal state (issue #534).
///
/// Restricted to `origin = 'scheduled'` so a backfill storm or ad-hoc manual fire
/// never inflates the failure ratio an operator reads off the cadence summary.
/// Shard-local; the plugin sums the per-shard counts.
pub async fn schedule_run_state_summary(
    conn: &mut AsyncPgConnection,
    schedule_id: uuid::Uuid,
    since: Option<chrono::DateTime<Utc>>,
    until: Option<chrono::DateTime<Utc>>,
) -> HarvestResult<Vec<ScheduleRunStateCount>> {
    use crate::schema::harvest_workflow_executions::dsl;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let mut q = harvest_workflow_executions::table
        .filter(dsl::schedule_id.eq(schedule_id))
        .filter(dsl::origin.eq(ORIGIN_SCHEDULED))
        .into_boxed();

    if let Some(since) = since {
        q = q.filter(dsl::started_at.ge(since));
    }
    if let Some(until) = until {
        q = q.filter(dsl::started_at.lt(until));
    }

    // Diesel's `group_by` does not compose with a boxed query; per-schedule
    // cardinality is bounded by the retention window, so we load the state column
    // for scheduled-origin runs and tally in memory. Deterministic ordering keeps
    // the cross-shard merge and tests stable.
    let states = q
        .select(dsl::state)
        .load::<String>(conn)
        .await
        .map_err(database_error)?;

    let mut counts: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for state in states {
        *counts.entry(state).or_insert(0) += 1;
    }

    Ok(counts
        .into_iter()
        .map(|(state, count)| ScheduleRunStateCount { state, count })
        .collect())
}

/// One workflow-type row from the per-shard non-terminal execution count
/// (issue #520, workflow-type reachability).
///
/// Each row groups the **non-terminal** executions on a single shard by
/// `workflow_name`. A non-terminal execution is one whose state is not in the
/// terminal set recognised by [`crate::erase::is_terminal_state`]
/// (`COMPLETED`/`FAILED`/`CANCELLED`/`TIMED_OUT`/`CONTINUED_AS_NEW`/`TERMINATED`)
/// — i.e. a `RUNNING`, `SUSPENDED`, or `PAUSED` run whose next replay still
/// requires the `#[workflow]` handler named by `workflow_name`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTypeNonTerminalCount {
    /// Workflow type name — the handler its non-terminal executions replay against.
    pub workflow_name: String,
    /// Count of non-terminal executions of this type on the queried shard.
    pub non_terminal_count: i64,
    /// Start time of the oldest non-terminal execution of this type on the shard.
    pub oldest_started_at: chrono::DateTime<Utc>,
}

#[derive(Debug, diesel::QueryableByName)]
struct WorkflowTypeNonTerminalSqlRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    non_terminal_count: i64,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    oldest_started_at: chrono::DateTime<Utc>,
}

/// SQL for [`non_terminal_counts_by_workflow_name`].
///
/// Read-only `GROUP BY workflow_name` over `harvest_workflow_executions`,
/// filtered to non-terminal states. The state set is the exact complement of
/// [`crate::erase::is_terminal_state`]. `$1` optionally narrows to a single
/// workflow type; `$2` scopes the query to a single logical shard (mirrors the
/// `shard_id` predicate in the version-usage query so that two logical shards
/// sharing the same Postgres database are never double-counted). Both params
/// are nullable — `NULL` means "no filter". Side-effect-free: no claims, no
/// writes, no events appended.
const NON_TERMINAL_COUNTS_SQL: &str = r"
SELECT
    workflow_name::TEXT AS workflow_name,
    COUNT(*)::BIGINT AS non_terminal_count,
    MIN(started_at) AS oldest_started_at
FROM harvest_workflow_executions
WHERE state NOT IN (
        'COMPLETED',
        'FAILED',
        'CANCELLED',
        'TIMED_OUT',
        'CONTINUED_AS_NEW',
        'TERMINATED'
      )
  AND ($1::TEXT IS NULL OR workflow_name = $1::TEXT)
  AND ($2::INT4 IS NULL OR shard_id = $2::INT4)
GROUP BY workflow_name
ORDER BY workflow_name
";

/// Count non-terminal workflow executions grouped by `workflow_name` on one shard.
///
/// Powers the workflow-type reachability check (issue #520): a non-terminal
/// execution directly names — via `workflow_name` — the `#[workflow]` handler
/// its next replay requires, so a non-zero count means deleting or renaming
/// that handler would strand in-flight runs in permanent replay failure.
///
/// `shard_id` scopes the query to a single logical shard. Pass `Some(id)` from
/// the per-shard fan-out so that two logical shards sharing the same Postgres
/// database are never double-counted (mirrors the `shard_id` predicate in
/// `load_version_usage`). Pass `None` only in tests or single-shard contexts
/// where the database is exclusively owned by one shard.
///
/// The optional `workflow_type` filter narrows to a single type; the result
/// shape is unchanged (an empty `Vec` when that type has no non-terminal
/// executions on this shard).
///
/// This is a read-only query: it claims nothing, mutates no state, and appends
/// no [`WorkflowEvent`].
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the query fails.
pub async fn non_terminal_counts_by_workflow_name(
    conn: &mut AsyncPgConnection,
    shard_id: Option<i32>,
    workflow_type: Option<&str>,
) -> HarvestResult<Vec<WorkflowTypeNonTerminalCount>> {
    let rows = diesel::sql_query(NON_TERMINAL_COUNTS_SQL)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(workflow_type)
        .bind::<diesel::sql_types::Nullable<diesel::sql_types::Integer>, _>(shard_id)
        .load::<WorkflowTypeNonTerminalSqlRow>(conn)
        .await
        .map_err(database_error)?;

    Ok(rows
        .into_iter()
        .map(|row| WorkflowTypeNonTerminalCount {
            workflow_name: row.workflow_name,
            non_terminal_count: row.non_terminal_count,
            oldest_started_at: row.oldest_started_at,
        })
        .collect())
}

#[cfg(test)]
mod non_terminal_sql_tests {
    use super::NON_TERMINAL_COUNTS_SQL;
    use crate::erase::is_terminal_state;

    /// The `NOT IN (...)` state list in `NON_TERMINAL_COUNTS_SQL` must be the
    /// exact complement of `erase::is_terminal_state`. If a new terminal state is
    /// added to `is_terminal_state`, this test fails until the SQL is updated,
    /// preventing the reachability query from counting terminal runs as non-terminal
    /// and blocking safe handler removal forever.
    #[test]
    fn non_terminal_sql_excludes_exactly_terminal_states() {
        let terminal_states = [
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ];
        for state in &terminal_states {
            assert!(
                is_terminal_state(state),
                "State '{state}' is listed in NON_TERMINAL_COUNTS_SQL's NOT IN clause \
                 but is_terminal_state returns false — update one of them to match"
            );
            assert!(
                NON_TERMINAL_COUNTS_SQL.contains(state),
                "is_terminal_state returns true for '{state}' but it is missing from \
                 NON_TERMINAL_COUNTS_SQL's NOT IN clause — add it to keep the lists in sync"
            );
        }
        let candidate_non_terminal = ["RUNNING", "SUSPENDED", "PAUSED"];
        for state in &candidate_non_terminal {
            assert!(
                !is_terminal_state(state),
                "State '{state}' appears in the non-terminal candidate list \
                 but is_terminal_state returned true — remove it from this test"
            );
            assert!(
                !NON_TERMINAL_COUNTS_SQL.contains(&format!("'{state}'")),
                "State '{state}' appears in the NOT IN clause of NON_TERMINAL_COUNTS_SQL \
                 but should be non-terminal — remove it from the exclusion list"
            );
        }
    }
}

#[cfg(test)]
mod pause_helper_tests {
    use super::pause_timeout_exceeded;
    use chrono::{Duration as ChronoDuration, Utc};
    use std::time::Duration;

    #[test]
    fn pause_not_expired_within_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::minutes(30);
        assert!(
            !pause_timeout_exceeded(paused_at, now, Duration::from_secs(3600)),
            "a 30-minute pause must not exceed a 1-hour ceiling"
        );
    }

    #[test]
    fn pause_expired_past_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::hours(25);
        assert!(
            pause_timeout_exceeded(paused_at, now, Duration::from_secs(24 * 3600)),
            "a 25-hour pause must exceed the 24-hour ceiling"
        );
    }

    #[test]
    fn pause_expired_exactly_at_ceiling() {
        let now = Utc::now();
        let paused_at = now - ChronoDuration::hours(24);
        assert!(
            pause_timeout_exceeded(paused_at, now, Duration::from_secs(24 * 3600)),
            "a pause exactly at the ceiling is expired (>=)"
        );
    }

    #[test]
    fn zero_ceiling_expires_immediately() {
        let now = Utc::now();
        assert!(
            pause_timeout_exceeded(now, now, Duration::ZERO),
            "a zero ceiling must not strand a paused execution"
        );
    }
}
