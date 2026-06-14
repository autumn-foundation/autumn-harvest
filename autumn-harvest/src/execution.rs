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
}

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
        reason: String,
        failed_task_count: usize,
        workflow_name: String,
        queue_name: String,
        prior_state: String,
    ) -> Self {
        Self {
            exec_id,
            state: "CANCELLED".to_string(),
            reason,
            newly_cancelled: true,
            failed_task_count,
            workflow_name,
            queue_name,
            prior_state,
        }
    }
}

/// Start a workflow execution or load the existing one, applying the caller's
/// [`WorkflowIdReusePolicy`] when a duplicate `(workflow_name, workflow_id)`
/// collision occurs.
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
#[allow(clippy::too_many_lines)]
pub async fn start_or_load_workflow_execution(
    conn: &mut AsyncPgConnection,
    request: StartWorkflowParams<'_>,
) -> HarvestResult<StartedWorkflowExecution> {
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

    // For TerminateIfRunning: if there is an existing RUNNING execution, cancel
    // it (Transaction 1) before the start transaction below (Transaction 2). A
    // crash between the two leaves the prior workflow CANCELLED with no new run;
    // retrying with the same policy starts fresh on the next attempt because the
    // CANCELLED row is treated as "start fresh" by TerminateIfRunning.
    if request.reuse_policy == WorkflowIdReusePolicy::TerminateIfRunning
        && let Some(existing) =
            try_load_by_key(conn, request.workflow_name, request.workflow_id).await?
        && matches!(existing.state.as_str(), "RUNNING" | "PAUSED")
    {
        let existing_exec_id = ExecutionId::from_uuid(existing.id);
        // Ignore Config errors: the execution may have transitioned to a terminal
        // state between the pre-check and the cancel lock. In that race the prior
        // run is already done, so we just continue to the start transaction below.
        match cancel_workflow_execution(
            conn,
            existing_exec_id,
            "terminated to start new execution",
            &crate::telemetry::NoOpMetrics,
        )
        .await
        {
            Ok(_) | Err(HarvestError::Config(_)) => {}
            Err(e) => return Err(e),
        }
    }

    // Look up the active build policy for this queue. If a policy exists, new
    // executions are stamped with its build_id so workers can enforce routing.
    let policy = build_routing::get_build_policy(conn, request.queue_name).await?;
    let assigned_build = policy.map(|p| p.build_id);

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

    let (cancel_result, deferred_starts) = conn
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
                        let started_event = WorkflowEvent::WorkflowStarted {
                            input: request.input.clone(),
                            timestamp: target_start_time,
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
        .await?;

    for start in deferred_starts {
        start.spawn();
    }

    Ok(cancel_result)
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
    let started_event = WorkflowEvent::WorkflowStarted {
        input: request.input.clone(),
        timestamp: start_timestamp,
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

                let updated =
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
                        .set((
                            harvest_workflow_executions::state.eq("CANCELLED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(reason.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
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

/// Hard-finalize a workflow execution to `CANCELLED` regardless of its
/// current state.
///
/// `cancel_workflow_execution` is the graceful path: it requires the
/// execution to be `RUNNING` and (in Phase 4) will run cancellation
/// handlers before flipping the state. `terminate_workflow_execution` is
/// the operator escape hatch — it accepts every non-cancelled state
/// (including `RUNNING`, `FAILED`, `TIMED_OUT`) and forces the row to
/// `CANCELLED`. Open task rows are still failed so workers don't keep
/// chewing on a torn-down execution.
///
/// Like the cancel path, this emits a [`WorkflowEvent::WorkflowCancelled`]
/// (no new event variant — the issue's append-only contract is intact),
/// records the supplied reason on the row, and is idempotent against an
/// execution that is already `CANCELLED`.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] when the execution does not exist
/// and [`HarvestError::Database`] for persistence failures. Unlike
/// `cancel_workflow_execution`, this never returns
/// [`HarvestError::Config`] for "already terminal" — that's the whole
/// point of the operator override.
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

                    if execution.state == "CANCELLED" {
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

                    // No state-precondition filter: operator override force-writes.
                    diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
                        .set((
                            harvest_workflow_executions::state.eq("CANCELLED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(reason.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
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
                    let mut deferred = apply_parent_close_cascade(conn, exec_id).await?;
                    let triggers = crate::completion_trigger::evaluate_triggers_for_execution(
                        conn,
                        exec_id,
                        crate::completion_trigger::TerminalState::Cancelled,
                        None,
                    )
                    .await?;
                    deferred.extend(triggers);

                    let prior_state = execution.state.clone();
                    Ok((
                        CancelledWorkflowExecution::newly_cancelled(
                            exec_id,
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
async fn try_load_by_key(
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
            let effective_policy = resolve_effective_signal_with_start_policy(
                conn,
                request.workflow_name,
                request.workflow_id,
                request.reuse_policy,
            )
            .await?;

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
                };

            let started = start_or_load_workflow_execution(
                conn,
                build_start_request(request.exec_id, effective_policy),
            )
            .await?;

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
            let effective_policy = resolve_effective_signal_with_start_policy(
                conn,
                request.workflow_name,
                request.workflow_id,
                request.reuse_policy,
            )
            .await?;

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
                };

            let started = start_or_load_workflow_execution(
                conn,
                build_start_request(request.exec_id, effective_policy),
            )
            .await?;

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
