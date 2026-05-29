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
}

impl CancelledWorkflowExecution {
    fn idempotent(exec_id: ExecutionId, execution: WorkflowExecution) -> Self {
        Self {
            exec_id,
            state: execution.state,
            reason: execution
                .error
                .unwrap_or_else(|| "workflow already cancelled".to_string()),
            newly_cancelled: false,
            failed_task_count: 0,
        }
    }

    fn newly_cancelled(exec_id: ExecutionId, reason: String, failed_task_count: usize) -> Self {
        Self {
            exec_id,
            state: "CANCELLED".to_string(),
            reason,
            newly_cancelled: true,
            failed_task_count,
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
        && existing.state == "RUNNING"
    {
        let existing_exec_id = ExecutionId::from_uuid(existing.id);
        // Ignore Config errors: the execution may have transitioned to a terminal
        // state between the pre-check and the cancel lock. In that race the prior
        // run is already done, so we just continue to the start transaction below.
        match cancel_workflow_execution(conn, existing_exec_id, "terminated to start new execution")
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

    conn.transaction::<StartedWorkflowExecution, HarvestError, _>(|conn| {
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
                return Ok(StartedWorkflowExecution::from_row(execution, true));
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
                WorkflowIdReusePolicy::AllowDuplicate => {
                    Ok(StartedWorkflowExecution::from_row(existing, false))
                }

                WorkflowIdReusePolicy::RejectDuplicate => Err(HarvestError::AlreadyExists {
                    existing_exec_id: ExecutionId::from_uuid(existing.id),
                    existing_state: existing.state,
                }),

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
                            Ok(StartedWorkflowExecution::from_row(existing, false))
                        }
                    }
                }

                WorkflowIdReusePolicy::TerminateIfRunning => {
                    // The pre-check above cancelled any RUNNING prior execution
                    // (Transaction 1). By the time we reach this point the prior
                    // execution's state is CANCELLED, FAILED, COMPLETED, or —
                    // under extreme concurrency — still RUNNING. All cases start
                    // fresh; for the still-RUNNING race we inline the cancel here
                    // so the new start is not silently blocked.
                    if existing.state == "RUNNING" {
                        inline_cancel(conn, ExecutionId::from_uuid(existing.id)).await?;
                    }
                    replace_execution(conn, existing, &row, &enqueue, exec_id, &request, now).await
                }
            }
        }
        .scope_boxed()
    })
    .await
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
) -> HarvestResult<StartedWorkflowExecution> {
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

    Ok(StartedWorkflowExecution::from_row(new_execution, true))
}

/// Inline cancellation for the `TerminateIfRunning` race condition where a
/// RUNNING row appears inside the start transaction despite the pre-check.
/// Appends a `WorkflowCancelled` event, transitions to CANCELLED, and fails
/// open tasks — all within the caller's transaction.
async fn inline_cancel(conn: &mut AsyncPgConnection, exec_id: ExecutionId) -> HarvestResult<()> {
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
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason)),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    queue::fail_open_tasks_for_execution(conn, exec_id, &format!("workflow cancelled: {reason}"))
        .await?;
    Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    Ok(())
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
pub async fn cancel_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<CancelledWorkflowExecution> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow cancellation requested".to_string()
    } else {
        reason.to_string()
    };

    let cancel_result = conn
        .transaction::<CancelledWorkflowExecution, HarvestError, _>(|conn| {
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
                    "CANCELLED" => {
                        return Ok(CancelledWorkflowExecution::idempotent(exec_id, execution));
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
                        .filter(harvest_workflow_executions::state.eq("RUNNING"))
                        .set((
                            harvest_workflow_executions::state.eq("CANCELLED"),
                            harvest_workflow_executions::output.eq(None::<serde_json::Value>),
                            harvest_workflow_executions::error.eq(Some(reason.clone())),
                            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
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
                apply_parent_close_cascade(conn, exec_id).await?;

                Ok(CancelledWorkflowExecution::newly_cancelled(
                    exec_id,
                    reason,
                    total_failed_or_deleted,
                ))
            }
            .scope_boxed()
        })
        .await?;

    Ok(cancel_result)
}

/// Count running detached children that would append a parent cascade event.
pub(crate) async fn parent_close_cascade_event_count(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<u64> {
    let policies: Vec<Option<String>> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
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

/// Apply parent-close cascade to all running detached children of `parent_exec_id`.
///
/// Queries children with `parent_close_policy IS NOT NULL AND state = 'RUNNING'`.
/// - Abandon: no-op
/// - `RequestCancel`: appends `WorkflowCancelled`, transitions to CANCELLED, fails tasks
/// - `Terminate`: appends `WorkflowFailed`, transitions to FAILED, fails tasks
///
/// Appends a `ChildWorkflowCascadeApplied` event to the parent history for each
/// non-Abandon action. Idempotent: acts only on RUNNING children.
pub(crate) async fn apply_parent_close_cascade(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
) -> HarvestResult<()> {
    use crate::store;

    let running_children: Vec<(Uuid, Option<String>)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(parent_exec_id.as_uuid())))
        .filter(harvest_workflow_executions::parent_close_policy.is_not_null())
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::parent_close_policy,
        ))
        .load::<(Uuid, Option<String>)>(conn)
        .await
        .map_err(database_error)?;

    for (child_uuid, policy_opt) in running_children {
        let child_exec_id = ExecutionId::from_uuid(child_uuid);
        let policy_str = policy_opt.expect("filtered by is_not_null");
        let policy = policy_str
            .parse::<ParentClosePolicy>()
            .map_err(HarvestError::Config)?;

        let action = match policy {
            ParentClosePolicy::Abandon => None,
            ParentClosePolicy::RequestCancel => {
                cascade_cancel_detached_child(conn, child_exec_id, "parent closed")
                    .await?
                    .then_some("request_cancel")
            }
            ParentClosePolicy::Terminate => {
                cascade_terminate_detached_child(conn, child_exec_id, "ParentClosed")
                    .await?
                    .then_some("terminate")
            }
        };
        let Some(action) = action else {
            continue;
        };

        store::append_single_event(
            conn,
            parent_exec_id,
            crate::event::WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id: child_exec_id,
                policy,
                action: action.to_string(),
            },
        )
        .await?;
    }

    Ok(())
}

async fn cascade_cancel_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<bool> {
    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .set((
            harvest_workflow_executions::state.eq("CANCELLED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok(false);
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
    Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    Ok(true)
}

async fn cascade_terminate_detached_child(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<bool> {
    let updated = diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
        .filter(harvest_workflow_executions::state.eq("RUNNING"))
        .set((
            harvest_workflow_executions::state.eq("FAILED"),
            harvest_workflow_executions::error.eq(Some(reason.to_string())),
            harvest_workflow_executions::completed_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
    if updated == 0 {
        return Ok(false);
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
    Box::pin(apply_parent_close_cascade(conn, exec_id)).await?;
    Ok(true)
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
pub async fn terminate_workflow_execution(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    reason: &str,
) -> HarvestResult<CancelledWorkflowExecution> {
    let reason = reason.trim();
    let reason = if reason.is_empty() {
        "workflow termination requested".to_string()
    } else {
        reason.to_string()
    };

    let cancel_result = conn
        .transaction::<CancelledWorkflowExecution, HarvestError, _>(|conn| {
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
                    return Ok(CancelledWorkflowExecution::idempotent(exec_id, execution));
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
                apply_parent_close_cascade(conn, exec_id).await?;

                Ok(CancelledWorkflowExecution::newly_cancelled(
                    exec_id,
                    reason,
                    failed_task_count,
                ))
            }
            .scope_boxed()
        })
        .await?;

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
            let started = if started.state != "RUNNING"
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
            if started.state == "RUNNING" {
                check_sws_payload_cap(
                    &request.signal_payload,
                    crate::error::PayloadKind::SignalPayload,
                    request.max_signal_payload_bytes,
                    request.workflow_name,
                )?;
            }

            let signal_delivered = if started.state == "RUNNING" {
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
    if existing.state == "RUNNING" {
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
