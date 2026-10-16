//! Child workflows and commands between runs (issue #2026).
//!
//! Every helper here runs inside the caller's transaction. One database
//! holds every run. So a child start, a result, a cancel or a signal commits
//! with the event that asked for it. The backend needs no outbox.
//!
//! Each event has the shape that the Postgres worker writes, so a history
//! from this backend replays on the core `WorkflowReplayer`.
//!
//! One rule differs from Postgres, because one file is authoritative. An
//! unknown target fails at once with `target_unknown`. Postgres waits a grace
//! window first, because another shard can still create the run.
//!
//! Two smaller differences change no event. A child result reaches only a
//! `RUNNING` parent, because a closed parent can never consume it. And this
//! backend writes a batch in command order. Postgres writes the external
//! signal, cancel and await events of a mixed batch first. Replay accepts
//! both orders, because it drains external events wherever they sit.

use std::collections::HashMap;

use autumn_harvest::types::{
    ExternalAwaitId, ExternalCancelId, ExternalSignalId, ExternalTarget, ParentClosePolicy,
};
use autumn_harvest::{ExecutionId, WorkflowCommand, WorkflowEvent, WorkflowHandlerFn};
use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::error::{SqliteError, SqliteResult};
use crate::runtime::WorkflowPolicy;
use crate::{queue, store, worker};
use rusqlite::Connection;

/// The cancel reason of an external cancel. It matches Postgres.
pub const EXTERNAL_CANCEL_REASON: &str = "cancelled by external request";

/// The cancel reason of a child that lost a race. It matches Postgres.
pub const RACE_LOSER_REASON: &str = "lost race to a sibling branch";

/// The cancel reason of a `request_cancel` parent-close cascade.
const PARENT_CLOSED_REASON: &str = "parent closed";

/// The failure text of a `terminate` parent-close cascade.
const PARENT_CLOSED_TERMINATE: &str = "ParentClosed";

/// The id prefix of a `spawn_child_workflow_timeout` deadline timer. It
/// mirrors the core `CHILD_TIMEOUT_TIMER_PREFIX`, which is not public.
const CHILD_TIMEOUT_TIMER_PREFIX: &str = "__child_timeout:";

/// What a child start reads from the runtime's registry (issue #2026).
pub struct Registry<'a> {
    /// The registered workflow handlers. A child's type must be one of them.
    pub workflows: &'a HashMap<String, WorkflowHandlerFn>,
    /// The timeout and the retry policy of each type (issue #2029).
    pub policies: &'a HashMap<String, WorkflowPolicy>,
}

/// Apply one composition command of a suspending cycle (issue #2026).
///
/// The command is one of `StartChildWorkflow`, `SpawnDetachedChildWorkflow`,
/// `SignalExternalWorkflow`, `RequestCancelExternalWorkflow` or
/// `AwaitExternalWorkflow`. Any other command writes nothing.
///
/// Returns `true` when the command changed durable state.
///
/// # Errors
///
/// Returns [`SqliteError::UnknownWorkflow`] for a child with no handler, or a
/// persistence error. The caller then drops its transaction.
pub fn apply_command(
    conn: &Connection,
    registry: &Registry<'_>,
    exec: ExecutionId,
    history: &[WorkflowEvent],
    cmd: &WorkflowCommand,
    now: i64,
) -> SqliteResult<bool> {
    match cmd {
        WorkflowCommand::StartChildWorkflow {
            child_id,
            workflow_name,
            input,
            ..
        } => start_child(
            conn,
            registry,
            exec,
            history,
            *child_id,
            workflow_name,
            input,
            None,
            now,
        ),
        WorkflowCommand::SpawnDetachedChildWorkflow {
            child_id,
            workflow_name,
            input,
            parent_close_policy,
        } => start_child(
            conn,
            registry,
            exec,
            history,
            *child_id,
            workflow_name,
            input,
            Some(*parent_close_policy),
            now,
        ),
        WorkflowCommand::SignalExternalWorkflow {
            signal_id,
            target,
            signal_name,
            payload,
            already_requested,
            idempotency_key,
            ..
        } => {
            let request = SignalRequest {
                signal_id: *signal_id,
                target,
                signal_name,
                payload,
                idempotency_key: idempotency_key.as_deref(),
                already_requested: *already_requested,
            };
            signal_external(conn, exec, history, &request, now)
        }
        WorkflowCommand::RequestCancelExternalWorkflow {
            cancel_id,
            target,
            already_requested,
            ..
        } => cancel_external(
            conn,
            exec,
            history,
            *cancel_id,
            target,
            *already_requested,
            now,
        ),
        WorkflowCommand::AwaitExternalWorkflow {
            await_id,
            target,
            already_requested,
            ..
        } => await_external(conn, exec, history, *await_id, *target, *already_requested),
        _ => Ok(false),
    }
}

/// Start a child run inside the parent's cycle transaction.
///
/// `policy` is `None` for an awaited child and `Some` for a detached child.
/// The parent records `ChildWorkflowStarted` or `ChildWorkflowSpawnedDetached`.
/// The child gets its row and its `WorkflowStarted` event. It starts at `now`,
/// the cycle clock in epoch milliseconds, so its deadline counts from there.
///
/// A re-drive of an in-progress child re-emits its command with the recorded
/// id. That id is in `history` or has a row, so the call writes nothing.
///
/// Returns `true` when it started a child.
///
/// # Errors
///
/// Returns [`SqliteError::UnknownWorkflow`] when no handler has the name. The
/// caller drops its transaction, so the parent stays `RUNNING` and unchanged.
#[allow(clippy::too_many_arguments)]
pub fn start_child(
    conn: &Connection,
    registry: &Registry<'_>,
    parent: ExecutionId,
    history: &[WorkflowEvent],
    child_id: ExecutionId,
    workflow_name: &str,
    input: &Value,
    policy: Option<ParentClosePolicy>,
    now: i64,
) -> SqliteResult<bool> {
    if child_recorded(history, child_id) || store::execution_exists(conn, child_id)? {
        return Ok(false);
    }
    if !registry.workflows.contains_key(workflow_name) {
        return Err(SqliteError::UnknownWorkflow(workflow_name.to_string()));
    }
    let parent_event = policy.map_or_else(
        || WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: workflow_name.to_string(),
            input: input.clone(),
        },
        |parent_close_policy| WorkflowEvent::ChildWorkflowSpawnedDetached {
            child_id,
            workflow_name: workflow_name.to_string(),
            input: input.clone(),
            parent_close_policy,
        },
    );
    store::append_event(conn, parent, &parent_event)?;
    // A child keeps the timeout and the retry policy of its type (issue
    // #2029). An awaited child never retries, as on Postgres, so only a
    // detached child stores the retry policy.
    let mut config = crate::runtime::fresh_run_config(registry.policies.get(workflow_name), now)?;
    if policy.is_none() {
        config.retry_policy_json = None;
    }
    store::insert_child_execution(
        conn,
        child_id,
        workflow_name,
        input,
        (parent, policy.map(ParentClosePolicy::as_str)),
        &config,
    )?;
    // List and describe read the start time from the row (issue #2027). The
    // deadline counts from it.
    let started_at = DateTime::from_timestamp_millis(now).unwrap_or_else(Utc::now);
    store::set_start_fields(conn, child_id, started_at.timestamp_micros(), None, None)?;
    store::append_event(
        conn,
        child_id,
        &WorkflowEvent::workflow_started(input.clone(), started_at),
    )?;
    Ok(true)
}

/// Record a child dispatch that a failing cycle dropped (issue #952).
///
/// The parent gets `ChildWorkflowStarted` and then `ChildWorkflowFailed` with
/// the core's abandoned-dispatch reason. No child row is made, so the child
/// never runs. The call skips a child that already started.
pub fn record_abandoned_child(
    conn: &Connection,
    parent: ExecutionId,
    history: &[WorkflowEvent],
    child_id: ExecutionId,
    workflow_name: &str,
    input: &Value,
) -> SqliteResult<()> {
    if child_recorded(history, child_id) || store::execution_exists(conn, child_id)? {
        return Ok(());
    }
    store::append_event(
        conn,
        parent,
        &WorkflowEvent::ChildWorkflowStarted {
            child_id,
            workflow_name: workflow_name.to_string(),
            input: input.clone(),
        },
    )?;
    store::append_event(
        conn,
        parent,
        &WorkflowEvent::ChildWorkflowFailed {
            child_id,
            error: autumn_harvest::event::ABANDONED_DISPATCH_REASON.to_string(),
            error_type: None,
            details: None,
            non_retryable: Some(true),
        },
    )
}

/// True if `history` already records a start of `child_id`.
fn child_recorded(history: &[WorkflowEvent], child_id: ExecutionId) -> bool {
    history.iter().any(|e| {
        matches!(
            e,
            WorkflowEvent::ChildWorkflowStarted { child_id: id, .. }
                | WorkflowEvent::ChildWorkflowSpawnedDetached { child_id: id, .. }
                if *id == child_id
        )
    })
}

/// Settle what a closed run owes other runs.
///
/// Call it in the transaction that closes `exec`, after the caller appends
/// `terminal` and sets the row state. It does two things:
///
/// 1. An awaited child writes its result to its `RUNNING` parent.
/// 2. Each `RUNNING` detached child gets its parent-close policy. The parent
///    then records `ChildWorkflowCascadeApplied` for that child.
///
/// Every terminal path calls it. A path that skips it leaves a parent parked
/// permanently, or a detached child running after its parent closed.
pub fn close_execution(
    conn: &Connection,
    exec: ExecutionId,
    terminal: &WorkflowEvent,
    now: i64,
) -> SqliteResult<()> {
    // A worklist, not recursion: a deep chain of detached children must not
    // overflow the stack. Each entry is a run that just closed.
    let mut closed = vec![(exec, terminal.clone())];
    while let Some((exec, terminal)) = closed.pop() {
        notify_parent(conn, exec, &terminal, now)?;
        for (child, policy) in store::running_detached_children(conn, exec)? {
            let policy: ParentClosePolicy = policy
                .parse()
                .map_err(|_| SqliteError::corrupt("parent_close_policy"))?;
            let (action, child_terminal) = match policy {
                ParentClosePolicy::Abandon => continue,
                ParentClosePolicy::RequestCancel => (
                    "request_cancel",
                    seal_cancelled(conn, child, PARENT_CLOSED_REASON)?,
                ),
                ParentClosePolicy::Terminate => (
                    "terminate",
                    seal_failed(conn, child, PARENT_CLOSED_TERMINATE)?,
                ),
            };
            store::append_event(
                conn,
                exec,
                &WorkflowEvent::ChildWorkflowCascadeApplied {
                    child_id: child,
                    policy,
                    action: action.to_string(),
                },
            )?;
            closed.push((child, child_terminal));
        }
    }
    Ok(())
}

/// The number of `ChildWorkflowCascadeApplied` events that a close of `exec`
/// writes now: one per `RUNNING` detached child whose policy is not
/// `abandon`. The terminal cap check counts them, as Postgres does.
pub fn cascade_event_count(conn: &Connection, exec: ExecutionId) -> SqliteResult<u64> {
    let mut count: u64 = 0;
    for (_, policy) in store::running_detached_children(conn, exec)? {
        let policy: ParentClosePolicy = policy
            .parse()
            .map_err(|_| SqliteError::corrupt("parent_close_policy"))?;
        if policy != ParentClosePolicy::Abandon {
            count = count.saturating_add(1);
        }
    }
    Ok(count)
}

/// Write the result of an awaited child to its `RUNNING` parent.
///
/// A child-timeout deadline of the parent that is due at `now` fires first,
/// as the Postgres `materialize_due_child_timeout_deadlines` does (issue
/// #779). The deadline then wins over a child that finished too late.
fn notify_parent(
    conn: &Connection,
    exec: ExecutionId,
    terminal: &WorkflowEvent,
    now: i64,
) -> SqliteResult<()> {
    let Some((parent, None)) = store::parent_link(conn, exec)? else {
        return Ok(());
    };
    if store::execution_state(conn, parent)? != "RUNNING" {
        return Ok(());
    }
    for timer_id in queue::due_timers(conn, parent, now)? {
        if timer_id.starts_with(CHILD_TIMEOUT_TIMER_PREFIX) {
            worker::fire_timer_within_tx(conn, parent, &timer_id)?;
        }
    }
    // `terminate` writes `WorkflowCancelled` too. Only the state tells it apart.
    let terminated = store::execution_state(conn, exec)? == "TERMINATED";
    let event = match terminal {
        // A timeout keeps the stored error text, as the Postgres
        // `wake_parent_for_child_timeout` does (issue #2029).
        WorkflowEvent::WorkflowExecutionTimedOut { .. } => store::execution_error(conn, exec)?
            .map(|error| WorkflowEvent::child_workflow_failed(exec, error)),
        _ => child_result_event(exec, terminal, terminated),
    };
    if let Some(event) = event {
        store::append_event(conn, parent, &event)?;
    }
    Ok(())
}

/// The event an awaited child's terminal writes to its parent.
///
/// A completion maps to `ChildWorkflowCompleted`. A failure keeps its typed
/// fields. A cancel or a terminate becomes an untyped `ChildWorkflowFailed`,
/// as on Postgres. `terminated` selects the terminate text.
fn child_result_event(
    child_id: ExecutionId,
    terminal: &WorkflowEvent,
    terminated: bool,
) -> Option<WorkflowEvent> {
    match terminal {
        WorkflowEvent::WorkflowCompleted { output } => {
            Some(WorkflowEvent::ChildWorkflowCompleted {
                child_id,
                output: output.clone(),
            })
        }
        WorkflowEvent::WorkflowFailed {
            error,
            error_type,
            details,
            non_retryable,
        } => Some(WorkflowEvent::ChildWorkflowFailed {
            child_id,
            error: error.clone(),
            error_type: error_type.clone(),
            details: details.clone(),
            non_retryable: *non_retryable,
        }),
        // A reuse-policy seal also cancels, as the Postgres pre-check path does.
        WorkflowEvent::WorkflowCancelled { reason } => Some(WorkflowEvent::ChildWorkflowFailed {
            child_id,
            error: if terminated {
                format!("child workflow terminated: {reason}")
            } else {
                format!("child workflow cancelled: {reason}")
            },
            error_type: None,
            details: None,
            non_retryable: None,
        }),
        _ => None,
    }
}

/// Cancel a `RUNNING` run, as the Postgres `cancel_workflow_execution` does.
///
/// The run gets `WorkflowCancelled { reason }` and ends `CANCELLED`. Its open
/// rows go, and [`close_execution`] settles its parent and children. This is a
/// hard seal: the run's body does not run again.
pub fn cancel_execution(
    conn: &Connection,
    exec: ExecutionId,
    reason: &str,
    now: i64,
) -> SqliteResult<()> {
    let event = seal_cancelled(conn, exec, reason)?;
    close_execution(conn, exec, &event, now)
}

/// Seal a `RUNNING` run `CANCELLED`, and return its terminal event. The caller
/// then closes the run.
fn seal_cancelled(
    conn: &Connection,
    exec: ExecutionId,
    reason: &str,
) -> SqliteResult<WorkflowEvent> {
    let event = WorkflowEvent::WorkflowCancelled {
        reason: reason.to_string(),
    };
    store::append_event(conn, exec, &event)?;
    store::set_cancelled(conn, exec, reason)?;
    release_open_work(conn, exec)?;
    Ok(event)
}

/// Seal a `RUNNING` run `FAILED` with an untyped error, for a `terminate`
/// cascade. Return its terminal event. The caller then closes the run.
fn seal_failed(conn: &Connection, exec: ExecutionId, error: &str) -> SqliteResult<WorkflowEvent> {
    let event = WorkflowEvent::WorkflowFailed {
        error: error.to_string(),
        error_type: None,
        details: None,
        non_retryable: None,
    };
    store::append_event(conn, exec, &event)?;
    store::set_failed(conn, exec, error)?;
    release_open_work(conn, exec)?;
    Ok(event)
}

/// Remove the pending tasks, unfired timers and staged signals of a run that
/// will never be driven again. This backend has no retention pass.
pub fn release_open_work(conn: &Connection, exec: ExecutionId) -> SqliteResult<()> {
    queue::delete_pending_tasks_for_execution(conn, exec)?;
    queue::delete_unfired_timers_for_execution(conn, exec)?;
    queue::delete_undelivered_signals_for_execution(conn, exec)?;
    Ok(())
}

/// The id and state of the run `target` names, or `None` if no run matches.
fn resolve_target(
    conn: &Connection,
    target: &ExternalTarget,
) -> SqliteResult<Option<(ExecutionId, String)>> {
    match target {
        ExternalTarget::ExecutionId(id) => state_of(conn, *id).map(|s| s.map(|s| (*id, s))),
        ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        } => store::resolve_run_by_key(conn, workflow_name, workflow_id),
    }
}

/// The state of `exec`, or `None` if it has no row.
pub fn state_of(conn: &Connection, exec: ExecutionId) -> SqliteResult<Option<String>> {
    match store::execution_state(conn, exec) {
        Ok(state) => Ok(Some(state)),
        Err(SqliteError::ExecutionNotFound(_)) => Ok(None),
        Err(err) => Err(err),
    }
}

/// One external signal request, as the core command carries it.
struct SignalRequest<'a> {
    /// The correlation id of the request.
    signal_id: ExternalSignalId,
    /// The run to signal.
    target: &'a ExternalTarget,
    /// The signal name.
    signal_name: &'a str,
    /// The signal payload.
    payload: &'a Value,
    /// The dedup key, if any.
    idempotency_key: Option<&'a str>,
    /// `true` when history already holds the `ExternalSignalRequested`.
    already_requested: bool,
}

/// Resolve a `SignalExternalWorkflow` command in the caller's transaction.
///
/// The caller records `ExternalSignalRequested` and then the outcome. A
/// `RUNNING` target gets a staged signal that arrived at `now`. A key that the
/// target already holds is a duplicate and reports delivered, as on Postgres.
///
/// Returns `true` when it appended an event.
fn signal_external(
    conn: &Connection,
    caller: ExecutionId,
    history: &[WorkflowEvent],
    request: &SignalRequest<'_>,
    now: i64,
) -> SqliteResult<bool> {
    let signal_id = request.signal_id;
    if history.iter().any(|e| {
        matches!(e,
            WorkflowEvent::ExternalSignalDelivered { signal_id: id }
            | WorkflowEvent::ExternalSignalFailed { signal_id: id, .. } if *id == signal_id)
    }) {
        return Ok(false);
    }
    if !request.already_requested {
        store::append_event(
            conn,
            caller,
            &WorkflowEvent::ExternalSignalRequested {
                signal_id,
                target: request.target.clone(),
                signal_name: request.signal_name.to_string(),
                payload: request.payload.clone(),
                idempotency_key: request.idempotency_key.map(str::to_string),
            },
        )?;
    }
    // Postgres treats an empty key as no key.
    let key = request.idempotency_key.filter(|k| !k.is_empty());
    let outcome = signal_failure(conn, request, key, now)?.map_or(
        WorkflowEvent::ExternalSignalDelivered { signal_id },
        |reason_code| WorkflowEvent::ExternalSignalFailed {
            signal_id,
            reason_code: reason_code.to_string(),
        },
    );
    store::append_event(conn, caller, &outcome)?;
    Ok(true)
}

/// Deliver the signal, or return the reason code of the failure.
fn signal_failure(
    conn: &Connection,
    request: &SignalRequest<'_>,
    key: Option<&str>,
    now: i64,
) -> SqliteResult<Option<&'static str>> {
    if let (
        Some(key),
        ExternalTarget::WorkflowId {
            workflow_name,
            workflow_id,
        },
    ) = (key, request.target)
        && store::find_signal_key_in_chain(conn, workflow_name, workflow_id, key)?.is_some()
    {
        return Ok(None);
    }
    let Some((target, state)) = resolve_target(conn, request.target)? else {
        return Ok(Some("target_unknown"));
    };
    if let Some(key) = key
        && store::has_signal_key(conn, target, key)?
    {
        return Ok(None);
    }
    if state != "RUNNING" {
        return Ok(Some(match request.target {
            ExternalTarget::ExecutionId(_) => "target_terminal",
            ExternalTarget::WorkflowId { .. } => "not_running",
        }));
    }
    store::stage_signal(conn, target, request.signal_name, request.payload, now, key)?;
    Ok(None)
}

/// Resolve a `RequestCancelExternalWorkflow` command in the caller's
/// transaction.
///
/// It cancels a `RUNNING` target with [`EXTERNAL_CANCEL_REASON`]. A terminal
/// target is a no-op success. An unknown target fails with `target_unknown`.
///
/// Returns `true` when it appended an event.
fn cancel_external(
    conn: &Connection,
    caller: ExecutionId,
    history: &[WorkflowEvent],
    cancel_id: ExternalCancelId,
    target: &ExternalTarget,
    already_requested: bool,
    now: i64,
) -> SqliteResult<bool> {
    if history.iter().any(|e| {
        matches!(e,
            WorkflowEvent::ExternalCancelDelivered { cancel_id: id }
            | WorkflowEvent::ExternalCancelFailed { cancel_id: id, .. } if *id == cancel_id)
    }) {
        return Ok(false);
    }
    if !already_requested {
        store::append_event(
            conn,
            caller,
            &WorkflowEvent::ExternalCancelRequested {
                cancel_id,
                target: target.clone(),
            },
        )?;
    }
    let Some((id, state)) = resolve_target(conn, target)? else {
        let failed = WorkflowEvent::ExternalCancelFailed {
            cancel_id,
            reason_code: "target_unknown".to_string(),
        };
        store::append_event(conn, caller, &failed)?;
        return Ok(true);
    };
    // Record the outcome first. The cancel can seal the caller too, through a
    // parent-close cascade, and a sealed run gets no more events.
    store::append_event(
        conn,
        caller,
        &WorkflowEvent::ExternalCancelDelivered { cancel_id },
    )?;
    if state == "RUNNING" {
        cancel_execution(conn, id, EXTERNAL_CANCEL_REASON, now)?;
    }
    Ok(true)
}

/// Resolve an `AwaitExternalWorkflow` command in the caller's transaction.
///
/// The caller records `ExternalAwaitRequested` once. A terminal target then
/// resolves the await at once. A `RUNNING` target leaves it open, and each
/// later cycle checks the target again, as the Postgres outbox does.
///
/// Returns `true` when it appended an event.
fn await_external(
    conn: &Connection,
    caller: ExecutionId,
    history: &[WorkflowEvent],
    await_id: ExternalAwaitId,
    target: ExecutionId,
    already_requested: bool,
) -> SqliteResult<bool> {
    if history.iter().any(|e| {
        matches!(e,
            WorkflowEvent::ExternalAwaitResolved { await_id: id, .. }
            | WorkflowEvent::ExternalAwaitFailed { await_id: id, .. } if *id == await_id)
    }) {
        return Ok(false);
    }
    if !already_requested {
        store::append_event(
            conn,
            caller,
            &WorkflowEvent::ExternalAwaitRequested { await_id, target },
        )?;
    }
    let outcome = await_outcome(conn, await_id, target)?;
    if let Some(event) = &outcome {
        store::append_event(conn, caller, event)?;
    }
    Ok(!already_requested || outcome.is_some())
}

/// The terminal event of an await, or `None` while the target runs.
///
/// The mapping follows the Postgres `read_external_await_outcome`.
fn await_outcome(
    conn: &Connection,
    await_id: ExternalAwaitId,
    target: ExecutionId,
) -> SqliteResult<Option<WorkflowEvent>> {
    let Some((target, state)) = current_run(conn, target)? else {
        return Ok(Some(await_failed(await_id, "target_unknown", None)));
    };
    let event = match state.as_str() {
        "RUNNING" => return Ok(None),
        "COMPLETED" => WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: store::execution_output(conn, target)?.unwrap_or(Value::Null),
        },
        "FAILED" => failed_target(conn, await_id, target)?,
        "CANCELLED" => await_failed(
            await_id,
            "target_cancelled",
            store::execution_error(conn, target)?,
        ),
        "TIMED_OUT" => await_failed(
            await_id,
            "target_timed_out",
            store::execution_error(conn, target)?,
        ),
        "TERMINATED" => await_failed(
            await_id,
            "target_terminated",
            store::execution_error(conn, target)?,
        ),
        // A reuse-policy seal (`CONTINUED_AS_NEW`) records no successor here.
        // Report the run's own last terminal event instead.
        _ => sealed_target(conn, await_id, target)?,
    };
    Ok(Some(event))
}

/// The most recent run in the continue-as-new chain of `target`, and its state.
///
/// A run that continued as new hands the await to its successor, as on
/// Postgres (issue #2023). A reuse-policy seal has no successor, so the chain
/// stops there. `None` means that `target` has no row.
fn current_run(
    conn: &Connection,
    target: ExecutionId,
) -> SqliteResult<Option<(ExecutionId, String)>> {
    let mut run = target;
    loop {
        let Some(state) = state_of(conn, run)? else {
            return Ok(None);
        };
        let successor = if state == "CONTINUED_AS_NEW" {
            store::successor_of(conn, run)?
        } else {
            None
        };
        match successor {
            Some(next) => run = next,
            None => return Ok(Some((run, state))),
        }
    }
}

/// `ExternalAwaitFailed` for a target that ended `FAILED`, with the typed
/// fields of its last `WorkflowFailed` event.
fn failed_target(
    conn: &Connection,
    await_id: ExternalAwaitId,
    target: ExecutionId,
) -> SqliteResult<WorkflowEvent> {
    let history = store::load_history(conn, target)?;
    let last_failure = history.iter().rev().find_map(|e| match e {
        WorkflowEvent::WorkflowFailed {
            error,
            error_type,
            details,
            non_retryable,
        } => Some((
            error.clone(),
            error_type.clone(),
            details.clone(),
            *non_retryable,
        )),
        _ => None,
    });
    Ok(match last_failure {
        Some((message, error_type, details, non_retryable)) => WorkflowEvent::ExternalAwaitFailed {
            await_id,
            reason_code: "target_failed".to_string(),
            message: Some(message),
            error_type,
            details,
            non_retryable,
        },
        None => WorkflowEvent::ExternalAwaitFailed {
            await_id,
            reason_code: "target_failed".to_string(),
            message: store::execution_error(conn, target)?,
            error_type: None,
            details: None,
            non_retryable: None,
        },
    })
}

/// The await outcome of a sealed target, read from its last terminal event.
fn sealed_target(
    conn: &Connection,
    await_id: ExternalAwaitId,
    target: ExecutionId,
) -> SqliteResult<WorkflowEvent> {
    let history = store::load_history(conn, target)?;
    let terminal = history.iter().rev().find(|e| {
        matches!(
            e,
            WorkflowEvent::WorkflowCompleted { .. }
                | WorkflowEvent::WorkflowFailed { .. }
                | WorkflowEvent::WorkflowCancelled { .. }
        )
    });
    Ok(match terminal {
        Some(WorkflowEvent::WorkflowCompleted { output }) => WorkflowEvent::ExternalAwaitResolved {
            await_id,
            output: output.clone(),
        },
        Some(WorkflowEvent::WorkflowFailed { .. }) => failed_target(conn, await_id, target)?,
        // Postgres reports a replaced run by its last terminal event, so a
        // cancelled one is `target_cancelled`.
        Some(WorkflowEvent::WorkflowCancelled { reason }) => {
            await_failed(await_id, "target_cancelled", Some(reason.clone()))
        }
        _ => await_failed(
            await_id,
            "target_terminated",
            store::execution_error(conn, target)?,
        ),
    })
}

/// An untyped `ExternalAwaitFailed`. A known terminal state also names itself
/// in `error_type`, as on Postgres. `target_unknown` carries no fields.
fn await_failed(
    await_id: ExternalAwaitId,
    reason_code: &str,
    message: Option<String>,
) -> WorkflowEvent {
    let error_type = (reason_code != "target_unknown").then(|| reason_code.to_string());
    WorkflowEvent::ExternalAwaitFailed {
        await_id,
        reason_code: reason_code.to_string(),
        message,
        error_type,
        details: None,
        non_retryable: None,
    }
}

/// The `RUNNING` runs that a parked `commands` batch waits on, in command order.
///
/// Each is an awaited child or an await target. The driver can drive these
/// runs itself, so this check comes before the signal and timer checks.
pub fn awaited_running_runs(
    conn: &Connection,
    commands: &[WorkflowCommand],
) -> SqliteResult<Vec<ExecutionId>> {
    let mut running = Vec::new();
    for cmd in commands {
        let current = match cmd {
            WorkflowCommand::StartChildWorkflow { child_id, .. } => {
                state_of(conn, *child_id)?.map(|state| (*child_id, state))
            }
            WorkflowCommand::AwaitExternalWorkflow { target, .. } => current_run(conn, *target)?,
            _ => continue,
        };
        if let Some((id, state)) = current
            && state == "RUNNING"
            && !running.contains(&id)
        {
            running.push(id);
        }
    }
    Ok(running)
}
