//! Workflow executor -- runs a single workflow function through replay + live execution.
//!
//! The executor builds a [`WorkflowContext`] from the event history, runs the
//! handler with a short timeout, and classifies the outcome:
//!
//! - **Completed**: handler returned `Ok(output)`.
//! - **Failed**: handler returned `Err(error)`.
//! - **Suspended**: handler blocked on a oneshot (waiting for activity/timer resolution).
//!
//! This module is pure async logic and does NOT require the `db` feature.

use std::time::Duration;

use serde_json::Value;
use tracing::Instrument;

use crate::context::{
    SharedState, WorkflowCommand, WorkflowContext, WorkflowHistoryPolicy, empty_shared_state,
};
use crate::event::WorkflowEvent;
use crate::info::{QueryHandlerInfo, UpdateHandlerInfo, WorkflowHandlerFn};
use crate::telemetry::{
    ATTR_EXECUTION_ID, ATTR_QUEUE, ATTR_REPLAY, ATTR_SHARD_ID, ATTR_WORKFLOW_ID,
};
use crate::types::ExecutionId;

/// The outcome of running a workflow function through the executor.
#[derive(Debug)]
pub enum WorkflowOutcome {
    /// The workflow ran to completion and returned a value.
    Completed {
        /// The final result serialized as JSON.
        output: Value,
    },
    /// The workflow function returned an error.
    Failed {
        /// The string description of the error encountered.
        error: String,
        /// Structured details if the error is a non-determinism divergence.
        non_deterministic_details: Option<crate::error::NonDeterministicDetails>,
    },
    /// The workflow suspended awaiting activity results or timer firings.
    /// The accumulated commands describe what the worker needs to schedule.
    Suspended {
        /// A list of commands representing the side effects (e.g. activities) requested.
        commands: Vec<WorkflowCommand>,
    },
    /// The workflow signalled `continue_as_new`. The current execution is
    /// terminal and the worker should atomically start a fresh execution
    /// with the same logical `WorkflowId` but a new `ExecutionId`, passing
    /// `input` as the initial payload.
    ContinuedAsNew {
        /// JSON payload to pass to the next iteration of the workflow.
        input: Value,
    },
}

/// Default timeout for detecting suspension -- if the workflow hasn't completed
/// within this window, it's blocked on a oneshot channel (suspended).
const SUSPENSION_TIMEOUT: Duration = Duration::from_millis(100);

/// Caller-supplied metadata recorded onto the `harvest.workflow.execute` span.
pub struct WorkflowExecuteSpanMeta {
    /// Logical workflow name (recorded as `harvest.workflow.id`).
    pub workflow_name: String,
    /// Business-level workflow identifier (e.g. `"subscription-123"`).
    /// Forwarded to [`WorkflowContext`] so [`WorkflowLogger`] can tag events.
    pub workflow_id: String,
    /// Shard identifier (recorded as `harvest.shard.id`).
    pub shard_id: i64,
    /// Task queue name (recorded as `harvest.queue`).
    pub queue_name: String,
    /// Whether this cycle is a deterministic replay (recorded as `harvest.replay`).
    pub is_replay: bool,
    /// W3C traceparent linking back to the original trace, present only on
    /// replay runs and only when a prior carrier stored a link.
    pub link_traceparent: Option<String>,
    /// The worker build ID of the worker executing this workflow.
    pub build_id: Option<String>,
}

/// Run a workflow function through replay and live execution.
///
/// Builds a [`WorkflowContext`] from the provided event history, invokes the
/// handler, and returns the outcome. If the handler completes within the
/// timeout, the result is `Completed` or `Failed`. If it blocks (suspended on
/// a oneshot waiting for activity/timer resolution), the accumulated commands
/// are returned as `Suspended`.
///
/// # Arguments
///
/// * `exec_id` - The execution ID for this workflow run.
/// * `history` - The event history to replay (must start with `WorkflowStarted`).
/// * `handler` - The type-erased workflow handler function.
/// * `input` - The serialized input to pass to the workflow.
pub async fn run_workflow(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
) -> WorkflowOutcome {
    let (outcome, _pending, _span) =
        run_workflow_with_state(exec_id, history, handler, input, empty_shared_state(), None).await;
    outcome
}

/// Like [`run_workflow`] but runs in strict replay mode.
///
/// Uses [`WorkflowContext::for_replay_strict`] so that activity and local-activity
/// dispatch additionally compare input payloads against the recorded history,
/// returning a non-determinism error on any mismatch.  This is used by
/// [`WorkflowReplayer`](crate::testing::WorkflowReplayer) to catch
/// input-changing code changes before deployment.
#[allow(clippy::implicit_hasher)]
pub async fn run_workflow_strict(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    context_headers: std::collections::HashMap<String, String>,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_strict_with_state(exec_id, history, state)
        .with_context_headers(context_headers);

    // ADR-0001 §2.1: strict mode is always a replay cycle.
    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = true,
    );

    async {
        let timeout_result = tokio::time::timeout(SUSPENSION_TIMEOUT, handler(&ctx, input)).await;
        match timeout_result {
            // An infallible built-in primitive (system_now/new_uuid/random_*) may
            // have absorbed a divergence and returned a fallback value (issue #384);
            // surface it before the other completion checks.
            Ok(Ok(output)) => ctx.take_deferred_nd_error().map_or_else(
                || {
                    if ctx.history_has_unconsumed_events() {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<end of history>".to_string()),
                                actual: Some("<workflow returned early>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: early completion mismatch: \
                                    expected <end of history>, got <workflow returned early>"
                                .to_string(),
                            non_deterministic_details: nd,
                        }
                    } else if ctx.drain_commands().into_iter().any(|cmd| {
                        // UpsertSearchAttributes and SetCurrentDetails are pure metadata
                        // and do not affect replay determinism; exclude from this check.
                        !matches!(
                            cmd,
                            WorkflowCommand::UpsertSearchAttributes { .. }
                                | WorkflowCommand::SetCurrentDetails { .. }
                        )
                    }) {
                        // New commands emitted after history was fully consumed (e.g. a
                        // newly-added version() or side_effect() call on an old history).
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<no new commands>".to_string()),
                                actual: Some("<new commands emitted>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: new commands emitted beyond \
                                    recorded history"
                                .to_string(),
                            non_deterministic_details: nd,
                        }
                    } else {
                        WorkflowOutcome::Completed { output }
                    }
                },
                |nd| {
                    let details = ctx.take_nd_details();
                    WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    }
                },
            ),
            // A primitive may have drifted before the workflow returned Err from
            // its own logic; prefer the non-determinism error (issue #384).
            Ok(Err(error)) => {
                let details = ctx.take_nd_details();
                ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    },
                )
            }
            Err(_elapsed) => {
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have recorded a divergence before the workflow parked on an
                // await point. Fail the execution now rather than suspending from
                // a non-deterministic state (issue #384).
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    };
                }
                let mut commands = ctx.drain_commands();
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
}

/// Run a workflow function through replay canary mode.
///
/// Simulates workflow execution under strict replay, but utilizing a canary
/// context. If execution reaches the end of the recorded history and suspends,
/// it returns `WorkflowOutcome::Suspended` rather than a non-determinism error.
/// If it suspends *before* all events in history are processed, it fails.
#[allow(clippy::implicit_hasher, clippy::too_many_lines)]
pub async fn run_workflow_canary(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    context_headers: std::collections::HashMap<String, String>,
) -> WorkflowOutcome {
    let ctx = WorkflowContext::for_replay_canary_with_state(exec_id, history, state)
        .with_context_headers(context_headers);

    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = true,
    );

    async {
        let timeout_result = tokio::time::timeout(SUSPENSION_TIMEOUT, handler(&ctx, input)).await;
        match timeout_result {
            Ok(Ok(output)) => ctx.take_deferred_nd_error().map_or_else(
                || {
                    if ctx.history_has_unconsumed_events() {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<end of history>".to_string()),
                                actual: Some("<workflow returned early>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: early completion mismatch: \
                                    expected <end of history>, got <workflow returned early>"
                                .to_string(),
                            non_deterministic_details: nd,
                        }
                    } else if ctx.drain_commands().into_iter().any(|cmd| {
                        !matches!(
                            cmd,
                            WorkflowCommand::UpsertSearchAttributes { .. }
                                | WorkflowCommand::SetCurrentDetails { .. }
                        )
                    }) {
                        let nd = ctx.take_nd_details().or_else(|| {
                            Some(crate::error::NonDeterministicDetails {
                                event_index: i32::try_from(ctx.replay_position()).ok(),
                                expected: Some("<no new commands>".to_string()),
                                actual: Some("<new commands emitted>".to_string()),
                                workflow_type: Some(ctx.workflow_type().to_string()),
                                build_id: ctx.build_id().map(String::from),
                            })
                        });
                        WorkflowOutcome::Failed {
                            error: "non-deterministic replay: new commands emitted beyond \
                                    recorded history"
                                .to_string(),
                            non_deterministic_details: nd,
                        }
                    } else {
                        WorkflowOutcome::Completed { output }
                    }
                },
                |nd| {
                    let details = ctx.take_nd_details();
                    WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    }
                },
            ),
            Ok(Err(error)) => {
                let details = ctx.take_nd_details();
                ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    },
                )
            }
            Err(_elapsed) => {
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    };
                }

                // If history still has unconsumed events when we suspend, that's non-deterministic
                if ctx.history_has_unconsumed_events() {
                    let nd = ctx.take_nd_details().or_else(|| {
                        Some(crate::error::NonDeterministicDetails {
                            event_index: i32::try_from(ctx.replay_position()).ok(),
                            expected: Some("<consume all history>".to_string()),
                            actual: Some("<workflow suspended early>".to_string()),
                            workflow_type: Some(ctx.workflow_type().to_string()),
                            build_id: ctx.build_id().map(String::from),
                        })
                    });
                    return WorkflowOutcome::Failed {
                        error: "non-deterministic replay: workflow suspended before all history events were replayed".to_string(),
                        non_deterministic_details: nd,
                    };
                }

                let mut commands = ctx.drain_commands();
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return WorkflowOutcome::ContinuedAsNew { input };
                }
                WorkflowOutcome::Suspended { commands }
            }
        }
    }
    .instrument(span)
    .await
}

/// Run a workflow function through replay and live execution with shared state.
///
/// Returns a triple of `(outcome, pending_commands, span_handle)`:
/// - `outcome`: the workflow's terminal or suspended state.
/// - `pending_commands`: commands emitted during a `Completed` or `Failed` run
///   that the worker must persist before recording the terminal event. This is
///   non-empty only when the workflow invoked `execute_admitted_update` in live
///   mode — the `RecordUpdateResult` commands must be appended to history before
///   `WorkflowCompleted`/`WorkflowFailed`. For `Suspended` outcomes the commands
///   are already carried inside the variant; this Vec will be empty.
/// - `span_handle`: the open `harvest.workflow.execute` span. The caller should
///   hold it alive while persisting producer-side side-effects (activity
///   schedules, child workflow starts) so those producer spans are nested inside
///   the executor cycle. Dropping the handle closes the span.
pub async fn run_workflow_with_state(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    run_workflow_with_state_and_history_policy(
        exec_id,
        history,
        handler,
        input,
        state,
        WorkflowHistoryPolicy::default(),
        span_meta,
        &[],
        &[],
    )
    .await
}

/// Like [`run_workflow_with_state`] but installs explicit history guardrails,
/// workflow name, and payload size caps into the [`WorkflowContext`].
#[allow(clippy::too_many_arguments)]
pub async fn run_workflow_with_state_and_history_policy(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    history_policy: WorkflowHistoryPolicy,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
    declarative_query_handlers: &[&QueryHandlerInfo],
    declarative_update_handlers: &[&UpdateHandlerInfo],
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    run_workflow_with_state_history_policy_and_caps(
        exec_id,
        history,
        handler,
        input,
        state,
        history_policy,
        span_meta,
        declarative_query_handlers,
        declarative_update_handlers,
        "",
        crate::builder::DEFAULT_MAX_ACTIVITY_INPUT_BYTES,
        crate::builder::DEFAULT_MAX_SIGNAL_PAYLOAD_BYTES,
        crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES,
        crate::context::DEFAULT_CURRENT_DETAILS_CAP_BYTES,
        std::collections::HashMap::new(),
        None,
    )
    .await
}

/// Full executor entry point used by the worker, which injects the workflow name
/// and payload size caps configured on the `BuiltHarvest` instance.
#[allow(clippy::too_many_arguments, clippy::implicit_hasher)]
pub async fn run_workflow_with_state_history_policy_and_caps(
    exec_id: ExecutionId,
    history: Vec<WorkflowEvent>,
    handler: WorkflowHandlerFn,
    input: Value,
    state: SharedState,
    history_policy: WorkflowHistoryPolicy,
    span_meta: Option<&WorkflowExecuteSpanMeta>,
    declarative_query_handlers: &[&QueryHandlerInfo],
    declarative_update_handlers: &[&UpdateHandlerInfo],
    workflow_name: &str,
    max_activity_input_bytes: u64,
    max_signal_payload_bytes: u64,
    max_workflow_input_bytes: u64,
    max_current_details_bytes: usize,
    context_headers: std::collections::HashMap<String, String>,
    payload_offload_threshold: Option<u64>,
) -> (WorkflowOutcome, Vec<WorkflowCommand>, tracing::Span) {
    let ctx = WorkflowContext::for_replay_with_state_and_history_policy(
        exec_id,
        history,
        state,
        history_policy,
    )
    .with_workflow_name(workflow_name)
    .with_workflow_id(span_meta.map_or("", |m| m.workflow_id.as_str()))
    .with_build_id(span_meta.and_then(|m| m.build_id.clone()))
    .with_payload_caps(
        max_activity_input_bytes,
        0,
        max_signal_payload_bytes,
        max_workflow_input_bytes,
    )
    .with_current_details_cap(max_current_details_bytes)
    .with_payload_offload_threshold(payload_offload_threshold)
    .with_context_headers(context_headers);

    // Auto-register declarative handlers before any workflow code runs.
    // This satisfies the AC: "authors do not call ctx.register_*_handler in
    // their workflow body; the runtime guarantees registration happens first."
    for h in declarative_query_handlers {
        ctx.register_declarative_query_handler(h);
    }
    for h in declarative_update_handlers {
        ctx.register_declarative_update_handler(h);
    }

    // ADR-0001 §2.1: emit harvest.workflow.execute for every executor cycle.
    // harvest.replay defaults to false at span creation so subscribers that only
    // observe on_new_span (e.g. tests) see the correct value for callers that
    // don't supply span_meta. The worker passes span_meta to override it and to
    // populate the Empty fields (workflow.id, shard.id, queue) that only the
    // worker context knows.
    let span = tracing::info_span!(
        "harvest.workflow.execute",
        "otel.kind" = "internal",
        { ATTR_EXECUTION_ID } = %exec_id,
        { ATTR_REPLAY } = false,
        { ATTR_WORKFLOW_ID } = tracing::field::Empty,
        { ATTR_SHARD_ID } = tracing::field::Empty,
        { ATTR_QUEUE } = tracing::field::Empty,
        "link.traceparent" = tracing::field::Empty,
    );
    if let Some(meta) = span_meta {
        span.record(ATTR_REPLAY, meta.is_replay);
        span.record(ATTR_WORKFLOW_ID, meta.workflow_name.as_str());
        span.record(ATTR_SHARD_ID, meta.shard_id);
        span.record(ATTR_QUEUE, meta.queue_name.as_str());
        if let Some(link) = meta.link_traceparent.as_deref() {
            span.record("link.traceparent", link);
        }
    }

    // Clone the span handle BEFORE passing ownership to .instrument().
    // The clone keeps the ref-count above zero after .instrument() exits so the
    // OTel span is not ended until the caller explicitly drops the returned handle.
    // This allows caller-side producer spans (activity.schedule,
    // child_workflow.start) to be created as children of this span even though
    // the instrumented future has already completed.
    let span_handle = span.clone();

    let (outcome, pending) = async {
        // Run the handler with a timeout. If it completes, we get the result.
        // If it blocks on a oneshot (suspended), the timeout fires and we drain
        // the accumulated commands.
        let timeout_result = tokio::time::timeout(SUSPENSION_TIMEOUT, handler(&ctx, input)).await;

        match timeout_result {
            // Handler completed within the timeout window.  Drain any commands
            // emitted during live execution (e.g. RecordUpdateResult from
            // execute_admitted_update) so the worker can persist them before the
            // terminal WorkflowCompleted/WorkflowFailed event.
            Ok(Ok(output)) => {
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have absorbed a replay divergence and recorded it as a
                // deferred non-determinism error (issue #384). Surface it as a
                // failure rather than letting the workflow complete silently.
                let details = ctx.take_nd_details();
                let outcome = ctx.take_deferred_nd_error().map_or_else(
                    || WorkflowOutcome::Completed { output },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    },
                );
                (outcome, ctx.drain_commands())
            }
            // A primitive may have drifted before the workflow returned Err from
            // its own logic; prefer the non-determinism error (issue #384).
            Ok(Err(error)) => {
                let details = ctx.take_nd_details();
                let outcome = ctx.take_deferred_nd_error().map_or(
                    WorkflowOutcome::Failed {
                        error,
                        non_deterministic_details: details.clone(),
                    },
                    |nd| WorkflowOutcome::Failed {
                        error: format!("non-deterministic replay: {nd}"),
                        non_deterministic_details: details,
                    },
                );
                (outcome, ctx.drain_commands())
            }

            // Timeout elapsed -- the handler is suspended on a oneshot channel.
            // Drain the commands it emitted before suspending. RecordUpdateResult
            // commands emitted in this cycle are included in the commands list and
            // will be handled by the worker alongside the suspension side-effects.
            Err(_elapsed) => {
                // A plain-value built-in primitive (system_now/new_uuid/random_*)
                // may have recorded a divergence before the workflow parked on an
                // await point. Fail the execution now rather than suspending from
                // a non-deterministic state (issue #384).
                if let Some(nd) = ctx.take_deferred_nd_error() {
                    let details = ctx.take_nd_details();
                    return (
                        WorkflowOutcome::Failed {
                            error: format!("non-deterministic replay: {nd}"),
                            non_deterministic_details: details,
                        },
                        ctx.drain_commands(),
                    );
                }
                let mut commands = ctx.drain_commands();
                // ContinueAsNew is terminal: when the workflow body parks on
                // the dedicated suspension future, the latest command in the
                // drain is the ContinueAsNew the user requested. Bookkeeping
                // commands earlier in the drain (e.g. RecordMarker, side_effect)
                // are returned as pending_cmds so the worker can still apply
                // any UpsertSearchAttributes patches before sealing the execution.
                if let Some(idx) = commands
                    .iter()
                    .rposition(|cmd| matches!(cmd, WorkflowCommand::ContinueAsNew { .. }))
                    && let WorkflowCommand::ContinueAsNew { input } = commands.swap_remove(idx)
                {
                    return (WorkflowOutcome::ContinuedAsNew { input }, commands);
                }
                (WorkflowOutcome::Suspended { commands }, vec![])
            }
        }
    }
    .instrument(span)
    .await;

    (outcome, pending, span_handle)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::WorkflowEvent;
    use crate::types::{ActivityExecId, ExecutionId};
    use chrono::Utc;
    use std::pin::Pin;

    /// A trivial workflow that just returns its input.
    fn echo_workflow<'a>(
        _ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Ok(input) })
    }

    /// A workflow that always fails.
    fn failing_workflow<'a>(
        _ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move { Err("something went wrong".to_string()) })
    }

    /// A workflow that captures a side-effect (drifts against history) and then
    /// returns Err from its own logic.
    fn drift_then_error_workflow<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let _ = ctx.system_now(); // diverges from the recorded activity event
            Err("business rule violated".to_string())
        })
    }

    /// A workflow that calls an activity (will suspend if not in history).
    fn activity_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let result = ctx
                .execute_activity_raw("send_email", input, "default")
                .await
                .map_err(|e| e.to_string())?;
            Ok(result)
        })
    }

    #[tokio::test]
    async fn executor_replays_completed_workflow() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"greeting": "hello"});

        // Full history: workflow started and the echo handler completes immediately.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, echo_workflow, input.clone()).await;

        match outcome {
            WorkflowOutcome::Completed { output } => {
                assert_eq!(output, input);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_returns_failed_for_erroring_workflow() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, failing_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { error, .. } => {
                assert!(error.contains("something went wrong"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_prefers_deferred_drift_over_workflow_error() {
        // Regression (issue #384): a primitive that drifts before the workflow
        // returns Err from its own logic must surface as non-determinism rather
        // than masquerading as an ordinary workflow failure.
        let exec_id = ExecutionId::new();
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: Value::Null,
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            // The workflow calls system_now() here, but history recorded an
            // activity — a genuine divergence.
            WorkflowEvent::ActivityScheduled {
                activity_id: ActivityExecId::new(),
                name: "some_activity".into(),
                input: Value::Null,
                queue: "default".into(),
            },
        ];

        let outcome = run_workflow(exec_id, history, drift_then_error_workflow, Value::Null).await;

        match outcome {
            WorkflowOutcome::Failed { error, .. } => {
                assert!(
                    error.contains("non-deterministic replay"),
                    "drift must win over the workflow's own error: {error}"
                );
                assert!(
                    !error.contains("business rule violated"),
                    "the workflow's Err must not mask the drift: {error}"
                );
            }
            other => panic!("expected Failed(non-determinism), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_suspends_on_new_activity() {
        let exec_id = ExecutionId::new();
        let input = serde_json::json!({"to": "alice@example.com"});

        // History has only WorkflowStarted -- no activity events.
        // The workflow will call execute_activity_raw which will emit a
        // ScheduleActivity command and block on the oneshot.
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: input.clone(),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                assert_eq!(commands.len(), 1, "expected exactly one command");
                assert!(
                    matches!(&commands[0], WorkflowCommand::ScheduleActivity { name, .. } if name == "send_email"),
                    "expected ScheduleActivity command for send_email"
                );
            }
            other => panic!("expected Suspended, got {other:?}"),
        }
    }

    /// A workflow that triggers `continue_as_new` mid-flight.
    fn continue_as_new_workflow<'a>(
        ctx: &'a WorkflowContext,
        input: Value,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            // The future returned by continue_as_new never resolves on its
            // own; the executor's suspension timeout drains the command and
            // surfaces it as ContinuedAsNew.
            let _ = ctx
                .continue_as_new(serde_json::json!({"prev": input}))
                .await;
            unreachable!("continue_as_new must not resolve");
        })
    }

    #[tokio::test]
    async fn executor_returns_continued_as_new_when_command_drained() {
        let exec_id = ExecutionId::new();
        let history = vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }];

        let outcome = run_workflow(
            exec_id,
            history,
            continue_as_new_workflow,
            serde_json::json!("v1"),
        )
        .await;

        match outcome {
            WorkflowOutcome::ContinuedAsNew { input } => {
                assert_eq!(input, serde_json::json!({"prev": "v1"}));
            }
            other => panic!("expected ContinuedAsNew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn executor_replays_activity_from_history() {
        let exec_id = ExecutionId::new();
        let activity_id = ActivityExecId::new();
        let input = serde_json::json!({"to": "alice@example.com"});
        let activity_output = serde_json::json!({"email_id": "msg-001"});

        // Full history with completed activity -- replay should complete.
        let history = vec![
            WorkflowEvent::WorkflowStarted {
                input: input.clone(),
                timestamp: Utc::now(),
                last_completion_result: None,
                last_error: None,
                scheduled_time: None,
            },
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name: "send_email".into(),
                input: input.clone(),
                queue: "default".into(),
            },
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output: activity_output.clone(),
            },
        ];

        let outcome = run_workflow(exec_id, history, activity_workflow, input).await;

        match outcome {
            WorkflowOutcome::Completed { output } => {
                assert_eq!(output, activity_output);
            }
            other => panic!("expected Completed, got {other:?}"),
        }
    }
}
