//! Tests for `WorkflowTestEnv` — the in-process workflow unit-test harness (issue #250).
//!
//! Three required example tests per AC:
//! - (a) Happy-path workflow with one activity.
//! - (b) Workflow whose first attempt fails and retry succeeds.
//! - (c) Workflow that races a timer against a signal.
//!
//! Additional tests cover per-attempt mocks, child-workflow stubbing,
//! local-activity mocking, event-log inspection, cancellation, and the
//! replay-mode self-check.
//!
//! Run with:
//!   cargo test -p autumn-harvest --test `workflow_test_env_tests` \
//!     --features testing --no-default-features

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::ExecutionId;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
use autumn_harvest::types::ParentClosePolicy;
use serde_json::{Value, json};

// ──────────────────────────── workflow helpers ────────────────────────────────

/// (a) One activity, returns its output wrapped.
fn one_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"sent": result}))
    })
}

/// (b) Explicitly retries a failing activity by calling it again on error.
fn retry_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = match ctx
            .execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
            .await
        {
            Ok(v) => v,
            Err(_first_err) => {
                // Explicit retry: call the activity again after first failure.
                ctx.execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
                    .await
                    .map_err(|e| e.to_string())?
            }
        };
        Ok(result)
    })
}

/// Publishes an operator status breadcrumb at each phase via
/// `ctx.set_current_details`, overwriting it between activities and clearing
/// it (empty string) before completing (issue #593).
fn current_details_status_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.set_current_details("step 1/2: charging card");
        let charge = ctx
            .execute_activity_raw("charge_card", json!({"amount": 100}), "payments")
            .await
            .map_err(|e| e.to_string())?;
        ctx.set_current_details("step 2/2: sending receipt");
        let receipt = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        // Clear the breadcrumb on completion.
        ctx.set_current_details("");
        Ok(json!({"charge": charge, "receipt": receipt}))
    })
}

/// (c) Races a timer against a signal; returns which path fired first.
fn race_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        tokio::select! {
            biased;
            r = ctx.timer("race", 60) => {
                r.map_err(|e| e.to_string())?;
                Ok(json!("timer"))
            }
            payload = ctx.wait_for_signal("approve") => {
                Ok(json!({"signal": payload.map_err(|e| e.to_string())?}))
            }
        }
    })
}

/// Captures a deterministic side-effect (`system_now`) BEFORE suspending on an
/// activity. Exercises that the test harness persists the pre-suspension
/// `SideEffectRecorded` event so the next replay iteration does not see drift.
fn side_effect_then_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let captured_at = ctx.system_now().timestamp_millis();
        let result = ctx
            .execute_activity_raw("send_email", json!({"to": "user@example.com"}), "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"sent": result, "captured_at": captured_at}))
    })
}

/// Two sequential activities, used for event-log ordering assertions.
fn two_step_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let a = ctx
            .execute_activity_raw("step_a", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let b = ctx
            .execute_activity_raw("step_b", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!([a, b]))
    })
}

/// Spawns a child workflow and returns its output.
fn parent_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .spawn_child_workflow_raw("child_processing", json!({"id": 42}))
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"child": result}))
    })
}

/// Uses a local activity.
fn local_activity_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let result = ctx
            .execute_local_activity_raw("compute_hash", json!([1, 2, 3]), None, None)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"hash": result}))
    })
}

/// Checks for cancellation and bails out early.
fn cancellable_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.check_cancellation().map_err(|e| e.to_string())?;
        let result = ctx
            .execute_activity_raw("long_op", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

fn terminal_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let child_id = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;
        Ok(json!({ "child_id": child_id.to_string() }))
    })
}

fn terminal_policy_detached_spawns_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let request_cancel_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_request_cancel_child",
                json!({"mode": "request_cancel"}),
                ParentClosePolicy::RequestCancel,
            )
            .map_err(|e| e.to_string())?;
        let terminate_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_terminate_child",
                json!({"mode": "terminate"}),
                ParentClosePolicy::Terminate,
            )
            .map_err(|e| e.to_string())?;
        let abandoned_child = ctx
            .spawn_child_workflow_detached_raw(
                "terminal_abandoned_child",
                json!({"mode": "abandon"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())?;

        Ok(json!({
            "request_cancel_child": request_cancel_child.to_string(),
            "terminate_child": terminate_child.to_string(),
            "abandoned_child": abandoned_child.to_string(),
        }))
    })
}

fn activity_and_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activity = ctx.execute_activity_raw("batched_work", Value::Null, "default");
        let detached = async {
            ctx.spawn_child_workflow_detached_raw(
                "batched_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())
        };
        let (activity, child_id) = tokio::join!(activity, detached);
        let activity = activity.map_err(|e| e.to_string())?;
        let child_id = child_id?;

        Ok(json!({
            "activity": activity,
            "child_id": child_id.to_string(),
        }))
    })
}

fn signal_and_detached_spawn_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let target = ExecutionId::from_uuid(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000123")
                .map_err(|e| e.to_string())?,
        );
        let signal = ctx.signal_external_workflow(target, "refresh", json!({"ok": true}));
        let detached = async {
            ctx.spawn_child_workflow_detached_raw(
                "signal_detached_child",
                json!({"mode": "audit"}),
                ParentClosePolicy::Abandon,
            )
            .map_err(|e| e.to_string())
        };
        let (signal, child_id) = tokio::join!(signal, detached);
        signal.map_err(|e| e.to_string())?;
        let child_id = child_id?;

        Ok(json!({ "child_id": child_id.to_string() }))
    })
}

// ─────────────────────── (a) Happy-path test ─────────────────────────────────

#[tokio::test]
async fn test_happy_path_one_activity() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(one_activity_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"sent": "delivered"})));

    let events = outcome.events();
    assert!(
        events.iter().any(|e| matches!(e,
            WorkflowEvent::ActivityScheduled { name, .. } if name == "send_email"
        )),
        "expected ActivityScheduled for send_email"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "expected ActivityCompleted"
    );
}

#[tokio::test]
async fn test_side_effect_captured_before_suspension_is_persisted() {
    // Regression (issue #384): a deterministic primitive emitted before parking
    // on an activity must be persisted to history by the harness. If dropped,
    // the next iteration would replay system_now() against ActivityScheduled and
    // record spurious side-effect drift, failing the run.
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(side_effect_then_activity_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_ok(),
        "workflow must not fail with spurious side-effect drift: {:?}",
        outcome.result
    );
    assert_eq!(outcome.result.as_ref().unwrap()["sent"], json!("delivered"));

    let events = outcome.events();
    // The SideEffectRecorded event must be persisted, and ahead of the activity.
    let se_idx = events
        .iter()
        .position(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .expect("SideEffectRecorded must be persisted in history");
    let act_idx = events
        .iter()
        .position(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "send_email"),
        )
        .expect("ActivityScheduled must be present");
    assert!(
        se_idx < act_idx,
        "side effect must be recorded before the activity it precedes"
    );
    // Exactly one capture — not duplicated across replay iterations.
    let se_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::SideEffectRecorded { .. }))
        .count();
    assert_eq!(se_count, 1, "side effect must be recorded exactly once");
}

// ─────────────────────── (b) Retry test ──────────────────────────────────────

#[tokio::test]
async fn test_first_attempt_fails_second_succeeds() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_attempt("charge_card", 1, Err("transient gateway error".into()))
        .mock_activity_attempt("charge_card", 2, Ok(json!({"status": "charged"})))
        .run(retry_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"status": "charged"})));

    let events = outcome.events();
    let scheduled_count = events
        .iter()
        .filter(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "charge_card"),
        )
        .count();
    assert_eq!(
        scheduled_count, 2,
        "expected two ActivityScheduled events (one failed, one succeeded)"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "expected an ActivityFailed event for the first attempt"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. })),
        "expected an ActivityCompleted event for the second attempt"
    );
}

// ─────────────────────── (c) Timer vs signal race ────────────────────────────

#[tokio::test]
async fn test_timer_wins_when_no_signal_queued() {
    let outcome = WorkflowTestEnv::new().run(race_workflow, json!(null)).await;

    assert_eq!(outcome.result, Ok(json!("timer")));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerStarted { .. })),
        "expected TimerStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "signal must not be in events when timer wins"
    );
}

#[tokio::test]
async fn test_signal_wins_when_queued() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approve", json!("green-light"))
        .run(race_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"signal": "green-light"})));

    let events = outcome.events();
    assert!(
        events.iter().any(|e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "approve")),
        "expected SignalReceived(approve)"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "timer must not fire when signal wins"
    );
}

// ──────────────────── Child-workflow stubbing ─────────────────────────────────

#[tokio::test]
async fn test_child_workflow_stub() {
    let outcome = WorkflowTestEnv::new()
        .mock_child_workflow("child_processing", |input| {
            let id = input["id"].as_i64().unwrap_or(0);
            Ok(json!({"processed": id * 2}))
        })
        .run(parent_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"child": {"processed": 84}})));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowStarted { .. })),
        "expected ChildWorkflowStarted"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ChildWorkflowCompleted { .. })),
        "expected ChildWorkflowCompleted"
    );
}

#[tokio::test]
async fn test_terminal_detached_spawn_records_history_event() {
    let outcome = WorkflowTestEnv::new()
        .run(terminal_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    assert!(
        events.iter().any(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached {
                    workflow_name,
                    input,
                    parent_close_policy,
                    ..
                } if workflow_name == "terminal_detached_child"
                    && input == &json!({"mode": "audit"})
                    && *parent_close_policy == ParentClosePolicy::Abandon
            )
        }),
        "terminal detached spawns must be recorded before WorkflowCompleted: {events:?}"
    );
    let spawn_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ChildWorkflowSpawnedDetached { .. }))
        .expect("spawn event should exist");
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("workflow completed event should exist");
    assert!(
        spawn_pos < completed_pos,
        "detached spawn should precede terminal event: {events:?}"
    );
}

#[tokio::test]
async fn test_terminal_detached_spawn_records_parent_close_cascades() {
    let outcome = WorkflowTestEnv::new()
        .run(terminal_policy_detached_spawns_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::WorkflowCompleted { .. }))
        .expect("workflow completed event should exist");
    let request_cancel_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_request_cancel_child" => Some(*child_id),
            _ => None,
        })
        .expect("request-cancel detached spawn should exist");
    let terminate_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_terminate_child" => Some(*child_id),
            _ => None,
        })
        .expect("terminate detached spawn should exist");
    let abandoned_child = events
        .iter()
        .find_map(|event| match event {
            WorkflowEvent::ChildWorkflowSpawnedDetached {
                child_id,
                workflow_name,
                ..
            } if workflow_name == "terminal_abandoned_child" => Some(*child_id),
            _ => None,
        })
        .expect("abandoned detached spawn should exist");

    let cascades = events
        .iter()
        .enumerate()
        .filter_map(|(pos, event)| match event {
            WorkflowEvent::ChildWorkflowCascadeApplied {
                child_id,
                policy,
                action,
            } => Some((pos, *child_id, *policy, action.as_str())),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        cascades.len(),
        2,
        "only non-Abandon detached children should cascade: {events:?}"
    );
    assert!(cascades.iter().all(|(pos, ..)| *pos > completed_pos));
    assert!(cascades.iter().any(|(_, child_id, policy, action)| {
        *child_id == request_cancel_child
            && *policy == ParentClosePolicy::RequestCancel
            && *action == "request_cancel"
    }));
    assert!(cascades.iter().any(|(_, child_id, policy, action)| {
        *child_id == terminate_child
            && *policy == ParentClosePolicy::Terminate
            && *action == "terminate"
    }));
    assert!(
        !cascades
            .iter()
            .any(|(_, child_id, _, _)| *child_id == abandoned_child),
        "Abandon detached child should not receive a cascade event: {events:?}"
    );
}

#[tokio::test]
async fn test_detached_spawn_is_recorded_before_activity_terminal_in_batch() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("batched_work", |_| Ok(json!({"ok": true})))
        .run(activity_and_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let scheduled_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ActivityScheduled { name, .. } if name == "batched_work"
            )
        })
        .expect("activity should be scheduled");
    let detached_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached { workflow_name, .. }
                    if workflow_name == "batched_detached_child"
            )
        })
        .expect("detached child spawn should be recorded");
    let completed_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ActivityCompleted { .. }))
        .expect("activity should complete");

    assert!(
        scheduled_pos < detached_pos && detached_pos < completed_pos,
        "detached spawn should be recorded between activity schedule and terminal: {events:?}"
    );
}

#[tokio::test]
async fn test_signal_terminal_is_recorded_before_detached_spawn_in_batch() {
    let outcome = WorkflowTestEnv::new()
        .run(signal_and_detached_spawn_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok());
    let events = outcome.events();
    let requested_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ExternalSignalRequested { .. }))
        .expect("external signal request should be recorded");
    let delivered_pos = events
        .iter()
        .position(|event| matches!(event, WorkflowEvent::ExternalSignalDelivered { .. }))
        .expect("external signal delivery should be recorded");
    let detached_pos = events
        .iter()
        .position(|event| {
            matches!(
                event,
                WorkflowEvent::ChildWorkflowSpawnedDetached { workflow_name, .. }
                    if workflow_name == "signal_detached_child"
            )
        })
        .expect("detached child spawn should be recorded");

    assert!(
        requested_pos < delivered_pos && delivered_pos < detached_pos,
        "test history must mirror production mixed-signal order: {events:?}"
    );
}

// ──────────────────── Event-log inspection ───────────────────────────────────

#[tokio::test]
async fn test_event_log_ordering() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("step_a", |_| Ok(json!("a")))
        .mock_activity("step_b", |_| Ok(json!("b")))
        .run(two_step_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!(["a", "b"])));

    let activity_names: Vec<&str> = outcome
        .events()
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        activity_names,
        ["step_a", "step_b"],
        "activities must appear in order"
    );
}

// ──────────────────── Local-activity mocking ─────────────────────────────────

#[tokio::test]
async fn test_local_activity_mock() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("compute_hash", |input| {
            let sum: i64 = input
                .as_array()
                .map_or(&[][..], Vec::as_slice)
                .iter()
                .filter_map(|v: &Value| v.as_i64())
                .sum();
            Ok(json!(sum))
        })
        .run(local_activity_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"hash": 6})));

    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityScheduled { .. })),
        "expected LocalActivityScheduled"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::LocalActivityCompleted { .. })),
        "expected LocalActivityCompleted"
    );
}

// ──────────────────── Missing stub → loud failure ────────────────────────────

#[tokio::test]
async fn test_missing_stub_fails_loudly() {
    let outcome = WorkflowTestEnv::new()
        // Intentionally omit the mock for "send_email"
        .run(one_activity_workflow, json!(null))
        .await;

    // The workflow should fail because no mock is registered.
    assert!(
        outcome.result.is_err(),
        "missing stub must cause an error result, not a panic"
    );
    let err = outcome.result.unwrap_err();
    assert!(
        err.contains("send_email"),
        "error should name the unregistered activity; got: {err}"
    );
}

// ──────────────────── Replay self-check ──────────────────────────────────────

#[tokio::test]
async fn test_replay_self_check_succeeds_for_deterministic_workflow() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(one_activity_workflow, json!(null))
        .await;

    // The test env records a real history; replaying through WorkflowReplayer
    // must succeed, proving the workflow is deterministic.
    let report = outcome.replay_check(one_activity_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay self-check failed:\n{report}"
    );
}

// ──────────────── set_current_details status breadcrumb (issue #593) ─────────

#[tokio::test]
async fn test_current_details_set_overwrite_clear_leaves_no_event_footprint() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("charge_card", |_| Ok(json!({"status": "charged"})))
        .mock_activity("send_email", |_| Ok(json!("delivered")))
        .run(current_details_status_workflow, json!(null))
        .await;

    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    // set_current_details (initial set, overwrite, and a trailing empty-string
    // clear) must leave zero footprint in harvest_events -- only the two
    // activities' events are recorded, exactly as if the calls were never made.
    let events = outcome.events();
    let scheduled = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. }))
        .count();
    let completed = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }))
        .count();
    assert_eq!(
        scheduled, 2,
        "expected exactly two ActivityScheduled events"
    );
    assert_eq!(
        completed, 2,
        "expected exactly two ActivityCompleted events"
    );

    let report = outcome.replay_check(current_details_status_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "set_current_details calls (set, overwrite, and empty-string clear) \
         must replay deterministically:\n{report}"
    );
}

// ──────────────────── Cancellation injection ─────────────────────────────────

#[tokio::test]
async fn test_cancellation_injection() {
    let outcome = WorkflowTestEnv::new()
        .with_cancellation("operator requested")
        .run(cancellable_workflow, json!(null))
        .await;

    // Workflow bails out at check_cancellation() before reaching the activity.
    assert!(
        outcome.result.is_err(),
        "cancelled workflow must return Err"
    );
    assert!(
        outcome.result.as_ref().unwrap_err().contains("cancelled"),
        "error should indicate cancellation"
    );
    let events = outcome.events();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::WorkflowCancelled { .. })),
        "expected WorkflowCancelled in the event log"
    );
    assert!(
        !events
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityScheduled { .. })),
        "activity must not be scheduled after cancellation"
    );
}

// ──────────────────── env.now() ─────────────────────────────────────────────

#[tokio::test]
async fn test_simulated_time_is_stable() {
    let env = WorkflowTestEnv::new();
    let t1 = env.now();
    let t2 = env.now();
    assert_eq!(t1, t2, "simulated time must be deterministic");
}

// ──────────────────── with_state ────────────────────────────────────────────

struct CounterConfig {
    multiplier: i64,
}

fn stateful_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let mult = ctx.state::<CounterConfig>().map_or(1, |c| c.multiplier);
        let v = input.as_i64().unwrap_or(0);
        Ok(json!(v * mult))
    })
}

#[tokio::test]
async fn test_shared_state_injection() {
    let outcome = WorkflowTestEnv::new()
        .with_state(CounterConfig { multiplier: 7 })
        .run(stateful_workflow, json!(3))
        .await;

    assert_eq!(outcome.result, Ok(json!(21)));
}

// ──────────────────── Parameterised retry-policy edge cases ─────────────────

/// A workflow that keeps retrying until success or exhaustion.
fn max_retries_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        const MAX: usize = 3;
        for attempt in 1..=MAX {
            match ctx
                .execute_activity_raw("flaky_op", json!({"attempt": attempt}), "q")
                .await
            {
                Ok(v) => return Ok(v),
                Err(_) if attempt < MAX => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        unreachable!()
    })
}

#[tokio::test]
async fn test_all_attempts_fail_returns_error() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("flaky_op", |_| Err("always broken".into()))
        .run(max_retries_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "exhausted retries should return Err"
    );
}

#[tokio::test]
async fn test_third_attempt_succeeds() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_attempt("flaky_op", 1, Err("fail1".into()))
        .mock_activity_attempt("flaky_op", 2, Err("fail2".into()))
        .mock_activity_attempt("flaky_op", 3, Ok(json!("finally")))
        .run(max_retries_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!("finally")));
    let scheduled = outcome
        .events()
        .iter()
        .filter(
            |e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "flaky_op"),
        )
        .count();
    assert_eq!(scheduled, 3, "expected exactly 3 scheduled events");
}

// ─────────────── mock_activity_retries (worker-level retry sequences) ─────────

/// Workflow that calls one activity a single time, expecting the worker to
/// handle retries transparently.
fn single_call_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.execute_activity_raw("resilient_op", json!(null), "default")
            .await
            .map_err(|e| e.to_string())
    })
}

#[tokio::test]
async fn test_mock_activity_retries_succeeds_on_third_worker_attempt() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("transient_1".into()),
                Err("transient_2".into()),
                Ok(json!({"status": "ok"})),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"status": "ok"})));
}

#[tokio::test]
async fn test_mock_activity_retries_all_fail_returns_error() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![Err("err1".into()), Err("err2".into()), Err("err3".into())],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "all-fail retry sequence should propagate error to workflow"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_history_has_one_scheduled_event() {
    // Worker-level retries are transparent: one execute_activity_raw call →
    // one ActivityScheduled, but multiple ActivityFailed + ActivityCompleted.
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("resilient_op", vec![Err("fail".into()), Ok(json!("done"))])
        .run(single_call_workflow, json!(null))
        .await;

    let scheduled_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityScheduled { name, .. } if name == "resilient_op"))
        .count();
    assert_eq!(
        scheduled_count, 1,
        "worker-level retries must share one ActivityScheduled event"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_history_has_no_intermediate_failures() {
    // Non-terminal retries do not produce ActivityFailed events in the real
    // worker (requeue_for_retry is called instead), so the test harness must
    // match: only the terminal outcome writes ActivityFailed.
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("transient_1".into()),
                Err("transient_2".into()),
                Ok(json!("done")),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    let events = outcome.events();

    // No ActivityFailed at all — the sequence ends in ActivityCompleted.
    let failed_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(
        failed_count, 0,
        "intermediate retries must not write ActivityFailed to history"
    );

    // Terminal ActivityCompleted event is present.
    let completed = events
        .iter()
        .any(|e| matches!(e, WorkflowEvent::ActivityCompleted { .. }));
    assert!(completed, "expected a final ActivityCompleted event");

    // Three ActivityStarted events (one per attempt) are present.
    let started_count = events
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityStarted { .. }))
        .count();
    assert_eq!(started_count, 3, "expected one ActivityStarted per attempt");
}

#[tokio::test]
async fn test_mock_activity_retries_all_fail_terminal_failure_is_non_retryable() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries(
            "resilient_op",
            vec![
                Err("fail_1".into()),
                Err("fail_2".into()),
                Err("fail_3".into()),
            ],
        )
        .run(single_call_workflow, json!(null))
        .await;

    assert!(
        outcome.result.is_err(),
        "exhausted retry sequence must fail"
    );

    // Exactly one ActivityFailed event — the terminal failure — with
    // non_retryable: false, matching production behaviour for plain
    // Err(String) payloads (exhaustion is determined by the retry policy,
    // not the non_retryable flag on the event).
    let terminal_failure_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(
        terminal_failure_count, 1,
        "exactly one ActivityFailed event expected when all attempts fail"
    );
}

#[tokio::test]
async fn test_mock_activity_retries_single_ok_returns_success() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity_retries("resilient_op", vec![Ok(json!("immediate"))])
        .run(single_call_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!("immediate")));

    // No intermediate failures when the first attempt succeeds.
    let failed_count = outcome
        .events()
        .iter()
        .filter(|e| matches!(e, WorkflowEvent::ActivityFailed { .. }))
        .count();
    assert_eq!(failed_count, 0);
}

// ─────────── receive_signal_timeout / wait_for_signal_timeout (issue #476) ───────────

/// Awaits an approval signal with a deadline; escalates to auto-reject on timeout.
fn approval_with_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let decision: Option<Value> = ctx
            .receive_signal_timeout("approval", std::time::Duration::from_secs(3600))
            .await
            .map_err(|e| e.to_string())?;
        Ok(decision.map_or_else(
            || json!({"outcome": "auto_rejected"}),
            |payload| json!({"outcome": "decided", "payload": payload}),
        ))
    })
}

#[tokio::test]
async fn test_receive_signal_timeout_signal_branch() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approval", json!({"approved": true}))
        .run(approval_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"outcome": "decided", "payload": {"approved": true}}))
    );
    assert!(
        outcome.events().iter().any(
            |e| matches!(e, WorkflowEvent::SignalReceived { signal_name, .. } if signal_name == "approval")
        ),
        "expected SignalReceived(approval)"
    );

    let report = outcome.replay_check(approval_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "signal branch must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_receive_signal_timeout_timeout_branch() {
    // No signal queued — the stubbed timer fires immediately (no real sleeping)
    // and the workflow takes the timeout/escalation branch.
    let outcome = WorkflowTestEnv::new()
        .run(approval_with_timeout_workflow, json!(null))
        .await;

    assert_eq!(outcome.result, Ok(json!({"outcome": "auto_rejected"})));
    assert!(
        outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::TimerFired { .. })),
        "expected TimerFired for the deadline timer"
    );
    assert!(
        !outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::SignalReceived { .. })),
        "no signal must be recorded on the timeout branch"
    );

    let report = outcome.replay_check(approval_with_timeout_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timeout branch must replay deterministically:\n{report}"
    );
}

// ── issue #488: last_completion_result / last_error carryover (unit) ─────────

fn carryover_reader_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let cursor: Option<Value> = ctx
            .last_completion_result::<Value>()
            .map_err(|e| e.to_string())?;
        let err: Option<String> = ctx.last_error();
        Ok(json!({"cursor": cursor, "last_error": err}))
    })
}

/// No carryover seeded → both accessors return None.
#[tokio::test]
async fn test_last_completion_result_none_on_first_run() {
    let outcome = WorkflowTestEnv::new()
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"cursor": null, "last_error": null}))
    );
}

/// Seeded carryover is returned by `last_completion_result` and `last_error` returns None.
#[tokio::test]
async fn test_last_completion_result_seeded_value() {
    let cursor = json!({"processed_at": "2026-06-14T00:00:00Z", "cursor": 42});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(cursor.clone())
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({"cursor": cursor, "last_error": null}))
    );
}

/// Seeded `last_error` is returned; `last_completion_result` still shows the prior value.
#[tokio::test]
async fn test_last_error_seeded_value() {
    let prior_result = json!({"cursor": 10});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(prior_result.clone())
        .with_last_error("upstream timed out")
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({
            "cursor": prior_result,
            "last_error": "upstream timed out"
        }))
    );
}

/// Replay determinism: the seeded carryover must replay identically.
#[tokio::test]
async fn test_last_completion_result_replays_deterministically() {
    let cursor = json!({"cursor": 99});
    let outcome = WorkflowTestEnv::new()
        .with_last_completion_result(cursor)
        .run(carryover_reader_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    let report = outcome.replay_check(carryover_reader_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "carryover must replay deterministically:\n{report}"
    );
}

// ── issue #508: scheduled_time accessor (unit) ────────────────────────────────

fn scheduled_time_reader_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let slot: Option<String> = ctx.scheduled_time().map(|t| t.to_rfc3339());
        Ok(json!({ "scheduled_time": slot }))
    })
}

/// No `scheduled_time` seeded → accessor returns None.
#[tokio::test]
async fn test_scheduled_time_none_by_default() {
    let outcome = WorkflowTestEnv::new()
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    assert_eq!(outcome.result, Ok(json!({ "scheduled_time": null })));
}

/// Seeded `scheduled_time` is returned by the accessor.
#[tokio::test]
async fn test_scheduled_time_seeded_value() {
    use chrono::{TimeZone as _, Utc};
    let slot = Utc.with_ymd_and_hms(2026, 3, 15, 0, 0, 0).unwrap();
    let outcome = WorkflowTestEnv::new()
        .with_scheduled_time(slot)
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    match outcome.result {
        Ok(v) => {
            let s = v["scheduled_time"].as_str().expect("expected a string");
            assert!(s.contains("2026-03-15"), "slot round-trip, got: {s}");
        }
        Err(e) => panic!("run failed: {e}"),
    }
}

/// Replay determinism: the seeded `scheduled_time` replays identically.
#[tokio::test]
async fn test_scheduled_time_replays_deterministically() {
    use chrono::{TimeZone as _, Utc};
    let slot = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
    let outcome = WorkflowTestEnv::new()
        .with_scheduled_time(slot)
        .run(scheduled_time_reader_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);

    let report = outcome.replay_check(scheduled_time_reader_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "scheduled_time must replay deterministically:\n{report}"
    );
}

// ── issue #526: virtual clock advancement when durable timers fire ─────────────

fn clock_after_timer_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.timer("sleep30d", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        Ok(json!({ "t0": t0, "t1": t1, "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_timer_advances_virtual_clock() {
    let outcome = WorkflowTestEnv::new()
        .run(clock_after_timer_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let diff_secs = v["diff_secs"].as_i64().expect("diff_secs must be integer");
    let expected = 30_i64 * 24 * 3600;
    assert_eq!(
        diff_secs, expected,
        "ctx.now() must advance by 30 days after timer fires"
    );
}

fn two_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.timer("hour1", 3600).await.map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        ctx.timer("hour2", 7200).await.map_err(|e| e.to_string())?;
        let t2 = ctx.now().timestamp();
        Ok(json!({ "t0": t0, "t1": t1, "t2": t2 }))
    })
}

#[tokio::test]
async fn test_sequential_timers_accumulate() {
    let outcome = WorkflowTestEnv::new()
        .run(two_timers_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let t0 = v["t0"].as_i64().unwrap();
    let t1 = v["t1"].as_i64().unwrap();
    let t2 = v["t2"].as_i64().unwrap();
    assert_eq!(t1 - t0, 3600, "first timer must advance clock by 1h");
    assert_eq!(
        t2 - t0,
        3600 + 7200,
        "second timer must accumulate to 3h total"
    );
}

fn activity_then_now_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        ctx.execute_activity_raw("noop", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
        let t1 = ctx.now().timestamp();
        Ok(json!({ "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_activities_do_not_advance_clock() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("noop", |_| Ok(json!(null)))
        .run(activity_then_now_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    let diff_secs = v["diff_secs"].as_i64().unwrap();
    assert_eq!(
        diff_secs, 0,
        "activities must not advance the virtual clock"
    );
}

fn signal_or_timer_now_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t0 = ctx.now().timestamp();
        let won = match ctx
            .wait_for_signal_timeout("approve", std::time::Duration::from_secs(30 * 24 * 3600))
            .await
        {
            Ok(Some(_)) => "signal",
            Ok(None) => "timer",
            Err(e) => return Err(e.to_string()),
        };
        let t1 = ctx.now().timestamp();
        Ok(json!({ "won": won, "diff_secs": t1 - t0 }))
    })
}

#[tokio::test]
async fn test_signal_preempts_timer_no_advance() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approve", json!("go"))
        .run(signal_or_timer_now_workflow, json!(null))
        .await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.unwrap();
    assert_eq!(v["won"], json!("signal"), "signal must have won");
    let diff_secs = v["diff_secs"].as_i64().unwrap();
    assert_eq!(
        diff_secs, 0,
        "signal-preempted timer must not advance the clock"
    );
}

fn billing_loop_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let t_start = ctx.now().timestamp();
        ctx.timer("cycle1", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t_after_first = ctx.now().timestamp();
        ctx.timer("cycle2", 30 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        let t_after_second = ctx.now().timestamp();
        Ok(json!({
            "t_start": t_start,
            "t_after_first": t_after_first,
            "t_after_second": t_after_second,
        }))
    })
}

#[tokio::test]
async fn test_billing_loop_dates_and_elapsed() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let outcome = env.run(billing_loop_workflow, json!(null)).await;
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    let v = outcome.result.as_ref().unwrap();

    let t_start = v["t_start"].as_i64().unwrap();
    let t_after_first = v["t_after_first"].as_i64().unwrap();
    let t_after_second = v["t_after_second"].as_i64().unwrap();

    assert_eq!(
        t_after_first - t_start,
        30 * 24 * 3600_i64,
        "first 30d advance"
    );
    assert_eq!(
        t_after_second - t_start,
        60 * 24 * 3600_i64,
        "second 30d accumulates to 60d"
    );

    let sixty_days = chrono::Duration::days(60);
    assert_eq!(
        outcome.final_now(),
        start + sixty_days,
        "outcome.final_now() must be start + 60 days"
    );
    assert_eq!(
        outcome.elapsed(),
        sixty_days,
        "outcome.elapsed() must be 60 days"
    );
    assert_eq!(
        outcome.final_now().timestamp(),
        t_after_second,
        "outcome.final_now() must match workflow-observed ctx.now()"
    );
}

fn year_of_timers_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        for i in 0..12_u32 {
            ctx.timer(&format!("month{i}"), 30 * 24 * 3600)
                .await
                .map_err(|e| e.to_string())?;
        }
        ctx.timer("extra5d", 5 * 24 * 3600)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!(null))
    })
}

#[tokio::test]
async fn test_365_days_under_50ms() {
    let env = WorkflowTestEnv::new();
    let start = env.now();
    let wall_start = std::time::Instant::now();
    let outcome = env.run(year_of_timers_workflow, json!(null)).await;
    let elapsed_wall = wall_start.elapsed();
    assert!(outcome.result.is_ok(), "run failed: {:?}", outcome.result);
    // 50ms target in release builds; 5000ms budget for unoptimized debug builds.
    let wall_budget_ms = if cfg!(debug_assertions) { 5_000 } else { 50 };
    assert!(
        elapsed_wall.as_millis() < wall_budget_ms,
        "365 days of virtual time must complete in < {wall_budget_ms}ms wall-clock, got {}ms",
        elapsed_wall.as_millis()
    );
    assert_eq!(
        outcome.final_now(),
        start + chrono::Duration::days(365),
        "outcome.final_now() must be start + 365 days"
    );
}

// ──────────────────────── ctx.race() tests (issue #600) ────────────────────────

/// Hedge two providers, cancel the loser, take the winner — the AC4 ≤5-line
/// DX example, exercised end-to-end through the no-DB test harness.
fn race_two_providers_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .activity_raw("fetch_primary", input.clone(), "default")
            .activity_raw("fetch_fallback", input, "default")
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner_index": winner.index, "value": winner.value}))
    })
}

#[tokio::test]
async fn test_race_two_activities_discards_loser_and_replays_deterministically() {
    let outcome = WorkflowTestEnv::new()
        .mock_activity("fetch_primary", |_input| Ok(json!({"provider": "primary"})))
        .mock_activity("fetch_fallback", |_input| {
            Ok(json!({"provider": "fallback"}))
        })
        .run(race_two_providers_workflow, json!({"query": "widgets"}))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 0, "value": {"provider": "primary"}}))
    );

    // The winner is durably recorded via the existing MarkerRecorded event
    // (issue #600 AC2 — no new WorkflowEvent variant).
    assert!(
        outcome.events().iter().any(
            |e| matches!(e, WorkflowEvent::MarkerRecorded { name, .. } if name == "race_winner:1")
        ),
        "expected a race_winner marker in history: {:?}",
        outcome.events()
    );
    // WorkflowTestEnv resolves every mocked ScheduleActivity synchronously
    // within the same iteration, so by the time the race is evaluated *both*
    // branches already have a real terminal in history — there is nothing
    // durably "open" left to cancel, and no synthetic "lost race" terminal is
    // (or should be) recorded. Both activities' real completions are visible
    // in history; only the non-winning one's value is discarded. The
    // still-open-loser-gets-cancelled path (a genuinely durable resource) is
    // covered by the `race_activity_replays_winner_and_records_marker_plus_cancel_losers`
    // and `race_activity_winner_is_stable_across_randomized_completion_positions`
    // unit tests in `context.rs`, which construct that history state directly.
    assert!(
        !outcome
            .events()
            .iter()
            .any(|e| matches!(e, WorkflowEvent::ActivityFailed { .. })),
        "no branch should be force-failed when both already completed: {:?}",
        outcome.events()
    );
    let fallback_completed = outcome.events().iter().any(|e| matches!(
        e,
        WorkflowEvent::ActivityCompleted { output, .. } if output == &json!({"provider": "fallback"})
    ));
    assert!(
        fallback_completed,
        "the losing branch's own real completion is still recorded in history: {:?}",
        outcome.events()
    );

    let report = outcome.replay_check(race_two_providers_workflow).await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "race must replay deterministically:\n{report}"
    );
}

/// Approval-signal-or-timeout expressed via `ctx.race()` (issue #600's other
/// headline example from the User Story) — a thin wrapper over the
/// already-shipped `receive_signal_timeout` (issue #476). `winner.index` for
/// this shape is a fixed role-based value (timer = 0, signal = 1),
/// independent of the `.signal()`/`.timer()` call order below.
fn race_approval_or_timeout_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let winner = ctx
            .race()
            .signal("approval")
            .timer(std::time::Duration::from_secs(3600))
            .run()
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({"winner_index": winner.index, "value": winner.value}))
    })
}

#[tokio::test]
async fn test_race_approval_or_timeout_signal_branch() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("approval", json!({"approved": true}))
        .run(race_approval_or_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 1, "value": {"approved": true}}))
    );

    let report = outcome
        .replay_check(race_approval_or_timeout_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "signal-branch race must replay deterministically:\n{report}"
    );
}

#[tokio::test]
async fn test_race_approval_or_timeout_timer_branch() {
    // No signal queued — the stubbed timer fires immediately.
    let outcome = WorkflowTestEnv::new()
        .run(race_approval_or_timeout_workflow, json!(null))
        .await;

    assert_eq!(
        outcome.result,
        Ok(json!({"winner_index": 0, "value": null}))
    );

    let report = outcome
        .replay_check(race_approval_or_timeout_workflow)
        .await;
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "timer-branch race must replay deterministically:\n{report}"
    );
}
