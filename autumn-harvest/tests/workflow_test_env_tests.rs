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

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{ReplayStatus, WorkflowTestEnv};
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
