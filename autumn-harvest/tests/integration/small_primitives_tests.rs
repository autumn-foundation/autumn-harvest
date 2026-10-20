//! Replay and test-env tests for the issue #1985 primitives.
//!
//! - The payload-matching signal wait (`ctx.wait_for_signal_matching`).
//! - The durable promise (`ctx.promise`, `ctx.new_promise`).
//!
//! The `AllowAll` overlap mode has no workflow-side surface. Its replay test
//! needs a scheduler and a worker, so it is in `small_primitives_db_tests.rs`.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::durable_promise::{PromiseId, PromiseSettlement};
use autumn_harvest::event::{SideEffectKind, WorkflowEvent};
use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer, WorkflowTestEnv};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::{Value, json};

type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

fn started() -> WorkflowEvent {
    WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }
}

fn signal(name: &str, payload: Value) -> WorkflowEvent {
    WorkflowEvent::SignalReceived {
        signal_name: name.to_string(),
        payload,
    }
}

const fn completed(result: Value) -> WorkflowEvent {
    WorkflowEvent::WorkflowCompleted { output: result }
}

fn snapshot(name: &str, events: Vec<WorkflowEvent>) -> HistorySnapshot {
    HistorySnapshot {
        workflow_name: name.to_string(),
        execution_id: ExecutionId::new(),
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        start_provenance: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    }
}

fn assert_succeeded(report: &autumn_harvest::testing::ReplayReport) {
    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "expected a clean replay:\n{report}"
    );
}

// ── Payload-matching signal wait ─────────────────────────────────────────

/// Waits for the order with id 42.
fn order_42_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal_matching("order", |p| p["id"] == 42)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Waits for the order with id 41. This is a changed deploy of
/// `order_42_workflow`.
fn order_41_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        ctx.wait_for_signal_matching("order", |p| p["id"] == 41)
            .await
            .map_err(|e| e.to_string())
    })
}

/// Waits for order 42, then takes the next order of any id.
fn match_then_any_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let matched = ctx
            .wait_for_signal_matching("order", |p| p["id"] == 42)
            .await
            .map_err(|e| e.to_string())?;
        let next = ctx
            .wait_for_signal("order")
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "matched": matched, "next": next }))
    })
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Order {
    id: u64,
}

/// The typed form of `order_42_workflow`.
fn typed_order_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let order: Order = ctx
            .receive_signal_matching("order", |o: &Order| o.id == 42)
            .await
            .map_err(|e| e.to_string())?;
        serde_json::to_value(order).map_err(|e| e.to_string())
    })
}

#[tokio::test]
async fn test_signal_matching_skips_a_non_matching_payload() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("order", json!({ "id": 41 }))
        .queue_signal("order", json!({ "id": 42 }))
        .run(order_42_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!({ "id": 42 })));
    assert_succeeded(&outcome.replay_check(order_42_workflow).await);
}

#[tokio::test]
async fn test_signal_matching_leaves_a_skipped_payload_for_a_later_wait() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("order", json!({ "id": 41 }))
        .queue_signal("order", json!({ "id": 42 }))
        .run(match_then_any_workflow, Value::Null)
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "matched": { "id": 42 }, "next": { "id": 41 } }))
    );
    assert_succeeded(&outcome.replay_check(match_then_any_workflow).await);
}

#[tokio::test]
async fn test_signal_matching_typed_form_skips_an_undecodable_payload() {
    let outcome = WorkflowTestEnv::new()
        .queue_signal("order", json!("not an order"))
        .queue_signal("order", json!({ "id": 42 }))
        .run(typed_order_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!({ "id": 42 })));
}

#[tokio::test]
async fn signal_matching_completed_history_replays_clean() {
    let events = vec![
        started(),
        signal("order", json!({ "id": 41 })),
        signal("order", json!({ "id": 42 })),
        completed(json!({ "id": 42 })),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("order_42", order_42_workflow)
        .replay_from_events(events)
        .await;
    assert_succeeded(&report);
}

#[tokio::test]
async fn signal_matching_parked_history_is_a_healthy_canary() {
    let events = vec![started(), signal("order", json!({ "id": 41 }))];
    let report = WorkflowReplayer::new()
        .register_fn("order_42", order_42_workflow)
        .replay_canary_snapshot(snapshot("order_42", events))
        .await;
    assert_succeeded(&report);
}

#[tokio::test]
async fn signal_matching_changed_predicate_is_reported_as_drift() {
    let events = vec![
        started(),
        signal("order", json!({ "id": 41 })),
        signal("order", json!({ "id": 42 })),
        completed(json!({ "id": 42 })),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("order_41", order_41_workflow)
        .replay_from_events(events)
        .await;
    assert!(
        !matches!(report.status, ReplayStatus::ReplaySucceeded),
        "a predicate that now takes a different event must not replay clean:\n{report}"
    );
}

// ── Durable promise ──────────────────────────────────────────────────────

/// Waits on the named promise `approval`.
fn approval_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut promise = ctx.promise("approval").map_err(|e| e.to_string())?;
        match promise.wait::<Value>().await.map_err(|e| e.to_string())? {
            Ok(value) => Ok(json!({ "resolved": value })),
            Err(rejected) => Ok(json!({ "rejected": rejected.error })),
        }
    })
}

/// Waits on the named promise `approval` for at most 60 seconds.
fn approval_timeout_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut promise = ctx.promise("approval").map_err(|e| e.to_string())?;
        let settled = promise
            .wait_timeout::<Value>(std::time::Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())?;
        Ok(match settled {
            None => json!("timed out"),
            Some(Ok(value)) => json!({ "resolved": value }),
            Some(Err(rejected)) => json!({ "rejected": rejected.error }),
        })
    })
}

/// Creates a promise with a generated key and returns its token.
fn new_promise_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let promise = ctx.new_promise().map_err(|e| e.to_string())?;
        Ok(json!(promise.id().to_string()))
    })
}

fn approval_signal_name() -> String {
    approval_id().signal_name()
}

fn approval_id() -> PromiseId {
    PromiseId::new(ExecutionId::new(), "approval").expect("valid key")
}

/// The event that records the token of `id`.
fn promise_recorded(id: &PromiseId) -> WorkflowEvent {
    WorkflowEvent::SideEffectRecorded {
        kind: SideEffectKind::Custom,
        name: Some("harvest.promise".to_string()),
        value: json!(id.to_string()),
    }
}

#[tokio::test]
async fn test_durable_promise_resolves_with_a_value() {
    let settlement = PromiseSettlement::resolved(json!({ "approved_by": "ops" }));
    let outcome = WorkflowTestEnv::new()
        .queue_signal(approval_signal_name(), settlement.to_value())
        .run(approval_workflow, Value::Null)
        .await;
    assert_eq!(
        outcome.result,
        Ok(json!({ "resolved": { "approved_by": "ops" } }))
    );
    assert_succeeded(&outcome.replay_check(approval_workflow).await);
}

#[tokio::test]
async fn test_durable_promise_surfaces_a_rejection() {
    let settlement = PromiseSettlement::rejected("budget exceeded");
    let outcome = WorkflowTestEnv::new()
        .queue_signal(approval_signal_name(), settlement.to_value())
        .run(approval_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!({ "rejected": "budget exceeded" })));
}

#[tokio::test]
async fn test_durable_promise_wait_timeout_returns_the_settlement() {
    let settlement = PromiseSettlement::resolved(json!(true));
    let outcome = WorkflowTestEnv::new()
        .queue_signal(approval_signal_name(), settlement.to_value())
        .run(approval_timeout_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!({ "resolved": true })));
}

/// Waits on one `approval` handle three times in sequence.
fn approval_repeated_wait_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut promise = ctx.promise("approval").map_err(|e| e.to_string())?;
        let first = promise.wait::<Value>().await.map_err(|e| e.to_string())?;
        let second = promise.wait::<Value>().await.map_err(|e| e.to_string())?;
        let third = promise
            .wait_timeout::<Value>(std::time::Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())?
            .ok_or("the timed wait must see the settlement")?;
        let all = [first, second, third];
        Ok(json!(all.into_iter().map(Result::ok).collect::<Vec<_>>()))
    })
}

/// Waits on one `approval` handle with a timeout, then without one.
fn approval_timed_then_plain_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut promise = ctx.promise("approval").map_err(|e| e.to_string())?;
        let timed = promise
            .wait_timeout::<Value>(std::time::Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())?
            .map(Result::ok);
        let plain = promise
            .wait::<Value>()
            .await
            .map_err(|e| e.to_string())?
            .ok();
        Ok(json!([timed, plain]))
    })
}

/// A promise has one settlement. Every later wait on one handle returns it,
/// so a second wait never parks for a settlement that cannot come.
#[tokio::test]
async fn test_durable_promise_handle_returns_its_settlement_to_every_wait() {
    let settlement = PromiseSettlement::resolved(json!("ops"));
    let outcome = WorkflowTestEnv::new()
        .queue_signal(approval_signal_name(), settlement.to_value())
        .run(approval_repeated_wait_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!(["ops", "ops", "ops"])));
    assert_succeeded(&outcome.replay_check(approval_repeated_wait_workflow).await);

    let outcome = WorkflowTestEnv::new()
        .queue_signal(approval_signal_name(), settlement.to_value())
        .run(approval_timed_then_plain_workflow, Value::Null)
        .await;
    assert_eq!(outcome.result, Ok(json!(["ops", "ops"])));
    assert_succeeded(
        &outcome
            .replay_check(approval_timed_then_plain_workflow)
            .await,
    );
}

/// Makes two handles for the named promise `approval`.
fn duplicate_handle_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let _first = ctx.promise("approval").map_err(|e| e.to_string())?;
        match ctx.promise("approval") {
            Ok(_) => Ok(json!("second handle made")),
            Err(e) => Ok(json!(e.to_string())),
        }
    })
}

/// A second handle for one key would wait for a second settlement, and none
/// comes. So the context refuses it, before it records a token.
#[tokio::test]
async fn test_durable_promise_refuses_a_second_handle_for_one_key() {
    let outcome = WorkflowTestEnv::new()
        .run(duplicate_handle_workflow, Value::Null)
        .await;
    let message = outcome.result.clone().expect("the workflow completes");
    assert!(
        message
            .as_str()
            .is_some_and(|m| m.contains("already has a handle")),
        "the second handle must be refused: {message}"
    );
    assert_succeeded(&outcome.replay_check(duplicate_handle_workflow).await);
}

/// Makes a promise with a generated key, then asks for a second handle with
/// that key.
fn duplicate_generated_handle_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let first = ctx.new_promise().map_err(|e| e.to_string())?;
        let key = first.id().key().to_owned();
        match ctx.promise(key) {
            Ok(_) => Ok(json!("second handle made")),
            Err(e) => Ok(json!(e.to_string())),
        }
    })
}

/// A generated key settles once too. So the context refuses a second handle
/// for it, as for a named key.
#[tokio::test]
async fn test_durable_promise_refuses_a_second_handle_for_a_generated_key() {
    let outcome = WorkflowTestEnv::new()
        .run(duplicate_generated_handle_workflow, Value::Null)
        .await;
    let message = outcome.result.clone().expect("the workflow completes");
    assert!(
        message
            .as_str()
            .is_some_and(|m| m.contains("already has a handle")),
        "the second handle must be refused: {message}"
    );
    assert_succeeded(
        &outcome
            .replay_check(duplicate_generated_handle_workflow)
            .await,
    );
}

#[tokio::test]
async fn test_durable_promise_new_promise_token_names_this_run() {
    let outcome = WorkflowTestEnv::new()
        .run(new_promise_workflow, Value::Null)
        .await;
    let token = outcome.result.clone().expect("the workflow completes");
    let id: PromiseId = token
        .as_str()
        .expect("the token is a string")
        .parse()
        .expect("the token parses");
    assert_ne!(id.key(), "");
    assert_succeeded(&outcome.replay_check(new_promise_workflow).await);
}

#[tokio::test]
async fn durable_promise_completed_history_replays_clean() {
    let id = approval_id();
    let events = vec![
        started(),
        promise_recorded(&id),
        signal(
            &id.signal_name(),
            PromiseSettlement::resolved(json!(7)).to_value(),
        ),
        completed(json!({ "resolved": 7 })),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("approval", approval_workflow)
        .replay_from_events(events)
        .await;
    assert_succeeded(&report);
}

#[tokio::test]
async fn durable_promise_parked_history_is_a_healthy_canary() {
    let report = WorkflowReplayer::new()
        .register_fn("approval", approval_workflow)
        .replay_canary_snapshot(snapshot(
            "approval",
            vec![started(), promise_recorded(&approval_id())],
        ))
        .await;
    assert_succeeded(&report);
}

/// Creates a promise, hands its token to an activity, then waits.
fn publish_and_wait_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let mut promise = ctx.new_promise().map_err(|e| e.to_string())?;
        ctx.execute_activity_raw("publish_token", json!(promise.id().to_string()), "default")
            .await
            .map_err(|e| e.to_string())?;
        let value = promise
            .wait::<Value>()
            .await
            .map_err(|e| e.to_string())?
            .map_err(|rejected| rejected.error)?;
        Ok(value)
    })
}

/// A reset fork replays carried history under a new execution id. The
/// recorded token must keep the activity input byte-identical.
#[tokio::test]
async fn durable_promise_token_replays_under_a_new_execution_id() {
    let recorded = PromiseId::new(ExecutionId::new(), "0192f3a4-5b6c-7d8e-9f01-23456789abcd")
        .expect("valid key");
    let token = recorded.to_string();
    let activity_id = ActivityExecId::new();
    let events = vec![
        started(),
        promise_recorded(&recorded),
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "publish_token".to_string(),
            input: json!(token),
            queue: "default".to_string(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        },
        signal(
            &recorded.signal_name(),
            PromiseSettlement::resolved(json!("ok")).to_value(),
        ),
        completed(json!("ok")),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("publish_and_wait", publish_and_wait_workflow)
        .replay_from_events(events)
        .await;
    assert_succeeded(&report);
}

/// Keys a promise on this run's id, then asks for a second handle with the
/// key on the first handle. The workflow fails unless the context refuses it
/// as a second handle.
fn duplicate_run_keyed_handle_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let first = ctx
            .promise(ctx.execution_id().to_string())
            .map_err(|e| e.to_string())?;
        let key = first.id().key().to_owned();
        match ctx.promise(key) {
            Err(e) if e.to_string().contains("already has a handle") => Ok(json!("refused")),
            Err(e) => Err(format!("refused for another reason: {e}")),
            Ok(_) => Err("second handle made".to_string()),
        }
    })
}

/// A reset fork replays the first token with the source run's key. The fork
/// must still refuse a second handle for that key, as the source run did.
#[tokio::test]
async fn durable_promise_refuses_a_second_handle_for_a_replayed_key() {
    let source = ExecutionId::new();
    let recorded = PromiseId::new(source, source.to_string()).expect("valid key");
    let events = vec![
        started(),
        promise_recorded(&recorded),
        completed(json!("refused")),
    ];
    let report = WorkflowReplayer::new()
        .register_fn("duplicate_run_keyed", duplicate_run_keyed_handle_workflow)
        .replay_from_events(events)
        .await;
    assert_ne!(
        report.execution_id, source,
        "the replay runs under a new id"
    );
    assert_succeeded(&report);
}

/// Sends a `harvest.promise:` signal whose payload is not a settlement, by
/// workflow id and by execution id.
fn invalid_promise_signal_workflow(ctx: &WorkflowContext, _input: Value) -> HandlerFuture<'_> {
    Box::pin(async move {
        let by_id = ctx
            .signal_external_workflow_by_id("other", "other-1", "harvest.promise:k", json!("bad"))
            .await;
        let by_exec = ctx
            .signal_external_workflow(ExecutionId::new(), "harvest.promise:k", json!("bad"))
            .await;
        let reserved = ctx
            .signal_external_workflow_with_idempotency(
                ExecutionId::new(),
                "order",
                json!(1),
                Some("harvest.promise:k".to_string()),
            )
            .await;
        Ok(json!([
            by_id.map_err(|e| e.to_string()).err(),
            by_exec.map_err(|e| e.to_string()).err(),
            reserved.map_err(|e| e.to_string()).err(),
        ]))
    })
}

/// A broken promise signal is the sender's bug. The context refuses it
/// before it records a request, so the outbox never reports it as a target
/// state such as `not_running`.
#[tokio::test]
async fn an_invalid_promise_signal_is_refused_at_the_sender() {
    let outcome = WorkflowTestEnv::new()
        .run(invalid_promise_signal_workflow, Value::Null)
        .await;
    let errors = outcome.result.clone().expect("the workflow completes");
    let errors = errors.as_array().expect("an array");
    assert_eq!(errors.len(), 3);
    for error in errors {
        let message = error.as_str().unwrap_or_default();
        assert!(
            message.contains("invalid configuration"),
            "each send must be refused as a configuration error: {errors:?}"
        );
    }
    assert_succeeded(&outcome.replay_check(invalid_promise_signal_workflow).await);
}
