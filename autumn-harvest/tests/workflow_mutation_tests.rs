//! Tests for the Update primitive (issue #140).
//!
//! Covers: event serde, `UpdateId`, `UpdateRegistry`, `WorkflowContext`
//! registration/validation/dispatch, and `HistoryMatcher` update replay.

#![cfg(feature = "testing")]

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::types::{ExecutionId, UpdateId};
use chrono::Utc;

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_admitted_history(update_id: UpdateId, name: &str) -> Vec<WorkflowEvent> {
    vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::json!({}),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: name.to_string(),
            input: serde_json::json!({"amount": 100}),
            timestamp: Utc::now(),
        },
    ]
}

fn make_completed_history(update_id: UpdateId, name: &str) -> Vec<WorkflowEvent> {
    let mut events = make_admitted_history(update_id, name);
    events.push(WorkflowEvent::UpdateCompleted {
        update_id,
        output: serde_json::json!({"approved": true}),
    });
    events
}

fn make_failed_history(update_id: UpdateId, name: &str) -> Vec<WorkflowEvent> {
    let mut events = make_admitted_history(update_id, name);
    events.push(WorkflowEvent::UpdateFailed {
        update_id,
        error: "insufficient funds".to_string(),
    });
    events
}

// ── Event serde round-trips ────────────────────────────────────────────────

#[test]
fn update_admitted_round_trips_serde() {
    let id = UpdateId::new();
    let event = WorkflowEvent::UpdateAdmitted {
        update_id: id,
        name: "approve".to_string(),
        input: serde_json::json!({"amount": 50}),
        timestamp: Utc::now(),
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(back, WorkflowEvent::UpdateAdmitted { .. }));
    // Adjacently-tagged: {"type":"UpdateAdmitted","data":{...}}
    assert!(json.contains("\"type\":\"UpdateAdmitted\""));
}

#[test]
fn update_completed_round_trips_serde() {
    let id = UpdateId::new();
    let event = WorkflowEvent::UpdateCompleted {
        update_id: id,
        output: serde_json::json!({"ok": true}),
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(back, WorkflowEvent::UpdateCompleted { .. }));
    assert!(json.contains("\"type\":\"UpdateCompleted\""));
}

#[test]
fn update_failed_round_trips_serde() {
    let id = UpdateId::new();
    let event = WorkflowEvent::UpdateFailed {
        update_id: id,
        error: "bad input".to_string(),
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(
        matches!(back, WorkflowEvent::UpdateFailed { error, .. } if error == "bad input"),
        "error field should round-trip"
    );
}

#[test]
fn update_event_type_names_are_stable() {
    let id = UpdateId::new();
    assert_eq!(
        WorkflowEvent::UpdateAdmitted {
            update_id: id,
            name: "x".to_string(),
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
        }
        .type_name(),
        "UpdateAdmitted"
    );
    assert_eq!(
        WorkflowEvent::UpdateCompleted {
            update_id: id,
            output: serde_json::Value::Null,
        }
        .type_name(),
        "UpdateCompleted"
    );
    assert_eq!(
        WorkflowEvent::UpdateFailed {
            update_id: id,
            error: "x".to_string(),
        }
        .type_name(),
        "UpdateFailed"
    );
}

// ── UpdateId type ─────────────────────────────────────────────────────────

#[test]
fn update_id_new_is_unique() {
    let a = UpdateId::new();
    let b = UpdateId::new();
    assert_ne!(a, b);
}

#[test]
fn update_id_serde_round_trip() {
    let id = UpdateId::new();
    let json = serde_json::to_string(&id).expect("serialize");
    let back: UpdateId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(id, back);
}

#[test]
fn update_id_display_is_uuid() {
    let id = UpdateId::new();
    let s = id.to_string();
    // UUID format: 8-4-4-4-12 hex chars
    assert_eq!(s.len(), 36);
    assert_eq!(s.chars().filter(|&c| c == '-').count(), 4);
}

// ── register_update_handler ────────────────────────────────────────────────

#[test]
fn register_update_handler_is_idempotent() {
    let ctx = WorkflowContext::new_test();
    // Registering twice must not panic or error
    ctx.register_update_handler(
        "approve",
        |_input: &serde_json::Value| -> Result<(), String> { Ok(()) },
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({"ok": true}))
        },
    );
    ctx.register_update_handler(
        "approve",
        |_input: &serde_json::Value| -> Result<(), String> { Ok(()) },
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({"ok": true}))
        },
    );
    // No panic = pass
}

#[test]
fn register_update_handler_emits_no_commands() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler(
        "approve",
        |_input: &serde_json::Value| -> Result<(), String> { Ok(()) },
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({}))
        },
    );
    let cmds = ctx.drain_commands();
    assert!(cmds.is_empty(), "registration must not emit any commands");
}

// ── validate_update ────────────────────────────────────────────────────────

#[test]
fn validate_update_no_handler_returns_not_found() {
    let ctx = WorkflowContext::new_test();
    let result = ctx.validate_update("approve", &serde_json::json!({}));
    assert!(
        result.is_err(),
        "should error when no handler is registered"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("not found")
            || err_msg.to_lowercase().contains("no handler")
            || err_msg.to_lowercase().contains("approve"),
        "error should mention the missing handler or 'not found', got: {err_msg}"
    );
}

#[test]
fn validate_update_passes_when_validator_accepts() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler(
        "approve",
        |input: &serde_json::Value| -> Result<(), String> {
            if input["amount"].as_i64().unwrap_or(0) > 0 {
                Ok(())
            } else {
                Err("amount must be positive".to_string())
            }
        },
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({}))
        },
    );
    let result = ctx.validate_update("approve", &serde_json::json!({"amount": 100}));
    assert!(result.is_ok(), "validator should accept positive amount");
}

#[test]
fn validate_update_rejected_when_validator_rejects() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler(
        "approve",
        |input: &serde_json::Value| -> Result<(), String> {
            if input["amount"].as_i64().unwrap_or(0) > 0 {
                Ok(())
            } else {
                Err("amount must be positive".to_string())
            }
        },
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({}))
        },
    );
    let result = ctx.validate_update("approve", &serde_json::json!({"amount": -1}));
    assert!(result.is_err(), "validator should reject negative amount");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("amount must be positive"),
        "error should contain validator message, got: {err_msg}"
    );
}

#[test]
fn validate_update_passes_when_no_validator_provided() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler_no_validator("approve", |_input: serde_json::Value| async move {
        Ok::<serde_json::Value, String>(serde_json::json!({}))
    });
    let result = ctx.validate_update("approve", &serde_json::json!({"anything": true}));
    assert!(result.is_ok(), "no validator should always accept");
}

// ── execute_admitted_update ────────────────────────────────────────────────

#[tokio::test]
async fn execute_admitted_update_runs_handler_in_live_mode() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler(
        "approve",
        |_: &serde_json::Value| Ok::<(), String>(()),
        |input: serde_json::Value| async move {
            let amount = input["amount"].as_i64().unwrap_or(0);
            Ok::<serde_json::Value, String>(serde_json::json!({"approved": amount > 0}))
        },
    );
    let update_id = UpdateId::new();
    let result = ctx
        .execute_admitted_update(update_id, "approve", serde_json::json!({"amount": 50}))
        .await;
    assert!(result.is_ok(), "handler should succeed");
    let output = result.unwrap();
    assert_eq!(output["approved"], serde_json::json!(true));
}

#[tokio::test]
async fn execute_admitted_update_propagates_handler_error() {
    let ctx = WorkflowContext::new_test();
    ctx.register_update_handler(
        "approve",
        |_: &serde_json::Value| Ok::<(), String>(()),
        |_input: serde_json::Value| async move {
            Err::<serde_json::Value, String>("insufficient funds".to_string())
        },
    );
    let update_id = UpdateId::new();
    let result = ctx
        .execute_admitted_update(update_id, "approve", serde_json::json!({}))
        .await;
    assert!(result.is_err(), "handler error should propagate");
    assert_eq!(result.unwrap_err(), "insufficient funds");
}

#[tokio::test]
async fn execute_admitted_update_replays_completed_result_without_rerunning() {
    let update_id = UpdateId::new();
    let exec_id = ExecutionId::new();
    let history = make_completed_history(update_id, "approve");
    let ctx = WorkflowContext::for_replay(exec_id, history);

    let handler_ran = false;
    ctx.register_update_handler(
        "approve",
        |_: &serde_json::Value| Ok::<(), String>(()),
        |_input: serde_json::Value| async move {
            // This should NOT run during replay
            Ok::<serde_json::Value, String>(serde_json::json!({"handler_ran": true}))
        },
    );

    let result = ctx
        .execute_admitted_update(update_id, "approve", serde_json::json!({"amount": 100}))
        .await;
    assert!(result.is_ok());
    // Should get the recorded output, not the handler's output
    assert_eq!(result.unwrap()["approved"], serde_json::json!(true));
    // handler_ran stays false because the closure is a different one; the key assertion
    // is that the returned value matches the recorded history, not the live handler.
    let _ = handler_ran; // suppress unused warning
}

#[tokio::test]
async fn execute_admitted_update_replays_failed_result_without_rerunning() {
    let update_id = UpdateId::new();
    let exec_id = ExecutionId::new();
    let history = make_failed_history(update_id, "approve");
    let ctx = WorkflowContext::for_replay(exec_id, history);

    ctx.register_update_handler(
        "approve",
        |_: &serde_json::Value| Ok::<(), String>(()),
        |_input: serde_json::Value| async move {
            Ok::<serde_json::Value, String>(serde_json::json!({"should_not_run": true}))
        },
    );

    let result = ctx
        .execute_admitted_update(update_id, "approve", serde_json::json!({}))
        .await;
    assert!(result.is_err(), "should replay recorded failure");
    assert_eq!(result.unwrap_err(), "insufficient funds");
}

// ── HistoryMatcher::match_update ──────────────────────────────────────────

#[test]
fn history_matcher_finds_update_completed() {
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};

    let update_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "approve".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: serde_json::json!({"result": 42}),
        },
    ];

    let matcher = HistoryMatcher::new(events);
    let result = matcher.match_update(update_id);
    assert!(
        matches!(result, HistoryMatch::Matched { .. }),
        "should find UpdateCompleted"
    );
    if let HistoryMatch::Matched { output } = result {
        assert_eq!(output["result"], 42);
    }
}

#[test]
fn history_matcher_finds_update_failed() {
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};

    let update_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "approve".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateFailed {
            update_id,
            error: "insufficient funds".to_string(),
        },
    ];

    let matcher = HistoryMatcher::new(events);
    let result = matcher.match_update(update_id);
    assert!(
        matches!(result, HistoryMatch::Failed { .. }),
        "should find UpdateFailed"
    );
    if let HistoryMatch::Failed { error, .. } = result {
        assert_eq!(error, "insufficient funds");
    }
}

#[test]
fn history_matcher_returns_no_match_for_admitted_without_completion() {
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};

    let update_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "approve".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        // No UpdateCompleted or UpdateFailed
    ];

    let matcher = HistoryMatcher::new(events);
    let result = matcher.match_update(update_id);
    assert!(
        matches!(result, HistoryMatch::NoMatch),
        "admitted-without-completion should be NoMatch"
    );
}

#[test]
fn history_matcher_returns_no_match_for_unknown_update_id() {
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};

    let known_id = UpdateId::new();
    let unknown_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateCompleted {
            update_id: known_id,
            output: serde_json::json!({}),
        },
    ];

    let matcher = HistoryMatcher::new(events);
    let result = matcher.match_update(unknown_id);
    assert!(
        matches!(result, HistoryMatch::NoMatch),
        "should not find result for different update_id"
    );
}

// ── Update events are transparent to activity replay ─────────────────────

#[test]
fn update_events_do_not_break_activity_replay() {
    use autumn_harvest::replay::{HistoryMatch, HistoryMatcher};
    use autumn_harvest::types::ActivityExecId;

    let update_id = UpdateId::new();
    let activity_id = ActivityExecId::new();
    // History interleaves an update between an activity scheduled and completed
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id,
            name: "send_email".to_string(),
            input: serde_json::json!({}),
            queue: "default".to_string(),
        },
        // An update was admitted and completed while the activity was running
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "query_status".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateCompleted {
            update_id,
            output: serde_json::json!({"status": "running"}),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id,
            output: serde_json::json!("sent"),
        },
    ];

    let mut matcher = HistoryMatcher::new(events);
    // Skip WorkflowStarted
    matcher.advance();
    let result = matcher.match_activity("send_email");
    assert!(
        matches!(result, HistoryMatch::Matched { .. }),
        "activity replay should succeed even with interleaved update events, got: {result:?}"
    );
}

// ── drain_admitted_updates ────────────────────────────────────────────────

#[test]
fn drain_admitted_updates_returns_pending_update() {
    use autumn_harvest::replay::HistoryMatcher;

    let update_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id,
            name: "approve".to_string(),
            input: serde_json::json!({"amount": 10}),
            timestamp: Utc::now(),
        },
        // No completion — this update is pending
    ];

    let matcher = HistoryMatcher::new(events);
    let pending = matcher.drain_admitted_updates();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].0, update_id);
    assert_eq!(pending[0].1, "approve");
}

#[test]
fn drain_admitted_updates_excludes_already_completed() {
    use autumn_harvest::replay::HistoryMatcher;

    let pending_id = UpdateId::new();
    let completed_id = UpdateId::new();
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: serde_json::Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
        },
        WorkflowEvent::UpdateAdmitted {
            update_id: completed_id,
            name: "step_one".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        WorkflowEvent::UpdateCompleted {
            update_id: completed_id,
            output: serde_json::json!({}),
        },
        WorkflowEvent::UpdateAdmitted {
            update_id: pending_id,
            name: "step_two".to_string(),
            input: serde_json::json!({}),
            timestamp: Utc::now(),
        },
        // step_two has no completion — it is pending
    ];

    let matcher = HistoryMatcher::new(events);
    let pending = matcher.drain_admitted_updates();
    assert_eq!(
        pending.len(),
        1,
        "only the unresolved update should be pending"
    );
    assert_eq!(pending[0].0, pending_id);
    assert_eq!(pending[0].1, "step_two");
}
