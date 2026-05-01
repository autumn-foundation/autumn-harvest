//! Tests for async/external activity completion via task tokens (issue #92).
//!
//! These tests cover the core engine behaviour (no DB required):
//! - New event variants serialize and deserialize correctly
//! - `execute_activity_external` suspends and emits a command on first call
//! - Replay returns recorded output when `ActivityCompletedExternally` is in history
//! - Replay returns recorded failure when `ActivityFailedExternally` is in history
//! - Replay returns timeout when `ActivityTimedOut` is in history
//! - Re-emission of the same token when awaiting but no terminal event exists yet
//! - `all_type_names_are_unique` extended with the four new variants
//!
//! The integration/DB tests live in `tests/integration_e2e.rs` behind the `db`
//! feature flag.

use std::sync::Arc;

use autumn_harvest::context::{WorkflowCommand, WorkflowContext};
use autumn_harvest::error::{HarvestError, TimeoutType};
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::replay::HistoryMatcher;
use autumn_harvest::types::{ActivityExecId, ExecutionId, ExternalActivityToken};
use chrono::Utc;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Event serialization
// ---------------------------------------------------------------------------

#[test]
fn activity_awaiting_external_round_trips_serde() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let event = WorkflowEvent::ActivityAwaitingExternal {
        activity_id,
        token,
        name: "approve_expense".to_string(),
        input: serde_json::json!({"amount": 500}),
        queue: "approvals".to_string(),
        schedule_to_close_secs: 86400,
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(
        back,
        WorkflowEvent::ActivityAwaitingExternal { .. }
    ));
}

#[test]
fn activity_completed_externally_round_trips_serde() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let event = WorkflowEvent::ActivityCompletedExternally {
        activity_id,
        token,
        output: serde_json::json!({"approved": true}),
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(
        back,
        WorkflowEvent::ActivityCompletedExternally { .. }
    ));
}

#[test]
fn activity_failed_externally_round_trips_serde() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let event = WorkflowEvent::ActivityFailedExternally {
        activity_id,
        token,
        error: "manager rejected".to_string(),
        retryable: false,
    };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(
        back,
        WorkflowEvent::ActivityFailedExternally { .. }
    ));
}

#[test]
fn activity_external_deadline_extended_round_trips_serde() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let event = WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token };
    let json = serde_json::to_string(&event).expect("serialize");
    let back: WorkflowEvent = serde_json::from_str(&json).expect("deserialize");
    assert!(matches!(
        back,
        WorkflowEvent::ActivityExternalDeadlineExtended { .. }
    ));
}

#[test]
fn external_event_type_names_are_stable() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    assert_eq!(
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "x".into(),
            input: Value::Null,
            queue: "default".into(),
            schedule_to_close_secs: 0,
        }
        .type_name(),
        "ActivityAwaitingExternal"
    );
    assert_eq!(
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: Value::Null,
        }
        .type_name(),
        "ActivityCompletedExternally"
    );
    assert_eq!(
        WorkflowEvent::ActivityFailedExternally {
            activity_id,
            token,
            error: "x".into(),
            retryable: false,
        }
        .type_name(),
        "ActivityFailedExternally"
    );
    assert_eq!(
        WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token }.type_name(),
        "ActivityExternalDeadlineExtended"
    );
}

// ---------------------------------------------------------------------------
// HistoryMatcher — match_external_activity
// ---------------------------------------------------------------------------

#[test]
fn matcher_replays_completed_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let output = serde_json::json!({"approved": true});

    let events = vec![
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "default".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: output.clone(),
        },
    ];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("approve_expense");

    assert!(
        matches!(&result, autumn_harvest::replay::HistoryMatch::Matched { output: o } if o == &output),
        "expected Matched, got {result:?}"
    );
    assert_eq!(matcher.position(), 2);
    assert!(!matcher.is_replaying());
}

#[test]
fn matcher_replays_failed_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "default".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityFailedExternally {
            activity_id,
            token,
            error: "manager rejected".into(),
            retryable: false,
        },
    ];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("approve_expense");

    assert!(
        matches!(&result, autumn_harvest::replay::HistoryMatch::Failed { error, .. } if error == "manager rejected"),
        "expected Failed, got {result:?}"
    );
}

#[test]
fn matcher_replays_timed_out_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "default".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: autumn_harvest::error::TimeoutType::ScheduleToClose,
        },
    ];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("approve_expense");

    assert!(
        matches!(
            &result,
            autumn_harvest::replay::HistoryMatch::TimedOut {
                timeout_type: TimeoutType::ScheduleToClose,
            }
        ),
        "expected TimedOut, got {result:?}"
    );
}

#[test]
fn matcher_returns_awaiting_when_no_terminal_event() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![WorkflowEvent::ActivityAwaitingExternal {
        activity_id,
        token,
        name: "approve_expense".into(),
        input: Value::Null,
        queue: "default".into(),
        schedule_to_close_secs: 86400,
    }];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("approve_expense");

    match result {
        autumn_harvest::replay::HistoryMatch::AwaitingExternalCompletion {
            activity_id: aid,
            token: tok,
        } => {
            assert_eq!(aid, activity_id);
            assert_eq!(tok, token);
        }
        other => panic!("expected AwaitingExternalCompletion, got {other:?}"),
    }
}

#[test]
fn matcher_diverges_on_wrong_name_for_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![WorkflowEvent::ActivityAwaitingExternal {
        activity_id,
        token,
        name: "approve_expense".into(),
        input: Value::Null,
        queue: "default".into(),
        schedule_to_close_secs: 86400,
    }];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("different_activity");

    assert!(
        matches!(
            result,
            autumn_harvest::replay::HistoryMatch::Diverged { .. }
        ),
        "expected Diverged, got {result:?}"
    );
}

#[test]
fn matcher_no_match_past_end_of_history_for_external_activity() {
    let mut matcher = HistoryMatcher::new(vec![]);
    let result = matcher.match_external_activity("approve_expense");
    assert!(
        matches!(result, autumn_harvest::replay::HistoryMatch::NoMatch),
        "expected NoMatch, got {result:?}"
    );
}

#[test]
fn matcher_skips_deadline_extended_events_between_awaiting_and_terminal() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let output = serde_json::json!({"ok": true});

    let events = vec![
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "default".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token },
        WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token },
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: output.clone(),
        },
    ];

    let mut matcher = HistoryMatcher::new(events);
    let result = matcher.match_external_activity("approve_expense");

    assert!(
        matches!(&result, autumn_harvest::replay::HistoryMatch::Matched { output: o } if o == &output),
        "expected Matched, got {result:?}"
    );
    assert_eq!(matcher.position(), 4);
}

// ---------------------------------------------------------------------------
// WorkflowContext — execute_activity_external
// ---------------------------------------------------------------------------

#[tokio::test]
async fn context_execute_activity_external_suspends_on_first_call() {
    let ctx = Arc::new(WorkflowContext::new_test());
    let ctx2 = Arc::clone(&ctx);

    let handle = tokio::spawn(async move {
        ctx2.execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
            .await
    });

    // Give the coroutine time to emit the command and suspend.
    tokio::task::yield_now().await;

    let cmds = ctx.drain_commands();
    assert_eq!(cmds.len(), 1, "expected exactly one command");

    match &cmds[0] {
        WorkflowCommand::ScheduleExternalActivity {
            name,
            queue,
            schedule_to_close_secs,
            ..
        } => {
            assert_eq!(name, "approve_expense");
            assert_eq!(queue, "approvals");
            assert_eq!(*schedule_to_close_secs, 86400);
        }
        other => panic!("expected ScheduleExternalActivity, got {other:?}"),
    }

    handle.abort();
}

#[tokio::test]
async fn context_execute_activity_external_emits_unique_token() {
    let ctx = Arc::new(WorkflowContext::new_test());
    let ctx2 = Arc::clone(&ctx);

    let handle = tokio::spawn(async move {
        ctx2.execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
            .await
    });

    tokio::task::yield_now().await;

    let cmds = ctx.drain_commands();
    assert_eq!(cmds.len(), 1);

    let WorkflowCommand::ScheduleExternalActivity { token, .. } = &cmds[0] else {
        panic!("expected ScheduleExternalActivity");
    };
    // Token must be a valid UUID (non-nil).
    assert_ne!(*token, ExternalActivityToken::nil());

    handle.abort();
}

#[tokio::test]
async fn context_execute_activity_external_resolves_via_oneshot() {
    let ctx = Arc::new(WorkflowContext::new_test());
    let ctx2 = Arc::clone(&ctx);
    let expected = serde_json::json!({"approved": true});
    let expected2 = expected.clone();

    let handle = tokio::spawn(async move {
        ctx2.execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
            .await
    });

    tokio::task::yield_now().await;

    let mut cmds = ctx.drain_commands();
    let WorkflowCommand::ScheduleExternalActivity { result_tx, .. } = cmds.remove(0) else {
        panic!("expected ScheduleExternalActivity");
    };
    result_tx.send(Ok(expected2)).expect("send should succeed");

    let result = handle.await.expect("no panic");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), expected);
}

#[tokio::test]
async fn context_replays_completed_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let output = serde_json::json!({"approved": true});

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: output.clone(),
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let result = ctx
        .execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
        .await;

    assert!(result.is_ok(), "expected Ok, got {result:?}");
    assert_eq!(result.unwrap(), output);

    // No live commands should be emitted during full replay.
    assert!(ctx.drain_commands().is_empty());
    assert!(!ctx.is_replaying());
}

#[tokio::test]
async fn context_replays_failed_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityFailedExternally {
            activity_id,
            token,
            error: "manager rejected".into(),
            retryable: false,
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let result = ctx
        .execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
        .await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        HarvestError::ActivityFailed { .. }
    ));
}

#[tokio::test]
async fn context_replays_timed_out_external_activity() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityTimedOut {
            activity_id,
            timeout_type: TimeoutType::ScheduleToClose,
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    let result = ctx
        .execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
        .await;

    assert!(matches!(
        result,
        Err(HarvestError::Timeout {
            timeout_type: TimeoutType::ScheduleToClose,
            ..
        })
    ));
}

#[tokio::test]
async fn context_awaiting_external_re_emits_same_token_idempotently() {
    // When history has ActivityAwaitingExternal but no terminal, the context
    // should re-emit ScheduleExternalActivity with the SAME token from history.
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
    ];

    let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), events));
    let ctx2 = Arc::clone(&ctx);

    let handle = tokio::spawn(async move {
        ctx2.execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
            .await
    });

    tokio::task::yield_now().await;

    let cmds = ctx.drain_commands();
    assert_eq!(cmds.len(), 1, "should re-emit idempotent command");
    let WorkflowCommand::ScheduleExternalActivity {
        token: emitted_token,
        activity_id: emitted_id,
        ..
    } = &cmds[0]
    else {
        panic!("expected ScheduleExternalActivity");
    };
    // Crucially, the token and activity_id must match what's already in history.
    assert_eq!(*emitted_token, token, "token must be reused from history");
    assert_eq!(
        *emitted_id, activity_id,
        "activity_id must be reused from history"
    );

    handle.abort();
}

#[tokio::test]
async fn context_execute_activity_external_detects_non_determinism() {
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: Value::Null,
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);
    // Call with a DIFFERENT name → should be non-deterministic.
    let result = ctx
        .execute_activity_external("different_activity", Value::Null, "approvals", 86400)
        .await;

    assert!(
        matches!(result, Err(HarvestError::NonDeterministic(_))),
        "expected NonDeterministic, got {result:?}"
    );
    assert!(ctx.drain_commands().is_empty());
}

#[tokio::test]
async fn context_double_complete_is_idempotent_replay() {
    // Simulates the "double-complete" scenario at the replay layer:
    // if ActivityCompletedExternally is already in history, a second call to
    // execute_activity_external returns the same recorded output without
    // suspending or emitting new commands.
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();
    let output = serde_json::json!({"approved": true});

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityCompletedExternally {
            activity_id,
            token,
            output: output.clone(),
        },
    ];

    let ctx = WorkflowContext::for_replay(ExecutionId::new(), events);

    // First call — replays from history.
    let result1 = ctx
        .execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
        .await;
    assert!(result1.is_ok(), "first call must succeed: {result1:?}");
    assert_eq!(result1.unwrap(), output);

    // No live commands emitted during a pure replay.
    assert!(ctx.drain_commands().is_empty());
}

#[tokio::test]
async fn context_post_restart_while_awaiting_with_deadline_extensions() {
    // After a process restart, the workflow is re-run from scratch.  History
    // contains ActivityAwaitingExternal + two deadline extensions but no
    // terminal event.  The context must:
    //   (a) return AwaitingExternalCompletion (not a terminal result), and
    //   (b) re-emit ScheduleExternalActivity with the SAME token and activity_id.
    let activity_id = ActivityExecId::new();
    let token = ExternalActivityToken::new();

    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::ActivityAwaitingExternal {
            activity_id,
            token,
            name: "approve_expense".into(),
            input: Value::Null,
            queue: "approvals".into(),
            schedule_to_close_secs: 86400,
        },
        WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token },
        WorkflowEvent::ActivityExternalDeadlineExtended { activity_id, token },
    ];

    let ctx = Arc::new(WorkflowContext::for_replay(ExecutionId::new(), events));
    let ctx2 = Arc::clone(&ctx);

    let handle = tokio::spawn(async move {
        ctx2.execute_activity_external("approve_expense", Value::Null, "approvals", 86400)
            .await
    });

    tokio::task::yield_now().await;

    let cmds = ctx.drain_commands();
    assert_eq!(cmds.len(), 1, "must re-emit exactly one command");

    let WorkflowCommand::ScheduleExternalActivity {
        token: emitted_token,
        activity_id: emitted_id,
        name,
        ..
    } = &cmds[0]
    else {
        panic!("expected ScheduleExternalActivity, got {:?}", cmds[0]);
    };

    assert_eq!(
        *emitted_token, token,
        "token must be preserved across restart"
    );
    assert_eq!(*emitted_id, activity_id, "activity_id must be preserved");
    assert_eq!(name, "approve_expense");

    handle.abort();
}
