//! Fan-out primitive tests -- parallel activity dispatch, replay determinism,
//! non-determinism detection, and cancellation propagation.
//!
//! All tests are pure unit tests; no database required.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Test workflow handlers
// ---------------------------------------------------------------------------

fn fan_out_three_parallel<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities = vec![
            (
                "task_a".to_string(),
                json!("input_a"),
                "default".to_string(),
            ),
            (
                "task_b".to_string(),
                json!("input_b"),
                "default".to_string(),
            ),
            (
                "task_c".to_string(),
                json!("input_c"),
                "default".to_string(),
            ),
        ];
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

fn fan_out_fail_fast<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities = vec![
            ("task_ok".to_string(), json!(null), "default".to_string()),
            ("task_fail".to_string(), json!(null), "default".to_string()),
            ("task_ok2".to_string(), json!(null), "default".to_string()),
        ];
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

fn fan_out_collect_all<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities = vec![
            ("task_ok".to_string(), json!(null), "default".to_string()),
            ("task_fail".to_string(), json!(null), "default".to_string()),
            ("task_ok2".to_string(), json!(null), "default".to_string()),
        ];
        let results = ctx
            .execute_activity_fan_out_collect_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        // Serialize per-slot results into a JSON-compatible form
        let serialized: Vec<Value> = results
            .into_iter()
            .map(|r| match r {
                Ok(v) => json!({ "ok": v }),
                Err(e) => json!({ "err": e }),
            })
            .collect();
        Ok(json!({ "results": serialized }))
    })
}

fn fan_out_empty<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let results = ctx
            .execute_activity_fan_out_raw(vec![])
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "count": results.len() }))
    })
}

// Three-activity fan-out using dynamic input derived from a prior activity result
fn fan_out_dynamic_from_prior<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // First activity returns a list of items
        let items_json = ctx
            .execute_activity_raw("list_items", json!(null), "default")
            .await
            .map_err(|e| e.to_string())?;
        let items = items_json.as_array().cloned().unwrap_or_default();

        // Fan-out: process each item in parallel
        let activities: Vec<_> = items
            .into_iter()
            .map(|item| ("process_item".to_string(), item, "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "processed": results }))
    })
}

// Workflow for non-determinism (count mismatch) detection test
fn fan_out_count_changed<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        // The count is derived from the input so it can differ between runs
        let n = usize::try_from(input.as_u64().unwrap_or(2)).unwrap_or(2);
        let activities: Vec<_> = (0..n)
            .map(|i| (format!("task_{i}"), json!(i), "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "count": results.len() }))
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Basic fan-out: 3 activities in parallel, all complete successfully.
/// The workflow should return results in input order (not completion order).
#[tokio::test]
async fn fan_out_raw_three_parallel_all_succeed() {
    let exec_id = ExecutionId::new();
    let id_a = ActivityExecId::new();
    let id_b = ActivityExecId::new();
    let id_c = ActivityExecId::new();

    // History has the marker + 3 scheduled activities + 3 completions
    // Note: completions are in reverse order to verify input-order result collection
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_a,
            name: "task_a".into(),
            input: json!("input_a"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_b,
            name: "task_b".into(),
            input: json!("input_b"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_c,
            name: "task_c".into(),
            input: json!("input_c"),
            queue: "default".into(),
        },
        // Completions out-of-order: C finishes first, then A, then B
        WorkflowEvent::ActivityCompleted {
            activity_id: id_c,
            output: json!("result_c"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_a,
            output: json!("result_a"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_b,
            output: json!("result_b"),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 3, "should have 3 results");
            // Results must be in INPUT order, not completion order
            assert_eq!(results[0], json!("result_a"), "slot 0 should be result_a");
            assert_eq!(results[1], json!("result_b"), "slot 1 should be result_b");
            assert_eq!(results[2], json!("result_c"), "slot 2 should be result_c");
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Fan-out fail-fast: one activity fails, the whole fan-out fails.
#[tokio::test]
async fn fan_out_raw_fail_fast_on_first_failure() {
    let exec_id = ExecutionId::new();
    let id_ok = ActivityExecId::new();
    let id_fail = ActivityExecId::new();
    let id_ok2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_ok,
            name: "task_ok".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_fail,
            name: "task_fail".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_ok2,
            name: "task_ok2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_ok,
            output: json!("success"),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_fail,
            error: "boom".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_ok2,
            output: json!("also_success"),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_fail_fast, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error } => {
            assert!(
                error.contains("boom"),
                "error should mention 'boom', got: {error}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// Fan-out collect-all: one activity fails, others succeed.
/// All per-slot results are returned (not fail-fast).
#[tokio::test]
async fn fan_out_collect_all_returns_per_slot_results() {
    let exec_id = ExecutionId::new();
    let id_ok = ActivityExecId::new();
    let id_fail = ActivityExecId::new();
    let id_ok2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_ok,
            name: "task_ok".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_fail,
            name: "task_fail".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_ok2,
            name: "task_ok2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_ok,
            output: json!("success"),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_fail,
            error: "slot_1_failed".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_ok2,
            output: json!("also_success"),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_collect_all, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 3, "collect-all should return all 3 slots");
            // Slot 0: ok
            assert!(results[0].get("ok").is_some(), "slot 0 should be ok");
            assert_eq!(results[0]["ok"], json!("success"));
            // Slot 1: failed
            assert!(results[1].get("err").is_some(), "slot 1 should be err");
            let err_msg = results[1]["err"].as_str().unwrap();
            assert!(
                err_msg.contains("slot_1_failed"),
                "slot 1 error message mismatch"
            );
            // Slot 2: ok
            assert!(results[2].get("ok").is_some(), "slot 2 should be ok");
            assert_eq!(results[2]["ok"], json!("also_success"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Empty fan-out: zero activities should return immediately with empty Vec.
#[tokio::test]
async fn fan_out_empty_activities_returns_empty_vec() {
    let exec_id = ExecutionId::new();

    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
    }];

    let outcome = run_workflow(exec_id, history, fan_out_empty, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            assert_eq!(
                output["count"],
                json!(0),
                "empty fan-out should return 0 results"
            );
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// First-time live execution: no history → fan-out suspends emitting commands.
/// Verifies that the correct `WorkflowCommand`s are emitted.
#[tokio::test]
async fn fan_out_raw_live_execution_emits_marker_and_schedule_commands() {
    let exec_id = ExecutionId::new();

    // Only WorkflowStarted — no activity history, no marker
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
    }];

    let outcome = run_workflow(exec_id, history, fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            // First command should be RecordMarker for the fan-out count
            let has_marker = commands.iter().any(|c| {
                matches!(c, autumn_harvest::context::WorkflowCommand::RecordMarker {
                    name, ..
                } if name.starts_with("fan_out:"))
            });
            assert!(
                has_marker,
                "should emit a fan_out marker command; got: {commands:?}"
            );

            // Should have 3 ScheduleActivity commands
            let schedule_count = commands
                .iter()
                .filter(|c| {
                    matches!(
                        c,
                        autumn_harvest::context::WorkflowCommand::ScheduleActivity { .. }
                    )
                })
                .count();
            assert_eq!(
                schedule_count, 3,
                "should emit 3 ScheduleActivity commands; got {commands:?}"
            );
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}

/// Non-determinism detection: fan-out called with different count than recorded.
/// The workflow changed from 3 to 2 activities between deploy and replay.
#[tokio::test]
async fn fan_out_count_mismatch_returns_non_deterministic_error() {
    let exec_id = ExecutionId::new();
    let id_a = ActivityExecId::new();
    let id_b = ActivityExecId::new();
    let id_c = ActivityExecId::new();

    // History recorded 3 activities in the fan-out
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64), // recorded 3
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_a,
            name: "task_0".into(),
            input: json!(0u64),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_b,
            name: "task_1".into(),
            input: json!(1u64),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_c,
            name: "task_2".into(),
            input: json!(2u64),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_a,
            output: json!("r0"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_b,
            output: json!("r1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_c,
            output: json!("r2"),
        },
    ];

    // New code passes input=2 which creates only 2 activities (changed from 3)
    let outcome = run_workflow(exec_id, history, fan_out_count_changed, json!(2u64)).await;

    match outcome {
        WorkflowOutcome::Failed { error } => {
            assert!(
                error.to_lowercase().contains("non-deterministic")
                    || error.to_lowercase().contains("fan_out")
                    || error.to_lowercase().contains("count"),
                "error should mention non-determinism or fan_out count; got: {error}"
            );
        }
        other => panic!("expected Failed (non-determinism), got {other:?}"),
    }
}

/// Fan-out with dynamic N derived from a prior activity output.
/// Verifies replay works correctly when N is derived from earlier state.
#[tokio::test]
async fn fan_out_dynamic_from_prior_activity_replays_correctly() {
    let exec_id = ExecutionId::new();
    let list_id = ActivityExecId::new();
    let item_ids: Vec<ActivityExecId> = (0..3).map(|_| ActivityExecId::new()).collect();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        // Prior activity returns a list of 3 items
        WorkflowEvent::ActivityScheduled {
            activity_id: list_id,
            name: "list_items".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: list_id,
            output: json!([1, 2, 3]),
        },
        // Fan-out marker: 3 items
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: item_ids[0],
            name: "process_item".into(),
            input: json!(1),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: item_ids[1],
            name: "process_item".into(),
            input: json!(2),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: item_ids[2],
            name: "process_item".into(),
            input: json!(3),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: item_ids[0],
            output: json!("done_1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: item_ids[1],
            output: json!("done_2"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: item_ids[2],
            output: json!("done_3"),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_dynamic_from_prior, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            let processed = output["processed"].as_array().unwrap();
            assert_eq!(processed.len(), 3, "should process 3 items");
            assert_eq!(processed[0], json!("done_1"));
            assert_eq!(processed[1], json!("done_2"));
            assert_eq!(processed[2], json!("done_3"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Cancellation test: workflow cancelled → fan-out returns Cancelled.
#[tokio::test]
async fn fan_out_cancelled_workflow_returns_cancelled_error() {
    let exec_id = ExecutionId::new();

    // History has a WorkflowCancelled event → fan-out should surface Cancelled
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "user_requested".into(),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error } => {
            assert!(
                error.contains("cancelled") || error.contains("cancel"),
                "cancelled workflow fan-out should report cancellation; got: {error}"
            );
        }
        other => panic!("expected Failed (Cancelled), got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// WorkflowContext unit tests (no executor needed)
// ---------------------------------------------------------------------------

/// Typed fan-out: `execute_activity_fan_out` accepts `(&ActivityInfo, I)` pairs.
#[tokio::test]
async fn fan_out_typed_single_activity_type_replays_correctly() {
    use autumn_harvest::info::ActivityInfo;

    let info = ActivityInfo {
        name: "echo",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let exec_id = ExecutionId::new();
    let id_1 = ActivityExecId::new();
    let id_2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_1,
            name: "echo".into(),
            input: json!("hello"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_2,
            name: "echo".into(),
            input: json!("world"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_1,
            output: json!("hello"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_2,
            output: json!("world"),
        },
    ];

    let ctx = WorkflowContext::for_replay(exec_id, history);
    let results: Vec<String> = ctx
        .execute_activity_fan_out(&info, vec!["hello".to_string(), "world".to_string()])
        .await
        .expect("fan-out should succeed");

    assert_eq!(results, vec!["hello".to_string(), "world".to_string()]);
}

/// Typed collect-all fan-out: mixed results returned as `Vec<Result<O, String>>`.
#[tokio::test]
async fn fan_out_typed_collect_all_returns_per_slot_results() {
    use autumn_harvest::info::ActivityInfo;

    let info = ActivityInfo {
        name: "maybe_fail",
        module: "tests",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: Some("default"),
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let exec_id = ExecutionId::new();
    let id_1 = ActivityExecId::new();
    let id_2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_1,
            name: "maybe_fail".into(),
            input: json!("ok_input"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: id_2,
            name: "maybe_fail".into(),
            input: json!("fail_input"),
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: id_1,
            output: json!("ok_output"),
        },
        WorkflowEvent::ActivityFailed {
            activity_id: id_2,
            error: "deliberate_failure".into(),
            attempt: 1,
            error_type: "Error".into(),
            non_retryable: false,
            details: None,
        },
    ];

    let ctx = WorkflowContext::for_replay(exec_id, history);
    let results: Vec<Result<Value, String>> = ctx
        .execute_activity_fan_out_collect(&info, vec![json!("ok_input"), json!("fail_input")])
        .await
        .expect("collect should not fail at the workflow level");

    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok(), "slot 0 should be ok");
    assert_eq!(results[0].as_ref().unwrap(), &json!("ok_output"));
    assert!(results[1].is_err(), "slot 1 should be err");
    assert!(
        results[1]
            .as_ref()
            .unwrap_err()
            .contains("deliberate_failure")
    );
}

/// Second fan-out call in the same workflow gets a different sequence number.
/// Verifies the sequence counter increments correctly for replay.
#[tokio::test]
async fn fan_out_two_groups_in_same_workflow() {
    fn two_fan_outs<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let batch1 = ctx
                .execute_activity_fan_out_raw(vec![
                    ("a1".to_string(), json!(null), "default".to_string()),
                    ("a2".to_string(), json!(null), "default".to_string()),
                ])
                .await
                .map_err(|e| e.to_string())?;
            let batch2 = ctx
                .execute_activity_fan_out_raw(vec![(
                    "b1".to_string(),
                    json!(null),
                    "default".to_string(),
                )])
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "b1": batch1, "b2": batch2 }))
        })
    }

    let exec_id = ExecutionId::new();
    let a1 = ActivityExecId::new();
    let a2 = ActivityExecId::new();
    let b1 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
        },
        // First fan-out group (seq=1, count=2)
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(2u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a1,
            name: "a1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: a2,
            name: "a2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a1,
            output: json!("ra1"),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: a2,
            output: json!("ra2"),
        },
        // Second fan-out group (seq=2, count=1)
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:2".into(),
            details: json!(1u64),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: b1,
            name: "b1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: b1,
            output: json!("rb1"),
        },
    ];

    let outcome = run_workflow(exec_id, history, two_fan_outs, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output } => {
            let b1 = output["b1"].as_array().unwrap();
            let b2 = output["b2"].as_array().unwrap();
            assert_eq!(b1.len(), 2);
            assert_eq!(b2.len(), 1);
            assert_eq!(b1[0], json!("ra1"));
            assert_eq!(b1[1], json!("ra2"));
            assert_eq!(b2[0], json!("rb1"));
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}
