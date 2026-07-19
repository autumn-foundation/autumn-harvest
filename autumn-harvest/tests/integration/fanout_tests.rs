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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Completed { output, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Failed { error, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Completed { output, .. } => {
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
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    let outcome = run_workflow(exec_id, history, fan_out_empty, Value::Null).await;

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
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
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Failed { error, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Completed { output, .. } => {
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "user_requested".into(),
        },
    ];

    let outcome = run_workflow(exec_id, history, fan_out_three_parallel, Value::Null).await;

    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
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
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let exec_id = ExecutionId::new();
    let id_1 = ActivityExecId::new();
    let id_2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        rate_limit_key_expr: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };

    let exec_id = ExecutionId::new();
    let id_1 = ActivityExecId::new();
    let id_2 = ActivityExecId::new();

    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
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
        WorkflowOutcome::Completed { output, .. } => {
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

// ===========================================================================
// Bounded / windowed activity fan-out (issue #750)
// ===========================================================================

use autumn_harvest::context::WorkflowCommand;
use autumn_harvest::info::WorkflowHandlerFn;

/// Windowed raw fan-out handler. Reads `{ "n": <count>, "w": <window> }` from
/// the workflow input (recorded state), builds `n` distinct activities each
/// with input `json!(i)`, and fans out with window `w`.
fn windowed_raw_handler<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input["n"].as_u64().unwrap_or(0)).unwrap_or(0);
        let w = usize::try_from(input["w"].as_u64().unwrap_or(1)).unwrap_or(1);
        let activities: Vec<_> = (0..n)
            .map(|i| (format!("task_{i}"), json!(i), "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw_windowed(activities, w)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

/// Windowed collect-all raw fan-out handler.
fn windowed_collect_handler<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input["n"].as_u64().unwrap_or(0)).unwrap_or(0);
        let w = usize::try_from(input["w"].as_u64().unwrap_or(1)).unwrap_or(1);
        let activities: Vec<_> = (0..n)
            .map(|i| (format!("task_{i}"), json!(i), "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_collect_raw_windowed(activities, w)
            .await
            .map_err(|e| e.to_string())?;
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

/// Drive a workflow through repeated `run_workflow` cycles, feeding each
/// suspension's `ScheduleActivity` commands back as recorded
/// `ActivityScheduled` + `ActivityCompleted`/`ActivityFailed` events until the
/// workflow reaches a terminal outcome.
///
/// `output_fn(name, input) -> Ok(output) | Err(error)` decides each activity's
/// recorded result. Returns `(outcome, max_scheduled_per_suspension,
/// total_scheduled, history)`.
async fn drive_windowed<F>(
    handler: WorkflowHandlerFn,
    input: Value,
    output_fn: F,
) -> (WorkflowOutcome, usize, usize, Vec<WorkflowEvent>)
where
    F: Fn(&str, &Value) -> Result<Value, String>,
{
    let exec_id = ExecutionId::new();
    let mut history = vec![WorkflowEvent::WorkflowStarted {
        input: input.clone(),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    let mut max_per_suspension = 0usize;
    let mut total_scheduled = 0usize;

    for _guard in 0..1000 {
        let outcome = run_workflow(exec_id, history.clone(), handler, input.clone()).await;
        match outcome {
            WorkflowOutcome::Suspended { commands } => {
                let sched_this_wave = commands
                    .iter()
                    .filter(|c| matches!(c, WorkflowCommand::ScheduleActivity { .. }))
                    .count();
                max_per_suspension = max_per_suspension.max(sched_this_wave);

                for cmd in &commands {
                    match cmd {
                        WorkflowCommand::RecordMarker { name, details } => {
                            history.push(WorkflowEvent::MarkerRecorded {
                                name: name.clone(),
                                details: details.clone(),
                            });
                        }
                        WorkflowCommand::ScheduleActivity {
                            activity_id,
                            name,
                            input,
                            queue,
                            ..
                        } => {
                            history.push(WorkflowEvent::ActivityScheduled {
                                activity_id: *activity_id,
                                name: name.clone(),
                                input: input.clone(),
                                queue: queue.clone(),
                            });
                            total_scheduled += 1;
                            match output_fn(name, input) {
                                Ok(output) => history.push(WorkflowEvent::ActivityCompleted {
                                    activity_id: *activity_id,
                                    output,
                                }),
                                Err(error) => history.push(WorkflowEvent::ActivityFailed {
                                    activity_id: *activity_id,
                                    error,
                                    attempt: 1,
                                    error_type: "Error".into(),
                                    non_retryable: false,
                                    details: None,
                                }),
                            }
                        }
                        other => panic!("unexpected command in windowed fan-out drive: {other:?}"),
                    }
                }
            }
            terminal => return (terminal, max_per_suspension, total_scheduled, history),
        }
    }
    panic!("windowed fan-out drive did not terminate within 1000 cycles");
}

/// AC2 (first wave): W=2 over 5 inputs — a single `run_workflow` cycle from a
/// bare `WorkflowStarted` suspends with exactly 2 `ScheduleActivity` and a
/// `fan_out:` marker recording N=5 (never W).
#[tokio::test]
async fn windowed_fan_out_first_wave_schedules_at_most_window() {
    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: json!({ "n": 5, "w": 2 }),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];

    let outcome = run_workflow(exec_id, history, windowed_raw_handler, json!({ "n": 5, "w": 2 })).await;

    match outcome {
        WorkflowOutcome::Suspended { commands } => {
            let schedule_count = commands
                .iter()
                .filter(|c| matches!(c, WorkflowCommand::ScheduleActivity { .. }))
                .count();
            assert_eq!(schedule_count, 2, "first wave must schedule exactly W=2; got {commands:?}");

            let marker = commands.iter().find_map(|c| match c {
                WorkflowCommand::RecordMarker { name, details } if name.starts_with("fan_out:") => {
                    Some(details.clone())
                }
                _ => None,
            });
            assert_eq!(marker, Some(json!(5u64)), "marker must record N=5, never W");
        }
        other => panic!("expected Suspended, got {other:?}"),
    }
}

/// AC2 (core): W=3 over 10 inputs — EVERY suspension schedules at most W=3
/// activities, and all 10 inputs are ultimately processed in input order.
#[tokio::test]
async fn windowed_fan_out_at_most_window_in_flight_every_wave() {
    let (outcome, max_per_suspension, total_scheduled, _history) =
        drive_windowed(windowed_raw_handler, json!({ "n": 10, "w": 3 }), |_name, input| {
            Ok(json!(format!("done_{input}")))
        })
        .await;

    assert!(
        max_per_suspension <= 3,
        "no suspension may schedule more than W=3; peak was {max_per_suspension}"
    );
    assert_eq!(total_scheduled, 10, "all 10 inputs must be scheduled exactly once");

    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 10);
            for (i, r) in results.iter().enumerate() {
                assert_eq!(*r, json!(format!("done_{i}")), "slot {i} out of order");
            }
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// AC3: results are returned in input order (W=2 over [1..5], activity doubles).
#[tokio::test]
async fn windowed_fan_out_processes_all_inputs_in_order() {
    let (outcome, _max, total, _history) =
        drive_windowed(windowed_raw_handler, json!({ "n": 5, "w": 2 }), |_name, input| {
            let v = input.as_u64().unwrap();
            Ok(json!(v * 2))
        })
        .await;

    assert_eq!(total, 5);
    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results, &vec![json!(0), json!(2), json!(4), json!(6), json!(8)]);
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Project a command list to a comparable representation that ignores random
/// `activity_id`s and non-comparable `result_tx` channels.
fn project_commands(commands: &[WorkflowCommand]) -> Vec<String> {
    commands
        .iter()
        .filter_map(|c| match c {
            WorkflowCommand::RecordMarker { name, details } => {
                Some(format!("marker:{name}:{details}"))
            }
            WorkflowCommand::ScheduleActivity {
                name, input, queue, ..
            } => Some(format!("schedule:{name}:{input}:{queue}")),
            _ => None,
        })
        .collect()
}

/// AC4: `W >= N` produces command history **identical** to the unbounded path.
#[tokio::test]
async fn windowed_window_ge_count_identical_history_to_unbounded() {
    let exec_id = ExecutionId::new();
    let started = |input: Value| {
        vec![WorkflowEvent::WorkflowStarted {
            input,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }]
    };

    // Unbounded fan-out over 4 inputs.
    let unbounded_out = run_workflow(
        exec_id,
        started(Value::Null),
        fan_out_three_parallel_n4,
        Value::Null,
    )
    .await;
    // Windowed(W=100 >= N=4) over the same 4 inputs.
    let windowed_out = run_workflow(
        exec_id,
        started(json!({ "n": 4, "w": 100 })),
        windowed_raw_handler,
        json!({ "n": 4, "w": 100 }),
    )
    .await;

    let unbounded_cmds = match unbounded_out {
        WorkflowOutcome::Suspended { commands } => project_commands(&commands),
        other => panic!("unbounded expected Suspended, got {other:?}"),
    };
    let windowed_cmds = match windowed_out {
        WorkflowOutcome::Suspended { commands } => project_commands(&commands),
        other => panic!("windowed expected Suspended, got {other:?}"),
    };

    assert_eq!(
        unbounded_cmds, windowed_cmds,
        "W>=N must emit identical marker + N ScheduleActivity commands in the same order"
    );
}

/// Unbounded raw fan-out over 4 inputs named `task_{i}` with input `json!(i)`,
/// matching `windowed_raw_handler`'s activity shape so the two are comparable.
fn fan_out_three_parallel_n4<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let activities: Vec<_> = (0..4)
            .map(|i| (format!("task_{i}"), json!(i), "default".to_string()))
            .collect();
        let results = ctx
            .execute_activity_fan_out_raw(activities)
            .await
            .map_err(|e| e.to_string())?;
        Ok(json!({ "results": results }))
    })
}

/// AC5: `W == 0` clamps to 1 — never panics, hangs, or no-ops. Each suspension
/// schedules at most 1, all inputs processed.
#[tokio::test]
async fn windowed_fan_out_w_zero_clamps_to_one() {
    let (outcome, max_per_suspension, total, _history) =
        drive_windowed(windowed_raw_handler, json!({ "n": 3, "w": 0 }), |_name, input| {
            Ok(json!(format!("done_{input}")))
        })
        .await;

    assert!(
        max_per_suspension <= 1,
        "W=0 must clamp to 1; peak scheduled was {max_per_suspension}"
    );
    assert_eq!(total, 3, "all 3 inputs must still be processed");
    assert!(matches!(outcome, WorkflowOutcome::Completed { .. }));
}

/// Collect-all: W=2 over 4, slot 1 fails — returns 4 per-slot results (slot 1 =
/// Err) and wave 2 IS dispatched (all 4 scheduled).
#[tokio::test]
async fn windowed_fan_out_collect_all_processes_every_input_despite_failure() {
    let (outcome, _max, total, _history) =
        drive_windowed(windowed_collect_handler, json!({ "n": 4, "w": 2 }), |_name, input| {
            if *input == json!(1u64) {
                Err("slot_1_failed".into())
            } else {
                Ok(json!(format!("ok_{input}")))
            }
        })
        .await;

    assert_eq!(total, 4, "collect-all must dispatch every wave (all 4 inputs)");
    match outcome {
        WorkflowOutcome::Completed { output, .. } => {
            let results = output["results"].as_array().unwrap();
            assert_eq!(results.len(), 4);
            assert!(results[0].get("ok").is_some());
            assert!(results[1].get("err").is_some());
            assert!(results[1]["err"].as_str().unwrap().contains("slot_1_failed"));
            assert!(results[2].get("ok").is_some());
            assert!(results[3].get("ok").is_some());
        }
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Fail-fast: W=2 over 4, a wave-1 slot fails — returns Err and wave 2 is
/// NEVER dispatched (only 2 scheduled). Pins the fail-fast backpressure.
#[tokio::test]
async fn windowed_fan_out_fail_fast_stops_launching_after_wave_failure() {
    let (outcome, _max, total, _history) =
        drive_windowed(windowed_raw_handler, json!({ "n": 4, "w": 2 }), |_name, input| {
            if *input == json!(1u64) {
                Err("boom".into())
            } else {
                Ok(json!("ok"))
            }
        })
        .await;

    assert_eq!(
        total, 2,
        "fail-fast must not dispatch wave 2 after a wave-1 failure; scheduled {total}"
    );
    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(error.contains("boom"), "error should mention boom; got {error}");
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

/// Shared seq counter: a windowed fan-out then an unbounded fan-out → markers
/// `fan_out:1`, `fan_out:2`.
#[tokio::test]
async fn windowed_fan_out_shares_seq_counter() {
    fn windowed_then_unbounded<'a>(
        ctx: &'a WorkflowContext,
        _input: Value,
    ) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
        Box::pin(async move {
            let b1 = ctx
                .execute_activity_fan_out_raw_windowed(
                    vec![("w1".to_string(), json!(0), "default".to_string())],
                    2,
                )
                .await
                .map_err(|e| e.to_string())?;
            let b2 = ctx
                .execute_activity_fan_out_raw(vec![("u1".to_string(), json!(1), "default".to_string())])
                .await
                .map_err(|e| e.to_string())?;
            Ok(json!({ "b1": b1, "b2": b2 }))
        })
    }

    let (outcome, _max, _total, history) =
        drive_windowed(windowed_then_unbounded, Value::Null, |_name, _input| Ok(json!("r"))).await;

    assert!(matches!(outcome, WorkflowOutcome::Completed { .. }));
    let markers: Vec<String> = history
        .iter()
        .filter_map(|e| match e {
            WorkflowEvent::MarkerRecorded { name, .. } if name.starts_with("fan_out:") => {
                Some(name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(markers, vec!["fan_out:1".to_string(), "fan_out:2".to_string()]);
}

/// Empty windowed fan-out records `marker(count=0)` then returns empty (AC:
/// preserve empty-input ordering — marker recorded BEFORE the empty check).
#[tokio::test]
async fn windowed_fan_out_empty_records_marker_and_returns_empty() {
    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let ctx = WorkflowContext::for_replay(exec_id, history);

    let results = ctx
        .execute_activity_fan_out_raw_windowed(vec![], 2)
        .await
        .expect("empty windowed fan-out should succeed");
    assert!(results.is_empty(), "empty fan-out returns empty vec");

    let commands = ctx.drain_commands();
    let marker = commands.iter().find_map(|c| match c {
        WorkflowCommand::RecordMarker { name, details } if name.starts_with("fan_out:") => {
            Some(details.clone())
        }
        _ => None,
    });
    assert_eq!(marker, Some(json!(0u64)), "empty fan-out must record marker(count=0)");
}

/// Count mismatch: history recorded marker(count=3) but code (windowed) supplies
/// 2 → non-deterministic failure, same as unbounded.
#[tokio::test]
async fn windowed_fan_out_count_mismatch_non_deterministic() {
    let exec_id = ExecutionId::new();
    let ids: Vec<ActivityExecId> = (0..3).map(|_| ActivityExecId::new()).collect();

    let mut history = vec![
        WorkflowEvent::WorkflowStarted {
            input: json!({ "n": 2, "w": 2 }),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::MarkerRecorded {
            name: "fan_out:1".into(),
            details: json!(3u64), // recorded 3
        },
    ];
    for (i, id) in ids.iter().enumerate() {
        history.push(WorkflowEvent::ActivityScheduled {
            activity_id: *id,
            name: format!("task_{i}"),
            input: json!(i),
            queue: "default".into(),
        });
    }
    for (i, id) in ids.iter().enumerate() {
        history.push(WorkflowEvent::ActivityCompleted {
            activity_id: *id,
            output: json!(format!("r{i}")),
        });
    }

    // Code now supplies only 2 (windowed).
    let outcome = run_workflow(exec_id, history, windowed_raw_handler, json!({ "n": 2, "w": 2 })).await;
    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            let e = error.to_lowercase();
            assert!(
                e.contains("non-deterministic") || e.contains("fan_out") || e.contains("count"),
                "error should mention non-determinism/fan_out/count; got {error}"
            );
        }
        other => panic!("expected Failed (non-determinism), got {other:?}"),
    }
}

/// Cancellation: a cancelled ctx → `HarvestError::Cancelled` before dispatch.
#[tokio::test]
async fn windowed_fan_out_cancellation_returns_cancelled() {
    let exec_id = ExecutionId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: json!({ "n": 3, "w": 2 }),
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::WorkflowCancelled {
            reason: "user_requested".into(),
        },
    ];
    let outcome = run_workflow(exec_id, history, windowed_raw_handler, json!({ "n": 3, "w": 2 })).await;
    match outcome {
        WorkflowOutcome::Failed { error, .. } => {
            assert!(
                error.contains("cancel"),
                "cancelled windowed fan-out should report cancellation; got {error}"
            );
        }
        other => panic!("expected Failed (Cancelled), got {other:?}"),
    }
}
