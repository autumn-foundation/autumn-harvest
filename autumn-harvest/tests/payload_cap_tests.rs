//! Payload size cap tests (issue #252).
//!
//! Red phase: these tests describe the expected behaviour of the payload
//! capping feature. They are written before the implementation so they fail
//! first, then pass after the green phase.

use autumn_harvest::builder::HarvestBuilder;
use autumn_harvest::context::WorkflowContext;
use autumn_harvest::error::{HarvestError, PayloadKind};
use autumn_harvest::info::{ActivityInfo, WorkflowInfo};
use autumn_harvest::telemetry::{METRIC_PAYLOAD_BYTES, METRIC_PAYLOAD_REJECTED};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_large_json(bytes: usize) -> Value {
    let s = "x".repeat(bytes);
    json!({ "data": s })
}

fn fake_activity_info(name: &'static str) -> ActivityInfo {
    ActivityInfo {
        name,
        module: "test",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: None,
        max_result_bytes: None,
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    }
}

fn fake_workflow_info(name: &'static str) -> WorkflowInfo {
    WorkflowInfo {
        name,
        module: "test",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: None,
        max_input_bytes: None,
        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    }
}

// ---------------------------------------------------------------------------
// AC: PayloadKind enum exists with all 7 variants
// ---------------------------------------------------------------------------

#[test]
fn payload_kind_all_variants_exist() {
    // All 7 variants must exist and be constructable.
    let _ = PayloadKind::ActivityInput;
    let _ = PayloadKind::ActivityResult;
    let _ = PayloadKind::SignalPayload;
    let _ = PayloadKind::WorkflowInput;
    let _ = PayloadKind::ChildWorkflowInput;
    let _ = PayloadKind::ChildWorkflowResult;
    let _ = PayloadKind::SideEffectValue;
}

#[test]
fn payload_kind_is_non_exhaustive() {
    // The non-exhaustive attribute means we can construct known variants
    // but a match without a wildcard arm won't compile. We test via
    // the Display (Debug) representation.
    let kind = PayloadKind::ActivityInput;
    let debug_str = format!("{kind:?}");
    assert!(debug_str.contains("ActivityInput"));
}

// ---------------------------------------------------------------------------
// AC: PayloadTooLarge error variant has all required fields
// ---------------------------------------------------------------------------

#[test]
fn payload_too_large_variant_has_all_fields() {
    let e = HarvestError::PayloadTooLarge {
        kind: PayloadKind::ActivityInput,
        observed_bytes: 5_000_000,
        cap_bytes: 2_097_152,
        workflow_type: "onboarding".to_string(),
        activity_name: Some("send_email".to_string()),
    };
    let msg = e.to_string();
    assert!(
        msg.contains("payload too large")
            || msg.contains("PayloadTooLarge")
            || msg.contains("onboarding"),
        "Error message should mention the relevant context: {msg}"
    );
    assert!(
        msg.contains("5000000") || msg.contains("5_000_000"),
        "should include observed bytes: {msg}"
    );
    assert!(
        msg.contains("2097152") || msg.contains("2_097_152"),
        "should include cap bytes: {msg}"
    );
}

#[test]
fn payload_too_large_with_no_activity_name() {
    let e = HarvestError::PayloadTooLarge {
        kind: PayloadKind::WorkflowInput,
        observed_bytes: 3_000_000,
        cap_bytes: 2_097_152,
        workflow_type: "billing_report".to_string(),
        activity_name: None,
    };
    let msg = e.to_string();
    assert!(!msg.is_empty(), "Error display should not be empty: {msg}");
}

#[test]
fn payload_too_large_is_non_exhaustive() {
    // We can pattern match on PayloadTooLarge with ..
    match (HarvestError::PayloadTooLarge {
        kind: PayloadKind::SignalPayload,
        observed_bytes: 1000,
        cap_bytes: 500,
        workflow_type: "wf".to_string(),
        activity_name: None,
    }) {
        HarvestError::PayloadTooLarge {
            kind,
            observed_bytes,
            cap_bytes,
            ..
        } => {
            assert_eq!(kind, PayloadKind::SignalPayload);
            assert_eq!(observed_bytes, 1000);
            assert_eq!(cap_bytes, 500);
        }
        _ => panic!("should match PayloadTooLarge"),
    }
}

// ---------------------------------------------------------------------------
// AC: HarvestBuilder gains four configurable knobs
// ---------------------------------------------------------------------------

#[test]
fn builder_default_max_activity_input_bytes() {
    let built = HarvestBuilder::new().build();
    // Default: 2 MiB
    assert_eq!(
        built.max_activity_input_bytes,
        2 * 1024 * 1024,
        "Default max_activity_input_bytes must be 2 MiB"
    );
}

#[test]
fn builder_default_max_activity_result_bytes() {
    let built = HarvestBuilder::new().build();
    // Default: 2 MiB
    assert_eq!(
        built.max_activity_result_bytes,
        2 * 1024 * 1024,
        "Default max_activity_result_bytes must be 2 MiB"
    );
}

#[test]
fn builder_default_max_signal_payload_bytes() {
    let built = HarvestBuilder::new().build();
    // Default: 256 KiB
    assert_eq!(
        built.max_signal_payload_bytes,
        256 * 1024,
        "Default max_signal_payload_bytes must be 256 KiB"
    );
}

#[test]
fn builder_default_max_workflow_input_bytes() {
    let built = HarvestBuilder::new().build();
    // Default: 2 MiB
    assert_eq!(
        built.max_workflow_input_bytes,
        2 * 1024 * 1024,
        "Default max_workflow_input_bytes must be 2 MiB"
    );
}

#[test]
fn builder_custom_payload_caps_survive_build() {
    let built = HarvestBuilder::new()
        .max_activity_input_bytes(4 * 1024 * 1024) // 4 MiB
        .max_activity_result_bytes(8 * 1024 * 1024) // 8 MiB
        .max_signal_payload_bytes(512 * 1024) // 512 KiB
        .max_workflow_input_bytes(1024 * 1024) // 1 MiB
        .build();

    assert_eq!(built.max_activity_input_bytes, 4 * 1024 * 1024);
    assert_eq!(built.max_activity_result_bytes, 8 * 1024 * 1024);
    assert_eq!(built.max_signal_payload_bytes, 512 * 1024);
    assert_eq!(built.max_workflow_input_bytes, 1024 * 1024);
}

// ---------------------------------------------------------------------------
// AC: WorkflowInfo gains max_input_bytes field
// ---------------------------------------------------------------------------

#[test]
fn workflow_info_has_max_input_bytes_field() {
    let info = WorkflowInfo {
        name: "test_wf",
        module: "test",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: None,
        max_input_bytes: None,

        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    };
    assert!(info.max_input_bytes.is_none());

    let info_with_cap = WorkflowInfo {
        name: "large_wf",
        module: "test",
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
        execution_timeout: None,
        sla: None,
        concurrency: None,
        max_input_bytes: Some(8 * 1024 * 1024),

        owner: None,
        runbook_url: None,
        severity: None,
        description: None,
        input_schema: None,
        output_schema: None,
        error_schema: None,
    };
    assert_eq!(info_with_cap.max_input_bytes, Some(8 * 1024 * 1024));
}

// ---------------------------------------------------------------------------
// AC: ActivityInfo gains max_input_bytes and max_result_bytes fields
// ---------------------------------------------------------------------------

#[test]
fn activity_info_has_max_input_and_result_bytes_fields() {
    let info = ActivityInfo {
        name: "test_act",
        module: "test",
        default_retry_policy: None,
        default_start_to_close: None,
        default_heartbeat_timeout: None,
        default_schedule_to_start: None,
        default_schedule_to_close: None,
        default_queue: None,
        max_concurrent: None,
        concurrency_key: None,
        is_local: false,
        max_input_bytes: Some(4 * 1024 * 1024),
        max_result_bytes: Some(8 * 1024 * 1024),
        rate_limit_rps: None,
        rate_limit_burst: None,
        rate_limit_key: None,
        circuit_breaker: None,
        requires: None,
        handler: |_ctx, input| Box::pin(async move { Ok(input) }),
    };
    assert_eq!(info.max_input_bytes, Some(4 * 1024 * 1024));
    assert_eq!(info.max_result_bytes, Some(8 * 1024 * 1024));
}

// ---------------------------------------------------------------------------
// AC: Telemetry metric constants exist
// ---------------------------------------------------------------------------

#[test]
fn metric_payload_bytes_constant_exists() {
    assert_eq!(METRIC_PAYLOAD_BYTES, "harvest.payload.bytes");
}

#[test]
fn metric_payload_rejected_constant_exists() {
    assert_eq!(METRIC_PAYLOAD_REJECTED, "harvest.payload.rejected");
}

// ---------------------------------------------------------------------------
// AC: Activity input cap enforced at schedule time
// ---------------------------------------------------------------------------

#[tokio::test]
async fn context_rejects_oversized_activity_input_at_schedule_time() {
    // A 3 MiB input should be rejected when cap is 1 MiB
    let ctx = WorkflowContext::new_test().with_payload_caps(
        1024 * 1024,
        2 * 1024 * 1024,
        256 * 1024,
        2 * 1024 * 1024,
    );

    let oversized = make_large_json(2 * 1024 * 1024); // ~2 MiB JSON (above 1 MiB cap)

    // Spawn in background because execute_activity_raw suspends via oneshot
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.execute_activity_raw("send_email", oversized, "default"),
    )
    .await;

    // Should get a PayloadTooLarge error, NOT a timeout
    match result {
        Ok(Err(HarvestError::PayloadTooLarge { kind, .. })) => {
            assert_eq!(kind, PayloadKind::ActivityInput);
        }
        Ok(Ok(_)) => panic!("Should have rejected oversized input"),
        Ok(Err(e)) => panic!("Wrong error: {e}"),
        Err(timeout) => {
            panic!("Timed out ({timeout}) — the cap check should be synchronous before the suspend")
        }
    }
}

#[tokio::test]
async fn context_accepts_activity_input_within_cap() {
    let ctx = WorkflowContext::new_test().with_payload_caps(
        4 * 1024 * 1024,
        4 * 1024 * 1024,
        256 * 1024,
        4 * 1024 * 1024,
    );

    let small_input = json!({ "email": "user@example.com" });

    // Should NOT get a PayloadTooLarge error — should either succeed or suspend (timeout)
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.execute_activity_raw("send_email", small_input, "default"),
    )
    .await;

    // A timeout means it properly suspended (waiting for a worker to complete the activity)
    // which is correct — cap check passed
    match result {
        Err(_timeout) => {} // Correctly suspended — cap check passed
        Ok(Err(HarvestError::PayloadTooLarge { .. })) => {
            panic!("Should not reject small input as PayloadTooLarge")
        }
        Ok(Err(HarvestError::Cancelled(_)) | Ok(_)) => {}
        Ok(Err(e)) => panic!("Unexpected error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// AC: side_effect value cap enforced at recording time
// ---------------------------------------------------------------------------

#[test]
fn context_rejects_oversized_side_effect_value() {
    let ctx = WorkflowContext::new_test().with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        256 * 1024,
        1024 * 1024,
    );

    // Make a large string that exceeds the cap
    let large_value = "y".repeat(2 * 1024 * 1024); // 2 MiB string

    let result = ctx.side_effect("big_value", || large_value.clone());
    match result {
        Err(HarvestError::PayloadTooLarge { kind, .. }) => {
            assert_eq!(kind, PayloadKind::SideEffectValue);
        }
        Ok(_) => panic!("Should have rejected oversized side effect value"),
        Err(e) => panic!("Wrong error type: {e}"),
    }
}

#[test]
fn context_accepts_small_side_effect_value() {
    let ctx = WorkflowContext::new_test().with_payload_caps(
        2 * 1024 * 1024,
        2 * 1024 * 1024,
        256 * 1024,
        2 * 1024 * 1024,
    );

    let small_value = "hello".to_string();
    let result: autumn_harvest::error::HarvestResult<String> =
        ctx.side_effect("small_value", || small_value.clone());
    assert!(result.is_ok(), "Small side effect value should be accepted");
}

// ---------------------------------------------------------------------------
// AC: Replay safety — caps not applied when reading from history
// ---------------------------------------------------------------------------

#[test]
fn replay_does_not_recheck_cap_on_existing_events() {
    use autumn_harvest::event::WorkflowEvent;

    // Build a history that contains a side_effect with a large value (was accepted before)
    let large_payload = json!("x".repeat(3 * 1024 * 1024)); // 3 MiB — above cap
    let events = vec![
        WorkflowEvent::WorkflowStarted {
            input: json!(null),
            timestamp: chrono::Utc::now(),
        },
        WorkflowEvent::MarkerRecorded {
            name: "side_effect:big_value".to_string(),
            details: large_payload,
        },
    ];

    let exec_id = autumn_harvest::types::ExecutionId::new();

    // Create a replay context with a TIGHT cap (1 MiB) — LOWER than the stored payload
    let ctx = WorkflowContext::for_replay(exec_id, events).with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        256 * 1024,
        1024 * 1024,
    );

    // Replaying the side_effect should return the stored large value WITHOUT
    // triggering a PayloadTooLarge error — the cap only applies to new writes.
    let result: autumn_harvest::error::HarvestResult<String> =
        ctx.side_effect("big_value", || "fallback".to_string());

    match result {
        Ok(v) => {
            // The stored value should be returned intact
            assert_eq!(
                v.len(),
                3 * 1024 * 1024,
                "Should return the full stored value"
            );
        }
        Err(HarvestError::PayloadTooLarge { .. }) => {
            panic!("Must NOT reject stored payloads during replay — replay safety broken!");
        }
        Err(e) => panic!("Unexpected error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// AC: per-activity cap raises (never lowers) the global cap
// ---------------------------------------------------------------------------

#[tokio::test]
async fn activity_cap_override_raises_global_cap() {
    // Global cap: 1 MiB (tight)
    // Per-activity override: 4 MiB (raised)
    // Payload: 2 MiB — should pass with override, fail without
    let ctx = WorkflowContext::new_test()
        .with_payload_caps(1024 * 1024, 2 * 1024 * 1024, 256 * 1024, 2 * 1024 * 1024)
        .with_activity_input_override("big_activity", 4 * 1024 * 1024);

    let two_mib = make_large_json(2 * 1024 * 1024);

    // With override for "big_activity", 2 MiB should pass (suspend, not reject)
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.execute_activity_raw("big_activity", two_mib, "default"),
    )
    .await;

    match result {
        Err(_timeout) => {} // Correctly suspended (cap check passed, waiting for worker)
        Ok(Err(HarvestError::PayloadTooLarge { .. })) => {
            panic!("Should NOT reject — the per-activity override raises cap to 4 MiB")
        }
        Ok(Err(HarvestError::Cancelled(_)) | Ok(_)) => {}
        Ok(Err(e)) => panic!("Unexpected error: {e}"),
    }
}

#[tokio::test]
async fn activity_cap_override_does_not_lower_global_cap() {
    // Global cap: 4 MiB (generous)
    // "Per-activity override": 1 MiB (trying to lower — should be ignored, global wins)
    // Payload: 2 MiB — should STILL pass because effective cap = max(4 MiB, 1 MiB) = 4 MiB
    let ctx = WorkflowContext::new_test()
        .with_payload_caps(
            4 * 1024 * 1024,
            4 * 1024 * 1024,
            256 * 1024,
            4 * 1024 * 1024,
        )
        .with_activity_input_override("small_activity", 1024 * 1024); // attempt to lower

    let two_mib = make_large_json(2 * 1024 * 1024);

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.execute_activity_raw("small_activity", two_mib, "default"),
    )
    .await;

    // 2 MiB is within the 4 MiB global cap, so should suspend (pass)
    match result {
        Err(_timeout) => {} // Correctly suspended — 2 MiB is within 4 MiB global cap
        Ok(Err(HarvestError::PayloadTooLarge { .. })) => {
            panic!("Global cap (4 MiB) must not be lowered by per-activity override (1 MiB)")
        }
        Ok(Err(HarvestError::Cancelled(_)) | Ok(_)) => {}
        Ok(Err(e)) => panic!("Unexpected error: {e}"),
    }
}

// ---------------------------------------------------------------------------
// AC: child workflow input cap enforced at child-start time
// ---------------------------------------------------------------------------

#[tokio::test]
async fn context_rejects_oversized_child_workflow_input() {
    let ctx = WorkflowContext::new_test().with_payload_caps(
        1024 * 1024,
        1024 * 1024,
        256 * 1024,
        1024 * 1024,
    );

    let oversized = make_large_json(2 * 1024 * 1024); // 2 MiB, above 1 MiB cap

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        ctx.spawn_child_workflow_raw("child_workflow", oversized),
    )
    .await;

    match result {
        Ok(Err(HarvestError::PayloadTooLarge { kind, .. })) => {
            assert_eq!(kind, PayloadKind::ChildWorkflowInput);
        }
        Ok(Ok(_)) => panic!("Should reject oversized child workflow input"),
        Ok(Err(e)) => panic!("Wrong error: {e}"),
        Err(timeout) => panic!("Timed out ({timeout}) — cap check should be synchronous"),
    }
}

// ---------------------------------------------------------------------------
// AC: parse_byte_size utility function
// ---------------------------------------------------------------------------

#[test]
fn parse_byte_size_parses_kib() {
    assert_eq!(autumn_harvest::parse_byte_size("256KiB"), Some(256 * 1024));
    assert_eq!(autumn_harvest::parse_byte_size("512KiB"), Some(512 * 1024));
}

#[test]
fn parse_byte_size_parses_mib() {
    assert_eq!(
        autumn_harvest::parse_byte_size("2MiB"),
        Some(2 * 1024 * 1024)
    );
    assert_eq!(
        autumn_harvest::parse_byte_size("8MiB"),
        Some(8 * 1024 * 1024)
    );
}

#[test]
fn parse_byte_size_parses_mb() {
    assert_eq!(autumn_harvest::parse_byte_size("4MB"), Some(4 * 1_000_000));
}

#[test]
fn parse_byte_size_parses_plain_bytes() {
    assert_eq!(autumn_harvest::parse_byte_size("1024"), Some(1024));
}

#[test]
fn parse_byte_size_rejects_invalid() {
    assert_eq!(autumn_harvest::parse_byte_size("invalid"), None);
    assert_eq!(autumn_harvest::parse_byte_size(""), None);
    assert_eq!(autumn_harvest::parse_byte_size("5x"), None);
}

// ---------------------------------------------------------------------------
// AC: #[workflow(max_input_bytes = "...")] macro attribute
// (tested via macros_workflow.rs integration test file, not here)
// The following test verifies that the WorkflowInfo field is accessible when
// produced from the builder without the macro.
// ---------------------------------------------------------------------------

#[test]
fn workflow_info_max_input_bytes_none_by_default() {
    let info = fake_workflow_info("test_wf");
    assert!(
        info.max_input_bytes.is_none(),
        "max_input_bytes should default to None (uses global cap)"
    );
}

#[test]
fn activity_info_max_bytes_none_by_default() {
    let info = fake_activity_info("test_act");
    assert!(info.max_input_bytes.is_none());
    assert!(info.max_result_bytes.is_none());
}

// ---------------------------------------------------------------------------
// AC: MetricsRecorder has record_payload_observed and record_payload_rejected
// ---------------------------------------------------------------------------

#[test]
fn metrics_recorder_has_payload_methods() {
    use autumn_harvest::error::PayloadKind;
    use autumn_harvest::telemetry::{MetricsRecorder, NoOpMetrics};

    let recorder = NoOpMetrics;
    // These should compile and not panic
    recorder.record_payload_observed(
        &PayloadKind::ActivityInput,
        "onboarding",
        Some("send_email"),
        1024,
    );
    recorder.record_payload_rejected(&PayloadKind::ActivityInput, "onboarding");
}
