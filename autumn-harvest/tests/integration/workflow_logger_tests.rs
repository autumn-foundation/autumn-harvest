/// Tests for issue #379: replay-safe workflow logger.
///
/// Red phase: these tests define the expected behaviour before implementation.
/// They fail until `WorkflowLogger`, `ctx.logger()`, `ctx.log_info`, `ctx.log_warn`,
/// `ctx.log_error`, and guardrail rule HVG009 are in place.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::executor::{WorkflowOutcome, run_workflow};
use autumn_harvest::guardrail::{RuleCategory, catalog, rule_by_id};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::layer::SubscriberExt;

// ── Tracing capture helpers ───────────────────────────────────────────────────

/// A visitor that extracts string values from tracing event fields.
struct FieldCapture {
    pub fields: std::collections::HashMap<String, String>,
}

impl tracing::field::Visit for FieldCapture {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        self.fields
            .insert(field.name().to_string(), s.trim_matches('"').to_string());
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

/// Captured event from the test subscriber.
#[derive(Debug)]
struct CapturedEvent {
    pub level: String,
    pub fields: std::collections::HashMap<String, String>,
}

/// A `tracing_subscriber::Layer` that records events from the workflow logger.
struct CapturingLayer {
    captured: Arc<Mutex<Vec<CapturedEvent>>>,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CapturingLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let meta = event.metadata();
        // Only capture events from the workflow logger target.
        if meta.target() != "autumn_harvest::context" {
            return;
        }
        let mut visitor = FieldCapture {
            fields: std::collections::HashMap::new(),
        };
        event.record(&mut visitor);
        self.captured.lock().unwrap().push(CapturedEvent {
            level: meta.level().to_string(),
            fields: visitor.fields,
        });
    }
}

/// Install a capturing subscriber for the duration of the test. Returns the
/// captured event list and a guard that restores the prior subscriber on drop.
fn install_capture() -> (Arc<Mutex<Vec<CapturedEvent>>>, DefaultGuard) {
    let captured = Arc::new(Mutex::new(Vec::<CapturedEvent>::new()));
    let layer = CapturingLayer {
        captured: Arc::clone(&captured),
    };
    let subscriber = tracing_subscriber::registry().with(layer);
    let guard = tracing::subscriber::set_default(subscriber);
    (captured, guard)
}

// ── Workflow helpers ──────────────────────────────────────────────────────────

/// Workflow that calls `ctx.logger().info("hello from logger")` once, then suspends.
fn logging_workflow_via_logger<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.logger().info("hello from logger");
        let result = ctx
            .execute_activity_raw("step_1", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

/// Workflow that calls `ctx.log_info`, `ctx.log_warn`, `ctx.log_error` convenience methods once, then suspends.
fn logging_workflow_direct<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.log_info("direct log info");
        ctx.log_warn("direct log warn");
        ctx.log_error("direct log error");
        let result = ctx
            .execute_activity_raw("step_1", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(result)
    })
}

/// Workflow that logs once on the live path, then suspends through 3 activities.
fn multi_suspend_logging_workflow<'a>(
    ctx: &'a WorkflowContext,
    _input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        ctx.logger().info("started");
        let r1 = ctx
            .execute_activity_raw("step_1", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r2 = ctx
            .execute_activity_raw("step_2", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        let r3 = ctx
            .execute_activity_raw("step_3", Value::Null, "default")
            .await
            .map_err(|e| e.to_string())?;
        Ok(serde_json::json!([r1, r2, r3]))
    })
}

/// Build a fully-completed history for `multi_suspend_logging_workflow`.
fn make_full_history() -> Vec<WorkflowEvent> {
    let act1 = ActivityExecId::new();
    let act2 = ActivityExecId::new();
    let act3 = ActivityExecId::new();
    vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act1,
            output: serde_json::json!("r1"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act2,
            name: "step_2".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act2,
            output: serde_json::json!("r2"),
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act3,
            name: "step_3".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act3,
            output: serde_json::json!("r3"),
        },
    ]
}

// ── Helper: create a live (non-replay) context ────────────────────────────────

fn live_ctx() -> WorkflowContext {
    WorkflowContext::for_replay(
        ExecutionId::new(),
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    )
}

// ── Tests: WorkflowLogger API surface ────────────────────────────────────────

#[test]
fn workflow_context_exposes_logger_method() {
    let ctx = live_ctx();
    let _logger = ctx.logger();
}

#[test]
fn workflow_logger_has_info_warn_error_methods() {
    let ctx = live_ctx();
    let logger = ctx.logger();
    logger.info("test info");
    logger.warn("test warn");
    logger.error("test error");
}

#[test]
fn workflow_context_has_direct_log_methods() {
    let ctx = live_ctx();
    ctx.log_info("info message");
    ctx.log_warn("warn message");
    ctx.log_error("error message");
}

// ── Tests: replay suppression ─────────────────────────────────────────────────

#[tokio::test]
async fn logger_emits_event_in_live_mode() {
    let (captured, _guard) = install_capture();

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let outcome = run_workflow(exec_id, history, logging_workflow_via_logger, Value::Null).await;
    assert!(
        matches!(outcome, WorkflowOutcome::Suspended { .. }),
        "expected suspension, got {outcome:?}"
    );

    let event_count = captured.lock().unwrap().len();
    assert_eq!(
        event_count, 1,
        "expected exactly 1 log event in live mode, got {event_count}"
    );
}

#[tokio::test]
async fn logger_suppresses_events_during_full_replay() {
    let (captured, _guard) = install_capture();

    let exec_id = ExecutionId::new();
    let act1 = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act1,
            output: Value::Null,
        },
    ];
    let outcome = run_workflow(exec_id, history, logging_workflow_via_logger, Value::Null).await;
    assert!(
        matches!(outcome, WorkflowOutcome::Completed { .. }),
        "expected completion, got {outcome:?}"
    );

    let event_count = captured.lock().unwrap().len();
    assert_eq!(
        event_count, 0,
        "expected 0 log events during full replay, got {event_count}"
    );
}

#[tokio::test]
async fn direct_log_methods_suppressed_during_replay() {
    let (captured, _guard) = install_capture();

    let exec_id = ExecutionId::new();
    let act1 = ActivityExecId::new();
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act1,
            output: Value::Null,
        },
    ];
    let outcome = run_workflow(exec_id, history, logging_workflow_direct, Value::Null).await;
    assert!(
        matches!(outcome, WorkflowOutcome::Completed { .. }),
        "expected completion, got {outcome:?}"
    );

    let event_count = captured.lock().unwrap().len();
    assert_eq!(
        event_count, 0,
        "expected 0 log events during full replay, got {event_count}"
    );
}

#[tokio::test]
async fn direct_log_methods_emit_in_live_mode() {
    let (captured, _guard) = install_capture();

    let exec_id = ExecutionId::new();
    let history = vec![WorkflowEvent::WorkflowStarted {
        input: Value::Null,
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    }];
    let outcome = run_workflow(exec_id, history, logging_workflow_direct, Value::Null).await;
    assert!(
        matches!(outcome, WorkflowOutcome::Suspended { .. }),
        "expected suspension, got {outcome:?}"
    );

    // log_info + log_warn + log_error = 3 events
    let event_count = captured.lock().unwrap().len();
    assert_eq!(
        event_count, 3,
        "expected 3 log events (info + warn + error) in live mode, got {event_count}"
    );
}

/// AC: A workflow that logs once and suspends N times emits exactly 1 log event.
///
/// Verifies that the number of log events equals the number of logger calls in
/// source (1), regardless of how many replay cycles the executor performs.
#[tokio::test]
async fn log_events_independent_of_replay_cycle_count() {
    let exec_id = ExecutionId::new();

    // ── First pass: live execution (only WorkflowStarted → suspension) ──
    let first_pass_count = {
        let (captured, _guard) = install_capture();
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
            multi_suspend_logging_workflow,
            Value::Null,
        )
        .await;
        assert!(matches!(outcome, WorkflowOutcome::Suspended { .. }));
        captured.lock().unwrap().len()
        // _guard drops here, uninstalling the first subscriber
    };
    assert_eq!(
        first_pass_count, 1,
        "live pass must emit exactly 1 log event"
    );

    // ── Replay pass: full history with all 3 activities already completed ──
    let replay_count = {
        let (captured2, _guard2) = install_capture();
        let history = make_full_history();
        let outcome = run_workflow(
            exec_id,
            history,
            multi_suspend_logging_workflow,
            Value::Null,
        )
        .await;
        assert!(
            matches!(outcome, WorkflowOutcome::Completed { .. }),
            "expected completion on full history replay"
        );
        captured2.lock().unwrap().len()
        // _guard2 drops here
    };
    assert_eq!(
        replay_count, 0,
        "replay pass must emit 0 log events, got {replay_count}"
    );
}

// ── Tests: auto-tagged fields ─────────────────────────────────────────────────

#[test]
fn logger_emits_execution_id_field() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    );

    let (captured, _guard) = install_capture();
    ctx.logger().info("test message");

    let (has_exec_id, keys_str) = captured
        .lock()
        .unwrap()
        .first()
        .map(|e| {
            let keys: Vec<_> = e.fields.keys().collect();
            (e.fields.contains_key("execution_id"), format!("{keys:?}"))
        })
        .expect("expected one log event");
    assert!(
        has_exec_id,
        "expected execution_id field, got fields: {keys_str}"
    );
}

#[test]
fn logger_emits_workflow_type_field() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    )
    .with_workflow_name("my_workflow");

    let (captured, _guard) = install_capture();
    ctx.logger().info("field test");

    let (wf_type, fields_str) = captured
        .lock()
        .unwrap()
        .first()
        .map(|e| {
            (
                e.fields.get("workflow_type").cloned(),
                format!("{:?}", e.fields),
            )
        })
        .expect("expected one log event");
    assert_eq!(
        wf_type.as_deref(),
        Some("my_workflow"),
        "expected workflow_type = my_workflow, got: {fields_str}"
    );
}

#[test]
fn logger_emits_replay_false_field() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    );

    let (captured, _guard) = install_capture();
    ctx.logger().info("replay field test");

    let (replay_val, fields_str) = captured
        .lock()
        .unwrap()
        .first()
        .map(|e| (e.fields.get("replay").cloned(), format!("{:?}", e.fields)))
        .expect("expected one log event");
    assert_eq!(
        replay_val.as_deref(),
        Some("false"),
        "expected replay = false, got: {fields_str}"
    );
}

#[test]
fn logger_emits_workflow_id_field() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    )
    .with_workflow_id("order-42");

    let (captured, _guard) = install_capture();
    ctx.logger().info("workflow_id field test");

    let (wf_id, fields_str) = captured
        .lock()
        .unwrap()
        .first()
        .map(|e| {
            (
                e.fields.get("workflow_id").cloned(),
                format!("{:?}", e.fields),
            )
        })
        .expect("expected one log event");
    assert_eq!(
        wf_id.as_deref(),
        Some("order-42"),
        "expected workflow_id = order-42, got: {fields_str}"
    );
}

#[test]
fn log_level_matches_method() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    );

    let (captured, _guard) = install_capture();
    ctx.logger().info("info msg");
    ctx.logger().warn("warn msg");
    ctx.logger().error("error msg");

    let (len, l0, l1, l2) = {
        let events = captured.lock().unwrap();
        (
            events.len(),
            events[0].level.clone(),
            events[1].level.clone(),
            events[2].level.clone(),
        )
    };
    assert_eq!(len, 3);
    assert_eq!(l0, "INFO");
    assert_eq!(l1, "WARN");
    assert_eq!(l2, "ERROR");
}

// ── Tests: guardrail catalog HVG009 ──────────────────────────────────────────

#[test]
fn hvg009_exists_in_catalog() {
    let rule = rule_by_id("HVG009").expect("HVG009 must be in the guardrail catalog");
    assert_eq!(rule.id, "HVG009");
}

#[test]
fn hvg009_category_is_unsafe_logging() {
    let rule = rule_by_id("HVG009").expect("HVG009 must exist");
    assert!(
        matches!(rule.category, RuleCategory::UnsafeLogging),
        "HVG009 must be in the UnsafeLogging category, got {:?}",
        rule.category
    );
}

#[test]
fn hvg009_alternative_mentions_logger() {
    let rule = rule_by_id("HVG009").expect("HVG009 must exist");
    let alt_lower = rule.alternative.to_lowercase();
    assert!(
        alt_lower.contains("logger")
            || alt_lower.contains("log_info")
            || alt_lower.contains("ctx.log"),
        "HVG009 alternative must mention the logger API, got: {}",
        rule.alternative
    );
}

#[test]
fn hvg009_explanation_mentions_replay() {
    let rule = rule_by_id("HVG009").expect("HVG009 must exist");
    assert!(
        rule.explanation.to_lowercase().contains("replay"),
        "HVG009 explanation must mention replay, got: {}",
        rule.explanation
    );
}

#[test]
fn catalog_contains_unsafe_logging_category() {
    let has = catalog()
        .iter()
        .any(|e| matches!(e.category, RuleCategory::UnsafeLogging));
    assert!(has, "catalog must have at least one UnsafeLogging rule");
}

// ── Tests: WorkflowContext::workflow_id() accessor ────────────────────────────

#[test]
fn workflow_context_exposes_workflow_id_accessor() {
    let ctx = live_ctx();
    let _id: &str = ctx.workflow_id();
}

#[test]
fn with_workflow_id_sets_id() {
    let exec_id = ExecutionId::new();
    let ctx = WorkflowContext::for_replay(
        exec_id,
        vec![WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        }],
    )
    .with_workflow_id("subscription-99");
    assert_eq!(ctx.workflow_id(), "subscription-99");
}

// ── Tests: WorkflowReplayer interaction ──────────────────────────────────────

/// Replaying a workflow with logger calls must produce 0 log events on pure replay.
#[cfg(feature = "testing")]
#[tokio::test]
async fn replayer_produces_zero_log_events() {
    use autumn_harvest::{ReplayStatus, WorkflowReplayer};

    let act1 = ActivityExecId::new();

    // Build a history that represents a completed execution of `logging_workflow_via_logger`.
    let history = vec![
        WorkflowEvent::WorkflowStarted {
            input: Value::Null,
            timestamp: Utc::now(),
            last_completion_result: None,
            last_error: None,
            scheduled_time: None,
        },
        WorkflowEvent::ActivityScheduled {
            activity_id: act1,
            name: "step_1".into(),
            input: Value::Null,
            queue: "default".into(),
        },
        WorkflowEvent::ActivityCompleted {
            activity_id: act1,
            output: Value::Null,
        },
        WorkflowEvent::WorkflowCompleted {
            output: Value::Null,
        },
    ];

    let (captured, _guard) = install_capture();

    let report = WorkflowReplayer::new()
        .register_fn("logging_workflow", logging_workflow_via_logger)
        .replay_from_events(history)
        .await;

    assert!(
        matches!(report.status, ReplayStatus::ReplaySucceeded),
        "replay must succeed, got: {report}"
    );

    let event_count = captured.lock().unwrap().len();
    assert_eq!(
        event_count, 0,
        "WorkflowReplayer must produce 0 log events (pure replay), got {event_count}"
    );
}
