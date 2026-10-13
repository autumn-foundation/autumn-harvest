//! One `#[activity]` fn that runs unchanged on SQLite and on Postgres (issue #2024).
//!
//! Two suites include this file. `portable_activity.rs` runs it on SQLite.
//! `autumn-harvest/tests/integration/sqlite_portable_activity_tests.rs` runs it
//! on Postgres. Both compare the report with [`expected_report`].

use std::time::Duration;

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

/// The workflow type both suites start.
pub const PORTABLE_WORKFLOW: &str = "portable_flow";

/// The activity type both suites register.
pub const PORTABLE_ACTIVITY: &str = "portable_probe";

/// Fails attempt 1, then reports what its `ActivityContext` saw.
///
/// The zero delay keeps the SQLite retry inside one drive.
#[activity(retry = RetryPolicy::fixed(3, Duration::ZERO))]
pub async fn portable_probe(ctx: &ActivityContext, label: String) -> Result<Value, String> {
    let info = ctx.info();
    if info.attempt == 1 {
        return Err(format!("first attempt of {label}"));
    }
    ctx.check_cancellation().await.map_err(|e| e.to_string())?;
    let key = ctx.idempotency_key().map_err(|e| e.to_string())?;
    Ok(json!({
        "label": label,
        "execution_id": info.execution_id.to_string(),
        "workflow_id": info.workflow_id,
        "workflow_type": info.workflow_type,
        "activity_type": info.activity_type,
        "attempt": info.attempt,
        "max_attempts": info.max_attempts,
        "is_last_attempt": ctx.is_last_attempt(),
        "is_local": info.is_local,
        "queue_name": info.queue_name,
        "has_task_id": info.task_id.is_some(),
        "previous_failure": ctx.previous_failure(),
        "key_matches_activity_id": *key == IdempotencyKey::from_activity_exec_id(info.activity_id),
        "cancelled": ctx.is_cancelled(),
    }))
}

/// Runs [`portable_probe`] once and returns its report.
#[workflow]
pub async fn portable_flow(ctx: &WorkflowContext, label: String) -> Result<Value, String> {
    ctx.execute_activity(&portable_probe_info(), label)
        .await
        .map_err(|e| e.to_string())
}

/// The report both backends must produce for one run.
#[must_use]
pub fn expected_report(label: &str, execution_id: ExecutionId, workflow_id: &str) -> Value {
    json!({
        "label": label,
        "execution_id": execution_id.to_string(),
        "workflow_id": workflow_id,
        "workflow_type": PORTABLE_WORKFLOW,
        "activity_type": PORTABLE_ACTIVITY,
        "attempt": 2,
        "max_attempts": 3,
        "is_last_attempt": false,
        "is_local": false,
        "queue_name": "default",
        "has_task_id": true,
        "previous_failure": format!("first attempt of {label}"),
        "key_matches_activity_id": true,
        "cancelled": false,
    })
}
