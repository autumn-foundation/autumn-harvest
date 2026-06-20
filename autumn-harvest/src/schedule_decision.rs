//! Schedule decisions tracking.
//!
//! When a schedule's cron expression fires or an interval elapses, the engine evaluates whether
//! to start the target workflow. A schedule decision represents the outcome of this evaluation.
//! These decisions are persisted to the `harvest_schedule_decisions` table to provide operators
//! with a durable audit trail of why a scheduled workflow did or did not start at a particular time.
//!
//! Reasons for skipping a workflow start include concurrency limits (e.g., `max_active_runs`),
//! the schedule being paused, or prior runs still being active without a `catchup` policy.
//!
//! This module provides functions to safely record these decisions (swallowing database
//! errors to preserve scheduler progress) and to purge old decisions based on retention policies.

use crate::models::NewScheduleDecision;
use crate::schema::harvest_schedule_decisions;
use crate::telemetry::MetricsRecorder;
use chrono::{DateTime, Utc};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde_json::Value;
use uuid::Uuid;

/// Record a scheduler fire/skip decision to the database.
///
/// Under any database execution error, this logs a warning and increments the
/// failure metric but proceeds normally to guarantee scheduler robustness.
#[allow(clippy::too_many_arguments)]
pub async fn record_decision_graceful(
    conn: &mut AsyncPgConnection,
    metrics: Option<&dyn MetricsRecorder>,
    schedule_id: Option<Uuid>,
    schedule_name: &str,
    target_kind: &str,
    decision: &str,
    reason_code: &str,
    detail: Option<Value>,
    occurred_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
    shard_id: i16,
) {
    let new_dec = NewScheduleDecision {
        id: Uuid::new_v4(),
        schedule_id,
        schedule_name: schedule_name.to_owned(),
        target_kind: target_kind.to_owned(),
        decision: decision.to_owned(),
        reason_code: reason_code.to_owned(),
        detail,
        occurred_at,
        next_fire_at,
        shard_id,
    };

    if let Err(e) = diesel::insert_into(harvest_schedule_decisions::table)
        .values(&new_dec)
        .execute(conn)
        .await
    {
        tracing::warn!(error = %e, schedule_name = %schedule_name, "harvest: failed to record schedule decision");
        if let Some(m) = metrics {
            m.record_schedule_decision_write_failed();
        }
    }
}

/// Purge schedule decisions older than a specified number of days.
///
/// # Errors
///
/// Returns a `database_error` if the database query execution fails.
pub async fn purge_old_schedule_decisions(
    conn: &mut AsyncPgConnection,
    retention_days: i64,
) -> crate::error::HarvestResult<usize> {
    use crate::error::database_error;
    use diesel::ExpressionMethods;
    use diesel::QueryDsl;

    let cutoff = Utc::now() - chrono::Duration::days(retention_days.max(0));

    let deleted = diesel::delete(
        harvest_schedule_decisions::table
            .filter(harvest_schedule_decisions::occurred_at.lt(cutoff)),
    )
    .execute(conn)
    .await
    .map_err(database_error)?;
    Ok(deleted)
}
