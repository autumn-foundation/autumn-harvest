//! Dead letter queue (DLQ) operations.
//!
//! Tasks that exhaust all retry attempts are moved to the `harvest_dead_letters`
//! table for post-mortem inspection and potential manual reprocessing. This is
//! the final resting place for permanently failed tasks.

use diesel::ExpressionMethods;
use diesel::OptionalExtension;
use diesel::QueryDsl;
use diesel::SelectableHelper;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use scoped_futures::ScopedFutureExt;
use uuid::Uuid;

use crate::error::{HarvestError, HarvestResult};
use crate::models::{DeadLetter, NewDeadLetter};
use crate::queue::{EnqueueParams, TaskType};

const DEFAULT_BULK_LIMIT: u32 = 100;
const MAX_BULK_LIMIT: u32 = 1000;

/// Filter for bulk DLQ operations.
///
/// At least one of `activity_name`, `workflow_name`, `failed_after`, or
/// `failed_before` must be set. A filter with only `limit` or `dry_run` is
/// considered empty and rejected with 400 at the API layer.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct BulkDlqFilter {
    /// Exact match on `activity_name`.
    pub activity_name: Option<String>,
    /// Exact match on the workflow name of the parent execution.
    pub workflow_name: Option<String>,
    /// Inclusive lower bound on `failed_at`.
    pub failed_after: Option<chrono::DateTime<chrono::Utc>>,
    /// Exclusive upper bound on `failed_at`.
    pub failed_before: Option<chrono::DateTime<chrono::Utc>>,
    /// Cap on rows acted on per call. Defaults to 100, hard-capped at 1000.
    pub limit: Option<u32>,
    /// When `true`, return matching rows and count without writing.
    #[serde(default)]
    pub dry_run: bool,
}

impl BulkDlqFilter {
    /// Returns `true` if no substantive filter criterion is specified.
    ///
    /// `limit` and `dry_run` alone do not count as criteria.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.activity_name.is_none()
            && self.workflow_name.is_none()
            && self.failed_after.is_none()
            && self.failed_before.is_none()
    }

    /// Effective row limit: uses the provided value clamped to [1, 1000],
    /// defaulting to 100 when not set.
    #[must_use]
    pub fn effective_limit(&self) -> i64 {
        i64::from(self.limit.unwrap_or(DEFAULT_BULK_LIMIT).min(MAX_BULK_LIMIT))
    }
}

/// A single per-row failure within a bulk DLQ operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BulkDlqFailure {
    /// Dead-letter row ID that failed.
    pub id: String,
    /// Error message for this row.
    pub reason: String,
}

/// Structured result of a bulk DLQ replay or discard operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BulkDlqResult {
    /// Total rows matching the filter (before limit clip).
    pub matched: usize,
    /// Rows successfully acted on.
    pub acted_on: usize,
    /// Rows that matched but were skipped (e.g. already replayed concurrently).
    pub skipped: usize,
    /// IDs of rows successfully acted on.
    pub ids: Vec<String>,
    /// Whether this was a dry-run (no writes performed).
    pub dry_run: bool,
    /// Per-row failures that did not roll back other rows.
    pub failures: Vec<BulkDlqFailure>,
}

fn dead_letter_task_type(dead_letter_id: Uuid, task_type: &str) -> HarvestResult<TaskType> {
    if task_type.eq_ignore_ascii_case("workflow") {
        Ok(TaskType::Workflow)
    } else if task_type.eq_ignore_ascii_case("activity") {
        Ok(TaskType::Activity)
    } else {
        Err(HarvestError::Config(format!(
            "dead-letter {dead_letter_id} has invalid task_type '{task_type}'"
        )))
    }
}

/// Convenience struct for building a new dead-letter entry.
///
/// Mirrors [`NewDeadLetter`] but owns its strings, making it easier to
/// construct from runtime data without lifetime gymnastics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewDeadLetterEntry {
    pub original_task_id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub error: String,
    pub attempts: i32,
}

/// Insert a task into the dead-letter queue and return the generated DLQ entry ID.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on insert failure.
pub async fn dead_letter(
    conn: &mut AsyncPgConnection,
    entry: &NewDeadLetterEntry,
) -> HarvestResult<Uuid> {
    use crate::schema::harvest_dead_letters;

    let row = NewDeadLetter {
        original_task_id: entry.original_task_id,
        queue_name: &entry.queue_name,
        task_type: &entry.task_type,
        workflow_exec_id: entry.workflow_exec_id,
        activity_name: entry.activity_name.as_deref(),
        input: entry.input.clone(),
        error: &entry.error,
        attempts: entry.attempts,
    };

    let inserted: Vec<Uuid> = diesel::insert_into(harvest_dead_letters::table)
        .values(&row)
        .returning(harvest_dead_letters::id)
        .get_results(conn)
        .await
        .map_err(crate::error::database_error)?;

    inserted
        .into_iter()
        .next()
        .ok_or_else(|| HarvestError::Database("insert returned no ID".into()))
}

/// Count the total number of entries in the dead-letter queue.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn dead_letter_count(conn: &mut AsyncPgConnection) -> HarvestResult<i64> {
    use crate::schema::harvest_dead_letters::dsl;

    let count: i64 = dsl::harvest_dead_letters
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(count)
}

/// List dead-letter entries, newest first.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn list_dead_letters(
    conn: &mut AsyncPgConnection,
    limit: i64,
) -> HarvestResult<Vec<DeadLetter>> {
    use crate::schema::harvest_dead_letters::dsl;

    dsl::harvest_dead_letters
        .order(dsl::failed_at.desc())
        .limit(limit)
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Requeue a dead-letter entry and remove it from the DLQ.
///
/// The replayed task starts fresh in `PENDING` state with the same queue, task
/// type, activity name, workflow execution ID, and input as the dead-letter row.
/// Its `max_attempts` is set to the number of attempts recorded on the original
/// dead-letter entry, with a floor of one.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`] if the DLQ entry does not exist,
/// [`HarvestError::Config`] if the stored task type is invalid, or
/// [`HarvestError::Database`] if requeue/delete work fails.
pub async fn replay_dead_letter(
    conn: &mut AsyncPgConnection,
    dead_letter_id: Uuid,
) -> HarvestResult<Uuid> {
    use crate::schema::harvest_dead_letters::dsl;

    conn.transaction::<Uuid, HarvestError, _>(|conn| {
        async move {
            let entry = dsl::harvest_dead_letters
                .find(dead_letter_id)
                .select(DeadLetter::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("dead-letter {dead_letter_id}")))?;

            let task_type = dead_letter_task_type(dead_letter_id, &entry.task_type)?;

            let mut params = EnqueueParams::new(entry.queue_name, task_type, entry.input);
            params.workflow_exec_id = entry.workflow_exec_id;
            params.activity_name = entry.activity_name;
            params.max_attempts = entry.attempts.max(1);

            let task_id = crate::queue::enqueue(conn, &params).await?;
            let deleted = diesel::delete(dsl::harvest_dead_letters.find(dead_letter_id))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            if deleted == 0 {
                return Err(HarvestError::NotFound(format!(
                    "dead-letter {dead_letter_id}"
                )));
            }

            Ok(task_id)
        }
        .scope_boxed()
    })
    .await
}

// ---------------------------------------------------------------------------
// Bulk operations
// ---------------------------------------------------------------------------

/// Count dead-letter rows matching `filter` without applying a row limit.
///
/// Used to populate `BulkDlqResult::matched` so callers can detect when the
/// limit clips the result set.
async fn count_dead_letters_for_bulk(
    conn: &mut AsyncPgConnection,
    filter: &BulkDlqFilter,
) -> HarvestResult<i64> {
    use crate::schema::harvest_dead_letters::dsl;
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Text};

    let mut query = dsl::harvest_dead_letters.into_boxed();

    if let Some(ref name) = filter.activity_name {
        query = query.filter(dsl::activity_name.eq(name.clone()));
    }
    if let Some(ref wf_name) = filter.workflow_name {
        query = query.filter(
            sql::<Bool>("workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ")
                .bind::<Text, _>(wf_name.clone())
                .sql(")"),
        );
    }
    if let Some(after) = filter.failed_after {
        query = query.filter(dsl::failed_at.ge(after));
    }
    if let Some(before) = filter.failed_before {
        query = query.filter(dsl::failed_at.lt(before));
    }

    query
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Query dead-letter rows matching `filter`, ordered oldest-first, up to
/// `filter.effective_limit()` rows.
async fn query_dead_letters_for_bulk(
    conn: &mut AsyncPgConnection,
    filter: &BulkDlqFilter,
) -> HarvestResult<Vec<DeadLetter>> {
    use crate::schema::harvest_dead_letters::dsl;
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Text};

    let mut query = dsl::harvest_dead_letters
        .into_boxed()
        .order(dsl::failed_at.asc())
        .limit(filter.effective_limit());

    if let Some(ref name) = filter.activity_name {
        query = query.filter(dsl::activity_name.eq(name.clone()));
    }
    if let Some(ref wf_name) = filter.workflow_name {
        query = query.filter(
            sql::<Bool>("workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ")
                .bind::<Text, _>(wf_name.clone())
                .sql(")"),
        );
    }
    if let Some(after) = filter.failed_after {
        query = query.filter(dsl::failed_at.ge(after));
    }
    if let Some(before) = filter.failed_before {
        query = query.filter(dsl::failed_at.lt(before));
    }

    query
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Bulk replay dead-letter entries matching `filter`.
///
/// Each row is replayed independently through the same path as
/// [`replay_dead_letter`]. A per-row failure does not roll back already-replayed
/// rows. When `filter.dry_run` is `true`, no writes are performed.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the initial filter query fails.
/// Per-row replay errors are captured in [`BulkDlqResult::failures`].
pub async fn bulk_replay_dead_letters(
    conn: &mut AsyncPgConnection,
    filter: &BulkDlqFilter,
) -> HarvestResult<BulkDlqResult> {
    let matched = usize::try_from(count_dead_letters_for_bulk(conn, filter).await?).unwrap_or(0);
    let rows = query_dead_letters_for_bulk(conn, filter).await?;
    let preview_ids: Vec<String> = rows.iter().map(|r| r.id.to_string()).collect();

    if filter.dry_run {
        return Ok(BulkDlqResult {
            matched,
            acted_on: 0,
            skipped: 0,
            ids: preview_ids,
            dry_run: true,
            failures: Vec::new(),
        });
    }

    let mut acted_on = 0usize;
    let mut skipped = 0usize;
    let mut acted_ids: Vec<String> = Vec::new();
    let mut failures: Vec<BulkDlqFailure> = Vec::new();

    for row in &rows {
        match replay_dead_letter(conn, row.id).await {
            Ok(_task_id) => {
                acted_on += 1;
                acted_ids.push(row.id.to_string());
            }
            Err(HarvestError::NotFound(_)) => {
                skipped += 1;
            }
            Err(e) => {
                failures.push(BulkDlqFailure {
                    id: row.id.to_string(),
                    reason: e.to_string(),
                });
            }
        }
    }

    Ok(BulkDlqResult {
        matched,
        acted_on,
        skipped,
        ids: acted_ids,
        dry_run: false,
        failures,
    })
}

/// Bulk discard dead-letter entries matching `filter`.
///
/// Each matching row is deleted from `harvest_dead_letters` without
/// re-enqueueing. A per-row failure does not affect other rows. When
/// `filter.dry_run` is `true`, no deletes are performed.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the initial filter query fails.
/// Per-row delete errors are captured in [`BulkDlqResult::failures`].
pub async fn bulk_discard_dead_letters(
    conn: &mut AsyncPgConnection,
    filter: &BulkDlqFilter,
) -> HarvestResult<BulkDlqResult> {
    use crate::schema::harvest_dead_letters::dsl;

    let matched = usize::try_from(count_dead_letters_for_bulk(conn, filter).await?).unwrap_or(0);
    let rows = query_dead_letters_for_bulk(conn, filter).await?;
    let preview_ids: Vec<String> = rows.iter().map(|r| r.id.to_string()).collect();

    if filter.dry_run {
        return Ok(BulkDlqResult {
            matched,
            acted_on: 0,
            skipped: 0,
            ids: preview_ids,
            dry_run: true,
            failures: Vec::new(),
        });
    }

    let mut acted_on = 0usize;
    let mut skipped = 0usize;
    let mut acted_ids: Vec<String> = Vec::new();
    let mut failures: Vec<BulkDlqFailure> = Vec::new();

    for row in &rows {
        match diesel::delete(dsl::harvest_dead_letters.find(row.id))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)
        {
            Ok(0) => {
                skipped += 1;
            }
            Ok(_) => {
                acted_on += 1;
                acted_ids.push(row.id.to_string());
            }
            Err(e) => {
                failures.push(BulkDlqFailure {
                    id: row.id.to_string(),
                    reason: e.to_string(),
                });
            }
        }
    }

    Ok(BulkDlqResult {
        matched,
        acted_on,
        skipped,
        ids: acted_ids,
        dry_run: false,
        failures,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letter_entry_builds() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "email-queue".into(),
            task_type: "ACTIVITY".into(),
            workflow_exec_id: Some(Uuid::new_v4()),
            activity_name: Some("send_email".into()),
            input: serde_json::json!({"to": "alice@example.com"}),
            error: "SMTP connection refused after 3 attempts".into(),
            attempts: 3,
        };

        assert_eq!(entry.queue_name, "email-queue");
        assert_eq!(entry.task_type, "ACTIVITY");
        assert_eq!(entry.attempts, 3);
        assert!(entry.activity_name.is_some());
        assert!(entry.workflow_exec_id.is_some());
        assert!(entry.error.contains("SMTP"));
    }

    #[test]
    fn dead_letter_entry_without_optional_fields() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "default".into(),
            task_type: "WORKFLOW".into(),
            workflow_exec_id: None,
            activity_name: None,
            input: serde_json::Value::Null,
            error: "unknown failure".into(),
            attempts: 1,
        };

        assert!(entry.workflow_exec_id.is_none());
        assert!(entry.activity_name.is_none());
        assert_eq!(entry.attempts, 1);
    }

    #[test]
    fn invalid_dead_letter_task_type_is_config_error() {
        let dead_letter_id = Uuid::new_v4();
        let error = dead_letter_task_type(dead_letter_id, "timer").unwrap_err();

        assert!(
            matches!(error, HarvestError::Config(message) if message.contains("invalid task_type"))
        );
    }

    #[test]
    fn bulk_filter_is_empty_when_no_criteria_set() {
        let filter = BulkDlqFilter::default();
        assert!(filter.is_empty());
    }

    #[test]
    fn bulk_filter_is_not_empty_when_activity_name_set() {
        let filter = BulkDlqFilter {
            activity_name: Some("send_email".into()),
            ..BulkDlqFilter::default()
        };
        assert!(!filter.is_empty());
    }

    #[test]
    fn bulk_filter_is_not_empty_when_workflow_name_set() {
        let filter = BulkDlqFilter {
            workflow_name: Some("onboarding".into()),
            ..BulkDlqFilter::default()
        };
        assert!(!filter.is_empty());
    }

    #[test]
    fn bulk_filter_is_not_empty_when_failed_after_set() {
        let filter = BulkDlqFilter {
            failed_after: Some(chrono::Utc::now()),
            ..BulkDlqFilter::default()
        };
        assert!(!filter.is_empty());
    }

    #[test]
    fn bulk_filter_is_not_empty_when_failed_before_set() {
        let filter = BulkDlqFilter {
            failed_before: Some(chrono::Utc::now()),
            ..BulkDlqFilter::default()
        };
        assert!(!filter.is_empty());
    }

    #[test]
    fn bulk_filter_effective_limit_defaults_to_100() {
        let filter = BulkDlqFilter::default();
        assert_eq!(filter.effective_limit(), 100);
    }

    #[test]
    fn bulk_filter_effective_limit_uses_provided_value() {
        let filter = BulkDlqFilter {
            limit: Some(250),
            ..BulkDlqFilter::default()
        };
        assert_eq!(filter.effective_limit(), 250);
    }

    #[test]
    fn bulk_filter_effective_limit_clamps_at_1000() {
        let filter = BulkDlqFilter {
            limit: Some(9999),
            ..BulkDlqFilter::default()
        };
        assert_eq!(filter.effective_limit(), 1000);
    }

    #[test]
    fn bulk_filter_dry_run_defaults_to_false_from_json() {
        let filter: BulkDlqFilter = serde_json::from_str(r#"{"activity_name":"foo"}"#).unwrap();
        assert!(!filter.dry_run);
    }

    #[test]
    fn bulk_filter_serialization_roundtrip() {
        let filter = BulkDlqFilter {
            activity_name: Some("charge_card".into()),
            workflow_name: Some("billing".into()),
            failed_after: None,
            failed_before: None,
            limit: Some(200),
            dry_run: true,
        };
        let json = serde_json::to_string(&filter).unwrap();
        let back: BulkDlqFilter = serde_json::from_str(&json).unwrap();
        assert_eq!(back.activity_name.as_deref(), Some("charge_card"));
        assert_eq!(back.workflow_name.as_deref(), Some("billing"));
        assert_eq!(back.limit, Some(200));
        assert!(back.dry_run);
    }

    #[test]
    fn dead_letter_entry_serializes_to_json() {
        let entry = NewDeadLetterEntry {
            original_task_id: Uuid::new_v4(),
            queue_name: "billing".into(),
            task_type: "ACTIVITY".into(),
            workflow_exec_id: None,
            activity_name: Some("charge_card".into()),
            input: serde_json::json!({"amount": 99.99}),
            error: "payment declined".into(),
            attempts: 5,
        };

        let json = serde_json::to_string(&entry).expect("should serialize");
        let back: NewDeadLetterEntry = serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(back.queue_name, "billing");
        assert_eq!(back.attempts, 5);
    }
}
