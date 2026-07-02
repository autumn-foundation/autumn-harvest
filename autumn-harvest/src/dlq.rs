//! Dead letter queue (DLQ) operations.
//!
//! Tasks that exhaust all retry attempts are moved to the `harvest_dead_letters`
//! table for post-mortem inspection and potential manual reprocessing. This is
//! the final resting place for permanently failed tasks.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
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
use crate::worker::HandlerRegistry;

pub const DEFAULT_BULK_LIMIT: u32 = 100;
/// Maximum number of DLQ rows a single bulk operation can act on.
pub const MAX_BULK_LIMIT: u32 = 1000;

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
    /// defaulting to 100 when not set. Zero is treated as 1 so callers never
    /// get a silent no-op from `LIMIT 0`.
    #[must_use]
    pub fn effective_limit(&self) -> i64 {
        i64::from(
            self.limit
                .unwrap_or(DEFAULT_BULK_LIMIT)
                .clamp(1, MAX_BULK_LIMIT),
        )
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

// ---------------------------------------------------------------------------
// Redrive (issue #510)
// ---------------------------------------------------------------------------

/// Filter for the operator redrive endpoint (`POST /dlq/redrive`, issue #510).
///
/// Selects dead-letter rows to re-enqueue with a fresh retry budget. At least
/// one substantive criterion must be set — a filter with only `max`/`dry_run`
/// is rejected with 400 at the API layer. The time bounds map onto the
/// `failed_at` column (`dead_lettered_after` → `>=`, `dead_lettered_before`
/// → `<`).
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RedriveFilter {
    /// Exact match on `queue_name`.
    pub queue: Option<String>,
    /// Exact match on the workflow name of the owning execution.
    pub workflow_name: Option<String>,
    /// Inclusive lower bound on `failed_at`.
    pub dead_lettered_after: Option<DateTime<Utc>>,
    /// Exclusive upper bound on `failed_at`.
    pub dead_lettered_before: Option<DateTime<Utc>>,
    /// Case-insensitive substring match on the `error` column.
    pub error_contains: Option<String>,
    /// Explicit set of dead-letter row IDs to target.
    pub dead_letter_ids: Option<Vec<Uuid>>,
    /// Cap on rows redriven per call. Defaults to 100, hard-capped at 1000.
    pub max: Option<u32>,
    /// When `true`, return matching rows and count without writing.
    #[serde(default)]
    pub dry_run: bool,
}

impl RedriveFilter {
    /// Returns `true` if no substantive filter criterion is specified.
    ///
    /// `max` and `dry_run` alone do not count as criteria.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.queue.is_none()
            && self.workflow_name.is_none()
            && self.dead_lettered_after.is_none()
            && self.dead_lettered_before.is_none()
            && self.error_contains.is_none()
            && self.dead_letter_ids.as_ref().is_none_or(Vec::is_empty)
    }

    /// Effective row cap: the provided `max` clamped to `[1, MAX_BULK_LIMIT]`,
    /// defaulting to `DEFAULT_BULK_LIMIT`. Zero is treated as 1 so callers never
    /// get a silent no-op from `LIMIT 0`.
    #[must_use]
    pub fn effective_max(&self) -> i64 {
        i64::from(
            self.max
                .unwrap_or(DEFAULT_BULK_LIMIT)
                .clamp(1, MAX_BULK_LIMIT),
        )
    }
}

/// Structured result of a redrive operation (issue #510).
///
/// `matched` vs `redriven` makes silent truncation impossible: when the filter
/// matches more rows than `max`, the operator sees there is more to do.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RedriveResult {
    /// Total rows matching the filter across the shard, before the `max` clip.
    pub matched: usize,
    /// Rows re-enqueued with a fresh retry budget.
    pub redriven: usize,
    /// Rows that matched but were a no-op (already redriven / execution already
    /// progressed). Reported, never a duplicate enqueue.
    pub skipped: usize,
    /// Rows rejected with a clear error (e.g. owning execution is COMPLETED).
    pub failed: usize,
    /// IDs of rows actually redriven (in dry-run, the bounded sample that would
    /// be redriven).
    pub ids: Vec<String>,
    /// Whether this was a dry-run (no writes performed).
    pub dry_run: bool,
    /// Per-row rejections that did not roll back other rows.
    pub failures: Vec<BulkDlqFailure>,
}

/// Outcome of redriving a single dead-letter entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedriveOutcome {
    /// The entry was re-enqueued; carries the new task-queue row id.
    Redriven(Uuid),
    /// The entry was a no-op (missing row, or owning execution already running).
    Skipped,
}

/// Escape `%` and `_` so an operator-supplied substring is matched literally by
/// a SQL `ILIKE` pattern.
fn escape_like(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

/// Typed dead-letter reason for engine-authored DLQ entries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum DeadLetterReason {
    /// Workflow history reached the operator-configured hard cap without
    /// author code explicitly rotating via `continue_as_new`.
    HistoryCapExceeded {
        count: u64,
        cap: u64,
        workflow_type: String,
    },
    /// A task crashed the worker process `crash_strikes` times in a row
    /// (issue #367). The orphan-reclaim scanner quarantined it instead of
    /// re-dispatching it to crash another worker. `last_worker_id` is the
    /// worker that held the row when the final strike was observed.
    PoisonPill {
        crash_strikes: i32,
        last_worker_id: Option<String>,
    },
    /// A workflow-task dispatch did not complete or suspend within the
    /// `workflow_task_timeout` budget `task_timeout_strikes` times in a row
    /// (issue #494). The worker reclaimed the concurrency slot on each
    /// occasion; after hitting the threshold the task was quarantined rather
    /// than re-queued indefinitely.
    ///
    /// `timeout_secs` is the configured budget (whole seconds) at quarantine
    /// time. `task_timeout_strikes` is the in-process consecutive-timeout
    /// count that triggered escalation.
    WorkflowTaskTimeout {
        task_timeout_strikes: i32,
        timeout_secs: u64,
    },
}

impl std::fmt::Display for DeadLetterReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(json) => f.write_str(&json),
            Err(_) => f.write_str("DeadLetterReasonSerializationFailed"),
        }
    }
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
///
/// ## Examples
///
/// ```rust
/// use uuid::Uuid;
/// use autumn_harvest::dlq::NewDeadLetterEntry;
///
/// let entry = NewDeadLetterEntry {
///     original_task_id: Uuid::new_v4(),
///     queue_name: "default".to_string(),
///     task_type: "ACTIVITY".to_string(),
///     workflow_exec_id: Some(Uuid::new_v4()),
///     activity_name: Some("send_email".to_string()),
///     input: serde_json::json!({"to": "user@example.com"}),
///     error: "connection refused".to_string(),
///     attempts: 3,
///     owner: None,
///     severity: None,
/// };
/// ```
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
    pub owner: Option<String>,
    pub severity: Option<String>,
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
        owner: entry.owner.as_deref(),
        severity: entry.severity.as_deref(),
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
    owner: Option<&str>,
) -> HarvestResult<Vec<DeadLetter>> {
    use crate::schema::harvest_dead_letters::dsl;

    let mut query = dsl::harvest_dead_letters
        .into_boxed()
        .order(dsl::failed_at.desc())
        .limit(limit);

    if let Some(o) = owner {
        query = query.filter(dsl::owner.eq(o.to_string()));
    }

    query
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
    registry: Option<&HandlerRegistry>,
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

            // Restore required_build_id and concurrency policy from the owning
            // execution so the replayed task is subject to the same constraints.
            if let Some(exec_id) = params.workflow_exec_id {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                let row: Option<(Option<String>, String, String)> =
                    exec_dsl::harvest_workflow_executions
                        .find(exec_id)
                        .select((
                            exec_dsl::assigned_build_id,
                            exec_dsl::workflow_name,
                            exec_dsl::state,
                        ))
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;
                if let Some((build_id, workflow_name, state)) = row {
                    // Refuse to revive a task into a workflow that has already
                    // reached a terminal state. Re-running the activity/workflow
                    // task would execute user code and append events against a
                    // dead execution, corrupting its history (issue #367). This
                    // is what makes a poison-pill activity DLQ entry — whose
                    // owning workflow is failed at quarantine time —
                    // non-replayable.
                    if state != "RUNNING" {
                        return Err(HarvestError::WorkflowNotRunning(
                            exec_id.to_string().parse().map_err(|_| {
                                HarvestError::Database(format!(
                                    "execution id {exec_id} is not a valid ExecutionId"
                                ))
                            })?,
                        ));
                    }
                    params.required_build_id = build_id;
                    // Concurrency policy lives on WorkflowInfo and governs
                    // workflow-task slots only.  Activity tasks are not subject
                    // to the workflow-level cap (the claim query enforces caps
                    // per task_type, so mixing them would throttle activities
                    // against the wrong budget).
                    if task_type == TaskType::Workflow
                        && let Some(reg) = registry
                        && let Some(info) = reg.workflows.get(&workflow_name)
                        && let Some(policy) = &info.concurrency
                    {
                        params.concurrency_key = crate::concurrency::resolve_concurrency_key(
                            policy.key_expr,
                            &params.input,
                        );
                        params.max_concurrent = Some(policy.limit);
                    }
                }
            }

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
/// Used both internally (to populate `BulkDlqResult::matched`) and by the
/// API fan-out layer to count matches on shards where the global limit is
/// already exhausted, so that `matched` in the aggregated response always
/// reflects the true pre-limit total across all shards.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn count_bulk_filter_matches(
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
    registry: Option<&HandlerRegistry>,
) -> HarvestResult<BulkDlqResult> {
    let matched = usize::try_from(count_bulk_filter_matches(conn, filter).await?).unwrap_or(0);
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
    let mut acted_ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut failures: Vec<BulkDlqFailure> = Vec::with_capacity(rows.len());

    for row in &rows {
        match replay_dead_letter(conn, row.id, registry).await {
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

    let matched = usize::try_from(count_bulk_filter_matches(conn, filter).await?).unwrap_or(0);
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
    let mut acted_ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut failures: Vec<BulkDlqFailure> = Vec::with_capacity(rows.len());

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
// Redrive operations (issue #510)
// ---------------------------------------------------------------------------

/// Apply the redrive filter dimensions to a boxed `harvest_dead_letters` query.
///
/// Shared by [`count_redrive_filter_matches`] and
/// [`query_dead_letters_for_redrive`] so the count and the acted-on set always
/// see identical predicates.
fn apply_redrive_filter<'a>(
    mut query: crate::schema::harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg>,
    filter: &RedriveFilter,
) -> crate::schema::harvest_dead_letters::BoxedQuery<'a, diesel::pg::Pg> {
    use crate::schema::harvest_dead_letters::dsl;
    use diesel::PgTextExpressionMethods;

    if let Some(ref queue) = filter.queue {
        query = query.filter(dsl::queue_name.eq(queue.clone()));
    }
    if let Some(ref wf_name) = filter.workflow_name {
        use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
        use diesel::NullableExpressionMethods;
        query = query.filter(
            dsl::workflow_exec_id.eq_any(
                exec_dsl::harvest_workflow_executions
                    .filter(exec_dsl::workflow_name.eq(wf_name.clone()))
                    .select(exec_dsl::id.nullable()),
            ),
        );
    }
    if let Some(after) = filter.dead_lettered_after {
        query = query.filter(dsl::failed_at.ge(after));
    }
    if let Some(before) = filter.dead_lettered_before {
        query = query.filter(dsl::failed_at.lt(before));
    }
    if let Some(ref needle) = filter.error_contains {
        query = query.filter(dsl::error.ilike(format!("%{}%", escape_like(needle))));
    }
    if let Some(ref ids) = filter.dead_letter_ids {
        query = query.filter(dsl::id.eq_any(ids.clone()));
    }
    query
}

/// Count dead-letter rows matching `filter` without applying a row cap.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn count_redrive_filter_matches(
    conn: &mut AsyncPgConnection,
    filter: &RedriveFilter,
) -> HarvestResult<i64> {
    use crate::schema::harvest_dead_letters::dsl;

    apply_redrive_filter(dsl::harvest_dead_letters.into_boxed(), filter)
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Query dead-letter rows matching `filter`, oldest-first, up to
/// `filter.effective_max()` rows.
async fn query_dead_letters_for_redrive(
    conn: &mut AsyncPgConnection,
    filter: &RedriveFilter,
) -> HarvestResult<Vec<DeadLetter>> {
    use crate::schema::harvest_dead_letters::dsl;

    let query = dsl::harvest_dead_letters
        .into_boxed()
        .order(dsl::failed_at.asc())
        .limit(filter.effective_max());

    apply_redrive_filter(query, filter)
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Redrive a single dead-letter entry (issue #510).
///
/// Re-enqueues the dead-lettered task onto its original queue with a fresh
/// retry budget. Unlike [`replay_dead_letter`], a redrive **reactivates** an
/// owning execution that was sealed `FAILED` at quarantine time (`FAILED →
/// RUNNING`, appending a [`WorkflowEvent::WorkflowRedriven`] event so replay
/// resumes from existing history append-only). The whole operation is one
/// transaction.
///
/// Outcomes:
/// - Missing DLQ row → `Ok(Skipped)` (idempotent: a second redrive of the same
///   id is a no-op, since the row is deleted on the first redrive).
/// - Owning execution `RUNNING` → `Ok(Skipped)` and the row is deleted (the
///   live execution already owns the work; re-enqueuing would double-dispatch).
/// - Owning execution `FAILED` → reactivate + re-enqueue → `Ok(Redriven(task))`.
/// - Owning execution any other terminal state (`COMPLETED`/`CANCELLED`/…) →
///   `Err` with a clear message; the row is left in place for the operator.
///
/// # Errors
///
/// Returns [`HarvestError::Config`] when the owning execution is in a
/// non-`FAILED` terminal state, or [`HarvestError::Database`] on query failure.
pub async fn redrive_dead_letter(
    conn: &mut AsyncPgConnection,
    dead_letter_id: Uuid,
    registry: Option<&HandlerRegistry>,
    reason: Option<&str>,
) -> HarvestResult<RedriveOutcome> {
    use crate::schema::harvest_dead_letters::dsl;

    conn.transaction::<RedriveOutcome, HarvestError, _>(|conn| {
        async move {
            let Some(entry) = dsl::harvest_dead_letters
                .find(dead_letter_id)
                .select(DeadLetter::as_select())
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
            else {
                // Already redriven (or discarded): idempotent no-op.
                return Ok(RedriveOutcome::Skipped);
            };

            let task_type = dead_letter_task_type(dead_letter_id, &entry.task_type)?;

            let mut params =
                EnqueueParams::new(entry.queue_name.clone(), task_type, entry.input.clone());
            params.workflow_exec_id = entry.workflow_exec_id;
            params.activity_name = entry.activity_name.clone();
            params.max_attempts = entry.attempts.max(1);

            if let Some(exec_uuid) = entry.workflow_exec_id {
                use crate::schema::harvest_workflow_executions::dsl as exec_dsl;
                let row: Option<(Option<String>, String, String)> =
                    exec_dsl::harvest_workflow_executions
                        .find(exec_uuid)
                        .select((
                            exec_dsl::assigned_build_id,
                            exec_dsl::workflow_name,
                            exec_dsl::state,
                        ))
                        .for_update()
                        .first(conn)
                        .await
                        .optional()
                        .map_err(crate::error::database_error)?;

                let (build_id, workflow_name, state) = row.ok_or_else(|| {
                    HarvestError::NotFound(format!("workflow execution {exec_uuid}"))
                })?;

                match state.as_str() {
                    // Live execution already owns the work — converge state by
                    // deleting the stale DLQ row and report a skip rather than
                    // double-dispatching a second task.
                    "RUNNING" | "PAUSED" => {
                        diesel::delete(dsl::harvest_dead_letters.find(dead_letter_id))
                            .execute(conn)
                            .await
                            .map_err(crate::error::database_error)?;
                        return Ok(RedriveOutcome::Skipped);
                    }
                    // The load-bearing differentiator (AC #7): reopen a run
                    // sealed FAILED at quarantine time so it can resume.
                    "FAILED" => {
                        let exec_id = crate::types::ExecutionId::from_uuid(exec_uuid);
                        crate::execution::reactivate_failed_execution(
                            conn,
                            exec_id,
                            dead_letter_id,
                            reason,
                        )
                        .await?;
                    }
                    // Non-FAILED terminal states are not resurrectable.
                    other => {
                        return Err(HarvestError::Config(format!(
                            "cannot redrive dead-letter {dead_letter_id}: owning execution \
                             {exec_uuid} is {other} (terminal, not resurrectable)"
                        )));
                    }
                }

                params.required_build_id = build_id;
                if task_type == TaskType::Workflow
                    && let Some(reg) = registry
                    && let Some(info) = reg.workflows.get(&workflow_name)
                    && let Some(policy) = &info.concurrency
                {
                    params.concurrency_key =
                        crate::concurrency::resolve_concurrency_key(policy.key_expr, &params.input);
                    params.max_concurrent = Some(policy.limit);
                }
            }

            let task_id = crate::queue::enqueue(conn, &params).await?;
            diesel::delete(dsl::harvest_dead_letters.find(dead_letter_id))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            Ok(RedriveOutcome::Redriven(task_id))
        }
        .scope_boxed()
    })
    .await
}

/// Redrive all dead-letter entries matching `filter` (issue #510).
///
/// Each row is redriven independently through [`redrive_dead_letter`] so a
/// per-row rejection never rolls back already-redriven rows. When
/// `filter.dry_run` is `true`, no writes are performed and `ids` carries the
/// bounded sample that would be redriven. One `harvest.dlq.redriven{queue,
/// outcome}` counter increment is emitted per non-dry-run row.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] if the initial filter query fails.
/// Per-row rejections are captured in [`RedriveResult::failures`].
pub async fn redrive_dead_letters(
    conn: &mut AsyncPgConnection,
    filter: &RedriveFilter,
    registry: Option<&HandlerRegistry>,
    reason: Option<&str>,
    metrics: &dyn crate::telemetry::MetricsRecorder,
) -> HarvestResult<RedriveResult> {
    let matched = usize::try_from(count_redrive_filter_matches(conn, filter).await?).unwrap_or(0);
    let rows = query_dead_letters_for_redrive(conn, filter).await?;

    if filter.dry_run {
        return Ok(RedriveResult {
            matched,
            redriven: 0,
            skipped: 0,
            failed: 0,
            ids: rows.iter().map(|r| r.id.to_string()).collect(),
            dry_run: true,
            failures: Vec::new(),
        });
    }

    let mut redriven = 0usize;
    let mut skipped = 0usize;
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut failures: Vec<BulkDlqFailure> = Vec::new();

    for row in &rows {
        match redrive_dead_letter(conn, row.id, registry, reason).await {
            Ok(RedriveOutcome::Redriven(_task_id)) => {
                redriven += 1;
                ids.push(row.id.to_string());
                metrics.record_dlq_redriven(&row.queue_name, "redriven");
            }
            Ok(RedriveOutcome::Skipped) => {
                skipped += 1;
                metrics.record_dlq_redriven(&row.queue_name, "skipped");
            }
            Err(e) => {
                failures.push(BulkDlqFailure {
                    id: row.id.to_string(),
                    reason: e.to_string(),
                });
                metrics.record_dlq_redriven(&row.queue_name, "failed");
            }
        }
    }

    Ok(RedriveResult {
        matched,
        redriven,
        skipped,
        failed: failures.len(),
        ids,
        dry_run: false,
        failures,
    })
}

// ---------------------------------------------------------------------------
// Root-cause aggregation (issue #385)
// ---------------------------------------------------------------------------
//
// The DLQ aggregation endpoint answers the operator's first incident question —
// "what is the shape of this fire?" — by grouping dead-letter rows along a small,
// named set of dimensions and returning per-group counts plus a handful of
// representative `dead_letter_id`s. It is a read-path-only feature: no new
// `WorkflowEvent` variants and no schema change.
//
// **Signature derivation choice.** `harvest_dead_letters.error` is freeform
// `TEXT`, so grouping on the raw column yields a long tail of singletons. This
// slice implements the *compute-on-read normalized substring* option (the
// zero-migration choice called out in the issue): [`failure_signature`] takes
// the first line of the error, normalizes dynamic runs (UUIDs, long hex strings,
// and decimal integers) to fixed placeholders, and truncates to
// [`SIGNATURE_MAX_LEN`] characters. Because it is a pure function of the error
// text, the same root cause aggregates identically across queries and across
// shards.

/// Maximum length of a derived failure signature, in characters.
pub const SIGNATURE_MAX_LEN: usize = 200;

/// Default cap on the number of groups returned by an aggregation query.
pub const DEFAULT_LIMIT_GROUPS: u32 = 50;
/// Hard ceiling on `limit_groups`.
pub const MAX_LIMIT_GROUPS: u32 = 500;
/// Default number of sample `dead_letter_id`s included per group.
pub const DEFAULT_SAMPLES_PER_GROUP: u32 = 3;
/// Hard ceiling on `samples_per_group`.
pub const MAX_SAMPLES_PER_GROUP: u32 = 10;

const PLACEHOLDER_UUID: &str = "<UUID>";
const PLACEHOLDER_HEX: &str = "<HEX>";
const PLACEHOLDER_NUM: &str = "<NUM>";

/// Derive a stable, deterministic failure signature from a freeform DLQ error.
///
/// The signature is the first line of `error`, with dynamic runs (UUIDs, long
/// hex strings, and decimal integers) normalized to fixed placeholders and the
/// result truncated to [`SIGNATURE_MAX_LEN`] characters. It is a pure function
/// of its input, so it is identical across queries and across shards for the
/// same root-cause error text (issue #385).
#[must_use]
pub fn failure_signature(error: &str) -> String {
    let first_line = error.lines().next().unwrap_or("").trim();
    let normalized = normalize_dynamic_runs(first_line);
    truncate_chars(normalized.trim(), SIGNATURE_MAX_LEN)
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect()
    }
}

const fn is_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'
}

fn normalize_dynamic_runs(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut token = String::new();
    for ch in input.chars() {
        if is_token_char(ch) {
            token.push(ch);
        } else {
            if !token.is_empty() {
                out.push_str(&normalize_token(&token));
                token.clear();
            }
            out.push(ch);
        }
    }
    if !token.is_empty() {
        out.push_str(&normalize_token(&token));
    }
    out
}

fn normalize_token(token: &str) -> String {
    if is_uuid(token) {
        return PLACEHOLDER_UUID.to_string();
    }
    if is_hex_run(token) {
        return PLACEHOLDER_HEX.to_string();
    }
    replace_digit_runs(token)
}

fn is_uuid(token: &str) -> bool {
    let bytes = token.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(i, b)| {
        if matches!(i, 8 | 13 | 18 | 23) {
            *b == b'-'
        } else {
            b.is_ascii_hexdigit()
        }
    })
}

fn is_hex_run(token: &str) -> bool {
    if let Some(body) = token
        .strip_prefix("0x")
        .or_else(|| token.strip_prefix("0X"))
    {
        return !body.is_empty() && body.bytes().all(|b| b.is_ascii_hexdigit());
    }
    // Plain hex: require length >= 8 to avoid normalizing short words, and
    // require at least one hex letter (a-f/A-F) so pure decimal strings ≥8
    // digits fall through to replace_digit_runs instead of becoming <HEX>.
    token.len() >= 8
        && token.bytes().all(|b| b.is_ascii_hexdigit())
        && token
            .bytes()
            .any(|b| matches!(b, b'a'..=b'f' | b'A'..=b'F'))
}

fn replace_digit_runs(token: &str) -> String {
    let mut out = String::with_capacity(token.len());
    let mut in_digits = false;
    for ch in token.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push_str(PLACEHOLDER_NUM);
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(ch);
        }
    }
    out
}

/// A dimension the DLQ aggregation can group on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DlqGroupDimension {
    /// Workflow name of the owning execution (resolved via `workflow_exec_id`).
    WorkflowName,
    /// Activity name recorded on the dead-letter row.
    ActivityName,
    /// Task queue name.
    QueueName,
    /// Task type (`WORKFLOW` / `ACTIVITY`).
    TaskType,
    /// `failed_at` truncated to the configured [`TimeBucketGranularity`].
    TimeBucket,
    /// Derived [`failure_signature`] of the error text.
    FailureSignature,
}

impl DlqGroupDimension {
    /// Wire name used in query parameters and response keys.
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::WorkflowName => "workflow_name",
            Self::ActivityName => "activity_name",
            Self::QueueName => "queue_name",
            Self::TaskType => "task_type",
            Self::TimeBucket => "time_bucket",
            Self::FailureSignature => "failure_signature",
        }
    }

    /// Parse a dimension from its wire name, or `None` if unknown.
    #[must_use]
    pub fn from_wire(s: &str) -> Option<Self> {
        Some(match s {
            "workflow_name" => Self::WorkflowName,
            "activity_name" => Self::ActivityName,
            "queue_name" => Self::QueueName,
            "task_type" => Self::TaskType,
            "time_bucket" => Self::TimeBucket,
            "failure_signature" => Self::FailureSignature,
            _ => return None,
        })
    }
}

/// Granularity of the `time_bucket` `group_by` dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeBucketGranularity {
    /// Truncate `failed_at` to the hour.
    Hour,
    /// Truncate `failed_at` to the day.
    Day,
}

impl TimeBucketGranularity {
    /// Format a timestamp into its bucket key.
    #[must_use]
    pub fn format(self, ts: DateTime<Utc>) -> String {
        match self {
            Self::Hour => ts.format("%Y-%m-%dT%H:00:00Z").to_string(),
            Self::Day => ts.format("%Y-%m-%d").to_string(),
        }
    }
}

/// Parsed, validated parameters for a DLQ aggregation query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqAggregateParams {
    /// Ordered, de-duplicated grouping dimensions (hierarchical key).
    pub group_by: Vec<DlqGroupDimension>,
    /// Granularity for the `time_bucket` dimension.
    pub time_bucket: TimeBucketGranularity,
    /// Filter: exact workflow name (applied before grouping).
    pub workflow_name: Option<String>,
    /// Filter: exact activity name.
    pub activity_name: Option<String>,
    /// Filter: exact queue name.
    pub queue_name: Option<String>,
    /// Filter: exact task type (`"activity"` or `"workflow"`, case-insensitive).
    pub task_type: Option<String>,
    /// Filter: inclusive lower bound on `failed_at`.
    pub since: Option<DateTime<Utc>>,
    /// Filter: exclusive upper bound on `failed_at`.
    pub until: Option<DateTime<Utc>>,
    /// Filter: minimum number of attempts.
    pub min_attempts: Option<i32>,
    /// Cap on returned groups; long tail rolls into `_other`.
    pub limit_groups: u32,
    /// Number of sample `dead_letter_id`s per group.
    pub samples_per_group: u32,
}

impl Default for DlqAggregateParams {
    fn default() -> Self {
        Self {
            group_by: Vec::new(),
            time_bucket: TimeBucketGranularity::Hour,
            workflow_name: None,
            activity_name: None,
            queue_name: None,
            task_type: None,
            since: None,
            until: None,
            min_attempts: None,
            limit_groups: DEFAULT_LIMIT_GROUPS,
            samples_per_group: DEFAULT_SAMPLES_PER_GROUP,
        }
    }
}

impl DlqAggregateParams {
    /// Parse and validate query-string pairs into aggregation parameters.
    ///
    /// `now` anchors relative durations (e.g. `since=24h`). Unknown query keys
    /// are ignored (forward-compatible), but invalid values for known keys —
    /// an unknown `group_by` dimension, a malformed duration, or an out-of-range
    /// `limit_groups`/`samples_per_group` — return `Err(message)` so the caller
    /// can answer `400 Bad Request` rather than silently matching nothing.
    ///
    /// # Errors
    ///
    /// Returns a human-readable error message string on any invalid parameter
    /// value, or when no `group_by` dimension is supplied.
    pub fn from_query_pairs(
        pairs: &[(String, String)],
        now: DateTime<Utc>,
    ) -> Result<Self, String> {
        let mut params = Self::default();

        for (key, value) in pairs {
            match key.as_str() {
                "group_by" => {
                    let dim = DlqGroupDimension::from_wire(value.trim()).ok_or_else(|| {
                        format!(
                            "unknown group_by dimension '{value}'; expected one of: \
                             workflow_name, activity_name, queue_name, task_type, \
                             time_bucket, failure_signature"
                        )
                    })?;
                    if !params.group_by.contains(&dim) {
                        params.group_by.push(dim);
                    }
                }
                "time_bucket" => {
                    params.time_bucket = match value.trim() {
                        "hour" => TimeBucketGranularity::Hour,
                        "day" => TimeBucketGranularity::Day,
                        other => {
                            return Err(format!(
                                "invalid time_bucket '{other}'; expected 'hour' or 'day'"
                            ));
                        }
                    };
                }
                "workflow_name" => set_filter(&mut params.workflow_name, value),
                "activity_name" => set_filter(&mut params.activity_name, value),
                "queue_name" => set_filter(&mut params.queue_name, value),
                "task_type" => {
                    let v = value.trim();
                    if !v.eq_ignore_ascii_case("activity") && !v.eq_ignore_ascii_case("workflow") {
                        return Err(format!(
                            "invalid task_type '{v}'; expected 'activity' or 'workflow'"
                        ));
                    }
                    params.task_type = Some(v.to_string());
                }
                "since" => params.since = Some(parse_instant("since", value, now)?),
                "until" => params.until = Some(parse_instant("until", value, now)?),
                "min_attempts" => {
                    let n: i32 = value.trim().parse().map_err(|_| {
                        format!("invalid min_attempts '{value}'; expected a non-negative integer")
                    })?;
                    if n < 0 {
                        return Err(format!(
                            "invalid min_attempts '{value}'; expected a non-negative integer"
                        ));
                    }
                    params.min_attempts = Some(n);
                }
                "limit_groups" => {
                    let n: u32 = value.trim().parse().map_err(|_| {
                        format!(
                            "invalid limit_groups '{value}'; expected an integer in [1, {MAX_LIMIT_GROUPS}]"
                        )
                    })?;
                    if !(1..=MAX_LIMIT_GROUPS).contains(&n) {
                        return Err(format!(
                            "invalid limit_groups '{value}'; expected an integer in [1, {MAX_LIMIT_GROUPS}]"
                        ));
                    }
                    params.limit_groups = n;
                }
                "samples_per_group" => {
                    let n: u32 = value.trim().parse().map_err(|_| {
                        format!(
                            "invalid samples_per_group '{value}'; expected an integer in [0, {MAX_SAMPLES_PER_GROUP}]"
                        )
                    })?;
                    if n > MAX_SAMPLES_PER_GROUP {
                        return Err(format!(
                            "invalid samples_per_group '{value}'; expected an integer in [0, {MAX_SAMPLES_PER_GROUP}]"
                        ));
                    }
                    params.samples_per_group = n;
                }
                _ => {}
            }
        }

        if params.group_by.is_empty() {
            return Err("at least one group_by dimension is required".to_string());
        }

        Ok(params)
    }
}

fn set_filter(slot: &mut Option<String>, value: &str) {
    let trimmed = value.trim();
    if !trimmed.is_empty() {
        *slot = Some(trimmed.to_string());
    }
}

pub fn parse_instant(
    field: &str,
    value: &str,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, String> {
    let trimmed = value.trim();
    if let Ok(parsed) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(parsed.with_timezone(&Utc));
    }
    if let Some(dur) = crate::task_duration(trimmed)
        && let Ok(chrono_dur) = chrono::Duration::from_std(dur)
    {
        return Ok(now - chrono_dur);
    }
    Err(format!(
        "invalid {field} '{value}'; expected an RFC 3339 timestamp or a relative \
         duration like '24h'"
    ))
}

/// A single grouped result with its raw (per-dimension) key, used internally
/// before the key is rendered to JSON. Shared by the per-shard DB aggregator
/// and the cross-shard merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRawGroup {
    /// Per-dimension values aligned with [`DlqAggregateParams::group_by`].
    /// `None` represents a missing/null dimension value (e.g. no activity name).
    pub key: Vec<Option<String>>,
    /// Number of DLQ rows in this group.
    pub count: i64,
    /// Earliest `failed_at` in the group.
    pub first_seen: Option<DateTime<Utc>>,
    /// Latest `failed_at` in the group.
    pub last_seen: Option<DateTime<Utc>>,
    /// Representative `dead_letter_id`s (capped at `samples_per_group`).
    pub sample_ids: Vec<String>,
}

/// Per-shard partial aggregate, merged across shards by [`merge_dlq_aggregates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqAggregatePartial {
    /// Total DLQ rows on this shard, before filters.
    pub total: i64,
    /// DLQ rows on this shard matching the filters, before grouping.
    pub filtered_total: i64,
    /// Per-group partial counts on this shard.
    pub groups: Vec<DlqRawGroup>,
}

/// One group in the merged aggregation response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DlqGroup {
    /// Hierarchical key: a JSON object mapping each grouping dimension to its
    /// value, or `{"_other": true}` for the long-tail rollup row.
    pub key: serde_json::Value,
    /// Number of DLQ rows in this group, summed across shards.
    pub count: i64,
    /// Earliest `failed_at` across shards (omitted for the `_other` row).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<DateTime<Utc>>,
    /// Latest `failed_at` across shards (omitted for the `_other` row).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<DateTime<Utc>>,
    /// Representative `dead_letter_id`s, taken from any shard.
    pub sample_dead_letter_ids: Vec<String>,
}

/// The full DLQ aggregation response.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DlqAggregateResponse {
    /// Total DLQ rows across all shards, before filters.
    pub total: i64,
    /// DLQ rows across all shards matching the filters, before grouping.
    pub filtered_total: i64,
    /// Returned groups, ordered by descending count.
    pub groups: Vec<DlqGroup>,
    /// `true` when the long tail was rolled into an `_other` group.
    pub truncated: bool,
}

fn min_instant(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.min(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn max_instant(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (Some(x), None) | (None, Some(x)) => Some(x),
        (None, None) => None,
    }
}

fn render_group_key(params: &DlqAggregateParams, key: &[Option<String>]) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(key.len());
    for (dim, value) in params.group_by.iter().zip(key.iter()) {
        let v = value
            .clone()
            .map_or(serde_json::Value::Null, serde_json::Value::String);
        obj.insert(dim.as_wire().to_string(), v);
    }
    serde_json::Value::Object(obj)
}

/// Bounded-cardinality "top-N + other" rollup shared by every grouped
/// aggregation read model in this codebase (DLQ aggregation here, and the
/// workflow-count fleet snapshot in `autumn-harvest-plugin`).
///
/// `groups` is sorted descending by `count_of` (ties broken by `key`,
/// ascending) and truncated to `limit`. When the dropped tail's summed count
/// is positive, it is folded into one rollup entry appended via `make_other`
/// and the returned `bool` is `true`; a truncation that only drops zero-count
/// entries reports `false` and appends no rollup row (this never happens for
/// real `COUNT(*)` rows, which are always `>= 1`, but keeps the helper exact
/// for any future caller with synthetic zero-count groups). Per-group counts
/// always reconcile to the pre-rollup total either way.
#[must_use]
pub fn rollup_top_n<K: Ord, A, T>(
    mut groups: Vec<(K, A)>,
    limit: usize,
    count_of: impl Fn(&A) -> i64,
    to_entry: impl Fn(K, A) -> T,
    make_other: impl FnOnce(i64) -> T,
) -> (Vec<T>, bool) {
    groups.sort_by(|a, b| {
        count_of(&b.1)
            .cmp(&count_of(&a.1))
            .then_with(|| a.0.cmp(&b.0))
    });

    if groups.len() <= limit {
        return (
            groups.into_iter().map(|(k, a)| to_entry(k, a)).collect(),
            false,
        );
    }

    let other_count: i64 = groups[limit..].iter().map(|(_, a)| count_of(a)).sum();
    let mut out: Vec<T> = groups.drain(..limit).map(|(k, a)| to_entry(k, a)).collect();
    let truncated = other_count > 0;
    if truncated {
        out.push(make_other(other_count));
    }
    (out, truncated)
}

/// Merge per-shard partial aggregates into a single response.
///
/// Counts sum across shards; `first_seen`/`last_seen` take the global min/max;
/// sample IDs are taken from any shard up to `samples_per_group`. Groups are
/// ordered by descending count (ties broken by key for determinism). When more
/// than `limit_groups` distinct groups exist, the long tail is rolled into a
/// single `{"_other": true}` group so the per-group counts reconcile to
/// `filtered_total`, and `truncated` is set to `true`.
#[must_use]
pub fn merge_dlq_aggregates(
    params: &DlqAggregateParams,
    partials: Vec<DlqAggregatePartial>,
) -> DlqAggregateResponse {
    let mut total = 0i64;
    let mut filtered_total = 0i64;
    let mut merged: HashMap<Vec<Option<String>>, DlqRawGroup> = HashMap::new();

    for partial in partials {
        total += partial.total;
        filtered_total += partial.filtered_total;
        for group in partial.groups {
            let entry = merged
                .entry(group.key.clone())
                .or_insert_with(|| DlqRawGroup {
                    key: group.key.clone(),
                    count: 0,
                    first_seen: None,
                    last_seen: None,
                    sample_ids: Vec::new(),
                });
            entry.count += group.count;
            entry.first_seen = min_instant(entry.first_seen, group.first_seen);
            entry.last_seen = max_instant(entry.last_seen, group.last_seen);
            for id in group.sample_ids {
                if entry.sample_ids.len() < params.samples_per_group as usize {
                    entry.sample_ids.push(id);
                }
            }
        }
    }

    let groups: Vec<(Vec<Option<String>>, DlqRawGroup)> = merged
        .into_values()
        .map(|group| (group.key.clone(), group))
        .collect();
    let limit = params.limit_groups as usize;

    let (groups, truncated) = rollup_top_n(
        groups,
        limit,
        |group| group.count,
        |key, group| DlqGroup {
            key: render_group_key(params, &key),
            count: group.count,
            first_seen: group.first_seen,
            last_seen: group.last_seen,
            sample_dead_letter_ids: group.sample_ids,
        },
        |other_count| DlqGroup {
            key: serde_json::json!({ "_other": true }),
            count: other_count,
            first_seen: None,
            last_seen: None,
            sample_dead_letter_ids: Vec::new(),
        },
    );

    DlqAggregateResponse {
        total,
        filtered_total,
        groups,
        truncated,
    }
}

type AggregateRow = (
    Uuid,
    Option<Uuid>,
    Option<String>,
    String,
    String,
    DateTime<Utc>,
    String,
);

/// Compute a per-shard DLQ aggregate against the connection's own database.
///
/// Loads the filtered dead-letter rows and groups them in process so the derived
/// [`failure_signature`] (which has no stored column) can be computed on read.
/// The returned [`DlqAggregatePartial`] is shard-local; callers fan out across
/// shards with `iter_shards()` and combine the partials with
/// [`merge_dlq_aggregates`].
///
/// # Errors
///
/// Returns [`HarvestError::Database`] on query failure.
pub async fn aggregate_dead_letters(
    conn: &mut AsyncPgConnection,
    params: &DlqAggregateParams,
) -> HarvestResult<DlqAggregatePartial> {
    use crate::schema::harvest_dead_letters::dsl;
    use diesel::dsl::sql;
    use diesel::sql_types::{Bool, Text};

    let total: i64 = dsl::harvest_dead_letters
        .count()
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    let mut query = dsl::harvest_dead_letters.into_boxed();
    if let Some(ref name) = params.activity_name {
        query = query.filter(dsl::activity_name.eq(name.clone()));
    }
    if let Some(ref q) = params.queue_name {
        query = query.filter(dsl::queue_name.eq(q.clone()));
    }
    if let Some(ref tt) = params.task_type {
        query = query.filter(
            sql::<Bool>("LOWER(task_type) = LOWER(")
                .bind::<Text, _>(tt.clone())
                .sql(")"),
        );
    }
    if let Some(since) = params.since {
        query = query.filter(dsl::failed_at.ge(since));
    }
    if let Some(until) = params.until {
        query = query.filter(dsl::failed_at.lt(until));
    }
    if let Some(min) = params.min_attempts {
        query = query.filter(dsl::attempts.ge(min));
    }
    if let Some(ref wf_name) = params.workflow_name {
        query = query.filter(
            sql::<Bool>(
                "workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ",
            )
            .bind::<Text, _>(wf_name.clone())
            .sql(")"),
        );
    }

    let rows: Vec<AggregateRow> = query
        .select((
            dsl::id,
            dsl::workflow_exec_id,
            dsl::activity_name,
            dsl::queue_name,
            dsl::task_type,
            dsl::failed_at,
            dsl::error,
        ))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let filtered_total = i64::try_from(rows.len()).unwrap_or(i64::MAX);

    // Resolve workflow names only when grouping on that dimension.
    let workflow_names = if params.group_by.contains(&DlqGroupDimension::WorkflowName) {
        let exec_ids: Vec<Uuid> = rows.iter().filter_map(|r| r.1).collect();
        load_workflow_names(conn, &exec_ids).await?
    } else {
        HashMap::new()
    };

    let mut groups: HashMap<Vec<Option<String>>, DlqRawGroup> = HashMap::new();
    for (id, exec_id, activity_name, queue_name, task_type, failed_at, error) in rows {
        let key: Vec<Option<String>> = params
            .group_by
            .iter()
            .map(|dim| match dim {
                DlqGroupDimension::WorkflowName => {
                    exec_id.and_then(|e| workflow_names.get(&e).cloned())
                }
                DlqGroupDimension::ActivityName => activity_name.clone(),
                DlqGroupDimension::QueueName => Some(queue_name.clone()),
                DlqGroupDimension::TaskType => Some(task_type.clone()),
                DlqGroupDimension::TimeBucket => Some(params.time_bucket.format(failed_at)),
                DlqGroupDimension::FailureSignature => Some(failure_signature(&error)),
            })
            .collect();

        let entry = groups.entry(key.clone()).or_insert_with(|| DlqRawGroup {
            key,
            count: 0,
            first_seen: None,
            last_seen: None,
            sample_ids: Vec::new(),
        });
        entry.count += 1;
        entry.first_seen = min_instant(entry.first_seen, Some(failed_at));
        entry.last_seen = max_instant(entry.last_seen, Some(failed_at));
        if entry.sample_ids.len() < params.samples_per_group as usize {
            entry.sample_ids.push(id.to_string());
        }
    }

    Ok(DlqAggregatePartial {
        total,
        filtered_total,
        groups: groups.into_values().collect(),
    })
}

async fn load_workflow_names(
    conn: &mut AsyncPgConnection,
    exec_ids: &[Uuid],
) -> HarvestResult<HashMap<Uuid, String>> {
    use crate::schema::harvest_workflow_executions::dsl as wf;

    if exec_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<(Uuid, String)> = wf::harvest_workflow_executions
        .filter(wf::id.eq_any(exec_ids))
        .select((wf::id, wf::workflow_name))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(rows.into_iter().collect())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Redrive filter unit tests (issue #510) ───────────────────────────────

    #[test]
    fn redrive_filter_is_empty_only_when_no_substantive_criterion() {
        assert!(RedriveFilter::default().is_empty());
        // max / dry_run alone do not count.
        assert!(
            RedriveFilter {
                max: Some(10),
                dry_run: true,
                ..Default::default()
            }
            .is_empty()
        );
        // Each substantive field alone is non-empty.
        assert!(
            !RedriveFilter {
                queue: Some("q".into()),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !RedriveFilter {
                workflow_name: Some("wf".into()),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !RedriveFilter {
                error_contains: Some("timeout".into()),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !RedriveFilter {
                dead_lettered_after: Some(Utc::now()),
                ..Default::default()
            }
            .is_empty()
        );
        assert!(
            !RedriveFilter {
                dead_letter_ids: Some(vec![Uuid::new_v4()]),
                ..Default::default()
            }
            .is_empty()
        );
        // An empty id vector is not a substantive criterion.
        assert!(
            RedriveFilter {
                dead_letter_ids: Some(vec![]),
                ..Default::default()
            }
            .is_empty()
        );
    }

    #[test]
    fn redrive_filter_effective_max_clamps() {
        assert_eq!(RedriveFilter::default().effective_max(), 100);
        assert_eq!(
            RedriveFilter {
                max: Some(0),
                ..Default::default()
            }
            .effective_max(),
            1,
            "zero must clamp to 1, never a silent LIMIT 0"
        );
        assert_eq!(
            RedriveFilter {
                max: Some(50_000),
                ..Default::default()
            }
            .effective_max(),
            i64::from(MAX_BULK_LIMIT)
        );
        assert_eq!(
            RedriveFilter {
                max: Some(250),
                ..Default::default()
            }
            .effective_max(),
            250
        );
    }

    #[test]
    fn escape_like_escapes_wildcards() {
        assert_eq!(escape_like("plain"), "plain");
        assert_eq!(escape_like("50%"), "50\\%");
        assert_eq!(escape_like("a_b"), "a\\_b");
        assert_eq!(escape_like("path\\x"), "path\\\\x");
        assert_eq!(escape_like("%_\\"), "\\%\\_\\\\");
    }

    #[test]
    fn redrive_outcome_is_copy_eq() {
        let id = Uuid::new_v4();
        assert_eq!(RedriveOutcome::Redriven(id), RedriveOutcome::Redriven(id));
        assert_ne!(RedriveOutcome::Redriven(id), RedriveOutcome::Skipped);
    }

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
            owner: None,
            severity: None,
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
            owner: None,
            severity: None,
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
    fn dead_letter_reason_history_cap_is_typed_json() {
        let reason = DeadLetterReason::HistoryCapExceeded {
            count: 12_001,
            cap: 12_000,
            workflow_type: "billing_poll".into(),
        };

        let json = reason.to_string();
        let back: DeadLetterReason =
            serde_json::from_str(&json).expect("typed reason should deserialize");

        assert_eq!(back, reason);
        assert!(json.contains("HistoryCapExceeded"));
    }

    #[test]
    fn dead_letter_reason_poison_pill_is_typed_json() {
        let reason = DeadLetterReason::PoisonPill {
            crash_strikes: 3,
            last_worker_id: Some("worker-7".into()),
        };

        let json = reason.to_string();
        let back: DeadLetterReason =
            serde_json::from_str(&json).expect("typed reason should deserialize");

        assert_eq!(back, reason);
        // Carries the discriminator distinguishing it from clean retry exhaustion,
        // plus the crash-strike count and last worker for triage (issue #367 AC5).
        assert!(json.contains("PoisonPill"));
        assert!(json.contains("crash_strikes"));
        assert!(json.contains("worker-7"));
    }

    #[test]
    fn dead_letter_reason_workflow_task_timeout_is_typed_json() {
        let reason = DeadLetterReason::WorkflowTaskTimeout {
            task_timeout_strikes: 3,
            timeout_secs: 10,
        };

        let json = reason.to_string();
        let back: DeadLetterReason =
            serde_json::from_str(&json).expect("typed reason should deserialize");

        assert_eq!(back, reason);
        assert!(json.contains("WorkflowTaskTimeout"));
        assert!(json.contains("task_timeout_strikes"));
        assert!(json.contains("timeout_secs"));
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
    fn bulk_filter_effective_limit_floors_at_1_for_zero() {
        let filter = BulkDlqFilter {
            limit: Some(0),
            ..BulkDlqFilter::default()
        };
        assert_eq!(filter.effective_limit(), 1);
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

    // ----- Failure signature derivation (issue #385) -----

    #[test]
    fn failure_signature_passes_through_clean_message() {
        assert_eq!(
            failure_signature("stripe: rate limited"),
            "stripe: rate limited"
        );
    }

    #[test]
    fn failure_signature_normalizes_uuid() {
        let sig = failure_signature("order 550e8400-e29b-41d4-a716-446655440000 not found");
        assert_eq!(sig, "order <UUID> not found");
    }

    #[test]
    fn failure_signature_normalizes_decimal_runs() {
        let sig = failure_signature("timeout after 30000ms connecting to port 5432");
        assert_eq!(sig, "timeout after <NUM>ms connecting to port <NUM>");
    }

    #[test]
    fn failure_signature_pure_decimal_gte8_digits_is_num_not_hex() {
        // Pure decimal strings ≥8 digits must not be classified as <HEX>.
        let sig = failure_signature("error for user 12345678 in tenant 999999999");
        assert_eq!(sig, "error for user <NUM> in tenant <NUM>");
    }

    #[test]
    fn failure_signature_normalizes_long_hex_run() {
        let sig = failure_signature("checksum mismatch deadbeef0123cafe babe");
        // "deadbeef0123cafe" contains hex letters (d, e, a, b, e, f, c, a, f, e) → <HEX>;
        // "babe" is short → left intact.
        assert_eq!(sig, "checksum mismatch <HEX> babe");
    }

    #[test]
    fn failure_signature_normalizes_0x_prefixed_hex() {
        let sig = failure_signature("bad pointer 0xDEAD here");
        assert_eq!(sig, "bad pointer <HEX> here");
    }

    #[test]
    fn failure_signature_takes_first_line_only() {
        let sig = failure_signature("connection refused\n  at src/net.rs:42\n  backtrace...");
        assert_eq!(sig, "connection refused");
    }

    #[test]
    fn failure_signature_truncates_to_max_len() {
        let long = "x".repeat(500);
        let sig = failure_signature(&long);
        assert_eq!(sig.chars().count(), SIGNATURE_MAX_LEN);
    }

    #[test]
    fn failure_signature_is_deterministic_and_shard_stable() {
        let a = failure_signature("stripe charge ch_3Abc declined for user 1234");
        let b = failure_signature("stripe charge ch_3Abc declined for user 1234");
        assert_eq!(a, b);
        // Same root cause with different dynamic ids collapses identically.
        let c = failure_signature("stripe charge ch_3Abc declined for user 9999");
        assert_eq!(a, c);
    }

    // ----- Parameter parsing & validation (issue #385) -----

    fn pairs(items: &[(&str, &str)]) -> Vec<(String, String)> {
        items
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn params_parse_single_group_by() {
        let p = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "workflow_name")]),
            Utc::now(),
        )
        .expect("valid");
        assert_eq!(p.group_by, vec![DlqGroupDimension::WorkflowName]);
        assert_eq!(p.limit_groups, DEFAULT_LIMIT_GROUPS);
        assert_eq!(p.samples_per_group, DEFAULT_SAMPLES_PER_GROUP);
    }

    #[test]
    fn params_parse_hierarchical_group_by_preserves_order() {
        let p = DlqAggregateParams::from_query_pairs(
            &pairs(&[
                ("group_by", "workflow_name"),
                ("group_by", "failure_signature"),
            ]),
            Utc::now(),
        )
        .expect("valid");
        assert_eq!(
            p.group_by,
            vec![
                DlqGroupDimension::WorkflowName,
                DlqGroupDimension::FailureSignature
            ]
        );
    }

    #[test]
    fn params_dedupe_repeated_group_by() {
        let p = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("group_by", "queue_name")]),
            Utc::now(),
        )
        .expect("valid");
        assert_eq!(p.group_by, vec![DlqGroupDimension::QueueName]);
    }

    #[test]
    fn params_require_at_least_one_group_by() {
        let err =
            DlqAggregateParams::from_query_pairs(&pairs(&[("limit_groups", "10")]), Utc::now())
                .unwrap_err();
        assert!(err.contains("at least one group_by"), "{err}");
    }

    #[test]
    fn params_reject_unknown_group_by() {
        let err =
            DlqAggregateParams::from_query_pairs(&pairs(&[("group_by", "tenant_id")]), Utc::now())
                .unwrap_err();
        assert!(err.contains("unknown group_by dimension"), "{err}");
    }

    #[test]
    fn params_reject_invalid_time_bucket() {
        let err = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "time_bucket"), ("time_bucket", "week")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err.contains("invalid time_bucket"), "{err}");
    }

    #[test]
    fn params_reject_limit_groups_out_of_range() {
        let err = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("limit_groups", "9999")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err.contains("limit_groups"), "{err}");

        let err_zero = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("limit_groups", "0")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err_zero.contains("limit_groups"), "{err_zero}");
    }

    #[test]
    fn params_reject_samples_per_group_out_of_range() {
        let err = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("samples_per_group", "50")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err.contains("samples_per_group"), "{err}");
    }

    #[test]
    fn params_reject_malformed_duration() {
        let err = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("since", "yesterday")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err.contains("invalid since"), "{err}");
    }

    #[test]
    fn params_parse_relative_since_duration() {
        let now = Utc::now();
        let p = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("since", "24h")]),
            now,
        )
        .expect("valid");
        let since = p.since.expect("since set");
        let delta = (now - since).num_seconds();
        assert!((86_390..=86_410).contains(&delta), "delta was {delta}");
    }

    #[test]
    fn params_parse_rfc3339_since() {
        let p = DlqAggregateParams::from_query_pairs(
            &pairs(&[
                ("group_by", "queue_name"),
                ("since", "2026-05-18T03:00:00Z"),
            ]),
            Utc::now(),
        )
        .expect("valid");
        assert_eq!(
            p.since
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "2026-05-18T03:00:00Z"
        );
    }

    #[test]
    fn params_reject_negative_min_attempts() {
        let err = DlqAggregateParams::from_query_pairs(
            &pairs(&[("group_by", "queue_name"), ("min_attempts", "-1")]),
            Utc::now(),
        )
        .unwrap_err();
        assert!(err.contains("min_attempts"), "{err}");
    }

    // ----- Merge logic (issue #385) -----

    fn raw_group(values: &[Option<&str>], count: i64, samples: &[&str]) -> DlqRawGroup {
        DlqRawGroup {
            key: values.iter().map(|v| v.map(str::to_string)).collect(),
            count,
            first_seen: None,
            last_seen: None,
            sample_ids: samples.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    #[test]
    fn merge_sums_counts_across_shards() {
        let params = DlqAggregateParams {
            group_by: vec![DlqGroupDimension::QueueName],
            ..Default::default()
        };
        let partials = vec![
            DlqAggregatePartial {
                total: 10,
                filtered_total: 6,
                groups: vec![raw_group(&[Some("email")], 4, &["a"])],
            },
            DlqAggregatePartial {
                total: 8,
                filtered_total: 5,
                groups: vec![raw_group(&[Some("email")], 3, &["b"])],
            },
        ];
        let resp = merge_dlq_aggregates(&params, partials);
        assert_eq!(resp.total, 18);
        assert_eq!(resp.filtered_total, 11);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].count, 7);
        assert_eq!(resp.groups[0].key["queue_name"], "email");
        assert!(!resp.truncated);
    }

    #[test]
    fn merge_caps_samples_per_group() {
        let params = DlqAggregateParams {
            group_by: vec![DlqGroupDimension::QueueName],
            samples_per_group: 2,
            ..Default::default()
        };
        let partials = vec![
            DlqAggregatePartial {
                total: 0,
                filtered_total: 3,
                groups: vec![raw_group(&[Some("q")], 3, &["a", "b"])],
            },
            DlqAggregatePartial {
                total: 0,
                filtered_total: 2,
                groups: vec![raw_group(&[Some("q")], 2, &["c", "d"])],
            },
        ];
        let resp = merge_dlq_aggregates(&params, partials);
        assert_eq!(resp.groups[0].sample_dead_letter_ids.len(), 2);
    }

    #[test]
    fn merge_rolls_long_tail_into_other_and_reconciles() {
        let params = DlqAggregateParams {
            group_by: vec![DlqGroupDimension::FailureSignature],
            limit_groups: 2,
            ..Default::default()
        };
        let partials = vec![DlqAggregatePartial {
            total: 100,
            filtered_total: 100,
            groups: vec![
                raw_group(&[Some("sig-a")], 50, &["a"]),
                raw_group(&[Some("sig-b")], 30, &["b"]),
                raw_group(&[Some("sig-c")], 15, &["c"]),
                raw_group(&[Some("sig-d")], 5, &["d"]),
            ],
        }];
        let resp = merge_dlq_aggregates(&params, partials);
        assert!(resp.truncated);
        // 2 top groups + 1 _other rollup.
        assert_eq!(resp.groups.len(), 3);
        assert_eq!(resp.groups[0].count, 50);
        assert_eq!(resp.groups[1].count, 30);
        let other = resp.groups.last().unwrap();
        assert_eq!(other.key["_other"], true);
        assert_eq!(other.count, 20);
        // Totals reconcile: sum of group counts == filtered_total.
        let sum: i64 = resp.groups.iter().map(|g| g.count).sum();
        assert_eq!(sum, resp.filtered_total);
    }

    #[test]
    fn merge_orders_groups_by_descending_count() {
        let params = DlqAggregateParams {
            group_by: vec![DlqGroupDimension::QueueName],
            ..Default::default()
        };
        let partials = vec![DlqAggregatePartial {
            total: 0,
            filtered_total: 60,
            groups: vec![
                raw_group(&[Some("small")], 10, &[]),
                raw_group(&[Some("big")], 50, &[]),
            ],
        }];
        let resp = merge_dlq_aggregates(&params, partials);
        assert_eq!(resp.groups[0].key["queue_name"], "big");
        assert_eq!(resp.groups[1].key["queue_name"], "small");
    }

    #[test]
    fn merge_renders_hierarchical_key() {
        let params = DlqAggregateParams {
            group_by: vec![
                DlqGroupDimension::WorkflowName,
                DlqGroupDimension::FailureSignature,
            ],
            ..Default::default()
        };
        let partials = vec![DlqAggregatePartial {
            total: 0,
            filtered_total: 1,
            groups: vec![raw_group(
                &[Some("onboarding"), Some("stripe: rate limited")],
                1,
                &["x"],
            )],
        }];
        let resp = merge_dlq_aggregates(&params, partials);
        assert_eq!(resp.groups[0].key["workflow_name"], "onboarding");
        assert_eq!(
            resp.groups[0].key["failure_signature"],
            "stripe: rate limited"
        );
    }

    #[test]
    fn merge_renders_null_for_missing_dimension_value() {
        let params = DlqAggregateParams {
            group_by: vec![DlqGroupDimension::ActivityName],
            ..Default::default()
        };
        let partials = vec![DlqAggregatePartial {
            total: 0,
            filtered_total: 1,
            groups: vec![raw_group(&[None], 1, &["x"])],
        }];
        let resp = merge_dlq_aggregates(&params, partials);
        assert!(resp.groups[0].key["activity_name"].is_null());
    }

    #[test]
    fn dimension_wire_round_trip() {
        for dim in [
            DlqGroupDimension::WorkflowName,
            DlqGroupDimension::ActivityName,
            DlqGroupDimension::QueueName,
            DlqGroupDimension::TaskType,
            DlqGroupDimension::TimeBucket,
            DlqGroupDimension::FailureSignature,
        ] {
            assert_eq!(DlqGroupDimension::from_wire(dim.as_wire()), Some(dim));
        }
    }

    #[test]
    fn time_bucket_formats() {
        let ts = DateTime::parse_from_rfc3339("2026-05-18T03:42:17Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(
            TimeBucketGranularity::Hour.format(ts),
            "2026-05-18T03:00:00Z"
        );
        assert_eq!(TimeBucketGranularity::Day.format(ts), "2026-05-18");
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
            owner: None,
            severity: None,
        };

        let json = serde_json::to_string(&entry).expect("should serialize");
        let back: NewDeadLetterEntry = serde_json::from_str(&json).expect("should deserialize");

        assert_eq!(back.queue_name, "billing");
        assert_eq!(back.attempts, 5);
    }
}
