//! Debounced workflow starts — collapse trigger bursts into one run (issue #499).
//!
//! # Overview
//!
//! Webhook deliveries and user actions arrive in bursts: a provider retries an
//! event 5×, a user mashes "save", an upstream fan-out emits a dozen row-change
//! notifications. Today every trigger starts a run. `DebouncePolicy` lets an
//! embedder declare a *key expression* and a *window*:
//!
//! ```rust
//! use autumn_harvest::debounce::DebouncePolicy;
//! use std::time::Duration;
//!
//! let policy = DebouncePolicy {
//!     key_expr: "input.user_id",
//!     window: Duration::from_secs(30),
//!     max_wait: Some(Duration::from_secs(300)),
//! };
//! assert_eq!(policy.window, Duration::from_secs(30));
//! ```
//!
//! # Semantics
//!
//! **Trailing-edge**: each qualifying start request for the same resolved key
//! (re)sets the fire deadline to `now + window`. The run begins only after
//! `window` elapses with no further qualifying request.
//!
//! **Burst collapse**: given K start requests for the same key all arriving
//! inside the window, exactly **one** workflow execution is started.
//!
//! **Last-input-wins**: the started run uses the most recent qualifying
//! request's input. Embedders must not assume the first input is used.
//!
//! **Max-wait cap**: a configurable upper bound (`max_wait`) prevents an
//! endlessly-bursting key from starving the run forever. Default is the
//! [`WorkerConfig::default_debounce_max_wait`] value (1 hour unless
//! overridden).
//!
//! # Scoping
//!
//! Debounce coordination is **shard-local** — consistent with the per-key
//! concurrency scope established by issue #247. Cross-shard global debounce
//! coordination is out of scope; embedders wanting a global cap should route
//! all executions for a given key to a single shard.
//!
//! # Replay safety
//!
//! Debounce is a **pre-start admission gate** that runs *before*
//! `WorkflowStarted` is appended. It introduces **no new `WorkflowEvent`
//! variant** and has zero effect on replay determinism of started runs.
//!
//! [`WorkerConfig::default_debounce_max_wait`]: crate::builder::WorkerConfig::default_debounce_max_wait

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Declarative debounce configuration attached to a [`crate::info::WorkflowInfo`].
///
/// Declared via `#[workflow(debounce(key = "input.user_id", window = "30s"))]`
/// or the [`WorkflowInfo::with_debounce`](crate::info::WorkflowInfo::with_debounce)
/// fluent builder method.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DebouncePolicy {
    /// JSON field path (dot-notation) resolved against the workflow input to
    /// produce the debounce group key. The `"input."` prefix is stripped if
    /// present so `"input.user_id"` and `"user_id"` are equivalent.
    ///
    /// Nested paths like `"event.tenant_id"` walk into nested JSON objects.
    pub key_expr: &'static str,
    /// Trailing-edge window. Each qualifying start (re)sets the fire deadline
    /// to `now + window`. The run begins only after this much quiet time.
    pub window: Duration,
    /// Optional absolute cap on total deferral. When `Some(d)`, a continuously-
    /// bursting key fires no later than `first_seen + d` regardless of ongoing
    /// requests. When `None`, the [`crate::builder::WorkerConfig::default_debounce_max_wait`]
    /// value (1 hour by default) is used.
    pub max_wait: Option<Duration>,
}

/// Resolve the debounce key for a given input, using the same dot-notation
/// path resolution as per-key concurrency (issue #247).
///
/// Delegates to [`crate::concurrency::resolve_concurrency_key`] so both
/// features share the same key resolver without duplication.
///
/// Returns `None` when the path is missing or resolves to JSON `null`; in
/// that case the start falls through to the normal (non-debounced) path.
#[must_use]
pub fn resolve_debounce_key(expr: &str, input: &serde_json::Value) -> Option<String> {
    crate::concurrency::resolve_concurrency_key(expr, input)
}

/// Compute the trailing-edge fire deadline for one debounce upsert.
///
/// Returns `min(now + window, first_seen + max_wait)`:
/// - `now + window` is the trailing-edge extension.
/// - `first_seen + max_wait` is the hard cap that prevents endless deferral.
///
/// This function is pure and unit-testable without a database.
#[must_use]
pub fn compute_fire_deadline(
    now: DateTime<Utc>,
    window: Duration,
    first_seen: DateTime<Utc>,
    max_wait: Duration,
) -> DateTime<Utc> {
    let window_chrono = chrono::Duration::from_std(window).unwrap_or(chrono::Duration::MAX);
    let max_wait_chrono = chrono::Duration::from_std(max_wait).unwrap_or(chrono::Duration::MAX);

    let trailing_edge = now + window_chrono;
    let cap = first_seen + max_wait_chrono;

    trailing_edge.min(cap)
}

// ---------------------------------------------------------------------------
// DB-gated types and functions
// ---------------------------------------------------------------------------

/// Serialised start options stored alongside the last qualifying input.
///
/// Persisted as opaque JSONB in `harvest_debounce.start_options` so the
/// schema stays stable even as `StartWorkflowParams` grows.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct DebounceStartOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reuse_policy: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_timeout_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memo: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_attrs: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sla_secs: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_headers: Option<std::collections::HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency_limit: Option<u32>,
}

/// Parameters for [`admit_debounced_start`].
#[cfg(feature = "db")]
pub struct AdmitDebounceParams<'a> {
    pub workflow_name: &'a str,
    pub debounce_key: &'a str,
    pub workflow_id: &'a str,
    pub queue_name: &'a str,
    pub last_input: serde_json::Value,
    pub start_options: DebounceStartOptions,
    /// Pre-computed window for this admission (from the effective policy).
    pub window: Duration,
    /// Pre-computed `max_wait` cap (policy value or builder default).
    pub max_wait: Duration,
    pub shard_id: i32,
}

/// Result of a successful debounce admission.
#[cfg(feature = "db")]
#[derive(Debug)]
pub struct DebounceAdmitOutcome {
    /// The resolved debounce key used for the upsert.
    pub debounce_key: String,
    /// Current fire deadline after this admission.
    pub fire_at: DateTime<Utc>,
    /// Total qualifying requests seen for this key since the record was created.
    pub pending_count: i32,
    /// `true` when this admission created a brand-new pending record.
    pub is_new_record: bool,
}

/// A pending debounce record surfaced by the management API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingDebounceRecord {
    pub id: uuid::Uuid,
    pub workflow_name: String,
    pub debounce_key: String,
    pub workflow_id: String,
    pub queue_name: String,
    pub effective_fire_at: DateTime<Utc>,
    pub max_fire_at: DateTime<Utc>,
    pub pending_count: i32,
    pub shard_id: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Upsert a pending debounce record for the given key.
///
/// On the **first qualifying request** for a key, inserts a new row with
/// `effective_fire_at = min(now + window, now + max_wait)` and
/// `max_fire_at = now + max_wait`.
///
/// On **subsequent requests**, updates `effective_fire_at = min(now + window,
/// existing max_fire_at)`, increments `pending_count`, and overwrites
/// `last_input` and `start_options` with the most recent request's values
/// (**last-input-wins**).
///
/// Returns the current state of the record after the upsert.
///
/// # Errors
/// Returns `HarvestError` if the database upsert fails.
#[cfg(feature = "db")]
pub async fn admit_debounced_start(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitDebounceParams<'_>,
) -> crate::error::HarvestResult<DebounceAdmitOutcome> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct UpsertRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        debounce_key: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        effective_fire_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pending_count: i32,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        is_new_record: bool,
    }

    let now = Utc::now();
    let window_chrono = chrono::Duration::from_std(params.window).unwrap_or(chrono::Duration::MAX);
    let max_wait_chrono =
        chrono::Duration::from_std(params.max_wait).unwrap_or(chrono::Duration::MAX);

    let initial_fire_at = (now + window_chrono).min(now + max_wait_chrono);
    let max_fire_at = now + max_wait_chrono;

    let new_id = uuid::Uuid::new_v4();
    let start_options_json = serde_json::to_value(&params.start_options)
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));

    // Upsert: insert on first request, update deadline + last-input on collision.
    // effective_fire_at is LEAST(now + window, existing max_fire_at) so the cap
    // is honoured across the entire burst.
    let sql = "
        INSERT INTO harvest_debounce
            (id, workflow_name, debounce_key, workflow_id, queue_name,
             last_input, start_options, effective_fire_at, max_fire_at,
             pending_count, shard_id, created_at, updated_at)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1, $10, NOW(), NOW())
        ON CONFLICT (workflow_name, debounce_key) DO UPDATE SET
            workflow_id       = EXCLUDED.workflow_id,
            last_input        = EXCLUDED.last_input,
            start_options     = EXCLUDED.start_options,
            effective_fire_at = LEAST($8, harvest_debounce.max_fire_at),
            pending_count     = harvest_debounce.pending_count + 1,
            updated_at        = NOW()
        RETURNING
            debounce_key,
            effective_fire_at,
            pending_count,
            -- xmax = 0 means the row was just inserted (not updated)
            (xmax = 0) AS is_new_record
    ";

    let row: UpsertRow = diesel::sql_query(sql)
        .bind::<diesel::sql_types::Uuid, _>(new_id)
        .bind::<diesel::sql_types::Text, _>(params.workflow_name)
        .bind::<diesel::sql_types::Text, _>(&params.debounce_key)
        .bind::<diesel::sql_types::Text, _>(&params.workflow_id)
        .bind::<diesel::sql_types::Text, _>(params.queue_name)
        .bind::<diesel::sql_types::Jsonb, _>(params.last_input)
        .bind::<diesel::sql_types::Jsonb, _>(start_options_json)
        .bind::<diesel::sql_types::Timestamptz, _>(initial_fire_at)
        .bind::<diesel::sql_types::Timestamptz, _>(max_fire_at)
        .bind::<diesel::sql_types::Integer, _>(params.shard_id)
        .get_result(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(DebounceAdmitOutcome {
        debounce_key: row.debounce_key,
        fire_at: row.effective_fire_at,
        pending_count: row.pending_count,
        is_new_record: row.is_new_record,
    })
}

/// Maximum number of debounce rows fired per scanner tick.
/// Prevents a single shard from being overwhelmed if many keys come due simultaneously.
pub const DEBOUNCE_FIRE_BATCH_SIZE: i64 = 100;

/// Internal row type returned by the debounce fire query.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct FireDueRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    debounce_key: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    last_input: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    start_options: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    shard_id: i32,
}

/// Fire all pending debounce records whose `effective_fire_at` has elapsed.
///
/// For each due row: claims it (`FOR UPDATE SKIP LOCKED`), calls
/// `start_or_load_workflow_execution` with the stored last-input and options,
/// then deletes the row. All three steps happen in one transaction so a crash
/// rolls back and the next scanner tick retries.
///
/// Called from [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is spawned.
/// Returns the number of executions started.
///
/// # Errors
/// Returns `HarvestError` if the database query or any execution start fails.
#[cfg(feature = "db")]
pub async fn fire_due_debounced_starts(
    conn: &mut diesel_async::AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    use diesel_async::RunQueryDsl;

    let now = Utc::now();

    // Claim due rows with SKIP LOCKED so multiple workers never fire the same key.
    let due_sql = "
        SELECT id, workflow_name, debounce_key, workflow_id, queue_name,
               last_input, start_options, shard_id
        FROM harvest_debounce
        WHERE effective_fire_at <= $1
        LIMIT $2
        FOR UPDATE SKIP LOCKED
    ";

    let due_rows: Vec<FireDueRow> = diesel::sql_query(due_sql)
        .bind::<diesel::sql_types::Timestamptz, _>(now)
        .bind::<diesel::sql_types::BigInt, _>(DEBOUNCE_FIRE_BATCH_SIZE)
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = due_rows.len();
    if count == 0 {
        return Ok(0);
    }

    for row in due_rows {
        fire_single_debounce_row(conn, row, metrics).await?;
    }

    Ok(count)
}

/// Fire a single due debounce row: start the execution and delete the record,
/// all within one transaction. Extracted to keep `fire_due_debounced_starts`
/// under the 100-line function length limit.
#[cfg(feature = "db")]
async fn fire_single_debounce_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueRow,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<()> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let opts: DebounceStartOptions = serde_json::from_value(row.start_options).unwrap_or_default();

    let shard = crate::types::ShardId::new(row.shard_id);
    let exec_id = crate::types::ExecutionId::new_for_shard(shard);

    let reuse_policy = opts
        .reuse_policy
        .as_deref()
        .and_then(parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);

    let execution_timeout = opts
        .execution_timeout_secs
        .and_then(chrono::Duration::try_seconds);

    let sla = opts.sla_secs.and_then(chrono::Duration::try_seconds);

    let concurrency_key = opts.concurrency_key;
    let concurrency_limit = opts.concurrency_limit;
    let priority = opts
        .priority
        .and_then(crate::types::Priority::from_i32)
        .unwrap_or_default();

    let workflow_name = row.workflow_name;
    let workflow_id = row.workflow_id;
    let queue_name = row.queue_name;
    let debounce_key = row.debounce_key;
    let row_id = row.id;
    let last_input = row.last_input;

    let params = crate::execution::StartWorkflowParams {
        workflow_name: &workflow_name,
        workflow_id: &workflow_id,
        exec_id,
        input: last_input,
        parent_id: None,
        queue_name: &queue_name,
        execution_timeout,
        memo: opts.memo,
        search_attrs: opts.search_attrs,
        reuse_policy,
        trace_context: None,
        max_execution_timeout_ceiling: None,
        concurrency_key,
        concurrency_limit,
        priority,
        max_workflow_input_bytes: u64::MAX,
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: None,
        runbook_url: None,
        severity: None,
        context_headers: opts.context_headers,
        sla,
        schedule_id: None,
        scheduled_for: None,
    };

    let started_result = conn
        .transaction::<crate::execution::StartedWorkflowExecution, crate::error::HarvestError, _>(
            |conn| {
                let params = params;
                let workflow_name = workflow_name.clone();
                let queue_name = queue_name.clone();
                let debounce_key = debounce_key.clone();
                Box::pin(async move {
                    let started =
                        crate::execution::start_or_load_workflow_execution(conn, params).await?;

                    // Delete the pending record now that the execution is started.
                    diesel::sql_query("DELETE FROM harvest_debounce WHERE id = $1")
                        .bind::<diesel::sql_types::Uuid, _>(row_id)
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;

                    tracing::info!(
                        workflow_name = %workflow_name,
                        debounce_key = %debounce_key,
                        exec_id = %started.exec_id,
                        queue = %queue_name,
                        "debounced start fired",
                    );

                    Ok(started)
                })
            },
        )
        .await?;

    metrics.record_debounce_fired(&started_result.workflow_name, &queue_name);
    Ok(())
}

#[cfg(feature = "db")]
fn parse_reuse_policy(s: &str) -> Option<crate::types::WorkflowIdReusePolicy> {
    use crate::types::WorkflowIdReusePolicy::{
        AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate, TerminateIfRunning,
    };
    match s {
        "allow_duplicate" => Some(AllowDuplicate),
        "reject_duplicate" => Some(RejectDuplicate),
        "allow_duplicate_failed_only" => Some(AllowDuplicateFailedOnly),
        "terminate_if_running" => Some(TerminateIfRunning),
        _ => None,
    }
}

/// List all pending debounce records on this shard for the management API.
///
/// # Errors
/// Returns `HarvestError` if the database query fails.
#[cfg(feature = "db")]
pub async fn list_pending_debounce(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Vec<PendingDebounceRecord>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct ListRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        debounce_key: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        effective_fire_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        max_fire_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pending_count: i32,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        shard_id: i32,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    let rows: Vec<ListRow> = diesel::sql_query(
        "SELECT id, workflow_name, debounce_key, workflow_id, queue_name,
                effective_fire_at, max_fire_at, pending_count, shard_id,
                created_at, updated_at
         FROM harvest_debounce
         ORDER BY effective_fire_at ASC",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows
        .into_iter()
        .map(|row| PendingDebounceRecord {
            id: row.id,
            workflow_name: row.workflow_name,
            debounce_key: row.debounce_key,
            workflow_id: row.workflow_id,
            queue_name: row.queue_name,
            effective_fire_at: row.effective_fire_at,
            max_fire_at: row.max_fire_at,
            pending_count: row.pending_count,
            shard_id: row.shard_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Unit tests (no DB required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    fn ts(year: i32, month: u32, day: u32, h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(year, month, day, h, m, s).unwrap()
    }

    // ── DebouncePolicy construction ──────────────────────────────────────────

    #[test]
    fn policy_fields_accessible() {
        let policy = DebouncePolicy {
            key_expr: "input.user_id",
            window: Duration::from_secs(30),
            max_wait: Some(Duration::from_secs(300)),
        };
        assert_eq!(policy.key_expr, "input.user_id");
        assert_eq!(policy.window, Duration::from_secs(30));
        assert_eq!(policy.max_wait, Some(Duration::from_secs(300)));
    }

    #[test]
    fn policy_max_wait_none() {
        let policy = DebouncePolicy {
            key_expr: "tenant_id",
            window: Duration::from_secs(10),
            max_wait: None,
        };
        assert!(policy.max_wait.is_none());
    }

    // ── resolve_debounce_key ─────────────────────────────────────────────────

    #[test]
    fn resolve_top_level_field() {
        let input = serde_json::json!({ "user_id": "alice" });
        assert_eq!(
            resolve_debounce_key("user_id", &input),
            Some("alice".to_string())
        );
    }

    #[test]
    fn resolve_input_prefix_stripped() {
        let input = serde_json::json!({ "user_id": "alice" });
        assert_eq!(
            resolve_debounce_key("input.user_id", &input),
            Some("alice".to_string())
        );
    }

    #[test]
    fn resolve_nested_path() {
        let input = serde_json::json!({ "event": { "tenant_id": "acme" } });
        assert_eq!(
            resolve_debounce_key("event.tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_missing_field_returns_none() {
        let input = serde_json::json!({ "other": "val" });
        assert_eq!(resolve_debounce_key("user_id", &input), None);
    }

    #[test]
    fn resolve_null_returns_none() {
        let input = serde_json::json!({ "user_id": null });
        assert_eq!(resolve_debounce_key("user_id", &input), None);
    }

    #[test]
    fn resolve_integer_coerced_to_string() {
        let input = serde_json::json!({ "user_id": 42 });
        assert_eq!(
            resolve_debounce_key("user_id", &input),
            Some("42".to_string())
        );
    }

    #[test]
    fn resolve_non_object_input_returns_none() {
        let input = serde_json::json!("plain");
        assert_eq!(resolve_debounce_key("user_id", &input), None);
    }

    // ── compute_fire_deadline ────────────────────────────────────────────────

    #[test]
    fn trailing_edge_extension() {
        // Each call to compute_fire_deadline pushes the deadline out.
        let t0 = ts(2026, 6, 18, 10, 0, 0);
        let window = Duration::from_secs(30);
        let max_wait = Duration::from_secs(300);
        let first_seen = t0;

        // First request: deadline = t0 + 30s
        let d1 = compute_fire_deadline(t0, window, first_seen, max_wait);
        assert_eq!(d1, t0 + chrono::Duration::seconds(30));

        // Second request arrives 10s later: deadline = (t0+10) + 30s = t0+40s
        let t1 = t0 + chrono::Duration::seconds(10);
        let d2 = compute_fire_deadline(t1, window, first_seen, max_wait);
        assert_eq!(d2, t0 + chrono::Duration::seconds(40));

        // Deadline moved out (trailing edge).
        assert!(d2 > d1);
    }

    #[test]
    fn max_wait_cap_clamps_deadline() {
        let t0 = ts(2026, 6, 18, 10, 0, 0);
        let window = Duration::from_secs(30);
        let max_wait = Duration::from_secs(60); // tight cap

        // Request arrives 40s after first_seen: trailing edge = now+30s = t0+70s,
        // but cap = t0+60s → deadline is clamped to t0+60s.
        let t1 = t0 + chrono::Duration::seconds(40);
        let deadline = compute_fire_deadline(t1, window, t0, max_wait);
        let cap = t0 + chrono::Duration::seconds(60);
        assert_eq!(deadline, cap, "deadline should be clamped to max_fire_at");
    }

    #[test]
    fn deadline_never_exceeds_max_fire_at() {
        let t0 = ts(2026, 6, 18, 10, 0, 0);
        let window = Duration::from_secs(30);
        let max_wait = Duration::from_secs(60);
        let cap = t0 + chrono::Duration::seconds(60);

        // Simulate 20 rapid-fire requests
        for i in 0..20u64 {
            let now = t0 + chrono::Duration::seconds(i as i64 * 5);
            let deadline = compute_fire_deadline(now, window, t0, max_wait);
            assert!(
                deadline <= cap,
                "deadline {deadline} exceeded cap {cap} at request {i}"
            );
        }
    }

    #[test]
    fn independent_keys_have_independent_deadlines() {
        let t0 = ts(2026, 6, 18, 10, 0, 0);
        let window = Duration::from_secs(30);
        let max_wait = Duration::from_secs(300);

        // Key A: last request at t0
        let deadline_a = compute_fire_deadline(t0, window, t0, max_wait);

        // Key B: last request at t0 + 15s
        let t1 = t0 + chrono::Duration::seconds(15);
        let deadline_b = compute_fire_deadline(t1, window, t1, max_wait);

        // Independent keys produce different deadlines
        assert_ne!(deadline_a, deadline_b);
        assert_eq!(deadline_a, t0 + chrono::Duration::seconds(30));
        assert_eq!(deadline_b, t0 + chrono::Duration::seconds(45));
    }

    #[test]
    fn window_zero_fires_immediately() {
        let t0 = ts(2026, 6, 18, 10, 0, 0);
        // A zero window means every request fires immediately (trailing edge = now).
        let deadline = compute_fire_deadline(t0, Duration::ZERO, t0, Duration::from_secs(300));
        assert_eq!(deadline, t0, "zero window should produce fire_at = now");
    }
}
