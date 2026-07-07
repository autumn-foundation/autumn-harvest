//! Workflow-start throttle — pace admissions, defer the excess (issue #607).
//!
//! # Overview
//!
//! The dominant trigger sources for harvest workflows — scheduled fan-outs,
//! post-downtime backfills (#337), webhook storms (#344), and batch starts —
//! routinely admit *hundreds of distinct, legitimate starts in a single tick*.
//! When each of those workflows touches the same rate-limited downstream (a
//! third-party API capped at 100 req/min, a fragile internal service, a vendor
//! quota), the operator wants a front-door knob to **pace admissions to a
//! sustained rate and defer the excess** — dropping nothing, rejecting nothing.
//!
//! `ThrottlePolicy` lets a workflow author declare a *rate*, a *burst*, and an
//! optional *key expression* over the start input:
//!
//! ```rust
//! use autumn_harvest::throttle::ThrottlePolicy;
//!
//! let policy = ThrottlePolicy::from_rate_str("100/m", Some(20.0), Some("input.tenant_id"), None)
//!     .expect("valid rate");
//! assert!((policy.refill_per_sec - 100.0 / 60.0).abs() < 1e-9);
//! assert_eq!(policy.burst, 20.0);
//! ```
//!
//! # Semantics
//!
//! **Token bucket / GCRA:** admitted starts for a given throttle key never
//! exceed `rate` (+ `burst`) over any window. Each admission (immediate or a
//! later deferred-fire) reserves one token from the shared per-key bucket in
//! `harvest_rate_limit_buckets`, the same battle-tested machinery the activity
//! rate limiter (#332) uses.
//!
//! **Defer, don't drop:** when the bucket is empty, the start is **durably
//! deferred** — a row is written to `harvest_start_throttle` *before*
//! `WorkflowStarted` is appended — and a background scanner
//! ([`fire_due_throttled_starts`]) admits it later as tokens refill. Nothing is
//! dropped (contrast debounce, #499) and nothing is rejected (contrast
//! `RejectDuplicate`).
//!
//! **Contrast with debounce:** debounce *collapses* a key's burst into one run
//! (an upsert — one row per key). A throttle *preserves every start* (a plain
//! insert — **many rows per key**), drained FIFO by the scanner.
//!
//! # Scoping
//!
//! Throttle coordination is **shard-local** — the bucket and pending rows live
//! on the *producer's own shard* (consistent with per-key concurrency, #247, and
//! the activity limiter). On a single-shard deployment that is globally correct.
//! Across a multi-shard cluster a key's effective rate is
//! `per-shard-rate × shards`; a true global-across-shards ceiling is out of
//! scope (see `docs/sharding.md`).
//!
//! # Replay safety
//!
//! Throttling is a **pre-start admission gate** that runs *before*
//! `WorkflowStarted` is appended. It introduces **no new `WorkflowEvent`
//! variant** and has zero effect on replay determinism of started runs.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Prefix for the per-key token bucket stored in `harvest_rate_limit_buckets`,
/// namespaced so it can never collide with an activity rate-limit bucket key.
pub const THROTTLE_BUCKET_PREFIX: &str = "start-throttle";

/// Maximum number of pending-start rows fired per scanner tick, per shard.
pub const THROTTLE_FIRE_BATCH_SIZE: i64 = 100;

/// Maximum allowed byte length for a resolved throttle key.
///
/// Keys longer than this are rejected at admission with
/// [`crate::error::HarvestError::Config`] rather than persisted and failing a
/// Postgres index-size constraint. Embedders with long natural keys should hash
/// or truncate before use.
pub const THROTTLE_KEY_MAX_BYTES: usize = 2048;

/// Parsed representation of a rate string like `"100/m"`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateSpec {
    /// Bucket refill rate in tokens per second (e.g. `"100/m"` → `1.666…`).
    pub refill_per_sec: f64,
    /// The numerator of the rate — the number of starts per period
    /// (e.g. `"100/m"` → `100.0`). Used as the default burst when the author
    /// does not set one explicitly.
    pub per_period_count: f64,
}

/// Declarative start-throttle configuration attached to a
/// [`crate::info::WorkflowInfo`].
///
/// Declared via
/// `#[workflow(throttle(rate = "100/m", burst = 20, key = "input.tenant_id"))]`
/// or the [`crate::info::WorkflowInfo::with_throttle`] fluent builder method.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ThrottlePolicy {
    /// Bucket refill rate in tokens per second. Derived from the declared rate
    /// string (`"100/m"` → `100.0 / 60.0`).
    pub refill_per_sec: f64,
    /// Bucket burst capacity (max tokens). Defaults to the rate's per-period
    /// count when the author does not set it explicitly.
    pub burst: f64,
    /// Optional JSON field path (dot-notation) resolved against the workflow
    /// input to produce the throttle group key. The `"input."` prefix is
    /// stripped. `None` = a single global bucket for this workflow type.
    pub key_expr: Option<&'static str>,
    /// Optional deadline: a start deferred longer than this is **timed out**
    /// (dropped) by the scanner rather than run stale. `None` = deferred
    /// indefinitely until a token frees.
    pub schedule_to_start: Option<Duration>,
}

impl ThrottlePolicy {
    /// Build a policy from a rate string plus optional burst / key / deadline.
    ///
    /// `burst` defaults to the rate's per-period count (e.g. `"100/m"` → `100`)
    /// when `None`, so a single window's worth of starts can burst through.
    ///
    /// # Errors
    /// Returns `Err(message)` when the rate string is malformed, the rate is
    /// non-positive, or an explicit `burst` is non-positive.
    pub fn from_rate_str(
        rate: &str,
        burst: Option<f64>,
        key_expr: Option<&'static str>,
        schedule_to_start: Option<Duration>,
    ) -> Result<Self, String> {
        let spec = parse_rate(rate)?;
        let burst = match burst {
            Some(b) if b > 0.0 => b,
            Some(b) => return Err(format!("throttle burst must be > 0, got {b}")),
            None => spec.per_period_count,
        };
        Ok(Self {
            refill_per_sec: spec.refill_per_sec,
            burst,
            key_expr,
            schedule_to_start,
        })
    }
}

/// Parse a rate string of the form `"<count>/<unit>"` where `<unit>` is one of
/// `s` (per second), `m` (per minute), or `h` (per hour).
///
/// # Errors
/// Returns `Err(message)` when the string has no `/`, the count is not a
/// positive number, or the unit is unknown.
pub fn parse_rate(s: &str) -> Result<RateSpec, String> {
    let (count_str, unit_str) = s.split_once('/').ok_or_else(|| {
        format!("throttle rate '{s}' must be of the form '<count>/<unit>' (e.g. \"100/m\")")
    })?;

    let count: f64 = count_str
        .trim()
        .parse()
        .map_err(|_| format!("throttle rate count '{}' is not a number", count_str.trim()))?;
    // `parse::<f64>()` accepts "nan"/"inf" literals, so reject NaN explicitly
    // rather than `!(count > 0.0)` (NaN comparisons are always false, which
    // would make the negation silently pass NaN through as "valid").
    if count.is_nan() || count <= 0.0 {
        return Err(format!("throttle rate count must be > 0, got {count}"));
    }

    let divisor = match unit_str.trim() {
        "s" => 1.0,
        "m" => 60.0,
        "h" => 3600.0,
        other => {
            return Err(format!(
                "throttle rate unit '{other}' is not one of 's', 'm', 'h'"
            ));
        }
    };

    Ok(RateSpec {
        refill_per_sec: count / divisor,
        per_period_count: count,
    })
}

/// Resolve the throttle key for a given input, using the same dot-notation path
/// resolution as per-key concurrency (issue #247).
///
/// Returns `None` when the path is missing or resolves to JSON `null`, in which
/// case the start falls through to the normal (non-throttled) path.
#[must_use]
pub fn resolve_throttle_key(expr: &str, input: &serde_json::Value) -> Option<String> {
    crate::concurrency::resolve_concurrency_key(expr, input)
}

/// Build the per-key token-bucket key stored in `harvest_rate_limit_buckets`.
///
/// The resolved key is empty (`""`) for an unkeyed / global-per-workflow
/// throttle, giving one bucket per workflow type.
#[must_use]
pub fn bucket_key(workflow_name: &str, resolved_key: &str) -> String {
    format!("{THROTTLE_BUCKET_PREFIX}:{workflow_name}:{resolved_key}")
}

/// Compute the absolute stale-out deadline for a deferred start.
///
/// Returns `Some(deferred_at + schedule_to_start)` when a deadline is
/// configured, `None` for indefinite deferral. Overflowing/absurd durations are
/// clamped to a large-but-finite value so this can never panic.
#[must_use]
pub fn compute_expiry(
    deferred_at: DateTime<Utc>,
    schedule_to_start: Option<Duration>,
) -> Option<DateTime<Utc>> {
    let d = schedule_to_start?;
    // Clamp absurd/overflowing durations to a large-but-finite value and use
    // checked addition so an extreme deadline can never panic.
    let chrono_d =
        chrono::Duration::from_std(d).unwrap_or_else(|_| chrono::Duration::days(365 * 100));
    Some(
        deferred_at
            .checked_add_signed(chrono_d)
            .unwrap_or(deferred_at),
    )
}

// ---------------------------------------------------------------------------
// DB-gated types and functions
// ---------------------------------------------------------------------------

/// Parameters for [`reserve_or_defer`].
///
/// `start_options` reuses [`crate::debounce::DebounceStartOptions`] verbatim so
/// the persisted schema stays stable as `StartWorkflowParams` grows; it is only
/// consulted on the defer branch (the reserve branch proceeds with the caller's
/// own params).
#[cfg(feature = "db")]
pub struct AdmitThrottleParams<'a> {
    pub workflow_name: &'a str,
    /// Resolved throttle key (`""` for an unkeyed / global-per-workflow throttle).
    pub throttle_key: &'a str,
    pub workflow_id: &'a str,
    pub queue_name: &'a str,
    pub input: serde_json::Value,
    pub start_options: crate::debounce::DebounceStartOptions,
    /// Bucket refill rate in tokens/sec (from the effective policy).
    pub refill_per_sec: f64,
    /// Bucket burst capacity (from the effective policy).
    pub burst: f64,
    /// Stale-out deadline for a deferred start (from the effective policy).
    pub schedule_to_start: Option<Duration>,
    pub shard_id: i32,
}

/// Outcome of a deferred throttle admission.
#[cfg(feature = "db")]
#[derive(Debug, Clone)]
pub struct ThrottleDeferOutcome {
    pub workflow_id: String,
    pub throttle_key: String,
    pub deferred_at: DateTime<Utc>,
}

/// Result of [`reserve_or_defer`].
#[cfg(feature = "db")]
#[derive(Debug)]
pub enum ThrottleAdmission {
    /// A token was reserved: the caller should proceed with the normal start on
    /// the same connection, then [`crate::queue::refund_rate_limit_token`] the
    /// bucket if the start short-circuits (an id-reuse policy returned an
    /// existing run without creating a fresh one).
    Reserved {
        /// The bucket key the token was reserved from, for the refund call.
        bucket_key: String,
    },
    /// No token was available: a durable pending-start row was written. The
    /// caller must **not** start; over HTTP it returns `202 Accepted`.
    Deferred(ThrottleDeferOutcome),
}

/// A pending throttle record surfaced by the management API.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingThrottleRecord {
    pub id: uuid::Uuid,
    pub workflow_name: String,
    pub throttle_key: String,
    pub workflow_id: String,
    pub queue_name: String,
    pub deferred_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub shard_id: i32,
}

/// One `(workflow_name, throttle_key)` backlog group for the admin read.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ThrottleBacklogEntry {
    pub workflow_name: String,
    pub throttle_key: String,
    pub deferred_count: i64,
    pub oldest_deferred_at: DateTime<Utc>,
    pub shard_id: i32,
}

/// Ensure a token bucket exists for `key`, preserving any operator override.
///
/// Mirrors the activity limiter's `register_rate_limit_buckets`
/// (`INSERT … ON CONFLICT (key) DO NOTHING`, initial `tokens = burst`), so a
/// rate change across a deploy does not silently reset a live bucket.
#[cfg(feature = "db")]
async fn ensure_throttle_bucket(
    conn: &mut diesel_async::AsyncPgConnection,
    key: &str,
    refill_per_sec: f64,
    burst: f64,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query(
        "INSERT INTO harvest_rate_limit_buckets (key, refill_rate, burst, tokens, last_refilled_at) \
         VALUES ($1, $2, $3, $3, NOW()) \
         ON CONFLICT (key) DO NOTHING",
    )
    .bind::<diesel::sql_types::Text, _>(key)
    .bind::<diesel::sql_types::Double, _>(refill_per_sec)
    .bind::<diesel::sql_types::Double, _>(burst)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Whether any pending-start row already exists for a bucket key.
///
/// Used for the fast-path FIFO guard: if a backlog already exists, a new start
/// is appended (deferred) rather than allowed to jump the queue by consuming a
/// freshly-refilled token ahead of older waiters.
#[cfg(feature = "db")]
async fn pending_backlog_exists(
    conn: &mut diesel_async::AsyncPgConnection,
    bucket_key: &str,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Exists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let row: Exists = diesel::sql_query(
        "SELECT EXISTS (SELECT 1 FROM harvest_start_throttle WHERE bucket_key = $1) AS present",
    )
    .bind::<diesel::sql_types::Text, _>(bucket_key)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.present)
}

/// Insert one durable pending-start row (defer branch).
#[cfg(feature = "db")]
async fn insert_pending_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    params: &AdmitThrottleParams<'_>,
    bucket_key: &str,
    deferred_at: DateTime<Utc>,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    let expires_at = compute_expiry(deferred_at, params.schedule_to_start);
    let start_options_json = serde_json::to_value(&params.start_options)
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
    diesel::sql_query(
        "INSERT INTO harvest_start_throttle \
            (id, workflow_name, throttle_key, bucket_key, workflow_id, queue_name, \
             input, start_options, deferred_at, expires_at, shard_id, created_at) \
         VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())",
    )
    .bind::<diesel::sql_types::Text, _>(params.workflow_name)
    .bind::<diesel::sql_types::Text, _>(params.throttle_key)
    .bind::<diesel::sql_types::Text, _>(bucket_key)
    .bind::<diesel::sql_types::Text, _>(params.workflow_id)
    .bind::<diesel::sql_types::Text, _>(params.queue_name)
    .bind::<diesel::sql_types::Jsonb, _>(&params.input)
    .bind::<diesel::sql_types::Jsonb, _>(start_options_json)
    .bind::<diesel::sql_types::Timestamptz, _>(deferred_at)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(expires_at)
    .bind::<diesel::sql_types::Integer, _>(params.shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Reserve a token for a start, or durably defer it when the bucket is empty.
///
/// The admission primitive shared by every producer (HTTP start, batch, and the
/// scheduler/backfill fires). Behaviour:
///
/// 1. **FIFO fast-path guard:** if a pending backlog already exists for this
///    key, defer (append) so a new start cannot jump ahead of older waiters or
///    starve them.
/// 2. Otherwise ensure the bucket exists and attempt a single-token debit
///    ([`crate::queue::try_consume_rate_limit_token`]):
///    - success → [`ThrottleAdmission::Reserved`] (caller proceeds to start;
///      refunds on an id-reuse short-circuit);
///    - failure (empty bucket) → insert a pending row and return
///      [`ThrottleAdmission::Deferred`].
///
/// Shard-local: every operation runs on the passed connection's shard.
///
/// # Errors
/// Returns `HarvestError` if the key is over-long or a database call fails.
#[cfg(feature = "db")]
pub async fn reserve_or_defer(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitThrottleParams<'_>,
) -> crate::error::HarvestResult<ThrottleAdmission> {
    if params.throttle_key.len() > THROTTLE_KEY_MAX_BYTES {
        return Err(crate::error::HarvestError::Config(format!(
            "throttle key length {} bytes exceeds maximum {} bytes; \
             use a shorter key or hash the value before using it as a key",
            params.throttle_key.len(),
            THROTTLE_KEY_MAX_BYTES
        )));
    }

    let key = bucket_key(params.workflow_name, params.throttle_key);
    let now = Utc::now();

    // (1) FIFO fast-path guard: an existing backlog means we must append, not
    // jump the queue.
    if !pending_backlog_exists(conn, &key).await? {
        // (2) No backlog — ensure the bucket and try to reserve a token.
        ensure_throttle_bucket(conn, &key, params.refill_per_sec, params.burst).await?;
        if crate::queue::try_consume_rate_limit_token(conn, &key).await? {
            return Ok(ThrottleAdmission::Reserved { bucket_key: key });
        }
    }

    // Defer: durably persist the start before any WorkflowStarted event exists.
    insert_pending_throttle_row(conn, &params, &key, now).await?;
    Ok(ThrottleAdmission::Deferred(ThrottleDeferOutcome {
        workflow_id: params.workflow_id.to_string(),
        throttle_key: params.throttle_key.to_string(),
        deferred_at: now,
    }))
}

/// Internal row type returned by the throttle fire query.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct FireDueRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    throttle_key: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    bucket_key: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    input: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    start_options: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    expires_at: Option<DateTime<Utc>>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    shard_id: i32,
}

#[cfg(feature = "db")]
type FiredThrottle = (
    String, // workflow_name
    String, // queue_name
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(crate::types::ExecutionId, String)>,
    Vec<(String, String)>,
);

/// Delete a single pending throttle row by id.
#[cfg(feature = "db")]
async fn delete_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row_id: uuid::Uuid,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("DELETE FROM harvest_start_throttle WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(row_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Fire one already-claimed (row-locked) throttle row: reserve a token, start
/// the execution, and delete the record. Must run inside the caller's
/// transaction so the `FOR UPDATE` lock is held through the delete.
///
/// Returns:
/// - `Ok(Some(FiredThrottle))` when a run was started (caller spawns deferred
///   trigger-starts + records metrics after commit);
/// - `Ok(None)` when the row was dropped without starting — either it timed out
///   past its `schedule_to_start` deadline (issue #607 AC-c), the bucket had no
///   token yet (left for a later tick — **not** deleted), or the start was a
///   no-op under a reuse policy (deleted, token refunded).
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn fire_claimed_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueRow,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Option<FiredThrottle>> {
    // AC-c: a start deferred past its schedule_to_start deadline times out
    // rather than running stale.
    if let Some(expires_at) = row.expires_at
        && expires_at < now
    {
        delete_throttle_row(conn, row.id).await?;
        tracing::warn!(
            workflow_name = %row.workflow_name,
            throttle_key = %row.throttle_key,
            workflow_id = %row.workflow_id,
            "throttled start timed out past schedule_to_start; dropped",
        );
        return Ok(None);
    }

    // Pace: reserve a token. If none is available yet, leave the row for a later
    // scanner tick (do NOT delete).
    if !crate::queue::try_consume_rate_limit_token(conn, &row.bucket_key).await? {
        return Ok(None);
    }

    let opts: crate::debounce::DebounceStartOptions =
        serde_json::from_value(row.start_options).unwrap_or_default();

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
    let max_execution_timeout_ceiling = opts
        .max_execution_timeout_ceiling_secs
        .and_then(chrono::Duration::try_seconds);
    let priority = opts
        .priority
        .and_then(crate::types::Priority::from_i32)
        .unwrap_or_default();

    let workflow_name = row.workflow_name;
    let workflow_id = row.workflow_id;
    let queue_name = row.queue_name;
    let throttle_key = row.throttle_key;
    let bucket = row.bucket_key;
    let row_id = row.id;
    let owner = opts.owner;
    let runbook_url = opts.runbook_url;
    let severity = opts.severity;

    let params = crate::execution::StartWorkflowParams {
        workflow_name: &workflow_name,
        workflow_id: &workflow_id,
        exec_id,
        input: row.input,
        parent_id: None,
        queue_name: &queue_name,
        execution_timeout,
        memo: opts.memo,
        search_attrs: opts.search_attrs,
        reuse_policy,
        trace_context: opts.trace_context,
        max_execution_timeout_ceiling,
        concurrency_key: opts.concurrency_key,
        concurrency_limit: opts.concurrency_limit,
        priority,
        max_workflow_input_bytes: opts.max_workflow_input_bytes.unwrap_or(u64::MAX),
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: owner.as_deref(),
        runbook_url: runbook_url.as_deref(),
        severity: severity.as_deref(),
        context_headers: opts.context_headers,
        sla,
        // Carryover lineage (issue #488) is preserved across a throttle deferral:
        // the scheduler/backfill paths persist schedule_id/scheduled_for/origin
        // into the pending row's options so the eventual fire is attributed to
        // the schedule exactly as an immediate fire would be.
        schedule_id: opts.schedule_id,
        scheduled_for: opts.scheduled_for,
        workflow_attempt: 1,
        workflow_retry_policy: opts
            .workflow_retry_policy
            .and_then(|v| serde_json::from_value(v).ok()),
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: opts.max_workflow_attempts_ceiling,
        origin: opts.origin.as_deref(),
        completion_callbacks: opts.completion_callbacks,
    };

    // `in_outer_transaction = true`: runs inside the scanner's fire transaction,
    // so follow-ups are collected and spawned only after commit.
    match crate::execution::start_or_load_workflow_execution_collect(
        conn, params, true, false, None,
    )
    .await
    {
        Ok((started, deferred_starts, deferred_checks, cancel_metrics)) => {
            delete_throttle_row(conn, row_id).await?;
            // AC-a symmetry: a start that attached to an existing run (rather
            // than creating one) consumed no real admission — refund the token.
            if !started.created {
                crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            }
            tracing::info!(
                workflow_name = %workflow_name,
                throttle_key = %throttle_key,
                exec_id = %started.exec_id,
                queue = %queue_name,
                "throttled start fired",
            );
            Ok(Some((
                started.workflow_name,
                queue_name,
                deferred_starts,
                deferred_checks,
                cancel_metrics,
            )))
        }
        // The workflow_id is already taken under the reuse policy — drop the row
        // (so the scanner doesn't retry a doomed start forever) and refund the
        // reserved token (no run was admitted).
        Err(crate::error::HarvestError::AlreadyExists { .. }) => {
            delete_throttle_row(conn, row_id).await?;
            crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            tracing::warn!(
                workflow_name = %workflow_name,
                throttle_key = %throttle_key,
                workflow_id = %workflow_id,
                "throttled start skipped: workflow_id already exists under reuse policy",
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Scan and fire due throttle rows on a single shard connection.
#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Vec<FiredThrottle>> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    // Tolerate a missing table (mixed-schema fixtures / un-migrated deployment).
    #[derive(diesel::QueryableByName)]
    struct TableExists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let exists: TableExists =
        diesel::sql_query("SELECT to_regclass('harvest_start_throttle') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok(Vec::new());
    }

    // Claim + fire + delete the whole due batch in one transaction so each
    // `FOR UPDATE SKIP LOCKED` lock is held until its row is deleted (or left).
    let fired: Vec<FiredThrottle> = conn
        .transaction::<Vec<FiredThrottle>, crate::error::HarvestError, _>(|conn| {
            Box::pin(async move {
                let now = Utc::now();
                let due_sql = "
                    SELECT id, workflow_name, throttle_key, bucket_key, workflow_id,
                           queue_name, input, start_options, expires_at, shard_id
                    FROM harvest_start_throttle
                    ORDER BY deferred_at ASC
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                ";
                let due_rows: Vec<FireDueRow> = diesel::sql_query(due_sql)
                    .bind::<diesel::sql_types::BigInt, _>(THROTTLE_FIRE_BATCH_SIZE)
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                let mut results = Vec::with_capacity(due_rows.len());
                for row in due_rows {
                    if let Some(item) = fire_claimed_throttle_row(conn, row, now).await? {
                        results.push(item);
                    }
                }
                Ok(results)
            })
        })
        .await?;

    Ok(fired)
}

/// Fire all due throttled starts across every assigned shard.
///
/// Mirrors [`crate::debounce::fire_due_debounced_starts`]: per shard, claim +
/// fire + delete in one transaction, then spawn deferred trigger-starts after
/// the transaction commits. Called from [`crate::timeout::enforce_timeouts_once`]
/// on the existing timeout poll interval — no new background task.
///
/// # Errors
/// Returns `HarvestError` if a database query or any execution start fails.
#[cfg(feature = "db")]
pub async fn fire_due_throttled_starts(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    async fn spawn_fired(
        fired: Vec<FiredThrottle>,
        metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
        conn: &mut diesel_async::AsyncPgConnection,
    ) -> usize {
        let count = fired.len();
        for (workflow_name, queue_name, deferred_starts, deferred_checks, cancel_metrics) in fired {
            for start in deferred_starts {
                start.spawn();
            }
            for check in deferred_checks {
                let _ = crate::execution::check_and_report_unfinished_handlers(
                    conn,
                    check.0,
                    &check.1,
                    Some(metrics),
                )
                .await;
            }
            for (wf_name, q_name) in cancel_metrics {
                metrics.record_workflow_terminal(
                    &wf_name,
                    &q_name,
                    crate::telemetry::WorkflowStatus::Cancelled,
                );
            }
            let _ = (&workflow_name, &queue_name);
        }
        count
    }

    let mut fired_count = 0usize;
    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            for shard in shard_assignments {
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    continue;
                };
                let mut shard_conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "[throttle] failed to get connection to shard {shard:?}: {e:?}"
                        );
                        continue;
                    }
                };
                let fired = fire_due_on_conn(&mut shard_conn).await?;
                fired_count += spawn_fired(fired, metrics, &mut shard_conn).await;
            }
        }
        _ => {
            let fired = fire_due_on_conn(conn).await?;
            fired_count += spawn_fired(fired, metrics, conn).await;
        }
    }

    Ok(fired_count)
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

/// List all pending throttle records on this shard (management API).
///
/// # Errors
/// Returns `HarvestError` if the database query fails.
#[cfg(feature = "db")]
pub async fn list_pending_throttle(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Vec<PendingThrottleRecord>> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct ListRow {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        throttle_key: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        deferred_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        expires_at: Option<DateTime<Utc>>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        shard_id: i32,
    }
    let rows: Vec<ListRow> = diesel::sql_query(
        "SELECT id, workflow_name, throttle_key, workflow_id, queue_name,
                deferred_at, expires_at, shard_id
         FROM harvest_start_throttle
         ORDER BY deferred_at ASC",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(rows
        .into_iter()
        .map(|r| PendingThrottleRecord {
            id: r.id,
            workflow_name: r.workflow_name,
            throttle_key: r.throttle_key,
            workflow_id: r.workflow_id,
            queue_name: r.queue_name,
            deferred_at: r.deferred_at,
            expires_at: r.expires_at,
            shard_id: r.shard_id,
        })
        .collect())
}

/// Per-`(workflow_name, throttle_key)` backlog counts on this shard (admin read).
///
/// # Errors
/// Returns `HarvestError` if the database query fails.
#[cfg(feature = "db")]
pub async fn throttle_backlog_by_key(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<Vec<ThrottleBacklogEntry>> {
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct GroupRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        throttle_key: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        deferred_count: i64,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        oldest_deferred_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        shard_id: i32,
    }
    let rows: Vec<GroupRow> = diesel::sql_query(
        "SELECT workflow_name, throttle_key, shard_id,
                COUNT(*) AS deferred_count,
                MIN(deferred_at) AS oldest_deferred_at
         FROM harvest_start_throttle
         GROUP BY workflow_name, throttle_key, shard_id
         ORDER BY deferred_count DESC",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(rows
        .into_iter()
        .map(|r| ThrottleBacklogEntry {
            workflow_name: r.workflow_name,
            throttle_key: r.throttle_key,
            deferred_count: r.deferred_count,
            oldest_deferred_at: r.oldest_deferred_at,
            shard_id: r.shard_id,
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

    fn ts(h: u32, m: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 6, h, m, s).unwrap()
    }

    // ── parse_rate ───────────────────────────────────────────────────────────

    #[test]
    fn parse_rate_per_minute() {
        let spec = parse_rate("100/m").unwrap();
        assert!((spec.refill_per_sec - 100.0 / 60.0).abs() < 1e-9);
        assert!((spec.per_period_count - 100.0).abs() < 1e-9);
    }

    #[test]
    fn parse_rate_per_second() {
        let spec = parse_rate("5/s").unwrap();
        assert!((spec.refill_per_sec - 5.0).abs() < 1e-9);
        assert!((spec.per_period_count - 5.0).abs() < 1e-9);
    }

    #[test]
    fn parse_rate_per_hour() {
        let spec = parse_rate("3600/h").unwrap();
        assert!((spec.refill_per_sec - 1.0).abs() < 1e-9);
        assert!((spec.per_period_count - 3600.0).abs() < 1e-9);
    }

    #[test]
    fn parse_rate_accepts_whitespace() {
        let spec = parse_rate(" 60 / m ").unwrap();
        assert!((spec.refill_per_sec - 1.0).abs() < 1e-9);
    }

    #[test]
    fn parse_rate_rejects_missing_slash() {
        assert!(parse_rate("100").is_err());
    }

    #[test]
    fn parse_rate_rejects_bad_number() {
        assert!(parse_rate("abc/m").is_err());
    }

    #[test]
    fn parse_rate_rejects_unknown_unit() {
        assert!(parse_rate("100/x").is_err());
    }

    #[test]
    fn parse_rate_rejects_zero() {
        assert!(parse_rate("0/m").is_err());
    }

    #[test]
    fn parse_rate_rejects_negative() {
        assert!(parse_rate("-5/m").is_err());
    }

    // ── ThrottlePolicy::from_rate_str ────────────────────────────────────────

    #[test]
    fn from_rate_str_defaults_burst_to_per_period_count() {
        let p = ThrottlePolicy::from_rate_str("100/m", None, None, None).unwrap();
        assert!((p.burst - 100.0).abs() < 1e-9);
        assert!(p.key_expr.is_none());
        assert!(p.schedule_to_start.is_none());
    }

    #[test]
    fn from_rate_str_uses_explicit_burst() {
        let p = ThrottlePolicy::from_rate_str("100/m", Some(20.0), Some("input.tenant_id"), None)
            .unwrap();
        assert!((p.burst - 20.0).abs() < 1e-9);
        assert_eq!(p.key_expr, Some("input.tenant_id"));
    }

    #[test]
    fn from_rate_str_carries_schedule_to_start() {
        let p = ThrottlePolicy::from_rate_str("10/s", None, None, Some(Duration::from_secs(300)))
            .unwrap();
        assert_eq!(p.schedule_to_start, Some(Duration::from_secs(300)));
    }

    #[test]
    fn from_rate_str_rejects_bad_rate() {
        assert!(ThrottlePolicy::from_rate_str("nope", None, None, None).is_err());
    }

    #[test]
    fn from_rate_str_rejects_non_positive_burst() {
        assert!(ThrottlePolicy::from_rate_str("100/m", Some(0.0), None, None).is_err());
    }

    // ── resolve_throttle_key ─────────────────────────────────────────────────

    #[test]
    fn resolve_top_level() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_throttle_key("tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_input_prefix_stripped() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_throttle_key("input.tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_nested() {
        let input = serde_json::json!({ "event": { "tenant_id": "acme" } });
        assert_eq!(
            resolve_throttle_key("event.tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_missing_returns_none() {
        let input = serde_json::json!({ "other": "x" });
        assert_eq!(resolve_throttle_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_null_returns_none() {
        let input = serde_json::json!({ "tenant_id": null });
        assert_eq!(resolve_throttle_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_integer_coerced() {
        let input = serde_json::json!({ "tenant_id": 42 });
        assert_eq!(
            resolve_throttle_key("tenant_id", &input),
            Some("42".to_string())
        );
    }

    // ── bucket_key ───────────────────────────────────────────────────────────

    #[test]
    fn bucket_key_keyed() {
        assert_eq!(bucket_key("send", "acme"), "start-throttle:send:acme");
    }

    #[test]
    fn bucket_key_unkeyed() {
        assert_eq!(bucket_key("send", ""), "start-throttle:send:");
    }

    // ── compute_expiry ───────────────────────────────────────────────────────

    #[test]
    fn compute_expiry_none_when_no_deadline() {
        assert_eq!(compute_expiry(ts(10, 0, 0), None), None);
    }

    #[test]
    fn compute_expiry_adds_deadline() {
        let deferred = ts(10, 0, 0);
        let got = compute_expiry(deferred, Some(Duration::from_secs(300))).unwrap();
        assert_eq!(got, deferred + chrono::Duration::seconds(300));
    }

    #[test]
    fn compute_expiry_clamps_absurd_duration() {
        // A near-u64::MAX duration must not panic; it clamps to a finite value.
        let deferred = ts(10, 0, 0);
        let got = compute_expiry(deferred, Some(Duration::from_secs(u64::MAX)));
        assert!(got.is_some());
        assert!(got.unwrap() > deferred);
    }
}
