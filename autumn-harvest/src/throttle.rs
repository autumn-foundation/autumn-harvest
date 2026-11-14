//! Workflow-start throttle — pace admissions, defer the excess (issue #607).
//!
//! # Overview
//!
//! Scheduled fan-outs, post-downtime backfills (#337), webhook storms (#344)
//! and batch starts often admit *hundreds of distinct, legitimate starts in a
//! single tick*. These workflows can all touch the same rate-limited
//! downstream: a third-party API capped at 100 req/min, a fragile internal
//! service, or a vendor quota. The operator then wants a front-door knob to
//! **pace admissions to a sustained rate and defer the excess**. Nothing is
//! dropped and nothing is rejected.
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
//! exceed `rate` (+ `burst`) over any window. Each admission, immediate or a
//! later deferred fire, reserves one token from the shared per-key bucket in
//! `harvest_rate_limit_buckets`. The activity rate limiter (#332) uses the
//! same machinery.
//!
//! **Defer, do not drop:** when the bucket is empty, the start is **durably
//! deferred**. A row is written to `harvest_start_throttle` *before*
//! `WorkflowStarted` is appended. A background scanner
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
//!
//! # Known gaps — call-site duplication and ungated producers
//!
//! Four call sites invoke `reserve_or_defer` independently, with no central
//! choke point. Two are in `autumn-harvest-plugin`'s `api.rs`: the HTTP start
//! route and the batch-start handler. Two are in `scheduler.rs`: the
//! scheduler-tick loop and the buffered/backfill-fire loop. The admission-gate
//! feature (#377) has the same per-producer duplication, and this module does
//! not fix it. [`crate::admission_gate::producer_contract`] lists the full
//! producer contract of the gate.
//!
//! A consolidated design would put throttle admission into
//! `execution::start_or_load_workflow_execution_collect`. Every current *and
//! future* producer would then get it for free. That refactor touches code
//! that every producer depends on. So it is out of proportion for this
//! feature and is left as a follow-up. Until then, these producers of
//! workflow starts do **not** consult a workflow's [`ThrottlePolicy`]:
//!
//! * **`signal_with_start_workflow_execution` / `update_with_start_workflow_execution`**
//!   (`execution.rs`): signals or updates can feed an entity workflow. That
//!   makes the same start burst as an HTTP `POST .../start` caller, with no
//!   pacing.
//! * **Completion-trigger continuations** (`completion_trigger.rs`): a
//!   terminal-state trigger that starts a follow-up workflow does not consult a
//!   `ThrottlePolicy`. (It DOES honour the admission gate as of #618 — the
//!   throttle and the gate are independent controls.)
//! * **The outbox relay** (`autumn-harvest-plugin`'s `outbox.rs`): replays
//!   workflow-start events durably queued before a bucket was ever consulted.
//!
//! An author who uses `#[workflow(throttle(...))]` to bound a rate-limited
//! downstream should confirm that none of these paths starts that workflow.
//! Otherwise the admitted rate can exceed the declared rate.
//!
//! Separately, the scanner (`fire_due_on_conn`/`fire_claimed_throttle_row`/
//! `fire_due_throttled_starts`) is a near-total structural copy of the
//! `debounce.rs` scanner (`fire_due_on_conn`/`fire_claimed_debounce_row`/
//! `fire_due_debounced_starts`). There is no shared generic scanner. So a bug
//! fix to the shared shape must go in both places. That shape includes the
//! `to_regclass` missing-table tolerance, the `FOR UPDATE SKIP LOCKED`
//! claim-batch transaction and the spawn-after-commit pattern for deferred
//! trigger-starts.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Prefix for the per-key token bucket stored in `harvest_rate_limit_buckets`,
/// namespaced so it can never collide with an activity rate-limit bucket key.
pub const THROTTLE_BUCKET_PREFIX: &str = "start-throttle";

/// Maximum number of pending-start rows fired per scanner tick, per shard.
pub const THROTTLE_FIRE_BATCH_SIZE: i64 = 100;

/// Maximum number of rows drained from a single `bucket_key` in one scanner tick.
///
/// The cap bounds one hot or starving key to this share of the
/// [`THROTTLE_FIRE_BATCH_SIZE`] budget of a tick, however large its backlog is
/// (issue #607). So an unbounded single-key backlog never starves newer rows
/// under *other* keys.
pub const THROTTLE_FIRE_PER_KEY_CAP: i64 = 10;

/// Backoff applied to a throttle row's `deferred_at` when an admission gate
/// blocks its fire (issue #1053).
///
/// A gate is **queue-scoped**, so the SQL candidate pre-filter cannot see it.
/// The pre-filter only knows token levels and `expires_at`. Without a bump, a
/// gate-blocked row that has a token keeps its older `deferred_at`. It stays
/// at the front of the `ORDER BY deferred_at ASC` fire ordering, so every
/// tick selects it again. A queue-wide incident gate over many tenant keys can
/// block ≥[`THROTTLE_FIRE_BATCH_SIZE`] such rows together. They then
/// monopolize the fixed-size fire batch. They **starve newer un-gated throttle
/// keys on other queues indefinitely**. That reopens the cross-key fairness
/// pathology that the per-key cap and the token pre-filter close.
///
/// A re-defer bumps `deferred_at` forward. That rotates a gated row *behind*
/// every un-gated row in the fire ordering, because an un-gated row keeps its
/// real, earlier deferral time. Un-gated keys are then selected and fire
/// first. A gated row is attempted less often, with fewer block-metric
/// increments and log lines per tick. It is a modest fixed tuning value, not
/// a config knob.
///
/// The bump is safe against the `schedule_to_start` staleness drop (AC-c,
/// issue #607). The candidate SELECT finds expired rows through an
/// OR-`expires_at` clause that does not depend on `deferred_at`. There is no
/// `deferred_at <= NOW()` candidate floor. So a future `deferred_at` never
/// hides an expired row from its drop. The bump leaves `expires_at` unchanged,
/// so a gated row still times out at its original deadline.
// Gated like its only users, `redefer_throttle_row` and
// `fire_claimed_throttle_row`, which need `db`. Without the gate, the const is
// dead code under narrow feature sets. The CLI Lint job compiles
// `autumn-harvest` without `db`, and clippy 1.97 `-D warnings` rejects it.
#[cfg(feature = "db")]
const GATE_REDEFER_BACKOFF: Duration = Duration::from_secs(5);

/// Re-defer backoff for a row that an exhausted per-tenant quota blocks at
/// fire time (issue #946).
///
/// It has the value and rationale of [`GATE_REDEFER_BACKOFF`]. It is a
/// separate constant only so that each block cause has its own name. Nothing
/// depends on the two values staying equal.
#[cfg(feature = "db")]
const QUOTA_REDEFER_BACKOFF: Duration = Duration::from_secs(5);

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
    /// The final `burst` (explicit or defaulted) must be `>= 1.0` (issue #607).
    /// The token debit path (`queue::try_consume_rate_limit_token`) admits a
    /// start only when the refilled bucket reaches `>= 1.0`. Refill is capped
    /// at `burst`. So a bucket capacity below one token can never debit. An
    /// explicit `burst = 0.5` has that capacity, and so does a fractional
    /// per-unit rate like `"0.5/s"` with no explicit burst. Every start under
    /// that key then defers forever, or until `schedule_to_start` drops it. It
    /// is never merely paced.
    ///
    /// # Errors
    /// Returns `Err(message)` when the rate string is malformed or the rate is
    /// non-positive. It also fails when the final `burst`, explicit or
    /// defaulted from the per-period count of the rate, is below `1.0`.
    pub fn from_rate_str(
        rate: &str,
        burst: Option<f64>,
        key_expr: Option<&'static str>,
        schedule_to_start: Option<Duration>,
    ) -> Result<Self, String> {
        let spec = parse_rate(rate)?;
        let burst = match burst {
            Some(b) if b.is_finite() && b >= 1.0 => b,
            Some(b) if !b.is_finite() => {
                // `ensure_throttle_bucket` inserts an infinite (or NaN) burst
                // verbatim as the initial `tokens` of the bucket (issue #607).
                // Then `LEAST(burst, tokens + refill)` degrades to
                // `tokens + refill`. That allowance grows without bound and
                // never throttles, so it disables the throttle for that key.
                return Err(format!("throttle burst must be a finite number, got {b}"));
            }
            Some(b) => {
                return Err(format!(
                    "throttle burst must be >= 1.0 (a bucket capacity below \
                     one token can never successfully debit), got {b}"
                ));
            }
            None if spec.per_period_count >= 1.0 => spec.per_period_count,
            None => {
                return Err(format!(
                    "throttle rate '{rate}' has a per-period count of \
                     {count} (< 1.0); pass an explicit burst >= 1.0 to use a \
                     sub-unit rate",
                    count = spec.per_period_count
                ));
            }
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
    // `parse::<f64>()` accepts "nan", "inf" and "infinity", so reject both
    // through `!is_finite()`. A bare `count <= 0.0` test is false for NaN,
    // because every NaN comparison is false. So it alone would silently pass
    // NaN as valid. Infinity passes `> 0.0` and gives an unbounded refill rate
    // and burst. That defeats the throttle.
    if !count.is_finite() || count <= 0.0 {
        return Err(format!(
            "throttle rate count must be a finite number > 0, got {count}"
        ));
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
/// `start_options` reuses [`crate::debounce::DebounceStartOptions`] verbatim,
/// so the persisted schema stays stable as `StartWorkflowParams` grows. Only
/// the defer branch reads it. The reserve branch uses the params of the caller.
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
    /// `true` when this call durably inserted a brand-new pending row.
    ///
    /// `false` when the idempotency shortcut of `reserve_or_defer` (step 1)
    /// found an *already-existing* pending row for this `workflow_id`. A
    /// client retry does that. So does an operator who repeats a
    /// `schedule_backfill` window while the deferred slots of an earlier call
    /// are still pending. A caller that counts new admissions, such as budget
    /// spend or dispatched counters, must gate on `fresh` (issue #607). If it
    /// counts every `Deferred`, a repeated call double-counts. It also
    /// double-spends budget for a no-op idempotent attach.
    pub fresh: bool,
}

/// Result of [`reserve_or_defer`].
#[cfg(feature = "db")]
#[derive(Debug)]
pub enum ThrottleAdmission {
    /// A token was reserved: the caller should proceed with the normal start.
    ///
    /// The start uses the same connection. If the start short-circuits, the
    /// caller passes the bucket to [`crate::queue::refund_rate_limit_token`].
    /// An id-reuse policy short-circuits when it returns an existing run
    /// without creating a fresh one.
    Reserved {
        /// The bucket key the token was reserved from, for the refund call.
        bucket_key: String,
    },
    /// No token was available: a durable pending-start row was written. The
    /// caller must **not** start; over HTTP it returns `202 Accepted`.
    Deferred(ThrottleDeferOutcome),
    /// An active execution already exists for this `(workflow_name,
    /// workflow_id)`. Its reuse policy makes admission a no-op
    /// (`AllowDuplicate`, `RejectDuplicate`). Or the policy resolves against a
    /// non-terminal prior (`AllowDuplicateFailedOnly` when the existing row
    /// is not FAILED/CANCELLED). No token was reserved and no pending row was
    /// written. The caller must proceed straight to
    /// `start_or_load_workflow_execution*`, which resolves the reuse policy
    /// itself. It returns the existing run, or rejects it under
    /// `RejectDuplicate`.
    Bypassed,
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
///
/// It also carries the resolved state of the shared token bucket of that
/// throttle (issue #945). That state is the declared baseline, any live TTL'd
/// operator override, and the resulting effective rate-limit parameters.
///
/// The bucket-derived fields are `None`/`false` only when a backlog row has no
/// matching `harvest_rate_limit_buckets` row. That case is practically
/// unreachable: [`reserve_or_defer`] always creates the bucket before it
/// writes the backlog row. So this is a defensive fallback, not an expected
/// shape.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ThrottleBacklogEntry {
    pub workflow_name: String,
    pub throttle_key: String,
    pub deferred_count: i64,
    pub oldest_deferred_at: DateTime<Utc>,
    pub shard_id: i32,
    /// The declared baseline refill rate (`#[workflow(throttle(rate = ...))]`).
    pub declared_refill_rate: Option<f64>,
    /// The declared baseline burst.
    pub declared_burst: Option<f64>,
    /// A live or expired TTL'd operator override refill rate, if one has
    /// ever been set (issue #945).
    pub override_refill_rate: Option<f64>,
    /// A live or expired TTL'd operator override burst.
    pub override_burst: Option<f64>,
    /// Whether the override is currently in effect (`expires_at` set and
    /// strictly in the future).
    pub override_active: bool,
    /// The refill rate actually enforced right now: the override's, when
    /// `override_active`, else `declared_refill_rate`.
    pub effective_refill_rate: Option<f64>,
    /// The burst actually enforced right now: the override's, when
    /// `override_active`, else `declared_burst`.
    pub effective_burst: Option<f64>,
    /// When the override expires (or expired); `None` when no override has
    /// ever been set.
    pub expires_at: Option<DateTime<Utc>>,
}

/// Resolves the effective-rate-limit view for one throttle backlog row's
/// `LEFT JOIN`ed bucket columns (issue #945) -- the pure mapping
/// [`throttle_backlog_by_key`] applies to each grouped row.
///
/// `(declared_refill_rate, declared_burst)` are `None` only on a `LEFT JOIN`
/// miss: no `harvest_rate_limit_buckets` row matches the `bucket_key` of the
/// backlog row. [`reserve_or_defer`] always creates the bucket first, so the
/// miss is practically unreachable. The function handles it without a panic.
#[cfg(feature = "db")]
fn resolve_backlog_bucket_state(
    declared_refill_rate: Option<f64>,
    declared_burst: Option<f64>,
    override_refill_rate: Option<f64>,
    override_burst: Option<f64>,
    override_expires_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> (bool, Option<f64>, Option<f64>) {
    match (declared_refill_rate, declared_burst) {
        (Some(refill_rate), Some(burst)) => {
            let effective = crate::queue::resolve_effective_rate_limit(
                refill_rate,
                burst,
                override_refill_rate,
                override_burst,
                override_expires_at,
                now,
            );
            (
                effective.override_active,
                Some(effective.refill_rate),
                Some(effective.burst),
            )
        }
        _ => (false, None, None),
    }
}

/// Ensure a token bucket exists for `key`, preserving any operator override.
///
/// Delegates to [`crate::queue::ensure_rate_limit_bucket`] and does not
/// re-issue the same `INSERT`. The two paths write the *same* table with the
/// same "never reset a live bucket" contract. Since issue #1127, that
/// statement also carries the stale-row touch that interlocks a registration
/// against the idle-bucket GC. A second copy here would silently miss that
/// interlock. A deferred start whose bucket the GC collects mid-flight can
/// then never debit a token again.
#[cfg(feature = "db")]
async fn ensure_throttle_bucket(
    conn: &mut diesel_async::AsyncPgConnection,
    key: &str,
    refill_per_sec: f64,
    burst: f64,
) -> crate::error::HarvestResult<()> {
    crate::queue::ensure_rate_limit_bucket(conn, key, refill_per_sec, burst).await
}

/// Whether any pending-start row already exists for a bucket key.
///
/// The fast-path FIFO guard uses it. When a backlog exists, a new start is
/// deferred behind it. It does not jump the queue by taking a freshly-refilled
/// token ahead of older waiters.
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

/// Look up an already-pending row for this exact `(workflow_name, workflow_id)`,
/// regardless of which bucket/key it was filed under.
///
/// `workflow_id` is the natural idempotency boundary that the rest of the
/// engine keys on, through reuse policies and uniqueness indexes. With it, a
/// retried admission for a durably deferred start lands on the *same* pending
/// row. Without it, the retry silently queues a second, independent row. A
/// later `reject_duplicate` fire would drop that row with no signal back to
/// the retried caller.
///
/// A row already past its `schedule_to_start` deadline is never returned,
/// even if the scanner has not dropped it yet (issue #607). Otherwise a retry
/// would land on a row that the scanner is about to time out and delete
/// without ever starting a workflow. The retry would then report `Deferred`
/// for a start that never runs. Ignoring the row here lets the retry proceed
/// to a genuinely fresh admission instead.
#[cfg(feature = "db")]
async fn existing_pending_throttle_for_workflow_id(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> crate::error::HarvestResult<Option<ThrottleDeferOutcome>> {
    use diesel::OptionalExtension;
    use diesel_async::RunQueryDsl;
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::Text)]
        throttle_key: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        deferred_at: DateTime<Utc>,
    }
    let row: Option<Row> = diesel::sql_query(
        "SELECT throttle_key, deferred_at FROM harvest_start_throttle \
         WHERE workflow_name = $1 AND workflow_id = $2 \
           AND (expires_at IS NULL OR expires_at >= NOW()) \
         ORDER BY deferred_at ASC LIMIT 1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .bind::<diesel::sql_types::Text, _>(workflow_id)
    .get_result(conn)
    .await
    .optional()
    .map_err(crate::error::database_error)?;
    Ok(row.map(|r| ThrottleDeferOutcome {
        workflow_id: workflow_id.to_string(),
        throttle_key: r.throttle_key,
        deferred_at: r.deferred_at,
        fresh: false,
    }))
}

/// Insert one durable pending-start row (defer branch).
#[cfg(feature = "db")]
async fn insert_pending_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    params: &AdmitThrottleParams<'_>,
    bucket_key: &str,
    deferred_at: DateTime<Utc>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    let expires_at = compute_expiry(deferred_at, params.schedule_to_start);
    // The row stores the input in the column form (issue #2043).
    let stored_input = codecs.encode_column(&params.input)?;
    let start_options_json = serde_json::to_value(&params.start_options)
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));
    // The headers take the column form too (issue #2043).
    let start_options_json =
        crate::debounce::encode_stored_start_options(codecs, start_options_json)?;
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
    .bind::<diesel::sql_types::Jsonb, _>(&stored_input)
    .bind::<diesel::sql_types::Jsonb, _>(start_options_json)
    .bind::<diesel::sql_types::Timestamptz, _>(deferred_at)
    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>, _>(expires_at)
    .bind::<diesel::sql_types::Integer, _>(params.shard_id)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Whether an active execution for `(workflow_name, workflow_id)` already
/// makes a throttle admission a no-op or an immediate reject under `reuse_policy`.
///
/// - `AllowDuplicate` and `RejectDuplicate` bypass against any existing state.
/// - `AllowDuplicateFailedOnly` bypasses unless the existing row is
///   FAILED/CANCELLED, which makes a genuine fresh start.
/// - `TerminateIfRunning` never bypasses. It always starts fresh: cancel and
///   replace.
///
/// [`reserve_or_defer`] uses it. So does a caller that decides whether to
/// apply a *different* admission-time check, such as the workflow input
/// byte-cap. Both share one implementation of the lookup and of the
/// reuse-policy match.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn resolve_bypass(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    reuse_policy: crate::types::WorkflowIdReusePolicy,
) -> crate::error::HarvestResult<bool> {
    if matches!(
        reuse_policy,
        crate::types::WorkflowIdReusePolicy::TerminateIfRunning
    ) {
        return Ok(false);
    }
    let Some(existing) =
        crate::execution::try_load_by_key(conn, workflow_name, workflow_id).await?
    else {
        return Ok(false);
    };
    Ok(matches!(
        reuse_policy,
        crate::types::WorkflowIdReusePolicy::AllowDuplicate
            | crate::types::WorkflowIdReusePolicy::RejectDuplicate
    ) || (reuse_policy == crate::types::WorkflowIdReusePolicy::AllowDuplicateFailedOnly
        && !matches!(existing.state.as_str(), "FAILED" | "CANCELLED")))
}

/// Whether a pending throttle row already exists for this exact
/// `(workflow_name, workflow_id)`, regardless of which bucket/key it was filed under.
///
/// A thin boolean wrapper over [`existing_pending_throttle_for_workflow_id`].
/// It serves callers that need only the yes/no answer, not the full
/// [`ThrottleDeferOutcome`]. An example is the decision to apply the workflow
/// input byte-cap.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn has_pending_throttle_for_workflow_id(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
) -> crate::error::HarvestResult<bool> {
    Ok(
        existing_pending_throttle_for_workflow_id(conn, workflow_name, workflow_id)
            .await?
            .is_some(),
    )
}

/// Whether the fresh-deferral workflow-input byte-cap check should be
/// *skipped* for this `(workflow_name, workflow_id)` under `reuse_policy_str`.
///
/// `reuse_policy_str` is the same wire-format string `AdmitThrottleParams`/
/// `DebounceStartOptions` use, parsed via [`crate::debounce::parse_reuse_policy`],
/// defaulting to `AllowDuplicate` when unset or unrecognized — identical to
/// [`reserve_or_defer`]'s own parsing.
///
/// A caller can apply a byte-cap check *before* it calls [`reserve_or_defer`]
/// (issue #607). It must not reject a retry that [`reserve_or_defer`] resolves
/// through [`ThrottleAdmission::Bypassed`], where an active execution already
/// satisfies the reuse policy. It must not reject an idempotent attach to an
/// already-pending row either. Neither case inserts a fresh row, so the cap
/// does not apply. This function composes [`resolve_bypass`] and
/// [`has_pending_throttle_for_workflow_id`]. [`reserve_or_defer`] uses the
/// same two primitives for its steps (0) and (1). So each question has
/// exactly one implementation.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn skip_size_check(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    reuse_policy_str: Option<&str>,
) -> crate::error::HarvestResult<bool> {
    let reuse_policy = reuse_policy_str
        .and_then(crate::debounce::parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);
    if resolve_bypass(conn, workflow_name, workflow_id, reuse_policy).await? {
        return Ok(true);
    }
    has_pending_throttle_for_workflow_id(conn, workflow_name, workflow_id).await
}

/// Reserve a token for a start, or durably defer it when the bucket is empty.
///
/// The admission primitive shared by every producer (HTTP start, batch, and the
/// scheduler/backfill fires). Behaviour:
///
/// 0. **Active-execution bypass:** unless the caller's reuse policy is
///    `TerminateIfRunning` (which always starts fresh), look up whether an
///    active execution already exists for `(workflow_name, workflow_id)`. The
///    reuse policy can make this admission a no-op or an immediate reject.
///    `AllowDuplicate`/`RejectDuplicate` do so against any existing state.
///    `AllowDuplicateFailedOnly` does so against a non-terminal prior. Then
///    skip throttle admission and return [`ThrottleAdmission::Bypassed`]. No
///    token is reserved and no pending row is written. The caller proceeds
///    straight to the normal start, which resolves the reuse policy itself.
/// 1. **Idempotency, checked next:** a pending row can already exist for this
///    exact `workflow_id`. That is a retried admission for a durably queued
///    start, possibly under a *different* throttle key. Return the outcome of
///    *that* row. Do not reserve a fresh token under the new key and leave the
///    old row orphaned.
/// 2. **Bucket ensure:** ensure the bucket exists before either outcome below.
///    Every later path persists a dependent on it (issue #1127).
/// 3. **FIFO fast-path guard:** if a pending backlog already exists for this
///    key, defer (append). A new start then cannot jump ahead of older
///    waiters or starve them. Otherwise attempt a single-token debit
///    ([`crate::queue::try_consume_rate_limit_token`]):
///    - success → [`ThrottleAdmission::Reserved`] (caller proceeds to start;
///      refunds on an id-reuse short-circuit);
///    - failure (empty bucket) → insert a fresh pending row and return
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
    reserve_or_defer_with_codecs(conn, params, &crate::store::DEFAULT_PAYLOAD_CODECS).await
}

/// [`reserve_or_defer`], storing a deferred input through `codecs`
/// (issue #2043).
///
/// The row stores the input in the column form. The fire path decodes it
/// before the start.
///
/// # Errors
/// As [`reserve_or_defer`].
#[cfg(feature = "db")]
pub async fn reserve_or_defer_with_codecs(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitThrottleParams<'_>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<ThrottleAdmission> {
    admit(conn, params, false, codecs).await
}

/// [`reserve_or_defer`] for an HTTP start, which load shedding governs
/// (issue #1794).
///
/// The steps are the same. A start that would write a fresh pending row is
/// shed instead while its queue sheds. The check runs after every await of the
/// admission and right before the write. A queue that trips during the
/// admission therefore cannot defer a start past every shed check. A deferred
/// row is exempt when it fires, so this is the last check it meets.
///
/// A bypass, an attach to a pending row and a reserved token are not shed
/// here. A bypass and a reserved token continue to the start primitive, which
/// sheds a fresh create itself.
///
/// Scheduler and backfill fires keep [`reserve_or_defer_with_codecs`]. They
/// cannot act on `Retry-After`, so load shedding exempts them.
///
/// # Errors
/// As [`reserve_or_defer`], plus [`crate::error::HarvestError::LoadShed`]
/// when the queue sheds a fresh deferral.
#[cfg(feature = "db")]
pub async fn reserve_or_defer_or_shed(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitThrottleParams<'_>,
) -> crate::error::HarvestResult<ThrottleAdmission> {
    reserve_or_defer_or_shed_with_codecs(conn, params, &crate::store::DEFAULT_PAYLOAD_CODECS).await
}

/// [`reserve_or_defer_or_shed`], storing a deferred input through `codecs`
/// (issue #2043).
///
/// # Errors
/// As [`reserve_or_defer_or_shed`].
#[cfg(feature = "db")]
pub async fn reserve_or_defer_or_shed_with_codecs(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitThrottleParams<'_>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<ThrottleAdmission> {
    admit(conn, params, true, codecs).await
}

/// The shared body of [`reserve_or_defer`] and [`reserve_or_defer_or_shed`].
#[cfg(feature = "db")]
async fn admit(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitThrottleParams<'_>,
    shed_fresh_deferral: bool,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<ThrottleAdmission> {
    // Reject an empty id before any lookup, reservation, or persisted row
    // (issue #1353). A reserved token or deferred row with no id could only
    // be discarded on fire, not started.
    if params.workflow_id.is_empty() {
        return Err(crate::error::HarvestError::EmptyWorkflowId);
    }

    // (0) Bypass entirely when an active execution already makes this
    // admission a no-op or an immediate reject under the caller's reuse
    // policy. TerminateIfRunning always starts fresh (cancel + replace). So
    // it is excluded from the lookup and always proceeds to normal pacing.
    let reuse_policy = params
        .start_options
        .reuse_policy
        .as_deref()
        .and_then(crate::debounce::parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);
    if resolve_bypass(conn, params.workflow_name, params.workflow_id, reuse_policy).await? {
        return Ok(ThrottleAdmission::Bypassed);
    }

    // (1) Idempotency: a retry of a durably deferred start must land on the
    // *same* pending row. It must not queue a second row for the same
    // `workflow_id`. A client-side timeout that never saw the first 202 can
    // cause such a retry. This check runs *before* a fresh token reservation.
    // Otherwise a retry that resolves to a *different* throttle key with a
    // free token could win Reserved. The original pending row under the old
    // key would then sit orphaned.
    if let Some(existing) =
        existing_pending_throttle_for_workflow_id(conn, params.workflow_name, params.workflow_id)
            .await?
    {
        return Ok(ThrottleAdmission::Deferred(existing));
    }

    // Shortcuts 0 and 1 resolve only from `workflow_id` and never persist
    // anything keyed by `throttle_key`. So neither may depend on the key
    // length (issue #607). An input that resolves to an over-long key must
    // still attach to the existing execution or pending row, not fail. Only a
    // fresh admission below persists a bucket or row keyed by `throttle_key`.
    // So the length check is here, not before steps 0 and 1.
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

    // (2) Ensure the bucket BEFORE either outcome below, backlog or not.
    //
    // It has to be before the FIFO guard, not inside its no-backlog branch
    // (issue #1127). Every path from here persists a dependent on this bucket:
    // a reserved token, or a pending row that the scanner later debits. The
    // idle-bucket GC's anti-join can only see dependents that were already
    // COMMITTED when it took its snapshot. On the append path the old code did
    // not touch the bucket at all. Suppose the scanner drops an observed
    // backlog row between the check and this insert. (A `schedule_to_start`
    // stale-out debits no token.) The sweep then sees an idle, full,
    // dependent-free bucket. The pending row that this transaction commits
    // then references a bucket that no longer exists.
    // `fire_claimed_throttle_row` fails closed on a missing bucket, so that
    // start would sit deferred forever.
    //
    // Ensuring here closes it the same way the enqueue path does. The
    // registration touch locks any GC-eligible row for the rest of this
    // transaction, so the sweep skips it. An ensure that lost the race
    // re-inserts the bucket. It is also a robustness win in its own right. The
    // append path previously appended to a backlog whose bucket might not
    // exist at all, and nothing would ever have created it.
    ensure_throttle_bucket(conn, &key, params.refill_per_sec, params.burst).await?;

    // (3) FIFO fast-path guard: with an existing backlog, the start appends
    // and does not jump the queue.
    if !pending_backlog_exists(conn, &key).await?
        && crate::queue::try_consume_rate_limit_token(conn, &key).await?
    {
        return Ok(ThrottleAdmission::Reserved { bucket_key: key });
    }

    // Load shedding (issue #1794): no await follows this check before the
    // write, so the decision is the freshest the admission can take.
    if shed_fresh_deferral
        && let Some(decision) =
            crate::admission_gate::global_admission_gate_cache().and_then(|cache| {
                cache
                    .load_shedder()
                    .check(params.queue_name, std::time::Instant::now())
            })
    {
        return Err(crate::error::HarvestError::LoadShed {
            queue: decision.queue,
            oldest_pending_age_secs: decision.oldest_pending_age_secs,
            retry_after_secs: decision.retry_after_secs,
        });
    }

    // Defer: durably persist the start before any WorkflowStarted event exists.
    insert_pending_throttle_row(conn, &params, &key, now, codecs).await?;
    Ok(ThrottleAdmission::Deferred(ThrottleDeferOutcome {
        workflow_id: params.workflow_id.to_string(),
        throttle_key: params.throttle_key.to_string(),
        deferred_at: now,
        fresh: true,
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
    /// When the row was deferred. A backoff moves it into the future (issue
    /// #2043).
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    deferred_at: DateTime<Utc>,
}

/// How far a row that does not decode moves its `deferred_at` (issue #2043).
///
/// The claim has no `deferred_at` floor, so the move reorders the row behind
/// its neighbours and does not hide it. The row is a candidate on every tick.
/// The window limits the work instead: a row inside it is not written and not
/// warned about again. The row keeps its `schedule_to_start` deadline.
#[cfg(feature = "db")]
const UNDECODABLE_REDEFER_BACKOFF: Duration = Duration::from_secs(30);

/// Decode the stored input of each claimed row (issue #2043).
///
/// The row stores `input` in the column form. The quota ordering and the
/// start both need the plain input. A row that does not decode logs a warning
/// with no payload text, once per backoff window. A row past its
/// `schedule_to_start` deadline is dropped, as `fire_claimed_throttle_row`
/// drops it, because that needs no payload. Any other row moves back and fires
/// after an operator restores the key. A row still inside its window is left as
/// it is. The row thus cannot sit at the head of its key, and it cannot abort
/// the claim of its neighbours.
///
/// # Errors
///
/// Database failures while the row moves back or is dropped.
#[cfg(feature = "db")]
async fn decode_due_rows(
    conn: &mut diesel_async::AsyncPgConnection,
    rows: Vec<FireDueRow>,
    codecs: &crate::payload_codec::PayloadCodecs,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Vec<FireDueRow>> {
    let mut decodable = Vec::with_capacity(rows.len());
    for mut row in rows {
        match codecs.decode_column(&row.input).and_then(|plain| {
            let options =
                crate::debounce::decode_stored_start_options(codecs, row.start_options.clone())?;
            Ok((plain, options))
        }) {
            Ok((plain, options)) => {
                row.input = plain;
                row.start_options = options;
                decodable.push(row);
            }
            Err(error) => {
                let expired = row.expires_at.is_some_and(|at| at < now);
                if !expired && row.deferred_at > now {
                    // Inside the window of an earlier move. The row is already
                    // behind its neighbours, so a write would add no order.
                    tracing::debug!(
                        throttle_id = %row.id,
                        "throttle input still cannot be decoded; the row stays in its backoff window"
                    );
                    continue;
                }
                tracing::warn!(
                    throttle_id = %row.id,
                    expired,
                    error_kind = crate::codec_rotation::sweep_error_kind(&error),
                    "throttle input or headers could not be decoded"
                );
                if expired {
                    delete_throttle_row(conn, row.id).await?;
                } else {
                    redefer_throttle_row(conn, row.id, now + UNDECODABLE_REDEFER_BACKOFF).await?;
                }
            }
        }
    }
    Ok(decodable)
}

#[cfg(feature = "db")]
impl crate::quota_lock_order::QuotaLockRow for FireDueRow {
    fn workflow_name(&self) -> &str {
        &self.workflow_name
    }
    fn quota_input(&self) -> &serde_json::Value {
        &self.input
    }
}

#[cfg(feature = "db")]
type FiredThrottle = (
    String, // workflow_name
    String, // queue_name
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(crate::types::ExecutionId, String)>,
    Vec<crate::execution::StartCancelledRun>,
    // Whether this fire counts toward `harvest.schedule.runs` (issue #607).
    // It is true only when the persisted `origin == "scheduled"` and the
    // start created a fresh execution. Only the scheduler-tick and
    // buffered-run throttle branches set that origin. The HTTP manual-backfill
    // endpoint never sets it, although it also persists a `schedule_id`. The
    // gate mirrors the immediate path of those callers
    // (`if outcome.created() { metrics.record_schedule_run(...) }`). So a
    // throttled scheduled fire counts exactly once, on the same terms as an
    // unthrottled one. HTTP-start, batch and backfill fires never count here.
    // They carry no `schedule_id` or a different `origin`.
    bool,
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

/// Push a gate-blocked throttle row's `deferred_at` forward by
/// [`GATE_REDEFER_BACKOFF`] (issue #1053).
///
/// The row then yields the oldest-candidate fire slot to un-gated keys and is
/// attempted less often.
///
/// `expires_at` is deliberately left untouched: the row still times out at its
/// original `schedule_to_start` deadline (AC-c). Runs inside the caller's fire
/// transaction so the row-level `FOR UPDATE` lock is held through the update.
#[cfg(feature = "db")]
async fn redefer_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row_id: uuid::Uuid,
    new_deferred_at: DateTime<Utc>,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("UPDATE harvest_start_throttle SET deferred_at = $2 WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(row_id)
        .bind::<diesel::sql_types::Timestamptz, _>(new_deferred_at)
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
/// - `Ok(None)` when the row was dropped or held without starting, in these
///   cases:
///   - It timed out past its `schedule_to_start` deadline (issue #607 AC-c).
///   - The bucket had no token yet. The row stays for a later tick and is
///     **not** deleted.
///   - The start was a no-op under a reuse policy. The row is deleted and the
///     token is refunded.
///   - The admission gate was closed on the real queue of the workflow at
///     fire time (issue #1053). The row stays for a later tick and is **not**
///     deleted. The token is refunded.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn fire_claimed_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueRow,
    now: DateTime<Utc>,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    codecs: &crate::payload_codec::PayloadCodecs,
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

    // A deserialize failure must never crash the scanner. A schema mismatch
    // across a rolling deploy or a corrupted row can cause one. But a silent
    // fallback to defaults drops schedule_id, scheduled_for and origin of the
    // row with no signal. That breaks #488 carryover and #534 run-history
    // attribution, and it loses every other persisted option. So log the loss.
    let opts: crate::debounce::DebounceStartOptions = serde_json::from_value(row.start_options)
        .unwrap_or_else(|e| {
            tracing::error!(
                workflow_name = %row.workflow_name,
                throttle_key = %row.throttle_key,
                workflow_id = %row.workflow_id,
                error = %e,
                "failed to deserialize persisted throttle start_options; falling back to \
                 defaults (schedule attribution, memo, retry policy, and other options for \
                 this fire will be lost)",
            );
            crate::debounce::DebounceStartOptions::default()
        });

    let shard = crate::types::ShardId::new(row.shard_id);
    let exec_id = crate::types::ExecutionId::new_for_shard(shard);

    let crate::debounce::DeferredAdmissionFields {
        reuse_policy,
        execution_timeout,
        sla,
        max_execution_timeout_ceiling,
        chain_execution_timeout,
        max_workflow_chain_timeout_ceiling,
        priority,
    } = crate::debounce::decode_deferred_admission_fields(&opts);

    let workflow_name = row.workflow_name;
    let workflow_id = row.workflow_id;
    let queue_name = row.queue_name;
    let throttle_key = row.throttle_key;
    let bucket = row.bucket_key;
    let row_id = row.id;
    let owner = opts.owner;
    let runbook_url = opts.runbook_url;
    let severity = opts.severity;
    // Restore the provenance captured at admission (issue #740). A pre-#740 row
    // has no captured source, so derive a default from the persisted dispatch
    // origin. Scheduled and backfill throttled fires stay attributed to their
    // schedule. Everything else defaults to `Api`.
    let start_source = opts.start_source.as_deref().map_or_else(
        || match opts.origin.as_deref() {
            Some(o) if o == crate::execution::ORIGIN_SCHEDULED => {
                crate::types::StartSource::Schedule
            }
            Some(o) if o == crate::execution::ORIGIN_BACKFILL => {
                crate::types::StartSource::Backfill
            }
            _ => crate::types::StartSource::Api,
        },
        crate::types::StartSource::from_str,
    );
    let start_source_ref = opts.start_source_ref;
    let started_by = opts.started_by;
    // Gate on `origin == "scheduled"`, not on `schedule_id.is_some()` (issue
    // #607). The HTTP manual-backfill endpoint also persists a `schedule_id`
    // for carryover lineage, but it uses `origin = "backfill"`. Its own
    // immediate path never calls `record_schedule_run`. If a throttled
    // backfill fire counted here, throttling alone would inflate the metric.
    let is_scheduled_fire = opts.origin.as_deref() == Some(crate::execution::ORIGIN_SCHEDULED);

    // The stored JSON can mark a schedule policy. Keep that source (issue #2409).
    let (retry_policy, retry_source) =
        crate::workflow_retry::decode_optional(opts.workflow_retry_policy.as_ref());
    let params = crate::execution::StartWorkflowParams {
        execution_timeout,
        memo: opts.memo,
        search_attrs: opts.search_attrs,
        reuse_policy,
        trace_context: opts.trace_context,
        max_execution_timeout_ceiling,
        chain_execution_timeout,
        max_workflow_chain_timeout_ceiling,
        concurrency_key: opts.concurrency_key,
        // Issue #2407: the limit of the type when the start fires.
        concurrency_limit: crate::completion_trigger::fire_time_concurrency_limit(
            &workflow_name,
            opts.concurrency_limit,
        ),
        concurrency_on_conflict: opts.concurrency_on_conflict.unwrap_or_default(),
        priority,
        max_workflow_input_bytes: opts.max_workflow_input_bytes.unwrap_or(u64::MAX),
        owner: owner.as_deref(),
        runbook_url: runbook_url.as_deref(),
        severity: severity.as_deref(),
        context_headers: opts.context_headers,
        sla,
        // A throttle deferral keeps carryover lineage (issue #488). The
        // scheduler and backfill paths persist schedule_id, scheduled_for and
        // origin into the options of the pending row. So the fire is attributed
        // to the schedule exactly as an immediate fire is.
        schedule_id: opts.schedule_id,
        scheduled_for: opts.scheduled_for,
        workflow_retry_policy: retry_policy,
        workflow_retry_source: retry_source,
        max_workflow_attempts_ceiling: opts.max_workflow_attempts_ceiling,
        origin: opts.origin.as_deref(),
        completion_callbacks: opts.completion_callbacks,
        start_source,
        start_source_ref: start_source_ref.as_deref(),
        started_by: started_by.as_deref(),
        ..crate::execution::StartWorkflowParams::new(
            &workflow_name,
            &workflow_id,
            exec_id,
            row.input,
            &queue_name,
        )
    };

    // `in_outer_transaction = true`: runs inside the scanner's fire transaction,
    // so follow-ups are collected and spawned only after commit.
    // `Some(GateMode::CheckCached)` (issue #1053) honors the admission gate on
    // the real queue of this workflow at fire time. The `202` promise of a
    // deferred start is "will run when tokens allow AND the gate is open at
    // fire time". It is not merely "will run when tokens allow". `_collect`
    // applies the gate iff the start will CREATE. When it blocks, it records
    // `harvest.admission.blocked` on the passed recorder.
    match crate::execution::start_or_load_workflow_execution_collect_with_codecs(
        conn,
        params,
        true,
        false,
        Some(metrics),
        Some(crate::admission_gate::GateMode::CheckCached),
        codecs,
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
                is_scheduled_fire && started.created,
            )))
        }
        // The workflow_id is already taken under the reuse policy. Drop the row,
        // so that the scanner does not retry a doomed start forever. Refund the
        // reserved token, because no run was admitted.
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
        // Mirrors the identical arm in `debounce.rs::fire_claimed_debounce_row`
        // (issue #1353). An empty workflow_id
        // here can only be a LEGACY row. The admission path now rejects an
        // empty id before a throttle row can ever be written. Such a row
        // can never start. An un-caught `?` would abort this whole batch's
        // fire transaction and repeat the same failure every scanner tick,
        // starving every later scanner duty. Drop the row and refund its
        // reserved token, the same as the `AlreadyExists` arm.
        Err(crate::error::HarvestError::EmptyWorkflowId) => {
            delete_throttle_row(conn, row_id).await?;
            crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            tracing::warn!(
                workflow_name = %workflow_name,
                throttle_key = %throttle_key,
                "throttled start skipped: legacy row has an empty workflow_id (issue #1353)",
            );
            Ok(None)
        }
        // Issue #1053: the admission gate is closed on the real queue of this
        // workflow at fire time. Halt beats pace, so RE-DEFER. LEAVE the row
        // (do NOT delete), so a later tick fires it once the gate opens. Refund
        // the token reserved at the top, because nothing was admitted.
        // `_collect` already counted the block (`record_start_gate_block`).
        // This deliberately DIFFERS from the completion-trigger cross-shard
        // outbox relay (also `GatedAtRelay`), which DROPS its outbox row on
        // block. A completion trigger has already fired its source-terminal
        // decision, so a permanently-gated relay must not accumulate forever.
        // A throttled start is a pending admission with a `202` promise of
        // "nothing lost". So it is held, not dropped, until the gate opens or
        // `expires_at` passes.
        Err(crate::error::HarvestError::AdmissionBlocked { gate_id, reason }) => {
            crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            // Cross-key fairness (issue #1053): bump `deferred_at` forward by
            // `GATE_REDEFER_BACKOFF`. This gated row then rotates behind
            // un-gated rows in the `ORDER BY deferred_at ASC` fire ordering.
            // The doc of `GATE_REDEFER_BACKOFF` explains the starvation that
            // the bump prevents. It also explains why the bump never hides an
            // expired row from its drop. The update runs in the same fire
            // transaction as the refund, which holds the `FOR UPDATE` lock.
            redefer_throttle_row(conn, row_id, now + GATE_REDEFER_BACKOFF).await?;
            // `debug!`, not `info!`: this arm re-fires per gated row per scanner
            // tick. The block metric is intentionally per attempt, and the bump
            // above backs it off. An `info!` here would flood logs in the
            // high-load incident window that operators read. The silent
            // no-token re-defer above behaves the same way.
            tracing::debug!(
                workflow_name = %workflow_name,
                throttle_key = %throttle_key,
                workflow_id = %workflow_id,
                gate_id = %gate_id,
                reason = %reason,
                "throttled start blocked by an admission gate at fire time; \
                 re-deferred with backoff (row left, token refunded)",
            );
            Ok(None)
        }
        // Issue #946: a declared per-tenant quota is exhausted at fire time.
        // Unlike `AlreadyExists` above, this is TEMPORARY. The usage of the
        // tenant can free up as an existing execution completes or is deleted.
        // So the row is RE-DEFERRED, in the exact shape of the
        // `AdmissionBlocked` arm: left, token refunded, `deferred_at` bumped
        // with backoff. It is not dropped. A bare `?` must not abort the claim
        // transaction of this whole scanner tick either. That `?` would roll
        // back every OTHER already-started row in the same batch. It would
        // propagate out through `enforce_timeouts_once` and starve every later
        // scanner duty on every tick. Those duties include history-ceiling
        // enforcement, broken-session and mutex-lease reclaim, and the
        // start-idempotency sweep. The stall lasts while one quota-blocked
        // tenant sits at the head of the claim queue. That turns the "runaway
        // tenant" scenario that #946 contains into a fleet-wide scanner stall.
        Err(crate::error::HarvestError::QuotaExceeded {
            workflow_name,
            key,
            resource,
            limit,
            current,
        }) => {
            crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            redefer_throttle_row(conn, row_id, now + QUOTA_REDEFER_BACKOFF).await?;
            tracing::debug!(
                workflow_name = %workflow_name,
                throttle_key = %throttle_key,
                workflow_id = %workflow_id,
                quota_key = %key,
                resource = %resource,
                limit,
                current,
                "throttled start blocked by a per-tenant quota at fire time; \
                 re-deferred with backoff (row left, token refunded)",
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Pre-acquire every distinct rate-limit bucket row a claimed due-row batch
/// needs, in one deterministic (sorted) order, before any row fires (issue
/// #1230).
///
/// [`fire_claimed_throttle_row`] debits a bucket's token with a plain
/// `UPDATE harvest_rate_limit_buckets ... WHERE key = $1`. Postgres holds
/// that row's lock for the rest of this transaction, exactly like a quota
/// advisory lock. Two independent scanner transactions can therefore
/// deadlock on BUCKET locks alone (review). It is the same ABBA shape as
/// the original quota-lock hazard, but on a dimension
/// [`crate::quota_lock_order::order_rows_by_quota_lock_id`] never looks at. Batch A fires (bucket X,
/// quota 1) then (bucket Y, quota 2). Batch B fires (bucket Y, quota 1)
/// then (bucket X, quota 2).
///
/// Bucket locks and quota locks need a JOINT global order, not two
/// separate ones. This uses tiering. Every row's bucket lock is acquired
/// before its own execution-row lock and quota lock. That ordering is
/// already fixed, unconditionally, by [`fire_claimed_throttle_row`]'s own
/// code. So this defines "every bucket lock in the batch" as one tier,
/// acquired in sorted order before any row starts its
/// execution-row-then-quota-key work. That keeps every row's own
/// bucket-before-quota order intact. It adds bucket-vs-bucket
/// deadlock-freedom on top of the already-proven quota-vs-quota and
/// execution-row-vs-quota-lock safety, instead of competing with them.
///
/// This is safe where the equivalent pre-lock for QUOTA keys was NOT
/// (issue #1230 Finding 2 follow-up, P1 -- see
/// [`crate::quota_lock_order`]'s module docs). That
/// pre-lock inverted lock order against a concurrent DIRECT start, which
/// also touches quota locks. Nothing outside this scanner ever touches a
/// rate-limit bucket row. `reserve_or_defer` only ever INSERTs the
/// pending throttle row; it never debits a token itself. So there is no
/// external caller whose acquisition order this pre-lock could invert.
///
/// Locking (not debiting) a bucket row ahead of time is also safe on its
/// own terms. `reserve_or_defer` calls `ensure_throttle_bucket` before
/// `insert_pending_throttle_row`, so the bucket row this pre-lock targets
/// already exists by the time any throttle row can reference it. This
/// takes a bare `FOR UPDATE` read lock, not the debit itself.
/// [`fire_claimed_throttle_row`]'s later `try_consume_rate_limit_token`
/// re-acquires the SAME already-held row lock. Postgres row locks are
/// re-entrant within one session. It still does the actual
/// debit-if-available check then, unchanged.
///
/// One round trip locks the whole batch. An earlier cut issued one
/// `FOR UPDATE` statement per distinct bucket key. A claimed batch of
/// [`THROTTLE_FIRE_BATCH_SIZE`] rows from that many tenants paid that
/// many extra round trips on every scanner tick.
///
/// `ORDER BY key` on the batched query preserves the sorted-order
/// requirement above. Postgres plans a `LockRows` node above the `Sort`.
/// Rows lock in the sorted order the query returns them, not in scan
/// order. `EXPLAIN (ANALYZE, BUFFERS)` on this exact shape confirms this
/// (`docs/perf-artifacts/rate-limit-bucket-prelock-batch/`).
///
/// `COLLATE "C"` pins that order to a plain byte comparison. Rust's
/// `BTreeSet<String>` -- [`collect_distinct_bucket_keys`]'s own type --
/// always sorts by byte value, never by locale. A database with a
/// locale-aware collation (`en_US.UTF-8` and similar) would otherwise
/// lock in a different order. A peer still running the pre-fix per-key
/// loop locks in Rust's byte order. During a rolling upgrade that
/// mismatch reopens the same ABBA hazard this function exists to close.
#[cfg(feature = "db")]
async fn pre_lock_rate_limit_buckets_for_claimed_batch(
    conn: &mut diesel_async::AsyncPgConnection,
    due_rows: &[FireDueRow],
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    let bucket_keys: Vec<String> = collect_distinct_bucket_keys(due_rows).into_iter().collect();
    if bucket_keys.is_empty() {
        return Ok(());
    }
    diesel::sql_query(
        "SELECT key FROM harvest_rate_limit_buckets WHERE key = ANY($1) \
         ORDER BY key COLLATE \"C\" FOR UPDATE",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&bucket_keys)
    .execute(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(())
}

/// Pure half of [`pre_lock_rate_limit_buckets_for_claimed_batch`]: the
/// distinct, sorted bucket keys a claimed batch needs locked. Split out so
/// the ordering invariant is unit-testable without a database (issue #1230).
#[cfg(feature = "db")]
fn collect_distinct_bucket_keys(due_rows: &[FireDueRow]) -> std::collections::BTreeSet<String> {
    due_rows.iter().map(|row| row.bucket_key.clone()).collect()
}

/// Scan and fire due throttle rows on a single shard connection.
#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<Vec<FiredThrottle>> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    // Both queries return a single `present` boolean; share one row type.
    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }

    // Tolerate a missing table (mixed-schema fixtures / un-migrated deployment).
    let exists: Present =
        diesel::sql_query("SELECT to_regclass('harvest_start_throttle') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok(Vec::new());
    }

    // Cheap emptiness pre-check: skip opening a claim transaction entirely
    // when there is nothing pending (the common steady-state case). A row
    // inserted after this check is picked up on the next tick. The same
    // happens when the claim query itself returns zero rows.
    let has_rows: Present = diesel::sql_query(
        "SELECT EXISTS (SELECT 1 FROM harvest_start_throttle LIMIT 1) AS present",
    )
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    if !has_rows.present {
        return Ok(Vec::new());
    }

    // Claim + fire + delete the whole due batch in one transaction so each
    // `FOR UPDATE SKIP LOCKED` lock is held until its row is deleted (or left).
    //
    // Issue #1822: a deadlock or serialization abort runs the batch again.
    // The rollback releases every claimed row and debited token.
    let fired: Vec<FiredThrottle> = Box::pin(crate::tx_retry::run_with_conflict_retry(
        conn,
        crate::tx_retry::SITE_SCANNER,
        metrics,
        crate::tx_retry::TxRetryPolicy::DEFAULT,
        async |conn| {
            Box::pin(
                conn.transaction::<Vec<FiredThrottle>, crate::error::HarvestError, _>(
                    async |conn| {
                        let now = Utc::now();
                        // Per-key-fair claim (issue #607). A flat
                        // `ORDER BY deferred_at ASC LIMIT N` lets one key with a large
                        // backlog monopolize every scanner tick, because its rows are
                        // always the globally oldest. Newer rows under other keys then
                        // starve indefinitely. The `candidates` CTE caps the rows that
                        // one `bucket_key` can contribute (THROTTLE_FIRE_PER_KEY_CAP)
                        // before the outer LIMIT applies.
                        //
                        // `candidates` keeps only rows that this tick can examine. A
                        // row qualifies when it is past its `schedule_to_start`
                        // deadline, so that AC-c can time it out. It also qualifies
                        // when the *effective* token level of its bucket shows >= 1.0
                        // available (`effective_available_tokens_expr`, issue #945).
                        // That level honors a live TTL'd rate-limit override, not only
                        // the declared baseline `refill_rate`/`burst`. A genuinely
                        // exhausted bucket is out of the candidate set at any scale
                        // (issue #607).
                        //
                        // A baseline-only formula here would silently defeat an
                        // operator override (issue #945). An operator raises the rate
                        // of a throttled workflow to unstick its deferred backlog. That
                        // is the whole point of an override on this bucket family. The
                        // pre-filter would still never admit those rows. Yet the debit
                        // below (`fire_claimed_throttle_row` ->
                        // `try_consume_rate_limit_token`) is already override-aware
                        // and would fire them.
                        //
                        // Without this pre-filter, enough concurrently-exhausted keys
                        // can permanently fill the batch with rows that never fire.
                        // Ordering cannot help. Rank-then-date ordering alone only
                        // bounds the damage to the first `THROTTLE_FIRE_BATCH_SIZE`
                        // distinct exhausted keys. Beyond that, their rank-1 rows fill
                        // the whole budget on every tick and starve a ready key
                        // forever.
                        //
                        // A row with a missing bucket is a data anomaly, because
                        // `reserve_or_defer` always creates the bucket before it
                        // inserts the pending row. Such a row conservatively stays a
                        // candidate. `try_consume_rate_limit_token` leaves it parked,
                        // not deleted, when there is no bucket to debit.
                        //
                        // `selected` orders the *pre-filtered* candidates by per-key
                        // rank first and `deferred_at` second. It still caps one key
                        // at `THROTTLE_FIRE_PER_KEY_CAP`, so several ready keys share
                        // the batch fairly. Postgres forbids `FOR UPDATE` in the same
                        // query block as a window function. So the outer,
                        // window-function-free SELECT takes the lock
                        // (`FOR UPDATE OF t`, not a bare `FOR UPDATE`).
                        let effective_tokens = crate::queue::effective_available_tokens_expr("b");
                        let due_sql = format!(
                            "
                WITH candidates AS (
                    SELECT t.id, t.deferred_at,
                           ROW_NUMBER() OVER (
                               PARTITION BY t.bucket_key ORDER BY t.deferred_at ASC
                           ) AS rn
                    FROM harvest_start_throttle t
                    LEFT JOIN harvest_rate_limit_buckets b ON b.key = t.bucket_key
                    WHERE (t.expires_at IS NOT NULL AND t.expires_at < NOW())
                       OR b.key IS NULL
                       OR {effective_tokens} >= 1.0
                ),
                selected AS (
                    SELECT id FROM candidates WHERE rn <= $1
                    ORDER BY rn ASC, deferred_at ASC LIMIT $2
                )
                SELECT t.id, t.workflow_name, t.throttle_key, t.bucket_key, t.workflow_id,
                       t.queue_name, t.input, t.start_options, t.expires_at, t.shard_id,
                       t.deferred_at
                FROM harvest_start_throttle t
                JOIN selected s ON s.id = t.id
                ORDER BY t.deferred_at ASC
                FOR UPDATE OF t SKIP LOCKED
            "
                        );
                        let due_rows: Vec<FireDueRow> = diesel::sql_query(due_sql)
                            .bind::<diesel::sql_types::BigInt, _>(THROTTLE_FIRE_PER_KEY_CAP)
                            .bind::<diesel::sql_types::BigInt, _>(THROTTLE_FIRE_BATCH_SIZE)
                            .load(conn)
                            .await
                            .map_err(crate::error::database_error)?;

                        let due_rows = decode_due_rows(conn, due_rows, codecs, now).await?;
                        pre_lock_rate_limit_buckets_for_claimed_batch(conn, &due_rows).await?;
                        // The sort uses the quota key, not `bucket_key`. A bucket that
                        // mixes quota keys can fire a newer row before an older one in
                        // one tick. Deadlock freedom has priority over that order. The
                        // delayed row fires on the next scan (issue #607).
                        let due_rows =
                            crate::quota_lock_order::order_due_rows_for_deadlock_free_firing(
                                conn, due_rows,
                            )
                            .await?;

                        let mut results = Vec::with_capacity(due_rows.len());
                        for row in due_rows {
                            if let Some(item) =
                                fire_claimed_throttle_row(conn, row, now, metrics, codecs).await?
                            {
                                results.push(item);
                            }
                        }
                        Ok(results)
                    },
                ),
            )
            .await
        },
    ))
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
    fire_due_throttled_starts_with_codecs(
        conn,
        sharded_pool,
        shard_assignments,
        metrics,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`fire_due_throttled_starts`], encoding a flushed `WorkflowStarted.input`
/// through `codecs` (issue #1243).
///
/// # Errors
///
/// Same as [`fire_due_throttled_starts`].
#[cfg(feature = "db")]
pub async fn fire_due_throttled_starts_with_codecs(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<usize> {
    fire_due_throttled_starts_on_conn_shard(
        conn,
        None,
        sharded_pool.as_ref(),
        shard_assignments,
        metrics,
        codecs,
    )
    .await
}

/// [`fire_due_throttled_starts_with_codecs`] for a caller that knows `conn`'s
/// shard. See [`crate::shard::connect_or_reuse`].
#[cfg(feature = "db")]
pub(crate) async fn fire_due_throttled_starts_on_conn_shard(
    conn: &mut diesel_async::AsyncPgConnection,
    conn_shard: Option<crate::types::ShardId>,
    sharded_pool: Option<&crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
    codecs: &crate::payload_codec::PayloadCodecs,
) -> crate::error::HarvestResult<usize> {
    async fn spawn_fired(
        fired: Vec<FiredThrottle>,
        metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
        conn: &mut diesel_async::AsyncPgConnection,
    ) -> usize {
        let count = fired.len();
        for (
            workflow_name,
            queue_name,
            deferred_starts,
            deferred_checks,
            cancel_metrics,
            is_scheduled_run,
        ) in fired
        {
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
            crate::execution::emit_start_cancel_metrics(metrics, &cancel_metrics);
            // Count a throttled scheduler-tick or buffered-backfill-fire
            // dispatch here, on the terms of its immediate-path sibling
            // (`if outcome.created() { metrics.record_schedule_run(...) }`).
            // Otherwise `harvest.schedule.runs` silently undercounts every
            // scheduled slot that deferred (issue #607).
            if is_scheduled_run {
                metrics.record_schedule_run("workflow", &workflow_name);
            }
            // Issue #1053: `spawn_fired` iterates only the `fired` list. Those
            // rows genuinely started: the gate was OPEN at fire time and the
            // relay proceeded. A fire that the admission gate BLOCKED returns
            // `Ok(None)` from `fire_claimed_throttle_row`, with the row
            // re-deferred and the token refunded. It is NOT in `fired`. So it
            // counts a `harvest.admission.blocked` (recorded by `_collect`) and
            // NEVER a bypass here. Block and bypass never double-count one fire.
            // The Throttle producer is `GatedAtRelay`. It changed from
            // `GatedAtAdmission` in #618. A genuinely-fired start counts an
            // exempt bypass, so an open-gate relay never slips a gate silently.
            // See `admission_gate::producer_contract`.
            metrics
                .record_admission_bypassed(crate::admission_gate::StartProducer::Throttle.as_str());
            let _ = &queue_name;
        }
        count
    }

    let mut fired_count = 0usize;
    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            for shard in shard_assignments {
                let Some(mut shard_conn) = crate::shard::connect_or_reuse(
                    conn,
                    conn_shard,
                    sp,
                    *shard,
                    "throttle",
                    crate::shard::ShardConnectError::LogAndSkip,
                )
                .await?
                else {
                    continue;
                };
                let fired = fire_due_on_conn(&mut shard_conn, metrics, codecs).await?;
                fired_count += spawn_fired(fired, metrics, &mut shard_conn).await;
            }
        }
        _ => {
            let fired = fire_due_on_conn(conn, metrics, codecs).await?;
            fired_count += spawn_fired(fired, metrics, conn).await;
        }
    }

    Ok(fired_count)
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

/// Per-`(workflow_name, throttle_key)` backlog counts on this shard (admin
/// read).
///
/// It joins the shared token bucket of each key (issue #945). So the response
/// also surfaces `override_active`, the resolved effective refill rate/burst,
/// and the declared baseline. The join is
/// `LEFT JOIN ... ON b.key = t.bucket_key`. Like the `GROUP BY`, it reads
/// only the tables of this shard, as the rest of the throttle admission path
/// does.
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
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
        declared_refill_rate: Option<f64>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
        declared_burst: Option<f64>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
        override_refill_rate: Option<f64>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Double>)]
        override_burst: Option<f64>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        override_expires_at: Option<DateTime<Utc>>,
    }
    let rows: Vec<GroupRow> = diesel::sql_query(
        "SELECT t.workflow_name, t.throttle_key, t.shard_id,
                COUNT(*) AS deferred_count,
                MIN(t.deferred_at) AS oldest_deferred_at,
                MAX(b.refill_rate) AS declared_refill_rate,
                MAX(b.burst) AS declared_burst,
                MAX(b.override_refill_rate) AS override_refill_rate,
                MAX(b.override_burst) AS override_burst,
                MAX(b.override_expires_at) AS override_expires_at
         FROM harvest_start_throttle t
         LEFT JOIN harvest_rate_limit_buckets b ON b.key = t.bucket_key
         GROUP BY t.workflow_name, t.throttle_key, t.shard_id
         ORDER BY deferred_count DESC",
    )
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;
    let now = Utc::now();
    Ok(rows
        .into_iter()
        .map(|r| {
            let (override_active, effective_refill_rate, effective_burst) =
                resolve_backlog_bucket_state(
                    r.declared_refill_rate,
                    r.declared_burst,
                    r.override_refill_rate,
                    r.override_burst,
                    r.override_expires_at,
                    now,
                );
            ThrottleBacklogEntry {
                workflow_name: r.workflow_name,
                throttle_key: r.throttle_key,
                deferred_count: r.deferred_count,
                oldest_deferred_at: r.oldest_deferred_at,
                shard_id: r.shard_id,
                declared_refill_rate: r.declared_refill_rate,
                declared_burst: r.declared_burst,
                override_refill_rate: r.override_refill_rate,
                override_burst: r.override_burst,
                override_active,
                effective_refill_rate,
                effective_burst,
                expires_at: r.override_expires_at,
            }
        })
        .collect())
}

/// Count of pending (not-yet-fired) `harvest_start_throttle` rows for a
/// given `workflow_name`, across every throttle key.
///
/// A throttled admission durably defers *before* any `WorkflowStarted`
/// event exists -- no `harvest_workflow_executions` row is created until the
/// scanner fires it. A caller enforcing a workflow-name-scoped concurrency
/// bound (e.g. a schedule's `max_active_runs`/overlap policy, issue #241/#478)
/// against a plain `COUNT(*) ... WHERE state IN ('RUNNING','PAUSED')` query
/// would therefore under-count. A deferred fire is a logically in-flight run
/// that has not materialized yet. Omitting it lets the caller admit more
/// concurrent runs than its own limit allows (issue #607).
/// Callers should add this count to their own running-count before
/// comparing against a `max_active_runs`-style ceiling.
///
/// Returns `0` when the `harvest_start_throttle` table is missing, as in
/// mixed-schema fixtures or a deployment mid-upgrade. That matches the
/// `to_regclass` tolerance of [`fire_due_throttled_starts`]. Scheduler test
/// fixtures that never migrated the throttle table then keep working.
///
/// # Errors
/// Returns `HarvestError` if the database query fails.
#[cfg(feature = "db")]
pub async fn pending_throttle_count_for_workflow(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_name: &str,
) -> crate::error::HarvestResult<i64> {
    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    use diesel_async::RunQueryDsl;
    let exists: Present =
        diesel::sql_query("SELECT to_regclass('harvest_start_throttle') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok(0);
    }

    let row: Count = diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_start_throttle WHERE workflow_name = $1",
    )
    .bind::<diesel::sql_types::Text, _>(workflow_name)
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(row.n)
}

/// Batched form of [`pending_throttle_count_for_workflow`] for many names at once.
///
/// It sends one `to_regclass` existence check and one grouped
/// `COUNT(*) ... GROUP BY workflow_name` query for all of `workflow_names`.
/// That replaces one existence check plus one count query per name. The
/// per-name form ran once per schedule row through
/// `scheduler::schedule_running_basis` (Ledger perf pass on
/// `GET /admin/schedules`).
///
/// A name with no pending throttle rows is absent from the returned map.
/// [`pending_throttle_count_for_workflow`] returns `0` for it, so callers
/// should treat a missing key as zero.
///
/// # Errors
///
/// Returns a database error if either the existence check or the grouped
/// count query fails.
#[cfg(feature = "db")]
pub async fn pending_throttle_counts_for_workflows(
    conn: &mut diesel_async::AsyncPgConnection,
    workflow_names: &[&str],
) -> crate::error::HarvestResult<std::collections::HashMap<String, i64>> {
    #[derive(diesel::QueryableByName)]
    struct Present {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }

    use diesel_async::RunQueryDsl;

    if workflow_names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let exists: Present =
        diesel::sql_query("SELECT to_regclass('harvest_start_throttle') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok(std::collections::HashMap::new());
    }

    let names: Vec<String> = workflow_names.iter().map(|s| (*s).to_string()).collect();
    let rows: Vec<Count> = diesel::sql_query(
        "SELECT workflow_name, COUNT(*) AS n FROM harvest_start_throttle \
         WHERE workflow_name = ANY($1) GROUP BY workflow_name",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(names)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().map(|r| (r.workflow_name, r.n)).collect())
}

// ---------------------------------------------------------------------------
// Unit tests (no DB required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone as _;

    #[cfg(feature = "db")]
    #[test]
    fn fire_due_row_gives_the_quota_lock_inputs() {
        use crate::quota_lock_order::QuotaLockRow;
        let row = FireDueRow {
            id: uuid::Uuid::new_v4(),
            workflow_name: "wf_a".to_string(),
            throttle_key: "k".to_string(),
            bucket_key: "b".to_string(),
            workflow_id: "w".to_string(),
            queue_name: "default".to_string(),
            input: serde_json::json!({ "tenant_id": "t1" }),
            start_options: serde_json::json!({ "tenant_id": "wrong" }),
            expires_at: None,
            shard_id: 0,
            deferred_at: Utc::now(),
        };
        assert_eq!(row.workflow_name(), "wf_a");
        assert_eq!(row.quota_input()["tenant_id"], "t1");
    }

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

    #[test]
    fn parse_rate_rejects_infinity() {
        assert!(parse_rate("inf/m").is_err());
        assert!(parse_rate("infinity/s").is_err());
    }

    #[test]
    fn parse_rate_rejects_negative_infinity() {
        assert!(parse_rate("-inf/m").is_err());
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

    /// Issue #607: an explicit burst below 1.0 must be rejected. The token
    /// debit path (`LEAST(burst, refilled) >= 1.0`) can never reserve a token
    /// when `burst < 1.0`. Every start under that key would defer forever.
    #[test]
    fn from_rate_str_rejects_explicit_burst_below_one() {
        let err = ThrottlePolicy::from_rate_str("100/m", Some(0.5), None, None).unwrap_err();
        assert!(
            err.contains(">= 1.0"),
            "error should explain the >= 1.0 floor: {err}"
        );
    }

    /// Issue #607: an infinite explicit burst passes `b >= 1.0` (IEEE 754
    /// comparison). Unchecked, it is inserted verbatim as the initial `tokens`
    /// of the bucket. Then `LEAST(burst, tokens + refill)` degrades to an unbounded,
    /// ever-growing allowance, which disables the throttle for that key. The
    /// `is_finite` check rejects it with a "finite" error.
    #[test]
    fn from_rate_str_rejects_infinite_explicit_burst() {
        let err =
            ThrottlePolicy::from_rate_str("100/m", Some(f64::INFINITY), None, None).unwrap_err();
        assert!(err.contains("finite"), "{err}");
    }

    #[test]
    fn from_rate_str_rejects_nan_explicit_burst() {
        assert!(ThrottlePolicy::from_rate_str("100/m", Some(f64::NAN), None, None).is_err());
    }

    /// Same bug, reached through a *defaulted* burst. A fractional per-unit
    /// rate like `"0.5/s"` with no explicit burst defaults to
    /// `per_period_count` (0.5). That is as unusable as an explicit sub-one
    /// burst.
    #[test]
    fn from_rate_str_rejects_fractional_rate_with_no_explicit_burst() {
        assert!(ThrottlePolicy::from_rate_str("0.5/s", None, None, None).is_err());
    }

    /// An explicit burst >= 1.0 overrides the defaulted per-period count of a
    /// fractional rate. The sub-unit *rate* is fine. Only the resulting bucket
    /// *capacity* must reach at least one token.
    #[test]
    fn from_rate_str_accepts_explicit_burst_overriding_a_fractional_rate() {
        let p = ThrottlePolicy::from_rate_str("0.5/s", Some(1.0), None, None).unwrap();
        assert!((p.burst - 1.0).abs() < 1e-9);
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

    // ── resolve_backlog_bucket_state (issue #945) ───────────────────────────
    //
    // `throttle_backlog_by_key` applies this pure mapping to each LEFT-JOINed
    // backlog row. So `GET /admin/start-throttle` surfaces `override_active`,
    // the resolved effective values and the declared baseline beside the
    // backlog counts.

    #[cfg(feature = "db")]
    #[test]
    fn resolve_backlog_bucket_state_no_override() {
        let now = ts(12, 0, 0);
        let (active, refill, burst) =
            resolve_backlog_bucket_state(Some(5.0), Some(10.0), None, None, None, now);
        assert!(!active);
        assert_eq!(refill, Some(5.0));
        assert_eq!(burst, Some(10.0));
    }

    #[cfg(feature = "db")]
    #[test]
    fn resolve_backlog_bucket_state_active_override() {
        let now = ts(12, 0, 0);
        let expires = now + chrono::Duration::seconds(60);
        let (active, refill, burst) = resolve_backlog_bucket_state(
            Some(5.0),
            Some(10.0),
            Some(50.0),
            Some(100.0),
            Some(expires),
            now,
        );
        assert!(active);
        assert_eq!(refill, Some(50.0));
        assert_eq!(burst, Some(100.0));
    }

    #[cfg(feature = "db")]
    #[test]
    fn resolve_backlog_bucket_state_expired_override_falls_back_to_declared() {
        let now = ts(12, 0, 0);
        let expired = now - chrono::Duration::seconds(1);
        let (active, refill, burst) = resolve_backlog_bucket_state(
            Some(5.0),
            Some(10.0),
            Some(50.0),
            Some(100.0),
            Some(expired),
            now,
        );
        assert!(!active);
        assert_eq!(refill, Some(5.0));
        assert_eq!(burst, Some(10.0));
    }

    #[cfg(feature = "db")]
    #[test]
    fn resolve_backlog_bucket_state_missing_bucket_row_is_defensive_none() {
        // A `LEFT JOIN` miss must not panic and must report no override and no
        // effective values. It is practically unreachable: `reserve_or_defer`
        // always creates the bucket before it writes the backlog row.
        let now = ts(12, 0, 0);
        let (active, refill, burst) =
            resolve_backlog_bucket_state(None, None, None, None, None, now);
        assert!(!active);
        assert_eq!(refill, None);
        assert_eq!(burst, None);
    }

    // ── collect_distinct_bucket_keys (issue #1230) ───────────────────────────

    #[cfg(feature = "db")]
    mod bucket_lock_ordering {
        use super::*;

        fn row(bucket_key: &str) -> FireDueRow {
            FireDueRow {
                id: uuid::Uuid::new_v4(),
                workflow_name: "wf_a".to_string(),
                throttle_key: "irrelevant".to_string(),
                bucket_key: bucket_key.to_string(),
                workflow_id: uuid::Uuid::new_v4().to_string(),
                queue_name: "default".to_string(),
                input: serde_json::json!({}),
                start_options: serde_json::json!({}),
                expires_at: None,
                shard_id: 0,
                deferred_at: Utc::now(),
            }
        }

        #[test]
        fn dedupes_and_sorts_the_same_regardless_of_claim_order() {
            // The exact invariant that closes the bucket-vs-bucket ABBA
            // hazard. Two claimed batches need the SAME two bucket keys,
            // presented in OPPOSITE claim order. They must still lock
            // those keys in the SAME order.
            let forward: Vec<_> = collect_distinct_bucket_keys(&[row("bucket-a"), row("bucket-b")])
                .into_iter()
                .collect();
            let reverse: Vec<_> = collect_distinct_bucket_keys(&[row("bucket-b"), row("bucket-a")])
                .into_iter()
                .collect();

            assert_eq!(forward, reverse);
            assert_eq!(
                forward,
                vec!["bucket-a".to_string(), "bucket-b".to_string()]
            );
        }

        #[test]
        fn dedupes_repeated_bucket_within_one_batch() {
            let rows = [row("bucket-a"), row("bucket-a"), row("bucket-a")];
            assert_eq!(collect_distinct_bucket_keys(&rows).len(), 1);
        }
    }
}
