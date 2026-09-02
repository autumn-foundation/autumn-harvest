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
//!
//! # Known gaps — call-site duplication and ungated producers
//!
//! `reserve_or_defer` is invoked independently from **four** call sites (the
//! HTTP start route and the batch-start handler in `autumn-harvest-plugin`'s
//! `api.rs`, plus the scheduler-tick and scheduler-buffered/backfill-fire loops
//! in `scheduler.rs`) rather than from one central choke point. This mirrors —
//! and does not fix — the same per-producer duplication shape the admission-gate
//! feature (#377) has (its full producer contract is enumerated by
//! [`crate::admission_gate::producer_contract`]). A
//! consolidated design would thread throttle admission into
//! `execution::start_or_load_workflow_execution_collect` itself so every
//! current *and future* producer gets it for free; that refactor was judged
//! out of proportion for this feature (it touches code every producer
//! depends on) and is left as a follow-up. Until then, the following
//! producers of workflow starts do **not** consult a workflow's
//! [`ThrottlePolicy`] at all:
//!
//! * **`signal_with_start_workflow_execution` / `update_with_start_workflow_execution`**
//!   (`execution.rs`): an entity-workflow pattern fed by signals/updates can
//!   generate exactly the same kind of start burst an HTTP `POST .../start`
//!   caller can, with zero pacing.
//! * **Completion-trigger continuations** (`completion_trigger.rs`): a
//!   terminal-state trigger that starts a follow-up workflow does not consult a
//!   `ThrottlePolicy`. (It DOES honour the admission gate as of #618 — the
//!   throttle and the gate are independent controls.)
//! * **The outbox relay** (`autumn-harvest-plugin`'s `outbox.rs`): replays
//!   workflow-start events durably queued before a bucket was ever consulted.
//!
//! A workflow author relying on `#[workflow(throttle(...))]` to bound a
//! rate-limited downstream should confirm none of these paths can also start
//! that workflow, or the effective admitted rate can exceed the declared one.
//!
//! Separately, the scanner (`fire_due_on_conn`/`fire_claimed_throttle_row`/
//! `fire_due_throttled_starts`) is a near-total structural copy of
//! `debounce.rs`'s equivalent (`fire_due_on_conn`/`fire_claimed_debounce_row`/
//! `fire_due_debounced_starts`) rather than a shared generic scanner — a bug
//! fix to the shared shape (the `to_regclass` missing-table tolerance, the
//! `FOR UPDATE SKIP LOCKED` claim-batch transaction structure, or the
//! spawn-after-commit pattern for deferred trigger-starts) must currently be
//! applied in both places.

use std::time::Duration;

use chrono::{DateTime, Utc};

/// Prefix for the per-key token bucket stored in `harvest_rate_limit_buckets`,
/// namespaced so it can never collide with an activity rate-limit bucket key.
pub const THROTTLE_BUCKET_PREFIX: &str = "start-throttle";

/// Maximum number of pending-start rows fired per scanner tick, per shard.
pub const THROTTLE_FIRE_BATCH_SIZE: i64 = 100;

/// Maximum number of rows drained from a single `bucket_key` in one scanner tick.
///
/// Regardless of how large that key's backlog is (fairness fix, issue #607
/// code review), this bounds one hot/starving key to at most this fraction of
/// a tick's [`THROTTLE_FIRE_BATCH_SIZE`] budget so newer rows under *other*
/// keys are never starved by an unbounded single-key backlog.
pub const THROTTLE_FIRE_PER_KEY_CAP: i64 = 10;

/// Backoff applied to a throttle row's `deferred_at` when its fire is blocked by
/// an admission gate (issue #1053, review finding B-G1).
///
/// A gate is **queue-scoped**, so the SQL candidate pre-filter cannot see it (it
/// only knows token levels + `expires_at`). Without bumping `deferred_at`, a
/// gate-blocked-but-tokened row keeps its original (older) `deferred_at`, so it
/// stays at the front of the `ORDER BY deferred_at ASC` fire ordering and keeps
/// being re-selected every tick. If ≥[`THROTTLE_FIRE_BATCH_SIZE`] such rows are
/// gated together (a queue-wide incident gate over many tenant keys), they
/// monopolize the fixed-size fire batch and **starve newer un-gated throttle
/// keys on other queues indefinitely** — reopening exactly the cross-key
/// fairness pathology the per-key cap + token pre-filter above closed.
///
/// Bumping `deferred_at` forward on re-defer rotates a gated row *behind* every
/// un-gated row (whose `deferred_at` is its real, earlier deferral time) in the
/// fire ordering, so un-gated keys are selected and fire first, and a gated row
/// is re-attempted less often (fewer per-tick block-metric increments + log
/// lines). It is a modest fixed tuning value, not a config knob.
///
/// This is safe against the `schedule_to_start` staleness drop (AC-c, issue
/// #607): the candidate SELECT surfaces expired rows via an OR-`expires_at`
/// clause that is independent of `deferred_at` (there is no `deferred_at <= NOW()`
/// candidate floor), so pushing `deferred_at` into the future never hides an
/// expired row from its drop — `expires_at` is left untouched, so a gated row
/// still times out at its original deadline.
// Gated to match its sole user (the db-only throttle scanner path,
// `redefer_throttle_row`/`fire_claimed_throttle_row`): without this the const is
// dead code under narrow feature sets (e.g. the CLI Lint job, which compiles
// `autumn-harvest` without `db`), surfaced by clippy 1.97 `-D warnings`.
#[cfg(feature = "db")]
const GATE_REDEFER_BACKOFF: Duration = Duration::from_secs(5);

/// Re-defer backoff for a row blocked by an exhausted per-tenant quota
/// (issue #946) at fire time — same value and same rationale as
/// [`GATE_REDEFER_BACKOFF`] (a separate constant purely for the log/intent
/// clarity of naming the two distinct block causes independently; nothing
/// depends on them staying numerically equal).
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
    /// The final `burst` (explicit or defaulted) must be `>= 1.0`. The token
    /// debit path (`queue::try_consume_rate_limit_token`) only admits a start
    /// when the refilled bucket reaches `>= 1.0`, and refill is capped at
    /// `burst` — a bucket capacity below one token (e.g. an explicit
    /// `burst = 0.5`, or a fractional per-unit rate like `"0.5/s"` with no
    /// explicit burst) can therefore never successfully debit, deferring
    /// every start under that key forever (or until `schedule_to_start` drops
    /// it, if configured) rather than merely pacing them (code review, issue
    /// #607).
    ///
    /// # Errors
    /// Returns `Err(message)` when the rate string is malformed, the rate is
    /// non-positive, or the final `burst` (explicit or defaulted from the
    /// rate's per-period count) is below `1.0`.
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
                // An infinite (or NaN) burst is inserted verbatim as the
                // bucket's initial `tokens` (`ensure_throttle_bucket`), and
                // `LEAST(burst, tokens + refill)` degrades to `tokens + refill`
                // -- an unbounded, ever-growing allowance that never actually
                // throttles anything, effectively disabling the throttle for
                // that key (code review, issue #607).
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
    // `parse::<f64>()` accepts "nan"/"inf"/"infinity" literals, so reject
    // both explicitly via `!is_finite()` rather than `!(count > 0.0)` (NaN
    // comparisons are always false, which would make the negation silently
    // pass NaN through as "valid"; infinity would otherwise pass `> 0.0` and
    // produce an unbounded refill rate/burst that defeats the throttle).
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
    /// `true` when this call durably inserted a brand-new pending row;
    /// `false` when it instead resolved to an *already-existing* pending row
    /// for this `workflow_id` via `reserve_or_defer`'s idempotency shortcut
    /// (step 1) -- e.g. a client retry, or an operator repeating the same
    /// `schedule_backfill` window while an earlier call's deferred slots are
    /// still pending. A caller that tracks "how many new admissions did this
    /// call make" (budget spend, dispatched counters) must gate on `fresh`
    /// rather than treating every `Deferred` as a new admission (code
    /// review, issue #607) -- otherwise a repeated call double-counts and
    /// double-spends budget for a no-op idempotent attach.
    pub fresh: bool,
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
    /// An active execution already exists for this `(workflow_name,
    /// workflow_id)` under a reuse policy that makes admission a no-op
    /// (`AllowDuplicate`, `RejectDuplicate`) or resolves against a
    /// non-terminal prior (`AllowDuplicateFailedOnly` when the existing row
    /// isn't FAILED/CANCELLED). No token was reserved and no pending row was
    /// written — the caller must proceed straight to
    /// `start_or_load_workflow_execution*`, which resolves the reuse policy
    /// itself (returns the existing run, or rejects it under
    /// `RejectDuplicate`).
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
/// Alongside the resolved state of that throttle's shared token bucket
/// (issue #945): the declared baseline, any live TTL'd operator override,
/// and the resulting effective rate-limit parameters.
///
/// The bucket-derived fields are `None`/`false` only in the (practically
/// unreachable) case that a backlog row exists with no matching
/// `harvest_rate_limit_buckets` row -- [`reserve_or_defer`] always creates
/// the bucket before writing the backlog row, so this is a defensive
/// fallback rather than an expected shape.
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
/// `(declared_refill_rate, declared_burst)` are `None` only when the
/// backlog row's `bucket_key` has no matching `harvest_rate_limit_buckets`
/// row (a `LEFT JOIN` miss) -- practically unreachable since
/// [`reserve_or_defer`] always creates the bucket first, but handled
/// defensively rather than panicking.
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

/// Look up an already-pending row for this exact `(workflow_name, workflow_id)`,
/// regardless of which bucket/key it was filed under.
///
/// `workflow_id` is the natural idempotency boundary the rest of the engine
/// already keys on (reuse policies, uniqueness indexes); reusing it here makes a
/// retried admission for a start that is already durably deferred land on the
/// *same* pending row instead of silently queuing a second, independent one
/// that a later `reject_duplicate` fire would drop with no signal back to the
/// original (retried) caller.
///
/// A row already past its `schedule_to_start` deadline is never returned,
/// even if the scanner hasn't dropped it yet (code review, issue #607) --
/// otherwise a retry would land on a row the scanner is about to time out
/// and delete without ever starting a workflow, reporting `Deferred` for a
/// start that will never actually run. Ignoring it here lets the retry
/// proceed to a genuinely fresh admission instead.
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
    // Exclude a row already past its schedule_to_start deadline even if the
    // scanner hasn't deleted it yet (code review, issue #607): otherwise a
    // retry lands on a row the scanner is about to drop without ever
    // starting a workflow, silently reporting `Deferred` for a start that
    // will never run.
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

/// Whether an active execution for `(workflow_name, workflow_id)` already
/// makes a throttle admission a no-op or an immediate reject under `reuse_policy`.
///
/// `AllowDuplicate`/`RejectDuplicate` bypass unconditionally against any
/// existing state; `AllowDuplicateFailedOnly` bypasses unless the existing row
/// is FAILED/CANCELLED (a genuine fresh start); `TerminateIfRunning` never
/// bypasses (it always starts fresh: cancel + replace).
///
/// Extracted out of [`reserve_or_defer`] so both it and callers deciding
/// whether to apply a *different* admission-time check (e.g. the workflow
/// input byte-cap) can ask the identical question without duplicating the
/// lookup or the reuse-policy matching.
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
/// A thin boolean wrapper over [`existing_pending_throttle_for_workflow_id`]
/// for callers that only need the yes/no answer (e.g. deciding whether to
/// apply the workflow input byte-cap), not the full [`ThrottleDeferOutcome`].
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
/// A caller applying a byte-cap check *before* ever calling [`reserve_or_defer`]
/// (code-review fix, issue #607) must not reject a retry that
/// [`reserve_or_defer`] would resolve via [`ThrottleAdmission::Bypassed`] (an
/// active execution already satisfies the reuse policy) or via an idempotent
/// attach to an already-pending row — neither case inserts a fresh row, so
/// the cap must not apply to either. Composes [`resolve_bypass`] and
/// [`has_pending_throttle_for_workflow_id`], the same two primitives
/// [`reserve_or_defer`] itself now uses for its own steps (0) and (1), so
/// there is exactly one implementation of each question.
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
///    active execution already exists for `(workflow_name, workflow_id)`. If
///    the reuse policy makes this admission a no-op or an immediate reject —
///    `AllowDuplicate`/`RejectDuplicate` (any existing state), or
///    `AllowDuplicateFailedOnly` against a non-terminal prior — skip throttle
///    admission entirely and return [`ThrottleAdmission::Bypassed`]: no token
///    reserved, no pending row written. The caller proceeds straight to the
///    normal start, which resolves the reuse policy itself.
/// 1. **Idempotency, checked next:** if a pending row already exists for this
///    exact `workflow_id` (a retried admission for a start that is already
///    durably queued — possibly under a *different* throttle key than this
///    call resolves to), return *that* row's outcome rather than reserving a
///    fresh token under the new key and leaving the old row orphaned.
/// 2. **FIFO fast-path guard:** if a pending backlog already exists for this
///    key, defer (append) so a new start cannot jump ahead of older waiters or
///    starve them.
/// 3. Otherwise ensure the bucket exists and attempt a single-token debit
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
    // (0) Bypass entirely when an active execution already makes this
    // admission a no-op or an immediate reject under the caller's reuse
    // policy. TerminateIfRunning always starts fresh (cancel + replace), so
    // it's excluded from the lookup and always proceeds to normal pacing.
    let reuse_policy = params
        .start_options
        .reuse_policy
        .as_deref()
        .and_then(crate::debounce::parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);
    if resolve_bypass(conn, params.workflow_name, params.workflow_id, reuse_policy).await? {
        return Ok(ThrottleAdmission::Bypassed);
    }

    // (1) Idempotency: a retry of a request whose start is already durably
    // deferred (e.g. after a client-side timeout that never saw the first
    // 202) must land on the *same* pending row rather than queue a second one
    // for the same workflow_id. Checked *before* attempting a fresh token
    // reservation: otherwise a retry that resolves to a *different* throttle
    // key with an available token could win Reserved while the original
    // pending row under the old key sits orphaned.
    if let Some(existing) =
        existing_pending_throttle_for_workflow_id(conn, params.workflow_name, params.workflow_id)
            .await?
    {
        return Ok(ThrottleAdmission::Deferred(existing));
    }

    // Both idempotent-retry shortcuts above (0 and 1) resolve entirely from
    // `workflow_id` and never persist anything keyed by `throttle_key`, so
    // neither may depend on the key's length -- a stale/new input resolving
    // to an over-long key must still attach to the existing execution/pending
    // row rather than fail (code review, issue #607). Only a genuinely fresh
    // admission below actually persists a bucket/row keyed by `throttle_key`,
    // so the length is validated here, not before steps 0/1.
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

    // (2) FIFO fast-path guard: an existing backlog means we must append, not
    // jump the queue.
    if !pending_backlog_exists(conn, &key).await? {
        // (3) No backlog — ensure the bucket and try to reserve a token.
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
}

#[cfg(feature = "db")]
type FiredThrottle = (
    String, // workflow_name
    String, // queue_name
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(crate::types::ExecutionId, String)>,
    Vec<crate::execution::StartCancelledRun>,
    // Whether this fire should count toward `harvest.schedule.runs` (issue
    // #607 code review): true only when the deferred start's persisted
    // `origin == "scheduled"` (set only by the scheduler-tick and
    // buffered-run throttle branches, never by the HTTP manual-backfill
    // endpoint even though it also persists a `schedule_id`) and it actually
    // created a fresh execution, mirroring those callers' own immediate-path
    // gate (`if outcome.created() { metrics.record_schedule_run(...) }`) so a
    // throttled scheduled fire is counted exactly once, on the same terms as
    // an unthrottled one -- never for HTTP-start/batch/backfill-originated
    // throttled fires, which either carry no `schedule_id` at all or use a
    // different `origin` and were never counted here either.
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
/// [`GATE_REDEFER_BACKOFF`] (issue #1053, review finding B-G1) so it yields the
/// oldest-candidate fire slot to un-gated keys and is re-attempted less often.
///
/// `expires_at` is deliberately left untouched: the row still times out at its
/// original `schedule_to_start` deadline (AC-c). Runs inside the caller's fire
/// transaction so the row-level `FOR UPDATE` lock is held through the update.
/// Mirrors [`delete_throttle_row`]'s parameterized `sql_query` style (the
/// file-wide convention for row mutations).
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
/// - `Ok(None)` when the row was dropped or held without starting — either it
///   timed out past its `schedule_to_start` deadline (issue #607 AC-c), the
///   bucket had no token yet (left for a later tick — **not** deleted), the
///   start was a no-op under a reuse policy (deleted, token refunded), or the
///   admission gate was closed on the workflow's real queue at fire time
///   (left for a later tick — **not** deleted — token refunded; issue #1053).
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn fire_claimed_throttle_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueRow,
    now: DateTime<Utc>,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
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

    // A deserialize failure here (e.g. a schema mismatch across a rolling deploy,
    // or a corrupted row) must never crash the scanner — but silently falling
    // back to defaults would also silently drop this row's schedule_id/
    // scheduled_for/origin (breaking #488 carryover / #534 run-history
    // attribution) and every other persisted option with zero signal. Log it so
    // the loss is observable instead of invisible.
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

    let reuse_policy = opts
        .reuse_policy
        .as_deref()
        .and_then(crate::debounce::parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);
    let execution_timeout = opts
        .execution_timeout_secs
        .and_then(chrono::Duration::try_seconds);
    let sla = opts.sla_secs.and_then(chrono::Duration::try_seconds);
    let max_execution_timeout_ceiling = opts
        .max_execution_timeout_ceiling_secs
        .and_then(chrono::Duration::try_seconds);
    // Chain-scoped lifetime cap captured at admission (issue #617), so a throttled
    // start of a chain-capped workflow does not silently drop the declared cap.
    let chain_execution_timeout = opts
        .chain_execution_timeout_secs
        .and_then(chrono::Duration::try_seconds);
    let max_workflow_chain_timeout_ceiling = opts
        .max_workflow_chain_timeout_ceiling_secs
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
    // Restore the provenance captured at admission (issue #740). A pre-#740 row
    // has no captured source, so derive a sensible default from the persisted
    // dispatch origin: scheduled/backfill throttled fires stay attributed to
    // their schedule, everything else defaults to `Api`.
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
    // Only a scheduler-tick / buffered-run throttle branch should ever count
    // toward `harvest.schedule.runs` -- gated on `origin == "scheduled"`, not
    // merely `schedule_id.is_some()` (code review, issue #607): the HTTP
    // manual-backfill endpoint also persists a `schedule_id` (for carryover
    // lineage) but uses `origin = "backfill"` and its own immediate-path
    // dispatch never calls `record_schedule_run` either, so a throttled
    // backfill fire must not count here -- otherwise a throttled backfill
    // would inflate the metric while an unthrottled one (going through the
    // same handler) never does.
    let is_scheduled_fire = opts.origin.as_deref() == Some(crate::execution::ORIGIN_SCHEDULED);

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
        conflict_policy: crate::types::WorkflowIdConflictPolicy::Unspecified,
        trace_context: opts.trace_context,
        max_execution_timeout_ceiling,
        chain_execution_timeout,
        max_workflow_chain_timeout_ceiling,
        inherited_chain_deadline_at: None,
        concurrency_key: opts.concurrency_key,
        concurrency_limit: opts.concurrency_limit,
        concurrency_on_conflict: opts.concurrency_on_conflict.unwrap_or_default(),
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
        start_source,
        start_source_ref: start_source_ref.as_deref(),
        started_by: started_by.as_deref(),
    };

    // `in_outer_transaction = true`: runs inside the scanner's fire transaction,
    // so follow-ups are collected and spawned only after commit.
    // `Some(GateMode::CheckCached)` (issue #1053): honor the admission gate on
    // this workflow's real queue at fire time — the throttle deferred-start
    // `202` promise is "will run when tokens allow AND the gate is open at fire
    // time", not merely "will run when tokens allow". `_collect` applies the
    // gate iff the start will CREATE and records `harvest.admission.blocked` on
    // the passed recorder when it blocks.
    match crate::execution::start_or_load_workflow_execution_collect(
        conn,
        params,
        true,
        false,
        Some(metrics),
        Some(crate::admission_gate::GateMode::CheckCached),
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
        // issue #1053: the admission gate is closed on this workflow's real
        // queue at fire time. Halt-beats-pace — RE-DEFER: LEAVE the row (do NOT
        // delete) so a later tick fires it once the gate opens, and refund the
        // token reserved at the top (nothing was admitted). The block was
        // already counted by `_collect` (`record_start_gate_block`). This
        // deliberately DIFFERS from the completion-trigger cross-shard outbox
        // relay (also `GatedAtRelay`), which DROPS its outbox row on block: a
        // completion trigger has already fired its source-terminal decision, so
        // a permanently-gated relay must not accumulate forever; a throttled
        // start is a pending admission whose `202` promise is "nothing lost", so
        // it is held (not dropped) until the gate opens or `expires_at` passes.
        Err(crate::error::HarvestError::AdmissionBlocked { gate_id, reason }) => {
            crate::queue::refund_rate_limit_token(conn, &bucket).await?;
            // Cross-key fairness (issue #1053, review finding B-G1): bump
            // `deferred_at` forward by `GATE_REDEFER_BACKOFF` so this gated row
            // rotates behind un-gated rows in the `ORDER BY deferred_at ASC` fire
            // ordering. A queue-scoped gate is invisible to the SQL candidate
            // pre-filter, so without this a gated-but-tokened row keeps its
            // original (older) `deferred_at`, stays the oldest candidate, and is
            // re-selected every tick — ≥100 such rows would monopolize the
            // fixed-size fire batch and starve newer un-gated keys on other
            // queues indefinitely. `expires_at` is untouched, so the row still
            // times out at its original `schedule_to_start` deadline (AC-c), and
            // the candidate SELECT surfaces expired rows independent of
            // `deferred_at`, so the bump never hides the drop. Runs in the same
            // fire transaction as the refund (the `FOR UPDATE` lock is held).
            redefer_throttle_row(conn, row_id, now + GATE_REDEFER_BACKOFF).await?;
            // `debug!`, not `info!`: this arm re-fires per gated row per scanner
            // tick (the block metric is intentionally per-attempt, backed off by
            // the bump above), so an `info!` here would flood logs during exactly
            // the high-load incident window operators are reading — matching the
            // silent no-token re-defer twin above.
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
        // issue #946 (Task #7 hardening): a declared per-tenant quota (#946)
        // is exhausted at fire time. Unlike `AlreadyExists` above this is
        // TEMPORARY — the tenant's usage can free up as an existing execution
        // completes or is deleted — so the row is RE-DEFERRED (left, token
        // refunded, `deferred_at` bumped with backoff), mirroring the
        // `AdmissionBlocked` arm's exact shape, rather than dropped or
        // (via a bare `?`) allowed to abort this whole scanner-tick's claim
        // transaction. Un-caught, that `?` would roll back every OTHER
        // already-started row in the same batch and propagate out through
        // `enforce_timeouts_once`, starving every later scanner duty
        // (history-ceiling enforcement, broken-session/mutex-lease reclaim,
        // start-idempotency sweep, …) on every tick for as long as one
        // quota-blocked tenant sits at the head of the claim queue — the
        // exact "runaway tenant" scenario #946 exists to contain, turned
        // into a fleet-wide scanner stall instead.
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

/// Scan and fire due throttle rows on a single shard connection.
#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
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
    // inserted in the gap between this check and the next tick is simply
    // picked up then -- identical to today's behavior when the claim query
    // itself already returns zero rows.
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
    let fired: Vec<FiredThrottle> = Box::pin(
        conn.transaction::<Vec<FiredThrottle>, crate::error::HarvestError, _>(async |conn| {
            let now = Utc::now();
            // Per-key-fair claim (code review P1, issue #607): a flat
            // `ORDER BY deferred_at ASC LIMIT N` lets one throttle key with a
            // large backlog monopolize every scanner tick (its rows are
            // always the globally oldest), starving newer rows under other
            // keys indefinitely. The `candidates` CTE caps how many rows a
            // single `bucket_key` can contribute (THROTTLE_FIRE_PER_KEY_CAP)
            // before the outer LIMIT applies. Postgres forbids `FOR UPDATE`
            // in the same query block as a window function, so the lock
            // must be taken on the outer, window-function-free SELECT
            // (`FOR UPDATE OF t`, not a bare `FOR UPDATE`).
            // `candidates` is pre-filtered to rows that are actually
            // examinable this tick -- either already past their
            // `schedule_to_start` deadline (must be examined so AC-c can
            // time them out), or whose bucket's *effective* token level
            // (`effective_available_tokens_expr`, issue #945 -- honors a
            // live TTL'd rate-limit override, not just the declared
            // baseline `refill_rate`/`burst`) currently shows >= 1.0
            // available (a genuinely exhausted bucket is excluded from the
            // candidate set entirely, at any scale -- code review, issue
            // #607). Using the baseline-only formula here would silently
            // defeat an operator's own override: raising a throttled
            // workflow's rate specifically to unstick its already-deferred
            // backlog (the whole point of an override on this bucket
            // family) would never make the pre-filter admit those rows,
            // even though the debit below (`fire_claimed_throttle_row` ->
            // `try_consume_rate_limit_token`) is already override-aware and
            // would happily fire them if only they reached it (issue #945
            // review, P1). Without this pre-filter, enough
            // concurrently-exhausted keys can permanently occupy the
            // entire batch with rows that can never fire regardless of
            // ordering: this round's earlier fix (rank-then-date
            // ordering) only bounded the damage to the first
            // `THROTTLE_FIRE_BATCH_SIZE` distinct exhausted keys --
            // beyond that many, their rank-1 rows alone still fill the
            // whole budget on every tick, starving a ready key forever.
            // A row whose bucket happens to be missing (a data anomaly;
            // `reserve_or_defer` always creates the bucket before
            // inserting the pending row, so this should not occur in
            // practice) is conservatively still treated as a candidate,
            // matching this query's pre-existing behavior for that case
            // -- `try_consume_rate_limit_token` naturally leaves it
            // parked (not deleted) when there is truly no bucket to debit.
            // `selected` then orders the *pre-filtered* candidates by
            // per-key rank first, `deferred_at` second, still capping
            // one key at `THROTTLE_FIRE_PER_KEY_CAP` so several
            // simultaneously-ready keys share the batch fairly. Postgres
            // forbids `FOR UPDATE` in the same query block as a window
            // function, so the lock must be taken on the outer,
            // window-function-free SELECT (`FOR UPDATE OF t`, not a bare
            // `FOR UPDATE`).
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
                       t.queue_name, t.input, t.start_options, t.expires_at, t.shard_id
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

            let mut results = Vec::with_capacity(due_rows.len());
            for row in due_rows {
                if let Some(item) = fire_claimed_throttle_row(conn, row, now, metrics).await? {
                    results.push(item);
                }
            }
            Ok(results)
        }),
    )
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
            // A throttled scheduler-tick / buffered-backfill-fire dispatch is
            // counted here, on the same terms its immediate-path sibling
            // uses (`if outcome.created() { metrics.record_schedule_run(...) }`)
            // -- otherwise `harvest.schedule.runs` silently undercounts every
            // scheduled slot that happened to defer (issue #607 code review).
            if is_scheduled_run {
                metrics.record_schedule_run("workflow", &workflow_name);
            }
            // issue #1053: `spawn_fired` only ever iterates the `fired` list —
            // rows that genuinely started (the gate was OPEN at fire time, the
            // relay proceeded). A fire BLOCKED by the admission gate returns
            // `Ok(None)` from `fire_claimed_throttle_row` (row re-deferred,
            // token refunded) and is therefore NOT in `fired`, so it counts a
            // `harvest.admission.blocked` (recorded by `_collect`) and NEVER a
            // bypass here — block-vs-bypass can never double-count for one fire.
            // The Throttle producer is now `GatedAtRelay` (was `GatedAtAdmission`
            // per #618-F1): a genuinely-fired start counts an exempt bypass so an
            // open-gate relay never slips a gate silently. See
            // `admission_gate::producer_contract`.
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
                let fired = fire_due_on_conn(&mut shard_conn, metrics).await?;
                fired_count += spawn_fired(fired, metrics, &mut shard_conn).await;
            }
        }
        _ => {
            let fired = fire_due_on_conn(conn, metrics).await?;
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
/// Joined against that key's shared token bucket so the response also
/// surfaces `override_active`, the resolved effective refill rate/burst,
/// and the declared baseline (issue #945). The join is
/// `LEFT JOIN ... ON b.key = t.bucket_key`, scoped to this
/// shard's own tables exactly like the pre-existing `GROUP BY` -- shard-local,
/// consistent with the rest of the throttle admission path.
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
/// would therefore under-count: a deferred fire is a logically in-flight run
/// that just hasn't materialized yet, and omitting it lets the caller admit
/// more concurrent runs than its own limit allows (code review, issue #607).
/// Callers should add this count to their own running-count before
/// comparing against a `max_active_runs`-style ceiling.
///
/// Tolerates a missing `harvest_start_throttle` table (mixed-schema
/// fixtures / a deployment mid-upgrade) by returning `0`, matching
/// [`fire_due_throttled_starts`]'s own `to_regclass` tolerance -- this lets
/// every pre-existing scheduler test fixture that never migrated the
/// throttle table keep working unchanged.
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
/// One `to_regclass` existence check and one grouped
/// `COUNT(*) ... GROUP BY workflow_name` query covering every name in
/// `workflow_names`, instead of one existence check plus one count query per
/// name (Ledger perf pass on `GET /admin/schedules`, called once per schedule
/// row via `scheduler::schedule_running_basis`).
///
/// A name with no pending throttle rows is absent from the returned map,
/// matching what [`pending_throttle_count_for_workflow`] returns for it (`0`)
/// -- callers should treat a missing key as zero.
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

    /// Code-review fix (issue #607): an explicit burst below 1.0 must be
    /// rejected -- the token debit path (`LEAST(burst, refilled) >= 1.0`) can
    /// never successfully reserve a token when `burst < 1.0`, deferring every
    /// start under that key forever.
    #[test]
    fn from_rate_str_rejects_explicit_burst_below_one() {
        let err = ThrottlePolicy::from_rate_str("100/m", Some(0.5), None, None).unwrap_err();
        assert!(
            err.contains(">= 1.0"),
            "error should explain the >= 1.0 floor: {err}"
        );
    }

    /// Code-review fix (issue #607): an infinite (or NaN) explicit burst
    /// passes `b >= 1.0` (IEEE 754 comparison) but, inserted verbatim as the
    /// bucket's initial `tokens`, degrades `LEAST(burst, tokens + refill)` to
    /// an unbounded, ever-growing allowance -- effectively disabling the
    /// throttle for that key.
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

    /// Same bug, reached via a *defaulted* burst: a fractional per-unit rate
    /// like `"0.5/s"` with no explicit burst defaults to `per_period_count`
    /// (0.5), which is just as unusable as an explicit sub-one burst.
    #[test]
    fn from_rate_str_rejects_fractional_rate_with_no_explicit_burst() {
        assert!(ThrottlePolicy::from_rate_str("0.5/s", None, None, None).is_err());
    }

    /// An explicit burst >= 1.0 overrides a fractional rate's defaulted
    /// per-period count -- the sub-unit *rate* is fine, only the resulting
    /// bucket *capacity* must reach at least one token.
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
    // Pure mapping `throttle_backlog_by_key` applies to each LEFT-JOINed
    // backlog row so `GET /admin/start-throttle` surfaces `override_active`,
    // the resolved effective values, and the declared baseline alongside the
    // existing backlog counts.

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
        // A `LEFT JOIN` miss (practically unreachable -- `reserve_or_defer`
        // always creates the bucket before writing the backlog row) must not
        // panic and must report no override / no effective values.
        let now = ts(12, 0, 0);
        let (active, refill, burst) =
            resolve_backlog_bucket_state(None, None, None, None, None, now);
        assert!(!active);
        assert_eq!(refill, None);
        assert_eq!(burst, None);
    }
}
