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
    // Clamp absurd/overflowing durations to a large-but-finite value and use
    // checked addition so an extreme `window`/`max_wait` can never panic.
    let window_chrono =
        chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::days(365 * 100));
    let max_wait_chrono =
        chrono::Duration::from_std(max_wait).unwrap_or_else(|_| chrono::Duration::days(365 * 100));

    let trailing_edge = now.checked_add_signed(window_chrono).unwrap_or(now);
    let cap = first_seen
        .checked_add_signed(max_wait_chrono)
        .unwrap_or(first_seen);

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
    /// Effective owner/runbook/severity resolved from `WorkflowInfo` at admission
    /// time, so a debounced run carries the same operator metadata as a normal
    /// start (the fire path has no access to the plugin registry).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runbook_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
    /// Effective server-side execution-timeout ceiling (seconds), captured at
    /// admission so a debounced start can't bypass the cap the normal path
    /// enforces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_execution_timeout_ceiling_secs: Option<i64>,
    /// Effective chain-scoped lifetime cap DURATION (seconds), captured at
    /// admission from the workflow-type `chain_execution_timeout` so a debounced /
    /// throttled / batched start does NOT silently drop the declared chain cap
    /// (issue #617). Mirrors `execution_timeout_secs` exactly. `None` = no
    /// workflow-declared chain cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_execution_timeout_secs: Option<i64>,
    /// Effective fleet-wide chain-cap ceiling / default (seconds), captured at
    /// admission (issue #617). Mirrors `max_execution_timeout_ceiling_secs`.
    /// `None` = no fleet-wide chain cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_workflow_chain_timeout_ceiling_secs: Option<i64>,
    /// Effective workflow-input byte cap, captured at admission so a debounced
    /// start can't bypass the size limit the normal path enforces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_workflow_input_bytes: Option<u64>,
    /// W3C trace context captured at admission time, restored at fire time so
    /// debounced runs carry parent trace linkage (ADR-0001 §3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<crate::telemetry::TraceContextCarrier>,
    /// Effective workflow-level retry policy, captured at admission so a
    /// debounced run respects the same retry config as a normal start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_retry_policy: Option<serde_json::Value>,
    /// Server-side ceiling on workflow retry attempts, captured at admission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_workflow_attempts_ceiling: Option<u32>,
    /// Completion-callback targets validated at admission time, so a
    /// debounced/batched start honors the caller's registered targets
    /// instead of silently discarding them (issue #605 code review).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_callbacks: Option<serde_json::Value>,
    /// Schedule id that triggered this deferred start (issue #607 throttle +
    /// #488 carryover). Only ever set by the scheduler/backfill/manual-trigger
    /// throttle paths; debounce/batch always leave it `None`, so the carryover
    /// lineage of a throttled scheduled fire survives the deferral. A manual
    /// trigger sets `scheduled_for: None` alongside it (matching its own
    /// immediate-start path), so it is attributed to the schedule without
    /// participating in carryover.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_id: Option<uuid::Uuid>,
    /// Logical schedule slot this deferred start fires for (issue #488).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_for: Option<DateTime<Utc>>,
    /// Dispatch origin (issue #534): `scheduled`/`backfill`/`manual_trigger`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    /// Workflow-start provenance captured at admission and restored at fire
    /// (issue #740), so a debounced/throttled/batched run records where it
    /// truly came from (e.g. `webhook`, `api`, `batch`) rather than a hardcoded
    /// carrier-specific default. `None` on a pre-#740 serialized row → the fire
    /// path falls back to a sensible per-carrier default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_source: Option<String>,
    /// Provenance correlation ref captured at admission (issue #740).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_source_ref: Option<String>,
    /// Operator attribution captured at admission (issue #740).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_by: Option<String>,
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
    /// The stable `workflow_id` the eventual run will be created with. This is
    /// the **first** request's id for the key (kept across retriggers), which
    /// the caller should echo instead of its own generated id.
    pub workflow_id: String,
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
/// existing max_fire_at)`, increments `pending_count`, refreshes `queue_name`,
/// and overwrites `last_input` and `start_options` with the most recent
/// request's values (**last-input-wins**). The `workflow_id` is **kept from the
/// first request** so the stable id echoed in every `202` response is the one
/// the run will actually be created with; the stored id is returned so the
/// caller can echo it rather than its own (possibly-discarded) generated id.
///
/// Returns the current state of the record after the upsert.
///
/// # Errors
/// Returns `HarvestError` if the database upsert fails.
///
/// `gate_active` carries the caller's admission-gate decision for this request's
/// scope. The upsert and the gate decision run in **one transaction**: when the
/// gate is active the transaction is rolled back so the row is never committed
/// and `Ok(None)` is returned. This is the authoritative gate enforcement —
/// doing it post-commit (delete after the upsert is visible) is racy, because a
/// concurrent request for the same key could observe the row, update it, and be
/// accepted, only for this cleanup to then delete the shared row and drop that
/// accepted retrigger. Rolling back also preserves a pre-existing pending row
/// (a gated *update* leaves the prior committed row intact), and blocks a
/// burst update that would mutate a gate-relevant field (e.g. `queue_name`)
/// into the gated scope. `Ok(Some(outcome))` is returned when admitted.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub async fn admit_debounced_start(
    conn: &mut diesel_async::AsyncPgConnection,
    params: AdmitDebounceParams<'_>,
    gate_active: bool,
) -> crate::error::HarvestResult<Option<DebounceAdmitOutcome>> {
    // Sentinel error used to roll the transaction back when the gate is active
    // (an Ok return would commit). Distinguished from a real DB error below.
    enum AdmitTxnErr {
        Gated,
        Db(diesel::result::Error),
    }
    impl From<diesel::result::Error> for AdmitTxnErr {
        fn from(e: diesel::result::Error) -> Self {
            Self::Db(e)
        }
    }

    #[derive(diesel::QueryableByName)]
    struct UpsertRow {
        #[diesel(sql_type = diesel::sql_types::Text)]
        debounce_key: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        effective_fire_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        pending_count: i32,
        #[diesel(sql_type = diesel::sql_types::Bool)]
        is_new_record: bool,
    }

    use diesel_async::{AsyncConnection, RunQueryDsl};

    if params.debounce_key.len() > DEBOUNCE_KEY_MAX_BYTES {
        return Err(crate::error::HarvestError::Config(format!(
            "debounce key length {} bytes exceeds maximum {} bytes; \
             use a shorter key_expr or hash the value before using it as a key",
            params.debounce_key.len(),
            DEBOUNCE_KEY_MAX_BYTES
        )));
    }

    let now = Utc::now();
    // Clamp absurd/overflowing durations to a large-but-finite value and use
    // checked addition so an extreme `window`/`max_wait` can never panic.
    let window_chrono = chrono::Duration::from_std(params.window)
        .unwrap_or_else(|_| chrono::Duration::days(365 * 100));
    let max_wait_chrono = chrono::Duration::from_std(params.max_wait)
        .unwrap_or_else(|_| chrono::Duration::days(365 * 100));

    let max_fire_at = now.checked_add_signed(max_wait_chrono).unwrap_or(now);
    let initial_fire_at = now
        .checked_add_signed(window_chrono)
        .unwrap_or(now)
        .min(max_fire_at);

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
            -- workflow_id is intentionally NOT overwritten: the first request's
            -- id is the one the run is created with, and every 202 echoes it.
            queue_name        = EXCLUDED.queue_name,
            last_input        = EXCLUDED.last_input,
            -- issue #921 review (Codex P2): start_options is last-input-wins
            -- for every field (matching this function's documented semantics)
            -- EXCEPT completion_callbacks, which is merged (array-concatenated)
            -- across every admission in the burst instead of overwritten. A
            -- caller whose request registered a callback must still be
            -- notified when the collapsed execution eventually completes,
            -- even if a later burst member carried no callbacks (or a
            -- different one) and would otherwise clobber the earlier
            -- registration. Duplicate `{url, filter}` entries are harmless:
            -- `resolve_all_targets` already dedups by (url, filter) at
            -- workflow-start time, so this union does not need to dedup here.
            start_options = jsonb_set(
                EXCLUDED.start_options,
                '{completion_callbacks}',
                COALESCE(harvest_debounce.start_options->'completion_callbacks', '[]'::jsonb)
                    || COALESCE(EXCLUDED.start_options->'completion_callbacks', '[]'::jsonb)
            ),
            effective_fire_at = LEAST($8, harvest_debounce.max_fire_at),
            pending_count     = harvest_debounce.pending_count + 1,
            updated_at        = NOW()
        RETURNING
            debounce_key,
            workflow_id,
            effective_fire_at,
            pending_count,
            -- xmax = 0 means the row was just inserted (not updated)
            (xmax = 0) AS is_new_record
    ";

    let last_input = params.last_input;
    let workflow_name = params.workflow_name;
    let debounce_key = params.debounce_key;
    let workflow_id = params.workflow_id;
    let queue_name = params.queue_name;
    let shard_id = params.shard_id;

    let txn = Box::pin(
        conn.transaction::<DebounceAdmitOutcome, AdmitTxnErr, _>(async |conn| {
            let row: UpsertRow = diesel::sql_query(sql)
                .bind::<diesel::sql_types::Uuid, _>(new_id)
                .bind::<diesel::sql_types::Text, _>(workflow_name)
                .bind::<diesel::sql_types::Text, _>(debounce_key)
                .bind::<diesel::sql_types::Text, _>(workflow_id)
                .bind::<diesel::sql_types::Text, _>(queue_name)
                .bind::<diesel::sql_types::Jsonb, _>(last_input)
                .bind::<diesel::sql_types::Jsonb, _>(start_options_json)
                .bind::<diesel::sql_types::Timestamptz, _>(initial_fire_at)
                .bind::<diesel::sql_types::Timestamptz, _>(max_fire_at)
                .bind::<diesel::sql_types::Integer, _>(shard_id)
                .get_result(conn)
                .await?;

            // Roll back: a gated admission must not leave a committed row. The
            // upsert held a row lock for the duration of this transaction, so a
            // concurrent admission for the same key serialized behind it and
            // never observed the rolled-back row.
            if gate_active {
                return Err(AdmitTxnErr::Gated);
            }

            Ok(DebounceAdmitOutcome {
                debounce_key: row.debounce_key,
                workflow_id: row.workflow_id,
                fire_at: row.effective_fire_at,
                pending_count: row.pending_count,
                is_new_record: row.is_new_record,
            })
        }),
    )
    .await;

    match txn {
        Ok(outcome) => Ok(Some(outcome)),
        Err(AdmitTxnErr::Gated) => Ok(None),
        Err(AdmitTxnErr::Db(e)) => Err(crate::error::database_error(e)),
    }
}

/// Maximum number of debounce rows fired per scanner tick.
/// Prevents a single shard from being overwhelmed if many keys come due simultaneously.
pub const DEBOUNCE_FIRE_BATCH_SIZE: i64 = 100;

/// Maximum allowed byte length for a debounce key.
///
/// Keys that exceed this limit are rejected at admission time with
/// [`crate::error::HarvestError::Config`] rather than being persisted and
/// subsequently failing with a Postgres constraint violation (VARCHAR index
/// limit). Embedders with long natural keys should hash or truncate them before
/// use.
pub const DEBOUNCE_KEY_MAX_BYTES: usize = 2048;

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
/// The claim (`SELECT ... FOR UPDATE SKIP LOCKED`), the
/// `start_or_load_workflow_execution_collect` call, and the row `DELETE` all run
/// inside **one** transaction so the row lock is held from claim through delete.
/// This is essential: a `FOR UPDATE` lock taken in autocommit mode is released
/// the instant the `SELECT` completes, which would let a concurrent worker fire
/// the same key twice and let a concurrent `admit_debounced_start` overwrite the
/// row (extending the deadline / updating `last_input`) only to have the
/// scanner fire a stale snapshot and delete the updated row — breaking the
/// trailing-edge / last-input-wins guarantees. Holding the lock makes a
/// concurrent upsert block until this transaction commits, after which the row
/// is gone and the new trigger starts a fresh debounce cycle.
///
/// Deferred completion-trigger starts (from `start_or_load_workflow_execution_collect`)
/// are spawned **after** the outer transaction commits so a rollback on a later
/// row can never leave orphaned completion-trigger workflows started for a start
/// that did not become durable.
///
/// # Note: scanner gate limitation
///
/// The scanner fires via the core start path (`start_or_load_workflow_execution_collect`)
/// and has no access to plugin-level admission gates set after debounce admission.
/// Gates are evaluated at HTTP admission time; any gate changes between admission
/// and fire are not checked. This is a documented limitation of shard-local debounce
/// (consistent with #247).
///
/// Called from [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is spawned.
/// Returns the number of executions started.
///
/// # Errors
/// Returns `HarvestError` if the database query or any execution start fails.
#[cfg(feature = "db")]
type FiredDebounce = (
    String,
    String,
    Vec<crate::completion_trigger::DeferredTriggerStart>,
    Vec<(crate::types::ExecutionId, String)>,
    Vec<(String, String)>,
);

/// Scan and fire due debounce rows on a single shard connection. Returns the
/// fired records (`workflow_name`, `queue_name`, deferred trigger-starts, deferred checks) for the
/// caller to spawn + record metrics after all shards are processed.
#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    _metrics: Option<&(dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> crate::error::HarvestResult<Vec<FiredDebounce>> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    // Tolerate a missing `harvest_debounce` table (mixed-schema test fixtures
    // that exercise unrelated timeout behaviour, or a deployment that hasn't run
    // the migration yet). `to_regclass` returns NULL instead of erroring, so a
    // cheap catalog lookup lets the rest of the timeout sweep proceed rather
    // than aborting the whole pass. In production the table always exists.
    #[derive(diesel::QueryableByName)]
    struct TableExists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let exists: TableExists =
        diesel::sql_query("SELECT to_regclass('harvest_debounce') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok(Vec::new());
    }

    // Claim + fire + delete the whole due batch in one transaction so the
    // `FOR UPDATE SKIP LOCKED` locks are held until each row is deleted.
    // Deferred trigger-starts are collected and spawned *after* the transaction
    // commits so a rollback can't leave orphaned completion-trigger workflows.
    let fired: Vec<FiredDebounce> = Box::pin(
        conn.transaction::<Vec<FiredDebounce>, crate::error::HarvestError, _>(async |conn| {
            let now = Utc::now();
            let due_sql = "
                SELECT id, workflow_name, debounce_key, workflow_id, queue_name,
                       last_input, start_options, shard_id
                FROM harvest_debounce
                WHERE effective_fire_at <= $1
                ORDER BY effective_fire_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            ";

            let due_rows: Vec<FireDueRow> = diesel::sql_query(due_sql)
                .bind::<diesel::sql_types::Timestamptz, _>(now)
                .bind::<diesel::sql_types::BigInt, _>(DEBOUNCE_FIRE_BATCH_SIZE)
                .load(conn)
                .await
                .map_err(crate::error::database_error)?;

            let mut results = Vec::with_capacity(due_rows.len());
            for row in due_rows {
                if let Some(item) = fire_claimed_debounce_row(conn, row).await? {
                    results.push(item);
                }
            }
            Ok(results)
        }),
    )
    .await?;

    Ok(fired)
}

/// Fire all due debounced starts across every assigned shard.
///
/// Spawns deferred trigger-starts and records the `debounce_fired` metric after
/// each shard's claim transaction commits, and returns the number of executions
/// started.
///
/// **Sharding:** `harvest_debounce` rows are stored on the *debounce-key* shard
/// (`start_workflow` routes them there), so a single-connection scan would never
/// fire rows on non-default shards. This iterates `shard_assignments` against
/// `sharded_pool` (mirroring the per-shard model the outbox scanners use). When
/// no sharded pool is configured (single-shard deployments), it scans the passed
/// default `conn`, which is the only shard.
///
/// Called from [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is spawned.
///
/// # Errors
/// Returns `HarvestError` if a database query or any execution start fails.
#[cfg(feature = "db")]
pub async fn fire_due_debounced_starts(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> crate::error::HarvestResult<usize> {
    // Spawn a shard's fired follow-ups + record metrics, returning the count.
    // Done per-shard immediately after that shard's claim transaction commits, so
    // an error on a *later* shard can never drop an earlier shard's already-
    // committed completion-trigger/parent-close follow-ups (Codex 580).
    async fn spawn_fired(
        fired: Vec<FiredDebounce>,
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
                crate::telemetry::emit_workflow_terminal(
                    metrics,
                    &wf_name,
                    &q_name,
                    crate::telemetry::WorkflowStatus::Cancelled,
                );
            }
            metrics.record_debounce_fired(&workflow_name, &queue_name);
            // issue #618, F1: the debounce scanner relays a start already
            // admitted through the gate at HTTP time; count the deferred fire as
            // an exempt bypass so an operator can see it never slips a gate
            // silently. See `admission_gate::producer_contract`.
            metrics
                .record_admission_bypassed(crate::admission_gate::StartProducer::Debounce.as_str());
        }
        count
    }

    let mut fired_count = 0usize;

    match sharded_pool {
        // Multi-shard: scan each assigned shard's own harvest_debounce table.
        Some(sp) if !shard_assignments.is_empty() => {
            for shard in shard_assignments {
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    continue;
                };
                let mut shard_conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "[debounce] failed to get connection to shard {shard:?}: {e:?}"
                        );
                        continue;
                    }
                };
                // Spawn this shard's results before moving on; on this shard's own
                // error the transaction rolled back, so there is nothing committed
                // to drain and propagating is safe.
                let fired = fire_due_on_conn(&mut shard_conn, Some(metrics)).await?;
                fired_count += spawn_fired(fired, metrics, &mut shard_conn).await;
            }
        }
        // Single-shard / no sharded pool: the passed connection is the only shard.
        _ => {
            let fired = fire_due_on_conn(conn, Some(metrics)).await?;
            fired_count += spawn_fired(fired, metrics, conn).await;
        }
    }

    Ok(fired_count)
}

/// Delete a single pending debounce row by id.
#[cfg(feature = "db")]
async fn delete_debounce_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row_id: uuid::Uuid,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;
    diesel::sql_query("DELETE FROM harvest_debounce WHERE id = $1")
        .bind::<diesel::sql_types::Uuid, _>(row_id)
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Fire one already-claimed (row-locked) debounce row: start the execution and
/// delete the record. Must run inside the caller's transaction so the row lock
/// taken by the claim `SELECT ... FOR UPDATE` is held through the delete.
///
/// Returns `Some((workflow_name, queue_name, deferred_starts))` when a run was
/// started (so the caller can spawn deferred trigger-starts and record the
/// metric after the outer transaction commits), or `None` when the start was a
/// no-op (e.g. `AlreadyExists` under a reject/duplicate reuse policy — the row
/// is deleted so the scanner does not retry the doomed start forever).
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
async fn fire_claimed_debounce_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueRow,
) -> crate::error::HarvestResult<Option<FiredDebounce>> {
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
    let max_execution_timeout_ceiling = opts
        .max_execution_timeout_ceiling_secs
        .and_then(chrono::Duration::try_seconds);
    // Chain-scoped lifetime cap captured at admission (issue #617), so a debounced
    // start of a `#[workflow(chain_execution_timeout = ...)]` workflow does not
    // silently drop the declared cap.
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
    let debounce_key = row.debounce_key;
    let row_id = row.id;
    let owner = opts.owner;
    let runbook_url = opts.runbook_url;
    let severity = opts.severity;
    // Restore the provenance captured at admission (issue #740). A pre-#740 row
    // (no captured source) falls back to `Api`: the debounce admission path is
    // the plain HTTP start route.
    let start_source = opts.start_source.as_deref().map_or(
        crate::types::StartSource::Api,
        crate::types::StartSource::from_str,
    );
    let start_source_ref = opts.start_source_ref;
    let started_by = opts.started_by;

    let params = crate::execution::StartWorkflowParams {
        workflow_name: &workflow_name,
        workflow_id: &workflow_id,
        exec_id,
        input: row.last_input,
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
        schedule_id: None,
        scheduled_for: None,
        workflow_attempt: 1,
        workflow_retry_policy: opts
            .workflow_retry_policy
            .and_then(|v| serde_json::from_value(v).ok()),
        retry_of_exec_id: None,
        max_workflow_attempts_ceiling: opts.max_workflow_attempts_ceiling,
        origin: None,
        completion_callbacks: opts.completion_callbacks,
        start_source,
        start_source_ref: start_source_ref.as_deref(),
        started_by: started_by.as_deref(),
    };

    // `in_outer_transaction = true`: this runs inside the scanner's fire
    // transaction, so a TerminateIfRunning pre-check cancellation is a savepoint
    // that rolls back with this transaction on error — the collect fn must not
    // spawn its follow-ups (they'd be orphaned). Deferred starts returned on
    // success are spawned by the caller only after the fire transaction commits.
    match crate::execution::start_or_load_workflow_execution_collect(
        conn, params, true, false, None, None,
    )
    .await
    {
        Ok((started, deferred_starts, deferred_checks, cancel_metrics)) => {
            delete_debounce_row(conn, row_id).await?;
            tracing::info!(
                workflow_name = %workflow_name,
                debounce_key = %debounce_key,
                exec_id = %started.exec_id,
                queue = %queue_name,
                "debounced start fired",
            );
            Ok(Some((
                started.workflow_name,
                queue_name,
                deferred_starts,
                deferred_checks,
                cancel_metrics,
            )))
        }
        // The target workflow_id is already taken under the reuse policy, so the
        // debounce intent can't produce a new run. Drop the record so the
        // scanner doesn't retry the doomed start on every tick (and block the
        // rest of the batch).
        Err(crate::error::HarvestError::AlreadyExists { .. }) => {
            delete_debounce_row(conn, row_id).await?;
            tracing::warn!(
                workflow_name = %workflow_name,
                debounce_key = %debounce_key,
                workflow_id = %workflow_id,
                "debounced start skipped: workflow_id already exists under reuse policy",
            );
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Parse a persisted reuse-policy string back into its typed form.
///
/// Shared with `throttle.rs`, which persists the same string representation in
/// its own deferred-start options blob (`DebounceStartOptions.reuse_policy`).
#[cfg(feature = "db")]
pub(crate) fn parse_reuse_policy(s: &str) -> Option<crate::types::WorkflowIdReusePolicy> {
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
        for i in 0..20i64 {
            let now = t0 + chrono::Duration::seconds(i * 5);
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
