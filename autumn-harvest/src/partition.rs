//! Opt-in native Postgres declarative partitioning for `harvest_events`
//! (issue #958).
//!
//! # The problem
//!
//! `harvest_events` is one append-only heap with a `BIGSERIAL` primary key, and
//! retention reclaims space through the `ON DELETE CASCADE` on
//! `harvest_workflow_executions`: deleting an expired execution row deletes its
//! event rows one at a time. At sustained volume that is the classic Postgres
//! failure mode — millions of dead tuples per pass, bloating the heap and every
//! index, and driving autovacuum pressure that competes with the non-blocking
//! task-claim query and the append hot path. Dropping a partition is a metadata
//! operation: O(1), no dead tuples, no vacuum debt.
//!
//! # The partition key
//!
//! The key is **not** `harvest_events.timestamp`. Postgres requires the
//! partition key in every `UNIQUE` constraint, so a `timestamp` key would turn
//! `UNIQUE (workflow_exec_id, event_id)` into `UNIQUE (workflow_exec_id,
//! event_id, timestamp)` — silently destroying the per-execution id uniqueness
//! that *is* the engine's optimistic-concurrency detector. `timestamp` is also
//! caller-settable and back-datable by operator tooling, so it is not a safe
//! routing key.
//!
//! The key is a dedicated `cohort` column: the row's **append instant**,
//! floored to a fixed width by a plain column `DEFAULT`. Two properties follow.
//!
//! 1. `cohort` comes from a `DEFAULT`, never from a statement the engine
//!    issues, so `UNIQUE (workflow_exec_id, event_id, cohort)` still rejects
//!    exactly what the old constraint rejected: a second row for the same
//!    `(execution, event_id)`. The concurrency contract is preserved.
//! 2. **Past partitions are sealed.** A cohort's range is a window of wall
//!    clock that has already closed, and the `DEFAULT` can only ever produce a
//!    cohort at or after "now", so no future `INSERT` can route into a
//!    partition whose upper bound is in the past. Once the sweeper proves a
//!    closed partition holds no live execution's rows, nothing can race an
//!    append into it before the drop. The safety argument is structural, not a
//!    lock.
//!
//! ## Why a `DEFAULT` and not a trigger
//!
//! The first iteration of this module stamped `cohort` from the owning
//! execution's `created_at` in a `BEFORE INSERT` trigger, so every event of one
//! execution landed in one partition. **Postgres forbids that.** Tuple routing
//! happens *before* row triggers fire, so a trigger that changes the partition
//! key fails with `moving row to another partition during a BEFORE FOR EACH ROW
//! trigger is not supported` — and, worse, silently succeeds whenever the
//! pre-trigger and post-trigger destinations happen to coincide (both the
//! `DEFAULT` partition, say), which looks like it works. The value has to be
//! present before routing, and for a column the engine's SQL never mentions,
//! that means a `DEFAULT`.
//!
//! That trades whole-execution cohesion for sealed partitions. It is precisely
//! the trade issue #958 anticipates ("an execution's events span time"), and
//! the drop gate is exact about it.
//!
//! # The drop gate
//!
//! A closed partition is droppable when **no row in it belongs to a
//! still-existing execution**. Two tiers answer that:
//!
//! - **Fast path** — `NOT EXISTS (SELECT 1 FROM harvest_workflow_executions
//!   WHERE created_at < upper)`. An execution cannot have appended a row before
//!   it existed, so if nothing predates the partition's upper bound, nothing
//!   that could own a row in it survives. One index probe. This is the steady
//!   state, because retention collects oldest-first.
//! - **Exact path** — only when the fast probe says "maybe": a bounded
//!   semi-join proving no row in the partition has a live owner, under a
//!   `statement_timeout` so one huge partition cannot stall the tick.
//!
//! Legal holds (#747), per-type overrides (#737) and long-running executions
//! need **no special-casing at all**: each keeps its execution row alive, which
//! keeps its rows owned, which blocks the drop. There is no second copy of the
//! retention policy to drift out of sync with the first.
//!
//! # What this costs, honestly
//!
//! - **No foreign key.** The partitioned layout drops
//!   `harvest_events_workflow_exec_id_fkey`, because that FK's `ON DELETE
//!   CASCADE` *is* the delete storm being eliminated. Its insert-time half is
//!   restored by a validate-only trigger (a primary-key probe either way; the
//!   trigger takes no `FOR KEY SHARE` lock, so it is cheaper in lock traffic
//!   than the FK trigger it replaces). What is deliberately *not* restored is
//!   the delete-time cascade: deleting an execution leaves orphan event rows,
//!   and the sweeper is their garbage collector. Orphans are invisible to every
//!   read path in the engine — all of them filter by a `workflow_exec_id` the
//!   caller already resolved.
//! - **Reads do not prune.** History reads filter on `workflow_exec_id`, not on
//!   `cohort`, so each one probes every partition's index. Keep the live
//!   partition count small (retention horizon ÷ cohort width, plus the
//!   lookahead window); `docs/partitioned-events.md` publishes the measured
//!   cost and the sizing rule.
//! - **A long-running execution pins the cohorts it wrote into.** Its
//!   siblings' rows in those cohorts are reclaimed late.
//!   [`SweepOptions::straggler_grace`] opts into a targeted orphan `DELETE` for
//!   that case; it is **off by default**, so the default configuration never
//!   issues a row-level delete against `harvest_events`.
//!
//! # Invisible to Diesel
//!
//! `cohort` is absent from [`crate::schema`] on purpose. Diesel always emits
//! explicit column lists, so a column it does not know about is neither read
//! nor written by any generated statement — every read and write SQL string is
//! byte-for-byte identical in both layouts. AC2 ("per-execution semantics are
//! byte-identical") therefore holds *by construction* rather than by testing
//! luck.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};

#[cfg(feature = "db")]
use diesel::sql_types::{Array, BigInt, Bool, Nullable, Text, Timestamptz, Uuid as SqlUuid};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult, database_error};

// ── Constants ──────────────────────────────────────────────────────────────

/// Default cohort width: one UTC day.
///
/// Sized so a typical retention horizon yields a live partition count in the
/// low tens — small enough that the non-pruning read path stays cheap, large
/// enough that a retention pass drops whole days rather than thousands of
/// slivers.
pub const DEFAULT_COHORT_WIDTH_SECS: i64 = 86_400;

/// How many cohorts beyond the current one the engine keeps pre-created.
///
/// Every live partition costs on the read path (history reads filter on
/// `workflow_exec_id`, not `cohort`, so each one probes every partition), so
/// the window is sized for resilience rather than generosity: at the default
/// hourly tick and daily cohorts, maintenance would have to be down for three
/// days before an append could reach an uncovered cohort — and even then the
/// `DEFAULT` partition catches it rather than failing the append.
pub const DEFAULT_LOOKAHEAD_COHORTS: u32 = 3;

/// Smallest accepted cohort width: one hour, matching the retention janitor's
/// default tick interval.
///
/// Not an arbitrary floor. Coverage created per tick is
/// `width × (lookahead + 1)`, so a width far below the tick interval leaves the
/// write window uncovered for most of every hour and sends every append to the
/// `DEFAULT` partition — whose drain then holds `ACCESS EXCLUSIVE` while it
/// moves an hour of events, which is precisely the append stall this whole
/// change exists to avoid. Narrower widths also multiply the partition count
/// (every non-pruning read probes every partition) without improving
/// reclamation granularity in any way an operator can use.
pub const MIN_COHORT_WIDTH_SECS: i64 = 3_600;

/// Largest accepted cohort width (365 days).
pub const MAX_COHORT_WIDTH_SECS: i64 = 86_400 * 365;

/// The catch-all partition. Always present, normally empty.
///
/// An append whose cohort has no partition lands here instead of failing with
/// `no partition of relation found`, which would stall a live workflow on a
/// maintenance gap.
pub const DEFAULT_PARTITION: &str = "harvest_events_p_default";

/// The pre-cutover partition holding every row that existed before
/// partitioning was enabled.
pub const LEGACY_PARTITION: &str = "harvest_events_legacy";

/// Name prefix shared by every cohort partition.
pub const PARTITION_PREFIX: &str = "harvest_events_p_";

/// Suffix appended to the legacy table's own indexes and constraints so the
/// new parent can reuse their original names.
const LEGACY_RENAME_SUFFIX: &str = "__pre958";

/// The validate-only `BEFORE INSERT` trigger that replaces the FK's
/// insert-time half. It must never modify `NEW`: Postgres rejects a `BEFORE
/// ROW` trigger that changes a partitioned row's destination.
const EXEC_FK_TRIGGER: &str = "harvest_events_exec_fk_trg";

// ── Sweep "blocked" reasons ────────────────────────────────────────────────
//
// Constants, not inline literals, because `docs/partitioned-events.md` explains
// each one to an operator and `partitioned_events_docs.rs` asserts the doc
// covers every one of them. Inline strings would let a reason be reworded here
// and go stale there with nothing failing.

/// Something in the cohort is still retained: a run in flight, a legal hold
/// (#747), a longer per-type override (#737), or a row not yet past its
/// horizon.
pub const OWNED_REASON: &str = "a live execution still owns rows";

/// The exact ownership scan exceeded its `statement_timeout`.
pub const SCAN_BUDGET_REASON: &str = "ownership scan exceeded its budget";

/// The `ACCESS EXCLUSIVE` lock could not be taken in time, or the re-check
/// under it found an owner that appeared after the gate ran.
pub const RECHECK_REASON: &str = "lock not acquired, or an owner appeared before the drop";

/// A partition with no upper bound cannot be closed, so it is never a candidate.
///
/// Structurally impossible for the layouts this module creates; reported rather
/// than silently skipped so a hand-made partition shows up.
pub const UNBOUNDED_REASON: &str = "unbounded upper bound";

/// Every reason [`sweep`] can report. Used by the documentation guard.
pub const SWEEP_REASONS: &[&str] = &[
    OWNED_REASON,
    SCAN_BUDGET_REASON,
    RECHECK_REASON,
    UNBOUNDED_REASON,
];

// ── Cohort algebra (pure) ──────────────────────────────────────────────────

/// Floor `ts` to the start of its cohort.
///
/// Uses `div_euclid`, not `/`: Rust integer division truncates toward zero,
/// which would round a pre-1970 timestamp *up* into the next cohort and route
/// an execution's events to the wrong partition. The SQL side
/// (`harvest_event_cohort`) uses `floor()` for the same reason.
///
/// # Panics
///
/// Never for `width_secs >= 1`; a non-positive width is clamped to 1 second
/// rather than dividing by zero. [`EnableOptions::validate`] rejects such a
/// width long before it can reach the database.
#[must_use]
pub fn cohort_start(ts: DateTime<Utc>, width_secs: i64) -> DateTime<Utc> {
    let width = width_secs.max(1);
    let floored = ts.timestamp().div_euclid(width) * width;
    Utc.timestamp_opt(floored, 0)
        .single()
        .unwrap_or(DateTime::<Utc>::MIN_UTC)
}

/// The partition table name for a cohort start.
///
/// Encodes the cohort's UTC instant so an operator reading `\dt
/// harvest_events*` can tell at a glance what each partition holds. Unique for
/// any width down to one second.
#[must_use]
pub fn partition_name(cohort_start: DateTime<Utc>) -> String {
    format!("{PARTITION_PREFIX}{}", cohort_start.format("%Y%m%d%H%M%S"))
}

// ── Configuration ──────────────────────────────────────────────────────────

/// The physical layout of `harvest_events` on a given shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "layout")]
#[non_exhaustive]
pub enum EventLayout {
    /// The stock ordinary table. Every pre-#958 deployment, until an operator
    /// opts in.
    Unpartitioned,
    /// Native range partitioning on `cohort`, with the width the operator chose
    /// when enabling.
    Partitioned {
        /// Cohort width in seconds, read back from the deployed
        /// `harvest_event_cohort` function.
        cohort_width_secs: i64,
    },
}

impl EventLayout {
    /// Whether this layout reclaims by partition drop.
    #[must_use]
    pub const fn is_partitioned(&self) -> bool {
        matches!(self, Self::Partitioned { .. })
    }

    /// The cohort width, or the default for an unpartitioned deployment.
    #[must_use]
    pub const fn cohort_width_secs(&self) -> i64 {
        match self {
            Self::Unpartitioned => DEFAULT_COHORT_WIDTH_SECS,
            Self::Partitioned { cohort_width_secs } => *cohort_width_secs,
        }
    }
}

/// Options for converting a shard to the partitioned layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnableOptions {
    /// Cohort width in seconds. Governs both reclamation granularity and the
    /// live partition count (`retention horizon / width + lookahead`).
    pub cohort_width_secs: i64,
    /// How many cohorts ahead of "now" to pre-create.
    pub lookahead_cohorts: u32,
    /// How long to wait for the `ACCESS EXCLUSIVE` lock on `harvest_events`
    /// during the swap before giving up. Failing fast is correct: a conversion
    /// that queues behind a long transaction would block every append behind
    /// it.
    pub lock_timeout: Duration,
}

impl Default for EnableOptions {
    fn default() -> Self {
        Self {
            cohort_width_secs: DEFAULT_COHORT_WIDTH_SECS,
            lookahead_cohorts: DEFAULT_LOOKAHEAD_COHORTS,
            lock_timeout: Duration::from_secs(5),
        }
    }
}

impl EnableOptions {
    /// Reject configurations that cannot produce a working layout.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when the cohort width is outside
    /// [`MIN_COHORT_WIDTH_SECS`]..=[`MAX_COHORT_WIDTH_SECS`], or when the
    /// lookahead is zero (which would leave every append landing in the
    /// `DEFAULT` partition).
    pub fn validate(&self) -> crate::error::HarvestResult<()> {
        use crate::error::HarvestError;
        if self.cohort_width_secs < MIN_COHORT_WIDTH_SECS
            || self.cohort_width_secs > MAX_COHORT_WIDTH_SECS
        {
            return Err(HarvestError::Config(format!(
                "cohort width {}s is outside the supported range \
                 {MIN_COHORT_WIDTH_SECS}s..={MAX_COHORT_WIDTH_SECS}s",
                self.cohort_width_secs
            )));
        }
        if self.lookahead_cohorts == 0 {
            return Err(HarvestError::Config(
                "lookahead_cohorts must be at least 1; with no lookahead every \
                 append lands in the DEFAULT partition and reclamation stalls"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Which conversion path [`enable_partitioning`] took.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
#[non_exhaustive]
pub enum EnableMode {
    /// `harvest_events` was already partitioned; nothing changed.
    AlreadyPartitioned,
    /// The table was empty, so it was recreated as partitioned outright.
    /// Instant, no data movement.
    Fresh,
    /// The table had rows, so it was attached whole as the pre-cutover
    /// partition. No row is copied or rewritten.
    AttachLegacy {
        /// Exclusive upper bound of the legacy partition. Every execution that
        /// existed at conversion time has a cohort strictly below this, so all
        /// of its events — including ones appended *after* the conversion —
        /// stay in the legacy partition together.
        cutover: DateTime<Utc>,
    },
}

/// What [`enable_partitioning`] did.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EnableReport {
    /// The conversion path taken.
    pub mode: EnableMode,
    /// Cohort partitions created for the lookahead window.
    pub partitions_created: Vec<String>,
    /// The active cohort width after conversion.
    pub cohort_width_secs: i64,
}

/// One partition of `harvest_events`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct PartitionInfo {
    /// Table name.
    pub name: String,
    /// Inclusive lower bound; `None` for `MINVALUE` (the legacy partition) and
    /// for the `DEFAULT` partition.
    pub lower: Option<DateTime<Utc>>,
    /// Exclusive upper bound; `None` for `MAXVALUE` and for `DEFAULT`.
    pub upper: Option<DateTime<Utc>>,
    /// Whether this is the catch-all `DEFAULT` partition.
    pub is_default: bool,
}

/// Tuning for one sweep pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepOptions {
    /// Maximum partitions to drop in one pass.
    ///
    /// Each drop takes a brief `ACCESS EXCLUSIVE` lock on the parent, so an
    /// unbounded pass could hold the append path off for as long as it takes to
    /// drop a backlog. Bounded passes converge over successive ticks instead.
    pub max_drops: usize,
    /// How long to wait for that lock before giving up on a partition.
    ///
    /// Failing fast and retrying next tick is what keeps the concurrent-p99
    /// budget: a sweep must never queue behind a long-running transaction while
    /// every append queues behind the sweep.
    pub lock_timeout: Duration,
    /// Opt-in targeted `DELETE` of orphan rows in a cohort that a straggler
    /// execution has pinned for longer than this.
    ///
    /// `None` (the default) means the sweeper issues **zero** row-level deletes
    /// against `harvest_events` — the strongest reading of AC3. Set it when a
    /// deployment has long-lived executions whose cohorts would otherwise pin
    /// their siblings' rows indefinitely.
    pub straggler_grace: Option<Duration>,
    /// Rows per straggler `DELETE` statement, so a straggler pass cannot open
    /// an unbounded transaction.
    pub straggler_batch: usize,
    /// How many surviving old executions the narrow ownership probe will
    /// enumerate before falling back to the exact scan.
    ///
    /// The narrow probe asks "do any of THESE executions have a row here?",
    /// one index probe each, which is what keeps a single legal hold or
    /// long-running execution from forcing a full ownership scan of every
    /// closed partition on every tick. Above this many survivors the scan is
    /// cheaper than the probes.
    pub owner_probe_cap: usize,
    /// Budget for the exact ownership scan that runs only when the cheap
    /// `created_at` probe cannot decide.
    ///
    /// Enforced as a `statement_timeout`. Exceeding it reports the partition as
    /// blocked and retries next tick — the fail-safe direction for a janitor:
    /// an unfinished proof of "nothing lives here" is not a proof.
    pub exact_scan_timeout: Duration,
}

impl Default for SweepOptions {
    fn default() -> Self {
        Self {
            max_drops: 32,
            lock_timeout: Duration::from_secs(2),
            straggler_grace: None,
            straggler_batch: 1_000,
            owner_probe_cap: 1_000,
            exact_scan_timeout: Duration::from_secs(15),
        }
    }
}

/// What one sweep pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SweepOutcome {
    /// Partitions dropped, oldest first.
    pub dropped: Vec<String>,
    /// Partitions considered but left in place, each with the reason — a live
    /// execution still owns rows there, or the lock could not be taken in time.
    /// Reported rather than silently skipped so an operator can see *why*
    /// space is not coming back.
    pub blocked: Vec<String>,
    /// Orphan rows removed by the opt-in straggler fallback.
    pub straggler_rows_deleted: usize,
}

// ── Layout detection ───────────────────────────────────────────────────────

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct TextRow {
    #[diesel(sql_type = Text)]
    v: String,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct UuidRow {
    #[diesel(sql_type = SqlUuid)]
    id: uuid::Uuid,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct BoolRow {
    #[diesel(sql_type = Bool)]
    v: bool,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct TsRow {
    #[diesel(sql_type = Nullable<Timestamptz>)]
    v: Option<DateTime<Utc>>,
}

/// Report the current `harvest_events` layout for this shard.
///
/// Shard-local by construction: a shard is a database, and this reads that
/// database's own catalog.
///
/// # Errors
///
/// [`HarvestError::Database`] if the catalog query fails.
#[cfg(feature = "db")]
pub async fn detect_layout(conn: &mut AsyncPgConnection) -> HarvestResult<EventLayout> {
    let relkind = diesel::sql_query(
        "SELECT c.relkind::text AS v
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()",
    )
    .get_result::<TextRow>(conn)
    .await
    .map_err(database_error)?;

    if relkind.v != "p" {
        return Ok(EventLayout::Unpartitioned);
    }
    Ok(EventLayout::Partitioned {
        cohort_width_secs: deployed_cohort_width(conn).await?,
    })
}

/// Read the cohort width back out of the deployed `harvest_event_cohort`
/// function rather than trusting a caller-supplied value.
///
/// The function body is the single source of truth: it is what actually stamps
/// rows, so a mismatch between it and any config value would tear an
/// execution's history across partitions. Deriving the width from it makes that
/// impossible.
#[cfg(feature = "db")]
async fn deployed_cohort_width(conn: &mut AsyncPgConnection) -> HarvestResult<i64> {
    let body = diesel::sql_query(
        "SELECT pg_get_functiondef(p.oid) AS v
           FROM pg_proc p
           JOIN pg_namespace n ON n.oid = p.pronamespace
          WHERE p.proname = 'harvest_event_cohort' AND n.nspname = current_schema()",
    )
    .get_result::<TextRow>(conn)
    .await
    .map_err(database_error)?;

    parse_cohort_width(&body.v).ok_or_else(|| {
        HarvestError::Database(
            "harvest_event_cohort() does not have the expected epoch-floor body; \
             the partition layout cannot be trusted"
                .to_string(),
        )
    })
}

/// Extract the baked-in width literal from a `harvest_event_cohort` definition.
///
/// Pure so it can be unit-tested without a database.
#[must_use]
pub fn parse_cohort_width(function_def: &str) -> Option<i64> {
    // ... floor(extract(epoch FROM $1) / 86400) * 86400 ...
    let after = function_def.split("epoch FROM $1) / ").nth(1)?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse::<i64>().ok().filter(|w| *w > 0)
}

// ── Bound parsing ──────────────────────────────────────────────────────────

/// Parse a `pg_get_expr(relpartbound, …)` expression into `(lower, upper,
/// is_default)`.
///
/// Postgres renders these as `DEFAULT` or
/// `FOR VALUES FROM (MINVALUE) TO ('2026-08-31 00:00:00+00')`. Pure, so the
/// parsing is unit-tested rather than only exercised through a live catalog.
#[must_use]
pub fn parse_partition_bound(expr: &str) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool) {
    let expr = expr.trim();
    if expr.eq_ignore_ascii_case("DEFAULT") {
        return (None, None, true);
    }
    let lower = between(expr, "FROM (", ")").and_then(parse_bound_literal);
    let upper = between(expr, "TO (", ")").and_then(parse_bound_literal);
    (lower, upper, false)
}

fn between<'a>(haystack: &'a str, open: &str, close: &str) -> Option<&'a str> {
    let start = haystack.find(open)? + open.len();
    let rest = &haystack[start..];
    let end = rest.find(close)?;
    Some(&rest[..end])
}

/// `MINVALUE`/`MAXVALUE` become `None`; a quoted timestamp is parsed.
fn parse_bound_literal(raw: &str) -> Option<DateTime<Utc>> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("MINVALUE") || raw.eq_ignore_ascii_case("MAXVALUE") {
        return None;
    }
    let inner = raw.trim_matches('\'').trim();
    if inner.eq_ignore_ascii_case("-infinity") || inner.eq_ignore_ascii_case("infinity") {
        return None;
    }
    for fmt in [
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%:z",
    ] {
        if let Ok(dt) = DateTime::parse_from_str(inner, fmt) {
            return Some(dt.with_timezone(&Utc));
        }
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(inner, fmt) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }
    None
}

// ── Catalog helpers ────────────────────────────────────────────────────────

/// List every partition of `harvest_events`, oldest cohort first.
///
/// Returns an empty vector on an unpartitioned deployment.
///
/// # Errors
///
/// [`HarvestError::Database`] if the catalog query fails.
#[cfg(feature = "db")]
pub async fn list_partitions(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<PartitionInfo>> {
    #[derive(diesel::QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        name: String,
        #[diesel(sql_type = Text)]
        bound: String,
    }

    let rows = diesel::sql_query(
        "SELECT child.relname AS name,
                pg_get_expr(child.relpartbound, child.oid) AS bound
           FROM pg_inherits i
           JOIN pg_class parent ON parent.oid = i.inhparent
           JOIN pg_class child  ON child.oid  = i.inhrelid
           JOIN pg_namespace n  ON n.oid = parent.relnamespace
          WHERE parent.relname = 'harvest_events' AND n.nspname = current_schema()",
    )
    .load::<Row>(conn)
    .await
    .map_err(database_error)?;

    let mut out: Vec<PartitionInfo> = rows
        .into_iter()
        .map(|r| {
            let (lower, upper, is_default) = parse_partition_bound(&r.bound);
            PartitionInfo {
                name: r.name,
                lower,
                upper,
                is_default,
            }
        })
        .collect();
    out.sort_by(compare_partitions);
    Ok(out)
}

// ── Partition creation ─────────────────────────────────────────────────────

/// Sweep order: oldest closed cohort first, with the `DEFAULT` partition last.
///
/// `DEFAULT` sorting last is load-bearing, not cosmetic: it is the one
/// partition that must never be dropped (an append whose cohort has no
/// partition lands there instead of failing), so a bounded sweep must never
/// spend its budget reaching it.
///
/// Extracted so [`list_partitions`] and its unit test share one comparator —
/// a test that re-implements the ordering it is meant to guard tests the copy.
fn compare_partitions(a: &PartitionInfo, b: &PartitionInfo) -> std::cmp::Ordering {
    a.is_default
        .cmp(&b.is_default)
        .then(a.upper.cmp(&b.upper))
        .then(a.name.cmp(&b.name))
}

/// Format a timestamp as a SQL literal.
///
/// The only value interpolated into partition DDL. RFC 3339 output from
/// `chrono` cannot contain a quote, so this cannot be an injection vector, but
/// the value is still emitted through one audited helper rather than ad hoc at
/// each call site.
fn ts_literal(ts: DateTime<Utc>) -> String {
    format!("'{}'", ts.to_rfc3339())
}

/// Create the partition covering `ts`'s cohort if it does not already exist.
///
/// Idempotent and safe to race: a concurrent creator that wins is treated as
/// success. Returns the partition name, and whether this call created it.
///
/// # Errors
///
/// [`HarvestError::Database`] on any failure other than a benign
/// already-exists/overlap race.
#[cfg(feature = "db")]
#[doc(hidden)]
pub async fn ensure_cohort(
    conn: &mut AsyncPgConnection,
    ts: DateTime<Utc>,
) -> HarvestResult<(String, bool)> {
    let width = match detect_layout(conn).await? {
        EventLayout::Unpartitioned => return Ok((String::new(), false)),
        EventLayout::Partitioned { cohort_width_secs } => cohort_width_secs,
    };
    ensure_cohort_with_width(conn, ts, width).await
}

#[cfg(feature = "db")]
async fn ensure_cohort_with_width(
    conn: &mut AsyncPgConnection,
    ts: DateTime<Utc>,
    width: i64,
) -> HarvestResult<(String, bool)> {
    let lower = cohort_start(ts, width);
    let Some(upper) = lower.checked_add_signed(chrono::Duration::seconds(width)) else {
        return Err(HarvestError::Database(format!(
            "cohort starting at {lower} overflows when advanced by {width}s"
        )));
    };
    let name = partition_name(lower);
    // Probed first, so `was_created` means what it says. `CREATE TABLE IF NOT
    // EXISTS … PARTITION OF` is a successful NO-OP when a relation of that name
    // already exists, so a bare `Ok` arm would report every already-covered
    // cohort as newly created on every single maintenance tick — and would
    // report a colliding unrelated table as coverage while appends kept piling
    // into the DEFAULT partition.
    if cohort_partition_is_attached(conn, &name, lower, upper).await? {
        return Ok((name, false));
    }
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {name} PARTITION OF harvest_events \
         FOR VALUES FROM ({}) TO ({})",
        ts_literal(lower),
        ts_literal(upper)
    );
    match diesel::sql_query(&sql).execute(conn).await {
        // Verified rather than assumed, for the same reason: the statement can
        // succeed without having created the partition we asked for.
        Ok(_) => {
            if cohort_partition_is_attached(conn, &name, lower, upper).await? {
                Ok((name, true))
            } else {
                Err(HarvestError::Database(format!(
                    "cohort {lower} is not covered: creating `{name}` reported success but it \
                     is not attached to harvest_events with the expected bounds (a relation of \
                     that name already exists). Appends for this cohort will land in \
                     {DEFAULT_PARTITION}."
                )))
            }
        }
        // A race is only benign if the partition that now exists is the one we
        // wanted. `CREATE TABLE IF NOT EXISTS <name> PARTITION OF …` is a
        // silent no-op when `<name>` exists as an unrelated relation, and the
        // overlap arm can fire on a genuine bounds mismatch (e.g. after the
        // cohort width was changed under a live grid) — both would otherwise
        // report the cohort as covered when it is not, and the next append
        // would land in the DEFAULT partition with nothing said.
        Err(e) if is_benign_partition_race(&e) => {
            if cohort_partition_is_attached(conn, &name, lower, upper).await? {
                Ok((name, false))
            } else {
                Err(HarvestError::Database(format!(
                    "cohort {lower} is not covered: `{name}` exists but is not attached to \
                     harvest_events with the expected bounds. underlying error: {e}"
                )))
            }
        }
        // The DEFAULT partition already holds rows for this range, so Postgres
        // refuses to carve it out. Named explicitly because the raw message
        // ("updated partition constraint for default partition ... would be
        // violated by some row") tells an operator nothing about the remedy,
        // which is a drain — something `maintain` does automatically before it
        // ever gets here.
        Err(e) if is_default_partition_conflict(&e) => Err(HarvestError::Database(format!(
            "cannot create {name}: the DEFAULT partition holds rows for its range; \
             drain it first (partition::drain_default, or `harvest partition maintain`). \
             underlying error: {e}"
        ))),
        Err(e) => Err(database_error(e)),
    }
}

/// Classifying the Postgres errors this module's fail-safe paths depend on.
///
/// Three behaviours hinge on recognising a specific error — "a concurrent
/// maintainer won the race", "the exact scan blew its budget, retain", "the
/// lock was not available, retry next tick" — and getting one wrong turns a
/// benign path into a hard error that then disappears into a best-effort
/// warning while reclamation quietly stops.
///
/// Diesel's [`DatabaseErrorKind`](diesel::result::DatabaseErrorKind) is used
/// wherever it distinguishes the case, because it is locale-independent. It
/// does not cover `duplicate_table`, `lock_not_available` or `query_canceled`,
/// so those fall back to matching Postgres's message — the same idiom
/// `error.rs` already uses for constraint names. That fallback IS
/// locale-sensitive: on a server with a non-English `lc_messages` these
/// degrade to "treat as a hard error", which is the safe direction (the pass
/// fails loudly and retries) but noisier than it should be. `lc_messages` is a
/// superuser-only GUC, so it cannot be pinned per transaction from here.
///
/// A concurrent maintainer created the same partition first. Benign either
/// way: the partition this call wanted now exists, which is the outcome it
/// wanted — and the caller verifies the bounds before believing it.
#[cfg(feature = "db")]
fn is_benign_partition_race(e: &diesel::result::Error) -> bool {
    let msg = e.to_string();
    msg.contains("already exists")
        || msg.contains("would overlap")
        || msg.contains("overlaps with existing")
        || msg.contains("42P07")
        || msg.contains("23P01")
}

/// The `DEFAULT` partition already holds rows for the range being carved out.
///
/// Postgres reports the default partition's updated partition constraint as a
/// check violation, which Diesel does classify.
#[cfg(feature = "db")]
fn is_default_partition_conflict(e: &diesel::result::Error) -> bool {
    matches!(
        e,
        diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::CheckViolation, _)
    ) || e.to_string().contains("default partition")
}

/// `lock_timeout` fired (SQLSTATE `55P03`).
#[must_use]
pub fn is_lock_timeout(msg: &str) -> bool {
    msg.contains("55P03") || msg.contains("lock timeout")
}

/// `statement_timeout` fired (SQLSTATE `57014`).
#[must_use]
pub fn is_statement_timeout(msg: &str) -> bool {
    msg.contains("57014") || msg.contains("statement timeout")
}

/// Is `name` attached to `harvest_events` with exactly these bounds?
#[cfg(feature = "db")]
async fn cohort_partition_is_attached(
    conn: &mut AsyncPgConnection,
    name: &str,
    lower: DateTime<Utc>,
    upper: DateTime<Utc>,
) -> HarvestResult<bool> {
    Ok(list_partitions(conn)
        .await?
        .into_iter()
        .any(|p| p.name == name && p.lower == Some(lower) && p.upper == Some(upper)))
}

/// Ensure every cohort from now through the lookahead window exists.
///
/// This is what makes AC8's "no operator cron required" true: the retention
/// runtime calls it every tick and at startup.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog or DDL failure.
#[cfg(feature = "db")]
pub async fn ensure_partitions(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    lookahead_cohorts: u32,
) -> HarvestResult<Vec<String>> {
    let width = match detect_layout(conn).await? {
        EventLayout::Unpartitioned => return Ok(Vec::new()),
        EventLayout::Partitioned { cohort_width_secs } => cohort_width_secs,
    };
    let mut created = Vec::new();
    for step in 0..=i64::from(lookahead_cohorts) {
        let Some(at) = now.checked_add_signed(chrono::Duration::seconds(width * step)) else {
            break;
        };
        let (name, was_created) = ensure_cohort_with_width(conn, at, width).await?;
        if was_created {
            created.push(name);
        }
    }
    Ok(created)
}

// ── Enabling the layout ────────────────────────────────────────────────────

/// Convert this shard's `harvest_events` to the partitioned layout.
///
/// Idempotent: on an already-partitioned shard it reports
/// [`EnableMode::AlreadyPartitioned`] and changes nothing.
///
/// Two paths, chosen by whether the table has rows:
///
/// - **[`EnableMode::Fresh`]** — empty table, recreated as partitioned. Instant.
/// - **[`EnableMode::AttachLegacy`]** — the existing table is attached *whole*
///   as the pre-cutover partition. No row is copied, moved or rewritten; the
///   `cohort` column already carries its `-infinity` sentinel from the
///   migration's metadata-only `ADD COLUMN … DEFAULT`.
///
/// The whole conversion runs in ONE transaction under a bounded `lock_timeout`,
/// so a failure leaves the deployment exactly as it was. For a table large
/// enough that the in-transaction index builds and constraint validation would
/// hold that lock too long, use [`migration_plan`] instead: it emits the same
/// algorithm with the expensive steps moved outside the lock window
/// (`CREATE INDEX CONCURRENTLY`, `NOT VALID` + `VALIDATE CONSTRAINT`).
///
/// # Errors
///
/// [`HarvestError::Config`] for invalid options; [`HarvestError::Database`] if
/// any step fails — including the lock timeout, which is the designed outcome
/// when a long transaction is holding `harvest_events`.
#[cfg(feature = "db")]
pub async fn enable_partitioning(
    conn: &mut AsyncPgConnection,
    opts: &EnableOptions,
) -> HarvestResult<EnableReport> {
    opts.validate()?;
    if let EventLayout::Partitioned { cohort_width_secs } = detect_layout(conn).await? {
        return Ok(EnableReport {
            mode: EnableMode::AlreadyPartitioned,
            partitions_created: Vec::new(),
            cohort_width_secs,
        });
    }

    let width = opts.cohort_width_secs;
    let now = Utc::now();

    // `enable_sql` is the single implementation of the conversion (the test
    // harness feeds the very same string to a container's init SQL), so there
    // is no second copy of these steps here to drift out of sync with it.
    //
    // Sent through `batch_execute`, not `sql_query`: the script is two
    // statements (the cohort function, then the `DO` block), and Postgres's
    // extended protocol — which `sql_query` uses — rejects multiple commands in
    // one prepared statement. `batch_execute` uses the simple query protocol,
    // where a multi-statement string runs as ONE implicit transaction, so the
    // atomicity the conversion needs is preserved: a failure anywhere rolls the
    // whole script back and leaves the deployment exactly as it was.
    diesel_async::SimpleAsyncConnection::batch_execute(conn, &enable_sql(opts))
        .await
        .map_err(|e| HarvestError::Database(format!("partition enable script failed: {e}")))?;

    // Report which path the script took by reading the catalog it produced,
    // rather than by predicting it: whether the table had rows is the script's
    // decision, made under the lock it holds.
    let mode = match list_partitions(conn)
        .await?
        .into_iter()
        .find(|p| p.name == LEGACY_PARTITION)
    {
        Some(legacy) => EnableMode::AttachLegacy {
            cutover: legacy.upper.unwrap_or(now),
        },
        None => EnableMode::Fresh,
    };

    let partitions_created = ensure_partitions(conn, now, opts.lookahead_cohorts).await?;
    Ok(EnableReport {
        mode,
        partitions_created,
        cohort_width_secs: width,
    })
}

/// The complete, self-contained SQL that converts a fresh or small
/// `harvest_events` to the partitioned layout.
///
/// This is the **single implementation** of the conversion.
/// [`enable_partitioning`] executes exactly this script and then reads the
/// resulting catalog to report which path it took, and the test harness feeds
/// the same string to a container's `init_sql` so the entire existing DB test
/// corpus can be re-run against the partitioned layout by setting one
/// environment variable (issue #958, AC2). There is deliberately no second copy
/// of these steps in Rust to drift out of sync with this one.
///
/// It introspects rather than hard-codes: the index set is read from the
/// catalog and replayed, so a later migration that adds an index to
/// `harvest_events` is carried onto the partitioned parent with no change here.
///
/// Idempotent — a second run against an already-partitioned table returns
/// immediately.
///
/// For a table large enough that the in-transaction index builds and constraint
/// validation would hold `ACCESS EXCLUSIVE` too long, use [`migration_plan`]
/// instead: the same algorithm with the expensive steps moved out of the lock
/// window.
// One `format!` of a SQL script. Splitting it to satisfy a line budget would
// scatter a single readable runbook across helpers that only ever concatenate.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn enable_sql(opts: &EnableOptions) -> String {
    let width = opts.cohort_width_secs.max(1);
    let lookahead = opts.lookahead_cohorts;
    let lock_ms = opts.lock_timeout.as_millis().max(1);
    let cohort_fn = cohort_function_sql(width);
    let suffix_len = LEGACY_RENAME_SUFFIX.len();
    format!(
        r#"-- Issue #958: convert harvest_events to the partitioned layout.
-- Generated by autumn_harvest::partition::enable_sql(); safe to re-run.
DO $harvest_enable_958$
DECLARE
    width_secs  bigint := {width};
    lookahead   int    := {lookahead};
    idx_defs    text[];
    idx_def     text;
    obj         record;
    cutover     timestamptz;
    lo          timestamptz;
    hi          timestamptz;
    step        int;
    had_rows    boolean;
BEGIN
    -- Idempotent: already partitioned, nothing to do.
    IF (SELECT c.relkind
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()) = 'p'
    THEN
        RETURN;
    END IF;

    -- Fail fast rather than queue: a conversion that cannot get the lock must
    -- not leave every append waiting behind it.
    EXECUTE 'SET LOCAL lock_timeout = ' || quote_literal('{lock_ms}ms');

    -- Deliberately INSIDE the idempotency guard above. Replacing the cohort
    -- function is what changes the partition grid, so running it before the
    -- guard would let `enable --cohort-width-secs X` on an ALREADY-partitioned
    -- shard silently re-cut the grid: `detect_layout` would read back the new
    -- width while every existing partition still had the old bounds, and the
    -- next `ensure_partitions` would get `would overlap` on every cohort --
    -- swallowed as a benign race, so nothing created and nothing reported.
    EXECUTE $harvest_cohort_def${cohort_fn}$harvest_cohort_def$;

    -- Captured BEFORE the rename, so each definition still names
    -- `harvest_events` and replays verbatim onto the new parent, where Postgres
    -- propagates it to every partition. Constraint-backed indexes are excluded:
    -- their replacements must include the partition key and are added below.
    SELECT coalesce(array_agg(pg_get_indexdef(i.indexrelid)), ARRAY[]::text[])
      INTO idx_defs
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()
       AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid);

    -- The legacy partition covers everything below the current cohort. Every
    -- pre-conversion row carries the migration's `-infinity` sentinel, so all
    -- of them fall inside it with no row touched; every row appended from here
    -- takes the new DEFAULT, whose value is at or after this instant. The two
    -- ranges meet exactly, with no gap and no overlap -- and because wall clock
    -- only advances, no later append can route back into legacy. The legacy
    -- partition is sealed from the moment it is attached.
    cutover := harvest_event_cohort(now());

    -- Detached so the sequence survives a DROP of an empty legacy table and can
    -- be re-owned by the new parent. The BIGSERIAL cursor must stay continuous
    -- across the conversion: reusing an id already present in the legacy
    -- partition would violate the parent's primary key.
    EXECUTE 'ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE';
    EXECUTE 'ALTER TABLE harvest_events RENAME TO {LEGACY_PARTITION}';

    -- Probed only AFTER the rename, which is the first statement to take
    -- ACCESS EXCLUSIVE. A `SELECT EXISTS` before it takes only ACCESS SHARE,
    -- which does not conflict with a concurrent INSERT: a workflow that started
    -- and wrote its first events in the gap would commit before the rename,
    -- `had_rows` would still be false, and the ELSE branch below would DROP the
    -- table containing them. Enabling on a live-but-currently-empty shard is
    -- exactly the recommended rollout, so that window is the common case.
    EXECUTE 'SELECT EXISTS (SELECT 1 FROM {LEGACY_PARTITION})' INTO had_rows;

    -- Renaming a table renames neither its indexes nor its constraints, so
    -- without this the new parent could not reclaim their schema-scoped names.
    FOR obj IN SELECT conname AS n FROM pg_constraint
                WHERE conrelid = '{LEGACY_PARTITION}'::regclass
                  AND right(conname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'
    LOOP
        EXECUTE format('ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT %I TO %I',
                       obj.n, obj.n || '{LEGACY_RENAME_SUFFIX}');
    END LOOP;
    FOR obj IN SELECT indexname AS n FROM pg_indexes
                WHERE schemaname = current_schema() AND tablename = '{LEGACY_PARTITION}'
                  AND right(indexname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'
    LOOP
        EXECUTE format('ALTER INDEX %I RENAME TO %I', obj.n, obj.n || '{LEGACY_RENAME_SUFFIX}');
    END LOOP;

    -- `LIKE ... INCLUDING DEFAULTS` copies the columns, their NOT NULLs and the
    -- `nextval(...)` id default -- and keeps working when a later migration
    -- adds a column, rather than pinning a hand-written column list that would
    -- silently drop it. Foreign keys are NOT copied, which is the point: the
    -- FK's ON DELETE CASCADE is the row-by-row delete storm being eliminated.
    EXECUTE 'CREATE TABLE harvest_events '
         || '(LIKE {LEGACY_PARTITION} INCLUDING DEFAULTS INCLUDING COMMENTS INCLUDING STORAGE) '
         || 'PARTITION BY RANGE (cohort)';

    -- Swap the migration's constant `-infinity` sentinel for the live cohort
    -- expression. Metadata-only, and it is what actually routes every
    -- subsequent append: the engine's INSERT statements never mention `cohort`,
    -- so the DEFAULT is always what Postgres uses to pick the partition --
    -- before any row trigger could run.
    --
    -- `clock_timestamp()`, NOT `now()`. `now()` is transaction START time, so a
    -- transaction that began before a cohort boundary and inserts after it
    -- would stamp the PREVIOUS, already-closed cohort -- contradicting the
    -- sealed-partition argument this design rests on, and letting a row appear
    -- in a partition the sweeper has already proved empty.
    -- `append_events_offloaded` holds a transaction across unbounded payload
    -- uploads, so that gap is not hypothetical.
    EXECUTE 'ALTER TABLE harvest_events '
         || 'ALTER COLUMN cohort SET DEFAULT harvest_event_cohort(clock_timestamp())';

    -- Both constraints gain `cohort` because Postgres requires the partition
    -- key in every unique constraint. The second still enforces exactly "one
    -- row per (execution, event_id)": `cohort` comes from a DEFAULT that the
    -- engine never overrides, so it cannot be used to slip a duplicate past it.
    EXECUTE 'ALTER TABLE harvest_events '
         || 'ADD CONSTRAINT harvest_events_pkey PRIMARY KEY (id, cohort)';
    EXECUTE 'ALTER TABLE harvest_events '
         || 'ADD CONSTRAINT harvest_events_workflow_exec_id_event_id_key '
         || 'UNIQUE (workflow_exec_id, event_id, cohort)';

    FOREACH idx_def IN ARRAY idx_defs LOOP
        EXECUTE idx_def;
    END LOOP;

    EXECUTE 'ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id';

    -- Restores the insert-time half of the FK the partitioned layout cannot
    -- keep. Validate-only: routing has already happened by the time a row
    -- trigger fires, so a trigger that touched the partition key would be
    -- rejected by Postgres outright.
    EXECUTE 'CREATE TRIGGER {EXEC_FK_TRIGGER} BEFORE INSERT ON harvest_events '
         || 'FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()';

    -- The catch-all, created before any cohort partition so there is never an
    -- instant in which an append could find no partition at all.
    EXECUTE 'CREATE TABLE {DEFAULT_PARTITION} PARTITION OF harvest_events DEFAULT';

    IF had_rows THEN
        -- ATTACH propagates the parent's PRIMARY KEY onto the partition, and a
        -- table may have only one, so the old single-column key must go. `id`
        -- alone can no longer be a key anyway: uniqueness on a partitioned
        -- table has to include the partition column. Global `id` uniqueness is
        -- not lost -- one sequence still feeds every partition.
        EXECUTE 'ALTER TABLE {LEGACY_PARTITION} DROP CONSTRAINT IF EXISTS '
             || 'harvest_events_workflow_exec_id_fkey{LEGACY_RENAME_SUFFIX}';
        EXECUTE 'ALTER TABLE {LEGACY_PARTITION} DROP CONSTRAINT IF EXISTS '
             || 'harvest_events_pkey{LEGACY_RENAME_SUFFIX}';
        EXECUTE 'ALTER TABLE {LEGACY_PARTITION} DROP CONSTRAINT IF EXISTS '
             || 'harvest_events_workflow_exec_id_event_id_key{LEGACY_RENAME_SUFFIX}';
        EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS {LEGACY_PARTITION}_pk_idx '
             || 'ON {LEGACY_PARTITION} (id, cohort)';
        EXECUTE 'CREATE UNIQUE INDEX IF NOT EXISTS {LEGACY_PARTITION}_exec_event_idx '
             || 'ON {LEGACY_PARTITION} (workflow_exec_id, event_id, cohort)';
        -- The NOT VALID + VALIDATE pair is what lets ATTACH skip its own
        -- full-table verification scan. In one transaction here (fresh/small
        -- scale); migration_plan() splits it out so the validation scan runs
        -- under SHARE UPDATE EXCLUSIVE on a large live table.
        EXECUTE format(
            'ALTER TABLE {LEGACY_PARTITION} ADD CONSTRAINT {LEGACY_PARTITION}_cohort_ck '
            || 'CHECK (cohort < %L) NOT VALID', cutover);
        EXECUTE 'ALTER TABLE {LEGACY_PARTITION} '
             || 'VALIDATE CONSTRAINT {LEGACY_PARTITION}_cohort_ck';
        EXECUTE format(
            'ALTER TABLE harvest_events ATTACH PARTITION {LEGACY_PARTITION} '
            || 'FOR VALUES FROM (MINVALUE) TO (%L)', cutover);
    ELSE
        EXECUTE 'DROP TABLE {LEGACY_PARTITION}';
    END IF;

    -- Pre-create the lookahead window so the engine starts covered. Retention
    -- maintenance extends it every tick from here; no operator cron is needed.
    FOR step IN 0..lookahead LOOP
        lo := harvest_event_cohort(now() + (step * width_secs) * interval '1 second');
        hi := lo + (width_secs * interval '1 second');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF harvest_events '
            || 'FOR VALUES FROM (%L) TO (%L)',
            '{PARTITION_PREFIX}' || to_char(lo AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS'),
            lo, hi);
    END LOOP;
END
$harvest_enable_958$;
"#
    )
}

/// Definitions of every non-constraint index on `harvest_events`, verbatim.
///
/// Replayed against the partitioned parent, each becomes a partitioned index
/// that Postgres propagates to every partition — so a later migration adding an
/// index needs no change here.
#[cfg(feature = "db")]
async fn capture_index_defs(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    let rows = diesel::sql_query(
        "SELECT pg_get_indexdef(i.indexrelid) AS v
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()
            AND NOT EXISTS (
                SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid
            )",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(|r| r.v).collect())
}

/// The `harvest_event_cohort` body with `width` baked in as a literal.
///
/// Regenerating the function is how the operator's width choice reaches the
/// append path without a per-row config lookup.
#[must_use]
pub fn cohort_function_sql(width_secs: i64) -> String {
    let width = width_secs.max(1);
    // A NAMED dollar tag, not `$$`: this definition is embedded in a larger
    // script alongside a `DO` block, and an anonymous tag there would terminate
    // at the first nested `$$` instead of its own.
    format!(
        "CREATE OR REPLACE FUNCTION harvest_event_cohort(ts TIMESTAMPTZ)\n\
         RETURNS TIMESTAMPTZ LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $harvest_cohort_fn$\n    \
         SELECT to_timestamp(floor(extract(epoch FROM $1) / {width}) * {width})\n\
         $harvest_cohort_fn$"
    )
}

/// Double-quote an identifier, escaping embedded quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(feature = "db")]
async fn exec(conn: &mut AsyncPgConnection, sql: &str) -> HarvestResult<()> {
    diesel::sql_query(sql)
        .execute(conn)
        .await
        .map_err(|e| HarvestError::Database(format!("{e} (while running: {sql})")))?;
    Ok(())
}

#[cfg(feature = "db")]
async fn scalar_bool(conn: &mut AsyncPgConnection, sql: &str) -> HarvestResult<bool> {
    Ok(diesel::sql_query(sql)
        .get_result::<BoolRow>(conn)
        .await
        .map_err(database_error)?
        .v)
}

// ── Disabling the layout ───────────────────────────────────────────────────

/// Revert this shard to the ordinary unpartitioned table.
///
/// Copies every surviving row back into a plain table, restores the foreign key
/// and drops the partitioned parent. This **rewrites the whole table**, so it is
/// an operator escape hatch (and the reset the test suite uses to run the same
/// assertions against both layouts) — not something to run on a large live
/// deployment without a window.
///
/// A no-op on an already-unpartitioned shard.
///
/// # Errors
///
/// [`HarvestError::Database`] if any step fails; the whole revert is one
/// transaction, so a failure leaves the partitioned layout intact.
#[cfg(feature = "db")]
// One transaction of sequential DDL; splitting it would scatter a single
// reversal across helpers that only ever run in this order.
#[allow(clippy::too_many_lines)]
pub async fn disable_partitioning(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Option<DisableReport>> {
    if !detect_layout(conn).await?.is_partitioned() {
        return Ok(None);
    }
    let report = Box::pin(
        conn.transaction::<DisableReport, HarvestError, _>(async |conn| {
            let index_defs = capture_index_defs(conn).await?;

            // The flat layout restores `UNIQUE (workflow_exec_id, event_id)` and
            // the `ON DELETE CASCADE` foreign key. The partitioned layout
            // deliberately permits rows that violate BOTH, so rebuilding them over
            // the live data fails — which would make the operator escape hatch
            // unavailable on exactly the shards that have been running long enough
            // to need it:
            //
            //   * ORPHANS are the design's own garbage. Deleting an execution
            //     leaves its events behind for the sweeper, and the sweeper only
            //     collects at whole-partition granularity, so a converted shard
            //     essentially always has some. They violate the FK.
            //   * DUPLICATE (workflow_exec_id, event_id) rows can exist from the
            //     residual window the insert trigger cannot close (two appends of
            //     the same event_id in flight at once, landing in different
            //     cohorts). They violate the unique constraint.
            //
            // Both are removed here rather than left to blow up the `ALTER TABLE`,
            // and both counts are reported: silently discarding rows is not
            // something an operator should have to infer.
            let orphans = diesel::sql_query(
                "DELETE FROM harvest_events e
              WHERE NOT EXISTS (
                  SELECT 1 FROM harvest_workflow_executions x WHERE x.id = e.workflow_exec_id
              )",
            )
            .execute(conn)
            .await
            .map_err(database_error)?;

            // Keep the LOWEST `id` of each duplicate group — the first row written
            // for that `(execution, event_id)`, which is the one every earlier read
            // would already have ordered first.
            let duplicates = diesel::sql_query(
                "DELETE FROM harvest_events e
              WHERE EXISTS (
                  SELECT 1 FROM harvest_events k
                   WHERE k.workflow_exec_id = e.workflow_exec_id
                     AND k.event_id = e.event_id
                     AND k.id < e.id
              )",
            )
            .execute(conn)
            .await
            .map_err(database_error)?;
            exec(conn, "ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE").await?;
            exec(
                conn,
                "ALTER TABLE harvest_events RENAME TO harvest_events_partitioned",
            )
            .await?;
            // Rename the parent's constraints/indexes so the flat table can reclaim
            // their names, exactly as the enable path does in reverse.
            for (list_sql, rename_tmpl) in [
                (
                    "SELECT conname AS v FROM pg_constraint \
                 WHERE conrelid = 'harvest_events_partitioned'::regclass",
                    "ALTER TABLE harvest_events_partitioned RENAME CONSTRAINT {q} TO {t}",
                ),
                (
                    "SELECT indexname AS v FROM pg_indexes \
                 WHERE schemaname = current_schema() \
                   AND tablename = 'harvest_events_partitioned'",
                    "ALTER INDEX {q} RENAME TO {t}",
                ),
            ] {
                let rows = diesel::sql_query(list_sql)
                    .load::<TextRow>(conn)
                    .await
                    .map_err(database_error)?;
                for r in rows {
                    exec(
                        conn,
                        // Both sides quoted: a mixed-case or special-character
                        // name (a user-added index on harvest_events) would
                        // otherwise be case-folded or produce invalid syntax.
                        &rename_tmpl
                            .replace("{q}", &quote_ident(&r.v))
                            .replace("{t}", &quote_ident(&format!("{}__old", r.v))),
                    )
                    .await?;
                }
            }
            exec(
                conn,
                "CREATE TABLE harvest_events \
             (LIKE harvest_events_partitioned INCLUDING DEFAULTS INCLUDING COMMENTS)",
            )
            .await?;
            // `INCLUDING DEFAULTS` copies the PARTITIONED parent's cohort
            // default — `harvest_event_cohort(clock_timestamp())` — onto the
            // flat table, where it has no business being. Left alone, every
            // append after a revert stamps a live cohort into a column the
            // unpartitioned layout treats as inert, and a later `enable` then
            // fails: its legacy `CHECK (cohort < cutover)` is violated by every
            // row written in the current cohort. That would make reverting a
            // one-way door, on the one path the documentation offers an
            // operator for exactly that.
            exec(
                conn,
                "ALTER TABLE harvest_events \
                 ALTER COLUMN cohort SET DEFAULT '-infinity'::timestamptz",
            )
            .await?;
            exec(
                conn,
                "INSERT INTO harvest_events SELECT * FROM harvest_events_partitioned",
            )
            .await?;
            // Reset the partition key to the inert sentinel the unpartitioned
            // layout is defined to carry.
            //
            // Without this, reverting is a ONE-WAY DOOR. The copied rows keep
            // the real cohorts they were written with, and a later `enable`
            // computes `cutover = harvest_event_cohort(now())` and attaches the
            // legacy table with `CHECK (cohort < cutover)` — which every row
            // written in the current cohort violates, so the conversion fails
            // outright. An operator who rolled back could never roll forward
            // again, on the one path the documentation offers them for exactly
            // that.
            //
            // Free here: `disable` already rewrites the whole table.
            exec(
                conn,
                "UPDATE harvest_events SET cohort = '-infinity'::timestamptz \
                 WHERE cohort <> '-infinity'::timestamptz",
            )
            .await?;
            exec(
                conn,
                "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_pkey PRIMARY KEY (id)",
            )
            .await?;
            exec(
                conn,
                "ALTER TABLE harvest_events \
             ADD CONSTRAINT harvest_events_workflow_exec_id_event_id_key \
             UNIQUE (workflow_exec_id, event_id)",
            )
            .await?;
            exec(
                conn,
                "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_workflow_exec_id_fkey \
             FOREIGN KEY (workflow_exec_id) REFERENCES harvest_workflow_executions(id) \
             ON DELETE CASCADE",
            )
            .await?;
            for def in &index_defs {
                exec(conn, def).await?;
            }
            exec(
                conn,
                "ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id",
            )
            .await?;
            exec(conn, "DROP TABLE harvest_events_partitioned CASCADE").await?;
            Ok(DisableReport {
                orphans_removed: orphans,
                duplicates_removed: duplicates,
            })
        }),
    )
    .await?;
    Ok(Some(report))
}

/// What [`disable_partitioning`] had to discard to rebuild the flat layout's
/// constraints.
///
/// Both counts are normally the first two numbers an operator wants after a
/// revert, because both are rows that existed a moment ago and no longer do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct DisableReport {
    /// Event rows whose owning execution no longer existed. The partitioned
    /// layout's designed garbage; the flat layout's foreign key forbids them.
    pub orphans_removed: usize,
    /// Duplicate `(workflow_exec_id, event_id)` rows beyond the first. Zero on
    /// any shard that never hit the residual append race.
    pub duplicates_removed: usize,
}

// ── The sweeper ────────────────────────────────────────────────────────────

/// Report what [`sweep`] would do, without dropping anything.
///
/// Read-only: it runs the same gate over the same candidates and returns the
/// same `blocked` reasons, but never takes an `ACCESS EXCLUSIVE` lock and never
/// issues DDL. This is what backs `harvest partition status`'s answer to "why
/// has space not come back?" — the reasons live in the sweep, so a status
/// command that did not evaluate them could only ever list partitions.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog failure.
#[cfg(feature = "db")]
pub async fn evaluate(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    opts: &SweepOptions,
) -> HarvestResult<SweepOutcome> {
    sweep_inner(conn, now, opts, false).await
}

/// Drop every fully-reclaimable cohort partition, oldest first.
///
/// A partition is reclaimable when **no row in it belongs to a still-existing
/// execution** — see [`cohort_occupancy`] for the three tiers that decide that,
/// cheapest first. Note what this is *not*: cohorts are append instants, so an
/// execution's events span partitions and the absence of an execution created
/// in a partition's range proves nothing on its own. (An earlier iteration of
/// this module did derive the cohort from the owning execution, which would
/// have made that simpler predicate correct; Postgres forbids it — see the
/// module documentation.) Optimising toward the simpler predicate would delete
/// live executions' history.
///
/// The gate makes legal holds (#747) and per-type overrides (#737) work with no
/// special-casing: each keeps its execution row alive, which keeps its rows
/// owned, which blocks the drop. There is no second copy of the retention
/// policy here to drift out of sync with the janitor's.
///
/// Each drop runs in its own short transaction under `lock_timeout`. A
/// partition whose lock cannot be taken in time is reported as blocked and
/// retried next tick, rather than making the append path queue behind the
/// sweep.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog failure. A per-partition lock
/// timeout is *not* an error — it is reported in
/// [`SweepOutcome::blocked`].
#[cfg(feature = "db")]
pub async fn sweep(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    opts: &SweepOptions,
) -> HarvestResult<SweepOutcome> {
    sweep_inner(conn, now, opts, true).await
}

/// The shared body of [`sweep`] and [`evaluate`].
///
/// One implementation so a read-only status report and the pass it predicts can
/// never disagree about which partitions are droppable or why.
#[cfg(feature = "db")]
async fn sweep_inner(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    opts: &SweepOptions,
    apply: bool,
) -> HarvestResult<SweepOutcome> {
    let mut outcome = SweepOutcome::default();
    if !detect_layout(conn).await?.is_partitioned() {
        return Ok(outcome);
    }

    for part in list_partitions(conn).await? {
        if outcome.dropped.len() >= opts.max_drops {
            break;
        }
        // The DEFAULT partition is structural: dropping it would make an
        // append for an uncovered cohort fail outright. It is drained, never
        // dropped.
        if part.is_default {
            continue;
        }
        // Only cohorts entirely in the past are candidates. A partition still
        // accepting writes can always gain a row between the gate check and the
        // drop.
        let Some(upper) = part.upper else {
            outcome
                .blocked
                .push(format!("{} ({UNBOUNDED_REASON})", part.name));
            continue;
        };
        if upper > now {
            continue;
        }

        if let Some(reason) = cohort_occupancy(conn, part.lower, upper, opts).await? {
            outcome.blocked.push(format!("{} ({reason})", part.name));
            // Deliberately NOT after a scan timeout. That reason means the
            // partition was too big to prove anything about; following it with
            // an unbounded orphan DELETE over that same partition inverts the
            // "bounded pass, retry next tick" contract this module is built on.
            if apply
                && reason != SCAN_BUDGET_REASON
                && let Some(grace) = opts.straggler_grace
                && let Ok(grace) = chrono::Duration::from_std(grace)
                && upper + grace <= now
            {
                outcome.straggler_rows_deleted +=
                    delete_orphan_rows(conn, part.lower, upper, opts.straggler_batch).await?;
            }
            continue;
        }

        if !apply {
            // Read-only: report the partition as droppable without taking a
            // lock or issuing DDL.
            outcome.dropped.push(part.name);
            continue;
        }
        if drop_partition(conn, &part, upper, opts).await? {
            outcome.dropped.push(part.name);
        } else {
            outcome
                .blocked
                .push(format!("{} ({RECHECK_REASON})", part.name));
        }
    }
    Ok(outcome)
}

/// Is any row in this closed cohort still owned by a live execution?
///
/// Returns `None` when the partition is provably reclaimable, or `Some(reason)`
/// naming why it was left alone — which is the operator's answer to "why has
/// space not come back?".
///
/// Three tiers, cheapest first. Each is a *sufficient* condition for the tier
/// below to be skipped; all three answer the same question.
///
/// 1. **Nothing predates the partition** — `NOT EXISTS (… WHERE created_at <
///    upper)`. An execution cannot have appended a row before it existed, so if
///    nothing predates the partition's upper bound then nothing that could own
///    a row in it survives. One index probe on `idx_harvest_we_created_at`.
///
/// 2. **Few survivors** — when tier 1 says "maybe", read back the surviving
///    old executions, bounded by [`SweepOptions::owner_probe_cap`]. If they fit
///    under the cap, ask the *narrow* question — do any of THESE executions
///    have a row in this partition? — which is one index probe per execution on
///    `idx_harvest_events_exec`, pruned to this partition by the cohort
///    predicate.
///
///    This tier is what makes the sweeper survive the common case. Tier 1's
///    predicate is a property of the whole executions table, not of this
///    partition: **one** long-lived execution — a legal hold (#747), a
///    per-type override (#737), a 60-day run — makes it say "maybe" for
///    *every* closed partition, forever. Without tier 2 that means a full
///    ownership scan of every partition on every tick, and on a large one an
///    unwinnable scan that times out every time and never reclaims anything.
///
/// 3. **Exact scan** — only when more old executions survive than the cap: a
///    semi-join proving no row in the range has a live owner, bounded by a
///    `statement_timeout`. A timeout retains and retries; an unfinished proof
///    is not a proof.
///
/// Legal holds and per-type overrides need no special-casing in any tier: each
/// keeps its execution row alive, which keeps its rows owned. There is no
/// second copy of the retention policy here to drift out of sync with the
/// janitor's.
#[cfg(feature = "db")]
async fn cohort_occupancy(
    conn: &mut AsyncPgConnection,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
    opts: &SweepOptions,
) -> HarvestResult<Option<String>> {
    // ── Tier 1 ────────────────────────────────────────────────────────────
    let predates = diesel::sql_query(
        "SELECT EXISTS (
             SELECT 1 FROM harvest_workflow_executions WHERE created_at < $1
         ) AS v",
    )
    .bind::<Timestamptz, _>(upper)
    .get_result::<BoolRow>(conn)
    .await
    .map_err(database_error)?
    .v;
    if !predates {
        return Ok(None);
    }

    // ── Tier 2 ────────────────────────────────────────────────────────────
    let cap = i64::try_from(opts.owner_probe_cap)
        .unwrap_or(i64::MAX)
        .max(1);
    let survivors = diesel::sql_query(
        "SELECT id FROM harvest_workflow_executions WHERE created_at < $1 LIMIT $2",
    )
    .bind::<Timestamptz, _>(upper)
    .bind::<BigInt, _>(cap + 1)
    .load::<UuidRow>(conn)
    .await
    .map_err(database_error)?;

    if i64::try_from(survivors.len()).unwrap_or(i64::MAX) <= cap {
        let ids: Vec<uuid::Uuid> = survivors.into_iter().map(|r| r.id).collect();
        let owned = cohort_has_rows_for(conn, lower, upper, &ids).await?;
        return Ok(owned.then(|| OWNED_REASON.to_string()));
    }

    // ── Tier 3 ────────────────────────────────────────────────────────────
    let ms = u64::try_from(opts.exact_scan_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let scan = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL statement_timeout = '{ms}ms'")).await?;
        // `lower_predicate` rather than a substituted sentinel: the legacy
        // partition's lower bound is `MINVALUE`, and its rows carry the
        // migration's `-infinity` cohort, which sorts BELOW every finite
        // timestamptz. Binding any finite value for `MINVALUE` here would make
        // `cohort >= $lower` exclude 100% of the legacy partition's rows — so
        // the scan would find no live owner, declare the partition reclaimable,
        // and DROP the entire pre-conversion history of a deployment that had
        // just opted in, running executions and legal holds included.
        let sql = format!(
            "SELECT EXISTS (
                 SELECT 1 FROM harvest_events e
                  WHERE {} e.cohort < $1
                    AND EXISTS (
                        SELECT 1 FROM harvest_workflow_executions x
                         WHERE x.id = e.workflow_exec_id
                    )
             ) AS v",
            lower.map_or(String::new(), |_| "e.cohort >= $2 AND".to_string())
        );
        let query = diesel::sql_query(sql).bind::<Timestamptz, _>(upper);
        let row = if let Some(lower) = lower {
            query
                .bind::<Timestamptz, _>(lower)
                .get_result::<BoolRow>(conn)
                .await
        } else {
            query.get_result::<BoolRow>(conn).await
        };
        Ok(row.map_err(database_error)?.v)
    }))
    .await;

    match scan {
        Ok(false) => Ok(None),
        Ok(true) => Ok(Some(OWNED_REASON.to_string())),
        // Fail safe toward RETAINING: an unfinished proof is not a proof.
        Err(HarvestError::Database(msg)) if is_statement_timeout(&msg) => {
            Ok(Some(SCAN_BUDGET_REASON.to_string()))
        }
        Err(e) => Err(e),
    }
}

/// Do any of `ids` have a row in this cohort range?
///
/// Tier 2's narrow question, and also the authoritative re-check the drop
/// transaction runs while holding `ACCESS EXCLUSIVE`. `ids` empty ⇒ `false`
/// without a query.
#[cfg(feature = "db")]
async fn cohort_has_rows_for(
    conn: &mut AsyncPgConnection,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
    ids: &[uuid::Uuid],
) -> HarvestResult<bool> {
    if ids.is_empty() {
        return Ok(false);
    }
    // The `MINVALUE` lower bound is omitted rather than substituted — see the
    // comment in `cohort_occupancy`'s tier 3.
    let sql = format!(
        "SELECT EXISTS (
             SELECT 1 FROM harvest_events e
              WHERE e.workflow_exec_id = ANY($1) AND {} e.cohort < $2
         ) AS v",
        lower.map_or(String::new(), |_| "e.cohort >= $3 AND".to_string())
    );
    let query = diesel::sql_query(sql)
        .bind::<Array<SqlUuid>, _>(ids.to_vec())
        .bind::<Timestamptz, _>(upper);
    let row = if let Some(lower) = lower {
        query
            .bind::<Timestamptz, _>(lower)
            .get_result::<BoolRow>(conn)
            .await
    } else {
        query.get_result::<BoolRow>(conn).await
    };
    Ok(row.map_err(database_error)?.v)
}

/// Drop one partition under a bounded lock wait, re-proving it is reclaimable
/// while holding the lock.
///
/// Returns `false` (not an error) when the partition was left in place: either
/// the `ACCESS EXCLUSIVE` lock could not be taken in time, or the re-check
/// found an owner. Leaving it for the next tick is the correct response to
/// both; making every concurrent append wait is not.
///
/// **The re-check is not belt-and-braces.** `cohort_occupancy` runs in its own
/// transaction and commits before this one starts, so between the two a row can
/// legitimately appear in the partition:
///
/// - `drain_default` moves parked rows in with an explicit `cohort`, which is
///   the one writer that can land a row in an already-closed cohort;
/// - a transaction that *began* before the cohort boundary and inserts after it
///   stamps the previous cohort, because the column default is evaluated per
///   row but a long transaction's clock is not the sweeper's;
/// - nothing stops a second retention runtime from running maintenance on the
///   same shard concurrently — candidates are leased per execution, maintenance
///   is not.
///
/// Repeating the check *after* `LOCK TABLE … ACCESS EXCLUSIVE` makes it exact:
/// that lock conflicts with every writer, so no row can appear between the
/// re-check and the `DROP` in the same transaction.
#[cfg(feature = "db")]
async fn drop_partition(
    conn: &mut AsyncPgConnection,
    part: &PartitionInfo,
    upper: DateTime<Utc>,
    opts: &SweepOptions,
) -> HarvestResult<bool> {
    let ms = u64::try_from(opts.lock_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let name = part.name.clone();
    let lower = part.lower;
    let cap = i64::try_from(opts.owner_probe_cap)
        .unwrap_or(i64::MAX)
        .max(1);
    let result = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL lock_timeout = '{ms}ms'")).await?;
        // Taken explicitly, before the re-check, rather than relying on the
        // DROP to take it afterwards — the whole point is that the check runs
        // under the lock.
        exec(conn, "LOCK TABLE harvest_events IN ACCESS EXCLUSIVE MODE").await?;

        let survivors = diesel::sql_query(
            "SELECT id FROM harvest_workflow_executions WHERE created_at < $1 LIMIT $2",
        )
        .bind::<Timestamptz, _>(upper)
        .bind::<BigInt, _>(cap + 1)
        .load::<UuidRow>(conn)
        .await
        .map_err(database_error)?;

        if i64::try_from(survivors.len()).unwrap_or(i64::MAX) > cap {
            // More survivors than the narrow re-check can probe. Retaining is
            // the correct failure mode: the tick that selected this partition
            // used the exact scan, but that evidence is now stale and we will
            // not drop on stale evidence.
            return Ok(false);
        }
        let ids: Vec<uuid::Uuid> = survivors.into_iter().map(|r| r.id).collect();
        if cohort_has_rows_for(conn, lower, upper, &ids).await? {
            return Ok(false);
        }

        exec(
            conn,
            &format!("DROP TABLE IF EXISTS {}", quote_ident(&name)),
        )
        .await?;
        Ok(true)
    }))
    .await;
    match result {
        Ok(dropped) => Ok(dropped),
        Err(HarvestError::Database(msg)) if is_lock_timeout(&msg) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Targeted removal of orphan rows from a cohort a straggler has pinned.
///
/// The opt-in fallback issue #958 sanctions for the non-steady-state case.
/// Deletes only rows whose owning execution no longer exists, in bounded
/// batches, so a straggler pass can neither touch a live execution's history
/// nor open an unbounded transaction.
#[cfg(feature = "db")]
async fn delete_orphan_rows(
    conn: &mut AsyncPgConnection,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
    batch: usize,
) -> HarvestResult<usize> {
    let batch = i64::try_from(batch).unwrap_or(i64::MAX).max(1);
    // Total work per partition per tick is capped, not just per statement. The
    // inner SELECT restarts from the top of the range each iteration, so on a
    // partition where orphans are SPARSE — the common shape for one pinned by a
    // straggler — each pass re-scans the leading owned rows before finding its
    // batch, which is quadratic in the partition size. Bounded here; the next
    // tick continues.
    let max_batches = 16;
    let mut total = 0usize;
    for _ in 0..max_batches {
        // The `MINVALUE` lower bound is omitted rather than substituted, for
        // the same reason as in `cohort_occupancy`: the legacy partition's rows
        // carry `-infinity`, which sorts below every finite timestamptz, so a
        // finite lower bind would silently match none of them.
        let sql = format!(
            "DELETE FROM harvest_events
              WHERE ctid IN (
                  SELECT e.ctid FROM harvest_events e
                   WHERE {} e.cohort < $1
                     AND NOT EXISTS (
                         SELECT 1 FROM harvest_workflow_executions x
                          WHERE x.id = e.workflow_exec_id
                     )
                   LIMIT $2
              )",
            lower.map_or(String::new(), |_| "e.cohort >= $3 AND".to_string())
        );
        let query = diesel::sql_query(sql)
            .bind::<Timestamptz, _>(upper)
            .bind::<BigInt, _>(batch);
        let deleted = if let Some(lower) = lower {
            query.bind::<Timestamptz, _>(lower).execute(conn).await
        } else {
            query.execute(conn).await
        }
        .map_err(database_error)?;
        total += deleted;
        if deleted == 0 || i64::try_from(deleted).unwrap_or(i64::MAX) < batch {
            break;
        }
    }
    Ok(total)
}

// ── Draining the DEFAULT partition ─────────────────────────────────────────

/// Move rows out of the `DEFAULT` partition into real cohort partitions.
///
/// The `DEFAULT` partition exists so an append can never fail with `no
/// partition of relation found` — a maintenance gap or a clock-skewed execution
/// must not stall a workflow. But rows sitting there block creation of the very
/// partitions that would cover them, so maintenance drains it.
///
/// Runs as one transaction: the default partition is detached, the missing
/// cohorts are created, the rows are re-inserted with the cohort trigger
/// disabled (they already carry correct cohorts; re-stamping would reject rows
/// whose execution has since been collected), and the emptied default is
/// reattached. Normally a no-op — the drain path exists for the rare case, and
/// says so by returning `0`.
///
/// # Errors
///
/// [`HarvestError::Database`] if any step fails; the whole drain is one
/// transaction, so a failure leaves every row where it was.
#[cfg(feature = "db")]
pub async fn drain_default(conn: &mut AsyncPgConnection) -> HarvestResult<usize> {
    let width = match detect_layout(conn).await? {
        EventLayout::Unpartitioned => return Ok(0),
        EventLayout::Partitioned { cohort_width_secs } => cohort_width_secs,
    };
    let has_rows = scalar_bool(
        conn,
        &format!("SELECT EXISTS (SELECT 1 FROM {DEFAULT_PARTITION}) AS v"),
    )
    .await?;
    if !has_rows {
        return Ok(0);
    }

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        exec(conn, "SET LOCAL lock_timeout = '5s'").await?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events DETACH PARTITION {DEFAULT_PARTITION}"),
        )
        .await?;

        // Read the work list AFTER the DETACH, not before. A cohort that
        // arrived between an outside-the-transaction read and this point would
        // have no partition created for it, and the INSERT below would fail
        // with `no partition of relation found` — aborting the whole drain, and
        // with it the `ensure_partitions` that keeps the write window covered.
        let cohorts = diesel::sql_query(format!(
            "SELECT DISTINCT cohort AS v FROM {DEFAULT_PARTITION} ORDER BY 1"
        ))
        .load::<TsRow>(conn)
        .await
        .map_err(database_error)?;
        for c in &cohorts {
            if let Some(cohort) = c.v {
                ensure_cohort_with_width(conn, cohort, width).await?;
            }
        }

        // `INSERT … SELECT *` supplies `cohort` explicitly, so the DEFAULT does
        // not re-fire and every row keeps the cohort it was written with. The
        // integrity trigger is disabled for the move because a parked row whose
        // execution has since been collected is exactly the orphan the sweeper
        // is meant to reclaim later — re-validating it here would turn a
        // maintenance drain into data loss.
        //
        // Disabled on EACH PARTITION, not just the parent: `ALTER TABLE …
        // DISABLE TRIGGER` on a partitioned parent only recurses to its
        // partitions from Postgres 14. On 12/13 the parent-only form is a
        // silent no-op, the cloned trigger fires for every moved row, and the
        // first orphan aborts the drain permanently. The ACCESS EXCLUSIVE lock
        // held by the DETACH above makes this safe for the transaction.
        let targets = diesel::sql_query(
            "SELECT c.relname AS v
               FROM pg_inherits i
               JOIN pg_class p ON p.oid = i.inhparent
               JOIN pg_class c ON c.oid = i.inhrelid
               JOIN pg_namespace n ON n.oid = p.relnamespace
              WHERE p.relname = 'harvest_events' AND n.nspname = current_schema()",
        )
        .load::<TextRow>(conn)
        .await
        .map_err(database_error)?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events DISABLE TRIGGER {EXEC_FK_TRIGGER}"),
        )
        .await?;
        for t in &targets {
            exec(
                conn,
                &format!(
                    "ALTER TABLE {} DISABLE TRIGGER {EXEC_FK_TRIGGER}",
                    quote_ident(&t.v)
                ),
            )
            .await
            .ok();
        }

        let moved = diesel::sql_query(format!(
            "INSERT INTO harvest_events SELECT * FROM {DEFAULT_PARTITION}"
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;

        for t in &targets {
            exec(
                conn,
                &format!(
                    "ALTER TABLE {} ENABLE TRIGGER {EXEC_FK_TRIGGER}",
                    quote_ident(&t.v)
                ),
            )
            .await
            .ok();
        }
        exec(
            conn,
            &format!("ALTER TABLE harvest_events ENABLE TRIGGER {EXEC_FK_TRIGGER}"),
        )
        .await?;
        exec(conn, &format!("TRUNCATE {DEFAULT_PARTITION}")).await?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events ATTACH PARTITION {DEFAULT_PARTITION} DEFAULT"),
        )
        .await?;
        Ok(moved)
    }))
    .await
}

// ── Engine-automated maintenance ───────────────────────────────────────────

/// One full maintenance pass: extend the lookahead window, drain the `DEFAULT`
/// partition, then sweep.
///
/// This is what AC8's "no operator cron required" means in practice — the
/// retention runtime calls it every tick and at startup. A no-op on an
/// unpartitioned shard, so it is safe to call unconditionally.
///
/// Ordered deliberately: draining first, because rows parked in `DEFAULT` block
/// creation of the partitions that would cover them; then creating, so a tick
/// that drops a backlog still leaves the write window covered; then sweeping,
/// so a cohort freed earlier in the same tick is reclaimed now rather than
/// next time.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog or DDL failure.
#[cfg(feature = "db")]
pub async fn maintain(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    lookahead_cohorts: u32,
    sweep_opts: &SweepOptions,
) -> HarvestResult<MaintenanceOutcome> {
    if !detect_layout(conn).await?.is_partitioned() {
        // Still stamped: a caller polling for "maintenance has run" must not
        // hang forever on an unpartitioned shard, where there is nothing to do.
        return Ok(MaintenanceOutcome {
            at: Some(Utc::now()),
            ..MaintenanceOutcome::default()
        });
    }
    // Drain first: rows parked in the DEFAULT partition BLOCK creation of the
    // very cohort partitions that would cover them, so an ensure before a drain
    // would fail on exactly the deployment that needs it most.
    //
    // Best-effort, deliberately: the drain is the heaviest step (it holds
    // ACCESS EXCLUSIVE while it moves rows) and the most likely to lose a lock
    // race. Propagating its failure would take `ensure_partitions` down with
    // it — and extending the write window is the one thing that must never
    // stop, because an uncovered cohort is what fills the DEFAULT partition in
    // the first place. A failed drain is recorded and retried next tick.
    let (drained, drain_error) = match drain_default(conn).await {
        Ok(n) => (n, None),
        Err(e) => (0, Some(e.to_string())),
    };
    let created = ensure_partitions(conn, now, lookahead_cohorts).await?;
    let sweep = sweep(conn, now, sweep_opts).await?;
    Ok(MaintenanceOutcome {
        at: Some(Utc::now()),
        created,
        drained,
        sweep,
        last_error: drain_error,
    })
}

/// What one [`maintain`] pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct MaintenanceOutcome {
    /// When this pass finished.
    ///
    /// `None` only on the zero value. Reported because the retention tick
    /// publishes its history-retention counters *before* maintenance runs (a
    /// cohort is only droppable once the candidate loop has collected its
    /// executions), so "the tick ran" and "maintenance ran" are genuinely
    /// different instants — and an operator diagnosing "why is space not coming
    /// back?" needs to know which one they are looking at.
    pub at: Option<DateTime<Utc>>,
    /// Cohort partitions created to extend the lookahead window.
    pub created: Vec<String>,
    /// Rows moved out of the `DEFAULT` partition.
    pub drained: usize,
    /// The sweep result.
    pub sweep: SweepOutcome,
    /// Why this pass did not complete, when it did not.
    ///
    /// Maintenance is best-effort — it must never fail a retention tick, since
    /// history retention and reclamation are independent. But "best effort"
    /// must not mean "invisible": without this a permanently-failing shard
    /// reports exactly what a shard that never opted in reports, and the
    /// operator has no way to tell them apart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

impl MaintenanceOutcome {
    /// A pass that could not run, carrying the reason.
    #[must_use]
    pub fn failed(error: String) -> Self {
        Self {
            at: Some(Utc::now()),
            last_error: Some(error),
            ..Self::default()
        }
    }
}

// ── The large-live-table migration plan ────────────────────────────────────

/// One statement of the large-live-table conversion plan.
///
/// The plan exists in exactly one form — this list — and
/// [`migration_plan`] renders it. That is what makes it *executable*: an
/// earlier revision printed the catalog-driven parts as prose ("emit one line
/// per object from: SELECT format(…)"), so an operator who ran the generated
/// file verbatim never renamed the legacy constraints, and step 4 aborted on
/// `ADD CONSTRAINT harvest_events_pkey` because the old schema-scoped index
/// still held the name. Everything catalog-driven is a `DO` block now, and an
/// integration test runs these steps against a real populated database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanStep {
    /// Numbered phase this statement belongs to (1-5), for the rendered script.
    pub phase: u8,
    /// The SQL, without a trailing semicolon.
    pub sql: String,
    /// `true` when the statement cannot run inside a transaction block —
    /// `CREATE INDEX CONCURRENTLY`. Each must be sent on its own.
    pub concurrent: bool,
}

/// The conversion plan for a **large live** `harvest_events`, as executable
/// statements.
///
/// [`enable_partitioning`] runs the same algorithm in one transaction, which is
/// right for a fresh or small table and wrong for a ten-million-row one: the
/// index builds and the constraint validation would hold `ACCESS EXCLUSIVE` for
/// their whole duration, blocking every append. These steps move both out of
/// the lock window — `CREATE INDEX CONCURRENTLY` builds without blocking
/// reads or writes, and `ADD CONSTRAINT … NOT VALID` + `VALIDATE CONSTRAINT`
/// does the full scan under `SHARE UPDATE EXCLUSIVE`, which concurrent readers
/// and writers do not conflict with. Because the constraint is then valid,
/// `ATTACH PARTITION` skips its own verification scan entirely.
///
/// What remains inside the exclusive window is metadata-only: a rename, a
/// `CREATE TABLE`, and the `ATTACH`.
// One ordered list of statements. Splitting it would scatter a runbook that has
// to be read (and executed) top to bottom across helpers that only concatenate.
#[allow(clippy::too_many_lines)]
#[must_use]
pub fn migration_plan_steps(opts: &EnableOptions, now: DateTime<Utc>) -> Vec<PlanStep> {
    let width = opts.cohort_width_secs.max(1);
    let lookahead = opts.lookahead_cohorts;
    let lock_ms = opts.lock_timeout.as_millis().max(1);
    let suffix_len = LEGACY_RENAME_SUFFIX.len();
    let cutover_lit = ts_literal(cohort_start(now, width));

    let step = |phase: u8, sql: String| PlanStep {
        phase,
        sql,
        concurrent: false,
    };
    let concurrent = |phase: u8, sql: String| PlanStep {
        phase,
        sql,
        concurrent: true,
    };

    vec![
        // ── 1: bake the chosen width into the cohort function ─────────────
        step(1, cohort_function_sql(width)),
        // ── 2: the partition-key indexes, built without blocking ──────────
        concurrent(
            2,
            format!(
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {LEGACY_PARTITION}_pk_idx\n    \
                 ON harvest_events (id, cohort)"
            ),
        ),
        concurrent(
            2,
            format!(
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS \
                 {LEGACY_PARTITION}_exec_event_idx\n    \
                 ON harvest_events (workflow_exec_id, event_id, cohort)"
            ),
        ),
        // ── 3: pre-validate so ATTACH skips its own scan ──────────────────
        step(
            3,
            format!(
                "ALTER TABLE harvest_events\n    \
                 ADD CONSTRAINT {LEGACY_PARTITION}_cohort_ck\n    \
                 CHECK (cohort < {cutover_lit}) NOT VALID"
            ),
        ),
        step(
            3,
            format!("ALTER TABLE harvest_events VALIDATE CONSTRAINT {LEGACY_PARTITION}_cohort_ck"),
        ),
        // ── 4: THE WINDOW — one transaction, metadata only ────────────────
        step(4, "BEGIN".to_string()),
        step(4, format!("SET LOCAL lock_timeout = '{lock_ms}ms'")),
        // Captured BEFORE the rename, so each definition still names
        // `harvest_events` and replays verbatim onto the new parent. The two
        // indexes built in step 2 are excluded: replaying them would duplicate
        // the constraint indexes added below.
        step(
            4,
            format!(
                "CREATE TEMP TABLE harvest_events_idx_defs ON COMMIT DROP AS\n  \
                 SELECT pg_get_indexdef(i.indexrelid) AS def\n    \
                 FROM pg_index i\n    \
                 JOIN pg_class c ON c.oid = i.indrelid\n    \
                 JOIN pg_namespace n ON n.oid = c.relnamespace\n   \
                 WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()\n     \
                 AND NOT EXISTS (SELECT 1 FROM pg_constraint con\n                      \
                 WHERE con.conindid = i.indexrelid)\n     \
                 AND c.oid <> 0\n     \
                 AND i.indexrelid::regclass::text NOT IN\n         \
                 ('{LEGACY_PARTITION}_pk_idx', '{LEGACY_PARTITION}_exec_event_idx')"
            ),
        ),
        step(
            4,
            "ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE".to_string(),
        ),
        step(
            4,
            format!("ALTER TABLE harvest_events RENAME TO {LEGACY_PARTITION}"),
        ),
        // Renaming a table renames neither its indexes nor its constraints, so
        // without this the new parent cannot reclaim their schema-scoped names
        // — and `ADD CONSTRAINT harvest_events_pkey` below aborts.
        step(
            4,
            format!(
                "DO $harvest_rename_958$\nDECLARE obj record;\nBEGIN\n    \
                 FOR obj IN SELECT conname AS n FROM pg_constraint\n                \
                 WHERE conrelid = '{LEGACY_PARTITION}'::regclass\n                  \
                 AND right(conname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'\n    \
                 LOOP\n        \
                 EXECUTE format('ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT %I TO %I',\n                       \
                 obj.n, obj.n || '{LEGACY_RENAME_SUFFIX}');\n    \
                 END LOOP;\n    \
                 FOR obj IN SELECT indexname AS n FROM pg_indexes\n                \
                 WHERE schemaname = current_schema() AND tablename = '{LEGACY_PARTITION}'\n                  \
                 AND right(indexname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'\n    \
                 LOOP\n        \
                 EXECUTE format('ALTER INDEX %I RENAME TO %I', obj.n,\n                       \
                 obj.n || '{LEGACY_RENAME_SUFFIX}');\n    \
                 END LOOP;\nEND\n$harvest_rename_958$"
            ),
        ),
        // The FK's ON DELETE CASCADE is the delete storm being eliminated; its
        // insert-time half lives on in the trigger below. The old PK and unique
        // constraint must go too: ATTACH propagates the parent's, and a table
        // may have only one primary key.
        step(
            4,
            format!(
                "ALTER TABLE {LEGACY_PARTITION}\n    \
                 DROP CONSTRAINT IF EXISTS \
                 harvest_events_workflow_exec_id_fkey{LEGACY_RENAME_SUFFIX},\n    \
                 DROP CONSTRAINT IF EXISTS harvest_events_pkey{LEGACY_RENAME_SUFFIX},\n    \
                 DROP CONSTRAINT IF EXISTS \
                 harvest_events_workflow_exec_id_event_id_key{LEGACY_RENAME_SUFFIX}"
            ),
        ),
        step(
            4,
            format!(
                "CREATE TABLE harvest_events\n    \
                 (LIKE {LEGACY_PARTITION} INCLUDING DEFAULTS INCLUDING COMMENTS \
                 INCLUDING STORAGE)\n    PARTITION BY RANGE (cohort)"
            ),
        ),
        // WITHOUT THIS every new row keeps the `-infinity` sentinel and lands
        // in the legacy partition forever, which looks fine until nothing is
        // ever droppable. `clock_timestamp()`, not `now()`: `now()` is
        // transaction START time, so a long transaction begun before a cohort
        // boundary would stamp the previous, already-closed cohort.
        step(
            4,
            "ALTER TABLE harvest_events\n    \
             ALTER COLUMN cohort SET DEFAULT harvest_event_cohort(clock_timestamp())"
                .to_string(),
        ),
        step(
            4,
            "ALTER TABLE harvest_events\n    \
             ADD CONSTRAINT harvest_events_pkey PRIMARY KEY (id, cohort)"
                .to_string(),
        ),
        step(
            4,
            "ALTER TABLE harvest_events\n    \
             ADD CONSTRAINT harvest_events_workflow_exec_id_event_id_key\n    \
             UNIQUE (workflow_exec_id, event_id, cohort)"
                .to_string(),
        ),
        step(
            4,
            "DO $harvest_idx_958$\nDECLARE d text;\nBEGIN\n    \
             FOR d IN SELECT def FROM harvest_events_idx_defs LOOP\n        \
             EXECUTE d;\n    END LOOP;\nEND\n$harvest_idx_958$"
                .to_string(),
        ),
        step(
            4,
            "ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id".to_string(),
        ),
        // Validate-only: a BEFORE ROW trigger on a partitioned table must not
        // touch the partition key, because routing has already happened when it
        // fires. It also enforces `(workflow_exec_id, event_id)` uniqueness
        // ACROSS partitions, which the constraint above can only do within one.
        step(
            4,
            format!(
                "CREATE TRIGGER {EXEC_FK_TRIGGER} BEFORE INSERT ON harvest_events\n    \
                 FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()"
            ),
        ),
        step(
            4,
            format!("CREATE TABLE {DEFAULT_PARTITION} PARTITION OF harvest_events DEFAULT"),
        ),
        step(
            4,
            format!(
                "ALTER TABLE harvest_events ATTACH PARTITION {LEGACY_PARTITION}\n    \
                 FOR VALUES FROM (MINVALUE) TO ({cutover_lit})"
            ),
        ),
        // The write window, created here rather than left to the first
        // retention tick: metadata-only, and deferring it would send every
        // append for up to a tick interval into the DEFAULT partition, whose
        // drain then holds ACCESS EXCLUSIVE while it moves them back — the
        // append stall this whole change exists to avoid.
        step(
            4,
            format!(
                "DO $harvest_window_958$\nDECLARE lo timestamptz; hi timestamptz; step int;\n\
                 BEGIN\n    FOR step IN 0..{lookahead} LOOP\n        \
                 lo := harvest_event_cohort(now() + (step * {width}) * interval '1 second');\n        \
                 hi := lo + ({width} * interval '1 second');\n        \
                 EXECUTE format(\n            \
                 'CREATE TABLE IF NOT EXISTS %I PARTITION OF harvest_events \
                 FOR VALUES FROM (%L) TO (%L)',\n            \
                 '{PARTITION_PREFIX}' || to_char(lo AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS'),\n            \
                 lo, hi);\n    END LOOP;\nEND\n$harvest_window_958$"
            ),
        ),
        step(4, "COMMIT".to_string()),
    ]
}

/// Render the operator-run conversion script for a **large live**
/// `harvest_events`.
///
/// Every statement comes from [`migration_plan_steps`], so the script an
/// operator runs and the statements CI executes are the same list — the plan
/// cannot drift into prose that looks like SQL but does nothing.
///
/// Steps 1–3 are online and may take a long time on a large table. Only step 4
/// takes `ACCESS EXCLUSIVE`, and everything it does is metadata-only, so it is
/// a seconds-long window rather than a scan.
#[must_use]
pub fn migration_plan(opts: &EnableOptions, now: DateTime<Utc>) -> String {
    let width = opts.cohort_width_secs.max(1);
    let cutover_lit = ts_literal(cohort_start(now, width));
    let mut out = format!(
        r"-- ────────────────────────────────────────────────────────────────
-- harvest_events -> partitioned layout (issue #958), LARGE LIVE TABLE
--
-- `harvest partition enable` runs the same algorithm in ONE transaction,
-- which is right for a fresh or small table and wrong for a
-- ten-million-row one: the index builds and the constraint validation
-- would hold ACCESS EXCLUSIVE for their whole duration, blocking every
-- append. This script moves both out of the lock window.
--
-- Steps 1-3 are ONLINE: they hold no lock that blocks appends, and may
-- take a long time on a large table. Only step 4 takes ACCESS EXCLUSIVE,
-- and everything it does is metadata-only.
--
-- Run the CREATE INDEX CONCURRENTLY statements in step 2 ONE AT A TIME:
-- CONCURRENTLY cannot run inside a transaction block, so a client that
-- wraps a whole file in one transaction will reject them.
--
-- Recheck the cutover ({cutover_lit}) before running step 3. It was
-- computed when this script was generated. A STALE (older) cutover is
-- safe -- every pre-conversion row carries the `-infinity` sentinel, so
-- all of them still fall inside the legacy range -- but it leaves the
-- cohorts between then and now with no partition, so their rows land in
-- the DEFAULT partition until maintenance drains them. To avoid that,
-- regenerate this script (or substitute `SELECT harvest_event_cohort(now());`)
-- immediately before step 3. A cutover in the FUTURE is NOT safe: rows
-- appended after the swap would fall inside a partition meant to be sealed.
--
-- Rollback: until step 4 commits, nothing is committed but two extra
-- indexes and one CHECK constraint, all droppable with no downtime.
-- ────────────────────────────────────────────────────────────────
"
    );
    let mut last_phase = 0u8;
    for st in migration_plan_steps(opts, now) {
        if st.phase != last_phase {
            out.push_str(match st.phase {
                1 => "\n-- Step 1 (online). Bake the chosen cohort width into the cohort function.\n",
                2 => "\n-- Step 2 (online, may take a while). Build the two indexes the parent's\n\
                      -- partition-key-bearing PRIMARY KEY and UNIQUE constraints require.\n\
                      -- Run each on its own: CONCURRENTLY cannot run in a transaction block.\n",
                3 => "\n-- Step 3 (online). Pre-validate the range constraint so ATTACH PARTITION in\n\
                      -- step 4 skips its own full-table verification scan. ADD ... NOT VALID\n\
                      -- takes a brief lock; VALIDATE does the scan under SHARE UPDATE EXCLUSIVE,\n\
                      -- which concurrent readers and writers do not conflict with.\n",
                _ => "\n-- Step 4 (THE WINDOW: ACCESS EXCLUSIVE, metadata-only). One transaction:\n\
                      -- if anything fails, nothing changed.\n",
            });
            last_phase = st.phase;
        }
        out.push_str(&st.sql);
        out.push_str(";\n");
    }
    out.push_str(
        "\n-- Step 5 (online). Let the engine take over: it pre-creates the lookahead\n\
         -- window, drains the DEFAULT partition and sweeps droppable cohorts on every\n\
         -- retention tick. Nothing further is required of the operator, and no cron\n\
         -- job needs to exist.\n--   harvest partition status --shard <dsn>\n",
    );
    out
}

// ── Unit tests (no database) ───────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cohort_start_floors_to_the_width() {
        let ts = Utc.with_ymd_and_hms(2026, 8, 31, 17, 42, 9).unwrap();
        assert_eq!(
            cohort_start(ts, 86_400),
            Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap()
        );
        assert_eq!(
            cohort_start(ts, 3_600),
            Utc.with_ymd_and_hms(2026, 8, 31, 17, 0, 0).unwrap()
        );
    }

    #[test]
    fn cohort_start_floors_downward_before_the_epoch() {
        // `/` truncates toward zero, which would round this UP into 1970-01-01
        // and route a pre-epoch execution's events to the wrong partition.
        // `div_euclid` is what makes this correct.
        let ts = Utc.with_ymd_and_hms(1969, 12, 31, 23, 0, 0).unwrap();
        assert_eq!(
            cohort_start(ts, 86_400),
            Utc.with_ymd_and_hms(1969, 12, 31, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn cohort_start_is_idempotent() {
        let ts = Utc.with_ymd_and_hms(2026, 2, 14, 6, 30, 0).unwrap();
        let once = cohort_start(ts, 86_400);
        assert_eq!(cohort_start(once, 86_400), once);
    }

    #[test]
    fn cohort_start_never_divides_by_zero() {
        // Defence in depth: `validate` rejects this long before it can reach
        // the database, but a panic here would take down a worker.
        let ts = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(cohort_start(ts, 0), ts);
    }

    #[test]
    fn partition_names_carry_their_cohort() {
        let name = partition_name(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
        assert_eq!(name, "harvest_events_p_20260831000000");
        assert!(name.starts_with(PARTITION_PREFIX));
    }

    #[test]
    fn partition_names_are_distinct_at_second_granularity() {
        let a = partition_name(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
        let b = partition_name(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 1).unwrap());
        assert_ne!(a, b);
    }

    #[test]
    fn the_cohort_function_bakes_in_the_width_and_round_trips() {
        for width in [60_i64, 3_600, 86_400, 604_800] {
            let sql = cohort_function_sql(width);
            assert!(sql.contains(&format!("/ {width}")), "{sql}");
            assert_eq!(
                parse_cohort_width(&sql),
                Some(width),
                "the deployed width must be readable back out of the function \
                 body — it is the single source of truth the trigger uses"
            );
        }
    }

    #[test]
    fn parse_cohort_width_rejects_an_unrecognised_body() {
        assert_eq!(parse_cohort_width("CREATE FUNCTION f() ..."), None);
        assert_eq!(
            parse_cohort_width("... epoch FROM $1) / 0) * 0 ..."),
            None,
            "a zero width must not be accepted: it would divide by zero"
        );
    }

    #[test]
    fn partition_bounds_parse_from_postgres_expressions() {
        let (lo, hi, default) = parse_partition_bound(
            "FOR VALUES FROM ('2026-08-31 00:00:00+00') TO ('2026-09-01 00:00:00+00')",
        );
        assert!(!default);
        assert_eq!(
            lo,
            Some(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap())
        );
        assert_eq!(hi, Some(Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()));
    }

    #[test]
    fn the_legacy_partitions_minvalue_bound_parses_as_open() {
        let (lo, hi, default) =
            parse_partition_bound("FOR VALUES FROM (MINVALUE) TO ('2026-08-31 00:00:00+00')");
        assert!(!default);
        assert_eq!(lo, None, "MINVALUE is an open lower bound");
        assert_eq!(
            hi,
            Some(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap())
        );
    }

    #[test]
    fn the_default_partition_is_recognised() {
        let (lo, hi, default) = parse_partition_bound("DEFAULT");
        assert!(default, "the DEFAULT partition must never be dropped");
        assert_eq!(lo, None);
        assert_eq!(hi, None);
    }

    #[test]
    fn fractional_second_bounds_parse() {
        let (_, hi, _) = parse_partition_bound(
            "FOR VALUES FROM (MINVALUE) TO ('2026-08-31 00:00:00.123456+00')",
        );
        assert!(hi.is_some(), "a sub-second bound must still parse");
    }

    #[test]
    fn enable_options_validate_the_cohort_width() {
        assert!(EnableOptions::default().validate().is_ok());
        assert!(
            EnableOptions {
                cohort_width_secs: 0,
                ..EnableOptions::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            EnableOptions {
                cohort_width_secs: MAX_COHORT_WIDTH_SECS + 1,
                ..EnableOptions::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            EnableOptions {
                lookahead_cohorts: 0,
                ..EnableOptions::default()
            }
            .validate()
            .is_err(),
            "a zero lookahead sends every append to the DEFAULT partition"
        );
    }

    #[test]
    fn the_migration_plan_keeps_the_expensive_steps_out_of_the_lock_window() {
        let plan = migration_plan(&EnableOptions::default(), Utc::now());
        let window = plan
            .split("BEGIN;")
            .nth(1)
            .expect("the plan must contain the exclusive window");
        assert!(
            !window.contains("VALIDATE CONSTRAINT"),
            "the validation scan must happen BEFORE the exclusive window, or \
             the window becomes as long as a full table scan"
        );
        assert!(
            !window.contains("CREATE UNIQUE INDEX"),
            "index builds must happen CONCURRENTLY before the window"
        );
        assert!(
            plan.contains("CREATE UNIQUE INDEX CONCURRENTLY"),
            "and they must be built concurrently"
        );
        assert!(
            window.contains("lock_timeout"),
            "the window must be bounded: a conversion that cannot get the lock \
             must fail rather than stall every append behind it"
        );
    }

    #[test]
    fn the_migration_plan_tells_the_operator_to_recheck_the_cutover() {
        let plan = migration_plan(&EnableOptions::default(), Utc::now());
        assert!(
            plan.contains("Recheck the cutover"),
            "a cutover computed when the plan was generated can be stale by the \
             time step 4 runs; a pre-conversion execution created after that \
             point would tear its history across two partitions"
        );
    }

    /// The operator-run plan and the executed script must perform the same
    /// conversion.
    ///
    /// They are necessarily two texts — one is a transaction the engine runs,
    /// the other is a numbered runbook a human follows — so they cannot share
    /// an implementation. This pins the load-bearing statements to both. It is
    /// not a hypothetical: the plan really did drift once, and every one of
    /// these assertions is a bug it shipped with.
    ///
    /// - A stale trigger function name (`harvest_events_stamp_cohort`, which no
    ///   longer exists): the plan would have failed outright at step 4.
    /// - A missing `ALTER COLUMN cohort SET DEFAULT`: silent, and far worse —
    ///   every new row would keep the `-infinity` sentinel and land in the
    ///   legacy partition forever, so nothing would ever become droppable and
    ///   the operator would conclude partitioning does not work.
    /// - Missing legacy PK/unique drops: `ATTACH PARTITION` fails with
    ///   "multiple primary keys ... are not allowed" *after* the operator has
    ///   already spent an hour on the CONCURRENTLY index builds.
    #[test]
    fn the_operator_plan_performs_the_same_conversion_as_the_executed_script() {
        let opts = EnableOptions::default();
        let plan = migration_plan(&opts, Utc::now());
        let script = enable_sql(&opts);

        for needle in [
            // The trigger must name the function that actually exists.
            "harvest_events_require_execution()",
            // Without this the cohort DEFAULT never becomes the live
            // expression and every append lands in the legacy partition.
            "ALTER COLUMN cohort SET DEFAULT harvest_event_cohort(clock_timestamp())",
            // ATTACH propagates the parent PK; the child may not keep its own.
            "harvest_events_pkey__pre958",
            "harvest_events_workflow_exec_id_event_id_key__pre958",
            // The FK whose cascade is the delete storm being eliminated.
            "harvest_events_workflow_exec_id_fkey__pre958",
            // The partitioned shape itself.
            "PARTITION BY RANGE (cohort)",
            "PRIMARY KEY (id, cohort)",
            "UNIQUE (workflow_exec_id, event_id, cohort)",
            // The catch-all that keeps an append from ever failing.
            DEFAULT_PARTITION,
        ] {
            assert!(
                script.contains(needle),
                "the executed script must contain `{needle}`"
            );
            assert!(
                plan.contains(needle),
                "the operator plan has drifted from the executed script: it is \
                 missing `{needle}`. An operator following it would get a \
                 different (or broken) layout from the one `harvest partition \
                 enable` produces."
            );
        }

        assert!(
            !plan.contains("harvest_events_stamp_cohort"),
            "the plan must not reference the cohort-STAMPING trigger: Postgres \
             rejects a BEFORE ROW trigger that changes a partitioned row's \
             destination, and that function no longer exists"
        );
        assert!(
            !script.contains("harvest_events_stamp_cohort"),
            "nor may the executed script"
        );
    }

    /// The plan's cutover and the script's must be the same instant.
    ///
    /// The script computes `harvest_event_cohort(now())` inside the database;
    /// the plan bakes a literal in when it is printed. If the two used
    /// different rules, an operator following the plan would attach the legacy
    /// partition at a boundary the engine does not agree with — leaving either
    /// a gap (rows with no partition) or an overlap (`ATTACH` fails).
    #[test]
    fn the_plans_cutover_is_the_current_cohort_boundary() {
        let now = Utc.with_ymd_and_hms(2026, 8, 31, 17, 42, 9).unwrap();
        let plan = migration_plan(&EnableOptions::default(), now);
        let expected = cohort_start(now, DEFAULT_COHORT_WIDTH_SECS);
        assert!(
            plan.contains(&expected.to_rfc3339()),
            "the plan must attach legacy at the CURRENT cohort boundary \
             ({expected}), the same value `harvest_event_cohort(now())` \
             produces inside the script"
        );
    }

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("harvest_events"), "\"harvest_events\"");
        assert_eq!(quote_ident("odd\"name"), "\"odd\"\"name\"");
    }

    #[test]
    fn timestamp_literals_cannot_contain_a_quote() {
        let lit = ts_literal(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
        assert_eq!(
            lit.matches('\'').count(),
            2,
            "exactly the delimiters: {lit}"
        );
    }

    #[test]
    fn sweep_defaults_issue_no_row_deletes() {
        assert!(
            SweepOptions::default().straggler_grace.is_none(),
            "the default configuration must never issue a row-level DELETE \
             against harvest_events — that is the strongest reading of AC3"
        );
        assert!(SweepOptions::default().max_drops > 0);
    }

    #[test]
    fn the_default_partition_sorts_last_so_a_bounded_sweep_never_reaches_it() {
        let mut parts = [
            PartitionInfo {
                name: DEFAULT_PARTITION.to_string(),
                lower: None,
                upper: None,
                is_default: true,
            },
            PartitionInfo {
                name: "harvest_events_p_20260902000000".to_string(),
                lower: Some(Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap()),
                upper: Some(Utc.with_ymd_and_hms(2026, 9, 3, 0, 0, 0).unwrap()),
                is_default: false,
            },
            PartitionInfo {
                name: "harvest_events_p_20260901000000".to_string(),
                lower: Some(Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()),
                upper: Some(Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap()),
                is_default: false,
            },
        ];
        parts.sort_by(compare_partitions);
        assert_eq!(parts[0].name, "harvest_events_p_20260901000000");
        assert!(parts[2].is_default, "DEFAULT sorts last");
    }
}
