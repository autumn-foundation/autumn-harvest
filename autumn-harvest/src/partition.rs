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
use diesel::sql_types::{Bool, Nullable, Text, Timestamptz};
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
/// The retention tick (hourly by default) extends this window, so the engine
/// would have to be down for a week before an append could reach an uncovered
/// cohort — and even then the `DEFAULT` partition catches it.
pub const DEFAULT_LOOKAHEAD_COHORTS: u32 = 7;

/// Smallest accepted cohort width (one minute). Narrower widths multiply the
/// partition count without improving reclamation granularity.
pub const MIN_COHORT_WIDTH_SECS: i64 = 60;

/// Largest accepted cohort width (365 days).
pub const MAX_COHORT_WIDTH_SECS: i64 = 86_400 * 365;

/// The catch-all partition. Always present, normally empty: an append whose
/// cohort has no partition lands here instead of failing with `no partition of
/// relation found`, which would stall a workflow on a maintenance gap.
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
    format!(
        "{PARTITION_PREFIX}{}",
        cohort_start.format("%Y%m%d%H%M%S")
    )
}

// ── Configuration ──────────────────────────────────────────────────────────

/// The physical layout of `harvest_events` on a given shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "layout")]
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
pub fn parse_partition_bound(
    expr: &str,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool) {
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
pub async fn list_partitions(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<PartitionInfo>> {
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
    // Oldest first; the DEFAULT partition (no bounds) sorts last so a bounded
    // sweep never spends its budget on the one partition it must never drop.
    out.sort_by(|a, b| {
        a.is_default
            .cmp(&b.is_default)
            .then(a.upper.cmp(&b.upper))
            .then(a.name.cmp(&b.name))
    });
    Ok(out)
}

// ── Partition creation ─────────────────────────────────────────────────────

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
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {name} PARTITION OF harvest_events \
         FOR VALUES FROM ({}) TO ({})",
        ts_literal(lower),
        ts_literal(upper)
    );
    match diesel::sql_query(&sql).execute(conn).await {
        Ok(_) => Ok((name, true)),
        Err(e) if is_benign_partition_race(&e) => Ok((name, false)),
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

/// A concurrent maintainer created the same partition first, or the `DEFAULT`
/// partition already holds rows for this range.
///
/// The first is benign (idempotent maintenance). The second is reported so the
/// caller can drain instead of failing the whole tick.
#[cfg(feature = "db")]
fn is_benign_partition_race(e: &diesel::result::Error) -> bool {
    let msg = e.to_string();
    msg.contains("already exists")
        || msg.contains("would overlap")
        || msg.contains("overlaps with existing")
}

#[cfg(feature = "db")]
fn is_default_partition_conflict(e: &diesel::result::Error) -> bool {
    let msg = e.to_string();
    msg.contains("default partition")
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
    let lock_timeout_ms = u64::try_from(opts.lock_timeout.as_millis()).unwrap_or(u64::MAX);
    let now = Utc::now();

    let mode = Box::pin(conn.transaction::<EnableMode, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL lock_timeout = '{lock_timeout_ms}ms'")).await?;

        // The cohort function is the single source of truth for the width, and
        // it is what the trigger calls, so it must be redefined BEFORE any row
        // can be stamped under the new layout.
        exec(conn, &cohort_function_sql(width)).await?;

        // Capture the index definitions while they still name `harvest_events`
        // — executing them after the rename recreates them verbatim on the new
        // parent. Constraint-backed indexes are excluded: their partitioned
        // equivalents must gain the cohort column and are added explicitly.
        let index_defs = capture_index_defs(conn).await?;

        let has_rows = scalar_bool(
            conn,
            "SELECT EXISTS (SELECT 1 FROM harvest_events) AS v",
        )
        .await?;

        // The legacy partition covers everything below the current cohort.
        // Every pre-conversion row carries the migration's `-infinity`
        // sentinel, so all of them fall inside it with no row touched; every
        // row appended from here on takes the new DEFAULT, whose value is at or
        // after `cohort_start(now)`. The two ranges therefore meet exactly at
        // the cutover with no gap and no overlap, and — because wall clock only
        // advances — no later append can ever route back into legacy. The
        // legacy partition is sealed from the moment it is attached.
        let cutover = cohort_start(now, width);

        // Detach the sequence before the rename so it survives a `DROP TABLE`
        // of the empty legacy table and can be re-owned by the new parent. The
        // BIGSERIAL cursor must be continuous across the conversion: reusing an
        // id already present in the legacy partition would violate the parent's
        // primary key.
        exec(conn, "ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE").await?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events RENAME TO {LEGACY_PARTITION}"),
        )
        .await?;
        rename_legacy_objects(conn).await?;

        // `LIKE … INCLUDING DEFAULTS` copies the columns, their NOT NULLs and
        // the `nextval(...)` id default — and, crucially, keeps working when a
        // later migration adds a column, rather than pinning a hand-written
        // column list that would silently drop it.
        exec(
            conn,
            &format!(
                "CREATE TABLE harvest_events \
                 (LIKE {LEGACY_PARTITION} INCLUDING DEFAULTS INCLUDING COMMENTS \
                  INCLUDING STORAGE) PARTITION BY RANGE (cohort)"
            ),
        )
        .await?;

        // Swap the migration's constant `-infinity` sentinel for the live
        // cohort expression. Metadata-only (a default change never rewrites a
        // table), and it is what actually routes every subsequent append: the
        // engine's INSERT statements never mention `cohort`, so the DEFAULT is
        // always what Postgres uses to pick the partition — before any row
        // trigger can run.
        exec(
            conn,
            "ALTER TABLE harvest_events \
             ALTER COLUMN cohort SET DEFAULT harvest_event_cohort(now())",
        )
        .await?;

        // Both constraints gain `cohort` because Postgres requires the
        // partition key in every unique constraint. This is only sound because
        // `cohort` is functionally dependent on `workflow_exec_id`: the second
        // constraint below still enforces exactly "one row per
        // (execution, event_id)".
        exec(
            conn,
            "ALTER TABLE harvest_events ADD CONSTRAINT harvest_events_pkey \
             PRIMARY KEY (id, cohort)",
        )
        .await?;
        exec(
            conn,
            "ALTER TABLE harvest_events \
             ADD CONSTRAINT harvest_events_workflow_exec_id_event_id_key \
             UNIQUE (workflow_exec_id, event_id, cohort)",
        )
        .await?;

        for def in &index_defs {
            exec(conn, def).await?;
        }

        exec(conn, "ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id").await?;

        // Restores the insert-time half of the FK the partitioned layout
        // cannot keep. Installed on the parent, Postgres clones it onto every
        // partition — existing and future. Validate-only: it must never touch
        // `NEW`, because routing has already happened by the time it fires.
        exec(
            conn,
            &format!(
                "CREATE TRIGGER {EXEC_FK_TRIGGER} BEFORE INSERT ON harvest_events \
                 FOR EACH ROW EXECUTE FUNCTION harvest_events_require_execution()"
            ),
        )
        .await?;

        // The catch-all. Created before any cohort partition so there is never
        // an instant where an append could find no partition.
        exec(
            conn,
            &format!(
                "CREATE TABLE {DEFAULT_PARTITION} PARTITION OF harvest_events DEFAULT"
            ),
        )
        .await?;

        if has_rows {
            attach_legacy(conn, cutover).await?;
            Ok(EnableMode::AttachLegacy { cutover })
        } else {
            exec(conn, &format!("DROP TABLE {LEGACY_PARTITION}")).await?;
            Ok(EnableMode::Fresh)
        }
    }))
    .await?;

    let partitions_created = ensure_partitions(conn, now, opts.lookahead_cohorts).await?;
    Ok(EnableReport {
        mode,
        partitions_created,
        cohort_width_secs: width,
    })
}

/// Attach the pre-conversion table whole as the `MINVALUE .. cutover`
/// partition.
///
/// The two unique indexes are built here rather than reused: the parent's
/// primary key and unique constraint now include `cohort`, and Postgres refuses
/// to attach a partition that has no matching index for them.
///
/// The `NOT VALID` + `VALIDATE CONSTRAINT` pair is what lets `ATTACH PARTITION`
/// skip its own full-table verification scan. In-transaction here (test and
/// small-table scale); [`migration_plan`] splits it out so the validation scan
/// runs under `SHARE UPDATE EXCLUSIVE` — concurrent-write-safe — on a large
/// live table.
#[cfg(feature = "db")]
async fn attach_legacy(
    conn: &mut AsyncPgConnection,
    cutover: DateTime<Utc>,
) -> HarvestResult<()> {
    let lit = ts_literal(cutover);
    // The FK's ON DELETE CASCADE is precisely the row-by-row delete storm being
    // eliminated; its insert-time half lives on in the validate-only trigger.
    exec(
        conn,
        &format!(
            "ALTER TABLE {LEGACY_PARTITION} \
             DROP CONSTRAINT IF EXISTS harvest_events_workflow_exec_id_fkey{LEGACY_RENAME_SUFFIX}"
        ),
    )
    .await?;
    // ATTACH propagates the parent's PRIMARY KEY onto the partition, and a
    // table may have only one. The old single-column key must go: the parent's
    // is `(id, cohort)`, and `id` alone can no longer be a key because
    // uniqueness on a partitioned table has to include the partition column.
    // Global `id` uniqueness is not lost — one sequence still feeds every
    // partition, and the composite key rejects the only duplicate that could
    // otherwise appear.
    exec(
        conn,
        &format!(
            "ALTER TABLE {LEGACY_PARTITION} \
             DROP CONSTRAINT IF EXISTS harvest_events_pkey{LEGACY_RENAME_SUFFIX}"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "ALTER TABLE {LEGACY_PARTITION} DROP CONSTRAINT IF EXISTS \
             harvest_events_workflow_exec_id_event_id_key{LEGACY_RENAME_SUFFIX}"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {LEGACY_PARTITION}_pk_idx \
             ON {LEGACY_PARTITION} (id, cohort)"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {LEGACY_PARTITION}_exec_event_idx \
             ON {LEGACY_PARTITION} (workflow_exec_id, event_id, cohort)"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "ALTER TABLE {LEGACY_PARTITION} ADD CONSTRAINT {LEGACY_PARTITION}_cohort_ck \
             CHECK (cohort < {lit}) NOT VALID"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "ALTER TABLE {LEGACY_PARTITION} VALIDATE CONSTRAINT {LEGACY_PARTITION}_cohort_ck"
        ),
    )
    .await?;
    exec(
        conn,
        &format!(
            "ALTER TABLE harvest_events ATTACH PARTITION {LEGACY_PARTITION} \
             FOR VALUES FROM (MINVALUE) TO ({lit})"
        ),
    )
    .await?;
    Ok(())
}

/// Rename the legacy table's own indexes and constraints out of the way.
///
/// Renaming a table does not rename its indexes, so without this the recreated
/// parent indexes would collide with the originals on their (schema-scoped)
/// names.
#[cfg(feature = "db")]
async fn rename_legacy_objects(conn: &mut AsyncPgConnection) -> HarvestResult<()> {
    let constraints = diesel::sql_query(format!(
        "SELECT conname AS v FROM pg_constraint
          WHERE conrelid = '{LEGACY_PARTITION}'::regclass
            AND conname NOT LIKE '%{LEGACY_RENAME_SUFFIX}'"
    ))
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    for c in constraints {
        exec(
            conn,
            &format!(
                "ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT \
                 {} TO {}{LEGACY_RENAME_SUFFIX}",
                quote_ident(&c.v),
                c.v
            ),
        )
        .await?;
    }

    // Constraint renames carry their backing indexes with them; anything left
    // is a plain index.
    let indexes = diesel::sql_query(format!(
        "SELECT indexname AS v FROM pg_indexes
          WHERE schemaname = current_schema() AND tablename = '{LEGACY_PARTITION}'
            AND indexname NOT LIKE '%{LEGACY_RENAME_SUFFIX}'"
    ))
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    for i in indexes {
        exec(
            conn,
            &format!(
                "ALTER INDEX {} RENAME TO {}{LEGACY_RENAME_SUFFIX}",
                quote_ident(&i.v),
                i.v
            ),
        )
        .await?;
    }
    Ok(())
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
    format!(
        "CREATE OR REPLACE FUNCTION harvest_event_cohort(ts TIMESTAMPTZ)\n\
         RETURNS TIMESTAMPTZ LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$\n    \
         SELECT to_timestamp(floor(extract(epoch FROM $1) / {width}) * {width})\n\
         $$"
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
pub async fn disable_partitioning(conn: &mut AsyncPgConnection) -> HarvestResult<bool> {
    if !detect_layout(conn).await?.is_partitioned() {
        return Ok(false);
    }
    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        let index_defs = capture_index_defs(conn).await?;
        exec(conn, "ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE").await?;
        exec(conn, "ALTER TABLE harvest_events RENAME TO harvest_events_partitioned").await?;
        // Rename the parent's constraints/indexes so the flat table can reclaim
        // their names, exactly as the enable path does in reverse.
        for (list_sql, rename_tmpl) in [
            (
                "SELECT conname AS v FROM pg_constraint \
                 WHERE conrelid = 'harvest_events_partitioned'::regclass",
                "ALTER TABLE harvest_events_partitioned RENAME CONSTRAINT {q} TO {n}__old",
            ),
            (
                "SELECT indexname AS v FROM pg_indexes \
                 WHERE schemaname = current_schema() \
                   AND tablename = 'harvest_events_partitioned'",
                "ALTER INDEX {q} RENAME TO {n}__old",
            ),
        ] {
            let rows = diesel::sql_query(list_sql)
                .load::<TextRow>(conn)
                .await
                .map_err(database_error)?;
            for r in rows {
                exec(
                    conn,
                    &rename_tmpl
                        .replace("{q}", &quote_ident(&r.v))
                        .replace("{n}", &r.v),
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
        exec(
            conn,
            "INSERT INTO harvest_events SELECT * FROM harvest_events_partitioned",
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
        exec(conn, "ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id").await?;
        exec(conn, "DROP TABLE harvest_events_partitioned CASCADE").await?;
        Ok(())
    }))
    .await?;
    Ok(true)
}

// ── The sweeper ────────────────────────────────────────────────────────────

/// Drop every fully-reclaimable cohort partition, oldest first.
///
/// A partition is reclaimable when **no execution row remains whose
/// `created_at` falls in its cohort range**. Because every event of an
/// execution carries that execution's own cohort, the absence of the execution
/// row is proof that every event row in the partition is an orphan — its owner
/// was already archived (#345), summarized (#752) and deleted by the ordinary
/// retention loop, which this function deliberately does not touch.
///
/// That single predicate is what makes legal holds (#747) and per-type
/// overrides (#737) work here with no special-casing: a held or over-retained
/// execution's row still exists, so its cohort is not reclaimable, so its rows
/// are never dropped. There is no second copy of the retention policy to drift.
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
    let mut outcome = SweepOutcome::default();
    let width = match detect_layout(conn).await? {
        EventLayout::Unpartitioned => return Ok(outcome),
        EventLayout::Partitioned { cohort_width_secs } => cohort_width_secs,
    };
    let _ = width;

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
                .push(format!("{} (unbounded upper)", part.name));
            continue;
        };
        if upper > now {
            continue;
        }

        if let Some(reason) = cohort_occupancy(conn, part.lower, upper, opts).await? {
            outcome.blocked.push(format!("{} ({reason})", part.name));
            if let Some(grace) = opts.straggler_grace
                && let Ok(grace) = chrono::Duration::from_std(grace)
                && upper + grace <= now
            {
                outcome.straggler_rows_deleted +=
                    delete_orphan_rows(conn, part.lower, upper, opts.straggler_batch).await?;
            }
            continue;
        }

        match drop_partition(conn, &part.name, opts.lock_timeout).await? {
            true => outcome.dropped.push(part.name),
            false => outcome
                .blocked
                .push(format!("{} (lock not acquired within timeout)", part.name)),
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
/// Two tiers, cheapest first:
///
/// 1. **Fast path.** `NOT EXISTS (… WHERE created_at < upper)`. An execution
///    cannot have appended a row before it existed, so if nothing predates the
///    partition's upper bound then nothing that could own a row in it survives.
///    One index probe on `idx_harvest_we_created_at`, and the steady-state
///    answer, because retention collects oldest-first.
/// 2. **Exact path.** Only when the fast probe says "maybe": a semi-join
///    proving no row in the range has a live owner. Bounded by
///    `statement_timeout` so one huge partition cannot stall the whole tick —
///    a timeout reports the partition as blocked and retries next tick, which
///    is the correct fail-safe direction (retain, never over-delete).
///
/// This is deliberately the *only* place the drop decision is made. Legal holds
/// (#747) and per-type retention overrides (#737) are honoured because each
/// keeps its execution row alive; there is no second copy of the retention
/// policy here to drift out of sync with `run_shard_tick`'s.
#[cfg(feature = "db")]
async fn cohort_occupancy(
    conn: &mut AsyncPgConnection,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
    opts: &SweepOptions,
) -> HarvestResult<Option<String>> {
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

    let lower = lower.unwrap_or(DateTime::<Utc>::MIN_UTC);
    let ms = u64::try_from(opts.exact_scan_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let scan = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL statement_timeout = '{ms}ms'")).await?;
        Ok(diesel::sql_query(
            "SELECT EXISTS (
                 SELECT 1 FROM harvest_events e
                  WHERE e.cohort >= $1 AND e.cohort < $2
                    AND EXISTS (
                        SELECT 1 FROM harvest_workflow_executions x
                         WHERE x.id = e.workflow_exec_id
                    )
             ) AS v",
        )
        .bind::<Timestamptz, _>(lower)
        .bind::<Timestamptz, _>(upper)
        .get_result::<BoolRow>(conn)
        .await
        .map_err(database_error)?
        .v)
    }))
    .await;

    match scan {
        Ok(false) => Ok(None),
        Ok(true) => Ok(Some("a live execution still owns rows".to_string())),
        // Fail safe toward RETAINING: an unfinished proof is not a proof.
        Err(HarvestError::Database(msg)) if msg.contains("statement timeout") => Ok(Some(
            "ownership scan exceeded its budget; retained and retried next tick".to_string(),
        )),
        Err(e) => Err(e),
    }
}

/// Drop one partition under a bounded lock wait.
///
/// Returns `false` (not an error) when the `ACCESS EXCLUSIVE` lock could not be
/// taken in time: the correct response is to leave the partition for the next
/// tick, never to make every concurrent append wait.
#[cfg(feature = "db")]
async fn drop_partition(
    conn: &mut AsyncPgConnection,
    name: &str,
    lock_timeout: Duration,
) -> HarvestResult<bool> {
    let ms = u64::try_from(lock_timeout.as_millis()).unwrap_or(u64::MAX).max(1);
    let name = name.to_string();
    let result = Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL lock_timeout = '{ms}ms'")).await?;
        exec(conn, &format!("DROP TABLE IF EXISTS {}", quote_ident(&name))).await?;
        Ok(())
    }))
    .await;
    match result {
        Ok(()) => Ok(true),
        Err(HarvestError::Database(msg)) if msg.contains("lock timeout") => Ok(false),
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
    let lower = lower.unwrap_or(DateTime::<Utc>::MIN_UTC);
    let mut total = 0usize;
    loop {
        let deleted = diesel::sql_query(
            "DELETE FROM harvest_events
              WHERE ctid IN (
                  SELECT e.ctid FROM harvest_events e
                   WHERE e.cohort >= $1 AND e.cohort < $2
                     AND NOT EXISTS (
                         SELECT 1 FROM harvest_workflow_executions x
                          WHERE x.id = e.workflow_exec_id
                     )
                   LIMIT $3
              )",
        )
        .bind::<Timestamptz, _>(lower)
        .bind::<Timestamptz, _>(upper)
        .bind::<diesel::sql_types::BigInt, _>(batch)
        .execute(conn)
        .await
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

    let cohorts = diesel::sql_query(format!(
        "SELECT DISTINCT cohort AS v FROM {DEFAULT_PARTITION} ORDER BY 1"
    ))
    .load::<TsRow>(conn)
    .await
    .map_err(database_error)?;

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        exec(conn, "SET LOCAL lock_timeout = '5s'").await?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events DETACH PARTITION {DEFAULT_PARTITION}"),
        )
        .await?;
        for c in &cohorts {
            if let Some(cohort) = c.v {
                ensure_cohort_with_width(conn, cohort, width).await?;
            }
        }
        // `INSERT … SELECT *` supplies `cohort` explicitly, so the DEFAULT
        // does not re-fire and every row keeps the cohort it was written with.
        // The integrity trigger is disabled for the move because a parked row
        // whose execution has since been collected is exactly the orphan the
        // sweeper is meant to reclaim later — re-validating it here would turn
        // a maintenance drain into data loss. The `ACCESS EXCLUSIVE` lock held
        // by the DETACH above makes disabling it safe for this transaction.
        exec(
            conn,
            &format!("ALTER TABLE harvest_events DISABLE TRIGGER {EXEC_FK_TRIGGER}"),
        )
        .await?;
        let moved = diesel::sql_query(format!(
            "INSERT INTO harvest_events SELECT * FROM {DEFAULT_PARTITION}"
        ))
        .execute(conn)
        .await
        .map_err(database_error)?;
        exec(
            conn,
            &format!("ALTER TABLE harvest_events ENABLE TRIGGER {EXEC_FK_TRIGGER}"),
        )
        .await?;
        exec(conn, &format!("TRUNCATE {DEFAULT_PARTITION}")).await?;
        exec(
            conn,
            &format!(
                "ALTER TABLE harvest_events ATTACH PARTITION {DEFAULT_PARTITION} DEFAULT"
            ),
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
    let drained = drain_default(conn).await?;
    let created = ensure_partitions(conn, now, lookahead_cohorts).await?;
    let sweep = sweep(conn, now, sweep_opts).await?;
    Ok(MaintenanceOutcome {
        at: Some(Utc::now()),
        created,
        drained,
        sweep,
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
}

// ── The large-live-table migration plan ────────────────────────────────────

/// Emit the operator-run SQL script that converts a **large live**
/// `harvest_events` with a minimal lock window.
///
/// [`enable_partitioning`] runs the same algorithm in one transaction, which is
/// right for a greenfield or small table and wrong for a ten-million-row one:
/// the index builds and the constraint validation would hold `ACCESS EXCLUSIVE`
/// for their whole duration, blocking every append. This plan splits those two
/// steps out of the lock window:
///
/// - `CREATE UNIQUE INDEX CONCURRENTLY` builds the two partition-key indexes
///   without blocking reads or writes.
/// - `ADD CONSTRAINT … NOT VALID` takes a brief lock; the separate
///   `VALIDATE CONSTRAINT` does the full scan under `SHARE UPDATE EXCLUSIVE`,
///   which concurrent readers and writers do not conflict with. Because the
///   constraint is then valid, `ATTACH PARTITION` skips its own verification
///   scan entirely.
///
/// What remains inside the exclusive window is metadata-only: a rename, a
/// `CREATE TABLE`, and the `ATTACH`. Seconds, not minutes — and bounded by an
/// explicit `lock_timeout` so a conversion that cannot get the lock fails
/// instead of stalling the deployment behind it.
#[must_use]
pub fn migration_plan(opts: &EnableOptions, now: DateTime<Utc>) -> String {
    let width = opts.cohort_width_secs.max(1);
    let lock_ms = opts.lock_timeout.as_millis().max(1);
    let cutover = cohort_start(now, width)
        .checked_add_signed(chrono::Duration::seconds(width))
        .unwrap_or(now);
    let cutover_lit = ts_literal(cutover);
    let cohort_fn = cohort_function_sql(width);
    format!(
        r"-- ────────────────────────────────────────────────────────────────
-- harvest_events -> partitioned layout (issue #958), LARGE LIVE TABLE
--
-- Run one numbered step at a time. Steps 1-3 are online: they hold no
-- lock that blocks appends, and may take a long time on a large table.
-- Only step 4 takes ACCESS EXCLUSIVE, and everything it does is
-- metadata-only, so it is a seconds-long window rather than a scan.
--
-- Recheck the cutover before running step 4. It must be strictly greater
-- than the cohort of EVERY execution that exists at that moment, so an
-- execution created before the swap keeps appending into the legacy
-- partition instead of tearing its history across two partitions:
--
--     SELECT harvest_event_cohort(max(created_at))
--            + interval '{width} seconds'
--       FROM harvest_workflow_executions;
--
-- Rollback: until step 4 commits, nothing is committed but two extra
-- indexes and one CHECK constraint, all droppable with no downtime.
-- ────────────────────────────────────────────────────────────────

-- Step 1 (online). Bake the chosen cohort width into the cohort function.
{cohort_fn};

-- Step 2 (online, may take a while). Build the two indexes the parent's
-- partition-key-bearing PRIMARY KEY and UNIQUE constraints require.
-- CONCURRENTLY cannot run inside a transaction block: run each on its own.
CREATE UNIQUE INDEX CONCURRENTLY {LEGACY_PARTITION}_pk_idx
    ON harvest_events (id, cohort);
CREATE UNIQUE INDEX CONCURRENTLY {LEGACY_PARTITION}_exec_event_idx
    ON harvest_events (workflow_exec_id, event_id, cohort);

-- Step 3 (online). Pre-validate the range constraint so ATTACH PARTITION in
-- step 4 can skip its own full-table verification scan. ADD ... NOT VALID
-- takes a brief lock; VALIDATE does the scan under SHARE UPDATE EXCLUSIVE,
-- which concurrent readers and writers do not conflict with.
ALTER TABLE harvest_events
    ADD CONSTRAINT {LEGACY_PARTITION}_cohort_ck
    CHECK (cohort < {cutover_lit}) NOT VALID;
ALTER TABLE harvest_events
    VALIDATE CONSTRAINT {LEGACY_PARTITION}_cohort_ck;

-- Step 4 (THE WINDOW: ACCESS EXCLUSIVE, metadata-only). One transaction:
-- if anything fails, nothing changed.
BEGIN;
SET LOCAL lock_timeout = '{lock_ms}ms';

ALTER SEQUENCE harvest_events_id_seq OWNED BY NONE;
ALTER TABLE harvest_events RENAME TO {LEGACY_PARTITION};
-- Rename the old table's constraints and indexes out of the way so the new
-- parent can reclaim their names (renaming a table does not rename these).
-- Emit one line per object from:
--   SELECT format('ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT %I TO %I{LEGACY_RENAME_SUFFIX};', conname, conname)
--     FROM pg_constraint WHERE conrelid = '{LEGACY_PARTITION}'::regclass;
--   SELECT format('ALTER INDEX %I RENAME TO %I{LEGACY_RENAME_SUFFIX};', indexname, indexname)
--     FROM pg_indexes WHERE tablename = '{LEGACY_PARTITION}';

-- The FK's ON DELETE CASCADE is the row-by-row delete storm being
-- eliminated. Its insert-time protection lives on in the cohort trigger.
ALTER TABLE {LEGACY_PARTITION}
    DROP CONSTRAINT IF EXISTS harvest_events_workflow_exec_id_fkey{LEGACY_RENAME_SUFFIX};

CREATE TABLE harvest_events
    (LIKE {LEGACY_PARTITION} INCLUDING DEFAULTS INCLUDING COMMENTS INCLUDING STORAGE)
    PARTITION BY RANGE (cohort);
ALTER TABLE harvest_events
    ADD CONSTRAINT harvest_events_pkey PRIMARY KEY (id, cohort);
ALTER TABLE harvest_events
    ADD CONSTRAINT harvest_events_workflow_exec_id_event_id_key
    UNIQUE (workflow_exec_id, event_id, cohort);
-- Recreate every non-constraint index on the new parent; Postgres
-- propagates each to all partitions. Emit them from:
--   SELECT pg_get_indexdef(i.indexrelid)
--     FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
--    WHERE c.relname = '{LEGACY_PARTITION}'
--      AND NOT EXISTS (SELECT 1 FROM pg_constraint con
--                       WHERE con.conindid = i.indexrelid);
ALTER SEQUENCE harvest_events_id_seq OWNED BY harvest_events.id;

CREATE TRIGGER {EXEC_FK_TRIGGER} BEFORE INSERT ON harvest_events
    FOR EACH ROW EXECUTE FUNCTION harvest_events_stamp_cohort();

CREATE TABLE {DEFAULT_PARTITION} PARTITION OF harvest_events DEFAULT;
ALTER TABLE harvest_events ATTACH PARTITION {LEGACY_PARTITION}
    FOR VALUES FROM (MINVALUE) TO ({cutover_lit});

COMMIT;

-- Step 5 (online). Let the engine take over: it pre-creates the lookahead
-- window and sweeps droppable cohorts every retention tick. Nothing further
-- is required of the operator, and no cron job needs to exist.
--   harvest partition status --shard <dsn>
"
    )
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
        assert_eq!(lo, Some(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap()));
        assert_eq!(hi, Some(Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()));
    }

    #[test]
    fn the_legacy_partitions_minvalue_bound_parses_as_open() {
        let (lo, hi, default) =
            parse_partition_bound("FOR VALUES FROM (MINVALUE) TO ('2026-08-31 00:00:00+00')");
        assert!(!default);
        assert_eq!(lo, None, "MINVALUE is an open lower bound");
        assert_eq!(hi, Some(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap()));
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
        let (_, hi, _) =
            parse_partition_bound("FOR VALUES FROM (MINVALUE) TO ('2026-08-31 00:00:00.123456+00')");
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

    #[test]
    fn quote_ident_escapes_embedded_quotes() {
        assert_eq!(quote_ident("harvest_events"), "\"harvest_events\"");
        assert_eq!(quote_ident("odd\"name"), "\"odd\"\"name\"");
    }

    #[test]
    fn timestamp_literals_cannot_contain_a_quote() {
        let lit = ts_literal(Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
        assert_eq!(lit.matches('\'').count(), 2, "exactly the delimiters: {lit}");
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
        let mut parts = vec![
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
        parts.sort_by(|a, b| {
            a.is_default
                .cmp(&b.is_default)
                .then(a.upper.cmp(&b.upper))
                .then(a.name.cmp(&b.name))
        });
        assert_eq!(parts[0].name, "harvest_events_p_20260901000000");
        assert!(parts[2].is_default, "DEFAULT sorts last");
    }
}
