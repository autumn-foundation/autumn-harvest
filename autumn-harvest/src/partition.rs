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

/// The `COMMENT` `enable` stamps on `idx_harvest_we_created_at`.
///
/// Stamped the moment `enable` actually creates that index. Never stamped
/// when `CREATE INDEX IF NOT EXISTS` finds an operator's own index already
/// at that name.
///
/// A shape check alone cannot tell harvest's index apart from an
/// operator's own index of the identical shape. Building an index on
/// `harvest_workflow_executions (created_at)` independently is an
/// ordinary thing to do. Recording ownership at creation time removes the
/// ambiguity instead of guessing from the result.
const WE_CREATED_AT_IDX_OWNERSHIP_TAG: &str =
    "harvest #958: created by partition enable, safe to drop on disable";

/// A boolean SQL expression, evaluating to exactly one row.
///
/// Checks that `idx_harvest_we_created_at` exists and carries
/// [`WE_CREATED_AT_IDX_OWNERSHIP_TAG`]. Also checks it has the shape
/// `enable` creates: a plain (non-unique) single-column btree on
/// `harvest_workflow_executions (created_at)`, with default opclass and no
/// predicate or expression. `false` (not an error) when the index does not
/// exist at all.
///
/// [`disable_partitioning`] uses this. It drops the index only when BOTH
/// signals agree. The tag alone could in principle survive some
/// hypothetical future `ALTER INDEX`. The shape alone cannot distinguish
/// harvest's index from an operator's identically shaped one. Requiring
/// both is the conservative choice: an index missing either signal is
/// left alone.
#[cfg(feature = "db")]
fn we_created_at_idx_owned_check_sql() -> String {
    format!(
        "SELECT COALESCE((
       SELECT i.indrelid = 'harvest_workflow_executions'::regclass
              AND NOT i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL
              AND i.indnkeyatts = 1 AND i.indnatts = 1
              AND i.indkey[0] = (SELECT a.attnum FROM pg_attribute a
                                  WHERE a.attrelid = 'harvest_workflow_executions'::regclass
                                    AND a.attname = 'created_at')
              AND NOT EXISTS (
                  SELECT 1 FROM unnest(i.indclass) AS oc(opclass)
                  JOIN pg_opclass op ON op.oid = oc.opclass
                 WHERE NOT op.opcdefault)
              AND obj_description(c.oid, 'pg_class') = \
                  $harvest_we_idx_tag_958${WE_CREATED_AT_IDX_OWNERSHIP_TAG}$harvest_we_idx_tag_958$
         FROM pg_class c
         JOIN pg_index i ON i.indexrelid = c.oid
         JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = 'idx_harvest_we_created_at' AND n.nspname = current_schema()
     ), false) AS v"
    )
}

/// Name of the transient function [`bounded_rename_fn_sql`] defines,
/// schema-qualified into `pg_temp`.
///
/// Review finding: a bare, fixed name in the application schema is not
/// guaranteed free. An operator's own schema could already hold a function
/// of this exact name and signature. The plain `CREATE FUNCTION` below
/// would then abort the conversion. For the large-table plan, that abort
/// lands inside phase 4, after phases 2 and 3 already paid for the
/// expensive online preparation. `pg_temp` is each session's private,
/// connection-scoped schema. No persistent object can ever occupy a name
/// there ahead of time, so this can never collide. Nothing is left behind
/// either, even if the session ends before the trailing `DROP FUNCTION`
/// runs.
const BOUNDED_RENAME_FN: &str = "pg_temp.harvest_bounded_rename_958";

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
    /// Convert even when a logical-replication publication covers
    /// `harvest_events`.
    ///
    /// See [`incompatible_publications`] for the two ways that breaks a standby
    /// and why `publish_via_partition_root` alone is not enough. Set this only
    /// when the subscriber runs the partitioned layout too, or when the
    /// publication is not feeding a Harvest standby at all.
    pub allow_incompatible_publications: bool,
}

impl Default for EnableOptions {
    fn default() -> Self {
        Self {
            cohort_width_secs: DEFAULT_COHORT_WIDTH_SECS,
            lookahead_cohorts: DEFAULT_LOOKAHEAD_COHORTS,
            lock_timeout: Duration::from_secs(5),
            allow_incompatible_publications: false,
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
    /// Maximum partitions to *evaluate* in one pass, dropped or not.
    ///
    /// `max_drops` bounds successful drops, not the work of finding them. A
    /// blocked partition still costs a full gate evaluation — up to a tier-3
    /// scan under `exact_scan_timeout`. It does not count against
    /// `max_drops`. A shard with one long-lived execution pinning many old
    /// cohorts can then spend an entire tick evaluating every closed
    /// partition. It drops none, at up to `partitions × exact_scan_timeout`.
    /// This field bounds that cost directly. [`SweepOutcome::truncated`]
    /// reports when the budget was reached before every partition was
    /// considered.
    pub max_attempts: usize,
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
            max_attempts: 128,
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
    /// Whether this pass stopped before every partition was considered,
    /// because it hit [`SweepOptions::max_drops`] or
    /// [`SweepOptions::max_attempts`].
    ///
    /// Not itself a problem — bounded passes that converge over successive
    /// ticks are the design. An operator reading a pass that dropped and
    /// blocked nothing needs to know which state that is. Either "the shard
    /// is clean", or "the pass ran out of budget before it looked at the
    /// rest".
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
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
struct CohortCountRow {
    #[diesel(sql_type = Nullable<Timestamptz>)]
    v: Option<DateTime<Utc>>,
    #[diesel(sql_type = BigInt)]
    n: i64,
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

    // `pg_get_expr` renders a bound's timestamp literals in the session's
    // `DateStyle`, not a fixed format — issue #1270 item 15. The parser
    // below accepts only ISO year-first forms. A connection (or a pooler
    // that inherited a non-default setting) using, say, `SQL, DMY` would
    // parse every finite bound as `None`. Existing cohorts would then fail
    // the exact-bound check. `ensure_partitions` would error on every tick,
    // and the sweeper would treat bounded partitions as unbounded rather
    // than reclaiming them. `SET LOCAL` inside an explicit transaction
    // scopes the override to this one query, so it never leaks onto a
    // pooled connection reused for something else afterward.
    let rows = Box::pin(conn.transaction::<Vec<Row>, HarvestError, _>(async |conn| {
        exec(conn, "SET LOCAL DateStyle = 'ISO, MDY'").await?;
        diesel::sql_query(
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
        .map_err(database_error)
    }))
    .await?;

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
// Used by `list_partitions` and by its unit test. Both are gated — the
// former on `db`, the latter on `test` — so without this the function is
// dead code in a `--no-default-features` build, which is how dependent
// crates (autumn-harvest-sqlite) compile the library.
#[cfg(any(feature = "db", test))]
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
    ensure_cohort_with_width(conn, ts, width, Duration::from_secs(2)).await
}

#[cfg(feature = "db")]
async fn ensure_cohort_with_width(
    conn: &mut AsyncPgConnection,
    ts: DateTime<Utc>,
    width: i64,
    lock_timeout: Duration,
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
    // Bounded, and in its own transaction so the bound is scoped to it.
    //
    // `CREATE TABLE ... PARTITION OF` takes ACCESS EXCLUSIVE on the parent, and
    // Postgres queues later conflicting requests behind a WAITER rather than
    // only behind held locks — so behind one long history query still holding
    // ACCESS SHARE, this statement waits and every append arriving after it
    // queues behind this statement. Unbounded, that turns a routine maintenance
    // tick into a shard-wide write outage that ends when the reader does.
    //
    // A cohort that cannot be carved out in time is reported blocked and
    // retried next tick, which is what `ensure_partitions` already does with
    // every other per-cohort failure.
    let ms = u64::try_from(lock_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let create = Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL lock_timeout = '{ms}ms'")).await?;
        exec(conn, &sql).await
    }))
    .await;
    match create {
        // Verified rather than assumed, for the same reason: the statement can
        // succeed without having created the partition we asked for.
        Ok(()) => {
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
        Err(e) if is_benign_partition_race(&e.to_string()) => {
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
        Err(e) if is_default_partition_conflict(&e.to_string()) => {
            Err(HarvestError::Database(format!(
                "cannot create {name}: the DEFAULT partition holds rows for its range; \
             drain it first (partition::drain_default, or `harvest partition maintain`). \
             underlying error: {e}"
            )))
        }
        Err(e) => Err(e),
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
fn is_benign_partition_race(msg: &str) -> bool {
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
fn is_default_partition_conflict(msg: &str) -> bool {
    // Matched on the message rather than on `DatabaseErrorKind`: the DDL now
    // runs inside a transaction helper that reports failures as
    // `HarvestError::Database`, so the typed variant is no longer in hand. The
    // SQLSTATE is carried in the message either way.
    msg.contains("default partition") || msg.contains("23514")
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

/// Postgres broke a lock cycle by aborting this transaction (SQLSTATE `40P01`).
///
/// Retryable by construction: the work the loser was doing is still there to be
/// done, and the winner has moved on.
#[must_use]
pub fn is_deadlock(msg: &str) -> bool {
    msg.contains("40P01") || msg.contains("deadlock detected")
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
/// Returns `(created, blocked)`. `blocked` names each cohort in the window
/// that could not be carved out this pass. Causes include a generated name
/// colliding with an unrelated relation, or a bounded lock attempt that ran
/// out of time. This can happen even when the rest of the window was
/// created successfully. A caller that only looks at `created` cannot tell
/// "the window is fully covered" from "part of it is not". That is exactly
/// the distinction an uncovered write range needs reported.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog or DDL failure, or when NONE of the
/// window could be covered (every step blocked).
#[cfg(feature = "db")]
pub async fn ensure_partitions(
    conn: &mut AsyncPgConnection,
    now: DateTime<Utc>,
    lookahead_cohorts: u32,
    lock_timeout: Duration,
) -> HarvestResult<(Vec<String>, Vec<String>)> {
    let width = match detect_layout(conn).await? {
        EventLayout::Unpartitioned => return Ok((Vec::new(), Vec::new())),
        EventLayout::Partitioned { cohort_width_secs } => cohort_width_secs,
    };
    let mut created = Vec::new();
    let mut blocked: Vec<String> = Vec::new();
    let mut attempted = 0usize;
    for step in 0..=i64::from(lookahead_cohorts) {
        let Some(at) = now.checked_add_signed(chrono::Duration::seconds(width * step)) else {
            break;
        };
        attempted += 1;
        match ensure_cohort_with_width(conn, at, width, lock_timeout).await {
            Ok((name, true)) => created.push(name),
            Ok((_, false)) => {}
            // One cohort that cannot be carved out must not stop the REST of
            // the window from being created.
            //
            // The case that matters: a `drain_default` that lost its bounded
            // lock attempt leaves current-cohort rows in the DEFAULT partition,
            // and Postgres then rejects creating THAT cohort because the
            // updated default constraint would be violated. Aborting the loop
            // there would stop coverage extension during exactly the
            // maintenance-gap recovery it is meant to protect — every later
            // cohort would keep piling into DEFAULT until a drain finally
            // succeeded, making the gap self-perpetuating.
            //
            // Later cohorts are almost never represented in DEFAULT, so they
            // are created normally and the write window is restored; the
            // blocked one is retried next tick, after the drain.
            Err(e) => {
                tracing::warn!(
                    cohort = %cohort_start(at, width),
                    error = %e,
                    "harvest could not create a cohort partition; continuing with the rest \
                     of the lookahead window"
                );
                blocked.push(cohort_start(at, width).to_rfc3339());
            }
        }
    }
    if attempted > 0 && blocked.len() == attempted {
        // EVERY cohort in this window was blocked. Not merely
        // `created.is_empty()`. A prior pass's already-attached cohorts
        // (`Ok((_, false))`) are tracked in neither `created` nor
        // `blocked`. `created` can stay empty even when the window is
        // nearly fully covered and only the newest cohort is blocked.
        // That is a partial success, not the total failure this error
        // reports.
        return Err(HarvestError::Database(format!(
            "no cohort partition could be created; appends will land in \
             {DEFAULT_PARTITION}. blocked cohorts: {}",
            blocked.join(", ")
        )));
    }
    Ok((created, blocked))
}

// ── Enabling the layout ────────────────────────────────────────────────────

/// Publications that cover `harvest_events`, all of which the partitioned
/// layout is incompatible with.
///
/// `docs/cross-region-dr.md` tells an operator to run `CREATE PUBLICATION
/// harvest_dr FOR ALL TABLES` on the primary and to apply Harvest's migrations
/// on the standby first, "logical replication does not carry DDL". Converting
/// under that breaks the standby **twice**, and only the first break looks like
/// a configuration problem.
///
/// **The leaf names.** `publish_via_partition_root` defaults to false, so a
/// partitioned table's changes are published under the names of the *leaf*
/// partitions — `harvest_events_p20260901000000`, `harvest_events_legacy`. The
/// standby has only the flat `harvest_events` the migrations created, and the
/// partitions come from DDL that is not replicated, so those relations will
/// never exist there. The subscription's apply worker stops on the first event
/// after the conversion.
///
/// **The deletes, which `publish_via_partition_root = true` does not fix.**
/// The partitioned layout drops the `ON DELETE CASCADE` foreign key on purpose
/// — that cascade *is* the delete storm being eliminated — so deleting an
/// execution no longer deletes its events. The rows go away when their
/// partition is dropped, and `DROP TABLE` is DDL: logical replication does not
/// carry it in any configuration. The subscriber's own cascade cannot cover for
/// that either, because apply runs with replica trigger behaviour and
/// referential-integrity triggers do not fire under it. So the standby's
/// `harvest_events` keeps every event forever, and
/// [`crate::backup_verify::FindingKind::DanglingEventExecution`] is an
/// *Incoherent* finding — "do not start workers". The standby stops being
/// failover-capable, which is the one thing it exists for.
///
/// Nothing about either break is loud. The primary keeps accepting writes, and
/// the second one does not even stop the subscription: it just quietly makes
/// the copy unrestorable.
///
/// So [`enable_partitioning`] refuses while any publication covers the table,
/// rather than only the ones with `pubviaroot = false` — treating
/// `publish_via_partition_root` as sufficient is precisely the trap. The
/// workable configuration is the partitioned layout on **both** sides with
/// `publish_via_partition_root = true`, where the standby's own maintenance can
/// reclaim once promoted; [`EnableOptions::allow_incompatible_publications`] is
/// the operator's statement that they have done that, or that the publication
/// is not feeding a Harvest standby at all.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog failure.
#[cfg(feature = "db")]
pub async fn incompatible_publications(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    // `pg_publication_tables` resolves FOR ALL TABLES, FOR TABLES IN SCHEMA and
    // explicit table lists alike, so there is one query rather than three
    // catalog shapes to keep in step.
    let rows = diesel::sql_query(
        "SELECT DISTINCT t.pubname AS v
           FROM pg_publication_tables t
          WHERE t.schemaname = current_schema()
            AND t.tablename = 'harvest_events'
          ORDER BY 1",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(|r| r.v).collect())
}

/// Row-level security configured on the events table, if any.
///
/// Returns the policy names, plus whether row security is enabled at all — a
/// table can have `ENABLE ROW LEVEL SECURITY` with no policy yet, which denies
/// all rows to non-owners and is very much a configuration a swap must not
/// silently discard.
///
/// **Why this blocks the conversion.** Both conversion paths replace
/// `harvest_events` with a table built by `CREATE TABLE ... (LIKE ...)`, and
/// `LIKE` copies neither `relrowsecurity`/`relforcerowsecurity` nor any
/// `pg_policy` entry — measured, not assumed. `copy_acl_sql` then faithfully
/// replays the original's owner and grants onto that replacement, so the same
/// roles reach a parent on which row security is simply off. Rows a policy had
/// been filtering become readable the moment the conversion commits, and
/// nothing about it is loud.
///
/// That is the same failure the ACL clearing exists to prevent, arriving by a
/// different route, which is why this refuses rather than warns.
///
/// It refuses rather than recreating the configuration deliberately. A policy
/// carries a command, a permissive/restrictive mode, a role list and two
/// separate expressions, and on a partitioned parent the policies that matter
/// are the ones on the parent AND on every partition. Replaying that wrongly
/// is itself an exposure, so this reports what it found and leaves the decision
/// with the operator, exactly as the publication guard does.
///
/// # Errors
///
/// [`HarvestError::Database`] if either catalog query fails.
#[cfg(feature = "db")]
pub async fn row_security_config(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<(bool, Vec<String>)> {
    let enabled = scalar_bool(
        conn,
        "SELECT COALESCE(bool_or(c.relrowsecurity OR c.relforcerowsecurity), false) AS v
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()",
    )
    .await?;
    let policies = diesel::sql_query(
        "SELECT p.polname AS v
           FROM pg_policy p
           JOIN pg_class c ON c.oid = p.polrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()
          ORDER BY 1",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok((enabled, policies.into_iter().map(|r| r.v).collect()))
}

/// The refusal both conversion directions share.
#[cfg(feature = "db")]
async fn refuse_if_row_security(conn: &mut AsyncPgConnection, verb: &str) -> HarvestResult<()> {
    let (enabled, policies) = row_security_config(conn).await?;
    if !enabled && policies.is_empty() {
        return Ok(());
    }
    let what = if policies.is_empty() {
        "row level security is enabled on it (with no policy, which denies every row to \
         non-owners)"
            .to_string()
    } else {
        format!(
            "it carries row security {} ({})",
            if policies.len() == 1 {
                "policy"
            } else {
                "policies"
            },
            policies.join(", ")
        )
    };
    Err(HarvestError::Config(format!(
        "refusing to {verb} harvest_events: {what}. The conversion replaces the table with \
         one built by CREATE TABLE ... (LIKE ...), which copies neither the row-security \
         flags nor any policy, while the owner and grants ARE replayed onto it — so the \
         same roles would reach a table with row security off, and rows a policy had been \
         filtering would become readable the moment this commits. Recreating the policies \
         automatically is not offered because replaying one wrongly is the same exposure by \
         another route. Drop the policies (and DISABLE ROW LEVEL SECURITY) if they are \
         obsolete, or reproduce them on the converted layout by hand afterwards."
    )))
}

/// The `NOT EXISTS (...)` fragment that exempts harvest's own two
/// constraints from the unique-index refusal below. The direct path, the
/// scripted plan, and their respective in-lock rechecks all share it, so
/// the four copies of that refusal cannot drift apart.
///
/// Review finding: naming these two constraints was not enough on its
/// own. An operator's own constraint could reuse one of these two
/// conventional names. Say, a replacement for
/// `harvest_events_workflow_exec_id_event_id_key` defined as `UNIQUE
/// (event_type)`. Name-only matching treated that as harvest-owned. Its
/// real index would then be excluded from replay. The hard-coded `ADD
/// CONSTRAINT` step would recreate HARVEST's shape under that name
/// instead, silently discarding the operator's own uniqueness guarantee.
/// Verifying `contype` and the exact ordered key columns closes that
/// gap. Only a constraint shaped exactly like harvest's own pkey
/// (`(id)`) or unique key (`(workflow_exec_id, event_id)`) is exempt
/// now. An impostor of the same name no longer qualifies.
const HARVEST_OWNED_CONSTRAINT_EXEMPTION_SQL: &str = "NOT EXISTS (\n                SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid\n                  AND (\n                      (con.conname = 'harvest_events_pkey' AND con.contype = 'p'\n                       AND (SELECT array_agg(a.attname::text ORDER BY k)\n                              FROM generate_series(0, i.indnkeyatts - 1) k\n                              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[k]\n                           ) = ARRAY['id'])\n                      OR\n                      (con.conname = 'harvest_events_workflow_exec_id_event_id_key' AND con.contype = 'u'\n                       AND (SELECT array_agg(a.attname::text ORDER BY k)\n                              FROM generate_series(0, i.indnkeyatts - 1) k\n                              JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[k]\n                           ) = ARRAY['workflow_exec_id', 'event_id'])\n                  )\n            )";

/// User-defined unique indexes on `harvest_events` that cannot survive
/// conversion unchanged: either missing `cohort`, or backed by a
/// constraint the replay step cannot carry forward.
///
/// Excludes only the two constraint-backed indexes the enable script itself
/// owns and replaces: `harvest_events_pkey` and
/// `harvest_events_workflow_exec_id_event_id_key`, both of which add
/// `cohort` explicitly.
///
/// Review finding: naming harvest's own two constraints explicitly (rather
/// than exempting every constraint-backed index) closed one gap, but left
/// another. `capture_index_defs` skips every constraint-backed index
/// unconditionally. It does this regardless of whether the index carries
/// `cohort`. An operator's own compatible constraint, say `UNIQUE
/// (external_id, cohort)`, now passes THIS check, since cohort is
/// present. It is still silently dropped by that replay step. No code
/// path recreates any constraint but harvest's own two. Flagging every
/// non-harvest constraint-backed index here, regardless of `cohort`,
/// closes that gap. There is no support for replaying an arbitrary
/// constraint, so any operator constraint refuses the conversion rather
/// than being silently lost.
///
/// **Why an ordinary index without `cohort` blocks the conversion.**
/// Postgres requires the partition key in every unique index on a
/// partitioned table. `capture_index_defs` replays every non-constraint
/// index verbatim onto the new parent. A unique index that predates
/// partitioning and does not carry `cohort` is a perfectly ordinary index
/// on the flat layout. It makes `CREATE UNIQUE INDEX` fail once the parent
/// is partitioned.
///
/// Checked against only the first `indnkeyatts` entries of `indkey` — the
/// true key columns. An `INCLUDE`d column does not participate in
/// uniqueness. `cohort` sitting there does not satisfy the requirement,
/// even though it is present in the wider `indkey` array.
///
/// # Errors
///
/// [`HarvestError::Database`] if the catalog query fails.
#[cfg(feature = "db")]
pub async fn unique_indexes_missing_cohort(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Vec<String>> {
    let rows = diesel::sql_query(format!(
        "SELECT i.indexrelid::regclass::text AS v
           FROM pg_index i
           JOIN pg_class c ON c.oid = i.indrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_attribute cohort_attr
             ON cohort_attr.attrelid = c.oid AND cohort_attr.attname = 'cohort'
          WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()
            AND i.indisunique
            AND {HARVEST_OWNED_CONSTRAINT_EXEMPTION_SQL}
            AND (
                EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
                OR NOT EXISTS (
                    SELECT 1 FROM generate_series(0, i.indnkeyatts - 1) k
                     WHERE i.indkey[k] = cohort_attr.attnum
                )
            )
          ORDER BY 1"
    ))
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(|r| r.v).collect())
}

/// The refusal [`enable_partitioning`] and the scripted plan share.
///
/// Detected before anything mutates, exactly like the row-security and
/// publication guards. A mid-conversion `duplicate_object` /
/// `insufficient columns in UNIQUE constraint definition` from Postgres
/// names neither the index nor why it is unsupported.
#[cfg(feature = "db")]
async fn refuse_if_unique_index_without_cohort(
    conn: &mut AsyncPgConnection,
    verb: &str,
) -> HarvestResult<()> {
    let bad = unique_indexes_missing_cohort(conn).await?;
    if bad.is_empty() {
        return Ok(());
    }
    Err(HarvestError::Config(format!(
        "refusing to {verb} harvest_events: {} that cannot survive conversion unchanged \
         ({}). Either it does not include `cohort` — Postgres requires the partition key \
         in every unique index on a partitioned table, so replaying it onto the \
         partitioned parent would fail — or it is backed by a table constraint other \
         than harvest's own PRIMARY KEY and (workflow_exec_id, event_id) UNIQUE \
         constraint, which the conversion has no way to replay and would otherwise drop \
         silently. Adding `cohort` to a plain index is not offered automatically: \
         `cohort` is the row's append instant, so a unique index that spans it is weaker \
         than the index is today — exactly the reason the engine's own (workflow_exec_id, \
         event_id) uniqueness moved into the insert trigger rather than a wider \
         constraint. Drop the index or constraint if it is obsolete, or recreate it \
         including `cohort` yourself if that weaker guarantee is acceptable for your use \
         of it.",
        if bad.len() == 1 {
            "it carries a unique index"
        } else {
            "it carries unique indexes"
        },
        bad.join(", ")
    )))
}

/// Views, including materialized views, that depend on `harvest_events` or
/// on one of its leaf partitions.
///
/// **Why this blocks the conversion.** Postgres records a view's dependency
/// by relation OID, not by name — a materialized view the same way as an
/// ordinary one. Both conversion directions rename `harvest_events` out of
/// the way, then create the replacement under the original name. A
/// dependent view keeps pointing at the RENAMED relation. On the populated
/// path, that relation is thereafter only the pre-cutover partition. The
/// view keeps returning rows. It silently stops returning any row appended
/// after the conversion. On the empty-table path the rename target is
/// dropped outright. The dependency makes that `DROP` fail instead —
/// safer, but still not loud about the cause.
///
/// Review finding: on a partitioned shard, `harvest_events` names only the
/// parent. A view can depend directly on a LEAF partition instead — Postgres
/// allows querying one by name like any other table. That view used to pass
/// this check unnoticed, then `disable_partitioning`'s `DROP ... CASCADE`
/// took the leaf, and the view with it. Matching the parent's direct
/// children too closes that gap.
///
/// # Errors
///
/// [`HarvestError::Database`] if the catalog query fails.
#[cfg(feature = "db")]
pub async fn dependent_views(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    let rows = diesel::sql_query(
        "SELECT DISTINCT (v_ns.nspname || '.' || v.relname) AS v
           FROM pg_depend d
           JOIN pg_rewrite r ON r.oid = d.objid
           JOIN pg_class v ON v.oid = r.ev_class
           JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
           JOIN pg_class t ON t.oid = d.refobjid
           JOIN pg_namespace t_ns ON t_ns.oid = t.relnamespace
          WHERE t_ns.nspname = current_schema()
            AND (
                t.relname = 'harvest_events'
                OR t.oid IN (
                    SELECT i.inhrelid
                      FROM pg_inherits i
                      JOIN pg_class parent ON parent.oid = i.inhparent
                      JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                     WHERE parent.relname = 'harvest_events' AND pn.nspname = current_schema()
                )
            )
            AND v.relkind IN ('v', 'm')
          ORDER BY 1",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(|r| r.v).collect())
}

/// The refusal both conversion directions share.
#[cfg(feature = "db")]
async fn refuse_if_dependent_views(conn: &mut AsyncPgConnection, verb: &str) -> HarvestResult<()> {
    let views = dependent_views(conn).await?;
    if views.is_empty() {
        return Ok(());
    }
    Err(HarvestError::Config(format!(
        "refusing to {verb} harvest_events: {} depend on it ({}). Postgres tracks a view's \
         dependency by relation OID, not by name, and the conversion renames harvest_events \
         out of the way and creates the replacement under the original name — so a dependent \
         view would keep pointing at the OLD relation, silently returning fewer rows than it \
         should from the moment this commits, rather than failing loudly. Recreating the \
         view automatically is not offered: `CREATE OR REPLACE VIEW` cannot change its \
         column list, and this module cannot know whether that is safe for yours. Drop the \
         view first (and recreate it against harvest_events afterward) to proceed.",
        if views.len() == 1 { "a view" } else { "views" },
        views.join(", ")
    )))
}

/// Operator-installed triggers on `harvest_events` or on one of its leaf
/// partitions.
///
/// **Why this blocks the conversion.** `CREATE TABLE ... (LIKE ...)` does not
/// carry triggers, and both conversion directions build the replacement
/// relation that way. On the populated path an operator trigger (an audit or
/// validation trigger, say) stays on the renamed legacy table. That table
/// receives no new rows after cutover. The trigger then stops firing for
/// every event, while it still exists — nothing reports the loss. On the
/// empty path the trigger is destroyed with the table it was on.
///
/// [`EXEC_FK_TRIGGER`] is excluded by its FUNCTION
/// (`harvest_events_require_execution`), not by its name. That function is
/// harvest's own, part of the base migration. A trigger invoking it is
/// never operator-installed. Excluding by name alone has a gap: an
/// operator's own trigger could happen to share the reserved name. On an
/// unpartitioned shard, `EXEC_FK_TRIGGER` cannot yet be harvest's, since
/// only a conversion creates it.
///
/// Review finding: on a partitioned shard, `harvest_events` names only the
/// parent. Postgres lets an operator install a trigger directly on a LEAF
/// partition instead. That trigger used to pass this check unnoticed, then
/// `disable_partitioning`'s `DROP ... CASCADE` destroyed it along with the
/// leaf. Matching the parent's direct children too closes that gap.
///
/// # Errors
///
/// [`HarvestError::Database`] if the catalog query fails.
#[cfg(feature = "db")]
pub async fn operator_triggers(conn: &mut AsyncPgConnection) -> HarvestResult<Vec<String>> {
    let rows = diesel::sql_query(
        "SELECT tg.tgname AS v
           FROM pg_trigger tg
           JOIN pg_class c ON c.oid = tg.tgrelid
           JOIN pg_namespace n ON n.oid = c.relnamespace
           JOIN pg_proc p ON p.oid = tg.tgfoid
          WHERE n.nspname = current_schema()
            AND (
                c.relname = 'harvest_events'
                OR c.oid IN (
                    SELECT i.inhrelid
                      FROM pg_inherits i
                      JOIN pg_class parent ON parent.oid = i.inhparent
                      JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                     WHERE parent.relname = 'harvest_events' AND pn.nspname = current_schema()
                )
            )
            AND NOT tg.tgisinternal
            AND NOT (p.proname = 'harvest_events_require_execution'
                     AND p.pronamespace = c.relnamespace)
          ORDER BY 1",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().map(|r| r.v).collect())
}

/// The refusal both conversion directions share.
#[cfg(feature = "db")]
async fn refuse_if_operator_triggers(
    conn: &mut AsyncPgConnection,
    verb: &str,
) -> HarvestResult<()> {
    let triggers = operator_triggers(conn).await?;
    if triggers.is_empty() {
        return Ok(());
    }
    Err(HarvestError::Config(format!(
        "refusing to {verb} harvest_events: {} not carried by CREATE TABLE ... (LIKE ...) \
         ({}). An operator trigger stays on the renamed table, where it stops firing for \
         every new row from cutover onward while still existing — silent for an audit or \
         validation trigger, since nothing reports the loss. Drop the trigger first (and \
         recreate it against harvest_events afterward) to proceed.",
        if triggers.len() == 1 {
            "a trigger is"
        } else {
            "triggers are"
        },
        triggers.join(", ")
    )))
}

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
///
/// [`HarvestError::Config`] also when a logical-replication publication would
/// break — see [`incompatible_publications`].
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

    if !opts.allow_incompatible_publications {
        let pubs = incompatible_publications(conn).await?;
        if !pubs.is_empty() {
            return Err(HarvestError::Config(format!(
                "harvest_events is published by {}, and the partitioned layout is not \
                 compatible with a flat logical-replication subscriber. Two separate \
                 breaks: with publish_via_partition_root = false the rows publish under \
                 leaf partition names the standby has no tables for, which stops the \
                 subscription; and reclamation becomes DROP TABLE, which is DDL and is \
                 never replicated, while the execution delete no longer cascades — so the \
                 standby keeps every event forever and `harvest backup verify` reports it \
                 Incoherent (DanglingEventExecution). Setting \
                 publish_via_partition_root = true fixes only the first. Run the \
                 partitioned layout on the subscriber too, with \
                 `ALTER PUBLICATION <name> SET (publish_via_partition_root = true)`, then \
                 set EnableOptions::allow_incompatible_publications to proceed.",
                pubs.join(", ")
            )));
        }
    }

    refuse_if_row_security(conn, "convert").await?;
    refuse_if_unique_index_without_cohort(conn, "convert").await?;
    refuse_if_dependent_views(conn, "convert").await?;
    refuse_if_operator_triggers(conn, "convert").await?;

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

    let (partitions_created, lookahead_blocked) =
        ensure_partitions(conn, now, opts.lookahead_cohorts, opts.lock_timeout).await?;
    if !lookahead_blocked.is_empty() {
        // Not an error: the conversion itself already committed, and
        // `created` is non-empty (the all-blocked case errors inside
        // `ensure_partitions` above). So this is a partial catch-up gap, not
        // a failed enable. Left for the next maintenance tick to close, same
        // as any other tick.
        tracing::warn!(
            blocked = %lookahead_blocked.join(", "),
            "harvest partition enable: the lookahead window is not fully covered; \
             the next maintenance tick will retry the rest"
        );
    }
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
    // Inlined rather than run as a following statement: on a shard that was
    // empty the legacy table is dropped before this block ends, so the ACLs
    // have to be read while it still exists.
    let copy_acl = copy_acl_body(LEGACY_PARTITION, "harvest_events");
    let bounded_rename_fn = bounded_rename_fn_sql();
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
    we_idx_existed boolean;
    bad_view    text;
    bad_trg     text;
    bad_idx     text;
{COPY_ACL_DECLARE}
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

    -- Review finding: the dependent-view and operator-trigger checks run
    -- in Rust, a separate round-trip before this script starts. A session
    -- that created either kind of object in that gap could commit before
    -- the rename above took its lock, and the object would then be
    -- silently stranded on the table the rename just moved out from under
    -- it. Repeat both checks now, holding the ACCESS EXCLUSIVE lock this
    -- rename just acquired. Postgres tracks both dependencies by OID, so
    -- the rename does not hide either one — querying by the new name,
    -- {LEGACY_PARTITION}, still finds them.
    SELECT string_agg(DISTINCT (v_ns.nspname || '.' || v.relname), ', '
                      ORDER BY (v_ns.nspname || '.' || v.relname))
      INTO bad_view
      FROM pg_depend d
      JOIN pg_rewrite r ON r.oid = d.objid
      JOIN pg_class v ON v.oid = r.ev_class
      JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace
      JOIN pg_class t ON t.oid = d.refobjid
      JOIN pg_namespace t_ns ON t_ns.oid = t.relnamespace
     WHERE t.relname = '{LEGACY_PARTITION}' AND t_ns.nspname = current_schema()
       AND v.relkind IN ('v', 'm');
    IF bad_view IS NOT NULL THEN
        RAISE EXCEPTION 'harvest #958: view(s) depend on harvest_events (%), created after \
the preflight check ran but before this transaction''s ACCESS EXCLUSIVE lock. Drop the view \
(and recreate it against harvest_events afterward), then re-run.', bad_view;
    END IF;

    SELECT string_agg(DISTINCT tg.tgname, ', ' ORDER BY tg.tgname) INTO bad_trg
      FROM pg_trigger tg
      JOIN pg_class c ON c.oid = tg.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_proc p ON p.oid = tg.tgfoid
     WHERE c.relname = '{LEGACY_PARTITION}' AND n.nspname = current_schema()
       AND NOT tg.tgisinternal
       AND NOT (p.proname = 'harvest_events_require_execution'
                AND p.pronamespace = c.relnamespace);
    IF bad_trg IS NOT NULL THEN
        RAISE EXCEPTION 'harvest #958: trigger(s) on harvest_events not carried by CREATE \
TABLE ... (LIKE ...) (%), installed after the preflight check ran but before this \
transaction''s ACCESS EXCLUSIVE lock. Drop the trigger (and recreate it against \
harvest_events afterward), then re-run.', bad_trg;
    END IF;

    -- Review finding: `idx_defs` above is captured before the rename takes
    -- ACCESS EXCLUSIVE, from a plain read that does not conflict with a
    -- concurrent `ALTER TABLE ... ADD CONSTRAINT ... UNIQUE`. Such a
    -- constraint could commit in that gap; its backing index is excluded
    -- from `idx_defs` unconditionally, and `capture_index_defs` never
    -- replays it, so it would silently vanish from every new partition.
    -- Recheck now, holding the lock, the same way the view and trigger
    -- checks just did.
    SELECT string_agg(i.indexrelid::regclass::text, ', ' ORDER BY 1) INTO bad_idx
      FROM pg_index i
      JOIN pg_class c ON c.oid = i.indrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_attribute ca
        ON ca.attrelid = c.oid AND ca.attname = 'cohort'
     WHERE c.relname = '{LEGACY_PARTITION}' AND n.nspname = current_schema()
       AND i.indisunique
       AND {HARVEST_OWNED_CONSTRAINT_EXEMPTION_SQL}
       AND (
           EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
           OR NOT EXISTS (
               SELECT 1 FROM generate_series(0, i.indnkeyatts - 1) k
                WHERE i.indkey[k] = ca.attnum
           )
       );
    IF bad_idx IS NOT NULL THEN
        RAISE EXCEPTION 'harvest #958: harvest_events carries a unique index that cannot \
survive conversion unchanged (%), added after the preflight check ran but before this \
transaction''s ACCESS EXCLUSIVE lock. Drop the index or constraint if it is obsolete, or \
recreate it including `cohort` yourself, then re-run.', bad_idx;
    END IF;

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
    --
    -- The rename target is never a bare `obj.n || suffix`. At the 63-byte
    -- identifier limit that renames a name to itself. It never frees it.
    -- See `bounded_rename_fn_sql` for why, and how the safe name is
    -- computed.
    --
    -- `CREATE FUNCTION` is DDL, so it must run through EXECUTE like every
    -- other statement in this block; wrapped in its OWN dollar-quote tag,
    -- distinct from the one the function body uses internally.
    EXECUTE $harvest_br_958_wrap${bounded_rename_fn}$harvest_br_958_wrap$;
    FOR obj IN SELECT conname AS n FROM pg_constraint
                WHERE conrelid = '{LEGACY_PARTITION}'::regclass
                  AND right(conname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'
    LOOP
        EXECUTE format('ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT %I TO %I',
                       obj.n,
                       {BOUNDED_RENAME_FN}('constraint', '{LEGACY_PARTITION}'::regclass, obj.n,
                                           '{LEGACY_RENAME_SUFFIX}'));
    END LOOP;
    FOR obj IN SELECT indexname AS n FROM pg_indexes
                WHERE schemaname = current_schema() AND tablename = '{LEGACY_PARTITION}'
                  AND right(indexname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'
    LOOP
        EXECUTE format('ALTER INDEX %I RENAME TO %I', obj.n,
                       {BOUNDED_RENAME_FN}('index', NULL, obj.n, '{LEGACY_RENAME_SUFFIX}'));
    END LOOP;
    EXECUTE 'DROP FUNCTION {BOUNDED_RENAME_FN}(text, oid, text, text)';

    -- `LIKE ... INCLUDING DEFAULTS` copies the columns, their NOT NULLs and the
    -- `nextval(...)` id default -- and keeps working when a later migration
    -- adds a column, rather than pinning a hand-written column list that would
    -- silently drop it. Foreign keys are NOT copied, which is the point: the
    -- FK's ON DELETE CASCADE is the row-by-row delete storm being eliminated.
    EXECUTE 'CREATE TABLE harvest_events '
         || '(LIKE {LEGACY_PARTITION} INCLUDING DEFAULTS INCLUDING COMMENTS INCLUDING STORAGE) '
         || 'PARTITION BY RANGE (cohort)';

    -- `LIKE` copies columns, defaults, comments and storage. It does NOT copy
    -- the owner or the ACLs, so on a deployment where migrations run as one
    -- role and the engine connects as another, the runtime's SELECT/INSERT
    -- grants would stay behind on the renamed legacy table and every read and
    -- append would fail from the moment this transaction commits. Replayed
    -- here, while {LEGACY_PARTITION} still exists -- the empty-shard branch
    -- below drops it.
{copy_acl}

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

    -- The sweeper's tier-1 drop gate reads
    -- `harvest_workflow_executions (created_at)`; without this index that probe
    -- is a sequential scan of the executions table, per cohort, per tick.
    --
    -- Built here rather than in the migration, which is inert on apply: a plain
    -- CREATE INDEX holds SHARE on the executions table for its whole build,
    -- blocking every insert, state update and retention delete, and a
    -- deployment that never opts in would pay that for nothing. This
    -- transaction is the documented fresh/small-table path and already holds
    -- ACCESS EXCLUSIVE on harvest_events; `migration_plan` builds the same
    -- index CONCURRENTLY, outside any lock window, for the large tables where
    -- the difference is felt.
    --
    -- Tagged only when this statement is the one that actually creates the
    -- index -- never when IF NOT EXISTS finds an operator's own index
    -- already at that name. `disable_partitioning` reads the tag back to
    -- decide whether it may drop the index.
    --
    -- Review finding: the existence probe and the CREATE INDEX below are
    -- two separate statements. Without a lock spanning both, a concurrent
    -- session could commit an identically-shaped index between them. This
    -- statement's own IF NOT EXISTS would then skip creation silently, but
    -- `we_idx_existed` was already captured as false, so the index gets
    -- tagged as harvest-owned anyway -- and a later `disable_partitioning`
    -- would drop the OTHER session's index. SHARE UPDATE EXCLUSIVE
    -- conflicts with itself, so it serializes this whole probe-create-tag
    -- sequence against any other session doing the same thing, while still
    -- allowing ordinary reads and writes on the table.
    LOCK TABLE harvest_workflow_executions IN SHARE UPDATE EXCLUSIVE MODE;
    SELECT EXISTS (
        SELECT 1 FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = 'idx_harvest_we_created_at' AND n.nspname = current_schema()
    ) INTO we_idx_existed;
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_harvest_we_created_at '
         || 'ON harvest_workflow_executions (created_at)';
    IF NOT we_idx_existed THEN
        EXECUTE 'COMMENT ON INDEX idx_harvest_we_created_at IS '
             || quote_literal('{WE_CREATED_AT_IDX_OWNERSHIP_TAG}');
    END IF;

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

/// SQL that replays `source`'s owner and grants onto `target`.
///
/// Every conversion path replaces `harvest_events` with a freshly created
/// table, and `CREATE TABLE ... LIKE` copies columns, defaults, comments and
/// storage — but **not** ACLs, and not the owner.
///
/// That is invisible when migrations and the runtime share one role, and a
/// total history outage when they do not: the plugin's preflight requires
/// `SELECT` + `INSERT` on `harvest_events` for the runtime role
/// (`HARVEST_WRITE_PRIVILEGE_REQUIREMENTS`), and after an uncorrected
/// conversion those grants sit on the renamed legacy table while the new parent
/// is unreachable. Reads and appends fail from the instant the conversion
/// commits until someone re-issues the `GRANT`s by hand.
///
/// `ALTER TABLE ... OWNER TO` is always permitted here: reaching this point
/// required renaming the source table, which requires owning it, which means
/// being a member of its owning role.
///
/// Column-level ACLs (`pg_attribute.attacl`) are not replayed. Nothing in this
/// engine grants at column granularity, and a table-level `GRANT` is what the
/// preflight probes.
///
/// SQL defining [`BOUNDED_RENAME_FN`], a transient helper both rename loops
/// (in [`enable_sql`] and [`migration_plan_steps`]) call to compute a
/// collision-safe temporary name.
///
/// # The bug this closes
///
/// Postgres silently truncates an identifier over 63 bytes. The rename loops
/// append a fixed suffix (`__pre958`, `__old`) to reclaim an original name
/// for the replacement relation. But appending a suffix to a name
/// **already** at the 63-byte limit renames it to itself. The
/// schema-scoped name is never freed. Replaying the captured
/// `pg_get_indexdef` / `ADD CONSTRAINT` for the real name onto the
/// replacement then fails with `duplicate_relation` / `duplicate_object`.
/// The conversion is transactional, so this fails safely and loudly. But
/// not at a line that names the cause.
///
/// # The fix
///
/// Truncate the base *before* appending the suffix, so the result always
/// fits. A truncated base can (rarely) collide with another renamed sibling
/// that happens to share the same 63-byte prefix. So the result is
/// verified against the catalog. It is disambiguated with a numeric
/// counter if needed:
/// "generate, then verify the result is actually free", not "generate and
/// hope".
///
/// Scoped correctly per kind. A constraint name only has to be unique among
/// the constraints of `p_relid` (`pg_constraint.conrelid`). An index, or any
/// other relation, name has to be unique across the whole schema
/// (`pg_class`). That is why the function takes a `p_relid`, used only for
/// the `'constraint'` case.
///
/// Created immediately before the rename loops that use it and dropped right
/// after — schema debris a re-run should not leave behind.
#[must_use]
fn bounded_rename_fn_sql() -> String {
    format!(
        "CREATE FUNCTION {BOUNDED_RENAME_FN}(p_kind text, p_relid oid, p_base text, p_suffix text)
RETURNS text LANGUAGE plpgsql AS $harvest_bounded_rename_958_fn$
DECLARE
    budget    int;
    keep      int;
    candidate text;
    disambig  int := 0;
    taken     boolean;
BEGIN
    LOOP
        IF disambig = 0 THEN
            budget := 63 - octet_length(p_suffix);
        ELSE
            budget := greatest(63 - octet_length('_' || disambig || p_suffix), 1);
        END IF;
        -- `left()` counts characters, but Postgres's 63 limit is BYTES.
        -- Shrinking one character at a time, checking `octet_length` each
        -- time, never splits a multibyte character mid-codepoint --
        -- `left()` itself only ever cuts on a character boundary.
        keep := length(p_base);
        WHILE keep > 0 AND octet_length(left(p_base, keep)) > budget LOOP
            keep := keep - 1;
        END LOOP;
        IF disambig = 0 THEN
            candidate := left(p_base, keep) || p_suffix;
        ELSE
            candidate := left(p_base, keep) || '_' || disambig || p_suffix;
        END IF;
        IF p_kind = 'constraint' THEN
            SELECT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conrelid = p_relid AND conname = candidate
            ) INTO taken;
        ELSE
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = current_schema() AND c.relname = candidate
            ) INTO taken;
        END IF;
        EXIT WHEN NOT taken;
        disambig := disambig + 1;
        IF disambig > 1000 THEN
            RAISE EXCEPTION 'harvest #1270: could not find a free % name for % after 1000 \
attempts', p_kind, p_base;
        END IF;
    END LOOP;
    RETURN candidate;
END;
$harvest_bounded_rename_958_fn$;"
    )
}

/// [`copy_acl_body`] is the same thing as bare plpgsql statements, for the
/// conversion paths that already run inside a `DO` block; it needs
/// [`COPY_ACL_DECLARE`] in that block's `DECLARE` section.
#[must_use]
fn copy_acl_sql(source: &str, target: &str) -> String {
    format!(
        "DO $harvest_acl_958$
DECLARE
{COPY_ACL_DECLARE}
BEGIN
{}
END
$harvest_acl_958$;",
        copy_acl_body(source, target)
    )
}

/// Variables [`copy_acl_body`] needs declared. Prefixed so they cannot collide
/// with the declarations of a block that inlines it.
#[cfg_attr(not(feature = "db"), allow(dead_code))]
const COPY_ACL_DECLARE: &str = "    harvest_acl_row   record;\n    harvest_acl_owner text;";

/// The owner-and-grant replay, as plpgsql statements.
#[must_use]
fn copy_acl_body(source: &str, target: &str) -> String {
    format!(
        "    SELECT pg_get_userbyid(relowner) INTO harvest_acl_owner
      FROM pg_class WHERE oid = '{source}'::regclass;
    EXECUTE format('ALTER TABLE {target} OWNER TO %I', harvest_acl_owner);
    -- Clear what the replacement was BORN with before replaying the source's
    -- grants. `CREATE TABLE` applies the creating role's default privileges
    -- (ALTER DEFAULT PRIVILEGES), which have no relationship to what the
    -- original table actually carried: a default grant added after the original
    -- was created, or a role deliberately REVOKEd from it, would silently
    -- reappear on the replacement. Replaying grants only ADDS, so without this
    -- the conversion — and the revert — can widen who can read event data.
    --
    -- The owner is left alone: revoking from them would strip the privileges
    -- the replay may not restore (a source with a default NULL ACL grants
    -- nothing explicitly), and ownership is being set to the source's owner
    -- immediately above.
    FOR harvest_acl_row IN
        SELECT CASE WHEN e.grantee = 0 THEN 'PUBLIC'
                    ELSE quote_ident(pg_get_userbyid(e.grantee)) END AS grantee
          FROM pg_class c, aclexplode(c.relacl) e
         WHERE c.oid = '{target}'::regclass
           AND e.grantee <> c.relowner
         GROUP BY 1
    LOOP
        EXECUTE format('REVOKE ALL ON TABLE {target} FROM %s', harvest_acl_row.grantee);
    END LOOP;
    FOR harvest_acl_row IN
        SELECT CASE WHEN e.grantee = 0 THEN 'PUBLIC'
                    ELSE quote_ident(pg_get_userbyid(e.grantee)) END AS grantee,
               e.privilege_type AS priv,
               e.is_grantable AS grantable
          FROM pg_class c, aclexplode(c.relacl) e
         WHERE c.oid = '{source}'::regclass
    LOOP
        EXECUTE format('GRANT %s ON TABLE {target} TO %s%s',
                       harvest_acl_row.priv, harvest_acl_row.grantee,
                       CASE WHEN harvest_acl_row.grantable THEN ' WITH GRANT OPTION' ELSE '' END);
    END LOOP;"
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
// Same gating as `compare_partitions`: every caller is behind `db`, and the
// escaping test is behind `test`.
#[cfg(any(feature = "db", test))]
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Truncate `s` to at most `max_bytes` bytes, on a `char` boundary.
///
/// Postgres identifiers are conventionally ASCII. But truncating blindly at
/// a byte offset could still split a multi-byte `char` in a quoted
/// identifier and produce invalid UTF-8. Used only by
/// [`bounded_rename_name`]'s Rust-side truncation. The PL/pgSQL path's
/// `left()` is Postgres's own encoding-aware truncation and needs no
/// equivalent.
#[cfg(any(feature = "db", test))]
fn truncate_ident(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Rust-side equivalent of [`bounded_rename_fn_sql`] for
/// [`disable_partitioning`], which renames via individual `exec()` calls
/// rather than embedded PL/pgSQL.
///
/// `table` is `None` for an index/relation name. That case checks
/// schema-scoped uniqueness against `pg_class`. `table` is
/// `Some(table_name)` for a constraint name, checked against
/// `pg_constraint.conrelid` and scoped to that one table. See
/// [`bounded_rename_fn_sql`] for why a bare `base || suffix` is unsafe at
/// Postgres's 63-byte identifier limit.
#[cfg(feature = "db")]
async fn bounded_rename_name(
    conn: &mut AsyncPgConnection,
    table: Option<&str>,
    base: &str,
    suffix: &str,
) -> HarvestResult<String> {
    let max_base = 63usize.saturating_sub(suffix.len());
    let mut disambig: u32 = 0;
    loop {
        let candidate = if disambig == 0 {
            format!("{}{suffix}", truncate_ident(base, max_base))
        } else {
            let tag = disambig.to_string();
            let room = max_base.saturating_sub(tag.len() + 1).max(1);
            format!("{}_{tag}{suffix}", truncate_ident(base, room))
        };
        let taken = match table {
            Some(table) => {
                diesel::sql_query(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_constraint
                          WHERE conrelid = ($1::text)::regclass AND conname = $2
                     ) AS v",
                )
                .bind::<Text, _>(table)
                .bind::<Text, _>(&candidate)
                .get_result::<BoolRow>(conn)
                .await
            }
            None => {
                diesel::sql_query(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_class c
                           JOIN pg_namespace n ON n.oid = c.relnamespace
                          WHERE n.nspname = current_schema() AND c.relname = $1
                     ) AS v",
                )
                .bind::<Text, _>(&candidate)
                .get_result::<BoolRow>(conn)
                .await
            }
        }
        .map_err(database_error)?
        .v;
        if !taken {
            return Ok(candidate);
        }
        disambig += 1;
        if disambig > 1000 {
            return Err(HarvestError::Database(format!(
                "could not find a free name for {base} after 1000 attempts"
            )));
        }
    }
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
    // The reverse swap has the identical property: `disable` rebuilds a flat
    // table with `LIKE` and replays the grants onto it, so a policy added after
    // the conversion would be dropped while access is restored.
    refuse_if_row_security(conn, "revert").await?;
    // Same reason as `enable`'s check: a view depending on the partitioned
    // parent would keep pointing at the renamed relation, not the reclaimed
    // flat table.
    refuse_if_dependent_views(conn, "revert").await?;
    // Same reason again: a trigger `CREATE TABLE ... (LIKE ...)` did not
    // carry stays on the partitioned parent being reclaimed, not the flat
    // table `disable` builds in its place.
    refuse_if_operator_triggers(conn, "revert").await?;
    let report = Box::pin(
        conn.transaction::<DisableReport, HarvestError, _>(async |conn| {
            // Review finding: the dependent-view and operator-trigger checks
            // above ran in a separate round-trip, before this transaction
            // even opened. That is well before anything here takes a lock
            // stronger than ACCESS SHARE. A view or trigger created in that
            // gap could commit before the rename further down. So could
            // one created during the row deletes below, which lock only
            // the rows they touch. Either way it would then be destroyed
            // when the reclaimed parent is dropped. `LOCK TABLE` on a
            // partitioned parent recurses onto every partition by
            // default, so this also covers a view or trigger attached
            // directly to a leaf. Recheck both now, under the lock this
            // revert holds for the rest of the transaction.
            exec(conn, "LOCK TABLE harvest_events IN ACCESS EXCLUSIVE MODE").await?;
            let views = dependent_views(conn).await?;
            if !views.is_empty() {
                return Err(HarvestError::Config(format!(
                    "refusing to revert harvest_events: {} depend on it ({}), created \
                     after the preflight check ran but before this transaction's ACCESS \
                     EXCLUSIVE lock. Drop the view (and recreate it against \
                     harvest_events afterward), then re-run.",
                    if views.len() == 1 { "a view" } else { "views" },
                    views.join(", ")
                )));
            }
            let triggers = operator_triggers(conn).await?;
            if !triggers.is_empty() {
                return Err(HarvestError::Config(format!(
                    "refusing to revert harvest_events: {} not carried by CREATE TABLE \
                     ... (LIKE ...) ({}), installed after the preflight check ran but \
                     before this transaction's ACCESS EXCLUSIVE lock. Drop the trigger \
                     (and recreate it against harvest_events afterward), then re-run.",
                    if triggers.len() == 1 {
                        "a trigger is"
                    } else {
                        "triggers are"
                    },
                    triggers.join(", ")
                )));
            }

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
            //
            // The target is never a bare `{r.v}__old`: see
            // `bounded_rename_fn_sql` for why that silently fails to free a
            // name already at Postgres's 63-byte identifier limit.
            for (list_sql, rename_tmpl, table_scope) in [
                (
                    "SELECT conname AS v FROM pg_constraint \
                 WHERE conrelid = 'harvest_events_partitioned'::regclass",
                    "ALTER TABLE harvest_events_partitioned RENAME CONSTRAINT {q} TO {t}",
                    Some("harvest_events_partitioned"),
                ),
                (
                    "SELECT indexname AS v FROM pg_indexes \
                 WHERE schemaname = current_schema() \
                   AND tablename = 'harvest_events_partitioned'",
                    "ALTER INDEX {q} RENAME TO {t}",
                    None,
                ),
            ] {
                let rows = diesel::sql_query(list_sql)
                    .load::<TextRow>(conn)
                    .await
                    .map_err(database_error)?;
                for r in rows {
                    let target = bounded_rename_name(conn, table_scope, &r.v, "__old").await?;
                    exec(
                        conn,
                        // Both sides quoted: a mixed-case or special-character
                        // name (a user-added index on harvest_events) would
                        // otherwise be case-folded or produce invalid syntax.
                        &rename_tmpl
                            .replace("{q}", &quote_ident(&r.v))
                            .replace("{t}", &quote_ident(&target)),
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
            // `LIKE` copies no ACLs and no owner, so the escape hatch would
            // lock a separately-granted runtime role out of `harvest_events`
            // just as surely as the conversion does. See `copy_acl_sql`.
            exec(
                conn,
                &copy_acl_sql("harvest_events_partitioned", "harvest_events"),
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
            // Issue #1270 item 13: `harvest partition enable` (and `plan`)
            // creates this index as part of opting in. The migration itself
            // is inert and never builds it, precisely so a deployment that
            // never opts in never pays for it. `disable` is the reverse of
            // `enable`, so it is the path that removes it again,
            // symmetrically. The FK restored above makes the partitioned
            // drop gate's index moot on the flat layout anyway.
            //
            // Review finding: `enable`'s `CREATE INDEX IF NOT EXISTS` leaves
            // an operator's own pre-existing index of this exact name
            // untouched, so it never becomes harvest's to remove. Dropping
            // by name alone would delete that unrelated index. A shape
            // check alone is not enough either — an operator's own index
            // can happen to have the identical shape. Drop it only when
            // `enable` tagged it AND its shape still matches.
            if scalar_bool(conn, &we_created_at_idx_owned_check_sql()).await? {
                exec(conn, "DROP INDEX idx_harvest_we_created_at").await?;
            }
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

    let mut attempts = 0usize;
    for part in list_partitions(conn).await? {
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

        // Checked here, after the cheap skips above, not at the top of the
        // loop. A tick can exhaust its budget on exactly the last eligible
        // partition. It must not then report `truncated` just because the
        // remaining partitions in the list are DEFAULT or still open.
        // Those cost nothing and were never going to be attempted anyway.
        if outcome.dropped.len() >= opts.max_drops || attempts >= opts.max_attempts {
            outcome.truncated = true;
            break;
        }

        // Counted here, not at the top of the loop. This is the gate
        // evaluation the budget exists to bound, up to a tier-3 scan under
        // `exact_scan_timeout`. A blocked partition costs exactly as much as
        // a dropped one. The cheap skips above (DEFAULT, still open,
        // unbounded) reach no such scan and do not spend the budget.
        attempts += 1;
        if let Some(reason) =
            cohort_occupancy(conn, &EventScope::cohort(part.lower, upper), upper, opts).await?
        {
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
                outcome.straggler_rows_deleted += delete_orphan_rows(
                    conn,
                    part.lower,
                    upper,
                    opts.straggler_batch,
                    opts.exact_scan_timeout,
                )
                .await?;
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

/// Where an occupancy proof looks for events.
///
/// Two shapes, and the difference between them is the whole reason the drop
/// path does not stall a shard:
///
/// - [`EventScope::cohort`] reads the partitioned **parent** under a cohort
///   range predicate. That is the right shape for the unlocked gate, which
///   runs before any lock is taken and is free to touch every partition.
/// - [`EventScope::partition`] reads **one child table** directly, with no
///   predicate at all — the child's boundaries *are* the cohort. That is the
///   shape the re-check under `ACCESS EXCLUSIVE` uses, so the lock it needs is
///   the one partition's, not the parent's.
///
/// The child form also sidesteps the `MINVALUE` trap described below outright:
/// with no cohort predicate there is no lower bound to get wrong.
#[cfg(feature = "db")]
#[derive(Debug, Clone)]
struct EventScope {
    /// `FROM` target, already quoted.
    table: String,
    /// Predicate on `e`, ending in `AND ` when non-empty.
    cohort_pred: String,
}

#[cfg(feature = "db")]
impl EventScope {
    /// The partitioned parent, narrowed to one cohort range.
    ///
    /// The bounds are inlined as literals rather than bound as parameters so
    /// Postgres can prune to the partitions that can match at plan time.
    ///
    /// A `MINVALUE` lower bound is **omitted rather than substituted**: the
    /// legacy partition's rows carry the migration's `-infinity` cohort, which
    /// sorts BELOW every finite timestamptz. Binding any finite value for
    /// `MINVALUE` would make `cohort >= $lower` exclude 100% of the legacy
    /// partition's rows — so a scan would find no live owner, declare the
    /// partition reclaimable, and DROP the entire pre-conversion history of a
    /// deployment that had just opted in, running executions and legal holds
    /// included.
    fn cohort(lower: Option<DateTime<Utc>>, upper: DateTime<Utc>) -> Self {
        use std::fmt::Write as _;
        let mut cohort_pred = String::new();
        if let Some(lower) = lower {
            let _ = write!(
                cohort_pred,
                "e.cohort >= {}::timestamptz AND ",
                ts_literal(lower)
            );
        }
        let _ = write!(
            cohort_pred,
            "e.cohort < {}::timestamptz AND ",
            ts_literal(upper)
        );
        Self {
            table: "harvest_events".to_string(),
            cohort_pred,
        }
    }

    /// One child partition, read directly.
    fn partition(name: &str) -> Self {
        Self {
            table: quote_ident(name),
            cohort_pred: String::new(),
        }
    }
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
    scope: &EventScope,
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
        let owned = cohort_has_rows_for(conn, scope, &ids).await?;
        return Ok(owned.then(|| OWNED_REASON.to_string()));
    }

    // ── Tier 3 ────────────────────────────────────────────────────────────
    let ms = u64::try_from(opts.exact_scan_timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    let scan = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL statement_timeout = '{ms}ms'")).await?;
        let sql = format!(
            "SELECT EXISTS (
                 SELECT 1 FROM {} e
                  WHERE {} EXISTS (
                        SELECT 1 FROM harvest_workflow_executions x
                         WHERE x.id = e.workflow_exec_id
                    )
             ) AS v",
            scope.table, scope.cohort_pred
        );
        let row = diesel::sql_query(sql).get_result::<BoolRow>(conn).await;
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
    scope: &EventScope,
    ids: &[uuid::Uuid],
) -> HarvestResult<bool> {
    if ids.is_empty() {
        return Ok(false);
    }
    let sql = format!(
        "SELECT EXISTS (
             SELECT 1 FROM {} e
              WHERE {} e.workflow_exec_id = ANY($1)
         ) AS v",
        scope.table, scope.cohort_pred
    );
    let row = diesel::sql_query(sql)
        .bind::<Array<SqlUuid>, _>(ids.to_vec())
        .get_result::<BoolRow>(conn)
        .await;
    Ok(row.map_err(database_error)?.v)
}

/// How long the `DROP`'s `SHARE` → `ACCESS EXCLUSIVE` upgrade may wait, in
/// milliseconds — capped by [`SweepOptions::lock_timeout`] when that is
/// smaller.
///
/// Deliberately near zero, and deliberately not the same bound as acquiring the
/// initial `SHARE`. An exclusive *waiter* makes every later conflicting request
/// queue behind it, so each millisecond spent waiting here is a millisecond of
/// stalled appends across the whole shard — see [`drop_partition`]. A partition
/// that cannot be dropped in that window is retried next tick, which costs
/// nothing but time.
#[cfg(feature = "db")]
const DROP_UPGRADE_TIMEOUT_MS: u64 = 50;

/// Rows one `drain_default` pass may move.
///
/// The drain runs inside the transaction that `DETACH PARTITION` opened, which
/// holds `ACCESS EXCLUSIVE` on the parent — so every row it copies is a row the
/// whole shard waits for. `lock_timeout` bounds *acquiring* that lock, never
/// holding it, so an unbounded drain of a large backlog is an outage on the
/// automatic retention tick that is supposed to be repairing things.
///
/// A pass therefore moves whole cohorts up to this budget and re-attaches; the
/// next tick continues. Whole cohorts, not whole rows, because a cohort's
/// partition cannot exist while `DEFAULT` still holds rows in its range — the
/// re-`ATTACH` would fail its constraint check. The budget is a floor, not a
/// ceiling: one oversized cohort is irreducible and moves in a single pass.
#[cfg(feature = "db")]
const DRAIN_MAX_ROWS: usize = 50_000;

/// Cohorts one `drain_default` pass may take.
///
/// The row budget does not bound the pass on its own. Each cohort taken needs
/// its partition created — one `CREATE TABLE ... PARTITION OF` apiece — and
/// that DDL cannot run before the `DETACH`, because a cohort's partition may
/// not be created while `DEFAULT` still holds rows in its range. So it runs
/// under the parent's `ACCESS EXCLUSIVE` by necessity, and a maintenance gap
/// spanning hundreds of closed cohorts parks few rows in each: the row budget
/// never binds, and one pass stopped the shard for hundreds of DDL statements.
///
/// Bounding the cohort count bounds that DDL directly. Like the row budget it
/// is a floor, not a ceiling — a pass always takes at least one cohort, so a
/// backlog always converges.
#[cfg(feature = "db")]
#[doc(hidden)]
pub const DRAIN_MAX_COHORTS: usize = 32;

// A `statement_timeout` inside the drain's window is DELIBERATELY absent, and
// this is the second thing to know about the pass after the budgets.
//
// It looks like the obvious bound and it is a trap. Every statement in the
// transaction scans `DEFAULT` in proportion to the backlog — `cohort` carries
// no index, so the move's `DELETE` scans it, and the mandatory re-`ATTACH ...
// DEFAULT` scans it too, because Postgres must prove no remaining row belongs
// to an existing partition (measured: ~160ms per 3M rows on PG16, and it is
// cancellable, so the timeout really does fire).
//
// So a timeout small relative to the backlog does not bound the work; it
// discards it. The transaction rolls back, the rows just moved go back, the
// next tick retries against an identical table and times out at the same
// point. The backlog never shrinks and its partitions are never reclaimed —
// a permanent livelock on exactly the deployment the drain exists for, and one
// that gets worse every tick.
//
// The work is bounded by the row and cohort budgets instead, which bound it
// without ever throwing away a pass that succeeded. What remains unbounded is
// the per-pass scan of whatever is still parked, and that is irreducible: the
// re-`ATTACH` cannot be skipped and cannot be made to not scan.

/// Drop one partition under a bounded lock wait, re-proving it is reclaimable
/// while holding the lock.
///
/// Returns `false` (not an error) when the partition was left in place: the
/// lock could not be taken in time, a concurrent writer deadlocked with the
/// attempt, or the re-check found an owner. Leaving it for the next tick is the
/// correct response to all three; making every concurrent append wait is not.
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
/// So the check has to be repeated under a lock that excludes every writer that
/// could land such a row — and, critically, under **no more than that**. It
/// takes `SHARE` on the one child partition:
///
/// - `SHARE` conflicts with `ROW EXCLUSIVE`, so nothing can INSERT, UPDATE or
///   DELETE in this partition while the proof runs, which is exactly the
///   guarantee the re-check needs. Tuple routing locks the destination
///   partition, so appends to every *other* cohort — which is all of them, this
///   one being closed — are untouched.
/// - `SHARE` does **not** conflict with `ACCESS SHARE`, so readers are
///   untouched. That is not a nicety: the insert trigger's cross-partition
///   `(workflow_exec_id, event_id)` uniqueness check reads the partitioned
///   parent, which locks every child in `ACCESS SHARE`. An exclusive lock on
///   *any* child therefore stalls *every* append on the shard, whichever
///   cohort it belongs to.
///
/// `ACCESS EXCLUSIVE` — on the parent or on the child — would be correct and
/// unusable. The wait for the lock is bounded by `lock_timeout`, but the
/// re-check that follows it is bounded by
/// [`SweepOptions::exact_scan_timeout`], 15 seconds by default, and a
/// `lock_timeout` bounds *acquiring* a lock, never holding one. On a shard
/// where tier 3 is reached — more surviving old executions than
/// `owner_probe_cap`, which is the shape this feature exists for — every append
/// and every read on the table would queue for that whole scan, once per drop
/// attempt, on every tick.
///
/// The re-check reads the child table **directly** ([`EventScope::partition`]),
/// which keeps the proof inside the relation the lock covers and is also
/// strictly less work: no cohort predicate, no pruning, no sibling partitions.
///
/// `ACCESS EXCLUSIVE` is taken only by the `DROP` itself, on the child and (to
/// update the partition descriptor) on the parent. That is a catalog change:
/// metadata-only and immediate.
///
/// Taking the child's lock before the parent's inverts the order a concurrent
/// insert uses (parent, then destination child), so an append landing in *this*
/// closed cohort at exactly the wrong moment can deadlock with the drop — as
/// can the `SHARE`-to-`ACCESS EXCLUSIVE` upgrade at the `DROP`. Postgres
/// detects both and aborts one side; if it is ours, the partition is reported
/// blocked and retried next tick, exactly as for a lock timeout.
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
    let opts = *opts;
    let result = Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        exec(conn, &format!("SET LOCAL lock_timeout = '{ms}ms'")).await?;
        // Taken explicitly, before the re-check, rather than relying on the
        // DROP to take it afterwards — the whole point is that the check runs
        // under a lock that freezes this partition's contents.
        exec(
            conn,
            &format!("LOCK TABLE {} IN SHARE MODE", quote_ident(&name)),
        )
        .await?;

        // The SAME three-tier proof, re-run under the lock — not a narrower
        // one. An earlier revision bailed out whenever more executions survived
        // than `owner_probe_cap`, which is precisely the condition under which
        // the gate had used the exact scan: every partition that needed tier 3
        // to prove itself droppable was then rejected here, forever, so
        // reclamation stopped entirely on high-volume or legal-hold-heavy
        // shards — the deployments this feature exists for.
        if cohort_occupancy(conn, &EventScope::partition(&name), upper, &opts)
            .await?
            .is_some()
        {
            return Ok(false);
        }

        // A near-zero bound on the lock UPGRADE, separate from the bound on
        // acquiring the SHARE above, because the two cost different things.
        //
        // Waiting for `SHARE` is free to bystanders: it conflicts with
        // `ROW EXCLUSIVE` — writers to this closed cohort, of which there are
        // none — and not with `ACCESS SHARE`, so a pending `SHARE` request
        // queues nobody behind it.
        //
        // The `DROP`'s upgrade to `ACCESS EXCLUSIVE` is the opposite. Postgres
        // queues a new request behind an existing *waiter* it conflicts with,
        // not merely behind held locks, so while this upgrade waits — for one
        // long history query still holding `ACCESS SHARE` on this child — every
        // append's cross-partition uniqueness probe, which takes `ACCESS SHARE`
        // on every child, queues behind it. Whatever cohort it is writing.
        // Bounding that by `lock_timeout` would stall the whole shard for two
        // seconds per drop attempt, per tick.
        //
        // So the upgrade gets one brief attempt and the partition waits for the
        // next tick, where reclamation is bounded and retried by design.
        let upgrade_ms = ms.min(DROP_UPGRADE_TIMEOUT_MS);
        exec(conn, &format!("SET LOCAL lock_timeout = '{upgrade_ms}ms'")).await?;
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
        Err(HarvestError::Database(msg)) if is_lock_timeout(&msg) || is_deadlock(&msg) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Targeted removal of orphan rows from a cohort a straggler has pinned.
///
/// The opt-in fallback issue #958 sanctions for the non-steady-state case.
/// Deletes only rows whose owning execution no longer exists, in bounded
/// batches, so a straggler pass can neither touch a live execution's history
/// nor open an unbounded transaction.
///
/// Each batch runs under `statement_timeout`, exactly like the exact
/// ownership scan this fallback runs alongside — see [`cohort_occupancy`].
/// Without it, a batch on a partition where orphans are SPARSE re-scans the
/// leading owned rows every iteration before finding one to delete. That is
/// quadratic in the partition size, and otherwise runs for as long as that
/// scan takes, inside the retention tick. A timeout is "did what it could
/// this tick, retry next" — not an error. That matches every other budget
/// in this module: what was deleted before the timeout stays deleted.
#[cfg(feature = "db")]
async fn delete_orphan_rows(
    conn: &mut AsyncPgConnection,
    lower: Option<DateTime<Utc>>,
    upper: DateTime<Utc>,
    batch: usize,
    timeout: Duration,
) -> HarvestResult<usize> {
    let batch = i64::try_from(batch).unwrap_or(i64::MAX).max(1);
    let ms = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(1);
    // Total work per partition per tick is capped, not just per statement. The
    // inner SELECT restarts from the top of the range each iteration, so on a
    // partition where orphans are SPARSE — the common shape for one pinned by a
    // straggler — each pass re-scans the leading owned rows before finding its
    // batch, which is quadratic in the partition size. Bounded here; the next
    // tick continues.
    let max_batches = 16;
    let mut total = 0usize;
    for _ in 0..max_batches {
        // Keyed on `(id, cohort)` — the partitioned table's PRIMARY KEY — and
        // NOT on `ctid`.
        //
        // A `ctid` is unique only WITHIN one physical child table. Matching it
        // against the partitioned PARENT, whose scan spans every partition,
        // deletes any row in any other partition that happens to share the same
        // physical location — including events belonging to live executions.
        // That is silent, unbounded data loss, and it is invisible in testing
        // because a small corpus rarely reuses a `ctid` across partitions.
        //
        // `(id, cohort)` is globally unique (one sequence feeds every
        // partition, and the key carries the partition column), so the outer
        // DELETE targets exactly the rows the inner SELECT chose — and prunes
        // to their partition instead of scanning all of them.
        //
        // The `MINVALUE` lower bound is omitted rather than substituted, for
        // the same reason as in `cohort_occupancy`: the legacy partition's rows
        // carry `-infinity`, which sorts below every finite timestamptz, so a
        // finite lower bind would silently match none of them.
        let sql = format!(
            "DELETE FROM harvest_events
              WHERE (id, cohort) IN (
                  SELECT e.id, e.cohort FROM harvest_events e
                   WHERE {} e.cohort < $1
                     AND NOT EXISTS (
                         SELECT 1 FROM harvest_workflow_executions x
                          WHERE x.id = e.workflow_exec_id
                     )
                   LIMIT $2
              )",
            lower.map_or(String::new(), |_| "e.cohort >= $3 AND".to_string())
        );
        let result = Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
            exec(conn, &format!("SET LOCAL statement_timeout = '{ms}ms'")).await?;
            let query = diesel::sql_query(sql)
                .bind::<Timestamptz, _>(upper)
                .bind::<BigInt, _>(batch);
            let deleted = if let Some(lower) = lower {
                query.bind::<Timestamptz, _>(lower).execute(conn).await
            } else {
                query.execute(conn).await
            };
            deleted.map_err(database_error)
        }))
        .await;
        let deleted = match result {
            Ok(n) => n,
            // Fail safe toward "stop here". The rows this pass already
            // deleted in earlier batches stay deleted. The rest of this
            // partition is retried next tick — the same direction every
            // other budget in this module fails in.
            Err(HarvestError::Database(msg)) if is_statement_timeout(&msg) => break,
            Err(e) => return Err(e),
        };
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
    drain_default_bounded(conn, DRAIN_MAX_ROWS).await
}

/// [`drain_default`] with an explicit per-pass row budget.
///
/// Exposed for tests, which need a budget small enough to force the multi-pass
/// path without seeding tens of thousands of rows.
///
/// # Errors
///
/// [`HarvestError::Database`] if any step fails; the pass is one transaction,
/// so a failure leaves every row where it was.
// A census, then one transaction whose steps must be read in order — detach,
// create, move, re-enable, re-attach. Splitting it to satisfy a line budget
// would scatter that sequence across helpers that only ever call each other
// once, and the order is the whole correctness argument.
#[allow(clippy::too_many_lines)]
#[cfg(feature = "db")]
#[doc(hidden)]
pub async fn drain_default_bounded(
    conn: &mut AsyncPgConnection,
    max_rows: usize,
) -> HarvestResult<usize> {
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

    // Census BEFORE the lock. This `GROUP BY` scans every row in the DEFAULT
    // partition, and `cohort` carries no index — on the large backlog a
    // maintenance gap leaves, exactly the case this budget exists for, it is
    // the most expensive thing the pass does. Run after the `DETACH` it was
    // unbounded work under the parent's ACCESS EXCLUSIVE, so the row budget
    // bounded only what was MOVED while the shard stayed stopped for the whole
    // scan. Here it takes ACCESS SHARE and costs bystanders nothing.
    let census = diesel::sql_query(format!(
        "SELECT cohort AS v, count(*)::bigint AS n
           FROM {DEFAULT_PARTITION} GROUP BY 1 ORDER BY 1"
    ))
    .load::<CohortCountRow>(&mut *conn)
    .await
    .map_err(database_error)?;

    // Take whole cohorts, oldest first, up to BOTH budgets — and always at
    // least one, so a cohort larger than the row budget still makes progress
    // rather than the drain spinning forever on a backlog it refuses to touch.
    //
    // Whole cohorts is not a simplification, it is required: a cohort's
    // partition cannot exist while `DEFAULT` still holds rows in its range, so
    // a half-moved cohort would make the re-`ATTACH` below fail its constraint
    // check and roll the whole pass back.
    let mut work: Vec<DateTime<Utc>> = Vec::new();
    let mut cutoff = None;
    let mut budget = 0usize;
    for c in &census {
        let Some(cohort) = c.v else { continue };
        let n = usize::try_from(c.n).unwrap_or(usize::MAX);
        if cutoff.is_some()
            && (budget.saturating_add(n) > max_rows.max(1) || work.len() >= DRAIN_MAX_COHORTS)
        {
            break;
        }
        budget = budget.saturating_add(n);
        cutoff = Some(cohort);
        work.push(cohort);
    }

    Box::pin(conn.transaction::<usize, HarvestError, _>(async |conn| {
        exec(conn, "SET LOCAL lock_timeout = '5s'").await?;
        // No `statement_timeout` here — see the note above the budgets. It
        // would discard a completed pass rather than bound one.
        exec(
            conn,
            &format!("ALTER TABLE harvest_events DETACH PARTITION {DEFAULT_PARTITION}"),
        )
        .await?;

        for cohort in &work {
            ensure_cohort_with_width(conn, *cohort, width, Duration::from_secs(2)).await?;
        }
        // The work list was read before the lock, so a row could have landed in
        // `DEFAULT` between the census and the `DETACH`. Only ONE cohort can
        // have: an append's cohort is `clock_timestamp()` floored, so a late
        // arrival carries the currently-open cohort (or the next one, if the
        // boundary rolled while the `DETACH` waited for its lock). Covering
        // both makes the pre-lock list valid without re-scanning to revalidate
        // it — and both are no-ops when a partition already exists.
        //
        // Without this the move could meet a row with no partition and fail
        // with `no partition of relation found`, rolling the pass back. Not
        // data loss, but a drain that never converges on a shard whose write
        // window is uncovered, which is the shard that needs it.
        if let Some(cutoff) = cutoff {
            let now = Utc::now();
            for ts in [now, now + chrono::Duration::seconds(width)] {
                if cohort_start(ts, width) <= cutoff {
                    ensure_cohort_with_width(conn, ts, width, Duration::from_secs(2)).await?;
                }
            }
        }
        let Some(cutoff) = cutoff else {
            // Nothing with a usable cohort: re-attach and report no progress
            // rather than leaving `DEFAULT` detached.
            exec(
                conn,
                &format!("ALTER TABLE harvest_events ATTACH PARTITION {DEFAULT_PARTITION} DEFAULT"),
            )
            .await?;
            return Ok(0);
        };

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

        // One statement, so the rows leave `DEFAULT` exactly as they arrive in
        // their cohort partitions — there is no window in which a row exists in
        // both, and no `TRUNCATE` that could discard a row this pass did not
        // move.
        let moved = diesel::sql_query(format!(
            "WITH moved AS (
                 DELETE FROM {DEFAULT_PARTITION} WHERE cohort <= {} RETURNING *
             )
             INSERT INTO harvest_events SELECT * FROM moved",
            ts_literal(cutoff)
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
        // No TRUNCATE: the move above already removed exactly the rows it
        // copied, and anything still here belongs to a cohort this pass did not
        // take. Truncating would destroy it.
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

/// The owner of `harvest_events` when the connected role cannot act as it.
///
/// Every partition operation is DDL on that table — `CREATE TABLE ... PARTITION
/// OF` to extend the write window, `DETACH`/`ATTACH` to drain the `DEFAULT`
/// partition, `DROP TABLE` to reclaim — and Postgres checks **ownership** for
/// all of it. There is no lesser privilege that grants it.
///
/// That matters because this engine supports, and its preflight probes for, a
/// split-role deployment: migrations and `harvest partition enable` run as an
/// owning role while the engine connects as a separately granted one that needs
/// only `SELECT`/`INSERT` on `harvest_events`. Automatic maintenance runs on
/// the engine's pool, so on that topology every pass fails — the lookahead
/// window stops being extended, appends pile into the `DEFAULT` partition, and
/// nothing is ever reclaimed.
///
/// Postgres checks ownership by role *membership*, not identity, so
/// `GRANT <owner> TO <engine role>` is the fix and no second connection pool is
/// needed. Returning the owner's name lets [`maintain`] say that, instead of
/// relaying a `permission denied for table harvest_events_p_default` that names
/// neither the role nor the grant.
///
/// `None` when the connected role can act as owner — the single-role default,
/// where this costs one catalog probe per tick.
///
/// # Errors
///
/// [`HarvestError::Database`] on a catalog failure.
#[cfg(feature = "db")]
async fn maintenance_owner_gap(
    conn: &mut AsyncPgConnection,
) -> HarvestResult<Option<(String, String)>> {
    let rows = diesel::sql_query(
        "SELECT pg_get_userbyid(c.relowner) || ' ' || current_user AS v
           FROM pg_class c
           JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE c.relname = 'harvest_events'
            AND n.nspname = current_schema()
            AND NOT pg_has_role(current_user, c.relowner, 'USAGE')",
    )
    .load::<TextRow>(conn)
    .await
    .map_err(database_error)?;
    Ok(rows.into_iter().next().and_then(|r| {
        r.v.split_once(' ')
            .map(|(owner, me)| (owner.to_string(), me.to_string()))
    }))
}

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
    // Before any DDL, because the failure is otherwise unreadable: the first
    // thing to fail is whichever partition operation runs first, and its
    // `permission denied for table harvest_events_p_default` names neither the
    // role that needs the grant nor the grant itself.
    if let Some((owner, me)) = maintenance_owner_gap(conn).await? {
        return Err(HarvestError::Config(format!(
            "partition maintenance requires ownership of harvest_events, and the connected \
             role ({me}) does not have it. Every partition operation is DDL — CREATE TABLE \
             ... PARTITION OF to extend the write window, DETACH/ATTACH to drain the \
             DEFAULT partition, DROP TABLE to reclaim — and Postgres checks ownership for \
             all of it. harvest_events is owned by {owner}. Postgres checks ownership by \
             role membership, so `GRANT {owner} TO {me};` is enough; alternatively point \
             the engine at a role that owns the table. Until then the write window stops \
             being extended, appends land in the DEFAULT partition, and no cohort is \
             reclaimed."
        )));
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
    let (created, lookahead_blocked) =
        ensure_partitions(conn, now, lookahead_cohorts, sweep_opts.lock_timeout).await?;
    let sweep = sweep(conn, now, sweep_opts).await?;
    // A partial catch-up must not report as a healthy, empty-`last_error`
    // pass. `ensure_partitions` keeps creating the rest of the window when
    // one cohort is blocked (deliberately — see its doc). So `created` can
    // be non-empty even though the write window is still not fully covered.
    // This never overwrites `drain_error`. Both are real, independent
    // failures this tick, and neither may hide the other.
    let last_error = if lookahead_blocked.is_empty() {
        drain_error
    } else {
        let msg = format!(
            "the lookahead window is not fully covered: {} of {} cohort(s) blocked ({}); \
             appends for those cohorts land in {DEFAULT_PARTITION} until a later pass succeeds",
            lookahead_blocked.len(),
            lookahead_cohorts + 1,
            lookahead_blocked.join(", ")
        );
        Some(drain_error.map_or_else(|| msg.clone(), |prev| format!("{prev}; {msg}")))
    };
    Ok(MaintenanceOutcome {
        at: Some(Utc::now()),
        created,
        lookahead_blocked,
        drained,
        sweep,
        last_error,
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
    /// Cohorts in the lookahead window that could NOT be created this pass.
    /// Causes include a generated name colliding with an unrelated relation,
    /// or a bounded lock attempt that ran out of time.
    ///
    /// Non-empty here means the write window is not fully covered, even
    /// though `created` may also be non-empty. `ensure_partitions` keeps
    /// creating the rest of the window when one cohort is blocked. A partial
    /// catch-up must not be mistaken for a healthy pass. Retried
    /// automatically next tick.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lookahead_blocked: Vec<String>,
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

/// A boolean SQL expression checking that `c`/`i` name a **correctly
/// shaped** unique index on exactly `columns`, in order. `c`/`i` are
/// aliases for `pg_class` and `pg_index`, already joined and filtered to
/// `indrelid = 'harvest_events'`.
///
/// Used by the phase-4 assertion below to close a narrow but real hole. An
/// operator's own pre-existing, valid index can happen to hold one of the
/// two fixed names (`{LEGACY_PARTITION}_pk_idx`,
/// `{LEGACY_PARTITION}_exec_event_idx`). That makes phase 2's `CREATE ...
/// IF NOT EXISTS` skip building the real one. Checking only the name and
/// `indisvalid` — the assertion's original form — counts that impostor as
/// ready. `ATTACH PARTITION` then builds the real index inside the window
/// this plan advertises as metadata-only.
///
/// Checks uniqueness, and the exact key column list (position included —
/// `(a, b)` is not `(b, a)`). Also checks that there is no partial-index
/// predicate or expression column, and that every key column uses its
/// type's default
/// operator class. `indkey` is an `int2vector`, whose Postgres-defined
/// array lower bound is `0`, so `indkey[0]` is the first key column.
#[must_use]
fn index_shape_check_sql(index_name: &str, columns: &[&str]) -> String {
    let col_checks: String = columns
        .iter()
        .enumerate()
        .map(|(pos, col)| {
            format!(
                "i.indkey[{pos}] = (SELECT a.attnum FROM pg_attribute a \
                 WHERE a.attrelid = 'harvest_events'::regclass AND a.attname = '{col}')"
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let n = columns.len();
    format!(
        "(c.relname = '{index_name}' AND i.indisunique AND i.indpred IS NULL \
         AND i.indexprs IS NULL AND i.indnkeyatts = {n} AND i.indnatts = {n} \
         AND {col_checks} \
         AND NOT EXISTS (\
             SELECT 1 FROM unnest(i.indclass) AS oc(opclass) \
             JOIN pg_opclass op ON op.oid = oc.opclass \
             WHERE NOT op.opcdefault\
         ))"
    )
}

/// A `DO` block refusing over a unique index that cannot survive conversion
/// unchanged. Tagged with `tag`, so it can appear more than once in the same
/// generated script.
///
/// Review finding: phase 1's copy of this check runs hours before phase 4's
/// rename, under `migration_plan`'s online path. An operator could add a
/// compatible-looking constraint-backed unique index in that window and
/// pass phase 1 clean. `capture_index_defs` would still drop it silently
/// at phase 4, exactly like [`unique_indexes_missing_cohort`]'s own
/// docstring explains. Recheck it under phase 4's lock too, alongside the
/// view and trigger rechecks.
#[must_use]
fn unique_index_guard_sql(tag: &str) -> String {
    format!(
        "DO ${tag}$\nDECLARE bad text;\nBEGIN\n    \
         SELECT string_agg(i.indexrelid::regclass::text, ', ' ORDER BY 1) INTO bad\n      \
         FROM pg_index i\n      \
         JOIN pg_class c ON c.oid = i.indrelid\n      \
         JOIN pg_namespace n ON n.oid = c.relnamespace\n      \
         JOIN pg_attribute ca\n        \
         ON ca.attrelid = c.oid AND ca.attname = 'cohort'\n     \
         WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()\n       \
         AND i.indisunique\n       \
         AND {HARVEST_OWNED_CONSTRAINT_EXEMPTION_SQL}\n       \
         AND (\n           \
         EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)\n           \
         OR NOT EXISTS (\n               \
         SELECT 1 FROM generate_series(0, i.indnkeyatts - 1) k\n                \
         WHERE i.indkey[k] = ca.attnum\n           \
         )\n       \
         );\n    \
         IF bad IS NOT NULL THEN\n        \
         RAISE EXCEPTION 'harvest #958: harvest_events carries a unique index that \
         cannot survive conversion unchanged (%). Either it does not include \
         `cohort` -- Postgres requires the partition key in every unique index on a \
         partitioned table, so phase 4 replaying it would fail -- or it is backed by \
         a table constraint other than harvest''s own two, which phase 4 has no way \
         to replay and would otherwise drop silently. Drop the index or constraint if \
         it is obsolete, or recreate it including `cohort` yourself, before running \
         this plan.', bad;\n    \
         END IF;\nEND\n${tag}$;"
    )
}

/// A `DO` block refusing when a view depends on `harvest_events`, tagged
/// with `tag` so it can appear more than once in the same generated script.
///
/// Review finding: phase 1's copy of this check runs hours before phase 4's
/// rename, under `migration_plan`'s online path. A view created (or
/// repointed) at `harvest_events` in that window would pass phase 1 clean.
/// It would then end up silently stale after phase 4 anyway.
/// [`migration_plan_steps`] runs this same check again as the first thing
/// inside phase 4's lock. The catalog state being validated is then the
/// state actually being converted.
#[must_use]
fn dependent_views_guard_sql(tag: &str) -> String {
    format!(
        "DO ${tag}$\nDECLARE bad text;\nBEGIN\n    \
         SELECT string_agg(DISTINCT (v_ns.nspname || '.' || v.relname), ', ') INTO bad\n      \
         FROM pg_depend d\n      \
         JOIN pg_rewrite r ON r.oid = d.objid\n      \
         JOIN pg_class v ON v.oid = r.ev_class\n      \
         JOIN pg_namespace v_ns ON v_ns.oid = v.relnamespace\n      \
         JOIN pg_class t ON t.oid = d.refobjid\n      \
         JOIN pg_namespace t_ns ON t_ns.oid = t.relnamespace\n     \
         WHERE t.relname = 'harvest_events' AND t_ns.nspname = current_schema()\n       \
         AND v.relkind IN ('v', 'm');\n    \
         IF bad IS NOT NULL THEN\n        \
         RAISE EXCEPTION 'harvest #958: view(s) depend on harvest_events (%). Postgres \
         tracks a view''s dependency by relation OID, not by name, and phase 4 renames \
         harvest_events out of the way — so the view would keep pointing at the OLD \
         relation, silently returning fewer rows than it should. Drop the view (and \
         recreate it against harvest_events afterward) before running this plan.', bad;\n    \
         END IF;\nEND\n${tag}$;"
    )
}

/// A `DO` block refusing when an operator trigger sits on `harvest_events`.
/// Tagged with `tag`, so it can appear more than once in the same
/// generated script.
///
/// Review finding: phase 1's copy of this check has the identical gap
/// [`dependent_views_guard_sql`] closes. Phase 4 recheck it too, right
/// alongside the view recheck, under the same lock.
#[must_use]
fn operator_triggers_guard_sql(tag: &str) -> String {
    format!(
        "DO ${tag}$\nDECLARE bad text;\nBEGIN\n    \
         SELECT string_agg(tg.tgname, ', ' ORDER BY 1) INTO bad\n      \
         FROM pg_trigger tg\n      \
         JOIN pg_class c ON c.oid = tg.tgrelid\n      \
         JOIN pg_namespace n ON n.oid = c.relnamespace\n     \
         JOIN pg_proc p ON p.oid = tg.tgfoid\n     \
         WHERE c.relname = 'harvest_events' AND n.nspname = current_schema()\n       \
         AND NOT tg.tgisinternal\n       \
         AND NOT (p.proname = 'harvest_events_require_execution'\n                      \
         AND p.pronamespace = c.relnamespace);\n    \
         IF bad IS NOT NULL THEN\n        \
         RAISE EXCEPTION 'harvest #958: trigger(s) on harvest_events not carried by \
         CREATE TABLE ... (LIKE ...) (%). An operator trigger would stay on the \
         renamed legacy table after phase 4, where it stops firing for every new row \
         from cutover onward while still existing. Drop the trigger (and recreate it \
         against harvest_events afterward) before running this plan.', bad;\n    \
         END IF;\nEND\n${tag}$;"
    )
}

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
    let pk_idx_check =
        index_shape_check_sql(&format!("{LEGACY_PARTITION}_pk_idx"), &["id", "cohort"]);
    let exec_event_idx_check = index_shape_check_sql(
        &format!("{LEGACY_PARTITION}_exec_event_idx"),
        &["workflow_exec_id", "event_id", "cohort"],
    );

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

    let mut steps = vec![
        // ── 1: refuse outright if the table is already partitioned ────────
        //
        // Re-running the plan after a completed step 4 is not a no-op, it is
        // destructive. Step 3's guard looks for its constraint BY NAME on
        // `harvest_events`, and the conversion renamed the legacy constraint
        // onto the legacy partition — so on the new parent the guard finds
        // nothing and adds `CHECK (cohort < cutover) NOT VALID` to the live
        // partitioned parent. `NOT VALID` skips the scan of existing rows but
        // still enforces the check for NEW ones, whose cohort is at or after
        // the cutover by construction. Every subsequent append fails, on a
        // shard that was working.
        //
        // First, before anything mutates, and repeated inside step 3 because
        // the runbook explicitly tells an operator to re-run that step alone
        // after a failed validation.
        step(
            1,
            "DO $harvest_relkind_958$\nBEGIN\n    \
             IF (SELECT c.relkind FROM pg_class c\n          \
             JOIN pg_namespace n ON n.oid = c.relnamespace\n         \
             WHERE c.relname = 'harvest_events'\n           \
             AND n.nspname = current_schema()) = 'p' THEN\n        \
             RAISE EXCEPTION 'harvest #958: harvest_events is already partitioned. This \
             plan converts an ordinary table, and re-running it over a converted one adds \
             the legacy cohort CHECK to the live parent, where NOT VALID still enforces it \
             for new rows and every append then fails. Nothing to do.';\n    \
             END IF;\nEND\n$harvest_relkind_958$;"
                .to_string(),
        ),
        // ── 1: refuse early if logical replication would break ────────────
        //
        // First, before hours of `CONCURRENTLY` index building, because the
        // remedy is one `ALTER PUBLICATION` on the primary. `enable_partitioning`
        // makes the same check in Rust; the scripted path needs its own, since
        // it never calls it. See `incompatible_publications` for why a
        // leaf-publishing publication silently stops the standby.
        step(
            1,
            "DO $harvest_pub_958$\nDECLARE bad text;\nBEGIN\n    \
             SELECT string_agg(DISTINCT t.pubname, ', ') INTO bad\n      \
             FROM pg_publication_tables t\n     \
             WHERE t.schemaname = current_schema()\n       \
             AND t.tablename = 'harvest_events';\n    \
             IF bad IS NOT NULL THEN\n        \
             RAISE EXCEPTION 'harvest #958: harvest_events is published by %. The \
             partitioned layout is not compatible with a flat logical-replication \
             subscriber: leaf partition names the standby has no tables for, and \
             reclamation by DROP TABLE which is never replicated, leaving the standby \
             with dangling events it reports as Incoherent. publish_via_partition_root \
             fixes only the first. Run the partitioned layout on the subscriber too, or \
             drop the publication, before re-running this plan.', bad;\n    \
             END IF;\nEND\n$harvest_pub_958$;"
                .to_string(),
        ),
        // ── 1: refuse early if row security would be silently dropped ─────
        //
        // Same reason as the publication guard, and same phase: before
        // anything mutates. `enable_partitioning` makes this check in Rust;
        // the scripted path needs its own because it never calls it. See
        // `row_security_config` — the swap keeps the grants and loses the
        // policies, so the rows a policy was filtering become readable.
        step(
            1,
            "DO $harvest_rls_958$\nDECLARE pols text; rls bool;\nBEGIN\n    \
             SELECT COALESCE(bool_or(c.relrowsecurity OR c.relforcerowsecurity), false)\n      \
             INTO rls\n      \
             FROM pg_class c\n      \
             JOIN pg_namespace n ON n.oid = c.relnamespace\n     \
             WHERE c.relname = 'harvest_events' AND n.nspname = current_schema();\n    \
             SELECT string_agg(p.polname, ', ' ORDER BY p.polname) INTO pols\n      \
             FROM pg_policy p\n      \
             JOIN pg_class c ON c.oid = p.polrelid\n      \
             JOIN pg_namespace n ON n.oid = c.relnamespace\n     \
             WHERE c.relname = 'harvest_events' AND n.nspname = current_schema();\n    \
             IF rls OR pols IS NOT NULL THEN\n        \
             RAISE EXCEPTION 'harvest #958: harvest_events has row level security              configured (policies: %). Phase 4 replaces it with a table built by CREATE              TABLE ... (LIKE ...), which copies neither the row-security flags nor any              policy, while the owner and grants ARE replayed onto the replacement — so the              same roles would reach a table with row security off and rows a policy had              been filtering would become readable. Drop the policies if obsolete, or              reproduce them on the converted layout by hand, before running this plan.',              COALESCE(pols, 'none, but row security is enabled');\n    \
             END IF;\nEND\n$harvest_rls_958$;"
                .to_string(),
        ),
        // ── 1: refuse early over a unique index missing `cohort` ──────────
        //
        // Same reason and same phase as the guards above.
        // `enable_partitioning` makes this check in Rust; the scripted path
        // needs its own because it never calls it. See
        // `refuse_if_unique_index_without_cohort` — Postgres requires the
        // partition key in every unique index on a partitioned table, and
        // phase 4 replays every captured index verbatim.
        step(1, unique_index_guard_sql("harvest_uniq_958")),
        // ── 1: refuse early over a view depending on harvest_events ───────
        //
        // Same reason and same phase as the guards above.
        // `enable_partitioning` makes this check in Rust; the scripted path
        // needs its own because it never calls it. See `dependent_views`.
        // Postgres tracks a view's dependency by OID, not by name. A
        // dependent view would keep pointing at the renamed relation after
        // phase 4, silently returning fewer rows than it should.
        step(1, dependent_views_guard_sql("harvest_view_958")),
        // ── 1: refuse early over an operator trigger on harvest_events ────
        //
        // Same reason and same phase as the guards above.
        // `enable_partitioning` makes this check in Rust; the scripted path
        // needs its own because it never calls it. See `operator_triggers`.
        // `CREATE TABLE ... (LIKE ...)` does not carry triggers. An operator
        // trigger would stay on the renamed legacy table after phase 4. It
        // would then stop firing for every new row from cutover onward.
        step(1, operator_triggers_guard_sql("harvest_trg_958")),
        // ── 1: bake the chosen width into the cohort function ─────────────
        step(1, cohort_function_sql(width)),
        // ── 2: the partition-key indexes, built without blocking ──────────
        //
        // A cancelled or failed `CREATE INDEX CONCURRENTLY` leaves the index
        // behind, INVALID — and it is the build most likely to be lost, since
        // it runs for hours on the table this plan exists for. `IF NOT EXISTS`
        // then reports success on a re-run without looking at
        // `pg_index.indisvalid`, so the invalid index survives to phase 4,
        // where `ATTACH PARTITION` cannot reuse it and builds a replacement
        // *while holding the parent-wide exclusive lock*. The window the plan
        // promises is metadata-only becomes a full index build with every
        // append queued behind it.
        //
        // Dropped, not reindexed: a plain `DROP INDEX` on an invalid index is a
        // catalog change with no readers to wait for, and the `CONCURRENTLY`
        // build below then rebuilds it without blocking.
        step(2, "BEGIN".to_string()),
        step(2, format!("SET LOCAL lock_timeout = '{lock_ms}ms'")),
        step(
            2,
            format!(
                "DO $harvest_reindex_958$\nDECLARE idx text;\nBEGIN\n    \
                 FOR idx IN SELECT c.relname FROM pg_class c\n                \
                 JOIN pg_index i ON i.indexrelid = c.oid\n                \
                 JOIN pg_namespace n ON n.oid = c.relnamespace\n               \
                 WHERE n.nspname = current_schema() AND NOT i.indisvalid\n                 \
                 AND c.relname IN ('{LEGACY_PARTITION}_pk_idx',\n                                   \
                 '{LEGACY_PARTITION}_exec_event_idx',\n                                   \
                 'idx_harvest_we_created_at')\n    \
                 LOOP\n        \
                 EXECUTE format('DROP INDEX %I', idx);\n    \
                 END LOOP;\nEND\n$harvest_reindex_958$;"
            ),
        ),
        step(2, "COMMIT".to_string()),
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
        // The sweeper's tier-1 drop gate. On `harvest_workflow_executions`,
        // the busiest table in the schema, so the concurrent form is not
        // optional here: a plain build holds SHARE for its duration and every
        // insert, state update and retention delete waits behind it.
        //
        // Unlike `enable_sql`, this step does not stamp
        // `WE_CREATED_AT_IDX_OWNERSHIP_TAG`. `CREATE INDEX CONCURRENTLY`
        // must run alone, outside any transaction. It cannot also check
        // pre-existence and conditionally tag in one atomic step the way
        // `enable_sql`'s single transaction does. An index this plan
        // builds is therefore never dropped by `disable_partitioning`,
        // which requires the tag. That is the safe default. Leaving the
        // index behind costs an operator one manual `DROP INDEX`.
        // Dropping an untagged index on a guess could destroy one of
        // theirs instead.
        concurrent(
            2,
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_harvest_we_created_at\n    \
             ON harvest_workflow_executions (created_at)"
                .to_string(),
        ),
        // ── 3: pre-validate so ATTACH skips its own scan ──────────────────
        //
        // Both statements carry a bounded lock wait, and neither may be run
        // bare. `NOT VALID` skips the table scan but still takes
        // `ACCESS EXCLUSIVE` to write the catalog row, and `VALIDATE` takes
        // `SHARE UPDATE EXCLUSIVE`; behind one idle-in-transaction reader
        // either request queues — and because Postgres queues lock requests in
        // order, every append arriving after it queues behind the ALTER. That
        // is the opposite of what this phase is for: it exists so the
        // conversion's expensive work happens with the shard *online*. Failing
        // fast leaves the operator a bounded, retryable error instead.
        step(3, "BEGIN".to_string()),
        step(3, format!("SET LOCAL lock_timeout = '{lock_ms}ms'")),
        // Guarded rather than bare, because this phase is now TWO transactions
        // and the half-done state is reachable by design: the constraint lands,
        // the validation scan then hits its bounded lock wait and rolls back.
        // The runbook's answer to that is "clear the blocker and re-run step
        // 3", and an unconditional ADD CONSTRAINT fails with `duplicate_object`
        // before the validation is ever retried — so bounding the lock wait
        // would have traded a hang for a dead end, with regenerating the plan
        // no help either since it bakes in the same constraint name.
        step(
            3,
            format!(
                "DO $harvest_cohort_ck_958$\nBEGIN\n    \
                 IF (SELECT c.relkind FROM pg_class c\n          \
                 JOIN pg_namespace n ON n.oid = c.relnamespace\n         \
                 WHERE c.relname = 'harvest_events'\n           \
                 AND n.nspname = current_schema()) = 'p' THEN\n        \
                 RAISE EXCEPTION 'harvest #958: harvest_events is already partitioned; \
                 adding the legacy cohort CHECK to the live parent would make every \
                 append fail. Nothing to do.';\n    \
                 END IF;\n    \
                 IF NOT EXISTS (SELECT 1 FROM pg_constraint\n                    \
                 WHERE conname = '{LEGACY_PARTITION}_cohort_ck'\n                      \
                 AND conrelid = 'harvest_events'::regclass) THEN\n        \
                 ALTER TABLE harvest_events\n            \
                 ADD CONSTRAINT {LEGACY_PARTITION}_cohort_ck\n            \
                 CHECK (cohort < {cutover_lit}) NOT VALID;\n    \
                 END IF;\nEND\n$harvest_cohort_ck_958$;"
            ),
        ),
        step(3, "COMMIT".to_string()),
        // A separate transaction, deliberately: the whole point of `NOT VALID`
        // is that the scan below happens outside the window that added the
        // constraint.
        step(3, "BEGIN".to_string()),
        step(3, format!("SET LOCAL lock_timeout = '{lock_ms}ms'")),
        step(
            3,
            format!("ALTER TABLE harvest_events VALIDATE CONSTRAINT {LEGACY_PARTITION}_cohort_ck"),
        ),
        step(3, "COMMIT".to_string()),
        // ── 4: THE WINDOW — one transaction, metadata only ────────────────
        step(4, "BEGIN".to_string()),
        step(4, format!("SET LOCAL lock_timeout = '{lock_ms}ms'")),
        // Review finding: the recheck below is only as good as the lock it
        // runs under. `BEGIN` and `SET LOCAL` take no lock on
        // `harvest_events` at all. Every statement up to this point is a
        // plain catalog read, ACCESS SHARE at most. Without this explicit
        // `LOCK TABLE`, a concurrent `CREATE VIEW` could still commit
        // after the recheck. It could still commit before the actual
        // rename several steps below. That is exactly the race being
        // closed. Taking ACCESS EXCLUSIVE here, once, makes every
        // catalog read for the rest of this transaction see a state
        // that cannot change before the rename commits. That covers the
        // recheck, the phase-2 completeness assertion, and the
        // index-definition capture alike.
        step(
            4,
            "LOCK TABLE harvest_events IN ACCESS EXCLUSIVE MODE".to_string(),
        ),
        // Review finding: phase 1's unique-index check has the identical
        // hours-long gap to phase 4's rename. An operator could add a
        // compatible-looking constraint-backed unique index in that gap.
        // It would pass phase 1 clean, and still have it silently dropped
        // by `capture_index_defs` at phase 4. Recheck it too, under the
        // same lock.
        step(4, unique_index_guard_sql("harvest_uniq_cutover_958")),
        // Review finding: phase 1's dependent-view check ran hours before
        // this window, under this plan's online path. A view created (or
        // repointed) at `harvest_events` in that gap would pass phase 1
        // clean, then silently go stale after the rename below anyway.
        // Re-run the identical check now, holding the lock this phase
        // actually converts under, so the state being checked is the
        // state being converted.
        step(4, dependent_views_guard_sql("harvest_view_cutover_958")),
        // Review finding: phase 1's trigger check has the identical gap
        // the view recheck above just closed. Recheck it too, under the
        // same lock.
        step(4, operator_triggers_guard_sql("harvest_trg_cutover_958")),
        // Still before anything renames, so a plan resumed over a lost
        // phase-2 build aborts before it has renamed anything. Without it,
        // `ATTACH PARTITION` below discovers the missing index only once the
        // exclusive lock is held, and builds it there.
        //
        // Checks SHAPE, not just name and `indisvalid` (issue #1270 item
        // 11). An operator's own pre-existing, valid index can happen to
        // hold one of these two fixed names. That makes phase 2's
        // `IF NOT EXISTS` skip building the real one. This assertion must
        // not count that impostor as ready.
        step(
            4,
            format!(
                "DO $harvest_assert_958$\nDECLARE n int;\nBEGIN\n    \
                 SELECT count(*) INTO n FROM pg_class c\n      \
                 JOIN pg_index i ON i.indexrelid = c.oid\n      \
                 JOIN pg_namespace ns ON ns.oid = c.relnamespace\n     \
                 WHERE ns.nspname = current_schema() AND i.indisvalid\n       \
                 AND i.indrelid = 'harvest_events'::regclass\n       \
                 AND ({pk_idx_check} OR {exec_event_idx_check});\n    \
                 IF n <> 2 THEN\n        \
                 RAISE EXCEPTION 'harvest #958: phase 2 left % of 2 valid, correctly-shaped \
                 indexes on harvest_events. ATTACH PARTITION would build the missing one \
                 while holding ACCESS EXCLUSIVE. Re-run phase 2, then this window.', n;\n    \
                 END IF;\nEND\n$harvest_assert_958$;"
            ),
        ),
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
        //
        // The rename target is never a bare `obj.n || suffix`. See
        // `bounded_rename_fn_sql` for why that silently fails to free a name
        // already at Postgres's 63-byte identifier limit. It also explains
        // how the safe name is computed instead. `CREATE FUNCTION` is valid
        // as its own top-level statement here, unlike in `enable_sql`'s
        // single `DO` block. So it is its own step here, dropped again once
        // the rename loop is done.
        step(4, bounded_rename_fn_sql()),
        step(
            4,
            format!(
                "DO $harvest_rename_958$\nDECLARE obj record;\nBEGIN\n    \
                 FOR obj IN SELECT conname AS n FROM pg_constraint\n                \
                 WHERE conrelid = '{LEGACY_PARTITION}'::regclass\n                  \
                 AND right(conname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'\n    \
                 LOOP\n        \
                 EXECUTE format('ALTER TABLE {LEGACY_PARTITION} RENAME CONSTRAINT %I TO %I',\n                       \
                 obj.n, {BOUNDED_RENAME_FN}('constraint', '{LEGACY_PARTITION}'::regclass,\n                            \
                 obj.n, '{LEGACY_RENAME_SUFFIX}'));\n    \
                 END LOOP;\n    \
                 FOR obj IN SELECT indexname AS n FROM pg_indexes\n                \
                 WHERE schemaname = current_schema() AND tablename = '{LEGACY_PARTITION}'\n                  \
                 AND right(indexname, {suffix_len}) <> '{LEGACY_RENAME_SUFFIX}'\n    \
                 LOOP\n        \
                 EXECUTE format('ALTER INDEX %I RENAME TO %I', obj.n,\n                       \
                 {BOUNDED_RENAME_FN}('index', NULL, obj.n, '{LEGACY_RENAME_SUFFIX}'));\n    \
                 END LOOP;\nEND\n$harvest_rename_958$"
            ),
        ),
        step(
            4,
            format!("DROP FUNCTION {BOUNDED_RENAME_FN}(text, oid, text, text)"),
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
        // `LIKE` copies no owner and no ACLs — see `copy_acl_sql`.
        step(4, copy_acl_sql(LEGACY_PARTITION, "harvest_events")),
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
    ];

    // Issue #1270 item 7: `enable_partitioning` honours
    // `allow_incompatible_publications` for the same guard. This scripted
    // path must too. Otherwise an operator could use the override on a
    // small table (`enable`) but not on a large one (`plan`). That operator
    // has done the supported thing: brought the subscriber onto the
    // partitioned layout with `publish_via_partition_root = true`. `plan`
    // is the only path large deployments are told to use. The tag is
    // unique to this one `DO` block, so filtering on it cannot drop any
    // other step.
    if opts.allow_incompatible_publications {
        steps.retain(|s| !s.sql.contains("$harvest_pub_958$"));
    }
    steps
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
        let opts = EnableOptions::default();
        let now = Utc::now();
        let plan = migration_plan(&opts, now);
        let steps = migration_plan_steps(&opts, now);

        // The window is phase 4, located BY PHASE rather than by the first
        // `BEGIN;` in the rendered text. Phases 2 and 3 open short bounded
        // transactions of their own now — an invalid-index cleanup and the
        // constraint add — so a test that assumed the first `BEGIN` was the
        // window would quietly start asserting about one of those instead, and
        // pass or fail for reasons unrelated to what it is guarding.
        let window = steps
            .iter()
            .filter(|s| s.phase == 4)
            .map(|s| s.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
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
        for step in &steps {
            assert!(
                !step.sql.contains("CREATE UNIQUE INDEX") || step.concurrent,
                "every index build in the plan must be concurrent, whatever \
                 phase it lands in:\n{}",
                step.sql
            );
        }
        // Each transaction the plan opens has to close, or the operator's
        // session sits idle-in-transaction holding whatever it took.
        assert_eq!(
            steps.iter().filter(|s| s.sql == "BEGIN").count(),
            steps.iter().filter(|s| s.sql == "COMMIT").count(),
            "every BEGIN in the plan needs its COMMIT"
        );
    }

    #[test]
    fn the_plan_builds_the_drop_gate_index_concurrently() {
        // It lives on `harvest_workflow_executions`, the busiest table in the
        // schema. A plain build holds SHARE for its duration, which conflicts
        // with the ROW EXCLUSIVE every insert, state update and retention
        // delete takes — so on the large tables this plan exists for, a
        // non-concurrent build is an outage.
        let step = migration_plan_steps(&EnableOptions::default(), Utc::now())
            .into_iter()
            .find(|s| s.sql.contains("idx_harvest_we_created_at") && s.sql.contains("CREATE INDEX"))
            .expect("the plan must build the drop gate's index");
        assert!(
            step.concurrent && step.sql.contains("CONCURRENTLY"),
            "{}",
            step.sql
        );
        assert_eq!(step.phase, 2, "and before the lock window, not inside it");
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
    fn allow_incompatible_publications_omits_the_phase_1_publication_guard() {
        // Issue #1270 item 7: `enable_partitioning` already honours the
        // override for this guard. The scripted large-table plan must too.
        // Otherwise an operator could use the override on `enable` but not
        // on `plan`, the only path large deployments are told to use. That
        // operator has done the supported thing: run the partitioned
        // layout on the subscriber too.
        let now = Utc::now();
        let default_steps = migration_plan_steps(&EnableOptions::default(), now);
        assert!(
            default_steps
                .iter()
                .any(|s| s.sql.contains("$harvest_pub_958$")),
            "the guard must be present by default"
        );

        let overridden_steps = migration_plan_steps(
            &EnableOptions {
                allow_incompatible_publications: true,
                ..EnableOptions::default()
            },
            now,
        );
        assert!(
            overridden_steps
                .iter()
                .all(|s| !s.sql.contains("$harvest_pub_958$")),
            "the override must omit the guard entirely, not merely neuter it, \
             so the printed script does not confuse an operator with a check \
             that can never fire"
        );

        // Every other phase-1 guard (already-partitioned, row security) must
        // survive — the override is specific to the publication check.
        assert!(
            overridden_steps
                .iter()
                .any(|s| s.sql.contains("$harvest_relkind_958$")),
            "the already-partitioned guard is unrelated and must remain"
        );
        assert!(
            overridden_steps
                .iter()
                .any(|s| s.sql.contains("$harvest_rls_958$")),
            "the row-security guard is unrelated and must remain"
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
    fn truncate_ident_is_a_no_op_under_the_limit() {
        assert_eq!(truncate_ident("harvest_events", 63), "harvest_events");
        assert_eq!(truncate_ident("short", 5), "short");
    }

    #[test]
    fn truncate_ident_shortens_at_a_char_boundary() {
        // Issue #1270 item 9: a name already at or near the 63-byte
        // identifier limit must be truncated. Otherwise Postgres silently
        // truncates it again (to the same value) once a suffix is appended.
        let long = "a".repeat(70);
        let truncated = truncate_ident(&long, 63);
        assert_eq!(truncated.len(), 63);

        // A multi-byte character must never be split — the result would not
        // be valid UTF-8, and quoting it would produce broken SQL.
        let multibyte = format!("{}{}", "a".repeat(62), 'é');
        let truncated = truncate_ident(&multibyte, 63);
        assert!(truncated.len() <= 63);
        assert!(std::str::from_utf8(truncated.as_bytes()).is_ok());
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
