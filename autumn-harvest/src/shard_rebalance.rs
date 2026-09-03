//! Shard rebalancing — migrating quiescent workflows across shards (issue #964).
//!
//! ## Why
//!
//! The sharding contract used to end at *"cross-shard rebalancing of existing
//! workflows is out of scope"*. The consequences compound: adding a shard only
//! helps **new** starts, so a hot shard stays hot for as long as its residents
//! live — forever, for a continue-as-new entity workflow — and a shard can
//! never be decommissioned. This module is the operator-initiated primitive
//! that fixes that, scoped to what the replay engine can actually prove.
//!
//! ## The shape
//!
//! ```text
//! copy ──▶ replay-verify ──▶ ONE atomic cutover commit ──▶ sealed source
//! ```
//!
//! Restricting to **quiescent** executions is the move that makes the rest
//! tractable: it converts a live distributed-migration problem into a
//! copy-verify-cutover of an *inert, append-only event log* — exactly the
//! artifact Harvest's replay engine exists to verify. That is what deletes the
//! catch-up phase every online shard-move in the literature needs, and with it
//! the dual-write and the two-phase commit.
//!
//! ## The identity decision (issue #964 AC4)
//!
//! **A migrated execution keeps its `ExecutionId`.** The first two bytes stop
//! meaning "the shard this run lives on" and start meaning "the shard this run
//! **originated** on" — its routing entry point, not its residence. Nothing
//! that holds an id has to learn anything, because no id changed: parents'
//! recorded `ChildWorkflowStarted.child_id`, stored handles, external
//! signal/cancel targets, webhooks and schedule lineage all keep resolving
//! structurally rather than by enumeration.
//!
//! Resolution is two-level:
//!
//! 1. **Origin-shard forwarding** — the sealed source row (`MIGRATED`) carries
//!    `migrated_to_shard`. An id-routed operation lands on the origin shard as
//!    it always did, finds the seal, and follows the pointer
//!    ([`resolve_execution_shard`]). Chains (A→B→C) are followed up to
//!    [`MAX_FORWARD_HOPS`] and then fail closed rather than loop.
//! 2. **Router-declared shard forwards** — for a shard that has been *removed*,
//!    `ShardRouter::with_shard_forwards` maps its id straight to the successor
//!    with no origin database involved.
//!
//! See `docs/plans/2026-09-02-shard-rebalancing.md` for the full design note
//! (AC4 requires one) and `docs/sharding.md` for the operator contract.
//!
//! ## Relationship to the append-only invariant
//!
//! Nothing here appends to, reorders, or rewrites `harvest_events`. The copy
//! `INSERT`s new rows on a **different** database and never touches a stored row
//! on the source. Shard rebalancing is therefore **not** a fourth exception to
//! the `harvest_events` invariant in `CLAUDE.md` — it is an instance of it, and
//! it introduces **zero** new `WorkflowEvent` variants.

use sha2::{Digest, Sha256};

use crate::error::{HarvestError, HarvestResult};
use crate::replay::HistoryMatcher;
use crate::types::ShardId;

// ── Quiescence ───────────────────────────────────────────────────────────────

/// Everything the quiescence decision needs, gathered explicitly.
///
/// The predicate is a pure function over this struct rather than a SQL `WHERE`
/// clause precisely so every branch is unit-testable without Postgres. That
/// matters more here than almost anywhere else in the engine: the first draft
/// of the predicate said "no task rows at all", which would have refused to
/// migrate every timer-parked workflow — i.e. the entire population this
/// feature exists to move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuiescenceObservation {
    /// `harvest_workflow_executions.state`.
    pub state: String,
    /// The execution's parent, if any. A child's terminal appends to its
    /// parent's history in a shard-local transaction, so a child may not move
    /// away from its parent.
    pub parent_id: Option<crate::types::ExecutionId>,
    /// Whether the execution was started by a schedule
    /// (`harvest_workflow_executions.schedule_id IS NOT NULL`). A schedule's
    /// overlap enforcement is shard-local, so its runs may not move away from
    /// it — see [`QuiescenceBlocker::ScheduleAttributed`].
    pub schedule_attributed: bool,
    /// Workflow task rows a worker currently holds
    /// (`state = 'RUNNING' AND worker_id IS NOT NULL`).
    pub claimed_workflow_tasks: i64,
    /// Workflow task rows dispatchable right now
    /// (`state = 'PENDING' AND scheduled_at <= NOW()`).
    pub due_pending_tasks: i64,
    /// Workflow task rows in a *parked* shape: either the signal-park
    /// (`RUNNING` with no worker) or the timer-park (`PENDING` scheduled in the
    /// future). This is the migratable shape — it is **copied**, not refused.
    pub parked_workflow_tasks: i64,
    /// The durable "a wake raced the park" flag on any live workflow task row.
    pub wake_requested: bool,
    /// Non-terminal activity / non-workflow task rows.
    pub live_activity_tasks: i64,
    /// Staged signals not yet folded into history.
    pub unconsumed_signals: i64,
    /// Completion-callback deliveries currently in flight.
    pub inflight_completion_deliveries: i64,
    /// `ACTIVE` worker sessions, whose state lives on exactly one worker.
    pub active_sessions: i64,
    /// Non-terminal external (human-in-the-loop) tasks.
    pub live_external_tasks: i64,
    /// Non-terminal children on this shard.
    pub live_children: i64,
    /// In-flight `harvest_cross_shard_children` rows parented by this execution.
    pub cross_shard_child_rows: i64,
    /// Durable mutex keys this execution currently **holds**
    /// (`harvest_mutex_locks.holder_exec_id`, issue #691).
    pub held_mutex_locks: i64,
    /// Durable mutex keys this execution is **queued for**
    /// (`harvest_mutex_waiters.waiter_exec_id`, issue #691).
    pub queued_mutex_waiters: i64,
    /// Dead-letter rows attributed to this execution.
    pub dead_letter_rows: i64,
    /// Whether the run is currently parked by the replay non-determinism block
    /// (`nd_blocked_at IS NOT NULL`), which carries a pending re-dispatch.
    pub nd_blocked: bool,
}

/// Why an execution may not be migrated. One variant per fact, so a dry-run can
/// explain itself completely instead of an operator clearing one blocker at a
/// time and re-running forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QuiescenceBlocker {
    /// The execution is not in `RUNNING` — terminal, or already mid-migration.
    NotRunning,
    /// The execution has a parent; only roots migrate (see the module docs).
    NotARoot,
    /// A worker is mid-cycle on this execution.
    ClaimedWorkflowTask,
    /// A wake is dispatchable right now.
    DuePendingTask,
    /// A wake raced the park and is recorded on the task row.
    WakeRequested,
    /// More than one workflow task row exists, which the engine's own
    /// invariants say cannot happen — refuse rather than copy an ambiguous set.
    AmbiguousParkedTaskSet,
    /// In-flight activity work, which is out of scope by design.
    LiveActivityTask,
    /// A staged signal has not been folded into history yet.
    UnconsumedSignal,
    /// An outbound completion delivery is in flight.
    InflightCompletionDelivery,
    /// A worker session is open.
    ActiveSession,
    /// A non-terminal external task is outstanding.
    LiveExternalTask,
    /// A non-terminal child lives on this shard.
    LiveChild,
    /// An in-flight cross-shard child lifecycle row is parented here.
    CrossShardChildRow,
    /// The execution holds a durable mutex lock. The lock row is shard-local
    /// and keyed by the holder, so moving the holder away would leave the key
    /// held by an execution that no longer lives here — and every waiter on it
    /// blocked forever.
    HoldsMutexLock,
    /// The execution is queued for a durable mutex lock. The grant is delivered
    /// by waking the waiter on **this** shard, so a migrated waiter's grant
    /// would be delivered to a sealed row: a lost wake, which is exactly what
    /// the quiescence bar exists to prevent.
    QueuedForMutex,
    /// A dead-letter row is attributed to this execution. Redriving it enqueues
    /// a task on this shard, which after a migration would target a sealed row.
    HasDeadLetterRow,
    /// The run is parked by the replay non-determinism block.
    NonDeterminismBlocked,
    /// The execution was started by a **schedule**, whose overlap policy is
    /// enforced shard-locally.
    ///
    /// A schedule row does not move with its runs. Its tick counts
    /// `RUNNING`/`PAUSED` executions on **its own** shard to honour
    /// `max_active_runs`, and its `CancelOther`/`TerminateOther` overlap
    /// policies cancel priors with the same shard-local query. After a
    /// migration the local row reads `MIGRATED`, so the scheduler stops
    /// counting the still-running copy and stops cancelling it: the schedule
    /// silently exceeds its own cap, or starts a run without terminating the
    /// prior it was told to replace.
    ///
    /// Out of scope by design, like in-flight activity work and held mutexes.
    /// Making it safe means teaching schedule overlap enforcement to see
    /// forwarded residences, which is a change to the scheduler rather than to
    /// this feature.
    ScheduleAttributed,
}

impl QuiescenceBlocker {
    /// A short operator-readable explanation, for dry-run output.
    #[must_use]
    pub const fn describe(self) -> &'static str {
        match self {
            Self::NotRunning => "the execution is not RUNNING",
            Self::NotARoot => "the execution has a parent; only root executions migrate",
            Self::ClaimedWorkflowTask => "a worker is currently running a workflow task",
            Self::DuePendingTask => "a workflow task is dispatchable right now",
            Self::WakeRequested => "a wake is recorded but not yet delivered",
            Self::AmbiguousParkedTaskSet => "more than one workflow task row exists",
            Self::LiveActivityTask => "an activity task is in flight",
            Self::UnconsumedSignal => "a staged signal has not been folded into history",
            Self::InflightCompletionDelivery => "a completion-callback delivery is in flight",
            Self::ActiveSession => "a worker session is open",
            Self::ScheduleAttributed => {
                "the execution belongs to a schedule, whose overlap enforcement is shard-local"
            }
            Self::LiveExternalTask => "an external task is outstanding",
            Self::LiveChild => "a non-terminal child workflow lives on this shard",
            Self::CrossShardChildRow => "an in-flight cross-shard child is parented here",
            Self::HoldsMutexLock => "the execution holds a durable mutex lock",
            Self::QueuedForMutex => "the execution is queued for a durable mutex lock",
            Self::HasDeadLetterRow => {
                "a dead-letter row is attributed to this execution; redrive or discard it first"
            }
            Self::NonDeterminismBlocked => "the run is blocked on a replay non-determinism",
        }
    }
}

/// The verdict of [`assess_quiescence`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Quiescence {
    /// Parked with nothing in flight: safe to migrate.
    Eligible,
    /// Every reason it may not move, in a stable order.
    Blocked(Vec<QuiescenceBlocker>),
}

impl Quiescence {
    /// Is this execution migratable?
    #[must_use]
    pub const fn is_eligible(&self) -> bool {
        matches!(self, Self::Eligible)
    }

    /// The blockers, or an empty slice when eligible.
    #[must_use]
    pub fn blockers(&self) -> &[QuiescenceBlocker] {
        match self {
            Self::Eligible => &[],
            Self::Blocked(blockers) => blockers,
        }
    }
}

/// Decide whether one execution is quiescent enough to migrate.
///
/// Pure, total, and evaluated **twice** in a real migration: once to select
/// candidates, and again inside the cutover statement's own `WHERE` clause
/// against the same facts. The second evaluation is what makes a wake arriving
/// mid-migration abort cleanly rather than get lost.
#[must_use]
pub fn assess_quiescence(obs: &QuiescenceObservation) -> Quiescence {
    let mut blockers = Vec::new();

    if obs.state != "RUNNING" {
        blockers.push(QuiescenceBlocker::NotRunning);
    }
    if obs.parent_id.is_some() {
        blockers.push(QuiescenceBlocker::NotARoot);
    }
    if obs.schedule_attributed {
        blockers.push(QuiescenceBlocker::ScheduleAttributed);
    }
    if obs.claimed_workflow_tasks > 0 {
        blockers.push(QuiescenceBlocker::ClaimedWorkflowTask);
    }
    if obs.due_pending_tasks > 0 {
        blockers.push(QuiescenceBlocker::DuePendingTask);
    }
    if obs.parked_workflow_tasks > 1 {
        blockers.push(QuiescenceBlocker::AmbiguousParkedTaskSet);
    }
    if obs.wake_requested {
        blockers.push(QuiescenceBlocker::WakeRequested);
    }
    if obs.live_activity_tasks > 0 {
        blockers.push(QuiescenceBlocker::LiveActivityTask);
    }
    if obs.unconsumed_signals > 0 {
        blockers.push(QuiescenceBlocker::UnconsumedSignal);
    }
    if obs.inflight_completion_deliveries > 0 {
        blockers.push(QuiescenceBlocker::InflightCompletionDelivery);
    }
    if obs.active_sessions > 0 {
        blockers.push(QuiescenceBlocker::ActiveSession);
    }
    if obs.live_external_tasks > 0 {
        blockers.push(QuiescenceBlocker::LiveExternalTask);
    }
    if obs.live_children > 0 {
        blockers.push(QuiescenceBlocker::LiveChild);
    }
    if obs.cross_shard_child_rows > 0 {
        blockers.push(QuiescenceBlocker::CrossShardChildRow);
    }
    if obs.held_mutex_locks > 0 {
        blockers.push(QuiescenceBlocker::HoldsMutexLock);
    }
    if obs.queued_mutex_waiters > 0 {
        blockers.push(QuiescenceBlocker::QueuedForMutex);
    }
    if obs.dead_letter_rows > 0 {
        blockers.push(QuiescenceBlocker::HasDeadLetterRow);
    }
    if obs.nd_blocked {
        blockers.push(QuiescenceBlocker::NonDeterminismBlocked);
    }

    if blockers.is_empty() {
        Quiescence::Eligible
    } else {
        Quiescence::Blocked(blockers)
    }
}

// ── The phase machine ────────────────────────────────────────────────────────

/// Where one migration has got to. Persisted as `harvest_shard_migrations.phase`
/// on the **source** shard — the database that stays authoritative right up to
/// the cutover commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationPhase {
    /// Opened; nothing copied yet.
    Pending,
    /// The target holds a staged, inert `MIGRATING` copy.
    Copied,
    /// The staged copy passed replay verification.
    Verified,
    /// **The point of no return.** The source is sealed and forwards; the
    /// target is authoritative but not yet claimable.
    Committed,
    /// The target is `RUNNING` and the migration is finished.
    Done,
    /// Abandoned before cutover; the source was never touched.
    Aborted,
}

impl MigrationPhase {
    /// The value stored in `harvest_shard_migrations.phase`.
    #[must_use]
    pub const fn as_db(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Copied => "COPIED",
            Self::Verified => "VERIFIED",
            Self::Committed => "COMMITTED",
            Self::Done => "DONE",
            Self::Aborted => "ABORTED",
        }
    }

    /// Parse a stored phase. `None` for anything unrecognised, which a caller
    /// must treat as "do not touch this row" rather than as any known phase.
    #[must_use]
    pub fn from_db(raw: &str) -> Option<Self> {
        match raw {
            "PENDING" => Some(Self::Pending),
            "COPIED" => Some(Self::Copied),
            "VERIFIED" => Some(Self::Verified),
            "COMMITTED" => Some(Self::Committed),
            "DONE" => Some(Self::Done),
            "ABORTED" => Some(Self::Aborted),
            _ => None,
        }
    }

    /// Has this migration passed the single atomic cutover commit?
    ///
    /// Past it the source is sealed, so "the source is no longer quiescent" is
    /// not a reason to stop — the only correct move is forward.
    #[must_use]
    pub const fn is_past_cutover(self) -> bool {
        matches!(self, Self::Committed | Self::Done)
    }
}

/// What a resume sweep sees when it finds a migration row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationObservation {
    /// The row's persisted phase.
    pub phase: MigrationPhase,
    /// Whether the **source** execution still satisfies [`assess_quiescence`].
    /// Ignored past the cutover, where the source is sealed by definition.
    pub source_still_quiescent: bool,
}

/// The single next step for a migration row. Pure, so every kill point resumes
/// deterministically.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationAction {
    /// (Re-)stage the copy on the target. Idempotent: a partial prior copy is
    /// discarded first, which is safe because a pre-cutover target copy is
    /// inert.
    StageCopy,
    /// Replay-verify the staged copy against the source.
    Verify,
    /// Re-check quiescence and commit the cutover.
    Cutover,
    /// Flip the target from `MIGRATING` to `RUNNING` and restore its parked
    /// workflow task. Idempotent.
    ActivateTarget,
    /// Abandon before cutover, leaving the source untouched.
    Abort,
    /// Nothing left to do; the row may be retired.
    Retire,
}

/// The kill-point contract, in pure form.
///
/// | Phase at crash | Action | Source authoritative? |
/// |---|---|---|
/// | `Pending` | re-stage | yes |
/// | `Copied` | verify | yes |
/// | `Verified` | cutover | yes |
/// | `Committed` | activate the target | **no** — the target is |
/// | `Done` / `Aborted` | retire | — |
///
/// Only `Verified → Committed` changes who is authoritative, and it is a
/// single-statement commit on one database.
#[must_use]
pub const fn next_migration_action(obs: &MigrationObservation) -> MigrationAction {
    match obs.phase {
        MigrationPhase::Done | MigrationPhase::Aborted => MigrationAction::Retire,
        // Past the cutover the source is sealed; rolling back would strand the
        // run on a shard that no longer claims it.
        MigrationPhase::Committed => MigrationAction::ActivateTarget,
        _ if !obs.source_still_quiescent => MigrationAction::Abort,
        MigrationPhase::Pending => MigrationAction::StageCopy,
        MigrationPhase::Copied => MigrationAction::Verify,
        MigrationPhase::Verified => MigrationAction::Cutover,
    }
}

// ── Forward-chain resolution ─────────────────────────────────────────────────

/// How many forwarding hops an id-routed lookup will follow before failing
/// closed.
///
/// Deliberately small. A chain longer than this means either a cycle (which a
/// routing call must never spin on) or a run migrated more often than the
/// best-effort chain collapse could keep up with, and both are operator-visible
/// problems rather than things to paper over with a longer walk.
pub const MAX_FORWARD_HOPS: usize = 4;

/// How many phase transitions one `resume_incomplete_migrations` call will drive
/// for a single migration row before leaving it for the next call.
///
/// The phase machine is a five-step line (`PENDING → COPIED → VERIFIED →
/// COMMITTED → DONE`), so a row picked up at its very first phase settles in
/// four steps; the extra one is headroom for a step that legitimately does not
/// advance the phase. It exists so a row that cannot make progress is abandoned
/// rather than spun on.
pub const MAX_RESUME_STEPS: usize = 6;

/// Follow a forwarding chain from an execution's origin shard to the shard it
/// currently lives on.
///
/// `lookup` answers "where did this shard forward this execution to?" —
/// `None` meaning "it lives here". Chains arise when a run is migrated more
/// than once (A→B, then B→C) before the best-effort chain collapse rewrites
/// A's pointer straight to C; correctness comes from following the chain,
/// performance from collapsing it.
///
/// # Errors
///
/// Returns [`HarvestError::ShardUnavailable`] when the chain exceeds
/// [`MAX_FORWARD_HOPS`], which is how a cycle surfaces as a typed, retryable
/// error instead of an infinite loop inside a routing call.
pub fn resolve_forward_chain(
    origin: ShardId,
    mut lookup: impl FnMut(ShardId) -> Option<ShardId>,
) -> HarvestResult<ShardId> {
    let mut current = origin;
    for _ in 0..MAX_FORWARD_HOPS {
        match lookup(current) {
            None => return Ok(current),
            Some(next) => current = next,
        }
    }
    Err(HarvestError::ShardUnavailable {
        shard_id: origin.as_i32(),
        reason: format!(
            "execution forwarding chain from shard {} exceeded {MAX_FORWARD_HOPS} hops; \
             this is a forwarding cycle or an unresolvably long migration chain",
            origin.as_i32()
        ),
    })
}

/// Relations copied verbatim to the target shard, in insert order.
///
/// Parents first. `harvest_events` is copied separately because its `id` is a
/// shard-local `BIGSERIAL` that must **not** be carried across.
///
/// Named here rather than inline so the schema-parity guard and the copy walk
/// the same list: a relation added to one and not the other is a compile-time
/// impossibility rather than a silent data loss.
pub const COPIED_RELATIONS: &[&str] = &[
    "harvest_workflow_executions",
    "harvest_timers",
    "harvest_signals",
    "harvest_payload_refs",
    "harvest_workflow_logs",
];

/// Relations whose schemas must match between source and target before a
/// migration may start.
///
/// A superset of [`COPIED_RELATIONS`]: it also carries `harvest_events` (copied
/// with an explicit column list) and `harvest_task_queue`, whose parked row is
/// restored on the target **at activation** — that is, *past* the cutover, where
/// there is no longer anything to abort. A column mismatch discovered there
/// would leave the run `RUNNING` on the target with no task row on either shard,
/// so `harvest_task_queue` has to be checked here, before the seal, even though
/// it is not part of the staged copy.
pub const SCHEMA_PARITY_RELATIONS: &[&str] = &[
    "harvest_workflow_executions",
    "harvest_events",
    "harvest_timers",
    "harvest_signals",
    "harvest_payload_refs",
    "harvest_workflow_logs",
    "harvest_task_queue",
];

/// The columns copied out of `harvest_events`.
///
/// Deliberately *not* `SELECT *`: `id` is a shard-local `BIGSERIAL` and
/// `cohort` (issue #958 partitioning) is the row's append instant on *this*
/// shard, so both are re-derived by the target's own defaults. Everything that
/// is part of the history's meaning — `event_id`, `event_type`, `event_data`,
/// `timestamp` — is carried byte-for-byte.
pub const COPIED_EVENT_COLUMNS: &[&str] = &[
    "workflow_exec_id",
    "event_id",
    "event_type",
    "event_data",
    "timestamp",
];

/// A `WorkflowReplayer`-grade fingerprint of the state a history replays to.
///
/// Two histories with the same fingerprint agree on every decoded event
/// *and* on the next-command state the replay cursor reaches after
/// consuming them: the terminal-failure frontier, whether unconsumed
/// non-lifecycle history remains, which signals are still unconsumed and at
/// what multiplicity, and which update handlers are still unfinished. That
/// is precisely the state that decides what command the workflow issues
/// next, which is what "replays to the identical next-command state" means.
///
/// Hashed rather than compared structurally so the value can be stored on
/// the migration row and shown to an operator: verification says not only
/// *that* it passed but *what* it agreed on.
#[must_use]
pub fn history_fingerprint(events: &[crate::event::WorkflowEvent]) -> String {
    let mut hasher = Sha256::new();

    // 1. The decoded events themselves, in order.
    for event in events {
        let canonical =
            serde_json::to_string(event).unwrap_or_else(|e| format!("<unserializable event: {e}>"));
        hasher.update(canonical.as_bytes());
        hasher.update([0u8]);
    }

    // 2. The replay cursor's own reading of that history — the state that
    //    decides what command the workflow issues next. Every accessor here
    //    is end-anchored or whole-history, so nothing depends on how far a
    //    cursor happened to be driven.
    let matcher = HistoryMatcher::new(events.to_vec());
    hasher.update(b"|cursor|");
    hasher.update(matcher.event_count().to_be_bytes());
    hasher.update(
        HistoryMatcher::terminal_failure_tail_start(events)
            .map_or(u64::MAX, |index| u64::try_from(index).unwrap_or(u64::MAX))
            .to_be_bytes(),
    );
    hasher.update([u8::from(matcher.has_non_lifecycle_unconsumed())]);
    hasher.update([u8::from(matcher.all_handlers_finished_at_end())]);
    for (name, count) in matcher.unconsumed_signals_by_name() {
        hasher.update(name.as_bytes());
        hasher.update(b"=");
        hasher.update(count.to_be_bytes());
        hasher.update([0u8]);
    }
    for update_id in matcher.unfinished_update_handlers_at_end() {
        hasher.update(update_id.to_string().as_bytes());
        hasher.update([0u8]);
    }

    format!("{:x}", hasher.finalize())
}

#[cfg(feature = "db")]
pub use db::{
    MigrationBatchReport, MigrationOutcome, MigrationRecord, ShardMigrationCandidate,
    abort_migration, activate_target, assert_schema_parity, begin_migration, commit_cutover,
    conn_for_execution_forwarded, conn_for_live_shard, conn_for_shard, forward_of_held_row,
    list_migration_candidates, load_migration, migrate_execution, migrate_quiescent_executions,
    observe_quiescence, residence_chain, resolve_execution_shard, resolve_target_shard,
    resolve_target_shard_holding, resume_incomplete_migrations, shard_of_held_row, stage_copy,
    verify_target_copy,
};

#[cfg(feature = "db")]
mod db {
    use chrono::{DateTime, Utc};
    use diesel::sql_types::{
        BigInt, Bool, Integer, Jsonb, Nullable, Text, Timestamptz, Uuid as SqlUuid,
    };
    use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
    use serde::Serialize;
    use serde_json::Value;
    use uuid::Uuid;

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::payload_codec::PayloadCodecs;
    use crate::shard::ShardedDbPool;
    use crate::types::{ExecutionId, ShardId};

    use super::{
        MAX_FORWARD_HOPS, MAX_RESUME_STEPS, MigrationAction, MigrationObservation, MigrationPhase,
        Quiescence, QuiescenceBlocker, QuiescenceObservation, SCHEMA_PARITY_RELATIONS,
        assess_quiescence, history_fingerprint, next_migration_action, resolve_forward_chain,
    };

    /// The SQL half of the quiescence predicate, as one AND-able fragment over
    /// an alias `e` bound to `harvest_workflow_executions`.
    ///
    /// This exists so the cutover's `WHERE` clause and the candidate re-check
    /// evaluate **the same facts** the pure predicate does. It is the one place
    /// SQL and Rust could drift, so it is written once and
    /// `the_sql_predicate_agrees_with_the_pure_predicate` in
    /// `shard_rebalance_db_tests.rs` pins them together against real rows.
    const QUIESCENCE_SQL: &str = "\
        e.state = 'RUNNING' \
        AND e.parent_id IS NULL \
        AND e.schedule_id IS NULL \
        AND e.nd_blocked_at IS NULL \
        AND NOT EXISTS (SELECT 1 FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
            AND t.task_type = 'workflow' AND t.state = 'RUNNING' AND t.worker_id IS NOT NULL) \
        AND NOT EXISTS (SELECT 1 FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
            AND t.task_type = 'workflow' AND t.state = 'PENDING' AND t.scheduled_at <= NOW()) \
        AND NOT EXISTS (SELECT 1 FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
            AND t.task_type = 'workflow' AND t.state IN ('PENDING', 'RUNNING') AND t.wake_requested) \
        AND (SELECT count(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
            AND t.task_type = 'workflow' AND ((t.state = 'RUNNING' AND t.worker_id IS NULL) \
            OR (t.state = 'PENDING' AND t.scheduled_at > NOW()))) <= 1 \
        AND NOT EXISTS (SELECT 1 FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
            AND t.task_type <> 'workflow' AND t.state IN ('PENDING', 'RUNNING')) \
        AND NOT EXISTS (SELECT 1 FROM harvest_signals s WHERE s.workflow_exec_id = e.id \
            AND NOT s.consumed) \
        AND NOT EXISTS (SELECT 1 FROM harvest_completion_deliveries d \
            WHERE d.workflow_exec_id = e.id \
            AND d.state IN ('PENDING', 'INFLIGHT')) \
        AND NOT EXISTS (SELECT 1 FROM harvest_sessions ss WHERE ss.workflow_exec_id = e.id \
            AND ss.state = 'ACTIVE') \
        AND NOT EXISTS (SELECT 1 FROM harvest_external_tasks x WHERE x.workflow_exec_id = e.id \
            AND x.state = 'PENDING') \
        AND NOT EXISTS (SELECT 1 FROM harvest_workflow_executions c WHERE c.parent_id = e.id \
            AND c.state IN ('RUNNING', 'PAUSED', 'MIGRATING', 'MIGRATED')) \
        AND NOT EXISTS (SELECT 1 FROM harvest_cross_shard_children x \
            WHERE x.parent_exec_id = e.id) \
        AND NOT EXISTS (SELECT 1 FROM harvest_mutex_locks ml \
            WHERE ml.holder_exec_id = e.id) \
        AND NOT EXISTS (SELECT 1 FROM harvest_mutex_waiters mw \
            WHERE mw.waiter_exec_id = e.id) \
        AND NOT EXISTS (SELECT 1 FROM harvest_dead_letters dl \
            WHERE dl.workflow_exec_id = e.id)";

    /// The second half of the cutover's `WHERE`: "the source history is still
    /// exactly the history verification passed on".
    ///
    /// Quiescence alone is not enough to license a seal. Verification proves the
    /// copy matches the source *as of the copy*; the cutover happens afterwards —
    /// immediately in the end-to-end path, but possibly hours later on a resume
    /// after a crash at `VERIFIED`. In between, the run can legitimately wake,
    /// execute a full decision cycle, append events and park again on a fresh
    /// timer. It is quiescent once more, and every predicate in
    /// [`QUIESCENCE_SQL`] passes — so a cutover checking only quiescence would seal
    /// it and activate a copy missing everything that cycle did. Lost progress, not
    /// a lost wake, and invisible afterwards.
    ///
    /// Events are append-only with a monotonic per-execution `event_id`, so the
    /// `(count, max)` pair recorded at verification moves on any append. A record
    /// with no recorded mark — one written before this guard existed — matches
    /// nothing and declines, which is the fail-closed direction.
    const HISTORY_UNCHANGED_SQL: &str = "\
        EXISTS (SELECT 1 FROM harvest_shard_migrations m \
                 WHERE m.execution_id = e.id \
                   AND m.verified_event_count IS NOT NULL \
                   AND m.verified_max_event_id IS NOT NULL \
                   AND m.verified_event_count = (SELECT count(*) FROM harvest_events ev \
                                                  WHERE ev.workflow_exec_id = e.id) \
                   AND m.verified_max_event_id = \
                         COALESCE((SELECT max(ev.event_id) FROM harvest_events ev \
                                    WHERE ev.workflow_exec_id = e.id), -1))";

    #[derive(diesel::QueryableByName)]
    struct QuiescenceRow {
        #[diesel(sql_type = Text)]
        state: String,
        #[diesel(sql_type = Nullable<SqlUuid>)]
        parent_id: Option<Uuid>,
        #[diesel(sql_type = Bool)]
        schedule_attributed: bool,
        #[diesel(sql_type = Bool)]
        nd_blocked: bool,
        #[diesel(sql_type = BigInt)]
        claimed_workflow_tasks: i64,
        #[diesel(sql_type = BigInt)]
        due_pending_tasks: i64,
        #[diesel(sql_type = BigInt)]
        parked_workflow_tasks: i64,
        #[diesel(sql_type = Bool)]
        wake_requested: bool,
        #[diesel(sql_type = BigInt)]
        live_activity_tasks: i64,
        #[diesel(sql_type = BigInt)]
        unconsumed_signals: i64,
        #[diesel(sql_type = BigInt)]
        inflight_completion_deliveries: i64,
        #[diesel(sql_type = BigInt)]
        active_sessions: i64,
        #[diesel(sql_type = BigInt)]
        live_external_tasks: i64,
        #[diesel(sql_type = BigInt)]
        live_children: i64,
        #[diesel(sql_type = BigInt)]
        cross_shard_child_rows: i64,
        #[diesel(sql_type = BigInt)]
        held_mutex_locks: i64,
        #[diesel(sql_type = BigInt)]
        queued_mutex_waiters: i64,
        #[diesel(sql_type = BigInt)]
        dead_letter_rows: i64,
    }

    const OBSERVE_SQL: &str = "\
        SELECT e.state, \
               e.parent_id, \
               (e.schedule_id IS NOT NULL) AS schedule_attributed, \
               (e.nd_blocked_at IS NOT NULL) AS nd_blocked, \
               (SELECT count(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
                  AND t.task_type = 'workflow' AND t.state = 'RUNNING' \
                  AND t.worker_id IS NOT NULL)::BIGINT AS claimed_workflow_tasks, \
               (SELECT count(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
                  AND t.task_type = 'workflow' AND t.state = 'PENDING' \
                  AND t.scheduled_at <= NOW())::BIGINT AS due_pending_tasks, \
               (SELECT count(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
                  AND t.task_type = 'workflow' \
                  AND ((t.state = 'RUNNING' AND t.worker_id IS NULL) \
                       OR (t.state = 'PENDING' AND t.scheduled_at > NOW())))::BIGINT \
                  AS parked_workflow_tasks, \
               COALESCE((SELECT bool_or(t.wake_requested) FROM harvest_task_queue t \
                  WHERE t.workflow_exec_id = e.id AND t.task_type = 'workflow' \
                    AND t.state IN ('PENDING', 'RUNNING')), FALSE) AS wake_requested, \
               (SELECT count(*) FROM harvest_task_queue t WHERE t.workflow_exec_id = e.id \
                  AND t.task_type <> 'workflow' \
                  AND t.state IN ('PENDING', 'RUNNING'))::BIGINT AS live_activity_tasks, \
               (SELECT count(*) FROM harvest_signals s WHERE s.workflow_exec_id = e.id \
                  AND NOT s.consumed)::BIGINT AS unconsumed_signals, \
               (SELECT count(*) FROM harvest_completion_deliveries d \
                  WHERE d.workflow_exec_id = e.id \
                    AND d.state IN ('PENDING', 'INFLIGHT'))::BIGINT \
                  AS inflight_completion_deliveries, \
               (SELECT count(*) FROM harvest_sessions ss WHERE ss.workflow_exec_id = e.id \
                  AND ss.state = 'ACTIVE')::BIGINT AS active_sessions, \
               (SELECT count(*) FROM harvest_external_tasks x WHERE x.workflow_exec_id = e.id \
                  AND x.state = 'PENDING')::BIGINT AS live_external_tasks, \
               (SELECT count(*) FROM harvest_workflow_executions c WHERE c.parent_id = e.id \
                  AND c.state IN ('RUNNING', 'PAUSED', 'MIGRATING', 'MIGRATED'))::BIGINT \
                  AS live_children, \
               (SELECT count(*) FROM harvest_cross_shard_children x \
                  WHERE x.parent_exec_id = e.id)::BIGINT AS cross_shard_child_rows, \
               (SELECT count(*) FROM harvest_mutex_locks ml \
                  WHERE ml.holder_exec_id = e.id)::BIGINT AS held_mutex_locks, \
               (SELECT count(*) FROM harvest_mutex_waiters mw \
                  WHERE mw.waiter_exec_id = e.id)::BIGINT AS queued_mutex_waiters, \
               (SELECT count(*) FROM harvest_dead_letters dl \
                  WHERE dl.workflow_exec_id = e.id)::BIGINT AS dead_letter_rows \
        FROM harvest_workflow_executions e \
        WHERE e.id = $1";

    /// Gather the facts [`assess_quiescence`] needs for one execution.
    ///
    /// # Errors
    ///
    /// [`HarvestError::NotFound`] when the execution does not exist on this
    /// shard, [`HarvestError::Database`] on query failure.
    pub async fn observe_quiescence(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<QuiescenceObservation> {
        let row: Option<QuiescenceRow> = diesel::sql_query(OBSERVE_SQL)
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .optional_row()?;

        let row = row.ok_or_else(|| HarvestError::NotFound(exec_id.to_string()))?;

        Ok(QuiescenceObservation {
            state: row.state,
            parent_id: row.parent_id.map(ExecutionId::from_uuid),
            schedule_attributed: row.schedule_attributed,
            claimed_workflow_tasks: row.claimed_workflow_tasks,
            due_pending_tasks: row.due_pending_tasks,
            parked_workflow_tasks: row.parked_workflow_tasks,
            wake_requested: row.wake_requested,
            live_activity_tasks: row.live_activity_tasks,
            unconsumed_signals: row.unconsumed_signals,
            inflight_completion_deliveries: row.inflight_completion_deliveries,
            active_sessions: row.active_sessions,
            live_external_tasks: row.live_external_tasks,
            live_children: row.live_children,
            cross_shard_child_rows: row.cross_shard_child_rows,
            held_mutex_locks: row.held_mutex_locks,
            queued_mutex_waiters: row.queued_mutex_waiters,
            dead_letter_rows: row.dead_letter_rows,
            nd_blocked: row.nd_blocked,
        })
    }

    trait OptionalRow<T> {
        fn optional_row(self) -> HarvestResult<Option<T>>;
    }

    impl<T> OptionalRow<T> for Result<T, diesel::result::Error> {
        fn optional_row(self) -> HarvestResult<Option<T>> {
            match self {
                Ok(value) => Ok(Some(value)),
                Err(diesel::result::Error::NotFound) => Ok(None),
                Err(e) => Err(database_error(e)),
            }
        }
    }

    // ── Schema parity ────────────────────────────────────────────────────────

    const COLUMN_SIGNATURE_SQL: &str = "\
        SELECT COALESCE(string_agg( \
                   table_name || '.' || column_name || ':' || data_type \
                     || ':' || COALESCE(udt_name, '') \
                     || ':' || COALESCE(character_maximum_length::text, '') \
                     || ':' || COALESCE(numeric_precision::text, '') \
                     || ':' || COALESCE(numeric_scale::text, '') \
                     || ':' || is_nullable, ',' \
                   ORDER BY table_name, ordinal_position), '') AS signature \
        FROM information_schema.columns \
        WHERE table_schema = current_schema() AND table_name = ANY($1)";

    #[derive(diesel::QueryableByName)]
    struct SignatureRow {
        #[diesel(sql_type = Text)]
        signature: String,
    }

    /// Refuse to copy between shards whose schemas disagree.
    ///
    /// The copy is deliberately column-list-free (`to_jsonb` on the source,
    /// `jsonb_populate_record` on the target) so that adding a column to
    /// `harvest_workflow_executions` can never silently drop it from a
    /// migration. The price of that is the converse hazard: if the target shard
    /// is behind on migrations, its row type lacks the column,
    /// `jsonb_populate_record` **ignores the unknown key silently**, and the
    /// value is lost with no error. This guard converts that into a refusal
    /// before anything is written.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Config`] when the two shards' copied relations differ,
    /// [`HarvestError::Database`] on query failure.
    pub async fn assert_schema_parity(
        source: &mut AsyncPgConnection,
        target: &mut AsyncPgConnection,
    ) -> HarvestResult<()> {
        let relations: Vec<String> = SCHEMA_PARITY_RELATIONS
            .iter()
            .map(|r| (*r).to_string())
            .collect();

        let signature_of = async |conn: &mut AsyncPgConnection| -> HarvestResult<String> {
            let row: SignatureRow = diesel::sql_query(COLUMN_SIGNATURE_SQL)
                .bind::<diesel::sql_types::Array<Text>, _>(&relations)
                .get_result(conn)
                .await
                .map_err(database_error)?;
            Ok(row.signature)
        };

        let source_signature = signature_of(source).await?;
        let target_signature = signature_of(target).await?;

        if source_signature == target_signature {
            return Ok(());
        }
        Err(HarvestError::Config(
            "source and target shards disagree on the schema of the copied relations; \
             run `harvest migrate run` against both shards so they are at the same \
             migration level before rebalancing (a column present on one and not the \
             other would be silently dropped by the copy)"
                .to_string(),
        ))
    }

    // ── The durable migration record ─────────────────────────────────────────

    /// One row of `harvest_shard_migrations`, on the source shard.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct MigrationRecord {
        /// The execution being moved. Never re-minted by a migration.
        pub execution_id: ExecutionId,
        /// The shard the run is moving off.
        pub source_shard: ShardId,
        /// The shard the run is moving to.
        pub target_shard: ShardId,
        /// How far the migration has got.
        pub phase: MigrationPhase,
        /// The replay fingerprint the staged copy produced, once verified.
        pub verified_fingerprint: Option<String>,
        /// Why an aborted migration aborted.
        pub abort_reason: Option<String>,
        /// Non-progress attempts, which also drive the resume backoff.
        pub attempts: i32,
        /// The last failure this row saw, verbatim.
        pub last_error: Option<String>,
        /// When the row was opened.
        pub created_at: DateTime<Utc>,
        /// When the row last advanced.
        pub updated_at: DateTime<Utc>,
    }

    #[derive(diesel::QueryableByName)]
    struct MigrationRow {
        #[diesel(sql_type = SqlUuid)]
        execution_id: Uuid,
        #[diesel(sql_type = Integer)]
        source_shard: i32,
        #[diesel(sql_type = Integer)]
        target_shard: i32,
        #[diesel(sql_type = Text)]
        phase: String,
        #[diesel(sql_type = Nullable<Text>)]
        verified_fingerprint: Option<String>,
        #[diesel(sql_type = Nullable<Text>)]
        abort_reason: Option<String>,
        #[diesel(sql_type = Integer)]
        attempts: i32,
        #[diesel(sql_type = Nullable<Text>)]
        last_error: Option<String>,
        #[diesel(sql_type = Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    impl MigrationRow {
        fn into_record(self) -> HarvestResult<MigrationRecord> {
            let phase = MigrationPhase::from_db(&self.phase).ok_or_else(|| {
                HarvestError::Database(format!(
                    "harvest_shard_migrations row for {} carries an unrecognised phase {:?}",
                    self.execution_id, self.phase
                ))
            })?;
            Ok(MigrationRecord {
                execution_id: ExecutionId::from_uuid(self.execution_id),
                source_shard: ShardId::new(self.source_shard),
                target_shard: ShardId::new(self.target_shard),
                phase,
                verified_fingerprint: self.verified_fingerprint,
                abort_reason: self.abort_reason,
                attempts: self.attempts,
                last_error: self.last_error,
                created_at: self.created_at,
                updated_at: self.updated_at,
            })
        }
    }

    const MIGRATION_COLUMNS: &str = "execution_id, source_shard, target_shard, phase, \
        verified_fingerprint, abort_reason, attempts, last_error, created_at, updated_at";

    /// Read one migration record.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on query failure, or when a stored phase is
    /// not one this build understands.
    pub async fn load_migration(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<MigrationRecord>> {
        let row: Option<MigrationRow> = diesel::sql_query(format!(
            "SELECT {MIGRATION_COLUMNS} FROM harvest_shard_migrations WHERE execution_id = $1"
        ))
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .optional_row()?;
        row.map(MigrationRow::into_record).transpose()
    }

    /// Open (or re-open) a migration for one execution.
    ///
    /// The primary key on `execution_id` is what makes two concurrent operators
    /// safe: the second `INSERT` collides rather than opening a second
    /// migration for the same run. A previously settled row (`DONE`/`ABORTED`)
    /// is reset to `PENDING`, so a run that failed verification once can be
    /// retried without operator surgery.
    ///
    /// # Errors
    ///
    /// [`HarvestError::AlreadyExists`] when a migration for this execution is
    /// already in flight, [`HarvestError::Database`] on failure.
    pub async fn begin_migration(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        source_shard: ShardId,
        target_shard: ShardId,
    ) -> HarvestResult<MigrationRecord> {
        let row: Option<MigrationRow> = diesel::sql_query(format!(
            "INSERT INTO harvest_shard_migrations \
                 (execution_id, source_shard, target_shard, phase) \
             VALUES ($1, $2, $3, 'PENDING') \
             ON CONFLICT (execution_id) DO UPDATE \
                 SET phase = 'PENDING', target_shard = EXCLUDED.target_shard, \
                     source_shard = EXCLUDED.source_shard, verified_fingerprint = NULL, \
                     abort_reason = NULL, attempts = 0, last_error = NULL, \
                     updated_at = NOW() \
                 WHERE harvest_shard_migrations.phase IN ('DONE', 'ABORTED') \
             RETURNING {MIGRATION_COLUMNS}"
        ))
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Integer, _>(source_shard.as_i32())
        .bind::<Integer, _>(target_shard.as_i32())
        .get_result(conn)
        .await
        .optional_row()?;

        // `DO UPDATE ... WHERE` matching nothing means a migration for this
        // execution is already in flight. Never silently adopt it — the
        // in-flight one may be targeting a different shard.
        row.map_or_else(
            || {
                Err(HarvestError::AlreadyExists {
                    existing_exec_id: exec_id,
                    existing_state: "a shard migration for this execution is already in flight"
                        .to_string(),
                })
            },
            MigrationRow::into_record,
        )
    }

    async fn record_attempt(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        error: &str,
    ) -> HarvestResult<()> {
        diesel::sql_query(
            "UPDATE harvest_shard_migrations \
                SET attempts = attempts + 1, last_error = $2, last_attempt_at = NOW(), \
                    updated_at = NOW() \
              WHERE execution_id = $1",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(error)
        .execute(conn)
        .await
        .map_err(database_error)?;
        Ok(())
    }

    async fn set_phase(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        from: MigrationPhase,
        to: MigrationPhase,
    ) -> HarvestResult<()> {
        let updated = diesel::sql_query(
            "UPDATE harvest_shard_migrations SET phase = $3, updated_at = NOW() \
              WHERE execution_id = $1 AND phase = $2",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(from.as_db())
        .bind::<Text, _>(to.as_db())
        .execute(conn)
        .await
        .map_err(database_error)?;
        if updated == 0 {
            return Err(HarvestError::Database(format!(
                "migration for {exec_id} was not in phase {} when advancing to {}; \
                 another process is driving it",
                from.as_db(),
                to.as_db()
            )));
        }
        Ok(())
    }

    // ── Phase 1: stage the copy ──────────────────────────────────────────────

    #[derive(diesel::QueryableByName)]
    struct JsonRow {
        #[diesel(sql_type = Jsonb)]
        payload: Value,
    }

    #[derive(diesel::QueryableByName)]
    struct NullableJsonRow {
        #[diesel(sql_type = Nullable<Jsonb>)]
        payload: Option<Value>,
    }

    #[derive(diesel::QueryableByName)]
    struct TextRow {
        #[diesel(sql_type = Nullable<Text>)]
        value: Option<String>,
    }

    async fn read_json(
        conn: &mut AsyncPgConnection,
        sql: &str,
        exec_id: ExecutionId,
    ) -> HarvestResult<Value> {
        let row: JsonRow = diesel::sql_query(sql)
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .get_result(conn)
            .await
            .map_err(database_error)?;
        Ok(row.payload)
    }

    /// Copy one execution's durable state onto the target shard as an inert
    /// `MIGRATING` row.
    ///
    /// Everything is written in **one target-shard transaction**, so a crash
    /// mid-copy leaves the target either untouched or fully staged, never
    /// half-copied. The staged copy is inert on two independent grounds: its
    /// execution state is `MIGRATING` (which no dispatch path treats as
    /// runnable) and it has **no workflow task row at all** — that row is held
    /// in `harvest_shard_migrations.staged_task` on the source until activation.
    ///
    /// The copy is column-list-free by design: `to_jsonb` on the source and
    /// `jsonb_populate_record` on the target mean a column added to
    /// `harvest_workflow_executions` is carried automatically rather than
    /// silently dropped by a hand-maintained list that nobody remembered to
    /// update. [`assert_schema_parity`] closes the converse hazard.
    ///
    /// The source is not modified in any way.
    ///
    /// # Errors
    ///
    /// [`HarvestError::NotFound`] when the execution is not on the source,
    /// [`HarvestError::AlreadyExists`] when the target already holds a live row
    /// for this id, [`HarvestError::Database`] on failure.
    #[allow(clippy::too_many_lines)] // One linear copy sequence: splitting it
    // would scatter the single target transaction that makes staging atomic.
    pub async fn stage_copy(
        source: &mut AsyncPgConnection,
        target: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        target_shard: ShardId,
    ) -> HarvestResult<()> {
        assert_schema_parity(source, target).await?;

        let execution = {
            let row: Option<JsonRow> = diesel::sql_query(
                "SELECT to_jsonb(e) AS payload FROM harvest_workflow_executions e WHERE e.id = $1",
            )
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .get_result(source)
            .await
            .optional_row()?;
            row.ok_or_else(|| HarvestError::NotFound(exec_id.to_string()))?
                .payload
        };

        let events = read_json(
            source,
            "SELECT COALESCE(jsonb_agg(jsonb_build_object( \
                 'workflow_exec_id', ev.workflow_exec_id, \
                 'event_id', ev.event_id, \
                 'event_type', ev.event_type, \
                 'event_data', ev.event_data, \
                 'timestamp', ev.timestamp) ORDER BY ev.event_id), '[]'::jsonb) AS payload \
             FROM harvest_events ev WHERE ev.workflow_exec_id = $1",
            exec_id,
        )
        .await?;

        let timers = read_json(
            source,
            "SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb) AS payload \
             FROM harvest_timers t WHERE t.workflow_exec_id = $1",
            exec_id,
        )
        .await?;

        let signals = read_json(
            source,
            "SELECT COALESCE(jsonb_agg(to_jsonb(s)), '[]'::jsonb) AS payload \
             FROM harvest_signals s WHERE s.workflow_exec_id = $1",
            exec_id,
        )
        .await?;

        let payload_refs = read_json(
            source,
            "SELECT COALESCE(jsonb_agg(to_jsonb(p)), '[]'::jsonb) AS payload \
             FROM harvest_payload_refs p WHERE p.workflow_exec_id = $1",
            exec_id,
        )
        .await?;

        // Author-emitted `ctx.logger()` lines. Copied rather than left behind
        // for a privacy reason as much as an operational one: they are free
        // text, so a classic PII sink, and `erase.rs` scrubs them via the
        // execution's own shard. A run whose logs stayed on the source would
        // have them out of reach of an erasure issued against the run.
        let workflow_logs = read_json(
            source,
            "SELECT COALESCE(jsonb_agg(to_jsonb(l)), '[]'::jsonb) AS payload \
             FROM harvest_workflow_logs l WHERE l.workflow_exec_id = $1",
            exec_id,
        )
        .await?;

        // The parked workflow task, captured but NOT staged (see the doc
        // comment). `to_jsonb` keeps every column, including the sticky hint and
        // the concurrency key, so the restored row is the one that was parked.
        let staged_task: Option<Value> = {
            let row: Option<NullableJsonRow> = diesel::sql_query(
                "SELECT to_jsonb(t) AS payload FROM harvest_task_queue t \
                  WHERE t.workflow_exec_id = $1 AND t.task_type = 'workflow' \
                    AND ((t.state = 'RUNNING' AND t.worker_id IS NULL) \
                         OR (t.state = 'PENDING' AND t.scheduled_at > NOW())) \
                  ORDER BY t.scheduled_at LIMIT 1",
            )
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .get_result(source)
            .await
            .optional_row()?;
            row.and_then(|r| r.payload)
        };

        Box::pin(target.transaction::<(), HarvestError, _>(async |conn| {
            // On a REVERSE migration (A -> B -> A) the target is a shard this
            // run has already lived on, so it holds A's `MIGRATED` seal — the
            // forwarding pointer every id that routes to A resolves through.
            // Staging replaces that row, so the pointer is carried onto the
            // staged copy and kept there until activation clears it.
            //
            // Without this, ids routing to A would stop at an inert `MIGRATING`
            // copy for the whole staging window instead of reaching live B, and
            // an abort would delete even that, leaving the execution with no row
            // on its origin shard at all: an id that resolves nowhere.
            let prior_seal = existing_seal(&mut *conn, exec_id).await?;
            discard_staged_copy(&mut *conn, exec_id).await?;

            diesel::sql_query(
                "INSERT INTO harvest_workflow_executions \
                         SELECT * FROM jsonb_populate_record( \
                             NULL::harvest_workflow_executions, \
                             $1::jsonb || jsonb_build_object( \
                                 'shard_id', $2::int, \
                                 'state', 'MIGRATING', \
                                 'migrated_to_shard', $3::int, \
                                 'migrated_at', $4::timestamptz))",
            )
            .bind::<Jsonb, _>(&execution)
            .bind::<Integer, _>(target_shard.as_i32())
            .bind::<Nullable<Integer>, _>(prior_seal.map(|(shard, _)| shard))
            .bind::<Nullable<Timestamptz>, _>(prior_seal.map(|(_, at)| at))
            .execute(&mut *conn)
            .await
            .map_err(database_error)?;

            diesel::sql_query(
                "INSERT INTO harvest_events \
                             (workflow_exec_id, event_id, event_type, event_data, timestamp) \
                         SELECT workflow_exec_id, event_id, event_type, event_data, \"timestamp\" \
                         FROM jsonb_to_recordset($1::jsonb) AS r( \
                             workflow_exec_id uuid, event_id integer, event_type text, \
                             event_data jsonb, \"timestamp\" timestamptz) \
                         ORDER BY event_id",
            )
            .bind::<Jsonb, _>(&events)
            .execute(&mut *conn)
            .await
            .map_err(database_error)?;

            for (table, rows) in [
                ("harvest_timers", &timers),
                ("harvest_signals", &signals),
                ("harvest_payload_refs", &payload_refs),
            ] {
                diesel::sql_query(format!(
                    "INSERT INTO {table} SELECT * FROM \
                             jsonb_populate_recordset(NULL::{table}, $1::jsonb)"
                ))
                .bind::<Jsonb, _>(rows)
                .execute(&mut *conn)
                .await
                .map_err(database_error)?;
            }

            // `harvest_workflow_logs` gets an EXPLICIT column list, for exactly
            // the reason `harvest_events` does: its `id` is a shard-local
            // `BIGSERIAL`. Carrying the source's value would either collide
            // with a row the target already has -- two independent sequences
            // hand out the same small integers -- or, worse, land in a gap
            // WITHOUT advancing the target's sequence, so a later log insert on
            // the target collides with the copy instead. Letting the target
            // mint the id keeps `seq`, which is the per-execution ordering that
            // actually carries the meaning.
            diesel::sql_query(
                "INSERT INTO harvest_workflow_logs \
                     (workflow_exec_id, seq, level, message, occurred_at) \
                 SELECT workflow_exec_id, seq, level, message, occurred_at \
                 FROM jsonb_to_recordset($1::jsonb) AS r( \
                     workflow_exec_id uuid, seq bigint, level text, \
                     message text, occurred_at timestamptz) \
                 ORDER BY seq",
            )
            .bind::<Jsonb, _>(&workflow_logs)
            .execute(&mut *conn)
            .await
            .map_err(database_error)?;

            Ok(())
        }))
        .await?;

        diesel::sql_query(
            "UPDATE harvest_shard_migrations SET staged_task = $2, updated_at = NOW() \
              WHERE execution_id = $1",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Nullable<Jsonb>, _>(staged_task)
        .execute(source)
        .await
        .map_err(database_error)?;

        set_phase(
            source,
            exec_id,
            MigrationPhase::Pending,
            MigrationPhase::Copied,
        )
        .await
    }

    /// Delete a staged (`MIGRATING`) copy from the target, or verify there is
    /// nothing to delete.
    ///
    /// Refuses loudly if the target holds a row for this id in any other state:
    /// that is a live execution the copy would clobber, and the only safe
    /// answer is to stop.
    /// Undo a staged copy, **restoring** any forwarding seal the target shard
    /// held before staging replaced it.
    ///
    /// [`discard_staged_copy`] deletes the row outright, which is right when the
    /// target never hosted this run. On a reverse migration (A → B → A) it is
    /// not: the row staging replaced was A's own seal, and deleting it leaves
    /// every id that routes to A resolving nowhere. A staged row that carries a
    /// forwarding pointer is therefore returned to `MIGRATED` in place rather
    /// than removed — its copied history is discarded either way, since the live
    /// copy on the other shard is a superset of it.
    async fn discard_staged_copy_restoring_seal(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        for sql in STAGED_CHILD_DELETES.iter().copied() {
            diesel::sql_query(sql)
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .execute(&mut *conn)
                .await
                .map_err(database_error)?;
        }
        // A carried pointer is the tell: the staged row stands where a seal
        // stood, so restore the seal instead of removing the row.
        let restored = diesel::sql_query(
            "UPDATE harvest_workflow_executions \
                SET state = 'MIGRATED', completed_at = COALESCE(completed_at, NOW()) \
              WHERE id = $1 AND state = 'MIGRATING' AND migrated_to_shard IS NOT NULL",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(&mut *conn)
        .await
        .map_err(database_error)?;
        if restored == 0 {
            diesel::sql_query(
                "DELETE FROM harvest_workflow_executions WHERE id = $1 \
                   AND state IN ('MIGRATING', 'MIGRATED')",
            )
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .execute(&mut *conn)
            .await
            .map_err(database_error)?;
        }
        Ok(())
    }

    /// Everything a staged copy writes apart from the execution row itself.
    const STAGED_CHILD_DELETES: &[&str] = &[
        "DELETE FROM harvest_events WHERE workflow_exec_id = $1",
        "DELETE FROM harvest_timers WHERE workflow_exec_id = $1",
        "DELETE FROM harvest_signals WHERE workflow_exec_id = $1",
        "DELETE FROM harvest_payload_refs WHERE workflow_exec_id = $1",
        "DELETE FROM harvest_workflow_logs WHERE workflow_exec_id = $1",
        "DELETE FROM harvest_task_queue WHERE workflow_exec_id = $1",
    ];

    async fn discard_staged_copy(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        let existing: Option<TextRow> = diesel::sql_query(
            "SELECT state AS value FROM harvest_workflow_executions WHERE id = $1",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .optional_row()?;

        match existing.and_then(|r| r.value).as_deref() {
            None => return Ok(()),
            // Either a staged copy from this or a previous attempt, or a row
            // this execution left behind when it was migrated OFF this shard
            // earlier. Accepting the latter is what makes a drain reversible:
            // without it, `--from A --to B` over a large population could never
            // be undone by `--from B --to A`, because A still holds the sealed
            // row. The fresh copy carries the same history plus everything that
            // happened since, so replacing it loses nothing.
            Some("MIGRATING" | "MIGRATED") => {}
            Some(other) => {
                return Err(HarvestError::AlreadyExists {
                    existing_exec_id: exec_id,
                    existing_state: format!("already present on the target shard in state {other}"),
                });
            }
        }

        for sql in STAGED_CHILD_DELETES
            .iter()
            .copied()
            .chain(["DELETE FROM harvest_workflow_executions WHERE id = $1 \
               AND state IN ('MIGRATING', 'MIGRATED')"])
        {
            diesel::sql_query(sql)
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .execute(conn)
                .await
                .map_err(database_error)?;
        }
        Ok(())
    }

    /// The forwarding pointer a shard is currently holding for `exec_id`, if it
    /// holds a sealed row for it at all.
    ///
    /// Read **before** a staging discard on a reverse migration (A → B → A), so
    /// the seal can be carried onto the staged copy and restored if the
    /// migration aborts. Without it, staging onto A deletes A's `MIGRATED` row
    /// and replaces it with a `MIGRATING` row whose pointer is NULL: every id
    /// that routes to A stops at an inert copy instead of reaching live B, and
    /// an abort then deletes even that, leaving the execution with no row on its
    /// origin shard and no pointer anywhere — an id that resolves nowhere, the
    /// one outcome this design must never produce.
    async fn existing_seal(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<(i32, DateTime<Utc>)>> {
        let row: Option<SealRow> = diesel::sql_query(
            "SELECT migrated_to_shard AS forward, migrated_at FROM harvest_workflow_executions \
              WHERE id = $1 AND state = 'MIGRATED' AND migrated_to_shard IS NOT NULL",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .optional_row()?;
        Ok(row.and_then(|r| match (r.forward, r.migrated_at) {
            (Some(shard), Some(at)) => Some((shard, at)),
            _ => None,
        }))
    }

    #[derive(diesel::QueryableByName)]
    struct SealRow {
        #[diesel(sql_type = Nullable<Integer>)]
        forward: Option<i32>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        migrated_at: Option<DateTime<Utc>>,
    }

    // ── Phase 2: replay verification ─────────────────────────────────────────

    /// Replay-verify the staged copy against the source, **before** any cutover.
    ///
    /// Two independent checks, both of which must pass:
    ///
    /// 1. **Verbatim rows.** The raw stored `harvest_events` tuples
    ///    (`event_id`, `event_type`, `event_data`, `timestamp`) must be
    ///    byte-identical. This is what proves the append-only log was copied and
    ///    not re-derived, and it is only satisfiable if nothing was appended,
    ///    reordered or rewritten.
    /// 2. **Identical replay.** Both histories must decode under the configured
    ///    codecs and produce the same [`history_fingerprint`] — the same decoded
    ///    events *and* the same next-command state.
    ///
    /// Returns the agreed fingerprint. A failure aborts the migration with the
    /// source untouched.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on query failure, or
    /// [`HarvestError::NonDeterministic`] when the copy does not verify.
    pub async fn verify_target_copy(
        source: &mut AsyncPgConnection,
        target: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<String> {
        const RAW_SQL: &str = "\
            SELECT COALESCE(jsonb_agg(jsonb_build_array( \
                       ev.event_id, ev.event_type, ev.event_data, ev.timestamp) \
                   ORDER BY ev.event_id), '[]'::jsonb) AS payload \
            FROM harvest_events ev WHERE ev.workflow_exec_id = $1";

        let source_raw = read_json(source, RAW_SQL, exec_id).await?;
        let target_raw = read_json(target, RAW_SQL, exec_id).await?;
        if source_raw != target_raw {
            return Err(HarvestError::NonDeterministic {
                reason: format!(
                    "shard migration of {exec_id} did not copy history verbatim: the \
                     target's stored event rows differ from the source's"
                ),
                details: Box::new(crate::error::NonDeterministicDetails {
                    event_index: None,
                    expected: None,
                    actual: None,
                    workflow_type: None,
                    build_id: None,
                }),
            });
        }

        let source_history =
            crate::store::load_history_with_codecs(source, exec_id, codecs).await?;
        let target_history =
            crate::store::load_history_with_codecs(target, exec_id, codecs).await?;

        let source_fingerprint = history_fingerprint(&source_history.events);
        let target_fingerprint = history_fingerprint(&target_history.events);
        if source_fingerprint != target_fingerprint {
            return Err(HarvestError::NonDeterministic {
                reason: format!(
                    "shard migration of {exec_id} failed replay verification: the copied \
                     history replays to a different next-command state \
                     (source {source_fingerprint}, target {target_fingerprint})"
                ),
                details: Box::new(crate::error::NonDeterministicDetails {
                    event_index: None,
                    expected: Some(source_fingerprint),
                    actual: Some(target_fingerprint),
                    workflow_type: None,
                    build_id: None,
                }),
            });
        }

        // Stamp the high-water mark of THE HISTORY THIS CALL ACTUALLY VERIFIED,
        // derived from `source_raw` — the very rows the byte-identity check
        // above compared — rather than re-queried here.
        //
        // Re-querying would reopen the hole the mark exists to close, one step
        // earlier: the source can wake, run a decision cycle, append and re-park
        // between the fingerprint reads and this UPDATE, and a freshly-computed
        // mark would then record a history that was never copied. The cutover
        // would find both quiescence and the mark unchanged and seal, losing
        // that cycle silently — exactly the failure the guard was added for,
        // just with a shorter window.
        //
        // Binding what was read makes the mark and the verified copy the same
        // artifact by construction. If the source moved on in the meantime, the
        // mark simply no longer matches the live history and the cutover
        // declines, which is the fail-closed direction.
        let (verified_count, verified_max) = history_high_water_mark(&source_raw);
        diesel::sql_query(
            "UPDATE harvest_shard_migrations m \
                SET phase = 'VERIFIED', verified_fingerprint = $2, updated_at = NOW(), \
                    verified_event_count = $3, verified_max_event_id = $4 \
              WHERE m.execution_id = $1 AND m.phase = 'COPIED'",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(&source_fingerprint)
        .bind::<BigInt, _>(verified_count)
        .bind::<Integer, _>(verified_max)
        .execute(source)
        .await
        .map_err(database_error)?;

        Ok(source_fingerprint)
    }

    /// `(event count, highest event_id)` of the verified history, read out of
    /// the raw tuple array that verification itself compared.
    ///
    /// `RAW_SQL` returns `[[event_id, event_type, event_data, timestamp], ...]`
    /// ordered by `event_id`, so the count is the array length and the maximum
    /// is the first field of the last element. An empty history marks as
    /// `(0, -1)`, matching the `COALESCE(..., -1)` the cutover compares against
    /// so a run migrated before its first event still cuts over.
    fn history_high_water_mark(source_raw: &Value) -> (i64, i32) {
        let Some(rows) = source_raw.as_array() else {
            return (0, -1);
        };
        let count = i64::try_from(rows.len()).unwrap_or(i64::MAX);
        // `as_slice()` on both: the diesel prelude is in scope here and brings
        // `LimitDsl::first` with it, which shadows `Vec`'s inherent method and
        // sends inference off into query-builder traits.
        let max = rows
            .as_slice()
            .last()
            .and_then(|row| row.as_array())
            .and_then(|fields| fields.as_slice().first())
            .and_then(serde_json::Value::as_i64)
            .and_then(|v| i32::try_from(v).ok())
            .unwrap_or(-1);
        (count, max)
    }

    // ── Phase 3: the single atomic cutover commit ────────────────────────────

    #[derive(diesel::QueryableByName)]
    struct CutoverRow {
        #[diesel(sql_type = BigInt)]
        sealed_rows: i64,
    }

    /// Seal the source and hand authority to the target, in one commit on one
    /// database.
    ///
    /// This is the **only** step that changes who is authoritative, and it is a
    /// single statement on the source shard. Its `WHERE` clause re-evaluates the
    /// full quiescence predicate, so a wake that landed at any point since the
    /// candidate scan makes it match zero rows: the migration aborts and the
    /// source — untouched — processes the wake normally. That is what makes a
    /// mid-migration signal *aborting* rather than *lost*.
    ///
    /// In the same statement the source's parked workflow task row is
    /// `CANCELLED`. That is not belt-and-braces: the claim query filters on the
    /// *task's* state, not the execution's, so a sealed `MIGRATED` execution
    /// with a live parked row would still be claimable on the source.
    ///
    /// Returns `true` when the cutover committed, `false` when the source was no
    /// longer quiescent (in which case nothing was written).
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on failure.
    pub async fn commit_cutover(
        source: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        target_shard: ShardId,
    ) -> HarvestResult<bool> {
        // NOTE: no `--` comments inside this literal. Every line ends in `\`,
        // which strips the newline, so the whole statement is ONE line — a `--`
        // would comment out everything after it, including the final SELECT,
        // and Postgres would answer `syntax error at end of input`. Explanations
        // go here instead.
        //
        // `cancelled` and `advanced` are data-modifying CTEs, which Postgres
        // always executes to completion whether or not the primary query reads
        // them; only the seal's row count is needed to answer "did the cutover
        // happen?".
        let sql = format!(
            "WITH sealed AS ( \
                 UPDATE harvest_workflow_executions e \
                    SET state = 'MIGRATED', migrated_to_shard = $2, migrated_at = NOW(), \
                        completed_at = COALESCE(e.completed_at, NOW()) \
                  WHERE e.id = $1 AND {QUIESCENCE_SQL} AND {HISTORY_UNCHANGED_SQL} \
                 RETURNING e.id \
             ), cancelled AS ( \
                 UPDATE harvest_task_queue t \
                    SET state = 'CANCELLED', worker_id = NULL, completed_at = NOW(), \
                        error = 'execution migrated to shard ' || $2::text \
                  WHERE t.workflow_exec_id = $1 AND t.task_type = 'workflow' \
                    AND t.state IN ('PENDING', 'RUNNING') \
                    AND EXISTS (SELECT 1 FROM sealed) \
                 RETURNING t.id \
             ) \
             SELECT (SELECT count(*) FROM sealed)::BIGINT AS sealed_rows"
        );

        // The re-check and the seal run inside an EXPLICIT transaction that
        // takes the execution row `FOR UPDATE` first.
        //
        // Without that lock the re-check is a single autocommit statement under
        // READ COMMITTED, and its cross-table `EXISTS (... harvest_signals ...)`
        // subqueries are evaluated against the statement snapshot. Postgres
        // re-evaluates a qual only when the *target tuple* was updated, and
        // `signal::send_signal_idempotent` merely takes `SELECT ... FOR UPDATE`
        // on the execution row — a lock, not an update. So a signal committing
        // between our snapshot and our write would not be seen: we would seal a
        // run that had just been woken, and cancel the task row the wake had
        // just re-pended. Taking the same lock the signal path takes is what
        // makes "aborts cleanly rather than losing the wake" true.
        let row: CutoverRow = Box::pin(source.transaction::<CutoverRow, HarvestError, _>(
            async |conn| {
                diesel::sql_query(
                    "SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE",
                )
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .execute(&mut *conn)
                .await
                .map_err(database_error)?;

                let row: CutoverRow = diesel::sql_query(&sql)
                    .bind::<SqlUuid, _>(exec_id.as_uuid())
                    .bind::<Integer, _>(target_shard.as_i32())
                    .get_result(&mut *conn)
                    .await
                    .map_err(database_error)?;

                if row.sealed_rows > 0 {
                    // The seal happened, so the phase MUST advance with it —
                    // they are the same commit. A zero here means a concurrent
                    // abort claimed the record out from under us (it takes the
                    // same row lock above, so it cannot have interleaved *within*
                    // this transaction, only before it). Fail, which rolls the
                    // seal back rather than leaving a sealed row whose record
                    // still says it was never cut over.
                    let advanced = diesel::sql_query(
                        "UPDATE harvest_shard_migrations \
                            SET phase = 'COMMITTED', updated_at = NOW() \
                          WHERE execution_id = $1 AND phase = 'VERIFIED'",
                    )
                    .bind::<SqlUuid, _>(exec_id.as_uuid())
                    .execute(&mut *conn)
                    .await
                    .map_err(database_error)?;
                    if advanced == 0 {
                        return Err(HarvestError::Config(format!(
                            "cutover of {exec_id} sealed the source but its migration \
                             record was no longer VERIFIED; rolling back"
                        )));
                    }
                }
                Ok(row)
            },
        ))
        .await?;

        Ok(row.sealed_rows > 0)
    }

    // ── Phase 4: activate the target ─────────────────────────────────────────

    /// Make the target copy claimable. Idempotent, and safe to re-run after any
    /// crash: it is driven entirely off the durable `COMMITTED` record on the
    /// source, which by then is already sealed.
    ///
    /// Three things happen in one target-shard transaction:
    ///
    /// 1. `MIGRATING → RUNNING`.
    /// 2. The parked workflow task row captured at stage time is restored.
    /// 3. If any signal arrived between the cutover and now — forwarded to the
    ///    target through the sealed source — the restored task is re-pended
    ///    `PENDING` at `NOW()` so the wake is delivered rather than left waiting
    ///    for a timer that may be days out. This is what closes the
    ///    "never lost" half of the wake contract on the post-cutover side.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on failure.
    pub async fn activate_target(
        source: &mut AsyncPgConnection,
        target: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<()> {
        // `staged_task` and the shard this copy came FROM, in one read. The
        // source shard is taken from the migration record rather than from the
        // source row's own `shard_id`, so a resume long after the cutover still
        // appends the shard the migration actually moved the run off.
        let staged: ActivationRow = diesel::sql_query(
            "SELECT staged_task AS payload, source_shard FROM harvest_shard_migrations \
              WHERE execution_id = $1",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(source)
        .await
        .optional_row()?
        .ok_or_else(|| {
            HarvestError::Config(format!(
                "cannot activate {exec_id} on the target: no migration record exists \
                 on the source shard"
            ))
        })?;
        let staged_task: Option<Value> = staged.payload;
        let source_shard: i32 = staged.source_shard;

        Box::pin(target.transaction::<(), HarvestError, _>(async |conn| {
            let staged_task = staged_task.clone();
            {
                // The residence-history append rides on the SAME statement as
                // the state transition, and inherits its `state = 'MIGRATING'`
                // guard. That is what makes it exactly-once across the
                // idempotent re-runs this function is required to tolerate: a
                // second activation matches zero rows and appends nothing,
                // instead of growing the array on every resume sweep.
                //
                // The copy carried the source's own `migrated_from_shards`
                // verbatim (the copy is column-list-free), so appending the
                // source shard here accumulates the full history across any
                // number of hops without a backwards walk.
                //
                // `migrated_to_shard`/`migrated_at` are CLEARED here. They are
                // normally already NULL, but on a reverse migration the staged
                // copy deliberately carried the target shard's own old seal
                // forward so ids kept resolving during staging. Leaving it set
                // on a now-live row would make the run forward to the shard it
                // just came from — a cycle, and `read_forward` matches on the
                // pointer rather than the state, so the live row's state would
                // not save it.
                diesel::sql_query(
                    "UPDATE harvest_workflow_executions \
                        SET state = 'RUNNING', \
                            migrated_to_shard = NULL, \
                            migrated_at = NULL, \
                            migrated_from_shards = \
                                COALESCE(migrated_from_shards, '[]'::jsonb) \
                                || to_jsonb($2::int) \
                      WHERE id = $1 AND state = 'MIGRATING'",
                )
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .bind::<Integer, _>(source_shard)
                .execute(&mut *conn)
                .await
                .map_err(database_error)?;

                if let Some(task) = staged_task {
                    diesel::sql_query(
                        "INSERT INTO harvest_task_queue \
                         SELECT * FROM jsonb_populate_record( \
                             NULL::harvest_task_queue, $1::jsonb) \
                         ON CONFLICT (id) DO NOTHING",
                    )
                    .bind::<Jsonb, _>(&task)
                    .execute(&mut *conn)
                    .await
                    .map_err(database_error)?;
                }

                // A wake that arrived after the cutover is a staged signal row
                // with nothing scheduled to consume it. Re-pend now.
                diesel::sql_query(
                    "UPDATE harvest_task_queue t \
                        SET state = 'PENDING', scheduled_at = NOW(), \
                            wake_requested = FALSE \
                      WHERE t.workflow_exec_id = $1 AND t.task_type = 'workflow' \
                        AND t.state IN ('PENDING', 'RUNNING') \
                        AND t.worker_id IS NULL AND t.started_at IS NULL \
                        AND EXISTS (SELECT 1 FROM harvest_signals s \
                                    WHERE s.workflow_exec_id = $1 AND NOT s.consumed)",
                )
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .execute(&mut *conn)
                .await
                .map_err(database_error)?;
                Ok(())
            }
        }))
        .await?;

        // `staged_task` carries the parked task row, whose `input` is the run's
        // payload. Once the target holds it there is no reason to keep a third
        // copy on the source in a table neither `erase.rs` nor the retention
        // janitor knows about, so the settling UPDATE clears it.
        diesel::sql_query(
            "UPDATE harvest_shard_migrations \
                SET phase = 'DONE', staged_task = NULL, updated_at = NOW() \
              WHERE execution_id = $1 AND phase = 'COMMITTED'",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .execute(source)
        .await
        .map_err(database_error)?;

        Ok(())
    }

    /// Abandon a pre-cutover migration, leaving the source exactly as it was.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on failure.
    pub async fn abort_migration(
        source: &mut AsyncPgConnection,
        target: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        reason: &str,
    ) -> HarvestResult<()> {
        // Serialize against `commit_cutover` on the execution row it locks, so
        // the claim below and the seal there are mutually exclusive rather than
        // merely unlikely to interleave.
        diesel::sql_query("SELECT id FROM harvest_workflow_executions WHERE id = $1 FOR UPDATE")
            .bind::<SqlUuid, _>(exec_id.as_uuid())
            .execute(&mut *source)
            .await
            .map_err(database_error)?;

        // CLAIM the abort atomically, before touching the target.
        //
        // A plain read-then-act would still race: two operators can legitimately
        // drive the same migration (the runbook tells them to run
        // `rebalance-resume` after an interruption), and a concurrent
        // `commit_cutover` could seal the source in the window between the read
        // and the delete — leaving a `MIGRATED` source forwarding to a copy this
        // call has just destroyed. The execution would then exist nowhere.
        //
        // So the phase transition happens FIRST, as a conditional UPDATE that
        // only a pre-cutover row matches. Winning it is what licenses the
        // delete; `commit_cutover`'s own seal is conditional on the phase still
        // being `VERIFIED`, so the two are mutually exclusive by the same row.
        let claimed = diesel::sql_query(
            "UPDATE harvest_shard_migrations \
                SET phase = 'ABORTED', abort_reason = $2, staged_task = NULL, \
                    updated_at = NOW() \
              WHERE execution_id = $1 \
                AND phase IN ('PENDING', 'COPIED', 'VERIFIED')",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Text, _>(reason)
        .execute(source)
        .await
        .map_err(database_error)?;

        if claimed == 0 {
            // Either the migration is past its cutover — the source is sealed
            // and the target copy is the only live one — or there is no record
            // at all. Neither is ours to discard.
            let phase = load_migration(source, exec_id)
                .await?
                .map_or_else(|| "absent".to_string(), |r| r.phase.as_db().to_string());
            return Err(HarvestError::Config(format!(
                "refusing to abort the migration of {exec_id}: its record is in phase \
                 {phase}, not a pre-cutover phase this call may claim. Past the cutover \
                 the source is sealed and the target copy is the only live one — run the \
                 resume sweep to finish it instead."
            )));
        }

        // The seal-restoring variant, because a reverse migration's target is a
        // shard this run has lived on before: the row staging replaced there was
        // that shard's own forwarding seal, and deleting it outright would leave
        // every id routing to it resolving nowhere.
        Box::pin(target.transaction::<(), HarvestError, _>(async |conn| {
            discard_staged_copy_restoring_seal(&mut *conn, exec_id).await
        }))
        .await?;
        Ok(())
    }

    // ── Candidate discovery ──────────────────────────────────────────────────

    /// One execution considered for migration, with the verdict that decided it.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct ShardMigrationCandidate {
        /// The execution examined.
        pub execution_id: ExecutionId,
        /// Its registered workflow type.
        pub workflow_name: String,
        /// Its business key.
        pub workflow_id: String,
        /// Empty when eligible; every reason it may not move otherwise.
        pub blockers: Vec<QuiescenceBlocker>,
    }

    impl ShardMigrationCandidate {
        /// Is this candidate migratable?
        #[must_use]
        pub const fn is_eligible(&self) -> bool {
            self.blockers.is_empty()
        }
    }

    #[derive(diesel::QueryableByName)]
    struct CandidateRow {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
        #[diesel(sql_type = Text)]
        workflow_name: String,
        #[diesel(sql_type = Text)]
        workflow_id: String,
    }

    /// Find quiescent executions on a shard, oldest first.
    ///
    /// The cheap `RUNNING` / root / not-blocked filter runs in SQL; the verdict
    /// itself comes from [`assess_quiescence`] over a per-execution observation.
    /// That is one query per candidate rather than one query total, and it is
    /// the deliberate trade: an operator batch tool can afford it, and it makes
    /// it structurally impossible for the SQL scan and the pure predicate to
    /// disagree about what "quiescent" means.
    ///
    /// `scan_limit` bounds how many executions are *examined*; the caller
    /// decides how many of the eligible ones to actually move.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on query failure.
    pub async fn list_migration_candidates(
        conn: &mut AsyncPgConnection,
        scan_limit: i64,
    ) -> HarvestResult<Vec<ShardMigrationCandidate>> {
        let rows: Vec<CandidateRow> = diesel::sql_query(
            "SELECT e.id, e.workflow_name, e.workflow_id \
             FROM harvest_workflow_executions e \
             WHERE e.state = 'RUNNING' AND e.parent_id IS NULL \
               AND NOT EXISTS (SELECT 1 FROM harvest_shard_migrations m \
                               WHERE m.execution_id = e.id \
                                 AND m.phase NOT IN ('DONE', 'ABORTED')) \
             ORDER BY e.created_at ASC \
             LIMIT $1",
        )
        .bind::<BigInt, _>(scan_limit)
        .load(conn)
        .await
        .map_err(database_error)?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let exec_id = ExecutionId::from_uuid(row.id);
            let observation = observe_quiescence(conn, exec_id).await?;
            out.push(ShardMigrationCandidate {
                execution_id: exec_id,
                workflow_name: row.workflow_name,
                workflow_id: row.workflow_id,
                blockers: assess_quiescence(&observation).blockers().to_vec(),
            });
        }
        Ok(out)
    }

    // ── Drivers ──────────────────────────────────────────────────────────────

    /// What one execution's migration attempt did.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    #[serde(tag = "outcome", rename_all = "snake_case")]
    pub enum MigrationOutcome {
        /// The run moved and is claimable on the target.
        Migrated {
            /// The execution that moved.
            execution_id: ExecutionId,
            /// The fingerprint both copies agreed on.
            fingerprint: String,
        },
        /// The run was not quiescent, so nothing was attempted.
        Skipped {
            /// The execution that was examined.
            execution_id: ExecutionId,
            /// Why it may not move.
            blockers: Vec<QuiescenceBlocker>,
        },
        /// A pre-cutover step failed; the source was left untouched.
        Aborted {
            /// The execution that was examined.
            execution_id: ExecutionId,
            /// What went wrong, in operator-readable words.
            reason: String,
        },
        /// A dry run: this execution *would* have been migrated.
        WouldMigrate {
            /// The execution that would move.
            execution_id: ExecutionId,
        },
    }

    impl MigrationOutcome {
        /// The execution this outcome is about.
        #[must_use]
        pub const fn execution_id(&self) -> ExecutionId {
            match self {
                Self::Migrated { execution_id, .. }
                | Self::Skipped { execution_id, .. }
                | Self::Aborted { execution_id, .. }
                | Self::WouldMigrate { execution_id } => *execution_id,
            }
        }
    }

    /// The result of one `migrate up to N quiescent executions from A to B` run.
    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct MigrationBatchReport {
        /// The shard executions were read from.
        pub source_shard: ShardId,
        /// The shard they were written to.
        pub target_shard: ShardId,
        /// Whether this run wrote anything at all.
        pub dry_run: bool,
        /// How many executions were examined.
        pub examined: usize,
        /// Per-execution outcomes, in the order they were attempted.
        pub outcomes: Vec<MigrationOutcome>,
    }

    impl MigrationBatchReport {
        /// How many executions actually moved (always `0` for a dry run).
        #[must_use]
        pub fn migrated(&self) -> usize {
            self.outcomes
                .iter()
                .filter(|o| matches!(o, MigrationOutcome::Migrated { .. }))
                .count()
        }

        /// How many *would* move, on a dry run.
        #[must_use]
        pub fn would_migrate(&self) -> usize {
            self.outcomes
                .iter()
                .filter(|o| matches!(o, MigrationOutcome::WouldMigrate { .. }))
                .count()
        }

        /// How many were skipped as not quiescent.
        #[must_use]
        pub fn skipped(&self) -> usize {
            self.outcomes
                .iter()
                .filter(|o| matches!(o, MigrationOutcome::Skipped { .. }))
                .count()
        }

        /// How many attempts aborted before cutover.
        #[must_use]
        pub fn aborted(&self) -> usize {
            self.outcomes
                .iter()
                .filter(|o| matches!(o, MigrationOutcome::Aborted { .. }))
                .count()
        }
    }

    /// Migrate one execution end to end: stage → verify → cutover → activate.
    ///
    /// Every pre-cutover failure aborts with the source untouched. Past the
    /// cutover there is no rollback — the source is sealed — so a failure there
    /// leaves the durable `COMMITTED` record for
    /// [`resume_incomplete_migrations`] and is surfaced as an error rather than
    /// swallowed.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on failure past the cutover, or
    /// [`HarvestError::ShardUnavailable`] when a shard has no pool here.
    pub async fn migrate_execution(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
        source_shard: ShardId,
        target_shard: ShardId,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<MigrationOutcome> {
        if source_shard == target_shard {
            return Ok(MigrationOutcome::Aborted {
                execution_id: exec_id,
                reason: "source and target shard are the same".to_string(),
            });
        }

        let mut source = checkout(pool, source_shard).await?;
        let mut target = checkout(pool, target_shard).await?;

        let observation = observe_quiescence(&mut source, exec_id).await?;
        if let Quiescence::Blocked(blockers) = assess_quiescence(&observation) {
            return Ok(MigrationOutcome::Skipped {
                execution_id: exec_id,
                blockers,
            });
        }

        // A migration another operator already has in flight is a SKIP, not a
        // batch-ending error: one contended execution must not abandon the
        // hundred behind it.
        if let Err(error) = begin_migration(&mut source, exec_id, source_shard, target_shard).await
        {
            if matches!(error, HarvestError::AlreadyExists { .. }) {
                return Ok(MigrationOutcome::Aborted {
                    execution_id: exec_id,
                    reason: "a migration for this execution is already in flight".to_string(),
                });
            }
            return Err(error);
        }

        // ── Everything below the cutover is abortable with the source intact ──
        let staged = stage_copy(&mut source, &mut target, exec_id, target_shard).await;
        if let Err(error) = staged {
            let reason = error.to_string();
            record_attempt(&mut source, exec_id, &reason).await?;
            abort_migration(&mut source, &mut target, exec_id, &reason).await?;
            return Ok(MigrationOutcome::Aborted {
                execution_id: exec_id,
                reason,
            });
        }

        let fingerprint = match verify_target_copy(&mut source, &mut target, exec_id, codecs).await
        {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                let reason = error.to_string();
                record_attempt(&mut source, exec_id, &reason).await?;
                abort_migration(&mut source, &mut target, exec_id, &reason).await?;
                return Ok(MigrationOutcome::Aborted {
                    execution_id: exec_id,
                    reason,
                });
            }
        };

        // ── The point of no return ───────────────────────────────────────────
        //
        // A cutover that *fails* is as abortable as one that declines: nothing
        // was sealed either way. Propagating the error instead would leave the
        // staged `MIGRATING` copy holding the target's `(workflow_name,
        // workflow_id)` uniqueness slot, blocking every new run of that
        // business key on the target, with no abort reason recorded anywhere.
        let cutover = commit_cutover(&mut source, exec_id, target_shard).await;
        let reason = match cutover {
            Ok(true) => None,
            Ok(false) => Some(
                "the execution was no longer quiescent at cutover time \
                 (a wake arrived mid-migration); the source is untouched"
                    .to_string(),
            ),
            Err(error) => Some(format!("the cutover failed: {error}")),
        };
        if let Some(reason) = reason {
            record_attempt(&mut source, exec_id, &reason).await?;
            abort_migration(&mut source, &mut target, exec_id, &reason).await?;
            return Ok(MigrationOutcome::Aborted {
                execution_id: exec_id,
                reason,
            });
        }

        activate_target(&mut source, &mut target, exec_id).await?;
        collapse_forward_chain(pool, exec_id, source_shard, target_shard).await;

        Ok(MigrationOutcome::Migrated {
            execution_id: exec_id,
            fingerprint,
        })
    }

    /// Batch: migrate up to `limit` quiescent executions from `source_shard` to
    /// `target_shard`.
    ///
    /// A dry run walks **the same code path** up to (and excluding) the first
    /// write, reporting `WouldMigrate` for exactly the population a real run
    /// would attempt. It is deliberately not a separate estimator: an operator
    /// running this during an incident must be able to trust that the dry run
    /// and the real run agree.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when either shard has no pool on this
    /// node, [`HarvestError::Database`] on query failure.
    pub async fn migrate_quiescent_executions(
        pool: &ShardedDbPool,
        source_shard: ShardId,
        target_shard: ShardId,
        limit: usize,
        dry_run: bool,
        actor: &str,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<MigrationBatchReport> {
        if source_shard == target_shard {
            return Err(HarvestError::Config(
                "source and target shard must differ".to_string(),
            ));
        }
        // Fail fast on an unreachable target: staging every candidate and then
        // discovering the target is gone would leave a trail of aborted rows.
        // The connection itself is not wanted, only the proof it can be had.
        drop(checkout(pool, target_shard).await?);

        let candidates = {
            let mut source = checkout(pool, source_shard).await?;
            // Scan wide enough that a shard whose head is busy still yields a
            // full batch, but bounded so one call cannot walk the whole shard.
            let scan_limit =
                i64::try_from(limit.saturating_mul(4).clamp(1, 10_000)).unwrap_or(10_000);
            list_migration_candidates(&mut source, scan_limit).await?
        };

        let examined = candidates.len();
        let mut outcomes = Vec::new();
        let mut moved = 0usize;

        for candidate in candidates {
            if moved >= limit {
                break;
            }
            if !candidate.is_eligible() {
                let outcome = MigrationOutcome::Skipped {
                    execution_id: candidate.execution_id,
                    blockers: candidate.blockers,
                };
                if !dry_run {
                    let mut source = checkout(pool, source_shard).await?;
                    record_migration_audit(
                        &mut source,
                        actor,
                        source_shard,
                        target_shard,
                        &outcome,
                    )
                    .await?;
                }
                outcomes.push(outcome);
                continue;
            }
            if dry_run {
                outcomes.push(MigrationOutcome::WouldMigrate {
                    execution_id: candidate.execution_id,
                });
                moved += 1;
                continue;
            }
            let outcome = migrate_execution(
                pool,
                candidate.execution_id,
                source_shard,
                target_shard,
                codecs,
            )
            .await?;
            if matches!(outcome, MigrationOutcome::Migrated { .. }) {
                moved += 1;
            }
            // Audited HERE, per outcome, not after the loop: a later error
            // would otherwise discard the audit record of a migration that has
            // already sealed its source — the one record an operator most needs.
            {
                let mut source = checkout(pool, source_shard).await?;
                record_migration_audit(&mut source, actor, source_shard, target_shard, &outcome)
                    .await?;
            }
            outcomes.push(outcome);
        }

        Ok(MigrationBatchReport {
            source_shard,
            target_shard,
            dry_run,
            examined,
            outcomes,
        })
    }

    /// Write one `harvest_audit_log` row for a migration attempt.
    ///
    /// On the **source** shard: the shard whose residents are moving, and the
    /// one an operator reads `GET /audit` against when asking "who moved this
    /// run?". Note the honest limit — a shard being decommissioned is
    /// eventually retired, taking this trail with it, so ship audit off-box
    /// (issue #953's audit export) before step 5 of the decommission runbook.
    async fn record_migration_audit(
        conn: &mut AsyncPgConnection,
        actor: &str,
        source_shard: ShardId,
        target_shard: ShardId,
        outcome: &MigrationOutcome,
    ) -> HarvestResult<()> {
        // `harvest_audit_log.status` is CHECK-constrained to exactly
        // ('succeeded', 'failed') -- the audit trail records whether the
        // OPERATION succeeded, not what it decided. A skip is a successful
        // evaluation that declined to move the run, so it is `succeeded` with
        // the blockers in `error_summary`; only an abort is a `failed` attempt.
        let (status, error_summary) = match outcome {
            MigrationOutcome::Migrated { .. } | MigrationOutcome::WouldMigrate { .. } => {
                ("succeeded", None)
            }
            MigrationOutcome::Skipped { blockers, .. } => (
                "succeeded",
                Some(
                    blockers
                        .iter()
                        .map(|b| b.describe())
                        .collect::<Vec<_>>()
                        .join("; "),
                ),
            ),
            MigrationOutcome::Aborted { reason, .. } => ("failed", Some(reason.clone())),
        };
        let exec_id = outcome.execution_id().to_string();
        let route = format!(
            "shard rebalance {} -> {}",
            source_shard.as_i32(),
            target_shard.as_i32()
        );
        let record = crate::models::NewAuditRecord {
            actor,
            operation: "shard.rebalance.migrate",
            target_type: "workflow_execution",
            target_id: Some(&exec_id),
            route_or_command: &route,
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: Some(source_shard.as_i32()),
            source: "cli",
        };
        crate::audit::insert_audit(conn, &record).await?;
        Ok(())
    }

    /// Drive every unsettled migration on one shard to a **settled** phase.
    ///
    /// This is the crash-recovery half of the design: a process that dies at any
    /// point leaves a durable row whose phase says what is outstanding, and
    /// [`next_migration_action`] turns that into the one correct next step.
    ///
    /// Each row is stepped repeatedly until it reaches `Retire` (i.e. `DONE` or
    /// `ABORTED`), so **one call finishes the job** rather than leaving a
    /// half-migrated run parked between phases until an operator happens to run
    /// the sweep again. That matters most at exactly the phase an operator is
    /// least likely to notice: a crash between the cutover commit and the
    /// target's activation leaves the run claimable on *neither* shard, and a
    /// one-step-per-call sweep would leave it that way. The step count is
    /// bounded by [`MAX_RESUME_STEPS`] so a row that cannot make progress is
    /// abandoned to the next call instead of spinning.
    ///
    /// Past the cutover the only action is `ActivateTarget`, which is
    /// idempotent, so re-running the whole sweep converges rather than
    /// oscillates.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on query failure. An individual row that
    /// cannot be advanced records its error and does not fail the sweep.
    #[allow(clippy::too_many_lines)] // The phase machine's dispatch is one
    // exhaustive `match`; extracting arms would hide which step each phase takes.
    pub async fn resume_incomplete_migrations(
        pool: &ShardedDbPool,
        source_shard: ShardId,
        limit: i64,
        actor: &str,
        codecs: &PayloadCodecs,
    ) -> HarvestResult<Vec<MigrationOutcome>> {
        let unsettled: Vec<MigrationRecord> = {
            let mut source = checkout(pool, source_shard).await?;
            let rows: Vec<MigrationRow> = diesel::sql_query(format!(
                "SELECT {MIGRATION_COLUMNS} FROM harvest_shard_migrations \
                  WHERE phase NOT IN ('DONE', 'ABORTED') \
                  ORDER BY created_at ASC LIMIT $1"
            ))
            .bind::<BigInt, _>(limit)
            .load(&mut *source)
            .await
            .map_err(database_error)?;
            rows.into_iter()
                .map(MigrationRow::into_record)
                .collect::<HarvestResult<Vec<_>>>()?
        };

        let mut outcomes = Vec::new();
        for record in unsettled {
            let exec_id = record.execution_id;
            let mut source = checkout(pool, record.source_shard).await?;
            let mut target = checkout(pool, record.target_shard).await?;
            let mut phase = record.phase;

            for _ in 0..MAX_RESUME_STEPS {
                // Propagated, never swallowed: `unwrap_or(false)` here would turn a
                // pool blip into "the source is no longer quiescent", and the phase
                // machine would abort a perfectly good migration and record a reason
                // naming a wake that never happened.
                let source_still_quiescent = if phase.is_past_cutover() {
                    false
                } else {
                    assess_quiescence(&observe_quiescence(&mut source, exec_id).await?)
                        .is_eligible()
                };

                let action = next_migration_action(&MigrationObservation {
                    phase,
                    source_still_quiescent,
                });
                if action == MigrationAction::Retire {
                    break;
                }

                let stepped: HarvestResult<Option<MigrationOutcome>> = async {
                    match action {
                        MigrationAction::StageCopy => {
                            stage_copy(&mut source, &mut target, exec_id, record.target_shard)
                                .await?;
                            Ok(None)
                        }
                        MigrationAction::Verify => {
                            verify_target_copy(&mut source, &mut target, exec_id, codecs).await?;
                            Ok(None)
                        }
                        MigrationAction::Cutover => {
                            if commit_cutover(&mut source, exec_id, record.target_shard).await? {
                                Ok(None)
                            } else {
                                // A declined cutover has to CLEAN UP, not merely
                                // report. Without this the record stays
                                // `VERIFIED`, the target keeps its `MIGRATING`
                                // copy and that shard's uniqueness slot, and the
                                // loop exits because the phase did not advance —
                                // so the command says "aborted" while nothing was
                                // undone, and a second resume is needed to
                                // actually finish the job.
                                let reason = "the execution woke up before the \
                                              cutover; the source is untouched"
                                    .to_string();
                                abort_migration(&mut source, &mut target, exec_id, &reason).await?;
                                Ok(Some(MigrationOutcome::Aborted {
                                    execution_id: exec_id,
                                    reason,
                                }))
                            }
                        }
                        MigrationAction::ActivateTarget => {
                            activate_target(&mut source, &mut target, exec_id).await?;
                            collapse_forward_chain(
                                pool,
                                exec_id,
                                record.source_shard,
                                record.target_shard,
                            )
                            .await;
                            Ok(Some(MigrationOutcome::Migrated {
                                execution_id: exec_id,
                                fingerprint: record
                                    .verified_fingerprint
                                    .clone()
                                    .unwrap_or_default(),
                            }))
                        }
                        MigrationAction::Abort => {
                            abort_migration(
                                &mut source,
                                &mut target,
                                exec_id,
                                "the execution woke up before cutover",
                            )
                            .await?;
                            Ok(Some(MigrationOutcome::Aborted {
                                execution_id: exec_id,
                                reason: "the execution woke up before cutover".to_string(),
                            }))
                        }
                        MigrationAction::Retire => Ok(None),
                    }
                }
                .await;

                match stepped {
                    Ok(Some(outcome)) => {
                        // The resume path performs real cutovers and activations,
                        // so it is audited exactly like the batch path; without this
                        // a migration completed by `rebalance-resume` would leave no
                        // audit record at all.
                        record_migration_audit(
                            &mut source,
                            actor,
                            record.source_shard,
                            record.target_shard,
                            &outcome,
                        )
                        .await?;
                        outcomes.push(outcome);
                    }
                    Ok(None) => {}
                    Err(error) => {
                        let reason = error.to_string();
                        record_attempt(&mut source, exec_id, &reason).await?;
                        outcomes.push(MigrationOutcome::Aborted {
                            execution_id: exec_id,
                            reason,
                        });
                        // A failed step leaves the phase where it was; stop rather
                        // than re-attempting it in a tight loop. The next sweep
                        // picks it up, under the recorded backoff.
                        break;
                    }
                }

                // Re-read rather than infer: `commit_cutover` can decline (the run
                // woke) and `abort_migration` can be a no-op past the cutover, so
                // the stored phase is the only honest source of what happened.
                let Some(next) = load_migration(&mut source, exec_id).await? else {
                    break;
                };
                if next.phase == phase {
                    // No forward progress — a declined cutover, or a step that
                    // could not advance. Leave it for the next sweep.
                    break;
                }
                phase = next.phase;
            }
        }
        Ok(outcomes)
    }

    // ── Forwarding ───────────────────────────────────────────────────────────

    /// Resolve where an execution physically lives, following the forwarding
    /// chain a migration leaves behind.
    ///
    /// This is the runtime half of the identity decision: the `ExecutionId` is
    /// never re-minted, so its encoded shard remains the **entry point**, and
    /// this walk turns that entry point into the current residence. For the
    /// overwhelmingly common case — an execution that never moved — it is one
    /// indexed lookup on the shard the id already pointed at.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when a shard on the chain has no pool
    /// here, or when the chain exceeds [`MAX_FORWARD_HOPS`].
    pub async fn resolve_execution_shard(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
    ) -> HarvestResult<ShardId> {
        let origin = pool.routed_shard_for_execution(exec_id);
        // Did routing apply an operator-declared retired-shard forward? If so
        // `origin` is a SUCCESSOR, which names one specific database exactly as
        // a `migrated_to_shard` pointer does: the operator has asserted A is
        // gone and its ids now live on B, so answering from the default shard
        // instead of B reads the wrong database and returns a confident 404.
        // Tolerate a missing pool only for an id resolving to its own encoded
        // shard -- the single-pool case `checkout_entry` exists for.
        let forwarded = !exec_id.shard().is_unencoded() && origin != exec_id.shard();
        let mut current = origin;
        for hop in 0..MAX_FORWARD_HOPS {
            // The ORIGIN hop is tolerant (see `checkout_entry`) unless routing
            // already forwarded it; every hop after it follows a stored pointer
            // that names one specific database and must resolve there or fail.
            let mut conn = if hop == 0 && !forwarded {
                checkout_entry(pool, current).await?
            } else {
                checkout(pool, current).await?
            };
            match read_forward(&mut conn, exec_id).await? {
                None => return Ok(current),
                Some(next) => current = next,
            }
        }
        // Mirrors `resolve_forward_chain`'s bound and error exactly; the pure
        // function is what the unit tests pin, this is its async twin.
        resolve_forward_chain(origin, |_| Some(origin))
    }

    /// Every shard this execution has ever physically occupied, oldest first,
    /// ending with the shard that hosts it **now**.
    ///
    /// [`resolve_execution_shard`] answers "where do I write?" and is what the
    /// runtime paths want. This answers "where does a copy of this run's data
    /// still sit?", which is a different and strictly larger question: a
    /// rebalance leaves the source copy in place, sealed, until retention
    /// collects it. Anything that must reach **all** of an execution's bytes
    /// rather than just its live ones — GDPR payload erasure above all — has to
    /// visit this whole list, not only its last element.
    ///
    /// **It is read from `migrated_from_shards`, not walked backwards along the
    /// forwarding pointers, and that distinction is load-bearing.** The pointers
    /// are deliberately *collapsed*: a completed migration best-effort rewrites
    /// the origin's pointer to skip straight to the newest residence, so hops do
    /// not accumulate past [`MAX_FORWARD_HOPS`]. After A → B → C that leaves A
    /// pointing at C and no trace of B anywhere in the pointer graph — while B's
    /// sealed copy still holds the run's full payloads. A chain derived from the
    /// pointers would therefore report `[A, C]` and an erasure built on it would
    /// claim success having never touched B. The durable array is appended to at
    /// every activation and is never collapsed, so it is exact regardless of
    /// what the routing layer does to the pointers.
    ///
    /// The chain is `[origin]` for every execution that has never moved, which
    /// is every execution on a single-shard deployment and all but a handful
    /// anywhere else.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when the live shard has no pool here
    /// or the forwarding walk exceeds [`MAX_FORWARD_HOPS`];
    /// [`HarvestError::Database`] when the stored residence array cannot be
    /// decoded — fail closed, because a caller that must reach every copy must
    /// not be handed a silently-shortened list.
    pub async fn residence_chain(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
    ) -> HarvestResult<Vec<ShardId>> {
        let live = resolve_execution_shard(pool, exec_id).await?;
        let mut conn = checkout_entry(pool, live).await?;
        // The execution row first, and the SUMMARY as the fallback. After a
        // migrated run terminates, the live shard's retention janitor deletes
        // the execution row and keeps only a compact
        // `harvest_execution_summaries` row — while the sealed source copies
        // survive, because retention deliberately never purges a `MIGRATED` row
        // (that would destroy the forwarding pointer). Reading "never migrated"
        // from the execution row's absence would therefore report a clean
        // erasure over exactly the copies that still hold the data, so the
        // array is carried onto the summary at demotion time and read back here.
        let row: Option<PriorShardsRow> = diesel::sql_query(
            "SELECT COALESCE(e.migrated_from_shards, s.migrated_from_shards) AS shards \
               FROM (SELECT $1::uuid AS id) k \
               LEFT JOIN harvest_workflow_executions e ON e.id = k.id \
               LEFT JOIN harvest_execution_summaries s ON s.execution_id = k.id \
              WHERE e.id IS NOT NULL OR s.execution_id IS NOT NULL",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(&mut *conn)
        .await
        .optional_row()?;

        let mut chain: Vec<ShardId> = match row.and_then(|r| r.shards) {
            None => Vec::new(),
            Some(value) => serde_json::from_value::<Vec<i32>>(value)
                .map_err(|e| {
                    HarvestError::Database(format!(
                        "execution {exec_id} has an undecodable migrated_from_shards \
                         residence history: {e}"
                    ))
                })?
                .into_iter()
                .map(ShardId::new)
                .collect(),
        };
        // De-duplicate PRESERVING THE LIVE SHARD AS THE LAST ELEMENT.
        //
        // A reverse migration (A → B → A, which the design supports so a drain
        // can be undone) stores `[A, B]` and is live on A, giving `[A, B, A]`.
        // A first-occurrence dedup would collapse that to `[A, B]` and leave B
        // in the final position — and every consumer reads the last element as
        // the live residence. `erase_workflow_payloads_all_residences` would
        // then apply the terminal-state and legal-hold gates to the SEALED copy
        // and report its outcome as the answer. So the live shard's earlier
        // occurrence is dropped, not its final one.
        chain.retain(|shard| *shard != live);
        let mut seen = std::collections::HashSet::new();
        chain.retain(|shard| seen.insert(*shard));
        chain.push(live);
        Ok(chain)
    }

    #[derive(diesel::QueryableByName)]
    struct ActivationRow {
        #[diesel(sql_type = Nullable<Jsonb>)]
        payload: Option<Value>,
        #[diesel(sql_type = Integer)]
        source_shard: i32,
    }

    #[derive(diesel::QueryableByName)]
    struct PriorShardsRow {
        #[diesel(sql_type = Nullable<Jsonb>)]
        shards: Option<Value>,
    }

    /// The shard an execution **currently** lives on, read from a connection
    /// that already holds it — no pool checkout of any kind.
    ///
    /// This is the caller-side companion to [`resolve_execution_shard`], and it
    /// exists because that function cannot be used here. The engine's outbox
    /// sweeps run inside a transaction on the caller's own shard and then decide
    /// whether the delivery target resolves to that *same pool*; getting that
    /// comparison wrong sends them down the cross-pool branch, which checks out
    /// a second connection from the pool already driving the transaction and
    /// self-deadlocks a pool of the supported minimum size one. `ExecutionId`
    /// encodes where a run *originated*, so a caller that has itself been
    /// rebalanced compares its origin against the target's residence and the
    /// two disagree even when both are the same database.
    ///
    /// Resolving the caller's residence the usual way would need a checkout of
    /// its own — the same deadlock, one level down. Its `shard_id` column
    /// follows the run across a migration (the copy carries every column and the
    /// cutover updates it), so the connection in hand already knows the answer.
    ///
    /// Returns `None` when the row is not on this connection at all, leaving the
    /// caller to fall back to the id's encoded shard, which is the pre-#964
    /// behaviour.
    pub async fn shard_of_held_row(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> Option<ShardId> {
        let row: Option<ShardIdRow> =
            diesel::sql_query("SELECT shard_id FROM harvest_workflow_executions WHERE id = $1")
                .bind::<SqlUuid, _>(exec_id.as_uuid())
                .get_result(conn)
                .await
                .optional_row()
                .ok()
                .flatten();
        row.map(|r| ShardId::new(r.shard_id))
    }

    #[derive(diesel::QueryableByName)]
    struct ShardIdRow {
        #[diesel(sql_type = Integer)]
        shard_id: i32,
    }

    /// Where an execution's row on the **held** database forwards to, or
    /// `None` when it has not moved.
    ///
    /// The connection-based twin of [`resolve_execution_shard`]'s first hop,
    /// and the reason it exists is a hard constraint rather than a convenience:
    /// a caller inside a transaction is holding a connection from one of
    /// `pool`'s databases, and taking a SECOND connection from that same pool
    /// blocks forever under the documented pool-size-1 configuration (issue
    /// #751). Any residence lookup performed while such a connection is held
    /// must therefore read on it, not check out beside it.
    ///
    /// One read is the whole answer: `collapse_forward_chain` rewrites an
    /// origin's pointer straight to the final target, so a pointer read here
    /// does not need the hop loop that [`resolve_execution_shard`] runs for the
    /// window in which a chain has not yet been collapsed.
    pub async fn forward_of_held_row(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> Option<ShardId> {
        read_forward(conn, exec_id).await.ok().flatten()
    }

    /// [`resolve_target_shard`], for a caller already holding a connection from
    /// the pool that serves `held_shard`.
    ///
    /// Identical in result. The difference is where the lookups run: every one
    /// that would land on the held database is issued on `conn`, because
    /// checking out a second connection from the pool that lent it
    /// self-deadlocks a size-1 pool (issue #751). Only a genuinely different
    /// database is reached through `pool`, where a checkout cannot contend with
    /// the connection the caller is holding.
    pub async fn resolve_target_shard_holding(
        conn: &mut AsyncPgConnection,
        pool: &ShardedDbPool,
        target: &crate::types::ExternalTarget,
        held_shard: ShardId,
    ) -> ShardId {
        let unforwarded = match target {
            crate::types::ExternalTarget::ExecutionId(id) => pool.routed_shard_for_execution(*id),
            crate::types::ExternalTarget::WorkflowId { .. } => {
                crate::shard::external_target_owning_shard(target).unwrap_or(held_shard)
            }
        };
        if pool.len() <= 1 {
            return unforwarded;
        }
        // `same_underlying_pool`, NOT `std::ptr::eq` on the two `&DbPool`
        // (issue #1146): `ShardedDbPool` stores every shard's pool in its own
        // map slot, so pointer equality of the slots is really *shard-id*
        // equality and calls two aliases of one database different. Here that
        // would be worse than cosmetic — an alias judged "not held" sends this
        // lookup to check out a second connection from the very pool the caller
        // is holding one from, which is the deadlock this function exists to
        // avoid.
        let on_held = |shard: ShardId| {
            crate::external_target_location::same_underlying_pool(
                pool.pool_for(shard),
                pool.pool_for(held_shard),
            )
        };

        let exec_id = match target {
            crate::types::ExternalTarget::ExecutionId(id) => *id,
            crate::types::ExternalTarget::WorkflowId {
                workflow_name,
                workflow_id,
            } => {
                let found = if on_held(unforwarded) {
                    business_key_on(conn, workflow_name, workflow_id).await
                } else {
                    resolve_business_key(pool, unforwarded, workflow_name, workflow_id).await
                };
                match found {
                    Some(id) => id,
                    None => return unforwarded,
                }
            }
        };

        if on_held(pool.routed_shard_for_execution(exec_id)) {
            forward_of_held_row(conn, exec_id)
                .await
                .unwrap_or(unforwarded)
        } else {
            resolve_execution_shard(pool, exec_id)
                .await
                .unwrap_or(unforwarded)
        }
    }

    /// Check out a connection to one specific shard, failing closed when this
    /// node has no pool for it.
    ///
    /// Exposed for the cross-residence sweeps that walk a [`residence_chain`]:
    /// they already hold the shard ids and must not re-derive them through any
    /// path that can fall back to the default shard.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when no pool is configured for
    /// `shard` on this node, or when the checkout itself fails.
    pub async fn conn_for_shard(
        pool: &ShardedDbPool,
        shard: ShardId,
    ) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        checkout(pool, shard).await
    }

    /// Check out a connection for an execution's **live** residence.
    ///
    /// Tolerates a deployment that registers no pool under that shard id. See
    /// [`checkout_entry`] for why the live residence resolves this way and a
    /// prior residence deliberately does not.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when the checkout itself fails. Unlike
    /// [`conn_for_shard`], an unregistered shard is not an error: it falls back
    /// to the pool's default.
    pub async fn conn_for_live_shard(
        pool: &ShardedDbPool,
        shard: ShardId,
    ) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        checkout_entry(pool, shard).await
    }

    /// The forwarding probe: "has this execution been rebalanced off this
    /// shard, and if so, to where?"
    ///
    /// One primary-key lookup against a partial index that holds only migrated
    /// rows — zero rows until an operator rebalances.
    async fn read_forward(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<Option<ShardId>> {
        // Matched on the POINTER, not on `state = 'MIGRATED'`. A sealed row can
        // legitimately be force-written to another state afterwards --
        // `terminate_workflow_execution` carries no state precondition by
        // design -- and an id must keep resolving across that, or an operator
        // override would silently orphan every reference to the run.
        let row: Option<ForwardRow> = diesel::sql_query(
            "SELECT migrated_to_shard AS forward FROM harvest_workflow_executions \
              WHERE id = $1 AND migrated_to_shard IS NOT NULL",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .get_result(conn)
        .await
        .optional_row()?;
        Ok(row.and_then(|r| r.forward).map(ShardId::new))
    }

    /// Check out a connection to the shard that **currently hosts** `exec_id`,
    /// following the forwarding pointer a rebalance leaves behind.
    ///
    /// This is the id-routed read/write path's half of the identity contract:
    /// because the `ExecutionId` is never re-minted, the first checkout goes
    /// exactly where it always did, and only a run that has actually moved pays
    /// for a second hop.
    ///
    /// **A single-shard deployment pays nothing at all.** With one pool there is
    /// nowhere to migrate to, so the probe is skipped entirely and the returned
    /// connection is byte-for-byte the one `pool_for_execution` would have
    /// handed back before issue #964. A multi-shard deployment pays one
    /// primary-key lookup against a partial index that is empty until an
    /// operator rebalances.
    ///
    /// # Errors
    ///
    /// [`HarvestError::ShardUnavailable`] when a shard on the chain cannot be
    /// reached from this node, or when the chain exceeds [`MAX_FORWARD_HOPS`].
    pub async fn conn_for_execution_forwarded(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
    ) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        // The first hop keeps `pool_for_execution`'s default-shard fallback, so
        // every pre-#964 routing behaviour (including the mid-rollout cases
        // where the pool map and the router legitimately disagree) is
        // unchanged.
        let origin = pool.routed_shard_for_execution(exec_id);
        let mut conn = pool.pool_for_execution(exec_id).get().await.map_err(|e| {
            HarvestError::ShardUnavailable {
                shard_id: origin.as_i32(),
                reason: format!("pool checkout failed: {e}"),
            }
        })?;

        if pool.len() <= 1 {
            return Ok(conn);
        }

        let mut current = origin;
        for _ in 0..MAX_FORWARD_HOPS {
            let Some(next) = read_forward(&mut conn, exec_id).await? else {
                return Ok(conn);
            };
            current = next;
            conn = checkout(pool, current).await?;
        }
        resolve_forward_chain(origin, |_| Some(current)).map(|_| conn)
    }

    #[derive(diesel::QueryableByName)]
    struct ForwardRow {
        #[diesel(sql_type = Nullable<Integer>)]
        forward: Option<i32>,
    }

    /// Resolve which shard an [`ExternalTarget`] currently lives on, following
    /// the forwarding pointer a rebalance leaves behind.
    ///
    /// The async, forwarding-aware companion to
    /// [`ShardedDbPool::exact_pool_for_target`]. It exists because the engine's
    /// own id-routed delivery paths — the external-signal, external-cancel and
    /// external-await outboxes — decode the raw shard bytes and would otherwise
    /// deliver to the shard a migrated run *originated* on. Landing there is not
    /// a delayed delivery but a permanent wrong answer: the sealed row reads as
    /// terminal, so the sender's history records `ExternalSignalFailed` with
    /// `target_terminal` for a workflow that is alive and well on another shard.
    ///
    /// **Best-effort by design.** A failure to resolve returns the
    /// un-forwarded shard, which is exactly the pre-#964 behaviour, so a
    /// transient error degrades to "route as before" rather than stalling the
    /// outbox. Single-shard deployments and `WorkflowId`-shaped targets skip the
    /// lookup entirely.
    pub async fn resolve_target_shard(
        pool: &ShardedDbPool,
        target: &crate::types::ExternalTarget,
        fallback_shard: ShardId,
    ) -> ShardId {
        let unforwarded = match target {
            crate::types::ExternalTarget::ExecutionId(id) => pool.routed_shard_for_execution(*id),
            crate::types::ExternalTarget::WorkflowId { .. } => {
                crate::shard::external_target_owning_shard(target).unwrap_or(fallback_shard)
            }
        };
        if pool.len() <= 1 {
            return unforwarded;
        }
        let exec_id = match target {
            crate::types::ExternalTarget::ExecutionId(id) => *id,
            // A business-key target hashes to a fixed shard, and that shard is
            // exactly where a rebalanced run's SEAL still sits — the migration
            // deliberately keeps `MIGRATED` inside the active-uniqueness index,
            // so the business key never moves. Stopping there would deliver to
            // the seal: the cancel outbox reads it as terminal and records
            // `ExternalCancelDelivered` for a workflow that keeps running, and
            // the signal outbox sits on it forever. So resolve the key to its
            // execution id on the hashed shard first, then follow that id's
            // pointer like any other.
            crate::types::ExternalTarget::WorkflowId {
                workflow_name,
                workflow_id,
            } => match resolve_business_key(pool, unforwarded, workflow_name, workflow_id).await {
                Some(id) => id,
                None => return unforwarded,
            },
        };
        resolve_execution_shard(pool, exec_id)
            .await
            .unwrap_or(unforwarded)
    }

    /// The execution id currently holding `(workflow_name, workflow_id)` on
    /// `shard`, if any.
    ///
    /// Best-effort, like its caller: `None` means "route as before", never an
    /// error. The lookup deliberately includes `MIGRATED`/`MIGRATING` — that is
    /// the whole point, since the seal is what still holds the business key on
    /// the hashed shard after the run itself has moved.
    async fn resolve_business_key(
        pool: &ShardedDbPool,
        shard: ShardId,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Option<ExecutionId> {
        let mut conn = checkout(pool, shard).await.ok()?;
        business_key_on(&mut conn, workflow_name, workflow_id).await
    }

    /// [`resolve_business_key`]'s query, against a connection the caller
    /// already holds.
    async fn business_key_on(
        conn: &mut AsyncPgConnection,
        workflow_name: &str,
        workflow_id: &str,
    ) -> Option<ExecutionId> {
        let row: Option<BusinessKeyRow> = diesel::sql_query(
            "SELECT id FROM harvest_workflow_executions \
              WHERE workflow_name = $1 AND workflow_id = $2 \
                AND state IN ('RUNNING', 'PAUSED', 'MIGRATED', 'MIGRATING') \
              ORDER BY started_at DESC LIMIT 1",
        )
        .bind::<Text, _>(workflow_name)
        .bind::<Text, _>(workflow_id)
        .get_result(conn)
        .await
        .optional_row()
        .ok()
        .flatten();
        row.map(|r| ExecutionId::from_uuid(r.id))
    }

    #[derive(diesel::QueryableByName)]
    struct BusinessKeyRow {
        #[diesel(sql_type = SqlUuid)]
        id: Uuid,
    }

    /// Best-effort: repoint the execution's **origin** shard straight at the new
    /// target, so a run migrated twice does not accumulate hops.
    ///
    /// Deliberately fallible-and-ignored. Correctness comes from following the
    /// chain in [`resolve_execution_shard`]; this only shortens it. A failure
    /// here costs one extra hop on later lookups and nothing else, so it must
    /// never fail a migration that has already committed.
    async fn collapse_forward_chain(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
        source_shard: ShardId,
        target_shard: ShardId,
    ) {
        let origin = exec_id.shard();
        if origin == source_shard || origin == target_shard {
            return;
        }
        let Ok(mut conn) = checkout(pool, origin).await else {
            return;
        };
        let _ = diesel::sql_query(
            "UPDATE harvest_workflow_executions SET migrated_to_shard = $2 \
              WHERE id = $1 AND migrated_to_shard IS NOT NULL",
        )
        .bind::<SqlUuid, _>(exec_id.as_uuid())
        .bind::<Integer, _>(target_shard.as_i32())
        .execute(&mut *conn)
        .await;
    }

    /// Check out a connection for an execution's **entry point** -- the shard
    /// its id hashes to, or the shard it lives on now.
    ///
    /// Unlike [`checkout`], this falls back to the pool's default shard when no
    /// pool is registered for `shard`, which is exactly the contract of
    /// [`ShardedDbPool::pool_for`] that every other read path in the engine uses
    /// to reach a run's own database. A single-pool deployment -- the default,
    /// and every non-sharded test harness -- registers one pool under one shard
    /// id, so an `ExecutionId` whose shard bits name anything else would
    /// otherwise be unreachable even though its row is sitting right there.
    ///
    /// Safe because every query behind it is keyed by execution id: a fallback
    /// that lands on the wrong database finds no row and answers `NotFound`,
    /// never another run's data.
    ///
    /// Deliberately NOT used for a shard named by a `migrated_to_shard` pointer,
    /// by a migration record, or by an operator-declared retired-shard forward.
    /// Each of those names one specific database, and a silent fallback to the
    /// default there would resolve the run to the wrong shard -- or, on the
    /// erase path, scrub the wrong copy.
    async fn checkout_entry(
        pool: &ShardedDbPool,
        shard: ShardId,
    ) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        pool.pool_for(shard)
            .get()
            .await
            .map_err(|e| HarvestError::ShardUnavailable {
                shard_id: shard.as_i32(),
                reason: format!("pool checkout failed: {e}"),
            })
    }

    async fn checkout(
        pool: &ShardedDbPool,
        shard: ShardId,
    ) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
        let shard_pool =
            pool.exact_pool_for(shard)
                .ok_or_else(|| HarvestError::ShardUnavailable {
                    shard_id: shard.as_i32(),
                    reason: "no database pool is configured for this shard on this node"
                        .to_string(),
                })?;
        shard_pool
            .get()
            .await
            .map_err(|e| HarvestError::ShardUnavailable {
                shard_id: shard.as_i32(),
                reason: format!("pool checkout failed: {e}"),
            })
    }
}
