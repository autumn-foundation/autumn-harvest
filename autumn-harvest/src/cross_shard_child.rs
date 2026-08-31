//! Cross-shard child workflows (issue #956).
//!
//! Children are pinned to the parent's shard by default, and that default is
//! permanent. When a spawn opts in to [`ChildPlacement::Distributed`](crate::shard::ChildPlacement::Distributed) (or an
//! explicit pin) and the resolved shard is not the parent's, the child cannot be
//! created inside the parent's decision transaction — per-execution ACID is
//! shard-local by design and never spans two databases.
//!
//! # The one-row lifecycle
//!
//! Instead, the spawn writes **one row** into `harvest_cross_shard_children` on
//! the parent's shard, in the *same transaction* as the parent's
//! `ChildWorkflowStarted` / `ChildWorkflowSpawnedDetached` event. That row is
//! not a message: it is the cross-shard child's lifecycle record on the parent's
//! side, and all four cross-shard edges are transitions of it.
//!
//! | Edge | Transition | Dedupe key |
//! |---|---|---|
//! | Child start | `PENDING_START` → `STARTED` | the child's `ExecutionId` is the PK on the target shard |
//! | Cancel | `cancel_requested` → cleared | `cancel_workflow_execution` is idempotent on a terminal target |
//! | Terminal notify | row deleted | the append + delete commit together on the parent's shard |
//! | Close cascade | row deleted | the cascade only acts on a `RUNNING`/`PAUSED` child |
//!
//! # Why the terminal notify is a *pull*
//!
//! The obvious design pushes a notify from the child's shard when it goes
//! terminal. That re-introduces the exact crash window AC3 rules out: a worker
//! that dies between the child's terminal commit and the parent's notify loses
//! the wake. Here the relay instead *reads* the child's state from the target
//! shard and appends the parent's terminal event and deletes the row in one
//! transaction on the parent's shard. Nothing is ever in flight, so there is
//! nothing to lose: a crash at any instant leaves the row exactly where it was,
//! and the next sweep re-observes the same durable fact.
//!
//! # Consistency contract
//!
//! - The parent's decision transaction is shard-local. Always.
//! - Cross-shard effects are **at-least-once with dedupe** (the table above).
//! - A cross-shard child's start and terminal wake are each one scanner tick
//!   away rather than one transaction away. That latency is the price of the
//!   placement, and it is the same price `enforce_external_signals_outbox` /
//!   `enforce_external_cancels_outbox` (issue #492) already pay.
//! - Placement never falls back silently: an unreachable target shard fails the
//!   spawn with the typed, retryable [`HarvestError::ShardUnavailable`].

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult};
use crate::event::WorkflowEvent;
use crate::models::{CrossShardChildRow, NewCrossShardChildRow, NewWorkflowExecution};
use crate::queue::TaskType;
use crate::schema::{harvest_cross_shard_children, harvest_workflow_executions};
use crate::shard::{
    CrossShardChildAction, CrossShardChildObservation, CrossShardChildStatus, ShardedDbPool,
    next_cross_shard_child_action,
};
use crate::types::{ExecutionId, ParentClosePolicy, ShardId};
use crate::{queue, store};

/// How many **actionable** outbox rows one sweep handles per shard.
///
/// Actionable means the row's own columns say work is owed: a child that has
/// not been created yet, or a pending cancel. Bounds a single tick so the relay
/// can never monopolise a scanner thread under a 10k-child fan-out.
const RELAY_BATCH: i64 = 200;

/// How many **already-started** rows one sweep polls for their child's terminal.
///
/// Deliberately larger than [`RELAY_BATCH`]: these rows are usually answered by
/// one batched `id = ANY(...)` read per target shard that returns only the
/// children that actually finished, so the cost is a wide read and a narrow
/// result rather than per-row work.
const POLL_BATCH: i64 = 1_000;

/// Per-row retry backoff, as a SQL due-predicate.
///
/// A row that keeps failing is re-tried after `min(attempts, 6) * 5s`, so a
/// permanently-broken row (an unreachable shard, a poison spec, an unparseable
/// stored policy) backs off to one attempt every 30s instead of being re-driven
/// at full poll cadence — and, more importantly, stops consuming a slot in every
/// single sweep and starving newer rows behind it.
const DUE_PREDICATE: &str = "(last_attempt_at IS NULL OR last_attempt_at < NOW() - \
     (LEAST(attempts, 6) * INTERVAL '5 seconds'))";

/// Everything the relay needs to create the child on the target shard, with
/// every default **already resolved** at spawn time.
///
/// Resolution happens on the spawning worker, which has the handler registry, at
/// the same moment and through the same `resolve_child_workflow_defaults` call
/// the same-shard path uses. The relay never re-derives a default, so a
/// cross-shard child cannot silently differ from the same-shard twin it would
/// otherwise have been.
///
/// Serialized into the row's `child_spec` JSONB. Every field is `Option` or has
/// a `#[serde(default)]` so an older row stays readable across an upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CrossShardChildSpec {
    /// The child's input payload.
    pub input: serde_json::Value,
    /// Queue the child's workflow task is enqueued on (inherited from the parent).
    pub queue_name: String,
    /// Build id the child's task requires, inherited from the parent.
    #[serde(default)]
    pub assigned_build_id: Option<String>,
    /// Ambient context headers inherited from the parent (issue #481).
    #[serde(default)]
    pub context_headers: Option<serde_json::Value>,
    #[serde(default)]
    pub owner: Option<String>,
    #[serde(default)]
    pub runbook_url: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub sla_secs: Option<i64>,
    #[serde(default)]
    pub sla_deadline_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub execution_timeout_secs: Option<i64>,
    #[serde(default)]
    pub deadline_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub chain_execution_timeout_secs: Option<i64>,
    #[serde(default)]
    pub chain_deadline_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub retry_policy: Option<serde_json::Value>,
    /// The child's OWN resolved quota key (issue #946), never the parent's.
    #[serde(default)]
    pub quota_key: Option<String>,
    /// The child's own declared quota **caps**, enforced on the target shard at
    /// creation time exactly as the same-shard path enforces them inline.
    ///
    /// Only the caps travel, not the whole [`crate::quota::QuotaPolicy`: its
    /// `key_expr` is a `&'static str` that cannot round-trip through JSON, and
    /// it would be dead weight anyway — the key it names was already resolved at
    /// spawn time into [`Self::quota_key`], and `enforce_quota_admission`
    /// consumes the resolved key, never the expression.
    #[serde(default)]
    pub quota: Option<QuotaCaps>,
    /// Pre-resolved concurrency group key for the child's task row (issue #247).
    #[serde(default)]
    pub concurrency_key: Option<String>,
    #[serde(default)]
    pub max_concurrent: Option<u32>,
}

/// The cap half of a [`crate::quota::QuotaPolicy`], in a form that survives a
/// JSON round trip (issue #956).
///
/// `QuotaPolicy::key_expr` is a `&'static str` pointing at registry-owned
/// storage, so the policy itself cannot be persisted. The expression is not
/// needed on the relay path regardless: it was already resolved against the
/// child's input at spawn time.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct QuotaCaps {
    /// See [`crate::quota::QuotaPolicy::max_active_executions`].
    #[serde(default)]
    pub max_active_executions: Option<u32>,
    /// See [`crate::quota::QuotaPolicy::max_history_bytes`].
    #[serde(default)]
    pub max_history_bytes: Option<u64>,
    /// See [`crate::quota::QuotaPolicy::max_dead_letters`].
    #[serde(default)]
    pub max_dead_letters: Option<u32>,
}

impl QuotaCaps {
    /// Capture the caps of a resolved policy.
    #[must_use]
    pub const fn from_policy(policy: &crate::quota::QuotaPolicy) -> Self {
        Self {
            max_active_executions: policy.max_active_executions,
            max_history_bytes: policy.max_history_bytes,
            max_dead_letters: policy.max_dead_letters,
        }
    }

    /// Rebuild a policy for `enforce_quota_admission`.
    ///
    /// `key_expr` is deliberately empty: the admission call takes the already
    /// resolved key as a separate argument and never re-resolves the
    /// expression, so there is nothing for it to be wrong about here.
    #[must_use]
    pub const fn to_policy(self) -> crate::quota::QuotaPolicy {
        crate::quota::QuotaPolicy {
            key_expr: "",
            max_active_executions: self.max_active_executions,
            max_history_bytes: self.max_history_bytes,
            max_dead_letters: self.max_dead_letters,
        }
    }
}

/// Refuse a cross-shard spawn whose target shard this process cannot reach
/// (issue #956 AC8).
///
/// Called at **spawn time**, inside the parent's decision cycle, before the
/// outbox row is written. Failing here rolls the parent's decision transaction
/// back with nothing recorded, so the spawn is retried later rather than
/// silently landing the child on the parent's shard — a fallback would break the
/// placement contract without trace, which is precisely the failure mode this
/// check exists to prevent.
///
/// Fails **closed** on a `None` pool. The router and the pool map are two
/// independent globals with two independent installers, so "a multi-shard router
/// with no `ShardedDbPool`" is a reachable misconfiguration (an API-only runtime,
/// an embedder, a half-wired test harness) — and in that state the relay would
/// return `Ok(0)` forever while the row sat there and the parent parked
/// indefinitely. This function is only ever called for a target that already
/// resolved *away* from the parent's shard, so there is no legitimate no-pool
/// case to admit.
///
/// # Errors
///
/// [`HarvestError::ShardUnavailable`] — typed and retryable — when there is no
/// pool map, or the map has no entry for `target`.
pub fn preflight_target_shard(
    sharded_pool: Option<&ShardedDbPool>,
    target: ShardId,
) -> HarvestResult<()> {
    let unavailable = |reason: &str| HarvestError::ShardUnavailable {
        shard_id: target.as_i32(),
        reason: reason.to_string(),
    };
    let pool = sharded_pool.ok_or_else(|| {
        unavailable(
            "this process has no sharded database pool, so a child cannot be \
             placed off the parent's shard",
        )
    })?;
    if pool.exact_pool_for(target).is_none() {
        return Err(unavailable(
            "no database pool is configured for this shard on this node",
        ));
    }
    Ok(())
}

/// Record one cross-shard child on the parent's shard.
///
/// MUST be called inside the parent's own decision transaction, so the row and
/// the parent's `ChildWorkflowStarted` / `ChildWorkflowSpawnedDetached` event
/// commit together or not at all. That atomicity is what makes an orphaned child
/// impossible: no committed row means no child was ever promised.
///
/// Idempotent by `child_exec_id`: a re-park that re-emits the same
/// `StartChildWorkflow` command for an already-recorded child is a no-op, which
/// mirrors the same-shard path's "which children are genuinely new?" filter.
///
/// # Errors
///
/// Propagates database errors.
pub async fn record_cross_shard_child(
    conn: &mut AsyncPgConnection,
    parent_exec_id: ExecutionId,
    child_exec_id: ExecutionId,
    workflow_name: &str,
    parent_close_policy: Option<ParentClosePolicy>,
    spec: &CrossShardChildSpec,
) -> HarvestResult<()> {
    let row = NewCrossShardChildRow {
        child_exec_id: child_exec_id.as_uuid(),
        parent_exec_id: parent_exec_id.as_uuid(),
        target_shard: child_exec_id.shard().as_i32(),
        status: CrossShardChildStatus::PendingStart.as_db_str().to_string(),
        parent_close_policy: parent_close_policy.map(|p| p.to_string()),
        workflow_name: workflow_name.to_string(),
        child_spec: serde_json::to_value(spec).map_err(HarvestError::Serialization)?,
    };
    diesel::insert_into(harvest_cross_shard_children::table)
        .values(&row)
        .on_conflict(harvest_cross_shard_children::child_exec_id)
        .do_nothing()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Durably request cancellation of a cross-shard child.
///
/// Called inside whatever parent-side transaction decided to cancel (a race
/// loser, an over-deadline child, an operator cancel), so the request commits
/// with that decision. The relay delivers it to the target shard on its next
/// sweep; delivery is idempotent, so an at-least-once redelivery is harmless.
///
/// Clears `last_attempt_at` so a row that had backed off after an earlier
/// failure is picked up on the very next sweep — a cancel is latency-sensitive
/// in a way a routine poll is not.
///
/// Returns the number of rows flagged — `0` means the child is not (or is no
/// longer) a tracked cross-shard child, which the caller treats as "nothing to
/// do here".
///
/// # Errors
///
/// Propagates database errors.
pub async fn request_cross_shard_cancel(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) -> HarvestResult<usize> {
    diesel::update(harvest_cross_shard_children::table.find(child_exec_id.as_uuid()))
        .set((
            harvest_cross_shard_children::cancel_requested.eq(true),
            harvest_cross_shard_children::last_attempt_at.eq(None::<DateTime<Utc>>),
            harvest_cross_shard_children::attempts.eq(0),
        ))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Which of `child_ids` are already recorded as cross-shard children here?
///
/// The spawn path's "is this child genuinely new?" test is a lookup in
/// `harvest_workflow_executions` **on the parent's shard**, where a cross-shard
/// child's row never appears — so without this every re-park would classify a
/// remote child as new and append a *second* `ChildWorkflowStarted` for it,
/// corrupting the parent's history and failing its next replay with a
/// non-determinism divergence.
///
/// # Errors
///
/// Propagates database errors.
pub async fn recorded_cross_shard_child_ids(
    conn: &mut AsyncPgConnection,
    child_ids: &[uuid::Uuid],
) -> HarvestResult<Vec<uuid::Uuid>> {
    if child_ids.is_empty() {
        return Ok(Vec::new());
    }
    harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::child_exec_id.eq_any(child_ids))
        .select(harvest_cross_shard_children::child_exec_id)
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// `(id, state, output, error)` as read from a target shard.
type ChildStateRow = (
    uuid::Uuid,
    String,
    Option<serde_json::Value>,
    Option<String>,
);

/// One in-flight child observed on its target shard.
#[derive(Debug, Clone)]
struct TargetChildState {
    state: String,
    output: Option<serde_json::Value>,
    error: Option<String>,
}

/// One sweep of the cross-shard child relay.
///
/// Runs on the parent's shard (this connection) and reaches out to each target
/// shard through `sharded_pool`. Returns how many rows made observable progress.
///
/// Failure of one row never aborts the sweep: a target shard that is down is
/// logged onto the row (`attempts` / `last_error` / `last_attempt_at`) and
/// retried after a backoff, which mirrors `attempt_signal_delivery`'s "one row's
/// transient failure must not abort the scan of every other row" contract from
/// issue #492.
///
/// # Errors
///
/// Only propagates a failure to read this shard's own work-list; every per-row
/// and per-target-shard failure is absorbed onto the row.
pub async fn enforce_cross_shard_children(
    conn: &mut AsyncPgConnection,
    sharded_pool: &Option<ShardedDbPool>,
) -> HarvestResult<usize> {
    // Every cross-shard checkout in this sweep is bounded. Harvest configures no
    // deadpool `Timeouts`, so a bare `pool.get().await` is an *unbounded* wait,
    // and the relay holds a connection on the parent's shard for the whole sweep
    // while reaching across to others — see `acquire_bounded` for the two-pool
    // wait-for cycle that creates. The relay only ever runs with a
    // `ShardedDbPool` present, so the multi-shard bound always applies; the
    // floor (rather than a poll interval) is used because a bounded pool busy
    // dispatching legitimately takes far longer than one poll to hand a
    // connection over.
    let acquire_bound = Some(crate::worker::MIN_SHARD_ACQUIRE_BOUND);
    let active_pool = sharded_pool.clone().or_else(|| {
        crate::shard::GLOBAL_SHARDED_POOL
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    });
    let Some(pool) = active_pool else {
        // No sharded pool means no second database to relay to. Any row here
        // would be unroutable; leave it for a node that has the pools.
        return Ok(0);
    };

    // Only sweep rows whose target shard this worker actually holds a pool for.
    // A row for a shard this node cannot see is left for a node that can — the
    // same "leave pending for other workers" contract the #492 outbox scanners
    // use.
    //
    // Deliberately NOT filtered by the caller's shard assignments: this row
    // lives on the PARENT's shard, which is already the connection we are
    // handed, and `monitor_shard_scope` narrows each per-shard timeout checker's
    // assignment list to that one shard. Intersecting `target_shard` with it
    // would keep only rows whose target IS the parent's shard — i.e. exactly the
    // rows that are never cross-shard — and the relay would sweep nothing at all
    // in the multi-shard deployments it exists for. The union across the fleet's
    // per-shard checkers still covers every shard's rows exactly once, because
    // each checker only ever sees its own database's table.
    let reachable: Vec<i32> = pool.shard_ids().into_iter().map(ShardId::as_i32).collect();
    if reachable.is_empty() {
        return Ok(0);
    }

    let rows = load_sweep_batch(conn, &reachable).await?;
    if rows.is_empty() {
        return Ok(0);
    }

    // Stamp every row this sweep looked at BEFORE acting on it. Two things
    // depend on this: the `last_attempt_at NULLS FIRST` ordering below rotates
    // through a large backlog instead of re-reading the same head every tick
    // (without it a handful of long-running children at the head of
    // `created_at` starve every newer row indefinitely), and a row whose target
    // shard is unreadable this sweep still gets a visible breadcrumb.
    let swept_ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.child_exec_id).collect();
    mark_swept(conn, &swept_ids).await;

    // One batched read per target shard, not one per row: a 10k-child fan-out
    // must not become a 10k-round-trip sweep (the `O(nodes x shards)` shape the
    // children-traversal N+1 fix already called out in this repo).
    let child_states = load_child_states(&pool, &rows, acquire_bound).await;
    let parent_terminal = load_parent_terminal_states(conn, &rows).await;

    let mut progressed = 0;
    for row in rows {
        let Some(status) = CrossShardChildStatus::from_db(&row.status) else {
            let reason = format!("unrecognised status {:?}", row.status);
            tracing::error!(
                child_exec_id = %row.child_exec_id,
                status = %row.status,
                "cross-shard child relay: unrecognised status; the row is stuck"
            );
            record_attempt_failure(conn, row.child_exec_id, &reason).await;
            continue;
        };
        let policy = match row
            .parent_close_policy
            .as_deref()
            .map(str::parse::<ParentClosePolicy>)
        {
            None => None,
            Some(Ok(policy)) => Some(policy),
            Some(Err(e)) => {
                tracing::error!(
                    child_exec_id = %row.child_exec_id,
                    error = %e,
                    "cross-shard child relay: unparseable parent_close_policy; \
                     the row is stuck"
                );
                record_attempt_failure(conn, row.child_exec_id, &e).await;
                continue;
            }
        };
        let observed_child = child_states.get(&row.child_exec_id);
        let observation = CrossShardChildObservation {
            status,
            cancel_requested: row.cancel_requested,
            parent_close_policy: policy,
            parent_terminal: parent_terminal
                .get(&row.parent_exec_id)
                .copied()
                // A parent row that has vanished (retention collection, erase)
                // is treated as closed: there is nobody left to wake.
                .unwrap_or(true),
            child_state: observed_child.map(|c| c.state.as_str()),
        };

        let action = next_cross_shard_child_action(&observation);
        match apply_action(
            conn,
            &pool,
            &row,
            action,
            &observation,
            observed_child,
            acquire_bound,
        )
        .await
        {
            Ok(true) => progressed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    child_exec_id = %row.child_exec_id,
                    parent_exec_id = %row.parent_exec_id,
                    target_shard = row.target_shard,
                    action = ?action,
                    error = %e,
                    "cross-shard child relay: step failed; retrying after a backoff"
                );
                record_attempt_failure(conn, row.child_exec_id, &e.to_string()).await;
            }
        }
    }

    Ok(progressed)
}

/// This sweep's work-list: actionable rows first, then a rotating window of
/// already-started rows.
///
/// Splitting the two is what stops head-of-line starvation. A single
/// `ORDER BY created_at LIMIT N` fills its whole window with rows that are
/// merely *waiting* — an awaited child that is still running is re-read every
/// tick and never deleted until it finishes — so under a 10k-child fan-out the
/// oldest N rows would occupy every slot and rows N+1.. would never be started
/// at all. Actionable rows are selected by their own columns (`PENDING_START` or
/// a pending cancel), so the start backlog always drains; waiting rows are then
/// polled least-recently-swept first, so the poll rotates through the whole
/// backlog rather than re-reading one end of it.
async fn load_sweep_batch(
    conn: &mut AsyncPgConnection,
    reachable: &[i32],
) -> HarvestResult<Vec<CrossShardChildRow>> {
    use diesel::dsl::sql;
    use diesel::sql_types::Bool;

    let mut rows: Vec<CrossShardChildRow> = harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::target_shard.eq_any(reachable))
        .filter(
            harvest_cross_shard_children::status
                .eq(CrossShardChildStatus::PendingStart.as_db_str())
                .or(harvest_cross_shard_children::cancel_requested.eq(true)),
        )
        .filter(sql::<Bool>(DUE_PREDICATE))
        .order(harvest_cross_shard_children::created_at.asc())
        .limit(RELAY_BATCH)
        .select(CrossShardChildRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    let started: Vec<CrossShardChildRow> = harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::target_shard.eq_any(reachable))
        .filter(harvest_cross_shard_children::status.eq(CrossShardChildStatus::Started.as_db_str()))
        .filter(harvest_cross_shard_children::cancel_requested.eq(false))
        .filter(sql::<Bool>(DUE_PREDICATE))
        .order((
            harvest_cross_shard_children::last_attempt_at
                .asc()
                .nulls_first(),
            harvest_cross_shard_children::created_at.asc(),
        ))
        .limit(POLL_BATCH)
        .select(CrossShardChildRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    rows.extend(started);
    Ok(rows)
}

/// Stamp `last_attempt_at` on every row this sweep examined.
///
/// Best effort: failing to record that we looked is not worth aborting a sweep
/// whose real work has not started yet.
async fn mark_swept(conn: &mut AsyncPgConnection, ids: &[uuid::Uuid]) {
    if ids.is_empty() {
        return;
    }
    let _ = diesel::update(
        harvest_cross_shard_children::table
            .filter(harvest_cross_shard_children::child_exec_id.eq_any(ids)),
    )
    .set(harvest_cross_shard_children::last_attempt_at.eq(Some(Utc::now())))
    .execute(conn)
    .await;
}

/// Batched `child_id -> (state, output, error)` read, one query per distinct
/// target shard.
///
/// The terminal payload is fetched here rather than re-read in
/// `deliver_terminal` so a delivery costs no second round trip to the target
/// shard, and so the state the action was *decided* from is the state it is
/// *delivered* from.
///
/// An unreachable shard contributes no entries rather than failing the sweep, so
/// its rows simply observe `child_state: None` and wait — degrading exactly like
/// the read path (AC7) rather than aborting every healthy shard's work.
async fn load_child_states(
    pool: &ShardedDbPool,
    rows: &[CrossShardChildRow],
    acquire_bound: Option<std::time::Duration>,
) -> std::collections::HashMap<uuid::Uuid, TargetChildState> {
    use std::collections::HashMap;
    let mut by_shard: HashMap<i32, Vec<uuid::Uuid>> = HashMap::new();
    for row in rows {
        by_shard
            .entry(row.target_shard)
            .or_default()
            .push(row.child_exec_id);
    }

    let mut states: HashMap<uuid::Uuid, TargetChildState> = HashMap::new();
    for (shard, ids) in by_shard {
        let Some(shard_pool) = pool.exact_pool_for(ShardId::new(shard)) else {
            continue;
        };
        let mut target_conn = match acquire_bounded(shard_pool, shard, acquire_bound).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    target_shard = shard,
                    error = %e,
                    "cross-shard child relay: target shard unreachable this sweep"
                );
                continue;
            }
        };
        let loaded: Result<Vec<ChildStateRow>, _> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(&ids))
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::state,
                harvest_workflow_executions::output,
                harvest_workflow_executions::error,
            ))
            .load(&mut *target_conn)
            .await;
        match loaded {
            Ok(pairs) => {
                for (id, state, output, error) in pairs {
                    states.insert(
                        id,
                        TargetChildState {
                            state,
                            output,
                            error,
                        },
                    );
                }
            }
            Err(e) => tracing::warn!(
                target_shard = shard,
                error = %e,
                "cross-shard child relay: failed to read child states"
            ),
        }
    }
    states
}

/// Batched `parent_id -> is_terminal` read on this (the parent's) shard.
///
/// Absorbs its own failure into an empty map rather than propagating: this runs
/// inside `enforce_timeouts_once`'s `?`-chain, so a propagated error would skip
/// every scanner duty ordered after the relay (debounce, throttle, event
/// batches, the idempotency sweep) for the whole tick. An empty map makes every
/// row observe `parent_terminal: true`; combined with the `DeliverTerminal`
/// step's own `FOR UPDATE` re-read of the parent, the worst outcome is that a
/// row is retired one sweep early — never a wrong history append.
async fn load_parent_terminal_states(
    conn: &mut AsyncPgConnection,
    rows: &[CrossShardChildRow],
) -> std::collections::HashMap<uuid::Uuid, bool> {
    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.parent_exec_id).collect();
    let loaded: Result<Vec<(uuid::Uuid, String)>, _> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq_any(&ids))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::state,
        ))
        .load(conn)
        .await;
    match loaded {
        Ok(pairs) => pairs
            .into_iter()
            .map(|(id, state)| (id, crate::erase::is_terminal_state(&state)))
            .collect(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "cross-shard child relay: failed to read parent states; \
                 treating this sweep's parents as unknown"
            );
            std::collections::HashMap::new()
        }
    }
}

/// Execute one decided action. Returns whether the row made observable progress.
#[allow(clippy::too_many_arguments)]
async fn apply_action(
    conn: &mut AsyncPgConnection,
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    action: CrossShardChildAction,
    observation: &CrossShardChildObservation<'_>,
    observed_child: Option<&TargetChildState>,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<bool> {
    match action {
        CrossShardChildAction::Wait => Ok(false),
        CrossShardChildAction::Retire => {
            delete_row(conn, row.child_exec_id).await?;
            Ok(true)
        }
        CrossShardChildAction::StartChild => {
            start_child_on_target(pool, row, acquire_bound).await?;
            // Only after the child is durably committed on the target shard.
            // A crash before this update simply re-runs the insert, which the
            // child's primary key makes a no-op.
            diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                .set((
                    harvest_cross_shard_children::status
                        .eq(CrossShardChildStatus::Started.as_db_str()),
                    harvest_cross_shard_children::last_error.eq(None::<String>),
                    harvest_cross_shard_children::attempts.eq(0),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }
        CrossShardChildAction::CancelChild => {
            cancel_child_on_target(pool, row, acquire_bound).await?;
            diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                .set((
                    harvest_cross_shard_children::cancel_requested.eq(false),
                    harvest_cross_shard_children::last_error.eq(None::<String>),
                    harvest_cross_shard_children::attempts.eq(0),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }
        CrossShardChildAction::ApplyCloseCascade => {
            let policy = observation
                .parent_close_policy
                .expect("ApplyCloseCascade is only decided for a detached child");
            cascade_child_on_target(pool, row, policy, acquire_bound).await?;
            apply_cascade_bookkeeping(conn, row, policy).await?;
            Ok(true)
        }
        CrossShardChildAction::DeliverTerminal => {
            let Some(child) = observed_child else {
                // `DeliverTerminal` is only decided from an observed terminal
                // state, so this is unreachable; treat it as "wait" rather than
                // panicking inside a scanner.
                return Ok(false);
            };
            deliver_terminal(conn, row, child).await?;
            Ok(true)
        }
    }
}

/// Record a completed cross-shard cascade on the parent, then retire the row.
///
/// Lock order is **execution row -> outbox row**, matching every other path in
/// the engine (the parent's own persist appends events — taking the parent row
/// lock — before it writes or flags an outbox row). Taking the outbox row first
/// here would let a relay sweep and a concurrent parent decision cycle form a
/// wait-for cycle; Postgres would abort one with a raw `deadlock_detected`,
/// which is neither `QuotaExceeded` nor `ShardUnavailable` and would therefore
/// terminally fail a perfectly healthy parent.
///
/// The claim-by-delete inside the transaction is what makes the append
/// exactly-once: every worker assigned this shard sweeps the same rows, so two
/// sweeps can decide the same cascade in the same tick. Their effect on the
/// target shard is idempotent; a history append is not.
async fn apply_cascade_bookkeeping(
    conn: &mut AsyncPgConnection,
    row: &CrossShardChildRow,
    policy: ParentClosePolicy,
) -> HarvestResult<()> {
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let action_str = match policy {
        ParentClosePolicy::RequestCancel => "request_cancel",
        ParentClosePolicy::Terminate => "terminate",
        ParentClosePolicy::Abandon => unreachable!("Abandon never reaches the cascade"),
    };
    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        // Execution row first (see the fn doc). A parent whose row is gone
        // entirely — retention-collected while its detached child's shard was
        // down — has no history to append to; retire the row rather than
        // failing forever on a `NotFound` from `append_single_event`.
        let parent_exists: Option<uuid::Uuid> = harvest_workflow_executions::table
            .find(parent_exec_id.as_uuid())
            .select(harvest_workflow_executions::id)
            .for_update()
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        if !claim_row_by_delete(conn, child_exec_id.as_uuid()).await? {
            // A peer sweep already recorded this cascade.
            return Ok(());
        }
        if parent_exists.is_some() {
            store::append_single_event(
                conn,
                parent_exec_id,
                WorkflowEvent::ChildWorkflowCascadeApplied {
                    child_id: child_exec_id,
                    policy,
                    action: action_str.to_string(),
                },
            )
            .await?;
        }
        Ok(())
    }))
    .await
}

async fn delete_row(conn: &mut AsyncPgConnection, child_exec_id: uuid::Uuid) -> HarvestResult<()> {
    claim_row_by_delete(conn, child_exec_id).await?;
    Ok(())
}

/// Delete the outbox row and report whether **this** transaction removed it.
///
/// This is the exactly-once gate for the two relay steps that append to the
/// parent's history (`DeliverTerminal` and `ApplyCloseCascade`). Every worker
/// assigned the parent's shard runs the relay over the same rows — the work-list
/// read is deliberately lock-free so a sweep never holds a transaction on the
/// parent's shard while it reaches across to another database — so two workers
/// can decide the same action in the same tick. Their cross-shard *effects* are
/// idempotent, but a history append is not: two appends would put two
/// `ChildWorkflowCompleted` events for one child into the parent's history and
/// break replay.
///
/// Making the delete the gate closes that: it takes a row lock, so the second
/// transaction blocks until the first commits and then deletes zero rows,
/// telling it to append nothing. The delete and the append commit together, so
/// the pair is atomic in both directions.
///
/// Callers must take the parent execution row **first** — see
/// [`apply_cascade_bookkeeping`]'s note on lock order.
async fn claim_row_by_delete(
    conn: &mut AsyncPgConnection,
    child_exec_id: uuid::Uuid,
) -> HarvestResult<bool> {
    let deleted = diesel::delete(harvest_cross_shard_children::table.find(child_exec_id))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(deleted > 0)
}

/// Record a failed relay step on the row, so an operator can see *why* a
/// cross-shard child is not progressing without reading logs.
///
/// Also drives the retry backoff: `attempts` is the exponent in `DUE_PREDICATE`,
/// so a row that keeps failing both backs off and stops occupying a slot in
/// every sweep. Best effort — a failure to record a failure is not worth failing
/// the sweep over.
async fn record_attempt_failure(
    conn: &mut AsyncPgConnection,
    child_exec_id: uuid::Uuid,
    error: &str,
) {
    let truncated: String = error.chars().take(500).collect();
    let _ = diesel::update(harvest_cross_shard_children::table.find(child_exec_id))
        .set((
            harvest_cross_shard_children::attempts.eq(harvest_cross_shard_children::attempts + 1),
            harvest_cross_shard_children::last_error.eq(Some(truncated)),
            harvest_cross_shard_children::last_attempt_at.eq(Some(Utc::now())),
        ))
        .execute(conn)
        .await;
}

/// Create the child execution on its target shard.
///
/// Idempotent by the child's primary key: an existence pre-check plus
/// `ON CONFLICT DO NOTHING` makes a repeated relay (a crash between this commit
/// and the row's status update) a no-op rather than a duplicate child. The whole
/// creation — row, its own `WorkflowStarted` event, its queue task — is one
/// transaction on the target shard, so a partially-created child is impossible.
// Long by construction: `NewWorkflowExecution` is a wide, fully-explicit row
// literal (every column named, no `..Default`), exactly as at the two
// same-shard child-insert sites. Splitting it would hide which columns a
// cross-shard child gets, which is the one thing a reader needs to check here.
#[allow(clippy::too_many_lines)]
async fn start_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;

    let spec: CrossShardChildSpec =
        serde_json::from_value(row.child_spec.clone()).map_err(HarvestError::Serialization)?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let child_workflow_id = child_exec_id.to_string();
    let parent_exec_id_str = parent_exec_id.to_string();
    let workflow_name = row.workflow_name.clone();
    let parent_close_policy = row.parent_close_policy.clone();

    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        let spec = spec.clone();
        {
            let already: Option<uuid::Uuid> = harvest_workflow_executions::table
                .find(child_exec_id.as_uuid())
                .select(harvest_workflow_executions::id)
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            if already.is_some() {
                return Ok(());
            }

            let child_row = NewWorkflowExecution {
                continued_from_exec_id: None,
                first_exec_id: None,
                chain_execution_timeout: spec
                    .chain_execution_timeout_secs
                    .map(chrono::Duration::seconds),
                chain_deadline_at: spec.chain_deadline_at,
                id: child_exec_id.as_uuid(),
                workflow_name: &workflow_name,
                workflow_id: &child_workflow_id,
                run_id: uuid::Uuid::new_v4(),
                // The child's row lives on the TARGET shard and must say so:
                // its `ExecutionId` already encodes this shard, and a mismatched
                // column would make every shard-filtered scanner query (timeouts,
                // outboxes, the SLA sweep) skip it.
                shard_id: row.target_shard,
                input: spec.input.clone(),
                parent_id: Some(parent_exec_id.as_uuid()),
                queue_name: &spec.queue_name,
                execution_timeout: spec.execution_timeout_secs.map(chrono::Duration::seconds),
                deadline_at: spec.deadline_at,
                sla: spec.sla_secs.map(chrono::Duration::seconds),
                sla_deadline_at: spec.sla_deadline_at,
                memo: None,
                search_attrs: None,
                assigned_build_id: spec.assigned_build_id.clone(),
                parent_close_policy: parent_close_policy.clone(),
                owner: spec.owner.as_deref(),
                runbook_url: spec.runbook_url.as_deref(),
                severity: spec.severity.as_deref(),
                context_headers: spec.context_headers.clone(),
                schedule_id: None,
                scheduled_for: None,
                workflow_attempt: 1,
                workflow_retry_policy: spec.retry_policy.clone(),
                retry_of_exec_id: None,
                origin: None,
                completion_callbacks: None,
                start_source: Some(crate::types::StartSource::Child.as_str()),
                start_source_ref: Some(parent_exec_id_str.as_str()),
                started_by: None,
                quota_key: spec.quota_key.as_deref(),
            };
            let inserted = diesel::insert_into(harvest_workflow_executions::table)
                .values(&child_row)
                .on_conflict(harvest_workflow_executions::id)
                .do_nothing()
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            if inserted == 0 {
                // Another sweep won the race; its transaction owns the child's
                // event and task.
                return Ok(());
            }

            // The child's OWN declared quota (issue #946), enforced against the
            // row this transaction just inserted and BEFORE its `WorkflowStarted`
            // event is appended — the identical insert-then-enforce ordering the
            // same-shard child path uses, so `history_bytes` reports usage
            // strictly before this admission.
            crate::execution::enforce_quota_admission(
                conn,
                spec.quota.map(QuotaCaps::to_policy),
                spec.quota_key.as_deref(),
                &workflow_name,
                None,
            )
            .await?;

            store::append_events(
                conn,
                child_exec_id,
                &[WorkflowEvent::WorkflowStarted {
                    input: spec.input.clone(),
                    timestamp: Utc::now(),
                    last_completion_result: None,
                    last_error: None,
                    scheduled_time: None,
                }],
                0,
            )
            .await?;

            let mut params = queue::EnqueueParams::new(
                spec.queue_name.clone(),
                TaskType::Workflow,
                spec.input.clone(),
            );
            params.workflow_exec_id = Some(child_exec_id.as_uuid());
            params.required_build_id = spec.assigned_build_id.clone();
            params.concurrency_key = spec.concurrency_key.clone();
            params.max_concurrent = spec.max_concurrent;
            queue::enqueue(conn, &params).await?;
            Ok(())
        }
    }))
    .await
}

/// Deliver an idempotent cancel to a cross-shard child on its target shard.
async fn cancel_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let result = crate::execution::cancel_workflow_execution(
        &mut conn,
        child_exec_id,
        "parent requested cancellation",
        &crate::telemetry::NoOpMetrics,
    )
    .await;
    absorb_already_settled(&mut conn, child_exec_id, result).await
}

/// Apply a `ParentClosePolicy` to a detached cross-shard child.
async fn cascade_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    policy: ParentClosePolicy,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row, acquire_bound).await?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let result = match policy {
        ParentClosePolicy::RequestCancel => {
            crate::execution::cancel_workflow_execution(
                &mut conn,
                child_exec_id,
                "parent closed",
                &crate::telemetry::NoOpMetrics,
            )
            .await
        }
        ParentClosePolicy::Terminate => {
            crate::execution::terminate_workflow_execution(
                &mut conn,
                child_exec_id,
                "ParentClosed",
                &crate::telemetry::NoOpMetrics,
            )
            .await
        }
        ParentClosePolicy::Abandon => unreachable!("Abandon never reaches the cascade"),
    };
    absorb_already_settled(&mut conn, child_exec_id, result).await
}

/// Treat "the child is already gone or already terminal" as success — but only
/// after **confirming** it.
///
/// Both cross-shard mutations (cancel and cascade) are delivered at-least-once,
/// so a redelivery must be indistinguishable from the first delivery.
/// `NotFound` says outright that the child row is gone, which meets the goal.
/// `Config` does not: the engine uses it both for "already terminal for another
/// reason" *and* for genuinely unrelated failures — `apply_parent_close_cascade`
/// returns `Config` when a **grandchild's** stored `parent_close_policy` string
/// will not parse, and swallowing that would have us record a successful cascade
/// while the child kept running, untracked. So a `Config` is only absorbed when
/// a re-read proves the child really is terminal; otherwise it is a real failure
/// and the row retries after a backoff.
async fn absorb_already_settled<T>(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
    result: HarvestResult<T>,
) -> HarvestResult<()> {
    match result {
        Ok(_) | Err(HarvestError::NotFound(_)) => Ok(()),
        Err(HarvestError::Config(reason)) => {
            let state: Option<String> = harvest_workflow_executions::table
                .find(child_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;
            match state {
                None => Ok(()),
                Some(state) if crate::erase::is_terminal_state(&state) => Ok(()),
                Some(state) => Err(HarvestError::Config(format!(
                    "cross-shard child {child_exec_id} is still {state} after a failed \
                     cancel/terminate: {reason}"
                ))),
            }
        }
        Err(e) => Err(e),
    }
}

/// Deliver a terminal child's outcome to its awaiting parent.
///
/// The child's terminal payload was already read from the target shard by
/// `load_child_states`, so this costs no second cross-shard round trip and
/// delivers exactly the state the action was decided from. The parent's
/// `ChildWorkflowCompleted`/`ChildWorkflowFailed` append, its wake, and the
/// outbox row's delete all commit **in one transaction on the parent's shard**.
///
/// Lock order is **execution row -> outbox row**, matching every other path in
/// the engine (see [`apply_cascade_bookkeeping`]). Within that order the
/// claim-by-delete is the exactly-once gate: two concurrent sweeps can decide
/// the same delivery, and while their observation of the child is
/// at-least-once, the parent must see exactly one terminal event.
///
/// A parent that has already sealed is skipped (the append would add
/// replay-visible history past closure) but its row is still deleted — the same
/// "append only to a live parent" rule `notify_awaited_parent_of_child_terminal`
/// enforces on the same-shard path.
async fn deliver_terminal(
    conn: &mut AsyncPgConnection,
    row: &CrossShardChildRow,
    child: &TargetChildState,
) -> HarvestResult<()> {
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
    let state = child.state.clone();
    let output = child.output.clone();
    let error = child.error.clone();

    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        {
            // Parent execution row FIRST — the engine-wide lock order. The
            // batched pre-read is only a hint: a parent that sealed between the
            // two must not receive history past closure, and this lock also
            // serialises the append against a concurrent parent termination,
            // exactly as the same-shard notify path does.
            let parent_state: Option<String> = harvest_workflow_executions::table
                .find(parent_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            // Then claim the row. Losing the claim means a peer sweep already
            // delivered this terminal; there is nothing left to do.
            if !claim_row_by_delete(conn, child_exec_id.as_uuid()).await? {
                return Ok(());
            }

            let parent_live =
                matches!(parent_state, Some(ref s) if !crate::erase::is_terminal_state(s));
            if !parent_live {
                return Ok(());
            }

            // Order any DUE child-deadline timer BEFORE the child terminal so
            // `match_child_or_timer` resolves an over-deadline child to the
            // timeout branch on pure recorded order — the same #779 ordering
            // rule every same-shard wake site applies.
            crate::worker::materialize_due_child_timeout_deadlines(conn, parent_exec_id).await?;
            let event = if state == "COMPLETED" {
                WorkflowEvent::ChildWorkflowCompleted {
                    child_id: child_exec_id,
                    output: output.unwrap_or(serde_json::Value::Null),
                }
            } else {
                // Cancel, terminate, timeout and failure all surface to the
                // parent as `ChildWorkflowFailed` — there is no
                // `ChildWorkflowCancelled` variant and issue #956 adds none.
                // The wording mirrors the same-shard operator-cancel path so a
                // parent cannot tell where its child lived from the message.
                let raw =
                    error.unwrap_or_else(|| format!("child workflow {}", state.to_lowercase()));
                let decoded = crate::failure::decode_workflow_failure(&raw);
                WorkflowEvent::child_workflow_failed_typed(child_exec_id, &decoded)
            };
            store::append_single_event(conn, parent_exec_id, event).await?;
            queue::wake_workflow_task(conn, parent_exec_id).await?;
            Ok(())
        }
    }))
    .await
}

/// Check out a connection to the target shard, under the multi-shard
/// acquisition bound.
async fn target_conn(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    acquire_bound: Option<std::time::Duration>,
) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
    let shard_pool = pool
        .exact_pool_for(ShardId::new(row.target_shard))
        .ok_or_else(|| HarvestError::ShardUnavailable {
            shard_id: row.target_shard,
            reason: "no database pool is configured for this shard on this node".to_string(),
        })?;
    acquire_bounded(shard_pool, row.target_shard, acquire_bound).await
}

/// `pool.get()` under an optional deadline.
///
/// Harvest configures no deadpool `Timeouts`, so a bare `pool.get().await` is an
/// **unbounded** wait. That matters more here than almost anywhere else: the
/// relay holds a checked-out connection on the *parent's* shard for the whole
/// sweep while reaching across to other shards, and `Distributed` placement is
/// symmetric — shard A's parents target B while B's parents target A — so two
/// per-shard checkers on the same node can form a wait-for cycle across two
/// pools with no timeout on either side. Bounding the acquisition converts that
/// from a permanent hang into "skip this shard, retry next sweep", which is
/// exactly what `shard_acquire_bound` (issue #961) exists for.
async fn acquire_bounded(
    shard_pool: &crate::worker::DbPool,
    shard: i32,
    bound: Option<std::time::Duration>,
) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
    let unavailable = |reason: String| HarvestError::ShardUnavailable {
        shard_id: shard,
        reason,
    };
    match bound {
        None => shard_pool
            .get()
            .await
            .map_err(|e| unavailable(format!("pool checkout failed: {e}"))),
        Some(bound) => match tokio::time::timeout(bound, shard_pool.get()).await {
            Ok(Ok(conn)) => Ok(conn),
            Ok(Err(e)) => Err(unavailable(format!("pool checkout failed: {e}"))),
            Err(_) => Err(unavailable(format!(
                "pool checkout did not complete within {bound:?}"
            ))),
        },
    }
}
