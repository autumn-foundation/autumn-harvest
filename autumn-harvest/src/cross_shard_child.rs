//! Cross-shard child workflows (issue #956).
//!
//! Children are pinned to the parent's shard by default, and that default is
//! permanent. When a spawn opts in to [`ChildPlacement::Distributed`] (or an
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
    ChildPlacement, CrossShardChildAction, CrossShardChildObservation, CrossShardChildStatus,
    ShardedDbPool, next_cross_shard_child_action,
};
use crate::types::{ExecutionId, ParentClosePolicy, ShardId};
use crate::{queue, store};

/// How many outbox rows one sweep processes per shard before yielding.
///
/// Bounds a single tick's work under a 10k-child fan-out so the relay can never
/// monopolise a scanner thread; the remainder is picked up on the next tick.
const RELAY_BATCH: i64 = 200;

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
/// A `None` pool (single-pool deployments and every test harness that never
/// builds a `ShardedDbPool`) is *not* an error: routing there is degenerate and
/// a resolved cross-shard target simply cannot occur, because the router that
/// produced it would have to have more than one writable shard.
///
/// # Errors
///
/// [`HarvestError::ShardUnavailable`] — typed and retryable — when the pool map
/// has no entry for `target`.
pub fn preflight_target_shard(
    sharded_pool: Option<&ShardedDbPool>,
    target: ShardId,
) -> HarvestResult<()> {
    let Some(pool) = sharded_pool else {
        return Ok(());
    };
    if pool.exact_pool_for(target).is_none() {
        return Err(HarvestError::ShardUnavailable {
            shard_id: target.as_i32(),
            reason: "no database pool is configured for this shard on this node".to_string(),
        });
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

/// Is `child_exec_id` a cross-shard child recorded on this shard?
///
/// Used by the parent-side cancel paths to decide between an inline
/// same-shard cancel and a durable cross-shard cancel request.
///
/// # Errors
///
/// Propagates database errors.
pub async fn is_cross_shard_child(
    conn: &mut AsyncPgConnection,
    child_exec_id: ExecutionId,
) -> HarvestResult<bool> {
    let found: Option<uuid::Uuid> = harvest_cross_shard_children::table
        .find(child_exec_id.as_uuid())
        .select(harvest_cross_shard_children::child_exec_id)
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;
    Ok(found.is_some())
}

/// Durably request cancellation of a cross-shard child.
///
/// Called inside whatever parent-side transaction decided to cancel (a race
/// loser, an over-deadline child, an operator cancel), so the request commits
/// with that decision. The relay delivers it to the target shard on its next
/// sweep; delivery is idempotent, so an at-least-once redelivery is harmless.
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
        .set(harvest_cross_shard_children::cancel_requested.eq(true))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)
}

/// One sweep of the cross-shard child relay.
///
/// Runs on the parent's shard (this connection) and reaches out to each target
/// shard through `sharded_pool`. Returns how many rows made observable progress.
///
/// Failure of one row never aborts the sweep: a target shard that is down is
/// logged onto the row (`attempts` / `last_error`) and retried next tick, which
/// mirrors `attempt_signal_delivery`'s "one row's transient failure must not
/// abort the scan of every other row" contract from issue #492.
///
/// # Errors
///
/// Only propagates a failure to *read* this shard's own work-list; per-row
/// failures are absorbed.
pub async fn enforce_cross_shard_children(
    conn: &mut AsyncPgConnection,
    sharded_pool: &Option<ShardedDbPool>,
    shard_assignments: &[ShardId],
) -> HarvestResult<usize> {
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
    // A row for a shard this node cannot see is left for a node that can, which
    // is the same "leave pending for other workers" contract the #492 outbox
    // scanners use.
    let reachable: Vec<i32> = pool
        .shard_ids()
        .into_iter()
        .filter(|shard| shard_assignments.is_empty() || shard_assignments.contains(shard))
        .map(ShardId::as_i32)
        .collect();
    if reachable.is_empty() {
        return Ok(0);
    }

    let rows: Vec<CrossShardChildRow> = harvest_cross_shard_children::table
        .filter(harvest_cross_shard_children::target_shard.eq_any(&reachable))
        .order(harvest_cross_shard_children::created_at.asc())
        .limit(RELAY_BATCH)
        .select(CrossShardChildRow::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    if rows.is_empty() {
        return Ok(0);
    }

    // One batched read per target shard, not one per row: a 10k-child fan-out
    // must not become a 10k-round-trip sweep (the `O(nodes × shards)` shape the
    // children-traversal N+1 fix already called out in this repo).
    let child_states = load_child_states(&pool, &rows).await;
    let parent_terminal = load_parent_terminal_states(conn, &rows).await?;

    let mut progressed = 0;
    for row in rows {
        let Some(status) = CrossShardChildStatus::from_db(&row.status) else {
            tracing::error!(
                child_exec_id = %row.child_exec_id,
                status = %row.status,
                "cross-shard child relay: unrecognised status; leaving the row untouched"
            );
            continue;
        };
        let policy = match row.parent_close_policy.as_deref().map(str::parse) {
            None => None,
            Some(Ok(policy)) => Some(policy),
            Some(Err(e)) => {
                tracing::error!(
                    child_exec_id = %row.child_exec_id,
                    error = %e,
                    "cross-shard child relay: unparseable parent_close_policy; \
                     leaving the row untouched"
                );
                continue;
            }
        };
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
            child_state: child_states.get(&row.child_exec_id).map(String::as_str),
        };

        let action = next_cross_shard_child_action(&observation);
        match apply_action(conn, &pool, &row, action, &observation).await {
            Ok(true) => progressed += 1,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    child_exec_id = %row.child_exec_id,
                    parent_exec_id = %row.parent_exec_id,
                    target_shard = row.target_shard,
                    action = ?action,
                    error = %e,
                    "cross-shard child relay: step failed; retrying on the next sweep"
                );
                record_attempt_failure(conn, row.child_exec_id, &e.to_string()).await;
            }
        }
    }

    Ok(progressed)
}

/// Batched `child_id → state` read, one query per distinct target shard.
///
/// An unreachable shard contributes no entries rather than failing the sweep, so
/// its rows simply observe `child_state: None` and wait — degrading exactly like
/// the read path (AC7) rather than aborting every healthy shard's work.
async fn load_child_states(
    pool: &ShardedDbPool,
    rows: &[CrossShardChildRow],
) -> std::collections::HashMap<uuid::Uuid, String> {
    use std::collections::HashMap;
    let mut by_shard: HashMap<i32, Vec<uuid::Uuid>> = HashMap::new();
    for row in rows {
        by_shard
            .entry(row.target_shard)
            .or_default()
            .push(row.child_exec_id);
    }

    let mut states: HashMap<uuid::Uuid, String> = HashMap::new();
    for (shard, ids) in by_shard {
        let Some(shard_pool) = pool.exact_pool_for(ShardId::new(shard)) else {
            continue;
        };
        let mut target_conn = match shard_pool.get().await {
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
        let loaded: Result<Vec<(uuid::Uuid, String)>, _> = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::id.eq_any(&ids))
            .select((
                harvest_workflow_executions::id,
                harvest_workflow_executions::state,
            ))
            .load(&mut target_conn)
            .await;
        match loaded {
            Ok(pairs) => states.extend(pairs),
            Err(e) => tracing::warn!(
                target_shard = shard,
                error = %e,
                "cross-shard child relay: failed to read child states"
            ),
        }
    }
    states
}

/// Batched `parent_id → is_terminal` read on this (the parent's) shard.
async fn load_parent_terminal_states(
    conn: &mut AsyncPgConnection,
    rows: &[CrossShardChildRow],
) -> HarvestResult<std::collections::HashMap<uuid::Uuid, bool>> {
    let ids: Vec<uuid::Uuid> = rows.iter().map(|r| r.parent_exec_id).collect();
    let loaded: Vec<(uuid::Uuid, String)> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::id.eq_any(&ids))
        .select((
            harvest_workflow_executions::id,
            harvest_workflow_executions::state,
        ))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(loaded
        .into_iter()
        .map(|(id, state)| (id, crate::erase::is_terminal_state(&state)))
        .collect())
}

/// Execute one decided action. Returns whether the row made observable progress.
async fn apply_action(
    conn: &mut AsyncPgConnection,
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    action: CrossShardChildAction,
    observation: &CrossShardChildObservation<'_>,
) -> HarvestResult<bool> {
    match action {
        CrossShardChildAction::Wait => Ok(false),
        CrossShardChildAction::Retire => {
            delete_row(conn, row.child_exec_id).await?;
            Ok(true)
        }
        CrossShardChildAction::StartChild => {
            start_child_on_target(pool, row).await?;
            // Only after the child is durably committed on the target shard.
            // A crash before this update simply re-runs the insert, which the
            // child's primary key makes a no-op.
            diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                .set((
                    harvest_cross_shard_children::status
                        .eq(CrossShardChildStatus::Started.as_db_str()),
                    harvest_cross_shard_children::last_error.eq(None::<String>),
                ))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }
        CrossShardChildAction::CancelChild => {
            cancel_child_on_target(pool, row).await?;
            diesel::update(harvest_cross_shard_children::table.find(row.child_exec_id))
                .set(harvest_cross_shard_children::cancel_requested.eq(false))
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            Ok(true)
        }
        CrossShardChildAction::ApplyCloseCascade => {
            let policy = observation
                .parent_close_policy
                .expect("ApplyCloseCascade is only decided for a detached child");
            cascade_child_on_target(pool, row, policy).await?;
            // Record the cascade on the parent exactly as the same-shard path
            // does — same event variant, same fields — then drop the row. The
            // append and the delete commit together, so the cascade is recorded
            // at most once even though its delivery is at-least-once.
            let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
            let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);
            let action_str = match policy {
                ParentClosePolicy::RequestCancel => "request_cancel",
                ParentClosePolicy::Terminate => "terminate",
                ParentClosePolicy::Abandon => {
                    unreachable!("Abandon never reaches ApplyCloseCascade")
                }
            };
            Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
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
                delete_row(conn, child_exec_id.as_uuid()).await?;
                Ok(())
            }))
            .await?;
            Ok(true)
        }
        CrossShardChildAction::DeliverTerminal => {
            deliver_terminal(conn, pool, row).await?;
            Ok(true)
        }
    }
}

async fn delete_row(conn: &mut AsyncPgConnection, child_exec_id: uuid::Uuid) -> HarvestResult<()> {
    diesel::delete(harvest_cross_shard_children::table.find(child_exec_id))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;
    Ok(())
}

/// Best-effort operability breadcrumb. A failure to record a failure is not
/// itself worth failing the sweep over.
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
/// Idempotent by the child's primary key: `ON CONFLICT DO NOTHING` makes a
/// repeated relay (a crash between this commit and the row's status update) a
/// no-op rather than a duplicate child. The whole creation — row, its own
/// `WorkflowStarted` event, its queue task — is one transaction on the target
/// shard, so a partially-created child is impossible.
// Long by construction: `NewWorkflowExecution` is a wide, fully-explicit row
// literal (every column named, no `..Default`), exactly as at the two
// same-shard child-insert sites. Splitting it would hide which columns a
// cross-shard child gets, which is the one thing a reader needs to check here.
#[allow(clippy::too_many_lines)]
async fn start_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row).await?;

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
///
/// `cancel_workflow_execution` is already a no-op against a terminal or missing
/// execution, so an at-least-once redelivery cannot double-cancel or resurrect.
async fn cancel_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row).await?;
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    // Already terminal, or never created: the cancel's goal is met either way.
    absorb_already_settled(
        crate::execution::cancel_workflow_execution(
            &mut conn,
            child_exec_id,
            "parent requested cancellation",
            &crate::telemetry::NoOpMetrics,
        )
        .await,
    )
}

/// Treat "the child is already gone or already terminal" as success.
///
/// Both cross-shard mutations (cancel and cascade) are delivered at-least-once,
/// so a redelivery must be indistinguishable from the first delivery. Exactly
/// two error shapes mean "the goal is already met": `NotFound` (the child row is
/// gone) and `Config` (the engine's "already terminal for another reason"
/// signal). Everything else is a genuine failure and is retried next sweep.
fn absorb_already_settled<T>(result: HarvestResult<T>) -> HarvestResult<()> {
    match result {
        Ok(_) | Err(HarvestError::NotFound(_) | HarvestError::Config(_)) => Ok(()),
        Err(e) => Err(e),
    }
}

/// Apply a `ParentClosePolicy` to a detached cross-shard child.
///
/// Idempotent by state: both branches only act on a `RUNNING`/`PAUSED` child, so
/// a redelivery after the child already closed does nothing.
async fn cascade_child_on_target(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
    policy: ParentClosePolicy,
) -> HarvestResult<()> {
    let mut conn = target_conn(pool, row).await?;
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
    // A missing or already-terminal child means the cascade's goal is met, so it
    // is deliberately as successful as a fresh cascade — an at-least-once
    // redelivery must not resurrect the child or fail the sweep. Anything else
    // retries next tick.
    absorb_already_settled(result)
}

/// Deliver a terminal child's outcome to its awaiting parent.
///
/// The child's terminal payload is read from the target shard, then the parent's
/// `ChildWorkflowCompleted`/`ChildWorkflowFailed` append, its wake, and the
/// outbox row's delete all commit **in one transaction on the parent's shard**.
/// That single commit is why the delivery is exactly-once from the parent's
/// point of view even though the relay's observation of the child is
/// at-least-once.
///
/// A parent that has already sealed is skipped (the append would add
/// replay-visible history past closure) but its row is still deleted — the same
/// "append only to a live parent" rule `notify_awaited_parent_of_child_terminal`
/// enforces on the same-shard path.
async fn deliver_terminal(
    conn: &mut AsyncPgConnection,
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
) -> HarvestResult<()> {
    let child_exec_id = ExecutionId::from_uuid(row.child_exec_id);
    let parent_exec_id = ExecutionId::from_uuid(row.parent_exec_id);

    let (state, output, error) = {
        let mut target_conn = target_conn(pool, row).await?;
        harvest_workflow_executions::table
            .find(row.child_exec_id)
            .select((
                harvest_workflow_executions::state,
                harvest_workflow_executions::output,
                harvest_workflow_executions::error,
            ))
            .first::<(String, Option<serde_json::Value>, Option<String>)>(&mut target_conn)
            .await
            .map_err(crate::error::database_error)?
    };

    Box::pin(conn.transaction::<(), HarvestError, _>(async |conn| {
        {
            // Re-read the parent under `FOR UPDATE` inside the delivery
            // transaction: the batched pre-read is only a hint, and a parent that
            // sealed between the two must not receive history past closure. The
            // lock also serialises this append against a concurrent parent
            // termination, exactly as the same-shard notify path does.
            let parent_state: Option<String> = harvest_workflow_executions::table
                .find(parent_exec_id.as_uuid())
                .select(harvest_workflow_executions::state)
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            let parent_live =
                matches!(parent_state, Some(ref s) if !crate::erase::is_terminal_state(s));

            if parent_live {
                // Order any DUE child-deadline timer BEFORE the child terminal so
                // `match_child_or_timer` resolves an over-deadline child to the
                // timeout branch on pure recorded order — the same #779 ordering
                // rule every same-shard wake site applies.
                crate::worker::materialize_due_child_timeout_deadlines(conn, parent_exec_id)
                    .await?;
                let event = if state == "COMPLETED" {
                    WorkflowEvent::ChildWorkflowCompleted {
                        child_id: child_exec_id,
                        output: output.unwrap_or(serde_json::Value::Null),
                    }
                } else {
                    // Cancel, terminate, timeout and failure all surface to the
                    // parent as `ChildWorkflowFailed` — there is no
                    // `ChildWorkflowCancelled` variant and issue #956 adds none.
                    let raw = error.unwrap_or_else(|| format!("child workflow {state}"));
                    let decoded = crate::failure::decode_workflow_failure(&raw);
                    WorkflowEvent::child_workflow_failed_typed(child_exec_id, &decoded)
                };
                store::append_single_event(conn, parent_exec_id, event).await?;
                queue::wake_workflow_task(conn, parent_exec_id).await?;
            }

            delete_row(conn, child_exec_id.as_uuid()).await?;
            Ok(())
        }
    }))
    .await
}

/// The target shard's pool, or the typed unavailable error.
///
/// Returns a *cloned pool handle* rather than a checked-out connection so the
/// borrow of `pool` ends here; callers take their own connection with their own
/// lifetime, which keeps each cross-shard step's connection scope explicit.
fn target_pool(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
) -> HarvestResult<crate::worker::DbPool> {
    pool.exact_pool_for(ShardId::new(row.target_shard))
        .cloned()
        .ok_or_else(|| HarvestError::ShardUnavailable {
            shard_id: row.target_shard,
            reason: "no database pool is configured for this shard on this node".to_string(),
        })
}

/// Check out a connection to the target shard.
async fn target_conn(
    pool: &ShardedDbPool,
    row: &CrossShardChildRow,
) -> HarvestResult<diesel_async::pooled_connection::deadpool::Object<AsyncPgConnection>> {
    target_pool(pool, row)?
        .get()
        .await
        .map_err(|e| HarvestError::ShardUnavailable {
            shard_id: row.target_shard,
            reason: format!("pool checkout failed: {e}"),
        })
}

/// Does `placement` mean "somewhere other than the parent's shard"?
///
/// A tiny helper so call sites read as intent rather than as an enum match.
#[must_use]
pub fn is_cross_shard(placement: &ChildPlacement, parent: ShardId, resolved: ShardId) -> bool {
    !placement.is_parent_shard() && resolved != parent
}
