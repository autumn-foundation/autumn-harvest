//! Registry-aware `quota_key` backfill for pre-upgrade executions (issue #1226).
//!
//! `harvest_workflow_executions.quota_key` is resolved once, at admission
//! time. The source is the workflow type's declared
//! [`crate::quota::QuotaPolicy`] (issue #946). An execution already
//! `RUNNING`/`PAUSED` before its type's policy existed never went through
//! that admission path. It therefore keeps `quota_key = NULL` for the rest
//! of its life, invisible to [`crate::quota::load_quota_usage`]. A tenant
//! with enough such pre-existing runs can admit a full new quota limit on
//! top of usage the engine cannot see. See the migration
//! `20260725000000_harvest_workflow_quotas`'s own "KNOWN LIMITATION" comment
//! and [`crate::quota`]'s module doc for the full gap description.
//!
//! This module closes the gap with a periodic, shard-local sweep. It finds
//! non-terminal rows with `quota_key IS NULL` and re-resolves the key with
//! the SAME [`crate::quota::resolve_quota_key`] the live admission path
//! uses, then backfills it. A row with no declared policy, an unresolvable
//! key, or an over-cap key is left `quota_key = NULL`. That matches the
//! fail-open outcome admission itself would produce for such a row today.
//!
//! # Idempotent by construction
//!
//! The sweep's own `WHERE quota_key IS NULL` predicate is what makes a
//! re-run a no-op. A backfilled row drops out of every later scan on its
//! own, with no completion marker to maintain. Contrast
//! [`crate::codec_rotation`]: its rows can cycle between "on the active
//! key" and "not" as the active key itself changes. `quota_key` here is set
//! at most once, ever, per row.
//!
//! # Fairness: a keyset cursor, not a bare `LIMIT`
//!
//! A row this sweep can never resolve -- no declared policy, an
//! unresolvable key, or an over-cap key -- never leaves the candidate set.
//! A bare `LIMIT` with no stable order could therefore return the SAME
//! stuck rows every tick. That would starve a genuinely resolvable row
//! sorted behind them, indefinitely -- the very quota-bypass gap this
//! module exists to close.
//!
//! [`reconcile_quota_keys_from`] returns a resumable `id` cursor instead,
//! and [`spawn_quota_key_reconciler_for_shard`] carries it forward across
//! ticks. Every tick therefore moves strictly past whatever it just
//! examined. A full pass over the candidate set completes in a bounded
//! number of ticks as a result. `quota_key IS NULL` is not itself a
//! stable cursor: a policy declared mid-uptime changes some rows' outcome
//! between passes. Only `id` order does not.
//!
//! A deployment with no workflow type declaring a `QuotaPolicy` anywhere
//! skips the scan entirely instead ([`any_quota_policy_registered`]).
//! That avoids sweeping rows it already knows it cannot act on.
//!
//! # Runs periodically, not once at startup
//!
//! A one-time startup pass would only close the rollout-window gap the
//! migration describes. It would miss a `QuotaPolicy` declared on an
//! already-running workflow type mid-uptime, with no accompanying
//! restart -- the second design question issue #1226 raises.
//! A periodic sweep (mirroring [`crate::poison_pill`]/[`crate::sessions`])
//! closes both cases with one mechanism. It never touches the startup
//! path, so it cannot delay boot on a deployment with a large non-terminal
//! backlog. `spawn_quota_key_reconciler_for_shard`'s per-tick work is
//! bounded by `batch_size` regardless of how many eligible rows exist.
//! The SCAN that finds them is bounded too, thanks to
//! `idx_harvest_we_quota_reconcile_candidates` (see
//! `quota_reconcile_candidate_query`'s doc comment). The existing
//! `idx_harvest_we_state` index covers only `RUNNING`, not `PAUSED`, so it
//! cannot serve this query on its own; the dedicated index closes that gap.
//!
//! # Residual window
//!
//! This sweep runs on `worker_heartbeat_interval` cadence, not
//! synchronously inside admission. A row backfilled by a policy declared
//! moments ago therefore stays invisible to `load_quota_usage` for up to
//! one reconcile interval after the policy takes effect. That is the same
//! order of magnitude as the rollout gap the migration already documents
//! as bounded and self-healing, not a new risk class.
//!
//! Concretely, this can transiently admit ONE execution over a cap. That
//! is not merely delayed enforcement: an admission racing a
//! not-yet-processed backfill can read a stale, undercounted usage and
//! admit. The physical count lands one over `max_active_executions` once
//! both commit.
//!
//! The backfill's own UPDATE takes [`crate::quota::lock_quota_key`]. That
//! is the same advisory lock
//! [`crate::execution::enforce_quota_admission`] holds around its own
//! check-then-admit, so whichever side acquires the lock first sees a
//! consistent count. This narrows the window: it now depends on ordinary
//! transaction-start ordering, not the entire interleaving space.
//!
//! It cannot fully close the window, though. Consider an admission that
//! already committed before this sweep's next tick even begins. It was
//! never going to see the not-yet-backfilled rows, no matter what lock
//! either side holds.
//!
//! Closing that residual slice would require resolving `quota_key`
//! synchronously inside every admission for a pre-existing row. That is
//! the design this module deliberately avoids (see "Runs periodically,
//! not once at startup" above).
//!
//! # Out of scope: `harvest_dead_letters.quota_key`
//!
//! This sweep only ever reads and writes `harvest_workflow_executions`. A
//! dead letter denormalizes its `quota_key` from its owning execution's row
//! at DLQ-insert time ([`crate::dlq::dead_letter`]). One inserted before
//! that execution's own row was backfilled -- or before this module existed
//! -- keeps `quota_key = NULL` permanently, since nothing ever revisits
//! `harvest_dead_letters` afterward. `max_dead_letters` accounting for such
//! historical rows stays blind. Issue #1226's own draft acceptance criteria
//! scope this module to non-terminal execution rows only, and call a
//! separate DLQ backfill lower priority. It is tracked, not silently
//! dropped, but is a deliberately separate follow-up rather than part of
//! this sweep.

use crate::quota::{QuotaPolicy, quota_key_over_cap, resolve_quota_key};

#[cfg(feature = "db")]
use diesel::sql_types::{BigInt, Jsonb, Nullable, Text};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
#[cfg(feature = "db")]
use uuid::Uuid;

#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult, database_error};

// ---------------------------------------------------------------------------
// Pure decision logic -- no DB dependency, unit-tested without the `db` feature
// ---------------------------------------------------------------------------

/// Outcome of resolving one candidate row's backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// The workflow type has a declared policy and the key resolved --
    /// carries the value to write to `quota_key`.
    Backfilled(String),
    /// The workflow type has a declared policy but `key_expr` did not
    /// resolve against the row's input. Matches admission's fail-open
    /// behavior for an unresolvable key: `quota_key` stays NULL.
    Unresolvable,
    /// The resolved key exceeds [`crate::quota::MAX_QUOTA_KEY_BYTES`].
    /// Admission rejects an over-cap key before a row ever exists. A
    /// pre-existing row predates that check, so it is left NULL rather
    /// than risking a raw Postgres index-row-size error on the UPDATE.
    /// Carries the observed byte length.
    OverCap(u64),
    /// The workflow type has no declared `QuotaPolicy` at reconcile time.
    NoPolicy,
}

/// Decide the backfill outcome for one candidate row.
///
/// Pure and side-effect-free: re-resolves `policy.key_expr` against `input`
/// via the exact function the live admission path calls
/// (`start_or_load_workflow_execution_collect` in `execution.rs`). A
/// backfilled value can therefore never drift from what a fresh admission
/// would have computed for the same input.
#[must_use]
pub fn resolve_backfill(
    policy: Option<QuotaPolicy>,
    input: &serde_json::Value,
) -> ReconcileOutcome {
    let Some(policy) = policy else {
        return ReconcileOutcome::NoPolicy;
    };
    resolve_quota_key(policy.key_expr, input).map_or(ReconcileOutcome::Unresolvable, |key| {
        quota_key_over_cap(&key)
            .map_or(ReconcileOutcome::Backfilled(key), ReconcileOutcome::OverCap)
    })
}

/// Summary of one reconcile sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReconcileSummary {
    /// Rows whose `quota_key` was written this sweep.
    pub backfilled: usize,
    /// Rows with a declared policy whose key did not resolve.
    pub unresolvable: usize,
    /// Rows with a declared policy whose resolved key exceeded the bound.
    pub over_cap: usize,
    /// Rows whose workflow type has no declared policy.
    pub no_policy: usize,
}

impl ReconcileSummary {
    /// Total candidate rows this sweep looked at.
    #[must_use]
    pub const fn total_scanned(&self) -> usize {
        self.backfilled + self.unresolvable + self.over_cap + self.no_policy
    }
}

// ---------------------------------------------------------------------------
// DB-gated sweep and periodic spawner
// ---------------------------------------------------------------------------

/// Default per-shard, per-tick batch size (mirrors
/// [`crate::codec_rotation::CODEC_ROTATION_DEFAULT_BATCH`]).
///
/// Ungated (unlike the sweep itself) so `WorkerConfig::default()` can
/// reference it without the `db` feature, mirroring
/// `CODEC_ROTATION_DEFAULT_BATCH`'s identical placement.
pub const QUOTA_RECONCILE_DEFAULT_BATCH: i64 = 200;

#[cfg(feature = "db")]
#[derive(Debug, diesel::QueryableByName)]
struct CandidateRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Jsonb)]
    input: serde_json::Value,
}

/// SQL for [`reconcile_quota_keys_from`]'s candidate scan.
///
/// Backed by `idx_harvest_we_quota_reconcile_candidates` (migration
/// `20260910192721_harvest_quota_reconcile_candidate_index`), a partial
/// index on `(id) WHERE quota_key IS NULL AND state IN ('RUNNING',
/// 'PAUSED')`. `idx_harvest_we_state` (migration
/// `20260409000000_harvest_initial`) covers only `state = 'RUNNING'`, not
/// `PAUSED`, so it cannot serve this query's `IN`. Without the dedicated
/// index the scan falls back to a full sequential scan of
/// `harvest_workflow_executions` on every tick, unbounded by the current
/// non-terminal row count.
///
/// The index's predicate already includes `quota_key IS NULL`, so a row
/// leaves the index the moment its `quota_key` is backfilled. The index
/// therefore always covers exactly today's candidate set, never the
/// table's full history.
///
/// `AND ($1::uuid IS NULL OR id > $1) ORDER BY id LIMIT $2` is a keyset
/// cursor, not a bare `LIMIT`. A row this sweep can never resolve --
/// [`ReconcileOutcome::Unresolvable`], [`ReconcileOutcome::OverCap`], or a
/// workflow type still declaring no policy -- never leaves the index.
/// A bare `LIMIT` with no stable order could therefore return the SAME
/// stuck rows every tick, starving every resolvable row sorted behind
/// them, indefinitely.
///
/// The cursor makes every tick move strictly past whatever it just
/// examined. A full pass over the candidate set therefore completes in a
/// bounded number of ticks, regardless of how many rows are permanently
/// stuck. See [`reconcile_quota_keys_from`]'s doc comment for how the
/// cursor wraps.
#[cfg(feature = "db")]
const CANDIDATE_SQL: &str = "\
    SELECT id, workflow_name, input FROM harvest_workflow_executions \
    WHERE quota_key IS NULL AND state IN ('RUNNING', 'PAUSED') \
      AND ($1::uuid IS NULL OR id > $1) \
    ORDER BY id \
    LIMIT $2";

/// The exact SQL text [`reconcile_quota_keys`] executes for its candidate
/// scan. Exposed read-only for tests, mirroring
/// [`crate::quota::quota_usage_query`]'s identical purpose.
#[cfg(feature = "db")]
#[must_use]
pub const fn quota_reconcile_candidate_query() -> &'static str {
    CANDIDATE_SQL
}

/// Look up the currently-registered [`QuotaPolicy`] for a workflow type, from
/// the same process-global mirror the live admission path reads.
#[cfg(feature = "db")]
fn registered_quota_policy(workflow_name: &str) -> Option<QuotaPolicy> {
    crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref()
                .and_then(|map| map.get(workflow_name))
                .and_then(|meta| meta.quota)
        })
}

/// `true` when at least one currently-registered workflow type declares a
/// [`QuotaPolicy`], anywhere in the process.
///
/// A deployment that has not adopted quotas at all leaves every active
/// execution matching the candidate scan's predicate forever. `NoPolicy`
/// never sets `quota_key`, so nothing shrinks the index for such a
/// deployment.
///
/// This check lets [`reconcile_quota_keys_from`] skip the scan entirely
/// instead. That avoids a real, unbounded query cost sweeping rows it
/// already knows it cannot act on. It matches issue #946 AC9's
/// zero-overhead promise for a no-quota deployment.
#[cfg(feature = "db")]
fn any_quota_policy_registered() -> bool {
    crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref()
                .map(|map| map.values().any(|meta| meta.quota.is_some()))
        })
        .unwrap_or(false)
}

/// Scan up to `batch_size` non-terminal executions with `quota_key IS NULL`.
///
/// Starts strictly after `after_id` in `id` order, and backfills any row
/// whose workflow type currently has a declared `QuotaPolicy`.
///
/// Returns the summary and the cursor for the NEXT call. That cursor is
/// the id of the last row examined, or `None` once a batch returns fewer
/// rows than `batch_size`. A short batch means the scan reached the end
/// of the candidate set's current `id` order. The next call should then
/// wrap back to the start.
///
/// A caller that keeps feeding the returned cursor back in (as
/// [`spawn_quota_key_reconciler_for_shard`] does) is guaranteed to make
/// forward progress past any row it cannot resolve. That row is never
/// re-examined until a full pass wraps around. It can therefore never
/// pin every tick's candidate scan on rows sorted ahead of it. An
/// unordered `LIMIT` with no cursor could (see [`CANDIDATE_SQL`]'s doc
/// comment).
///
/// `batch_size <= 0`, or no workflow type anywhere currently declaring a
/// `QuotaPolicy` (see [`any_quota_policy_registered`]), is a no-op that
/// returns `after_id` unchanged and issues no query at all.
///
/// Idempotent: a row already backfilled, by this call or a concurrent one,
/// no longer matches the candidate scan's `quota_key IS NULL` predicate.
/// The UPDATE itself repeats that predicate, so a race between two sweeps
/// skips rather than double-writes.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn reconcile_quota_keys_from(
    conn: &mut AsyncPgConnection,
    batch_size: i64,
    after_id: Option<Uuid>,
) -> HarvestResult<(ReconcileSummary, Option<Uuid>)> {
    let mut summary = ReconcileSummary::default();
    if batch_size <= 0 || !any_quota_policy_registered() {
        return Ok((summary, after_id));
    }

    let rows: Vec<CandidateRow> = diesel::sql_query(CANDIDATE_SQL)
        .bind::<Nullable<diesel::sql_types::Uuid>, _>(after_id)
        .bind::<BigInt, _>(batch_size)
        .load(conn)
        .await
        .map_err(database_error)?;

    let returned = rows.len();
    let last_id = rows.last().map(|row| row.id);

    for row in rows {
        let policy = registered_quota_policy(&row.workflow_name);
        match resolve_backfill(policy, &row.input) {
            ReconcileOutcome::Backfilled(key) => {
                let workflow_name = row.workflow_name.clone();
                // Same lock `enforce_quota_admission` takes around its own
                // check-then-admit (`quota::lock_quota_key`). A concurrent
                // admission for this exact key reads a stale, pre-backfill
                // count only if it wins the race to acquire this lock
                // first. See this function's doc comment for why that
                // residual, ordering-dependent window cannot be closed
                // further without a synchronous-with-admission design.
                let rows_affected = Box::pin(conn.transaction::<usize, HarvestError, _>(
                    async move |conn| {
                        crate::quota::lock_quota_key(conn, &workflow_name, &key).await?;
                        diesel::sql_query(
                            "UPDATE harvest_workflow_executions SET quota_key = $1 \
                             WHERE id = $2 AND quota_key IS NULL",
                        )
                        .bind::<Text, _>(key)
                        .bind::<diesel::sql_types::Uuid, _>(row.id)
                        .execute(conn)
                        .await
                        .map_err(database_error)
                    },
                ))
                .await?;
                // A concurrent sweep may have already backfilled this exact
                // row, between this call's candidate scan and its UPDATE.
                // `rows_affected == 0` then. The count must not credit a
                // write that did not happen.
                if rows_affected > 0 {
                    summary.backfilled += 1;
                }
            }
            ReconcileOutcome::Unresolvable => summary.unresolvable += 1,
            ReconcileOutcome::OverCap(observed_bytes) => {
                tracing::warn!(
                    execution_id = %row.id,
                    workflow_name = %row.workflow_name,
                    observed_bytes,
                    "quota_key reconcile: resolved key exceeds MAX_QUOTA_KEY_BYTES, leaving quota_key NULL"
                );
                summary.over_cap += 1;
            }
            ReconcileOutcome::NoPolicy => summary.no_policy += 1,
        }
    }

    // A short batch means the scan reached the end of the id-ordered
    // candidate set. Wrap to the start so the next call's pass covers rows
    // that sort ahead of every row already seen this pass. That includes
    // any row inserted after this pass began. `batch_size` is already
    // checked positive above, so this cast cannot wrap.
    let next_cursor = if returned < usize::try_from(batch_size).unwrap_or(usize::MAX) {
        None
    } else {
        last_id
    };
    Ok((summary, next_cursor))
}

/// Single-pass convenience wrapper over [`reconcile_quota_keys_from`].
///
/// Always starts from the beginning of the candidate set (`after_id =
/// None`). Prefer this for a one-shot sweep, e.g. in tests. A long-lived
/// periodic caller should instead use [`reconcile_quota_keys_from`]
/// directly and carry its cursor forward. Otherwise a large
/// permanently-stuck prefix can pin every call to the same rows (see
/// [`CANDIDATE_SQL`]'s doc comment).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure.
#[cfg(feature = "db")]
pub async fn reconcile_quota_keys(
    conn: &mut AsyncPgConnection,
    batch_size: i64,
) -> HarvestResult<ReconcileSummary> {
    reconcile_quota_keys_from(conn, batch_size, None)
        .await
        .map(|(summary, _next_cursor)| summary)
}

/// Spawn a background task that periodically backfills `quota_key` on
/// pre-upgrade non-terminal executions for one shard. Stops when `cancel`
/// is triggered.
///
/// `batch_size <= 0` disables the sweep: the task returns immediately
/// without polling, mirroring `codec_rotation_batch_size = 0`.
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_quota_key_reconciler_for_shard(
    pool: diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>,
    cancel: tokio_util::sync::CancellationToken,
    interval: std::time::Duration,
    batch_size: i64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if batch_size <= 0 {
            return;
        }
        // Carried across ticks so a permanently-stuck prefix of the
        // candidate set (see `CANDIDATE_SQL`'s doc comment) cannot pin
        // every tick to the same rows. Each tick resumes strictly past
        // the last row the previous tick examined.
        let mut cursor: Option<Uuid> = None;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            match pool.get().await {
                Ok(mut conn) => {
                    match reconcile_quota_keys_from(&mut conn, batch_size, cursor).await {
                        Ok((summary, next_cursor)) => {
                            cursor = next_cursor;
                            if summary.backfilled > 0 {
                                tracing::info!(
                                    backfilled = summary.backfilled,
                                    unresolvable = summary.unresolvable,
                                    over_cap = summary.over_cap,
                                    "backfilled quota_key on pre-upgrade executions"
                                );
                            }
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "quota_key reconcile sweep failed");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        "failed to acquire DB connection for quota_key reconcile"
                    );
                }
            }
            if cancel.is_cancelled() {
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- resolve_backfill ----------------------------------------------

    #[test]
    fn no_registered_policy_is_no_policy() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(resolve_backfill(None, &input), ReconcileOutcome::NoPolicy);
    }

    #[test]
    fn policy_with_resolvable_key_backfills() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_backfill(Some(policy), &input),
            ReconcileOutcome::Backfilled("acme".to_string())
        );
    }

    #[test]
    fn policy_with_unresolvable_key_is_unresolvable() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
        let input = serde_json::json!({ "other": "value" });
        assert_eq!(
            resolve_backfill(Some(policy), &input),
            ReconcileOutcome::Unresolvable
        );
    }

    #[test]
    fn policy_with_over_cap_key_is_over_cap() {
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
        let oversized = "x".repeat(300);
        let input = serde_json::json!({ "tenant_id": oversized });
        assert_eq!(
            resolve_backfill(Some(policy), &input),
            ReconcileOutcome::OverCap(300)
        );
    }

    #[test]
    fn policy_with_no_caps_declared_still_backfills() {
        // Matches admission: `execution.rs` resolves and stamps `quota_key`
        // from any declared policy, unconditionally on `has_any_cap()`.
        // The key is stamped for future usage accounting even when the
        // policy itself enforces nothing yet.
        let policy = QuotaPolicy::new("tenant_id");
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_backfill(Some(policy), &input),
            ReconcileOutcome::Backfilled("acme".to_string())
        );
    }

    #[test]
    fn resolve_backfill_matches_live_admission_resolver_byte_for_byte() {
        // No second resolver, same guarantee `quota.rs` proves for
        // `resolve_quota_key` itself against `resolve_concurrency_key`.
        let policy = QuotaPolicy::new("tenant_id").with_max_active_executions(10);
        let input = serde_json::json!({ "tenant_id": "acme" });
        let expected = resolve_quota_key(policy.key_expr, &input);
        assert_eq!(
            resolve_backfill(Some(policy), &input),
            ReconcileOutcome::Backfilled(expected.expect("resolves"))
        );
    }

    // -- ReconcileSummary -------------------------------------------------

    #[test]
    fn total_scanned_sums_every_outcome_kind() {
        let summary = ReconcileSummary {
            backfilled: 1,
            unresolvable: 2,
            over_cap: 3,
            no_policy: 4,
        };
        assert_eq!(summary.total_scanned(), 10);
    }

    #[test]
    fn default_summary_scanned_nothing() {
        assert_eq!(ReconcileSummary::default().total_scanned(), 0);
    }

    // -- SQL shape ----------------------------------------------------------

    #[cfg(feature = "db")]
    #[test]
    fn candidate_sql_scopes_to_non_terminal_null_quota_key_rows() {
        assert!(CANDIDATE_SQL.contains("quota_key IS NULL"));
        assert!(CANDIDATE_SQL.contains("state IN ('RUNNING', 'PAUSED')"));
        assert!(!CANDIDATE_SQL.contains("SUSPENDED"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn candidate_query_accessor_matches_the_executed_sql() {
        assert_eq!(quota_reconcile_candidate_query(), CANDIDATE_SQL);
    }
}
