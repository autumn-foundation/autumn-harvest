//! Registry-aware `quota_key` backfill for pre-upgrade executions (issue #1226).
//!
//! Admission resolves `harvest_workflow_executions.quota_key` once, from the
//! declared [`crate::quota::QuotaPolicy`] of the workflow type (issue #946).
//! An execution already `RUNNING`/`PAUSED` before its type's policy existed
//! never went through that path. It keeps `quota_key = NULL` for life,
//! invisible to [`crate::quota::load_quota_usage`]. A tenant with enough such
//! runs can admit a full new quota limit on top of usage the engine cannot
//! see. See the "KNOWN LIMITATION" comment of the migration
//! `20260725000000_harvest_workflow_quotas` and the [`crate::quota`] module
//! doc.
//!
//! This module closes the gap with a periodic, shard-local sweep. It finds
//! non-terminal rows with `quota_key IS NULL` and re-resolves the key with
//! the SAME [`crate::quota::resolve_quota_key`] that admission uses. A row
//! with no declared policy, an unresolvable key or an over-cap key keeps
//! `quota_key = NULL`. Admission fails open the same way for such a row.
//!
//! # Idempotent by construction
//!
//! The `WHERE quota_key IS NULL` predicate of the sweep makes a re-run a
//! no-op. A backfilled row drops out of every later scan, with no completion
//! marker. Contrast [`crate::codec_rotation`]: its rows can cycle between "on
//! the active key" and "not" as the active key changes. Here `quota_key` is
//! set at most once per row.
//!
//! # Fairness: a keyset cursor, not a bare `LIMIT`
//!
//! A row this sweep can never resolve (no declared policy, an unresolvable
//! key or an over-cap key) never leaves the candidate set. A bare `LIMIT`
//! with no stable order could return the SAME stuck rows every tick. That
//! starves a resolvable row sorted behind them indefinitely: the quota-bypass
//! gap this module exists to close.
//!
//! [`reconcile_quota_keys_from`] returns a resumable `id` cursor instead, and
//! [`spawn_quota_key_reconciler_for_shard`] carries it across ticks. Each
//! tick moves strictly past what it just examined. A full pass over the
//! CURRENT candidate set takes at most `ceil(candidates / batch_size)` ticks,
//! however large the backlog. Nothing cuts that count short, on purpose. An
//! early, fixed-tick wrap would abandon a large stuck prefix before the pass
//! reaches the resolvable rows behind it. That reopens the starvation this
//! cursor prevents.
//!
//! `quota_key IS NULL` is not a stable cursor: a policy declared mid-uptime
//! changes the outcome of some rows between passes. Only `id` order is
//! stable. A row sorted AHEAD of the cursor sees a newly declared policy on
//! the next tick. A row sorted BEHIND it waits until the current pass wraps.
//! The length of that pass bounds its staleness, not a fixed number of ticks.
//!
//! The scan reads only workflow types with a registered `QuotaPolicy`
//! ([`registered_quota_workflow_names`]). A deployment with no such policy
//! skips the scan. A mixed deployment never fetches the rows of a no-policy
//! type, which it cannot act on.
//!
//! # Runs periodically, not once at startup
//!
//! A one-time startup pass would close only the rollout-window gap that the
//! migration describes. It would miss a `QuotaPolicy` declared on a running
//! workflow type mid-uptime, with no restart. Issue #1226 raises that as its
//! second design question. A periodic sweep (as in [`crate::poison_pill`] and
//! [`crate::sessions`]) closes both cases with one mechanism. It never
//! touches the startup path, so a large non-terminal backlog cannot delay
//! boot.
//!
//! `batch_size` bounds the per-tick work of
//! `spawn_quota_key_reconciler_for_shard`, however many rows are eligible.
//! `idx_harvest_we_quota_reconcile_name_id` bounds the scan that finds them
//! (see the doc comment of `quota_reconcile_candidate_query`). The scan reads
//! at most `batch_size` rows per registered workflow name. The
//! `idx_harvest_we_state` index covers only `RUNNING`, not `PAUSED`, so it
//! cannot serve this query alone. The dedicated index closes that gap.
//!
//! # Multiple workers on one shard: redundant, not coordinated
//!
//! `spawn_monitoring_tasks` starts one reconciler per assigned shard, on each
//! worker process assigned to that shard. Nothing elects one reconciler or
//! splits batches between them, so two workers can fetch overlapping rows on
//! the same tick.
//!
//! [`crate::poison_pill::reclaim_orphaned_tasks`] and
//! [`crate::sessions::spawn_session_slot_reconciler`] do not coordinate
//! across workers either. That is the standing pattern for periodic
//! per-shard scanners, not a gap unique to this module.
//!
//! Correctness does not need exclusivity. The scan and the UPDATE both
//! filter on `quota_key IS NULL`, so a row that another worker backfilled is
//! not there to rewrite. The loser gets `rows_affected == 0` and credits no
//! count (see the doc comment of [`reconcile_quota_keys_from`]).
//!
//! An overlapping tick wastes work: one more transaction and advisory lock
//! per row fetched twice, scaling with the worker count of a shard. An
//! election, or a disjoint batch claim, would remove that waste. Doing that
//! only here, while each sibling scanner stays uncoordinated, would be an
//! inconsistent one-off. It is a cross-cutting improvement, not a fix this
//! module owes alone.
//!
//! # Residual window
//!
//! This sweep runs on the `worker_heartbeat_interval` cadence, not inside
//! admission. A row backfilled for a policy declared moments ago stays
//! invisible to `load_quota_usage` for up to one reconcile interval. The
//! migration documents a rollout gap of the same order as bounded and
//! self-healing, so this is not a new risk class.
//!
//! This can transiently admit ONE execution over a cap, not merely delay
//! enforcement. An admission that races a pending backfill can read a stale,
//! undercounted usage and admit. Once both commit, the count is one over
//! `max_active_executions`.
//!
//! The backfill UPDATE takes [`crate::quota::lock_quota_key`].
//! [`crate::execution::enforce_quota_admission`] holds the same advisory lock
//! around its check-then-admit, so the side that takes the lock first sees a
//! consistent count. The window then depends on ordinary transaction-start
//! order, not on the whole interleaving space.
//!
//! The lock cannot fully close the window. An admission can commit before
//! the next tick of this sweep. That admission never sees the rows that still
//! wait for a backfill, whatever lock either side holds.
//!
//! To close that slice, each admission for a pre-existing row would have to
//! resolve `quota_key` synchronously. This module avoids that design on
//! purpose (see "Runs periodically, not once at startup" above).
//!
//! # Cross-region DR fencing (issue #954)
//!
//! Each backfill UPDATE first asserts [`crate::replication::assert_fence`] in
//! the same transaction, as [`crate::store::append_single_event`] does for
//! `harvest_events`.
//!
//! A worker pinned to a superseded shard generation must release. It must
//! not write a quota key from its possibly stale registry view onto a
//! database that another region now owns. With no pinned generation for the
//! shard, the assert is a cheap in-process check.
//!
//! # Out of scope: `harvest_dead_letters.quota_key`
//!
//! This sweep reads and writes only `harvest_workflow_executions`. A dead
//! letter copies its `quota_key` from the row of its execution at DLQ-insert
//! time ([`crate::dlq::dead_letter`]). A dead letter inserted before that
//! backfill, or before this module existed, keeps `quota_key = NULL` for
//! good. Nothing revisits `harvest_dead_letters`, so `max_dead_letters`
//! accounting stays blind to such rows. The draft acceptance criteria of
//! issue #1226 scope this module to non-terminal execution rows. They call a
//! separate DLQ backfill lower priority. It is a tracked, separate follow-up.

use crate::quota::{QuotaPolicy, quota_key_over_cap, resolve_quota_key};

#[cfg(feature = "db")]
use diesel::sql_types::{BigInt, Jsonb, Nullable, Text};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
#[cfg(feature = "db")]
use uuid::Uuid;

#[cfg(feature = "db")]
use crate::error::{HarvestError, HarvestResult, database_error};

/// Outcome of resolving one candidate row's backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileOutcome {
    /// The type has a declared policy and the key resolved. Carries the
    /// value to write to `quota_key`.
    Backfilled(String),
    /// The workflow type has a declared policy but `key_expr` did not
    /// resolve against the row's input. Matches admission's fail-open
    /// behavior for an unresolvable key: `quota_key` stays NULL.
    Unresolvable,
    /// The resolved key exceeds [`crate::quota::MAX_QUOTA_KEY_BYTES`].
    /// Admission rejects an over-cap key before a row exists. A pre-existing
    /// row predates that check. It stays NULL, so the UPDATE cannot hit a
    /// Postgres index-row-size error. Carries the observed byte length.
    OverCap(u64),
    /// The workflow type has no declared `QuotaPolicy` at reconcile time.
    NoPolicy,
}

/// Decide the backfill outcome for one candidate row.
///
/// Pure: it re-resolves `policy.key_expr` against `input` with the function
/// that admission calls (`start_or_load_workflow_execution_collect` in
/// `execution.rs`). So a backfilled value cannot drift from a fresh admission.
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

/// Default per-shard, per-tick batch size.
///
/// It mirrors [`crate::codec_rotation::CODEC_ROTATION_DEFAULT_BATCH`].
///
/// Ungated, unlike the sweep, so `WorkerConfig::default()` can reference it
/// without the `db` feature.
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
/// The query seeks once per registered workflow name. `$1` holds the
/// names. A `LATERAL` subquery reads the first `$3` candidate rows of one
/// name in `id` order. The outer query merges those rows and keeps the
/// first `$3`. The result equals one global `id`-ordered scan over all
/// registered names, so the keyset cursor below is unchanged.
///
/// `idx_harvest_we_quota_reconcile_name_id` backs each seek (migration
/// `20261001192155_harvest_quota_reconcile_name_id_index`). It is a
/// partial index on `(workflow_name, id) WHERE quota_key IS NULL AND
/// state IN ('RUNNING', 'PAUSED')`. Each seek is an equality on
/// `workflow_name`, so the index returns rows in `id` order with no sort.
/// A tick reads at most `$3` rows per registered name. It never reads the
/// rows of unregistered types, or the resolved history of a registered
/// type (issue #1631). The bound is names times `$3`, not `$3`. A
/// deployment with hundreds of quota types and full backlogs reads more
/// rows per tick than the old scan did.
///
/// A row leaves the index when its `quota_key` is backfilled or its
/// execution turns terminal. So the index covers only the current
/// candidate set.
///
/// The old form was one scan with `workflow_name = ANY($1) ORDER BY id
/// LIMIT $3` over an `(id)` index. Postgres tested the name on every
/// row, so cost grew with the unrelated `NULL` backlog. The plan also
/// changed as a registered type matured. See
/// `docs/performance-quota-reconcile-candidate-scan.md`.
///
/// Only types with a registered `QuotaPolicy` are candidates (see
/// [`registered_quota_workflow_names`]). A `NoPolicy` row never gets a
/// `quota_key`. Without the name filter, a mixed deployment would re-fetch every no-policy row on every tick. Each
/// call re-reads `$1` from the live registry, so a policy declared or
/// removed mid-uptime takes effect on the next tick. The module doc says
/// when a given row becomes visible.
///
/// `AND id >= COALESCE($2, <nil uuid>) AND ($2 IS NULL OR id > $2) ORDER BY
/// id LIMIT $3` is a keyset cursor, not a bare `LIMIT`. The `>=` bound keeps
/// the cursor an index condition in a generic plan. An `IS NULL OR` form
/// alone would become a filter, and each tick would re-walk the rows below
/// the cursor. The second clause makes the cursor strict. It also keeps a
/// row with the nil UUID id in the first page.
///
/// A row this sweep can never resolve ([`ReconcileOutcome::Unresolvable`]
/// or [`ReconcileOutcome::OverCap`]) never leaves the index. Without the
/// cursor, such rows could fill every tick and starve the resolvable rows
/// behind them. See [`reconcile_quota_keys_from`] for how the cursor wraps.
#[cfg(feature = "db")]
const CANDIDATE_SQL: &str = "\
    SELECT c.id, c.workflow_name, c.input \
    FROM unnest($1::text[]) AS n(name) \
    CROSS JOIN LATERAL ( \
        SELECT e.id, e.workflow_name, e.input \
        FROM harvest_workflow_executions e \
        WHERE e.workflow_name = n.name \
          AND e.quota_key IS NULL AND e.state IN ('RUNNING', 'PAUSED') \
          AND e.id >= COALESCE($2::uuid, '00000000-0000-0000-0000-000000000000') \
          AND ($2::uuid IS NULL OR e.id > $2) \
        ORDER BY e.id \
        LIMIT $3 \
    ) c \
    ORDER BY c.id \
    LIMIT $3";

/// The exact SQL text of the candidate scan of [`reconcile_quota_keys`].
///
/// Exposed read-only for tests, as [`crate::quota::quota_usage_query`] is.
#[cfg(feature = "db")]
#[must_use]
pub const fn quota_reconcile_candidate_query() -> &'static str {
    CANDIDATE_SQL
}

/// The registered [`QuotaPolicy`] of a workflow type, from the
/// process-global mirror that admission reads.
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

/// `true` when the sweep must backfill the key of a type with this policy.
///
/// A policy with only run LLM caps does not use the key (issue #1997). A
/// start skips it, so the sweep skips it too. Otherwise an unresolvable key
/// would keep each row of the type a candidate on every sweep.
#[cfg(feature = "db")]
const fn reconciles_key(quota: Option<crate::quota::QuotaPolicy>) -> bool {
    match quota {
        Some(policy) => policy.uses_key(),
        None => false,
    }
}

/// Names of every registered workflow type that declares a [`QuotaPolicy`].
///
/// [`reconcile_quota_keys_from`] binds this as the `$1` name list of
/// `CANDIDATE_SQL`, so a type with no declared policy is never a candidate.
/// `NoPolicy` never sets `quota_key`, so a no-policy row never leaves the
/// candidate index. Without this filter, a mixed deployment would re-fetch
/// every no-policy row forever.
///
/// On an empty result, [`reconcile_quota_keys_from`] skips the query. It
/// would return nothing but still cost a round trip. That keeps the
/// zero-overhead promise of issue #946 AC9 for a deployment with no quotas.
#[cfg(feature = "db")]
fn registered_quota_workflow_names() -> Vec<String> {
    crate::completion_trigger::GLOBAL_WORKFLOW_METADATA
        .read()
        .ok()
        .and_then(|lock| {
            lock.as_ref().map(|map| {
                map.iter()
                    .filter(|(_, meta)| reconciles_key(meta.quota))
                    .map(|(name, _)| name.clone())
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Scan up to `batch_size` non-terminal executions with `quota_key IS NULL`.
///
/// Starts strictly after `after_id` in `id` order, and backfills each row
/// whose workflow type has a declared `QuotaPolicy`.
///
/// Returns the summary and the cursor for the NEXT call. The cursor is the id
/// of the last row examined. It is `None` once a batch returns fewer than
/// `batch_size` rows. A short batch reached the end of the candidate set, so the next
/// call wraps to the start.
///
/// A caller that feeds the cursor back in (as
/// [`spawn_quota_key_reconciler_for_shard`] does) always moves past a row
/// it cannot resolve. That row waits until a full pass wraps, so it cannot
/// pin every tick on the rows sorted ahead of it. An unordered `LIMIT` with
/// no cursor could (see [`CANDIDATE_SQL`]).
///
/// `batch_size <= 0`, or no registered `QuotaPolicy` (see
/// [`registered_quota_workflow_names`]), is a no-op. It returns `after_id`
/// unchanged and sends no query.
///
/// Idempotent: a row already backfilled, by this call or a concurrent one,
/// no longer matches the `quota_key IS NULL` predicate of the scan. The
/// UPDATE repeats that predicate, so a race between two sweeps skips rather
/// than double-writes.
///
/// Before each write, [`crate::replication::assert_fence`] checks `shard`,
/// as [`crate::store::append_single_event`] does for `harvest_events`
/// (cross-region DR, issue #954). A worker pinned to a superseded shard
/// generation can hold a stale registry view. It must not write a
/// `quota_key` from that view onto a database that another region owns.
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure, or
/// [`crate::error::HarvestError::ShardFenced`] if this process is pinned
/// to a superseded generation for `shard`.
#[cfg(feature = "db")]
pub async fn reconcile_quota_keys_from(
    conn: &mut AsyncPgConnection,
    batch_size: i64,
    after_id: Option<Uuid>,
    shard: Option<crate::types::ShardId>,
) -> HarvestResult<(ReconcileSummary, Option<Uuid>)> {
    reconcile_quota_keys_from_with_codecs(
        conn,
        batch_size,
        after_id,
        shard,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`reconcile_quota_keys_from`], decoding each input with `codecs` before
/// it resolves the key (issue #1979).
///
/// An input this registry cannot decode resolves no key, so its row counts
/// as unresolvable.
///
/// # Errors
///
/// As [`reconcile_quota_keys_from`].
#[cfg(feature = "db")]
pub async fn reconcile_quota_keys_from_with_codecs(
    conn: &mut AsyncPgConnection,
    batch_size: i64,
    after_id: Option<Uuid>,
    shard: Option<crate::types::ShardId>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<(ReconcileSummary, Option<Uuid>)> {
    let mut summary = ReconcileSummary::default();
    let workflow_names = registered_quota_workflow_names();
    if batch_size <= 0 || workflow_names.is_empty() {
        return Ok((summary, after_id));
    }
    // As `queue::fence_binding`. `UNENCODED` lets the fence registry
    // resolve its pinned default shard, as a single unsharded pool needs.
    let shard = shard.unwrap_or(crate::types::ShardId::UNENCODED);

    let rows: Vec<CandidateRow> = diesel::sql_query(CANDIDATE_SQL)
        .bind::<diesel::sql_types::Array<Text>, _>(workflow_names)
        .bind::<Nullable<diesel::sql_types::Uuid>, _>(after_id)
        .bind::<BigInt, _>(batch_size)
        .load(conn)
        .await
        .map_err(database_error)?;

    let returned = rows.len();
    let last_id = rows.last().map(|row| row.id);

    for row in rows {
        let policy = registered_quota_policy(&row.workflow_name);
        // An input this registry cannot decode resolves no key. Resolving it
        // against the envelope could match an envelope field by accident.
        let outcome = codecs
            .decode_column(&row.input)
            .map_or(ReconcileOutcome::Unresolvable, |input| {
                resolve_backfill(policy, &input)
            });
        match outcome {
            ReconcileOutcome::Backfilled(key) => {
                let workflow_name = row.workflow_name.clone();
                // `enforce_quota_admission` takes the same lock around its
                // check-then-admit (`quota::lock_quota_key`). A concurrent
                // admission for this key reads a stale, pre-backfill count
                // only if it takes this lock first. See this function's doc
                // comment for why only a synchronous design closes that window.
                let rows_affected = Box::pin(conn.transaction::<usize, HarvestError, _>(
                    async move |conn| {
                        // Cross-region DR fence (issue #954), before the
                        // quota lock. A fenced worker must release without
                        // taking a lock that the owning region needs.
                        crate::replication::assert_fence(conn, shard).await?;
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
                // A concurrent sweep can backfill this row between the scan
                // and the UPDATE. Then `rows_affected == 0`, and the count
                // must not credit it.
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

    // A short batch reached the end of the candidate set. Wrap, so the next
    // pass covers rows that sort ahead of this pass, new rows included.
    // `batch_size` is positive here, so the cast cannot wrap.
    let next_cursor = if returned < usize::try_from(batch_size).unwrap_or(usize::MAX) {
        None
    } else {
        last_id
    };
    Ok((summary, next_cursor))
}

/// Single-pass convenience wrapper over [`reconcile_quota_keys_from`].
///
/// Always starts at the beginning of the candidate set (`after_id = None`),
/// for a one-shot sweep such as a test. A periodic caller carries the cursor
/// of [`reconcile_quota_keys_from`] instead. Otherwise a large stuck prefix
/// can pin every call to the same rows (see [`CANDIDATE_SQL`]).
///
/// # Errors
///
/// Returns [`crate::error::HarvestError::Database`] on query failure, or
/// [`crate::error::HarvestError::ShardFenced`] if this process is pinned
/// to a superseded generation for `shard`.
#[cfg(feature = "db")]
pub async fn reconcile_quota_keys(
    conn: &mut AsyncPgConnection,
    batch_size: i64,
    shard: Option<crate::types::ShardId>,
) -> HarvestResult<ReconcileSummary> {
    reconcile_quota_keys_from(conn, batch_size, None, shard)
        .await
        .map(|(summary, _next_cursor)| summary)
}

/// Spawn a task that periodically backfills `quota_key` on pre-upgrade
/// non-terminal executions of one shard, until `cancel` fires.
///
/// `batch_size <= 0` disables the sweep: the task returns at once without
/// polling, as with `codec_rotation_batch_size = 0`.
///
/// Each tick passes `shard` to [`reconcile_quota_keys_from_with_codecs`] for
/// its cross-region DR fence (issue #954). `codecs` decodes the input of
/// each candidate (issue #1979).
#[cfg(feature = "db")]
#[must_use]
pub fn spawn_quota_key_reconciler_for_shard(
    pool: diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>,
    cancel: tokio_util::sync::CancellationToken,
    interval: std::time::Duration,
    batch_size: i64,
    shard: Option<crate::types::ShardId>,
    codecs: crate::payload_codec::PayloadCodecs,
) -> tokio::task::JoinHandle<()> {
    // Keep the worker dispatch binding for hints (issue #1431).
    crate::dispatch::spawn_bound(async move {
        if batch_size <= 0 {
            return;
        }
        // Carried across ticks, so a stuck prefix of the candidate set
        // cannot pin every tick to the same rows (see `CANDIDATE_SQL`).
        let mut cursor: Option<Uuid> = None;
        loop {
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(interval) => {}
            }
            // Issue #1823: a held shard skips the tick before it takes a connection.
            // It can be an unreachable standby, so a checkout could wait on it.
            if crate::replication::shard_writes_held(shard) {
                continue;
            }
            // Selected against `cancel` (issue #1426). A pool can have no
            // deadpool `Timeouts`, so `pool.get()` can park forever on an
            // exhausted pool. The first select guards only the sleep. Without
            // this one, the join in `shutdown_and_cleanup_monitors` could hang.
            let get_result = tokio::select! {
                () = cancel.cancelled() => break,
                result = pool.get() => result,
            };
            // The fence opens only after the checkout. A tick that waits for a
            // connection holds no barrier, so pool pressure cannot block a bump. The
            // tick runs under the barrier of this shard and each pinned shard
            // colocated with it. A fenced shard skips the tick, and a lost barrier
            // stops it.
            let Some(fence) = crate::replication::begin_shard_tick(&pool, shard).await else {
                continue;
            };
            match get_result {
                Ok(mut conn) => {
                    match crate::replication::run_fenced_pass(
                        &fence,
                        Box::pin(async {
                            // Issue #1823: the older connection joins the pass.
                            // A lost guard then ends its backend.
                            let _member =
                                crate::replication::join_fenced_pass(&pool, &mut conn).await;
                            reconcile_quota_keys_from_with_codecs(
                                &mut conn, batch_size, cursor, shard, &codecs,
                            )
                            .await
                        }),
                    )
                    .await
                    .and_then(|done| done)
                    {
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

    #[cfg(feature = "db")]
    #[test]
    fn a_run_only_llm_policy_is_not_reconciled() {
        use crate::quota::QuotaPolicy;

        assert!(!reconciles_key(None));
        assert!(reconciles_key(Some(QuotaPolicy::new("t"))));
        assert!(reconciles_key(Some(
            QuotaPolicy::new("t").with_max_tenant_llm_tokens(1)
        )));
        assert!(!reconciles_key(Some(
            QuotaPolicy::new("t").with_max_run_llm_tokens(1)
        )));
    }

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
        // As admission: `execution.rs` stamps `quota_key` from any declared
        // policy, whatever `has_any_cap()` says. Usage accounting needs the
        // key even when the policy enforces nothing yet.
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

    #[cfg(feature = "db")]
    #[test]
    fn candidate_sql_scopes_to_non_terminal_null_quota_key_rows() {
        assert!(CANDIDATE_SQL.contains("quota_key IS NULL"));
        assert!(CANDIDATE_SQL.contains("state IN ('RUNNING', 'PAUSED')"));
        assert!(!CANDIDATE_SQL.contains("SUSPENDED"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn candidate_sql_restricts_to_quota_governed_workflow_names() {
        assert!(CANDIDATE_SQL.contains("unnest($1::text[])"));
        assert!(CANDIDATE_SQL.contains("e.workflow_name = n.name"));
    }

    #[cfg(feature = "db")]
    #[test]
    fn candidate_query_accessor_matches_the_executed_sql() {
        assert_eq!(quota_reconcile_candidate_query(), CANDIDATE_SQL);
    }
}
