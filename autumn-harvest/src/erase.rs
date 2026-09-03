//! Targeted PII erasure for completed workflow executions (issue #495).
//!
//! ## Design
//!
//! This module implements **sanctioned in-place mutation exception #2** of
//! `harvest_events.event_data` rows. There are exactly two such writers: this
//! one and codec key re-encryption (`crate::codec_rotation`, issue #948,
//! exception #3); both are enumerated with their scope guarantees in the
//! "Engine Invariants" section of `CLAUDE.md`. (The heartbeat checkpoint this
//! comment used to name alongside them mutates `harvest_task_queue`, not the
//! event log — see that section.) Payload-bearing fields inside each event's
//! `data` object are
//! replaced with a tombstone marker while the append-only event log structure —
//! variant `type`, event IDs, timestamps, sequence — is left completely intact.
//!
//! Erasure always **wins** a race with the re-encryption sweep: that sweep
//! writes with a compare-and-swap on the row's previous bytes, so a tombstone
//! committed between its read and its write makes its update match zero rows
//! rather than resurrecting the ciphertext this module just destroyed.
//!
//! Erasure is **terminal-only**: the gate rejects any execution that is not
//! in a finished state (`COMPLETED`, `FAILED`, `CANCELLED`, `TIMED_OUT`,
//! `CONTINUED_AS_NEW`, `TERMINATED`). This protects replay determinism — a
//! resumable history must remain replayable.
//!
//! Erasure is **irreversible**. Once payload fields are tombstoned the
//! original values cannot be recovered unless the operator independently
//! maintains a backup.
//!
//! Erasure is **idempotent**: re-running against an already-erased execution
//! reports zero newly-scrubbed events without returning an error.
//!
//! ## Scope
//!
//! A single call to [`erase_workflow_payloads`] tombstones:
//! - All payload-bearing fields in `harvest_events.event_data` for the target
//!   execution (and, recursively, all terminal child executions on the same
//!   shard).
//! - The target execution row's own `input`, `output`, `memo`,
//!   `search_attrs`, and `context_headers` columns.
//! - All `harvest_signals.payload` rows associated with the execution.
//! - All `harvest_completion_deliveries.payload` rows associated with the
//!   execution (issue #605) — the frozen `CompletionEnvelope` JSON is
//!   tombstoned in place; delivery scheduling (`state`/`attempt`/
//!   `next_attempt_at`) is untouched, so a still-pending, in-flight, or
//!   later-redriven delivery posts the tombstone marker instead of the
//!   erased result/error.
//! - All `harvest_dead_letters.input` rows for the execution whose
//!   `task_type = "CALLBACK"` (issue #605) — a completion delivery that
//!   exhausted its retries writes a second, independent copy of the frozen
//!   envelope here; scoped to `CALLBACK` rows only, not a general expansion
//!   of erasure to every dead-lettered activity/task for the execution.
//!
//! Non-terminal child executions are skipped and reported in
//! [`EraseOutcome::skipped_children`]; they must be erased separately once
//! they reach a terminal state.

use serde_json::{Value, json};

/// The JSON key inserted when a payload field is tombstoned.
///
/// Downstream code (exporters, UI) can detect erased fields by checking for
/// this key rather than treating the field as absent.
pub const ERASURE_TOMBSTONE_KEY: &str = "_harvest_erased";

/// Payload-bearing field names inside an event's `data` object.
///
/// These are the same keys used by [`crate::payload_codec`] and
/// [`crate::history_export`] so all three subsystems agree on what is
/// considered a payload field.
const PAYLOAD_FIELDS: &[&str] = &[
    "input",
    "output",
    "payload",
    "details",
    "value",
    "last_completion_result",
];

/// Returns the canonical tombstone value: `{"_harvest_erased": true}`.
#[must_use]
pub fn erasure_tombstone() -> Value {
    json!({ ERASURE_TOMBSTONE_KEY: true })
}

/// Returns `true` if `value` is exactly the erasure tombstone
/// `{"_harvest_erased": true}` — the FIELD-AGNOSTIC predicate (issue #495).
///
/// [`erase_workflow_payloads`] writes this same canonical value into the
/// execution row's `input`, `memo`, and `search_attrs` columns (and into every
/// payload-bearing event field), so any consumer that must not PROPAGATE an
/// erased value can test it with this regardless of which column it came from.
/// [`execution_input_is_erased`] is the `input`-specific spelling used by the
/// O(1) erased-row check; prefer this one for any other column.
///
/// Note `context_headers` is `NULL`ed rather than tombstoned by the row scrub,
/// so it never needs this test.
#[must_use]
pub fn is_erasure_tombstone(value: &Value) -> bool {
    is_tombstone(value)
}

/// Returns `true` if `value` is already the erasure tombstone.
fn is_tombstone(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|m| m.len() == 1 && m.get(ERASURE_TOMBSTONE_KEY) == Some(&Value::Bool(true)))
}

/// Tombstone every payload-bearing field inside a serialised event value.
///
/// `event_value` must be the adjacently-tagged form stored in
/// `harvest_events.event_data`: `{"type": "...", "data": {...}}`.
///
/// Returns the number of **newly** tombstoned fields (0 on an already-erased
/// event, which makes the operation idempotent).
///
/// The `type` key and all structural fields outside of the known payload
/// allowlist are left untouched.
#[must_use]
pub fn tombstone_payload_fields(event_value: &mut Value) -> usize {
    let Some(data) = event_value.get_mut("data") else {
        return 0;
    };
    let Some(data_obj) = data.as_object_mut() else {
        return 0;
    };
    let tombstone = erasure_tombstone();
    let mut count = 0usize;
    for key in PAYLOAD_FIELDS {
        if let Some(field) = data_obj.get_mut(*key)
            && !is_tombstone(field)
        {
            *field = tombstone.clone();
            count += 1;
        }
    }
    count
}

/// The canonical set of recognised terminal execution states.
///
/// This is the single source of truth for terminal-state classification across
/// the engine: [`is_terminal_state`] is `TERMINAL_STATES.contains(&state)`, and
/// consumers that need the literal list for a SQL filter (e.g.
/// `execution::resolve_execution_id_by_workflow_id`, issue #805) reference this
/// constant directly rather than re-declaring the states, so the two can never
/// drift.
pub const TERMINAL_STATES: &[&str] = &[
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMED_OUT",
    "CONTINUED_AS_NEW",
    "TERMINATED",
    // Issue #964: the sealed source of a shard migration. Terminal-shaped in
    // exactly the sense this list means -- nothing more will ever happen to it
    // on THIS shard -- which is what lets an erasure reach the copy the
    // migration left behind. Without it the source's plaintext payloads would
    // be permanently unreachable: an erasure routed by ExecutionId follows the
    // forwarding pointer to the target, tombstones that, and reports success
    // while the source keeps every byte.
    //
    // Terminal for CLASSIFICATION is not the same as purgeable: the retention
    // janitor's candidate queries enumerate their own state list and do not
    // include `MIGRATED`, because hard-deleting a sealed row would destroy the
    // forwarding pointer every pre-migration id resolves through.
    "MIGRATED",
];

/// Returns `true` when `state` is one of the recognised terminal execution
/// states.
///
/// These are the states for which payload erasure is permitted. The set is
/// [`TERMINAL_STATES`], the single source of truth for terminal classification.
#[must_use]
pub fn is_terminal_state(state: &str) -> bool {
    TERMINAL_STATES.contains(&state)
}

/// Returns `true` if a workflow execution row's payload has been PII-erased
/// (issue #495), from an O(1) check of its already-loaded `input` column.
///
/// A PII-erased terminal history still replays *structurally* (event types,
/// order, and IDs are untouched), so a post-mortem query (issue #612) would
/// drive it to completion and then compute against `{"_harvest_erased": true}`
/// payloads — a subtly wrong answer. Callers serving queries on terminal
/// executions use this to reject such histories explicitly rather than return
/// misleading state.
///
/// This is authoritative and O(1): [`erase_workflow_payloads`] **always**
/// tombstones the execution row's own `input` column (it is the very first
/// column set in the row-scrub `UPDATE`), so testing `input` alone is
/// sufficient and far cheaper than re-serialising and tree-walking the entire
/// event history on every terminal query (the 10k-events-under-200ms budget).
///
/// `_harvest_erased` is a **reserved payload key**: a workflow whose real input
/// is literally `{"_harvest_erased": true}` would be indistinguishable from an
/// erased row here, so authors must not use that key as a top-level input shape.
#[must_use]
pub fn execution_input_is_erased(input: &Value) -> bool {
    is_tombstone(input)
}

// ── Outcome types ─────────────────────────────────────────────────────────────

/// A child execution skipped during cascade erasure.
///
/// Either it is not yet terminal, or it is under an active legal hold (issue
/// #747). The parent and other children still erase normally.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SkippedChild {
    /// Execution ID of the skipped child.
    pub execution_id: String,
    /// Current state of the skipped child.
    pub state: String,
    /// Why the child was skipped, when more specific than "not yet terminal" —
    /// e.g. an active legal hold (issue #747). `None` for the ordinary
    /// non-terminal skip.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A single failure encountered while erasing a child execution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EraseFailure {
    /// Execution ID for which the erasure attempt failed.
    pub execution_id: String,
    /// Human-readable description of the failure.
    pub reason: String,
}

/// Result of a single [`erase_workflow_payloads`] call.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EraseOutcome {
    /// The execution whose payloads were erased.
    pub execution_id: String,
    /// Number of event rows whose `event_data` was updated.
    pub events_scrubbed: usize,
    /// Total number of individual payload fields tombstoned across all events.
    pub fields_tombstoned: usize,
    /// Whether the execution row's own payload columns were scrubbed.
    pub execution_row_scrubbed: bool,
    /// Whether a matching `harvest_execution_summaries` row (issue #752) had
    /// its captured `result` and `search_attrs` tombstoned. `true` even when
    /// the original execution row was already retention-deleted and only the
    /// summary remained (a summary-only erase). The summary's `error` column is
    /// deliberately left intact, consistent with the #495 stance of keeping
    /// operational error text.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub summary_scrubbed: bool,
    /// Number of `harvest_signals` rows whose `payload` was tombstoned.
    pub signals_scrubbed: usize,
    /// Number of durable workflow log rows (issue #790) DELETED for this
    /// execution. Unlike every other surface here these are removed outright
    /// rather than tombstoned: a log line is a single free-form author string
    /// with no field structure to preserve, so a tombstone would carry no
    /// information a plain absence does not. Logs are observational and are
    /// never replayed, so deleting them cannot affect determinism.
    #[serde(default)]
    pub logs_deleted: usize,
    /// Number of `harvest_completion_deliveries` rows (issue #605) whose
    /// frozen `payload` envelope was tombstoned. The delivery row itself
    /// (state, retry schedule) is left untouched — a still-pending,
    /// in-flight, or redriven delivery now posts the tombstone marker
    /// instead of the workflow's erased result/error.
    pub completion_deliveries_scrubbed: usize,
    /// Number of `harvest_dead_letters` rows (`task_type = "CALLBACK"`,
    /// issue #605) whose `input` — a second, independent copy of the frozen
    /// `CompletionEnvelope` written when a callback delivery exhausts its
    /// retries — was tombstoned. Without this, erasing
    /// `harvest_completion_deliveries.payload` alone leaves the same PII
    /// sitting in the DLQ until retention or redrive.
    pub dead_letters_scrubbed: usize,
    /// Outcomes for terminal child executions that were recursively erased.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<Self>,
    /// Child executions that were skipped because they are not yet terminal.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped_children: Vec<SkippedChild>,
    /// Per-child failures (partial-failure reporting: the target itself
    /// succeeded but one or more children could not be erased).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failures: Vec<EraseFailure>,
    /// Erase outcomes for the **sealed source copies** a shard rebalance
    /// (issue #964) left behind on shards that previously hosted this run.
    ///
    /// A rebalance copies an execution to a new shard and seals — but does not
    /// delete — the original, which keeps its full event payloads until the
    /// source shard's own retention collects it. An erase that visited only the
    /// live residence would therefore report success while leaving a complete,
    /// readable copy of the subject's data on another database. Each entry here
    /// is the proof that one such copy was scrubbed too.
    ///
    /// Empty — and omitted from the JSON entirely — for every execution that has
    /// never been rebalanced, which is every execution on a single-shard
    /// deployment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prior_residences: Vec<ErasedResidence>,
    /// Shards on this execution's residence chain that have been **retired**
    /// (issue #964) — decommissioned, their pools removed from every node, and
    /// their ids forwarded to a successor — and so were not visited.
    ///
    /// This is reported rather than silently skipped because it is the one case
    /// where the erasure is complete only if the decommission was done properly:
    /// `docs/runbooks/shard-decommission.md` requires the retired shard's
    /// database to be destroyed (or its payloads erased) before its pool is
    /// dropped, and declaring the forward is the operator's assertion that it
    /// was. A merely *unreachable* residence is a different thing entirely and
    /// fails the whole call instead of appearing here.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retired_residences: Vec<i32>,
}

/// One previously-hosting shard's contribution to a cross-residence erase
/// (issue #964).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ErasedResidence {
    /// The shard whose sealed source copy was scrubbed.
    pub shard_id: i32,
    /// What the erase found and tombstoned there.
    pub outcome: EraseOutcome,
}

// ── DB-gated core function ────────────────────────────────────────────────────

#[cfg(feature = "db")]
mod db {
    use std::collections::HashSet;
    use std::future::Future;
    use std::pin::Pin;

    use chrono::{DateTime, Utc};
    use diesel::ExpressionMethods;
    use diesel::OptionalExtension;
    use diesel::QueryDsl;
    use diesel_async::AsyncConnection;
    use diesel_async::AsyncPgConnection;
    use diesel_async::RunQueryDsl;
    use uuid::Uuid;

    use crate::error::{HarvestError, HarvestResult, database_error};
    use crate::schema::{
        harvest_completion_deliveries, harvest_dead_letters, harvest_events,
        harvest_execution_summaries, harvest_signals, harvest_workflow_executions,
    };
    use crate::shard::ShardedDbPool;
    use crate::types::ExecutionId;

    use super::{
        EraseFailure, EraseOutcome, ErasedResidence, SkippedChild, erasure_tombstone,
        is_terminal_state, tombstone_payload_fields,
    };

    type EraseFuture<'a> = Pin<Box<dyn Future<Output = HarvestResult<EraseOutcome>> + Send + 'a>>;

    /// Erase the payload contents of a completed workflow execution and its
    /// terminal children on the same shard, within a single transaction.
    ///
    /// # Errors
    ///
    /// - [`HarvestError::NotFound`] when the execution does not exist (→ HTTP 404).
    /// - [`HarvestError::Config`] when the execution is not in a terminal state
    ///   (→ HTTP 409 via `conflict_from` in the API layer).
    /// - [`HarvestError::Database`] on any persistence failure.
    pub async fn erase_workflow_payloads(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        _reason: &str,
    ) -> HarvestResult<EraseOutcome> {
        Box::pin(
            conn.transaction::<EraseOutcome, HarvestError, _>(async |conn| {
                // A `visited` set guards the unified downward traversal against
                // diamonds and any pathological `parent_id` cycle across the two
                // child sources (`harvest_workflow_executions` and
                // `harvest_execution_summaries`).
                let mut visited: HashSet<Uuid> = HashSet::new();
                erase_top_level(conn, exec_id, &mut visited).await
            }),
        )
        .await
    }

    /// Top-level erase entry with gate-REJECT semantics (issue #495): a
    /// non-terminal execution → `Config` (409), a legal-held execution →
    /// `Config` (409). Falls back to a summary-only erase when the execution
    /// row is already retention-deleted (issue #752, AC6): a terminal execution
    /// may have been demoted into a `harvest_execution_summaries` row and its
    /// full row collected, so erasing PII must scrub the summary too and
    /// SUCCEED when only the summary (or a lingering child-summary subtree)
    /// remains. A truly-unknown id still returns `NotFound` (404).
    async fn erase_top_level(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        visited: &mut HashSet<Uuid>,
    ) -> HarvestResult<EraseOutcome> {
        visited.insert(exec_id.as_uuid());
        let now = Utc::now();
        match load_erase_gate_row(conn, exec_id).await {
            Ok((state, set_at, until, reason)) => {
                if !is_terminal_state(&state) {
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is not in a terminal state \
                         (current state: {state}); payload erasure is only permitted \
                         for terminal executions"
                    )));
                }
                if crate::retention::legal_hold_active(set_at, until, now) {
                    let reason = reason.as_deref().unwrap_or("no reason recorded");
                    return Err(HarvestError::Config(format!(
                        "workflow execution {exec_id} is under legal hold \
                         (reason: {reason}); payload erasure rejected until the hold \
                         is released"
                    )));
                }
                let mut outcome = scrub_execution_node(conn, exec_id, now, visited).await?;
                // Scrub the matching summary (if any) in the same tx. A live
                // execution row and a summary are mutually exclusive in the
                // steady state, but this is harmless and idempotent.
                outcome.summary_scrubbed = erase_execution_summary(conn, exec_id).await?;
                Ok(outcome)
            }
            Err(HarvestError::NotFound(_)) => {
                // No execution row: scrub a lingering summary (and any
                // summarized child subtree) and report success on that basis.
                // Nothing at all found → NotFound (404).
                erase_summary_only_node(conn, exec_id, now, visited)
                    .await?
                    .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
            }
            // A non-terminal execution (Config → 409) or a DB error propagates
            // unchanged.
            Err(e) => Err(e),
        }
    }

    /// Erase an execution's payloads at **every shard that still holds a copy
    /// of them** — the live residence and every sealed source a shard rebalance
    /// (issue #964) left behind.
    ///
    /// This is the entry point an erasure request must use on a sharded
    /// deployment. [`erase_workflow_payloads`] takes a connection, so it scrubs
    /// exactly one database; after a rebalance an execution's bytes exist in two
    /// (the live copy on the target, the sealed copy on the source, which stays
    /// readable until the source shard's own retention collects it). Scrubbing
    /// only the shard the id currently routes to would report a clean erasure
    /// while a complete copy of the subject's data sat on another database —
    /// exactly the outcome the erasure exists to prevent.
    ///
    /// **The live residence is erased first, and its result is the answer.**
    /// It is the only copy whose state can answer the gate questions: a
    /// non-terminal run must be refused (409) and a legal hold must be honoured
    /// *before* anything is destroyed anywhere. A sealed source always reads as
    /// terminal, so gating on it would let a live run be erased through its own
    /// stale shadow.
    ///
    /// Prior residences are then scrubbed in order and reported individually in
    /// [`EraseOutcome::prior_residences`]. A source copy that has already been
    /// collected yields `NotFound` there, which is success — nothing to scrub is
    /// not a gap. Every other failure propagates: an unscrubbed source copy is a
    /// compliance failure, not a partial one, so the caller must see it. The
    /// whole operation is idempotent, so a retry after such a failure is safe.
    ///
    /// On a single-shard deployment, and for any execution that has never been
    /// rebalanced, this is [`erase_workflow_payloads`] plus one pointer read.
    ///
    /// # Errors
    ///
    /// Everything [`erase_workflow_payloads`] returns, plus
    /// [`HarvestError::ShardUnavailable`] when a shard on the residence chain
    /// has no pool on this node — the erase cannot be shown to be complete, so
    /// it is not reported as complete.
    pub async fn erase_workflow_payloads_all_residences(
        pool: &ShardedDbPool,
        exec_id: ExecutionId,
        reason: &str,
    ) -> HarvestResult<EraseOutcome> {
        let chain = crate::shard_rebalance::residence_chain(pool, exec_id).await?;
        // `residence_chain` always yields at least the origin shard.
        let Some((live, priors)) = chain.split_last() else {
            return Err(HarvestError::Database(
                "residence chain resolved to no shard at all".to_string(),
            ));
        };

        // The live residence resolves tolerantly: a single-pool deployment
        // registers one pool under one shard id, and the run's row is in it
        // whatever its id's shard bits say. Prior residences below keep the
        // exact form -- a sealed source is one specific database, and falling
        // back to the default there would scrub the wrong copy.
        let mut conn = crate::shard_rebalance::conn_for_live_shard(pool, *live).await?;
        let mut outcome = erase_workflow_payloads(&mut conn, exec_id, reason).await?;
        drop(conn);

        for shard in priors {
            // A RETIRED shard is not an unreachable one. `with_shard_forwards`
            // refuses to declare a forward for a shard that is still readable,
            // so the declaration is the operator's assertion that the shard is
            // decommissioned and its database gone — which the decommission
            // runbook requires before the pool is dropped. Failing closed on it
            // would make every run that ever lived on a retired shard
            // permanently un-erasable, which is a worse answer than none. It is
            // reported rather than skipped silently, so the response never
            // implies a copy was scrubbed that this node could not see.
            if crate::shard::ShardedDbPool::shard_is_retired(*shard) {
                outcome.retired_residences.push(shard.as_i32());
                continue;
            }
            let mut conn = crate::shard_rebalance::conn_for_shard(pool, *shard).await?;
            match erase_workflow_payloads(&mut conn, exec_id, reason).await {
                Ok(prior) => outcome.prior_residences.push(ErasedResidence {
                    shard_id: shard.as_i32(),
                    outcome: prior,
                }),
                // The sealed copy can legitimately be gone already: retention on
                // the source shard collects it on its own schedule, and a
                // collected copy holds nothing left to erase.
                Err(HarvestError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(outcome)
    }

    /// Scrub the payload of a matching `harvest_execution_summaries` row
    /// (issue #752, AC6).
    ///
    /// Tombstones `result` and `search_attrs`, leaving `error` intact
    /// (consistent with the #495 stance of retaining operational error text).
    /// Returns `true` when a summary row existed and was scrubbed, `false` when
    /// none exists. Idempotent.
    ///
    /// # Errors
    ///
    /// [`HarvestError::Database`] on any persistence failure.
    pub async fn erase_execution_summary(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<bool> {
        let tombstone = erasure_tombstone();
        let updated = diesel::update(harvest_execution_summaries::table.find(exec_id.as_uuid()))
            .set((
                harvest_execution_summaries::result.eq(Some(&tombstone)),
                harvest_execution_summaries::search_attrs.eq(Some(&tombstone)),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;
        Ok(updated > 0)
    }

    /// A boxed future yielding an optional erase outcome (a summary-only node
    /// that turns out to reference nothing yields `None`).
    type OptEraseFuture<'a> =
        Pin<Box<dyn Future<Output = HarvestResult<Option<EraseOutcome>>> + Send + 'a>>;

    /// Erase one CHILD execution (row present, terminal, not held) and cascade.
    ///
    /// Scrubs the child's events/row/signals/deliveries/DLQ and its own summary
    /// (if any), then recurses into its children. Boxed to satisfy the async
    /// recursion requirement.
    fn erase_child_execution_node<'a>(
        conn: &'a mut AsyncPgConnection,
        exec_id: ExecutionId,
        now: DateTime<Utc>,
        visited: &'a mut HashSet<Uuid>,
    ) -> EraseFuture<'a> {
        Box::pin(async move {
            let mut outcome = scrub_execution_node(conn, exec_id, now, visited).await?;
            outcome.summary_scrubbed = erase_execution_summary(conn, exec_id).await?;
            Ok(outcome)
        })
    }

    /// Erase a summary-ONLY node (issue #752, AC6): the execution row is gone
    /// (retention-collected), only a `harvest_execution_summaries` row remains.
    ///
    /// Tombstones the summary's `result`/`search_attrs` and recurses into its
    /// children via the same two-source lookup (a summarized child may itself
    /// have summarized grandchildren, linked by `parent_id`). Returns `None`
    /// only when nothing at all is found for `exec_id` (no summary, no children,
    /// no skips, no failures) so a truly-unknown top-level id still 404s.
    fn erase_summary_only_node<'a>(
        conn: &'a mut AsyncPgConnection,
        exec_id: ExecutionId,
        now: DateTime<Utc>,
        visited: &'a mut HashSet<Uuid>,
    ) -> OptEraseFuture<'a> {
        Box::pin(async move {
            let summary_scrubbed = erase_execution_summary(conn, exec_id).await?;

            // Issue #958: on the opt-in partitioned layout the execution row's
            // deletion no longer cascades into `harvest_events`, so a
            // retention-collected (or summarized, #752) execution's full
            // PII-bearing `event_data` can still be sitting there as orphan
            // rows until the partition sweeper reclaims the whole cohort —
            // which a legal hold or long-running sibling can defer
            // indefinitely.
            //
            // Without this, a data-subject erasure request for such an
            // execution returned 200 with `events_scrubbed: 0` and "summary
            // scrubbed" while the plaintext survived in the event log, breaking
            // the #495 tombstoning contract exactly where #752 promised it
            // still held. `scrub_events` already works on orphans — it filters
            // on `workflow_exec_id` alone — so it only had to be called.
            let (events_scrubbed, fields_tombstoned) = scrub_events(conn, exec_id).await?;
            // The live execution row is gone, but retention deliberately leaves
            // non-DELIVERED completion deliveries and CALLBACK DLQ rows behind
            // (both keyed on `workflow_exec_id`) — each holds a frozen copy of
            // the workflow's own result/error PII. Scrub them here too (issue
            // #752 Codex P1), else a summarized run returns 200 while retryable
            // / redrivable PII survives.
            let (completion_deliveries_scrubbed, dead_letters_scrubbed) =
                scrub_callback_pii(conn, exec_id).await?;
            let (children, skipped_children, failures) =
                cascade_children(conn, exec_id, now, visited).await?;
            if !summary_scrubbed
                && completion_deliveries_scrubbed == 0
                && dead_letters_scrubbed == 0
                && children.is_empty()
                && skipped_children.is_empty()
                && failures.is_empty()
                && events_scrubbed == 0
            {
                return Ok(None);
            }
            Ok(Some(EraseOutcome {
                execution_id: exec_id.to_string(),
                events_scrubbed,
                fields_tombstoned,
                execution_row_scrubbed: false,
                summary_scrubbed,
                signals_scrubbed: 0,
                // A summary-only node has no live execution row, but its log
                // rows cascade-deleted with it, so there is nothing to remove.
                logs_deleted: 0,
                completion_deliveries_scrubbed,
                dead_letters_scrubbed,
                children,
                skipped_children,
                failures,
                prior_residences: Vec::new(),
                retired_residences: Vec::new(),
            }))
        })
    }

    /// Scrub event rows for one execution; return `(events_scrubbed, fields_tombstoned)`.
    async fn scrub_events(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<(usize, usize)> {
        let raw_events = harvest_events::table
            .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid()))
            .select((harvest_events::id, harvest_events::event_data))
            .load::<(i64, serde_json::Value)>(conn)
            .await
            .map_err(database_error)?;

        let mut events_scrubbed = 0usize;
        let mut fields_tombstoned = 0usize;
        for (row_id, mut event_data) in raw_events {
            let count = tombstone_payload_fields(&mut event_data);
            if count > 0 {
                // Keyed on `workflow_exec_id` as well as the row id.
                //
                // This does NOT prune partitions — the partition key is
                // `cohort`, the row's append instant, so an execution's history
                // genuinely spans partitions and no predicate on
                // `workflow_exec_id` can narrow the set. What it does buy is a
                // usable index: on the partitioned layout a bare `id` predicate
                // has no index to use at all (the primary key is
                // `(id, cohort)`), so every partition would be sequentially
                // scanned per row; `(workflow_exec_id, id)` matches
                // `idx_harvest_events_history_page` in each of them.
                //
                // The predicate is strictly narrower than `find(row_id)` — the
                // rows were just selected FOR this execution, and `id` is
                // globally unique from a single sequence — so the unpartitioned
                // layout behaves identically.
                diesel::update(
                    harvest_events::table
                        .filter(harvest_events::id.eq(row_id))
                        .filter(harvest_events::workflow_exec_id.eq(exec_id.as_uuid())),
                )
                .set(harvest_events::event_data.eq(event_data))
                .execute(conn)
                .await
                .map_err(database_error)?;
                events_scrubbed += 1;
                fields_tombstoned += count;
            }
        }
        Ok((events_scrubbed, fields_tombstoned))
    }

    /// Collect child execution ids from BOTH the live-execution table and the
    /// summary table (issue #752), deduped and in a stable order.
    ///
    /// A terminal child is independently retention-eligible, so by the time a
    /// parent is erased a child may exist as a live row, as a summary-only row,
    /// or (transiently) neither. Unioning both sources is what lets the erase
    /// cascade reach a child that was already demoted into a summary and had its
    /// own execution row collected.
    async fn collect_child_ids(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<Vec<Uuid>> {
        let exec_children = harvest_workflow_executions::table
            .filter(harvest_workflow_executions::parent_id.eq(Some(exec_id.as_uuid())))
            .select(harvest_workflow_executions::id)
            .load::<Uuid>(conn)
            .await
            .map_err(database_error)?;
        let summary_children = harvest_execution_summaries::table
            .filter(harvest_execution_summaries::parent_id.eq(Some(exec_id.as_uuid())))
            .select(harvest_execution_summaries::execution_id)
            .load::<Uuid>(conn)
            .await
            .map_err(database_error)?;

        let mut seen: HashSet<Uuid> = HashSet::new();
        let mut ids = Vec::with_capacity(exec_children.len() + summary_children.len());
        for id in exec_children.into_iter().chain(summary_children) {
            if seen.insert(id) {
                ids.push(id);
            }
        }
        Ok(ids)
    }

    /// Cascade erasure to child executions AND child summaries; return
    /// (children, skipped, failures).
    ///
    /// For each child id (from either source, deduped): a live terminal,
    /// non-held execution row is scrubbed and recursed; a non-terminal or held
    /// row is skipped; a child with no execution row is a summary-only node
    /// whose summary (and any summarized grandchildren) is scrubbed recursively.
    async fn cascade_children(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        now: DateTime<Utc>,
        visited: &mut HashSet<Uuid>,
    ) -> HarvestResult<(Vec<EraseOutcome>, Vec<SkippedChild>, Vec<EraseFailure>)> {
        let child_ids = collect_child_ids(conn, exec_id).await?;

        let mut children = Vec::new();
        let mut skipped_children = Vec::new();
        let mut failures = Vec::new();
        for child_uuid in child_ids {
            // Guard against diamonds / pathological parent_id cycles.
            if !visited.insert(child_uuid) {
                continue;
            }
            let child_exec_id = ExecutionId::from_uuid(child_uuid);
            // Re-read state + hold under a FOR UPDATE row lock (issue #747 MINOR
            // 2a): the parent's erase tx only locks the parent, so a hold placed
            // directly on this child after the unlocked list read above must
            // still be caught. Locking here serializes against `set_legal_hold`.
            match load_erase_gate_row(conn, child_exec_id).await {
                Ok((child_state, set_at, until, reason)) => {
                    if !is_terminal_state(&child_state) {
                        skipped_children.push(SkippedChild {
                            execution_id: child_exec_id.to_string(),
                            state: child_state,
                            reason: None,
                        });
                        continue;
                    }
                    // A held child is a deliberate SKIP, not a failure (issue
                    // #747 MINOR 2b): its events are left intact while the parent
                    // and other children erase normally.
                    if crate::retention::legal_hold_active(set_at, until, now) {
                        let hold_reason = reason.as_deref().unwrap_or("no reason recorded");
                        skipped_children.push(SkippedChild {
                            execution_id: child_exec_id.to_string(),
                            state: child_state,
                            reason: Some(format!("legal hold ({hold_reason})")),
                        });
                        continue;
                    }
                    match erase_child_execution_node(conn, child_exec_id, now, visited).await {
                        Ok(outcome) => children.push(outcome),
                        Err(e) => failures.push(EraseFailure {
                            execution_id: child_exec_id.to_string(),
                            reason: e.to_string(),
                        }),
                    }
                }
                // No execution row: a summary-only child (issue #752, AC6). Its
                // execution row was already retention-collected; scrub its
                // summary and any summarized grandchildren.
                Err(HarvestError::NotFound(_)) => {
                    match erase_summary_only_node(conn, child_exec_id, now, visited).await {
                        // `None` = the summary vanished between `collect_child_ids`
                        // and here (a concurrent GC race): nothing to do.
                        Ok(None) => {}
                        Ok(Some(outcome)) => children.push(outcome),
                        Err(e) => failures.push(EraseFailure {
                            execution_id: child_exec_id.to_string(),
                            reason: e.to_string(),
                        }),
                    }
                }
                Err(e) => {
                    failures.push(EraseFailure {
                        execution_id: child_exec_id.to_string(),
                        reason: e.to_string(),
                    });
                }
            }
        }
        Ok((children, skipped_children, failures))
    }

    /// The state + legal-hold columns read under the erase gate.
    type EraseGateRow = (
        String,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
    );

    /// Load the execution's `state` and legal-hold columns for the erase gates,
    /// acquiring a `FOR UPDATE` row lock.
    ///
    /// The lock is taken for BOTH the top-level execution and each cascaded
    /// child (issue #747 MINOR 2a): the outer transaction only locks the parent
    /// row, so a hold placed directly on a child between an unlocked read and
    /// its scrub could otherwise be missed. Locking the row here serializes the
    /// gate against `set_legal_hold`. A re-entrant lock on a row already locked
    /// by the same transaction is a no-op in Postgres.
    async fn load_erase_gate_row(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<EraseGateRow> {
        harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .select((
                harvest_workflow_executions::state,
                harvest_workflow_executions::legal_hold_set_at,
                harvest_workflow_executions::legal_hold_until,
                harvest_workflow_executions::legal_hold_reason,
            ))
            .for_update()
            .first::<EraseGateRow>(conn)
            .await
            .optional()
            .map_err(database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))
    }

    /// Scrub the frozen completion-callback PII for one execution, keyed only
    /// on `workflow_exec_id`, and return `(deliveries_scrubbed, dead_letters_scrubbed)`.
    ///
    /// Shared by the execution-exists path (`scrub_execution_node`) and the
    /// summary-only path (`erase_summary_only_node`) so a summarized execution
    /// whose live row is already gone still has its lingering callback PII
    /// erased (issue #752 Codex P1). Two independent copies of the workflow's
    /// own `result`/`error` can survive after retention:
    ///
    /// * `harvest_completion_deliveries.payload` — the frozen
    ///   `CompletionEnvelope` (issue #605 / PR #921 review). Retention leaves
    ///   non-`DELIVERED` deliveries in place, and a still-`PENDING`/`INFLIGHT`/
    ///   `FAILED` delivery could still be `POST`ed to the external receiver (or
    ///   redriven by an operator) after the erase. Only `payload` is tombstoned;
    ///   `state`/`attempt`/`next_attempt_at`/`last_status` are untouched so
    ///   delivery scheduling is unaffected — a pending delivery simply posts the
    ///   tombstone marker instead of the real data.
    /// * `harvest_dead_letters.input` where `task_type = 'CALLBACK'` — a second,
    ///   independent copy the callback-delivery scanner writes on retry
    ///   exhaustion (issue #921 review). Scoped to `CALLBACK` only: this is
    ///   specifically issue #605's callback copy, not a general expansion of
    ///   erasure to every dead-lettered activity/task (out of scope here).
    async fn scrub_callback_pii(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
    ) -> HarvestResult<(usize, usize)> {
        let tombstone = erasure_tombstone();
        let completion_deliveries_scrubbed = diesel::update(
            harvest_completion_deliveries::table
                .filter(harvest_completion_deliveries::workflow_exec_id.eq(exec_id.as_uuid())),
        )
        .set(harvest_completion_deliveries::payload.eq(&tombstone))
        .execute(conn)
        .await
        .map_err(database_error)?;

        let dead_letters_scrubbed = diesel::update(
            harvest_dead_letters::table
                .filter(harvest_dead_letters::workflow_exec_id.eq(Some(exec_id.as_uuid())))
                .filter(harvest_dead_letters::task_type.eq("CALLBACK")),
        )
        .set(harvest_dead_letters::input.eq(&tombstone))
        .execute(conn)
        .await
        .map_err(database_error)?;

        Ok((completion_deliveries_scrubbed, dead_letters_scrubbed))
    }

    /// Scrub a single execution node's own data (events, row columns, signals,
    /// completion deliveries, CALLBACK dead letters) and cascade to its
    /// children. Does NOT check the terminal/hold gate — the caller
    /// (`erase_top_level` for the top node, `cascade_children` for a child) has
    /// already gated. `summary_scrubbed` is left `false`; the caller sets it.
    async fn scrub_execution_node(
        conn: &mut AsyncPgConnection,
        exec_id: ExecutionId,
        now: DateTime<Utc>,
        visited: &mut HashSet<Uuid>,
    ) -> HarvestResult<EraseOutcome> {
        // ── Scrub events, execution row, signals ──────────────────────────────
        let (events_scrubbed, fields_tombstoned) = scrub_events(conn, exec_id).await?;
        let tombstone = erasure_tombstone();
        diesel::update(harvest_workflow_executions::table.find(exec_id.as_uuid()))
            .set((
                harvest_workflow_executions::input.eq(&tombstone),
                harvest_workflow_executions::output.eq(Some(&tombstone)),
                harvest_workflow_executions::memo.eq(Some(&tombstone)),
                harvest_workflow_executions::search_attrs.eq(Some(&tombstone)),
                harvest_workflow_executions::context_headers.eq(None::<serde_json::Value>),
            ))
            .execute(conn)
            .await
            .map_err(database_error)?;
        let signals_scrubbed = diesel::update(
            harvest_signals::table.filter(harvest_signals::workflow_exec_id.eq(exec_id.as_uuid())),
        )
        .set(harvest_signals::payload.eq(&tombstone))
        .execute(conn)
        .await
        .map_err(database_error)?;

        // Durable per-execution workflow logs (issue #790). Author-emitted log
        // messages are free-form text that can carry personal data, so a
        // payload erasure must remove them too.
        let logs_deleted = crate::store::delete_workflow_logs(conn, exec_id).await?;

        // Completion-callback PII (deliveries + CALLBACK DLQ), keyed only on
        // `workflow_exec_id` so it applies whether or not a live execution row
        // exists (issue #752 Codex P1 — the summary-only path must scrub these
        // too, since retention deliberately leaves non-DELIVERED deliveries and
        // CALLBACK DLQ rows behind).
        let (completion_deliveries_scrubbed, dead_letters_scrubbed) =
            scrub_callback_pii(conn, exec_id).await?;

        // ── Cascade to child executions AND child summaries ───────────────────
        let (children, skipped_children, failures) =
            cascade_children(conn, exec_id, now, visited).await?;

        Ok(EraseOutcome {
            execution_id: exec_id.to_string(),
            events_scrubbed,
            fields_tombstoned,
            execution_row_scrubbed: true,
            // Set by the caller (`erase_workflow_payloads`) after this returns,
            // which scrubs the matching summary in the same transaction.
            summary_scrubbed: false,
            signals_scrubbed,
            logs_deleted,
            completion_deliveries_scrubbed,
            dead_letters_scrubbed,
            children,
            skipped_children,
            failures,
            prior_residences: Vec::new(),
            retired_residences: Vec::new(),
        })
    }
}

#[cfg(feature = "db")]
pub use db::{
    erase_execution_summary, erase_workflow_payloads, erase_workflow_payloads_all_residences,
};

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    // ── tombstone_payload_fields ──────────────────────────────────────────────

    #[test]
    fn tombstones_input_field() {
        let mut event = json!({
            "type": "WorkflowStarted",
            "data": { "input": { "user_id": 42, "email": "alice@example.com" } }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 1);
        assert_eq!(event["data"]["input"], erasure_tombstone());
        // structural fields untouched
        assert_eq!(event["type"], "WorkflowStarted");
    }

    #[test]
    fn tombstones_output_field() {
        let mut event = json!({
            "type": "WorkflowCompleted",
            "data": { "output": { "result": "ok" } }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 1);
        assert_eq!(event["data"]["output"], erasure_tombstone());
    }

    #[test]
    fn tombstones_signal_payload() {
        let mut event = json!({
            "type": "SignalReceived",
            "data": { "signal_name": "approved", "payload": { "approver": "bob" } }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 1);
        assert_eq!(event["data"]["payload"], erasure_tombstone());
        // non-payload fields left intact
        assert_eq!(event["data"]["signal_name"], "approved");
    }

    #[test]
    fn tombstones_all_payload_fields_present() {
        let mut event = json!({
            "type": "ActivityScheduled",
            "data": {
                "input": "secret",
                "output": "result",
                "payload": "sig",
                "details": {"key": "val"},
                "value": 99,
                "last_completion_result": "prev"
            }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 6);
        for key in PAYLOAD_FIELDS {
            assert_eq!(
                event["data"][key],
                erasure_tombstone(),
                "field '{key}' not tombstoned"
            );
        }
    }

    #[test]
    fn idempotent_on_already_erased_event() {
        let mut event = json!({
            "type": "WorkflowStarted",
            "data": { "input": { "_harvest_erased": true } }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(
            count, 0,
            "re-running on an already-erased event must be idempotent"
        );
    }

    #[test]
    fn leaves_events_without_payload_fields_untouched() {
        let mut event = json!({
            "type": "TimerStarted",
            "data": { "timer_id": "t-1", "fires_at": "2026-06-01T00:00:00Z" }
        });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 0);
        // The event is entirely unchanged
        assert_eq!(event["data"]["timer_id"], "t-1");
    }

    #[test]
    fn returns_zero_for_event_without_data_key() {
        let mut event = json!({ "type": "WorkflowStarted" });
        let count = tombstone_payload_fields(&mut event);
        assert_eq!(count, 0);
    }

    #[test]
    fn tombstone_constant_key_is_correct() {
        assert_eq!(ERASURE_TOMBSTONE_KEY, "_harvest_erased");
    }

    #[test]
    fn erasure_tombstone_shape() {
        let t = erasure_tombstone();
        assert!(t.is_object());
        assert_eq!(t[ERASURE_TOMBSTONE_KEY], true);
        assert_eq!(t.as_object().unwrap().len(), 1);
    }

    // ── is_terminal_state ─────────────────────────────────────────────────────

    #[test]
    fn terminal_states_accepted() {
        for state in &[
            "COMPLETED",
            "FAILED",
            "CANCELLED",
            "TIMED_OUT",
            "CONTINUED_AS_NEW",
            "TERMINATED",
        ] {
            assert!(
                is_terminal_state(state),
                "expected '{state}' to be terminal"
            );
        }
    }

    #[test]
    fn non_terminal_states_rejected() {
        for state in &["RUNNING", "PAUSED", "SUSPENDED", "PENDING", ""] {
            assert!(
                !is_terminal_state(state),
                "expected '{state}' to be non-terminal"
            );
        }
    }

    // ── is_tombstone ──────────────────────────────────────────────────────────

    #[test]
    fn detects_tombstone_value() {
        assert!(is_tombstone(&erasure_tombstone()));
    }

    #[test]
    fn does_not_treat_other_objects_as_tombstones() {
        assert!(!is_tombstone(&json!({ "key": "value" })));
        assert!(!is_tombstone(&json!({ "_harvest_erased": false })));
        assert!(!is_tombstone(
            &json!({ "_harvest_erased": true, "extra": 1 })
        ));
        assert!(!is_tombstone(&json!("string")));
        assert!(!is_tombstone(&json!(null)));
    }

    // ── execution_input_is_erased (issue #612 O(1) row check) ─────────────────

    #[test]
    fn detects_erased_execution_row_input() {
        // Production tombstones the row's `input` column to exactly this shape.
        assert!(execution_input_is_erased(&erasure_tombstone()));
    }

    #[test]
    fn does_not_flag_a_clean_execution_row_input() {
        assert!(!execution_input_is_erased(
            &json!({ "user_id": 42, "email": "alice@example.com" })
        ));
        assert!(!execution_input_is_erased(&json!(null)));
        assert!(!execution_input_is_erased(&json!("plain string input")));
        // A nested-but-not-top-level tombstone is NOT an erased row: the row
        // check keys off the top-level column value only.
        assert!(!execution_input_is_erased(
            &json!({ "nested": { "_harvest_erased": true } })
        ));
    }

    // ── EraseOutcome serde ────────────────────────────────────────────────────

    #[test]
    fn erase_outcome_serialises_without_optional_vecs() {
        let outcome = EraseOutcome {
            execution_id: "exec-1".into(),
            events_scrubbed: 5,
            fields_tombstoned: 7,
            execution_row_scrubbed: true,
            summary_scrubbed: false,
            signals_scrubbed: 2,
            logs_deleted: 4,
            completion_deliveries_scrubbed: 3,
            dead_letters_scrubbed: 1,
            children: vec![],
            skipped_children: vec![],
            failures: vec![],
            prior_residences: vec![],
            retired_residences: vec![],
        };
        let v = serde_json::to_value(&outcome).unwrap();
        // empty vecs are omitted
        assert!(v.get("children").is_none());
        assert!(v.get("skipped_children").is_none());
        assert!(v.get("failures").is_none());
        assert_eq!(v["events_scrubbed"], 5);
        assert_eq!(v["execution_row_scrubbed"], true);
        // A false summary_scrubbed is omitted (issue #752).
        assert!(v.get("summary_scrubbed").is_none());
        // Durable workflow logs deleted by the erase (issue #790).
        assert_eq!(v["logs_deleted"], 4);
    }

    // ── summary scrub value (issue #752) ──────────────────────────────────────

    #[test]
    fn summary_scrub_uses_the_shared_erasure_tombstone() {
        // The summary `result`/`search_attrs` are tombstoned with the SAME
        // canonical tombstone as the execution-row erase, so a downstream
        // reader detects an erased summary field identically.
        let t = erasure_tombstone();
        assert!(execution_input_is_erased(&t));
    }

    #[test]
    fn summary_scrubbed_true_serialises() {
        let outcome = EraseOutcome {
            execution_id: "exec-2".into(),
            events_scrubbed: 0,
            fields_tombstoned: 0,
            execution_row_scrubbed: false,
            summary_scrubbed: true,
            signals_scrubbed: 0,
            logs_deleted: 0,
            completion_deliveries_scrubbed: 0,
            dead_letters_scrubbed: 0,
            children: vec![],
            skipped_children: vec![],
            failures: vec![],
            prior_residences: vec![],
            retired_residences: vec![],
        };
        let v = serde_json::to_value(&outcome).unwrap();
        assert_eq!(v["summary_scrubbed"], true);
        assert_eq!(v["execution_row_scrubbed"], false);
    }
}
