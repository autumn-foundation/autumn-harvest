//! Workflow signal delivery and management.
//!
//! Signals provide a way to send asynchronous events or payloads into a running workflow.
//! This module handles the durable enqueuing of signals into the database, loading pending
//! signals for a workflow, and marking them as consumed once processed by the workflow context.
#[cfg(feature = "db")]
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
#[cfg(feature = "db")]
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult};
#[cfg(feature = "db")]
use crate::models::{HarvestSignal, NewHarvestSignal};
#[cfg(feature = "db")]
use crate::telemetry::{ATTR_EXECUTION_ID, ATTR_WORKFLOW_ID};
use crate::types::ExecutionId;

/// Queue a workflow signal for durable delivery and wake the parked workflow.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist,
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal, and
/// [`HarvestError::Database`](crate::error::HarvestError::Database) if the
/// insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
) -> HarvestResult<()> {
    // With no key the partial unique index excludes the NULL row, so every
    // insert succeeds — the legacy at-least-once contract. Bool discarded.
    send_signal_idempotent(conn, exec_id, signal_name, payload, None)
        .await
        .map(|_delivered| ())
}

/// Queue a workflow signal, deduplicating on `idempotency_key` when supplied.
///
/// Returns `Ok(true)` when a row was freshly queued (the workflow was woken)
/// and `Ok(false)` when the key collided with an already-staged signal. A
/// `None` key always inserts, so the return is always `Ok(true)`. Dedupe scope
/// is shard-local, keyed on `(workflow_exec_id, idempotency_key)`.
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist,
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal, and
/// [`HarvestError::Database`](crate::error::HarvestError::Database) if the
/// insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal_idempotent(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<bool> {
    use crate::schema::harvest_signals;
    use crate::schema::harvest_workflow_executions;

    // An empty key is not in the partial index's NULL exclusion, so it would
    // collide across unrelated signals — treat it as no key (at-least-once).
    let idempotency_key = idempotency_key.filter(|k| !k.is_empty());

    Box::pin(conn.transaction::<bool, HarvestError, _>(async |conn| {
        let execution = harvest_workflow_executions::table
            .find(exec_id.as_uuid())
            .for_update()
            .select(crate::models::WorkflowExecution::as_select())
            .first(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?
            .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

        let row = NewHarvestSignal {
            workflow_exec_id: exec_id.as_uuid(),
            signal_name,
            payload,
            idempotency_key,
        };

        // Attempt the insert before validating state so a keyed retry that
        // already landed dedupes to a no-op even after the workflow has gone
        // terminal. `on_conflict_do_nothing()` (no explicit target) lets
        // Postgres arbitrate against the partial unique index
        // `uq_harvest_signals_idem`; a NULL key is excluded from the index,
        // so the insert always succeeds (rows-affected = 1).
        let inserted = diesel::insert_into(harvest_signals::table)
            .values(&row)
            .on_conflict_do_nothing()
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

        if inserted == 0 {
            // Idempotency-key collision: an equivalent signal already landed
            // once. Idempotent success regardless of current state — do not
            // re-wake (the original insert already did).
            return Ok(false);
        }

        // Fresh row: the execution must be able to accept it. Returning Err
        // here rolls back the transaction, undoing the insert above.
        match execution.state.as_str() {
            // PAUSED is a non-terminal active state: a paused workflow
            // waiting on a signal must still accept (buffer) it so it is
            // delivered on resume. The wake below re-pends the task, which
            // the claim gate defers until the execution is RUNNING.
            "RUNNING" | "PAUSED" => {}
            "CANCELLED" => {
                return Err(HarvestError::Cancelled(execution.error.unwrap_or_else(
                    || format!("workflow execution {exec_id} is cancelled"),
                )));
            }
            state => {
                return Err(HarvestError::Config(format!(
                    "workflow execution {exec_id} is terminal ({state})"
                )));
            }
        }

        // ADR-0001 §2.5: harvest.signal.send — PRODUCER, emitted only for an
        // accepted signal. in_scope is synchronous so EnteredSpan (!Send) is
        // dropped before any await.
        tracing::info_span!(
            "harvest.signal.send",
            "otel.kind" = "producer",
            { ATTR_WORKFLOW_ID } = execution.workflow_name.as_str(),
            { ATTR_EXECUTION_ID } = %exec_id,
            signal.name = %signal_name,
        )
        .in_scope(|| {});

        crate::queue::wake_workflow_task(conn, exec_id).await?;
        Ok(true)
    }))
    .await
}

/// Outcome of resolving a `(workflow_name, workflow_id)`-targeted signal
/// request to a concrete execution and attempting delivery (issue #751).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByIdSignalOutcome {
    /// The signal was queued for the resolved (current) run — freshly
    /// inserted or an idempotency-key replay of an existing one; either way
    /// the target has (or already had) this signal queued for delivery.
    Delivered,
    /// No run has ever existed for this `(workflow_name, workflow_id)`.
    /// Callers apply the same grace-window policy used for an unrecognized
    /// `ExecutionId` before reporting a definitive `target_unknown` failure.
    NoRunFound,
    /// The resolved run — whichever run was "current" at resolution time —
    /// is already terminal. Unlike cancellation, a signal's goal is never
    /// already met by a terminal target: the payload cannot be delivered, so
    /// this is a genuine failure (`not_running`), never success.
    NotRunning,
    /// The resolved run raced to `CONTINUED_AS_NEW` between resolution and
    /// the delivery attempt: a live successor now exists under the same
    /// business key. Not success, not failure — the caller should leave this
    /// attempt unresolved so a later attempt (immediate inline retry or the
    /// next outbox tick) re-resolves and finds the live successor.
    RacedToSuccessor,
}

/// Resolve a `(workflow_name, workflow_id)` target to its current run and
/// deliver a signal to it, closing the continue-as-new race by construction
/// (issue #751, AC2/AC3/AC4).
///
/// See [`crate::execution::resolve_and_cancel_by_workflow_id`] for the full
/// proof that a **direct** resolution via
/// [`crate::execution::resolve_execution_id_by_workflow_id`] can never
/// itself return `state == "CONTINUED_AS_NEW"` — the same argument applies
/// unchanged here, since both functions share the same resolve step. The one
/// race that requires handling is the tiny window between this function's
/// own resolve step and its delivery attempt: if the resolved run
/// continues-as-new in that window,
/// [`crate::execution::is_continued_as_new_race`] recognises it and this
/// function reports [`ByIdSignalOutcome::RacedToSuccessor`] rather than
/// misreporting either delivery or failure.
///
/// Unlike cancellation, a signal to an already-terminal resolved run is a
/// genuine failure ([`ByIdSignalOutcome::NotRunning`]) — the payload was not,
/// and cannot be, delivered — matching the existing `ExecutionId`-targeted
/// signal contract's "no signal silently dropped" invariant (issue #244).
///
/// # Keyed retries (issue #751 code review, PR #1145)
///
/// This function deliberately does **not** early-return `NotRunning` for a
/// terminal resolved run before attempting delivery. A keyed retry (the
/// caller-side outbox retrying a request whose target-shard insert already
/// committed, but whose caller-shard transaction rolled back before it could
/// record the delivery outcome) must dedupe to `Delivered` even after the
/// target has since gone terminal — an early terminal check would deny
/// [`send_signal_idempotent`]'s own "insert before validate" dedup (issue
/// #521) the chance to ever observe the existing key collision, misreporting
/// a request that was, in fact, already fulfilled.
///
/// A second, distinct race compounds this across a continue-as-new rotation:
/// [`send_signal_idempotent`]'s dedup is scoped to
/// `(workflow_exec_id, idempotency_key)`, so a key already used against a
/// predecessor's `exec_id` does not collide against a successor's — a naive
/// retry that re-resolves to the (now different) current run would insert a
/// *second*, duplicate signal for one logical request. This function closes
/// that gap with a `(workflow_name, workflow_id, idempotency_key)`-scoped
/// pre-check (reusing [`crate::execution::lookup_idempotent_signal_dedupe`],
/// the same lookup `signal_with_start` already uses for exactly this
/// purpose, issue #244) before ever attempting a fresh insert against the
/// freshly-resolved run.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] for persistence failures. Every other
/// outcome (no run found, not running, raced to a successor, or delivered)
/// is reported as `Ok`.
#[cfg(feature = "db")]
pub async fn resolve_and_signal_by_workflow_id(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<ByIdSignalOutcome> {
    let Some(run) =
        crate::execution::resolve_execution_id_by_workflow_id(conn, workflow_name, workflow_id)
            .await?
    else {
        return Ok(ByIdSignalOutcome::NoRunFound);
    };

    // Cross-execution dedup: a keyed request already delivered to ANY
    // execution in this (workflow_name, workflow_id) chain — including a
    // predecessor sealed by a continue-as-new that raced ahead of this
    // retry — is reported as already-delivered without a fresh insert.
    if let Some(key) = idempotency_key.filter(|k| !k.is_empty())
        && crate::execution::lookup_idempotent_signal_dedupe(conn, workflow_name, workflow_id, key)
            .await?
            .is_some()
    {
        return Ok(ByIdSignalOutcome::Delivered);
    }

    match send_signal_idempotent(conn, run.exec_id, signal_name, payload, idempotency_key).await {
        Ok(_delivered_or_deduped) => Ok(ByIdSignalOutcome::Delivered),
        Err(HarvestError::NotFound(_)) => {
            // Vanishingly unlikely (the row existed a moment ago under this
            // same connection) but not impossible under a concurrent
            // retention sweep; treat identically to "never existed".
            Ok(ByIdSignalOutcome::NoRunFound)
        }
        Err(ref e) if crate::execution::is_continued_as_new_race(e) => {
            Ok(ByIdSignalOutcome::RacedToSuccessor)
        }
        Err(HarvestError::Config(_) | HarvestError::Cancelled(_)) => {
            // A genuinely FRESH insert (the dedup pre-check above found no
            // prior delivery for this key) against a target that is
            // terminal — discovered either because `run` above already
            // resolved to a terminal state (no idempotency key was
            // supplied, or this key was never used before) or via the race
            // window between our resolve and the signal attempt's own lock.
            Ok(ByIdSignalOutcome::NotRunning)
        }
        Err(e) => Err(e),
    }
}

/// Read-only probe: does a queued signal row already exist for
/// `(workflow_exec_id, idempotency_key)`?
///
/// Used by the standalone signal route's committed-replay dedup short-circuit
/// (issues #521 / #610): a keyed retry whose row already landed must return the
/// documented `signal_delivered: false` no-op *before* any payload-schema
/// (#610) or payload-cap (#252) validation that may have been published or
/// tightened since the original delivery — otherwise a retry of an
/// already-accepted signal could be rejected `400` instead of the documented
/// `202`, regressing the exactly-once contract (including the "retry after the
/// run went terminal" case). Side-effect free (no insert, no wake).
///
/// The authoritative dedup remains [`send_signal_idempotent`]'s insert-time
/// `ON CONFLICT`; this is only a fast path for already-committed rows, so a
/// concurrent first-delivery race (neither request sees a row yet) still falls
/// through to validation + the `ON CONFLICT` insert. An empty key is excluded
/// from the partial unique index (never dedupes), so it reports `false` and the
/// caller falls through to the normal path — mirroring
/// [`send_signal_idempotent`]'s empty-key normalization.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the query fails.
#[cfg(feature = "db")]
pub async fn signal_idempotency_key_exists(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    idempotency_key: &str,
) -> HarvestResult<bool> {
    use crate::schema::harvest_signals::dsl;

    if idempotency_key.is_empty() {
        return Ok(false);
    }

    diesel::select(diesel::dsl::exists(
        dsl::harvest_signals
            .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(dsl::idempotency_key.eq(idempotency_key)),
    ))
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Load all unconsumed queued signals for an execution, ordered by receive time.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the query fails.
#[cfg(feature = "db")]
pub async fn load_pending_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<Vec<HarvestSignal>> {
    use crate::schema::harvest_signals::dsl;

    dsl::harvest_signals
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .filter(dsl::consumed.eq(false))
        .order((dsl::received_at.asc(), dsl::id.asc()))
        .select(HarvestSignal::as_select())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Mark the provided signal IDs consumed.
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the update fails.
#[cfg(feature = "db")]
pub async fn mark_signals_consumed(
    conn: &mut AsyncPgConnection,
    signal_ids: &[uuid::Uuid],
) -> HarvestResult<()> {
    use crate::schema::harvest_signals::dsl;

    if signal_ids.is_empty() {
        return Ok(());
    }

    diesel::update(dsl::harvest_signals.filter(dsl::id.eq_any(signal_ids)))
        .set(dsl::consumed.eq(true))
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}
