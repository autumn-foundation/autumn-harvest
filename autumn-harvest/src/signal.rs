//! Workflow signal delivery and management.
//!
//! A signal sends an asynchronous event or payload into a running workflow.
//! This module enqueues signals durably, loads the pending signals of a
//! workflow, and marks them consumed once the workflow context processes them.
#[cfg(feature = "db")]
use crate::cached_stmt::CachePreparedDsl as _;
#[cfg(feature = "db")]
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};

use crate::error::{HarvestError, HarvestResult};
#[cfg(feature = "db")]
use crate::models::{HarvestSignal, NewHarvestSignal};
#[cfg(feature = "db")]
use crate::telemetry::{ATTR_EXECUTION_ID, ATTR_WORKFLOW_ID};
use crate::types::ExecutionId;

/// Queue a workflow signal for durable delivery and wake the parked workflow.
///
/// This form takes no codec registry, so it stores the payload in clear. Use
/// [`send_signal_with_codecs`] to honour column encoding (issue #1979).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist. Returns
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal. Returns
/// [`HarvestError::Database`](crate::error::HarvestError::Database) if the
/// insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
) -> HarvestResult<()> {
    send_signal_with_codecs(
        conn,
        exec_id,
        signal_name,
        payload,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`send_signal`], encoding the payload column with `codecs` (issue #1979).
///
/// The payload is stored as an envelope only while column encoding is on.
///
/// # Errors
///
/// As [`send_signal`], plus a codec error when the payload cannot be encoded.
#[cfg(feature = "db")]
pub async fn send_signal_with_codecs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<()> {
    // With no key the partial unique index excludes the NULL row, so every
    // insert succeeds — the legacy at-least-once contract. Bool discarded.
    send_signal_idempotent_with_codecs(conn, exec_id, signal_name, payload, None, codecs)
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
/// This form takes no codec registry, so it stores the payload in clear.
/// Use [`send_signal_idempotent_with_codecs`] to honour column
/// encoding (issue #1979).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the workflow execution does not exist. Returns
/// [`HarvestError::Cancelled`](crate::error::HarvestError::Cancelled) or
/// [`HarvestError::Config`](crate::error::HarvestError::Config) if the
/// execution is already terminal. Returns
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
    send_signal_idempotent_with_codecs(
        conn,
        exec_id,
        signal_name,
        payload,
        idempotency_key,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// Refuse a signal for an execution that cannot accept one (issue #2065).
///
/// The insert path runs it after the insert. A codec failure runs it first, so
/// a terminal target still reports its terminal state, not the codec failure.
#[cfg(feature = "db")]
fn signal_state_refusal(
    exec_id: ExecutionId,
    execution: &crate::models::WorkflowExecution,
) -> HarvestResult<()> {
    match execution.state.as_str() {
        // PAUSED is a non-terminal active state: a paused workflow
        // waiting on a signal must still accept (buffer) it so it is
        // delivered on resume. The wake after the insert re-pends the task, which
        // the claim gate defers until the execution is RUNNING.
        "RUNNING" | "PAUSED" => {}
        // Issue #964. Neither is terminal, and a "terminal" answer does
        // damage. A caller that sees "terminal" stops retrying. The outbox
        // then records `target_terminal` in the sender's history for a
        // workflow that is alive.
        //
        // `MIGRATED` — the run was rebalanced onto another shard; this row
        // is the seal it left behind. The caller reached the wrong database
        // and should re-resolve through the forwarding pointer.
        // `MIGRATING` — a staged copy that is not live yet; it becomes
        // claimable at activation, moments away.
        //
        // Both report `ShardUnavailable`, the engine's existing RETRYABLE
        // classification (`HarvestError::is_shard_unavailable`). So the
        // outbox leaves the row pending and tries again. It does not fail
        // the delivery permanently.
        //
        // `MIGRATING` covers two different moments (issue #1317). This
        // row cannot tell them apart. `commit_cutover` writes only the
        // SOURCE database, on purpose. That lets it commit even when
        // the target is briefly unreachable. No distributed two-phase
        // commit is needed.
        //
        // A connection resolved to the target sees the same
        // `MIGRATING` row in both moments: before cutover, and after
        // cutover but before activation.
        //
        // Accepting the write in the first moment is not safe. If the
        // migration later aborts, the staged copy is discarded. This
        // signal would go with it. The caller would see `Ok` for a
        // signal that silently never existed.
        //
        // Refusing in the second moment is suboptimal, not unsafe.
        // Activation normally follows cutover in the same call.
        // `resume_incomplete_migrations` finishes the pair after a
        // crash in between.
        //
        // Telling the two moments apart needs a source connection, or
        // a target-side marker from `commit_cutover`. Callers of this
        // function are not guaranteed a source connection. A
        // target-side marker would revive the two-phase dependency
        // cutover deliberately avoids. So this refuses both,
        // retryably, rather than risk the unsafe one.
        state @ ("MIGRATED" | "MIGRATING") => {
            return Err(HarvestError::ShardUnavailable {
                shard_id: exec_id.shard().as_i32(),
                reason: format!(
                    "workflow execution {exec_id} is mid shard-rebalance on this shard \
                 ({state}); re-resolve it through its forwarding pointer and retry"
                ),
            });
        }
        "CANCELLED" => {
            return Err(HarvestError::Cancelled(
                execution
                    .error
                    .clone()
                    .unwrap_or_else(|| format!("workflow execution {exec_id} is cancelled")),
            ));
        }
        state => {
            return Err(HarvestError::Config(format!(
                "workflow execution {exec_id} is terminal ({state})"
            )));
        }
    }
    Ok(())
}

/// [`send_signal_idempotent`], encoding the payload column with `codecs`
/// (issue #1979).
///
/// # Errors
///
/// As [`send_signal_idempotent`], plus a codec error when the payload
/// cannot be encoded.
#[cfg(feature = "db")]
pub async fn send_signal_idempotent_with_codecs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<bool> {
    use crate::schema::harvest_signals;
    use crate::schema::harvest_workflow_executions;

    // An empty key is not in the partial index's NULL exclusion, so it would
    // collide across unrelated signals — treat it as no key (at-least-once).
    let idempotency_key = idempotency_key.filter(|k| !k.is_empty());
    // Issue #1985: a durable promise settles once on every signal path.
    let idempotency_key =
        crate::durable_promise::settlement_idempotency_key(signal_name, &payload, idempotency_key)?;

    // The wake at the end of this transaction re-pends a parked workflow task,
    // so it raises a dispatch hint (issue #1312). A hint published before the
    // COMMIT names a row that no reader outside this transaction can see. The
    // reader probes it, finds nothing, and drops the reference after three
    // short releases. The buffering scope holds the hint until the commit. The
    // signal then reaches a worker through the channel rather than waiting for
    // the reconcile sweep.
    crate::dispatch::buffered_settled(Box::pin(conn.transaction::<bool, HarvestError, _>(
        async |conn| {
            let execution = harvest_workflow_executions::table
                .find(exec_id.as_uuid())
                .for_update()
                .select(crate::models::WorkflowExecution::as_select())
                // A load by primary key has one plan. `first` adds this limit.
                .limit(1)
                .cache_prepared()
                .get_result(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?
                .ok_or_else(|| HarvestError::NotFound(format!("workflow execution {exec_id}")))?;

            // A keyed retry of a signal that already landed must still dedupe
            // to `Ok(false)` when the codec fails now (issue #1979).
            let payload = match codecs.encode_column(&payload) {
                Ok(payload) => payload,
                Err(error) => {
                    if let Some(key) = idempotency_key
                        && signal_idempotency_key_exists(conn, exec_id, key).await?
                    {
                        return Ok(false);
                    }
                    // A terminal target is a verdict. A codec failure is not.
                    // A shard move is not a verdict either, so the codec
                    // failure stays and the request stays pending.
                    if let Err(refusal) = signal_state_refusal(exec_id, &execution)
                        && !refusal.is_shard_unavailable()
                    {
                        return Err(refusal);
                    }
                    return Err(error);
                }
            };
            let row = NewHarvestSignal {
                workflow_exec_id: exec_id.as_uuid(),
                signal_name,
                payload,
                idempotency_key,
            };

            // Insert before the state check, so a keyed retry that already
            // landed dedupes to a no-op even after the workflow went terminal.
            // `on_conflict_do_nothing()` has no explicit target, so Postgres
            // arbitrates against the partial unique index
            // `uq_harvest_signals_idem`. The index excludes a NULL key, so that
            // insert always succeeds (rows-affected = 1).
            let inserted = diesel::insert_into(harvest_signals::table)
                .values(&row)
                .on_conflict_do_nothing()
                .cache_prepared()
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
            signal_state_refusal(exec_id, &execution)?;

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
        },
    )))
    .await
}

/// Outcome of a signal delivery routed across the workflow-level retry chain
/// (issue #843).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutedSignalDelivery {
    /// The execution that received the signal: the **live attempt** of the
    /// logical run. It differs from the requested id whenever the
    /// workflow-level retry chain (#523) has advanced.
    pub target: ExecutionId,
    /// `true` when a row was freshly queued (the workflow was woken); `false`
    /// when an `idempotency_key` collided with an already-staged signal.
    pub delivered: bool,
}

/// Deliver a signal to the **live attempt** of the logical run named by
/// `exec_id` (issue #843).
///
/// A caller that holds the id returned by `start` targets the logical run,
/// not one specific attempt. Without routing, a signal sent after attempt 1
/// sealed `FAILED` would land on the sealed predecessor and drop with it,
/// while the retry ran on. For a workflow with no retry policy this is
/// exactly [`send_signal_idempotent`].
///
/// **Between-attempts contract.** The chain always has an execution row. The
/// retry successor is inserted in the same transaction that seals the
/// predecessor `FAILED`. So a signal sent at any point routes to a row that
/// exists. When that row's task is a still-delayed queued retry, the signal
/// waits in `harvest_signals`. The claim of the retry ingests it into history.
/// That is "deliver on the next attempt", with no synthetic buffering layer
/// and no rejection window.
///
/// **#521 idempotency keys** stay scoped to
/// `(workflow_exec_id, idempotency_key)`. Routing moves *where* the row lands,
/// so two keyed deliveries that both route to the same live attempt still
/// dedupe to one row. An earlier attempt can consume a keyed delivery. A
/// re-send after the chain advanced IS delivered again. That is correct,
/// because the retry replays a fresh history that has not seen it.
///
/// Race handling: the resolved attempt can seal `FAILED` between the
/// resolution and the insert. The delivery then does not take effect. Either
/// the state check rejects and rolls back the insert, or a keyed insert
/// dedupes against the stale attempt's existing row before that check. Both
/// report "not delivered", so both re-drive against the freshly resolved live
/// attempt. Neither can double-deliver, because a re-drive only follows a
/// delivery that was *not* queued.
///
/// This form takes no codec registry, so it stores the payload in clear.
/// Use [`send_signal_to_live_attempt_with_codecs`] to honour column
/// encoding (issue #1979).
///
/// # Errors
///
/// Returns [`HarvestError::NotFound`](crate::error::HarvestError::NotFound) if
/// the execution does not exist. Returns [`HarvestError::Cancelled`] /
/// [`HarvestError::Config`] if the resolved live attempt is terminal. Returns
/// [`HarvestError::Database`] if the insert or wake fails.
#[cfg(feature = "db")]
pub async fn send_signal_to_live_attempt(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<RoutedSignalDelivery> {
    send_signal_to_live_attempt_with_codecs(
        conn,
        exec_id,
        signal_name,
        payload,
        idempotency_key,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`send_signal_to_live_attempt`], encoding the payload column with `codecs`
/// (issue #1979).
///
/// # Errors
///
/// As [`send_signal_to_live_attempt`], plus a codec error when the payload
/// cannot be encoded.
#[cfg(feature = "db")]
pub async fn send_signal_to_live_attempt_with_codecs(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<RoutedSignalDelivery> {
    // `conn` is supplied by the caller (generated typed-signal code predating
    // sharding), so its shard is not known here. `resolve_live_attempt_id_best_effort`
    // (issue #1596 review) recovers it from the row itself rather than
    // trusting a retry successor's stale `MIGRATED` stub across a rebalance.
    //
    // `send_signal_from_resolved` binds to `target`'s own shard itself
    // (issue #1596 follow-up review, comment 4052389744). Passing `conn`
    // through unchanged here is correct even when the resolve above hopped
    // to a different shard than `conn` currently holds.
    let (target, rebind) =
        crate::execution::resolve_live_attempt_id_best_effort(conn, exec_id).await?;
    // Drop this checkout before `send_signal_from_resolved` binds again for the
    // same shard. `rebind` is not reused below: this call passes `conn` and
    // lets `send_signal_from_resolved` rebind on its own. Holding both open at
    // once can deadlock a pool-size-one shard against itself.
    drop(rebind);
    send_signal_from_resolved_with_codecs(
        conn,
        exec_id,
        target,
        signal_name,
        payload,
        idempotency_key,
        codecs,
    )
    .await
}

/// [`send_signal_to_live_attempt`] for a caller that has **already** resolved
/// the live attempt (issue #843).
///
/// `logical_exec_id` is the id the caller addressed (the one a re-drive
/// re-resolves from); `resolved` is the live attempt that resolution produced.
/// A handler that must inspect the resolved execution before delivery uses
/// this. The management API's `/signal` route is one: it validates the #610
/// payload schema against the resolved row's `workflow_name`. So one request
/// walks the chain once, not twice. And the row it validated is provably the
/// row it delivers to.
///
/// `resolved` may have been resolved by a caller that holds no
/// [`crate::shard::ShardedDbPool`] of its own. This function's `conn`-only
/// signature is unchanged since before sharding. Or it may come from a
/// caller that does hold one, such as the management API. Either way,
/// `conn` is not guaranteed to already be bound to `resolved`'s own shard
/// (issue #1596 follow-up review, comment 4052389744). This function binds
/// to it itself, rather than trusting the caller to have done so. A hop
/// that moved shards mid-resolution leaves `conn` on the ORIGINAL shard,
/// not the resolved one.
///
/// This form takes no codec registry, so it stores the payload in clear.
/// Use [`send_signal_from_resolved_with_codecs`] to honour column
/// encoding (issue #1979).
///
/// # Errors
///
/// See [`send_signal_to_live_attempt`].
#[cfg(feature = "db")]
pub async fn send_signal_from_resolved(
    conn: &mut AsyncPgConnection,
    logical_exec_id: ExecutionId,
    resolved: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
) -> HarvestResult<RoutedSignalDelivery> {
    send_signal_from_resolved_with_codecs(
        conn,
        logical_exec_id,
        resolved,
        signal_name,
        payload,
        idempotency_key,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`send_signal_from_resolved`], encoding the payload column with `codecs`
/// (issue #1979).
///
/// # Errors
///
/// As [`send_signal_from_resolved`], plus a codec error when the payload
/// cannot be encoded.
#[cfg(feature = "db")]
pub async fn send_signal_from_resolved_with_codecs(
    conn: &mut AsyncPgConnection,
    logical_exec_id: ExecutionId,
    resolved: ExecutionId,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<RoutedSignalDelivery> {
    let exec_id = logical_exec_id;
    let mut target = resolved;
    let mut rebind = crate::execution::bind_to_shard_best_effort(conn, target).await?;
    for _ in 0..crate::execution::RETRY_CHAIN_MAX_REDRIVES {
        let active: &mut AsyncPgConnection = match &mut rebind {
            Some(fresh) => fresh,
            None => conn,
        };
        match send_signal_idempotent_with_codecs(
            active,
            target,
            signal_name,
            payload.clone(),
            idempotency_key,
            codecs,
        )
        .await
        {
            // A fresh insert is final: it landed on the attempt we resolved.
            Ok(true) => {
                return Ok(RoutedSignalDelivery {
                    target,
                    delivered: true,
                });
            }
            // A keyed dedup (`Ok(false)`) returns *before* the state check.
            // So it can report success against a row that has since sealed
            // `FAILED`, and swallow a re-send the live attempt still needs.
            // Nothing was queued, so re-driving cannot double-deliver.
            Ok(false) => {
                drop(rebind);
                let (fresh, fresh_rebind) =
                    crate::execution::resolve_live_attempt_id_best_effort(conn, exec_id)
                        .await
                        .unwrap_or((target, None));
                if !crate::execution::redrive_target(target, fresh) {
                    return Ok(RoutedSignalDelivery {
                        target,
                        delivered: false,
                    });
                }
                target = fresh;
                rebind = fresh_rebind;
            }
            Err(error) => {
                // The delivery was rolled back, so re-driving is safe: it can
                // never double-deliver a signal that was actually queued.
                drop(rebind);
                let (fresh, fresh_rebind) =
                    crate::execution::resolve_live_attempt_id_best_effort(conn, exec_id)
                        .await
                        .unwrap_or((target, None));
                if !crate::execution::redrive_target(target, fresh) {
                    return Err(error);
                }
                target = fresh;
                rebind = fresh_rebind;
            }
        }
    }
    let active: &mut AsyncPgConnection = match &mut rebind {
        Some(fresh) => fresh,
        None => conn,
    };
    let delivered = send_signal_idempotent_with_codecs(
        active,
        target,
        signal_name,
        payload,
        idempotency_key,
        codecs,
    )
    .await?;
    Ok(RoutedSignalDelivery { target, delivered })
}

/// Reassign **every** signal of `from` to `to`, resetting `consumed`, and
/// return how many rows moved (issue #843).
///
/// The transaction that seals a failed attempt and inserts its workflow-level
/// retry successor (#523) calls this. So the successor inherits the logical
/// run's whole signal mailbox.
///
/// **Consumed signals move too, and are re-armed.** `consumed = true` means
/// "already ingested into *that attempt's* history". A retry starts from a
/// fresh, empty history and re-executes every step from zero. So the retry
/// must observe again each signal the failed attempt observed. Otherwise a
/// retry that replays back to its `wait_for_signal` blocks forever. Its caller
/// was already told that the signal was delivered, and has no reason to
/// re-send. The retry re-runs every activity for the same reason: nothing from
/// the discarded attempt carries over except the mailbox.
///
/// The failed attempt's own `SignalReceived` events remain in its history, so
/// the audit record of what it observed is unaffected by the move.
///
/// This deliberately differs from the narrower reassignment of
/// `continue_as_new`, which moves unconsumed signals only. A continue-as-new
/// is *voluntary*: the workflow carries what it still needs into the
/// successor's input. A retry carries nothing forward at all.
///
/// #521 idempotency keys move with their rows. So a caller's at-least-once
/// re-send of a delivered key still dedupes against the live attempt. A sealed
/// predecessor does not swallow it.
///
/// # Errors
///
/// Returns [`HarvestError::Database`] when the update fails.
#[cfg(feature = "db")]
pub async fn forward_signals_to_retry_attempt(
    conn: &mut AsyncPgConnection,
    from: ExecutionId,
    to: ExecutionId,
) -> HarvestResult<usize> {
    use crate::schema::harvest_signals;

    diesel::update(
        harvest_signals::table.filter(harvest_signals::workflow_exec_id.eq(from.as_uuid())),
    )
    .set((
        harvest_signals::workflow_exec_id.eq(to.as_uuid()),
        harvest_signals::consumed.eq(false),
    ))
    .cache_prepared()
    .execute(conn)
    .await
    .map_err(crate::error::database_error)
}

/// Outcome of resolving a `(workflow_name, workflow_id)`-targeted signal
/// request to a concrete execution and attempting delivery (issue #751).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByIdSignalOutcome {
    /// The signal was queued for the resolved (current) run. It is a fresh
    /// insert or an idempotency-key replay of an existing one. Either way the
    /// target has (or already had) this signal queued for delivery.
    Delivered,
    /// No run has ever existed for this `(workflow_name, workflow_id)`.
    /// Callers apply the same grace-window policy used for an unrecognized
    /// `ExecutionId` before reporting a definitive `target_unknown` failure.
    NoRunFound,
    /// The resolved run (the "current" run at resolution time) is already
    /// terminal. Unlike cancellation, a terminal target never meets a
    /// signal's goal. The payload cannot be delivered, so this is a genuine
    /// failure (`not_running`), never success.
    NotRunning,
    /// The resolved run raced to `CONTINUED_AS_NEW` between resolution and
    /// the delivery attempt: a live successor now exists under the same
    /// business key. It is neither success nor failure. The caller leaves this
    /// attempt unresolved. A later attempt (an immediate inline retry or the
    /// next outbox tick) re-resolves and finds the live successor.
    RacedToSuccessor,
}

/// Resolve a `(workflow_name, workflow_id)` target to its current run and
/// deliver a signal to it, closing the continue-as-new race by construction
/// (issue #751, AC2/AC3/AC4).
///
/// [`crate::execution::resolve_and_cancel_by_workflow_id`] has the full proof
/// that a **direct** resolution via
/// [`crate::execution::resolve_execution_id_by_workflow_id`] never returns
/// `state == "CONTINUED_AS_NEW"`. The same argument applies here, because
/// both functions share the same resolve step. One race needs handling: the
/// tiny window between this function's resolve step and its delivery
/// attempt. If the resolved run continues-as-new in that window,
/// [`crate::execution::is_continued_as_new_race`] recognises it. This
/// function then reports [`ByIdSignalOutcome::RacedToSuccessor`], not a
/// delivery or a failure.
///
/// Unlike cancellation, a signal to an already-terminal resolved run is a
/// genuine failure ([`ByIdSignalOutcome::NotRunning`]). The payload was not,
/// and cannot be, delivered. That matches the "no signal silently dropped"
/// invariant of the `ExecutionId`-targeted signal contract (issue #244).
///
/// # Keyed retries (issue #751 code review, PR #1145)
///
/// This function deliberately does **not** return `NotRunning` early for a
/// terminal resolved run before it attempts delivery. Take a keyed retry from
/// the caller-side outbox. Its target-shard insert already committed, but its
/// caller-shard transaction rolled back before it recorded the outcome. That
/// retry must dedupe to `Delivered` even after the target has gone terminal.
/// An early terminal check would hide the existing key collision from the
/// "insert before validate" dedup of [`send_signal_idempotent`] (issue #521).
/// It would misreport a request that was, in fact, already fulfilled.
///
/// A second, distinct race compounds this across a continue-as-new rotation.
/// The dedup of [`send_signal_idempotent`] is scoped to
/// `(workflow_exec_id, idempotency_key)`. So a key used against a
/// predecessor's `exec_id` does not collide against a successor's. A naive
/// retry that re-resolves to the (now different) current run would insert a
/// *second*, duplicate signal for one logical request. This function closes
/// that gap with a `(workflow_name, workflow_id, idempotency_key)`-scoped
/// pre-check before any fresh insert against the freshly-resolved run. The
/// pre-check reuses [`crate::execution::lookup_idempotent_signal_dedupe`],
/// which `signal_with_start` uses for the same purpose (issue #244).
///
/// This form takes no codec registry, so it stores the payload in clear.
/// Use [`resolve_and_signal_by_workflow_id_with_codecs`] to honour column
/// encoding (issue #1979).
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
    resolve_and_signal_by_workflow_id_with_codecs(
        conn,
        workflow_name,
        workflow_id,
        signal_name,
        payload,
        idempotency_key,
        &crate::store::DEFAULT_PAYLOAD_CODECS,
    )
    .await
}

/// [`resolve_and_signal_by_workflow_id`], encoding the payload column with `codecs`
/// (issue #1979).
///
/// # Errors
///
/// As [`resolve_and_signal_by_workflow_id`], plus a codec error when the payload
/// cannot be encoded.
#[cfg(feature = "db")]
pub async fn resolve_and_signal_by_workflow_id_with_codecs(
    conn: &mut AsyncPgConnection,
    workflow_name: &str,
    workflow_id: &str,
    signal_name: &str,
    payload: serde_json::Value,
    idempotency_key: Option<&str>,
    codecs: &crate::payload_codec::PayloadCodecs,
) -> HarvestResult<ByIdSignalOutcome> {
    let Some(run) =
        crate::execution::resolve_execution_id_by_workflow_id(conn, workflow_name, workflow_id)
            .await?
    else {
        return Ok(ByIdSignalOutcome::NoRunFound);
    };

    // Issue #1985: apply the promise settlement rules before the dedupe
    // probe. A refusal ends as a refused insert below would.
    let idempotency_key = match crate::durable_promise::settlement_idempotency_key(
        signal_name,
        &payload,
        idempotency_key,
    ) {
        Ok(key) => key,
        Err(HarvestError::Config(_)) => return Ok(ByIdSignalOutcome::NotRunning),
        Err(e) => return Err(e),
    };
    // A promise belongs to one run, so its key dedupes per execution only,
    // at the insert (issue #1985).
    let promise_signal = signal_name.starts_with(crate::durable_promise::PROMISE_SIGNAL_PREFIX);

    // Cross-execution dedup: a keyed request already delivered to ANY
    // execution in this (workflow_name, workflow_id) chain reports as
    // delivered, with no fresh insert. That includes a predecessor sealed by
    // a continue-as-new that raced ahead of this retry.
    if let Some(key) = idempotency_key.filter(|k| !k.is_empty() && !promise_signal)
        && crate::execution::lookup_idempotent_signal_dedupe(conn, workflow_name, workflow_id, key)
            .await?
            .is_some()
    {
        return Ok(ByIdSignalOutcome::Delivered);
    }

    match send_signal_idempotent_with_codecs(
        conn,
        run.exec_id,
        signal_name,
        payload,
        idempotency_key,
        codecs,
    )
    .await
    {
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
            // A genuinely FRESH insert against a terminal target: the dedup
            // pre-check found no prior delivery for this key. `run` may have
            // resolved to a terminal state already (no key was supplied, or
            // this key is new). Or the target sealed in the window between the
            // resolve above and the signal attempt's own lock.
            Ok(ByIdSignalOutcome::NotRunning)
        }
        Err(e) => Err(e),
    }
}

/// Read-only probe: does a queued signal row already exist for
/// `(workflow_exec_id, idempotency_key)`?
///
/// The standalone signal route uses it for its committed-replay dedup
/// short-circuit (issues #521 / #610). A keyed retry whose row already landed
/// must return the documented `signal_delivered: false` no-op. That happens
/// *before* any payload-schema (#610) or payload-cap (#252) validation. Either
/// may have been published or tightened since the original delivery.
/// Otherwise a retry of an accepted signal could get `400`, not the documented
/// `202`. That breaks the exactly-once contract, including the "retry after
/// the run went terminal" case. Side-effect free (no insert, no wake).
///
/// The authoritative dedup remains the insert-time `ON CONFLICT` of
/// [`send_signal_idempotent`]. This is only a fast path for committed rows.
/// In a concurrent first-delivery race, neither request sees a row yet. So
/// both fall through to validation and the `ON CONFLICT` insert. The partial
/// unique index excludes an empty key, which never dedupes. So an empty key
/// reports `false` and the caller takes the normal path. That mirrors the
/// empty-key normalization of [`send_signal_idempotent`].
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

/// Report whether an execution has an unconsumed signal (issue #2079).
///
/// # Errors
///
/// Returns [`HarvestError::Database`](crate::error::HarvestError::Database) if
/// the query fails.
#[cfg(feature = "db")]
pub async fn has_pending_signals(
    conn: &mut AsyncPgConnection,
    exec_id: ExecutionId,
) -> HarvestResult<bool> {
    use crate::schema::harvest_signals::dsl;

    diesel::select(diesel::dsl::exists(
        dsl::harvest_signals
            .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
            .filter(diesel::dsl::not(dsl::consumed)),
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
        .filter(diesel::dsl::not(dsl::consumed))
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
        .cache_prepared()
        .execute(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(())
}
