//! Transactional outbox implementation for reliable workflow event emission.

use std::time::Duration;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use chrono::NaiveDateTime;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use uuid::Uuid;

use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::shard::ShardRouter;
use autumn_harvest::types::{ExecutionId, Priority};
use autumn_harvest::{
    StartWorkflowParams, start_or_load_workflow_execution_with_metrics_and_codecs,
};

use crate::config::HarvestOutboxConfig;
use crate::state::HarvestDbPool;

diesel::table! {
    harvest_workflow_outbox (id) {
        id -> BigInt,
        workflow_name -> Text,
        workflow_id -> Text,
        queue_name -> Text,
        input -> Jsonb,
        memo -> Nullable<Jsonb>,
        search_attrs -> Nullable<Jsonb>,
        delivery_attempts -> BigInt,
        last_error -> Nullable<Text>,
        delivered_execution_id -> Nullable<Text>,
        delivered_at -> Nullable<Timestamp>,
        next_attempt_at -> Timestamp,
        claimed_at -> Nullable<Timestamp>,
        claimed_by -> Nullable<Text>,
        created_at -> Timestamp,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkflowStartRequest {
    pub workflow_name: String,
    pub workflow_id: String,
    pub queue_name: String,
    pub input: Value,
    pub memo: Option<Value>,
    pub search_attrs: Option<Value>,
}

#[allow(dead_code)] // Row mirrors full table state across claim/update/retry paths and tests.
#[derive(Debug, Clone, diesel::Queryable, diesel::Selectable, diesel::QueryableByName)]
#[diesel(table_name = harvest_workflow_outbox)]
struct HarvestWorkflowOutboxRow {
    id: i64,
    workflow_name: String,
    workflow_id: String,
    queue_name: String,
    input: Value,
    memo: Option<Value>,
    search_attrs: Option<Value>,
    delivery_attempts: i64,
    last_error: Option<String>,
    delivered_execution_id: Option<String>,
    delivered_at: Option<NaiveDateTime>,
    next_attempt_at: NaiveDateTime,
    claimed_at: Option<NaiveDateTime>,
    claimed_by: Option<String>,
    created_at: NaiveDateTime,
}

#[derive(diesel::Insertable)]
#[diesel(table_name = harvest_workflow_outbox)]
struct NewHarvestWorkflowOutboxRow<'a> {
    workflow_name: &'a str,
    workflow_id: &'a str,
    queue_name: &'a str,
    input: Value,
    memo: Option<Value>,
    search_attrs: Option<Value>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct OutboxDrainStats {
    claimed: usize,
    delivered: usize,
}

impl HarvestWorkflowOutboxRow {
    fn request(&self) -> WorkflowStartRequest {
        WorkflowStartRequest {
            workflow_name: self.workflow_name.clone(),
            workflow_id: self.workflow_id.clone(),
            queue_name: self.queue_name.clone(),
            input: self.input.clone(),
            memo: self.memo.clone(),
            search_attrs: self.search_attrs.clone(),
        }
    }
}

/// Persist a workflow-start request in the application database outbox.
///
/// Duplicate `(workflow_name, workflow_id)` requests are ignored so callers can retry safely.
///
/// # Errors
///
/// Returns a Diesel error if the outbox insert cannot be executed, or if
/// `request.workflow_id` is an explicit empty string (issue #1353, Codex
/// review). `diesel::result::Error` has no dedicated variant for a rejected
/// precondition, so `QueryBuilderError` carries it -- the query is never
/// even built. Rejecting here, not only at dispatch time, means a
/// successful `Ok(())` never promises delivery of a row that can never
/// start.
pub async fn enqueue_workflow_start_outbox(
    conn: &mut AsyncPgConnection,
    request: &WorkflowStartRequest,
) -> Result<(), diesel::result::Error> {
    if request.workflow_id.is_empty() {
        return Err(diesel::result::Error::QueryBuilderError(
            "workflow_id must not be empty".into(),
        ));
    }
    diesel::insert_into(harvest_workflow_outbox::table)
        .values(NewHarvestWorkflowOutboxRow {
            workflow_name: &request.workflow_name,
            workflow_id: &request.workflow_id,
            queue_name: &request.queue_name,
            input: request.input.clone(),
            memo: request.memo.clone(),
            search_attrs: request.search_attrs.clone(),
        })
        .on_conflict((
            harvest_workflow_outbox::workflow_name,
            harvest_workflow_outbox::workflow_id,
        ))
        .do_nothing()
        .execute(conn)
        .await?;

    Ok(())
}

/// Claim one batch of due outbox rows and attempt delivery to Harvest storage.
///
/// The returned count is the number of rows successfully delivered, not the number claimed.
///
/// # Errors
///
/// Returns an [`AutumnError`] when the app database pool is unavailable or row claiming/updating
/// fails.
pub async fn drain_workflow_start_outbox_once(
    state: &AppState,
    limit: i64,
) -> Result<usize, AutumnError> {
    drain_workflow_start_outbox_batch(state, limit)
        .await
        .map(|stats| stats.delivered)
}

/// How many dispatch outcomes accumulate before their marks flush.
///
/// A row stays claimed -- unreachable by any relay's reclaim, and for a
/// failed row, unreachable for its own retry -- until its mark flushes.
/// Flushing every `OUTBOX_MARK_FLUSH_EVERY` outcomes bounds that wait to
/// a handful of dispatches. The alternative, flushing only once after
/// the whole batch has dispatched, bounds it to the rest of the batch
/// instead (issue #1620 review, Codex).
///
/// This bound alone is on outcome COUNT, not elapsed time. Dispatch does
/// not touch `app_conn`. A flush CAN run while one dispatch is still in
/// flight -- see the `tokio::select!` in the drain loop below. Without
/// that race, a single long-running dispatch would hold every row already
/// queued in its chunk. It would hold them for that whole call's duration
/// (issue #1620 review, Codex, second and third rounds).
/// `OUTBOX_MARK_FLUSH_MAX_DELAY` below bounds that case too.
const OUTBOX_MARK_FLUSH_EVERY: usize = 8;

/// How much wall time may pass since the last flush before pending marks
/// flush, regardless of outcome count. Checked between dispatches, and
/// also while a dispatch is still in flight via the drain loop's
/// `tokio::select!`. See `OUTBOX_MARK_FLUSH_EVERY`.
const OUTBOX_MARK_FLUSH_MAX_DELAY: std::time::Duration = std::time::Duration::from_millis(250);

/// Whether pending marks should flush now. Checked right after each
/// dispatch returns, and periodically while a dispatch is still in
/// flight. The drain loop's `tokio::select!` races a flush-deadline timer
/// against the dispatch future for that second case. `pending` is 0 when
/// nothing has accumulated, which is never due.
fn outbox_mark_flush_due(pending: usize, since_last_flush: std::time::Duration) -> bool {
    pending > 0
        && (pending >= OUTBOX_MARK_FLUSH_EVERY || since_last_flush >= OUTBOX_MARK_FLUSH_MAX_DELAY)
}

/// Margin subtracted from `claim_ttl_ms` before computing the claim
/// deadline (issue #1620 review, Codex, seventh round). Waking the flush
/// timer exactly at claim expiry leaves no time for the mark query's own
/// round trip. A concurrent relay can win a reclaim race in that gap.
/// Ten percent of the TTL, capped at one second, trades a small part of
/// the claim window for headroom.
fn claim_flush_margin_ms(claim_ttl_ms: u64) -> u64 {
    (claim_ttl_ms / 10).min(1_000)
}

/// How long the drain loop's inner `select!` should sleep before waking
/// to recheck whether pending marks are due. Pairs with
/// `outbox_mark_flush_due`, which makes the same due/not-due call from
/// an elapsed duration instead of absolute deadlines. `deadline` is the
/// soonest of three candidates (issue #1620 review, Codex, fifth round).
/// The fixed `last_flush + OUTBOX_MARK_FLUSH_MAX_DELAY` bound. Any failed
/// row's own configured retry deadline -- a short `base_retry_delay_ms`
/// must not wait behind the fixed bound. `claim_deadline`, while
/// anything is pending, so a row flushes before its claim goes stale.
fn outbox_mark_flush_remaining(
    now: std::time::Instant,
    pending: usize,
    last_flush: std::time::Instant,
    earliest_failed_deadline: Option<std::time::Instant>,
    claim_deadline: std::time::Instant,
) -> Duration {
    if pending >= OUTBOX_MARK_FLUSH_EVERY {
        return Duration::ZERO;
    }
    let mut deadline = last_flush + OUTBOX_MARK_FLUSH_MAX_DELAY;
    if let Some(earliest_failed) = earliest_failed_deadline {
        deadline = deadline.min(earliest_failed);
    }
    if pending > 0 {
        deadline = deadline.min(claim_deadline);
    }
    deadline.saturating_duration_since(now)
}

async fn drain_workflow_start_outbox_batch(
    state: &AppState,
    limit: i64,
) -> Result<OutboxDrainStats, AutumnError> {
    let config = outbox_config(state);
    if !config.enabled {
        return Ok(OutboxDrainStats::default());
    }

    let Some(app_pool) = state.pool().cloned() else {
        return Err(AutumnError::service_unavailable_msg(
            "Database not configured for Harvest outbox",
        ));
    };

    let claimant = format!("harvest-outbox-{}", Uuid::new_v4().simple());
    let mut app_conn = app_pool
        .get()
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    // Captured BEFORE issuing the claim query, not after it returns
    // (issue #1620 review, Codex, sixth round). Postgres sets
    // `claimed_at = NOW()` inside that query, strictly before this
    // `await` resolves. A post-return Instant is already later than the
    // real database claim time. A deadline computed from it would run
    // later than the actual expiry it is meant to stay clear of. A
    // pre-call Instant is <= the real claim time instead. A deadline
    // derived from it is always at or before the true expiry --
    // conservative in the safe direction, never the unsafe one.
    let claim_started_at = std::time::Instant::now();
    let rows = claim_due_outbox_rows(&mut app_conn, limit.max(1), &claimant, &config)
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
    // Every row in this batch got `claimed_at` set in that one claim
    // query. One deadline stands in for all of their claim times (issue
    // #1620 review, Codex, fifth round). The drain loop's flush deadline
    // must not run later than this. A row whose claim is close to
    // expiring could otherwise sit flushed-but-not-yet-due past
    // `claim_ttl_ms`, while a slower row's dispatch is still pending.
    // `claim_flush_margin_ms` backs this off further, so the flush query
    // itself has room to run before the real database expiry.
    let claim_deadline = claim_started_at
        + Duration::from_millis(
            config
                .claim_ttl_ms
                .saturating_sub(claim_flush_margin_ms(config.claim_ttl_ms)),
        );

    // issue #618, F-round8: the metrics recorder for the exempt-with-bypass-counter
    // "outbox" producer. Fetched once; the bypass is counted per row only AFTER the
    // app outbox row is durably marked delivered (see below).
    let outbox_metrics = state
        .extension::<std::sync::Arc<autumn_harvest::worker::HandlerRegistry>>()
        .map(|registry| std::sync::Arc::clone(&registry.telemetry().metrics));

    // Dispatch is the one part of this loop that cannot batch. Each row
    // starts a distinct workflow execution. The mark that follows dispatch
    // is a single fixed-shape `UPDATE ... WHERE id = $1 AND claimed_by =
    // $2`. It differs only in its bound values. This collects the marks
    // here and flushes them in chunks of `OUTBOX_MARK_FLUSH_EVERY`, below.
    // That replaces one mark per row (issue #1620, Ledger) and also
    // replaces one mark for the whole batch (issue #1620 review, Codex).
    let claimed = rows.len();
    let mut delivered = 0_usize;
    let mut delivered_marks: Vec<(i64, ExecutionId)> = Vec::new();
    // The retry deadline is captured as a monotonic `Instant`, not a
    // duration, right when each row's own dispatch fails (issue #1620
    // review). A later row's dispatch can still be running at that
    // point. The flush computes the deadline once it runs. Capturing it
    // here instead keeps this row's actual backoff close to
    // `retry_delay_ms`, whatever the rest of the chunk took.
    let mut failed_marks: Vec<(i64, String, std::time::Instant)> = Vec::new();
    let mut last_flush = std::time::Instant::now();
    for row in rows {
        // `dispatch_workflow_start_request` gets its own connection from
        // `HarvestDbPool` (issue #1620 review, Codex, third round).
        // `app_conn` above is idle for the whole call. Race the dispatch
        // future against a periodic flush deadline. A flush that becomes
        // due while this row's dispatch is still in flight then runs
        // right then, on `app_conn`, not after dispatch returns.
        let request = row.request();
        let dispatch_fut = dispatch_workflow_start_request(state, &request);
        tokio::pin!(dispatch_fut);
        let outcome = loop {
            let pending = delivered_marks.len() + failed_marks.len();
            let remaining = outbox_mark_flush_remaining(
                std::time::Instant::now(),
                pending,
                last_flush,
                failed_marks.iter().map(|(_, _, d)| *d).min(),
                claim_deadline,
            );
            tokio::select! {
                result = &mut dispatch_fut => break result,
                // `select!` evaluates this guard once, when this loop
                // iteration's call starts, not on every poll (issue #1620
                // review, Codex, fourth round). Guarding on
                // `outbox_mark_flush_due` -- true only once due -- would
                // disable the timer branch for iterations where nothing
                // is due YET. It could then never wake this select! to
                // notice a deadline that arrives while still disabled.
                // Guarding on `pending > 0` instead keeps the branch
                // armed whenever there is anything to flush. `remaining`
                // (0 if already due, otherwise time left) decides WHEN
                // the timer fires.
                () = tokio::time::sleep(remaining), if pending > 0 => {
                    delivered += flush_outbox_marks(
                        &mut app_conn,
                        &claimant,
                        &mut delivered_marks,
                        &mut failed_marks,
                        outbox_metrics.as_ref(),
                    )
                    .await
                    .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
                    last_flush = std::time::Instant::now();
                }
            }
        };
        match outcome {
            Ok(exec_id) => delivered_marks.push((row.id, exec_id)),
            Err(error) => {
                let delay_ms = retry_delay_ms(&config, &row);
                let deadline =
                    std::time::Instant::now() + std::time::Duration::from_millis(delay_ms);
                failed_marks.push((row.id, error.to_string(), deadline));
            }
        }

        let pending = delivered_marks.len() + failed_marks.len();
        if outbox_mark_flush_due(pending, last_flush.elapsed()) {
            delivered += flush_outbox_marks(
                &mut app_conn,
                &claimant,
                &mut delivered_marks,
                &mut failed_marks,
                outbox_metrics.as_ref(),
            )
            .await
            .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
            last_flush = std::time::Instant::now();
        }
    }
    delivered += flush_outbox_marks(
        &mut app_conn,
        &claimant,
        &mut delivered_marks,
        &mut failed_marks,
        outbox_metrics.as_ref(),
    )
    .await
    .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    Ok(OutboxDrainStats { claimed, delivered })
}

/// Marks one chunk's worth of dispatch outcomes and clears both buffers.
///
/// Called mid-loop, every `OUTBOX_MARK_FLUSH_EVERY` outcomes, and once
/// more after the loop for the remainder. Returns the number of rows this
/// flush durably marked delivered.
///
/// # Errors
///
/// Returns a Diesel error if either batched mark cannot be executed.
async fn flush_outbox_marks(
    conn: &mut AsyncPgConnection,
    claimant: &str,
    delivered_marks: &mut Vec<(i64, ExecutionId)>,
    failed_marks: &mut Vec<(i64, String, std::time::Instant)>,
    outbox_metrics: Option<&std::sync::Arc<dyn autumn_harvest::telemetry::MetricsRecorder>>,
) -> Result<usize, diesel::result::Error> {
    let delivered = delivered_marks.len();
    let marked_ids = mark_outbox_rows_delivered_batch(conn, claimant, delivered_marks).await?;

    // issue #618: count the "outbox" bypass EXACTLY ONCE per row. The
    // gate is THIS claimant durably marking that row delivered, i.e. the
    // batched mark's `RETURNING outbox.id` includes it. An earlier,
    // per-row form of this gate checked the mark's own affected-row
    // count instead. That count can be zero: the mark's `WHERE
    // claimed_by = $N` matches nothing when a concurrent relay reclaimed
    // the row past `claim_ttl_ms`. Counting a row absent from
    // `RETURNING` would let both this claimant AND the reclaimer that
    // actually delivers count the same committed start. Gating on set
    // membership keeps that same exactly-once guarantee for a batched
    // mark: exactly the claimant whose mark wins the row counts it. One
    // committed outbox start is one bypass, even across concurrent
    // reclaims.
    //
    // Recorded HERE, right after the delivered mark commits, and before
    // the failed-row mark below (issue #1620 review). The two marks are
    // separate statements, not one transaction. Recording the metric
    // only after BOTH marks ran would risk this: a failure in the
    // failed-row mark returns early via `?`. That would permanently
    // drop the bypass count for rows the delivered mark had already
    // durably committed. The loss is permanent because `delivered_at`
    // is no longer NULL, so no later flush ever reclaims those rows to
    // retry the count.
    if let Some(metrics) = outbox_metrics {
        let marked_id_set: std::collections::HashSet<i64> = marked_ids.into_iter().collect();
        for (id, _) in delivered_marks.iter() {
            if marked_id_set.contains(id) {
                metrics.record_admission_bypassed(
                    autumn_harvest::admission_gate::StartProducer::Outbox.as_str(),
                );
            }
        }
    }
    delivered_marks.clear();

    mark_outbox_rows_failed_batch(conn, claimant, failed_marks).await?;
    failed_marks.clear();

    Ok(delivered)
}

/// Drain all currently due workflow-start outbox rows.
///
/// The returned count is the number of rows successfully delivered.
///
/// # Errors
///
/// Returns an [`AutumnError`] when claiming or marking any outbox row fails.
pub async fn flush_workflow_start_outbox(state: &AppState) -> Result<usize, AutumnError> {
    let config = outbox_config(state);
    if !config.enabled {
        return Ok(0);
    }

    let batch_limit = config.batch_size.max(1);
    let batch_limit_usize = usize::try_from(batch_limit).unwrap_or(usize::MAX);
    let mut total = 0usize;
    loop {
        let drain = drain_workflow_start_outbox_batch(state, batch_limit).await?;
        total += drain.delivered;
        if drain.claimed < batch_limit_usize {
            break;
        }
    }

    Ok(total)
}

pub(crate) fn spawn_workflow_start_outbox_relay(
    state: AppState,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    let config = outbox_config(&state);

    tokio::spawn(async move {
        if !config.enabled {
            debug!("Harvest workflow outbox relay is disabled");
            return;
        }

        let mut interval = tokio::time::interval(Duration::from_millis(config.poll_interval_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                () = shutdown.cancelled() => {
                    debug!("Harvest workflow outbox relay shutting down");
                    break;
                }
                _ = interval.tick() => {
                    match flush_workflow_start_outbox(&state).await {
                        Ok(0) => {}
                        Ok(delivered) => {
                            debug!(delivered, "Harvest workflow outbox relay drained pending rows");
                        }
                        Err(error) => {
                            warn!(error = %error, "Harvest workflow outbox relay drain failed");
                        }
                    }
                }
            }
        }
    })
}

pub(crate) async fn dispatch_workflow_start_request(
    state: &AppState,
    request: &WorkflowStartRequest,
) -> HarvestResult<ExecutionId> {
    // issue #1353 (Codex review): `enqueue_workflow_start_outbox` below
    // already rejects a NEW empty workflow_id before persisting it. This is
    // the backstop for a row enqueued before that admission guard shipped.
    // Such a legacy row would otherwise retry with backoff forever. This
    // module has no dead-letter path for any permanent dispatch failure.
    // So reject it here too, at the one place that actually starts the
    // execution.
    if request.workflow_id.is_empty() {
        return Err(HarvestError::EmptyWorkflowId);
    }
    let harvest_pool = state.extension::<HarvestDbPool>().ok_or_else(|| {
        HarvestError::Config(
            "Harvest workflow publication is missing HarvestDbPool on AppState".into(),
        )
    })?;
    let router = state
        .extension::<ShardRouter>()
        .map(|router| router.as_ref().clone())
        .unwrap_or_default();
    let shard = router.pick_for_new_workflow(&request.workflow_name, &request.workflow_id);
    let exec_id = ExecutionId::new_for_shard(shard);
    let mut conn = harvest_pool
        .pool_for(shard)
        .get()
        .await
        .map_err(database_error)?;

    let registry_ext = state.extension::<std::sync::Arc<autumn_harvest::worker::HandlerRegistry>>();
    let (owner, runbook_url, severity, info_sla, info_retry_policy) = registry_ext
        .as_ref()
        .and_then(|registry| {
            registry.workflows.get(&request.workflow_name).map(|wf| {
                (
                    wf.owner,
                    wf.runbook_url,
                    wf.severity,
                    wf.sla,
                    wf.retry_policy.clone(),
                )
            })
        })
        .unwrap_or((None, None, None, None, None));
    // Honour the operator's server-side retry-attempt ceiling for outbox-started
    // workflows, consistent with the API/scheduler/typed start paths (issue #523).
    let max_workflow_attempts_ceiling = registry_ext
        .as_ref()
        .and_then(|registry| registry.max_workflow_attempts_ceiling);
    let sla = info_sla.and_then(|d| chrono::Duration::from_std(d).ok());

    // Issue #1243: `WorkflowStarted.input` is payload-bearing. Fall back to
    // the identity registry only when no `HandlerRegistry` extension is
    // installed at all, which never happens in a real deployment.
    let dispatch_codecs = registry_ext
        .as_ref()
        .map(|r| r.payload_codecs().clone())
        .unwrap_or_default();
    let start = start_or_load_workflow_execution_with_metrics_and_codecs(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &request.workflow_name,
            workflow_id: &request.workflow_id,
            exec_id,
            input: request.input.clone(),
            parent_id: None,
            queue_name: &request.queue_name,
            execution_timeout: None,
            memo: request.memo.clone(),
            search_attrs: request.search_attrs.clone(),
            reuse_policy: autumn_harvest::WorkflowIdReusePolicy::default(),
            conflict_policy: autumn_harvest::types::WorkflowIdConflictPolicy::Unspecified,
            trace_context: None,
            max_execution_timeout_ceiling: None,
            chain_execution_timeout: None,
            max_workflow_chain_timeout_ceiling: None,
            inherited_chain_deadline_at: None,
            concurrency_key: None,
            concurrency_limit: None,
            concurrency_on_conflict: autumn_harvest::concurrency::ConcurrencyOnConflict::Defer,
            priority: Priority::default(),
            max_workflow_input_bytes: 0,
            start_at: None,
            delay: None,
            max_workflow_start_delay: None,
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: info_retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling,
            // Outbox delivery is not a schedule fire (issue #534).
            origin: None,
            completion_callbacks: None,
            // Started by the cross-shard outbox dispatcher (issue #740).
            start_source: autumn_harvest::StartSource::Outbox,
            start_source_ref: None,
            started_by: None,
        },
        registry_ext.as_ref().map(|r| {
            r.telemetry().metrics.as_ref()
                as &(dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync)
        }),
        None,
        &dispatch_codecs,
    )
    .await?;

    // issue #618: the outbox relay is EXEMPT-BY-DESIGN from the admission gate.
    // It replays workflow-start requests that were durably committed to the
    // outbox before any gate was raised; gating them would drop already-accepted
    // in-flight work, which is the opposite of the gate contract ("halt NEW
    // starts while in-flight work drains").
    //
    // The `harvest.admission.bypassed{producer="outbox"}` count is recorded by the
    // CALLER (`drain_workflow_start_outbox_batch`), gated on the app outbox row
    // being DURABLY marked delivered (issue #618, F-round8) — NOT here. The start
    // succeeding is not the exactly-once boundary: if `mark_outbox_row_delivered`
    // then fails, the row stays eligible past its claim TTL and the retry re-enters
    // this path (`start_or_load` returns the SAME existing execution), so counting
    // here would report one committed start as multiple bypasses. Counting only
    // once the mark succeeds mirrors round 6's "gate the count on the row delete
    // actually removing the row". See `admission_gate::producer_contract`.
    Ok(start.exec_id)
}

fn outbox_config(state: &AppState) -> HarvestOutboxConfig {
    state
        .extension::<HarvestOutboxConfig>()
        .map(|config| config.as_ref().clone())
        .unwrap_or_default()
}

async fn claim_due_outbox_rows(
    conn: &mut AsyncPgConnection,
    limit: i64,
    claimant: &str,
    config: &HarvestOutboxConfig,
) -> Result<Vec<HarvestWorkflowOutboxRow>, diesel::result::Error> {
    diesel::sql_query(
        r"
        WITH due AS (
            SELECT id
            FROM harvest_workflow_outbox
            WHERE delivered_at IS NULL
              AND next_attempt_at <= NOW()
              AND (
                  claimed_at IS NULL
                  OR claimed_at < NOW() - ($1 * INTERVAL '1 millisecond')
              )
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT $2
        )
        UPDATE harvest_workflow_outbox AS outbox
        SET claimed_at = NOW(),
            claimed_by = $3
        FROM due
        WHERE outbox.id = due.id
        RETURNING outbox.*
        ",
    )
    .bind::<diesel::sql_types::BigInt, _>(i64::try_from(config.claim_ttl_ms).unwrap_or(i64::MAX))
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .bind::<diesel::sql_types::Text, _>(claimant)
    .load::<HarvestWorkflowOutboxRow>(conn)
    .await
}

#[derive(diesel::QueryableByName)]
struct MarkedIdRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
}

/// Marks every delivered row in `rows` (`(id, exec_id)` pairs) in one round
/// trip via `UNNEST`, instead of one `UPDATE` per row (issue #1620, Ledger).
/// `UNNEST` keeps the statement text fixed regardless of batch size. This
/// stays one prepared-statement shape. A literal `VALUES (...), (...), ...`
/// list would instead grow a distinct shape per batch size.
///
/// Returns the ids the `UPDATE` actually affected. A row is absent from
/// that set only in one case. This claimant lost it to a concurrent
/// reclaim past `claim_ttl_ms`. The `WHERE claimed_by = $3` guard then no
/// longer matches for that id. An earlier, per-row form of this mark
/// reported that same case as `0` affected rows. The caller uses the
/// returned set to gate the "outbox" bypass counter. That counter must
/// reflect an actual durable delivery, not a mark that updated nothing
/// for that row.
async fn mark_outbox_rows_delivered_batch(
    conn: &mut AsyncPgConnection,
    claimant: &str,
    rows: &[(i64, ExecutionId)],
) -> Result<Vec<i64>, diesel::result::Error> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
    let exec_ids: Vec<String> = rows
        .iter()
        .map(|(_, exec_id)| exec_id.to_string())
        .collect();

    let marked: Vec<MarkedIdRow> = diesel::sql_query(
        r"
        UPDATE harvest_workflow_outbox AS outbox
        SET delivery_attempts = delivery_attempts + 1,
            last_error = NULL,
            delivered_execution_id = v.exec_id,
            delivered_at = NOW(),
            claimed_at = NULL,
            claimed_by = NULL
        FROM UNNEST($1::bigint[], $2::text[]) AS v(id, exec_id)
        WHERE outbox.id = v.id
          AND outbox.claimed_by = $3
        RETURNING outbox.id
        ",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&ids)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&exec_ids)
    .bind::<diesel::sql_types::Text, _>(claimant)
    .load(conn)
    .await?;

    Ok(marked.into_iter().map(|row| row.id).collect())
}

/// Marks every failed row in `rows` (`(id, error, deadline)` triples) in
/// one round trip via `UNNEST`, instead of one `UPDATE` per row (issue
/// #1620, Ledger). Each row keeps its own already-computed retry
/// deadline. [`retry_delay_ms`] depends on that row's own
/// `delivery_attempts`, so batching does not change the per-row backoff
/// amount. `deadline` is a monotonic `Instant`, captured when that row's
/// own dispatch failed, not when this function runs. Re-deriving the
/// remaining delay from it here keeps a row's actual retry time close
/// to its configured backoff. That holds even when a later row's
/// dispatch is slow.
async fn mark_outbox_rows_failed_batch(
    conn: &mut AsyncPgConnection,
    claimant: &str,
    rows: &[(i64, String, std::time::Instant)],
) -> Result<(), diesel::result::Error> {
    if rows.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    let errors: Vec<&str> = rows.iter().map(|(_, error, _)| error.as_str()).collect();
    // Each `deadline` was computed from THIS row's own dispatch failure,
    // not from when the batch's remaining dispatches finish (issue
    // #1620 review). Re-deriving the remaining delay here, right before
    // the query, keeps that row's own backoff close to
    // `retry_delay_ms`, whatever the rest of the batch took.
    let now = std::time::Instant::now();
    let delays: Vec<i64> = rows
        .iter()
        .map(|(_, _, deadline)| {
            i64::try_from(deadline.saturating_duration_since(now).as_millis()).unwrap_or(i64::MAX)
        })
        .collect();

    diesel::sql_query(
        r"
        UPDATE harvest_workflow_outbox AS outbox
        SET delivery_attempts = delivery_attempts + 1,
            last_error = v.error,
            next_attempt_at = NOW() + (v.retry_delay_ms * INTERVAL '1 millisecond'),
            claimed_at = NULL,
            claimed_by = NULL
        FROM UNNEST($1::bigint[], $2::text[], $3::bigint[]) AS v(id, error, retry_delay_ms)
        WHERE outbox.id = v.id
          AND outbox.claimed_by = $4
        ",
    )
    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&ids)
    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&errors)
    .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(&delays)
    .bind::<diesel::sql_types::Text, _>(claimant)
    .execute(conn)
    .await
    .map(|_| ())
}

fn retry_delay_ms(config: &HarvestOutboxConfig, row: &HarvestWorkflowOutboxRow) -> u64 {
    let attempt = u32::try_from(row.delivery_attempts.max(0)).unwrap_or(u32::MAX);
    let multiplier = 1_u64 << attempt.min(16);
    config
        .base_retry_delay_ms
        .saturating_mul(multiplier)
        .min(config.max_retry_delay_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    /// This is the pure decision `outbox_mark_flush_due` makes. The drain
    /// loop's `tokio::select!` lets a flush run on it while a dispatch
    /// is still in flight (issue #1620 review, Codex, second and third
    /// rounds). Count alone cannot bound wall time -- a fixed
    /// outcome-count threshold says nothing about how long reaching it
    /// takes.
    #[test]
    fn outbox_mark_flush_due_bounds_by_count_or_by_elapsed_time() {
        use std::time::Duration;

        assert!(
            !outbox_mark_flush_due(0, Duration::from_secs(3600)),
            "nothing pending is never due, however long it has been"
        );
        assert!(
            !outbox_mark_flush_due(OUTBOX_MARK_FLUSH_EVERY - 1, Duration::ZERO),
            "under the count threshold and no time elapsed: not due"
        );
        assert!(
            outbox_mark_flush_due(OUTBOX_MARK_FLUSH_EVERY, Duration::ZERO),
            "count threshold reached: due regardless of elapsed time"
        );
        assert!(
            !outbox_mark_flush_due(
                1,
                OUTBOX_MARK_FLUSH_MAX_DELAY
                    .checked_sub(Duration::from_millis(1))
                    .expect("OUTBOX_MARK_FLUSH_MAX_DELAY is well over 1ms")
            ),
            "under the time threshold and under the count threshold: not due"
        );
        assert!(
            outbox_mark_flush_due(1, OUTBOX_MARK_FLUSH_MAX_DELAY),
            "time threshold reached: due even with only one outcome pending"
        );
    }

    /// `outbox_mark_flush_remaining` picks the soonest of its three
    /// candidate deadlines (issue #1620 review, Codex, fifth round). The
    /// fixed bound, the earliest failed-row deadline, and the claim
    /// deadline -- the last one only while something is pending.
    #[test]
    fn outbox_mark_flush_remaining_picks_the_soonest_candidate() {
        use std::time::Duration;

        let now = std::time::Instant::now();
        let last_flush = now;
        let far_future = now + Duration::from_secs(3600);

        assert_eq!(
            outbox_mark_flush_remaining(now, 0, last_flush, None, far_future),
            OUTBOX_MARK_FLUSH_MAX_DELAY,
            "nothing pending: only the fixed bound applies, claim deadline ignored"
        );
        assert_eq!(
            outbox_mark_flush_remaining(now, OUTBOX_MARK_FLUSH_EVERY, last_flush, None, far_future),
            Duration::ZERO,
            "count threshold met: due now regardless of any deadline"
        );
        let near_failed_deadline = now + Duration::from_millis(10);
        assert_eq!(
            outbox_mark_flush_remaining(now, 1, last_flush, Some(near_failed_deadline), far_future),
            Duration::from_millis(10),
            "a failed row's own deadline beats the fixed 250ms bound when sooner"
        );
        let near_claim_deadline = now + Duration::from_millis(5);
        assert_eq!(
            outbox_mark_flush_remaining(now, 1, last_flush, None, near_claim_deadline),
            Duration::from_millis(5),
            "the claim deadline beats the fixed bound when sooner, and pending > 0"
        );
    }

    /// Ten percent of the TTL, capped at one second (issue #1620 review,
    /// Codex, seventh round). Never more than the TTL itself, so
    /// `claim_ttl_ms.saturating_sub(margin)` never underflows to a
    /// deadline before `claim_started_at`.
    #[test]
    fn claim_flush_margin_ms_is_a_tenth_of_ttl_capped_at_one_second() {
        assert_eq!(claim_flush_margin_ms(30_000), 1_000, "default TTL: capped");
        assert_eq!(claim_flush_margin_ms(5_000), 500, "under the cap: a tenth");
        assert_eq!(
            claim_flush_margin_ms(5),
            0,
            "a tenth rounds down to zero for a tiny TTL, never exceeding it"
        );
        assert_eq!(
            claim_flush_margin_ms(1),
            0,
            "the minimum valid TTL: zero margin, not underflow"
        );
    }

    #[test]
    fn retry_delay_caps_growth() {
        let config = HarvestOutboxConfig {
            base_retry_delay_ms: 1_000,
            max_retry_delay_ms: 10_000,
            ..HarvestOutboxConfig::default()
        };
        let row = HarvestWorkflowOutboxRow {
            id: 1,
            workflow_name: "user_onboarding".to_owned(),
            workflow_id: "user-onboarding:1".to_owned(),
            queue_name: "default".to_owned(),
            input: Value::Null,
            memo: None,
            search_attrs: None,
            delivery_attempts: 8,
            last_error: None,
            delivered_execution_id: None,
            delivered_at: None,
            next_attempt_at: Utc::now().naive_utc(),
            claimed_at: None,
            claimed_by: None,
            created_at: Utc::now().naive_utc(),
        };

        assert_eq!(retry_delay_ms(&config, &row), 10_000);
    }

    /// issue #1353 (Codex review, pure, no DB): `dispatch_workflow_start_request`
    /// rejects an empty `workflow_id` before touching any state extension.
    /// This test therefore runs against a bare `AppState` with nothing
    /// installed. A caller of the public `enqueue_workflow_start_outbox`
    /// cannot bypass the guard that the HTTP start routes apply.
    #[tokio::test]
    async fn dispatch_rejects_empty_workflow_id() {
        let state = AppState::for_test();
        let request = WorkflowStartRequest {
            workflow_name: "user_onboarding".to_owned(),
            workflow_id: String::new(),
            queue_name: "default".to_owned(),
            input: Value::Null,
            memo: None,
            search_attrs: None,
        };

        let err = dispatch_workflow_start_request(&state, &request)
            .await
            .expect_err("empty workflow_id must be rejected");
        assert!(
            err.to_string().contains("workflow_id must not be empty"),
            "unexpected error: {err}"
        );
    }

    /// DB test for the batched mark (issue #618, issue #1620, Ledger).
    /// `mark_outbox_rows_delivered_batch` surfaces the ids it actually
    /// affected. The caller gates the bypass counter on that set, so it
    /// reflects an actual durable delivery. A row claimed by a different
    /// claimant is absent from that set. A concurrent reclaimer took it
    /// past `claim_ttl_ms`. An owned row is present. This also proves the
    /// batch does not cross-contaminate. Two rows in the SAME call, one
    /// owned and one not, must resolve independently. Two OWNED rows in
    /// the same call must each keep their own `exec_id`. Neither may take
    /// the other's -- the risk a naive `UNNEST` row-pairing bug would
    /// introduce. Runs against `HARVEST_TEST_DATABASE_URL` when set, and
    /// skips otherwise. Executed against a real local Postgres in CI's
    /// Docker-backed step.
    #[tokio::test]
    async fn mark_outbox_rows_delivered_batch_reports_affected_ids_and_keeps_rows_distinct() {
        use diesel_async::AsyncConnection;

        async fn insert_claimed(
            conn: &mut AsyncPgConnection,
            workflow_id: &str,
            claimant: &str,
        ) -> i64 {
            #[derive(diesel::QueryableByName)]
            struct IdRow {
                #[diesel(sql_type = diesel::sql_types::BigInt)]
                id: i64,
            }
            diesel::sql_query(
                "INSERT INTO harvest_workflow_outbox
                    (workflow_name, workflow_id, queue_name, input, claimed_by, claimed_at)
                 VALUES ('r1620_wf', $1, 'default', '{}'::jsonb, $2, NOW())
                 RETURNING id",
            )
            .bind::<diesel::sql_types::Text, _>(workflow_id)
            .bind::<diesel::sql_types::Text, _>(claimant)
            .get_result::<IdRow>(conn)
            .await
            .expect("insert claimed outbox row")
            .id
        }

        #[derive(diesel::QueryableByName)]
        struct DeliveredRow {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            delivered_execution_id: Option<String>,
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            claimed_by: Option<String>,
        }

        let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
            return;
        };
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to test DB");

        // Two rows: one owned by worker-A, one claimed by a concurrent worker-B.
        let owned_id = insert_claimed(&mut conn, "r1620-owned", "worker-A").await;
        let lost_id = insert_claimed(&mut conn, "r1620-lost", "worker-B").await;

        let owned_exec = ExecutionId::new();
        let lost_exec = ExecutionId::new();

        let marked = mark_outbox_rows_delivered_batch(
            &mut conn,
            "worker-A",
            &[(owned_id, owned_exec), (lost_id, lost_exec)],
        )
        .await
        .expect("batch mark");

        assert_eq!(
            marked,
            vec![owned_id],
            "only the row worker-A actually owns is affected by its mark"
        );

        let owned_row: DeliveredRow = diesel::sql_query(
            "SELECT delivered_execution_id, claimed_by FROM harvest_workflow_outbox WHERE id = $1",
        )
        .bind::<diesel::sql_types::BigInt, _>(owned_id)
        .get_result(&mut conn)
        .await
        .expect("read owned row");
        assert_eq!(
            owned_row.delivered_execution_id.as_deref(),
            Some(owned_exec.to_string().as_str()),
            "the owned row gets its OWN exec_id, not the other row's"
        );
        assert_eq!(owned_row.claimed_by, None, "delivery clears the claim");

        let lost_row: DeliveredRow = diesel::sql_query(
            "SELECT delivered_execution_id, claimed_by FROM harvest_workflow_outbox WHERE id = $1",
        )
        .bind::<diesel::sql_types::BigInt, _>(lost_id)
        .get_result(&mut conn)
        .await
        .expect("read lost row");
        assert_eq!(
            lost_row.delivered_execution_id, None,
            "a row worker-A does not own is left completely untouched"
        );
        assert_eq!(
            lost_row.claimed_by.as_deref(),
            Some("worker-B"),
            "the reclaimer's own claim on the untouched row survives"
        );

        diesel::sql_query("DELETE FROM harvest_workflow_outbox WHERE id = ANY($1)")
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(vec![owned_id, lost_id])
            .execute(&mut conn)
            .await
            .expect("cleanup");
    }

    /// Companion to the delivered-batch test above, for the failed path.
    /// Two owned rows in the SAME batched call must each keep their own
    /// `error` and their own `retry_delay_ms`, not the other row's
    /// (issue #1620, Ledger).
    #[tokio::test]
    async fn mark_outbox_rows_failed_batch_keeps_rows_distinct() {
        use diesel_async::AsyncConnection;

        async fn insert_claimed(conn: &mut AsyncPgConnection, workflow_id: &str) -> i64 {
            #[derive(diesel::QueryableByName)]
            struct IdRow {
                #[diesel(sql_type = diesel::sql_types::BigInt)]
                id: i64,
            }
            diesel::sql_query(
                "INSERT INTO harvest_workflow_outbox
                    (workflow_name, workflow_id, queue_name, input, claimed_by, claimed_at)
                 VALUES ('r1620_wf', $1, 'default', '{}'::jsonb, 'worker-A', NOW())
                 RETURNING id",
            )
            .bind::<diesel::sql_types::Text, _>(workflow_id)
            .get_result::<IdRow>(conn)
            .await
            .expect("insert claimed outbox row")
            .id
        }

        #[derive(diesel::QueryableByName)]
        struct FailedRow {
            #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
            last_error: Option<String>,
            #[diesel(sql_type = diesel::sql_types::Timestamp)]
            next_attempt_at: chrono::NaiveDateTime,
        }

        let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
            return;
        };
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to test DB");

        let short_id = insert_claimed(&mut conn, "r1620-short-delay").await;
        let long_id = insert_claimed(&mut conn, "r1620-long-delay").await;

        let now = std::time::Instant::now();
        mark_outbox_rows_failed_batch(
            &mut conn,
            "worker-A",
            &[
                (
                    short_id,
                    "boom-short".to_owned(),
                    now + std::time::Duration::from_secs(1),
                ),
                (
                    long_id,
                    "boom-long".to_owned(),
                    now + std::time::Duration::from_secs(100),
                ),
            ],
        )
        .await
        .expect("batch mark");

        let short_row: FailedRow = diesel::sql_query(
            "SELECT last_error, next_attempt_at FROM harvest_workflow_outbox WHERE id = $1",
        )
        .bind::<diesel::sql_types::BigInt, _>(short_id)
        .get_result(&mut conn)
        .await
        .expect("read short-delay row");
        let long_row: FailedRow = diesel::sql_query(
            "SELECT last_error, next_attempt_at FROM harvest_workflow_outbox WHERE id = $1",
        )
        .bind::<diesel::sql_types::BigInt, _>(long_id)
        .get_result(&mut conn)
        .await
        .expect("read long-delay row");

        assert_eq!(short_row.last_error.as_deref(), Some("boom-short"));
        assert_eq!(long_row.last_error.as_deref(), Some("boom-long"));
        assert!(
            long_row.next_attempt_at > short_row.next_attempt_at,
            "the row given the longer retry delay must retry later than the \
             row given the shorter one -- proves the delay wasn't swapped \
             between rows by the batched UPDATE"
        );

        diesel::sql_query("DELETE FROM harvest_workflow_outbox WHERE id = ANY($1)")
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(vec![short_id, long_id])
            .execute(&mut conn)
            .await
            .expect("cleanup");
    }

    async fn flush_outbox_marks_insert_claimed_row(
        conn: &mut AsyncPgConnection,
        workflow_id: &str,
    ) -> i64 {
        #[derive(diesel::QueryableByName)]
        struct IdRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            id: i64,
        }
        diesel::sql_query(
            "INSERT INTO harvest_workflow_outbox
                (workflow_name, workflow_id, queue_name, input, claimed_by, claimed_at)
             VALUES ('r1620_wf', $1, 'default', '{}'::jsonb, 'worker-A', NOW())
             RETURNING id",
        )
        .bind::<diesel::sql_types::Text, _>(workflow_id)
        .get_result::<IdRow>(conn)
        .await
        .expect("insert claimed outbox row")
        .id
    }

    #[derive(diesel::QueryableByName)]
    struct FlushOutboxMarksState {
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        delivered_execution_id: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        last_error: Option<String>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
        claimed_by: Option<String>,
    }

    async fn flush_outbox_marks_one_pair(
        conn: &mut AsyncPgConnection,
        ok_id: i64,
        err_id: i64,
        error: &str,
    ) {
        let mut delivered_marks = vec![(ok_id, ExecutionId::new())];
        let mut failed_marks = vec![(
            err_id,
            error.to_owned(),
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )];
        let flushed = flush_outbox_marks(
            conn,
            "worker-A",
            &mut delivered_marks,
            &mut failed_marks,
            None,
        )
        .await
        .expect("flush chunk");
        assert_eq!(flushed, 1, "chunk marks its one delivered row");
        assert!(
            delivered_marks.is_empty(),
            "delivered buffer clears after flush"
        );
        assert!(failed_marks.is_empty(), "failed buffer clears after flush");
    }

    /// Exercises `flush_outbox_marks` the way the chunked drain loop calls
    /// it: once per chunk, not once for the whole batch. A slow later
    /// dispatch must not hold an earlier row's claim past
    /// `OUTBOX_MARK_FLUSH_EVERY` outcomes (issue #1620 review, Codex).
    /// Two sequential chunks, each with one delivered and one failed
    /// row, must mark all four rows and leave both buffers empty after
    /// every call.
    #[tokio::test]
    async fn flush_outbox_marks_applies_each_chunk_and_clears_its_buffers() {
        use diesel_async::AsyncConnection;

        let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
            return;
        };
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to test DB");

        let chunk1_ok = flush_outbox_marks_insert_claimed_row(&mut conn, "r1620-chunk1-ok").await;
        let chunk1_err = flush_outbox_marks_insert_claimed_row(&mut conn, "r1620-chunk1-err").await;
        let chunk2_ok = flush_outbox_marks_insert_claimed_row(&mut conn, "r1620-chunk2-ok").await;
        let chunk2_err = flush_outbox_marks_insert_claimed_row(&mut conn, "r1620-chunk2-err").await;

        flush_outbox_marks_one_pair(&mut conn, chunk1_ok, chunk1_err, "boom-1").await;
        flush_outbox_marks_one_pair(&mut conn, chunk2_ok, chunk2_err, "boom-2").await;

        for (id, expect_delivered) in [
            (chunk1_ok, true),
            (chunk1_err, false),
            (chunk2_ok, true),
            (chunk2_err, false),
        ] {
            let row: FlushOutboxMarksState = diesel::sql_query(
                "SELECT delivered_execution_id, last_error, claimed_by \
                 FROM harvest_workflow_outbox WHERE id = $1",
            )
            .bind::<diesel::sql_types::BigInt, _>(id)
            .get_result(&mut conn)
            .await
            .expect("read row");
            assert_eq!(
                row.claimed_by, None,
                "row {id} claim is released, whichever chunk marked it"
            );
            if expect_delivered {
                assert!(
                    row.delivered_execution_id.is_some(),
                    "row {id} should be marked delivered"
                );
            } else {
                assert!(row.last_error.is_some(), "row {id} should be marked failed");
            }
        }

        diesel::sql_query("DELETE FROM harvest_workflow_outbox WHERE id = ANY($1)")
            .bind::<diesel::sql_types::Array<diesel::sql_types::BigInt>, _>(vec![
                chunk1_ok, chunk1_err, chunk2_ok, chunk2_err,
            ])
            .execute(&mut conn)
            .await
            .expect("cleanup");
    }
}
