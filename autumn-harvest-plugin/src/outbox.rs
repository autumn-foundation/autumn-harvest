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
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution_with_metrics};

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
/// Returns a Diesel error if the outbox insert cannot be executed.
pub async fn enqueue_workflow_start_outbox(
    conn: &mut AsyncPgConnection,
    request: &WorkflowStartRequest,
) -> Result<(), diesel::result::Error> {
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
    let rows = claim_due_outbox_rows(&mut app_conn, limit.max(1), &claimant, &config)
        .await
        .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;

    // issue #618, F-round8: the metrics recorder for the exempt-with-bypass-counter
    // "outbox" producer. Fetched once; the bypass is counted per row only AFTER the
    // app outbox row is durably marked delivered (see below).
    let outbox_metrics = state
        .extension::<std::sync::Arc<autumn_harvest::worker::HandlerRegistry>>()
        .map(|registry| std::sync::Arc::clone(&registry.telemetry().metrics));

    let claimed = rows.len();
    let mut delivered = 0usize;
    for row in rows {
        match dispatch_workflow_start_request(state, &row.request()).await {
            Ok(exec_id) => {
                let marked = mark_outbox_row_delivered(&mut app_conn, row.id, &claimant, exec_id)
                    .await
                    .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
                delivered += 1;
                // issue #618, F-round8 + F-round11: count the "outbox" bypass EXACTLY
                // ONCE, gated on THIS claimant actually, durably marking the row
                // delivered — i.e. the UPDATE affected its own claimed row (`marked ==
                // 1`). F-round8 gated on the mark returning `Ok`, but the mark's
                // `WHERE claimed_by = $2` can affect 0 rows (returning `Ok(0)`) when a
                // concurrent relay reclaimed the row past `claim_ttl_ms`; counting on
                // that 0-row `Ok` would let both this claimant AND the reclaimer that
                // actually delivers count the same committed start. Gating on `marked
                // == 1` makes exactly the claimant whose mark wins the row count it,
                // so one committed outbox start is one bypass across concurrent
                // reclaims. Mirrors round 6/7's "gate the count on the delete/UPDATE
                // actually affecting the row".
                if outbox_bypass_should_count(marked)
                    && let Some(metrics) = outbox_metrics.as_ref()
                {
                    metrics.record_admission_bypassed(
                        autumn_harvest::admission_gate::StartProducer::Outbox.as_str(),
                    );
                }
            }
            Err(error) => {
                mark_outbox_row_failed(&mut app_conn, &row, &claimant, &config, &error.to_string())
                    .await
                    .map_err(|db_error| {
                        AutumnError::service_unavailable_msg(db_error.to_string())
                    })?;
            }
        }
    }

    Ok(OutboxDrainStats { claimed, delivered })
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

    let start = start_or_load_workflow_execution_with_metrics(
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
            trace_context: None,
            max_execution_timeout_ceiling: None,
            concurrency_key: None,
            concurrency_limit: None,
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
            start_source: autumn_harvest::StartSource::Api,
            start_source_ref: None,
            started_by: None,
        },
        registry_ext.as_ref().map(|r| {
            r.telemetry().metrics.as_ref()
                as &(dyn autumn_harvest::telemetry::MetricsRecorder + Send + Sync)
        }),
        None,
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

/// Whether a `mark_outbox_row_delivered` result should count an "outbox" admission
/// bypass (issue #618, F-round11). Exactly-once per committed start: count only when
/// THIS claimant durably marked its own claimed row — i.e. the UPDATE affected
/// exactly one row (`== 1`). A `0` means the row was reclaimed by a concurrent relay
/// past `claim_ttl_ms` (the `WHERE claimed_by` guard matched nothing), so the
/// reclaimer that actually delivers is the one that counts — not this claimant.
const fn outbox_bypass_should_count(marked_rows: usize) -> bool {
    marked_rows == 1
}

/// Marks an outbox row delivered. Returns the number of rows actually updated
/// (0 or 1 — `id` is the PK). It is **0** when this claimant lost the row to a
/// concurrent reclaim past `claim_ttl_ms` (the `WHERE claimed_by = $2` guard no
/// longer matches); the affected count is surfaced (issue #618, F-round11) so the
/// caller can gate the "outbox" bypass counter on an actual durable delivery
/// (`== 1`) rather than on a mark that reported `Ok` but updated nothing.
async fn mark_outbox_row_delivered(
    conn: &mut AsyncPgConnection,
    row_id: i64,
    claimant: &str,
    exec_id: ExecutionId,
) -> Result<usize, diesel::result::Error> {
    diesel::sql_query(
        r"
        UPDATE harvest_workflow_outbox
        SET delivery_attempts = delivery_attempts + 1,
            last_error = NULL,
            delivered_execution_id = $3,
            delivered_at = NOW(),
            claimed_at = NULL,
            claimed_by = NULL
        WHERE id = $1
          AND claimed_by = $2
        ",
    )
    .bind::<diesel::sql_types::BigInt, _>(row_id)
    .bind::<diesel::sql_types::Text, _>(claimant)
    .bind::<diesel::sql_types::Text, _>(exec_id.to_string())
    .execute(conn)
    .await
}

async fn mark_outbox_row_failed(
    conn: &mut AsyncPgConnection,
    row: &HarvestWorkflowOutboxRow,
    claimant: &str,
    config: &HarvestOutboxConfig,
    error: &str,
) -> Result<(), diesel::result::Error> {
    let retry_delay_ms = i64::try_from(retry_delay_ms(config, row)).unwrap_or(i64::MAX);

    diesel::sql_query(
        r"
        UPDATE harvest_workflow_outbox
        SET delivery_attempts = delivery_attempts + 1,
            last_error = $3,
            next_attempt_at = NOW() + ($4 * INTERVAL '1 millisecond'),
            claimed_at = NULL,
            claimed_by = NULL
        WHERE id = $1
          AND claimed_by = $2
        ",
    )
    .bind::<diesel::sql_types::BigInt, _>(row.id)
    .bind::<diesel::sql_types::Text, _>(claimant)
    .bind::<diesel::sql_types::Text, _>(error)
    .bind::<diesel::sql_types::BigInt, _>(retry_delay_ms)
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

    /// F-round11 (pure, no DB): the "outbox" bypass is counted for EXACTLY a 1-row
    /// mark, and skipped for a 0-row mark (this claimant lost the row to a concurrent
    /// reclaim) — so one committed start is one bypass across concurrent reclaims.
    #[test]
    fn outbox_bypass_counted_only_for_a_one_row_mark() {
        assert!(
            !outbox_bypass_should_count(0),
            "a 0-row mark (lost the row to a reclaim) must NOT count the bypass"
        );
        assert!(
            outbox_bypass_should_count(1),
            "the claimant that durably marks its own row (1 row) counts exactly once"
        );
        // Defensive: id is the PK so >1 is impossible, but never count it as one start.
        assert!(!outbox_bypass_should_count(2));
    }

    /// F-round11 (DB): `mark_outbox_row_delivered` surfaces the affected-row count so
    /// the caller can gate the bypass counter on an actual durable delivery. A mark by
    /// a claimant that does NOT own the row (a concurrent reclaimer took it past
    /// `claim_ttl_ms`) affects 0 rows (the `WHERE claimed_by` guard matches nothing);
    /// the owning claimant's mark affects exactly 1. Runs against
    /// `HARVEST_TEST_DATABASE_URL` when set (skips otherwise); executed against a real
    /// local Postgres in CI's Docker-backed step.
    #[tokio::test]
    async fn mark_outbox_row_delivered_reports_affected_rows() {
        use diesel_async::AsyncConnection;

        #[derive(diesel::QueryableByName)]
        struct IdRow {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            id: i64,
        }

        let Ok(url) = std::env::var("HARVEST_TEST_DATABASE_URL") else {
            eprintln!("SKIP: HARVEST_TEST_DATABASE_URL unset");
            return;
        };
        let mut conn = AsyncPgConnection::establish(&url)
            .await
            .expect("connect to test DB");

        // An outbox row already CLAIMED by worker-A.
        let row_id: i64 = diesel::sql_query(
            "INSERT INTO harvest_workflow_outbox
                (workflow_name, workflow_id, queue_name, input, claimed_by, claimed_at)
             VALUES ('r11_wf', 'r11-mark', 'default', '{}'::jsonb, 'worker-A', NOW())
             RETURNING id",
        )
        .get_result::<IdRow>(&mut conn)
        .await
        .expect("insert claimed outbox row")
        .id;

        let exec = ExecutionId::new();

        // (a) worker-B (a reclaimer that does NOT own the row) → 0 rows affected.
        let lost = mark_outbox_row_delivered(&mut conn, row_id, "worker-B", exec)
            .await
            .expect("mark (non-owner)");
        assert_eq!(
            lost, 0,
            "a mark by a non-owning claimant affects 0 rows (it lost the reclaim)"
        );
        assert!(
            !outbox_bypass_should_count(lost),
            "a 0-row mark must not count the bypass"
        );

        // (b) worker-A (the owner) → exactly 1 row affected.
        let won = mark_outbox_row_delivered(&mut conn, row_id, "worker-A", exec)
            .await
            .expect("mark (owner)");
        assert_eq!(won, 1, "the owning claimant's mark affects exactly 1 row");
        assert!(
            outbox_bypass_should_count(won),
            "a 1-row mark counts the bypass exactly once"
        );

        diesel::sql_query("DELETE FROM harvest_workflow_outbox WHERE id = $1")
            .bind::<diesel::sql_types::BigInt, _>(row_id)
            .execute(&mut conn)
            .await
            .expect("cleanup");
    }
}
