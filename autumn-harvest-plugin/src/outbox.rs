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
use autumn_harvest::types::ExecutionId;
use autumn_harvest::{StartWorkflowParams, start_or_load_workflow_execution};

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

    let claimed = rows.len();
    let mut delivered = 0usize;
    for row in rows {
        match dispatch_workflow_start_request(state, &row.request()).await {
            Ok(exec_id) => {
                mark_outbox_row_delivered(&mut app_conn, row.id, &claimant, exec_id)
                    .await
                    .map_err(|error| AutumnError::service_unavailable_msg(error.to_string()))?;
                delivered += 1;
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
    let mut conn = harvest_pool.get().await.map_err(database_error)?;

    let start = start_or_load_workflow_execution(
        &mut conn,
        StartWorkflowParams {
            workflow_name: &request.workflow_name,
            workflow_id: &request.workflow_id,
            shard_id: 0,
            input: request.input.clone(),
            parent_id: None,
            queue_name: &request.queue_name,
            execution_timeout: None,
            memo: request.memo.clone(),
            search_attrs: request.search_attrs.clone(),
        },
    )
    .await?;

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

async fn mark_outbox_row_delivered(
    conn: &mut AsyncPgConnection,
    row_id: i64,
    claimant: &str,
    exec_id: ExecutionId,
) -> Result<(), diesel::result::Error> {
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
    .map(|_| ())
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
}
