#![allow(
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use crate::debounce::DebounceStartOptions;
use crate::error::{HarvestError, HarvestResult};
use crate::types::ExecutionId;
use chrono::{DateTime, Utc};
#[cfg(feature = "db")]
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchPolicy {
    pub key_expr: String,
    pub max_size: usize,
    pub max_wait: Duration,
}

pub struct AdmitBatchParams {
    pub workflow_name: String,
    pub batch_key: String,
    pub workflow_id: String,
    pub queue_name: String,
    pub payload: serde_json::Value,
    pub start_options: DebounceStartOptions,
    pub max_wait: Duration,
    pub max_size: usize,
    pub shard_id: i32,
}

#[derive(Debug, Clone)]
pub struct BatchAdmitOutcome {
    pub batch_key: String,
    pub workflow_id: String,
    pub fire_at: DateTime<Utc>,
    pub pending_count: i32,
    pub max_size: usize,
    pub is_flushed: bool,
    pub flushed_execution_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PendingEventBatchRecord {
    pub id: uuid::Uuid,
    pub workflow_name: String,
    pub batch_key: String,
    pub workflow_id: String,
    pub queue_name: String,
    pub current_size: i32,
    pub max_size: i32,
    pub fire_at: DateTime<Utc>,
    pub shard_id: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName, Debug)]
struct UpsertBatchRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    buffered_payloads: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    start_options: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    fire_at: DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    max_size: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    shard_id: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    current_size: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    buffered_bytes: i32,
}

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName, Debug)]
#[allow(dead_code)]
struct FireDueBatchRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    batch_key: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    buffered_payloads: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    start_options: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Timestamptz)]
    fire_at: DateTime<Utc>,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    max_size: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    shard_id: i32,
}

#[cfg(feature = "db")]
pub async fn admit_batched_start(
    conn: &mut AsyncPgConnection,
    params: AdmitBatchParams,
) -> HarvestResult<Option<(BatchAdmitOutcome, Vec<crate::completion_trigger::DeferredTriggerStart>)>> {
    use diesel_async::AsyncConnection;

    if params.batch_key.len() > 1024 {
        return Err(HarvestError::Config(format!(
            "batch key length {} bytes exceeds maximum 1024 bytes",
            params.batch_key.len()
        )));
    }

    let now = Utc::now();
    let max_wait_chrono = chrono::Duration::from_std(params.max_wait)
        .unwrap_or_else(|_| chrono::Duration::days(365 * 100));
    let initial_fire_at = now.checked_add_signed(max_wait_chrono).unwrap_or(now);

    let new_id = uuid::Uuid::new_v4();
    let start_options_json = serde_json::to_value(&params.start_options)
        .unwrap_or_else(|_| serde_json::Value::Object(serde_json::Map::new()));

    let sql = "
        INSERT INTO harvest_event_batches
            (id, workflow_name, batch_key, workflow_id, queue_name,
             buffered_payloads, start_options, fire_at, max_size, shard_id,
             created_at, updated_at)
        VALUES
            ($1, $2, $3, $4, $5, jsonb_build_array($6), $7, $8, $9, $10, NOW(), NOW())
        ON CONFLICT (workflow_name, batch_key) DO UPDATE SET
            buffered_payloads = harvest_event_batches.buffered_payloads || EXCLUDED.buffered_payloads,
            fire_at           = LEAST(harvest_event_batches.fire_at, EXCLUDED.fire_at),
            updated_at        = NOW()
        RETURNING
            id,
            workflow_name,
            workflow_id,
            queue_name,
            CASE WHEN jsonb_array_length(buffered_payloads) >= max_size THEN buffered_payloads ELSE NULL END AS buffered_payloads,
            start_options,
            fire_at,
            max_size,
            shard_id,
            jsonb_array_length(buffered_payloads) AS current_size,
            octet_length(buffered_payloads::text) AS buffered_bytes
    ";

    let workflow_name = params.workflow_name;
    let batch_key = params.batch_key;
    let workflow_id = params.workflow_id;
    let queue_name = params.queue_name;
    let shard_id = params.shard_id;
    let payload = params.payload;
    let max_size = params.max_size;

    let outcome = conn
        .transaction::<(BatchAdmitOutcome, Vec<crate::completion_trigger::DeferredTriggerStart>), HarvestError, _>(|conn| {
            Box::pin(async move {
                let row: UpsertBatchRow = diesel::sql_query(sql)
                    .bind::<diesel::sql_types::Uuid, _>(new_id)
                    .bind::<diesel::sql_types::Text, _>(&workflow_name)
                    .bind::<diesel::sql_types::Text, _>(&batch_key)
                    .bind::<diesel::sql_types::Text, _>(&workflow_id)
                    .bind::<diesel::sql_types::Text, _>(&queue_name)
                    .bind::<diesel::sql_types::Jsonb, _>(payload)
                    .bind::<diesel::sql_types::Jsonb, _>(start_options_json)
                    .bind::<diesel::sql_types::Timestamptz, _>(initial_fire_at)
                    .bind::<diesel::sql_types::Integer, _>(max_size as i32)
                    .bind::<diesel::sql_types::Integer, _>(shard_id)
                    .get_result(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                let opts: DebounceStartOptions =
                    serde_json::from_value(row.start_options.clone()).unwrap_or_default();

                let max_allowed_bytes = opts.max_workflow_input_bytes.unwrap_or(10 * 1024 * 1024);
                if row.buffered_bytes > max_allowed_bytes as i32 {
                    return Err(HarvestError::PayloadTooLarge {
                        kind: crate::error::PayloadKind::WorkflowInput,
                        observed_bytes: row.buffered_bytes as u64,
                        cap_bytes: max_allowed_bytes,
                        workflow_type: workflow_name,
                        activity_name: None,
                    });
                }

                let is_flushed = row.current_size >= row.max_size;
                let (flushed_execution_id, pending_deferred) = if is_flushed {
                    let exec_id =
                        ExecutionId::new_for_shard(crate::types::ShardId::new(row.shard_id));
                    let reuse_policy = opts
                        .reuse_policy
                        .as_deref()
                        .and_then(parse_reuse_policy)
                        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);

                    let execution_timeout = opts
                        .execution_timeout_secs
                        .and_then(chrono::Duration::try_seconds);
                    let sla = opts.sla_secs.and_then(chrono::Duration::try_seconds);
                    let max_execution_timeout_ceiling = opts
                        .max_execution_timeout_ceiling_secs
                        .and_then(chrono::Duration::try_seconds);
                    let priority = opts
                        .priority
                        .and_then(crate::types::Priority::from_i32)
                        .unwrap_or_default();

                    let params = crate::execution::StartWorkflowParams {
                        workflow_name: &row.workflow_name,
                        workflow_id: &row.workflow_id,
                        exec_id,
                        input: row.buffered_payloads.unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
                        parent_id: None,
                        queue_name: &row.queue_name,
                        execution_timeout,
                        memo: opts.memo,
                        search_attrs: opts.search_attrs,
                        reuse_policy,
                        trace_context: opts.trace_context,
                        max_execution_timeout_ceiling,
                        concurrency_key: opts.concurrency_key,
                        concurrency_limit: opts.concurrency_limit,
                        priority,
                        max_workflow_input_bytes: opts.max_workflow_input_bytes.unwrap_or(u64::MAX),
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner: opts.owner.as_deref(),
                        runbook_url: opts.runbook_url.as_deref(),
                        severity: opts.severity.as_deref(),
                        context_headers: opts.context_headers,
                        sla,
                        schedule_id: None,
                        scheduled_for: None,
                    };

                    let (started, deferred_starts) =
                        crate::execution::start_or_load_workflow_execution_collect(
                            conn, params, true, false,
                        )
                        .await?;

                    diesel::sql_query("DELETE FROM harvest_event_batches WHERE id = $1")
                        .bind::<diesel::sql_types::Uuid, _>(row.id)
                        .execute(conn)
                        .await
                        .map_err(crate::error::database_error)?;

                    (Some(started.exec_id.to_string()), deferred_starts)
                } else {
                    (None, Vec::new())
                };

                Ok((
                    BatchAdmitOutcome {
                        batch_key,
                        workflow_id: row.workflow_id,
                        fire_at: row.fire_at,
                        pending_count: row.current_size,
                        max_size: row.max_size as usize,
                        is_flushed,
                        flushed_execution_id,
                    },
                    pending_deferred,
                ))
            })
        })
        .await;

    outcome.map(Some)
}

#[cfg(feature = "db")]
const fn is_transient_error(e: &HarvestError) -> bool {
    matches!(e, HarvestError::Database(_) | HarvestError::Timeout { .. })
}

#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    shard_id: Option<i32>,
) -> HarvestResult<(Vec<String>, Vec<crate::completion_trigger::DeferredTriggerStart>)> {
    use diesel_async::{AsyncConnection, RunQueryDsl};

    #[derive(diesel::QueryableByName)]
    struct TableExists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let exists: TableExists =
        diesel::sql_query("SELECT to_regclass('harvest_event_batches') IS NOT NULL AS present")
            .get_result(conn)
            .await
            .map_err(crate::error::database_error)?;
    if !exists.present {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut fired_ids = Vec::new();
    let mut deferred_starts = Vec::new();

    let due_sql = if shard_id.is_some() {
        "
            SELECT id, workflow_name, batch_key, workflow_id, queue_name,
                   buffered_payloads, start_options, fire_at, max_size, shard_id
            FROM harvest_event_batches
            WHERE fire_at <= $1 AND shard_id = $3
            ORDER BY fire_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        "
    } else {
        "
            SELECT id, workflow_name, batch_key, workflow_id, queue_name,
                   buffered_payloads, start_options, fire_at, max_size, shard_id
            FROM harvest_event_batches
            WHERE fire_at <= $1
            ORDER BY fire_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        "
    };

    loop {
        let now = Utc::now();
        let processed: Option<(String, Vec<crate::completion_trigger::DeferredTriggerStart>)> = conn
            .transaction(|conn| {
                Box::pin(async move {
                    let due_rows: Vec<FireDueBatchRow> = if let Some(sid) = shard_id {
                        diesel::sql_query(due_sql)
                            .bind::<diesel::sql_types::Timestamptz, _>(now)
                            .bind::<diesel::sql_types::BigInt, _>(1i64)
                            .bind::<diesel::sql_types::Integer, _>(sid)
                            .load(conn)
                            .await
                            .map_err(crate::error::database_error)?
                    } else {
                        diesel::sql_query(due_sql)
                            .bind::<diesel::sql_types::Timestamptz, _>(now)
                            .bind::<diesel::sql_types::BigInt, _>(1i64)
                            .load(conn)
                            .await
                            .map_err(crate::error::database_error)?
                    };

                    if let Some(row) = due_rows.into_iter().next() {
                        match fire_claimed_batch_row(conn, row).await {
                            Ok(Some((exec_id, deferred))) => {
                                Ok(Some((exec_id, deferred)))
                            }
                            Ok(None) => {
                                Ok(None)
                            }
                            Err(e) => {
                                Err(e)
                            }
                        }
                    } else {
                        Ok(None)
                    }
                })
            })
            .await?;

        if let Some((exec_id, deferred)) = processed {
            fired_ids.push(exec_id);
            deferred_starts.extend(deferred);
        } else {
            break;
        }
    }

    Ok((fired_ids, deferred_starts))
}

#[cfg(feature = "db")]
async fn fire_claimed_batch_row(
    conn: &mut diesel_async::AsyncPgConnection,
    row: FireDueBatchRow,
) -> HarvestResult<Option<(String, Vec<crate::completion_trigger::DeferredTriggerStart>)>> {
    use diesel_async::RunQueryDsl;

    let opts: DebounceStartOptions = serde_json::from_value(row.start_options).unwrap_or_default();

    let shard = crate::types::ShardId::new(row.shard_id);
    let exec_id = crate::types::ExecutionId::new_for_shard(shard);

    let reuse_policy = opts
        .reuse_policy
        .as_deref()
        .and_then(parse_reuse_policy)
        .unwrap_or(crate::types::WorkflowIdReusePolicy::AllowDuplicate);

    let execution_timeout = opts
        .execution_timeout_secs
        .and_then(chrono::Duration::try_seconds);
    let sla = opts.sla_secs.and_then(chrono::Duration::try_seconds);
    let max_execution_timeout_ceiling = opts
        .max_execution_timeout_ceiling_secs
        .and_then(chrono::Duration::try_seconds);
    let priority = opts
        .priority
        .and_then(crate::types::Priority::from_i32)
        .unwrap_or_default();

    let workflow_name = row.workflow_name.clone();
    let workflow_id = row.workflow_id.clone();
    let queue_name = row.queue_name.clone();
    let batch_key = row.batch_key.clone();
    let row_id = row.id;

    let params = crate::execution::StartWorkflowParams {
        workflow_name: &workflow_name,
        workflow_id: &workflow_id,
        exec_id,
        input: row.buffered_payloads,
        parent_id: None,
        queue_name: &queue_name,
        execution_timeout,
        memo: opts.memo,
        search_attrs: opts.search_attrs,
        reuse_policy,
        trace_context: opts.trace_context,
        max_execution_timeout_ceiling,
        concurrency_key: opts.concurrency_key,
        concurrency_limit: opts.concurrency_limit,
        priority,
        max_workflow_input_bytes: opts.max_workflow_input_bytes.unwrap_or(u64::MAX),
        start_at: None,
        delay: None,
        max_workflow_start_delay: None,
        owner: opts.owner.as_deref(),
        runbook_url: opts.runbook_url.as_deref(),
        severity: opts.severity.as_deref(),
        context_headers: opts.context_headers,
        sla,
        schedule_id: None,
        scheduled_for: None,
    };

    let start_res = crate::execution::start_or_load_workflow_execution_collect(conn, params, true, false).await;

    match start_res {
        Ok((started, deferred_starts)) => {
            diesel::sql_query("DELETE FROM harvest_event_batches WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(row_id)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            tracing::info!(
                workflow_name = %workflow_name,
                batch_key = %batch_key,
                exec_id = %started.exec_id,
                queue = %queue_name,
                "batched start fired",
            );

            Ok(Some((started.exec_id.to_string(), deferred_starts)))
        }
        Err(crate::error::HarvestError::AlreadyExists { .. }) => {
            diesel::sql_query("DELETE FROM harvest_event_batches WHERE id = $1")
                .bind::<diesel::sql_types::Uuid, _>(row_id)
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;
            tracing::warn!(
                workflow_name = %workflow_name,
                batch_key = %batch_key,
                workflow_id = %workflow_id,
                "batched start skipped: workflow_id already exists under reuse policy",
            );
            Ok(None)
        }
        Err(e) => {
            if is_transient_error(&e) {
                Err(e)
            } else {
                diesel::sql_query("DELETE FROM harvest_event_batches WHERE id = $1")
                    .bind::<diesel::sql_types::Uuid, _>(row_id)
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                let ar = crate::models::NewAuditRecord {
                    actor: "system",
                    operation: "workflow.start",
                    target_type: "workflow",
                    target_id: Some(&workflow_id),
                    route_or_command: "background.batch_scanner",
                    request_id: None,
                    idempotency_key: None,
                    status: "failed",
                    error_summary: Some(&e.to_string()),
                    shard_id: Some(row.shard_id),
                    source: "worker",
                };
                let _ = crate::audit::insert_audit(conn, &ar).await;

                tracing::error!(
                    workflow_name = %workflow_name,
                    batch_key = %batch_key,
                    workflow_id = %workflow_id,
                    error = %e,
                    "batched start failed with non-transient error, batch deleted",
                );

                Ok(None)
            }
        }
    }
}

#[cfg(feature = "db")]
pub async fn fire_due_event_batches(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
    _metrics: &(dyn crate::telemetry::MetricsRecorder + Send + Sync),
) -> HarvestResult<usize> {
    let mut fired_count = 0usize;
    let mut deferred_to_spawn = Vec::new();

    if let Some(pool) = sharded_pool {
        for shard_id in shard_assignments {
            if let Some(shard_pool) = pool.exact_pool_for(*shard_id) {
                let mut shard_conn = shard_pool
                    .get()
                    .await
                    .map_err(|e| crate::error::HarvestError::Database(e.to_string()))?;
                let (fired, deferred) = fire_due_on_conn(&mut shard_conn, Some(shard_id.as_i32())).await?;
                fired_count += fired.len();
                deferred_to_spawn.extend(deferred);
            }
        }
    } else {
        let (fired, deferred) = fire_due_on_conn(conn, None).await?;
        fired_count += fired.len();
        deferred_to_spawn.extend(deferred);
    }

    for start in deferred_to_spawn {
        start.spawn();
    }

    Ok(fired_count)
}

#[cfg(feature = "db")]
pub async fn list_pending_event_batches(
    conn: &mut AsyncPgConnection,
    limit: i64,
) -> HarvestResult<Vec<PendingEventBatchRecord>> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct DbRecord {
        #[diesel(sql_type = diesel::sql_types::Uuid)]
        id: uuid::Uuid,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_name: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        batch_key: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        workflow_id: String,
        #[diesel(sql_type = diesel::sql_types::Text)]
        queue_name: String,
        #[diesel(sql_type = diesel::sql_types::Jsonb)]
        buffered_payloads: serde_json::Value,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        fire_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        max_size: i32,
        #[diesel(sql_type = diesel::sql_types::Integer)]
        shard_id: i32,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        created_at: DateTime<Utc>,
        #[diesel(sql_type = diesel::sql_types::Timestamptz)]
        updated_at: DateTime<Utc>,
    }

    let records: Vec<DbRecord> = diesel::sql_query(
        "SELECT id, workflow_name, batch_key, workflow_id, queue_name,
                buffered_payloads, fire_at, max_size, shard_id, created_at, updated_at
         FROM harvest_event_batches
         ORDER BY fire_at ASC
         LIMIT $1",
    )
    .bind::<diesel::sql_types::BigInt, _>(limit)
    .load(conn)
    .await
    .map_err(crate::error::database_error)?;

    Ok(records
        .into_iter()
        .map(|r| {
            let current_size = r.buffered_payloads.as_array().map_or(0, |a| a.len() as i32);
            PendingEventBatchRecord {
                id: r.id,
                workflow_name: r.workflow_name,
                batch_key: r.batch_key,
                workflow_id: r.workflow_id,
                queue_name: r.queue_name,
                current_size,
                max_size: r.max_size,
                fire_at: r.fire_at,
                shard_id: r.shard_id,
                created_at: r.created_at,
                updated_at: r.updated_at,
            }
        })
        .collect())
}

#[cfg(feature = "db")]
fn parse_reuse_policy(s: &str) -> Option<crate::types::WorkflowIdReusePolicy> {
    use crate::types::WorkflowIdReusePolicy::{
        AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate, TerminateIfRunning,
    };
    match s {
        "allow_duplicate" => Some(AllowDuplicate),
        "reject_duplicate" => Some(RejectDuplicate),
        "allow_duplicate_failed_only" => Some(AllowDuplicateFailedOnly),
        "terminate_if_running" => Some(TerminateIfRunning),
        _ => None,
    }
}
