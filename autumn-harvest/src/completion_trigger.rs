use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
pub enum InputMapping {
    Passthrough,
    Static(Value),
    Projection(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TerminalState {
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

impl TerminalState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::TimedOut => "TIMED_OUT",
        }
    }

    #[allow(clippy::should_implement_trait)]
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "COMPLETED" => Some(Self::Completed),
            "FAILED" => Some(Self::Failed),
            "CANCELLED" => Some(Self::Cancelled),
            "TIMED_OUT" => Some(Self::TimedOut),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompletionTrigger {
    pub id: Uuid,
    pub source_workflow_name: String,
    pub terminal_states: Vec<TerminalState>,
    pub target_workflow_name: String,
    pub input_mapping: InputMapping,
    pub queue_name: Option<String>,
}

impl CompletionTrigger {
    pub fn new(
        source_workflow_name: impl Into<String>,
        target_workflow_name: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            source_workflow_name: source_workflow_name.into(),
            terminal_states: vec![TerminalState::Completed],
            target_workflow_name: target_workflow_name.into(),
            input_mapping: InputMapping::Passthrough,
            queue_name: None,
        }
    }

    #[must_use]
    pub const fn with_id(mut self, id: Uuid) -> Self {
        self.id = id;
        self
    }

    #[must_use]
    pub fn with_terminal_states(mut self, states: Vec<TerminalState>) -> Self {
        self.terminal_states = states;
        self
    }

    #[must_use]
    pub fn with_input_mapping(mut self, mapping: InputMapping) -> Self {
        self.input_mapping = mapping;
        self
    }

    #[must_use]
    pub fn with_queue_name(mut self, queue_name: impl Into<String>) -> Self {
        self.queue_name = Some(queue_name.into());
        self
    }

    #[must_use]
    pub fn with_optional_queue_name(mut self, queue_name: Option<String>) -> Self {
        self.queue_name = queue_name;
        self
    }
}

#[must_use]
pub fn project_json_path(value: &Value, path: &str) -> Value {
    if path.is_empty() {
        return value.clone();
    }
    let mut current = value;
    for part in path.split('.') {
        if part.is_empty() {
            continue;
        }
        if let Some(next) = current.get(part) {
            current = next;
        } else {
            return Value::Null;
        }
    }
    current.clone()
}

/// Synchronizes completion triggers with the database.
///
/// # Errors
///
/// Returns an error if database operations fail.
#[cfg(feature = "db")]
pub async fn sync_completion_triggers(
    conn: &mut diesel_async::AsyncPgConnection,
    triggers: &[CompletionTrigger],
) -> crate::error::HarvestResult<()> {
    use crate::models::NewCompletionTriggerDb;
    use crate::schema::harvest_completion_triggers::dsl;
    use chrono::Utc;
    use diesel::prelude::*;
    use diesel_async::{AsyncConnection, RunQueryDsl};

    let active_ids: Vec<Uuid> = triggers.iter().map(|t| t.id).collect();
    let triggers = triggers.to_vec();

    conn.transaction(|tx| {
        Box::pin(async move {
            // First, delete any static triggers that are no longer present in the builder's triggers list.
            if active_ids.is_empty() {
                diesel::delete(dsl::harvest_completion_triggers)
                    .filter(dsl::is_static.eq(true))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            } else {
                diesel::delete(dsl::harvest_completion_triggers)
                    .filter(dsl::is_static.eq(true))
                    .filter(dsl::id.ne_all(&active_ids))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            for trigger in &triggers {
                let db_row = NewCompletionTriggerDb {
                    id: trigger.id,
                    source_workflow_name: trigger.source_workflow_name.clone(),
                    terminal_states: serde_json::to_value(&trigger.terminal_states)?,
                    target_workflow_name: trigger.target_workflow_name.clone(),
                    input_mapping: serde_json::to_value(&trigger.input_mapping)?,
                    queue_name: trigger.queue_name.clone(),
                    is_static: true,
                };

                diesel::insert_into(dsl::harvest_completion_triggers)
                    .values(&db_row)
                    .on_conflict(dsl::id)
                    .do_update()
                    .set((
                        dsl::source_workflow_name.eq(&db_row.source_workflow_name),
                        dsl::terminal_states.eq(&db_row.terminal_states),
                        dsl::target_workflow_name.eq(&db_row.target_workflow_name),
                        dsl::input_mapping.eq(&db_row.input_mapping),
                        dsl::queue_name.eq(&db_row.queue_name),
                        dsl::is_static.eq(true),
                        dsl::updated_at.eq(Utc::now()),
                    ))
                    .execute(tx)
                    .await
                    .map_err(crate::error::database_error)?;
            }

            Ok(())
        })
    })
    .await
}

#[cfg(feature = "db")]
pub struct WorkflowMetadata {
    pub concurrency: Option<crate::concurrency::ConcurrencyPolicy>,
    pub max_input_bytes: Option<u64>,
    pub owner: Option<String>,
    pub runbook_url: Option<String>,
    pub severity: Option<String>,
    pub input_schema: Option<fn() -> serde_json::Value>,
}

#[cfg(feature = "db")]
pub static GLOBAL_WORKFLOW_METADATA: std::sync::RwLock<
    Option<std::collections::HashMap<String, WorkflowMetadata>>,
> = std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub static GLOBAL_MAX_WORKFLOW_INPUT_BYTES: std::sync::RwLock<u64> =
    std::sync::RwLock::new(crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES);

#[cfg(feature = "db")]
pub static GLOBAL_DEFAULT_WORKFLOW_QUEUE: std::sync::RwLock<Option<String>> =
    std::sync::RwLock::new(None);

#[cfg(feature = "db")]
pub async fn resolve_target_queue(
    conn: &mut diesel_async::AsyncPgConnection,
    target_workflow_name: &str,
    target_shard: crate::types::ShardId,
) -> String {
    // If the target shard is 0 (the default shard), we can query it directly.
    if target_shard.as_i32() == 0 || target_shard.is_unencoded() {
        use crate::schema::harvest_schedules::dsl as sched_dsl;
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        if let Ok(Some(Some(q))) = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(target_workflow_name))
            .select(sched_dsl::queue_name)
            .first::<Option<String>>(conn)
            .await
            .optional()
        {
            return q;
        }
        return GLOBAL_DEFAULT_WORKFLOW_QUEUE
            .read()
            .ok()
            .and_then(|lock| lock.clone())
            .unwrap_or_else(|| "default".to_string());
    }

    // Otherwise, acquire a connection to Shard 0 to query the schedules.
    let default_pool = crate::shard::GLOBAL_SHARDED_POOL
        .read()
        .ok()
        .and_then(|p| p.clone())
        .map(|sp| sp.pool_for(crate::types::ShardId::new(0)).clone());

    if let Some(dp) = default_pool
        && let Ok(mut default_conn) = dp.get().await
    {
        use crate::schema::harvest_schedules::dsl as sched_dsl;
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        if let Ok(Some(Some(q))) = sched_dsl::harvest_schedules
            .filter(sched_dsl::workflow_name.eq(target_workflow_name))
            .select(sched_dsl::queue_name)
            .first::<Option<String>>(&mut default_conn)
            .await
            .optional()
        {
            return q;
        }
    }

    GLOBAL_DEFAULT_WORKFLOW_QUEUE
        .read()
        .ok()
        .and_then(|lock| lock.clone())
        .unwrap_or_else(|| "default".to_string())
}

#[cfg(feature = "db")]
#[derive(Debug, Clone)]
pub struct DeferredTriggerStart {
    pub outbox_id: Uuid,
    pub source_shard: crate::types::ShardId,
    pub target_shard: crate::types::ShardId,
    pub target_workflow_name: String,
    pub target_workflow_id: String,
    pub target_input: Value,
    pub queue_name: Option<String>,
    pub concurrency_key: Option<String>,
    pub concurrency_limit: Option<u32>,
    pub priority: crate::types::Priority,
    pub max_workflow_input_bytes: u64,
    pub trigger_name: String,
    pub owner: Option<String>,
    pub runbook_url: Option<String>,
    pub severity: Option<String>,
}

#[cfg(feature = "db")]
impl DeferredTriggerStart {
    pub fn spawn(self) {
        let Some(pool) = crate::shard::GLOBAL_SHARDED_POOL
            .read()
            .ok()
            .and_then(|p| p.clone())
            .and_then(|sp| sp.exact_pool_for(self.target_shard).cloned())
        else {
            tracing::error!(
                "[completion_trigger] GLOBAL_SHARDED_POOL is not initialized during spawn. Cannot start target cross-shard workflow."
            );
            return;
        };
        tokio::spawn(async move {
            let conn_res = pool.get().await;
            let mut target_conn = match conn_res {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(
                        "[completion_trigger] Failed to get connection for shard {:?}: {:?}",
                        self.target_shard,
                        e
                    );
                    return;
                }
            };
            let queue_name = if let Some(ref q) = self.queue_name {
                q.clone()
            } else {
                resolve_target_queue(
                    &mut target_conn,
                    &self.target_workflow_name,
                    self.target_shard,
                )
                .await
            };
            let start_res = crate::execution::start_or_load_workflow_execution(
                &mut target_conn,
                crate::execution::StartWorkflowParams {
                    workflow_name: &self.target_workflow_name,
                    workflow_id: &self.target_workflow_id,
                    exec_id: crate::types::ExecutionId::new_for_shard(self.target_shard),
                    input: self.target_input,
                    parent_id: None,
                    queue_name: &queue_name,
                    execution_timeout: None,
                    memo: None,
                    search_attrs: None,
                    reuse_policy: crate::types::WorkflowIdReusePolicy::AllowDuplicate,
                    trace_context: None,
                    max_execution_timeout_ceiling: None,
                    concurrency_key: self.concurrency_key,
                    concurrency_limit: self.concurrency_limit,
                    priority: self.priority,
                    max_workflow_input_bytes: self.max_workflow_input_bytes,
                    start_at: None,
                    delay: None,
                    max_workflow_start_delay: None,
                    owner: self.owner.as_deref(),
                    runbook_url: self.runbook_url.as_deref(),
                    severity: self.severity.as_deref(),
                    context_headers: None,
                },
            )
            .await;
            match start_res {
                Ok(_) => {
                    // Delete task from outbox on successful start
                    if let Some(source_pool) = crate::shard::GLOBAL_SHARDED_POOL
                        .read()
                        .ok()
                        .and_then(|p| p.clone())
                        .and_then(|sp| sp.exact_pool_for(self.source_shard).cloned())
                        && let Ok(mut source_conn) = source_pool.get().await
                    {
                        use diesel::prelude::*;
                        use diesel_async::RunQueryDsl;
                        let _ =
                            diesel::delete(crate::schema::harvest_completion_trigger_outbox::table)
                                .filter(
                                    crate::schema::harvest_completion_trigger_outbox::dsl::id
                                        .eq(self.outbox_id),
                                )
                                .execute(&mut source_conn)
                                .await;
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "[completion_trigger] Failed to start workflow execution cross-shard: {:?}",
                        e
                    );
                }
            }
        });
    }
}

#[allow(clippy::too_many_lines)]
#[cfg(feature = "db")]
pub fn evaluate_triggers_for_execution<'a>(
    conn: &'a mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    state: TerminalState,
    metrics: Option<&'a (dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> futures::future::BoxFuture<'a, crate::error::HarvestResult<Vec<DeferredTriggerStart>>> {
    use futures::FutureExt;
    async move {
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        use crate::schema::harvest_completion_triggers::dsl as triggers_dsl;
        use crate::schema::harvest_completion_trigger_fires::dsl as fires_dsl;
        use crate::schema::harvest_workflow_executions::dsl as execs_dsl;
        use crate::models::{CompletionTriggerDb, NewCompletionTriggerFireDb, WorkflowExecution, NewCompletionTriggerOutboxDb};
        use crate::execution::{StartWorkflowParams, start_or_load_workflow_execution};
        use crate::types::WorkflowIdReusePolicy;
        use crate::types::Priority;

        let mut deferred_starts = Vec::new();

        let execution = execs_dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .first::<WorkflowExecution>(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        let Some(execution) = execution else {
            return Ok(deferred_starts);
        };

        let triggers = triggers_dsl::harvest_completion_triggers
            .filter(triggers_dsl::source_workflow_name.eq(&execution.workflow_name))
            .load::<CompletionTriggerDb>(conn)
            .await
            .map_err(crate::error::database_error)?;

        for trigger_db in triggers {
            let terminal_states: Vec<TerminalState> = serde_json::from_value(trigger_db.terminal_states)
                .unwrap_or_default();

            if !terminal_states.contains(&state) {
                continue;
            }

            let input_mapping: InputMapping = serde_json::from_value(trigger_db.input_mapping)
                .unwrap_or(InputMapping::Passthrough);

            let source_output = execution.output.clone().unwrap_or(Value::Null);
            let target_input = match input_mapping {
                InputMapping::Passthrough => source_output,
                InputMapping::Static(v) => v,
                InputMapping::Projection(ref path) => project_json_path(&source_output, path),
            };

            let target_workflow_id = format!("completion-trigger-{}-{}", trigger_db.id, exec_id);

            // Validate mapped inputs against the target schema
            if let Some(ref target_schema_fn) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.input_schema)
            } {
                let schema = target_schema_fn();
                if let Err(violations) = crate::info::validate_against_schema(&schema, &target_input) {
                    tracing::warn!(
                        trigger_id = %trigger_db.id,
                        target_workflow_name = %trigger_db.target_workflow_name,
                        violations = ?violations,
                        "Completion trigger input validation failed; skipping trigger execution."
                    );
                    if let Some(m) = metrics {
                        m.record_completion_trigger_fired(&trigger_db.id.to_string(), "validation_failed");
                    }
                    continue;
                }
            }

            let inserted = diesel::insert_into(fires_dsl::harvest_completion_trigger_fires)
                .values(&NewCompletionTriggerFireDb {
                    source_exec_id: exec_id.as_uuid(),
                    trigger_id: trigger_db.id,
                })
                .on_conflict_do_nothing()
                .execute(conn)
                .await
                .map_err(crate::error::database_error)?;

            let trigger_name = trigger_db.id.to_string();

            if inserted == 0 {
                if let Some(m) = metrics {
                    m.record_completion_trigger_fired(&trigger_name, "deduped");
                }
                continue;
            }

            let router = crate::shard::GLOBAL_SHARD_ROUTER
                .read()
                .ok()
                .and_then(|guard| guard.as_ref().cloned())
                .ok_or_else(|| {
                    tracing::error!("[completion_trigger] GLOBAL_SHARD_ROUTER is not initialized.");
                    crate::error::database_error(diesel::result::Error::RollbackTransaction)
                })?;
            let target_shard = router.pick_for_new_workflow(&trigger_db.target_workflow_name, &target_workflow_id);
            let source_shard = router.shard_for_execution(exec_id);

            // Resolve target concurrency parameters
            let (concurrency_key, concurrency_limit) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.concurrency.as_ref())
                    .map_or((None, None), |policy| {
                        let key = crate::concurrency::resolve_concurrency_key(policy.key_expr, &target_input);
                        (key, Some(policy.limit))
                    })
            };

            // Resolve target input caps
            let max_workflow_input_bytes = {
                let global_default = GLOBAL_MAX_WORKFLOW_INPUT_BYTES.read().as_deref().copied().unwrap_or(crate::builder::DEFAULT_MAX_WORKFLOW_INPUT_BYTES);
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .and_then(|meta| meta.max_input_bytes)
                    .map_or(global_default, |per_wf| per_wf.max(global_default))
            };

            // Resolve target metadata (owner, runbook_url, severity)
            let (target_owner, target_runbook_url, target_severity) = {
                let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
                lock.as_ref()
                    .and_then(|guard| guard.as_ref())
                    .and_then(|meta_map| meta_map.get(&trigger_db.target_workflow_name))
                    .map_or((None, None, None), |meta| {
                        (meta.owner.clone(), meta.runbook_url.clone(), meta.severity.clone())
                    })
            };

            if target_shard == source_shard {
                let queue_name = if let Some(ref q) = trigger_db.queue_name {
                    q.clone()
                } else {
                    resolve_target_queue(conn, &trigger_db.target_workflow_name, target_shard).await
                };

                let target_exec_id = crate::types::ExecutionId::new_for_shard(target_shard);
                let start_res = match start_or_load_workflow_execution(
                    conn,
                    StartWorkflowParams {
                        workflow_name: &trigger_db.target_workflow_name,
                        workflow_id: &target_workflow_id,
                        exec_id: target_exec_id,
                        input: target_input,
                        parent_id: None,
                        queue_name: &queue_name,
                        execution_timeout: None,
                        memo: None,
                        search_attrs: None,
                        reuse_policy: WorkflowIdReusePolicy::AllowDuplicate,
                        trace_context: None,
                        max_execution_timeout_ceiling: None,
                        concurrency_key,
                        concurrency_limit,
                        priority: Priority::default(),
                        max_workflow_input_bytes,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                        owner: target_owner.as_deref(),
                        runbook_url: target_runbook_url.as_deref(),
                        severity: target_severity.as_deref(),
                        context_headers: None,
                    },
                )
                .await
                {
                    Ok(res) => res,
                    Err(crate::error::HarvestError::PayloadTooLarge {
                        kind,
                        observed_bytes,
                        cap_bytes,
                        workflow_type,
                        ..
                    }) => {
                        tracing::warn!(
                            trigger_id = %trigger_db.id,
                            target_workflow_name = %trigger_db.target_workflow_name,
                            kind = %kind,
                            observed_bytes = observed_bytes,
                            cap_bytes = cap_bytes,
                            workflow_type = %workflow_type,
                            "Oversized trigger input payload; skipping trigger execution."
                        );
                        if let Some(m) = metrics {
                            m.record_completion_trigger_fired(&trigger_name, "payload_too_large");
                        }
                        continue;
                    }
                    Err(e) => return Err(e),
                };

                if let Some(m) = metrics {
                    if start_res.created {
                        m.record_completion_trigger_fired(&trigger_name, "started");
                    } else {
                        m.record_completion_trigger_fired(&trigger_name, "skipped");
                    }
                }
            } else {
                // Verify cross-shard database pool is configured before proceeding
                let _pool = {
                    let lock = crate::shard::GLOBAL_SHARDED_POOL.read();
                    lock.ok().and_then(|p| p.clone()).and_then(|sp| sp.exact_pool_for(target_shard).cloned())
                }.ok_or_else(|| {
                    tracing::error!("[completion_trigger] GLOBAL_SHARDED_POOL is not initialized or does not have shard {}.", target_shard);
                    crate::error::database_error(diesel::result::Error::RollbackTransaction)
                })?;

                if let Some(m) = metrics {
                    m.record_completion_trigger_fired(&trigger_name, "started");
                }

                let outbox_row = diesel::insert_into(crate::schema::harvest_completion_trigger_outbox::table)
                    .values(&NewCompletionTriggerOutboxDb {
                        source_exec_id: exec_id.as_uuid(),
                        trigger_id: trigger_db.id,
                        target_shard: target_shard.as_i32(),
                        target_workflow_name: trigger_db.target_workflow_name.clone(),
                        target_workflow_id: target_workflow_id.clone(),
                        target_input: target_input.clone(),
                        queue_name: trigger_db.queue_name.clone(),
                        concurrency_key: concurrency_key.clone(),
                        concurrency_limit: concurrency_limit.map(|l| i32::try_from(l).unwrap_or(i32::MAX)),
                        priority: serde_json::to_value(Priority::default()).unwrap_or(Value::Null),
                        max_workflow_input_bytes: i64::try_from(max_workflow_input_bytes).unwrap_or(i64::MAX),
                    })
                    .get_result::<crate::models::CompletionTriggerOutboxDb>(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                deferred_starts.push(DeferredTriggerStart {
                    outbox_id: outbox_row.id,
                    source_shard,
                    target_shard,
                    target_workflow_name: trigger_db.target_workflow_name.clone(),
                    target_workflow_id,
                    target_input,
                    queue_name: trigger_db.queue_name.clone(),
                    concurrency_key,
                    concurrency_limit,
                    priority: Priority::default(),
                    max_workflow_input_bytes,
                    trigger_name,
                    owner: target_owner,
                    runbook_url: target_runbook_url,
                    severity: target_severity,
                });
            }
        }

        Ok(deferred_starts)
    }
    .boxed()
}

/// Enforces pending completion triggers outbox tasks.
///
/// # Errors
///
/// Returns an error if any database operations fail.
#[cfg(feature = "db")]
#[allow(clippy::too_many_lines)]
pub async fn enforce_completion_triggers_outbox(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
) -> crate::error::HarvestResult<usize> {
    use crate::models::CompletionTriggerOutboxDb;
    use crate::schema::harvest_completion_trigger_outbox::dsl as outbox_dsl;
    use crate::types::Priority;
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    let shards: Vec<i32> = if shard_assignments.is_empty() {
        vec![0]
    } else {
        shard_assignments.iter().map(|s| s.as_i32()).collect()
    };

    // Load up to 50 pending outbox tasks for shards assigned to this worker.
    let pending_tasks = outbox_dsl::harvest_completion_trigger_outbox
        .filter(outbox_dsl::target_shard.eq_any(&shards))
        .limit(50)
        .load::<CompletionTriggerOutboxDb>(conn)
        .await
        .map_err(crate::error::database_error)?;

    let count = pending_tasks.len();
    if count == 0 {
        return Ok(0);
    }

    for task in pending_tasks {
        let target_shard = crate::types::ShardId::new(task.target_shard);
        let Some(target_pool) = sharded_pool
            .as_ref()
            .and_then(|sp| sp.exact_pool_for(target_shard).cloned())
        else {
            continue;
        };

        let mut target_conn = match target_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(
                    "[completion_trigger outbox] Failed to get connection to target shard {:?}: {:?}",
                    target_shard,
                    e
                );
                continue;
            }
        };

        let queue_name = if let Some(ref q) = task.queue_name {
            q.clone()
        } else {
            resolve_target_queue(&mut target_conn, &task.target_workflow_name, target_shard).await
        };

        let priority: Priority = serde_json::from_value(task.priority).unwrap_or_default();

        let (target_owner, target_runbook_url, target_severity) = {
            let lock = GLOBAL_WORKFLOW_METADATA.read().ok();
            lock.as_ref()
                .and_then(|guard| guard.as_ref())
                .and_then(|meta_map| meta_map.get(&task.target_workflow_name))
                .map_or((None, None, None), |meta| {
                    (
                        meta.owner.clone(),
                        meta.runbook_url.clone(),
                        meta.severity.clone(),
                    )
                })
        };

        let start_res = crate::execution::start_or_load_workflow_execution(
            &mut target_conn,
            crate::execution::StartWorkflowParams {
                workflow_name: &task.target_workflow_name,
                workflow_id: &task.target_workflow_id,
                exec_id: crate::types::ExecutionId::new_for_shard(target_shard),
                input: task.target_input,
                parent_id: None,
                queue_name: &queue_name,
                execution_timeout: None,
                memo: None,
                search_attrs: None,
                reuse_policy: crate::types::WorkflowIdReusePolicy::AllowDuplicate,
                trace_context: None,
                max_execution_timeout_ceiling: None,
                concurrency_key: task.concurrency_key,
                concurrency_limit: task
                    .concurrency_limit
                    .map(|l| u32::try_from(l).unwrap_or(0)),
                priority,
                max_workflow_input_bytes: u64::try_from(task.max_workflow_input_bytes).unwrap_or(0),
                start_at: None,
                delay: None,
                max_workflow_start_delay: None,
                owner: target_owner.as_deref(),
                runbook_url: target_runbook_url.as_deref(),
                severity: target_severity.as_deref(),
                context_headers: None,
            },
        )
        .await;

        match start_res {
            Ok(_) => {
                // Delete task from outbox on successful start
                let _ = diesel::delete(outbox_dsl::harvest_completion_trigger_outbox)
                    .filter(outbox_dsl::id.eq(task.id))
                    .execute(conn)
                    .await;
            }
            Err(e) => {
                tracing::error!(
                    "[completion_trigger outbox] Failed to start workflow execution cross-shard: {:?}",
                    e
                );
            }
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_project_json_path() {
        let val = json!({
            "a": {
                "b": {
                    "c": 42
                }
            },
            "array": [1, 2, 3]
        });

        assert_eq!(project_json_path(&val, "a.b.c"), json!(42));
        assert_eq!(project_json_path(&val, "a.b"), json!({"c": 42}));
        assert_eq!(project_json_path(&val, "a.x"), Value::Null);
        assert_eq!(project_json_path(&val, ""), val);
    }
}
