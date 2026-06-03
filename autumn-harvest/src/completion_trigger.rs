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
    use diesel_async::RunQueryDsl;

    for trigger in triggers {
        let db_row = NewCompletionTriggerDb {
            id: trigger.id,
            source_workflow_name: trigger.source_workflow_name.clone(),
            terminal_states: serde_json::to_value(&trigger.terminal_states)?,
            target_workflow_name: trigger.target_workflow_name.clone(),
            input_mapping: serde_json::to_value(&trigger.input_mapping)?,
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
                dsl::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(())
}

#[allow(clippy::too_many_lines)]
#[cfg(feature = "db")]
pub fn evaluate_triggers_for_execution<'a>(
    conn: &'a mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    state: TerminalState,
    metrics: Option<&'a (dyn crate::telemetry::MetricsRecorder + Send + Sync)>,
) -> futures::future::BoxFuture<'a, crate::error::HarvestResult<()>> {
    use futures::FutureExt;
    async move {
        use diesel::prelude::*;
        use diesel_async::RunQueryDsl;
        use crate::schema::harvest_completion_triggers::dsl as triggers_dsl;
        use crate::schema::harvest_completion_trigger_fires::dsl as fires_dsl;
        use crate::schema::harvest_workflow_executions::dsl as execs_dsl;
        use crate::models::{CompletionTriggerDb, NewCompletionTriggerFireDb, WorkflowExecution};
        use crate::execution::{StartWorkflowParams, start_or_load_workflow_execution};
        use crate::types::WorkflowIdReusePolicy;
        use crate::types::Priority;

        let execution = execs_dsl::harvest_workflow_executions
            .find(exec_id.as_uuid())
            .first::<WorkflowExecution>(conn)
            .await
            .optional()
            .map_err(crate::error::database_error)?;

        let Some(execution) = execution else {
            return Ok(());
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

            let target_shard = crate::shard::GLOBAL_SHARD_ROUTER
                .read()
                .ok()
                .and_then(|guard| guard.as_ref().cloned())
                .unwrap_or_default()
                .pick_for_new_workflow(&trigger_db.target_workflow_name, &target_workflow_id);
            let source_shard = exec_id.shard();
            if target_shard == source_shard || source_shard.is_unencoded() {
                let target_wf = trigger_db.target_workflow_name.clone();
                let queue_name = {
                    use crate::schema::harvest_schedules::dsl as sched_dsl;
                    sched_dsl::harvest_schedules
                        .filter(sched_dsl::workflow_name.eq(target_wf))
                        .select(sched_dsl::queue_name)
                        .first::<Option<String>>(conn)
                        .await
                        .optional()
                        .ok()
                        .flatten()
                        .flatten()
                        .unwrap_or_else(|| "default".to_string())
                };

                let target_exec_id = crate::types::ExecutionId::new_for_shard(target_shard);
                let start_res = start_or_load_workflow_execution(
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
                        concurrency_key: None,
                        concurrency_limit: None,
                        priority: Priority::default(),
                        max_workflow_input_bytes: 0,
                        start_at: None,
                        delay: None,
                        max_workflow_start_delay: None,
                    },
                )
                .await;

                if let Some(m) = metrics {
                    match start_res {
                        Ok(started) => {
                            if started.created {
                                m.record_completion_trigger_fired(&trigger_name, "started");
                            } else {
                                m.record_completion_trigger_fired(&trigger_name, "skipped");
                            }
                        }
                        Err(_) => {
                            m.record_completion_trigger_fired(&trigger_name, "skipped");
                        }
                    }
                }
            } else {
                if let Some(m) = metrics {
                    m.record_completion_trigger_fired(&trigger_name, "started");
                }

                let pool_opt = {
                    let lock = crate::shard::GLOBAL_SHARDED_POOL.read();
                    lock.ok().and_then(|p| p.clone()).map(|sp| sp.pool_for(target_shard).clone())
                };
                if let Some(pool) = pool_opt {
                    let target_workflow_name = trigger_db.target_workflow_name.clone();
                    tokio::spawn(async move {
                        let conn_res = pool.get().await;
                        let mut target_conn = match conn_res {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!("[completion_trigger] Failed to get connection for shard {:?}: {:?}", target_shard, e);
                                return;
                            }
                        };
                        let queue_name = {
                            use crate::schema::harvest_schedules::dsl as sched_dsl;
                            sched_dsl::harvest_schedules
                                .filter(sched_dsl::workflow_name.eq(target_workflow_name.clone()))
                                .select(sched_dsl::queue_name)
                                .first::<Option<String>>(&mut target_conn)
                                .await
                                .optional()
                                .ok()
                                .flatten()
                                .flatten()
                                .unwrap_or_else(|| "default".to_string())
                        };

                        let target_exec_id = crate::types::ExecutionId::new_for_shard(target_shard);
                        let start_res = start_or_load_workflow_execution(
                            &mut target_conn,
                            StartWorkflowParams {
                                workflow_name: &target_workflow_name,
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
                                concurrency_key: None,
                                concurrency_limit: None,
                                priority: Priority::default(),
                                max_workflow_input_bytes: 0,
                                start_at: None,
                                delay: None,
                                max_workflow_start_delay: None,
                            },
                        )
                        .await;
                        if let Err(e) = start_res {
                            tracing::error!("[completion_trigger] Failed to start workflow execution cross-shard: {:?}", e);
                        }
                    });
                }
            }
        }

        Ok(())
    }
    .boxed()
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
