//! Diesel model structs mapping to harvest_* tables.
//!
//! Each table has two variants:
//! - Full struct (`Queryable` + `Selectable`) for reads
//! - `New*` struct (`Insertable`) for inserts — only the fields we supply,
//!   letting Postgres fill in defaults.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use uuid::Uuid;

use crate::schema::{
    harvest_dag_runs, harvest_dead_letters, harvest_events, harvest_schedules, harvest_signals,
    harvest_task_queue, harvest_timers, harvest_workflow_executions,
};

// ── WorkflowExecution ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_workflow_executions)]
#[diesel(check_fields)]
pub struct WorkflowExecution {
    pub id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub state: String,
    pub input: serde_json::Value,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
    pub parent_id: Option<Uuid>,
    pub sticky_worker_id: Option<String>,
    pub queue_name: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_workflow_executions)]
pub struct NewWorkflowExecution<'a> {
    pub id: Uuid,
    pub workflow_name: &'a str,
    pub workflow_id: &'a str,
    pub run_id: Uuid,
    pub shard_id: i32,
    pub input: serde_json::Value,
    pub queue_name: &'a str,
    pub execution_timeout: Option<chrono::Duration>,
    pub memo: Option<serde_json::Value>,
    pub search_attrs: Option<serde_json::Value>,
}

// ── HarvestEvent ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_events)]
#[diesel(check_fields)]
pub struct HarvestEvent {
    pub id: i64,
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_events)]
pub struct NewHarvestEvent<'a> {
    pub workflow_exec_id: Uuid,
    pub event_id: i32,
    pub event_type: &'a str,
    pub event_data: serde_json::Value,
}

// ── TaskQueueItem ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_task_queue)]
#[diesel(check_fields)]
pub struct TaskQueueItem {
    pub id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub state: String,
    pub priority: i32,
    pub worker_id: Option<String>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub error: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_task_queue)]
pub struct NewTaskQueueItem<'a> {
    pub id: Uuid,
    pub queue_name: &'a str,
    pub task_type: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<&'a str>,
    pub input: serde_json::Value,
    pub priority: i32,
    pub max_attempts: i32,
    pub scheduled_at: DateTime<Utc>,
    pub heartbeat_timeout: Option<chrono::Duration>,
    pub start_to_close: Option<chrono::Duration>,
    pub schedule_to_start: Option<chrono::Duration>,
    pub retry_policy: Option<serde_json::Value>,
}

// ── DagRun ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_dag_runs)]
#[diesel(check_fields)]
pub struct DagRun {
    pub id: Uuid,
    pub dag_name: String,
    pub workflow_exec_id: Option<Uuid>,
    pub state: String,
    pub logical_date: DateTime<Utc>,
    pub data_interval_start: DateTime<Utc>,
    pub data_interval_end: DateTime<Utc>,
    pub conf: Option<serde_json::Value>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_dag_runs)]
pub struct NewDagRun<'a> {
    pub id: Uuid,
    pub dag_name: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub logical_date: DateTime<Utc>,
    pub data_interval_start: DateTime<Utc>,
    pub data_interval_end: DateTime<Utc>,
    pub conf: Option<serde_json::Value>,
}

// ── Schedule ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_schedules)]
#[diesel(check_fields)]
pub struct HarvestSchedule {
    pub id: Uuid,
    pub dag_name: String,
    pub schedule_expr: Option<String>,
    pub timezone: String,
    pub catchup: bool,
    pub max_active_runs: i32,
    pub is_paused: bool,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_schedules)]
pub struct NewHarvestSchedule<'a> {
    pub id: Uuid,
    pub dag_name: &'a str,
    pub schedule_expr: Option<&'a str>,
    pub timezone: &'a str,
    pub catchup: bool,
    pub max_active_runs: i32,
}

// ── Signal ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_signals)]
#[diesel(check_fields)]
pub struct HarvestSignal {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub signal_name: String,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
    pub consumed: bool,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_signals)]
pub struct NewHarvestSignal<'a> {
    pub workflow_exec_id: Uuid,
    pub signal_name: &'a str,
    pub payload: serde_json::Value,
}

// ── Timer ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_timers)]
#[diesel(check_fields)]
pub struct HarvestTimer {
    pub id: Uuid,
    pub workflow_exec_id: Uuid,
    pub timer_id: String,
    pub fires_at: DateTime<Utc>,
    pub fired: bool,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_timers)]
pub struct NewHarvestTimer<'a> {
    pub workflow_exec_id: Uuid,
    pub timer_id: &'a str,
    pub fires_at: DateTime<Utc>,
}

// ── DeadLetter ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = harvest_dead_letters)]
#[diesel(check_fields)]
pub struct DeadLetter {
    pub id: Uuid,
    pub original_task_id: Uuid,
    pub queue_name: String,
    pub task_type: String,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<String>,
    pub input: serde_json::Value,
    pub error: String,
    pub attempts: i32,
    pub failed_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = harvest_dead_letters)]
pub struct NewDeadLetter<'a> {
    pub original_task_id: Uuid,
    pub queue_name: &'a str,
    pub task_type: &'a str,
    pub workflow_exec_id: Option<Uuid>,
    pub activity_name: Option<&'a str>,
    pub input: serde_json::Value,
    pub error: &'a str,
    pub attempts: i32,
}
