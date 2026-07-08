//! Vantage — an embedded, read-only HTML dashboard for Harvest workflows.
//!
//! Mounts alongside the management API (e.g. `/api/harvest/ui`). Renders a
//! paginated workflow list and a per-workflow detail page showing inputs,
//! outputs, and the full event history. Assets are inlined so the dashboard
//! works in network-restricted environments.
#![allow(clippy::literal_string_with_formatting_args)]

use std::fmt::Write as _;
use std::sync::Arc;

use std::collections::HashMap;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::extract::{Path, Query};
use autumn_web::reexports::axum;
use autumn_web::session::Session;
use axum::Extension;
use axum::Form;
use axum::Router;
use axum::middleware;
use axum::response::IntoResponse as _;
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use diesel::BoolExpressionMethods;
use diesel::dsl::sql;
use diesel::sql_types::{Bool, Text};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;
use tracing::warn;

use autumn_harvest::Schedule;
use autumn_harvest::ShardRouter;
use autumn_harvest::audit::{
    OP_BUILD_COMPAT_DECLARE, OP_BUILD_COMPAT_REVOKE, OP_BUILD_POLICY_SET, OP_GATE_LIFT,
    OP_SCHEDULE_DELETE, OP_SCHEDULE_PAUSE, OP_SCHEDULE_RESUME, OP_SCHEDULE_TRIGGER,
    OP_WORKFLOW_CANCEL, OP_WORKFLOW_PAUSE, OP_WORKFLOW_RESET, OP_WORKFLOW_RESUME,
    OP_WORKFLOW_SIGNAL, OP_WORKFLOW_TERMINATE, SOURCE_API, SOURCE_UI, STATUS_FAILED,
    STATUS_SUCCEEDED, TARGET_BUILD_ROUTING, TARGET_DEAD_LETTER, TARGET_GATE, TARGET_SCHEDULE,
    TARGET_WORKFLOW, insert_audit,
};
use autumn_harvest::build_routing::{
    BuildCompatEntry, BuildPolicy, BuildReachability, all_build_reachability, declare_compat,
    list_build_compat, list_build_policies, merge_reachability, revoke_compat, set_build_policy,
};
use autumn_harvest::error::{HarvestResult, database_error};
use autumn_harvest::execution::StartWorkflowParams;
use autumn_harvest::models::{
    DeadLetter, ExternalTask, HarvestEvent, HarvestSchedule, HarvestSignal, HarvestTimer,
    NewAuditRecord, ScheduleDecision, TaskQueueItem, WorkflowExecution,
};
use autumn_harvest::payload_codec::{LossyDecodeOutcome, PayloadCodecs};
use autumn_harvest::policy::TaskStatus;
use autumn_harvest::reset::{
    ResetSignalReapplyPolicy, WorkflowResetRequest, reset_workflow_execution,
};
use autumn_harvest::scheduler::RegisteredDag;
use autumn_harvest::schema::{
    harvest_dead_letters, harvest_events, harvest_external_tasks, harvest_schedules,
    harvest_signals, harvest_task_queue, harvest_timers, harvest_workflow_executions,
};
use autumn_harvest::signal::send_signal;
use autumn_harvest::start_or_load_workflow_execution_with_metrics;
use autumn_harvest::store::admit_update_event;
use autumn_harvest::types::{
    ExecutionId as HarvestExecutionId, Priority, ShardId, UpdateId, WorkflowIdReusePolicy,
};
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, list_workers};
use autumn_harvest::{
    cancel_workflow_execution, pause_workflow_execution, resume_workflow_execution,
    terminate_workflow_execution,
};

use crate::api::{
    HarvestApiRuntime, HarvestApiState, KNOWN_WORKFLOW_STATES, WorkflowFilters, acquire_conn,
    audit_decoded_read, db_conn_for_execution, db_conn_for_shard, decode_error_field,
    decode_workflow_execution_fields, extension_session, load_execution, load_workflows,
    load_workflows_from_shards, map_error, parse_execution_id, read_path_decoder,
    require_harvest_admin,
};

const DEFAULT_PAGE_SIZE: i64 = 25;
const DEFAULT_DLQ_PAGE_SIZE: i64 = 50;
const MAX_PAGE_SIZE: i64 = 200;
const DLQ_BULK_ACTION_LIMIT: usize = autumn_harvest::dlq::MAX_BULK_LIMIT as usize;
/// Default grouping for the DLQ summary view (issue #385).
const DEFAULT_DLQ_SUMMARY_GROUP_BY: &str = "workflow_name,failure_signature";
/// Top-N groups rendered in the DLQ summary view before long-tail rollup.
const DLQ_SUMMARY_GROUP_LIMIT: u32 = 25;
/// Sample dead-letter IDs surfaced per summary group.
const DLQ_SUMMARY_SAMPLES_PER_GROUP: u32 = 3;

const KNOWN_STATES: &[&str] = KNOWN_WORKFLOW_STATES;

const STYLE: &str = r#"
*,*::before,*::after{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:#0f172a;color:#e2e8f0}
a{color:#93c5fd;text-decoration:none}
a:hover{text-decoration:underline}
header{background:#1e293b;border-bottom:1px solid #334155;padding:16px 24px;display:flex;align-items:center;justify-content:space-between}
header h1{margin:0;font-size:20px;font-weight:600}
header h1 a{color:#f8fafc;text-decoration:none}
header .subtitle{color:#94a3b8;font-size:13px;margin-left:8px}
header nav{display:flex;gap:16px;font-size:13px}
header nav a{color:#cbd5e1}
header nav a.active{color:#f8fafc;font-weight:600}
main{padding:24px;max-width:1200px;margin:0 auto}
h2{font-size:18px;margin:0 0 16px;color:#f8fafc}
h3{font-size:14px;margin:24px 0 8px;color:#cbd5e1;text-transform:uppercase;letter-spacing:.06em}
.filters{display:flex;gap:12px;align-items:flex-end;margin-bottom:16px;flex-wrap:wrap}
.filters label{display:flex;flex-direction:column;font-size:12px;color:#94a3b8}
.filters select,.filters input{background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:6px 10px;font-size:13px;margin-top:4px}
.filters button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:13px;cursor:pointer;align-self:flex-end;height:32px}
.filters button:hover{background:#1d4ed8}
.filters .reset{background:transparent;color:#94a3b8;border:1px solid #334155}
.actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.actions form{margin:0}
.actions button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 10px;font-size:12px;cursor:pointer}
.actions button:hover{background:#1d4ed8}
.actions button.danger{background:#991b1b}
.actions button.danger:hover{background:#7f1d1d}
.bulk-actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 16px}
.bulk-actions form{margin:0}
.bulk-actions button{background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 12px;font-size:13px;cursor:pointer}
.bulk-actions button.danger{background:#991b1b}
.bulk-actions button:disabled{background:#334155;color:#64748b;cursor:not-allowed}
table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:8px;overflow:hidden;font-size:13px}
th,td{padding:10px 14px;text-align:left;border-bottom:1px solid #334155;vertical-align:top}
th{background:#0f172a;color:#94a3b8;font-weight:500;text-transform:uppercase;letter-spacing:.05em;font-size:11px}
tbody tr:last-child td{border-bottom:0}
tbody tr:hover{background:#263449}
tbody tr.stale-row{background:#1c1917}
tbody tr.stale-row:hover{background:#292524}
td code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;color:#cbd5e1}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;letter-spacing:.03em}
.badge.RUNNING{background:#1d4ed8;color:#dbeafe}
.badge.COMPLETED{background:#166534;color:#dcfce7}
.badge.FAILED{background:#991b1b;color:#fee2e2}
.badge.CANCELLED{background:#6b7280;color:#f3f4f6}
.badge.TERMINATED{background:#52525b;color:#f4f4f5}
.badge.UNKNOWN{background:#334155;color:#e2e8f0}
.badge.Active{background:#166534;color:#dcfce7}
.badge.Draining{background:#92400e;color:#fef3c7}
.badge.Stopped{background:#334155;color:#e2e8f0}
.badge.timezone{background:#1e3a8a;color:#93c5fd;border:1px solid #3b82f6}
.timezone-utc{color:#64748b;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px}
.badge-owner{background:#312e81;color:#c7d2fe;border:1px solid #4338ca}
.badge-sev-sev1{background:#7f1d1d;color:#fee2e2;border:1px solid #b91c1c}
.badge-sev-sev2{background:#7c2d12;color:#ffedd5;border:1px solid #c2410c}
.badge-sev-sev3{background:#713f12;color:#fef9c3;border:1px solid #a16207}
.badge-sev-sev4{background:#065f46;color:#d1fae5;border:1px solid #047857}
.banner{padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px;font-weight:500}
.banner.Healthy{background:#14532d;color:#bbf7d0;border:1px solid #166534}
.banner.Degraded{background:#431407;color:#fed7aa;border:1px solid #92400e}
.banner.Unhealthy{background:#450a0a;color:#fecaca;border:1px solid #991b1b}
.flash{background:#172554;color:#bfdbfe;border:1px solid #1d4ed8;padding:10px 14px;border-radius:6px;margin-bottom:16px;font-size:13px}
.shard-header{margin:20px 0 8px;font-size:13px;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1e293b;padding-bottom:6px}
.shard-error{background:#1c1917;border:1px solid #57534e;border-radius:6px;padding:12px 16px;color:#a8a29e;font-size:13px;margin-bottom:12px}
.view-toggle{display:inline-flex;gap:2px;margin:0 0 16px;border:1px solid #334155;border-radius:6px;overflow:hidden;font-size:13px}
.view-toggle a,.view-toggle span{padding:6px 14px;display:inline-block}
.view-toggle a{color:#93c5fd;text-decoration:none}
.view-toggle a:hover{background:#1e293b}
.view-toggle span.active{background:#2563eb;color:#fff;font-weight:600}
.summary-stats{display:flex;gap:18px;flex-wrap:wrap;margin:0 0 16px;font-size:13px;color:#94a3b8}
.summary-stats strong{color:#e2e8f0}
.summary-stats .note{color:#fbbf24}
code.sample{display:inline-block;margin:0 4px 2px 0;font-size:11px;color:#cbd5e1}
.pagination{display:flex;gap:8px;align-items:center;margin-top:16px;font-size:13px;color:#94a3b8}
.pagination a,.pagination span{padding:6px 10px;border-radius:6px;border:1px solid #334155}
.pagination a{color:#93c5fd}
.pagination span.disabled{color:#475569}
.card{background:#1e293b;border:1px solid #334155;border-radius:8px;padding:16px;margin-bottom:16px}
.card h3{margin-top:0}
.kv{display:grid;grid-template-columns:180px 1fr;row-gap:6px;column-gap:16px;font-size:13px}
.kv .k{color:#94a3b8}
.kv .v{color:#e2e8f0;word-break:break-all}
pre{background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:12px;overflow:auto;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;margin:0}
.error-banner{background:#7f1d1d;color:#fee2e2;padding:10px 14px;border-radius:6px;margin-bottom:16px;font-size:13px}
.empty{color:#94a3b8;font-style:italic;padding:24px;text-align:center}
.detail-row{display:flex;gap:16px;align-items:center;margin-bottom:16px;flex-wrap:wrap}
.detail-row .back{color:#93c5fd;font-size:13px}
details{margin-top:8px}
details summary{cursor:pointer;color:#93c5fd;font-size:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.detail-block{display:grid;gap:10px;margin-top:10px}
footer{padding:20px 24px;color:#64748b;font-size:12px;text-align:center;border-top:1px solid #1e293b;margin-top:32px}
.event-label{font-size:13px}
.event-label code{font-size:11px;color:#64748b;margin-left:4px}
.operator-actions{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.operator-actions form{margin:0}
.operator-actions button,.operator-actions a.btn{background:#1e3a5f;color:#93c5fd;border:1px solid #2563eb;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;text-decoration:none;display:inline-block}
.operator-actions button:hover,.operator-actions a.btn:hover{background:#2563eb;color:#fff}
.operator-actions button.danger{background:#450a0a;color:#fca5a5;border-color:#991b1b}
.operator-actions button.danger:hover{background:#991b1b;color:#fff}
"#;

#[derive(Debug, Deserialize)]
pub(crate) struct WorkflowListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    search_attr_key: Option<String>,
    #[serde(default)]
    search_attr_value: Option<String>,
    /// ISO 8601 / RFC 3339 lower bound on `started_at`.
    #[serde(default)]
    started_after: Option<String>,
    /// ISO 8601 / RFC 3339 upper bound on `started_at`.
    #[serde(default)]
    started_before: Option<String>,
    /// Free-text prefix/substring match on execution id (UUID string).
    #[serde(default)]
    exec_id_search: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct WorkflowDetailParams {
    /// Zero-based page index for the event timeline.
    #[serde(default)]
    event_page: Option<i64>,
    /// Flash message to display at the top of the detail page.
    #[serde(default)]
    flash: Option<String>,
    /// Jump to the page containing this 1-based event number.
    #[serde(default)]
    jump_event: Option<i64>,
}

// ---------------------------------------------------------------------------
// Workflow detail action form structs
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct WorkflowCancelForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WorkflowPauseForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct WorkflowTerminateForm {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowSignalForm {
    signal_name: String,
    #[serde(default)]
    payload: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowResetForm {
    reset_to_event_id: i64,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WorkflowTriggerUpdateForm {
    update_name: String,
    #[serde(default)]
    payload: Option<String>,
}

// ---------------------------------------------------------------------------
// Blocked-on panel data
// ---------------------------------------------------------------------------

struct BlockedOnData {
    activities: Vec<TaskQueueItem>,
    external_tasks: Vec<ExternalTask>,
    timers: Vec<HarvestTimer>,
    signals: Vec<HarvestSignal>,
    /// Default response-side byte cap applied to a pending activity's heartbeat
    /// checkpoint payload before rendering (global #252 activity-result cap,
    /// #503). `0` = uncapped. Used when no per-activity override applies.
    heartbeat_details_cap: u64,
    /// Per-activity effective heartbeat checkpoint cap, keyed by activity name
    /// (per-activity `max_result_bytes` raised against the global ceiling),
    /// matching the API stack handler so configured large-payload activities
    /// keep full checkpoint visibility (#503 review). Missing names fall back to
    /// `heartbeat_details_cap`.
    heartbeat_caps: std::collections::HashMap<String, u64>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct WorkerListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    /// Filter by lifecycle status: `Active`, `Draining`, or `Stopped`.
    #[serde(default)]
    status: Option<String>,
    /// Filter by source shard id.
    #[serde(default)]
    shard: Option<i32>,
    /// Set to `"true"` to show only stale workers.
    #[serde(default)]
    stale: Option<String>,
    /// Filter by build ID (exact match).
    #[serde(default)]
    build_id: Option<String>,
    /// Auto-refresh interval in seconds (emits a `<meta http-equiv="refresh">` tag).
    #[serde(default)]
    refresh: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct BuildRoutingListParams {
    /// Flash message forwarded after a form action redirect.
    #[serde(default)]
    flash: Option<String>,
    /// When set, filter tables to entries related to this build ID.
    #[serde(default)]
    build_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingSetPolicyForm {
    queue_name: String,
    build_id: String,
    #[serde(default)]
    deployment_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingCompatForm {
    build_id: String,
    compatible_with: String,
}

#[derive(Debug, Deserialize)]
struct BuildRoutingRetireForm {
    build_id: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeadLetterListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    task_kind: Option<String>,
    #[serde(default)]
    failed_after: Option<String>,
    #[serde(default)]
    failed_before: Option<String>,
    #[serde(default)]
    shard_id: Option<i32>,
    #[serde(default)]
    refresh: Option<u64>,
    #[serde(default)]
    flash: Option<String>,
    /// `summary` switches to the root-cause aggregation view (issue #385).
    #[serde(default)]
    view: Option<String>,
    /// Comma-separated grouping dimensions for the summary view.
    #[serde(default)]
    group_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeadLetterTaskKind {
    Activity,
    Workflow,
}

impl DeadLetterTaskKind {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "activity" => Ok(Self::Activity),
            "workflow" => Ok(Self::Workflow),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown task_kind '{other}'; expected Activity or Workflow"
            ))),
        }
    }

    const fn as_db_value(self) -> &'static str {
        match self {
            Self::Activity => "ACTIVITY",
            Self::Workflow => "WORKFLOW",
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::Activity => "Activity",
            Self::Workflow => "Workflow",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct DeadLetterUiFilters {
    workflow_name: Option<String>,
    task_kind: Option<DeadLetterTaskKind>,
    failed_after: Option<DateTime<Utc>>,
    failed_before: Option<DateTime<Utc>>,
    shard_id: Option<i32>,
}

impl DeadLetterUiFilters {
    const fn is_empty(&self) -> bool {
        self.workflow_name.is_none()
            && self.task_kind.is_none()
            && self.failed_after.is_none()
            && self.failed_before.is_none()
            && self.shard_id.is_none()
    }
}

#[derive(Debug, Clone)]
struct DeadLetterUiRow {
    shard_id: ShardId,
    dead_letter: DeadLetter,
    workflow_name: Option<String>,
    events: Vec<HarvestEvent>,
}

// ---------------------------------------------------------------------------
// Per-shard query result (Ok = rows, Err = error message)
// ---------------------------------------------------------------------------

type ShardWorkerResult = (ShardId, Result<Vec<WorkerRow>, String>);
type ShardDeadLetterResult = (ShardId, Result<Vec<DeadLetterUiRow>, String>);

// ---------------------------------------------------------------------------
// Internal fleet stats computed from the full unfiltered worker list
// ---------------------------------------------------------------------------

struct WorkerFleetStats {
    total: usize,
    active: usize,
    draining: usize,
    stopped: usize,
    stale: usize,
    any_shard_errored: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BannerState {
    Healthy,
    Degraded,
    Unhealthy,
}

impl BannerState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "Healthy",
            Self::Degraded => "Degraded",
            Self::Unhealthy => "Unhealthy",
        }
    }
}

fn compute_fleet_stats(shard_results: &[ShardWorkerResult]) -> WorkerFleetStats {
    let any_shard_errored = shard_results.iter().any(|(_, r)| r.is_err());
    let mut total = 0usize;
    let mut active = 0usize;
    let mut draining = 0usize;
    let mut stopped = 0usize;
    let mut stale = 0usize;

    for (_, result) in shard_results {
        if let Ok(rows) = result {
            for row in rows {
                total += 1;
                match row.worker.status.as_str() {
                    "Active" => active += 1,
                    "Draining" => draining += 1,
                    _ => stopped += 1,
                }
                if row.health == WorkerHealth::Stale {
                    stale += 1;
                }
            }
        }
    }

    WorkerFleetStats {
        total,
        active,
        draining,
        stopped,
        stale,
        any_shard_errored,
    }
}

const fn determine_banner_state(stats: &WorkerFleetStats) -> Option<BannerState> {
    if stats.total == 0 && !stats.any_shard_errored {
        return None;
    }
    // Shard errors mean partial visibility — we can't rule out active workers
    // on the unreachable shard, so cap severity at Degraded.
    if stats.any_shard_errored {
        return Some(BannerState::Degraded);
    }
    if stats.active == 0 {
        return Some(BannerState::Unhealthy);
    }
    if stats.stale > 0 {
        return Some(BannerState::Degraded);
    }
    Some(BannerState::Healthy)
}

// Numeric sort key for a worker: (status_rank, is_healthy, worker_id).
// Stale workers sort before healthy within the same status bucket.
fn worker_sort_key(row: &WorkerRow) -> (u8, u8, &str) {
    let status_rank = match row.worker.status.as_str() {
        "Active" => 0,
        "Draining" => 1,
        _ => 2,
    };
    let health_rank = u8::from(row.health != WorkerHealth::Stale);
    (status_rank, health_rank, row.worker.worker_id.as_str())
}

/// Build the Vantage dashboard router.
pub fn harvest_ui_router(api_state: HarvestApiState) -> Router<AppState> {
    let require_admin = middleware::from_fn_with_state(api_state.clone(), require_harvest_admin);

    Router::new()
        .route("/", get(index))
        .route("/dags", get(list_dags_ui))
        .route("/dags/{dag_name}", get(dag_detail_ui))
        .route("/workflows", get(list_workflows_ui))
        .route("/workflows/{id}", get(workflow_detail_ui))
        .route("/workflows/{id}/cancel", post(cancel_workflow_ui))
        .route(
            "/workflows/{id}/terminate",
            post(terminate_workflow_ui).route_layer(require_admin.clone()),
        )
        .route("/workflows/{id}/pause", post(pause_workflow_ui))
        .route("/workflows/{id}/resume", post(resume_workflow_ui))
        .route("/workflows/{id}/signal", post(signal_workflow_ui))
        .route("/workflows/{id}/reset", post(reset_workflow_ui))
        .route("/workflows/{id}/trigger-update", post(trigger_update_ui))
        .route("/workers", get(list_workers_ui))
        .route(
            "/dead-letters",
            get(list_dead_letters_ui).route_layer(require_admin.clone()),
        )
        .route("/build-routing", get(list_build_routing_ui))
        .route(
            "/build-routing/set-policy",
            post(build_routing_set_policy_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/declare-compat",
            post(build_routing_declare_compat_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/revoke-compat",
            post(build_routing_revoke_compat_ui).route_layer(require_admin.clone()),
        )
        .route(
            "/build-routing/retire",
            post(build_routing_retire_ui).route_layer(require_admin.clone()),
        )
        .route("/schedules", get(list_schedules_ui))
        .route("/schedules/bulk-pause", post(schedule_bulk_pause_ui))
        .route("/schedules/bulk-resume", post(schedule_bulk_resume_ui))
        .route("/schedules/{id}/pause", post(schedule_pause_ui))
        .route("/schedules/{id}/resume", post(schedule_resume_ui))
        .route("/schedules/{id}/delete", post(schedule_delete_ui))
        .route("/schedules/{id}/trigger-now", post(schedule_trigger_now_ui))
        // issue #377: admission gates UI page and one-click lift (lift requires admin)
        .route("/admin/gates", get(list_gates_ui))
        .route(
            "/admin/gates/{id}/lift",
            post(lift_gate_ui).route_layer(require_admin),
        )
        .layer(Extension(api_state))
}

async fn index() -> axum::response::Redirect {
    axum::response::Redirect::to("workflows")
}

#[derive(Clone)]
struct DagUiSummary {
    name: String,
    schedule_expr: Option<String>,
    task_count: usize,
    is_paused: bool,
    next_run_at: Option<DateTime<Utc>>,
    max_active_runs: i32,
    catchup: bool,
}

#[derive(Debug, Deserialize, Default)]
struct DagDetailParams {
    #[serde(default)]
    run: Option<String>,
    #[serde(default)]
    node: Option<usize>,
    #[serde(default)]
    refresh: Option<u64>,
}

async fn list_dags_ui(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Markup, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let schedules = load_schedules_from_shards_ui(&api_state).await;
    let mut dags: HashMap<String, DagUiSummary> = runtime
        .dags()
        .iter()
        .map(|(name, dag)| (name.clone(), dag_summary_from_registered(name, dag)))
        .collect();
    let mut shard_errors = Vec::new();

    for (shard_id, shard_result) in schedules {
        match shard_result {
            Ok(rows) => {
                for row in rows {
                    let Some(dag_name) = row.dag_name.clone() else {
                        continue;
                    };
                    let entry = dags
                        .entry(dag_name.clone())
                        .or_insert_with(|| DagUiSummary {
                            name: dag_name.clone(),
                            schedule_expr: row.schedule_expr.clone(),
                            task_count: 0,
                            is_paused: row.is_paused,
                            next_run_at: row.next_run_at,
                            max_active_runs: row.max_active_runs,
                            catchup: row.catchup,
                        });
                    merge_dag_schedule_row(entry, &row);
                }
            }
            Err(error) => shard_errors.push((shard_id, error)),
        }
    }
    let mut dags = dags.into_values().collect::<Vec<_>>();
    dags.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(render_dag_list(&dags, &shard_errors))
}

async fn dag_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(dag_name): Path<String>,
    Query(params): Query<DagDetailParams>,
) -> Result<Markup, AutumnError> {
    let runtime = api_state.runtime().map_err(map_error)?;
    let dag =
        runtime.dags().get(&dag_name).cloned().ok_or_else(|| {
            AutumnError::not_found_msg(format!("DAG '{dag_name}' is not registered"))
        })?;
    let filters = WorkflowFilters {
        workflow_name: Some(dag_name.clone()),
        limit: 50,
        ..WorkflowFilters::default()
    };
    let runs = load_dag_runs_from_owning_shard(&api_state, &runtime, &dag_name, &filters).await?;
    let requested_run = params
        .run
        .as_deref()
        .and_then(|raw| uuid::Uuid::parse_str(raw).ok());
    let requested_run_is_valid_for_dag = match requested_run {
        Some(run_id) if runs.iter().any(|run| run.id == run_id) => true,
        Some(run_id) => dag_run_exists_for_dag(&api_state, &dag_name, run_id).await?,
        None => false,
    };
    let selected_run = select_dag_run_id(
        requested_run,
        requested_run_is_valid_for_dag,
        runs.as_slice().first().map(|r| r.id),
    );
    let node_states = if let Some(exec_id) = selected_run {
        let mut conn = db_conn_for_execution(
            &api_state,
            autumn_harvest::types::ExecutionId::from_uuid(exec_id),
        )
        .await?;
        let task_rows = harvest_task_queue::table
            .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_id)))
            .select(TaskQueueItem::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;

        // Load dag_skip: markers to distinguish condition-skipped nodes (issue #482).
        let marker_events: Vec<HarvestEvent> = harvest_events::table
            .filter(harvest_events::workflow_exec_id.eq(exec_id))
            .filter(harvest_events::event_type.eq("MarkerRecorded"))
            .select(HarvestEvent::as_select())
            .load(&mut conn)
            .await
            .map_err(database_error)
            .map_err(map_error)?;
        let condition_skipped: std::collections::HashSet<usize> = marker_events
            .iter()
            .filter_map(|e| {
                // event_data is adjacently-tagged: {"type":"MarkerRecorded","data":{...}}
                let data = e.event_data.get("data")?;
                let name = data["name"].as_str()?;
                let idx = parse_dag_skip_marker_index(name)?;
                // Guard against task rename/reorder across deploys: only mark
                // the node as condition-skipped when the recorded activity name
                // still matches the task at that index in the current definition.
                // MarkerRecorded serializes as {"type":…,"data":{"name":…,"details":{…}}},
                // so the task field lives under data.details.task.
                let recorded_task = data.get("details").and_then(|d| d["task"].as_str())?;
                let current_task = dag.definition.tasks().get(idx)?;
                if recorded_task != current_task.activity_name.as_str() {
                    return None;
                }
                // Also validate upstream fingerprint when present (new-format markers).
                // Old markers without "upstreams" pass through for backward compat.
                if let Some(arr) = data
                    .get("details")
                    .and_then(|d| d.get("upstreams"))
                    .and_then(|v| v.as_array())
                {
                    let recorded: Vec<usize> = arr
                        .iter()
                        .filter_map(|v| v.as_u64().and_then(|n| usize::try_from(n).ok()))
                        .collect();
                    if recorded != current_task.upstreams {
                        return None;
                    }
                }
                Some(idx)
            })
            .collect();

        map_node_states(&dag.definition, &task_rows, &condition_skipped)
    } else {
        HashMap::<usize, DagNodeState>::new()
    };
    Ok(render_dag_detail(
        &dag_name,
        &dag,
        &runs,
        selected_run,
        params.node,
        params.refresh,
        &node_states,
    ))
}

fn select_dag_run_id(
    requested_run: Option<uuid::Uuid>,
    requested_run_is_valid_for_dag: bool,
    fallback_run: Option<uuid::Uuid>,
) -> Option<uuid::Uuid> {
    requested_run
        .filter(|_| requested_run_is_valid_for_dag)
        .or(fallback_run)
}

fn dag_run_shard(router: &ShardRouter, dag_name: &str) -> ShardId {
    router.pick_for_dag(dag_name)
}

async fn load_dag_runs_from_owning_shard(
    api_state: &HarvestApiState,
    runtime: &HarvestApiRuntime,
    dag_name: &str,
    filters: &WorkflowFilters,
) -> Result<Vec<WorkflowExecution>, AutumnError> {
    let shard = dag_run_shard(runtime.router(), dag_name);
    let mut conn = db_conn_for_shard(api_state, shard).await?;
    load_workflows(&mut conn, filters).await.map_err(map_error)
}

async fn dag_run_exists_for_dag(
    api_state: &HarvestApiState,
    dag_name: &str,
    run_id: uuid::Uuid,
) -> Result<bool, AutumnError> {
    let mut conn = db_conn_for_execution(
        api_state,
        autumn_harvest::types::ExecutionId::from_uuid(run_id),
    )
    .await?;
    harvest_workflow_executions::table
        .find(run_id)
        .filter(harvest_workflow_executions::workflow_name.eq(dag_name))
        .select(harvest_workflow_executions::id)
        .first::<uuid::Uuid>(&mut conn)
        .await
        .optional()
        .map(|row| row.is_some())
        .map_err(database_error)
        .map_err(map_error)
}

#[allow(clippy::too_many_lines)]
async fn list_workflows_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<WorkflowListParams>,
) -> Result<Markup, AutumnError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    let state_filter = params
        .state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let workflow_name_filter = params
        .workflow_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let search_attr_key = params
        .search_attr_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let search_attr_value = params
        .search_attr_value
        .as_deref()
        .map(str::to_string)
        .unwrap_or_default();
    // Only enforce the search_attr predicate when the user supplied a key.
    // The value may legitimately be the empty string.
    let search_attr_pair = search_attr_key
        .as_ref()
        .map(|key| (key.clone(), search_attr_value.clone()));

    let started_after = match params
        .started_after
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        None => None,
        Some(v) => Some(
            DateTime::parse_from_rfc3339(v)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| {
                    AutumnError::bad_request_msg(format!(
                        "invalid started_after: expected RFC 3339 (e.g. 2026-01-01T00:00:00Z), got '{v}'"
                    ))
                })?,
        ),
    };
    let started_before = match params
        .started_before
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        None => None,
        Some(v) => Some(
            DateTime::parse_from_rfc3339(v)
                .map(|d| d.with_timezone(&Utc))
                .map_err(|_| {
                    AutumnError::bad_request_msg(format!(
                        "invalid started_before: expected RFC 3339 (e.g. 2026-01-01T00:00:00Z), got '{v}'"
                    ))
                })?,
        ),
    };
    let exec_id_search = params
        .exec_id_search
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_lowercase);

    let fetch_limit = offset.saturating_add(limit).saturating_add(1);
    let mut filters = WorkflowFilters::default().with_limit(fetch_limit);
    if let Some(state) = state_filter.as_deref() {
        filters.states.push(state.to_string());
    }
    filters.workflow_name.clone_from(&workflow_name_filter);
    if let Some((key, value)) = search_attr_pair.clone() {
        let mut object = serde_json::Map::with_capacity(1);
        object.insert(key, Value::String(value));
        filters.search_attrs.push(Value::Object(object));
    }
    filters.started_after = started_after;
    filters.started_before = started_before;
    filters.exec_id_prefix = exec_id_search.clone();

    let (workflows, _next_cursor) = load_workflows_from_shards(&api_state, &filters).await?;

    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let has_next = workflows.len() > offset_usize.saturating_add(limit_usize);
    let workflows = workflows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

    // issue #377: check active gate count for the UI banner (no DB call — uses in-process cache).
    let active_gate_count = api_state.gate_cache().active_count();

    Ok(render_workflow_list(
        &workflows,
        page,
        limit,
        has_next,
        state_filter.as_deref(),
        workflow_name_filter.as_deref(),
        search_attr_pair.as_ref(),
        started_after,
        started_before,
        exec_id_search.as_deref(),
        active_gate_count,
    ))
}

#[allow(clippy::too_many_lines)]
async fn workflow_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
    Query(params): Query<WorkflowDetailParams>,
    headers: axum::http::HeaderMap,
    maybe_session: Option<Extension<Session>>,
) -> Result<Markup, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let exec_uuid = exec_id.as_uuid();
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;

    // Resolve event_page before any DB queries so we can use OFFSET/LIMIT directly.
    let page_size = DETAIL_EVENT_PAGE_SIZE;
    let event_page = if let Some(jump) = params.jump_event {
        let jump_zero = (jump - 1).max(0);
        jump_zero / page_size
    } else {
        params.event_page.unwrap_or(0).max(0)
    };

    // Total event count — used for pagination controls.
    let total_events: i64 = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .count()
        .get_result(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Page of events for the timeline — only the current page is fetched.
    let page_offset = event_page.saturating_mul(page_size);
    let page_events: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .order(harvest_events::event_id.asc())
        .offset(page_offset)
        .limit(page_size)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Activity-type events for the attempts panel. Heartbeats are excluded from
    // the type filter. We fetch the most recent ACTIVITY_PANEL_MAX_EVENTS rows
    // (DESC) and reverse them so collect_activity_attempts sees chronological
    // order; this ensures activities scheduled near the end of a long history
    // are never silently dropped by the cap.
    let mut activity_events: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_events::event_type.eq_any(ACTIVITY_PANEL_EVENT_TYPES))
        .order(harvest_events::event_id.desc())
        .limit(ACTIVITY_PANEL_MAX_EVENTS)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;
    activity_events.reverse();

    // Signal/update events for the signals panel. Fetch DESC so the most recent
    // entries are kept when the panel is capped; reverse before rendering so the
    // table reads oldest→newest.
    let signal_update_events_raw: Vec<HarvestEvent> = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_events::event_type.eq_any(SIGNAL_UPDATE_TYPES))
        .order(harvest_events::event_id.desc())
        .limit(i64::try_from(SIGNAL_UPDATE_PANEL_LIMIT).unwrap_or(20) + 1)
        .select(HarvestEvent::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let signal_update_overflow = signal_update_events_raw.len() > SIGNAL_UPDATE_PANEL_LIMIT;
    // Take the most recent SIGNAL_UPDATE_PANEL_LIMIT entries (raw is DESC) and
    // restore chronological order for the panel table.
    let mut signal_update_events: Vec<HarvestEvent> = signal_update_events_raw
        .into_iter()
        .take(SIGNAL_UPDATE_PANEL_LIMIT)
        .collect();
    signal_update_events.reverse();

    // Load direct children (cap at 50 to avoid overwhelming the UI).
    let children: Vec<WorkflowExecution> = harvest_workflow_executions::table
        .filter(harvest_workflow_executions::parent_id.eq(Some(exec_uuid)))
        .order(harvest_workflow_executions::created_at.asc())
        .limit(50)
        .select(WorkflowExecution::as_select())
        .load(&mut conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // Load blocked-on data for non-terminal workflows. Reuse the #252
    // activity-result payload cap as the response-side guard for heartbeat
    // checkpoints (#503); fall back to the default when no runtime is installed.
    let heartbeat_details_cap = api_state.runtime().ok().map_or(
        autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
        |r| r.registry().max_activity_result_bytes,
    );
    let mut blocked_on = load_blocked_on_data(
        &mut conn,
        exec_uuid,
        &execution.state,
        heartbeat_details_cap,
    )
    .await?;
    resolve_blocked_on_heartbeat_caps(&api_state, &mut blocked_on);

    // Resolve the continue-as-new threshold from the runtime registry if available.
    // This is a lightweight read of an in-memory value — no extra DB query.
    let continue_as_new_threshold = api_state
        .runtime()
        .ok()
        .map(|r| r.registry().history_policy().continue_as_new_threshold());

    // Read-path payload decoding (issue #608): decode the loaded copies only
    // (stored rows are never touched); one best-effort audit row per page
    // render that decoded or marked ≥1 envelope. Only the fields the page
    // actually renders are decoded — the attempts/signals panel event copies
    // (`activity_events`/`signal_update_events`) are deliberately excluded
    // since those panels never render payload fields (PR #936 review).
    let mut execution = execution;
    let mut page_events = page_events;
    decode_and_audit_workflow_detail(
        &api_state,
        &mut conn,
        &headers,
        extension_session(maybe_session),
        exec_id,
        &mut execution,
        &mut page_events,
        &mut blocked_on,
    )
    .await;

    Ok(render_workflow_detail(
        &execution,
        total_events,
        &page_events,
        &activity_events,
        &signal_update_events,
        signal_update_overflow,
        &children,
        event_page,
        &blocked_on,
        params.flash.as_deref(),
        continue_as_new_threshold,
    ))
}

/// Decode only the workflow-detail fields the renderer actually displays
/// (PR #936 review, round 5): the execution row's payload fields (the
/// Input/Output/Memo/Search-attributes cards + the error banner), the
/// timeline page's event payloads (rendered in full under "view payload"),
/// and the blocked-on panel's heartbeat checkpoints. Hidden fields are
/// deliberately NOT decoded — the pending-activity `input`, pending-signal
/// payloads, and the attempts/signals panel event copies render only
/// names / error strings / timestamps, never payload fields, so decoding
/// them would burn codec/KMS work and count envelopes in the
/// `payload.decode_read` audit outcome for plaintext the operator is never
/// shown. Returns the merged outcome for the page's single audit row, so the
/// audit accounting covers exactly the surfaced fields.
fn decode_workflow_detail_rendered_fields(
    codecs: &PayloadCodecs,
    execution: &mut WorkflowExecution,
    timeline_events: &mut [HarvestEvent],
    blocked_on: &mut BlockedOnData,
) -> LossyDecodeOutcome {
    let mut outcome = decode_workflow_execution_fields(execution, codecs);
    for event in timeline_events.iter_mut() {
        outcome = outcome.merged(codecs.decode_value_lossy(&mut event.event_data));
    }
    for task in &mut blocked_on.activities {
        if let Some(checkpoint) = task.heartbeat_details.as_mut() {
            outcome = outcome.merged(codecs.decode_value_lossy(checkpoint));
        }
    }
    outcome
}

/// Read-path payload decoding for the workflow-detail page (issue #608):
/// resolves the decode-only-when-admin gate ([`read_path_decoder`]) and, when
/// active, tolerantly decodes the loaded copies of the rendered fields only
/// (see [`decode_workflow_detail_rendered_fields`]), then writes the page's
/// single best-effort audit row when ≥1 surfaced envelope was touched.
/// Operates on in-memory copies only; stored rows are untouched.
///
/// `conn` is the page handler's own (execution-shard) pooled connection —
/// the audit row is written through it rather than acquiring a second
/// connection while the caller's is still live (PR #936 review).
#[allow(clippy::too_many_arguments)]
async fn decode_and_audit_workflow_detail(
    api_state: &HarvestApiState,
    conn: &mut AsyncPgConnection,
    headers: &axum::http::HeaderMap,
    session: Option<Session>,
    exec_id: HarvestExecutionId,
    execution: &mut WorkflowExecution,
    timeline_events: &mut [HarvestEvent],
    blocked_on: &mut BlockedOnData,
) {
    let Some(codecs) = read_path_decoder(api_state, session).await else {
        return;
    };
    let outcome =
        decode_workflow_detail_rendered_fields(&codecs, execution, timeline_events, blocked_on);
    let target = exec_id.to_string();
    audit_decoded_read(
        api_state,
        Some(conn),
        headers,
        TARGET_WORKFLOW,
        Some(&target),
        "GET /ui/workflows/{id}",
        Some(exec_id.shard()),
        outcome,
        Some(SOURCE_UI),
    )
    .await;
}

fn is_terminal_workflow_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "CONTINUED_AS_NEW" | "TERMINATED"
    )
}

async fn load_blocked_on_data(
    conn: &mut AsyncPgConnection,
    exec_uuid: uuid::Uuid,
    state: &str,
    heartbeat_details_cap: u64,
) -> Result<BlockedOnData, AutumnError> {
    if is_terminal_workflow_state(state) {
        return Ok(BlockedOnData {
            activities: vec![],
            external_tasks: vec![],
            timers: vec![],
            signals: vec![],
            heartbeat_details_cap,
            heartbeat_caps: std::collections::HashMap::new(),
        });
    }

    let activities: Vec<TaskQueueItem> = harvest_task_queue::table
        .filter(harvest_task_queue::workflow_exec_id.eq(Some(exec_uuid)))
        .filter(
            harvest_task_queue::state
                .eq("PENDING")
                .or(harvest_task_queue::state.eq("CLAIMED"))
                .or(harvest_task_queue::state.eq("RUNNING"))
                .or(harvest_task_queue::state.eq("BACKOFF")),
        )
        .filter(harvest_task_queue::task_type.eq("activity"))
        // Stable ordering (id tiebreaker) so the per-page checkpoint budget
        // (#503) keeps/omits the same checkpoints deterministically.
        .order((
            harvest_task_queue::scheduled_at.asc(),
            harvest_task_queue::id.asc(),
        ))
        .limit(20)
        .select(TaskQueueItem::as_select())
        .load::<TaskQueueItem>(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let timers: Vec<HarvestTimer> = harvest_timers::table
        .filter(harvest_timers::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_timers::fired.eq(false))
        .order(harvest_timers::fires_at.asc())
        .limit(20)
        .select(HarvestTimer::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    let signals: Vec<HarvestSignal> = harvest_signals::table
        .filter(harvest_signals::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_signals::consumed.eq(false))
        .order(harvest_signals::received_at.asc())
        .limit(20)
        .select(HarvestSignal::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    // External activities awaiting a third-party result (state = 'PENDING').
    let external_tasks: Vec<ExternalTask> = harvest_external_tasks::table
        .filter(harvest_external_tasks::workflow_exec_id.eq(exec_uuid))
        .filter(harvest_external_tasks::state.eq("PENDING"))
        .order(harvest_external_tasks::created_at.asc())
        .limit(20)
        .select(ExternalTask::as_select())
        .load(conn)
        .await
        .map_err(database_error)
        .map_err(map_error)?;

    Ok(BlockedOnData {
        activities,
        external_tasks,
        timers,
        signals,
        heartbeat_details_cap,
        heartbeat_caps: std::collections::HashMap::new(),
    })
}

/// Resolve each pending activity's effective heartbeat checkpoint cap
/// (per-activity `max_result_bytes` raised against the global ceiling),
/// mirroring the stack API so a configured large-payload activity keeps full
/// checkpoint visibility in the UI too (#503 review). No-op when no runtime is
/// installed (the global default cap then applies at render time).
fn resolve_blocked_on_heartbeat_caps(api_state: &HarvestApiState, blocked_on: &mut BlockedOnData) {
    if let Ok(rt) = api_state.runtime() {
        let registry = rt.registry();
        blocked_on.heartbeat_caps = blocked_on
            .activities
            .iter()
            .filter_map(|t| t.activity_name.clone())
            .map(|name| {
                let cap = registry.activity_result_cap(&name);
                (name, cap)
            })
            .collect();
    }
}

// ---------------------------------------------------------------------------
// Workflow UI action handlers
// ---------------------------------------------------------------------------

async fn cancel_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowCancelForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form.reason.as_deref().unwrap_or("").trim().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let cancel_result =
        cancel_workflow_execution(&mut conn, exec_id, &reason, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &cancel_result {
        Ok(_) => (STATUS_SUCCEEDED, None, url_encode("Workflow cancelled")),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Cancel failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_CANCEL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/cancel",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

/// Force-terminate a single workflow execution from the detail page (issue #788).
///
/// Mirrors [`cancel_workflow_ui`] exactly — same shard resolution, actor
/// extraction, metrics fallback, audit plumbing, and detail-page redirect — but
/// delegates to the forceful [`terminate_workflow_execution`] core path (#504)
/// and records the `OP_WORKFLOW_TERMINATE` audit op.
async fn terminate_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowTerminateForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form.reason.as_deref().unwrap_or("").trim().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let terminate_result =
        terminate_workflow_execution(&mut conn, exec_id, &reason, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &terminate_result {
        Ok(r) if r.newly_cancelled => (STATUS_SUCCEEDED, None, url_encode("Workflow terminated")),
        Ok(_) => (
            STATUS_SUCCEEDED,
            None,
            url_encode("Workflow was already terminal — no change made"),
        ),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Terminate failed: {msg}")),
            )
        }
    };
    if let Err(audit_err) = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_TERMINATE,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/terminate",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await
    {
        warn!(
            error = %audit_err,
            exec_id = %exec_id_str,
            "audit insert failed for workflow.terminate"
        );
    }

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn pause_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowPauseForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();
    let reason = form
        .reason
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty());

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let result =
        pause_workflow_execution(&mut conn, exec_id, reason, &actor, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &result {
        Ok(_) => (STATUS_SUCCEEDED, None, url_encode("Workflow paused")),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Pause failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_PAUSE,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/pause",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn resume_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let metrics_ref: Arc<dyn autumn_harvest::telemetry::MetricsRecorder> =
        api_state.runtime().map_or_else(
            |_| Arc::new(autumn_harvest::telemetry::NoOpMetrics) as _,
            |rt| Arc::clone(&rt.registry().telemetry().metrics),
        );
    let result = resume_workflow_execution(&mut conn, exec_id, &actor, metrics_ref.as_ref()).await;
    let (status, error_summary, flash) = match &result {
        Ok(_) => (STATUS_SUCCEEDED, None, url_encode("Workflow resumed")),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Resume failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_RESUME,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/resume",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn signal_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowSignalForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let payload_str = form.payload.as_deref().unwrap_or("").trim();
    let payload_result: Result<serde_json::Value, String> = if payload_str.is_empty() {
        Ok(serde_json::Value::Null)
    } else {
        serde_json::from_str(payload_str).map_err(|e| format!("Invalid JSON payload: {e}"))
    };
    let (status, error_summary, flash) = match payload_result {
        Err(e) => (STATUS_FAILED, Some(e.clone()), url_encode(&e)),
        Ok(payload_json) => {
            let cap = api_state
                .runtime()
                .ok()
                .map_or(0, |r| r.registry().max_signal_payload_bytes);
            let observed = serde_json::to_string(&payload_json).map_or(0, |s| s.len() as u64);
            if cap > 0 && observed > cap {
                let msg = format!(
                    "signal payload too large: {observed} bytes exceeds cap of {cap} bytes"
                );
                (STATUS_FAILED, Some(msg.clone()), url_encode(&msg))
            } else {
                match send_signal(&mut conn, exec_id, &form.signal_name, payload_json).await {
                    Ok(()) => (
                        STATUS_SUCCEEDED,
                        None,
                        url_encode(&format!("Signal '{}' sent", form.signal_name)),
                    ),
                    Err(e) => {
                        let msg = e.to_string();
                        (
                            STATUS_FAILED,
                            Some(msg.clone()),
                            url_encode(&format!("Signal failed: {msg}")),
                        )
                    }
                }
            }
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_SIGNAL,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/signal",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn reset_workflow_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowResetForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let reason = form
        .reason
        .as_deref()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("workflow reset requested")
        .to_string();

    // The form shows 1-based event numbers (matching the timeline "#" column).
    // The reset API accepts 0-based event IDs.
    let reset_to_event_id = form.reset_to_event_id.saturating_sub(1);

    let request = WorkflowResetRequest {
        reset_to_event_id: Some(reset_to_event_id),
        reset_point: None,
        reason,
        operator_id: actor.clone(),
        signal_reapply: ResetSignalReapplyPolicy::default(),
        allow_terminal_source: false,
    };

    let runtime = api_state.runtime().ok();
    let registry = runtime.as_ref().map(|r| r.registry().as_ref());
    let reset_result = reset_workflow_execution(&mut conn, exec_id, request, registry).await;
    let (status, error_summary, flash) = match &reset_result {
        Ok(result) => (
            STATUS_SUCCEEDED,
            None,
            url_encode(&format!(
                "Reset complete — new execution {}",
                result.new_exec_id
            )),
        ),
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Reset failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: OP_WORKFLOW_RESET,
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/reset",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

async fn trigger_update_ui(
    Extension(api_state): Extension<HarvestApiState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<WorkflowTriggerUpdateForm>,
) -> Result<axum::response::Response, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let actor = api_state.extract_actor(&headers);
    let exec_id_str = exec_id.as_uuid().to_string();

    let payload_str = form.payload.as_deref().unwrap_or("").trim();
    let payload_json: serde_json::Value = if payload_str.is_empty() {
        serde_json::Value::Null
    } else {
        match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(e) => {
                let err_msg = format!("Invalid JSON payload: {e}");
                let _ = insert_audit(
                    &mut conn,
                    &NewAuditRecord {
                        actor: &actor,
                        operation: "workflow.update",
                        target_type: TARGET_WORKFLOW,
                        target_id: Some(&exec_id_str),
                        route_or_command: "POST /workflows/{id}/trigger-update",
                        request_id: None,
                        idempotency_key: None,
                        status: STATUS_FAILED,
                        error_summary: Some(&err_msg),
                        shard_id: None,
                        source: SOURCE_UI,
                    },
                )
                .await;
                let flash = url_encode(&err_msg);
                let redirect_url = format!("../../workflows/{id}?flash={flash}");
                return Ok(axum::response::Redirect::to(&redirect_url).into_response());
            }
        }
    };

    let update_id = UpdateId::new();
    let (status, error_summary, flash) = match admit_update_event(
        &mut conn,
        exec_id,
        update_id,
        form.update_name.clone(),
        payload_json,
    )
    .await
    {
        Ok(()) => {
            // Wake the workflow task so it picks up the admitted update immediately.
            // Surface any wake failure in the flash so the operator knows to retry.
            let wake_note =
                match autumn_harvest::queue::wake_workflow_task(&mut conn, exec_id).await {
                    Ok(()) => String::new(),
                    Err(e) => format!(" (wake failed: {e})"),
                };
            (
                STATUS_SUCCEEDED,
                None,
                url_encode(&format!(
                    "Update '{}' admitted{}",
                    form.update_name, wake_note
                )),
            )
        }
        Err(e) => {
            let msg = e.to_string();
            (
                STATUS_FAILED,
                Some(msg.clone()),
                url_encode(&format!("Update failed: {msg}")),
            )
        }
    };
    let _ = insert_audit(
        &mut conn,
        &NewAuditRecord {
            actor: &actor,
            operation: "workflow.update",
            target_type: TARGET_WORKFLOW,
            target_id: Some(&exec_id_str),
            route_or_command: "POST /workflows/{id}/trigger-update",
            request_id: None,
            idempotency_key: None,
            status,
            error_summary: error_summary.as_deref(),
            shard_id: None,
            source: SOURCE_UI,
        },
    )
    .await;

    let redirect_url = format!("../../workflows/{id}?flash={flash}");
    Ok(axum::response::Redirect::to(&redirect_url).into_response())
}

// ---------------------------------------------------------------------------
// Dead-letter UI
// ---------------------------------------------------------------------------

async fn list_dead_letters_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<DeadLetterListParams>,
    headers: axum::http::HeaderMap,
    maybe_session: Option<Extension<Session>>,
) -> Result<Markup, AutumnError> {
    // Read-path payload decoding (issue #608): the page is admin-gated, so an
    // arriving request passes the same predicate the decoder re-checks.
    let decoder = read_path_decoder(&api_state, extension_session(maybe_session)).await;
    let limit = params
        .limit
        .unwrap_or(DEFAULT_DLQ_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);
    let filters = parse_dead_letter_ui_filters(
        params.workflow_name.as_deref(),
        params.task_kind.as_deref(),
        params.failed_after.as_deref(),
        params.failed_before.as_deref(),
        params.shard_id,
    )?;

    let pool = api_state.storage_pool().map_err(map_error)?;

    // Summary toggle (issue #385): the root-cause aggregation view.
    if params.view.as_deref() == Some("summary") {
        return render_dead_letters_summary_view(
            &pool,
            &filters,
            params.group_by.as_deref(),
            limit,
            params.refresh,
            params.flash.as_deref(),
        )
        .await;
    }

    let fetch_limit = offset.saturating_add(limit).saturating_add(1);
    let shard_results = load_dead_letters_from_shards_for_ui(&pool, &filters, fetch_limit).await;
    let is_multi_shard = shard_results.len() > 1;

    let mut all_rows: Vec<DeadLetterUiRow> = shard_results
        .iter()
        .flat_map(|(_, result)| result.iter().flat_map(|rows| rows.iter().cloned()))
        .collect();
    all_rows.sort_by(|left, right| {
        right
            .dead_letter
            .failed_at
            .cmp(&left.dead_letter.failed_at)
            .then_with(|| right.dead_letter.id.cmp(&left.dead_letter.id))
            .then_with(|| left.shard_id.as_i32().cmp(&right.shard_id.as_i32()))
    });

    let total_matching = count_dead_letters_from_shards_for_ui(&pool, &filters).await;
    let total_for_pagination = total_matching.unwrap_or(all_rows.len());
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_for_pagination > offset_usize.saturating_add(limit_usize);
    let mut page_rows = all_rows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

    if let Some(codecs) = decoder.as_ref() {
        // Read-path payload decoding (issue #608): decode each rendered row's
        // JSONB input, TEXT error, and last-events copies; one best-effort
        // audit row per page render that touched ≥1 envelope.
        let mut outcome = LossyDecodeOutcome::default();
        for row in &mut page_rows {
            outcome = outcome.merged(codecs.decode_value_lossy(&mut row.dead_letter.input));
            outcome = outcome.merged(decode_error_field(codecs, &mut row.dead_letter.error));
            for event in &mut row.events {
                outcome = outcome.merged(codecs.decode_value_lossy(&mut event.event_data));
            }
        }
        // No live connection here: the per-shard DLQ loads are scoped inside
        // their `_from_shards_for_ui` helpers, so the pool-acquiring branch
        // is safe (PR #936 review).
        audit_decoded_read(
            &api_state,
            None,
            &headers,
            TARGET_DEAD_LETTER,
            None,
            "GET /ui/dead-letters",
            None,
            outcome,
            Some(SOURCE_UI),
        )
        .await;
    }
    let page_rows = page_rows;

    let shard_errors: Vec<(ShardId, &str)> = shard_results
        .iter()
        .filter_map(|(shard_id, result)| result.as_ref().err().map(|e| (*shard_id, e.as_str())))
        .collect();

    Ok(render_dead_letters_page(
        &filters,
        &page_rows,
        &shard_errors,
        is_multi_shard,
        page,
        limit,
        has_next,
        total_for_pagination,
        params.refresh,
        params.flash.as_deref(),
    ))
}

fn parse_dead_letter_ui_filters(
    workflow_name: Option<&str>,
    task_kind: Option<&str>,
    failed_after: Option<&str>,
    failed_before: Option<&str>,
    shard_id: Option<i32>,
) -> Result<DeadLetterUiFilters, AutumnError> {
    let workflow_name = workflow_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let task_kind = task_kind
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(DeadLetterTaskKind::parse)
        .transpose()?;
    let failed_after = parse_dead_letter_time_filter("failed_after", failed_after)?;
    let failed_before = parse_dead_letter_time_filter("failed_before", failed_before)?;

    Ok(DeadLetterUiFilters {
        workflow_name,
        task_kind,
        failed_after,
        failed_before,
        shard_id,
    })
}

fn parse_dead_letter_time_filter(
    field: &str,
    raw: Option<&str>,
) -> Result<Option<DateTime<Utc>>, AutumnError> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| {
            AutumnError::bad_request_msg(format!("invalid {field}; expected RFC 3339 timestamp"))
        })?
        .with_timezone(&Utc);
    Ok(Some(parsed))
}

async fn load_dead_letters_from_shards_for_ui(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
    limit: i64,
) -> Vec<ShardDeadLetterResult> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if filters
                .shard_id
                .is_some_and(|wanted| wanted != shard_id.as_i32())
            {
                return (shard_id, Ok(Vec::new()));
            }
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                let rows = query_dead_letters_for_ui(&mut conn, filters, limit)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut out = Vec::with_capacity(rows.len());
                for dead_letter in rows {
                    let workflow_name = load_dead_letter_workflow_name(&mut conn, &dead_letter)
                        .await
                        .map_err(|e| e.to_string())?;
                    let events = load_dead_letter_events(&mut conn, &dead_letter)
                        .await
                        .map_err(|e| e.to_string())?;
                    out.push(DeadLetterUiRow {
                        shard_id,
                        dead_letter,
                        workflow_name,
                        events,
                    });
                }
                Ok(out)
            }
            .await;
            (shard_id, result)
        })
        .collect();
    futures::future::join_all(futs).await
}

async fn count_dead_letters_from_shards_for_ui(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
) -> Result<usize, String> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if filters
                .shard_id
                .is_some_and(|wanted| wanted != shard_id.as_i32())
            {
                return Ok(0usize);
            }
            let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
            count_dead_letters_for_ui(&mut conn, filters)
                .await
                .map(|count| usize::try_from(count).unwrap_or(0))
                .map_err(|e| e.to_string())
        })
        .collect();
    let counts = futures::future::join_all(futs).await;
    counts.into_iter().try_fold(0usize, |acc, count| {
        count.map(|count| acc.saturating_add(count))
    })
}

macro_rules! apply_dead_letter_ui_filters {
    ($query:ident, $filters:expr) => {
        if let Some(ref workflow_name) = $filters.workflow_name {
            $query = $query.filter(
                sql::<Bool>("workflow_exec_id IN (SELECT id FROM harvest_workflow_executions WHERE workflow_name = ")
                    .bind::<Text, _>(workflow_name.clone())
                    .sql(")"),
            );
        }
        if let Some(task_kind) = $filters.task_kind {
            $query = $query.filter(
                sql::<Bool>("LOWER(task_type) = LOWER(")
                    .bind::<Text, _>(task_kind.as_db_value().to_string())
                    .sql(")"),
            );
        }
        if let Some(failed_after) = $filters.failed_after {
            $query = $query.filter(harvest_dead_letters::failed_at.ge(failed_after));
        }
        if let Some(failed_before) = $filters.failed_before {
            $query = $query.filter(harvest_dead_letters::failed_at.lt(failed_before));
        }
    };
}

async fn query_dead_letters_for_ui(
    conn: &mut AsyncPgConnection,
    filters: &DeadLetterUiFilters,
    limit: i64,
) -> HarvestResult<Vec<DeadLetter>> {
    let mut query = harvest_dead_letters::table
        .into_boxed()
        .order(harvest_dead_letters::failed_at.desc())
        .limit(limit);
    apply_dead_letter_ui_filters!(query, filters);
    query
        .select(DeadLetter::as_select())
        .load(conn)
        .await
        .map_err(database_error)
}

async fn count_dead_letters_for_ui(
    conn: &mut AsyncPgConnection,
    filters: &DeadLetterUiFilters,
) -> HarvestResult<i64> {
    let mut query = harvest_dead_letters::table.into_boxed();
    apply_dead_letter_ui_filters!(query, filters);
    query.count().get_result(conn).await.map_err(database_error)
}

async fn load_dead_letter_workflow_name(
    conn: &mut AsyncPgConnection,
    dead_letter: &DeadLetter,
) -> HarvestResult<Option<String>> {
    let Some(exec_id) = dead_letter.workflow_exec_id else {
        return Ok(None);
    };
    harvest_workflow_executions::table
        .find(exec_id)
        .select(harvest_workflow_executions::workflow_name)
        .first(conn)
        .await
        .optional()
        .map_err(database_error)
}

async fn load_dead_letter_events(
    conn: &mut AsyncPgConnection,
    dead_letter: &DeadLetter,
) -> HarvestResult<Vec<HarvestEvent>> {
    let Some(exec_id) = dead_letter.workflow_exec_id else {
        return Ok(Vec::new());
    };
    let mut events = harvest_events::table
        .filter(harvest_events::workflow_exec_id.eq(exec_id))
        .order(harvest_events::event_id.desc())
        .limit(10)
        .select(HarvestEvent::as_select())
        .load(conn)
        .await
        .map_err(database_error)?;
    events.reverse();
    Ok(events)
}

// ---------------------------------------------------------------------------
// Workers UI
// ---------------------------------------------------------------------------

async fn list_workers_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<WorkerListParams>,
) -> Result<Markup, AutumnError> {
    let (status_filter, stale_only) = parse_worker_ui_filters(&params)?;

    let limit = params
        .limit
        .unwrap_or(DEFAULT_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    let stale_threshold = api_state.worker_stale_threshold();
    let pool = api_state.storage_pool().map_err(map_error)?;

    // Load all workers without status filter so fleet stats reflect true fleet
    // state regardless of the active UI filter.
    let shard_results = load_workers_from_shards(&pool, None, stale_threshold).await;

    let stats = compute_fleet_stats(&shard_results);
    let banner_state = determine_banner_state(&stats);
    let is_multi_shard = shard_results.len() > 1;

    let mut all_workers: Vec<(ShardId, WorkerRow)> = shard_results
        .iter()
        .flat_map(|(shard_id, result)| {
            let shard_id = *shard_id;
            result
                .iter()
                .flat_map(move |rows| rows.iter().map(move |r| (shard_id, r.clone())))
        })
        .filter(|(shard_id, row)| {
            if params.shard.is_some_and(|f| shard_id.as_i32() != f) {
                return false;
            }
            if let Some(sf) = status_filter
                && row.worker.status != sf
            {
                return false;
            }
            if stale_only && row.health != WorkerHealth::Stale {
                return false;
            }
            if let Some(ref bf) = params.build_id
                && !bf.is_empty()
                && row.worker.build_id != *bf
            {
                return false;
            }
            true
        })
        .collect();

    all_workers.sort_by(|(sa, a), (sb, b)| {
        sa.as_i32()
            .cmp(&sb.as_i32())
            .then_with(|| worker_sort_key(a).cmp(&worker_sort_key(b)))
    });

    let total_filtered = all_workers.len();
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_filtered > offset_usize.saturating_add(limit_usize);
    let page_workers: Vec<(ShardId, WorkerRow)> = all_workers
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect();

    let mut grouped: Vec<(ShardId, Vec<WorkerRow>)> = Vec::new();
    for (shard_id, row) in page_workers {
        match grouped.last_mut() {
            Some((sid, rows)) if *sid == shard_id => rows.push(row),
            _ => grouped.push((shard_id, vec![row])),
        }
    }

    let shard_errors: Vec<(ShardId, &str)> = shard_results
        .iter()
        .filter_map(|(shard_id, result)| result.as_ref().err().map(|e| (*shard_id, e.as_str())))
        .collect();

    let build_id_filter = params.build_id.as_deref().filter(|s| !s.is_empty());

    Ok(render_workers_page(
        &stats,
        banner_state,
        &grouped,
        &shard_errors,
        is_multi_shard,
        page,
        limit,
        has_next,
        status_filter,
        params.shard,
        stale_only,
        build_id_filter,
        params.refresh,
    ))
}

fn parse_worker_ui_filters(
    params: &WorkerListParams,
) -> Result<(Option<&'static str>, bool), AutumnError> {
    let status_filter = match params.status.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(s) => Some(match s.to_lowercase().as_str() {
            "active" => "Active",
            "draining" => "Draining",
            "stopped" => "Stopped",
            other => {
                return Err(AutumnError::bad_request_msg(format!(
                    "unknown status '{other}'; expected one of Active, Draining, Stopped"
                )));
            }
        }),
    };
    let stale_only = match params.stale.as_deref().map(str::trim) {
        None | Some("" | "false") => false,
        Some("true") => true,
        Some(other) => {
            return Err(AutumnError::bad_request_msg(format!(
                "unknown stale value '{other}'; expected 'true' or 'false'"
            )));
        }
    };
    Ok((status_filter, stale_only))
}

async fn load_workers_from_shards(
    pool: &crate::HarvestDbPool,
    status_filter: Option<&str>,
    stale_threshold: std::time::Duration,
) -> Vec<ShardWorkerResult> {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| {
            let unlimited = WorkerFilters {
                limit: i64::MAX,
                status: status_filter.map(str::to_string),
                ..WorkerFilters::new()
            };
            async move {
                let result = async {
                    let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                    list_workers(&mut conn, &unlimited, stale_threshold)
                        .await
                        .map_err(|e| e.to_string())
                }
                .await;
                (shard_id, result)
            }
        })
        .collect();
    futures::future::join_all(futs).await
}

#[allow(clippy::too_many_arguments)]
fn render_dead_letters_page(
    filters: &DeadLetterUiFilters,
    rows: &[DeadLetterUiRow],
    shard_errors: &[(ShardId, &str)],
    is_multi_shard: bool,
    page: i64,
    limit: i64,
    has_next: bool,
    total_matching: usize,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Markup {
    let body = html! {
        h2 { "Dead Letters" }
        @if let Some(message) = flash {
            div.flash { (message) }
        }
        (render_dead_letter_view_toggle(filters, limit, refresh, None, false))
        (render_dead_letter_filters(filters, limit, refresh))
        (render_dead_letter_bulk_actions(filters, limit, refresh, total_matching))

        @if rows.is_empty() && shard_errors.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No dead-lettered tasks. Healthy."
                } @else {
                    "No entries match this filter."
                }
            }
        } @else {
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }

            (render_dead_letter_table(rows, filters, limit, refresh))
        }

        (render_dead_letter_pagination(page, limit, has_next, filters, refresh))
    };

    layout_dead_letters("Dead Letters · Vantage", &body, refresh)
}

// ---------------------------------------------------------------------------
// DLQ root-cause summary view (issue #385)
// ---------------------------------------------------------------------------

/// Render the DLQ summary view: in-process root-cause aggregation, the same
/// computation behind `GET /dead-letters/aggregate`, surfaced as a UI toggle.
async fn render_dead_letters_summary_view(
    pool: &crate::HarvestDbPool,
    filters: &DeadLetterUiFilters,
    group_by_raw: Option<&str>,
    limit: i64,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Result<Markup, AutumnError> {
    let group_by = parse_dlq_summary_group_by(group_by_raw)?;
    let group_by_value = group_by
        .iter()
        .map(|dim| dim.as_wire())
        .collect::<Vec<_>>()
        .join(",");

    let params = autumn_harvest::dlq::DlqAggregateParams {
        group_by: group_by.clone(),
        time_bucket: autumn_harvest::dlq::TimeBucketGranularity::Hour,
        workflow_name: filters.workflow_name.clone(),
        activity_name: None,
        queue_name: None,
        task_type: filters.task_kind.map(|k| k.as_db_value().to_string()),
        since: filters.failed_after,
        until: filters.failed_before,
        min_attempts: None,
        limit_groups: DLQ_SUMMARY_GROUP_LIMIT,
        samples_per_group: DLQ_SUMMARY_SAMPLES_PER_GROUP,
    };

    let (response, shard_errors) =
        aggregate_dead_letters_for_ui(pool, &params, filters.shard_id).await;

    let body = html! {
        h2 { "Dead Letters" }
        @if let Some(message) = flash {
            div.flash { (message) }
        }
        (render_dead_letter_view_toggle(filters, limit, refresh, Some(&group_by_value), true))
        (render_dead_letter_filters(filters, limit, refresh))
        (render_dlq_summary_group_by_form(filters, limit, refresh, &group_by))

        @for (shard_id, error) in &shard_errors {
            div.shard-error {
                strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                (error)
            }
        }

        (render_dlq_summary_stats(&response))

        @if response.groups.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No dead-lettered tasks. Healthy."
                } @else {
                    "No entries match this filter."
                }
            }
        } @else {
            (render_dlq_summary_table(&response, &group_by, filters, limit, refresh))
        }
    };

    Ok(layout_dead_letters(
        "Dead Letters · Summary · Vantage",
        &body,
        refresh,
    ))
}

/// Parse the comma-separated `group_by` query value into validated dimensions,
/// falling back to [`DEFAULT_DLQ_SUMMARY_GROUP_BY`] when empty. Mirrors the
/// `400`-on-unknown-dimension contract of the aggregation endpoint.
fn parse_dlq_summary_group_by(
    raw: Option<&str>,
) -> Result<Vec<autumn_harvest::dlq::DlqGroupDimension>, AutumnError> {
    let raw = raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_DLQ_SUMMARY_GROUP_BY);

    let mut dims: Vec<autumn_harvest::dlq::DlqGroupDimension> = Vec::new();
    for part in raw.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let dim = autumn_harvest::dlq::DlqGroupDimension::from_wire(part).ok_or_else(|| {
            AutumnError::bad_request_msg(format!(
                "unknown group_by dimension '{part}'; expected one of: workflow_name, \
                 activity_name, queue_name, task_type, time_bucket, failure_signature"
            ))
        })?;
        if !dims.contains(&dim) {
            dims.push(dim);
        }
    }

    if dims.is_empty() {
        return Err(AutumnError::bad_request_msg(
            "at least one group_by dimension is required",
        ));
    }
    Ok(dims)
}

/// Fan out the per-shard aggregation and merge into a single response, mirroring
/// the management endpoint's `iter_shards()` merge. Per-shard errors are
/// surfaced rather than failing the whole view.
async fn aggregate_dead_letters_for_ui(
    pool: &crate::HarvestDbPool,
    params: &autumn_harvest::dlq::DlqAggregateParams,
    shard_filter: Option<i32>,
) -> (
    autumn_harvest::dlq::DlqAggregateResponse,
    Vec<(ShardId, String)>,
) {
    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            if shard_filter.is_some_and(|wanted| wanted != shard_id.as_i32()) {
                return (shard_id, Ok(None));
            }
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                autumn_harvest::dlq::aggregate_dead_letters(&mut conn, params)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            (shard_id, result.map(Some))
        })
        .collect();

    let results = futures::future::join_all(futs).await;
    let mut partials = Vec::new();
    let mut errors = Vec::new();
    for (shard_id, result) in results {
        match result {
            Ok(Some(partial)) => partials.push(partial),
            Ok(None) => {}
            Err(error) => errors.push((shard_id, error)),
        }
    }

    (
        autumn_harvest::dlq::merge_dlq_aggregates(params, partials),
        errors,
    )
}

fn render_dead_letter_view_toggle(
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
    group_by_value: Option<&str>,
    summary_active: bool,
) -> Markup {
    let base = build_dead_letter_query_string(limit, filters, refresh);
    let list_href = if base.is_empty() {
        "dead-letters".to_string()
    } else {
        format!("dead-letters?{}", &base[1..])
    };
    let group_by_query = group_by_value
        .filter(|value| !value.is_empty())
        .map(|value| format!("&group_by={}", url_encode(value)))
        .unwrap_or_default();
    let summary_href = format!("dead-letters?view=summary{base}{group_by_query}");

    html! {
        div."view-toggle" {
            @if summary_active {
                a href=(list_href) { "List" }
                span.active { "Summary" }
            } @else {
                span.active { "List" }
                a href=(summary_href) { "Summary" }
            }
        }
    }
}

fn render_dlq_summary_group_by_form(
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
    selected: &[autumn_harvest::dlq::DlqGroupDimension],
) -> Markup {
    // Presets cover the high-value triage cuts; the selected value is preserved
    // even if it is not one of the presets (custom query string).
    const PRESETS: &[(&str, &str)] = &[
        ("workflow_name,failure_signature", "Workflow × signature"),
        ("failure_signature", "Failure signature"),
        ("workflow_name", "Workflow"),
        ("activity_name", "Activity"),
        ("activity_name,failure_signature", "Activity × signature"),
        ("queue_name", "Queue"),
        ("task_type", "Task type"),
        ("time_bucket", "Time bucket (hour)"),
    ];
    let selected_value = selected
        .iter()
        .map(|dim| dim.as_wire())
        .collect::<Vec<_>>()
        .join(",");
    let selected_is_preset = PRESETS.iter().any(|(value, _)| *value == selected_value);

    html! {
        form.filters method="get" action="dead-letters" {
            input type="hidden" name="view" value="summary";
            (render_dead_letter_hidden_filters(filters))
            @if limit != DEFAULT_DLQ_PAGE_SIZE {
                input type="hidden" name="limit" value=(limit);
            }
            @if let Some(refresh) = refresh {
                input type="hidden" name="refresh" value=(refresh);
            }
            label {
                "Group by"
                select name="group_by" {
                    @for (value, label) in PRESETS {
                        option value=(value) selected[*value == selected_value] { (label) }
                    }
                    @if !selected_is_preset {
                        option value=(selected_value) selected { (selected_value) }
                    }
                }
            }
            button type="submit" { "Group" }
        }
    }
}

fn render_dlq_summary_stats(response: &autumn_harvest::dlq::DlqAggregateResponse) -> Markup {
    html! {
        div."summary-stats" {
            span { strong { (response.filtered_total) } " matching" }
            span { strong { (response.total) } " total in DLQ" }
            span { strong { (response.groups.len()) } " groups" }
            @if response.truncated {
                span.note { "long tail rolled into “other”" }
            }
        }
    }
}

fn render_dlq_summary_table(
    response: &autumn_harvest::dlq::DlqAggregateResponse,
    group_by: &[autumn_harvest::dlq::DlqGroupDimension],
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    html! {
        table {
            thead {
                tr {
                    @for dim in group_by {
                        th { (dim.as_wire()) }
                    }
                    th { "count" }
                    th { "first_seen" }
                    th { "last_seen" }
                    th { "samples" }
                    th { "actions" }
                }
            }
            tbody {
                @for group in &response.groups {
                    @let is_other = group
                        .key
                        .get("_other")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false);
                    tr {
                        @if is_other {
                            td colspan=(group_by.len()) { em { "other (long tail)" } }
                        } @else {
                            @for dim in group_by {
                                td { (dlq_summary_key_cell(&group.key, dim.as_wire())) }
                            }
                        }
                        td { (group.count) }
                        td { (format_timestamp(group.first_seen)) }
                        td { (format_timestamp(group.last_seen)) }
                        td {
                            @if group.sample_dead_letter_ids.is_empty() {
                                "—"
                            } @else {
                                @for sample in &group.sample_dead_letter_ids {
                                    code.sample { (sample) }
                                }
                            }
                        }
                        td {
                            @if is_other {
                                "—"
                            } @else {
                                @let (href, partial) = dlq_summary_drilldown_href(&group.key, group_by, filters, limit, refresh);
                                a href=(href) title=[partial.then_some("Some dimensions have no list-view filter — results may include extra rows from other groups")] {
                                    @if partial {
                                        "View entries (partial filter) →"
                                    } @else {
                                        "View entries →"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn dlq_summary_key_cell(key: &serde_json::Value, dim: &str) -> String {
    match key.get(dim) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Null) | None => "—".to_string(),
        Some(other) => other.to_string(),
    }
}

/// Build a click-through link into the list view with whatever filters the list
/// view can express pre-applied from this group's key.
///
/// Returns `(href, is_partial)`. `is_partial` is `true` when one or more
/// group dimensions (`activity_name`, `queue_name`, `time_bucket`,
/// `failure_signature`) have no equivalent list-view filter — the resulting
/// link will show a superset of the selected group.
fn dlq_summary_drilldown_href(
    key: &serde_json::Value,
    group_by: &[autumn_harvest::dlq::DlqGroupDimension],
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> (String, bool) {
    use autumn_harvest::dlq::DlqGroupDimension;

    // Start from the filters already applied to the summary so drill-down
    // narrows rather than widens.
    let mut drill = filters.clone();
    let mut partial = false;
    for dim in group_by {
        match dim {
            DlqGroupDimension::WorkflowName => {
                if let Some(serde_json::Value::String(name)) = key.get("workflow_name") {
                    drill.workflow_name = Some(name.clone());
                }
            }
            DlqGroupDimension::TaskType => {
                if let Some(serde_json::Value::String(task_type)) = key.get("task_type") {
                    drill.task_kind = DeadLetterTaskKind::parse(task_type).ok();
                }
            }
            // No list-view filter exists for these dimensions; the link will
            // show more rows than belong to this exact group.
            DlqGroupDimension::ActivityName
            | DlqGroupDimension::QueueName
            | DlqGroupDimension::TimeBucket
            | DlqGroupDimension::FailureSignature => {
                partial = true;
            }
        }
    }

    let query = build_dead_letter_query_string(limit, &drill, refresh);
    let href = if query.is_empty() {
        "dead-letters".to_string()
    } else {
        format!("dead-letters?{}", &query[1..])
    };
    (href, partial)
}

fn render_dead_letter_filters(
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let workflow_name = filters.workflow_name.as_deref().unwrap_or("");
    let task_kind = filters.task_kind.map(DeadLetterTaskKind::as_label);
    let failed_after = filters
        .failed_after
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_default();
    let failed_before = filters
        .failed_before
        .map(|ts| ts.to_rfc3339())
        .unwrap_or_default();
    let shard_id = filters
        .shard_id
        .map(|id| id.to_string())
        .unwrap_or_default();
    let refresh_value = refresh.map(|secs| secs.to_string()).unwrap_or_default();

    html! {
        form.filters method="get" action="dead-letters" {
            label {
                "Workflow name"
                input type="text" name="workflow_name" value=(workflow_name) placeholder="e.g. invoice_workflow";
            }
            label {
                "Task kind"
                select name="task_kind" {
                    option value="" selected[task_kind.is_none()] { "All" }
                    option value="Activity" selected[task_kind == Some("Activity")] { "Activity" }
                    option value="Workflow" selected[task_kind == Some("Workflow")] { "Workflow" }
                }
            }
            label {
                "Failed after"
                input type="text" name="failed_after" value=(failed_after) placeholder="2026-05-10T00:00:00Z";
            }
            label {
                "Failed before"
                input type="text" name="failed_before" value=(failed_before) placeholder="2026-05-11T00:00:00Z";
            }
            label {
                "Shard"
                input type="number" name="shard_id" value=(shard_id) placeholder="e.g. 0";
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            label {
                "Refresh"
                select name="refresh" {
                    option value="" selected[refresh.is_none()] { "Off" }
                    option value="30" selected[refresh == Some(30)] { "30s" }
                    option value="60" selected[refresh == Some(60)] { "60s" }
                    @if refresh.is_some_and(|secs| secs != 30 && secs != 60) {
                        option value=(refresh_value) selected { (refresh_value) "s" }
                    }
                }
            }
            button type="submit" { "Apply" }
            a.reset href="dead-letters" { "Reset" }
        }
    }
}

fn render_dead_letter_bulk_actions(
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
    total_matching: usize,
) -> Markup {
    let return_to = dead_letter_return_to_path(filters, limit, refresh);
    let action_limit = dead_letter_bulk_action_limit(total_matching);
    let replay_label = dead_letter_bulk_action_label("Replay", action_limit, total_matching);
    let discard_label = dead_letter_bulk_action_label("Discard", action_limit, total_matching);
    let replay_confirm = dead_letter_bulk_action_confirm("Replay", action_limit, total_matching);
    let discard_confirm = dead_letter_bulk_action_confirm("Discard", action_limit, total_matching);
    html! {
        div."bulk-actions" {
            form method="post" action="../dead-letters/replay" onsubmit={ "return confirm('" (replay_confirm) "')" } {
                (render_dead_letter_hidden_filters(filters))
                input type="hidden" name="limit" value=(action_limit);
                input type="hidden" name="return_to" value=(return_to);
                button type="submit" disabled[total_matching == 0 || filters.is_empty()] {
                    (replay_label)
                }
            }
            form method="post" action="../dead-letters/discard" onsubmit={ "return confirm('" (discard_confirm) "')" } {
                (render_dead_letter_hidden_filters(filters))
                input type="hidden" name="limit" value=(action_limit);
                input type="hidden" name="return_to" value=(return_to);
                button.danger type="submit" disabled[total_matching == 0 || filters.is_empty()] {
                    (discard_label)
                }
            }
        }
    }
}

fn dead_letter_bulk_action_limit(total_matching: usize) -> usize {
    total_matching.clamp(1, DLQ_BULK_ACTION_LIMIT)
}

fn dead_letter_bulk_action_label(verb: &str, action_limit: usize, total_matching: usize) -> String {
    if total_matching > action_limit {
        format!("{verb} first {action_limit} matching ({total_matching} total)")
    } else {
        format!("{verb} all matching ({total_matching})")
    }
}

fn dead_letter_bulk_action_confirm(
    verb: &str,
    action_limit: usize,
    total_matching: usize,
) -> String {
    if total_matching > action_limit {
        format!("{verb} first {action_limit} of {total_matching} matching dead-letter entries?")
    } else {
        format!("{verb} {total_matching} matching dead-letter entries?")
    }
}

fn render_dead_letter_table(
    rows: &[DeadLetterUiRow],
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let return_to = dead_letter_return_to_path(filters, limit, refresh);
    html! {
        table {
            thead {
                tr {
                    th { "dead_letter_id" }
                    th { "workflow_name" }
                    th { "workflow_exec_id" }
                    th { "task_kind" }
                    th { "attempt" }
                    th { "failed_at" }
                    th { "error_message" }
                    th { "shard_id" }
                    th { "actions" }
                }
            }
            tbody {
                @for row in rows {
                    @let id = row.dead_letter.id.to_string();
                    @let workflow_name = row.workflow_name.as_deref().unwrap_or("unknown");
                    @let task_kind = dead_letter_task_kind_label(&row.dead_letter.task_type);
                    tr {
                        td { code { (id) } }
                        td { (workflow_name) }
                        td {
                            @if let Some(exec_id) = row.dead_letter.workflow_exec_id {
                                @let exec = exec_id.to_string();
                                a href={ "workflows/" (exec) } { code { (exec) } }
                            } @else {
                                "—"
                            }
                        }
                        td { (task_kind) }
                        td { (row.dead_letter.attempts) }
                        td { (format_timestamp(Some(row.dead_letter.failed_at))) }
                        td {
                            (truncate_error(&row.dead_letter.error))
                            (render_dead_letter_detail(row))
                        }
                        td { (row.shard_id.as_i32()) }
                        td {
                            div.actions {
                                form method="post" action="../dead-letters/replay" onsubmit="return confirm('Replay this dead-letter entry?')" {
                                    input type="hidden" name="dead_letter_id" value=(id);
                                    input type="hidden" name="return_to" value=(return_to);
                                    button type="submit" { "Replay" }
                                }
                                form method="post" action="../dead-letters/discard" onsubmit="return confirm('Discard this dead-letter entry?')" {
                                    input type="hidden" name="dead_letter_id" value=(id);
                                    input type="hidden" name="return_to" value=(return_to);
                                    button.danger type="submit" { "Discard" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_dead_letter_detail(row: &DeadLetterUiRow) -> Markup {
    html! {
        details {
            summary { "details" }
            div."detail-block" {
                div {
                    h3 { "Full error" }
                    pre { (row.dead_letter.error) }
                }
                div {
                    h3 { "Original payload" }
                    pre { (pretty_json(&row.dead_letter.input)) }
                }
                div {
                    h3 { "Last 10 events" }
                    @if row.events.is_empty() {
                        div.empty { "No workflow events found." }
                    } @else {
                        table {
                            thead {
                                tr {
                                    th { "#" }
                                    th { "Type" }
                                    th { "Timestamp" }
                                    th { "Data" }
                                }
                            }
                            tbody {
                                @for event in &row.events {
                                    tr {
                                        td { (event.event_id) }
                                        td { code { (event.event_type) } }
                                        td { (format_timestamp(Some(event.timestamp))) }
                                        td { pre { (pretty_json(&event.event_data)) } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_dead_letter_hidden_filters(filters: &DeadLetterUiFilters) -> Markup {
    let task_kind = filters.task_kind.map(DeadLetterTaskKind::as_label);
    let failed_after = filters.failed_after.map(|ts| ts.to_rfc3339());
    let failed_before = filters.failed_before.map(|ts| ts.to_rfc3339());
    html! {
        @if let Some(workflow_name) = filters.workflow_name.as_deref() {
            input type="hidden" name="workflow_name" value=(workflow_name);
        }
        @if let Some(task_kind) = task_kind {
            input type="hidden" name="task_kind" value=(task_kind);
        }
        @if let Some(failed_after) = failed_after.as_deref() {
            input type="hidden" name="failed_after" value=(failed_after);
        }
        @if let Some(failed_before) = failed_before.as_deref() {
            input type="hidden" name="failed_before" value=(failed_before);
        }
        @if let Some(shard_id) = filters.shard_id {
            input type="hidden" name="shard_id" value=(shard_id);
        }
    }
}

fn render_dead_letter_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    filters: &DeadLetterUiFilters,
    refresh: Option<u64>,
) -> Markup {
    let base = build_dead_letter_query_string(limit, filters, refresh);
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "dead-letters?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "dead-letters?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_dead_letter_query_string(
    limit: i64,
    filters: &DeadLetterUiFilters,
    refresh: Option<u64>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_DLQ_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(workflow_name) = filters.workflow_name.as_deref() {
        let _ = write!(out, "&workflow_name={}", url_encode(workflow_name));
    }
    if let Some(task_kind) = filters.task_kind {
        let _ = write!(out, "&task_kind={}", task_kind.as_label());
    }
    if let Some(failed_after) = filters.failed_after {
        let _ = write!(
            out,
            "&failed_after={}",
            url_encode(&failed_after.to_rfc3339())
        );
    }
    if let Some(failed_before) = filters.failed_before {
        let _ = write!(
            out,
            "&failed_before={}",
            url_encode(&failed_before.to_rfc3339())
        );
    }
    if let Some(shard_id) = filters.shard_id {
        let _ = write!(out, "&shard_id={shard_id}");
    }
    if let Some(refresh) = refresh {
        let _ = write!(out, "&refresh={refresh}");
    }
    out
}

fn dead_letter_return_to_path(
    filters: &DeadLetterUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> String {
    let query = build_dead_letter_query_string(limit, filters, refresh);
    if query.is_empty() {
        "../ui/dead-letters".to_string()
    } else {
        format!("../ui/dead-letters?{}", &query[1..])
    }
}

fn dead_letter_task_kind_label(task_type: &str) -> &'static str {
    if task_type.eq_ignore_ascii_case("activity") {
        "Activity"
    } else if task_type.eq_ignore_ascii_case("workflow") {
        "Workflow"
    } else if task_type.eq_ignore_ascii_case("callback") {
        // Issue #605 completion-callback exhaustion. The generic "Replay"
        // action still works for this row -- `dlq::replay_dead_letter`
        // delegates it to the completion-delivery redrive primitive (issue
        // #921 review) -- so no separate UI treatment is needed beyond a
        // clear label.
        "Callback"
    } else {
        "Unknown"
    }
}

fn truncate_error(error: &str) -> String {
    const LIMIT: usize = 96;
    let mut chars = error.chars();
    let truncated = chars.by_ref().take(LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn layout_dead_letters(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a.active href="dead-letters" { "Dead Letters" }
                        a href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_workers_page(
    stats: &WorkerFleetStats,
    banner_state: Option<BannerState>,
    grouped: &[(ShardId, Vec<WorkerRow>)],
    shard_errors: &[(ShardId, &str)],
    is_multi_shard: bool,
    page: i64,
    limit: i64,
    has_next: bool,
    status_filter: Option<&str>,
    shard_filter: Option<i32>,
    stale_only: bool,
    build_id_filter: Option<&str>,
    refresh: Option<u64>,
) -> Markup {
    let total_workers: usize = grouped.iter().map(|(_, rows)| rows.len()).sum();

    let body = html! {
        h2 { "Workers" }

        // Fleet health banner
        (render_fleet_banner(stats, banner_state))

        // Filters
        (render_worker_filters(status_filter, shard_filter, stale_only, build_id_filter, limit))

        // Worker table (grouped by shard if multi-shard)
        @if total_workers == 0 && shard_errors.is_empty() {
            div.card.empty {
                @if stats.total == 0 {
                    "No workers registered. Start a worker to see it here."
                } @else {
                    "No workers match this filter."
                }
            }
        } @else {
            // Shard error stubs
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }

            // Worker rows grouped by shard
            @for (shard_id, rows) in grouped {
                @if is_multi_shard {
                    div.shard-header { "Shard " (shard_id.as_i32()) }
                }
                (render_worker_table(rows, *shard_id))
            }
        }

        (render_worker_pagination(page, limit, has_next, status_filter, shard_filter, stale_only, build_id_filter))
    };

    layout_workers("Workers · Vantage", &body, refresh)
}

fn render_fleet_banner(stats: &WorkerFleetStats, banner_state: Option<BannerState>) -> Markup {
    let Some(verdict) = banner_state else {
        return html! {
            div.banner.Healthy { "Healthy — 0 workers registered" }
        };
    };

    let label = verdict.as_str();
    let class = format!("banner {label}");
    html! {
        div class=(class) {
            strong { (label) }
            " — "
            (stats.total) " workers | "
            (stats.active) " active | "
            (stats.draining) " draining | "
            (stats.stopped) " stopped | "
            (stats.stale) " stale"
        }
    }
}

fn render_worker_table(rows: &[WorkerRow], shard_id: ShardId) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "Worker ID" }
                    th { "Status" }
                    th { "Build ID" }
                    th { "Deployment" }
                    th { "Last Heartbeat" }
                    th { "Shard" }
                    th { "In-Flight" }
                }
            }
            tbody {
                @for row in rows {
                    @let is_stale = row.health == WorkerHealth::Stale;
                    @let row_class = if is_stale { "stale-row" } else { "" };
                    tr class=(row_class) {
                        td { code { (short_id(&row.worker.worker_id)) } }
                        td { (worker_status_badge(&row.worker.status, is_stale)) }
                        td {
                            @if row.worker.build_id.is_empty() {
                                span style="color:#475569" { "—" }
                            } @else {
                                a href={ "build-routing?build_id=" (url_encode(&row.worker.build_id)) }
                                  title="View in Build Routing" {
                                    code { (row.worker.build_id.chars().take(16).collect::<String>()) }
                                }
                            }
                        }
                        td {
                            @if let Some(ref dep) = row.worker.deployment_name {
                                code { (dep) }
                            } @else {
                                span style="color:#475569" { "—" }
                            }
                        }
                        td {
                            @let rel = relative_time(row.worker.last_heartbeat_at);
                            @let abs = format_timestamp(Some(row.worker.last_heartbeat_at));
                            time datetime=(row.worker.last_heartbeat_at.to_rfc3339()) title=(abs) {
                                (rel)
                            }
                        }
                        td { (shard_id.as_i32()) }
                        td { (row.worker.in_flight_count) }
                    }
                }
            }
        }
    }
}

fn render_worker_filters(
    status_filter: Option<&str>,
    shard_filter: Option<i32>,
    stale_only: bool,
    build_id_filter: Option<&str>,
    limit: i64,
) -> Markup {
    let shard_value = shard_filter.map(|s| s.to_string()).unwrap_or_default();
    let build_id_value = build_id_filter.unwrap_or("");
    html! {
        form.filters method="get" action="workers" {
            label {
                "Status"
                select name="status" {
                    option value="" selected[status_filter.is_none()] { "All" }
                    @for s in ["Active", "Draining", "Stopped"] {
                        option value=(s) selected[status_filter == Some(s)] { (s) }
                    }
                }
            }
            label {
                "Build ID"
                input type="text" name="build_id" value=(build_id_value) placeholder="e.g. abc123";
            }
            label {
                "Shard"
                input type="number" name="shard" value=(shard_value) placeholder="e.g. 0";
            }
            label {
                "Stale only"
                select name="stale" {
                    option value="" selected[!stale_only] { "All" }
                    option value="true" selected[stale_only] { "Stale only" }
                }
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            button type="submit" { "Apply" }
            a.reset href="workers" { "Reset" }
        }
    }
}

fn render_worker_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    status_filter: Option<&str>,
    shard_filter: Option<i32>,
    stale_only: bool,
    build_id_filter: Option<&str>,
) -> Markup {
    let base = build_worker_query_string(
        limit,
        status_filter,
        shard_filter,
        stale_only,
        build_id_filter,
    );
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "workers?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "workers?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_worker_query_string(
    limit: i64,
    status_filter: Option<&str>,
    shard_filter: Option<i32>,
    stale_only: bool,
    build_id_filter: Option<&str>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(status) = status_filter {
        let _ = write!(out, "&status={}", url_encode(status));
    }
    if let Some(build_id) = build_id_filter {
        let _ = write!(out, "&build_id={}", url_encode(build_id));
    }
    if let Some(shard) = shard_filter {
        let _ = write!(out, "&shard={shard}");
    }
    if stale_only {
        let _ = write!(out, "&stale=true");
    }
    out
}

fn worker_status_badge(status: &str, stale: bool) -> Markup {
    let class = format!("badge {status}");
    html! {
        span class=(class) {
            (status)
            @if stale { " (stale)" }
        }
    }
}

/// Format a `DateTime<Utc>` as a human-readable relative time string.
fn relative_time(ts: DateTime<Utc>) -> String {
    let elapsed = Utc::now()
        .signed_duration_since(ts)
        .to_std()
        .unwrap_or(std::time::Duration::ZERO);
    let secs = elapsed.as_secs();
    if secs < 5 {
        "just now".to_string()
    } else if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m ago", secs / 60)
    } else if secs < 86400 {
        format!("{}h ago", secs / 3600)
    } else {
        format!("{}d ago", secs / 86400)
    }
}

fn layout_workers(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a.active href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a href="dead-letters" { "Dead Letters" }
                        a href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_workflow_list(
    workflows: &[WorkflowExecution],
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after: Option<DateTime<Utc>>,
    started_before: Option<DateTime<Utc>>,
    exec_id_search: Option<&str>,
    active_gate_count: usize,
) -> Markup {
    let body = html! {
        h2 { "Workflows" }

        // issue #377: admission gate banner — shown when any gate is active.
        @if active_gate_count > 0 {
            div class="banner Unhealthy" {
                strong { "⚠ Admission gate active" }
                " — "
                (active_gate_count)
                @if active_gate_count == 1 { " gate is" } @else { " gates are" }
                " blocking new workflow starts. "
                a href="../admin/gates" { "Manage gates →" }
            }
        }

        (render_filters(state_filter, workflow_name_filter, search_attr_filter, started_after, started_before, exec_id_search, limit))

        @if workflows.is_empty() {
            div.card.empty { "No workflows match this filter." }
        } @else {
            table {
                thead {
                    tr {
                        th { "ID" }
                        th { "Workflow" }
                        th { "State" }
                        th { "Queue" }
                        th { "Started" }
                        th { "Completed" }
                    }
                }
                tbody {
                    @for execution in workflows {
                        @let id = execution.id.to_string();
                        tr {
                            td {
                                a href={ "workflows/" (id) } { code { (short_id(&id)) } }
                            }
                            td { (execution.workflow_name) }
                            td { (state_badge(&execution.state)) }
                            td { code { (execution.queue_name) } }
                            td { (format_timestamp(Some(execution.started_at))) }
                            td { (format_timestamp(execution.completed_at)) }
                        }
                    }
                }
            }
        }

        (render_pagination(page, limit, has_next, state_filter, workflow_name_filter, search_attr_filter, started_after, started_before, exec_id_search))
    };

    layout("Workflows · Vantage", &body, "")
}

#[allow(clippy::too_many_arguments)]
fn render_filters(
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after: Option<DateTime<Utc>>,
    started_before: Option<DateTime<Utc>>,
    exec_id_search: Option<&str>,
    limit: i64,
) -> Markup {
    let (attr_key, attr_value) =
        search_attr_filter.map_or(("", ""), |(k, v)| (k.as_str(), v.as_str()));
    let workflow_name_value = workflow_name_filter.unwrap_or("");
    let started_after_value = started_after.map(|d| d.to_rfc3339()).unwrap_or_default();
    let started_before_value = started_before.map(|d| d.to_rfc3339()).unwrap_or_default();
    let exec_id_search_value = exec_id_search.unwrap_or("");

    html! {
        form.filters method="get" action="workflows" {
            label {
                "State"
                select name="state" {
                    option value="" { "All" }
                    @for state in KNOWN_STATES {
                        @let selected = state_filter.is_some_and(|filter| filter == *state);
                        @if selected {
                            option value=(*state) selected { (*state) }
                        } @else {
                            option value=(*state) { (*state) }
                        }
                    }
                }
            }
            label {
                "Workflow name"
                input type="text" name="workflow_name" value=(workflow_name_value) placeholder="e.g. onboarding";
            }
            label {
                "Started after"
                input type="text" name="started_after" value=(started_after_value) placeholder="2026-01-01T00:00:00Z";
            }
            label {
                "Started before"
                input type="text" name="started_before" value=(started_before_value) placeholder="2026-12-31T23:59:59Z";
            }
            label {
                "Exec ID search"
                input type="text" name="exec_id_search" value=(exec_id_search_value) placeholder="UUID prefix…";
            }
            label {
                "Search attr key"
                input type="text" name="search_attr_key" value=(attr_key) placeholder="e.g. tenant";
            }
            label {
                "Search attr value"
                input type="text" name="search_attr_value" value=(attr_value) placeholder="e.g. acme";
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            button type="submit" { "Apply" }
            a.reset href="workflows" { "Reset" }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn render_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after: Option<DateTime<Utc>>,
    started_before: Option<DateTime<Utc>>,
    exec_id_search: Option<&str>,
) -> Markup {
    let base_query = build_query_string(
        limit,
        state_filter,
        workflow_name_filter,
        search_attr_filter,
        started_after,
        started_before,
        exec_id_search,
    );

    html! {
        div.pagination {
            @if page > 0 {
                a href={ "workflows?page=" (page - 1) (PreEscaped(&base_query)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }

            span { "Page " (page + 1) }

            @if has_next {
                a href={ "workflows?page=" (page + 1) (PreEscaped(&base_query)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn build_query_string(
    limit: i64,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    started_after: Option<DateTime<Utc>>,
    started_before: Option<DateTime<Utc>>,
    exec_id_search: Option<&str>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(state) = state_filter {
        let _ = write!(out, "&state={}", url_encode(state));
    }
    if let Some(name) = workflow_name_filter {
        let _ = write!(out, "&workflow_name={}", url_encode(name));
    }
    if let Some((key, value)) = search_attr_filter {
        let _ = write!(out, "&search_attr_key={}", url_encode(key));
        let _ = write!(out, "&search_attr_value={}", url_encode(value));
    }
    if let Some(after) = started_after {
        let _ = write!(out, "&started_after={}", url_encode(&after.to_rfc3339()));
    }
    if let Some(before) = started_before {
        let _ = write!(out, "&started_before={}", url_encode(&before.to_rfc3339()));
    }
    if let Some(search) = exec_id_search {
        let _ = write!(out, "&exec_id_search={}", url_encode(search));
    }
    out
}

const DETAIL_EVENT_PAGE_SIZE: i64 = 100;
/// Maximum activity-type events fetched for the attempts panel.
/// Heartbeats are excluded from the filter, so this cap is only reached
/// on executions with a very large number of distinct activity attempts.
const ACTIVITY_PANEL_MAX_EVENTS: i64 = 2000;
const SIGNAL_UPDATE_TYPES: &[&str] = &[
    "SignalReceived",
    "UpdateAdmitted",
    "UpdateCompleted",
    "UpdateFailed",
];
const SIGNAL_UPDATE_PANEL_LIMIT: usize = 20;
const ACTIVITY_PANEL_EVENT_TYPES: &[&str] = &[
    "ActivityScheduled",
    "ActivityStarted",
    "ActivityCompleted",
    "ActivityFailed",
    "ActivityTimedOut",
    // ActivityHeartbeat intentionally excluded — heartbeats are high-cardinality
    // and carry no information useful for the attempts panel triage view.
    "ActivityAwaitingExternal",
    "ActivityCompletedExternally",
    "ActivityFailedExternally",
    "ActivityExternalDeadlineExtended",
    "LocalActivityScheduled",
    "LocalActivityCompleted",
    "LocalActivityFailed",
    "LocalActivityExhausted",
];

/// Extract a string field from the inner `data` object of an adjacently-tagged event payload.
///
/// Events are stored as `{"type": "...", "data": {...}}`. This helper reaches through the outer
/// wrapper so callers don't have to repeat the two-step lookup everywhere.
fn event_data_field<'a>(event_data: &'a Value, field: &str) -> Option<&'a str> {
    event_data.get("data")?.get(field)?.as_str()
}

/// Extract a numeric field from the inner `data` object of an adjacently-tagged event payload.
fn event_data_u64(event_data: &Value, field: &str) -> Option<u64> {
    event_data.get("data")?.get(field)?.as_u64()
}

/// Map a raw `event_type` string to a human-readable label.
///
/// `execution_state` disambiguates the `WorkflowCancelled` event, which is
/// reused for force-terminate (issue #504, no new event variant): a terminal
/// `WorkflowCancelled` on a `TERMINATED` execution is labelled "Workflow
/// terminated" so the timeline matches the (already correct) state badge.
fn event_human_label(event_type: &str, event_data: &Value, execution_state: &str) -> String {
    match event_type {
        "WorkflowStarted" => "Workflow started".to_string(),
        "WorkflowCompleted" => "Workflow completed".to_string(),
        "WorkflowFailed" => "Workflow failed".to_string(),
        "WorkflowCancelled" => {
            if execution_state == "TERMINATED" {
                "Workflow terminated".to_string()
            } else {
                "Workflow cancelled".to_string()
            }
        }
        "WorkflowTerminated" => "Workflow terminated".to_string(),
        "ActivityScheduled" => {
            let name = event_data_field(event_data, "name").unwrap_or("?");
            format!("Activity scheduled: {name}")
        }
        "ActivityStarted" => "Activity started".to_string(),
        "ActivityCompleted" => "Activity completed".to_string(),
        "ActivityFailed" => {
            let err = event_data_field(event_data, "error").unwrap_or("error");
            format!("Activity failed: {}", truncate_error(err))
        }
        "ActivityTimedOut" => "Activity timed out".to_string(),
        "ActivityHeartbeat" => "Activity heartbeat".to_string(),
        "ActivityAwaitingExternal" => {
            let name = event_data_field(event_data, "name").unwrap_or("?");
            format!("Activity awaiting external: {name}")
        }
        "ActivityCompletedExternally" => "Activity completed externally".to_string(),
        "ActivityFailedExternally" => "Activity failed externally".to_string(),
        "ActivityExternalDeadlineExtended" => "External activity deadline extended".to_string(),
        "TimerStarted" => "Timer started".to_string(),
        "TimerFired" => "Timer fired".to_string(),
        "SignalReceived" => {
            let name = event_data_field(event_data, "signal_name").unwrap_or("?");
            format!("Signal received: {name}")
        }
        "ChildWorkflowStarted" => "Child workflow started".to_string(),
        "ChildWorkflowCompleted" => "Child workflow completed".to_string(),
        "ChildWorkflowFailed" => "Child workflow failed".to_string(),
        "UpdateAdmitted" => "Update admitted".to_string(),
        "UpdateCompleted" => "Update completed".to_string(),
        "UpdateFailed" => "Update failed".to_string(),
        "LocalActivityScheduled" => "Local activity scheduled".to_string(),
        "LocalActivityCompleted" => "Local activity completed".to_string(),
        "LocalActivityFailed" => "Local activity failed".to_string(),
        "LocalActivityExhausted" => "Local activity exhausted".to_string(),
        "VersionMarker" => "Version marker".to_string(),
        "ContinueAsNew" => "Continue as new".to_string(),
        other => other.to_string(),
    }
}

/// A grouped summary row for the activity attempts panel.
struct ActivityAttemptRow {
    name: String,
    attempt_count: usize,
    last_status: String,
    last_ts: String,
    last_error: Option<String>,
}

fn collect_activity_attempts(events: &[HarvestEvent]) -> Vec<ActivityAttemptRow> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, ActivityAttemptRow> = HashMap::new();

    for event in events {
        if !ACTIVITY_PANEL_EVENT_TYPES.contains(&event.event_type.as_str()) {
            continue;
        }
        let Some(aid) = event_data_field(&event.event_data, "activity_id") else {
            continue;
        };
        let aid = aid.to_string();
        if !groups.contains_key(&aid) {
            let name = event_data_field(&event.event_data, "name")
                .unwrap_or("")
                .to_string();
            order.push(aid.clone());
            groups.insert(
                aid.clone(),
                ActivityAttemptRow {
                    name,
                    attempt_count: 0,
                    last_status: event.event_type.clone(),
                    last_ts: format_timestamp(Some(event.timestamp)),
                    last_error: None,
                },
            );
        }
        if let Some(row) = groups.get_mut(&aid) {
            // Scheduling events: one per attempt for regular activities, one total
            // for local activities (retries are tracked via the attempt field on
            // LocalActivityFailed/LocalActivityExhausted instead).
            if matches!(
                event.event_type.as_str(),
                "ActivityScheduled" | "ActivityAwaitingExternal" | "LocalActivityScheduled"
            ) {
                row.attempt_count += 1;
                if row.name.is_empty() {
                    row.name = event_data_field(&event.event_data, "name")
                        .unwrap_or("")
                        .to_string();
                }
            }
            // Failure events carry an authoritative `attempt` count. Use max() so the
            // panel is accurate whether or not every scheduled event was captured.
            // This covers regular retries, external failures, and local activity retries.
            // `attempt` is serialized as a JSON number, so use the u64 accessor.
            if matches!(
                event.event_type.as_str(),
                "ActivityFailed" | "LocalActivityFailed" | "LocalActivityExhausted"
            ) && let Some(n) = event_data_u64(&event.event_data, "attempt")
            {
                row.attempt_count = row
                    .attempt_count
                    .max(usize::try_from(n).unwrap_or(usize::MAX));
            }
            // Copy the error message from any failure event type.
            if matches!(
                event.event_type.as_str(),
                "ActivityFailed"
                    | "ActivityFailedExternally"
                    | "LocalActivityFailed"
                    | "LocalActivityExhausted"
            ) {
                row.last_error =
                    event_data_field(&event.event_data, "error").map(ToOwned::to_owned);
            }
            row.last_status.clone_from(&event.event_type);
            row.last_ts = format_timestamp(Some(event.timestamp));
        }
    }

    order
        .into_iter()
        .filter_map(|id| groups.remove(&id))
        .collect()
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
fn render_workflow_detail(
    execution: &WorkflowExecution,
    total_events: i64,
    page_events: &[HarvestEvent],
    activity_events: &[HarvestEvent],
    signal_update_events: &[HarvestEvent],
    signal_update_overflow: bool,
    children: &[WorkflowExecution],
    event_page: i64,
    blocked_on: &BlockedOnData,
    flash: Option<&str>,
    continue_as_new_threshold: Option<u64>,
) -> Markup {
    let exec_id_str = execution.id.to_string();
    let title = format!("{} · Vantage", execution.workflow_name);
    let detail_badge_class = format!("badge {}", badge_class(&execution.state));

    // Duration string.
    let duration = execution.completed_at.map(|end| {
        let secs = (end - execution.started_at).num_seconds().max(0);
        if secs < 60 {
            format!("{secs}s")
        } else if secs < 3600 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
        }
    });

    // Activity attempts from the pre-filtered activity events.
    let activity_attempts = collect_activity_attempts(activity_events);

    // Signal/update panel counts.
    let signal_update_shown = signal_update_events.len();
    let signal_update_label_total = if signal_update_overflow {
        signal_update_shown + 1
    } else {
        signal_update_shown
    };

    // Pagination arithmetic based on the DB-level total.
    let total_events_usize = usize::try_from(total_events).unwrap_or(usize::MAX);
    let page_size = usize::try_from(DETAIL_EVENT_PAGE_SIZE).unwrap_or(100);
    let event_page_idx = usize::try_from(event_page).unwrap_or(0);
    let page_start = event_page_idx
        .saturating_mul(page_size)
        .min(total_events_usize);
    let page_end = (page_start + page_events.len()).min(total_events_usize);
    let has_prev_page = event_page > 0;
    let has_next_page = page_end < total_events_usize;
    let last_page = if total_events == 0 {
        0_i64
    } else {
        (total_events - 1) / DETAIL_EVENT_PAGE_SIZE
    };

    let body = html! {
        div.detail-row { a.back href="../workflows" { (PreEscaped("&larr;")) " Back to workflows" } }

        h2 {
            (execution.workflow_name) " "
            span class=(detail_badge_class) aria-label={ "Status: " (execution.state) } role="status" {
                (execution.state)
            }
        }

        @if let Some(message) = flash {
            div.flash { (message) }
        }

        @if let Some(error) = execution.error.as_deref() {
            div."error-banner" {
                strong { "Error:" } " " (error)
            }
        }

        // Operator actions — use the exec_id in action URLs so they resolve correctly
        // whether the router is mounted at "/" or at a subpath like "/api/harvest/ui".
        div."operator-actions" {
            form method="post" action={ (exec_id_str) "/cancel" }
                  onsubmit="return confirm('Cancel this workflow execution?')" {
                button.danger type="submit" { "Cancel" }
            }
            @let terminal = is_terminal_workflow_state(&execution.state);
            @if execution.state == "PAUSED" {
                // Paused executions show a Resume action (issue #383).
                form method="post" action={ (exec_id_str) "/resume" } {
                    button type="submit" { "Resume" }
                }
            } @else {
                // Pause is disabled once the workflow is terminal.
                form method="post" action={ (exec_id_str) "/pause" }
                      onsubmit="return confirm('Pause this workflow execution?')" {
                    button type="submit" disabled[terminal]
                        title=[terminal.then_some("Workflow is terminal")] { "Pause" }
                }
            }
            // Forceful sibling of Cancel — seals the run TERMINATED (issue #788).
            // Disabled once the workflow is terminal, exactly like Pause.
            form method="post" action={ (exec_id_str) "/terminate" }
                  onsubmit="return confirm('Force-terminate this workflow execution? This seals it as TERMINATED.')" {
                button.danger type="submit" disabled[terminal]
                    title=[terminal.then_some("Workflow is terminal")] { "Terminate" }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Send signal" }
                form method="post" action={ (exec_id_str) "/signal" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Signal name"
                        input type="text" name="signal_name" required placeholder="e.g. approve" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Payload (JSON)"
                        textarea name="payload" placeholder="{}" rows="3" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-family:ui-monospace,monospace;font-size:12px" {}
                    }
                    button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" { "Send" }
                }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Reset to event N" }
                form method="post" action={ (exec_id_str) "/reset" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Event # (1-based, as shown in timeline)"
                        input type="number" name="reset_to_event_id" min="1" required placeholder="1" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Reason"
                        input type="text" name="reason" placeholder="rollback" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    button type="submit" style="background:#92400e;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" onclick="return confirm('Reset this workflow execution? This is destructive.')" { "Reset" }
                }
            }
            details style="display:inline-block" {
                summary style="cursor:pointer;color:#93c5fd;font-size:12px;display:inline-block;padding:6px 12px;border:1px solid #2563eb;border-radius:6px" { "Trigger update" }
                form method="post" action={ (exec_id_str) "/trigger-update" } style="margin-top:8px;background:#1e293b;border:1px solid #334155;border-radius:6px;padding:12px;display:flex;flex-direction:column;gap:8px;min-width:280px" {
                    label style="font-size:12px;color:#94a3b8" {
                        "Update name"
                        input type="text" name="update_name" required placeholder="e.g. set_priority" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
                    }
                    label style="font-size:12px;color:#94a3b8" {
                        "Payload (JSON)"
                        textarea name="payload" placeholder="{}" rows="3" style="display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-family:ui-monospace,monospace;font-size:12px" {}
                    }
                    button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:6px;padding:6px 12px;font-size:12px;cursor:pointer;align-self:flex-start" { "Submit" }
                }
            }
            a.btn href={ "../../workflows/" (exec_id_str) "/history/export" } {
                "Export history"
            }
        }

        div.card {
            h3 { "Metadata" }
            div.kv {
                (kv("Execution ID", &exec_id_str, true))
                (kv("Workflow ID", &execution.workflow_id, true))
                (kv("Run ID", &execution.run_id.to_string(), true))
                (kv("Shard ID", &execution.shard_id.to_string(), true))
                (kv("Queue", &execution.queue_name, true))
                (kv("Started", &format_timestamp(Some(execution.started_at)), false))
                (kv("Completed", &format_timestamp(execution.completed_at), false))
                @if let Some(dur) = &duration {
                    (kv("Duration", dur, false))
                }
                @if let Some(parent) = execution.parent_id {
                    div.k { "Parent" }
                    div.v {
                        a href={ "../../workflows/" (parent.to_string()) } {
                            code { (short_id(&parent.to_string())) }
                        }
                    }
                }
                @if let Some(worker) = execution.sticky_worker_id.as_deref() {
                    (kv("Current worker", worker, true))
                }
                @if let Some(timeout) = execution.execution_timeout {
                    (kv("Execution timeout", &format!("{}s", timeout.num_seconds()), false))
                }
                @if let Some(ref build_id) = execution.assigned_build_id {
                    div.k { "Assigned build" }
                    div.v {
                        a href={ "../build-routing?build_id=" (url_encode(build_id)) }
                           title="View in Build Routing" {
                            code { (build_id) }
                        }
                    }
                }
                @if let Some(threshold) = continue_as_new_threshold {
                    (kv("History events", &format!("{total_events} / threshold: {threshold}"), false))
                } @else {
                    (kv("History events", &total_events.to_string(), false))
                }
                @if let Some(ref owner) = execution.owner {
                    div.k { "Owner" }
                    div.v {
                        span class="badge badge-owner" { (owner) }
                    }
                }
                @if let Some(ref sev) = execution.severity {
                    @let sev_class = match sev.to_lowercase().as_str() {
                        "sev1" => "badge-sev-sev1",
                        "sev2" => "badge-sev-sev2",
                        "sev3" => "badge-sev-sev3",
                        "sev4" => "badge-sev-sev4",
                        _ => "",
                    };
                    div.k { "Severity" }
                    div.v {
                        span class={ "badge " (sev_class) } { (sev.to_uppercase()) }
                    }
                }
                @if let Some(ref rb) = execution.runbook_url {
                    div.k { "Runbook" }
                    div.v {
                        a href=(rb) target="_blank" rel="noopener noreferrer" { (rb) }
                    }
                }
            }
        }

        // Blocked-on panel
        (render_blocked_on_panel(blocked_on))

        (json_card("Input", &execution.input))
        @if let Some(output) = execution.output.as_ref() {
            (json_card("Output", output))
        }
        @if let Some(memo) = execution.memo.as_ref() {
            (json_card("Memo", memo))
        }
        @if let Some(attrs) = execution.search_attrs.as_ref() {
            (json_card("Search attributes", attrs))
        }

        // Activity attempts panel
        @if !activity_attempts.is_empty() {
            div.card {
                h3 { "Activity attempts" }
                table {
                    thead {
                        tr {
                            th { "Activity" }
                            th { "Attempts" }
                            th { "Last status" }
                            th { "Last updated" }
                        }
                    }
                    tbody {
                        @for row in &activity_attempts {
                            @let display_name = if row.name.is_empty() { "—".to_string() } else { row.name.clone() };
                            tr {
                                td { (display_name) }
                                td { (row.attempt_count.max(1)) }
                                td {
                                    code { (row.last_status) }
                                    @if let Some(err) = &row.last_error {
                                        " — " (truncate_error(err))
                                    }
                                }
                                td { (row.last_ts) }
                            }
                        }
                    }
                }
            }
        }

        // Children panel
        @if !children.is_empty() {
            div.card {
                h3 { "Children (" (children.len()) ")" }
                table {
                    thead {
                        tr {
                            th { "Exec ID" }
                            th { "Workflow" }
                            th { "Status" }
                            th { "Started" }
                        }
                    }
                    tbody {
                        @for child in children {
                            @let child_id = child.id.to_string();
                            tr {
                                td {
                                    a href={ "../../workflows/" (child_id) } {
                                        code { (short_id(&child_id)) }
                                    }
                                }
                                td { (child.workflow_name) }
                                td { (state_badge(&child.state)) }
                                td { (format_timestamp(Some(child.started_at))) }
                            }
                        }
                    }
                }
            }
        }

        // Signals & updates panel
        @if !signal_update_events.is_empty() {
            div.card {
                @if signal_update_overflow {
                    h3 { "Signals & Updates (showing " (SIGNAL_UPDATE_PANEL_LIMIT) " of " (signal_update_label_total) "+)" }
                } @else {
                    h3 { "Signals & Updates" }
                }
                table {
                    thead {
                        tr {
                            th { "Type" }
                            th { "Name / ID" }
                            th { "Timestamp" }
                        }
                    }
                    tbody {
                        @for event in signal_update_events {
                            @let label =
                                event_human_label(&event.event_type, &event.event_data, &execution.state);
                            @let name_or_id = event_data_field(&event.event_data, "signal_name")
                                .or_else(|| event_data_field(&event.event_data, "update_id"))
                                .unwrap_or("—");
                            tr {
                                td { (label) }
                                td { code { (name_or_id) } }
                                td { (format_timestamp(Some(event.timestamp))) }
                            }
                        }
                    }
                }
            }
        }

        // Event timeline
        div.card {
            h3 { "Event history (" (total_events) " events)" }
            @if total_events == 0 {
                div.empty { "No events recorded yet." }
            } @else {
                // Jump controls for large histories
                @if total_events > DETAIL_EVENT_PAGE_SIZE {
                    div.pagination style="margin-bottom:12px" {
                        @if has_prev_page {
                            a href={ "?event_page=" (event_page - 1) } {
                                (PreEscaped("&larr;")) " Previous"
                            }
                        } @else {
                            span.disabled { (PreEscaped("&larr;")) " Previous" }
                        }
                        span { " Events " (page_start + 1) "–" (page_end) " of " (total_events) " " }
                        @if has_next_page {
                            a href={ "?event_page=" (event_page + 1) } {
                                "Next " (PreEscaped("&rarr;"))
                            }
                        } @else {
                            span.disabled { "Next " (PreEscaped("&rarr;")) }
                        }
                        a href={ "?event_page=" (last_page) } { "Jump to latest" }
                    }
                }
                table {
                    thead {
                        tr {
                            th { "#" }
                            th { "Type" }
                            th { "Timestamp" }
                            th { "Data" }
                        }
                    }
                    tbody {
                        @for event in page_events {
                            @let label =
                                event_human_label(&event.event_type, &event.event_data, &execution.state);
                            @let ts = format_timestamp(Some(event.timestamp));
                            tr {
                                td { (event.event_id + 1) }
                                td title=(event.event_type) {
                                    span.event-label {
                                        (label)
                                        code { "(" (event.event_type) ")" }
                                    }
                                }
                                td { (ts) }
                                td {
                                    details {
                                        summary { "view payload" }
                                        pre { (pretty_json(&event.event_data)) }
                                    }
                                }
                            }
                        }
                    }
                }
                // Bottom pagination with jump-to-event control
                @if total_events > DETAIL_EVENT_PAGE_SIZE {
                    div.pagination style="margin-top:12px" {
                        @if has_prev_page {
                            a href={ "?event_page=" (event_page - 1) } {
                                (PreEscaped("&larr;")) " Previous"
                            }
                        } @else {
                            span.disabled { (PreEscaped("&larr;")) " Previous" }
                        }
                        span { "Page " (event_page + 1) }
                        @if has_next_page {
                            a href={ "?event_page=" (event_page + 1) } {
                                "Next " (PreEscaped("&rarr;"))
                            }
                        } @else {
                            span.disabled { "Next " (PreEscaped("&rarr;")) }
                        }
                        a href={ "?event_page=" (last_page) } { "Jump to latest" }
                        form method="get" style="display:inline-flex;gap:6px;align-items:center;margin-left:8px" {
                            label style="font-size:12px;color:#94a3b8" { "Jump to event:" }
                            input type="number" name="jump_event" min="1" max=(total_events) placeholder="N"
                                style="width:70px;background:#1e293b;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:4px 6px;font-size:12px";
                            button type="submit" style="background:#2563eb;color:#fff;border:0;border-radius:4px;padding:4px 10px;font-size:12px;cursor:pointer" { "Go" }
                        }
                    }
                }
            }
        }
    };

    layout(&title, &body, "../")
}

/// Per-row checkpoint rendering decision for the pending-activities table, after
/// applying both the per-activity cap and the cumulative per-page budget (#503
/// review). Carries the observed byte size for the marker text.
#[derive(Clone, Copy)]
enum CheckpointCellState {
    /// No heartbeat checkpoint has been flushed.
    Absent,
    /// Render the checkpoint JSON (within its cap and the page budget).
    Show,
    /// Withheld because this payload's own size exceeded its activity cap.
    Truncated(u64),
    /// Withheld because the cumulative per-page checkpoint budget was exhausted.
    OmittedForBudget(u64),
}

/// Build the per-row checkpoint decisions for the pending-activities table,
/// mirroring the stack API (#503 review): each checkpoint is judged against its
/// activity's effective cap (per-activity `max_result_bytes` raised against the
/// global ceiling), then a cumulative per-page budget — the global cap — bounds
/// the total rendered bytes so a large fan-out can't generate a huge HTML page.
/// The first payload-bearing checkpoint is always shown. The byte size is
/// measured once here via the shared non-allocating counter; the renderer reads
/// the resulting state without re-serializing.
fn plan_checkpoint_cells(blocked_on: &BlockedOnData) -> Vec<CheckpointCellState> {
    let per_item: Vec<(bool, Option<u64>)> = blocked_on
        .activities
        .iter()
        .map(|item| {
            let cap = item
                .activity_name
                .as_deref()
                .and_then(|n| blocked_on.heartbeat_caps.get(n).copied())
                .unwrap_or(blocked_on.heartbeat_details_cap);
            crate::api::heartbeat_details_truncation(item.heartbeat_details.as_ref(), cap)
        })
        .collect();
    // Only present, not-individually-truncated checkpoints participate in the
    // cumulative budget.
    let sizes: Vec<Option<u64>> = per_item
        .iter()
        .map(|(truncated, bytes)| match bytes {
            Some(b) if !truncated => Some(*b),
            _ => None,
        })
        .collect();
    let omit = crate::api::checkpoint_budget_decisions(&sizes, blocked_on.heartbeat_details_cap);
    per_item
        .iter()
        .zip(omit)
        .map(
            |((truncated, bytes), omit_budget)| match (bytes, truncated, omit_budget) {
                (None, _, _) => CheckpointCellState::Absent,
                (Some(b), true, _) => CheckpointCellState::Truncated(*b),
                (Some(b), false, true) => CheckpointCellState::OmittedForBudget(*b),
                (Some(_), false, false) => CheckpointCellState::Show,
            },
        )
        .collect()
}

/// Render the latest heartbeat checkpoint payload for a pending activity as a
/// collapsible JSON cell, from the precomputed [`CheckpointCellState`] (#503).
/// Shows `"—"` when absent, a truncation marker when over the activity cap, and
/// an omission marker when withheld by the per-page budget.
fn render_heartbeat_checkpoint_cell(item: &TaskQueueItem, state: CheckpointCellState) -> Markup {
    html! {
        @match state {
            CheckpointCellState::Absent => "—",
            CheckpointCellState::Show => {
                @if let Some(value) = item.heartbeat_details.as_ref() {
                    details {
                        summary { "checkpoint" }
                        pre { (pretty_json(value)) }
                    }
                } @else {
                    "—"
                }
            }
            CheckpointCellState::Truncated(bytes) => {
                span title="heartbeat payload exceeds the response size cap" {
                    "truncated (" (bytes) " bytes)"
                }
            }
            CheckpointCellState::OmittedForBudget(bytes) => {
                span title="omitted: per-page checkpoint budget exceeded" {
                    "omitted (" (bytes) " bytes)"
                }
            }
        }
    }
}

/// Render the "Pending activities" table, including each activity's latest
/// heartbeat checkpoint judged against its effective (per-activity) cap and the
/// cumulative per-page checkpoint budget (#503).
fn render_pending_activities_table(blocked_on: &BlockedOnData) -> Markup {
    let cell_states = plan_checkpoint_cells(blocked_on);
    html! {
        table {
            thead {
                tr {
                    th { "Activity" }
                    th { "State" }
                    th { "Attempt" }
                    th { "Scheduled" }
                    th { "Last heartbeat" }
                    th { "Checkpoint" }
                }
            }
            tbody {
                @for (item, state) in blocked_on.activities.iter().zip(cell_states) {
                    tr {
                        td { (item.activity_name.as_deref().unwrap_or("—")) }
                        td { code { (&item.state) } }
                        td { (item.attempt) }
                        td { (format_timestamp(Some(item.scheduled_at))) }
                        td { (format_timestamp(item.last_heartbeat_at)) }
                        td { (render_heartbeat_checkpoint_cell(item, state)) }
                    }
                }
            }
        }
    }
}

fn render_blocked_on_panel(blocked_on: &BlockedOnData) -> Markup {
    let has_anything = !blocked_on.activities.is_empty()
        || !blocked_on.external_tasks.is_empty()
        || !blocked_on.timers.is_empty()
        || !blocked_on.signals.is_empty();

    html! {
        div.card {
            h3 { "Blocked on" }
            @if !has_anything {
                div.empty { "No pending work items." }
            } @else {
                @if !blocked_on.activities.is_empty() {
                    h3 style="margin-top:8px" { "Pending activities" }
                    (render_pending_activities_table(blocked_on))
                }
                @if !blocked_on.external_tasks.is_empty() {
                    h3 style="margin-top:8px" { "Pending external activities" }
                    table {
                        thead {
                            tr {
                                th { "Activity" }
                                th { "Deadline" }
                                th { "Scheduled" }
                            }
                        }
                        tbody {
                            @for task in &blocked_on.external_tasks {
                                tr {
                                    td { (&task.name) }
                                    td { (format_timestamp(Some(task.schedule_to_close_at))) }
                                    td { (format_timestamp(Some(task.created_at))) }
                                }
                            }
                        }
                    }
                }
                @if !blocked_on.timers.is_empty() {
                    h3 style="margin-top:8px" { "Pending timers" }
                    table {
                        thead {
                            tr {
                                th { "Timer ID" }
                                th { "Fires at" }
                            }
                        }
                        tbody {
                            @for timer in &blocked_on.timers {
                                tr {
                                    td { code { (&timer.timer_id) } }
                                    td { (format_timestamp(Some(timer.fires_at))) }
                                }
                            }
                        }
                    }
                }
                @if !blocked_on.signals.is_empty() {
                    h3 style="margin-top:8px" { "Pending signals" }
                    table {
                        thead {
                            tr {
                                th { "Signal name" }
                                th { "Received at" }
                            }
                        }
                        tbody {
                            @for sig in &blocked_on.signals {
                                tr {
                                    td { (&sig.signal_name) }
                                    td { (format_timestamp(Some(sig.received_at))) }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn kv(key: &str, value: &str, mono: bool) -> Markup {
    html! {
        div.k { (key) }
        @if mono {
            div.v { code { (value) } }
        } @else {
            div.v { (value) }
        }
    }
}

fn json_card(title: &str, value: &Value) -> Markup {
    html! {
        div.card {
            h3 { (title) }
            pre { (pretty_json(value)) }
        }
    }
}

fn pretty_json(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

fn format_timestamp(ts: Option<DateTime<Utc>>) -> String {
    ts.map_or_else(
        || "—".to_string(),
        |ts| ts.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
    )
}

fn state_badge(state: &str) -> Markup {
    let class = format!("badge {}", badge_class(state));
    let aria = format!("Status: {state}");
    html! {
        span class=(class) aria-label=(aria) role="status" { (state) }
    }
}

fn badge_class(state: &str) -> &'static str {
    match state {
        "RUNNING" => "RUNNING",
        "COMPLETED" => "COMPLETED",
        "FAILED" => "FAILED",
        "CANCELLED" => "CANCELLED",
        "TERMINATED" => "TERMINATED",
        _ => "UNKNOWN",
    }
}

fn short_id(id: &str) -> String {
    id.chars().take(8).collect::<String>() + "…"
}

fn url_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

/// Escape a string for safe embedding inside a single-quoted JavaScript string literal.
///
/// Replaces `\` with `\\` and `'` with `\'` so the value cannot break out of the
/// surrounding `confirm('...')` or similar inline handler, preventing XSS via
/// operator-supplied build IDs or queue names.
fn js_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('\'', "\\'")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029")
}

fn layout(title: &str, body: &Markup, base_href: &str) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href={ (base_href) "workflows" } { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a.active href={ (base_href) "workflows" } { "Workflows" }
                        a href={ (base_href) "dags" } { "DAGs" }
                        a href={ (base_href) "workers" } { "Workers" }
                        a href={ (base_href) "schedules" } { "Schedules" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                        a href={ (base_href) "build-routing" } { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

fn render_dag_list(dags: &[DagUiSummary], shard_errors: &[(ShardId, String)]) -> Markup {
    let body = html! {
        h2 { "DAGs" }
        @for (shard_id, error) in shard_errors {
            div.shard-error {
                strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                (error)
            }
        }
        table {
            thead {
                tr {
                    th { "Name" }
                    th { "Schedule" }
                    th { "Paused" }
                    th { "Next Run" }
                    th { "Max Active" }
                    th { "Catchup" }
                    th { "Task Count" }
                }
            }
            tbody {
                @for dag in dags {
                    tr {
                        td { a href={ "dags/" (&dag.name) } { (&dag.name) } }
                        td { (dag.schedule_expr.clone().unwrap_or_else(|| "—".to_string())) }
                        td { (if dag.is_paused { "Yes" } else { "No" }) }
                        td { (format_timestamp(dag.next_run_at)) }
                        td { (dag.max_active_runs) }
                        td { (if dag.catchup { "Yes" } else { "No" }) }
                        td { (dag.task_count) }
                    }
                }
            }
        }
    };
    layout_dag_detail("DAGs · Vantage", &body, "", None)
}

fn dag_summary_from_registered(name: &str, dag: &RegisteredDag) -> DagUiSummary {
    DagUiSummary {
        name: name.to_string(),
        schedule_expr: dag.schedule.as_ref().map(schedule_expr_for_ui_summary),
        task_count: dag.task_count(),
        is_paused: false,
        next_run_at: None,
        max_active_runs: i32::try_from(dag.max_active_runs).unwrap_or(i32::MAX),
        catchup: dag.catchup,
    }
}

fn merge_dag_schedule_row(entry: &mut DagUiSummary, row: &HarvestSchedule) {
    if entry.schedule_expr.is_none() {
        entry.schedule_expr.clone_from(&row.schedule_expr);
    }
    entry.is_paused = row.is_paused;
    entry.next_run_at = row.next_run_at;
    entry.max_active_runs = row.max_active_runs;
    entry.catchup = row.catchup;
}

fn schedule_expr_for_ui_summary(schedule: &Schedule) -> String {
    match schedule {
        Schedule::Cron(expr) => expr.clone(),
        Schedule::Interval(duration) => {
            if duration.subsec_nanos() == 0 {
                format!("@every {}s", duration.as_secs())
            } else {
                format!(
                    "@every {}.{:09}s",
                    duration.as_secs(),
                    duration.subsec_nanos()
                )
            }
        }
        Schedule::Manual => "@manual".to_string(),
        Schedule::CronInTimezone { expr, tz } => format!("{expr} [{tz}]"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DagNodeState {
    Succeeded,
    Failed,
    Cancelled,
    Running,
    Queued,
    /// Skipped because a trigger rule evaluated to false over upstream statuses.
    Skipped,
    /// Skipped because a data-dependent condition predicate evaluated to false
    /// (issue #482).  Distinct from [`Skipped`] so the UI can show a different
    /// label ("Skipped (condition)").
    SkippedByCondition,
    Unknown,
}

/// Human-readable label for a [`DagNodeState`] used in the timeline table.
const fn dag_node_state_label(state: DagNodeState) -> &'static str {
    match state {
        DagNodeState::Succeeded => "Succeeded",
        DagNodeState::Failed => "Failed",
        DagNodeState::Cancelled => "Cancelled",
        DagNodeState::Running => "Running",
        DagNodeState::Queued => "Queued",
        DagNodeState::Skipped => "Skipped (upstream)",
        DagNodeState::SkippedByCondition => "Skipped (condition)",
        DagNodeState::Unknown => "Unknown",
    }
}

/// Parse `dag_skip:{idx}` from a marker event name; returns `Some(idx)` on match.
fn parse_dag_skip_marker_index(name: &str) -> Option<usize> {
    name.strip_prefix("dag_skip:").and_then(|s| s.parse().ok())
}

fn render_dag_detail(
    dag_name: &str,
    dag: &RegisteredDag,
    runs: &[WorkflowExecution],
    selected_run: Option<uuid::Uuid>,
    selected_node: Option<usize>,
    refresh: Option<u64>,
    node_states: &HashMap<usize, DagNodeState>,
) -> Markup {
    let too_large = dag.definition.tasks().len() > 200;
    let selected_task = selected_node.and_then(|idx| dag.definition.tasks().get(idx));
    let selected_node_state = selected_node
        .and_then(|idx| node_states.get(&idx).copied())
        .unwrap_or(DagNodeState::Unknown);
    let body = html! {
        h2 { "DAG " code { (dag_name) } " runs" }
        @if let Some(run_id) = selected_run {
            p { "Selected run: " code { (run_id) } }
        }
        @if too_large {
            div class="banner Warning" { "Topology has " (dag.definition.tasks().len()) " nodes; graph disabled." }
        } @else {
            h3 { "Topology" }
            table {
                thead { tr { th { "Node" } th { "Activity" } th { "Trigger Rule" } th { "State" } th { "Upstreams" } } }
                tbody {
                    @for (idx, task) in dag.definition.tasks().iter().enumerate() {
                        tr {
                            td {
                                @if let Some(run_id) = selected_run {
                                    a href={ "?run=" (run_id) "&node=" (idx) } { (idx) }
                                } @else {
                                    a href={ "?node=" (idx) } { (idx) }
                                }
                            }
                            td { (task.activity_name.as_str()) }
                            td { (format!("{:?}", task.trigger_rule)) }
                            td { (dag_node_state_label(node_states.get(&idx).copied().unwrap_or(DagNodeState::Unknown))) }
                            td {
                                @for (n, upstream) in task.upstreams.iter().enumerate() {
                                    @if n > 0 { ", " }
                                    (upstream)
                                }
                            }
                        }
                    }
                }
            }
            @if let Some(task) = selected_task {
                h3 { "Node panel" }
                p { "Activity: " code { (task.activity_name.as_str()) } }
                p { "Trigger rule: " (format!("{:?}", task.trigger_rule)) }
                p { "Current state: " (dag_node_state_label(selected_node_state)) }
            }
        }
        table {
            thead {
                tr {
                    th { "Execution" }
                    th { "State" }
                    th { "Started" }
                    th { "Duration" }
                }
            }
            tbody {
                @for run in runs {
                    tr {
                        td { a href={ "../workflows/" (run.id) } { code { (run.id) } } }
                        td { span class={ "badge " (run.state.to_uppercase()) } { (run.state.as_str()) } }
                        td { (format_timestamp(Some(run.started_at))) }
                        td { (format_run_duration(run.started_at, run.completed_at)) }
                    }
                }
            }
        }
    };
    layout_dag_detail(&format!("DAG {dag_name} · Vantage"), &body, "../", refresh)
}

fn layout_dag_detail(title: &str, body: &Markup, base_href: &str, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
                @if let Some(secs) = refresh { meta http-equiv="refresh" content=(secs); }
            }
            body {
                header {
                    h1 { a href={ (base_href) "workflows" } { "🔭 Vantage" } span.subtitle { "Harvest dashboard" } }
                    nav {
                        a href={ (base_href) "workflows" } { "Workflows" }
                        a.active href={ (base_href) "dags" } { "DAGs" }
                        a href={ (base_href) "workers" } { "Workers" }
                        a href={ (base_href) "schedules" } { "Schedules" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                        a href={ (base_href) "build-routing" } { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

fn map_node_states(
    dag: &autumn_harvest::dag::DagDefinition,
    tasks: &[TaskQueueItem],
    condition_skipped: &std::collections::HashSet<usize>,
) -> HashMap<usize, DagNodeState> {
    let mut out = HashMap::new();
    let mut has_task_row = vec![false; dag.tasks().len()];

    // Seed condition-skipped nodes first (highest-priority: they have a recorded
    // marker telling us *why* they were skipped).
    for &idx in condition_skipped {
        out.insert(idx, DagNodeState::SkippedByCondition);
    }

    for (idx, node) in dag.tasks().iter().enumerate() {
        let mut state = out.get(&idx).copied().unwrap_or(DagNodeState::Unknown);
        for task in tasks
            .iter()
            .filter(|t| t.activity_name.as_deref() == Some(node.activity_name.as_str()))
        {
            has_task_row[idx] = true;
            state = merge_dag_task_state(state, task.state.as_str());
        }
        out.insert(idx, state);
    }

    for level in dag.execution_levels() {
        for idx in level {
            // Only infer trigger-rule skips for nodes that aren't already
            // identified as condition-skipped or have task queue rows.
            if !has_task_row[*idx]
                && out.get(idx).copied() == Some(DagNodeState::Unknown)
                && let Some(state) = infer_skipped_node_state(dag, *idx, &out)
            {
                out.insert(*idx, state);
            }
        }
    }

    out
}

fn merge_dag_task_state(current: DagNodeState, task_state: &str) -> DagNodeState {
    // A condition-skip is backed by a recorded marker; task-row inference must
    // never overwrite it, even when a same-named node at a different index has
    // a FAILED/RUNNING/etc row (duplicate-activity-name scenario).
    if current == DagNodeState::SkippedByCondition {
        return current;
    }
    match task_state {
        "FAILED" => DagNodeState::Failed,
        "CANCELLED" if !matches!(current, DagNodeState::Failed) => DagNodeState::Cancelled,
        "RUNNING" if !matches!(current, DagNodeState::Failed | DagNodeState::Cancelled) => {
            DagNodeState::Running
        }
        "PENDING" | "QUEUED" if current == DagNodeState::Unknown => DagNodeState::Queued,
        "COMPLETED" if current == DagNodeState::Unknown => DagNodeState::Succeeded,
        "SKIPPED" if current == DagNodeState::Unknown => DagNodeState::Skipped,
        _ => current,
    }
}

fn infer_skipped_node_state(
    dag: &autumn_harvest::dag::DagDefinition,
    node_idx: usize,
    node_states: &HashMap<usize, DagNodeState>,
) -> Option<DagNodeState> {
    let node = dag.tasks().get(node_idx)?;
    let upstream_statuses = node
        .upstreams
        .iter()
        .map(|upstream_idx| {
            node_states
                .get(upstream_idx)
                .copied()
                .and_then(dag_node_terminal_status)
        })
        .collect::<Option<Vec<_>>>()?;

    (!node.trigger_rule.should_run(upstream_statuses.iter())).then_some(DagNodeState::Skipped)
}

const fn dag_node_terminal_status(state: DagNodeState) -> Option<TaskStatus> {
    match state {
        DagNodeState::Succeeded => Some(TaskStatus::Succeeded),
        DagNodeState::Failed | DagNodeState::Cancelled => Some(TaskStatus::Failed),
        // Both skip variants count as Skipped for trigger-rule propagation so
        // downstream AllDone/AllSuccess rules see the correct upstream status.
        DagNodeState::Skipped | DagNodeState::SkippedByCondition => Some(TaskStatus::Skipped),
        DagNodeState::Running | DagNodeState::Queued | DagNodeState::Unknown => None,
    }
}

fn format_run_duration(started_at: DateTime<Utc>, completed_at: Option<DateTime<Utc>>) -> String {
    let Some(end) = completed_at else {
        return "—".to_string();
    };
    let secs = (end - started_at).num_seconds().max(0);
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    }
}

// ---------------------------------------------------------------------------
// Build Routing UI page (issue #362)
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_lines)]
async fn list_build_routing_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<BuildRoutingListParams>,
) -> Result<Markup, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let is_multi_shard = pool.iter_shards().count() > 1;
    let stale_threshold = api_state.worker_stale_threshold();

    // Fan out to every shard to read policies, compat, and reachability.
    // Policy and compat mutations go to all shards; reading from a single shard
    // can hide partial-write divergence. We merge by queue_name / (build_id,
    // compatible_with) and detect queues whose active build_id differs across shards.
    let mut shard_errors: Vec<(ShardId, String)> = Vec::new();
    let mut policy_map: std::collections::HashMap<String, BuildPolicy> =
        std::collections::HashMap::new();
    let mut diverged_queues: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Per-shard presence tracking for policy and compat: each entry is the set of
    // queue names / (build_id, compatible_with) pairs returned by one shard that
    // successfully responded. Used to detect absent rows on healthy shards.
    let mut per_shard_policy_seen: Vec<std::collections::HashSet<String>> = Vec::new();
    let mut compat_map: std::collections::HashMap<(String, String), BuildCompatEntry> =
        std::collections::HashMap::new();
    let mut per_shard_compat_seen: Vec<std::collections::HashSet<(String, String)>> = Vec::new();
    let mut per_shard_reach: Vec<Vec<BuildReachability>> = Vec::new();

    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match list_build_policies(&mut conn).await {
                    Ok(shard_policies) => {
                        let mut seen: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        for policy in shard_policies {
                            seen.insert(policy.queue_name.clone());
                            match policy_map.get(&policy.queue_name) {
                                Some(existing) if existing.build_id != policy.build_id => {
                                    diverged_queues.insert(policy.queue_name.clone());
                                    if policy.updated_at > existing.updated_at {
                                        policy_map.insert(policy.queue_name.clone(), policy);
                                    }
                                }
                                Some(existing) if policy.updated_at > existing.updated_at => {
                                    policy_map.insert(policy.queue_name.clone(), policy);
                                }
                                None => {
                                    policy_map.insert(policy.queue_name.clone(), policy);
                                }
                                _ => {}
                            }
                        }
                        per_shard_policy_seen.push(seen);
                    }
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
                match list_build_compat(&mut conn).await {
                    Ok(entries) => {
                        let mut seen: std::collections::HashSet<(String, String)> =
                            std::collections::HashSet::new();
                        for entry in entries {
                            let key = (entry.build_id.clone(), entry.compatible_with.clone());
                            seen.insert(key.clone());
                            compat_map
                                .entry(key)
                                .and_modify(|e| {
                                    if entry.declared_at > e.declared_at {
                                        *e = entry.clone();
                                    }
                                })
                                .or_insert(entry);
                        }
                        per_shard_compat_seen.push(seen);
                    }
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
                match all_build_reachability(&mut conn, stale_threshold).await {
                    Ok(r) => per_shard_reach.push(r),
                    Err(e) => shard_errors.push((shard_id, e.to_string())),
                }
            }
            Err(e) => shard_errors.push((shard_id, e.to_string())),
        }
    }

    // Detect absent-row policy divergence (queue on some shards, missing on others).
    if per_shard_policy_seen.len() > 1 {
        for queue_name in policy_map.keys() {
            if per_shard_policy_seen
                .iter()
                .any(|seen| !seen.contains(queue_name.as_str()))
            {
                diverged_queues.insert(queue_name.clone());
            }
        }
    }

    // Detect compat pairs present on some shards but absent on others.
    let mut diverged_compat_pairs: Vec<String> = if per_shard_compat_seen.len() > 1 {
        let mut pairs: Vec<String> = compat_map
            .keys()
            .filter(|key| {
                per_shard_compat_seen
                    .iter()
                    .any(|seen| !seen.contains(*key))
            })
            .map(|(b, c)| format!("{b} \u{2192} {c}"))
            .collect();
        pairs.sort();
        pairs
    } else {
        vec![]
    };
    diverged_compat_pairs.dedup();

    let mut policies: Vec<BuildPolicy> = policy_map.into_values().collect();
    policies.sort_by(|a, b| a.queue_name.cmp(&b.queue_name));
    let mut all_compat: Vec<BuildCompatEntry> = compat_map.into_values().collect();
    all_compat.sort_by(|a, b| {
        a.build_id
            .cmp(&b.build_id)
            .then(a.compatible_with.cmp(&b.compatible_with))
    });
    let mut diverged_list: Vec<String> = diverged_queues.into_iter().collect();
    diverged_list.sort();
    let reachability = merge_reachability(per_shard_reach);

    let shard_error_refs: Vec<(ShardId, &str)> =
        shard_errors.iter().map(|(s, e)| (*s, e.as_str())).collect();

    // Apply optional build_id filter to narrow tables for drill-down from workers/executions.
    let build_id_filter = params.build_id.as_deref().filter(|s| !s.is_empty());
    let filtered_policies: Vec<BuildPolicy> = if let Some(bid) = build_id_filter {
        policies.into_iter().filter(|p| p.build_id == bid).collect()
    } else {
        policies
    };
    let filtered_compat: Vec<BuildCompatEntry> = if let Some(bid) = build_id_filter {
        all_compat
            .into_iter()
            .filter(|e| e.build_id == bid || e.compatible_with == bid)
            .collect()
    } else {
        all_compat
    };
    let filtered_reach: Vec<BuildReachability> = if let Some(bid) = build_id_filter {
        reachability
            .into_iter()
            .filter(|r| r.build_id == bid)
            .collect()
    } else {
        reachability
    };

    Ok(render_build_routing_page(
        &filtered_policies,
        &filtered_compat,
        &filtered_reach,
        &shard_error_refs,
        &diverged_list,
        &diverged_compat_pairs,
        is_multi_shard,
        params.flash.as_deref(),
        build_id_filter,
    ))
}

async fn build_routing_set_policy_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingSetPolicyForm>,
) -> Result<axum::response::Response, AutumnError> {
    let queue_name = form.queue_name.trim().to_string();
    let build_id = form.build_id.trim().to_string();
    if queue_name.is_empty() || build_id.is_empty() {
        let flash = url_encode("queue_name and build_id must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    let deployment_name = form.deployment_name.as_deref().filter(|s| !s.is_empty());
    // Fan out to all shards so every shard's get_build_policy() sees the new policy
    // when evaluating assigned_build_id at workflow start time.
    let mut last_policy = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match set_build_policy(&mut conn, &queue_name, &build_id, deployment_name)
                    .await
                    .map_err(map_error)
                {
                    Ok(p) => last_policy = Some(p),
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_POLICY_SET,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(queue_name.as_str()),
                route_or_command: "POST /ui/build-routing/set-policy",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if shard_errors.is_empty() {
        match last_policy {
            Some(p) => url_encode(&format!(
                "Build policy for queue '{}' set to '{}'",
                p.queue_name, p.build_id
            )),
            None => url_encode("No shards configured"),
        }
    } else {
        url_encode(&format!(
            "Partial failure setting build policy: {}",
            shard_errors.join("; ")
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_declare_compat_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingCompatForm>,
) -> Result<axum::response::Response, AutumnError> {
    let build_id = form.build_id.trim().to_string();
    let compatible_with = form.compatible_with.trim().to_string();
    if build_id.is_empty() || compatible_with.is_empty() {
        let flash = url_encode("build_id and compatible_with must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Fan out to all shards so load_compat_set() on each shard picks up the declaration.
    let mut last_entry = None;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match declare_compat(&mut conn, &build_id, &compatible_with)
                    .await
                    .map_err(map_error)
                {
                    Ok(e) => last_entry = Some(e),
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let target = format!("{build_id}→{compatible_with}");
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_COMPAT_DECLARE,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(target.as_str()),
                route_or_command: "POST /ui/build-routing/declare-compat",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if shard_errors.is_empty() {
        match last_entry {
            Some(e) => url_encode(&format!(
                "Declared: '{}' compatible with '{}'",
                e.build_id, e.compatible_with
            )),
            None => url_encode("No shards configured"),
        }
    } else {
        url_encode(&format!(
            "Partial failure declaring compat: {}",
            shard_errors.join("; ")
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_revoke_compat_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingCompatForm>,
) -> Result<axum::response::Response, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    // Fan out revoke to all shards; collect errors rather than aborting.
    let mut any_revoked = false;
    let mut shard_errors: Vec<String> = Vec::new();
    for (shard_id, shard_pool) in pool.iter_shards() {
        match acquire_conn(shard_pool).await {
            Ok(mut conn) => {
                match revoke_compat(&mut conn, form.build_id.trim(), form.compatible_with.trim())
                    .await
                    .map_err(map_error)
                {
                    Ok(r) => any_revoked |= r,
                    Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
                }
            }
            Err(e) => shard_errors.push(format!("shard {}: {e}", shard_id.as_i32())),
        }
    }
    let audit_status = if shard_errors.is_empty() {
        STATUS_SUCCEEDED
    } else {
        STATUS_FAILED
    };
    if let Ok(mut conn) = acquire_conn(pool.default_pool()).await {
        let error_summary = shard_errors.join("; ");
        let target = format!("{}→{}", form.build_id.trim(), form.compatible_with.trim());
        let _ = insert_audit(
            &mut conn,
            &NewAuditRecord {
                actor: "ui",
                operation: OP_BUILD_COMPAT_REVOKE,
                target_type: TARGET_BUILD_ROUTING,
                target_id: Some(target.as_str()),
                route_or_command: "POST /ui/build-routing/revoke-compat",
                request_id: None,
                idempotency_key: None,
                status: audit_status,
                error_summary: if error_summary.is_empty() {
                    None
                } else {
                    Some(error_summary.as_str())
                },
                shard_id: None,
                source: SOURCE_UI,
            },
        )
        .await;
    }
    let flash = if !shard_errors.is_empty() {
        url_encode(&format!(
            "Partial failure revoking compat: {}",
            shard_errors.join("; ")
        ))
    } else if any_revoked {
        url_encode(&format!(
            "Revoked compatibility: '{}' → '{}'",
            form.build_id, form.compatible_with
        ))
    } else {
        url_encode(&format!(
            "No compatibility declaration found for '{}' → '{}'",
            form.build_id, form.compatible_with
        ))
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

async fn build_routing_retire_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(form): Form<BuildRoutingRetireForm>,
) -> Result<axum::response::Response, AutumnError> {
    if form.build_id.trim().is_empty() {
        let flash = url_encode("build_id must not be empty");
        return Ok(
            axum::response::Redirect::to(&format!("../build-routing?flash={flash}"))
                .into_response(),
        );
    }
    let pool = api_state.storage_pool().map_err(map_error)?;
    let stale_threshold = api_state.worker_stale_threshold();

    // Check reachability across all shards before allowing retire. Any shard
    // error propagates immediately — silently skipping a shard could allow
    // retire when that shard still has active executions.
    let mut per_shard_reach: Vec<Vec<BuildReachability>> = Vec::new();
    for (_, shard_pool) in pool.iter_shards() {
        let mut conn = acquire_conn(shard_pool).await?;
        let r = all_build_reachability(&mut conn, stale_threshold)
            .await
            .map_err(map_error)?;
        per_shard_reach.push(r);
    }
    let merged = merge_reachability(per_shard_reach);
    let build_reach = merged.iter().find(|r| r.build_id == form.build_id.trim());

    let flash = match build_reach {
        Some(r) if !r.safe_to_retire => url_encode(&format!(
            "Cannot retire build '{}': {} open executions, {} pending tasks remain",
            form.build_id, r.open_executions, r.pending_tasks
        )),
        _ => {
            // Build is safe to retire (or not found, meaning nothing is running on it).
            // The retire action itself is a no-op at the DB level — the operator
            // removes their old workers out-of-band. We surface a confirmation message.
            url_encode(&format!(
                "Build '{}' is safe to retire — no open executions or pending tasks remain. \
                 You may now stop all workers running this build.",
                form.build_id.trim()
            ))
        }
    };
    Ok(axum::response::Redirect::to(&format!("../build-routing?flash={flash}")).into_response())
}

#[allow(clippy::too_many_arguments)]
fn render_build_policies_card(policies: &[BuildPolicy]) -> Markup {
    html! {
        div.card {
            h3 { "Build Policies" }
            @if policies.is_empty() {
                p.empty { "No build policies registered." }
            } @else {
                table {
                    thead { tr { th { "Queue" } th { "Active Build ID" } th { "Deployment" } th { "Last Updated" } } }
                    tbody {
                        @for policy in policies {
                            tr {
                                td { code { (policy.queue_name.clone()) } }
                                td { code { (policy.build_id.clone()) } }
                                td {
                                    @if let Some(ref dep) = policy.deployment_name {
                                        code { (dep) }
                                    } @else {
                                        span style="color:#475569" { "—" }
                                    }
                                }
                                td { (format_timestamp(Some(policy.updated_at))) }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_build_reachability_card(reachability: &[BuildReachability]) -> Markup {
    html! {
        div.card {
            h3 { "Build Reachability" }
            @if reachability.is_empty() {
                p.empty { "No build-tagged executions or workers found." }
            } @else {
                table {
                    thead { tr { th { "Build ID" } th { "Open Executions" } th { "Pending Tasks" } th { "Active Workers" } th { "Stale Workers" } th { "Status" } th { "Actions" } } }
                    tbody {
                        @for r in reachability {
                            @let status_color = if r.safe_to_retire { "#166534" } else { "#991b1b" };
                            @let status_bg = if r.safe_to_retire { "#dcfce7" } else { "#fee2e2" };
                            @let status_label = if r.safe_to_retire { "✓ Safe to retire" } else { "⚠ In use" };
                            tr {
                                td { code { (r.build_id.clone()) } }
                                td { (r.open_executions) }
                                td { (r.pending_tasks) }
                                td { (r.active_workers) }
                                td { (r.stale_workers) }
                                td {
                                    span style={ "background:" (status_bg) ";color:" (status_color) ";padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600" } {
                                        (status_label)
                                    }
                                }
                                td {
                                    @if r.safe_to_retire {
                                        form method="post" action="build-routing/retire"
                                              onsubmit={ "return confirm('Confirm retirement of build " (js_escape(&r.build_id)) "? All workers running this build should be stopped after confirmation.')" }
                                              style="margin:0" {
                                            input type="hidden" name="build_id" value=(r.build_id.clone());
                                            button.danger type="submit"
                                                style="background:#166534;color:#dcfce7;border:0;border-radius:6px;padding:4px 10px;font-size:11px;cursor:pointer" {
                                                "Retire"
                                            }
                                        }
                                    } @else {
                                        span style="color:#475569;font-size:12px" { "Not yet safe" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_compat_card(all_compat: &[BuildCompatEntry]) -> Markup {
    html! {
        div.card {
            h3 { "Compatibility Declarations" }
            p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                "Workers running build " strong { "A" } " can claim tasks assigned to build " strong { "B" }
                " when a declaration " code { "A → B" } " exists here."
            }
            @if all_compat.is_empty() {
                p.empty { "No compatibility declarations. Workers only claim tasks assigned to their own build." }
            } @else {
                table {
                    thead { tr { th { "Worker Build (A)" } th { "Compatible With (B)" } th { "Declared" } th { "Actions" } } }
                    tbody {
                        @for entry in all_compat {
                            tr {
                                td { code { (entry.build_id.clone()) } }
                                td { code { (entry.compatible_with.clone()) } }
                                td { (format_timestamp(Some(entry.declared_at))) }
                                td {
                                    form method="post" action="build-routing/revoke-compat"
                                          onsubmit={ "return confirm('Revoke compatibility: " (js_escape(&entry.build_id)) " → " (js_escape(&entry.compatible_with)) "?')" }
                                          style="margin:0" {
                                        input type="hidden" name="build_id" value=(entry.build_id.clone());
                                        input type="hidden" name="compatible_with" value=(entry.compatible_with.clone());
                                        button type="submit"
                                            style="background:#450a0a;color:#fca5a5;border:1px solid #991b1b;border-radius:6px;padding:3px 8px;font-size:11px;cursor:pointer" {
                                            "Revoke"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn render_build_routing_action_forms() -> Markup {
    let input_style = "display:block;width:100%;margin-top:4px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:4px;padding:6px 8px;font-size:12px";
    let btn_style = "background:#2563eb;color:#fff;border:0;border-radius:6px;padding:8px 14px;font-size:13px;cursor:pointer;align-self:flex-start";
    let label_style = "font-size:12px;color:#94a3b8";
    html! {
        div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-top:16px" {
            div.card {
                h3 style="margin-top:0" { "Set Build Policy" }
                p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                    "Sets which build ID is assigned to new workflow starts on a queue. "
                    "Does not affect in-flight executions."
                }
                form method="post" action="build-routing/set-policy"
                      style="display:flex;flex-direction:column;gap:10px" {
                    label style=(label_style) { "Queue name"
                        input type="text" name="queue_name" required placeholder="e.g. default" style=(input_style);
                    }
                    label style=(label_style) { "Build ID"
                        input type="text" name="build_id" required placeholder="e.g. sha-abc123" style=(input_style);
                    }
                    label style=(label_style) { "Deployment name (optional)"
                        input type="text" name="deployment_name" placeholder="e.g. prod-v2" style=(input_style);
                    }
                    button type="submit" style=(btn_style)
                        onclick="return confirm('Set build policy? New executions on this queue will use the specified build ID.')" {
                        "Set Policy"
                    }
                }
            }
            div.card {
                h3 style="margin-top:0" { "Declare Compatibility" }
                p style="color:#94a3b8;font-size:12px;margin-bottom:12px" {
                    "Declares that workers running build " strong { "A" }
                    " can safely replay histories assigned to build " strong { "B" }
                    ". Only declare after replay tests confirm safety."
                }
                form method="post" action="build-routing/declare-compat"
                      style="display:flex;flex-direction:column;gap:10px" {
                    label style=(label_style) { "Worker build (A)"
                        input type="text" name="build_id" required placeholder="e.g. sha-new" style=(input_style);
                    }
                    label style=(label_style) { "Compatible with (B)"
                        input type="text" name="compatible_with" required placeholder="e.g. sha-old" style=(input_style);
                    }
                    button type="submit" style=(btn_style)
                        onclick="return confirm('Declare compatibility? Ensure replay tests have confirmed the new build can handle histories from the old build.')" {
                        "Declare"
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn render_build_routing_page(
    policies: &[BuildPolicy],
    all_compat: &[BuildCompatEntry],
    reachability: &[BuildReachability],
    shard_errors: &[(ShardId, &str)],
    diverged_queues: &[String],
    diverged_compat_pairs: &[String],
    is_multi_shard: bool,
    flash: Option<&str>,
    build_id_filter: Option<&str>,
) -> Markup {
    let is_empty = policies.is_empty() && reachability.is_empty() && all_compat.is_empty();

    let body = html! {
        h2 { "Build Routing" }

        @if let Some(bid) = build_id_filter {
            div style="background:#1e293b;border:1px solid #334155;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px;color:#94a3b8" {
                "Filtered to build " code style="color:#e2e8f0" { (bid) }
                " · "
                a href="build-routing" style="color:#60a5fa" { "Show all builds" }
            }
        }

        @if let Some(msg) = flash {
            div.flash { (msg) }
        }

        @if !diverged_queues.is_empty() {
            div style="background:#431407;border:1px solid #ea580c;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#fed7aa" {
                strong { "Policy divergence detected" }
                " — the following queues have different active build IDs across shards, "
                "indicating a partial write failure. Re-apply the policy to resync: "
                @for (i, q) in diverged_queues.iter().enumerate() {
                    @if i > 0 { ", " }
                    code style="color:#fdba74" { (q) }
                }
            }
        }

        @if !diverged_compat_pairs.is_empty() {
            div style="background:#431407;border:1px solid #ea580c;border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:13px;color:#fed7aa" {
                strong { "Compat divergence detected" }
                " — the following pairs are declared on some shards but missing on others. "
                "Re-declare each pair to resync: "
                @for (i, pair) in diverged_compat_pairs.iter().enumerate() {
                    @if i > 0 { ", " }
                    code style="color:#fdba74" { (pair) }
                }
            }
        }

        @for (shard_id, error) in shard_errors {
            div.shard-error {
                @if is_multi_shard {
                    strong { "Shard " (shard_id.as_i32()) " error: " }
                } @else {
                    strong { "Shard error: " }
                }
                (error)
            }
        }

        @if is_empty && shard_errors.is_empty() {
            div.card {
                @if build_id_filter.is_some() {
                    h3 { "No results" }
                    p style="color:#94a3b8;font-size:13px;line-height:1.6" {
                        "No policies, reachability entries, or compat declarations match the active filter. "
                        "The build may not exist or may already be retired."
                    }
                } @else {
                    h3 { "No build routing configured" }
                    p style="color:#94a3b8;font-size:13px;line-height:1.6" {
                        "No build policies have been set and no executions carry a build tag. "
                        "Build routing is inactive — all workers can claim any task."
                    }
                    p style="color:#94a3b8;font-size:13px" {
                        "To start a rolling deploy, follow the operator playbook in "
                        code { "docs/runbooks/safe-deploy.md" }
                        "."
                    }
                }
            }
        } @else {
            (render_build_policies_card(policies))
            (render_build_reachability_card(reachability))
            (render_compat_card(all_compat))
        }

        (render_build_routing_action_forms())
    };

    layout_build_routing("Build Routing · Vantage", &body, None)
}

fn layout_build_routing(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a href="schedules" { "Schedules" }
                        a href="dead-letters" { "Dead Letters" }
                        a.active href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Schedules UI page
// ---------------------------------------------------------------------------

const DEFAULT_SCHEDULE_PAGE_SIZE: i64 = 50;

type ShardScheduleResult = (ShardId, Result<Vec<HarvestSchedule>, String>);

#[derive(Debug, Deserialize)]
pub(crate) struct ScheduleListParams {
    #[serde(default)]
    page: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    target: Option<String>,
    /// "Workflow", "Dag", or empty/absent for All.
    #[serde(default)]
    kind: Option<String>,
    /// "Paused", "Active", or empty/absent for All.
    #[serde(default)]
    paused: Option<String>,
    #[serde(default)]
    shard_id: Option<i32>,
    #[serde(default)]
    refresh: Option<u64>,
    #[serde(default)]
    flash: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ScheduleBulkParams {
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    paused: Option<String>,
    #[serde(default)]
    shard_id: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum ScheduleKindFilter {
    #[default]
    All,
    Workflow,
    Dag,
}

impl ScheduleKindFilter {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Self::All),
            "workflow" => Ok(Self::Workflow),
            "dag" => Ok(Self::Dag),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown kind '{other}'; expected Workflow, Dag, or empty"
            ))),
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Workflow => "Workflow",
            Self::Dag => "Dag",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SchedulePausedFilter {
    #[default]
    All,
    Paused,
    Active,
}

impl SchedulePausedFilter {
    fn parse(raw: &str) -> Result<Self, AutumnError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Self::All),
            "paused" => Ok(Self::Paused),
            "active" => Ok(Self::Active),
            other => Err(AutumnError::bad_request_msg(format!(
                "unknown paused value '{other}'; expected Paused, Active, or empty"
            ))),
        }
    }

    const fn as_label(self) -> &'static str {
        match self {
            Self::All => "",
            Self::Paused => "Paused",
            Self::Active => "Active",
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ScheduleUiFilters {
    target: Option<String>,
    kind: ScheduleKindFilter,
    paused: SchedulePausedFilter,
    shard_id: Option<i32>,
}

impl ScheduleUiFilters {
    fn matches(&self, shard_id: ShardId, row: &HarvestSchedule) -> bool {
        let name = row
            .workflow_name
            .as_deref()
            .or(row.dag_name.as_deref())
            .unwrap_or("");

        if !self
            .target
            .as_deref()
            .is_none_or(|t| name.to_lowercase().contains(&t.to_lowercase()))
        {
            return false;
        }
        match self.kind {
            ScheduleKindFilter::Workflow if row.workflow_name.is_none() => return false,
            ScheduleKindFilter::Dag if row.dag_name.is_none() => return false,
            _ => {}
        }
        match self.paused {
            SchedulePausedFilter::Paused if !row.is_paused => return false,
            SchedulePausedFilter::Active if row.is_paused => return false,
            _ => {}
        }
        if self.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            return false;
        }
        true
    }

    const fn is_empty(&self) -> bool {
        self.target.is_none()
            && matches!(self.kind, ScheduleKindFilter::All)
            && matches!(self.paused, SchedulePausedFilter::All)
            && self.shard_id.is_none()
    }
}

async fn load_schedules_from_shards_ui(api_state: &HarvestApiState) -> Vec<ShardScheduleResult> {
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => {
            return vec![(ShardId::UNENCODED, Err(e.to_string()))];
        }
    };

    let futs: Vec<_> = pool
        .iter_shards()
        .map(|(shard_id, shard_pool)| async move {
            let result = async {
                let mut conn = acquire_conn(shard_pool).await.map_err(|e| e.to_string())?;
                harvest_schedules::table
                    .order(harvest_schedules::next_run_at.asc())
                    .select(HarvestSchedule::as_select())
                    .load(&mut conn)
                    .await
                    .map_err(|e| e.to_string())
            }
            .await;
            (shard_id, result)
        })
        .collect();

    futures::future::join_all(futs).await
}

async fn load_recent_decisions(
    api_state: &HarvestApiState,
    schedule_ids: &[uuid::Uuid],
) -> std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>> {
    use autumn_harvest::schema::harvest_schedule_decisions::dsl;

    if schedule_ids.is_empty() {
        return std::collections::HashMap::new();
    }
    let Ok(pool) = api_state.storage_pool() else {
        return std::collections::HashMap::new();
    };

    let mut all_rows: Vec<ScheduleDecision> = Vec::new();

    for (_shard, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let mut rows: Vec<ScheduleDecision> = dsl::harvest_schedule_decisions
            .filter(dsl::schedule_id.eq_any(schedule_ids))
            .select(ScheduleDecision::as_select())
            .load(&mut conn)
            .await
            .unwrap_or_default();

        all_rows.append(&mut rows);
    }

    let mut map: std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>> =
        std::collections::HashMap::new();

    for row in all_rows {
        if let Some(sched_id) = row.schedule_id {
            map.entry(sched_id).or_default().push(row);
        }
    }

    for decisions in map.values_mut() {
        decisions.sort_by(|a, b| {
            b.occurred_at
                .cmp(&a.occurred_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        decisions.truncate(10);
    }

    map
}

async fn list_schedules_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<ScheduleListParams>,
) -> Result<Markup, AutumnError> {
    let limit = params
        .limit
        .unwrap_or(DEFAULT_SCHEDULE_PAGE_SIZE)
        .clamp(1, MAX_PAGE_SIZE);
    let page = params.page.unwrap_or(0).max(0);
    let offset = page.saturating_mul(limit);

    let kind = params
        .kind
        .as_deref()
        .map(ScheduleKindFilter::parse)
        .transpose()?
        .unwrap_or(ScheduleKindFilter::All);
    let paused_filter = params
        .paused
        .as_deref()
        .map(SchedulePausedFilter::parse)
        .transpose()?
        .unwrap_or(SchedulePausedFilter::All);
    let target = params
        .target
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    let filters = ScheduleUiFilters {
        target,
        kind,
        paused: paused_filter,
        shard_id: params.shard_id,
    };

    let shard_results = load_schedules_from_shards_ui(&api_state).await;
    let is_multi_shard = shard_results.len() > 1;

    let shard_errors: Vec<(ShardId, String)> = shard_results
        .iter()
        .filter_map(|(sid, r)| r.as_ref().err().map(|e| (*sid, e.clone())))
        .collect();

    // Flatten + filter across all shards.
    let mut all_rows: Vec<(ShardId, HarvestSchedule)> = shard_results
        .into_iter()
        .flat_map(|(shard_id, result)| {
            result
                .into_iter()
                .flat_map(move |rows| rows.into_iter().map(move |r| (shard_id, r)))
        })
        .filter(|(sid, row)| filters.matches(*sid, row))
        .collect();

    // Secondary sort: by name for stability when next_run_at is NULL.
    all_rows.sort_by(|(_, a), (_, b)| {
        let a_next = a.next_run_at;
        let b_next = b.next_run_at;
        match (a_next, b_next) {
            (Some(a_ts), Some(b_ts)) => a_ts.cmp(&b_ts),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => {
                let a_name = a
                    .workflow_name
                    .as_deref()
                    .or(a.dag_name.as_deref())
                    .unwrap_or("");
                let b_name = b
                    .workflow_name
                    .as_deref()
                    .or(b.dag_name.as_deref())
                    .unwrap_or("");
                a_name.cmp(b_name)
            }
        }
        .then_with(|| a.id.cmp(&b.id))
    });

    let total_filtered = all_rows.len();
    let distribution = schedule_kind_distribution(&all_rows);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_next = total_filtered > offset_usize.saturating_add(limit_usize);
    let page_rows: Vec<(ShardId, HarvestSchedule)> = all_rows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect();

    let schedule_ids: Vec<uuid::Uuid> = page_rows.iter().map(|(_, r)| r.id).collect();
    let decisions = load_recent_decisions(&api_state, &schedule_ids).await;

    Ok(render_schedules_page(
        &page_rows,
        &shard_errors,
        is_multi_shard,
        &filters,
        &decisions,
        page,
        limit,
        has_next,
        total_filtered,
        &distribution,
        params.refresh,
        params.flash.as_deref(),
    ))
}

/// Parse a `ScheduleUiFilters` from optional string fields.
fn parse_schedule_bulk_filters(params: &ScheduleBulkParams) -> ScheduleUiFilters {
    let kind = params
        .kind
        .as_deref()
        .and_then(|s| ScheduleKindFilter::parse(s).ok())
        .unwrap_or(ScheduleKindFilter::All);
    let paused = params
        .paused
        .as_deref()
        .and_then(|s| SchedulePausedFilter::parse(s).ok())
        .unwrap_or(SchedulePausedFilter::All);
    let target = params
        .target
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    ScheduleUiFilters {
        target,
        kind,
        paused,
        shard_id: params.shard_id,
    }
}

/// Find a schedule by id across all shards. Returns the row, the shard it lives
/// on, and a conn to that shard on success.
async fn find_schedule_row(
    api_state: &HarvestApiState,
    id_str: &str,
) -> Result<
    Option<(
        HarvestSchedule,
        autumn_harvest::types::ShardId,
        crate::api::PoolConn,
    )>,
    axum::response::Response,
> {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let Ok(id) = id_str.parse::<uuid::Uuid>() else {
        return Err(
            AutumnError::bad_request_msg(format!("invalid schedule id '{id_str}'")).into_response(),
        );
    };
    let pool = api_state
        .storage_pool()
        .map_err(|e| map_error(e).into_response())?;

    for (shard, shard_pool) in pool.iter_shards() {
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let row: Option<HarvestSchedule> = dsl::harvest_schedules
            .find(id)
            .select(HarvestSchedule::as_select())
            .first(&mut conn)
            .await
            .optional()
            .unwrap_or(None);
        if let Some(row) = row {
            return Ok(Some((row, shard, conn)));
        }
    }
    Ok(None)
}

fn schedule_name(row: &HarvestSchedule) -> String {
    row.workflow_name
        .as_deref()
        .or(row.dag_name.as_deref())
        .unwrap_or("")
        .to_string()
}

async fn schedule_pause_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let now = Utc::now();
        let _ = diesel::update(
            dsl::harvest_schedules
                .find(row.id)
                .filter(dsl::is_paused.ne(true)),
        )
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_at.eq(Some(now)),
            dsl::paused_by.eq(Some("ui")),
            dsl::updated_at.eq(now),
        ))
        .execute(&mut conn)
        .await;
        let ar = NewAuditRecord {
            actor: "ui",
            operation: OP_SCHEDULE_PAUSE,
            target_type: TARGET_SCHEDULE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/schedules/pause",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_API,
        };
        let _ = insert_audit(&mut conn, &ar).await;
        format!("Paused {name}")
    } else {
        format!("Paused schedule {}", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

async fn schedule_resume_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let now = Utc::now();
        let _ = diesel::update(
            dsl::harvest_schedules
                .find(row.id)
                .filter(dsl::is_paused.ne(false)),
        )
        .set((
            dsl::is_paused.eq(false),
            dsl::paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::paused_by.eq(None::<&str>),
            dsl::pause_reason.eq(None::<&str>),
            dsl::updated_at.eq(now),
        ))
        .execute(&mut conn)
        .await;
        let ar = NewAuditRecord {
            actor: "ui",
            operation: OP_SCHEDULE_RESUME,
            target_type: TARGET_SCHEDULE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/schedules/resume",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_API,
        };
        let _ = insert_audit(&mut conn, &ar).await;
        format!("Resumed {name}")
    } else {
        format!("Resumed schedule {}", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

async fn schedule_delete_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;

    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };

    let flash = if let Some((row, _shard, mut conn)) = found {
        let name = schedule_name(&row);
        let n = diesel::delete(dsl::harvest_schedules.find(row.id))
            .execute(&mut conn)
            .await
            .unwrap_or(0);
        if n > 0 {
            let ar = NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_DELETE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/delete",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: None,
                source: SOURCE_API,
            };
            let _ = insert_audit(&mut conn, &ar).await;
            format!("Deleted {name}")
        } else {
            format!(
                "Schedule {} was already deleted",
                &id_str[..8.min(id_str.len())]
            )
        }
    } else {
        format!("Schedule {} not found", &id_str[..8.min(id_str.len())])
    };
    schedule_redirect(&flash)
}

/// Inner logic for `schedule_trigger_now_ui` after the connection is acquired.
/// Handles the Skip overlap check, start call, audit write, and metric emit,
/// then returns the redirect response so the outer handler stays compact.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn execute_schedule_trigger_ui(
    conn: &mut crate::api::PoolConn,
    pool: &crate::HarvestDbPool,
    runtime: &HarvestApiRuntime,
    gate_cache: &autumn_harvest::admission_gate::AdmissionGateCache,
    row: &HarvestSchedule,
    id_str: &str,
    name: &str,
    workflow_name: &str,
    input: serde_json::Value,
    queue: &str,
) -> axum::response::Response {
    // Pre-generate workflow_id and triggered_at so the gate check uses the
    // actual execution shard (determined by the router) rather than the shard
    // where the schedule row was found.  The values are reused below.
    let triggered_at = chrono::Utc::now();
    let workflow_id = format!(
        "manual-{}-{}-{}",
        row.id,
        triggered_at.timestamp_millis(),
        uuid::Uuid::new_v4().simple()
    );
    let exec_shard = runtime
        .router()
        .pick_for_new_workflow(workflow_name, &workflow_id);

    // issue #377: check admission gates before firing this manual schedule trigger.
    {
        let dag_name_for_owner = row.dag_name.as_deref().unwrap_or(workflow_name);
        let wf_owner = runtime
            .registry()
            .workflows
            .get(workflow_name)
            .and_then(|i| i.owner)
            .or_else(|| {
                runtime
                    .dags()
                    .get(dag_name_for_owner)
                    .and_then(|d| d.owner.as_deref())
            });
        if let Some((gate_id, gate_reason, scope_kind)) =
            gate_cache.check(workflow_name, queue, exec_shard.as_i32(), wf_owner)
        {
            let reason_label = match gate_reason.char_indices().nth(64) {
                Some((idx, _)) => &gate_reason[..idx],
                None => &gate_reason,
            };
            runtime
                .registry()
                .telemetry()
                .metrics
                .record_admission_blocked(scope_kind, reason_label);
            let ar = build_trigger_audit("ui", id_str, STATUS_FAILED, Some("admission_blocked"));
            let _ = insert_audit(conn, &ar).await;
            return schedule_redirect(&format!("Trigger blocked by gate {gate_id}: {gate_reason}"));
        }
    }

    // Count active (RUNNING or PAUSED) executions across ALL shards. A PAUSED run
    // still occupies an active slot for overlap/Skip enforcement (issue #383),
    // matching the scheduler and backfill counters. The async block returns None
    // if any shard is unreachable — used for fail-closed Skip enforcement.
    let running_count: Option<i64> = async {
        let mut total: i64 = 0;
        for (_, shard_pool) in pool.iter_shards() {
            let mut c = acquire_conn(shard_pool).await.ok()?;
            let n: i64 = harvest_workflow_executions::table
                .filter(harvest_workflow_executions::workflow_name.eq(workflow_name))
                .filter(harvest_workflow_executions::state.eq_any(["RUNNING", "PAUSED"]))
                .count()
                .get_result(&mut c)
                .await
                .ok()?;
            total += n;
        }
        Some(total)
    }
    .await;
    if autumn_harvest::OverlapPolicy::from_db(&row.overlap_policy)
        == autumn_harvest::OverlapPolicy::Skip
    {
        match running_count {
            None => {
                let ar = build_trigger_audit("ui", id_str, STATUS_FAILED, Some("count_failed"));
                let _ = insert_audit(conn, &ar).await;
                runtime
                    .registry()
                    .telemetry()
                    .metrics
                    .record_schedule_manual_trigger(name, "start_failed");
                return schedule_redirect(&format!(
                    "Failed to trigger {name}: could not count active runs"
                ));
            }
            Some(n) if n >= i64::from(row.max_active_runs) => {
                let ar =
                    build_trigger_audit("ui", id_str, STATUS_SUCCEEDED, Some("skipped_overlap"));
                let _ = insert_audit(conn, &ar).await;
                runtime
                    .registry()
                    .telemetry()
                    .metrics
                    .record_schedule_manual_trigger(name, "skipped_overlap");
                return schedule_redirect(&format!(
                    "Skipped {name}: max_active_runs already reached"
                ));
            }
            Some(_) => {}
        }
    }
    // triggered_at and workflow_id were pre-generated above for the gate check.
    let exec_id = HarvestExecutionId::new();
    let (owner, runbook_url, severity) = {
        let wf_meta = runtime
            .registry()
            .workflows
            .get(workflow_name)
            .map(|info| (info.owner, info.runbook_url, info.severity));
        let dag_meta = runtime.dags().get(workflow_name).map(|dag| {
            (
                dag.owner.as_deref(),
                dag.runbook_url.as_deref(),
                dag.severity.as_deref(),
            )
        });
        match (wf_meta, dag_meta) {
            (Some((o, r, s)), Some((dag_owner, dag_runbook, dag_severity))) => {
                (o.or(dag_owner), r.or(dag_runbook), s.or(dag_severity))
            }
            (Some((o, r, s)), None) => (o, r, s),
            (None, Some((dag_owner, dag_runbook, dag_severity))) => {
                (dag_owner, dag_runbook, dag_severity)
            }
            (None, None) => (None, None, None),
        }
    };
    // Only registered workflows carry an SLA default; DAGs have no SLA concept.
    let (sla, wf_default_retry_policy) =
        runtime
            .registry()
            .workflows
            .get(workflow_name)
            .map_or((None, None), |info| {
                (
                    crate::api::clamp_info_default_sla(info.sla, info.execution_timeout),
                    info.retry_policy.clone(),
                )
            });
    // Schedule-level retry_policy takes precedence over the workflow-type default,
    // mirroring the automated tick, backfill, and API trigger-now paths.
    let ui_trigger_retry_policy = row
        .retry_policy
        .as_ref()
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .or(wf_default_retry_policy);

    let result = start_or_load_workflow_execution_with_metrics(
        conn,
        StartWorkflowParams {
            workflow_name,
            workflow_id: &workflow_id,
            exec_id,
            input,
            parent_id: None,
            queue_name: queue,
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
            owner,
            runbook_url,
            severity,
            context_headers: None,
            sla,
            // Manual trigger-now fires are attributed to the schedule (schedule_id is
            // set) so they appear in GET /admin/schedules/{id}/runs, but scheduled_for
            // stays None so resolve_carryover (issue #488) still short-circuits for
            // this run — NULL slot comparisons are false, so carryover is never
            // resolved for a manual fire.
            schedule_id: Some(row.id),
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: ui_trigger_retry_policy,
            retry_of_exec_id: None,
            max_workflow_attempts_ceiling: runtime.registry().max_workflow_attempts_ceiling,
            origin: Some(autumn_harvest::execution::ORIGIN_MANUAL_TRIGGER),
            completion_callbacks: None,
        },
        Some(runtime.registry().telemetry().metrics.as_ref()),
    )
    .await;
    let (status, outcome) = if result.is_ok() {
        (STATUS_SUCCEEDED, "fired")
    } else {
        (STATUS_FAILED, "start_failed")
    };
    let ar = build_trigger_audit(
        "ui",
        id_str,
        status,
        result.is_err().then_some("start_failed"),
    );
    let _ = insert_audit(conn, &ar).await;
    runtime
        .registry()
        .telemetry()
        .metrics
        .record_schedule_manual_trigger(name, outcome);
    schedule_redirect(&match result {
        Ok(_) => format!("Triggered run of {name}"),
        Err(e) => format!("Failed to trigger {name}: {e}"),
    })
}

/// Build a `NewAuditRecord` for UI schedule trigger operations.
const fn build_trigger_audit<'a>(
    actor: &'a str,
    target_id: &'a str,
    status: &'a str,
    error_summary: Option<&'a str>,
) -> NewAuditRecord<'a> {
    NewAuditRecord {
        actor,
        operation: OP_SCHEDULE_TRIGGER,
        target_type: TARGET_SCHEDULE,
        target_id: Some(target_id),
        route_or_command: "POST /ui/schedules/trigger-now",
        request_id: None,
        idempotency_key: None,
        status,
        error_summary,
        shard_id: None,
        source: SOURCE_API,
    }
}

/// Resolve `(workflow_name, input, queue)` for a manual trigger, consulting the
/// runtime registry for DAG-backed schedules so the correct default queue is used.
fn resolve_trigger_params(
    row: &HarvestSchedule,
    runtime: &HarvestApiRuntime,
) -> Result<(String, serde_json::Value, String), String> {
    match (row.workflow_name.as_deref(), row.dag_name.as_deref()) {
        (Some(wf), _) => {
            let q = row.queue_name.as_deref().unwrap_or("default").to_string();
            Ok((
                wf.to_string(),
                row.workflow_input
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
                q,
            ))
        }
        (None, Some(dag)) => {
            let q = runtime
                .dags()
                .get(dag)
                .and_then(|d| d.default_queue.as_deref())
                .or(row.queue_name.as_deref())
                .unwrap_or("default")
                .to_string();
            Ok((dag.to_string(), serde_json::Value::Null, q))
        }
        (None, None) => Err("schedule has no workflow or dag name".to_string()),
    }
}

async fn schedule_trigger_now_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id_str): Path<String>,
) -> axum::response::Response {
    let found = match find_schedule_row(&api_state, &id_str).await {
        Ok(f) => f,
        Err(response) => return response,
    };
    let Some((row, _found_shard, _)) = found else {
        return schedule_redirect(&format!(
            "Schedule {} not found",
            &id_str[..8.min(id_str.len())]
        ));
    };
    let name = schedule_name(&row);
    let runtime = match api_state.runtime() {
        Ok(r) => r,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    let (workflow_name, input, queue) = match resolve_trigger_params(&row, &runtime) {
        Ok(p) => p,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    // Use default_pool() so ExecutionId::new() (ShardId::UNENCODED) and the
    // connection target agree — consistent with the API handler's routing.
    let mut conn = match acquire_conn(pool.default_pool()).await {
        Ok(c) => c,
        Err(e) => return schedule_redirect(&format!("Failed to trigger {name}: {e}")),
    };
    execute_schedule_trigger_ui(
        &mut conn,
        &pool,
        &runtime,
        &api_state.gate_cache(),
        &row,
        &id_str,
        &name,
        &workflow_name,
        input,
        &queue,
    )
    .await
}

async fn schedule_bulk_pause_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(params): Form<ScheduleBulkParams>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let filters = parse_schedule_bulk_filters(&params);

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };

    let now = Utc::now();
    let mut acted_on = 0usize;

    for (shard_id, shard_pool) in pool.iter_shards() {
        if filters.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            continue;
        }
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        // Load just id + name fields to apply kind/target filters without N+1 updates.
        let candidates: Vec<(uuid::Uuid, Option<String>, Option<String>)> = dsl::harvest_schedules
            .filter(dsl::is_paused.ne(true))
            .select((dsl::id, dsl::workflow_name, dsl::dag_name))
            .load(&mut conn)
            .await
            .unwrap_or_default();
        let matching_ids: Vec<uuid::Uuid> = candidates
            .into_iter()
            .filter(|(_, wf, dag)| {
                let name = wf.as_deref().or(dag.as_deref()).unwrap_or("");
                match filters.kind {
                    ScheduleKindFilter::Workflow if wf.is_none() => return false,
                    ScheduleKindFilter::Dag if dag.is_none() => return false,
                    _ => {}
                }
                filters
                    .target
                    .as_deref()
                    .is_none_or(|t| name.to_lowercase().contains(&t.to_lowercase()))
            })
            .map(|(id, _, _)| id)
            .collect();
        if matching_ids.is_empty() {
            continue;
        }
        let updated_ids: Vec<uuid::Uuid> = diesel::update(
            dsl::harvest_schedules
                .filter(dsl::id.eq_any(&matching_ids))
                .filter(dsl::is_paused.ne(true)),
        )
        .set((
            dsl::is_paused.eq(true),
            dsl::paused_at.eq(Some(now)),
            dsl::paused_by.eq(Some("ui-bulk")),
            dsl::updated_at.eq(now),
        ))
        .returning(dsl::id)
        .get_results(&mut conn)
        .await
        .unwrap_or_default();
        acted_on += updated_ids.len();
        for id in &updated_ids {
            let id_str = id.to_string();
            let ar = NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_PAUSE,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/bulk-pause",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard_id.as_i32()),
                source: SOURCE_API,
            };
            let _ = insert_audit(&mut conn, &ar).await;
        }
    }

    schedule_redirect(&format!("Paused {acted_on} schedule(s)"))
}

async fn schedule_bulk_resume_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Form(params): Form<ScheduleBulkParams>,
) -> axum::response::Response {
    use autumn_harvest::schema::harvest_schedules::dsl;
    use axum::response::IntoResponse as _;

    let filters = parse_schedule_bulk_filters(&params);

    let pool = match api_state.storage_pool() {
        Ok(p) => p,
        Err(e) => return map_error(e).into_response(),
    };

    let now = Utc::now();
    let mut acted_on = 0usize;

    for (shard_id, shard_pool) in pool.iter_shards() {
        if filters.shard_id.is_some_and(|sid| shard_id.as_i32() != sid) {
            continue;
        }
        let Ok(mut conn) = acquire_conn(shard_pool).await else {
            continue;
        };
        let candidates: Vec<(uuid::Uuid, Option<String>, Option<String>)> = dsl::harvest_schedules
            .filter(dsl::is_paused.eq(true))
            .select((dsl::id, dsl::workflow_name, dsl::dag_name))
            .load(&mut conn)
            .await
            .unwrap_or_default();
        let matching_ids: Vec<uuid::Uuid> = candidates
            .into_iter()
            .filter(|(_, wf, dag)| {
                let name = wf.as_deref().or(dag.as_deref()).unwrap_or("");
                match filters.kind {
                    ScheduleKindFilter::Workflow if wf.is_none() => return false,
                    ScheduleKindFilter::Dag if dag.is_none() => return false,
                    _ => {}
                }
                filters
                    .target
                    .as_deref()
                    .is_none_or(|t| name.to_lowercase().contains(&t.to_lowercase()))
            })
            .map(|(id, _, _)| id)
            .collect();
        if matching_ids.is_empty() {
            continue;
        }
        let updated_ids: Vec<uuid::Uuid> = diesel::update(
            dsl::harvest_schedules
                .filter(dsl::id.eq_any(&matching_ids))
                .filter(dsl::is_paused.eq(true)),
        )
        .set((
            dsl::is_paused.eq(false),
            dsl::paused_at.eq(None::<chrono::DateTime<Utc>>),
            dsl::paused_by.eq(None::<&str>),
            dsl::pause_reason.eq(None::<&str>),
            dsl::updated_at.eq(now),
        ))
        .returning(dsl::id)
        .get_results(&mut conn)
        .await
        .unwrap_or_default();
        acted_on += updated_ids.len();
        for id in &updated_ids {
            let id_str = id.to_string();
            let ar = NewAuditRecord {
                actor: "ui",
                operation: OP_SCHEDULE_RESUME,
                target_type: TARGET_SCHEDULE,
                target_id: Some(id_str.as_str()),
                route_or_command: "POST /ui/schedules/bulk-resume",
                request_id: None,
                idempotency_key: None,
                status: STATUS_SUCCEEDED,
                error_summary: None,
                shard_id: Some(shard_id.as_i32()),
                source: SOURCE_API,
            };
            let _ = insert_audit(&mut conn, &ar).await;
        }
    }

    schedule_redirect(&format!("Resumed {acted_on} schedule(s)"))
}

fn schedule_redirect(flash: &str) -> axum::response::Response {
    use axum::response::IntoResponse as _;
    let location = format!("schedules?flash={}", url_encode(flash));
    axum::response::Redirect::to(&location).into_response()
}

// ---------------------------------------------------------------------------
// Schedule rendering helpers
// ---------------------------------------------------------------------------

/// Returns a short distribution string like "3 Workflow, 2 Dag" for all matching rows.
fn schedule_kind_distribution(rows: &[(ShardId, HarvestSchedule)]) -> String {
    let mut wf = 0usize;
    let mut dag = 0usize;
    for (_, row) in rows {
        if row.workflow_name.is_some() {
            wf += 1;
        } else {
            dag += 1;
        }
    }
    match (wf, dag) {
        (0, 0) => String::new(),
        (w, 0) => format!("{w} Workflow"),
        (0, d) => format!("{d} Dag"),
        (w, d) => format!("{w} Workflow, {d} Dag"),
    }
}

#[allow(clippy::too_many_arguments)]
fn render_schedules_page(
    rows: &[(ShardId, HarvestSchedule)],
    shard_errors: &[(ShardId, String)],
    is_multi_shard: bool,
    filters: &ScheduleUiFilters,
    decisions: &std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>>,
    page: i64,
    limit: i64,
    has_next: bool,
    total_filtered: usize,
    distribution: &str,
    refresh: Option<u64>,
    flash: Option<&str>,
) -> Markup {
    let body = html! {
        h2 { "Schedules" }

        @if let Some(message) = flash {
            div.flash { (message) }
        }

        (render_schedule_filters(filters, limit, refresh))
        (render_schedule_bulk_actions(filters, limit, refresh, total_filtered, distribution))

        @if rows.is_empty() && shard_errors.is_empty() {
            div.card.empty {
                @if filters.is_empty() {
                    "No schedules registered."
                } @else {
                    "No schedules match this filter."
                }
            }
        } @else {
            @for (shard_id, error) in shard_errors {
                div.shard-error {
                    @if is_multi_shard {
                        strong { "Shard " (shard_id.as_i32()) " unavailable: " }
                    } @else {
                        strong { "Shard unavailable: " }
                    }
                    (error)
                }
            }
            (render_schedule_table(rows, is_multi_shard, decisions))
        }

        (render_schedule_pagination(page, limit, has_next, filters, refresh))
    };

    layout_schedules("Schedules · Vantage", &body, refresh)
}

fn render_schedule_filters(
    filters: &ScheduleUiFilters,
    limit: i64,
    refresh: Option<u64>,
) -> Markup {
    let target_val = filters.target.as_deref().unwrap_or("");
    let kind_val = filters.kind.as_label();
    let paused_val = filters.paused.as_label();
    let shard_val = filters.shard_id.map(|s| s.to_string()).unwrap_or_default();
    let refresh_value = refresh.map(|s| s.to_string()).unwrap_or_default();

    html! {
        form.filters method="get" action="schedules" {
            label {
                "Target"
                input type="text" name="target" value=(target_val) placeholder="e.g. payment_workflow";
            }
            label {
                "Kind"
                select name="kind" {
                    option value="" selected[kind_val.is_empty()] { "All" }
                    option value="Workflow" selected[kind_val == "Workflow"] { "Workflow" }
                    option value="Dag" selected[kind_val == "Dag"] { "Dag" }
                }
            }
            label {
                "Paused"
                select name="paused" {
                    option value="" selected[paused_val.is_empty()] { "All" }
                    option value="Paused" selected[paused_val == "Paused"] { "Paused" }
                    option value="Active" selected[paused_val == "Active"] { "Active" }
                }
            }
            label {
                "Shard"
                input type="number" name="shard_id" value=(shard_val) placeholder="e.g. 0";
            }
            label {
                "Per page"
                input type="number" name="limit" min="1" max=(MAX_PAGE_SIZE) value=(limit);
            }
            label {
                "Refresh"
                select name="refresh" {
                    option value="" selected[refresh.is_none()] { "Off" }
                    option value="30" selected[refresh == Some(30)] { "30s" }
                    option value="60" selected[refresh == Some(60)] { "60s" }
                    @if refresh.is_some_and(|secs| secs != 30 && secs != 60) {
                        option value=(refresh_value) selected { (refresh_value) "s" }
                    }
                }
            }
            button type="submit" { "Apply" }
            a.reset href="schedules" { "Reset" }
        }
    }
}

fn render_schedule_bulk_actions(
    filters: &ScheduleUiFilters,
    limit: i64,
    refresh: Option<u64>,
    total_matching: usize,
    distribution: &str,
) -> Markup {
    let return_qs = build_schedule_query_string(limit, filters, refresh);
    let dist_suffix = if distribution.is_empty() {
        String::new()
    } else {
        format!(" ({distribution})")
    };
    html! {
        div."bulk-actions" {
            form method="post" action="schedules/bulk-pause"
                onsubmit={ "return confirm('Pause " (total_matching) " matching schedule(s)" (&dist_suffix) "?')" } {
                (render_schedule_hidden_filters(filters))
                button type="submit" disabled[total_matching == 0] {
                    "Pause all matching (" (total_matching) ")"
                }
            }
            form method="post" action="schedules/bulk-resume"
                onsubmit={ "return confirm('Resume " (total_matching) " matching schedule(s)" (&dist_suffix) "?')" } {
                (render_schedule_hidden_filters(filters))
                button type="submit" disabled[total_matching == 0] {
                    "Resume all matching (" (total_matching) ")"
                }
            }
            @if !return_qs.is_empty() {
                span { "Filters active" }
            }
        }
    }
}

fn render_schedule_hidden_filters(filters: &ScheduleUiFilters) -> Markup {
    html! {
        @if let Some(ref target) = filters.target {
            input type="hidden" name="target" value=(target);
        }
        @if !matches!(filters.kind, ScheduleKindFilter::All) {
            input type="hidden" name="kind" value=(filters.kind.as_label());
        }
        @if !matches!(filters.paused, SchedulePausedFilter::All) {
            input type="hidden" name="paused" value=(filters.paused.as_label());
        }
        @if let Some(shard_id) = filters.shard_id {
            input type="hidden" name="shard_id" value=(shard_id);
        }
    }
}

#[allow(clippy::too_many_lines)]
fn render_schedule_table(
    rows: &[(ShardId, HarvestSchedule)],
    is_multi_shard: bool,
    decisions: &std::collections::HashMap<uuid::Uuid, Vec<ScheduleDecision>>,
) -> Markup {
    html! {
        table {
            thead {
                tr {
                    th { "Schedule ID" }
                    th { "Kind" }
                    th { "Target" }
                    th { "Expression" }
                    th { "Timezone" }
                    th { "Next Run" }
                    th { "Last Run" }
                    th { "State" }
                    th { "Created" }
                    @if is_multi_shard { th { "Shard" } }
                    th { "Actions" }
                }
            }
            tbody {
                @for (shard_id, row) in rows {
                    @let id_str = row.id.to_string();
                    @let kind_label = if row.dag_name.is_some() { "Dag" } else { "Workflow" };
                    @let target_name = row.workflow_name.as_deref()
                        .or(row.dag_name.as_deref())
                        .unwrap_or("—");
                    @let expr = row.schedule_expr.as_deref().unwrap_or("—");
                    tr {
                        td { code { (short_id(&id_str)) } }
                        td { (kind_label) }
                        td {
                            code { (target_name) }
                            @if let Some(s_decisions) = decisions.get(&row.id) {
                                @if !s_decisions.is_empty() {
                                    div style="margin-top: 6px" {
                                        details {
                                            summary style="font-size: 11px; color: #93c5fd; cursor: pointer" { "Recent Decisions (" (s_decisions.len()) ")" }
                                            div style="display: flex; flex-direction: column; gap: 4px; padding: 6px; background: #0f172a; border-radius: 4px; margin-top: 4px; font-size: 11px; max-width: 400px" {
                                                @for dec in s_decisions {
                                                    @let dec_badge_class = match dec.decision.as_str() {
                                                        "fired" => "badge COMPLETED",
                                                        "skipped" => "badge Active",
                                                        "suppressed_paused" => "badge CANCELLED",
                                                        "backfilled" => "badge RUNNING",
                                                        _ => "badge UNKNOWN",
                                                    };
                                                    div style="display: flex; align-items: center; justify-content: space-between; gap: 8px" {
                                                        span class=(dec_badge_class) style="font-size: 10px; padding: 1px 6px" { (dec.decision) }
                                                        span style="color: #cbd5e1; font-family: monospace" { (dec.reason_code) }
                                                        span style="color: #64748b" { (format_timestamp(Some(dec.occurred_at))) }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        td { code { (expr) } }
                        td {
                            @if row.timezone == "UTC" {
                                span class="timezone-utc" { "UTC" }
                            } @else {
                                span.badge.timezone { (row.timezone) }
                            }
                        }
                        td { (format_timestamp(row.next_run_at)) }
                        td { (format_timestamp(row.last_run_at)) }
                        td { (schedule_state_badge(row.is_paused)) }
                        td { (format_timestamp(Some(row.created_at))) }
                        @if is_multi_shard { td { (shard_id.as_i32()) } }
                        td {
                            div.actions {
                                @if !row.is_paused {
                                    form method="post"
                                        action={ "schedules/" (id_str) "/pause" }
                                        onsubmit="return confirm('Pause this schedule?')" {
                                        button type="submit" { "Pause" }
                                    }
                                }
                                @if row.is_paused {
                                    form method="post"
                                        action={ "schedules/" (id_str) "/resume" }
                                        onsubmit="return confirm('Resume this schedule?')" {
                                        button type="submit" { "Resume" }
                                    }
                                }
                                form method="post"
                                    action={ "schedules/" (id_str) "/trigger-now" }
                                    onsubmit={
                                        @if row.is_paused {
                                            "return confirm('This schedule is paused. Force a manual run anyway?')"
                                        } @else {
                                            "return confirm('Trigger a one-off run of this schedule now?')"
                                        }
                                    } {
                                    button.secondary type="submit" { "Run now" }
                                }
                                form method="post"
                                    action={ "schedules/" (id_str) "/delete" }
                                    onsubmit={ "return confirm('Delete schedule " (id_str) "? This cannot be undone.')" } {
                                    button.danger type="submit" { "Delete" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn schedule_state_badge(is_paused: bool) -> Markup {
    if is_paused {
        html! { span.badge.CANCELLED { "Paused" } }
    } else {
        html! { span.badge.Active { "Active" } }
    }
}

fn render_schedule_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    filters: &ScheduleUiFilters,
    refresh: Option<u64>,
) -> Markup {
    let base = build_schedule_query_string(limit, filters, refresh);
    html! {
        div.pagination {
            @if page > 0 {
                a href={ "schedules?page=" (page - 1) (PreEscaped(&base)) } {
                    (PreEscaped("&larr;")) " Previous"
                }
            } @else {
                span.disabled { (PreEscaped("&larr;")) " Previous" }
            }
            span { "Page " (page + 1) }
            @if has_next {
                a href={ "schedules?page=" (page + 1) (PreEscaped(&base)) } {
                    "Next " (PreEscaped("&rarr;"))
                }
            } @else {
                span.disabled { "Next " (PreEscaped("&rarr;")) }
            }
        }
    }
}

fn build_schedule_query_string(
    limit: i64,
    filters: &ScheduleUiFilters,
    refresh: Option<u64>,
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_SCHEDULE_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(ref target) = filters.target {
        let _ = write!(out, "&target={}", url_encode(target));
    }
    if !matches!(filters.kind, ScheduleKindFilter::All) {
        let _ = write!(out, "&kind={}", filters.kind.as_label());
    }
    if !matches!(filters.paused, SchedulePausedFilter::All) {
        let _ = write!(out, "&paused={}", filters.paused.as_label());
    }
    if let Some(shard_id) = filters.shard_id {
        let _ = write!(out, "&shard_id={shard_id}");
    }
    if let Some(secs) = refresh {
        let _ = write!(out, "&refresh={secs}");
    }
    out
}

// ── Admission gates UI (issue #377) ──────────────────────────────────────────

async fn list_gates_ui(
    Extension(api_state): Extension<HarvestApiState>,
) -> Result<Markup, AutumnError> {
    let pool = api_state.storage_pool().map_err(map_error)?;
    let mut conn = acquire_conn(pool.default_pool()).await?;

    let rows = autumn_harvest::admission_gate::db::list_gates(&mut conn)
        .await
        .map_err(map_error)?;

    Ok(render_gates_page(&rows))
}

/// `POST /admin/gates/{id}/lift` — Vantage UI lift action (redirects back to gates list).
async fn lift_gate_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<uuid::Uuid>,
) -> axum::response::Response {
    let Ok(pool) = api_state.storage_pool() else {
        return axum::response::Redirect::to("../../admin/gates").into_response();
    };
    let Ok(mut conn) = acquire_conn(pool.default_pool()).await else {
        return axum::response::Redirect::to("../../admin/gates").into_response();
    };

    let id_str = id.to_string();
    if let Ok(Some(_gate)) =
        autumn_harvest::admission_gate::db::lift_gate(&mut conn, id, "ui").await
    {
        if let Ok(fresh) = autumn_harvest::admission_gate::db::load_active_gates(&mut conn).await {
            api_state.gate_cache().refresh(fresh);
        }
        let ar = autumn_harvest::models::NewAuditRecord {
            actor: "ui",
            operation: OP_GATE_LIFT,
            target_type: TARGET_GATE,
            target_id: Some(id_str.as_str()),
            route_or_command: "POST /ui/admin/gates/{id}/lift",
            request_id: None,
            idempotency_key: None,
            status: STATUS_SUCCEEDED,
            error_summary: None,
            shard_id: None,
            source: SOURCE_UI,
        };
        let _ = insert_audit(&mut conn, &ar).await;
    }

    // Correct relative target from /ui/admin/gates/{id}/lift back to the
    // gates list at /ui/admin/gates.  "../../admin/gates" would resolve to
    // /ui/admin/admin/gates (one "admin" too many).
    axum::response::Redirect::to("../../gates").into_response()
}

fn render_gates_page(rows: &[autumn_harvest::models::AdmissionGateRow]) -> Markup {
    let body = html! {
        h2 { "Admission Gates" }
        p.note {
            "Active gates block new workflow starts. In-flight executions are unaffected. "
            "Use the "
            a href="../../admin/gates" { "management API" }
            " ("
            code { "POST /admin/gates" }
            ", "
            code { "DELETE /admin/gates/{id}" }
            ") to create or lift gates."
        }

        @if rows.iter().all(|r| r.lifted_at.is_some()) {
            div.card.empty { "No active admission gates." }
        }

        @for row in rows.iter().filter(|r| r.lifted_at.is_none()) {
            @let id_str = row.id.to_string();
            @let id_short = &id_str[..8];
            @let now = chrono::Utc::now();
            @let is_expired = row.expires_at.is_some_and(|exp| exp <= now);
            div.card {
                div style="display:flex;justify-content:space-between;align-items:center" {
                    div {
                        strong { code { (id_short) } }
                        " "
                        @if is_expired {
                            span.badge style="background:#374151;color:#9ca3af" { "EXPIRED" }
                        } @else {
                            span.badge.FAILED { "ACTIVE" }
                        }
                        " "
                        span { (row.scope_kind) }
                        @if let Some(ref v) = row.scope_value {
                            " = "
                            code { (v) }
                        }
                    }
                    div style="display:flex;gap:16px;align-items:center" {
                        div style="font-size:12px;color:#94a3b8" {
                            "created by " (row.created_by)
                            " at " (row.created_at.format("%Y-%m-%d %H:%M:%S UTC"))
                            @if let Some(exp) = row.expires_at {
                                @if is_expired {
                                    " · expired " (exp.format("%Y-%m-%d %H:%M:%S UTC"))
                                } @else {
                                    " · expires " (exp.format("%Y-%m-%d %H:%M:%S UTC"))
                                }
                            }
                        }
                        @if !is_expired {
                            form method="POST" action={ "gates/" (id_str) "/lift" }
                                onsubmit="return confirm('Lift this gate?')" {
                                button type="submit"
                                    style="background:#15803d;color:#dcfce7;border:none;border-radius:4px;padding:4px 12px;cursor:pointer;font-size:12px" {
                                    "Lift"
                                }
                            }
                        }
                    }
                }
                @if !row.reason.is_empty() {
                    p style="margin:8px 0 0;color:#e2e8f0" { (row.reason) }
                }
                @if let Some(ref msg) = row.message {
                    p style="margin:4px 0 0;color:#94a3b8;font-size:12px" { (msg) }
                }
            }
        }

        @let lifted: Vec<_> = rows.iter().filter(|r| r.lifted_at.is_some()).collect();
        @if !lifted.is_empty() {
            details style="margin-top:20px" {
                summary style="cursor:pointer;color:#94a3b8" {
                    (lifted.len()) " lifted gate(s)"
                }
                @for row in &lifted {
                    @let id_short = &row.id.to_string()[..8];
                    div.card style="opacity:0.6" {
                        code { (id_short) }
                        " "
                        span.badge.COMPLETED { "LIFTED" }
                        " "
                        (row.scope_kind)
                        @if let Some(ref v) = row.scope_value {
                            " = " code { (v) }
                        }
                        " — " (row.reason)
                    }
                }
            }
        }
    };
    layout_gates("Admission Gates · Vantage", &body)
}

fn layout_gates(title: &str, body: &Markup) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="../workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="../workflows" { "Workflows" }
                        a href="../workers" { "Workers" }
                        a href="../schedules" { "Schedules" }
                        a href="../dead-letters" { "Dead Letters" }
                        a href="../build-routing" { "Build Routing" }
                        a.active href="gates" { "Gates" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

fn layout_schedules(title: &str, body: &Markup, refresh: Option<u64>) -> Markup {
    html! {
        (PreEscaped("<!DOCTYPE html>"))
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width,initial-scale=1";
                @if let Some(secs) = refresh {
                    meta http-equiv="refresh" content=(secs);
                }
                title { (title) }
                style { (PreEscaped(STYLE)) }
            }
            body {
                header {
                    h1 {
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a.active href="schedules" { "Schedules" }
                        a href="dead-letters" { "Dead Letters" }
                        a href="build-routing" { "Build Routing" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard — autumn-harvest" }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_encode_preserves_unreserved_and_encodes_space() {
        assert_eq!(url_encode("foo-bar.BAZ_1~"), "foo-bar.BAZ_1~");
        assert_eq!(url_encode("a b"), "a%20b");
        assert_eq!(url_encode("é"), "%C3%A9");
    }

    #[test]
    fn badge_class_buckets_known_and_unknown_states() {
        assert_eq!(badge_class("RUNNING"), "RUNNING");
        assert_eq!(badge_class("COMPLETED"), "COMPLETED");
        assert_eq!(badge_class("FAILED"), "FAILED");
        assert_eq!(badge_class("CANCELLED"), "CANCELLED");
        assert_eq!(badge_class("TERMINATED"), "TERMINATED");
        assert_eq!(badge_class("MYSTERY"), "UNKNOWN");
    }

    #[test]
    fn dead_letter_task_kind_label_recognizes_callback_rows() {
        // Issue #921 review: a completion-callback dead letter (issue #605)
        // used to render as "Unknown" instead of a recognized kind.
        assert_eq!(dead_letter_task_kind_label("CALLBACK"), "Callback");
        assert_eq!(dead_letter_task_kind_label("callback"), "Callback");
        assert_eq!(dead_letter_task_kind_label("ACTIVITY"), "Activity");
        assert_eq!(dead_letter_task_kind_label("WORKFLOW"), "Workflow");
        assert_eq!(dead_letter_task_kind_label("TIMER"), "Unknown");
    }

    #[test]
    fn event_label_disambiguates_terminate_from_cancel() {
        // The WorkflowCancelled event is reused for force-terminate (#504): the
        // timeline label must follow the authoritative execution state so a
        // terminated run reads "Workflow terminated", matching its badge.
        let data = serde_json::json!({});
        assert_eq!(
            event_human_label("WorkflowCancelled", &data, "TERMINATED"),
            "Workflow terminated"
        );
        assert_eq!(
            event_human_label("WorkflowCancelled", &data, "CANCELLED"),
            "Workflow cancelled"
        );
    }

    #[test]
    fn layout_escapes_title_but_keeps_body_markup() {
        let body = html! { p { "hello" } };
        let html = layout("<evil>", &body, "").into_string();
        assert!(html.contains("<title>&lt;evil&gt;</title>"));
        assert!(html.contains("<p>hello</p>"));
        assert!(html.contains("🔭 Vantage"));
    }

    #[test]
    fn build_query_string_omits_default_limit() {
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, None, None, None, None),
            ""
        );
        assert_eq!(
            build_query_string(10, None, None, None, None, None, None),
            "&limit=10"
        );
        assert_eq!(
            build_query_string(
                DEFAULT_PAGE_SIZE,
                Some("FAILED"),
                None,
                None,
                None,
                None,
                None
            ),
            "&state=FAILED"
        );
        assert_eq!(
            build_query_string(50, Some("with space"), None, None, None, None, None),
            "&limit=50&state=with%20space"
        );
    }

    #[test]
    fn build_query_string_includes_workflow_name_and_search_attrs() {
        assert_eq!(
            build_query_string(
                DEFAULT_PAGE_SIZE,
                None,
                Some("onboarding"),
                None,
                None,
                None,
                None
            ),
            "&workflow_name=onboarding"
        );
        let pair = ("tenant".to_string(), "acme".to_string());
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, Some(&pair), None, None, None),
            "&search_attr_key=tenant&search_attr_value=acme"
        );
    }

    #[test]
    fn dead_letter_bulk_actions_submit_explicit_limit_for_matching_rows() {
        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };

        let html = render_dead_letter_bulk_actions(&filters, DEFAULT_DLQ_PAGE_SIZE, None, 250)
            .into_string();

        assert!(html.contains("name=\"limit\" value=\"250\""));
        assert!(html.contains("Replay all matching (250)"));
        assert!(html.contains("Discard all matching (250)"));
        assert!(html.contains("Replay 250 matching dead-letter entries?"));
        assert!(html.contains("Discard 250 matching dead-letter entries?"));
    }

    #[test]
    fn dead_letter_bulk_actions_label_when_limited_by_api_cap() {
        let filters = DeadLetterUiFilters {
            workflow_name: Some("invoice_workflow".to_string()),
            ..DeadLetterUiFilters::default()
        };

        let html = render_dead_letter_bulk_actions(&filters, DEFAULT_DLQ_PAGE_SIZE, None, 1_200)
            .into_string();

        assert!(html.contains("name=\"limit\" value=\"1000\""));
        assert!(html.contains("Replay first 1000 matching (1200 total)"));
        assert!(html.contains("Discard first 1000 matching (1200 total)"));
        assert!(html.contains("Replay first 1000 of 1200 matching dead-letter entries?"));
        assert!(html.contains("Discard first 1000 of 1200 matching dead-letter entries?"));
    }

    #[test]
    fn state_badge_emits_class_and_label() {
        let html = state_badge("COMPLETED").into_string();
        assert!(html.contains("class=\"badge COMPLETED\""));
        assert!(html.contains(">COMPLETED<"));
    }

    #[test]
    fn json_card_escapes_quotes_in_payload() {
        let value = serde_json::json!({ "hello": "world" });
        let html = json_card("Input", &value).into_string();
        assert!(html.contains("&quot;hello&quot;"));
        assert!(html.contains("&quot;world&quot;"));
        assert!(!html.contains("<script"));
    }

    // -- Workers page pure-logic unit tests --

    fn fleet(total: usize, active: usize, stale: usize, errored: bool) -> WorkerFleetStats {
        WorkerFleetStats {
            total,
            active,
            draining: 0,
            stopped: total.saturating_sub(active),
            stale,
            any_shard_errored: errored,
        }
    }

    #[test]
    fn banner_healthy_when_active_no_stale() {
        let stats = fleet(2, 2, 0, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Healthy));
    }

    #[test]
    fn banner_degraded_when_stale_workers_exist() {
        let stats = fleet(3, 2, 1, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_unhealthy_when_no_active_workers() {
        let stats = fleet(2, 0, 0, false);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Unhealthy));
    }

    #[test]
    fn banner_none_when_empty_fleet_and_no_errors() {
        let stats = fleet(0, 0, 0, false);
        assert_eq!(determine_banner_state(&stats), None);
    }

    #[test]
    fn banner_degraded_when_shard_error_and_no_active_workers() {
        // Shard errored: state is partially unknown, so Degraded not Unhealthy.
        let stats = fleet(0, 0, 0, true);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_degraded_when_shard_errored_even_if_healthy_otherwise() {
        let stats = fleet(3, 3, 0, true);
        assert_eq!(determine_banner_state(&stats), Some(BannerState::Degraded));
    }

    #[test]
    fn banner_as_str_round_trips() {
        assert_eq!(BannerState::Healthy.as_str(), "Healthy");
        assert_eq!(BannerState::Degraded.as_str(), "Degraded");
        assert_eq!(BannerState::Unhealthy.as_str(), "Unhealthy");
    }

    #[test]
    fn relative_time_just_now_for_recent() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(2);
        assert_eq!(relative_time(ts), "just now");
    }

    #[test]
    fn relative_time_seconds_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(20);
        assert_eq!(relative_time(ts), "20s ago");
    }

    #[test]
    fn relative_time_minutes_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(90);
        assert_eq!(relative_time(ts), "1m ago");
    }

    #[test]
    fn relative_time_hours_ago() {
        let ts = chrono::Utc::now() - chrono::Duration::seconds(7200);
        assert_eq!(relative_time(ts), "2h ago");
    }

    #[test]
    fn build_worker_query_string_empty_defaults() {
        assert_eq!(
            build_worker_query_string(DEFAULT_PAGE_SIZE, None, None, false, None),
            ""
        );
    }

    #[test]
    fn build_worker_query_string_includes_all_params() {
        let q = build_worker_query_string(10, Some("Active"), Some(1), true, None);
        assert!(q.contains("limit=10"));
        assert!(q.contains("status=Active"));
        assert!(q.contains("shard=1"));
        assert!(q.contains("stale=true"));
    }

    #[test]
    fn layout_includes_workers_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("workers"),
            "layout must include a Workers nav link"
        );
    }

    #[test]
    fn worker_status_badge_shows_stale_annotation() {
        let html = worker_status_badge("Active", true).into_string();
        assert!(html.contains("stale"));
        assert!(html.contains("Active"));
    }

    #[test]
    fn worker_status_badge_no_annotation_when_healthy() {
        let html = worker_status_badge("Active", false).into_string();
        assert!(html.contains("Active"));
        assert!(!html.contains("stale"));
    }

    // -- Schedule page pure-logic unit tests --

    fn make_schedule(
        workflow_name: Option<&str>,
        dag_name: Option<&str>,
        is_paused: bool,
    ) -> HarvestSchedule {
        HarvestSchedule {
            id: uuid::Uuid::new_v4(),
            dag_name: dag_name.map(str::to_string),
            schedule_expr: Some("0 * * * *".to_string()),
            timezone: "UTC".to_string(),
            catchup: false,
            max_active_runs: 1,
            is_paused,
            last_run_at: None,
            next_run_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            workflow_name: workflow_name.map(str::to_string),
            workflow_input: None,
            queue_name: None,
            paused_at: None,
            paused_by: None,
            pause_reason: None,
            jitter_secs: 0,
            overlap_policy: "skip".to_string(),
            buffered_runs: serde_json::json!([]),
            buffer_all_max: 100,
            calendar_name: None,
            skip_policy: "skip".to_string(),
            fire_claim_token: None,
            fire_claimed_until: None,
            consecutive_failure_limit: None,
            consecutive_failure_count: 0,
            auto_paused_at: None,
            end_at: None,
            max_runs: None,
            runs_started: 0,
            exhausted_at: None,
            exhausted_reason: None,
            catchup_policy: None,
            catchup_window_secs: None,
            last_catchup_dropped: 0,
            last_catchup_at: None,
            retry_policy: None,
        }
    }

    #[test]
    fn schedule_filter_matches_all_when_empty() {
        let filters = ScheduleUiFilters::default();
        let wf = make_schedule(Some("my_workflow"), None, false);
        let dag = make_schedule(None, Some("my_dag"), true);
        assert!(filters.matches(ShardId::new(0), &wf));
        assert!(filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_kind_workflow_excludes_dags() {
        let filters = ScheduleUiFilters {
            kind: ScheduleKindFilter::Workflow,
            ..Default::default()
        };
        let wf = make_schedule(Some("wf"), None, false);
        let dag = make_schedule(None, Some("dag"), false);
        assert!(filters.matches(ShardId::new(0), &wf));
        assert!(!filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_kind_dag_excludes_workflows() {
        let filters = ScheduleUiFilters {
            kind: ScheduleKindFilter::Dag,
            ..Default::default()
        };
        let wf = make_schedule(Some("wf"), None, false);
        let dag = make_schedule(None, Some("dag"), false);
        assert!(!filters.matches(ShardId::new(0), &wf));
        assert!(filters.matches(ShardId::new(0), &dag));
    }

    #[test]
    fn schedule_filter_paused_excludes_active() {
        let filters = ScheduleUiFilters {
            paused: SchedulePausedFilter::Paused,
            ..Default::default()
        };
        let active = make_schedule(Some("wf"), None, false);
        let paused = make_schedule(Some("wf2"), None, true);
        assert!(!filters.matches(ShardId::new(0), &active));
        assert!(filters.matches(ShardId::new(0), &paused));
    }

    #[test]
    fn schedule_filter_active_excludes_paused() {
        let filters = ScheduleUiFilters {
            paused: SchedulePausedFilter::Active,
            ..Default::default()
        };
        let active = make_schedule(Some("wf"), None, false);
        let paused = make_schedule(Some("wf2"), None, true);
        assert!(filters.matches(ShardId::new(0), &active));
        assert!(!filters.matches(ShardId::new(0), &paused));
    }

    #[test]
    fn schedule_filter_target_substring_match() {
        let filters = ScheduleUiFilters {
            target: Some("payment".to_string()),
            ..Default::default()
        };
        let matching = make_schedule(Some("payment_workflow"), None, false);
        let other = make_schedule(Some("invoice_workflow"), None, false);
        assert!(filters.matches(ShardId::new(0), &matching));
        assert!(!filters.matches(ShardId::new(0), &other));
    }

    #[test]
    fn schedule_filter_target_case_insensitive() {
        let filters = ScheduleUiFilters {
            target: Some("PAYMENT".to_string()),
            ..Default::default()
        };
        let matching = make_schedule(Some("payment_workflow"), None, false);
        assert!(filters.matches(ShardId::new(0), &matching));
    }

    #[test]
    fn schedule_filter_shard_id_match() {
        let filters = ScheduleUiFilters {
            shard_id: Some(1),
            ..Default::default()
        };
        let row = make_schedule(Some("wf"), None, false);
        assert!(filters.matches(ShardId::new(1), &row));
        assert!(!filters.matches(ShardId::new(0), &row));
    }

    #[test]
    fn schedule_state_badge_paused() {
        let html = schedule_state_badge(true).into_string();
        assert!(html.contains("Paused"));
    }

    #[test]
    fn schedule_state_badge_active() {
        let html = schedule_state_badge(false).into_string();
        assert!(html.contains("Active"));
    }

    #[test]
    fn schedule_kind_distribution_empty() {
        assert_eq!(schedule_kind_distribution(&[]), "");
    }

    #[test]
    fn schedule_kind_distribution_workflow_only() {
        let row = make_schedule(Some("wf"), None, false);
        assert_eq!(
            schedule_kind_distribution(&[(ShardId::UNENCODED, row)]),
            "1 Workflow"
        );
    }

    #[test]
    fn schedule_kind_distribution_dag_only() {
        let row = make_schedule(None, Some("my_dag"), false);
        assert_eq!(
            schedule_kind_distribution(&[(ShardId::UNENCODED, row)]),
            "1 Dag"
        );
    }

    #[test]
    fn schedule_kind_distribution_mixed() {
        let wf = make_schedule(Some("wf"), None, false);
        let dag1 = make_schedule(None, Some("dag_a"), false);
        let dag2 = make_schedule(None, Some("dag_b"), false);
        let rows = vec![
            (ShardId::UNENCODED, wf),
            (ShardId::UNENCODED, dag1),
            (ShardId::UNENCODED, dag2),
        ];
        assert_eq!(schedule_kind_distribution(&rows), "1 Workflow, 2 Dag");
    }

    #[test]
    fn build_schedule_query_string_omits_defaults() {
        let filters = ScheduleUiFilters::default();
        assert_eq!(
            build_schedule_query_string(DEFAULT_SCHEDULE_PAGE_SIZE, &filters, None),
            ""
        );
    }

    #[test]
    fn build_schedule_query_string_includes_all_params() {
        let filters = ScheduleUiFilters {
            target: Some("payment".to_string()),
            kind: ScheduleKindFilter::Workflow,
            paused: SchedulePausedFilter::Paused,
            shard_id: Some(2),
        };
        let q = build_schedule_query_string(10, &filters, Some(30));
        assert!(q.contains("limit=10"), "missing limit: {q}");
        assert!(q.contains("target=payment"), "missing target: {q}");
        assert!(q.contains("kind=Workflow"), "missing kind: {q}");
        assert!(q.contains("paused=Paused"), "missing paused: {q}");
        assert!(q.contains("shard_id=2"), "missing shard_id: {q}");
        assert!(q.contains("refresh=30"), "missing refresh: {q}");
    }

    #[test]
    fn layout_schedules_has_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_schedules("Test", &body, None).into_string();
        assert!(
            html.contains("schedules"),
            "layout_schedules must include schedules link"
        );
        assert!(
            html.contains("Workflows"),
            "layout_schedules must include workflows link"
        );
        assert!(
            html.contains("Workers"),
            "layout_schedules must include workers link"
        );
    }

    #[test]
    fn layout_schedules_auto_refresh_tag() {
        let body = html! { p { "test" } };
        let html_with = layout_schedules("T", &body, Some(30)).into_string();
        assert!(html_with.contains("http-equiv=\"refresh\""));
        assert!(html_with.contains("content=\"30\""));
        let html_without = layout_schedules("T", &body, None).into_string();
        assert!(!html_without.contains("http-equiv=\"refresh\""));
    }

    #[test]
    fn layout_includes_schedules_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("schedules"),
            "layout must include schedules nav link"
        );
        assert!(html.contains("dags"), "layout must include dags nav link");
    }

    #[test]
    fn render_dag_list_includes_operational_columns() {
        let dags = vec![DagUiSummary {
            name: "payments".to_string(),
            schedule_expr: Some("0 * * * *".to_string()),
            task_count: 9,
            is_paused: true,
            next_run_at: None,
            max_active_runs: 3,
            catchup: false,
        }];
        let html = render_dag_list(&dags, &[]).into_string();
        assert!(html.contains("Paused"));
        assert!(html.contains("Next Run"));
        assert!(html.contains("Max Active"));
        assert!(html.contains("Catchup"));
        assert!(html.contains("payments"));
    }

    #[test]
    fn render_dag_list_surfaces_schedule_shard_errors() {
        let html = render_dag_list(&[], &[(ShardId::new(2), "connection refused".to_string())])
            .into_string();

        assert!(html.contains("Shard 2 unavailable"));
        assert!(html.contains("connection refused"));
    }

    #[test]
    fn dag_schedule_row_merge_preserves_runtime_formatted_expression() {
        let mut summary = DagUiSummary {
            name: "subsecond".to_string(),
            schedule_expr: Some("@every 0.500000000s".to_string()),
            task_count: 1,
            is_paused: false,
            next_run_at: None,
            max_active_runs: 1,
            catchup: false,
        };
        let mut row = make_schedule(None, Some("subsecond"), true);
        row.schedule_expr = Some("interval:0".to_string());
        row.max_active_runs = 3;
        row.catchup = true;

        merge_dag_schedule_row(&mut summary, &row);

        assert_eq!(
            summary.schedule_expr.as_deref(),
            Some("@every 0.500000000s")
        );
        assert!(summary.is_paused);
        assert_eq!(summary.max_active_runs, 3);
        assert!(summary.catchup);
    }

    #[test]
    fn dag_schedule_row_merge_uses_persisted_expression_when_runtime_has_none() {
        let mut summary = DagUiSummary {
            name: "manualish".to_string(),
            schedule_expr: None,
            task_count: 1,
            is_paused: false,
            next_run_at: None,
            max_active_runs: 1,
            catchup: false,
        };
        let row = make_schedule(None, Some("manualish"), false);

        merge_dag_schedule_row(&mut summary, &row);

        assert_eq!(summary.schedule_expr.as_deref(), Some("0 * * * *"));
    }

    #[test]
    fn layout_dag_detail_refresh_tag() {
        let body = html! { p { "x" } };
        let html = layout_dag_detail("D", &body, "", Some(30)).into_string();
        assert!(html.contains("http-equiv=\"refresh\""));
        assert!(html.contains("content=\"30\""));
    }

    #[test]
    fn render_dag_list_uses_dag_active_nav() {
        let dags = vec![];
        let html = render_dag_list(&dags, &[]).into_string();
        assert!(html.contains("class=\"active\" href=\"dags\""));
    }

    #[test]
    fn dag_run_selection_accepts_valid_requested_run_not_in_display_page() {
        let requested = uuid::Uuid::from_u128(1);
        let newest_listed = uuid::Uuid::from_u128(2);

        assert_eq!(
            select_dag_run_id(Some(requested), true, Some(newest_listed)),
            Some(requested)
        );
    }

    #[test]
    fn dag_run_selection_falls_back_when_requested_run_is_not_for_dag() {
        let requested = uuid::Uuid::from_u128(1);
        let newest_listed = uuid::Uuid::from_u128(2);

        assert_eq!(
            select_dag_run_id(Some(requested), false, Some(newest_listed)),
            Some(newest_listed)
        );
    }

    #[test]
    fn dag_run_shard_matches_router_dag_owner() {
        let router = autumn_harvest::ShardRouter::new(
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)],
            ShardId::new(0),
        );

        assert_eq!(
            dag_run_shard(&router, "daily_etl"),
            router.pick_for_dag("daily_etl")
        );
    }

    fn two_node_dag_with_downstream_rule(
        rule: autumn_harvest::policy::TriggerRule,
    ) -> (autumn_harvest::dag::DagDefinition, usize, usize) {
        fn upstream_for_ui_state_test() {}
        fn downstream_for_ui_state_test() {}

        let mut builder = autumn_harvest::dag::DagBuilder::new();
        let upstream = builder.activity(upstream_for_ui_state_test);
        let upstream_idx = upstream.index();
        let downstream = builder
            .activity(downstream_for_ui_state_test)
            .upstream(&upstream)
            .trigger_rule(rule);
        let downstream_idx = downstream.index();

        (
            builder.build().expect("test dag should compile"),
            upstream_idx,
            downstream_idx,
        )
    }

    fn out_of_order_two_node_dag() -> (autumn_harvest::dag::DagDefinition, usize, usize) {
        fn out_of_order_downstream_for_ui_state_test() {}
        fn out_of_order_upstream_for_ui_state_test() {}

        let mut builder = autumn_harvest::dag::DagBuilder::new();
        let downstream = builder
            .activity(out_of_order_downstream_for_ui_state_test)
            .trigger_rule(autumn_harvest::policy::TriggerRule::AllSuccess);
        let downstream_idx = downstream.index();
        let upstream = builder.activity(out_of_order_upstream_for_ui_state_test);
        let upstream_idx = upstream.index();
        let _downstream = downstream.upstream(&upstream);

        (
            builder.build().expect("test dag should compile"),
            upstream_idx,
            downstream_idx,
        )
    }

    fn task_queue_item_for_activity(activity_name: &str, state: &str) -> TaskQueueItem {
        let now = Utc::now();
        TaskQueueItem {
            id: uuid::Uuid::new_v4(),
            queue_name: "default".to_string(),
            task_type: "ACTIVITY".to_string(),
            workflow_exec_id: Some(uuid::Uuid::new_v4()),
            activity_name: Some(activity_name.to_string()),
            activity_id: None,
            input: Value::Null,
            state: state.to_string(),
            priority: 0,
            worker_id: None,
            attempt: 0,
            max_attempts: 1,
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            last_heartbeat_at: None,
            heartbeat_details: None,
            heartbeat_timeout: None,
            start_to_close: None,
            schedule_to_start: None,
            retry_policy: None,
            output: None,
            error: None,
            sticky_worker_id: None,
            sticky_until: None,
            sticky_timeout: None,
            trace_context: None,
            concurrency_key: None,
            concurrency_cap: None,
            required_build_id: None,
            rate_limit_key: None,
            crash_strikes: 0,
            schedule_to_close_at: None,
            required_capabilities: None,
            context_headers: None,
            created_at: Some(now),
            wake_requested: false,
            session_id: None,
        }
    }

    #[test]
    fn dag_node_state_infers_skipped_when_trigger_rule_blocks_unscheduled_node() {
        let (dag, upstream_idx, downstream_idx) =
            two_node_dag_with_downstream_rule(autumn_harvest::policy::TriggerRule::AllSuccess);
        let known_states = HashMap::from([(upstream_idx, DagNodeState::Failed)]);

        assert_eq!(
            infer_skipped_node_state(&dag, downstream_idx, &known_states),
            Some(DagNodeState::Skipped)
        );
    }

    #[test]
    fn dag_node_state_infers_skipped_when_upstream_declared_after_downstream() {
        let (dag, upstream_idx, downstream_idx) = out_of_order_two_node_dag();
        assert_eq!(
            dag.execution_levels(),
            &[vec![upstream_idx], vec![downstream_idx]]
        );

        let task_rows = vec![task_queue_item_for_activity(
            dag.tasks()[upstream_idx].activity_name.as_str(),
            "FAILED",
        )];
        let states = map_node_states(&dag, &task_rows, &std::collections::HashSet::new());

        assert_eq!(states.get(&downstream_idx), Some(&DagNodeState::Skipped));
    }

    #[test]
    fn dag_node_state_stays_unknown_until_upstreams_are_terminal() {
        let (dag, _, downstream_idx) =
            two_node_dag_with_downstream_rule(autumn_harvest::policy::TriggerRule::AllSuccess);

        assert_eq!(
            infer_skipped_node_state(&dag, downstream_idx, &HashMap::new()),
            None
        );
    }

    #[test]
    fn dag_task_state_cancelled_is_terminal() {
        assert_eq!(
            merge_dag_task_state(DagNodeState::Unknown, "CANCELLED"),
            DagNodeState::Cancelled
        );
    }

    #[test]
    fn format_run_duration_renders_elapsed_time() {
        let start = Utc::now();
        let end = start + chrono::Duration::seconds(125);
        assert_eq!(format_run_duration(start, Some(end)), "2m 5s");
    }

    #[test]
    fn schedule_expr_preserves_subsecond_interval() {
        let expr = schedule_expr_for_ui_summary(&Schedule::Interval(
            std::time::Duration::from_millis(500),
        ));
        assert_eq!(expr, "@every 0.500000000s");
    }

    #[test]
    fn schedule_kind_filter_parse_roundtrips() {
        assert!(matches!(
            ScheduleKindFilter::parse("").unwrap(),
            ScheduleKindFilter::All
        ));
        assert!(matches!(
            ScheduleKindFilter::parse("Workflow").unwrap(),
            ScheduleKindFilter::Workflow
        ));
        assert!(matches!(
            ScheduleKindFilter::parse("Dag").unwrap(),
            ScheduleKindFilter::Dag
        ));
        assert!(ScheduleKindFilter::parse("bogus").is_err());
    }

    #[test]
    fn schedule_paused_filter_parse_roundtrips() {
        assert!(matches!(
            SchedulePausedFilter::parse("").unwrap(),
            SchedulePausedFilter::All
        ));
        assert!(matches!(
            SchedulePausedFilter::parse("Paused").unwrap(),
            SchedulePausedFilter::Paused
        ));
        assert!(matches!(
            SchedulePausedFilter::parse("Active").unwrap(),
            SchedulePausedFilter::Active
        ));
        assert!(SchedulePausedFilter::parse("maybe").is_err());
    }

    // -- Issue #279: history event count and continue-as-new threshold --

    fn stub_execution() -> autumn_harvest::models::WorkflowExecution {
        use chrono::Utc;
        use uuid::Uuid;
        autumn_harvest::models::WorkflowExecution {
            id: Uuid::new_v4(),
            workflow_name: "test_workflow".to_string(),
            workflow_id: "wf-1".to_string(),
            run_id: Uuid::new_v4(),
            shard_id: 0,
            state: "RUNNING".to_string(),
            input: serde_json::json!(null),
            output: None,
            error: None,
            parent_id: None,
            sticky_worker_id: None,
            queue_name: "default".to_string(),
            started_at: Utc::now(),
            completed_at: None,
            execution_timeout: None,
            deadline_at: None,
            memo: None,
            search_attrs: None,
            created_at: Utc::now(),
            assigned_build_id: None,
            parent_close_policy: None,
            owner: None,
            runbook_url: None,
            severity: None,
            context_headers: None,
            sla: None,
            sla_deadline_at: None,
            sla_breached: false,
            sla_breached_at: None,
            paused_at: None,
            pause_reason: None,
            pause_actor: None,
            current_details: None,
            schedule_id: None,
            scheduled_for: None,
            workflow_attempt: 1,
            workflow_retry_policy: None,
            retry_of_exec_id: None,
            origin: None,
            nd_blocked_at: None,
            nd_block_reason: None,
            nd_block_count: 0,
            completion_callbacks: None,
        }
    }

    fn stub_blocked_on() -> BlockedOnData {
        BlockedOnData {
            activities: vec![],
            external_tasks: vec![],
            timers: vec![],
            signals: vec![],
            heartbeat_details_cap: 0,
            heartbeat_caps: std::collections::HashMap::new(),
        }
    }

    // ── Issue #608 / PR #936 round 5: decode only rendered detail fields ─────

    /// Reversing test codec so an envelope is distinguishable from plaintext.
    #[derive(Debug)]
    struct ReverseUiCodec;

    impl autumn_harvest::payload_codec::PayloadCodec for ReverseUiCodec {
        fn codec_id(&self) -> &'static str {
            "reverse"
        }
        fn encode(&self, raw: &[u8]) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
            let mut v = raw.to_vec();
            v.reverse();
            Ok(v)
        }
        fn decode(
            &self,
            encoded: &[u8],
        ) -> Result<Vec<u8>, autumn_harvest::payload_codec::CodecError> {
            let mut v = encoded.to_vec();
            v.reverse();
            Ok(v)
        }
    }

    fn ui_test_codecs() -> PayloadCodecs {
        let mut codecs = PayloadCodecs::default();
        codecs.set_default(Arc::new(ReverseUiCodec));
        codecs
    }

    /// Builds a well-formed `reverse` codec envelope for `plain` via the
    /// public `encode_event` round-trip (mirrors the integration fixture).
    fn ui_envelope(plain: &Value) -> Value {
        let event = autumn_harvest::WorkflowEvent::WorkflowCompleted {
            output: plain.clone(),
        };
        let encoded = ui_test_codecs().encode_event(&event).expect("encode event");
        encoded["data"]["output"].clone()
    }

    fn stub_timeline_event(event_data: Value) -> HarvestEvent {
        HarvestEvent {
            id: 1,
            workflow_exec_id: uuid::Uuid::new_v4(),
            event_id: 0,
            event_type: "ActivityScheduled".to_string(),
            event_data,
            timestamp: Utc::now(),
        }
    }

    fn stub_pending_signal(payload: Value) -> HarvestSignal {
        HarvestSignal {
            id: uuid::Uuid::new_v4(),
            workflow_exec_id: uuid::Uuid::new_v4(),
            signal_name: "approval".to_string(),
            payload,
            received_at: Utc::now(),
            consumed: false,
            idempotency_key: None,
        }
    }

    /// PR #936 round 5: the pending-activity `input` and pending-signal
    /// payloads are never rendered by the detail page, so they must not be
    /// decoded and must not count toward the `payload.decode_read` audit
    /// outcome — envelopes there stay untouched and the outcome stays empty
    /// (no audit row would be written for a page whose only envelopes are
    /// hidden).
    #[test]
    fn hidden_detail_fields_are_not_decoded_and_never_touch_the_audit_outcome() {
        let codecs = ui_test_codecs();
        let mut execution = stub_execution();
        let mut timeline: Vec<HarvestEvent> = vec![];

        let input_envelope = ui_envelope(&serde_json::json!({"card": "pii-task-input"}));
        let signal_envelope = ui_envelope(&serde_json::json!({"approver": "pii-signal"}));
        let mut task = task_queue_item_for_activity("charge_card", "PENDING");
        task.input = input_envelope.clone();
        let mut blocked_on = stub_blocked_on();
        blocked_on.activities.push(task);
        blocked_on
            .signals
            .push(stub_pending_signal(signal_envelope.clone()));

        let outcome = decode_workflow_detail_rendered_fields(
            &codecs,
            &mut execution,
            &mut timeline,
            &mut blocked_on,
        );

        assert_eq!(outcome.decoded, 0, "hidden fields must not be decoded");
        assert_eq!(outcome.failed, 0, "hidden fields must not be marked");
        assert!(
            !outcome.touched(),
            "a page whose only envelopes are hidden must not write an audit row"
        );
        assert_eq!(
            blocked_on.activities[0].input, input_envelope,
            "the pending-activity input must keep its stored envelope"
        );
        assert_eq!(
            blocked_on.signals[0].payload, signal_envelope,
            "the pending-signal payload must keep its stored envelope"
        );
    }

    /// The fields the detail page actually renders — execution payload
    /// fields, timeline event payloads, and heartbeat checkpoints — are
    /// decoded, and the audit outcome counts exactly those.
    #[test]
    fn rendered_detail_fields_are_decoded_and_counted_exactly() {
        let codecs = ui_test_codecs();
        let mut execution = stub_execution();
        execution.input = ui_envelope(&serde_json::json!({"user": "pii-exec-input"}));
        let mut timeline = vec![stub_timeline_event(serde_json::json!({
            "type": "ActivityScheduled",
            "data": { "input": ui_envelope(&serde_json::json!({"card": "pii-event"})) },
        }))];

        let checkpoint_envelope = ui_envelope(&serde_json::json!({"progress": "pii-checkpoint"}));
        let input_envelope = ui_envelope(&serde_json::json!({"card": "pii-task-input"}));
        let mut task = task_queue_item_for_activity("charge_card", "RUNNING");
        task.input = input_envelope.clone();
        task.heartbeat_details = Some(checkpoint_envelope);
        let mut blocked_on = stub_blocked_on();
        blocked_on.activities.push(task);

        let outcome = decode_workflow_detail_rendered_fields(
            &codecs,
            &mut execution,
            &mut timeline,
            &mut blocked_on,
        );

        assert_eq!(
            (outcome.decoded, outcome.failed),
            (3, 0),
            "exactly the three surfaced envelopes are decoded"
        );
        assert_eq!(
            execution.input,
            serde_json::json!({"user": "pii-exec-input"}),
            "the rendered execution input must be decoded"
        );
        assert_eq!(
            timeline[0].event_data["data"]["input"],
            serde_json::json!({"card": "pii-event"}),
            "the rendered timeline event payload must be decoded"
        );
        assert_eq!(
            blocked_on.activities[0].heartbeat_details,
            Some(serde_json::json!({"progress": "pii-checkpoint"})),
            "the rendered heartbeat checkpoint must be decoded"
        );
        assert_eq!(
            blocked_on.activities[0].input, input_envelope,
            "the hidden pending-activity input must stay an envelope even when \
             its sibling checkpoint is decoded"
        );
    }

    #[test]
    fn render_detail_shows_history_event_count_with_threshold() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            42,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            Some(10_000),
        )
        .into_string();

        assert!(
            html.contains("History events"),
            "metadata card must have a 'History events' label"
        );
        assert!(
            html.contains("42"),
            "metadata card must show the event count 42"
        );
        assert!(
            html.contains("10000") || html.contains("10,000"),
            "metadata card must show the threshold 10000"
        );
    }

    #[test]
    fn render_detail_shows_threshold_absent_when_none() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            5,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            None,
        )
        .into_string();

        assert!(
            html.contains("History events"),
            "label should still appear when threshold is None"
        );
        assert!(
            html.contains('5'),
            "event count 5 should appear even without threshold"
        );
    }

    #[test]
    fn render_detail_shows_custom_threshold() {
        let execution = stub_execution();
        let blocked = stub_blocked_on();
        let html = render_workflow_detail(
            &execution,
            300,
            &[],
            &[],
            &[],
            false,
            &[],
            0,
            &blocked,
            None,
            Some(500),
        )
        .into_string();

        assert!(html.contains("300"), "event count 300 must appear in HTML");
        assert!(
            html.contains("500"),
            "custom threshold 500 must appear in HTML"
        );
    }

    // ── Build Routing page unit tests (issue #362 — Red Phase) ─────────────

    #[test]
    fn layout_build_routing_has_active_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_build_routing("Build Routing · Vantage", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_build_routing must include the build-routing nav link"
        );
        // The active link must be present
        assert!(
            html.contains("Build Routing"),
            "layout_build_routing must show 'Build Routing' label"
        );
    }

    #[test]
    fn layout_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout("Test", &body, "").into_string();
        assert!(
            html.contains("build-routing"),
            "base layout must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_workers_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_workers("Test", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_workers must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_dead_letters_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_dead_letters("Test", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_dead_letters must include a Build Routing nav link"
        );
    }

    #[test]
    fn layout_schedules_includes_build_routing_nav_link() {
        let body = html! { p { "test" } };
        let html = layout_schedules("Test", &body, None).into_string();
        assert!(
            html.contains("build-routing"),
            "layout_schedules must include a Build Routing nav link"
        );
    }

    #[test]
    fn render_build_routing_page_empty_state_shows_docs_link() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("No build routing configured") || html.contains("No build policies"),
            "empty state must show a 'no policies' message"
        );
        assert!(
            html.contains("safe-deploy"),
            "empty state must link to safe-deploy runbook"
        );
    }

    #[test]
    fn render_build_routing_page_shows_policy_details() {
        let policy = BuildPolicy {
            id: uuid::Uuid::new_v4(),
            queue_name: "test-queue".to_string(),
            build_id: "abc123".to_string(),
            deployment_name: Some("prod-v2".to_string()),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            target_build_id: None,
            ramp_percent: None,
        };
        let html = render_build_routing_page(&[policy], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(html.contains("test-queue"), "must show queue name");
        assert!(html.contains("abc123"), "must show build_id");
        assert!(html.contains("prod-v2"), "must show deployment name");
    }

    #[test]
    fn render_build_routing_page_shows_reachability() {
        let reach = BuildReachability {
            build_id: "sha-old".to_string(),
            open_executions: 42,
            pending_tasks: 5,
            active_workers: 2,
            stale_workers: 1,
            safe_to_retire: false,
        };
        let html = render_build_routing_page(&[], &[], &[reach], &[], &[], &[], false, None, None)
            .into_string();
        assert!(html.contains("sha-old"), "must show build_id");
        assert!(html.contains("42"), "must show open_executions count");
        assert!(
            html.contains("In use"),
            "non-safe build must show In use status"
        );
    }

    #[test]
    fn render_build_routing_page_retire_enabled_when_safe() {
        let reach = BuildReachability {
            build_id: "sha-done".to_string(),
            open_executions: 0,
            pending_tasks: 0,
            active_workers: 0,
            stale_workers: 0,
            safe_to_retire: true,
        };
        let html = render_build_routing_page(&[], &[], &[reach], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("Retire"),
            "retire button must appear when safe_to_retire"
        );
        assert!(
            html.contains("Safe to retire"),
            "status must show Safe to retire"
        );
    }

    #[test]
    fn render_build_routing_page_shows_compat_entries() {
        let entry = BuildCompatEntry {
            id: uuid::Uuid::new_v4(),
            build_id: "sha-new".to_string(),
            compatible_with: "sha-old".to_string(),
            declared_at: chrono::Utc::now(),
        };
        let html = render_build_routing_page(&[], &[entry], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("sha-new"),
            "must show worker build in compat table"
        );
        assert!(
            html.contains("sha-old"),
            "must show compatible_with in compat table"
        );
        assert!(
            html.contains("Revoke"),
            "must show revoke button for each entry"
        );
    }

    #[test]
    fn render_build_routing_page_flash_message_shown() {
        let html = render_build_routing_page(
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
            false,
            Some("Policy updated"),
            None,
        )
        .into_string();
        assert!(
            html.contains("Policy updated"),
            "flash message must appear on page"
        );
    }

    #[test]
    fn render_build_routing_page_has_set_policy_form() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("set-policy"),
            "page must include Set Policy form action"
        );
        assert!(
            html.contains("queue_name"),
            "Set Policy form must include queue_name field"
        );
        assert!(
            html.contains("build_id"),
            "Set Policy form must include build_id field"
        );
    }

    #[test]
    fn render_build_routing_page_has_declare_compat_form() {
        let html = render_build_routing_page(&[], &[], &[], &[], &[], &[], false, None, None)
            .into_string();
        assert!(
            html.contains("declare-compat"),
            "page must include Declare Compat form action"
        );
        assert!(
            html.contains("compatible_with"),
            "Declare Compat form must include compatible_with field"
        );
    }

    #[test]
    fn render_worker_table_includes_build_id_column() {
        let html = render_worker_table(&[], ShardId::new(0)).into_string();
        assert!(
            html.contains("Build ID"),
            "worker table header must include Build ID column"
        );
        assert!(
            html.contains("Deployment"),
            "worker table header must include Deployment column"
        );
    }

    #[test]
    fn render_worker_filters_includes_build_id_filter() {
        let html = render_worker_filters(None, None, false, None, DEFAULT_PAGE_SIZE).into_string();
        assert!(
            html.contains("build_id"),
            "worker filters must include build_id input"
        );
    }

    #[test]
    fn build_worker_query_string_includes_build_id() {
        let q = build_worker_query_string(DEFAULT_PAGE_SIZE, None, None, false, Some("abc123"));
        assert!(
            q.contains("build_id=abc123"),
            "query string must include build_id"
        );
    }

    // ── Issue #482 — DagNodeState label and condition-skip inference ──────────

    #[test]
    fn dag_node_state_label_distinguishes_skip_variants() {
        assert_eq!(
            dag_node_state_label(DagNodeState::Skipped),
            "Skipped (upstream)"
        );
        assert_eq!(
            dag_node_state_label(DagNodeState::SkippedByCondition),
            "Skipped (condition)"
        );
        assert_eq!(dag_node_state_label(DagNodeState::Succeeded), "Succeeded");
        assert_eq!(dag_node_state_label(DagNodeState::Unknown), "Unknown");
    }

    #[test]
    fn parse_dag_skip_marker_index_matches_prefix() {
        assert_eq!(parse_dag_skip_marker_index("dag_skip:3"), Some(3));
        assert_eq!(parse_dag_skip_marker_index("dag_skip:0"), Some(0));
        assert_eq!(parse_dag_skip_marker_index("dag_skip:42"), Some(42));
        assert_eq!(parse_dag_skip_marker_index("fan_out:3"), None);
        assert_eq!(parse_dag_skip_marker_index("dag_skip:"), None);
        assert_eq!(parse_dag_skip_marker_index("dag_skip:abc"), None);
    }

    #[test]
    fn dag_node_terminal_status_treats_both_skip_variants_as_skipped() {
        assert_eq!(
            dag_node_terminal_status(DagNodeState::Skipped),
            Some(autumn_harvest::policy::TaskStatus::Skipped),
        );
        assert_eq!(
            dag_node_terminal_status(DagNodeState::SkippedByCondition),
            Some(autumn_harvest::policy::TaskStatus::Skipped),
        );
    }

    #[test]
    fn map_node_states_seeds_condition_skipped_nodes() {
        use autumn_harvest::DagBuilder;

        fn dummy() {}
        fn dummy2() {}

        let mut builder = DagBuilder::new();
        let a = builder.activity(dummy);
        let _b = builder.activity(dummy2).upstream(&a);
        let dag = builder.build().unwrap();

        let mut condition_skipped = std::collections::HashSet::new();
        condition_skipped.insert(1usize); // task idx 1 condition-skipped

        let states = map_node_states(&dag, &[], &condition_skipped);
        assert_eq!(
            states.get(&1),
            Some(&DagNodeState::SkippedByCondition),
            "condition-skipped node must be SkippedByCondition"
        );
        // SkippedByCondition is still Skipped for terminal status → AllSuccess
        // downstream stays Skipped (trigger-rule inference), not SkippedByCondition.
        assert_eq!(
            states.get(&0),
            Some(&DagNodeState::Unknown),
            "root with no task rows should remain Unknown"
        );
    }
}
