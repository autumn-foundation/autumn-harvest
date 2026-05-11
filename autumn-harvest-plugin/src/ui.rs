//! Vantage — an embedded, read-only HTML dashboard for Harvest workflows.
//!
//! Mounts alongside the management API (e.g. `/api/harvest/ui`). Renders a
//! paginated workflow list and a per-workflow detail page showing inputs,
//! outputs, and the full event history. Assets are inlined so the dashboard
//! works in network-restricted environments.

use std::fmt::Write as _;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::extract::{Path, Query};
use autumn_web::reexports::axum;
use axum::Extension;
use axum::Router;
use axum::routing::get;
use chrono::{DateTime, Utc};
use diesel::dsl::sql;
use diesel::sql_types::{Bool, Text};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, SelectableHelper};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;

use autumn_harvest::error::{HarvestError, HarvestResult, database_error};
use autumn_harvest::models::{DeadLetter, HarvestEvent, WorkflowExecution};
use autumn_harvest::schema::{harvest_dead_letters, harvest_events, harvest_workflow_executions};
use autumn_harvest::store;
use autumn_harvest::types::ShardId;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, list_workers};

use crate::api::{
    HarvestApiState, KNOWN_WORKFLOW_STATES, WorkflowFilters, acquire_conn, db_conn_for_execution,
    load_execution, load_workflows_from_shards, map_error, parse_execution_id,
};

const DEFAULT_PAGE_SIZE: i64 = 25;
const DEFAULT_DLQ_PAGE_SIZE: i64 = 50;
const MAX_PAGE_SIZE: i64 = 200;
const DLQ_BULK_ACTION_LIMIT: usize = autumn_harvest::dlq::MAX_BULK_LIMIT as usize;

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
.banner{padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px;font-weight:500}
.banner.Healthy{background:#14532d;color:#bbf7d0;border:1px solid #166534}
.banner.Degraded{background:#431407;color:#fed7aa;border:1px solid #92400e}
.banner.Unhealthy{background:#450a0a;color:#fecaca;border:1px solid #991b1b}
.flash{background:#172554;color:#bfdbfe;border:1px solid #1d4ed8;padding:10px 14px;border-radius:6px;margin-bottom:16px;font-size:13px}
.shard-header{margin:20px 0 8px;font-size:13px;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1e293b;padding-bottom:6px}
.shard-error{background:#1c1917;border:1px solid #57534e;border-radius:6px;padding:12px 16px;color:#a8a29e;font-size:13px;margin-bottom:12px}
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
    /// Auto-refresh interval in seconds (emits a `<meta http-equiv="refresh">` tag).
    #[serde(default)]
    refresh: Option<u64>,
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
    Router::new()
        .route("/", get(index))
        .route("/workflows", get(list_workflows_ui))
        .route("/workflows/{id}", get(workflow_detail_ui))
        .route("/workers", get(list_workers_ui))
        .route("/dead-letters", get(list_dead_letters_ui))
        .layer(Extension(api_state))
}

async fn index() -> axum::response::Redirect {
    axum::response::Redirect::to("workflows")
}

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

    let workflows = load_workflows_from_shards(&api_state, &filters).await?;
    let limit_usize = usize::try_from(limit).unwrap_or(usize::MAX);
    let offset_usize = usize::try_from(offset).unwrap_or(usize::MAX);
    let has_next = workflows.len() > offset_usize.saturating_add(limit_usize);
    let workflows = workflows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

    Ok(render_workflow_list(
        &workflows,
        page,
        limit,
        has_next,
        state_filter.as_deref(),
        workflow_name_filter.as_deref(),
        search_attr_pair.as_ref(),
    ))
}

async fn workflow_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Markup, AutumnError> {
    let exec_id = parse_execution_id(&id)?;
    let mut conn = db_conn_for_execution(&api_state, exec_id).await?;
    let execution = load_execution(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let history = store::load_history(&mut conn, exec_id)
        .await
        .map_err(map_error)?;
    let events = history
        .events
        .into_iter()
        .map(|event| serde_json::to_value(event).map_err(HarvestError::from))
        .collect::<HarvestResult<Vec<_>>>()
        .map_err(map_error)?;

    Ok(render_workflow_detail(&execution, &events))
}

// ---------------------------------------------------------------------------
// Dead-letter UI
// ---------------------------------------------------------------------------

async fn list_dead_letters_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Query(params): Query<DeadLetterListParams>,
) -> Result<Markup, AutumnError> {
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
    let page_rows = all_rows
        .into_iter()
        .skip(offset_usize)
        .take(limit_usize)
        .collect::<Vec<_>>();

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
                        a href="workflows" { "ðŸ”­ Vantage" }
                        span.subtitle { "Harvest dashboard" }
                    }
                    nav {
                        a href="workflows" { "Workflows" }
                        a href="workers" { "Workers" }
                        a.active href="dead-letters" { "Dead Letters" }
                    }
                }
                main { (body) }
                footer { "Operational dashboard â€” autumn-harvest" }
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
    refresh: Option<u64>,
) -> Markup {
    let total_workers: usize = grouped.iter().map(|(_, rows)| rows.len()).sum();

    let body = html! {
        h2 { "Workers" }

        // Fleet health banner
        (render_fleet_banner(stats, banner_state))

        // Filters
        (render_worker_filters(status_filter, shard_filter, stale_only, limit))

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

        (render_worker_pagination(page, limit, has_next, status_filter, shard_filter, stale_only))
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
    limit: i64,
) -> Markup {
    let shard_value = shard_filter.map(|s| s.to_string()).unwrap_or_default();
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
) -> Markup {
    let base = build_worker_query_string(limit, status_filter, shard_filter, stale_only);
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
) -> String {
    let mut out = String::new();
    if limit != DEFAULT_PAGE_SIZE {
        let _ = write!(out, "&limit={limit}");
    }
    if let Some(status) = status_filter {
        let _ = write!(out, "&status={}", url_encode(status));
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
                        a href="dead-letters" { "Dead Letters" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
            }
        }
    }
}

fn render_workflow_list(
    workflows: &[WorkflowExecution],
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
) -> Markup {
    let body = html! {
        h2 { "Workflows" }
        (render_filters(state_filter, workflow_name_filter, search_attr_filter, limit))

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

        (render_pagination(page, limit, has_next, state_filter, workflow_name_filter, search_attr_filter))
    };

    layout("Workflows · Vantage", &body, "")
}

fn render_filters(
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    limit: i64,
) -> Markup {
    let (attr_key, attr_value) =
        search_attr_filter.map_or(("", ""), |(k, v)| (k.as_str(), v.as_str()));
    let workflow_name_value = workflow_name_filter.unwrap_or("");

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

fn render_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
) -> Markup {
    let base_query = build_query_string(
        limit,
        state_filter,
        workflow_name_filter,
        search_attr_filter,
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

fn build_query_string(
    limit: i64,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
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
    out
}

fn render_workflow_detail(execution: &WorkflowExecution, events: &[Value]) -> Markup {
    let title = format!("{} · Vantage", execution.workflow_name);
    let detail_badge_class = format!("badge {}", badge_class(&execution.state));
    let body = html! {
        div.detail-row { a.back href="../workflows" { (PreEscaped("&larr;")) " Back to workflows" } }

        h2 {
            (execution.workflow_name) " "
            span class=(detail_badge_class) { (execution.state) }
        }

        @if let Some(error) = execution.error.as_deref() {
            div."error-banner" {
                strong { "Error:" } " " (error)
            }
        }

        div.card {
            h3 { "Metadata" }
            div.kv {
                (kv("Execution ID", &execution.id.to_string(), true))
                (kv("Workflow ID", &execution.workflow_id, true))
                (kv("Run ID", &execution.run_id.to_string(), true))
                (kv("Shard ID", &execution.shard_id.to_string(), true))
                (kv("Queue", &execution.queue_name, true))
                (kv("Started", &format_timestamp(Some(execution.started_at)), false))
                (kv("Completed", &format_timestamp(execution.completed_at), false))
                @if let Some(parent) = execution.parent_id {
                    (kv("Parent", &parent.to_string(), true))
                }
                @if let Some(worker) = execution.sticky_worker_id.as_deref() {
                    (kv("Sticky worker", worker, true))
                }
                @if let Some(timeout) = execution.execution_timeout {
                    (kv("Execution timeout", &format!("{}s", timeout.num_seconds()), false))
                }
            }
        }

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

        div.card {
            h3 { "Event history" }
            @if events.is_empty() {
                div.empty { "No events recorded yet." }
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
                        @for (index, event) in events.iter().enumerate() {
                            @let event_type = event.get("type").and_then(Value::as_str).unwrap_or("<unknown>");
                            @let timestamp = event.get("timestamp").and_then(Value::as_str).unwrap_or("—");
                            @let data_pretty = event.get("data").map_or_else(|| "{}".to_string(), pretty_json);
                            tr {
                                td { (index + 1) }
                                td { code { (event_type) } }
                                td { (timestamp) }
                                td {
                                    details {
                                        summary { "view payload" }
                                        pre { (data_pretty) }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    layout(&title, &body, "../")
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
    html! {
        span class=(class) { (state) }
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
                        a href={ (base_href) "workers" } { "Workers" }
                        a href={ (base_href) "dead-letters" } { "Dead Letters" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest" }
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
    fn layout_escapes_title_but_keeps_body_markup() {
        let body = html! { p { "hello" } };
        let html = layout("<evil>", &body, "").into_string();
        assert!(html.contains("<title>&lt;evil&gt;</title>"));
        assert!(html.contains("<p>hello</p>"));
        assert!(html.contains("🔭 Vantage"));
    }

    #[test]
    fn build_query_string_omits_default_limit() {
        assert_eq!(build_query_string(DEFAULT_PAGE_SIZE, None, None, None), "");
        assert_eq!(build_query_string(10, None, None, None), "&limit=10");
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, Some("FAILED"), None, None),
            "&state=FAILED"
        );
        assert_eq!(
            build_query_string(50, Some("with space"), None, None),
            "&limit=50&state=with%20space"
        );
    }

    #[test]
    fn build_query_string_includes_workflow_name_and_search_attrs() {
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, Some("onboarding"), None),
            "&workflow_name=onboarding"
        );
        let pair = ("tenant".to_string(), "acme".to_string());
        assert_eq!(
            build_query_string(DEFAULT_PAGE_SIZE, None, None, Some(&pair)),
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
            build_worker_query_string(DEFAULT_PAGE_SIZE, None, None, false),
            ""
        );
    }

    #[test]
    fn build_worker_query_string_includes_all_params() {
        let q = build_worker_query_string(10, Some("Active"), Some(1), true);
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
}
