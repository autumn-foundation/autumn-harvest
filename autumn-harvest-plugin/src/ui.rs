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
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;

use autumn_harvest::error::{HarvestError, HarvestResult};
use autumn_harvest::models::WorkflowExecution;
use autumn_harvest::store;
use autumn_harvest::types::ShardId;
use autumn_harvest::workers::{WorkerFilters, WorkerHealth, WorkerRow, list_workers};

use crate::api::{
    HarvestApiState, KNOWN_WORKFLOW_STATES, WorkflowFilters, acquire_conn, db_conn_for_execution,
    load_execution, load_workflows_from_shards, map_error, parse_execution_id,
};

const DEFAULT_PAGE_SIZE: i64 = 25;
const MAX_PAGE_SIZE: i64 = 200;

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
.badge.UNKNOWN{background:#334155;color:#e2e8f0}
.badge.Active{background:#166534;color:#dcfce7}
.badge.Draining{background:#92400e;color:#fef3c7}
.badge.Stopped{background:#334155;color:#e2e8f0}
.banner{padding:12px 16px;border-radius:8px;margin-bottom:20px;font-size:13px;font-weight:500}
.banner.Healthy{background:#14532d;color:#bbf7d0;border:1px solid #166534}
.banner.Degraded{background:#431407;color:#fed7aa;border:1px solid #92400e}
.banner.Unhealthy{background:#450a0a;color:#fecaca;border:1px solid #991b1b}
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

// ---------------------------------------------------------------------------
// Per-shard query result (Ok = rows, Err = error message)
// ---------------------------------------------------------------------------

type ShardWorkerResult = (ShardId, Result<Vec<WorkerRow>, String>);

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
