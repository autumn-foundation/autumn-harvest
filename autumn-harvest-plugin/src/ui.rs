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

use crate::api::{
    HarvestApiState, KNOWN_WORKFLOW_STATES, WorkflowFilters, db_conn_for_execution, load_execution,
    load_workflows_from_shards, map_error, parse_execution_id,
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
td code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;color:#cbd5e1}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;letter-spacing:.03em}
.badge.RUNNING{background:#1d4ed8;color:#dbeafe}
.badge.COMPLETED{background:#166534;color:#dcfce7}
.badge.FAILED{background:#991b1b;color:#fee2e2}
.badge.CANCELLED{background:#6b7280;color:#f3f4f6}
.badge.UNKNOWN{background:#334155;color:#e2e8f0}
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

/// Build the Vantage dashboard router.
pub fn harvest_ui_router(api_state: HarvestApiState) -> Router<AppState> {
    Router::new()
        .route("/", get(index))
        .route("/workflows", get(list_workflows_ui))
        .route("/workflows/{id}", get(workflow_detail_ui))
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

    layout("Workflows · Vantage", &body)
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

    layout(&title, &body)
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

fn layout(title: &str, body: &Markup) -> Markup {
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
                        a href="workflows" { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard" }
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
        let html = layout("<evil>", &body).into_string();
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
}
