//! Vantage — an embedded, read-only HTML dashboard for Harvest workflows.
//!
//! Mounts alongside the management API (e.g. `/api/harvest/ui`). Renders a
//! paginated workflow list and a per-workflow detail page showing inputs,
//! outputs, and the full event history. Assets are inlined so the dashboard
//! works in network-restricted environments.

use std::fmt::Write as _;

use autumn_web::AppState;
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Extension;
use axum::Router;
use axum::extract::{Path, Query};
use axum::response::Html;
use axum::routing::get;
use chrono::{DateTime, Utc};
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
) -> Result<Html<String>, AutumnError> {
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

    Ok(Html(render_workflow_list(
        &workflows,
        page,
        limit,
        has_next,
        state_filter.as_deref(),
        workflow_name_filter.as_deref(),
        search_attr_pair.as_ref(),
    )))
}

async fn workflow_detail_ui(
    Extension(api_state): Extension<HarvestApiState>,
    Path(id): Path<String>,
) -> Result<Html<String>, AutumnError> {
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

    Ok(Html(render_workflow_detail(&execution, &events)))
}

fn render_workflow_list(
    workflows: &[WorkflowExecution],
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
) -> String {
    let mut body = String::new();
    body.push_str("<h2>Workflows</h2>");
    body.push_str(&render_filters(
        state_filter,
        workflow_name_filter,
        search_attr_filter,
        limit,
    ));

    if workflows.is_empty() {
        body.push_str("<div class=\"card empty\">No workflows match this filter.</div>");
    } else {
        body.push_str("<table><thead><tr>");
        body.push_str("<th>ID</th><th>Workflow</th><th>State</th><th>Queue</th><th>Started</th><th>Completed</th>");
        body.push_str("</tr></thead><tbody>");
        for execution in workflows {
            let id = execution.id.to_string();
            let short = short_id(&id);
            let _ = write!(
                body,
                "<tr><td><a href=\"workflows/{id}\"><code>{short}</code></a></td>\
                 <td>{name}</td>\
                 <td>{badge}</td>\
                 <td><code>{queue}</code></td>\
                 <td>{started}</td>\
                 <td>{completed}</td></tr>",
                id = html_escape(&id),
                short = html_escape(&short),
                name = html_escape(&execution.workflow_name),
                badge = state_badge(&execution.state),
                queue = html_escape(&execution.queue_name),
                started = format_timestamp(Some(execution.started_at)),
                completed = format_timestamp(execution.completed_at),
            );
        }
        body.push_str("</tbody></table>");
    }

    body.push_str(&render_pagination(
        page,
        limit,
        has_next,
        state_filter,
        workflow_name_filter,
        search_attr_filter,
    ));

    layout("Workflows · Vantage", &body)
}

fn render_filters(
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
    limit: i64,
) -> String {
    let mut out = String::new();
    out.push_str("<form class=\"filters\" method=\"get\" action=\"workflows\">");
    out.push_str("<label>State<select name=\"state\">");
    out.push_str("<option value=\"\">All</option>");
    for state in KNOWN_STATES {
        let selected = state_filter.is_some_and(|filter| filter == *state);
        let _ = write!(
            out,
            "<option value=\"{state}\"{selected}>{state}</option>",
            selected = if selected { " selected" } else { "" }
        );
    }
    out.push_str("</select></label>");
    let _ = write!(
        out,
        "<label>Workflow name<input type=\"text\" name=\"workflow_name\" value=\"{value}\" placeholder=\"e.g. onboarding\"></label>",
        value = html_escape(workflow_name_filter.unwrap_or("")),
    );
    let (attr_key, attr_value) =
        search_attr_filter.map_or(("", ""), |(k, v)| (k.as_str(), v.as_str()));
    let _ = write!(
        out,
        "<label>Search attr key<input type=\"text\" name=\"search_attr_key\" value=\"{key}\" placeholder=\"e.g. tenant\"></label>",
        key = html_escape(attr_key),
    );
    let _ = write!(
        out,
        "<label>Search attr value<input type=\"text\" name=\"search_attr_value\" value=\"{value}\" placeholder=\"e.g. acme\"></label>",
        value = html_escape(attr_value),
    );
    let _ = write!(
        out,
        "<label>Per page<input type=\"number\" name=\"limit\" min=\"1\" max=\"{MAX_PAGE_SIZE}\" value=\"{limit}\"></label>"
    );
    out.push_str("<button type=\"submit\">Apply</button>");
    out.push_str("<a class=\"reset\" href=\"workflows\">Reset</a>");
    out.push_str("</form>");
    out
}

fn render_pagination(
    page: i64,
    limit: i64,
    has_next: bool,
    state_filter: Option<&str>,
    workflow_name_filter: Option<&str>,
    search_attr_filter: Option<&(String, String)>,
) -> String {
    let mut out = String::new();
    out.push_str("<div class=\"pagination\">");

    let base_query = build_query_string(
        limit,
        state_filter,
        workflow_name_filter,
        search_attr_filter,
    );

    if page > 0 {
        let _ = write!(
            out,
            "<a href=\"workflows?page={prev}{extra}\">&larr; Previous</a>",
            prev = page - 1,
            extra = base_query,
        );
    } else {
        out.push_str("<span class=\"disabled\">&larr; Previous</span>");
    }

    let _ = write!(out, "<span>Page {}</span>", page + 1);

    if has_next {
        let _ = write!(
            out,
            "<a href=\"workflows?page={next}{extra}\">Next &rarr;</a>",
            next = page + 1,
            extra = base_query,
        );
    } else {
        out.push_str("<span class=\"disabled\">Next &rarr;</span>");
    }
    out.push_str("</div>");
    out
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

fn render_workflow_detail(execution: &WorkflowExecution, events: &[Value]) -> String {
    let mut body = String::new();
    body.push_str("<div class=\"detail-row\"><a class=\"back\" href=\"../workflows\">&larr; Back to workflows</a></div>");

    let _ = write!(
        body,
        "<h2>{name} <span class=\"badge {class}\">{state}</span></h2>",
        name = html_escape(&execution.workflow_name),
        class = badge_class(&execution.state),
        state = html_escape(&execution.state),
    );

    if let Some(error) = execution.error.as_deref() {
        let _ = write!(
            body,
            "<div class=\"error-banner\"><strong>Error:</strong> {}</div>",
            html_escape(error)
        );
    }

    body.push_str("<div class=\"card\"><h3>Metadata</h3><div class=\"kv\">");
    kv(&mut body, "Execution ID", &execution.id.to_string(), true);
    kv(&mut body, "Workflow ID", &execution.workflow_id, true);
    kv(&mut body, "Run ID", &execution.run_id.to_string(), true);
    kv(&mut body, "Shard ID", &execution.shard_id.to_string(), true);
    kv(&mut body, "Queue", &execution.queue_name, true);
    kv(
        &mut body,
        "Started",
        &format_timestamp(Some(execution.started_at)),
        false,
    );
    kv(
        &mut body,
        "Completed",
        &format_timestamp(execution.completed_at),
        false,
    );
    if let Some(parent) = execution.parent_id {
        kv(&mut body, "Parent", &parent.to_string(), true);
    }
    if let Some(worker) = execution.sticky_worker_id.as_deref() {
        kv(&mut body, "Sticky worker", worker, true);
    }
    if let Some(timeout) = execution.execution_timeout {
        kv(
            &mut body,
            "Execution timeout",
            &format!("{}s", timeout.num_seconds()),
            false,
        );
    }
    body.push_str("</div></div>");

    push_json_card(&mut body, "Input", &execution.input);
    if let Some(output) = execution.output.as_ref() {
        push_json_card(&mut body, "Output", output);
    }
    if let Some(memo) = execution.memo.as_ref() {
        push_json_card(&mut body, "Memo", memo);
    }
    if let Some(attrs) = execution.search_attrs.as_ref() {
        push_json_card(&mut body, "Search attributes", attrs);
    }

    body.push_str("<div class=\"card\"><h3>Event history</h3>");
    if events.is_empty() {
        body.push_str("<div class=\"empty\">No events recorded yet.</div>");
    } else {
        body.push_str("<table><thead><tr>");
        body.push_str("<th>#</th><th>Type</th><th>Timestamp</th><th>Data</th>");
        body.push_str("</tr></thead><tbody>");
        for (index, event) in events.iter().enumerate() {
            let event_type = event
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("<unknown>");
            let timestamp = event
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("—");
            let data_pretty = event
                .get("data")
                .map_or_else(|| "{}".to_string(), pretty_json);

            let _ = write!(
                body,
                "<tr><td>{index}</td><td><code>{ty}</code></td><td>{ts}</td>\
                 <td><details><summary>view payload</summary><pre>{data}</pre></details></td></tr>",
                index = index + 1,
                ty = html_escape(event_type),
                ts = html_escape(timestamp),
                data = html_escape(&data_pretty),
            );
        }
        body.push_str("</tbody></table>");
    }
    body.push_str("</div>");

    layout(&format!("{} · Vantage", execution.workflow_name), &body)
}

fn kv(out: &mut String, key: &str, value: &str, mono: bool) {
    let _ = write!(out, "<div class=\"k\">{}</div>", html_escape(key));
    if mono {
        let _ = write!(
            out,
            "<div class=\"v\"><code>{}</code></div>",
            html_escape(value)
        );
    } else {
        let _ = write!(out, "<div class=\"v\">{}</div>", html_escape(value));
    }
}

fn push_json_card(out: &mut String, title: &str, value: &Value) {
    let _ = write!(
        out,
        "<div class=\"card\"><h3>{title}</h3><pre>{payload}</pre></div>",
        title = html_escape(title),
        payload = html_escape(&pretty_json(value)),
    );
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

fn state_badge(state: &str) -> String {
    format!(
        "<span class=\"badge {class}\">{state}</span>",
        class = badge_class(state),
        state = html_escape(state),
    )
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

fn html_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(ch),
        }
    }
    out
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

fn layout(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html lang=\"en\"><head>\
         <meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
         <title>{title}</title>\
         <style>{style}</style>\
         </head><body>\
         <header><h1><a href=\"workflows\">🔭 Vantage</a><span class=\"subtitle\">Harvest dashboard</span></h1></header>\
         <main>{body}</main>\
         <footer>Read-only dashboard — autumn-harvest</footer>\
         </body></html>",
        title = html_escape(title),
        style = STYLE,
        body = body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn html_escape_handles_special_chars() {
        assert_eq!(
            html_escape("<script>alert(\"x\")</script>"),
            "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;"
        );
        assert_eq!(html_escape("a & b"), "a &amp; b");
        assert_eq!(html_escape("it's"), "it&#x27;s");
    }

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
        let html = layout("<evil>", "<p>hello</p>");
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
        assert_eq!(
            build_query_string(
                DEFAULT_PAGE_SIZE,
                None,
                Some("onboarding"),
                Some(&("tenant".to_string(), "acme".to_string())),
            ),
            "&workflow_name=onboarding&search_attr_key=tenant&search_attr_value=acme"
        );
    }
}
