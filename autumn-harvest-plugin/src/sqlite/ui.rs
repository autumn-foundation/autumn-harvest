//! Vantage on the SQLite backend (issue #2468).
//!
//! `.api(path)` serves two read-only pages under `{path}/ui`, as on
//! Postgres. A tenant route serves them under `{path}/tenants/{key}/ui`:
//!
//! - `ui/workflows`: the run list, with the state, name and id filters of
//!   `GET /workflows`, and a cursor for the next page.
//! - `ui/workflows/{id}`: one run with its payloads, its children and a page
//!   of its history.
//!
//! The pages use the Vantage style and helpers. They link only to SQLite
//! pages and JSON routes. The Postgres pages for DAGs, workers, schedules
//! and dead letters read tables that this backend does not have. The pages
//! hold no form that writes, so the #1802 mutation gate has nothing to
//! guard. `maud` escapes every stored value.

use std::fmt::Write as _;

use autumn_harvest::ExecutionId;
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionSummary, HistoryFilter, ListCursor};
use autumn_web::reexports::axum;
use axum::Router;
use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;
use serde_json::Value;

use super::api::{ApiState, map_error, parse_exec};
use super::{HarvestSqlite, HarvestSqliteError};
use crate::ui::{STYLE, format_timestamp, json_card, kv, short_id, state_badge, url_encode};

/// The rows of one list page.
const LIST_PAGE: u32 = 50;

/// The events of one history page on the run page.
const EVENT_PAGE: u32 = 100;

/// The Vantage routes, relative to the API mount.
pub(super) fn routes() -> Router<ApiState> {
    Router::new()
        .route("/ui", get(index))
        .route("/ui/", get(index))
        .route("/ui/workflows", get(list))
        .route("/ui/workflows/{id}", get(detail))
}

/// `GET ui` goes to the run list, with the Postgres `Location`.
///
/// The target is `workflows` for both `ui` and `ui/`, as on Postgres. The
/// shared OIDC check asserts it on both backends.
async fn index() -> Redirect {
    Redirect::to("workflows")
}

/// The page shell. `up` leads from the page to the `ui` directory.
fn layout(title: &str, up: &str, body: &Markup) -> Markup {
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
                        a href={ (up) "workflows" } { "🔭 Vantage" }
                        span.subtitle { "Harvest dashboard · SQLite" }
                    }
                    nav {
                        a.active href={ (up) "workflows" } { "Workflows" }
                    }
                }
                main { (body) }
                footer { "Read-only dashboard — autumn-harvest on SQLite" }
            }
        }
    }
}

fn page(status: StatusCode, markup: Markup) -> Response {
    (status, markup).into_response()
}

/// An error page in the Vantage shell. The status is the API status.
fn error_page(status: StatusCode, up: &str, message: &str) -> Response {
    let body = html! {
        div.banner.Unhealthy { (message) }
        p { a href={ (up) "workflows" } { "← All workflows" } }
    };
    page(status, layout("Error · Vantage", up, &body))
}

fn error_response(error: HarvestSqliteError, up: &str) -> Response {
    let error = map_error(error);
    error_page(error.status(), up, &error.to_string())
}

// ── The run list ────────────────────────────────────────────────────────────

/// The filters of the list page. A bad value shows inline and is not
/// applied, as on the Postgres page.
#[derive(Debug, Default)]
struct ListParams {
    state: Option<String>,
    workflow_name: Option<String>,
    workflow_id_prefix: Option<String>,
    cursor: Option<i64>,
    errors: Vec<String>,
}

impl ListParams {
    fn parse(raw: Option<&str>) -> Self {
        let mut params = Self::default();
        let pairs = match crate::strict_query::decode_or_autumn_error(raw) {
            Ok(pairs) => pairs,
            Err(error) => {
                params.errors.push(error.to_string());
                return params;
            }
        };
        let blank = |value: &str| {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_owned())
        };
        for (key, value) in pairs {
            match key.as_str() {
                "state" => match blank(&value) {
                    Some(state) if crate::api::KNOWN_WORKFLOW_STATES.contains(&state.as_str()) => {
                        params.state = Some(state);
                    }
                    Some(state) => params.errors.push(format!("unknown state '{state}'")),
                    None => {}
                },
                "workflow_name" => params.workflow_name = blank(&value),
                "workflow_id_prefix" => params.workflow_id_prefix = blank(&value),
                "cursor" => match value.trim().parse() {
                    Ok(cursor) => params.cursor = Some(cursor),
                    Err(_) => params.errors.push(format!("invalid cursor '{value}'")),
                },
                // Other keys are ignored, as on the Postgres page.
                _ => {}
            }
        }
        params
    }

    fn filter(&self) -> ExecutionFilter {
        let mut filter = ExecutionFilter::new().limit(LIST_PAGE);
        filter.workflow_name.clone_from(&self.workflow_name);
        filter
            .workflow_id_prefix
            .clone_from(&self.workflow_id_prefix);
        filter.states.extend(self.state.clone());
        filter.before = self.cursor.map(ListCursor::from_raw);
        filter
    }

    /// The query string of the next page.
    fn next(&self, cursor: ListCursor) -> String {
        let mut query = format!("cursor={}", cursor.raw());
        for (key, value) in [
            ("state", &self.state),
            ("workflow_name", &self.workflow_name),
            ("workflow_id_prefix", &self.workflow_id_prefix),
        ] {
            if let Some(value) = value {
                let _ = write!(query, "&{key}={}", url_encode(value));
            }
        }
        query
    }
}

fn text(field: &autumn_harvest_sqlite::Stored<String>) -> &str {
    field.value().map_or("", String::as_str)
}

fn filters(params: &ListParams) -> Markup {
    let selected = params.state.as_deref().unwrap_or("");
    html! {
        form.filters method="get" action="workflows" {
            label {
                "State "
                select name="state" {
                    option value="" selected[selected.is_empty()] { "any" }
                    @for state in crate::api::KNOWN_WORKFLOW_STATES {
                        option value=(state) selected[*state == selected] { (state) }
                    }
                }
            }
            label {
                "Workflow "
                input type="text" name="workflow_name"
                    value=(params.workflow_name.as_deref().unwrap_or(""));
            }
            label {
                "Workflow id prefix "
                input type="text" name="workflow_id_prefix"
                    value=(params.workflow_id_prefix.as_deref().unwrap_or(""));
            }
            button type="submit" { "Filter" }
        }
    }
}

fn list_rows(rows: &[ExecutionSummary]) -> Markup {
    html! {
        @if rows.is_empty() {
            div.card.empty { "No workflows match this filter." }
        } @else {
            table {
                thead {
                    tr {
                        th { "ID" }
                        th { "Workflow" }
                        th { "Workflow id" }
                        th { "State" }
                        th { "Started" }
                        th { "Completed" }
                    }
                }
                tbody {
                    @for row in rows {
                        @let id = text(&row.exec_id);
                        tr {
                            td { a href={ "workflows/" (id) } { code { (short_id(id)) } } }
                            td { (text(&row.workflow_name)) }
                            td { code { (text(&row.workflow_id)) } }
                            td { (state_badge(text(&row.state))) }
                            td { (format_timestamp(row.started_at.value().copied())) }
                            td { (format_timestamp(row.closed_at.value().copied())) }
                        }
                    }
                }
            }
        }
    }
}

/// `GET ui/workflows`.
async fn list(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Response {
    let params = ListParams::parse(raw.as_deref());
    let handle = match state.handle() {
        Ok(handle) => handle,
        Err(error) => return error_page(error.status(), "", &error.to_string()),
    };
    let listing = match handle.list(params.filter()).await {
        Ok(listing) => listing,
        Err(error) => return error_response(error, ""),
    };
    let body = html! {
        h2 { "Workflows" }
        @for error in &params.errors {
            div.banner.Warning { "Not applied: " (error) }
        }
        (filters(&params))
        (list_rows(&listing.executions))
        @if let Some(next) = listing.next {
            nav.pagination {
                a href={ "workflows?" (params.next(next)) } { "Next page →" }
            }
        }
    };
    page(StatusCode::OK, layout("Workflows · Vantage", "", &body))
}

// ── One run ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct IdParams {
    id: String,
}

/// Everything the run page shows, read in one call.
struct Detail {
    description: autumn_harvest_sqlite::ExecutionDescription,
    parent: Option<ExecutionId>,
    children: Vec<autumn_harvest_sqlite::ChildRun>,
    history: autumn_harvest_sqlite::HistoryPage,
}

async fn read_detail(
    handle: &HarvestSqlite,
    exec: ExecutionId,
    after: Option<i64>,
) -> Result<Detail, HarvestSqliteError> {
    handle
        .read(move |reader| {
            Ok(Detail {
                description: reader.describe(exec)?,
                parent: reader.parent(exec)?,
                children: reader.children(&[exec], Some(LIST_PAGE))?,
                history: reader
                    .history_page(exec, &HistoryFilter::new().after(after).limit(EVENT_PAGE))?,
            })
        })
        .await
}

fn payload(text: &autumn_harvest_sqlite::Stored<String>) -> Option<Value> {
    text.value()
        .map(|text| serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.clone())))
}

fn summary_card(exec: ExecutionId, detail: &Detail) -> Markup {
    let row = &detail.description.summary;
    html! {
        div.card {
            (kv("Execution", &exec.to_string(), true))
            (kv("Workflow", text(&row.workflow_name), false))
            (kv("Workflow id", text(&row.workflow_id), true))
            div.kv { div.k { "State" } div.v { (state_badge(text(&row.state))) } }
            (kv("Started", &format_timestamp(row.started_at.value().copied()), false))
            (kv("Completed", &format_timestamp(row.closed_at.value().copied()), false))
            @if let Some(parent) = detail.parent {
                div.kv {
                    div.k { "Parent" }
                    div.v { a href=(parent.to_string()) { code { (parent.to_string()) } } }
                }
            }
        }
    }
}

fn children_table(exec: ExecutionId, children: &[autumn_harvest_sqlite::ChildRun]) -> Markup {
    let capped = children.len() >= usize::try_from(LIST_PAGE).unwrap_or(usize::MAX);
    html! {
        @if !children.is_empty() {
            h3 { "Children" }
            @if capped {
                p {
                    "The first " (LIST_PAGE) " children. "
                    a href={ "../../workflows/" (exec.to_string()) "/children" } { "All children (JSON)" }
                }
            }
            table {
                thead { tr { th { "ID" } th { "Workflow" } th { "Mode" } th { "State" } } }
                tbody {
                    @for child in children {
                        @let id = child.exec_id.to_string();
                        tr {
                            td { a href=(id) { code { (short_id(&id)) } } }
                            td { (child.workflow_name) }
                            td { (if child.detached { "detached" } else { "awaited" }) }
                            td { (state_badge(&child.state)) }
                        }
                    }
                }
            }
        }
    }
}

fn events_table(exec: ExecutionId, history: &autumn_harvest_sqlite::HistoryPage) -> Markup {
    html! {
        h3 { "History (" (history.total_events) " events)" }
        table {
            thead { tr { th { "#" } th { "Type" } th { "Recorded" } th { "Data" } } }
            tbody {
                @for paged in &history.events {
                    @let data = serde_json::to_value(&paged.event)
                        .ok()
                        .and_then(|mut value| value.get_mut("data").map(Value::take))
                        .unwrap_or(Value::Null);
                    tr {
                        td { (paged.event_id) }
                        td { code { (paged.event.type_name()) } }
                        td { (format_timestamp(paged.recorded_at)) }
                        td { pre { (serde_json::to_string(&data).unwrap_or_default()) } }
                    }
                }
            }
        }
        @if let Some(next) = history.next {
            nav.pagination {
                a href={ (exec.to_string()) "?after=" (next) } { "Next events →" }
            }
        }
    }
}

fn detail_body(exec: ExecutionId, detail: &Detail) -> Markup {
    let id = exec.to_string();
    html! {
        p { a href="../workflows" { "← All workflows" } }
        h2 { "Workflow " code { (short_id(&id)) } }
        (summary_card(exec, detail))
        @if let Some(input) = payload(&detail.description.input_json) {
            (json_card("Input", &input))
        }
        @if let Some(output) = payload(&detail.description.output_json) {
            (json_card("Output", &output))
        }
        @if let Some(error) = detail.description.error.value() {
            div.card { h3 { "Error" } pre { (error) } }
        }
        (children_table(exec, &detail.children))
        (events_table(exec, &detail.history))
        p {
            "JSON: "
            a href={ "../../workflows/" (id) } { "run" } " · "
            a href={ "../../workflows/" (id) "/history" } { "history" } " · "
            a href={ "../../workflows/" (id) "/stack" } { "stack" } " · "
            a href={ "../../workflows/" (id) "/timeline" } { "timeline" } " · "
            a href={ "../../workflows/" (id) "/history/export" } { "export" }
        }
    }
}

/// `after` of the run page, or `None`. A bad value starts at the first
/// event.
fn parse_after(raw: Option<&str>) -> Option<i64> {
    crate::strict_query::decode_or_autumn_error(raw)
        .ok()?
        .into_iter()
        .find(|(key, _)| key == "after")
        .and_then(|(_, value)| value.trim().parse().ok())
}

/// `GET ui/workflows/{id}`.
async fn detail(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Response {
    let up = "../";
    let exec = match parse_exec(&id) {
        Ok(exec) => exec,
        Err(error) => return error_page(error.status(), up, &error.to_string()),
    };
    let handle = match state.handle() {
        Ok(handle) => handle,
        Err(error) => return error_page(error.status(), up, &error.to_string()),
    };
    match read_detail(&handle, exec, parse_after(raw.as_deref())).await {
        Ok(detail) => page(
            StatusCode::OK,
            layout(
                &format!("{} · Vantage", short_id(&exec.to_string())),
                up,
                &detail_body(exec, &detail),
            ),
        ),
        Err(error) => error_response(error, up),
    }
}
