//! The read routes of the SQLite management API (issue #2468).
//!
//! Each route keeps its Postgres path, query keys, status codes and JSON
//! shape. A route reads the file through `SqliteReader`, never the writer
//! task. It then calls the same pure step as the Postgres route:
//!
//! | Route | Pure step |
//! |---|---|
//! | `count` | `build_count_response` |
//! | `children` | the Postgres filters, sort and cursor |
//! | `tree` | `LineageWalk` |
//! | `run-chain` | `reconstruct_legacy_chain_order`, `assemble_run_chain` |
//! | `timeline` | `derive_timeline` |
//! | `awaitables` | `classify_replayed_wait_set` |
//! | `history/export` | `export_history_decoded` |
//!
//! So a change to one of these shapes changes both backends.
//!
//! One file is one shard. A fan-out field reads `complete`, and a row
//! carries `shard_id: 0`.

use std::collections::{HashMap, HashSet};

use autumn_harvest::ExecutionId;
use autumn_harvest::store::{AwaitMode, LineageChildRow, WorkflowChildRow};
use autumn_harvest_sqlite::{ChildRun, CountFilter, ExecutionDescription, LogFilter, SqliteReader};
use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Json;
use axum::extract::{Path, RawQuery, State};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use super::HarvestSqlite;
use super::api::{ApiState, map_error, parse_exec, resolve_key};
use crate::api;
use crate::lineage::{LineageWalk, lineage_root_node};
use crate::shard_fanout::{FanoutStatus, ShardObservation};

/// The shard id of every row. One file is one shard.
const SHARD: i32 = 0;

/// A stored time, or the Unix epoch when the row has none.
///
/// A row from before issue #2027 has no start time. Postgres always has one,
/// so the field cannot be `null`.
fn or_epoch(at: Option<DateTime<Utc>>) -> DateTime<Utc> {
    at.unwrap_or(DateTime::UNIX_EPOCH)
}

/// The stored fields of a run that a read route reports.
struct Run {
    exec: ExecutionId,
    workflow_name: String,
    workflow_id: String,
    state: String,
    started_at: Option<DateTime<Utc>>,
    closed_at: Option<DateTime<Utc>>,
}

impl Run {
    fn of(exec: ExecutionId, row: &ExecutionDescription) -> Self {
        let text = |field: &autumn_harvest_sqlite::Stored<String>| {
            field.value().cloned().unwrap_or_default()
        };
        Self {
            exec,
            workflow_name: text(&row.summary.workflow_name),
            workflow_id: text(&row.summary.workflow_id),
            state: text(&row.summary.state),
            started_at: row.summary.started_at.value().copied(),
            closed_at: row.summary.closed_at.value().copied(),
        }
    }

    fn read(reader: &SqliteReader, exec: ExecutionId) -> autumn_harvest_sqlite::SqliteResult<Self> {
        Ok(Self::of(exec, &reader.describe(exec)?))
    }
}

/// A `400` for a parse error message.
fn bad_request(message: impl Into<String>) -> AutumnError {
    AutumnError::bad_request_msg(message.into())
}

fn decode(raw: Option<&str>) -> Result<Vec<(String, String)>, AutumnError> {
    crate::strict_query::decode_or_autumn_error(raw)
}

#[derive(Deserialize)]
pub(super) struct IdParams {
    id: String,
}

#[derive(Deserialize)]
pub(super) struct NameParams {
    name: String,
}

#[derive(Deserialize)]
pub(super) struct ByIdParams {
    workflow_name: String,
    workflow_id: String,
}

// ── Count and summaries ─────────────────────────────────────────────────────

/// `GET /workflows/count` (issue #544).
pub(super) async fn count(
    State(state): State<ApiState>,
    RawQuery(raw): RawQuery,
) -> Result<Json<crate::workflow_count::WorkflowCountResponse>, AutumnError> {
    use autumn_harvest::execution::{WorkflowCountDimension, WorkflowCountRow};
    let pairs = decode(raw.as_deref())?;
    let params = crate::workflow_count::WorkflowCountParams::from_query_pairs(
        &pairs,
        api::KNOWN_WORKFLOW_STATES,
    )
    .map_err(bad_request)?;
    let filter = CountFilter {
        workflow_name: params.workflow_name.clone(),
        states: params.states.clone(),
        started_after: params.started_after,
        started_before: params.started_before,
    };
    let rows = state
        .handle()?
        .read(move |reader| reader.count_executions(&filter))
        .await
        .map_err(map_error)?;
    let by_state = params.group_by.contains(&WorkflowCountDimension::State);
    let by_name = params
        .group_by
        .contains(&WorkflowCountDimension::WorkflowName);
    // The merge sums the rows of one key, so a dimension that is not grouped
    // folds its groups together.
    let rows = rows
        .into_iter()
        .map(|row| WorkflowCountRow {
            state: by_state.then_some(row.state),
            workflow_name: by_name.then_some(row.workflow_name),
            count: row.count,
        })
        .collect();
    let observation = ShardObservation {
        shard_id: SHARD,
        rows,
        error: None,
    };
    Ok(Json(crate::workflow_count::build_count_response(
        Utc::now(),
        usize::try_from(params.limit_groups).unwrap_or(usize::MAX),
        vec![observation],
    )))
}

/// `GET /workflows/summaries` (issue #752).
///
/// This backend never demotes a run to a summary. A purge removes the whole
/// run (issue #2031). So the page is always empty, after the filters pass
/// the Postgres checks.
pub(super) async fn summaries(
    RawQuery(raw): RawQuery,
) -> Result<Json<api::SummaryListPage>, AutumnError> {
    api::parse_summary_filters(&decode(raw.as_deref())?)?;
    Ok(Json(api::SummaryListPage {
        summaries: Vec::new(),
        next_cursor: None,
    }))
}

// ── Registered workflows ────────────────────────────────────────────────────

fn not_registered(name: &str) -> AutumnError {
    AutumnError::not_found_msg(format!("workflow '{name}' is not registered"))
}

/// `GET /workflows/registered/{name}/schema` (issue #373).
pub(super) async fn registered_schema(
    State(state): State<ApiState>,
    Path(NameParams { name }): Path<NameParams>,
) -> Result<Json<autumn_harvest::info::RegisteredWorkflowRecord>, AutumnError> {
    let handle = state.handle()?;
    let info = handle
        .workflow_info(&name)
        .ok_or_else(|| not_registered(&name))?;
    Ok(Json(
        autumn_harvest::info::RegisteredWorkflowRecord::from_info(info),
    ))
}

/// `GET /workflows/registered/{name}/interface` (issue #610).
pub(super) async fn registered_interface(
    State(state): State<ApiState>,
    Path(NameParams { name }): Path<NameParams>,
) -> Result<Json<autumn_harvest::WorkflowInterfaceRecord>, AutumnError> {
    use autumn_harvest::InterfaceHandlerRecord;
    let handle = state.handle()?;
    if handle.workflow_info(&name).is_none() {
        return Err(not_registered(&name));
    }
    let catalog = handle.catalog();
    let sorted = |mut records: Vec<InterfaceHandlerRecord>| {
        records.sort_by(|a, b| a.name.cmp(&b.name));
        records
    };
    Ok(Json(autumn_harvest::WorkflowInterfaceRecord {
        signals: sorted(
            catalog
                .signals
                .iter()
                .filter(|h| h.workflow == name)
                .map(InterfaceHandlerRecord::from_signal)
                .collect(),
        ),
        queries: sorted(
            catalog
                .queries
                .iter()
                .filter(|h| h.workflow == name)
                .map(InterfaceHandlerRecord::from_query)
                .collect(),
        ),
        updates: sorted(
            catalog
                .updates
                .iter()
                .filter(|h| h.workflow == name)
                .map(InterfaceHandlerRecord::from_update)
                .collect(),
        ),
    }))
}

// ── Children and tree ───────────────────────────────────────────────────────

const fn await_mode(child: &ChildRun) -> AwaitMode {
    if child.detached {
        AwaitMode::Detached
    } else {
        AwaitMode::Awaited
    }
}

fn close_policy(child: &ChildRun) -> Option<autumn_harvest::ParentClosePolicy> {
    child
        .parent_close_policy
        .as_deref()
        .and_then(|policy| policy.parse().ok())
}

fn child_row(child: ChildRun, depth: u8) -> WorkflowChildRow {
    WorkflowChildRow {
        exec_id: child.exec_id,
        await_mode: await_mode(&child),
        parent_close_policy: close_policy(&child),
        workflow_name: child.workflow_name,
        status: child.state,
        started_at: or_epoch(child.started_at),
        completed_at: child.closed_at,
        error_summary: autumn_harvest::store::summarize_error(child.error),
        shard_id: SHARD,
        depth,
    }
}

/// The descendants of `root` down to `max_depth`, as the Postgres page
/// reads them. Depth 0 is the direct children.
fn descendants(
    reader: &SqliteReader,
    root: ExecutionId,
    max_depth: u8,
) -> autumn_harvest_sqlite::SqliteResult<Vec<WorkflowChildRow>> {
    reader.describe(root)?;
    let mut rows = Vec::new();
    let mut seen = HashSet::from([root]);
    let mut frontier = vec![root];
    for depth in 0..=max_depth {
        if frontier.is_empty() {
            break;
        }
        let mut next = Vec::new();
        for child in reader.children(&frontier, None)? {
            // A corrupt parent link can form a cycle. Each run counts once.
            if seen.insert(child.exec_id) {
                next.push(child.exec_id);
                rows.push(child_row(child, depth));
            }
        }
        frontier = next;
    }
    Ok(rows)
}

async fn children_page(
    handle: &HarvestSqlite,
    exec: ExecutionId,
    pairs: &[(String, String)],
) -> Result<Json<api::WorkflowChildrenResponse>, AutumnError> {
    let filters = api::parse_workflow_children_filters(pairs)?;
    let max_depth = filters.max_depth;
    let mut rows: Vec<WorkflowChildRow> = handle
        .read(move |reader| descendants(reader, exec, max_depth))
        .await
        .map_err(map_error)?
        .into_iter()
        .filter(|row| {
            api::workflow_child_matches_filters(row, &filters)
                && api::workflow_child_is_after_cursor(row, filters.cursor.as_ref())
        })
        .collect();
    api::sort_workflow_child_rows(&mut rows);
    let next_cursor = (rows.len() > filters.limit).then(|| {
        let cursor = api::encode_workflow_children_cursor(&rows[filters.limit - 1]);
        rows.truncate(filters.limit);
        cursor
    });
    Ok(Json(api::WorkflowChildrenResponse {
        items: rows
            .into_iter()
            .map(api::WorkflowChildResponse::from)
            .collect(),
        next_cursor,
        status: FanoutStatus::from_counts(1, 0),
        unavailable_shards: Vec::new(),
    }))
}

/// `GET /workflows/{id}/children` (issue #956).
pub(super) async fn children(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Json<api::WorkflowChildrenResponse>, AutumnError> {
    let pairs = decode(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    children_page(&state.handle()?, exec, &pairs).await
}

/// `GET /workflows/by-id/{workflow_name}/{workflow_id}/children` (issue #805).
pub(super) async fn children_by_id(
    State(state): State<ApiState>,
    Path(ByIdParams {
        workflow_name,
        workflow_id,
    }): Path<ByIdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    let pairs = decode(raw.as_deref())?;
    let handle = state.handle()?;
    let exec = resolve_key(&handle, &workflow_name, &workflow_id).await?;
    Ok(super::api::by_id_response(
        children_page(&handle, exec, &pairs).await,
        exec,
    ))
}

fn lineage_row(child: ChildRun) -> LineageChildRow {
    LineageChildRow {
        exec_id: child.exec_id,
        parent_id: Some(child.parent),
        await_mode: await_mode(&child),
        parent_close_policy: close_policy(&child),
        workflow_name: child.workflow_name,
        workflow_id: child.workflow_id,
        state: child.state,
        started_at: or_epoch(child.started_at),
        completed_at: child.closed_at,
        shard_id: SHARD,
    }
}

/// The lineage report of `root`, by the walk of the Postgres route.
fn lineage_report(
    reader: &SqliteReader,
    root: &Run,
    limits: crate::lineage::LineageLimits,
) -> autumn_harvest_sqlite::SqliteResult<crate::lineage::LineageTreeReport> {
    let mut walk = LineageWalk::new(root.exec, limits);
    let mut frontier = vec![root.exec.as_uuid()];
    for depth in 1..=limits.max_depth {
        if frontier.is_empty() || walk.is_exhausted() {
            break;
        }
        // One row past the budget, so the walk sees an overflow.
        let fetch = u32::try_from(walk.remaining_budget().saturating_add(1)).unwrap_or(u32::MAX);
        let parents: Vec<ExecutionId> = frontier
            .iter()
            .copied()
            .map(ExecutionId::from_uuid)
            .collect();
        let rows: Vec<LineageChildRow> = reader
            .children(&parents, Some(fetch))?
            .into_iter()
            .map(lineage_row)
            .collect();
        let saturated = rows.len() >= usize::try_from(fetch).unwrap_or(usize::MAX);
        frontier = walk.admit_level(depth, rows);
        if saturated {
            walk.note_saturated_fetch_window();
        }
    }
    // Name only the unexpanded leaves that have children.
    if !frontier.is_empty() {
        let parents: Vec<ExecutionId> = frontier.into_iter().map(ExecutionId::from_uuid).collect();
        let with_children: Vec<uuid::Uuid> = reader
            .parents_with_children(&parents)?
            .iter()
            .map(ExecutionId::as_uuid)
            .collect();
        walk.record_probe_result(&with_children);
    }
    let mut report = walk.finish(lineage_root_node(&LineageChildRow {
        exec_id: root.exec,
        parent_id: reader.parent(root.exec)?,
        workflow_name: root.workflow_name.clone(),
        workflow_id: root.workflow_id.clone(),
        state: root.state.clone(),
        started_at: or_epoch(root.started_at),
        completed_at: root.closed_at,
        shard_id: SHARD,
        await_mode: AwaitMode::Awaited,
        parent_close_policy: None,
    }));
    report.status = FanoutStatus::from_counts(1, 0);
    Ok(report)
}

/// `GET /workflows/{id}/tree` (issue #621).
pub(super) async fn tree(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    let pairs = decode(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    let params = api::parse_lineage_query(&pairs)?;
    let limits = params.limits;
    let report = state
        .handle()?
        .read(move |reader| lineage_report(reader, &Run::read(reader, exec)?, limits))
        .await
        .map_err(map_error)?;
    Ok(if params.summary {
        Json(report.into_summary()).into_response()
    } else {
        Json(report).into_response()
    })
}

// ── Run chain ───────────────────────────────────────────────────────────────

/// The continue-as-new chain that `exec` belongs to (issue #701).
///
/// A run names its successor in its last event. A successor keeps the
/// workflow id, so the runs of the key hold the whole chain. The read links
/// each `CONTINUED_AS_NEW` run to its successor, then orders the links from
/// the queried run, as the Postgres legacy path does.
fn run_chain(
    reader: &SqliteReader,
    exec: ExecutionId,
) -> autumn_harvest_sqlite::SqliteResult<autumn_harvest::run_chain::RunChainResponse> {
    use autumn_harvest::run_chain::{
        RunChainRow, assemble_run_chain, reconstruct_legacy_chain_order,
    };
    let queried = Run::read(reader, exec)?;
    let cap = u32::try_from(api::MAX_LEGACY_CANDIDATES + 1).unwrap_or(u32::MAX);
    let filter = autumn_harvest_sqlite::ExecutionFilter::new()
        .workflow_name(queried.workflow_name.clone())
        .workflow_id(queried.workflow_id.clone())
        .limit(cap);
    let mut candidates = Vec::new();
    reader.visit_executions(&filter, |row| {
        if let Some(id) = row.summary.execution_id() {
            candidates.push(Run::of(id, &row));
        }
    })?;
    let row_of = |run: &Run, links: &[(uuid::Uuid, uuid::Uuid)], head: uuid::Uuid| RunChainRow {
        exec_id: run.exec.as_uuid(),
        // This backend has no run id. The execution id stands in for it.
        run_id: run.exec.as_uuid(),
        workflow_id: run.workflow_id.clone(),
        state: run.state.clone(),
        started_at: or_epoch(run.started_at),
        completed_at: run.closed_at,
        continued_from_exec_id: links
            .iter()
            .find(|(successor, _)| *successor == run.exec.as_uuid())
            .map(|(_, predecessor)| *predecessor),
        first_exec_id: (run.exec.as_uuid() != head).then_some(head),
    };
    if candidates.len() > api::MAX_LEGACY_CANDIDATES {
        // Too many runs of the key to order safely. Serve the queried run and
        // say that its head is unknown, as Postgres does.
        let mut response =
            assemble_run_chain(vec![row_of(&queried, &[], exec.as_uuid())], exec.as_uuid());
        response.head_unknown = true;
        return Ok(response);
    }
    let mut links = Vec::new();
    for run in candidates
        .iter()
        .filter(|run| run.state == "CONTINUED_AS_NEW")
    {
        if let Some(successor) = reader.successor(run.exec)? {
            links.push((successor.as_uuid(), run.exec.as_uuid()));
        }
    }
    let ordered = reconstruct_legacy_chain_order(exec.as_uuid(), &links, api::MAX_RUN_CHAIN_HOPS);
    let head = ordered.first().copied().unwrap_or_else(|| exec.as_uuid());
    let by_id: HashMap<uuid::Uuid, &Run> = candidates
        .iter()
        .map(|run| (run.exec.as_uuid(), run))
        .collect();
    let rows: Vec<RunChainRow> = ordered
        .iter()
        .filter_map(|id| by_id.get(id))
        .map(|run| row_of(run, &links, head))
        .collect();
    Ok(assemble_run_chain(rows, head))
}

/// `GET /workflows/{id}/run-chain` (issue #701).
pub(super) async fn run_chain_route(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<autumn_harvest::run_chain::RunChainResponse>, AutumnError> {
    let exec = parse_exec(&id)?;
    Ok(Json(
        state
            .handle()?
            .read(move |reader| run_chain(reader, exec))
            .await
            .map_err(map_error)?,
    ))
}

// ── Timeline ────────────────────────────────────────────────────────────────

/// The history of `exec` with a time on each event.
///
/// An event from before issue #2489 has no time. It takes the time of the
/// event before it, or the run start. So the event keeps its order and adds
/// no duration.
fn timed_rows(
    reader: &SqliteReader,
    exec: ExecutionId,
    started_at: DateTime<Utc>,
) -> autumn_harvest_sqlite::SqliteResult<Vec<(DateTime<Utc>, autumn_harvest::WorkflowEvent)>> {
    let mut last = started_at;
    Ok(reader
        .timed_history(exec)?
        .into_iter()
        .map(|timed| {
            last = timed.recorded_at.unwrap_or(last);
            (last, timed.event)
        })
        .collect())
}

/// `GET /workflows/{id}/timeline` (issue #739).
pub(super) async fn timeline(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<autumn_harvest::Timeline>, AutumnError> {
    let exec = parse_exec(&id)?;
    let (run, rows) = state
        .handle()?
        .read(move |reader| {
            let run = Run::read(reader, exec)?;
            let rows = timed_rows(reader, exec, or_epoch(run.started_at))?;
            Ok((run, rows))
        })
        .await
        .map_err(map_error)?;
    let rows: Vec<autumn_harvest::timeline::TimelineEventRow> = rows
        .into_iter()
        .map(|(timestamp, event)| autumn_harvest::timeline::TimelineEventRow { timestamp, event })
        .collect();
    Ok(Json(autumn_harvest::derive_timeline(
        &rows,
        or_epoch(run.started_at),
        Utc::now(),
        run.closed_at,
        exec.to_string(),
        run.workflow_id,
        run.workflow_name,
        run.state,
    )))
}

// ── Logs ────────────────────────────────────────────────────────────────────

/// `GET /workflows/{id}/logs` (issue #790). Admin only, as on Postgres.
pub(super) async fn logs(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Json<api::WorkflowLogsResponse>, AutumnError> {
    let pairs = decode(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    let (limit, cursor, levels, since) = api::parse_workflow_logs_query(&pairs)?;
    // One row past the page says whether a next page exists.
    let filter = LogFilter {
        after_seq: cursor,
        since,
        levels,
        limit: u32::try_from(limit.saturating_add(1)).unwrap_or(u32::MAX),
    };
    let (mut lines, total_lines, truncated) = state
        .handle()?
        .read(move |reader| {
            reader.describe(exec)?;
            Ok((
                reader.workflow_logs(exec, &filter)?,
                reader.count_workflow_logs(exec)?,
                reader.workflow_log_truncated(exec)?,
            ))
        })
        .await
        .map_err(map_error)?;
    let page = usize::try_from(limit).unwrap_or(usize::MAX);
    let has_more = lines.len() > page;
    lines.truncate(page);
    let next_cursor = if has_more {
        lines.last().map(|line| line.seq)
    } else {
        None
    };
    Ok(Json(api::WorkflowLogsResponse {
        execution_id: exec.to_string(),
        lines: lines
            .into_iter()
            .map(|line| api::WorkflowLogLineResponse {
                truncation_marker: line.seq == autumn_harvest_sqlite::WORKFLOW_LOG_TRUNCATION_SEQ,
                seq: line.seq,
                level: line.level,
                message: line.message,
                occurred_at: line.occurred_at,
            })
            .collect(),
        next_cursor,
        total_lines,
        truncated,
    }))
}

// ── Awaitables ──────────────────────────────────────────────────────────────

/// What an awaitables report reads from the file in one call.
struct AwaitablesRows {
    run: Run,
    rows: Vec<(DateTime<Utc>, autumn_harvest::WorkflowEvent)>,
    timers: HashSet<String>,
    deadlines: HashMap<String, DateTime<Utc>>,
    replay: autumn_harvest_sqlite::ReplayInput,
}

/// `GET /workflows/{id}/awaitables` (issue #615). Admin only, as on
/// Postgres.
pub(super) async fn awaitables(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<api::WorkflowAwaitablesResponse>, AutumnError> {
    use autumn_harvest::awaitables::AWAITABLE_CATEGORY_CAP;
    let exec = parse_exec(&id)?;
    let handle = state.handle()?;
    let terminal = handle
        .read(move |reader| {
            let run = Run::read(reader, exec)?;
            Ok(api::is_terminal_state(&run.state).then_some(run))
        })
        .await
        .map_err(map_error)?;
    if let Some(run) = terminal {
        return Ok(Json(api::WorkflowAwaitablesResponse {
            execution_id: exec.to_string(),
            workflow_id: run.workflow_id,
            workflow_name: run.workflow_name,
            state: run.state,
            is_terminal: true,
            wait_set: "terminal".to_owned(),
            wait_set_reason: None,
            awaitables: Vec::new(),
            truncated: false,
            truncated_kinds: Vec::new(),
            category_cap: AWAITABLE_CATEGORY_CAP,
        }));
    }
    let read = handle
        .read(move |reader| {
            let run = Run::read(reader, exec)?;
            let rows = timed_rows(reader, exec, or_epoch(run.started_at))?;
            let work = reader.pending_work(exec)?;
            Ok(AwaitablesRows {
                rows,
                timers: work
                    .timers
                    .into_iter()
                    .map(|timer| timer.timer_id)
                    .collect(),
                deadlines: work
                    .tasks
                    .into_iter()
                    .filter_map(|task| Some((task.activity_id, task.schedule_to_close_at?)))
                    .collect(),
                replay: reader.replay_input(exec)?,
                run,
            })
        })
        .await
        .map_err(map_error)?;
    let AwaitablesRows {
        run,
        rows,
        timers,
        deadlines,
        replay,
    } = read;
    let (wait_set, reason, mut projection) =
        wait_set(handle.catalog(), &run.workflow_name, replay, &rows, &timers).await;
    api::enrich_activity_deadlines(&mut projection, &deadlines);
    Ok(Json(api::WorkflowAwaitablesResponse {
        execution_id: exec.to_string(),
        workflow_id: run.workflow_id,
        workflow_name: run.workflow_name,
        state: run.state,
        is_terminal: false,
        wait_set: wait_set.to_owned(),
        wait_set_reason: reason.map(str::to_owned),
        awaitables: projection.awaitables,
        truncated: projection.truncated,
        truncated_kinds: projection.truncated_kinds,
        category_cap: projection.category_cap,
    }))
}

/// The wait-set of a parked run, by a replay with the drive's handlers.
///
/// A workflow that this app does not register degrades to the history-only
/// scan, as on Postgres.
async fn wait_set(
    catalog: &super::handle::Catalog,
    workflow_name: &str,
    replay: autumn_harvest_sqlite::ReplayInput,
    rows: &[(DateTime<Utc>, autumn_harvest::WorkflowEvent)],
    timers: &HashSet<String>,
) -> (
    &'static str,
    Option<&'static str>,
    autumn_harvest::awaitables::AwaitablesProjection,
) {
    use autumn_harvest::awaitables::{AWAITABLE_CATEGORY_CAP, WaitSetInput, project_awaitables};
    let Some(workflow) = catalog
        .workflows
        .iter()
        .find(|info| info.name == workflow_name)
    else {
        return (
            "history_only",
            Some("handler_not_registered"),
            project_awaitables(
                rows,
                WaitSetInput::HistoryOnly {
                    fire_eligible_timers: Some(timers),
                },
                AWAITABLE_CATEGORY_CAP,
            ),
        );
    };
    let (ctx, input) = replay.into_context(
        catalog.state(),
        autumn_harvest::WorkflowHistoryPolicy::default(),
    );
    // Register the declarative handlers before the drive, as the drive does.
    // Otherwise a pending update would drop out of the wait-set.
    for query in catalog
        .queries
        .iter()
        .filter(|h| h.workflow == workflow_name)
    {
        ctx.register_declarative_query_handler(query);
    }
    for update in catalog
        .updates
        .iter()
        .filter(|h| h.workflow == workflow_name)
    {
        ctx.register_declarative_update_handler(update);
    }
    let outcome = autumn_harvest::executor::drive_query_replay_async(
        &ctx,
        workflow.handler,
        input,
        autumn_harvest_sqlite::DEFAULT_QUERY_TIMEOUT,
    )
    .await;
    api::classify_replayed_wait_set(&ctx, outcome, rows, Some(timers))
}

// ── Stack ───────────────────────────────────────────────────────────────────

/// What a stack reads from the file in one call.
struct StackRows {
    run: Run,
    work: autumn_harvest_sqlite::PendingWork,
    history: Vec<(DateTime<Utc>, autumn_harvest::WorkflowEvent)>,
    children: Vec<ChildRun>,
}

fn pending_activity(
    task: autumn_harvest_sqlite::PendingTask,
    now: DateTime<Utc>,
) -> api::PendingActivity {
    let (heartbeat_details, heartbeat_details_truncated, heartbeat_details_bytes) =
        api::project_heartbeat_details(
            task.heartbeat_details,
            autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
        );
    let attempt = i32::try_from(task.attempt).unwrap_or(i32::MAX);
    let last_failure = task.last_error.map(|error| api::LastFailure {
        error_type: "Error".to_owned(),
        error,
        attempt,
        non_retryable: false,
        failed_at: None,
        details: None,
    });
    api::PendingActivity {
        activity_exec_id: task.activity_id,
        activity_name: task.name,
        queue: task.queue,
        // Postgres moves `scheduled_at` to the retry time. `run_at` is that
        // time on this backend.
        scheduled_at: task.run_at,
        attempt,
        max_attempts: task
            .max_attempts
            .and_then(|max| i32::try_from(max).ok())
            .unwrap_or(1),
        next_retry_at: (task.state == "PENDING" && task.run_at > now).then_some(task.run_at),
        task_status: task.state,
        claimed_by_worker_id: None,
        input: None,
        input_truncated: None,
        input_omitted_for_budget: None,
        input_bytes: None,
        last_heartbeat_at: None,
        heartbeat_details,
        heartbeat_details_truncated,
        heartbeat_details_omitted_for_budget: false,
        heartbeat_details_bytes,
        // This backend stores no attempt start and no heartbeat time, so it
        // derives neither deadline.
        schedule_to_start_deadline: None,
        start_to_close_deadline: None,
        heartbeat_deadline: None,
        last_failure,
    }
}

async fn stack_body(
    handle: &HarvestSqlite,
    exec: ExecutionId,
) -> Result<Json<api::WorkflowStackResponse>, AutumnError> {
    let read = handle
        .read(move |reader| {
            let run = Run::read(reader, exec)?;
            Ok(StackRows {
                work: reader.pending_work(exec)?,
                history: timed_rows(reader, exec, or_epoch(run.started_at))?,
                children: reader.children(&[exec], None)?,
                run,
            })
        })
        .await
        .map_err(map_error)?;
    let is_terminal = api::is_terminal_state(&read.run.state);
    // The 0-based index of the newest event, as on Postgres.
    let last_event_id = i64::try_from(read.history.len().saturating_sub(1)).unwrap_or(i64::MAX);
    let mut body = api::WorkflowStackResponse {
        exec_id: exec.to_string(),
        workflow_id: read.run.workflow_id,
        workflow_name: read.run.workflow_name,
        state: read.run.state,
        is_terminal,
        pending_activities: Vec::new(),
        pending_external_handoffs: Vec::new(),
        pending_local_activities: Vec::new(),
        pending_timers: Vec::new(),
        pending_signals: Vec::new(),
        buffered_signals: Vec::new(),
        pending_child_workflows: Vec::new(),
        checkpoints_truncated_for_budget: false,
        inputs_truncated_for_budget: None,
        last_event_id,
    };
    if is_terminal {
        return Ok(Json(body));
    }
    let now = Utc::now();
    body.pending_activities = read
        .work
        .tasks
        .into_iter()
        .map(|task| pending_activity(task, now))
        .collect();
    body.checkpoints_truncated_for_budget = api::apply_checkpoint_response_budget(
        &mut body.pending_activities,
        autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
    );
    let numbered: Vec<(i64, autumn_harvest::WorkflowEvent, DateTime<Utc>)> = read
        .history
        .iter()
        .enumerate()
        .map(|(index, (at, event))| (i64::try_from(index).unwrap_or(i64::MAX), event.clone(), *at))
        .collect();
    let failures = api::latest_failures_by_activity(&numbered);
    let local = read.history.into_iter().filter_map(|(at, event)| {
        let kind = event.type_name();
        kind.starts_with("LocalActivity")
            .then(|| serde_json::to_value(&event).ok())
            .flatten()
            .map(|data| (kind.to_owned(), data, at))
    });
    body.pending_local_activities = api::fold_pending_local_activities(local, &failures);
    body.pending_timers = read
        .work
        .timers
        .into_iter()
        .map(|timer| api::PendingTimer {
            timer_id: timer.timer_id,
            name: None,
            fires_at: timer.fire_at,
            created_at: None,
        })
        .collect();
    body.buffered_signals = read
        .work
        .signals
        .into_iter()
        .map(|signal| api::BufferedSignal {
            signal_name: signal.name,
            buffered: signal.count,
            oldest_received_at: signal.oldest_received_at,
        })
        .collect();
    body.pending_child_workflows = read
        .children
        .into_iter()
        .filter(|child| !api::is_terminal_state(&child.state))
        .map(|child| api::PendingChildWorkflow {
            child_exec_id: child.exec_id.to_string(),
            child_workflow_name: child.workflow_name,
            state: child.state,
        })
        .collect();
    Ok(Json(body))
}

/// `GET /workflows/{id}/stack` (issue #486).
pub(super) async fn stack(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
) -> Result<Json<api::WorkflowStackResponse>, AutumnError> {
    let exec = parse_exec(&id)?;
    stack_body(&state.handle()?, exec).await
}

/// `GET /workflows/by-id/{workflow_name}/{workflow_id}/stack` (issue #805).
pub(super) async fn stack_by_id(
    State(state): State<ApiState>,
    Path(ByIdParams {
        workflow_name,
        workflow_id,
    }): Path<ByIdParams>,
) -> Result<Response, AutumnError> {
    let handle = state.handle()?;
    let exec = resolve_key(&handle, &workflow_name, &workflow_id).await?;
    Ok(super::api::by_id_response(
        stack_body(&handle, exec).await,
        exec,
    ))
}

// ── History export ──────────────────────────────────────────────────────────

/// `GET /workflows/{id}/history/export` (issue #544).
///
/// This backend stores no codec envelope, so the export decodes nothing.
/// The `redacted` policy still drops each payload.
pub(super) async fn history_export(
    State(state): State<ApiState>,
    Path(IdParams { id }): Path<IdParams>,
    RawQuery(raw): RawQuery,
) -> Result<Response, AutumnError> {
    let pairs = decode(raw.as_deref())?;
    let exec = parse_exec(&id)?;
    let query = api::parse_history_export_query(&pairs)?;
    let (run, replay) = state
        .handle()?
        .read(move |reader| Ok((Run::read(reader, exec)?, reader.replay_input(exec)?)))
        .await
        .map_err(map_error)?;
    let (execution_timeout, deadline_at) = replay.deadline();
    let request = autumn_harvest::history_export::HistoryExportRequest {
        workflow_name: run.workflow_name,
        workflow_id: Some(run.workflow_id),
        queue_name: None,
        execution_id: exec,
        shard_id: SHARD,
        state: run.state,
        parent_execution_id: replay.parent(),
        events: replay.into_history(),
        exported_at: Utc::now(),
        payload_policy: query.payload_policy,
        max_bytes: Some(query.max_bytes),
        context_headers: None,
        execution_timeout,
        deadline_at,
        start_provenance: None,
    };
    let mut outcome = autumn_harvest::payload_codec::LossyDecodeOutcome::default();
    Ok(
        match autumn_harvest::history_export::export_history_decoded(request, None, &mut outcome) {
            Ok(document) => Json(document).into_response(),
            Err(error) => api::history_export_error_response(error),
        },
    )
}
