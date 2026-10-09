//! A recorder of task-row traces for TLA+ trace validation (issue #2003).
//!
//! Two triggers copy every committed write to `harvest_task_queue`, and each
//! activity or terminal event in `harvest_events`, into `harvest_tla_trace`.
//! A trigger row rolls back with its transaction, so the log holds committed
//! steps only.
//!
//! [`export`] turns the log into one NDJSON trace per task row. Each line is
//! one transaction. `scripts/check-formal-traces.sh` checks each trace with
//! TLC against `formal/tla/trace/<spec>Trace.tla`. See
//! `docs/testing/formal-methods.md`.
//!
//! A test opens a connection with [`actor_url`] to name the claim that
//! writes. The trigger reads the name from the `harvest.trace_actor`
//! setting. A line with no name can be explained by any writer.
//!
//! Recording runs only when `HARVEST_TLA_TRACE_DIR` is set. A run without
//! it therefore leaves no trigger on a shared test database. A run with it
//! leaves the triggers in place. A red test calls [`install_now`] to record
//! in every run.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::fmt::Write as _;
use std::path::PathBuf;

use diesel::QueryableByName;
use diesel_async::{AsyncPgConnection, SimpleAsyncConnection};
use serde_json::{Value, json};
use uuid::Uuid;

/// The directory that receives the traces. Unset turns recording off.
pub const TRACE_DIR_VAR: &str = "HARVEST_TLA_TRACE_DIR";

/// The workflow-level terminal events. They match `terminal_event_count`.
const WORKFLOW_TERMINALS: &[&str] = &[
    "WorkflowCompleted",
    "WorkflowFailed",
    "WorkflowCancelled",
    "WorkflowContinuedAsNew",
    "WorkflowResetTerminated",
    "WorkflowExecutionTimedOut",
];

/// Activity result events that always end the activity.
const ACTIVITY_FINAL: &[&str] = &["ActivityCompleted", "ActivityCompletedExternally"];

/// Activity result events that end the activity, unless the same
/// transaction requeues the row for a retry. A result with no requeue
/// therefore counts, so a stale result fails the check.
const ACTIVITY_RESULTS: &[&str] = &[
    "ActivityFailed",
    "ActivityTimedOut",
    "ActivityFailedExternally",
];

const INSTALL_SQL: &str = r"
CREATE TABLE IF NOT EXISTS harvest_tla_trace (
    id bigserial PRIMARY KEY,
    tx xid8 NOT NULL DEFAULT pg_current_xact_id(),
    kind text NOT NULL,
    op text NOT NULL,
    task_id uuid,
    task_type text,
    exec_id uuid,
    activity_id uuid,
    state text,
    worker_id text,
    attempt int,
    crash_strikes int,
    heartbeat_at timestamptz,
    event_type text,
    event_worker text,
    actor text NOT NULL DEFAULT COALESCE(current_setting('harvest.trace_actor', true), '')
);
CREATE OR REPLACE FUNCTION harvest_tla_trace_task() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO harvest_tla_trace (kind, op, task_id, task_type, exec_id, activity_id,
        state, worker_id, attempt, crash_strikes, heartbeat_at)
    VALUES ('row', TG_OP, NEW.id, NEW.task_type, NEW.workflow_exec_id, NEW.activity_id,
        NEW.state, NEW.worker_id, NEW.attempt, NEW.crash_strikes, NEW.last_heartbeat_at);
    RETURN NULL;
END $$;
CREATE OR REPLACE FUNCTION harvest_tla_trace_event() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO harvest_tla_trace (kind, op, exec_id, activity_id, event_type, event_worker)
    VALUES ('event', TG_OP, NEW.workflow_exec_id,
        (NEW.event_data -> 'data' ->> 'activity_id')::uuid, NEW.event_type,
        NEW.event_data -> 'data' ->> 'worker_id');
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS harvest_tla_trace_task ON harvest_task_queue;
CREATE TRIGGER harvest_tla_trace_task AFTER INSERT OR UPDATE ON harvest_task_queue
    FOR EACH ROW EXECUTE FUNCTION harvest_tla_trace_task();
DROP TRIGGER IF EXISTS harvest_tla_trace_event ON harvest_events;
CREATE TRIGGER harvest_tla_trace_event AFTER INSERT ON harvest_events
    FOR EACH ROW WHEN (NEW.event_type LIKE 'Activity%' OR NEW.event_type IN (
        'WorkflowCompleted', 'WorkflowFailed', 'WorkflowCancelled',
        'WorkflowContinuedAsNew', 'WorkflowResetTerminated', 'WorkflowExecutionTimedOut'))
    EXECUTE FUNCTION harvest_tla_trace_event();
TRUNCATE harvest_tla_trace;
";

const UNINSTALL_SQL: &str = r"
DROP TRIGGER IF EXISTS harvest_tla_trace_task ON harvest_task_queue;
DROP TRIGGER IF EXISTS harvest_tla_trace_event ON harvest_events;
DROP FUNCTION IF EXISTS harvest_tla_trace_task();
DROP FUNCTION IF EXISTS harvest_tla_trace_event();
DROP TABLE IF EXISTS harvest_tla_trace;
";

/// The trace directory, or `None` when recording is off.
pub fn trace_dir() -> Option<PathBuf> {
    std::env::var_os(TRACE_DIR_VAR)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Install the recorder and clear its log, when recording is on. Otherwise
/// remove a recorder that an earlier run left on a shared database.
pub async fn install(conn: &mut AsyncPgConnection) {
    if trace_dir().is_some() {
        install_now(conn).await;
    } else {
        uninstall_unless_recording(conn).await;
    }
}

/// Install the recorder and clear its log.
pub async fn install_now(conn: &mut AsyncPgConnection) {
    conn.batch_execute(INSTALL_SQL)
        .await
        .expect("install the TLA+ trace recorder");
}

/// Remove the recorder when recording is off. A test that calls
/// [`install_now`] calls this last, so a run without recording leaves no
/// trigger on a shared database.
pub async fn uninstall_unless_recording(conn: &mut AsyncPgConnection) {
    if trace_dir().is_none() {
        conn.batch_execute(UNINSTALL_SQL)
            .await
            .expect("remove the TLA+ trace recorder");
    }
}

/// `url` with a session setting that names the claim `(worker, attempt)` of
/// `task` as the writer of each statement.
pub fn actor_url(url: &str, task: Uuid, worker: &str, attempt: i32) -> String {
    let value = format!("-c harvest.trace_actor={task}/{worker}/{attempt}");
    let mut encoded = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            encoded.push(char::from(byte));
        } else {
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    let sep = if url.contains('?') { '&' } else { '?' };
    format!("{url}{sep}options={encoded}")
}

#[derive(QueryableByName, Debug, Clone)]
struct LogRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    id: i64,
    #[diesel(sql_type = diesel::sql_types::Text)]
    tx: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    kind: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    op: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    task_id: Option<Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    task_type: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    exec_id: Option<Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    activity_id: Option<Uuid>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    state: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    worker_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    attempt: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    crash_strikes: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    heartbeat_at: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    event_type: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    event_worker: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    actor: String,
}

/// The logged columns of a task row, as the trace specs see them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct View {
    state: String,
    worker: Option<String>,
    attempt: i32,
    strikes: i32,
    terminal: i32,
}

/// One traced task row.
#[derive(Debug)]
pub struct TaskTrace {
    /// The task row id.
    pub task_id: Uuid,
    /// `ActivityClaim` or `WorkflowTaskClaim`.
    pub spec: &'static str,
    /// The NDJSON step lines, without the header.
    pub lines: Vec<Value>,
}

/// The spec of a task type, or `None` for a type that no spec models.
fn spec_of(task_type: &str) -> Option<&'static str> {
    match task_type {
        "activity" => Some("ActivityClaim"),
        "workflow" => Some("WorkflowTaskClaim"),
        _ => None,
    }
}

/// Build the traces of every task row in `log`.
///
/// # Panics
///
/// Panics when a row's first logged write is not its insert. The trace
/// would then start in the middle of the row's history.
fn build(log: &[LogRow]) -> Vec<TaskTrace> {
    // The task row of each activity id and of each run's workflow task.
    let mut by_activity = BTreeMap::new();
    let mut by_exec = BTreeMap::new();
    for r in log.iter().filter(|r| r.kind == "row") {
        let (Some(task), Some(kind)) = (r.task_id, r.task_type.as_deref()) else {
            continue;
        };
        if let (Some(a), "activity") = (r.activity_id, kind) {
            by_activity.insert(a, task);
        }
        if let (Some(e), "workflow") = (r.exec_id, kind) {
            let first = *by_exec.entry(e).or_insert(task);
            assert_eq!(first, task, "run {e} has two workflow task rows");
        }
    }

    // Transactions in commit order. Row locks serialize the writes of one
    // task row, so the last log id of a transaction orders it.
    let mut txs: BTreeMap<&str, Vec<&LogRow>> = BTreeMap::new();
    for r in log {
        txs.entry(r.tx.as_str()).or_default().push(r);
    }
    let mut ordered: Vec<Vec<&LogRow>> = txs.into_values().collect();
    ordered.sort_by_key(|rows| rows.iter().map(|r| r.id).max());

    let mut traces: BTreeMap<Uuid, TaskTrace> = BTreeMap::new();
    let mut views: BTreeMap<Uuid, (View, Option<String>)> = BTreeMap::new();
    for rows in &ordered {
        // The task row that each log row belongs to.
        let mut linked: BTreeMap<Uuid, Vec<&LogRow>> = BTreeMap::new();
        for r in rows {
            let task = match r.kind.as_str() {
                "row" => r.task_id,
                _ => r
                    .activity_id
                    .and_then(|a| by_activity.get(&a).copied())
                    .or_else(|| {
                        let terminal = WORKFLOW_TERMINALS.contains(&r.event_type.as_deref()?);
                        terminal.then(|| by_exec.get(&r.exec_id?).copied())?
                    }),
            };
            if let Some(task) = task {
                linked.entry(task).or_default().push(r);
            }
        }
        // One connection writes a transaction, so every row has its actor.
        let actor = rows.first().map_or("", |r| r.actor.as_str());
        for (task, mine) in linked {
            step(task, &mine, actor, &mut traces, &mut views);
        }
    }
    traces.into_values().filter(|t| t.lines.len() > 1).collect()
}

/// Append the line of one transaction to the trace of `task`. `rows` are
/// the log rows of that transaction that belong to `task`.
fn step(
    task: Uuid,
    rows: &[&LogRow],
    actor: &str,
    traces: &mut BTreeMap<Uuid, TaskTrace>,
    views: &mut BTreeMap<Uuid, (View, Option<String>)>,
) {
    let mine: Vec<&LogRow> = rows.iter().copied().filter(|r| r.kind == "row").collect();
    let events: Vec<&str> = rows
        .iter()
        .filter(|r| r.kind == "event")
        .filter_map(|r| r.event_type.as_deref())
        .collect();
    let Some(spec) = traces.get(&task).map(|t| t.spec).or_else(|| {
        let [first, ..] = mine.as_slice() else {
            return None;
        };
        spec_of(first.task_type.as_deref()?)
    }) else {
        return;
    };

    if let Entry::Vacant(slot) = views.entry(task) {
        let [first, ..] = mine.as_slice() else {
            panic!("task {task}: an event precedes the row");
        };
        assert_eq!(
            first.op, "INSERT",
            "task {task}: the trace must start at the row's insert"
        );
        let view = view_of(first, spec, 0);
        let line = trace_line("init", &view, None);
        traces.insert(
            task,
            TaskTrace {
                task_id: task,
                spec,
                lines: vec![line],
            },
        );
        slot.insert((view, first.heartbeat_at.clone()));
    }

    let (old, old_hb) = views.get(&task).cloned().expect("the row has a view");
    let last = mine.last().copied();
    let mut view = last.map_or_else(|| old.clone(), |r| view_of(r, spec, old.terminal));
    let started = events.contains(&"ActivityStarted") && spec == "ActivityClaim";
    let retried = old.state != "PENDING" && view.state == "PENDING";
    view.terminal += terminal_events(spec, &events, retried);
    let hb = last.map_or_else(|| old_hb.clone(), |r| r.heartbeat_at.clone());
    let actor = parse_actor(actor, task);

    let op = if started {
        Some("start")
    } else if view != old {
        Some("write")
    } else if spec == "ActivityClaim" && hb.is_some() && hb != old_hb {
        // A beat in any state. A beat on a row that no claim holds fails.
        Some("heartbeat")
    } else {
        None
    };
    if let Some(op) = op {
        let mut line = trace_line(op, &view, actor.as_ref());
        if started {
            // The start fence appends ActivityStarted under the worker that
            // the event names.
            let worker = rows
                .iter()
                .find(|r| r.event_type.as_deref() == Some("ActivityStarted"))
                .and_then(|r| r.event_worker.clone());
            line["by"] = json!(worker);
        }
        traces.get_mut(&task).expect("trace").lines.push(line);
    }
    views.insert(task, (view, hb));
}

/// The view of one logged row write.
fn view_of(r: &LogRow, spec: &str, terminal: i32) -> View {
    let mut state = r.state.clone().unwrap_or_default();
    let worker = r.worker_id.clone();
    // A parked workflow task is RUNNING with no worker. No claim holds it.
    if spec == "WorkflowTaskClaim" && state == "RUNNING" && worker.is_none() {
        state = "PENDING".into();
    }
    View {
        state,
        worker,
        attempt: r.attempt.unwrap_or_default(),
        strikes: r.crash_strikes.unwrap_or_default(),
        terminal,
    }
}

/// The number of terminal events in one transaction of a row. `retried`
/// is true when the transaction requeued the row.
fn terminal_events(spec: &str, events: &[&str], retried: bool) -> i32 {
    let counts = |e: &&str| match spec {
        "WorkflowTaskClaim" => WORKFLOW_TERMINALS.contains(e),
        _ => ACTIVITY_FINAL.contains(e) || (ACTIVITY_RESULTS.contains(e) && !retried),
    };
    let n = events.iter().filter(|e| counts(e)).count();
    i32::try_from(n).expect("event count fits i32")
}

/// The claim that `actor` names, when it names a claim of `task`.
fn parse_actor(actor: &str, task: Uuid) -> Option<(String, i32)> {
    let mut parts = actor.splitn(3, '/');
    let named: Uuid = parts.next()?.parse().ok()?;
    let worker = parts.next()?.to_string();
    let attempt = parts.next()?.parse().ok()?;
    (named == task).then_some((worker, attempt))
}

/// One NDJSON step line.
fn trace_line(op: &str, view: &View, actor: Option<&(String, i32)>) -> Value {
    json!({
        "op": op,
        "state": view.state,
        "worker": view.worker,
        "attempt": view.attempt,
        "strikes": view.strikes,
        "terminal": view.terminal,
        "actor": actor.map(|(w, a)| json!({ "worker": w, "attempt": a })),
    })
}

/// Read the log, build the traces and clear the log.
pub async fn take(conn: &mut AsyncPgConnection) -> Vec<TaskTrace> {
    // Scoped here: the trait puts a `first` method on every type.
    use diesel_async::RunQueryDsl;

    let log: Vec<LogRow> = diesel::sql_query(
        "SELECT id, tx::text AS tx, kind, op, task_id, task_type, exec_id, activity_id, \
                state, worker_id, attempt, crash_strikes, heartbeat_at::text AS heartbeat_at, \
                event_type, event_worker, actor \
         FROM harvest_tla_trace ORDER BY id",
    )
    .load(conn)
    .await
    .expect("read the TLA+ trace log");
    conn.batch_execute("TRUNCATE harvest_tla_trace")
        .await
        .expect("clear the TLA+ trace log");
    build(&log)
}

/// The checks of a trace that the fixed engine wrote. The fixed spec must
/// accept it.
pub fn accept() -> Value {
    json!({ "fixed": "accept" })
}

/// Write `traces` to the trace directory. `checks` gives the expected
/// results of a trace.
pub fn write(case: &str, traces: &[TaskTrace], checks: impl Fn(&TaskTrace) -> Value) {
    let Some(dir) = trace_dir() else { return };
    std::fs::create_dir_all(&dir).expect("create the trace dir");
    let stem: String = case
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    // Remove the files of an earlier run of this case, so TLC never checks
    // a stale trace.
    for entry in std::fs::read_dir(&dir)
        .expect("read the trace dir")
        .flatten()
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{stem}-")) && name.ends_with(".ndjson") {
            std::fs::remove_file(entry.path()).expect("remove an old trace");
        }
    }
    for (n, trace) in traces.iter().enumerate() {
        let header = json!({
            "spec": trace.spec,
            "case": format!("{case}, task {}", trace.task_id),
            "checks": checks(trace),
        });
        let mut text = header.to_string();
        for line in &trace.lines {
            text.push('\n');
            text.push_str(&line.to_string());
        }
        text.push('\n');
        let path = dir.join(format!("{stem}-{n:02}-{}.ndjson", trace.spec));
        std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    }
}

/// The checks of a red trace. TLC must reject it at its last line, so a
/// false reject of an earlier line fails the check. `pre_fix` is the
/// expected result of the pre-fix spec.
pub fn reject_last(trace: &TaskTrace, pre_fix: &str) -> Value {
    // Line 1 of the file is the header.
    let last = format!("reject@{}", trace.lines.len() + 1);
    let pre_fix = if pre_fix == "reject" {
        last.clone()
    } else {
        pre_fix.to_string()
    };
    json!({ "fixed": last, "pre-fix": pre_fix })
}

/// Export the traces of one case that the fixed engine wrote, when
/// recording is on. Each trace must be accepted.
///
/// # Panics
///
/// Panics when recording is on and the case wrote no trace, because the
/// export would then check nothing.
pub async fn export(url: &str, case: &str) {
    if trace_dir().is_none() {
        return;
    }
    let mut conn = <AsyncPgConnection as diesel_async::AsyncConnection>::establish(url)
        .await
        .expect("connect to export the TLA+ traces");
    let traces = take(&mut conn).await;
    assert!(!traces.is_empty(), "{case}: no task row trace to export");
    write(case, &traces, |_| accept());
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row write in transaction `tx`.
    fn row(id: i64, tx: &str, op: &str, task: Uuid, kind: &str, state: &str) -> LogRow {
        LogRow {
            id,
            tx: tx.into(),
            kind: "row".into(),
            op: op.into(),
            task_id: Some(task),
            task_type: Some(kind.into()),
            exec_id: Some(Uuid::nil()),
            activity_id: None,
            state: Some(state.into()),
            worker_id: None,
            attempt: Some(0),
            crash_strikes: Some(0),
            heartbeat_at: None,
            event_type: None,
            event_worker: None,
            actor: String::new(),
        }
    }

    /// An event of the run `Uuid::nil()` in transaction `tx`.
    fn event(id: i64, tx: &str, event_type: &str) -> LogRow {
        LogRow {
            kind: "event".into(),
            op: "INSERT".into(),
            task_id: None,
            task_type: None,
            state: None,
            attempt: None,
            crash_strikes: None,
            event_type: Some(event_type.into()),
            ..row(id, tx, "INSERT", Uuid::nil(), "", "")
        }
    }

    fn claimed(mut r: LogRow, worker: &str, attempt: i32) -> LogRow {
        r.worker_id = Some(worker.into());
        r.attempt = Some(attempt);
        r
    }

    #[test]
    fn one_transaction_is_one_line_with_its_terminal_event() {
        let t = Uuid::new_v4();
        let log = vec![
            row(1, "1", "INSERT", t, "workflow", "PENDING"),
            claimed(row(2, "2", "UPDATE", t, "workflow", "RUNNING"), "w", 1),
            event(3, "3", "WorkflowCompleted"),
            claimed(row(4, "3", "UPDATE", t, "workflow", "COMPLETED"), "w", 1),
        ];
        let traces = build(&log);
        let lines = &traces[0].lines;
        assert_eq!(lines.len(), 3, "{lines:#?}");
        assert_eq!(lines[0]["op"], "init");
        assert_eq!(lines[2]["state"], "COMPLETED");
        assert_eq!(lines[2]["terminal"], 1);
    }

    #[test]
    fn a_parked_workflow_row_reads_as_pending() {
        let t = Uuid::new_v4();
        let log = vec![
            row(1, "1", "INSERT", t, "workflow", "PENDING"),
            claimed(row(2, "2", "UPDATE", t, "workflow", "RUNNING"), "w", 1),
            row(3, "3", "UPDATE", t, "workflow", "RUNNING"),
        ];
        let traces = build(&log);
        assert_eq!(traces[0].lines[2]["state"], "PENDING");
    }

    #[test]
    fn an_actor_counts_only_on_its_own_row() {
        let (t, other) = (Uuid::new_v4(), Uuid::new_v4());
        let mut write = claimed(row(2, "2", "UPDATE", t, "workflow", "RUNNING"), "w", 1);
        write.actor = format!("{other}/w/1");
        let log = vec![row(1, "1", "INSERT", t, "workflow", "PENDING"), write];
        assert_eq!(build(&log)[0].lines[1]["actor"], Value::Null);
        assert_eq!(
            parse_actor(&format!("{t}/w/2"), t),
            Some(("w".to_string(), 2))
        );
    }

    #[test]
    fn only_a_failure_that_retries_is_not_terminal() {
        assert_eq!(
            terminal_events("ActivityClaim", &["ActivityFailed"], true),
            0
        );
        assert_eq!(
            terminal_events("ActivityClaim", &["ActivityFailed"], false),
            1
        );
        assert_eq!(
            terminal_events("ActivityClaim", &["ActivityCompleted"], true),
            1
        );
        assert_eq!(
            terminal_events("WorkflowTaskClaim", &["ActivityCompleted"], false),
            0
        );
    }

    /// An activity row of the activity `Uuid::nil()`.
    fn activity(id: i64, tx: &str, op: &str, task: Uuid, state: &str) -> LogRow {
        LogRow {
            activity_id: Some(Uuid::nil()),
            ..row(id, tx, op, task, "activity", state)
        }
    }

    /// An event of the activity `Uuid::nil()`.
    fn activity_event(id: i64, tx: &str, event_type: &str, worker: Option<&str>) -> LogRow {
        LogRow {
            activity_id: Some(Uuid::nil()),
            event_worker: worker.map(String::from),
            ..event(id, tx, event_type)
        }
    }

    #[test]
    fn the_activity_path_gives_start_heartbeat_and_finish_lines() {
        let t = Uuid::new_v4();
        let mut beat = claimed(activity(4, "4", "UPDATE", t, "RUNNING"), "w", 1);
        beat.heartbeat_at = Some("t1".into());
        let log = vec![
            activity(1, "1", "INSERT", t, "PENDING"),
            claimed(activity(2, "2", "UPDATE", t, "RUNNING"), "w", 1),
            activity_event(3, "3", "ActivityStarted", Some("w")),
            beat,
            activity_event(5, "5", "ActivityCompleted", None),
            claimed(activity(6, "5", "UPDATE", t, "COMPLETED"), "w", 1),
        ];
        let lines = &build(&log)[0].lines;
        let ops: Vec<&str> = lines.iter().filter_map(|l| l["op"].as_str()).collect();
        assert_eq!(ops, ["init", "write", "start", "heartbeat", "write"]);
        assert_eq!(lines[2]["by"], "w");
        assert_eq!(lines[4]["terminal"], 1);
    }

    #[test]
    fn transactions_are_ordered_by_their_last_log_id() {
        let t = Uuid::new_v4();
        // Transaction "b" starts first but commits last.
        let log = vec![
            row(1, "a", "INSERT", t, "workflow", "PENDING"),
            event(2, "b", "WorkflowCompleted"),
            claimed(row(3, "c", "UPDATE", t, "workflow", "RUNNING"), "w", 1),
            claimed(row(4, "b", "UPDATE", t, "workflow", "COMPLETED"), "w", 1),
        ];
        let lines = &build(&log)[0].lines;
        assert_eq!(lines[1]["state"], "RUNNING");
        assert_eq!(lines[2]["state"], "COMPLETED");
        assert_eq!(lines[2]["terminal"], 1);
    }

    #[test]
    fn a_red_trace_names_its_last_line() {
        let trace = TaskTrace {
            task_id: Uuid::nil(),
            spec: "ActivityClaim",
            lines: vec![Value::Null; 4],
        };
        assert_eq!(
            reject_last(&trace, "accept"),
            json!({ "fixed": "reject@5", "pre-fix": "accept" })
        );
        assert_eq!(reject_last(&trace, "reject")["pre-fix"], "reject@5");
    }

    #[test]
    #[should_panic(expected = "must start at the row's insert")]
    fn a_trace_must_start_at_the_insert() {
        let t = Uuid::new_v4();
        build(&[row(1, "1", "UPDATE", t, "workflow", "RUNNING")]);
    }

    #[test]
    fn actor_url_encodes_the_setting() {
        let t = Uuid::nil();
        let url = actor_url("postgres://h/db", t, "w-1", 2);
        assert_eq!(
            url,
            format!("postgres://h/db?options=-c%20harvest.trace_actor%3D{t}%2Fw-1%2F2")
        );
        assert!(actor_url("postgres://h/db?x=1", t, "w", 1).contains("?x=1&options="));
    }
}
