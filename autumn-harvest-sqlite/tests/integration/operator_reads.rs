//! Issue #2468: the reads behind the ported management routes.
//!
//! The SQLite plugin serves count, children, tree, run chain, timeline,
//! stack, logs and awaitables from these `SqliteReader` reads. The log sink
//! and the declarative queries are runtime features of the same issue.

// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]
// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[query]` handler takes its args by value and returns a `Result`.
#![allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]

use std::path::Path;
use std::time::Duration;

use autumn_harvest::context::WorkflowLogPolicy;
use autumn_harvest::executor::{QueryReplayOutcome, drive_query_replay_async};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use autumn_harvest::{ParentClosePolicy, WorkflowCommand, WorkflowHistoryPolicy};
use autumn_harvest_sqlite::{
    CountFilter, LogFilter, RunState, SqliteError, SqliteReader, SqliteRuntime,
};
use serde_json::{Value, json};

#[workflow]
async fn leaf(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Spawns one awaited and one detached child, then waits for both signals.
#[workflow]
async fn parent(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.spawn_child_workflow_detached_raw("leaf", json!(2), ParentClosePolicy::Abandon)
        .map_err(|e| e.to_string())?;
    ctx.spawn_child_workflow_raw("leaf", json!(1))
        .await
        .map_err(|e| e.to_string())
}

#[activity(retry = RetryPolicy::fixed(3, Duration::from_secs(3600)))]
async fn down(_ctx: &ActivityContext, _n: i64) -> Result<i64, String> {
    Err("down".to_string())
}

/// Arms a timer, then waits on an activity that fails once and backs off.
#[workflow]
async fn busy(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    let info = down_info();
    let (nap, out) = tokio::join!(
        ctx.timer("nap", 3600),
        ctx.execute_activity::<i64, i64>(&info, n)
    );
    nap.map_err(|e| e.to_string())?;
    out.map_err(|e| e.to_string())
}

/// Logs three lines, then waits for `go`.
#[workflow]
async fn chatty(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.log_info("one");
    ctx.log_warn("two");
    ctx.log_error("three");
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

#[workflow]
async fn declarative(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// A declarative query of `declarative`. It reads the run's workflow id.
#[query(workflow = "declarative")]
fn phase(ctx: &WorkflowContext, suffix: String) -> Result<String, String> {
    Ok(format!("{}{suffix}", ctx.workflow_id()))
}

/// A query of another workflow. It must not answer on `declarative`.
#[query(workflow = "leaf")]
fn leaf_only(_ctx: &WorkflowContext) -> Result<String, String> {
    Ok("leaf".to_string())
}

fn runtime(db: &Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    for info in [
        leaf_info(),
        parent_info(),
        busy_info(),
        chatty_info(),
        declarative_info(),
    ] {
        rt.register_workflow(&info);
    }
    rt.register_activity(&down_info());
    rt
}

#[tokio::test]
async fn a_count_groups_by_state_and_name_and_applies_the_filters() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let first = rt.start_workflow("leaf", json!(0)).unwrap();
    rt.start_workflow("leaf", json!(0)).unwrap();
    rt.start_workflow("chatty", json!(0)).unwrap();
    rt.terminate(first, "stop").unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let mut rows = reader.count_executions(&CountFilter::default()).unwrap();
    rows.sort_by(|a, b| (&a.state, &a.workflow_name).cmp(&(&b.state, &b.workflow_name)));
    let flat: Vec<(&str, &str, i64)> = rows
        .iter()
        .map(|row| (row.state.as_str(), row.workflow_name.as_str(), row.count))
        .collect();
    assert_eq!(
        flat,
        [
            ("RUNNING", "chatty", 1),
            ("RUNNING", "leaf", 1),
            ("TERMINATED", "leaf", 1)
        ]
    );

    let filter = CountFilter {
        workflow_name: Some("leaf".to_string()),
        states: vec!["RUNNING".to_string()],
        ..CountFilter::default()
    };
    let rows = reader.count_executions(&filter).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].count, 1);

    let future = CountFilter {
        started_after: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
        ..CountFilter::default()
    };
    assert_eq!(reader.count_executions(&future).unwrap().len(), 0);
}

#[tokio::test]
async fn children_name_their_parent_and_their_close_policy() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let root = rt.start_workflow("parent", json!(0)).unwrap();
    let state = rt.run_until_blocked(root).await.unwrap();
    assert!(matches!(state, RunState::WaitingWorkflow(_)), "{state:?}");

    let reader = SqliteReader::open(&db).unwrap();
    let children = reader.children(&[root], None).unwrap();
    assert_eq!(children.len(), 2, "{children:?}");
    for child in &children {
        assert_eq!(child.parent, root);
        assert_eq!(child.workflow_name, "leaf");
        assert_eq!(child.state, "RUNNING");
        assert!(child.started_at.is_some());
    }
    let detached = children.iter().filter(|c| c.detached).count();
    assert_eq!(detached, 1, "one child is detached: {children:?}");

    let leaves: Vec<ExecutionId> = children.iter().map(|c| c.exec_id).collect();
    assert_eq!(reader.parents_with_children(&[root]).unwrap(), [root]);
    assert_eq!(reader.parents_with_children(&leaves).unwrap().len(), 0);
    assert_eq!(reader.children(&[root], Some(1)).unwrap().len(), 1);
    assert_eq!(reader.children(&[], None).unwrap().len(), 0);
}

#[tokio::test]
async fn the_timed_history_pairs_each_event_with_its_append_time() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = rt.start_workflow("leaf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.send_signal(exec, "go", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let timed = reader.timed_history(exec).unwrap();
    let plain = reader.load_history(exec).unwrap();
    assert_eq!(timed.len(), plain.len());
    assert!(timed.iter().all(|row| row.recorded_at.is_some()));
    assert_eq!(timed[0].event.type_name(), "WorkflowStarted");
    let unknown = reader.timed_history(ExecutionId::new()).unwrap_err();
    assert!(matches!(unknown, SqliteError::ExecutionNotFound(_)));
}

#[tokio::test]
async fn pending_work_lists_tasks_timers_and_buffered_signals() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = rt.start_workflow("busy", json!(5)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.send_signal(exec, "later", json!(1)).unwrap();
    rt.send_signal(exec, "later", json!(2)).unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let work = reader.pending_work(exec).unwrap();
    assert_eq!(work.tasks.len(), 1, "{work:?}");
    let task = &work.tasks[0];
    assert_eq!(task.name, "down");
    assert_eq!(task.state, "PENDING");
    assert_eq!(task.attempt, 1, "one attempt was consumed");
    assert_eq!(task.max_attempts, Some(3));
    assert_eq!(task.last_error.as_deref(), Some("down"));
    assert!(task.run_at > chrono::Utc::now(), "the retry waits");
    assert_eq!(work.timers.len(), 1, "{work:?}");
    assert_eq!(work.timers[0].timer_id, "nap");
    assert_eq!(work.signals.len(), 1, "{work:?}");
    assert_eq!(work.signals[0].name, "later");
    assert_eq!(work.signals[0].count, 2);
}

#[tokio::test]
async fn the_log_sink_stores_each_line_once_and_reads_by_filter() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    rt.set_workflow_log_policy(Some(WorkflowLogPolicy::new()));
    let exec = rt.start_workflow("chatty", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    // A second drive at the same position re-emits no line.
    rt.run_until_blocked(exec).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let lines = reader.workflow_logs(exec, &LogFilter::new(100)).unwrap();
    let text: Vec<(&str, &str)> = lines
        .iter()
        .map(|line| (line.level.as_str(), line.message.as_str()))
        .collect();
    assert_eq!(text, [("info", "one"), ("warn", "two"), ("error", "three")]);
    assert!(lines.windows(2).all(|pair| pair[0].seq < pair[1].seq));
    assert_eq!(reader.count_workflow_logs(exec).unwrap(), 3);
    assert!(!reader.workflow_log_truncated(exec).unwrap());

    let errors = LogFilter {
        levels: vec!["error".to_string()],
        ..LogFilter::new(100)
    };
    assert_eq!(reader.workflow_logs(exec, &errors).unwrap().len(), 1);
    let after_first = LogFilter {
        after_seq: Some(lines[0].seq),
        ..LogFilter::new(1)
    };
    let page = reader.workflow_logs(exec, &after_first).unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].message, "two");
}

#[tokio::test]
async fn the_log_cap_drops_the_newest_and_records_one_marker() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    rt.set_workflow_log_policy(Some(WorkflowLogPolicy::new().with_max_lines(2)));
    let exec = rt.start_workflow("chatty", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let lines = reader.workflow_logs(exec, &LogFilter::new(100)).unwrap();
    let text: Vec<&str> = lines.iter().map(|line| line.message.as_str()).collect();
    assert_eq!(text.len(), 3, "two lines and the marker: {text:?}");
    assert_eq!(&text[..2], ["one", "two"]);
    assert_eq!(lines[2].seq, i64::MAX);
    assert_eq!(lines[2].level, "warn");
    assert_eq!(reader.count_workflow_logs(exec).unwrap(), 2);
    assert!(reader.workflow_log_truncated(exec).unwrap());
}

#[tokio::test]
async fn no_policy_stores_no_line_and_a_purge_removes_the_lines() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let quiet = rt.start_workflow("chatty", json!(0)).unwrap();
    rt.run_until_blocked(quiet).await.unwrap();
    rt.set_workflow_log_policy(Some(WorkflowLogPolicy::new()));
    let logged = rt.start_workflow("chatty", json!(0)).unwrap();
    rt.run_until_blocked(logged).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    assert_eq!(reader.count_workflow_logs(quiet).unwrap(), 0);
    assert_eq!(reader.count_workflow_logs(logged).unwrap(), 3);

    rt.terminate(logged, "stop").unwrap();
    let later = chrono::Utc::now() + chrono::Duration::days(2);
    rt.purge_terminal_as_of(later, Duration::from_secs(60))
        .unwrap();
    assert_eq!(reader.count_workflow_logs(logged).unwrap(), 0);
}

#[tokio::test]
async fn a_declarative_query_answers_on_its_own_workflow_only() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    for info in queries![phase, leaf_only] {
        rt.register_query(&info);
    }
    let exec = rt
        .start_workflow_with_id("declarative", "order-7", json!(0))
        .unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let answer = rt.query(exec, "phase", json!("!")).await.unwrap();
    assert_eq!(answer, json!("order-7!"));
    let foreign = rt.query(exec, "leaf_only", Value::Null).await.unwrap_err();
    assert!(
        matches!(foreign, SqliteError::QueryNotFound { .. }),
        "{foreign:?}"
    );

    // A terminal run still answers from its recorded end.
    rt.send_signal(exec, "go", json!(1)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let answer = rt.query(exec, "phase", json!("?")).await.unwrap();
    assert_eq!(answer, json!("order-7?"));
}

#[tokio::test]
async fn a_replay_input_rebuilds_the_wait_set_of_a_parked_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = rt.start_workflow("leaf", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let input = reader.replay_input(exec).unwrap();
    assert_eq!(input.workflow_name(), "leaf");
    assert!(!input.is_terminal());
    let (ctx, run_input) = input.into_context(
        autumn_harvest::context::empty_shared_state(),
        WorkflowHistoryPolicy::default(),
    );
    let outcome =
        drive_query_replay_async(&ctx, leaf_info().handler, run_input, Duration::from_secs(5))
            .await;
    assert_eq!(outcome, QueryReplayOutcome::Suspended);
    let waits_on_go = ctx.drain_commands().iter().any(|command| {
        matches!(command, WorkflowCommand::WaitForSignal { signal_name, .. } if signal_name == "go")
    });
    assert!(waits_on_go, "the replay parks on the signal");
}

/// A log write is best effort, as on Postgres. A failed write drops the
/// lines of the cycle and never fails the cycle.
#[tokio::test]
async fn a_failed_log_write_does_not_fail_the_cycle() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    rt.set_workflow_log_policy(Some(WorkflowLogPolicy::new()));
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("DROP TABLE harvest_workflow_logs")
        .unwrap();
    let exec = rt.start_workflow("chatty", json!(0)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::WaitingSignal(_)), "{state:?}");
    rt.send_signal(exec, "go", json!(1)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
}
