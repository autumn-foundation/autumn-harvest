//! The engine contract of a durable Rhai script, driven in process.

use std::path::Path;
use std::sync::{Arc, Mutex};

use autumn_harvest::{ExecutionId, WorkflowEvent};
use autumn_harvest_sqlite::{LogFilter, RunState, SqliteReader, SqliteRuntime};
use chrono::{DateTime, TimeDelta, Utc};
use rhai_scripts::bridge::MAX_OPERATIONS;
use rhai_scripts::{Echo, ScriptInput, WORKFLOW};
use serde_json::{Value, json};
use tempfile::TempDir;

/// A scratch directory. It holds the database and is the script's `cwd`.
fn scratch() -> TempDir {
    tempfile::tempdir().expect("a temp dir")
}

/// The database path inside `dir`.
fn db(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("runs.db")
}

/// The workflow input of `source`, run in `cwd`.
fn input(source: &str, cwd: &Path, args: &[&str], max_operations: u64) -> Value {
    serde_json::to_value(ScriptInput {
        path: "test.rhai".to_string(),
        source: source.to_string(),
        args: args.iter().map(ToString::to_string).collect(),
        cwd: cwd.display().to_string(),
        max_operations,
    })
    .expect("the input serializes")
}

/// Start `source` in `dir` with no arguments.
fn start(rt: &mut SqliteRuntime, dir: &TempDir, source: &str) -> ExecutionId {
    rt.start_workflow(WORKFLOW, input(source, dir.path(), &[], MAX_OPERATIONS))
        .expect("the run starts")
}

/// Drive `exec` and expect it to complete.
async fn completes(rt: &mut SqliteRuntime, exec: ExecutionId) -> Value {
    match rt
        .run_until_blocked(exec)
        .await
        .expect("the drive succeeds")
    {
        RunState::Completed(value) => value,
        other => panic!("expected completion, got {other:?}"),
    }
}

/// Drive `exec` and expect it to fail.
async fn fails(rt: &mut SqliteRuntime, exec: ExecutionId) -> String {
    match rt
        .run_until_blocked(exec)
        .await
        .expect("the drive succeeds")
    {
        RunState::Failed(error) => error,
        other => panic!("expected a failure, got {other:?}"),
    }
}

/// The names of the activities in the history of `exec`, in order.
fn scheduled(path: &Path, exec: ExecutionId) -> Vec<String> {
    SqliteReader::open(path)
        .expect("a reader")
        .load_history(exec)
        .expect("the history")
        .into_iter()
        .filter_map(|event| match event {
            WorkflowEvent::ActivityScheduled { name, .. } => Some(name),
            _ => None,
        })
        .collect()
}

/// The lines of `name` in `dir`.
fn lines(dir: &TempDir, name: &str) -> Vec<String> {
    std::fs::read_to_string(dir.path().join(name))
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

#[tokio::test]
async fn host_calls_and_arguments_reach_the_script_value() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let source = r#"
        let a = sh("echo hi");
        write_file("nested/out.txt", a + "!");
        let b = read_file("nested/out.txt");
        #{ a: a, b: b, first: ARGS[0], unset: env("RHAI_SCRIPTS_TEST_UNSET_VAR") }
    "#;
    let exec = rt
        .start_workflow(
            WORKFLOW,
            input(source, dir.path(), &["one"], MAX_OPERATIONS),
        )
        .expect("start");

    let value = completes(&mut rt, exec).await;
    assert_eq!(
        value,
        json!({ "a": "hi", "b": "hi!", "first": "one", "unset": null })
    );
    assert_eq!(
        scheduled(&db(&dir), exec),
        ["rhai.sh", "rhai.write_file", "rhai.read_file", "rhai.env"]
    );
}

#[tokio::test]
async fn each_step_runs_once_across_a_signal_wait_and_a_restart() {
    let dir = scratch();
    let source = r#"
        sh("echo one >> steps.log");
        let who = wait_for("approve");
        sh(`echo two-${who} >> steps.log`);
        who
    "#;
    let exec = {
        let mut rt = rhai_scripts::open(db(&dir)).expect("open");
        let exec = start(&mut rt, &dir, source);
        let state = rt.run_until_blocked(exec).await.expect("drive");
        assert!(
            matches!(state, RunState::WaitingSignal(ref s) if s == "approve"),
            "got {state:?}"
        );
        exec
    };

    // A new runtime on the same file is a restart.
    let mut rt = rhai_scripts::open(db(&dir)).expect("reopen");
    rt.send_signal(exec, "approve", json!("ann"))
        .expect("signal");
    assert_eq!(completes(&mut rt, exec).await, json!("ann"));
    assert_eq!(lines(&dir, "steps.log"), ["one", "two-ann"]);
    assert_eq!(scheduled(&db(&dir), exec), ["rhai.sh", "rhai.sh"]);
}

#[tokio::test]
async fn sleep_is_a_durable_timer() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let exec = start(&mut rt, &dir, r#"sleep(60); sh("echo after")"#);
    let t0 = DateTime::<Utc>::from_timestamp(1_800_000_000, 0).expect("a time");

    let state = rt.run_until_blocked_as_of(exec, t0).await.expect("drive");
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    let early = t0 + TimeDelta::seconds(30);
    let state = rt
        .run_until_blocked_as_of(exec, early)
        .await
        .expect("drive");
    assert!(matches!(state, RunState::WaitingTimer), "got {state:?}");
    assert!(scheduled(&db(&dir), exec).is_empty(), "no step runs early");

    let due = t0 + TimeDelta::seconds(61);
    let state = rt.run_until_blocked_as_of(exec, due).await.expect("drive");
    assert!(
        matches!(state, RunState::Completed(ref v) if v == &json!("after")),
        "got {state:?}"
    );
}

#[tokio::test]
async fn a_failed_call_is_catchable_and_recorded() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let source = r#"
        let out = "none";
        try {
            sh("echo boom >&2; exit 7");
        } catch (err) {
            out = `caught: ${err}`;
        }
        out
    "#;
    let exec = start(&mut rt, &dir, source);

    let value = completes(&mut rt, exec).await;
    let text = value.as_str().expect("a string");
    assert!(text.starts_with("caught: "), "got {text}");
    assert!(text.contains("exit code 7"), "got {text}");
    let failed = SqliteReader::open(db(&dir))
        .expect("reader")
        .load_history(exec)
        .expect("history")
        .into_iter()
        .any(|event| matches!(event, WorkflowEvent::ActivityFailed { .. }));
    assert!(failed, "the failure is in history");
}

#[tokio::test]
async fn an_uncaught_failure_fails_the_run() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let exec = start(&mut rt, &dir, r#"sh("exit 3"); sh("echo never")"#);

    let error = fails(&mut rt, exec).await;
    assert!(error.contains("exit code 3"), "got {error}");
    assert_eq!(scheduled(&db(&dir), exec), ["rhai.sh"]);
}

#[tokio::test]
async fn the_sandbox_has_no_clock_and_no_imports() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");

    let clock = start(&mut rt, &dir, "timestamp()");
    let error = fails(&mut rt, clock).await;
    assert!(error.contains("timestamp"), "got {error}");

    let import = start(&mut rt, &dir, r#"import "helpers" as h; 1"#);
    let error = fails(&mut rt, import).await;
    assert!(error.contains("helpers"), "got {error}");
}

#[tokio::test]
async fn a_runaway_loop_fails_at_the_operation_bound() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let exec = rt
        .start_workflow(WORKFLOW, input("loop {}", dir.path(), &[], 10_000))
        .expect("start");

    let error = fails(&mut rt, exec).await;
    assert!(error.to_lowercase().contains("operations"), "got {error}");
}

#[tokio::test]
async fn a_print_reaches_the_sink_and_the_log_once() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let sink = Arc::clone(&seen);
    rhai_scripts::set_echo(
        &mut rt,
        Echo(Arc::new(move |line| {
            sink.lock().expect("sink").push(line.to_string());
        })),
    );
    let source = r#"
        print("a");
        sh("true");
        print("b");
        wait_for("go");
        print("c");
    "#;
    let exec = start(&mut rt, &dir, source);
    let state = rt.run_until_blocked(exec).await.expect("drive");
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");
    rt.send_signal(exec, "go", Value::Null).expect("signal");
    completes(&mut rt, exec).await;

    assert_eq!(*seen.lock().expect("sink"), ["a", "b", "c"]);
    let logged: Vec<String> = SqliteReader::open(db(&dir))
        .expect("reader")
        .workflow_logs(exec, &LogFilter::new(100))
        .expect("logs")
        .into_iter()
        .map(|line| line.message)
        .collect();
    assert_eq!(logged, ["a", "b", "c"]);
}

#[tokio::test]
async fn the_status_query_reports_progress_on_a_parked_run() {
    let dir = scratch();
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let source = r#"
        progress("stage one");
        sh("true");
        progress("stage two");
        wait_for("go");
    "#;
    let exec = start(&mut rt, &dir, source);
    let state = rt.run_until_blocked(exec).await.expect("drive");
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");

    let status = rt.query(exec, "status", Value::Null).await.expect("query");
    assert_eq!(status, json!({ "progress": "stage two", "calls": 2 }));
    assert_eq!(
        scheduled(&db(&dir), exec),
        ["rhai.sh"],
        "a query runs no step"
    );
}

#[tokio::test]
async fn a_parse_error_is_caught_before_a_run_starts() {
    let error = rhai_scripts::bridge::check("let x = ;").expect_err("a parse error");
    assert_ne!(error, "");
    rhai_scripts::bridge::check(r#"sh("true")"#).expect("a valid script parses");
}

#[tokio::test]
async fn status_reads_progress_while_the_writer_holds_the_database() {
    let dir = scratch();
    // This runtime keeps the writer lock for the whole test, as a live `run` does.
    let mut rt = rhai_scripts::open(db(&dir)).expect("open");
    let source = r#"
        progress("deploying");
        sh("true");
        wait_for("go");
    "#;
    let exec = start(&mut rt, &dir, source);
    let state = rt.run_until_blocked(exec).await.expect("drive");
    assert!(matches!(state, RunState::WaitingSignal(_)), "got {state:?}");

    // A second writer is refused, so the read must not need one.
    assert!(
        rhai_scripts::open(db(&dir)).is_err(),
        "the writer lock is held"
    );
    // `calls` counts the `wait_for` that the run is parked on.
    let status = rhai_scripts::status(db(&dir), exec).await.expect("status");
    assert_eq!(status, json!({ "progress": "deploying", "calls": 2 }));

    // The read changes nothing: the writer drives the run on as before.
    rt.send_signal(exec, "go", Value::Null).expect("signal");
    completes(&mut rt, exec).await;
    let done = rhai_scripts::status(db(&dir), exec).await.expect("status");
    assert_eq!(done, json!({ "progress": "deploying", "calls": 2 }));
}
