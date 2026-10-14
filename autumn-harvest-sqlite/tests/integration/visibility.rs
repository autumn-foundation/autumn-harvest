//! Issue #2027: list and describe executions, search attributes and memo.

// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]
// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]

use std::path::Path;
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    ExecutionFilter, ExecutionOutcome, ListCursor, MAX_LIST_LIMIT, RunState, SqliteError,
    SqliteReader, SqliteRuntime, StartOptions, Stored,
};
use chrono::Utc;
use rusqlite::Connection;
use serde_json::{Map, Value, json};

// ── Test workflows ───────────────────────────────────────────────────────────

/// Completes with its input.
#[workflow]
async fn echo(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// Fails with a fixed reason.
#[workflow]
async fn fails(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    Err("it broke".to_string())
}

/// Waits for `go`.
#[workflow]
async fn waits(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Upserts a stage, waits for `go`, then upserts again in the terminal cycle.
#[workflow]
async fn staged(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    ctx.upsert_search_attrs([
        ("stage".to_string(), Some(json!("waiting"))),
        ("count".to_string(), Some(json!(n))),
        ("urgent".to_string(), Some(json!(true))),
        ("scratch".to_string(), Some(json!("x"))),
    ])
    .map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    ctx.upsert_search_attrs([
        ("stage".to_string(), Some(json!("done"))),
        ("scratch".to_string(), None),
    ])
    .map_err(|e| e.to_string())?;
    Ok(n)
}

/// Upserts, then reaches an unsupported command in the same cycle.
#[workflow]
async fn upserts_then_spawns(ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    ctx.upsert_search_attrs([("stage".to_string(), Some(json!("lost")))])
        .map_err(|e| e.to_string())?;
    ctx.spawn_child_workflow_detached_raw(
        "child",
        json!({}),
        autumn_harvest::ParentClosePolicy::Abandon,
    )
    .map_err(|e| e.to_string())?;
    Ok(0)
}

/// Upserts its input once, then continues as new until the input is 2.
#[workflow]
async fn chained(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    if n == 0 {
        ctx.upsert_search_attrs([("generation".to_string(), Some(json!("first")))])
            .map_err(|e| e.to_string())?;
    }
    if n < 2 {
        ctx.continue_as_new(json!(n + 1))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(n)
}

fn runtime() -> SqliteRuntime {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    for info in [
        echo_info(),
        fails_info(),
        waits_info(),
        staged_info(),
        upserts_then_spawns_info(),
        chained_info(),
    ] {
        rt.register_workflow(&info);
    }
    rt
}

fn file_runtime(db: &Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    rt.register_workflow(&echo_info());
    rt.register_workflow(&waits_info());
    rt
}

fn attrs(pairs: &[(&str, Value)]) -> Map<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

/// The empty id list, for `assert_eq!`.
const NONE: Vec<ExecutionId> = Vec::new();

fn ids(rt: &SqliteRuntime, filter: &ExecutionFilter) -> Vec<ExecutionId> {
    rt.list_executions(filter)
        .unwrap()
        .executions
        .iter()
        .map(|row| row.execution_id().expect("a readable id"))
        .collect()
}

// ── Describe ─────────────────────────────────────────────────────────────────

/// AC1: describe carries each field the daemon reads. The daemon reads the
/// id, the name, the state, the input, the output, the error and the cursor.
#[tokio::test]
async fn describe_returns_every_field_the_daemon_reads() {
    let mut rt = runtime();
    let before = Utc::now();
    let started = rt
        .start_workflow_with_options(
            "echo",
            json!(7),
            StartOptions {
                workflow_id: Some("order-7".to_string()),
                memo: Some(json!({ "owner": "ops" })),
                search_attributes: attrs(&[("region", json!("eu"))]),
                ..StartOptions::default()
            },
        )
        .unwrap();
    assert!(started.created);
    let exec = started.exec_id;
    rt.run_until_blocked(exec).await.unwrap();

    let row = rt.describe(exec).unwrap();
    let summary = &row.summary;
    assert_eq!(summary.exec_id, Stored::Value(exec.to_string()));
    assert_eq!(summary.workflow_name, Stored::Value("echo".to_string()));
    assert_eq!(summary.workflow_id, Stored::Value("order-7".to_string()));
    assert_eq!(summary.state, Stored::Value("COMPLETED".to_string()));
    assert_eq!(row.input_json, Stored::Value("7".to_string()));
    assert_eq!(row.output_json, Stored::Value("7".to_string()));
    assert_eq!(row.error, Stored::Null);
    assert_eq!(summary.memo, Stored::Value(json!({ "owner": "ops" })));
    assert_eq!(
        summary.search_attributes,
        Stored::Value(attrs(&[("region", json!("eu"))]))
    );
    let started_at = *summary.started_at.value().expect("a start time");
    let closed_at = *summary.closed_at.value().expect("a close time");
    assert!(started_at >= before - chrono::TimeDelta::milliseconds(1));
    assert!(closed_at >= started_at);
    assert!(closed_at <= Utc::now());

    // The listed row is the same row, without the payloads.
    let page = rt
        .list_executions(&ExecutionFilter::new().exec_id(exec.to_string()))
        .unwrap();
    assert_eq!(page.executions, vec![summary.clone()]);
    assert_eq!(page.next, None);
}

/// The start time equals the `WorkflowStarted` timestamp.
#[tokio::test]
async fn the_start_time_is_the_started_event_time() {
    let mut rt = runtime();
    let exec = rt.start_workflow("echo", json!(1)).unwrap();
    let history = rt.load_history(exec).unwrap();
    let Some(WorkflowEvent::WorkflowStarted { timestamp, .. }) = history.first() else {
        panic!("the history starts with WorkflowStarted: {history:?}");
    };
    let row = rt.describe(exec).unwrap();
    assert_eq!(
        row.summary
            .started_at
            .value()
            .map(chrono::DateTime::timestamp_millis),
        Some(timestamp.timestamp_millis())
    );
    assert_eq!(row.summary.closed_at, Stored::Null, "a running run is open");
    assert_eq!(row.summary.search_attributes, Stored::Null);
    assert_eq!(row.summary.memo, Stored::Null);
}

#[tokio::test]
async fn describe_names_the_failure() {
    let mut rt = runtime();
    let exec = rt.start_workflow("fails", json!(0)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    let row = rt.describe(exec).unwrap();
    assert_eq!(row.summary.state, Stored::Value("FAILED".to_string()));
    assert_eq!(row.error, Stored::Value("it broke".to_string()));
    assert_eq!(row.output_json, Stored::Null);
    assert!(row.summary.closed_at.value().is_some());
}

/// A cancel and a terminate set the close time too.
#[tokio::test]
async fn every_terminal_write_sets_the_close_time() {
    let mut rt = runtime();
    let cancelled = rt.start_workflow("waits", json!(0)).unwrap();
    let terminated = rt.start_workflow("waits", json!(0)).unwrap();
    rt.run_until_blocked(cancelled).await.unwrap();
    rt.run_until_blocked(terminated).await.unwrap();
    rt.cancel(cancelled, "stop").unwrap();
    rt.run_until_blocked(cancelled).await.unwrap();
    rt.terminate(terminated, "kill").unwrap();
    for exec in [cancelled, terminated] {
        let row = rt.describe(exec).unwrap();
        assert!(
            row.summary.closed_at.value().is_some(),
            "{:?} has no close time",
            row.summary.state
        );
    }
}

#[test]
fn describe_of_an_unknown_id_is_not_found() {
    let rt = runtime();
    let exec = ExecutionId::new();
    assert!(matches!(
        rt.describe(exec),
        Err(SqliteError::ExecutionNotFound(id)) if id == exec
    ));
}

// ── List ─────────────────────────────────────────────────────────────────────

/// AC1: the list pages newest first, and the cursor reaches every row.
#[test]
fn list_pages_newest_first_with_a_cursor() {
    let mut rt = runtime();
    let started: Vec<ExecutionId> = (0..5)
        .map(|n| rt.start_workflow("echo", json!(n)).unwrap())
        .collect();

    let mut seen = Vec::new();
    let mut filter = ExecutionFilter::new().limit(2);
    let mut pages = 0;
    loop {
        let page = rt.list_executions(&filter).unwrap();
        pages += 1;
        assert!(page.executions.len() <= 2);
        seen.extend(page.executions.iter().map(|r| r.execution_id().unwrap()));
        match page.next {
            Some(cursor) => {
                assert_eq!(cursor, page.executions.last().unwrap().cursor);
                filter = filter.before(cursor);
            }
            None => break,
        }
    }
    assert_eq!(pages, 3);
    let newest_first: Vec<ExecutionId> = started.into_iter().rev().collect();
    assert_eq!(seen, newest_first);
}

/// A full last page says that no row is left.
#[test]
fn an_exact_last_page_has_no_next_cursor() {
    let mut rt = runtime();
    for n in 0..2 {
        rt.start_workflow("echo", json!(n)).unwrap();
    }
    let page = rt
        .list_executions(&ExecutionFilter::new().limit(2))
        .unwrap();
    assert_eq!(page.executions.len(), 2);
    assert_eq!(page.next, None);
}

/// A cursor survives a round trip through its raw value.
#[test]
fn a_cursor_round_trips_through_its_raw_value() {
    let mut rt = runtime();
    for n in 0..3 {
        rt.start_workflow("echo", json!(n)).unwrap();
    }
    let first = rt
        .list_executions(&ExecutionFilter::new().limit(1))
        .unwrap();
    let raw = first.next.unwrap().raw();
    let second = rt
        .list_executions(
            &ExecutionFilter::new()
                .limit(1)
                .before(ListCursor::from_raw(raw)),
        )
        .unwrap();
    assert_ne!(first.executions, second.executions);
    assert_eq!(second.executions.len(), 1);
}

#[test]
fn a_limit_over_the_maximum_is_cut() {
    let rt = runtime();
    let page = rt
        .list_executions(&ExecutionFilter::new().limit(u32::MAX))
        .unwrap();
    assert!(page.executions.len() <= MAX_LIST_LIMIT as usize);
}

#[tokio::test]
async fn list_filters_by_state_name_and_id_prefix() {
    let mut rt = runtime();
    let start = |rt: &mut SqliteRuntime, name: &str, id: &str| {
        rt.start_workflow_with_id(name, id, json!(1)).unwrap()
    };
    let done = start(&mut rt, "echo", "order-1");
    let open = start(&mut rt, "waits", "order-2");
    let other = start(&mut rt, "waits", "invoice-1");
    rt.run_until_blocked(done).await.unwrap();
    rt.run_until_blocked(open).await.unwrap();

    assert_eq!(
        ids(&rt, &ExecutionFilter::new().state("COMPLETED")),
        vec![done]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().state("RUNNING")),
        vec![other, open]
    );
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new().state("COMPLETED").state("RUNNING")
        ),
        vec![other, open, done]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().workflow_name("waits")),
        vec![other, open]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().workflow_id_prefix("order-")),
        vec![open, done]
    );
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new()
                .workflow_name("waits")
                .workflow_id_prefix("order-")
        ),
        vec![open]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().exec_id(done.to_string())),
        vec![done]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().workflow_name("nope")),
        NONE
    );
}

/// A prefix holds no pattern. `%` and `_` match only themselves.
#[test]
fn a_workflow_id_prefix_is_not_a_pattern() {
    let mut rt = runtime();
    let literal = rt.start_workflow_with_id("echo", "a%b", json!(1)).unwrap();
    rt.start_workflow_with_id("echo", "axb", json!(1)).unwrap();
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().workflow_id_prefix("a%")),
        vec![literal]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().workflow_id_prefix("a_")),
        NONE
    );
}

#[test]
fn list_filters_by_start_time() {
    let mut rt = runtime();
    let early = rt.start_workflow("echo", json!(1)).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    let middle = Utc::now();
    std::thread::sleep(Duration::from_millis(5));
    let late = rt.start_workflow("echo", json!(2)).unwrap();

    assert_eq!(
        ids(&rt, &ExecutionFilter::new().started_after(middle)),
        vec![late]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().started_before(middle)),
        vec![early]
    );
    let late_at = *rt
        .describe(late)
        .unwrap()
        .summary
        .started_at
        .value()
        .unwrap();
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().started_after(late_at)),
        vec![late],
        "the lower bound is inclusive"
    );
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new().started_before(early_at(&rt, early))
        ),
        NONE,
        "the upper bound is exclusive"
    );
}

fn early_at(rt: &SqliteRuntime, exec: ExecutionId) -> chrono::DateTime<Utc> {
    *rt.describe(exec)
        .unwrap()
        .summary
        .started_at
        .value()
        .unwrap()
}

/// `visit_executions` passes each row with its payloads, newest first.
#[tokio::test]
async fn visit_passes_each_row_with_its_payloads() {
    let mut rt = runtime();
    let first = rt.start_workflow("echo", json!(1)).unwrap();
    let second = rt.start_workflow("echo", json!(2)).unwrap();
    rt.run_until_idle().await.unwrap();

    let mut rows = Vec::new();
    let next = rt
        .visit_executions(&ExecutionFilter::new().limit(1), |row| rows.push(row))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].summary.execution_id(), Some(second));
    assert_eq!(rows[0].output_json, Stored::Value("2".to_string()));

    let mut rest = Vec::new();
    let end = rt
        .visit_executions(&ExecutionFilter::new().before(next.unwrap()), |row| {
            rest.push(row);
        })
        .unwrap();
    assert_eq!(end, None);
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].summary.execution_id(), Some(first));
    assert_eq!(rest[0].input_json, Stored::Value("1".to_string()));
}

// ── Search attributes ────────────────────────────────────────────────────────

/// AC2: a workflow upserts search attributes, and the list filters on them.
#[tokio::test]
async fn upserted_search_attributes_filter_the_list() {
    let mut rt = runtime();
    let exec = rt.start_workflow("staged", json!(3)).unwrap();
    let quiet = rt.start_workflow("waits", json!(0)).unwrap();

    // An upsert is not progress. The drive parks on the signal and does not
    // spin.
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(
        matches!(state, RunState::WaitingSignal(ref s) if s == "go"),
        "{state:?}"
    );
    rt.run_until_blocked(quiet).await.unwrap();

    assert_eq!(
        rt.describe(exec).unwrap().summary.search_attributes,
        Stored::Value(attrs(&[
            ("stage", json!("waiting")),
            ("count", json!(3)),
            ("urgent", json!(true)),
            ("scratch", json!("x")),
        ]))
    );
    let by = |key: &str, value: Value| ExecutionFilter::new().search_attribute(key, value);
    assert_eq!(ids(&rt, &by("stage", json!("waiting"))), vec![exec]);
    assert_eq!(ids(&rt, &by("count", json!(3))), vec![exec]);
    assert_eq!(
        ids(&rt, &by("count", json!(3.0))),
        vec![exec],
        "1 matches 1.0"
    );
    assert_eq!(ids(&rt, &by("urgent", json!(true))), vec![exec]);
    assert_eq!(ids(&rt, &by("urgent", json!(false))), NONE);
    assert_eq!(
        ids(&rt, &by("count", json!("3"))),
        NONE,
        "a type must match"
    );
    assert_eq!(ids(&rt, &by("stage", json!("done"))), NONE);
    assert_eq!(ids(&rt, &by("missing", json!(null))), NONE);
    assert_eq!(
        ids(
            &rt,
            &by("stage", json!("waiting")).search_attribute("count", json!(3))
        ),
        vec![exec]
    );
    assert_eq!(
        ids(
            &rt,
            &by("stage", json!("waiting")).search_attribute("count", json!(4))
        ),
        NONE
    );

    // The terminal cycle upserts again and removes a key.
    rt.send_signal(exec, "go", json!(null)).unwrap();
    let state = rt.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(_)), "{state:?}");
    assert_eq!(
        rt.describe(exec).unwrap().summary.search_attributes,
        Stored::Value(attrs(&[
            ("stage", json!("done")),
            ("count", json!(3)),
            ("urgent", json!(true)),
        ]))
    );
    assert_eq!(ids(&rt, &by("stage", json!("done"))), vec![exec]);
    assert_eq!(ids(&rt, &by("stage", json!("waiting"))), NONE);
    assert_eq!(ids(&rt, &by("scratch", json!("x"))), NONE);
}

/// A cycle that rolls back keeps no upsert.
#[tokio::test]
async fn a_rolled_back_cycle_keeps_no_upsert() {
    let mut rt = runtime();
    let exec = rt.start_workflow("upserts_then_spawns", json!(0)).unwrap();
    let err = rt.run_until_blocked(exec).await.unwrap_err();
    assert!(matches!(err, SqliteError::Unsupported(_)), "{err}");
    let row = rt.describe(exec).unwrap();
    assert_eq!(row.summary.state, Stored::Value("FAILED".to_string()));
    assert_eq!(row.summary.search_attributes, Stored::Null);
}

/// A null filter value matches a stored null.
#[tokio::test]
async fn a_null_filter_matches_a_stored_null() {
    let mut rt = runtime();
    let exec = rt
        .start_workflow_with_options(
            "echo",
            json!(1),
            StartOptions {
                search_attributes: attrs(&[("note", json!(null))]),
                ..StartOptions::default()
            },
        )
        .unwrap()
        .exec_id;
    rt.start_workflow("echo", json!(2)).unwrap();
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new().search_attribute("note", json!(null))
        ),
        vec![exec]
    );
}

#[test]
fn a_search_attribute_that_is_not_a_primitive_is_refused() {
    let mut rt = runtime();
    let err = rt
        .start_workflow_with_options(
            "echo",
            json!(1),
            StartOptions {
                search_attributes: attrs(&[("nested", json!({ "a": 1 }))]),
                ..StartOptions::default()
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::InvalidSearchAttribute(_)),
        "{err}"
    );
    assert_eq!(ids(&rt, &ExecutionFilter::new()), NONE, "no row is written");

    let err = rt
        .list_executions(&ExecutionFilter::new().search_attribute("k", json!([1])))
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::InvalidSearchAttribute(_)),
        "{err}"
    );
}

/// A key with quotes or dots is a plain key, not a JSON path.
#[test]
fn a_search_attribute_key_is_not_a_path() {
    let mut rt = runtime();
    let exec = rt
        .start_workflow_with_options(
            "echo",
            json!(1),
            StartOptions {
                search_attributes: attrs(&[("a.b", json!(1)), ("q\"k", json!(2))]),
                ..StartOptions::default()
            },
        )
        .unwrap()
        .exec_id;
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new().search_attribute("a.b", json!(1))
        ),
        vec![exec]
    );
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new().search_attribute("q\"k", json!(2))
        ),
        vec![exec]
    );
    assert_eq!(
        ids(&rt, &ExecutionFilter::new().search_attribute("a", json!(1))),
        NONE
    );
}

// ── Memo and continue-as-new ─────────────────────────────────────────────────

/// A successor keeps the memo and the search attributes, as on Postgres.
#[tokio::test]
async fn continue_as_new_copies_the_memo_and_the_search_attributes() {
    let mut rt = runtime();
    let first = rt
        .start_workflow_with_options(
            "chained",
            json!(0),
            StartOptions {
                memo: Some(json!("keep me")),
                search_attributes: attrs(&[("tenant", json!("acme"))]),
                ..StartOptions::default()
            },
        )
        .unwrap()
        .exec_id;
    let mut exec = first;
    let last = loop {
        match rt.run_until_blocked(exec).await.unwrap() {
            RunState::ContinuedAsNew(next) => exec = next,
            RunState::Completed(_) => break exec,
            other => panic!("unexpected {other:?}"),
        }
    };
    assert_ne!(first, last);
    let expected = attrs(&[("tenant", json!("acme")), ("generation", json!("first"))]);
    for exec in [first, last] {
        let summary = rt.describe(exec).unwrap().summary;
        assert_eq!(summary.memo, Stored::Value(json!("keep me")));
        assert_eq!(summary.search_attributes, Stored::Value(expected.clone()));
    }
    assert_eq!(
        ids(
            &rt,
            &ExecutionFilter::new()
                .search_attribute("tenant", json!("acme"))
                .state("COMPLETED")
        ),
        vec![last]
    );
}

/// A start that returns an existing run leaves its memo alone.
#[test]
fn an_attached_start_keeps_the_first_memo() {
    let mut rt = runtime();
    let options = |memo: &str| StartOptions {
        workflow_id: Some("same".to_string()),
        memo: Some(json!(memo)),
        ..StartOptions::default()
    };
    let first = rt
        .start_workflow_with_options("waits", json!(0), options("first"))
        .unwrap();
    let again = rt
        .start_workflow_with_options("waits", json!(0), options("second"))
        .unwrap();
    assert!(!again.created);
    assert_eq!(again.exec_id, first.exec_id);
    assert_eq!(
        rt.describe(first.exec_id).unwrap().summary.memo,
        Stored::Value(json!("first"))
    );
}

#[test]
fn a_memo_over_the_input_cap_is_refused() {
    let mut rt = runtime();
    let huge = "x".repeat(3 * 1024 * 1024);
    let err = rt
        .start_workflow_with_options(
            "echo",
            json!(1),
            StartOptions {
                memo: Some(json!(huge)),
                ..StartOptions::default()
            },
        )
        .unwrap_err();
    assert!(
        matches!(err, SqliteError::PayloadTooLarge { kind: "memo", .. }),
        "{err}"
    );
}

// ── The read-only reader ─────────────────────────────────────────────────────

/// The reader works beside a live runtime and takes no writer lock.
#[tokio::test]
async fn a_reader_lists_beside_a_live_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("live.db");
    let mut rt = file_runtime(&db);
    let exec = rt.start_workflow("echo", json!(5)).unwrap();

    let reader = SqliteReader::open(&db).unwrap();
    let row = reader.describe(exec).unwrap();
    assert_eq!(row.summary.state, Stored::Value("RUNNING".to_string()));

    // The runtime still writes while the reader is open.
    rt.run_until_blocked(exec).await.unwrap();
    let page = reader.list_executions(&ExecutionFilter::new()).unwrap();
    assert_eq!(page.executions.len(), 1);
    assert_eq!(
        page.executions[0].state,
        Stored::Value("COMPLETED".to_string())
    );
    assert!(matches!(
        rt.outcome(exec).unwrap(),
        ExecutionOutcome::Completed(_)
    ));

    // The reader cannot write.
    drop(rt);
    let reader = SqliteReader::open(&db).unwrap();
    assert_eq!(
        reader
            .visit_executions(&ExecutionFilter::new(), |_| {})
            .unwrap(),
        None
    );
}

#[test]
fn a_reader_of_a_missing_file_fails() {
    let dir = tempfile::tempdir().unwrap();
    assert!(SqliteReader::open(dir.path().join("absent.db")).is_err());
}

/// One damaged row does not fail the page. Each bad field is unreadable.
#[test]
fn a_damaged_row_does_not_fail_the_list() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("damaged.db");
    let mut rt = file_runtime(&db);
    let good = rt.start_workflow("echo", json!(1)).unwrap();
    drop(rt);

    let raw = Connection::open(&db).unwrap();
    raw.execute(
        "INSERT INTO harvest_executions \
         (exec_id, workflow_name, workflow_id, input_json, state, error, started_at, \
          search_attrs, memo) \
         VALUES (CAST('blob-id' AS BLOB), 'echo', '', CAST(x'FF' AS TEXT), x'FF', x'00', \
                 'soon', 'not json', '{')",
        [],
    )
    .unwrap();
    raw.execute(
        "INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
         VALUES ('blob-state', 'echo', '{}', CAST('RUNNING' AS BLOB))",
        [],
    )
    .unwrap();
    drop(raw);

    let reader = SqliteReader::open(&db).unwrap();
    let page = reader.list_executions(&ExecutionFilter::new()).unwrap();
    assert_eq!(page.executions.len(), 3);
    let blob_state = &page.executions[0];
    assert_eq!(blob_state.exec_id, Stored::Value("blob-state".to_string()));
    assert_eq!(blob_state.state, Stored::Unreadable);
    let damaged = &page.executions[1];
    assert_eq!(damaged.exec_id, Stored::Unreadable);
    assert_eq!(damaged.state, Stored::Unreadable);
    assert_eq!(damaged.started_at, Stored::Unreadable);
    assert_eq!(damaged.search_attributes, Stored::Unreadable);
    assert_eq!(damaged.memo, Stored::Unreadable);
    assert_eq!(page.executions[2].execution_id(), Some(good));

    let mut payloads = Vec::new();
    reader
        .visit_executions(&ExecutionFilter::new(), |row| {
            payloads.push((row.input_json, row.error));
        })
        .unwrap();
    assert_eq!(payloads[1], (Stored::Unreadable, Stored::Unreadable));

    // A state in the wrong storage class still matches by its bytes.
    let running = reader
        .list_executions(&ExecutionFilter::new().state("RUNNING"))
        .unwrap();
    let matched: Vec<_> = running
        .executions
        .iter()
        .map(|r| r.exec_id.clone())
        .collect();
    assert_eq!(
        matched,
        vec![
            Stored::Value("blob-state".to_string()),
            Stored::Value(good.to_string())
        ]
    );
}

/// The reader opens a file that an older version wrote. A missing column
/// reads as `Null`.
#[test]
fn a_reader_reads_a_file_without_the_new_columns() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("old.db");
    let raw = Connection::open(&db).unwrap();
    raw.execute_batch(
        "CREATE TABLE harvest_executions (exec_id TEXT, workflow_name TEXT, \
         state TEXT, input_json TEXT, output_json TEXT, error TEXT);
         INSERT INTO harvest_executions VALUES ('old', 'wf', 'COMPLETED', '1', '2', NULL);",
    )
    .unwrap();
    drop(raw);

    let reader = SqliteReader::open(&db).unwrap();
    let mut rows = Vec::new();
    reader
        .visit_executions(&ExecutionFilter::new().workflow_name("wf"), |row| {
            rows.push(row);
        })
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row.summary.exec_id, Stored::Value("old".to_string()));
    assert_eq!(row.summary.workflow_id, Stored::Null);
    assert_eq!(row.summary.started_at, Stored::Null);
    assert_eq!(row.summary.search_attributes, Stored::Null);
    assert_eq!(row.output_json, Stored::Value("2".to_string()));
    // A filter on a missing column matches nothing, and does not fail.
    let page = reader
        .list_executions(&ExecutionFilter::new().started_after(Utc::now()))
        .unwrap();
    assert_eq!(page.executions, Vec::new());
}

/// The runtime migrates an old file, and an old row keeps its data.
#[test]
fn an_old_file_gains_the_new_columns_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("migrate.db");
    let raw = Connection::open(&db).unwrap();
    raw.execute_batch(
        "CREATE TABLE harvest_executions (exec_id TEXT PRIMARY KEY, \
         workflow_name TEXT NOT NULL, workflow_id TEXT NOT NULL DEFAULT '', \
         input_json TEXT NOT NULL, state TEXT NOT NULL, output_json TEXT, error TEXT);
         INSERT INTO harvest_executions (exec_id, workflow_name, input_json, state) \
         VALUES ('old', 'echo', '1', 'COMPLETED');",
    )
    .unwrap();
    drop(raw);

    let rt = file_runtime(&db);
    let page = rt
        .list_executions(&ExecutionFilter::new().workflow_name("echo"))
        .unwrap();
    assert_eq!(page.executions.len(), 1);
    assert_eq!(page.executions[0].exec_id, Stored::Value("old".to_string()));
    assert_eq!(page.executions[0].started_at, Stored::Null);
}
