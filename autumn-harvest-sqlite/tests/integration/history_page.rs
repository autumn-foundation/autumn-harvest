//! Issue #2489: the paged history read and the successor read of
//! `SqliteReader`. The SQLite management API serves the Postgres history
//! cursor and the `/result` chain walk from these reads.

// A `#[workflow]` must be `async`, even when its body has no `.await`.
#![allow(clippy::unused_async)]
// The `#[workflow]` macro reads the unused `_n` input through its expansion.
#![allow(clippy::used_underscore_binding)]

use std::path::Path;
use std::time::Duration;

use autumn_harvest::policy::{JitterPolicy, RetryPolicy};
use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{
    HistoryFilter, MAX_HISTORY_PAGE, RunState, SqliteError, SqliteReader, SqliteRuntime,
};
use serde_json::{Value, json};

/// Waits for `go` twice, then completes with the last payload.
#[workflow]
async fn two_signals(ctx: &WorkflowContext, _n: i64) -> Result<Value, String> {
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())?;
    ctx.wait_for_signal("go").await.map_err(|e| e.to_string())
}

/// Continues as new once, then completes with its input.
#[workflow]
async fn hops(ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    if n == 0 {
        ctx.continue_as_new(json!(1))
            .await
            .map_err(|e| e.to_string())?;
    }
    Ok(n)
}

/// Fails, and its retry waits one hour before it runs.
#[workflow(retry = RetryPolicy {
    max_attempts: 2,
    initial_interval: Duration::from_secs(3600),
    backoff_coefficient: 1.0,
    max_interval: Duration::from_secs(3600),
    non_retryable_errors: Vec::new(),
    jitter: JitterPolicy::None,
})]
async fn retries_later(_ctx: &WorkflowContext, _n: i64) -> Result<i64, String> {
    Err("broken".to_string())
}

fn runtime(db: &Path) -> SqliteRuntime {
    let mut rt = SqliteRuntime::open(db).unwrap();
    for info in [two_signals_info(), hops_info(), retries_later_info()] {
        rt.register_workflow(&info);
    }
    rt
}

/// A completed `two_signals` run. Its history has four events.
async fn completed_run(rt: &mut SqliteRuntime) -> ExecutionId {
    let exec = rt.start_workflow("two_signals", json!(0)).unwrap();
    for n in 1..=2 {
        rt.run_until_blocked(exec).await.unwrap();
        rt.send_signal(exec, "go", json!(n)).unwrap();
    }
    assert!(matches!(
        rt.run_until_blocked(exec).await.unwrap(),
        RunState::Completed(_)
    ));
    exec
}

#[tokio::test]
async fn a_cursor_walks_the_whole_history_once_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = completed_run(&mut rt).await;
    let reader = SqliteReader::open(&db).unwrap();
    let full = reader.load_history(exec).unwrap();
    let total = u64::try_from(full.len()).unwrap();
    assert_eq!(total, 4, "{full:?}");

    let mut seen = Vec::new();
    let mut after = None;
    let mut pages = 0;
    loop {
        let page = reader
            .history_page(exec, &HistoryFilter::new().after(after).limit(2))
            .unwrap();
        pages += 1;
        assert!(page.events.len() <= 2);
        assert_eq!(page.total_events, total, "the total ignores the cursor");
        assert_eq!(page.last_event_id, i64::try_from(total).unwrap());
        seen.extend(page.events);
        match page.next {
            Some(cursor) => after = Some(cursor),
            None => break,
        }
    }
    assert!(pages > 1, "a two-event page cannot hold {total} events");
    let as_json = |events: &mut dyn Iterator<Item = &WorkflowEvent>| -> Vec<Value> {
        events.map(|e| serde_json::to_value(e).unwrap()).collect()
    };
    assert_eq!(
        as_json(&mut seen.iter().map(|paged| &paged.event)),
        as_json(&mut full.iter()),
        "no event is dropped or repeated"
    );
    for (index, paged) in seen.iter().enumerate() {
        let index = i64::try_from(index).unwrap();
        // `event_id` is the 0-based index, as on Postgres. `id` is one more,
        // so `after=0` starts at the first event, as a Postgres id does.
        assert_eq!(paged.event_id, index);
        assert_eq!(paged.id, index + 1);
        assert!(paged.recorded_at.is_some(), "a new event has a time");
    }
    let from_zero = reader
        .history_page(exec, &HistoryFilter::new().after(Some(0)))
        .unwrap();
    assert_eq!(from_zero.events.len(), full.len());
}

#[tokio::test]
async fn an_event_type_filter_keeps_the_unfiltered_totals() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = completed_run(&mut rt).await;
    let reader = SqliteReader::open(&db).unwrap();
    let total = u64::try_from(reader.load_history(exec).unwrap().len()).unwrap();

    let filter = HistoryFilter::new()
        .event_type("SignalReceived")
        .event_type("WorkflowCompleted");
    let page = reader.history_page(exec, &filter).unwrap();
    let types: Vec<&str> = page.events.iter().map(|e| e.event.type_name()).collect();
    assert_eq!(
        types,
        ["SignalReceived", "SignalReceived", "WorkflowCompleted"]
    );
    assert_eq!(page.total_events, total);
    assert_eq!(page.last_event_id, i64::try_from(total).unwrap());

    // A one-event page of the filtered set points past its own event.
    let first = reader.history_page(exec, &filter.limit(1)).unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.next, Some(first.events[0].id));

    let none = reader
        .history_page(exec, &HistoryFilter::new().event_type("NoSuchEvent"))
        .unwrap();
    assert!(none.events.is_empty());
    assert_eq!(none.next, None);
    assert_eq!(none.total_events, total);
}

#[tokio::test]
async fn a_page_size_is_cut_to_the_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = completed_run(&mut rt).await;
    let reader = SqliteReader::open(&db).unwrap();
    let all = reader
        .history_page(exec, &HistoryFilter::new().limit(MAX_HISTORY_PAGE + 1))
        .unwrap();
    assert_eq!(all.next, None);
    let one = reader
        .history_page(exec, &HistoryFilter::new().limit(0))
        .unwrap();
    assert_eq!(one.events.len(), 1, "a zero limit reads one event");
}

#[test]
fn a_history_page_of_an_unknown_run_is_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    drop(runtime(&db));
    let reader = SqliteReader::open(&db).unwrap();
    let unknown = ExecutionId::new();
    assert!(matches!(
        reader.history_page(unknown, &HistoryFilter::new()),
        Err(SqliteError::ExecutionNotFound(id)) if id == unknown
    ));
    assert!(matches!(
        reader.successor(unknown),
        Err(SqliteError::ExecutionNotFound(id)) if id == unknown
    ));
}

#[tokio::test]
async fn the_reader_names_the_successor_of_a_chain() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let reader = SqliteReader::open(&db).unwrap();

    let first = rt.start_workflow("hops", json!(0)).unwrap();
    let RunState::ContinuedAsNew(second) = rt.run_until_blocked(first).await.unwrap() else {
        panic!("the first run continues as new");
    };
    assert_eq!(reader.successor(first).unwrap(), Some(second));
    assert_eq!(reader.successor(second).unwrap(), None, "still running");
    rt.run_until_blocked(second).await.unwrap();
    assert_eq!(reader.successor(second).unwrap(), None, "completed");

    let failed = rt.start_workflow("retries_later", json!(0)).unwrap();
    rt.run_until_blocked(failed).await.unwrap();
    let retry = rt.successor(failed).unwrap().expect("one retry");
    assert_eq!(reader.successor(failed).unwrap(), Some(retry));
    assert_eq!(reader.successor(retry).unwrap(), None);
}

#[tokio::test]
async fn a_top_level_run_has_no_parent() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("h.db");
    let mut rt = runtime(&db);
    let exec = rt.start_workflow("hops", json!(1)).unwrap();
    let reader = SqliteReader::open(&db).unwrap();
    assert_eq!(reader.parent(exec).unwrap(), None);
    assert!(matches!(
        reader.parent(ExecutionId::new()),
        Err(SqliteError::ExecutionNotFound(_))
    ));
}
