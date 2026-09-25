//! 🪝 Snag: an unknown `ExecutionId` gets a different error shape depending on
//! which accessor is called.
//!
//! `SqliteError::ExecutionNotFound`'s own doc comment states the general
//! contract: "A referenced execution id does not exist in this database."
//! `SqliteRuntime::outcome` and `SqliteRuntime::send_signal` honor it. Both
//! call `store::execution_exists` first. Both return `ExecutionNotFound(exec)`
//! for an id with no row.
//!
//! `SqliteRuntime::run_until_blocked` does not. `drive_one_cycle` reads
//! `store::execution_state` (a bare `SELECT ... WHERE exec_id = ?1`) with no
//! existence check first. `rusqlite::Connection::query_row` on zero rows
//! returns `rusqlite::Error::QueryReturnedNoRows`. That converts into
//! `SqliteError::Sqlite(QueryReturnedNoRows)` through the `?` operator. This
//! is a generic, driver-level error. It names no execution id. It gives no
//! hint that the real problem is an unknown id, rather than a database
//! fault. The `# Errors` section on `run_until_blocked`'s own rustdoc lists
//! `Stuck`, `Unsupported`, and a generic persistence error. It never
//! mentions `ExecutionNotFound`. Yet an unknown id is a likely failure for
//! an embedder to hit. A typo in a stored id, a stale id from a dropped
//! `open_in_memory` database, or a wrong-file `open` can all cause it.
//!
//! `load_history` and `activity_attempts` share the same root cause: no
//! existence check. Each surfaces it a third way: a silent `Ok(vec![])`.
//! That result is indistinguishable from a real execution with no matching
//! rows yet. But a real execution always has at least one event
//! (`WorkflowStarted`) immediately after `start_workflow`, per §6 of
//! `docs/sqlite-backend.md`. So an empty history is never a real state here.
//! It is always "wrong id" — and no caller is told that.

use autumn_harvest::ExecutionId;
use autumn_harvest_sqlite::{SqliteError, SqliteRuntime};

/// `outcome()` on an unknown id is the documented, correct shape: a named,
/// typed `ExecutionNotFound(exec)` carrying the offending id.
#[tokio::test]
async fn outcome_on_unknown_id_is_execution_not_found() {
    let rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let err = rt.outcome(bogus).unwrap_err();
    assert!(
        matches!(err, SqliteError::ExecutionNotFound(id) if id == bogus),
        "outcome() must report the documented ExecutionNotFound(exec) shape, got: {err:?}"
    );
}

/// RED: pins the current (buggy) behavior. `run_until_blocked` on the exact
/// same unknown id produces an opaque `Sqlite(QueryReturnedNoRows)` instead
/// of `ExecutionNotFound`. `outcome()` reports the same condition cleanly one
/// call above. This test currently PASSES because it asserts the bug. Flip
/// the assertion to `ExecutionNotFound` once `drive_one_cycle` gets the same
/// `execution_exists` guard `outcome`/`send_signal` already use.
#[tokio::test]
async fn run_until_blocked_on_unknown_id_does_not_match_outcomes_shape() {
    let mut rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let err = rt.run_until_blocked(bogus).await.unwrap_err();

    assert!(
        !matches!(err, SqliteError::ExecutionNotFound(_)),
        "run_until_blocked NOW reports ExecutionNotFound for an unknown id. \
         Good — the inconsistency this test pins is fixed. Delete this test, \
         or invert it into a permanent assertion, rather than leaving it \
         green by accident: got {err:?}"
    );
    assert!(
        matches!(
            err,
            SqliteError::Sqlite(rusqlite::Error::QueryReturnedNoRows)
        ),
        "expected the current opaque QueryReturnedNoRows shape, got: {err:?}. \
         If this changed to something else, the bug likely moved rather than \
         closed. Re-check against outcome()'s ExecutionNotFound(exec)."
    );

    // The same unknown id, reported two different ways by the same struct in
    // the same crate: this is the defect. A caller cannot pattern-match one
    // error shape and handle "unknown execution" uniformly across the API.
    let outcome_err = rt.outcome(bogus).unwrap_err();
    assert!(matches!(outcome_err, SqliteError::ExecutionNotFound(id) if id == bogus));
}

/// Same root cause, third shape: silently empty rather than any error at
/// all. Documents the scope of the pattern; not the primary claim above.
#[tokio::test]
async fn load_history_and_activity_attempts_on_unknown_id_are_silently_empty() {
    let rt = SqliteRuntime::open_in_memory().unwrap();
    let bogus = ExecutionId::new();

    let history = rt.load_history(bogus).unwrap();
    assert!(
        history.is_empty(),
        "pinning current behavior: no error, no ExecutionNotFound. Just an \
         empty Vec, indistinguishable from a real execution with zero events \
         (a state that never actually happens in practice)."
    );

    let attempts = rt.activity_attempts(bogus, "whatever").unwrap();
    assert!(attempts.is_empty());
}
