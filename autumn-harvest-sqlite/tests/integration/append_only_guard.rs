//! Issue #2031: the append-only guard on `harvest_events`.
//!
//! Postgres guards the log with a trigger (issue #1817). This backend has no
//! sanctioned rewrite, so its guard refuses every `UPDATE`. It also refuses
//! an `INSERT OR REPLACE` of a stored event. `DELETE` stays open for
//! retention.

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::SqliteRuntime;
use rusqlite::Connection;
use serde_json::json;

#[workflow]
async fn echo(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n)
}

/// A file with one completed run. Returns the dir, the path and the run id.
async fn file_with_history() -> (tempfile::TempDir, std::path::PathBuf, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guard.sqlite3");
    let mut rt = SqliteRuntime::open(&path).unwrap();
    rt.register_workflow(&echo_info());
    let exec = rt.start_workflow("echo", json!(7)).unwrap();
    rt.run_until_blocked(exec).await.unwrap();
    drop(rt);
    (dir, path, exec.to_string())
}

fn first_event(conn: &Connection, exec: &str) -> String {
    conn.query_row(
        "SELECT event_json FROM harvest_events WHERE exec_id = ?1 AND seq = 0",
        [exec],
        |r| r.get(0),
    )
    .unwrap()
}

// AC3: an `UPDATE` on `harvest_events` fails, on every column, and the row
// keeps its bytes.
#[tokio::test]
async fn an_update_on_harvest_events_fails() {
    let (_dir, path, exec) = file_with_history().await;
    let conn = Connection::open(&path).unwrap();
    let before = first_event(&conn, &exec);

    for set in [
        "event_json = '{}'",
        "seq = seq + 100",
        "exec_id = 'other'",
        "event_json = event_json",
    ] {
        let err = conn
            .execute(
                &format!("UPDATE harvest_events SET {set} WHERE exec_id = ?1"),
                [&exec],
            )
            .expect_err("the guard refuses the update");
        assert!(
            err.to_string().contains("append-only"),
            "the error names the rule for `{set}`: {err}"
        );
    }
    assert_eq!(first_event(&conn, &exec), before, "the row is unchanged");
}

// An `INSERT OR REPLACE` deletes and re-inserts. It fires no `UPDATE`
// trigger, so the guard refuses it on insert.
#[tokio::test]
async fn insert_or_replace_cannot_rewrite_an_event() {
    let (_dir, path, exec) = file_with_history().await;
    let conn = Connection::open(&path).unwrap();
    let before = first_event(&conn, &exec);

    for sql in [
        "INSERT OR REPLACE INTO harvest_events (exec_id, seq, event_json) VALUES (?1, 0, '{}')",
        "REPLACE INTO harvest_events (exec_id, seq, event_json) VALUES (?1, 0, '{}')",
        "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, 0, '{}') \
         ON CONFLICT (exec_id, seq) DO UPDATE SET event_json = excluded.event_json",
    ] {
        let err = conn
            .execute(sql, [&exec])
            .expect_err("the guard refuses it");
        assert!(err.to_string().contains("append-only"), "{sql}: {err}");
    }
    assert_eq!(first_event(&conn, &exec), before, "the row is unchanged");

    conn.execute(
        "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, 999, '{}')",
        [&exec],
    )
    .expect("a new event still appends");
}

// Retention needs `DELETE`, as on Postgres.
#[tokio::test]
async fn delete_is_not_guarded() {
    let (_dir, path, exec) = file_with_history().await;
    let conn = Connection::open(&path).unwrap();
    let removed = conn
        .execute("DELETE FROM harvest_events WHERE exec_id = ?1", [&exec])
        .unwrap();
    assert!(removed > 0);
}

// A file written before the guard gains it on its next open.
#[tokio::test]
async fn an_old_file_gains_the_guard_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.sqlite3");
    {
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE harvest_events (exec_id TEXT NOT NULL, seq INTEGER NOT NULL, \
             event_json TEXT NOT NULL, PRIMARY KEY (exec_id, seq)); \
             INSERT INTO harvest_events VALUES ('e', 0, '{}');",
        )
        .unwrap();
        conn.execute("UPDATE harvest_events SET event_json = '[]'", [])
            .expect("the old file has no guard yet");
    }

    drop(SqliteRuntime::open(&path).unwrap());

    let conn = Connection::open(&path).unwrap();
    let err = conn
        .execute("UPDATE harvest_events SET event_json = '{}'", [])
        .expect_err("the reopened file has the guard");
    assert!(err.to_string().contains("append-only"), "{err}");
}
