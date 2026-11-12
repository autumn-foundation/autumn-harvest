//! The next wake time of a Harvest file.
//!
//! A fleet host closes an idle tenant. A closed tenant runs no drive. The
//! host opens the tenant again at the time that [`next_wake_at`] returns.
//!
//! The value is the earliest time at which a drive has due work:
//!
//! - an unfired timer of a running run, an external task deadline included;
//! - the slot of an unpaused schedule, while the scheduler is on;
//! - the retry backoff of a pending activity task of a running run;
//! - the deadline of a running run;
//! - the backoff of a running run (`start_at` and `local_retry_at`).
//!
//! A backoff gates a drive only while it is in the future. A drive does not
//! clear it. So a past backoff is no deadline.

use rusqlite::{Connection, params};

use crate::error::SqliteResult;

/// The earliest due time of each source.
///
/// `CROSS JOIN` keeps the running runs as the outer loop. SQLite then reads
/// the timers and the tasks of each running run by key. A fired timer and a
/// finished task stay until retention, so a scan from that side grows with
/// history.
const NEXT_WAKE_AT_SQL: &str = "\
SELECT MIN(at) FROM (
    SELECT t.fire_at AS at
      FROM harvest_executions e CROSS JOIN harvest_timers t ON t.exec_id = e.exec_id
     WHERE e.state = 'RUNNING' AND t.fired = 0
    UNION ALL
    SELECT k.run_at
      FROM harvest_executions e CROSS JOIN harvest_tasks k ON k.exec_id = e.exec_id
     WHERE e.state = 'RUNNING' AND k.state = 'PENDING'
    UNION ALL
    SELECT deadline_at FROM harvest_executions WHERE state = 'RUNNING'
    UNION ALL
    SELECT start_at FROM harvest_executions WHERE state = 'RUNNING' AND start_at > ?1
    UNION ALL
    SELECT local_retry_at FROM harvest_executions
     WHERE state = 'RUNNING' AND local_retry_at > ?1
    UNION ALL
    SELECT next_run_at FROM harvest_schedules WHERE ?2 AND paused = 0
)";

/// The earliest time, in epoch milliseconds, at which a drive has due work.
/// `None` when no source has a deadline.
///
/// `now` is the clock of the runtime. `schedules` is `false` while the
/// scheduler is off. A slot then fires nothing, so it is no deadline.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`](crate::SqliteError::Sqlite) when the read
/// fails.
pub fn next_wake_at(conn: &Connection, now: i64, schedules: bool) -> SqliteResult<Option<i64>> {
    let mut stmt = conn.prepare_cached(NEXT_WAKE_AT_SQL)?;
    Ok(stmt.query_row(params![now, schedules], |row| row.get(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The read starts from the running runs. It reads timers and tasks by
    /// run key, never by a scan of either table.
    #[test]
    fn the_read_finds_timers_and_tasks_by_run() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::schema::SCHEMA).unwrap();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {NEXT_WAKE_AT_SQL}"))
            .unwrap()
            .query_map(params![0_i64, true], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        // `t` is the timer alias and `k` the task alias.
        for alias in ["t", "k"] {
            assert!(
                plan.iter()
                    .any(|step| step.starts_with(&format!("SEARCH {alias} "))
                        && step.ends_with("(exec_id=?)")),
                "{alias}: {plan:#?}"
            );
            assert!(
                !plan.iter().any(|step| step == &format!("SCAN {alias}")),
                "{alias}: {plan:#?}"
            );
        }
    }
}
