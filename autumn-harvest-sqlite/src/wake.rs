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
/// `running` reads the running runs once, through the state index. The
/// index holds `cast(state as blob)`, so the filter uses the same form.
/// `CROSS JOIN` keeps the running runs as the outer loop. SQLite then reads
/// the timers and the tasks of each running run by key. A fired timer and a
/// finished task stay until retention, so a scan from that side grows with
/// history.
const NEXT_WAKE_AT_SQL: &str = "\
WITH running AS MATERIALIZED (
    SELECT exec_id, deadline_at, start_at, local_retry_at FROM harvest_executions
     WHERE cast(state as blob) = cast('RUNNING' as blob)
)
SELECT MIN(at) FROM (
    SELECT t.fire_at AS at
      FROM running r CROSS JOIN harvest_timers t ON t.exec_id = r.exec_id
     WHERE t.fired = 0
    UNION ALL
    SELECT k.run_at
      FROM running r CROSS JOIN harvest_tasks k ON k.exec_id = r.exec_id
     WHERE k.state = 'PENDING'
    UNION ALL
    SELECT deadline_at FROM running
    UNION ALL
    SELECT start_at FROM running WHERE start_at > ?1
    UNION ALL
    SELECT local_retry_at FROM running WHERE local_retry_at > ?1
    UNION ALL
    SELECT next_run_at FROM harvest_schedules WHERE ?2 AND paused = 0
) WHERE at > ?3";

/// The earliest time, in epoch milliseconds, at which a drive has due work
/// after `after`. `None` when no source has such a deadline.
///
/// `now` is the clock of the runtime. `schedules` is `false` while the
/// scheduler is off. A slot then fires nothing, so it is no deadline. Pass
/// `i64::MIN` as `after` for every deadline.
///
/// # Errors
///
/// Returns [`SqliteError::Sqlite`](crate::SqliteError::Sqlite) when the read
/// fails.
pub fn next_wake_after(
    conn: &Connection,
    now: i64,
    schedules: bool,
    after: i64,
) -> SqliteResult<Option<i64>> {
    let mut stmt = conn.prepare_cached(NEXT_WAKE_AT_SQL)?;
    Ok(stmt.query_row(params![now, schedules, after], |row| row.get(0))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The read finds the running runs through the state index. It reads
    /// timers and tasks by run key, never by a scan of either table.
    #[test]
    fn the_read_finds_timers_and_tasks_by_run() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::schema::SCHEMA).unwrap();
        let plan: Vec<String> = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {NEXT_WAKE_AT_SQL}"))
            .unwrap()
            .query_map(params![0_i64, true, 0_i64], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            plan.iter().any(|step| step
                .starts_with("SEARCH harvest_executions USING INDEX idx_harvest_executions_state")),
            "{plan:#?}"
        );
        assert!(
            !plan.iter().any(|step| step == "SCAN harvest_executions"),
            "{plan:#?}"
        );
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
