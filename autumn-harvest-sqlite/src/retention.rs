//! Retention: remove old terminal runs (issue #2031).
//!
//! A run is a candidate when it is in a terminal state and its `closed_at`
//! is before the cutoff. A candidate stays when a `RUNNING` run shares its
//! `(workflow_name, workflow_id)`. That run still reads the keyed signals of
//! its chain to dedupe a retry (issue #2028).
//!
//! A row closed before issue #2027 has no `closed_at`. Its age is unknown,
//! so it stays.

use rusqlite::{Connection, TransactionBehavior};

use crate::error::SqliteResult;

/// The runs that one transaction removes. A batch keeps each write short, so
/// a large purge does not hold the write lock for long.
const PURGE_BATCH: u32 = 256;

/// The tables that hold rows of a run. The execution row goes last.
const RUN_TABLES: [&str; 6] = [
    "harvest_events",
    "harvest_tasks",
    "harvest_timers",
    "harvest_signals",
    "harvest_activity_attempts",
    "harvest_executions",
];

/// One batch of candidates. The partial close-time index finds them. The
/// key index answers the live-run probe.
const CANDIDATES: &str = "\
    SELECT done.exec_id FROM harvest_executions AS done \
    WHERE done.closed_at IS NOT NULL AND done.closed_at < ?1 \
      AND done.state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TERMINATED', \
                         'TIMED_OUT', 'CONTINUED_AS_NEW') \
      AND NOT EXISTS (SELECT 1 FROM harvest_executions AS live \
                      WHERE live.workflow_name = done.workflow_name \
                        AND live.workflow_id = done.workflow_id \
                        AND live.state = 'RUNNING') \
    LIMIT ?2";

/// Remove each terminal run closed before `cutoff_micros`, with all its
/// rows. Returns the number of runs removed.
pub fn purge_terminal(conn: &mut Connection, cutoff_micros: i64) -> SqliteResult<u64> {
    let mut removed = 0;
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let batch: Vec<String> = tx
            .prepare(CANDIDATES)?
            .query_map(rusqlite::params![cutoff_micros, PURGE_BATCH], |row| {
                row.get(0)
            })?
            .collect::<Result<_, _>>()?;
        if batch.is_empty() {
            return Ok(removed);
        }
        // One bound JSON array names the batch, so each table is one scan.
        let ids = serde_json::to_string(&batch)?;
        for table in RUN_TABLES {
            tx.execute(
                &format!("DELETE FROM {table} WHERE exec_id IN (SELECT value FROM json_each(?1))"),
                [&ids],
            )?;
        }
        tx.commit()?;
        removed += batch.len() as u64;
        if batch.len() < PURGE_BATCH as usize {
            return Ok(removed);
        }
    }
}
