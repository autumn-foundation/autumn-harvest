//! Retention: remove old terminal runs (issue #2031).
//!
//! A run is a candidate when it is in a terminal state and its `closed_at`
//! is before the cutoff. This is the Postgres rule. A `RUNNING` run is never
//! a candidate, because it has no `closed_at`.
//!
//! A run closed before issue #2027 has no `closed_at`. Its age is unknown,
//! so it stays.

use rusqlite::types::Value;
use rusqlite::{Connection, TransactionBehavior, params};

use crate::error::SqliteResult;

/// The runs that one transaction removes. A batch keeps each write short, so
/// a large purge does not hold the write lock for long.
const PURGE_BATCH: u32 = 256;

/// The tables that hold rows of a run. Each has an index that leads with
/// `exec_id`. The execution row goes last.
const RUN_TABLES: [&str; 6] = [
    "harvest_events",
    "harvest_tasks",
    "harvest_timers",
    "harvest_signals",
    "harvest_activity_attempts",
    "harvest_executions",
];

/// One batch of candidates, oldest first, after the keyset cursor `(?2,
/// ?3)`. The index on `(closed_at, exec_id)` serves both the filter and the
/// order. The cursor moves past a row that the purge cannot delete, so each
/// batch makes progress.
const CANDIDATES: &str = "\
    SELECT closed_at, exec_id FROM harvest_executions \
    WHERE closed_at IS NOT NULL AND closed_at < ?1 \
      AND (closed_at, exec_id) > (?2, ?3) \
      AND state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TERMINATED', \
                    'TIMED_OUT', 'CONTINUED_AS_NEW') \
    ORDER BY closed_at, exec_id \
    LIMIT ?4";

/// Remove each terminal run closed before `cutoff_micros`, with all its
/// rows. Returns the number of runs removed.
///
/// A candidate whose `exec_id` is not text is damaged. The purge skips it,
/// so one bad row cannot stop retention.
pub fn purge_terminal(conn: &mut Connection, cutoff_micros: i64) -> SqliteResult<u64> {
    let mut removed = 0;
    let mut cursor = (i64::MIN, Value::Text(String::new()));
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows: Vec<(i64, Value)> = tx
            .prepare(CANDIDATES)?
            .query_map(
                params![cutoff_micros, cursor.0, cursor.1, PURGE_BATCH],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?
            .collect::<Result<_, _>>()?;
        let Some(last) = rows.last().cloned() else {
            return Ok(removed);
        };
        let ids: Vec<&str> = rows
            .iter()
            .filter_map(|(_, id)| match id {
                Value::Text(id) => Some(id.as_str()),
                _ => None,
            })
            .collect();
        // One bound JSON array names the batch, so each table is one seek
        // per id.
        let ids = serde_json::to_string(&ids)?;
        let mut deleted = 0;
        for table in RUN_TABLES {
            deleted = tx.execute(
                &format!("DELETE FROM {table} WHERE exec_id IN (SELECT value FROM json_each(?1))"),
                [&ids],
            )?;
        }
        tx.commit()?;
        // The last delete is the execution rows, so it counts the runs.
        removed += deleted as u64;
        if rows.len() < PURGE_BATCH as usize {
            return Ok(removed);
        }
        cursor = last;
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{CANDIDATES, RUN_TABLES};
    use crate::schema;

    fn plan(conn: &Connection, sql: &str) -> String {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        rows.join("\n")
    }

    // Each batch is a set of index seeks. A full scan of a run table would
    // make a large purge quadratic.
    #[test]
    fn the_purge_uses_an_index_for_every_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        schema::migrate(&conn).unwrap();

        let candidates = plan(
            &conn,
            &CANDIDATES
                .replace("?1", "0")
                .replace("?2", "0")
                .replace("?3", "''")
                .replace("?4", "256"),
        );
        assert!(
            candidates.contains("idx_harvest_executions_closed")
                && !candidates.contains("TEMP B-TREE"),
            "{candidates}"
        );
        for table in RUN_TABLES {
            let delete = plan(
                &conn,
                &format!(
                    "DELETE FROM {table} WHERE exec_id IN (SELECT value FROM json_each('[]'))"
                ),
            );
            assert!(
                !delete.contains(&format!("SCAN {table}")),
                "{table}: {delete}"
            );
        }
    }
}
