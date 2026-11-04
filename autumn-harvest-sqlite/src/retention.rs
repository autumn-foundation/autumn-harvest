//! Retention: remove old terminal runs (issues #2031 and #2471).
//!
//! A run is a candidate when it is in a terminal state and its `closed_at`
//! is before the cutoff. This is the Postgres rule. A `RUNNING` run is never
//! a candidate, because it has no `closed_at`.
//!
//! A run closed before issue #2027 has no `closed_at`. Its age is unknown,
//! so it stays.
//!
//! A [`PurgePolicy`] gives each workflow type its own age, as the Postgres
//! `RetentionConfig::overrides` does. One scoped pass runs per age.

use std::collections::BTreeMap;
use std::time::Duration;

use rusqlite::types::Value;
use rusqlite::{Connection, TransactionBehavior, params};

use crate::error::SqliteResult;

/// The runs that one transaction removes. A batch keeps each write short, so
/// a large purge does not hold the write lock for long.
const PURGE_BATCH: u32 = 256;

/// The tables that hold rows of a run. Each has an index that leads with
/// `exec_id`. The execution row goes last.
const RUN_TABLES: [&str; 8] = [
    "harvest_events",
    "harvest_tasks",
    "harvest_timers",
    "harvest_signals",
    "harvest_activity_attempts",
    "harvest_llm_ledger",
    "harvest_workflow_logs",
    "harvest_executions",
];

/// One batch of candidates, oldest first, after the keyset cursor `(?2,
/// ?3)`. The index on `(closed_at, exec_id)` serves both the filter and the
/// order. The cursor moves past a row that the purge cannot delete, so each
/// batch makes progress.
///
/// `?5` names the one type of the pass, or is `NULL` for every type. `?6`
/// is a JSON array of the types that the pass leaves to their own pass.
const CANDIDATES: &str = "\
    SELECT closed_at, exec_id, workflow_name FROM harvest_executions \
    WHERE closed_at IS NOT NULL AND closed_at < ?1 \
      AND (closed_at, exec_id) > (?2, ?3) \
      AND state IN ('COMPLETED', 'FAILED', 'CANCELLED', 'TERMINATED', \
                    'TIMED_OUT', 'CONTINUED_AS_NEW') \
      AND (?5 IS NULL OR workflow_name = ?5) \
      AND workflow_name NOT IN (SELECT value FROM json_each(?6)) \
    ORDER BY closed_at, exec_id \
    LIMIT ?4";

/// The ages of one retention pass (issue #2471).
///
/// It mirrors the history part of the Postgres `RetentionConfig`. A type
/// override wins over `max_age`. A type with neither stays.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PurgePolicy {
    /// The age of each type that has no override. `None` keeps them.
    pub max_age: Option<Duration>,
    /// The age of each named workflow type.
    pub overrides: BTreeMap<String, Duration>,
    /// `true` counts the runs and removes none.
    pub dry_run: bool,
}

impl PurgePolicy {
    /// A policy that removes nothing.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the age of each type that has no override.
    #[must_use]
    pub const fn max_age(mut self, age: Duration) -> Self {
        self.max_age = Some(age);
        self
    }

    /// Set the age of the workflow type `workflow_name`.
    #[must_use]
    pub fn override_age(mut self, workflow_name: impl Into<String>, age: Duration) -> Self {
        self.overrides.insert(workflow_name.into(), age);
        self
    }

    /// Count the runs, and remove none.
    #[must_use]
    pub const fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}

/// What one retention pass removed, or would remove in a dry run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PurgeReport {
    /// The runs removed. A dry run counts the runs it would remove.
    pub removed: u64,
    /// The same count by workflow type.
    pub by_workflow: BTreeMap<String, u64>,
    /// `true` when the pass removed nothing on purpose.
    pub dry_run: bool,
}

/// The runs of one scoped pass.
struct Scope<'a> {
    /// The one type of the pass, or `None` for every type.
    only: Option<&'a str>,
    /// The types that the pass leaves alone, as a JSON array.
    except: String,
    dry_run: bool,
}

/// Remove each terminal run closed before `cutoff_micros`, with all its
/// rows. Returns the number of runs removed.
///
/// A candidate whose `exec_id` is not text is damaged. The purge skips it,
/// so one bad row cannot stop retention.
pub fn purge_terminal(conn: &mut Connection, cutoff_micros: i64) -> SqliteResult<u64> {
    let scope = Scope {
        only: None,
        except: "[]".to_owned(),
        dry_run: false,
    };
    let mut tally = BTreeMap::new();
    purge_scope(conn, cutoff_micros, &scope, &mut tally)
}

/// Apply `policy` with each window counted back from `now_micros`.
///
/// Each override runs its own pass on its own type. The global pass then
/// runs on every other type.
pub fn purge_with(
    conn: &mut Connection,
    now_micros: i64,
    policy: &PurgePolicy,
) -> SqliteResult<PurgeReport> {
    let cutoff = |age: Duration| {
        let window = i64::try_from(age.as_micros()).unwrap_or(i64::MAX);
        now_micros.saturating_sub(window)
    };
    let mut report = PurgeReport {
        dry_run: policy.dry_run,
        ..PurgeReport::default()
    };
    for (workflow_name, age) in &policy.overrides {
        let scope = Scope {
            only: Some(workflow_name),
            except: "[]".to_owned(),
            dry_run: policy.dry_run,
        };
        report.removed += purge_scope(conn, cutoff(*age), &scope, &mut report.by_workflow)?;
    }
    if let Some(age) = policy.max_age {
        let names: Vec<&str> = policy.overrides.keys().map(String::as_str).collect();
        let scope = Scope {
            only: None,
            except: serde_json::to_string(&names)?,
            dry_run: policy.dry_run,
        };
        report.removed += purge_scope(conn, cutoff(age), &scope, &mut report.by_workflow)?;
    }
    Ok(report)
}

/// One scoped pass in batches. It adds each removed run to `tally`, by type.
fn purge_scope(
    conn: &mut Connection,
    cutoff_micros: i64,
    scope: &Scope<'_>,
    tally: &mut BTreeMap<String, u64>,
) -> SqliteResult<u64> {
    let mut removed = 0;
    let mut cursor = (i64::MIN, Value::Text(String::new()));
    loop {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let rows: Vec<(i64, Value, Value)> = tx
            .prepare(CANDIDATES)?
            .query_map(
                params![
                    cutoff_micros,
                    cursor.0,
                    cursor.1,
                    PURGE_BATCH,
                    scope.only,
                    scope.except
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?
            .collect::<Result<_, _>>()?;
        let Some(last) = rows.last().map(|(at, id, _)| (*at, id.clone())) else {
            return Ok(removed);
        };
        let batch: Vec<(&str, &str)> = rows
            .iter()
            .filter_map(|(_, id, name)| match (id, name) {
                (Value::Text(id), Value::Text(name)) => Some((id.as_str(), name.as_str())),
                (Value::Text(id), _) => Some((id.as_str(), "")),
                _ => None,
            })
            .collect();
        let count = if scope.dry_run {
            batch.len()
        } else {
            delete_runs(&tx, &batch)?
        };
        tx.commit()?;
        if count == batch.len() {
            for (_, name) in &batch {
                *tally.entry((*name).to_owned()).or_default() += 1;
            }
        }
        removed += count as u64;
        if rows.len() < PURGE_BATCH as usize {
            return Ok(removed);
        }
        cursor = last;
    }
}

/// Delete every row of each run in `batch`. Returns the runs removed.
fn delete_runs(tx: &Connection, batch: &[(&str, &str)]) -> SqliteResult<usize> {
    // One bound JSON array names the batch, so each table is one seek per id.
    let ids: Vec<&str> = batch.iter().map(|(id, _)| *id).collect();
    let ids = serde_json::to_string(&ids)?;
    let mut deleted = 0;
    for table in RUN_TABLES {
        deleted = tx.execute(
            &format!("DELETE FROM {table} WHERE exec_id IN (SELECT value FROM json_each(?1))"),
            [&ids],
        )?;
    }
    // The last delete is the execution rows, so it counts the runs.
    Ok(deleted)
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
                .replace("?4", "256")
                .replace("?5", "'w'")
                .replace("?6", "'[]'"),
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
