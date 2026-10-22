//! The local `SQLite` side: a prototype disk cache read.
//!
//! The file has the schema of `autumn-harvest-sqlite`. A second connection
//! writes the rows, and the runtime reads them back through
//! `SqliteRuntime::load_history`. That is the real read path of the
//! backend: one ordered scan of `harvest_events` and a JSON decode of each
//! row.

use std::path::Path;

use autumn_harvest::WorkflowEvent;
use autumn_harvest::types::ExecutionId;
use autumn_harvest_sqlite::SqliteRuntime;
use rusqlite::Connection;

use crate::BoxError;

/// A `SQLite` file with a runtime that reads and a connection that writes.
pub struct SqliteStore {
    runtime: SqliteRuntime,
    writer: Connection,
}

impl SqliteStore {
    /// Opens or creates the file at `path`.
    ///
    /// The runtime applies the schema and takes the single-writer lock. The
    /// writer is a plain second connection. The lock is a separate file, so
    /// it does not block the writer.
    ///
    /// # Errors
    ///
    /// Returns an error when the runtime or the writer cannot open the file.
    pub fn open(path: &Path) -> Result<Self, BoxError> {
        let runtime = SqliteRuntime::open(path)?;
        let writer = Connection::open(path)?;
        writer.execute_batch("PRAGMA busy_timeout = 5000;")?;
        Ok(Self { runtime, writer })
    }

    /// Stores `events` as one new run. Returns its id.
    ///
    /// # Errors
    ///
    /// Returns an error when an insert fails.
    pub fn seed(&mut self, events: &[WorkflowEvent]) -> Result<ExecutionId, BoxError> {
        let exec_id = ExecutionId::new();
        let tx = self.writer.transaction()?;
        tx.execute(
            "INSERT INTO harvest_executions \
               (exec_id, workflow_name, workflow_id, input_json, state) \
             VALUES (?1, ?2, ?3, '{}', 'RUNNING')",
            rusqlite::params![
                exec_id.to_string(),
                crate::pg_side::WORKFLOW_NAME,
                format!("hlg-{exec_id}"),
            ],
        )?;
        {
            let mut insert = tx.prepare(
                "INSERT INTO harvest_events (exec_id, seq, event_json) VALUES (?1, ?2, ?3)",
            )?;
            for (seq, event) in events.iter().enumerate() {
                insert.execute(rusqlite::params![
                    exec_id.to_string(),
                    i64::try_from(seq)?,
                    serde_json::to_string(event)?,
                ])?;
            }
        }
        tx.commit()?;
        Ok(exec_id)
    }

    /// Copies the run `template` into `count` new runs. Returns their ids.
    ///
    /// # Errors
    ///
    /// Returns an error when an insert fails.
    pub fn clone_runs(
        &mut self,
        template: ExecutionId,
        count: usize,
    ) -> Result<Vec<ExecutionId>, BoxError> {
        let ids: Vec<ExecutionId> = (0..count).map(|_| ExecutionId::new()).collect();
        let tx = self.writer.transaction()?;
        {
            let mut run = tx.prepare(
                "INSERT INTO harvest_executions \
                   (exec_id, workflow_name, workflow_id, input_json, state) \
                 SELECT ?1, workflow_name, 'hlg-' || ?1, input_json, state \
                 FROM harvest_executions WHERE exec_id = ?2",
            )?;
            let mut events = tx.prepare(
                "INSERT INTO harvest_events (exec_id, seq, event_json) \
                 SELECT ?1, seq, event_json FROM harvest_events WHERE exec_id = ?2",
            )?;
            for id in &ids {
                let params = rusqlite::params![id.to_string(), template.to_string()];
                run.execute(params)?;
                events.execute(params)?;
            }
        }
        tx.commit()?;
        Ok(ids)
    }

    /// Loads the history of `exec_id` through the runtime read path.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown run or a row that does not decode.
    pub fn load(&self, exec_id: ExecutionId) -> Result<Vec<WorkflowEvent>, BoxError> {
        Ok(self.runtime.load_history(exec_id)?)
    }

    /// Deletes the runs `ids` and their events.
    ///
    /// # Errors
    ///
    /// Returns an error when a delete fails.
    pub fn delete_runs(&mut self, ids: &[ExecutionId]) -> Result<(), BoxError> {
        let tx = self.writer.transaction()?;
        {
            let mut events = tx.prepare("DELETE FROM harvest_events WHERE exec_id = ?1")?;
            let mut runs = tx.prepare("DELETE FROM harvest_executions WHERE exec_id = ?1")?;
            for id in ids {
                events.execute([id.to_string()])?;
                runs.execute([id.to_string()])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seed;

    fn values(events: &[WorkflowEvent]) -> serde_json::Value {
        serde_json::to_value(events).expect("encode")
    }

    #[test]
    fn a_seeded_run_loads_back_unchanged() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = SqliteStore::open(&dir.path().join("cache.db")).expect("open");
        let history = seed::history(1_001, 64);
        let exec_id = store.seed(&history).expect("seed");
        assert_eq!(
            values(&store.load(exec_id).expect("load")),
            values(&history)
        );
    }

    #[test]
    fn each_clone_loads_the_template_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = SqliteStore::open(&dir.path().join("cache.db")).expect("open");
        let history = seed::history(9, 16);
        let template = store.seed(&history).expect("seed");
        let clones = store.clone_runs(template, 5).expect("clone");
        assert_eq!(clones.len(), 5);
        for exec_id in &clones {
            assert_ne!(*exec_id, template);
            assert_eq!(
                values(&store.load(*exec_id).expect("load")),
                values(&history)
            );
        }
        store.delete_runs(&clones).expect("delete");
        assert!(store.load(clones[0]).is_err(), "a deleted run is unknown");
        assert_eq!(
            values(&store.load(template).expect("load")),
            values(&history)
        );
    }
}
