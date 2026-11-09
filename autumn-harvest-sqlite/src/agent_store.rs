//! The rows of the durable agent stores on `SQLite`.
//!
//! The tables and rules are those of `autumn_harvest::agent_store` on
//! Postgres. The typed stores are in `autumn_harvest_agent::stores`.
//!
//! [`SqliteAgentStoreDb`] opens its own connection. It can share the file
//! of a [`SqliteRuntime`](crate::SqliteRuntime). WAL mode lets the two
//! connections work side by side, so a second process sees each write.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::agent_store::{
    DocRow, EXPIRED_BATCH, MAX_AGE_SECS, WindowPut, next_doc_version,
};
use rusqlite::{Connection, OptionalExtension as _, params};

use crate::error::{SqliteError, SqliteResult};

/// The tables, applied on each open. The times are epoch milliseconds.
const AGENT_STORE_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_agent_store_docs (
    store         TEXT    NOT NULL CHECK (store IN ('episodes', 'memory')),
    scope         TEXT    NOT NULL CHECK (scope <> ''),
    version       INTEGER NOT NULL CHECK (version >= 1),
    body          BLOB    NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (store, scope)
);
CREATE TABLE IF NOT EXISTS harvest_agent_transcripts (
    digest        TEXT    NOT NULL CHECK (digest <> ''),
    session       TEXT    NOT NULL,
    run_id        TEXT    NOT NULL,
    body          BLOB    NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (digest, session)
);
CREATE INDEX IF NOT EXISTS harvest_agent_transcripts_run_idx
    ON harvest_agent_transcripts (session, run_id, created_at_ms DESC);
CREATE INDEX IF NOT EXISTS harvest_agent_transcripts_created_idx
    ON harvest_agent_transcripts (created_at_ms);
";

/// The rows of the agent stores in one `SQLite` file.
///
/// It does not read a body. Call it through the stores of
/// `autumn_harvest_agent::stores`, which do.
#[doc(hidden)]
pub struct SqliteAgentStoreDb {
    conn: Mutex<Connection>,
}

impl std::fmt::Debug for SqliteAgentStoreDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteAgentStoreDb").finish_non_exhaustive()
    }
}

impl SqliteAgentStoreDb {
    /// Open the rows in the database at `path`, and create the tables.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened or the
    /// schema cannot be applied.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        Self::prepare(Connection::open(path)?)
    }

    /// Open the rows in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the schema cannot be applied.
    pub fn open_in_memory() -> SqliteResult<Self> {
        Self::prepare(Connection::open_in_memory()?)
    }

    fn prepare(conn: Connection) -> SqliteResult<Self> {
        // As in `SqliteRuntime::open`: wait for another writer, and keep a
        // commit across a crash.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;",
        )?;
        conn.execute_batch(AGENT_STORE_SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Run `f` on the connection under its lock.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut conn)
    }

    /// Read the document of `store` and `scope`. `None` means that no
    /// writer stored one.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn read_doc(&self, store: &str, scope: &str) -> SqliteResult<Option<DocRow>> {
        self.with_conn(|conn| read_doc_in(conn, store, scope))
    }

    /// Write `body` as the document of `store` and `scope` when the stored
    /// version is still `expected`. `None` expects no document.
    ///
    /// Returns `false` when another writer changed the document first, or
    /// when the version counter is full. Nothing changes then.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn swap_doc(
        &self,
        store: &str,
        scope: &str,
        expected: Option<u64>,
        body: &[u8],
        now_ms: i64,
    ) -> SqliteResult<bool> {
        self.with_conn(|conn| swap_doc_in(conn, store, scope, expected, body, now_ms))
    }

    /// Delete the document of `store` and `scope`. Returns `true` when a
    /// document was there.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn delete_doc(&self, store: &str, scope: &str) -> SqliteResult<bool> {
        self.with_conn(|conn| delete_doc_in(conn, store, scope))
    }

    /// The newest window with `digest` that is younger than the age limit
    /// at `now_ms`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn get_window(
        &self,
        digest: &str,
        max_age_secs: u64,
        now_ms: i64,
    ) -> SqliteResult<Option<Vec<u8>>> {
        self.with_conn(|conn| get_window_in(conn, digest, max_age_secs, now_ms))
    }

    /// Keep one window, then delete the old windows of its run and a batch
    /// of expired windows. One `IMMEDIATE` transaction does both.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn put_window(&self, put: &WindowPut<'_>, now_ms: i64) -> SqliteResult<()> {
        self.with_conn(|conn| put_window_in(conn, put, now_ms))
    }

    /// Delete every window of `session`. Returns the number deleted.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn erase_windows(&self, session: &str) -> SqliteResult<u64> {
        self.with_conn(|conn| erase_windows_in(conn, session))
    }
}

/// The body of [`SqliteAgentStoreDb::read_doc`].
fn read_doc_in(conn: &Connection, store: &str, scope: &str) -> SqliteResult<Option<DocRow>> {
    let row: Option<(i64, Vec<u8>)> = conn
        .query_row(
            "SELECT version, body FROM harvest_agent_store_docs
             WHERE store = ?1 AND scope = ?2",
            params![store, scope],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(version, body)| {
        Ok(DocRow {
            version: to_version(version)?,
            body,
        })
    })
    .transpose()
}

/// The body of [`SqliteAgentStoreDb::swap_doc`].
fn swap_doc_in(
    conn: &Connection,
    store: &str,
    scope: &str,
    expected: Option<u64>,
    body: &[u8],
    now_ms: i64,
) -> SqliteResult<bool> {
    let Some(next) = next_doc_version(expected).and_then(|next| i64::try_from(next).ok()) else {
        return Ok(false);
    };
    let written = match expected {
        None => conn.execute(
            "INSERT INTO harvest_agent_store_docs (store, scope, version, body, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (store, scope) DO NOTHING",
            params![store, scope, next, body, now_ms],
        )?,
        Some(old) => conn.execute(
            "UPDATE harvest_agent_store_docs
             SET version = ?3, body = ?4, updated_at_ms = ?5
             WHERE store = ?1 AND scope = ?2 AND version = ?6",
            params![
                store,
                scope,
                next,
                body,
                now_ms,
                i64::try_from(old).unwrap_or(i64::MAX)
            ],
        )?,
    };
    Ok(written == 1)
}

/// The body of [`SqliteAgentStoreDb::delete_doc`].
fn delete_doc_in(conn: &Connection, store: &str, scope: &str) -> SqliteResult<bool> {
    let deleted = conn.execute(
        "DELETE FROM harvest_agent_store_docs WHERE store = ?1 AND scope = ?2",
        params![store, scope],
    )?;
    Ok(deleted > 0)
}

/// The earliest creation time that is still young at `now_ms`.
fn cutoff_ms(max_age_secs: u64, now_ms: i64) -> i64 {
    let age_ms = i64::try_from(max_age_secs.min(MAX_AGE_SECS))
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000);
    now_ms.saturating_sub(age_ms)
}

/// The body of [`SqliteAgentStoreDb::get_window`].
fn get_window_in(
    conn: &Connection,
    digest: &str,
    max_age_secs: u64,
    now_ms: i64,
) -> SqliteResult<Option<Vec<u8>>> {
    Ok(conn
        .query_row(
            "SELECT body FROM harvest_agent_transcripts
             WHERE digest = ?1 AND created_at_ms > ?2
             ORDER BY created_at_ms DESC
             LIMIT 1",
            params![digest, cutoff_ms(max_age_secs, now_ms)],
            |row| row.get(0),
        )
        .optional()?)
}

/// The body of [`SqliteAgentStoreDb::put_window`].
fn put_window_in(conn: &mut Connection, put: &WindowPut<'_>, now_ms: i64) -> SqliteResult<()> {
    let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    tx.execute(
        "INSERT INTO harvest_agent_transcripts
             (digest, session, run_id, body, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (digest, session) DO NOTHING",
        params![put.digest, put.session, put.run_id, put.body, now_ms],
    )?;
    tx.execute(
        "DELETE FROM harvest_agent_transcripts
         WHERE (digest, session) IN (
             SELECT digest, session FROM (
                 SELECT digest, session FROM harvest_agent_transcripts
                 WHERE session = ?1 AND run_id = ?2
                 ORDER BY created_at_ms DESC, digest
                 LIMIT -1 OFFSET ?3)
             UNION ALL
             SELECT digest, session FROM (
                 SELECT digest, session FROM harvest_agent_transcripts
                 WHERE created_at_ms < ?4
                 LIMIT ?5))",
        params![
            put.session,
            put.run_id,
            i64::from(put.keep.max(1)),
            cutoff_ms(put.max_age_secs, now_ms),
            i64::from(EXPIRED_BATCH)
        ],
    )?;
    tx.commit()?;
    Ok(())
}

/// The body of [`SqliteAgentStoreDb::erase_windows`].
fn erase_windows_in(conn: &Connection, session: &str) -> SqliteResult<u64> {
    let deleted = conn.execute(
        "DELETE FROM harvest_agent_transcripts WHERE session = ?1",
        params![session],
    )?;
    Ok(u64::try_from(deleted).unwrap_or(u64::MAX))
}

fn to_version(stored: i64) -> SqliteResult<u64> {
    u64::try_from(stored)
        .map_err(|_| SqliteError::Unsupported(format!("document version {stored} is negative")))
}
