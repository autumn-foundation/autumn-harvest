//! The operator policies of agent kinds on `SQLite` (issue #2439).
//!
//! The tables and rules are those of `autumn_harvest::agent_policy_log` on
//! Postgres. The typed store is `SqliteSamplingPolicyStore` in
//! `autumn-harvest-agent`.
//!
//! [`SqlitePolicyLog`] opens its own connection. It can share the file of a
//! [`SqliteRuntime`](crate::SqliteRuntime). WAL mode lets the two
//! connections work side by side, so a second process sees each change.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::agent_policy_log::{
    ACTION_RESET, ACTION_SET, PolicyAuditRow, PolicyChangeRow, PolicyRow, next_policy_revision,
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension as _, TransactionBehavior, params};

use crate::error::{SqliteError, SqliteResult};

/// The tables, applied on each open. `body` is the JSON text of a policy.
const POLICY_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_agent_policies (
    kind          TEXT    NOT NULL CHECK (kind <> ''),
    policy        TEXT    NOT NULL CHECK (policy <> ''),
    body          TEXT,
    revision      INTEGER NOT NULL CHECK (revision >= 1),
    updated_by    TEXT    NOT NULL CHECK (updated_by <> ''),
    updated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (kind, policy)
);
CREATE TABLE IF NOT EXISTS harvest_agent_policy_audit (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    kind     TEXT    NOT NULL CHECK (kind <> ''),
    policy   TEXT    NOT NULL CHECK (policy <> ''),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    action   TEXT    NOT NULL CHECK (action IN ('set', 'reset')),
    actor    TEXT    NOT NULL CHECK (actor <> ''),
    old_body TEXT,
    new_body TEXT,
    at_ms    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS harvest_agent_policy_audit_kind_idx
    ON harvest_agent_policy_audit (kind, policy, id);
";

/// The policies of agent kinds in one `SQLite` file.
///
/// It does not validate a body. Call it through `SqliteSamplingPolicyStore`
/// in `autumn-harvest-agent`, which does.
#[doc(hidden)]
pub struct SqlitePolicyLog {
    conn: Mutex<Connection>,
}

impl std::fmt::Debug for SqlitePolicyLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePolicyLog").finish_non_exhaustive()
    }
}

impl SqlitePolicyLog {
    /// Open the log in the database at `path`, and create its tables.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened or the
    /// schema cannot be applied.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        Self::prepare(Connection::open(path)?)
    }

    /// Open a log in a new, private in-memory database.
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
        conn.execute_batch(POLICY_SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Run `f` on the connection under its lock.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut conn)
    }

    /// Read the row of `kind` and `policy`. `None` means that no operator
    /// ever set or reset it.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn read(&self, kind: &str, policy: &str) -> SqliteResult<Option<PolicyRow>> {
        self.with_conn(|conn| read_in(conn, kind, policy))
    }

    /// Set the body of `kind` and `policy` to `body`, or reset it when
    /// `body` is `None`. Writes the audit row in the same transaction.
    ///
    /// One `IMMEDIATE` transaction reads the row, writes it and writes the
    /// audit row. So each audit row names the body that its change replaced.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails, and
    /// [`SqliteError::Unsupported`] when the revision counter is full.
    pub fn write(
        &self,
        kind: &str,
        policy: &str,
        body: Option<&str>,
        actor: &str,
    ) -> SqliteResult<PolicyChangeRow> {
        self.with_conn(|conn| write_in(conn, kind, policy, body, actor))
    }

    /// The newest `limit` audit rows of `kind` and `policy`, the newest
    /// first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn audit(&self, kind: &str, policy: &str, limit: u32) -> SqliteResult<Vec<PolicyAuditRow>> {
        self.with_conn(|conn| audit_in(conn, kind, policy, limit))
    }
}

/// The body of [`SqlitePolicyLog::read`].
fn read_in(conn: &Connection, kind: &str, policy: &str) -> SqliteResult<Option<PolicyRow>> {
    let row: Option<(Option<String>, i64, String, i64)> = conn
        .query_row(
            "SELECT body, revision, updated_by, updated_at_ms
             FROM harvest_agent_policies WHERE kind = ?1 AND policy = ?2",
            params![kind, policy],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    row.map(|(body, revision, updated_by, updated_at_ms)| {
        Ok(PolicyRow {
            body,
            revision: to_revision(revision)?,
            updated_by,
            updated_at: from_millis(updated_at_ms)?,
        })
    })
    .transpose()
}

/// The body of [`SqlitePolicyLog::write`].
fn write_in(
    conn: &mut Connection,
    kind: &str,
    policy: &str,
    body: Option<&str>,
    actor: &str,
) -> SqliteResult<PolicyChangeRow> {
    let action = if body.is_some() {
        ACTION_SET
    } else {
        ACTION_RESET
    };
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let stored = read_in(&tx, kind, policy)?;
    let current = stored.as_ref().map_or(0, |row| row.revision);
    let revision = next_policy_revision(current)
        .and_then(|next| i64::try_from(next).ok())
        .ok_or_else(|| {
            SqliteError::Unsupported(format!("the policy revision of {kind:?} is full"))
        })?;
    let old = stored.and_then(|row| row.body);
    let now = Utc::now().timestamp_millis();
    tx.execute(
        "INSERT INTO harvest_agent_policies
             (kind, policy, body, revision, updated_by, updated_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT (kind, policy) DO UPDATE
             SET body = excluded.body,
                 revision = excluded.revision,
                 updated_by = excluded.updated_by,
                 updated_at_ms = excluded.updated_at_ms",
        params![kind, policy, body, revision, actor, now],
    )?;
    tx.execute(
        "INSERT INTO harvest_agent_policy_audit
             (kind, policy, revision, action, actor, old_body, new_body, at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![kind, policy, revision, action, actor, old, body, now],
    )?;
    tx.commit()?;
    Ok(PolicyChangeRow {
        old,
        new: body.map(str::to_owned),
        revision: to_revision(revision)?,
    })
}

/// The body of [`SqlitePolicyLog::audit`].
fn audit_in(
    conn: &Connection,
    kind: &str,
    policy: &str,
    limit: u32,
) -> SqliteResult<Vec<PolicyAuditRow>> {
    let mut stmt = conn.prepare(
        "SELECT revision, action, actor, old_body, new_body, at_ms
         FROM harvest_agent_policy_audit
         WHERE kind = ?1 AND policy = ?2
         ORDER BY id DESC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(params![kind, policy, i64::from(limit)], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (revision, action, actor, old, new, at_ms) = row?;
        out.push(PolicyAuditRow {
            revision: to_revision(revision)?,
            action,
            actor,
            old,
            new,
            at: from_millis(at_ms)?,
        });
    }
    Ok(out)
}

fn to_revision(stored: i64) -> SqliteResult<u64> {
    u64::try_from(stored)
        .map_err(|_| SqliteError::Unsupported(format!("policy revision {stored} is negative")))
}

fn from_millis(ms: i64) -> SqliteResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| SqliteError::Unsupported(format!("policy time {ms} is out of range")))
}
