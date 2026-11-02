//! The log of agent profile overlays on `SQLite` (issue #2371).
//!
//! The tables and rules are those of `autumn_harvest::overlay_log` on
//! Postgres. Each kind has immutable versions and one active pointer. The
//! typed store is `SqliteOverlayStore` in `autumn-harvest-agent`.
//!
//! [`SqliteOverlayLog`] opens its own connection. It can share the file of
//! a [`SqliteRuntime`](crate::SqliteRuntime). WAL mode lets the two
//! connections work side by side.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::overlay_log::{
    ActivateDecision, ActivateOutcome, ActivationRow, OverlayRow, decide_activate, logs_activation,
    next_overlay_version,
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension as _, Row, TransactionBehavior, params};

use crate::error::{SqliteError, SqliteResult};

/// The tables and the guard, applied on each open.
///
/// - The `UPDATE` trigger refuses each change of a version row.
/// - The `INSERT` trigger refuses `INSERT OR REPLACE` over a stored version.
/// - The `DELETE` trigger on the versions refuses the delete of the active
///   version, also on a connection with no `foreign_keys` pragma.
/// - The `DELETE` trigger on the heads keeps each counter, so no version
///   number comes back.
/// - The `UPDATE` and `INSERT` triggers on the activation log refuse each
///   change of a move (issue #2374).
///
/// `author_kind` has no `CHECK`. `SQLite` cannot widen a `CHECK` in place,
/// and a later author kind would need one. The typed store checks it on
/// each read.
const OVERLAY_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_agent_overlays (
    kind          TEXT    NOT NULL CHECK (kind <> ''),
    version       INTEGER NOT NULL CHECK (version >= 1),
    body          BLOB    NOT NULL,
    author_kind   TEXT    NOT NULL,
    author        TEXT    NOT NULL CHECK (author <> ''),
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (kind, version)
);
CREATE TABLE IF NOT EXISTS harvest_agent_overlay_heads (
    kind   TEXT    PRIMARY KEY,
    latest INTEGER NOT NULL CHECK (latest >= 1),
    active INTEGER,
    FOREIGN KEY (kind, active) REFERENCES harvest_agent_overlays (kind, version)
);
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlays_immutable_upd
    BEFORE UPDATE ON harvest_agent_overlays
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlays is append-only (issue #2371)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlays_immutable_ins
    BEFORE INSERT ON harvest_agent_overlays
    WHEN EXISTS (SELECT 1 FROM harvest_agent_overlays
                 WHERE kind = NEW.kind AND version = NEW.version)
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlays is append-only (issue #2371)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlays_active_del
    BEFORE DELETE ON harvest_agent_overlays
    WHEN EXISTS (SELECT 1 FROM harvest_agent_overlay_heads
                 WHERE kind = OLD.kind AND active = OLD.version)
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlays keeps its active version (issue #2371)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlay_heads_keep_del
    BEFORE DELETE ON harvest_agent_overlay_heads
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlay_heads keeps its rows (issue #2371)');
END;
CREATE TABLE IF NOT EXISTS harvest_agent_overlay_activations (
    kind            TEXT    NOT NULL CHECK (kind <> ''),
    seq             INTEGER NOT NULL CHECK (seq >= 1),
    from_version    INTEGER CHECK (from_version >= 1),
    to_version      INTEGER NOT NULL CHECK (to_version >= 1),
    actor_kind      TEXT    NOT NULL,
    actor           TEXT    NOT NULL CHECK (actor <> ''),
    activated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (kind, seq),
    CHECK (from_version IS NOT to_version)
);
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlay_activations_immutable_upd
    BEFORE UPDATE ON harvest_agent_overlay_activations
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlay_activations is append-only (issue #2374)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_agent_overlay_activations_immutable_ins
    BEFORE INSERT ON harvest_agent_overlay_activations
    WHEN EXISTS (SELECT 1 FROM harvest_agent_overlay_activations
                 WHERE kind = NEW.kind AND seq = NEW.seq)
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_overlay_activations is append-only (issue #2374)');
END;
";

/// The columns of one version row.
const ROW_COLUMNS: &str = "version, body, author_kind, author, created_at_ms";

/// The log of agent profile overlays in one `SQLite` file.
///
/// It does not validate an overlay. Call it through `SqliteOverlayStore` in
/// `autumn-harvest-agent`, which does.
#[doc(hidden)]
pub struct SqliteOverlayLog {
    conn: Mutex<Connection>,
}

impl std::fmt::Debug for SqliteOverlayLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteOverlayLog").finish_non_exhaustive()
    }
}

impl SqliteOverlayLog {
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
        // As in `SqliteRuntime::open`: wait for another writer, keep a commit
        // across a crash, and turn on the foreign key of the pointer.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;\n\
             PRAGMA foreign_keys = ON;",
        )?;
        conn.execute_batch(OVERLAY_SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Run `f` on the connection under its lock.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let mut conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        f(&mut conn)
    }

    /// Append `body` as the next version of `kind`. Returns the stored row,
    /// or `None` when the counter of the kind is full.
    ///
    /// One `IMMEDIATE` transaction reads and bumps the counter, so two
    /// appends get two versions.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn append(
        &self,
        kind: &str,
        body: &[u8],
        author_kind: &str,
        author: &str,
    ) -> SqliteResult<Option<OverlayRow>> {
        self.with_conn(|conn| append_in(conn, kind, body, author_kind, author))
    }

    /// Move the pointer of `kind` to `version` when it is `expected`, and
    /// log the move with its actor.
    ///
    /// One `IMMEDIATE` transaction reads the pointer, checks the version,
    /// sets the pointer and logs the move. So of two activations with the
    /// same `expected`, only one moves the pointer. An activation of the
    /// active version logs nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn activate(
        &self,
        kind: &str,
        version: u64,
        expected: Option<u64>,
        actor_kind: &str,
        actor: &str,
    ) -> SqliteResult<ActivateOutcome> {
        let Ok(stored) = i64::try_from(version) else {
            return Ok(ActivateOutcome::UnknownVersion);
        };
        self.with_conn(|conn| {
            activate_in(
                conn,
                &Activation {
                    kind,
                    stored,
                    expected,
                    actor_kind,
                    actor,
                },
            )
        })
    }

    /// The last move of the pointer of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn last_activation(&self, kind: &str) -> SqliteResult<Option<ActivationRow>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT seq, from_version, to_version, actor_kind, actor, activated_at_ms
                 FROM harvest_agent_overlay_activations
                 WHERE kind = ?1 ORDER BY seq DESC LIMIT 1",
                params![kind],
                read_move,
            )
            .optional()?
            .transpose()
        })
    }

    /// The active version of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn active(&self, kind: &str) -> SqliteResult<Option<OverlayRow>> {
        self.with_conn(|conn| {
            conn.query_row(
                "SELECT o.version, o.body, o.author_kind, o.author, o.created_at_ms
                 FROM harvest_agent_overlay_heads h
                 JOIN harvest_agent_overlays o ON o.kind = h.kind AND o.version = h.active
                 WHERE h.kind = ?1",
                params![kind],
                read_row,
            )
            .optional()?
            .transpose()
        })
    }

    /// The version `version` of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn get(&self, kind: &str, version: u64) -> SqliteResult<Option<OverlayRow>> {
        let Ok(stored) = i64::try_from(version) else {
            return Ok(None);
        };
        self.with_conn(|conn| {
            conn.query_row(
                &format!(
                    "SELECT {ROW_COLUMNS} FROM harvest_agent_overlays
                     WHERE kind = ?1 AND version = ?2"
                ),
                params![kind, stored],
                read_row,
            )
            .optional()?
            .transpose()
        })
    }

    /// Every version of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn list(&self, kind: &str) -> SqliteResult<Vec<OverlayRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {ROW_COLUMNS} FROM harvest_agent_overlays
                 WHERE kind = ?1 ORDER BY version"
            ))?;
            let rows = stmt.query_map(params![kind], read_row)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row??);
            }
            Ok(out)
        })
    }
}

/// The body of [`SqliteOverlayLog::append`].
fn append_in(
    conn: &mut Connection,
    kind: &str,
    body: &[u8],
    author_kind: &str,
    author: &str,
) -> SqliteResult<Option<OverlayRow>> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let latest: Option<i64> = tx
        .query_row(
            "SELECT latest FROM harvest_agent_overlay_heads WHERE kind = ?1",
            params![kind],
            |row| row.get(0),
        )
        .optional()?;
    let latest = latest.map(to_version).transpose()?;
    let Some(version) = next_overlay_version(latest) else {
        return Ok(None);
    };
    let Ok(stored) = i64::try_from(version) else {
        return Ok(None);
    };
    let created_at = from_millis(Utc::now().timestamp_millis())?;
    tx.execute(
        "INSERT INTO harvest_agent_overlay_heads (kind, latest) VALUES (?1, ?2)
         ON CONFLICT (kind) DO UPDATE SET latest = excluded.latest",
        params![kind, stored],
    )?;
    tx.execute(
        "INSERT INTO harvest_agent_overlays
             (kind, version, body, author_kind, author, created_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            kind,
            stored,
            body,
            author_kind,
            author,
            created_at.timestamp_millis()
        ],
    )?;
    tx.commit()?;
    Ok(Some(OverlayRow {
        version,
        body: body.to_vec(),
        author_kind: author_kind.to_owned(),
        author: author.to_owned(),
        created_at,
    }))
}

/// The input of [`activate_in`].
struct Activation<'a> {
    kind: &'a str,
    stored: i64,
    expected: Option<u64>,
    actor_kind: &'a str,
    actor: &'a str,
}

/// The body of [`SqliteOverlayLog::activate`].
fn activate_in(conn: &mut Connection, call: &Activation<'_>) -> SqliteResult<ActivateOutcome> {
    let Activation {
        kind,
        stored,
        expected,
        ..
    } = *call;
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let head: Option<(Option<i64>, bool)> = tx
        .query_row(
            "SELECT h.active,
                    EXISTS (SELECT 1 FROM harvest_agent_overlays o
                            WHERE o.kind = h.kind AND o.version = ?2)
             FROM harvest_agent_overlay_heads h WHERE h.kind = ?1",
            params![kind, stored],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((active, known)) = head else {
        return Ok(ActivateOutcome::UnknownVersion);
    };
    let actual = active.map(to_version).transpose()?;
    let outcome = match decide_activate(known, expected, actual) {
        ActivateDecision::UnknownVersion => ActivateOutcome::UnknownVersion,
        ActivateDecision::Conflict => ActivateOutcome::Conflict { actual },
        ActivateDecision::Activate => {
            if logs_activation(actual, to_version(stored)?) {
                tx.execute(
                    "UPDATE harvest_agent_overlay_heads SET active = ?2 WHERE kind = ?1",
                    params![kind, stored],
                )?;
                tx.execute(
                    "INSERT INTO harvest_agent_overlay_activations
                         (kind, seq, from_version, to_version, actor_kind, actor,
                          activated_at_ms)
                     SELECT ?1, COALESCE(MAX(seq), 0) + 1, ?2, ?3, ?4, ?5, ?6
                     FROM harvest_agent_overlay_activations WHERE kind = ?1",
                    params![
                        kind,
                        active,
                        stored,
                        call.actor_kind,
                        call.actor,
                        Utc::now().timestamp_millis()
                    ],
                )?;
            }
            ActivateOutcome::Activated
        }
    };
    tx.commit()?;
    Ok(outcome)
}

/// One version row. The outer error is a read error. The inner one is a
/// stored value that does not decode.
fn read_row(row: &Row<'_>) -> rusqlite::Result<SqliteResult<OverlayRow>> {
    let version: i64 = row.get(0)?;
    let created_at_ms: i64 = row.get(4)?;
    let (version, created_at) = match (to_version(version), from_millis(created_at_ms)) {
        (Ok(version), Ok(created_at)) => (version, created_at),
        (Err(err), _) | (_, Err(err)) => return Ok(Err(err)),
    };
    Ok(Ok(OverlayRow {
        version,
        body: row.get(1)?,
        author_kind: row.get(2)?,
        author: row.get(3)?,
        created_at,
    }))
}

/// One move row. The outer error is a read error. The inner one is a
/// stored value that does not decode.
fn read_move(row: &Row<'_>) -> rusqlite::Result<SqliteResult<ActivationRow>> {
    let seq: i64 = row.get(0)?;
    let from: Option<i64> = row.get(1)?;
    let to: i64 = row.get(2)?;
    let activated_at_ms: i64 = row.get(5)?;
    let decoded = (|| {
        Ok((
            to_version(seq)?,
            from.map(to_version).transpose()?,
            to_version(to)?,
            from_millis(activated_at_ms)?,
        ))
    })();
    let (seq, from, to, activated_at) = match decoded {
        Ok(decoded) => decoded,
        Err(err) => return Ok(Err(err)),
    };
    Ok(Ok(ActivationRow {
        seq,
        from,
        to,
        actor_kind: row.get(3)?,
        actor: row.get(4)?,
        activated_at,
    }))
}

fn to_version(stored: i64) -> SqliteResult<u64> {
    u64::try_from(stored)
        .map_err(|_| SqliteError::Unsupported(format!("overlay version {stored} is negative")))
}

fn from_millis(ms: i64) -> SqliteResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| SqliteError::Unsupported(format!("overlay time {ms} is out of range")))
}
