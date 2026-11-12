//! The log of agent profile overlays on `SQLite` (issue #2371).
//!
//! The tables and rules are those of `autumn_harvest::overlay_log` on
//! Postgres. Each kind has immutable versions and one active pointer. The
//! typed store is `SqliteOverlayStore` in `autumn-harvest-agent`.
//!
//! [`SqliteOverlayLog::open`] keeps its own connection. It can share the
//! file of a [`SqliteRuntime`](crate::SqliteRuntime). WAL mode lets the two
//! connections work side by side.
//!
//! [`SqliteOverlayLog::with_connector`] opens a new connection for each
//! call, and drops it after the call. Use it in a database that a
//! replicator checkpoints.

use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use autumn_harvest::overlay_log::{
    AUTHOR_AUTO, ActivateDecision, ActivateOutcome, ActivationRow, AutoAdoptRaw, AutoAdoptRequest,
    AutoMoveDecision, MoveRequest, OverlayRow, PolicyAppend, PolicyRow, PolicyWrite,
    decide_auto_move, logs_activation, next_overlay_version, policy_write_passes, utc_day,
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
/// - The `UPDATE` and `INSERT` triggers on the refine policy log refuse each
///   change of a row (issue #2394).
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
CREATE TABLE IF NOT EXISTS harvest_agent_refine_policies (
    kind          TEXT    NOT NULL CHECK (kind <> ''),
    seq           INTEGER NOT NULL CHECK (seq >= 1),
    old_body      BLOB,
    new_body      BLOB,
    actor_kind    TEXT    NOT NULL CHECK (actor_kind = 'operator'),
    actor         TEXT    NOT NULL CHECK (actor <> ''),
    changed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (kind, seq)
);
CREATE TRIGGER IF NOT EXISTS harvest_agent_refine_policies_immutable_upd
    BEFORE UPDATE ON harvest_agent_refine_policies
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_refine_policies is append-only (issue #2394)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_agent_refine_policies_immutable_ins
    BEFORE INSERT ON harvest_agent_refine_policies
    WHEN EXISTS (SELECT 1 FROM harvest_agent_refine_policies
                 WHERE kind = NEW.kind AND seq = NEW.seq)
BEGIN
    SELECT RAISE(ABORT, 'harvest_agent_refine_policies is append-only (issue #2394)');
END;
";

/// Opens one connection to the database of an overlay log.
///
/// Each call must open the same database. Set the pragmas of the app in
/// the closure, such as `journal_mode`, `busy_timeout` and
/// `wal_autocheckpoint`. The app uses the `rusqlite` version of this
/// crate, so the `Connection` types match.
pub type SqliteConnector = Arc<dyn Fn() -> rusqlite::Result<Connection> + Send + Sync>;

/// The columns of one policy row.
const POLICY_COLUMNS: &str = "seq, old_body, new_body, actor_kind, actor, changed_at_ms";

/// The columns of one version row.
const ROW_COLUMNS: &str = "version, body, author_kind, author, created_at_ms";

/// The log of agent profile overlays in one `SQLite` file.
///
/// It does not validate an overlay. Call it through `SqliteOverlayStore` in
/// `autumn-harvest-agent`, which does.
#[doc(hidden)]
pub struct SqliteOverlayLog {
    source: Mutex<Source>,
}

/// Where a call of the log gets its connection.
enum Source {
    /// One connection for the life of the log.
    Held(Connection),
    /// A new connection for each call.
    Factory(SqliteConnector),
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

    /// Open a log that gets a new connection from `connect` for each call.
    ///
    /// The log applies its schema through `connect`. Each later call opens
    /// one connection, runs its statements and drops the connection. So the
    /// log keeps no connection between calls. The log runs one call at a
    /// time, so at most one of its connections is open.
    ///
    /// The log sets `foreign_keys = ON` on each connection, and no other
    /// pragma. The app sets the rest in `connect`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if `connect` fails or the schema
    /// cannot be applied.
    pub fn with_connector(connect: SqliteConnector) -> SqliteResult<Self> {
        let mut conn = connect_once(&connect)?;
        apply_schema(&mut conn)?;
        drop(conn);
        Ok(Self {
            source: Mutex::new(Source::Factory(connect)),
        })
    }

    fn prepare(mut conn: Connection) -> SqliteResult<Self> {
        // As in `SqliteRuntime::open`: wait for another writer, keep a commit
        // across a crash, and turn on the foreign key of the pointer.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;\n\
             PRAGMA foreign_keys = ON;",
        )?;
        apply_schema(&mut conn)?;
        Ok(Self {
            source: Mutex::new(Source::Held(conn)),
        })
    }

    /// Run `f` on a connection under the lock of the log.
    ///
    /// A factory log opens the connection for this call only. The
    /// connection closes before the lock is released.
    fn with_conn<T>(&self, f: impl FnOnce(&mut Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let mut source = self.source.lock().unwrap_or_else(PoisonError::into_inner);
        match &mut *source {
            Source::Held(conn) => f(conn),
            Source::Factory(connect) => {
                let mut conn = connect_once(connect)?;
                let out = f(&mut conn);
                drop(conn);
                out
            }
        }
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

    /// Move the pointer of `kind` as `request` asks, and log the move with
    /// its actor.
    ///
    /// One `IMMEDIATE` transaction reads the pointer, checks the version,
    /// sets the pointer and logs the move. So of two activations with the
    /// same `expected`, only one moves the pointer. A rollback also needs
    /// the move that it read to be the last one. An activation of the
    /// active version logs nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn activate(&self, kind: &str, request: &MoveRequest<'_>) -> SqliteResult<ActivateOutcome> {
        let Ok(stored) = i64::try_from(request.version) else {
            return Ok(ActivateOutcome::UnknownVersion);
        };
        self.with_conn(|conn| activate_in(conn, kind, stored, request))
    }

    /// The last move of the pointer of `kind`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn last_activation(&self, kind: &str) -> SqliteResult<Option<ActivationRow>> {
        self.with_conn(|conn| last_move(conn, kind))
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

    /// The last row of the refine policy log of `kind`, or `None` (issue
    /// #2394).
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn last_policy(&self, kind: &str) -> SqliteResult<Option<PolicyRow>> {
        self.with_conn(|conn| last_policy_in(conn, kind))
    }

    /// Every row of the refine policy log of `kind`, oldest first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails.
    pub fn policy_log(&self, kind: &str) -> SqliteResult<Vec<PolicyRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {POLICY_COLUMNS} FROM harvest_agent_refine_policies
                 WHERE kind = ?1 ORDER BY seq"
            ))?;
            let rows = stmt.query_map(params![kind], read_policy)?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row??);
            }
            Ok(out)
        })
    }

    /// Append one row to the refine policy log of `kind`.
    ///
    /// One `IMMEDIATE` transaction reads the last row, compares its `seq`,
    /// and inserts the next row with the old body. A stale compare returns
    /// [`PolicyAppend::Conflict`].
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn append_policy(&self, kind: &str, write: &PolicyWrite<'_>) -> SqliteResult<PolicyAppend> {
        self.with_conn(|conn| append_policy_in(conn, kind, write))
    }

    /// Put `request.body` as the next version of `kind`, and make it active,
    /// in one `IMMEDIATE` transaction (issue #2394).
    ///
    /// Under the transaction, it checks the pointer and counts the auto
    /// moves of the UTC day of `request.at`. A refusal rolls the put back.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn auto_adopt(
        &self,
        kind: &str,
        request: &AutoAdoptRequest<'_>,
    ) -> SqliteResult<AutoAdoptRaw> {
        self.with_conn(|conn| auto_adopt_in(conn, kind, request))
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

/// One connection from `connect`, with the foreign key of the pointer on.
fn connect_once(connect: &SqliteConnector) -> SqliteResult<Connection> {
    let conn = connect()?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    Ok(conn)
}

/// Apply [`OVERLAY_SCHEMA`] in one `IMMEDIATE` transaction.
fn apply_schema(conn: &mut Connection) -> SqliteResult<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(OVERLAY_SCHEMA)?;
    tx.commit()?;
    Ok(())
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

/// The last policy row of `kind` on `conn`, or `None`.
fn last_policy_in(conn: &Connection, kind: &str) -> SqliteResult<Option<PolicyRow>> {
    conn.query_row(
        &format!(
            "SELECT {POLICY_COLUMNS} FROM harvest_agent_refine_policies
             WHERE kind = ?1 ORDER BY seq DESC LIMIT 1"
        ),
        params![kind],
        read_policy,
    )
    .optional()?
    .transpose()
}

/// The body of [`SqliteOverlayLog::append_policy`].
fn append_policy_in(
    conn: &mut Connection,
    kind: &str,
    write: &PolicyWrite<'_>,
) -> SqliteResult<PolicyAppend> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let last = last_policy_in(&tx, kind)?;
    let actual = last.as_ref().map(|row| row.seq);
    if !policy_write_passes(write.expected, actual) {
        return Ok(PolicyAppend::Conflict { actual });
    }
    let seq = actual.map_or(Some(1), |seq| seq.checked_add(1));
    let Some(seq) = seq.and_then(|seq| i64::try_from(seq).ok()) else {
        return Err(SqliteError::Unsupported(
            "the refine policy log is full".to_owned(),
        ));
    };
    let old_body = last.and_then(|row| row.new_body);
    let changed_at = from_millis(Utc::now().timestamp_millis())?;
    tx.execute(
        "INSERT INTO harvest_agent_refine_policies
             (kind, seq, old_body, new_body, actor_kind, actor, changed_at_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            kind,
            seq,
            old_body,
            write.body,
            write.actor_kind,
            write.actor,
            changed_at.timestamp_millis()
        ],
    )?;
    tx.commit()?;
    Ok(PolicyAppend::Appended(PolicyRow {
        seq: to_version(seq)?,
        old_body,
        new_body: write.body.map(<[u8]>::to_vec),
        actor_kind: write.actor_kind.to_owned(),
        actor: write.actor.to_owned(),
        changed_at,
    }))
}

/// The body of [`SqliteOverlayLog::auto_adopt`]. A refusal drops the
/// transaction, which rolls the put back.
fn auto_adopt_in(
    conn: &mut Connection,
    kind: &str,
    request: &AutoAdoptRequest<'_>,
) -> SqliteResult<AutoAdoptRaw> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // The `IMMEDIATE` transaction holds the write lock, so no policy write
    // commits between this check and the move.
    let revision = last_policy_in(&tx, kind)?.map(|row| row.seq);
    if revision != request.policy_revision {
        return Ok(AutoAdoptRaw::PolicyMoved { actual: revision });
    }
    let head: Option<(i64, Option<i64>)> = tx
        .query_row(
            "SELECT latest, active FROM harvest_agent_overlay_heads WHERE kind = ?1",
            params![kind],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let latest = head.map(|(latest, _)| to_version(latest)).transpose()?;
    let active = head.and_then(|(_, active)| active);
    let actual = active.map(to_version).transpose()?;
    let used = match request.cap {
        Some(_) => {
            let (start, end) = utc_day(request.at);
            let used: i64 = tx.query_row(
                "SELECT COUNT(*) FROM harvest_agent_overlay_activations
                 WHERE kind = ?1 AND actor_kind = ?2
                   AND activated_at_ms >= ?3 AND activated_at_ms < ?4",
                params![
                    kind,
                    AUTHOR_AUTO,
                    start.timestamp_millis(),
                    end.timestamp_millis()
                ],
                |row| row.get(0),
            )?;
            u64::try_from(used).unwrap_or(0)
        }
        None => 0,
    };
    match decide_auto_move(request.expected, actual, used, request.cap) {
        AutoMoveDecision::Conflict => return Ok(AutoAdoptRaw::Conflict { actual }),
        AutoMoveDecision::CapReached => return Ok(AutoAdoptRaw::CapReached { used }),
        AutoMoveDecision::Move => {}
    }
    let Some(version) = next_overlay_version(latest) else {
        return Ok(AutoAdoptRaw::Full);
    };
    let Ok(stored) = i64::try_from(version) else {
        return Ok(AutoAdoptRaw::Full);
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
            request.body,
            AUTHOR_AUTO,
            request.actor,
            created_at.timestamp_millis()
        ],
    )?;
    tx.execute(
        "UPDATE harvest_agent_overlay_heads SET active = ?2 WHERE kind = ?1",
        params![kind, stored],
    )?;
    tx.execute(
        "INSERT INTO harvest_agent_overlay_activations
             (kind, seq, from_version, to_version, actor_kind, actor, activated_at_ms)
         SELECT ?1, COALESCE(MAX(seq), 0) + 1, ?2, ?3, ?4, ?5, ?6
         FROM harvest_agent_overlay_activations WHERE kind = ?1",
        params![
            kind,
            active,
            stored,
            AUTHOR_AUTO,
            request.actor,
            request.at.timestamp_millis()
        ],
    )?;
    tx.commit()?;
    Ok(AutoAdoptRaw::Adopted(OverlayRow {
        version,
        body: request.body.to_vec(),
        author_kind: AUTHOR_AUTO.to_owned(),
        author: request.actor.to_owned(),
        created_at,
    }))
}

/// The last move of `kind` on `conn`, or `None`.
fn last_move(conn: &Connection, kind: &str) -> SqliteResult<Option<ActivationRow>> {
    conn.query_row(
        "SELECT seq, from_version, to_version, actor_kind, actor, activated_at_ms
         FROM harvest_agent_overlay_activations
         WHERE kind = ?1 ORDER BY seq DESC LIMIT 1",
        params![kind],
        read_move,
    )
    .optional()?
    .transpose()
}

/// The body of [`SqliteOverlayLog::activate`].
fn activate_in(
    conn: &mut Connection,
    kind: &str,
    stored: i64,
    request: &MoveRequest<'_>,
) -> SqliteResult<ActivateOutcome> {
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
    let last_seq = if request.needs_last_seq() {
        last_move(&tx, kind)?.map(|row| row.seq)
    } else {
        None
    };
    let outcome = match request.decide(known, actual, last_seq) {
        ActivateDecision::UnknownVersion => ActivateOutcome::UnknownVersion,
        ActivateDecision::Conflict => ActivateOutcome::Conflict { actual },
        ActivateDecision::Activate => {
            if logs_activation(actual, request.version) {
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
                        request.actor_kind,
                        request.actor,
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

/// One policy row. The outer error is a read error. The inner one is a
/// stored value that does not decode.
fn read_policy(row: &Row<'_>) -> rusqlite::Result<SqliteResult<PolicyRow>> {
    let seq: i64 = row.get(0)?;
    let changed_at_ms: i64 = row.get(5)?;
    let (seq, changed_at) = match (to_version(seq), from_millis(changed_at_ms)) {
        (Ok(seq), Ok(changed_at)) => (seq, changed_at),
        (Err(err), _) | (_, Err(err)) => return Ok(Err(err)),
    };
    Ok(Ok(PolicyRow {
        seq,
        old_body: row.get(1)?,
        new_body: row.get(2)?,
        actor_kind: row.get(3)?,
        actor: row.get(4)?,
        changed_at,
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
