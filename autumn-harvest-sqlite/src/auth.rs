//! API tokens and the management audit log on `SQLite` (issue #2469).
//!
//! The tables and rules follow the Postgres tables of the same names:
//!
//! - `harvest_api_tokens` holds one row per scoped API token (issue #942).
//!   It stores the hash of the secret, never the secret.
//! - `harvest_audit_log` holds one row per audited management call (issue
//!   #158). A trigger refuses each `UPDATE`, so a row cannot change after
//!   its write.
//!
//! [`SqliteAuthStore`] opens its own connections. It can share the file of
//! a [`SqliteRuntime`](crate::SqliteRuntime):
//!
//! - Reads use a read-only connection. In WAL mode a read never waits for a
//!   writer, so a token check never waits for the driver task.
//! - Writes use a second connection. Only one connection writes at a time.
//!   A write waits on the busy timeout while the runtime writes. Each
//!   runtime transaction begins `IMMEDIATE`, so the runtime waits for a
//!   store write in the same way. Each store write is one short statement.
//!
//! With an audit retention, the store deletes the audit rows older than it.
//! It does so when it opens, and after each [`PURGE_EVERY`] audit writes.
//!
//! Each time is stored in epoch microseconds, the precision of a Postgres
//! `timestamptz`.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use chrono::{DateTime, Utc};
use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OpenFlags, OptionalExtension as _, Row, params, params_from_iter};
use uuid::Uuid;

use crate::error::{SqliteError, SqliteResult};

/// The tables, the indexes and the audit guard, applied on each open.
const AUTH_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_api_tokens (
    id              TEXT    PRIMARY KEY,
    name            TEXT    NOT NULL,
    token_hash      TEXT    NOT NULL UNIQUE,
    scope           TEXT    NOT NULL CHECK (scope IN ('read', 'mutate', 'admin')),
    created_at_us   INTEGER NOT NULL,
    expires_at_us   INTEGER,
    last_used_at_us INTEGER,
    revoked_at_us   INTEGER,
    created_by      TEXT    NOT NULL,
    tenant          TEXT
);
CREATE INDEX IF NOT EXISTS harvest_api_tokens_created_idx
    ON harvest_api_tokens (created_at_us DESC);
CREATE TABLE IF NOT EXISTS harvest_audit_log (
    id               TEXT    PRIMARY KEY,
    occurred_at_us   INTEGER NOT NULL,
    actor            TEXT    NOT NULL,
    operation        TEXT    NOT NULL,
    target_type      TEXT    NOT NULL,
    target_id        TEXT,
    route_or_command TEXT    NOT NULL,
    request_id       TEXT,
    idempotency_key  TEXT,
    status           TEXT    NOT NULL CHECK (status IN ('succeeded', 'failed')),
    error_summary    TEXT,
    shard_id         INTEGER,
    source           TEXT    NOT NULL CHECK (source IN ('api', 'cli', 'ui'))
);
CREATE INDEX IF NOT EXISTS harvest_audit_occurred_idx
    ON harvest_audit_log (occurred_at_us DESC, id DESC);
CREATE INDEX IF NOT EXISTS harvest_audit_actor_idx
    ON harvest_audit_log (actor, occurred_at_us DESC);
CREATE TRIGGER IF NOT EXISTS harvest_audit_log_immutable_upd
    BEFORE UPDATE ON harvest_audit_log
BEGIN
    SELECT RAISE(ABORT, 'harvest_audit_log rows do not change (issue #2469)');
END;
";

/// The columns of one token row, in the order [`read_token`] reads them.
const TOKEN_COLUMNS: &str = "id, name, token_hash, scope, created_at_us, expires_at_us, \
                             last_used_at_us, revoked_at_us, created_by, tenant";

/// The columns of one audit row, in the order [`read_audit`] reads them.
const AUDIT_COLUMNS: &str = "id, occurred_at_us, actor, operation, target_type, target_id, \
                             route_or_command, request_id, idempotency_key, status, \
                             error_summary, shard_id, source";

/// The most audit rows one read returns, as on Postgres.
pub const MAX_AUDIT_PAGE: i64 = 500;

/// The store deletes the expired audit rows after this many audit writes.
pub const PURGE_EVERY: u64 = 256;

/// A token to store.
#[derive(Clone)]
pub struct NewToken<'a> {
    /// The token id.
    pub id: Uuid,
    /// The label of the caller.
    pub name: &'a str,
    /// The hash of the secret. The secret itself is never stored.
    pub token_hash: &'a str,
    /// `read`, `mutate` or `admin`.
    pub scope: &'a str,
    /// The mint time.
    pub created_at: DateTime<Utc>,
    /// The expiry, if any.
    pub expires_at: Option<DateTime<Utc>>,
    /// The actor that minted the token.
    pub created_by: &'a str,
    /// The tenant claim, if any.
    pub tenant: Option<&'a str>,
}

impl std::fmt::Debug for NewToken<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The hash is the lookup key, so a log line must not show it.
        f.debug_struct("NewToken")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("token_hash", &"<redacted>")
            .field("scope", &self.scope)
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

/// One stored token row.
#[derive(Clone, PartialEq, Eq)]
pub struct StoredToken {
    /// The token id.
    pub id: Uuid,
    /// The label of the caller.
    pub name: String,
    /// The hash of the secret.
    pub token_hash: String,
    /// `read`, `mutate` or `admin`.
    pub scope: String,
    /// The mint time.
    pub created_at: DateTime<Utc>,
    /// The expiry, if any.
    pub expires_at: Option<DateTime<Utc>>,
    /// The last use, if any. The write is best effort.
    pub last_used_at: Option<DateTime<Utc>>,
    /// The revocation time, if any.
    pub revoked_at: Option<DateTime<Utc>>,
    /// The actor that minted the token.
    pub created_by: String,
    /// The tenant claim, if any.
    pub tenant: Option<String>,
}

impl std::fmt::Debug for StoredToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The hash is the lookup key, so a log line must not show it.
        f.debug_struct("StoredToken")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("token_hash", &"<redacted>")
            .field("scope", &self.scope)
            .field("revoked_at", &self.revoked_at)
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

/// An audit row to store.
#[derive(Debug, Clone)]
pub struct NewAuditRow<'a> {
    /// The time of the call.
    pub occurred_at: DateTime<Utc>,
    /// Who made the call.
    pub actor: &'a str,
    /// The operation, for example `token.create`.
    pub operation: &'a str,
    /// The kind of the target.
    pub target_type: &'a str,
    /// The target id, if any.
    pub target_id: Option<&'a str>,
    /// The route or the command.
    pub route_or_command: &'a str,
    /// The request id, if any.
    pub request_id: Option<&'a str>,
    /// The idempotency key, if any.
    pub idempotency_key: Option<&'a str>,
    /// `succeeded` or `failed`.
    pub status: &'a str,
    /// A short reason for a failure.
    pub error_summary: Option<&'a str>,
    /// The shard, if any.
    pub shard_id: Option<i32>,
    /// `api`, `cli` or `ui`.
    pub source: &'a str,
}

/// One stored audit row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditRow {
    /// The row id. It is a UUID v7, so it sorts by time.
    pub id: Uuid,
    /// The time of the call.
    pub occurred_at: DateTime<Utc>,
    /// Who made the call.
    pub actor: String,
    /// The operation.
    pub operation: String,
    /// The kind of the target.
    pub target_type: String,
    /// The target id, if any.
    pub target_id: Option<String>,
    /// The route or the command.
    pub route_or_command: String,
    /// The request id, if any.
    pub request_id: Option<String>,
    /// The idempotency key, if any.
    pub idempotency_key: Option<String>,
    /// `succeeded` or `failed`.
    pub status: String,
    /// A short reason for a failure.
    pub error_summary: Option<String>,
    /// The shard, if any.
    pub shard_id: Option<i32>,
    /// `api`, `cli` or `ui`.
    pub source: String,
}

/// The filters of an audit read. They match those of the Postgres read.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    /// Only this actor.
    pub actor: Option<String>,
    /// Only this operation.
    pub operation: Option<String>,
    /// Only this target kind.
    pub target_type: Option<String>,
    /// Only this target id.
    pub target_id: Option<String>,
    /// Only this status.
    pub status: Option<String>,
    /// Only rows at or after this time.
    pub since: Option<DateTime<Utc>>,
    /// Only rows before this time.
    pub before: Option<DateTime<Utc>>,
    /// With `before`, also rows at `before` with a smaller id.
    pub before_id: Option<Uuid>,
    /// The most rows to read. It is clamped to 1 through [`MAX_AUDIT_PAGE`].
    pub limit: i64,
}

/// The token and audit tables of one `SQLite` file.
pub struct SqliteAuthStore {
    /// The connection of each write.
    conn: Mutex<Connection>,
    /// A read-only connection, or `None` for an in-memory store.
    reader: Option<Mutex<Connection>>,
    /// Audit rows older than this many days are deleted. `None` keeps them.
    audit_retention_days: Option<i64>,
    /// The audit writes since the last purge.
    audit_writes: AtomicU64,
}

impl std::fmt::Debug for SqliteAuthStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteAuthStore").finish_non_exhaustive()
    }
}

impl SqliteAuthStore {
    /// Open the store in the database at `path`, and create its tables.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened or the
    /// schema cannot be applied.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        let mut store = Self::prepare(Connection::open(path.as_ref())?)?;
        // The read connection opens after the schema exists.
        let reader = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        reader.busy_timeout(std::time::Duration::from_secs(5))?;
        store.reader = Some(Mutex::new(reader));
        Ok(store)
    }

    /// Open a store in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the schema cannot be applied.
    pub fn open_in_memory() -> SqliteResult<Self> {
        Self::prepare(Connection::open_in_memory()?)
    }

    fn prepare(conn: Connection) -> SqliteResult<Self> {
        // As in `SqliteRuntime::open`: wait for the other writer, and keep a
        // commit across a crash.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;",
        )?;
        conn.execute_batch(AUTH_SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
            reader: None,
            audit_retention_days: None,
            audit_writes: AtomicU64::new(0),
        })
    }

    /// Keep audit rows for `days` days, as `audit_retention_days` does on
    /// Postgres. Zero or less keeps them for ever. The store deletes the
    /// expired rows now.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the first purge fails.
    pub fn with_audit_retention_days(mut self, days: i64) -> SqliteResult<Self> {
        self.audit_retention_days = (days > 0).then_some(days);
        self.purge_expired_audit()?;
        Ok(self)
    }

    /// Run `f` on the write connection under its lock.
    fn with_conn<T>(&self, f: impl FnOnce(&Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let conn = self.conn.lock().unwrap_or_else(PoisonError::into_inner);
        f(&conn)
    }

    /// Run `f` on the read connection under its lock. An in-memory store
    /// has one connection only.
    fn with_reader<T>(&self, f: impl FnOnce(&Connection) -> SqliteResult<T>) -> SqliteResult<T> {
        let Some(reader) = &self.reader else {
            return self.with_conn(f);
        };
        let conn = reader.lock().unwrap_or_else(PoisonError::into_inner);
        f(&conn)
    }

    /// Delete the audit rows older than `cutoff`. Returns how many it
    /// deleted.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the delete fails.
    pub fn purge_audit_before(&self, cutoff: DateTime<Utc>) -> SqliteResult<usize> {
        self.with_conn(|conn| {
            Ok(conn.execute(
                "DELETE FROM harvest_audit_log WHERE occurred_at_us < ?1",
                params![cutoff.timestamp_micros()],
            )?)
        })
    }

    /// Delete the audit rows that the retention no longer keeps.
    fn purge_expired_audit(&self) -> SqliteResult<()> {
        if let Some(days) = self.audit_retention_days {
            let cutoff = Utc::now() - chrono::Duration::days(days);
            let deleted = self.purge_audit_before(cutoff)?;
            if deleted > 0 {
                tracing::info!(deleted, days, "harvest: purged expired audit rows");
            }
        }
        Ok(())
    }

    /// Store a new token, and return the stored row.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the insert fails, for example on
    /// a hash that is already stored or an unknown scope.
    pub fn insert_token(&self, token: &NewToken<'_>) -> SqliteResult<StoredToken> {
        self.with_conn(|conn| {
            conn.execute(
                "INSERT INTO harvest_api_tokens
                 (id, name, token_hash, scope, created_at_us, expires_at_us, created_by, tenant)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    token.id.to_string(),
                    token.name,
                    token.token_hash,
                    token.scope,
                    token.created_at.timestamp_micros(),
                    token.expires_at.map(|at| at.timestamp_micros()),
                    token.created_by,
                    token.tenant,
                ],
            )?;
            token_where(conn, "id = ?1", &token.id.to_string())?
                .ok_or_else(|| SqliteError::Corrupt("a stored token is missing".to_owned()))
        })
    }

    /// Every token, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails, or
    /// [`SqliteError::Corrupt`] for a row that does not decode.
    pub fn list_tokens(&self) -> SqliteResult<Vec<StoredToken>> {
        self.with_reader(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {TOKEN_COLUMNS} FROM harvest_api_tokens
                 ORDER BY created_at_us DESC, id DESC"
            ))?;
            let rows = stmt.query_map([], read_token)?;
            rows.map(|row| row?).collect()
        })
    }

    /// The token whose stored hash is `token_hash`, or `None`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails, or
    /// [`SqliteError::Corrupt`] for a row that does not decode.
    pub fn token_by_hash(&self, token_hash: &str) -> SqliteResult<Option<StoredToken>> {
        self.with_reader(|conn| token_where(conn, "token_hash = ?1", token_hash))
    }

    /// Revoke the token `id` at `now`. Returns `false` when the token is
    /// unknown or already revoked.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the update fails.
    pub fn revoke_token(&self, id: Uuid, now: DateTime<Utc>) -> SqliteResult<bool> {
        self.with_conn(|conn| {
            let changed = conn.execute(
                "UPDATE harvest_api_tokens SET revoked_at_us = ?2
                 WHERE id = ?1 AND revoked_at_us IS NULL",
                params![id.to_string(), now.timestamp_micros()],
            )?;
            Ok(changed > 0)
        })
    }

    /// Record a use of the token `id` at `now`.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the update fails.
    pub fn touch_token(&self, id: Uuid, now: DateTime<Utc>) -> SqliteResult<()> {
        self.with_conn(|conn| {
            conn.execute(
                "UPDATE harvest_api_tokens SET last_used_at_us = ?2 WHERE id = ?1",
                params![id.to_string(), now.timestamp_micros()],
            )?;
            Ok(())
        })
    }

    /// Store one audit row, and return its id.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the insert fails, for example on
    /// an unknown status or source.
    pub fn insert_audit(&self, row: &NewAuditRow<'_>) -> SqliteResult<Uuid> {
        let id = Uuid::now_v7();
        self.with_conn(|conn| {
            conn.execute(
                &format!(
                    "INSERT INTO harvest_audit_log ({AUDIT_COLUMNS})
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)"
                ),
                params![
                    id.to_string(),
                    row.occurred_at.timestamp_micros(),
                    row.actor,
                    row.operation,
                    row.target_type,
                    row.target_id,
                    row.route_or_command,
                    row.request_id,
                    row.idempotency_key,
                    row.status,
                    row.error_summary,
                    row.shard_id,
                    row.source,
                ],
            )?;
            Ok(id)
        })?;
        // A purge failure must not lose the row just written.
        if self.audit_writes.fetch_add(1, Ordering::Relaxed) % PURGE_EVERY == PURGE_EVERY - 1
            && let Err(error) = self.purge_expired_audit()
        {
            tracing::warn!(%error, "harvest: audit purge failed");
        }
        Ok(id)
    }

    /// The audit rows that match `filter`, newest first.
    ///
    /// The order is `(occurred_at, id)`, descending. `before` with
    /// `before_id` pages past rows that share one time.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the query fails, or
    /// [`SqliteError::Corrupt`] for a row that does not decode.
    pub fn list_audit(&self, filter: &AuditFilter) -> SqliteResult<Vec<AuditRow>> {
        let mut clauses: Vec<&str> = Vec::new();
        let mut values: Vec<SqlValue> = Vec::new();
        for (clause, value) in [
            ("actor = ?", &filter.actor),
            ("operation = ?", &filter.operation),
            ("target_type = ?", &filter.target_type),
            ("target_id = ?", &filter.target_id),
            ("status = ?", &filter.status),
        ] {
            if let Some(value) = value {
                clauses.push(clause);
                values.push(SqlValue::Text(value.clone()));
            }
        }
        if let Some(since) = filter.since {
            clauses.push("occurred_at_us >= ?");
            values.push(SqlValue::Integer(since.timestamp_micros()));
        }
        match (filter.before, filter.before_id) {
            (Some(before), Some(before_id)) => {
                clauses.push("(occurred_at_us < ? OR (occurred_at_us = ? AND id < ?))");
                values.push(SqlValue::Integer(before.timestamp_micros()));
                values.push(SqlValue::Integer(before.timestamp_micros()));
                values.push(SqlValue::Text(before_id.to_string()));
            }
            (Some(before), None) => {
                clauses.push("occurred_at_us < ?");
                values.push(SqlValue::Integer(before.timestamp_micros()));
            }
            (None, _) => {}
        }
        let filter_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        values.push(SqlValue::Integer(filter.limit.clamp(1, MAX_AUDIT_PAGE)));
        self.with_reader(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {AUDIT_COLUMNS} FROM harvest_audit_log {filter_sql}
                 ORDER BY occurred_at_us DESC, id DESC LIMIT ?"
            ))?;
            let rows = stmt.query_map(params_from_iter(values), read_audit)?;
            rows.map(|row| row?).collect()
        })
    }
}

/// The one token row that matches `clause` with `value`, or `None`.
fn token_where(conn: &Connection, clause: &str, value: &str) -> SqliteResult<Option<StoredToken>> {
    conn.query_row(
        &format!("SELECT {TOKEN_COLUMNS} FROM harvest_api_tokens WHERE {clause}"),
        params![value],
        read_token,
    )
    .optional()?
    .transpose()
}

/// One token row. The outer error is a read error. The inner one is a
/// stored value that does not decode.
fn read_token(row: &Row<'_>) -> rusqlite::Result<SqliteResult<StoredToken>> {
    let id: String = row.get(0)?;
    let created_at: i64 = row.get(4)?;
    let expires_at: Option<i64> = row.get(5)?;
    let last_used_at: Option<i64> = row.get(6)?;
    let revoked_at: Option<i64> = row.get(7)?;
    let decoded = (|| {
        Ok((
            parse_id(&id)?,
            from_micros(created_at)?,
            expires_at.map(from_micros).transpose()?,
            last_used_at.map(from_micros).transpose()?,
            revoked_at.map(from_micros).transpose()?,
        ))
    })();
    let (id, created_at, expires_at, last_used_at, revoked_at) = match decoded {
        Ok(fields) => fields,
        Err(error) => return Ok(Err(error)),
    };
    Ok(Ok(StoredToken {
        id,
        name: row.get(1)?,
        token_hash: row.get(2)?,
        scope: row.get(3)?,
        created_at,
        expires_at,
        last_used_at,
        revoked_at,
        created_by: row.get(8)?,
        tenant: row.get(9)?,
    }))
}

/// One audit row. The outer error is a read error. The inner one is a
/// stored value that does not decode.
fn read_audit(row: &Row<'_>) -> rusqlite::Result<SqliteResult<AuditRow>> {
    let id: String = row.get(0)?;
    let occurred_at: i64 = row.get(1)?;
    let (id, occurred_at) = match (parse_id(&id), from_micros(occurred_at)) {
        (Ok(id), Ok(occurred_at)) => (id, occurred_at),
        (Err(error), _) | (_, Err(error)) => return Ok(Err(error)),
    };
    Ok(Ok(AuditRow {
        id,
        occurred_at,
        actor: row.get(2)?,
        operation: row.get(3)?,
        target_type: row.get(4)?,
        target_id: row.get(5)?,
        route_or_command: row.get(6)?,
        request_id: row.get(7)?,
        idempotency_key: row.get(8)?,
        status: row.get(9)?,
        error_summary: row.get(10)?,
        shard_id: row.get(11)?,
        source: row.get(12)?,
    }))
}

fn parse_id(raw: &str) -> SqliteResult<Uuid> {
    Uuid::parse_str(raw).map_err(|_| SqliteError::Corrupt(format!("a stored id {raw:?}")))
}

fn from_micros(micros: i64) -> SqliteResult<DateTime<Utc>> {
    DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| SqliteError::Corrupt(format!("a stored time {micros}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn store() -> SqliteAuthStore {
        SqliteAuthStore::open_in_memory().unwrap()
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + seconds, 0).unwrap()
    }

    fn token<'a>(name: &'a str, hash: &'a str, created_at: DateTime<Utc>) -> NewToken<'a> {
        NewToken {
            id: Uuid::new_v4(),
            name,
            token_hash: hash,
            scope: "read",
            created_at,
            expires_at: None,
            created_by: "ops",
            tenant: None,
        }
    }

    fn audit<'a>(actor: &'a str, operation: &'a str, at: DateTime<Utc>) -> NewAuditRow<'a> {
        NewAuditRow {
            occurred_at: at,
            actor,
            operation,
            target_type: "route",
            target_id: None,
            route_or_command: "GET /workflows",
            request_id: None,
            idempotency_key: None,
            status: "failed",
            error_summary: Some("denied"),
            shard_id: None,
            source: "api",
        }
    }

    #[test]
    fn a_token_round_trips_and_is_found_by_its_hash() {
        let store = store();
        let mut new = token("ci", "h1", at(0));
        new.expires_at = Some(at(60));
        new.tenant = Some("acme");
        new.scope = "admin";
        let stored = store.insert_token(&new).unwrap();
        assert_eq!(stored.id, new.id);
        assert_eq!(stored.scope, "admin");
        assert_eq!(stored.expires_at, Some(at(60)));
        assert_eq!(stored.tenant.as_deref(), Some("acme"));
        assert_eq!(store.token_by_hash("h1").unwrap(), Some(stored));
        assert_eq!(store.token_by_hash("other").unwrap(), None);
    }

    #[test]
    fn a_hash_is_unique_and_a_scope_is_checked() {
        let store = store();
        store.insert_token(&token("a", "h", at(0))).unwrap();
        assert!(store.insert_token(&token("b", "h", at(1))).is_err());
        let mut bad = token("c", "h2", at(2));
        bad.scope = "root";
        assert!(store.insert_token(&bad).is_err());
    }

    #[test]
    fn tokens_list_newest_first() {
        let store = store();
        store.insert_token(&token("old", "h1", at(0))).unwrap();
        store.insert_token(&token("new", "h2", at(5))).unwrap();
        let names: Vec<String> = store
            .list_tokens()
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert_eq!(names, ["new", "old"]);
    }

    #[test]
    fn a_revoke_is_idempotent_and_a_touch_records_the_use() {
        let store = store();
        let stored = store.insert_token(&token("ci", "h", at(0))).unwrap();
        assert!(store.revoke_token(stored.id, at(10)).unwrap());
        assert!(!store.revoke_token(stored.id, at(20)).unwrap());
        assert!(!store.revoke_token(Uuid::new_v4(), at(20)).unwrap());
        store.touch_token(stored.id, at(30)).unwrap();
        let read = store.token_by_hash("h").unwrap().unwrap();
        assert_eq!(read.revoked_at, Some(at(10)), "the first revoke stays");
        assert_eq!(read.last_used_at, Some(at(30)));
    }

    #[test]
    fn the_debug_form_hides_the_hash() {
        let store = store();
        let stored = store
            .insert_token(&token("ci", "secret-hash", at(0)))
            .unwrap();
        assert!(!format!("{stored:?}").contains("secret-hash"));
    }

    #[test]
    fn an_audit_row_round_trips_and_never_changes() {
        let store = store();
        let mut row = audit("token:1", "authz.deny", at(0));
        row.shard_id = Some(3);
        row.request_id = Some("r-1");
        let id = store.insert_audit(&row).unwrap();
        let read = store.list_audit(&AuditFilter {
            limit: 10,
            ..AuditFilter::default()
        });
        let read = read.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read[0].id, id);
        assert_eq!(read[0].shard_id, Some(3));
        assert_eq!(read[0].request_id.as_deref(), Some("r-1"));
        assert_eq!(read[0].error_summary.as_deref(), Some("denied"));
        let changed = store.with_conn(|conn| {
            Ok(conn.execute("UPDATE harvest_audit_log SET actor = 'forged'", []))
        });
        assert!(changed.unwrap().is_err(), "an audit row is immutable");
    }

    #[test]
    fn the_audit_checks_status_and_source() {
        let store = store();
        let mut row = audit("a", "op", at(0));
        row.status = "maybe";
        assert!(store.insert_audit(&row).is_err());
        let mut row = audit("a", "op", at(0));
        row.source = "shell";
        assert!(store.insert_audit(&row).is_err());
    }

    #[test]
    fn the_audit_filters_and_orders_newest_first() {
        let store = store();
        store
            .insert_audit(&audit("alice", "token.create", at(0)))
            .unwrap();
        store
            .insert_audit(&audit("bob", "authz.deny", at(1)))
            .unwrap();
        store
            .insert_audit(&audit("alice", "authz.deny", at(2)))
            .unwrap();
        let all = |filter: AuditFilter| -> Vec<(String, String)> {
            store
                .list_audit(&AuditFilter {
                    limit: 50,
                    ..filter
                })
                .unwrap()
                .into_iter()
                .map(|row| (row.actor, row.operation))
                .collect()
        };
        let pair = |a: &str, o: &str| (a.to_owned(), o.to_owned());
        assert_eq!(
            all(AuditFilter::default()),
            [
                pair("alice", "authz.deny"),
                pair("bob", "authz.deny"),
                pair("alice", "token.create")
            ]
        );
        assert_eq!(
            all(AuditFilter {
                actor: Some("alice".into()),
                operation: Some("authz.deny".into()),
                ..AuditFilter::default()
            }),
            [pair("alice", "authz.deny")]
        );
        assert_eq!(
            all(AuditFilter {
                since: Some(at(1)),
                before: Some(at(2)),
                ..AuditFilter::default()
            }),
            [pair("bob", "authz.deny")]
        );
        let limited = store
            .list_audit(&AuditFilter {
                limit: 0,
                ..AuditFilter::default()
            })
            .unwrap();
        assert_eq!(limited.len(), 1, "a limit below 1 reads one row");
    }

    #[test]
    fn before_id_pages_past_rows_that_share_a_time() {
        let store = store();
        let ids: Vec<Uuid> = (0..3)
            .map(|_| store.insert_audit(&audit("a", "op", at(0))).unwrap())
            .collect();
        let page = |before_id: Option<Uuid>| {
            store
                .list_audit(&AuditFilter {
                    before: Some(at(0)),
                    before_id,
                    limit: 10,
                    ..AuditFilter::default()
                })
                .unwrap()
                .into_iter()
                .map(|row| row.id)
                .collect::<Vec<_>>()
        };
        assert!(page(None).is_empty(), "before alone is strict");
        assert_eq!(page(Some(ids[2])), [ids[1], ids[0]]);
        let newest = store
            .list_audit(&AuditFilter {
                limit: 1,
                since: Some(at(0) - Duration::seconds(1)),
                ..AuditFilter::default()
            })
            .unwrap();
        assert_eq!(newest[0].id, ids[2], "a v7 id breaks the tie by order");
    }

    #[test]
    fn the_debug_form_of_a_new_token_hides_the_hash() {
        let new = token("ci", "secret-hash", at(0));
        assert!(!format!("{new:?}").contains("secret-hash"));
    }

    #[test]
    fn the_retention_purges_at_open_and_after_each_batch_of_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.db");
        let old = Utc::now() - Duration::days(10);
        let store = SqliteAuthStore::open(&path).unwrap();
        store.insert_audit(&audit("a", "old", old)).unwrap();
        store.insert_audit(&audit("a", "new", Utc::now())).unwrap();
        drop(store);

        let ops = |store: &SqliteAuthStore| -> Vec<String> {
            let filter = AuditFilter {
                limit: 500,
                ..AuditFilter::default()
            };
            let rows = store.list_audit(&filter).unwrap();
            rows.into_iter().map(|row| row.operation).collect()
        };
        let kept = SqliteAuthStore::open(&path)
            .unwrap()
            .with_audit_retention_days(0)
            .unwrap();
        assert_eq!(ops(&kept).len(), 2, "zero keeps every row");
        let store = SqliteAuthStore::open(&path)
            .unwrap()
            .with_audit_retention_days(7)
            .unwrap();
        assert_eq!(ops(&store), ["new"], "the open purges the old row");

        // The write that ends a batch of `PURGE_EVERY` runs the purge.
        for _ in 0..PURGE_EVERY - 2 {
            store.insert_audit(&audit("a", "fill", Utc::now())).unwrap();
        }
        store.insert_audit(&audit("a", "late", old)).unwrap();
        assert!(
            ops(&store).contains(&"late".to_owned()),
            "before the batch ends"
        );
        store.insert_audit(&audit("a", "end", Utc::now())).unwrap();
        assert!(
            !ops(&store).contains(&"late".to_owned()),
            "the batch purges it"
        );
    }

    #[test]
    fn the_store_shares_a_file_with_a_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("h.db");
        let runtime = crate::SqliteRuntime::open(&path).unwrap();
        let store = SqliteAuthStore::open(&path).unwrap();
        store.insert_token(&token("ci", "h", at(0))).unwrap();
        let again = SqliteAuthStore::open(&path).unwrap();
        assert_eq!(again.list_tokens().unwrap().len(), 1);
        drop(runtime);
    }
}
