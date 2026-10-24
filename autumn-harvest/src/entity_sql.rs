//! Read-only SQL over the state of one keyed entity (issue #2260).
//!
//! [`Worker::query_entity_sql`](crate::worker::Worker::query_entity_sql)
//! runs user SQL over one entity. Each query gets a fresh in-memory SQLite
//! database with one table, [`ENTITY_TABLE`], and one row:
//!
//! | Column | Type | Meaning |
//! |---|---|---|
//! | `workflow_name` | `TEXT` | The entity type. |
//! | `workflow_id` | `TEXT` | The entity key. |
//! | `execution_id` | `TEXT` | The run that recorded the snapshot. |
//! | `event_id` | `INTEGER` | The history position of the snapshot. |
//! | `deleted` | `INTEGER` | `1` when the snapshot ended the entity. |
//! | `state` | `TEXT` | The state, as JSON. |
//! | `stats` | `TEXT` | The [`EntityStats`](crate::entity::EntityStats), as JSON. |
//!
//! The SQLite JSON functions read the state, for example
//! `SELECT value FROM entity, json_each(entity.state)`.
//!
//! ## Rules
//!
//! - **Read-only.** An authorizer allows reads and functions only. Each
//!   statement must be read-only, and `query_only` is on.
//! - **No file.** The database lives in memory. `ATTACH` is refused, and
//!   temporary data stays in memory. So no plaintext reaches the disk.
//! - **Bounded.** A query stops after [`MAX_STEPS`] virtual machine steps,
//!   after [`MAX_QUERY_TIME`], or when SQLite memory grows by more than
//!   [`MAX_QUERY_MEMORY`]. A result stops past [`MAX_ROWS`] rows or
//!   [`MAX_RESULT_BYTES`] bytes. A string or blob holds at most
//!   [`MAX_VALUE_BYTES`] bytes.
//! - **One statement.** A second statement is refused.
//! - **Blobs as hex.** A blob result is a lowercase hex string.
//!
//! The owner-local projection file of
//! [ADR 0012](../../docs/adr/0012-owner-disk-cache.md) holds the snapshot
//! sealed. This module never reads or writes that file.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::limits::Limit;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, ErrorCode, params};
use serde_json::Value;

use crate::entity::EntitySnapshot;
use crate::error::HarvestError;
use crate::types::ExecutionId;

/// The table that user SQL reads.
pub const ENTITY_TABLE: &str = "entity";

/// The most rows that one query returns.
pub const MAX_ROWS: usize = 10_000;

/// The most virtual machine steps that one query runs.
pub const MAX_STEPS: u64 = 50_000_000;

/// The longest SQL text that a query accepts, in bytes.
pub const MAX_SQL_BYTES: i32 = 100_000;

/// The longest string or blob that a query makes, in bytes.
pub const MAX_VALUE_BYTES: i32 = 1024 * 1024;

/// The most bytes of text and blobs that one result holds.
pub const MAX_RESULT_BYTES: usize = 16 * 1024 * 1024;

/// The most time that one query runs.
pub const MAX_QUERY_TIME: Duration = Duration::from_secs(5);

/// The most that SQLite memory grows while one query runs, in bytes.
///
/// SQLite counts its memory for the whole process. So concurrent queries
/// share this budget, and [`MAX_CONCURRENT_QUERIES`] bounds their number.
pub const MAX_QUERY_MEMORY: i64 = 64 * 1024 * 1024;

/// The most queries that one process runs at a time.
pub const MAX_CONCURRENT_QUERIES: usize = 4;

/// The query slots of this process. A permit lives as long as its query
/// thread, also after the caller drops the future.
pub(crate) static QUERY_SLOTS: tokio::sync::Semaphore =
    tokio::sync::Semaphore::const_new(MAX_CONCURRENT_QUERIES);

/// The snapshot of one entity that a query reads.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EntityRecord {
    /// The entity type.
    pub workflow_name: String,
    /// The entity key.
    pub workflow_id: String,
    /// The run that recorded the snapshot.
    pub execution_id: ExecutionId,
    /// The history position of the snapshot in that run.
    pub event_id: i32,
    /// The snapshot ended the entity.
    pub deleted: bool,
    /// The state and the counters.
    pub snapshot: EntitySnapshot<Value>,
}

impl EntityRecord {
    /// A record of the snapshot at `event_id` of the run `execution_id`.
    #[must_use]
    pub fn new(
        workflow_name: impl Into<String>,
        workflow_id: impl Into<String>,
        execution_id: ExecutionId,
        event_id: i32,
        deleted: bool,
        snapshot: EntitySnapshot<Value>,
    ) -> Self {
        Self {
            workflow_name: workflow_name.into(),
            workflow_id: workflow_id.into(),
            execution_id,
            event_id,
            deleted,
            snapshot,
        }
    }
}

/// Where a query found the snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum EntitySqlSource {
    /// The owner-local projection file.
    File,
    /// The snapshot event in Postgres history.
    History,
}

/// The result of one query.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EntitySqlRows {
    /// The column names, in order.
    pub columns: Vec<String>,
    /// The rows. Each holds one value for each column.
    pub rows: Vec<Vec<Value>>,
    /// Where the query found the snapshot.
    pub source: EntitySqlSource,
}

/// Why a query failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EntitySqlError {
    /// The entity has no snapshot.
    #[error("the entity {workflow_name}/{workflow_id} has no snapshot")]
    NotFound {
        /// The entity type.
        workflow_name: String,
        /// The entity key.
        workflow_id: String,
    },
    /// An erasure removed the snapshot.
    #[error("an erasure removed the snapshot of the entity")]
    Erased,
    /// History holds no state for the snapshot, for example past a payload
    /// cap.
    #[error("history holds no state for the entity: {0}")]
    NotRecorded(String),
    /// The SQL is not one read-only statement.
    #[error("the SQL is not one read-only query: {0}")]
    Refused(String),
    /// SQLite refused or failed the query.
    #[error("the query failed: {0}")]
    Sql(String),
    /// The result has more than [`MAX_ROWS`] rows.
    #[error("the result has more than {limit} rows")]
    TooManyRows {
        /// The row cap.
        limit: usize,
    },
    /// The result holds more than [`MAX_RESULT_BYTES`] bytes.
    #[error("the result holds more than {limit} bytes")]
    TooLarge {
        /// The byte cap.
        limit: usize,
    },
    /// The query ran past [`MAX_STEPS`], [`MAX_QUERY_TIME`] or
    /// [`MAX_QUERY_MEMORY`], or its caller went away.
    #[error("the query ran past its step, time or memory budget")]
    TooCostly,
    /// A database, codec or runtime error.
    #[error(transparent)]
    Harvest(#[from] HarvestError),
}

/// Run `sql` over `record` with the positional `params`.
///
/// # Errors
///
/// [`EntitySqlError::Refused`] for SQL that is not one read-only statement,
/// [`EntitySqlError::TooManyRows`], [`EntitySqlError::TooLarge`],
/// [`EntitySqlError::TooCostly`] and [`EntitySqlError::Sql`] for any other
/// SQLite error.
pub fn run(
    record: &EntityRecord,
    sql: &str,
    params: &[Value],
    source: EntitySqlSource,
) -> Result<EntitySqlRows, EntitySqlError> {
    run_cancellable(
        record,
        sql,
        params,
        source,
        &Arc::new(AtomicBool::new(false)),
    )
}

/// [`run`] that also stops when `cancel` turns `true`.
///
/// The worker sets `cancel` when the caller drops its future. A blocking
/// thread then does not run on for nothing.
pub(crate) fn run_cancellable(
    record: &EntityRecord,
    sql: &str,
    params: &[Value],
    source: EntitySqlSource,
    cancel: &Arc<AtomicBool>,
) -> Result<EntitySqlRows, EntitySqlError> {
    let conn = open(record)?;
    let deadline = Instant::now() + MAX_QUERY_TIME;
    let memory_at_start = sqlite_memory_used();
    let cancel = Arc::clone(cancel);
    let mut ticks: u64 = 0;
    conn.progress_handler(
        STEP_INTERVAL,
        Some(move || {
            ticks += 1;
            ticks > MAX_TICKS
                || Instant::now() >= deadline
                || cancel.load(Ordering::Relaxed)
                || sqlite_memory_used().saturating_sub(memory_at_start) > MAX_QUERY_MEMORY
        }),
    )
    .map_err(|e| sql_error(&e))?;
    let mut stmt = conn.prepare(sql).map_err(|e| prepare_error(&e))?;
    if !stmt.readonly() {
        return Err(EntitySqlError::Refused(
            "the statement is not read-only".to_string(),
        ));
    }
    if stmt.column_count() == 0 {
        return Err(EntitySqlError::Refused(
            "the statement returns no columns".to_string(),
        ));
    }
    let columns: Vec<String> = stmt
        .column_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let bound: Vec<rusqlite::types::Value> = params.iter().map(to_sql).collect();
    let mut rows = stmt
        .query(rusqlite::params_from_iter(bound))
        .map_err(|e| step_error(&e))?;
    let mut out = Vec::new();
    let mut bytes: usize = 0;
    while let Some(row) = rows.next().map_err(|e| step_error(&e))? {
        if out.len() == MAX_ROWS {
            return Err(EntitySqlError::TooManyRows { limit: MAX_ROWS });
        }
        let mut values = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            let value = row.get_ref(index).map_err(|e| sql_error(&e))?;
            // Count each value before it turns into JSON, so a large result
            // never reaches the heap of the worker.
            bytes = bytes.saturating_add(result_bytes(value));
            if bytes > MAX_RESULT_BYTES {
                return Err(EntitySqlError::TooLarge {
                    limit: MAX_RESULT_BYTES,
                });
            }
            values.push(to_json(value));
        }
        out.push(values);
    }
    Ok(EntitySqlRows {
        columns,
        rows: out,
        source,
    })
}

/// How often the progress handler runs, in virtual machine steps.
///
/// One step can make a value of [`MAX_VALUE_BYTES`]. A short interval keeps
/// the memory growth between two checks small.
const STEP_INTERVAL: i32 = 100;

/// The progress handler calls that [`MAX_STEPS`] allows. The divisor is
/// [`STEP_INTERVAL`]. A unit test checks that the two agree.
const MAX_TICKS: u64 = MAX_STEPS / 100;

/// The bytes that every value costs in the result budget, on top of its
/// text or blob.
const VALUE_OVERHEAD_BYTES: usize = 16;

/// The memory that SQLite holds in this process, in bytes.
fn sqlite_memory_used() -> i64 {
    // SAFETY: `sqlite3_memory_used` reads a global counter under its own
    // mutex. It takes no pointer and is safe to call from any thread.
    unsafe { rusqlite::ffi::sqlite3_memory_used() }
}

/// The bytes that `value` costs in the result budget. A blob counts as its
/// hex text.
const fn result_bytes(value: ValueRef<'_>) -> usize {
    let payload = match value {
        ValueRef::Text(bytes) => bytes.len(),
        ValueRef::Blob(bytes) => bytes.len().saturating_mul(2),
        ValueRef::Null | ValueRef::Integer(_) | ValueRef::Real(_) => 0,
    };
    payload.saturating_add(VALUE_OVERHEAD_BYTES)
}

/// The schema of the in-memory database.
const SCHEMA: &str = "
CREATE TABLE entity (
    workflow_name TEXT NOT NULL,
    workflow_id   TEXT NOT NULL,
    execution_id  TEXT NOT NULL,
    event_id      INTEGER NOT NULL,
    deleted       INTEGER NOT NULL,
    state         TEXT NOT NULL,
    stats         TEXT NOT NULL
);
";

/// SQL functions that a query may not call, even when the build has them.
const DENIED_FUNCTIONS: &[&str] = &["load_extension", "fts3_tokenizer"];

/// Open a fresh in-memory database that holds `record`, and lock it down.
///
/// The setup runs before the authorizer. After it, only reads and functions
/// pass.
fn open(record: &EntityRecord) -> Result<Connection, EntitySqlError> {
    let conn = Connection::open_in_memory().map_err(|e| sql_error(&e))?;
    conn.execute_batch("PRAGMA temp_store = MEMORY;")
        .map_err(|e| sql_error(&e))?;
    conn.execute_batch(SCHEMA).map_err(|e| sql_error(&e))?;
    let state_json = serde_json::to_string(&record.snapshot.state).map_err(HarvestError::from)?;
    let counters_json =
        serde_json::to_string(&record.snapshot.stats).map_err(HarvestError::from)?;
    conn.execute(
        "INSERT INTO entity VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            record.workflow_name,
            record.workflow_id,
            record.execution_id.as_uuid().to_string(),
            record.event_id,
            record.deleted,
            state_json,
            counters_json
        ],
    )
    .map_err(|e| sql_error(&e))?;
    conn.execute_batch("PRAGMA query_only = ON;")
        .map_err(|e| sql_error(&e))?;
    for (limit, value) in [
        (Limit::SQLITE_LIMIT_ATTACHED, 0),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, MAX_SQL_BYTES),
        (Limit::SQLITE_LIMIT_LENGTH, MAX_VALUE_BYTES),
    ] {
        conn.set_limit(limit, value).map_err(|e| sql_error(&e))?;
    }
    conn.authorizer(Some(authorize))
        .map_err(|e| sql_error(&e))?;
    Ok(conn)
}

/// Allow reads and functions. Deny every other action.
fn authorize(ctx: AuthContext<'_>) -> Authorization {
    match ctx.action {
        AuthAction::Select | AuthAction::Read { .. } | AuthAction::Recursive => {
            Authorization::Allow
        }
        AuthAction::Function { function_name }
            if !DENIED_FUNCTIONS
                .iter()
                .any(|denied| function_name.eq_ignore_ascii_case(denied)) =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }
}

fn sql_error(err: &rusqlite::Error) -> EntitySqlError {
    EntitySqlError::Sql(err.to_string())
}

/// Map a prepare error. A denied action or a second statement is a
/// refusal.
fn prepare_error(err: &rusqlite::Error) -> EntitySqlError {
    match err {
        rusqlite::Error::MultipleStatement => {
            EntitySqlError::Refused("the SQL holds more than one statement".to_string())
        }
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::AuthorizationForStatementDenied =>
        {
            EntitySqlError::Refused(err.to_string())
        }
        _ => sql_error(err),
    }
}

/// Map a step error. An interrupt from the progress handler is a stop. A
/// denied action at run time, such as a pragma function, is a refusal.
fn step_error(err: &rusqlite::Error) -> EntitySqlError {
    match err {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::OperationInterrupted =>
        {
            EntitySqlError::TooCostly
        }
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::AuthorizationForStatementDenied =>
        {
            EntitySqlError::Refused(err.to_string())
        }
        _ => sql_error(err),
    }
}

/// Bind one JSON parameter. An array or an object binds as JSON text.
fn to_sql(value: &Value) -> rusqlite::types::Value {
    use rusqlite::types::Value as Sql;
    match value {
        Value::Null => Sql::Null,
        Value::Bool(flag) => Sql::Integer(i64::from(*flag)),
        Value::Number(number) => number.as_i64().map_or_else(
            || number.as_f64().map_or(Sql::Null, Sql::Real),
            Sql::Integer,
        ),
        Value::String(text) => Sql::Text(text.clone()),
        Value::Array(_) | Value::Object(_) => Sql::Text(value.to_string()),
    }
}

/// Map one result value. A blob is a lowercase hex string. A real that
/// JSON cannot hold, such as infinity, is `null`.
fn to_json(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(number) => Value::from(number),
        ValueRef::Real(number) => {
            serde_json::Number::from_f64(number).map_or(Value::Null, Value::Number)
        }
        ValueRef::Text(bytes) => Value::String(String::from_utf8_lossy(bytes).into_owned()),
        ValueRef::Blob(bytes) => Value::String(bytes.iter().fold(
            String::with_capacity(bytes.len() * 2),
            |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::entity::EntityStats;

    fn record() -> EntityRecord {
        let stats = EntityStats {
            applied: 3,
            ..EntityStats::default()
        };
        EntityRecord::new(
            "cart",
            "cart-7",
            ExecutionId::from_uuid(uuid::Uuid::from_u128(7)),
            9,
            false,
            EntitySnapshot {
                state: json!({"items": [
                    {"sku": "a", "qty": 2},
                    {"sku": "b", "qty": 5}
                ], "owner": "ada"}),
                stats,
            },
        )
    }

    fn query(sql: &str, params: &[Value]) -> Result<EntitySqlRows, EntitySqlError> {
        run(&record(), sql, params, EntitySqlSource::File)
    }

    fn refused(sql: &str) {
        match query(sql, &[]) {
            Err(EntitySqlError::Refused(_)) => {}
            other => panic!("{sql}: expected a refusal, got {other:?}"),
        }
    }

    #[test]
    fn a_select_reads_the_row() {
        let rows = query(
            "SELECT workflow_name, workflow_id, execution_id, event_id, deleted, \
             json_extract(state, '$.owner') AS owner, \
             json_extract(stats, '$.applied') AS applied FROM entity",
            &[],
        )
        .expect("query");
        assert_eq!(
            rows.columns,
            [
                "workflow_name",
                "workflow_id",
                "execution_id",
                "event_id",
                "deleted",
                "owner",
                "applied"
            ]
        );
        assert_eq!(
            rows.rows,
            vec![vec![
                json!("cart"),
                json!("cart-7"),
                json!(uuid::Uuid::from_u128(7).to_string()),
                json!(9),
                json!(0),
                json!("ada"),
                json!(3)
            ]]
        );
        assert_eq!(rows.source, EntitySqlSource::File);
    }

    #[test]
    fn json_each_lists_and_aggregates_the_state() {
        let rows = query(
            "SELECT json_extract(item.value, '$.sku') AS sku, \
             json_extract(item.value, '$.qty') AS qty \
             FROM entity, json_each(entity.state, '$.items') AS item ORDER BY qty DESC",
            &[],
        )
        .expect("query");
        assert_eq!(
            rows.rows,
            vec![vec![json!("b"), json!(5)], vec![json!("a"), json!(2)]]
        );
        let total = query(
            "SELECT sum(json_extract(value, '$.qty')) FROM entity, \
             json_each(entity.state, '$.items')",
            &[],
        )
        .expect("aggregate");
        assert_eq!(total.rows, vec![vec![json!(7)]]);
    }

    #[test]
    fn parameters_bind_in_order() {
        let rows = query(
            "SELECT json_extract(value, '$.qty') FROM entity, \
             json_each(entity.state, '$.items') \
             WHERE json_extract(value, '$.sku') = ?1 AND ?2 AND ?3 IS NULL",
            &[json!("b"), json!(true), Value::Null],
        )
        .expect("query");
        assert_eq!(rows.rows, vec![vec![json!(5)]]);
        let echoed = query(
            "SELECT ?1, ?2, ?3, json_extract(?4, '$.k')",
            &[json!(1.5), json!(-4), json!("x"), json!({"k": [1]})],
        )
        .expect("echo");
        assert_eq!(
            echoed.rows,
            vec![vec![json!(1.5), json!(-4), json!("x"), json!("[1]")]]
        );
    }

    #[test]
    fn values_map_to_json() {
        let rows = query("SELECT NULL, 2, 0.25, 'text', x'01ff', 1e999", &[]).expect("query");
        assert_eq!(
            rows.rows,
            vec![vec![
                Value::Null,
                json!(2),
                json!(0.25),
                json!("text"),
                json!("01ff"),
                Value::Null
            ]]
        );
    }

    #[test]
    fn a_query_with_no_rows_keeps_its_columns() {
        let rows = query("SELECT event_id AS id FROM entity WHERE deleted", &[]).expect("q");
        assert_eq!(rows.columns, ["id"]);
        assert_eq!(rows.rows.len(), 0);
    }

    #[test]
    fn writes_are_refused() {
        for sql in [
            "INSERT INTO entity (workflow_name) VALUES ('x')",
            "UPDATE entity SET deleted = 1",
            "DELETE FROM entity",
            "REPLACE INTO entity (workflow_name) VALUES ('x')",
            "DROP TABLE entity",
            "CREATE TABLE t (a)",
            "CREATE TEMP TABLE t (a)",
            "CREATE VIEW v AS SELECT 1",
            "CREATE INDEX i ON entity (event_id)",
            "ALTER TABLE entity ADD COLUMN c",
            "WITH x AS (SELECT 1) DELETE FROM entity",
        ] {
            refused(sql);
        }
        let rows = query("SELECT count(*) FROM entity", &[]).expect("count");
        assert_eq!(rows.rows, vec![vec![json!(1)]], "the row is intact");
    }

    #[test]
    fn no_statement_reaches_a_file() {
        let dir = tempfile::tempdir().expect("dir");
        let target = dir.path().join("leak.db");
        let path = target.display().to_string().replace('\'', "''");
        for sql in [
            format!("ATTACH DATABASE '{path}' AS leak"),
            format!("VACUUM INTO '{path}'"),
            "VACUUM".to_string(),
            "DETACH DATABASE main".to_string(),
        ] {
            refused(&sql);
        }
        assert!(!target.exists(), "no statement made a file");
    }

    #[test]
    fn pragmas_and_transactions_are_refused() {
        for sql in [
            "PRAGMA query_only = OFF",
            "PRAGMA temp_store = FILE",
            "PRAGMA table_info(entity)",
            "BEGIN",
            "COMMIT",
            "SAVEPOINT s",
            "ANALYZE",
            "REINDEX",
        ] {
            refused(sql);
        }
    }

    #[test]
    fn two_statements_are_refused() {
        refused("SELECT 1; SELECT 2");
        refused("SELECT 1; DELETE FROM entity");
    }

    #[test]
    fn an_empty_statement_is_refused() {
        refused("");
        refused("  -- only a comment");
    }

    #[test]
    fn a_runaway_query_stops() {
        let started = std::time::Instant::now();
        match query(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) \
             SELECT count(*) FROM n",
            &[],
        ) {
            Err(EntitySqlError::TooCostly) => {}
            other => panic!("expected a stop, got {other:?}"),
        }
        assert!(started.elapsed() < std::time::Duration::from_secs(60));
    }

    #[test]
    fn a_large_result_stops_at_the_row_cap() {
        let sql = format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
             WHERE i < {}) SELECT i FROM n",
            MAX_ROWS + 1
        );
        match query(&sql, &[]) {
            Err(EntitySqlError::TooManyRows { limit }) => assert_eq!(limit, MAX_ROWS),
            other => panic!("expected the row cap, got {other:?}"),
        }
        let at_cap = format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
             WHERE i < {MAX_ROWS}) SELECT i FROM n"
        );
        assert_eq!(
            query(&at_cap, &[]).expect("at the cap").rows.len(),
            MAX_ROWS
        );
    }

    #[test]
    fn a_huge_value_is_refused() {
        match query("SELECT zeroblob(?1)", &[json!(MAX_VALUE_BYTES + 1)]) {
            Err(EntitySqlError::Sql(_)) => {}
            other => panic!("expected a size error, got {other:?}"),
        }
    }

    #[test]
    fn a_long_statement_is_refused() {
        let sql = format!(
            "SELECT '{}'",
            "a".repeat(usize::try_from(MAX_SQL_BYTES).unwrap_or(0))
        );
        assert!(matches!(
            query(&sql, &[]),
            Err(EntitySqlError::Sql(_) | EntitySqlError::Refused(_))
        ));
    }

    #[test]
    fn a_sql_error_is_reported() {
        match query("SELECT nope FROM entity", &[]) {
            Err(EntitySqlError::Sql(text)) => assert!(text.contains("nope"), "{text}"),
            other => panic!("expected a SQL error, got {other:?}"),
        }
    }

    /// Issue #2260 review: a result past the byte cap stops before it turns
    /// into JSON.
    #[test]
    fn a_large_result_stops_at_the_byte_cap() {
        let sql = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
                   WHERE i < 20) SELECT printf('%.*c', 1000000, 'x') FROM n";
        match query(sql, &[]) {
            Err(EntitySqlError::TooLarge { limit }) => assert_eq!(limit, MAX_RESULT_BYTES),
            other => panic!("expected the byte cap, got {other:?}"),
        }
        let blobs = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
                     WHERE i < 10) SELECT zeroblob(1000000) FROM n";
        assert!(matches!(
            query(blobs, &[]),
            Err(EntitySqlError::TooLarge { .. })
        ));
    }

    /// Issue #2260 review: a query that grows SQLite memory past its budget
    /// stops.
    #[test]
    fn a_memory_hungry_query_stops() {
        let sql = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
                   WHERE i < 2000) \
                   SELECT count(DISTINCT printf('%.*c', 1000000, 'x') || i) FROM n";
        match query(sql, &[]) {
            Err(EntitySqlError::TooCostly) => {}
            other => panic!("expected a stop, got {other:?}"),
        }
    }

    /// Issue #2260 review: a slow query with a small step count stops at its
    /// deadline.
    #[test]
    fn a_slow_query_stops_at_its_deadline() {
        let started = Instant::now();
        let sql = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n \
                   WHERE i < 1000000) \
                   SELECT count(length(replace(printf('%.*c', 1000000 - i % 7, 'x'), 'x', 'y'))) \
                   FROM n";
        match query(sql, &[]) {
            Err(EntitySqlError::TooCostly) => {}
            other => panic!("expected a stop, got {other:?}"),
        }
        assert!(started.elapsed() < MAX_QUERY_TIME * 4, "the deadline holds");
    }

    /// Issue #2260 review: a cancelled query stops.
    #[test]
    fn a_cancelled_query_stops() {
        let cancel = Arc::new(AtomicBool::new(true));
        let sql = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) \
                   SELECT count(*) FROM n";
        match run_cancellable(&record(), sql, &[], EntitySqlSource::File, &cancel) {
            Err(EntitySqlError::TooCostly) => {}
            other => panic!("expected a stop, got {other:?}"),
        }
    }

    /// Issue #2260 review: a pragma function is a refusal at run time.
    #[test]
    fn a_pragma_function_is_refused() {
        refused("SELECT * FROM pragma_table_info('entity')");
    }

    #[test]
    fn the_step_budget_matches_the_interval() {
        let interval = u64::try_from(STEP_INTERVAL).expect("positive");
        assert_eq!(MAX_TICKS * interval, MAX_STEPS);
    }

    #[test]
    fn a_deleted_record_reads_as_one() {
        let mut deleted = record();
        deleted.deleted = true;
        let rows = run(
            &deleted,
            "SELECT deleted FROM entity",
            &[],
            EntitySqlSource::History,
        )
        .expect("query");
        assert_eq!(rows.rows, vec![vec![json!(1)]]);
        assert_eq!(rows.source, EntitySqlSource::History);
    }
}
