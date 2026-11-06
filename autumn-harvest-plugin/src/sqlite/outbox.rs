//! The transactional outbox on a SQLite app (issue #2470).
//!
//! App code writes a workflow start with [`enqueue_workflow_start_outbox`],
//! on the connection of its own business transaction. The start commits or
//! rolls back with the business rows. The relay then hands each committed
//! row to the driver task. `docs/design/DESIGN-2470.md` records the design.
//!
//! The table lives in the app's primary database. In a fleet app, that is
//! the control database. The startup hook creates it, as
//! `autumn_web::outbox` creates its own table. A tenant database has no
//! table. A write there fails, so a start never waits for a relay that does
//! not come.
//!
//! A committed start runs exactly once:
//!
//! - A crash before the relay leaves the row undelivered. The next boot
//!   drains it.
//! - The driver starts a row with `StartOnce`. It returns any run of the
//!   key, in any state, before it starts one. A crash after the start and
//!   before the mark therefore costs no second run.
//! - One drain runs at a time in a process. One process holds the writer
//!   lock on the Harvest file, so the table needs no claim column. Two apps
//!   with two Harvest files must not share one app database.
//!
//! A row whose key has a run from another path, such as an API start, counts
//! as delivered to that run. A sealed run counts too. This is stricter than
//! the Postgres reuse policy, which starts a fresh run over a sealed one.

use std::sync::Arc;
use std::time::Duration;

use autumn_harvest_sqlite::StartOptions;
use autumn_web::db::RuntimeConnection;
use autumn_web::error::AutumnError;
use diesel::sql_types::{BigInt, Nullable, Text};
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::{RunQueryDsl as _, SimpleAsyncConnection as _};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::handle::{HarvestSqlite, HarvestSqliteError};
use crate::config::HarvestOutboxConfig;
use crate::outbox::{WorkflowStartRequest, retry_delay_for_attempts};

/// DDL of the outbox table on a SQLite app database.
///
/// The startup hook runs it. Times are epoch milliseconds. The columns
/// follow the Postgres table, less the claim columns.
pub const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS harvest_workflow_outbox (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_name          TEXT    NOT NULL,
    workflow_id            TEXT    NOT NULL,
    queue_name             TEXT    NOT NULL,
    input                  TEXT    NOT NULL,
    memo                   TEXT,
    search_attrs           TEXT,
    delivery_attempts      INTEGER NOT NULL DEFAULT 0,
    last_error             TEXT,
    delivered_execution_id TEXT,
    delivered_at           INTEGER,
    next_attempt_at        INTEGER NOT NULL,
    created_at             INTEGER NOT NULL,
    UNIQUE (workflow_name, workflow_id)
);
CREATE INDEX IF NOT EXISTS idx_harvest_workflow_outbox_pending
    ON harvest_workflow_outbox (id)
    WHERE delivered_at IS NULL;";

/// The insert of the writer. A duplicate key is a no-op.
const INSERT_SQL: &str = "INSERT INTO harvest_workflow_outbox \
     (workflow_name, workflow_id, queue_name, input, memo, search_attrs, next_attempt_at, \
     created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
     ON CONFLICT (workflow_name, workflow_id) DO NOTHING";

/// The due rows, oldest first. The partial index on `id` answers it, so a
/// poll reads the undelivered rows only.
const DUE_SQL: &str = "SELECT id, workflow_name, workflow_id, input, memo, search_attrs, \
     delivery_attempts FROM harvest_workflow_outbox \
     WHERE delivered_at IS NULL AND next_attempt_at <= ? ORDER BY id LIMIT ?";

/// The mark of a delivered row. It counts once, so a repeat mark is a no-op.
const DELIVERED_SQL: &str = "UPDATE harvest_workflow_outbox \
     SET delivery_attempts = delivery_attempts + 1, last_error = NULL, \
     delivered_execution_id = ?, delivered_at = ? WHERE id = ? AND delivered_at IS NULL";

/// The mark of a failed start.
const FAILED_SQL: &str = "UPDATE harvest_workflow_outbox \
     SET delivery_attempts = delivery_attempts + 1, last_error = ?, next_attempt_at = ? \
     WHERE id = ? AND delivered_at IS NULL";

/// The one queue of the SQLite backend.
const QUEUE: &str = "default";

/// The refusal of a write to a database with no outbox table.
const NO_TABLE: &str = "this database has no harvest_workflow_outbox table. Write the start on \
     the app's primary database. HarvestPlugin creates the table there at boot, in \
     harvest.mode = \"embedded\" (autumn-harvest#2470)";

/// The refusal of a flush on the driver task.
const ON_DRIVER: &str = "the outbox cannot flush inside an activity or a workflow: the driver \
     task that runs it would wait for itself. The relay delivers the row (autumn-harvest#2470)";

/// Persist a workflow start in the outbox of a SQLite app database.
///
/// Call it on the connection of your business transaction. The start then
/// commits or rolls back with your rows. A duplicate
/// `(workflow_name, workflow_id)` is a no-op, so a retry is safe.
///
/// ```rust,ignore
/// conn.transaction(async |conn| {
///     insert_order(conn, &order).await?;
///     enqueue_workflow_start_outbox(conn, &start).await
/// })
/// .await?;
/// ```
///
/// # Errors
///
/// Returns a Diesel error when the insert fails. A request that this backend
/// cannot honour is a `QueryBuilderError`, and the query never runs:
///
/// - an empty or blank `workflow_id`;
/// - a `queue_name` other than `"default"`;
/// - a `search_attrs` that is not an object of JSON primitives.
///
/// A database with no outbox table, such as a fleet tenant database, is a
/// `QueryBuilderError` that names autumn-harvest#2470.
///
/// The relay retries a row that fails to start, with a backoff, as on
/// Postgres. An unknown workflow or an input over the cap fails that way.
pub async fn enqueue_workflow_start_outbox(
    conn: &mut RuntimeConnection,
    request: &WorkflowStartRequest,
) -> Result<(), diesel::result::Error> {
    check_request(request)
        .map_err(|message| diesel::result::Error::QueryBuilderError(message.into()))?;
    let now = now_ms();
    diesel::sql_query(INSERT_SQL)
        .bind::<Text, _>(request.workflow_name.clone())
        .bind::<Text, _>(request.workflow_id.clone())
        .bind::<Text, _>(request.queue_name.clone())
        .bind::<Text, _>(request.input.to_string())
        .bind::<Nullable<Text>, _>(request.memo.as_ref().map(Value::to_string))
        .bind::<Nullable<Text>, _>(search_attrs_text(request.search_attrs.as_ref()))
        .bind::<BigInt, _>(now)
        .bind::<BigInt, _>(now)
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(name_missing_table)
}

/// The refusal of a request that this backend cannot honour, or `Ok`.
fn check_request(request: &WorkflowStartRequest) -> Result<(), String> {
    // The driver refuses a blank id. Such a row could never start.
    if request.workflow_id.trim().is_empty() {
        return Err("workflow_id must not be empty".to_owned());
    }
    if request.queue_name != QUEUE {
        return Err(format!(
            "queue_name {:?} is not available on the SQLite backend: it has one queue, \
             {QUEUE:?} (autumn-harvest#2470)",
            request.queue_name
        ));
    }
    match &request.search_attrs {
        None | Some(Value::Null) => Ok(()),
        Some(Value::Object(attrs)) => attrs
            .iter()
            .find(|(_, value)| value.is_object() || value.is_array())
            .map_or(Ok(()), |(key, _)| {
                Err(format!(
                    "search attribute {key:?} must be a JSON string, number, boolean or null"
                ))
            }),
        Some(_) => Err("search_attrs must be a JSON object".to_owned()),
    }
}

/// The stored text of `search_attrs`. A JSON `null` stores no attribute.
fn search_attrs_text(attrs: Option<&Value>) -> Option<String> {
    attrs.filter(|attrs| !attrs.is_null()).map(Value::to_string)
}

/// A missing table as the refusal that names autumn-harvest#2470.
fn name_missing_table(error: diesel::result::Error) -> diesel::result::Error {
    match &error {
        diesel::result::Error::DatabaseError(_, info)
            if info
                .message()
                .contains("no such table: harvest_workflow_outbox") =>
        {
            diesel::result::Error::QueryBuilderError(NO_TABLE.into())
        }
        _ => error,
    }
}

/// The wall clock in epoch milliseconds.
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Create the outbox table on the app's primary database, and check its
/// shape.
///
/// A table that exists keeps its shape, so the check compiles each statement
/// of the writer and the relay with `EXPLAIN`. A missing column or unique
/// key then fails the boot, not each write.
///
/// # Errors
///
/// Returns a message when no connection is available, the DDL fails, or the
/// table has another shape.
pub async fn ensure_schema(pool: &Pool<RuntimeConnection>) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| format!("outbox schema: no connection: {error}"))?;
    conn.batch_execute(SCHEMA_SQL)
        .await
        .map_err(|error| format!("outbox schema: {error}"))?;
    let probe = [INSERT_SQL, DUE_SQL, DELIVERED_SQL, FAILED_SQL]
        .map(|sql| format!("EXPLAIN {sql};"))
        .join("\n");
    conn.batch_execute(&probe).await.map_err(|error| {
        format!(
            "harvest_workflow_outbox does not have the shape that HarvestPlugin needs: {error}. \
             Drop it, or make it match OUTBOX_SCHEMA_SQL (autumn-harvest#2470)"
        )
    })
}

/// The undelivered rows of an outbox that no relay drains (issue #2470).
///
/// Split and external mode run no SQLite relay. A row left from embedded
/// mode then waits. `None` when the table does not exist.
pub async fn undelivered_rows(pool: &Pool<RuntimeConnection>) -> Option<i64> {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = BigInt)]
        n: i64,
    }
    let mut conn = pool.get().await.ok()?;
    diesel::sql_query(
        "SELECT COUNT(*) AS n FROM harvest_workflow_outbox WHERE delivered_at IS NULL",
    )
    .get_result::<Count>(&mut *conn)
    .await
    .ok()
    .map(|count| count.n)
}

/// Warn when the app database holds starts that no relay drains.
///
/// The Postgres path of a SQLite app calls it at boot, in split and external
/// mode. A silent wait would let an app believe that the starts run.
pub async fn warn_if_stranded(state: &autumn_web::AppState) {
    let Some(pool) = state.pool() else {
        return;
    };
    if let Some(rows) = undelivered_rows(pool).await.filter(|rows| *rows > 0) {
        tracing::warn!(
            rows,
            "harvest_workflow_outbox holds undelivered starts, and no relay runs outside \
             harvest.mode = \"embedded\". Boot in embedded mode to run them (autumn-harvest#2470)"
        );
    }
}

/// One due row.
#[derive(Debug, diesel::QueryableByName)]
struct DueRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
    #[diesel(sql_type = Text)]
    workflow_name: String,
    #[diesel(sql_type = Text)]
    workflow_id: String,
    #[diesel(sql_type = Text)]
    input: String,
    #[diesel(sql_type = Nullable<Text>)]
    memo: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    search_attrs: Option<String>,
    #[diesel(sql_type = BigInt)]
    delivery_attempts: i64,
}

/// Why a drain stopped.
#[derive(Debug)]
enum DrainError {
    /// The driver has stopped. No row can start until the next boot.
    Stopped,
    /// The app database refused a read or a mark.
    Database(String),
}

impl From<DrainError> for AutumnError {
    fn from(error: DrainError) -> Self {
        match error {
            DrainError::Stopped => {
                Self::service_unavailable_msg(HarvestSqliteError::Stopped.to_string())
            }
            DrainError::Database(message) => Self::service_unavailable_msg(message),
        }
    }
}

/// The counts of one batch.
#[derive(Debug, Clone, Copy, Default)]
struct Batch {
    /// The due rows that the batch read.
    read: usize,
    /// The rows that the batch marked delivered.
    delivered: usize,
}

/// The relay of a SQLite app: an `AppState` extension (issue #2470).
#[derive(Clone)]
pub struct SqliteOutbox {
    inner: Arc<Inner>,
}

struct Inner {
    /// The app's primary pool. The outbox table lives there.
    pool: Pool<RuntimeConnection>,
    /// The control Harvest.
    harvest: HarvestSqlite,
    config: HarvestOutboxConfig,
    /// Serializes drains, so two drains never dispatch one row.
    drain: tokio::sync::Mutex<()>,
}

impl SqliteOutbox {
    /// A relay of the outbox in `pool` to the control Harvest `harvest`.
    pub fn new(
        pool: Pool<RuntimeConnection>,
        harvest: HarvestSqlite,
        config: HarvestOutboxConfig,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                pool,
                harvest,
                config,
                drain: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// Drain every due row. Return the number delivered.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver has stopped or the app database
    /// fails. A call on the driver task, from an activity or a workflow, is
    /// refused: the start would wait for that task.
    pub async fn flush(&self) -> Result<usize, AutumnError> {
        refuse_on_driver()?;
        Ok(self.flush_inner().await?)
    }

    /// Drain one batch of at most `limit` due rows. Return the number
    /// delivered.
    ///
    /// # Errors
    ///
    /// See [`flush`](Self::flush).
    pub async fn drain_once(&self, limit: i64) -> Result<usize, AutumnError> {
        refuse_on_driver()?;
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let _drain = self.inner.drain.lock().await;
        Ok(self.drain_batch(limit.max(1)).await?.delivered)
    }

    async fn flush_inner(&self) -> Result<usize, DrainError> {
        if !self.inner.config.enabled {
            return Ok(0);
        }
        let limit = self.inner.config.batch_size.max(1);
        let full = usize::try_from(limit).unwrap_or(usize::MAX);
        let _drain = self.inner.drain.lock().await;
        let mut delivered = 0;
        loop {
            let batch = self.drain_batch(limit).await?;
            delivered += batch.delivered;
            if batch.read < full {
                return Ok(delivered);
            }
        }
    }

    /// Read one batch of due rows, start each one, and mark it.
    ///
    /// The batch holds no app connection while a start runs. A start waits
    /// for the driver, and the driver can be in a long activity.
    async fn drain_batch(&self, limit: i64) -> Result<Batch, DrainError> {
        let rows = self.due_rows(limit).await?;
        let mut batch = Batch {
            read: rows.len(),
            delivered: 0,
        };
        for row in rows {
            match self.dispatch(&row).await {
                Ok(exec) => batch.delivered += self.mark_delivered(row.id, &exec).await?,
                Err(HarvestSqliteError::Stopped) => return Err(DrainError::Stopped),
                Err(error) => self.mark_failed(&row, &error.to_string()).await?,
            }
        }
        Ok(batch)
    }

    async fn connection(
        &self,
    ) -> Result<diesel_async::pooled_connection::deadpool::Object<RuntimeConnection>, DrainError>
    {
        self.inner
            .pool
            .get()
            .await
            .map_err(|error| DrainError::Database(format!("outbox: no connection: {error}")))
    }

    async fn due_rows(&self, limit: i64) -> Result<Vec<DueRow>, DrainError> {
        let mut conn = self.connection().await?;
        diesel::sql_query(DUE_SQL)
            .bind::<BigInt, _>(now_ms())
            .bind::<BigInt, _>(limit)
            .load(&mut *conn)
            .await
            .map_err(|error| database(&error))
    }

    /// Start the run of `row` once.
    async fn dispatch(&self, row: &DueRow) -> Result<String, HarvestSqliteError> {
        let options = options_of(row).map_err(HarvestSqliteError::Read)?;
        let input = serde_json::from_str(&row.input)
            .map_err(|error| HarvestSqliteError::Read(format!("input: {error}")))?;
        self.inner
            .harvest
            .start_once(&row.workflow_name, input, options)
            .await
            .map(|outcome| outcome.exec_id.to_string())
    }

    /// Mark `id` delivered to `exec`. Return 1 when this mark delivered it,
    /// and 0 when an earlier mark did.
    async fn mark_delivered(&self, id: i64, exec: &str) -> Result<usize, DrainError> {
        let mut conn = self.connection().await?;
        diesel::sql_query(DELIVERED_SQL)
            .bind::<Text, _>(exec.to_owned())
            .bind::<BigInt, _>(now_ms())
            .bind::<BigInt, _>(id)
            .execute(&mut *conn)
            .await
            .map_err(|error| database(&error))
    }

    /// Keep `error` on the row, and back off its next attempt.
    async fn mark_failed(&self, row: &DueRow, error: &str) -> Result<(), DrainError> {
        let delay = retry_delay_for_attempts(&self.inner.config, row.delivery_attempts);
        let next = now_ms().saturating_add(i64::try_from(delay).unwrap_or(i64::MAX));
        let mut conn = self.connection().await?;
        diesel::sql_query(FAILED_SQL)
            .bind::<Text, _>(error.to_owned())
            .bind::<BigInt, _>(next)
            .bind::<BigInt, _>(row.id)
            .execute(&mut *conn)
            .await
            .map(|_| ())
            .map_err(|error| database(&error))
    }

    /// Run the relay until `stop` fires or the driver stops.
    ///
    /// The caller spawns it only when `harvest.outbox.enabled` is true. A
    /// drain that `stop` interrupts loses nothing. A row that started and has
    /// no mark goes to the driver again, and `StartOnce` returns the run that
    /// exists.
    pub fn spawn(self, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(Duration::from_millis(self.inner.config.poll_interval_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut last_error: Option<String> = None;
            loop {
                tokio::select! {
                    () = stop.cancelled() => return,
                    _ = interval.tick() => {}
                }
                let outcome = tokio::select! {
                    () = stop.cancelled() => return,
                    outcome = self.flush_inner() => outcome,
                };
                match outcome {
                    Ok(delivered) => {
                        last_error = None;
                        if delivered > 0 {
                            tracing::debug!(
                                delivered,
                                "the Harvest SQLite outbox relay drained rows"
                            );
                        }
                    }
                    Err(DrainError::Stopped) => {
                        // A stop that the shutdown hook did not ask for leaves
                        // the rows waiting until the next boot.
                        if !stop.is_cancelled() {
                            tracing::warn!(
                                "the Harvest SQLite driver stopped, so the outbox relay stops. \
                                 Undelivered starts wait for the next boot"
                            );
                        }
                        return;
                    }
                    Err(DrainError::Database(error)) => {
                        // A broken table fails every pass. Log it once per change.
                        if last_error.as_deref() != Some(error.as_str()) {
                            tracing::warn!(%error, "the Harvest SQLite outbox relay drain failed");
                            last_error = Some(error);
                        }
                    }
                }
            }
        })
    }
}

/// Refuse a flush on the driver task. See [`ON_DRIVER`].
fn refuse_on_driver() -> Result<(), AutumnError> {
    if super::driver::on_driver() {
        return Err(AutumnError::service_unavailable_msg(ON_DRIVER));
    }
    Ok(())
}

fn database(error: &diesel::result::Error) -> DrainError {
    DrainError::Database(format!("outbox: {error}"))
}

/// The start options of `row`.
fn options_of(row: &DueRow) -> Result<StartOptions, String> {
    let mut options = StartOptions::new().workflow_id(row.workflow_id.clone());
    if let Some(memo) = &row.memo {
        options.memo = Some(serde_json::from_str(memo).map_err(|error| format!("memo: {error}"))?);
    }
    if let Some(attrs) = &row.search_attrs {
        match serde_json::from_str(attrs).map_err(|error| format!("search_attrs: {error}"))? {
            Value::Object(attrs) => options.search_attributes = attrs,
            _ => return Err("search_attrs must be a JSON object".to_owned()),
        }
    }
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_types::BigInt;
    use diesel_async::AsyncConnection as _;
    use serde_json::json;

    fn request(workflow_id: &str) -> WorkflowStartRequest {
        WorkflowStartRequest {
            workflow_name: "ship".to_owned(),
            workflow_id: workflow_id.to_owned(),
            queue_name: QUEUE.to_owned(),
            input: json!({ "order": workflow_id }),
            memo: None,
            search_attrs: None,
        }
    }

    /// A connection to a fresh file. `schema` false leaves out the table,
    /// as in a fleet tenant database.
    async fn connection(dir: &std::path::Path, schema: bool) -> RuntimeConnection {
        let url = dir.join("app.db").display().to_string();
        let mut conn = RuntimeConnection::establish(&url).await.expect("open");
        if schema {
            conn.batch_execute(SCHEMA_SQL).await.expect("schema");
        }
        conn
    }

    async fn rows(conn: &mut RuntimeConnection) -> i64 {
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        diesel::sql_query("SELECT COUNT(*) AS n FROM harvest_workflow_outbox")
            .get_result::<Count>(conn)
            .await
            .expect("count")
            .n
    }

    fn refusal(error: &diesel::result::Error) -> String {
        match error {
            diesel::result::Error::QueryBuilderError(message) => message.to_string(),
            other => panic!("not a refusal: {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_schema_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = connection(dir.path(), true).await;
        conn.batch_execute(SCHEMA_SQL).await.expect("a second run");
        assert_eq!(rows(&mut conn).await, 0);
    }

    #[tokio::test]
    async fn a_duplicate_key_is_a_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = connection(dir.path(), true).await;
        enqueue_workflow_start_outbox(&mut conn, &request("o-1"))
            .await
            .unwrap();
        enqueue_workflow_start_outbox(&mut conn, &request("o-1"))
            .await
            .unwrap();
        assert_eq!(rows(&mut conn).await, 1);
    }

    /// The writer opens no transaction of its own. A rollback of the
    /// caller's transaction removes the row.
    #[tokio::test]
    async fn the_write_rolls_back_with_the_caller() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = connection(dir.path(), true).await;
        let outcome = conn
            .transaction::<(), diesel::result::Error, _>(async |conn| {
                enqueue_workflow_start_outbox(conn, &request("o-rb")).await?;
                Err(diesel::result::Error::RollbackTransaction)
            })
            .await;
        assert!(outcome.is_err());
        assert_eq!(rows(&mut conn).await, 0);
    }

    #[tokio::test]
    async fn a_request_that_sqlite_cannot_honour_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = connection(dir.path(), true).await;
        let mut empty = request("");
        empty.workflow_id.clear();
        let mut queue = request("o-q");
        queue.queue_name = "fast".to_owned();
        let mut not_object = request("o-a");
        not_object.search_attrs = Some(json!(["web"]));
        let mut nested = request("o-n");
        nested.search_attrs = Some(json!({ "tags": ["a"] }));
        // The driver refuses a blank id, so such a row could never start.
        let blank = request("  ");
        for (request, want) in [
            (empty, "workflow_id must not be empty"),
            (blank, "workflow_id must not be empty"),
            (queue, "queue_name \"fast\""),
            (not_object, "search_attrs must be a JSON object"),
            (nested, "search attribute \"tags\""),
        ] {
            let error = enqueue_workflow_start_outbox(&mut conn, &request)
                .await
                .unwrap_err();
            let message = refusal(&error);
            assert!(message.contains(want), "{message}");
        }
        assert_eq!(rows(&mut conn).await, 0, "a refused write stores nothing");

        // A JSON null stores no attribute, and passes.
        let mut null = request("o-null");
        null.search_attrs = Some(Value::Null);
        enqueue_workflow_start_outbox(&mut conn, &null)
            .await
            .unwrap();
        assert_eq!(rows(&mut conn).await, 1);
    }

    /// A tenant database has no table. The refusal names the issue, so the
    /// app does not wait for a relay that never comes.
    #[tokio::test]
    async fn a_database_with_no_table_is_refused_with_the_issue() {
        let dir = tempfile::tempdir().unwrap();
        let mut conn = connection(dir.path(), false).await;
        let error = enqueue_workflow_start_outbox(&mut conn, &request("o-t"))
            .await
            .unwrap_err();
        let message = refusal(&error);
        assert!(message.contains("autumn-harvest#2470"), "{message}");
        assert!(message.contains("primary database"), "{message}");
    }

    /// A table with another shape fails the boot, not each write.
    #[tokio::test]
    async fn a_table_of_another_shape_fails_the_schema_check() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("app.db").display());
        let bad_pool = pool(&url);
        {
            let mut conn = bad_pool.get().await.unwrap();
            // No unique key, so the writer's `ON CONFLICT` cannot compile.
            conn.batch_execute(
                "CREATE TABLE harvest_workflow_outbox (id INTEGER PRIMARY KEY, \
                 workflow_name TEXT, workflow_id TEXT, queue_name TEXT, input TEXT, memo TEXT, \
                 search_attrs TEXT, delivery_attempts INTEGER, last_error TEXT, \
                 delivered_execution_id TEXT, delivered_at INTEGER, next_attempt_at INTEGER, \
                 created_at INTEGER)",
            )
            .await
            .unwrap();
        }
        let error = ensure_schema(&bad_pool).await.unwrap_err();
        assert!(error.contains("autumn-harvest#2470"), "{error}");

        let fresh = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", fresh.path().join("app.db").display());
        let fresh_pool = pool(&url);
        ensure_schema(&fresh_pool).await.unwrap();
        ensure_schema(&fresh_pool)
            .await
            .expect("a second boot passes");
    }

    /// Split mode warns about rows that no relay drains.
    #[tokio::test]
    async fn undelivered_rows_counts_what_no_relay_drains() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("app.db").display());
        let pool = pool(&url);
        assert_eq!(undelivered_rows(&pool).await, None, "no table");
        ensure_schema(&pool).await.unwrap();
        assert_eq!(undelivered_rows(&pool).await, Some(0));
        let mut conn = pool.get().await.unwrap();
        enqueue_workflow_start_outbox(&mut conn, &request("o-1"))
            .await
            .unwrap();
        drop(conn);
        assert_eq!(undelivered_rows(&pool).await, Some(1));
    }

    #[test]
    fn a_stored_row_gives_the_start_options() {
        let row = DueRow {
            id: 1,
            workflow_name: "ship".to_owned(),
            workflow_id: "o-1".to_owned(),
            input: "\"o-1\"".to_owned(),
            memo: Some("{\"a\":1}".to_owned()),
            search_attrs: Some("{\"channel\":\"web\"}".to_owned()),
            delivery_attempts: 0,
        };
        let options = options_of(&row).unwrap();
        assert_eq!(options.workflow_id.as_deref(), Some("o-1"));
        assert_eq!(options.memo, Some(json!({ "a": 1 })));
        assert_eq!(
            options.search_attributes.get("channel"),
            Some(&json!("web"))
        );

        let bare = DueRow {
            memo: None,
            search_attrs: None,
            ..row
        };
        let options = options_of(&bare).unwrap();
        assert_eq!(options.memo, None);
        assert!(options.search_attributes.is_empty());

        let broken = DueRow {
            search_attrs: Some("[1]".to_owned()),
            ..bare
        };
        assert!(options_of(&broken).is_err());
    }

    // ── The relay, driven directly ───────────────────────────────────────
    //
    // These tests build the relay with no background task, so each count
    // is exact. `tests/sqlite_plugin/outbox.rs` runs it in a booted app.

    use super::super::driver::{Driver, Registry, Retention};
    use super::super::handle::Catalog;
    use autumn_harvest::prelude::*;
    use autumn_harvest_sqlite::ExecutionFilter;

    /// Completes at once.
    #[workflow]
    async fn ship(_ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
        Ok(input)
    }

    /// Waits for a signal that never comes, so a terminate seals it.
    #[workflow]
    async fn hold(ctx: &WorkflowContext, input: Value) -> Result<Value, String> {
        ctx.wait_for_signal("never")
            .await
            .map_err(|error| error.to_string())?;
        Ok(input)
    }

    fn pool(url: &str) -> Pool<RuntimeConnection> {
        let mut config = autumn_web::config::DatabaseConfig::default();
        config.url = Some(url.to_owned());
        autumn_web::db::create_pool(&config)
            .expect("a pool")
            .expect("a SQLite URL")
    }

    /// A relay on a fresh app database and Harvest file.
    struct Relay {
        _dir: tempfile::TempDir,
        outbox: SqliteOutbox,
        harvest: HarvestSqlite,
        driver: Driver,
        pool: Pool<RuntimeConnection>,
    }

    async fn relay(enabled: bool) -> Relay {
        let dir = tempfile::tempdir().unwrap();
        let registry = Registry {
            workflows: vec![ship_info(), hold_info()],
            ..Registry::empty()
        };
        let path = dir.path().join("app.harvest.db");
        let runtime = Driver::open(&path, &registry).unwrap();
        let retention = Arc::new(Retention::new(&registry.retention));
        let driver = Driver::start(runtime, Duration::from_millis(20), Arc::clone(&retention));
        let reader = autumn_harvest_sqlite::SqliteReader::open(&path).unwrap();
        let harvest = HarvestSqlite::new(
            driver.sender.clone(),
            reader,
            path,
            Arc::new(Catalog::of(&registry)),
            retention,
        );
        let pool = pool(&format!("sqlite://{}", dir.path().join("app.db").display()));
        ensure_schema(&pool).await.unwrap();
        let config = HarvestOutboxConfig {
            enabled,
            poll_interval_ms: 10,
            ..HarvestOutboxConfig::default()
        };
        let outbox = SqliteOutbox::new(pool.clone(), harvest.clone(), config);
        Relay {
            _dir: dir,
            outbox,
            harvest,
            driver,
            pool,
        }
    }

    impl Relay {
        async fn enqueue(&self, workflow_id: &str) {
            self.enqueue_as("ship", workflow_id).await;
        }

        async fn enqueue_as(&self, workflow_name: &str, workflow_id: &str) {
            let mut conn = self.pool.get().await.unwrap();
            let mut start = request(workflow_id);
            start.workflow_name = workflow_name.to_owned();
            start.input = json!(workflow_id);
            enqueue_workflow_start_outbox(&mut conn, &start)
                .await
                .unwrap();
        }

        async fn runs(&self, workflow_id: &str) -> Vec<String> {
            self.runs_of("ship", workflow_id).await
        }

        async fn runs_of(&self, workflow_name: &str, workflow_id: &str) -> Vec<String> {
            let filter = ExecutionFilter::new()
                .workflow_name(workflow_name.to_owned())
                .workflow_id(workflow_id.to_owned());
            self.harvest
                .list(filter)
                .await
                .unwrap()
                .executions
                .iter()
                .filter_map(|run| run.execution_id().map(|exec| exec.to_string()))
                .collect()
        }

        /// `(delivery_attempts, delivered_execution_id)` of the row.
        async fn row(&self, workflow_id: &str) -> (i64, Option<String>) {
            #[derive(diesel::QueryableByName)]
            struct Row {
                #[diesel(sql_type = BigInt)]
                delivery_attempts: i64,
                #[diesel(sql_type = Nullable<Text>)]
                delivered_execution_id: Option<String>,
            }
            let mut conn = self.pool.get().await.unwrap();
            let row: Row = diesel::sql_query(
                "SELECT delivery_attempts, delivered_execution_id \
                 FROM harvest_workflow_outbox WHERE workflow_id = ?",
            )
            .bind::<Text, _>(workflow_id.to_owned())
            .get_result(&mut *conn)
            .await
            .unwrap();
            (row.delivery_attempts, row.delivered_execution_id)
        }

        async fn sql(&self, sql: &str) {
            let mut conn = self.pool.get().await.unwrap();
            conn.batch_execute(sql).await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn drain_once_respects_its_limit() {
        let relay = relay(true).await;
        for key in ["o-1", "o-2", "o-3"] {
            relay.enqueue(key).await;
        }
        assert_eq!(relay.outbox.drain_once(2).await.unwrap(), 2);
        assert_eq!(relay.outbox.drain_once(2).await.unwrap(), 1);
        assert_eq!(relay.outbox.drain_once(0).await.unwrap(), 0);
        for key in ["o-1", "o-2", "o-3"] {
            assert_eq!(relay.runs(key).await.len(), 1, "{key}");
        }
        relay.driver.shutdown().await;
    }

    /// Two drains never dispatch one row, and each delivery counts once.
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_flushes_start_each_row_once() {
        let relay = relay(true).await;
        let keys: Vec<String> = (0..20).map(|n| format!("o-{n}")).collect();
        for key in &keys {
            relay.enqueue(key).await;
        }
        let (a, b, c, d) = tokio::join!(
            relay.outbox.flush(),
            relay.outbox.flush(),
            relay.outbox.flush(),
            relay.outbox.flush(),
        );
        let total = a.unwrap() + b.unwrap() + c.unwrap() + d.unwrap();
        assert_eq!(total, keys.len());
        for key in &keys {
            assert_eq!(relay.runs(key).await.len(), 1, "{key}");
            assert_eq!(relay.row(key).await.0, 1, "{key}");
        }
        relay.driver.shutdown().await;
    }

    /// A disabled relay delivers nothing. The rows wait, as on Postgres.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_disabled_outbox_delivers_nothing() {
        let relay = relay(false).await;
        relay.enqueue("o-off").await;
        assert_eq!(relay.outbox.flush().await.unwrap(), 0);
        assert_eq!(relay.outbox.drain_once(10).await.unwrap(), 0);
        assert_eq!(relay.row("o-off").await, (0, None));
        assert!(relay.runs("o-off").await.is_empty());
        relay.driver.shutdown().await;
    }

    /// The relay ends when the driver stops. It marks no failure, so the
    /// next boot delivers the row on its first attempt.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_relay_stops_when_the_driver_stops() {
        let relay = relay(true).await;
        let stop = CancellationToken::new();
        relay.harvest.shutdown().await.unwrap();
        relay.enqueue("o-stop").await;
        let task = relay.outbox.clone().spawn(stop.clone());
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("the relay ends")
            .expect("the relay does not panic");
        assert!(!stop.is_cancelled());
        assert_eq!(relay.row("o-stop").await, (0, None));
        assert!(matches!(
            relay.outbox.flush().await,
            Err(error) if error.to_string().contains("stopped")
        ));
    }

    /// An activity or a workflow runs on the driver task. A flush there
    /// would wait for itself, so it is refused.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_flush_on_the_driver_task_is_refused() {
        let relay = relay(true).await;
        relay.enqueue("o-driver").await;
        let refused = super::super::driver::scope_as_driver(relay.outbox.flush()).await;
        let error = refused.unwrap_err().to_string();
        assert!(error.contains("autumn-harvest#2470"), "{error}");
        let refused = super::super::driver::scope_as_driver(relay.outbox.drain_once(1)).await;
        assert!(refused.is_err());
        assert_eq!(relay.row("o-driver").await, (0, None));
        relay.driver.shutdown().await;
    }

    /// AC2 through the real relay: the start commits, the mark fails, and
    /// the run is terminated before the redelivery. A plain start would run
    /// the key again over the terminated run. `StartOnce` marks the row with
    /// the first run instead.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_failed_mark_redelivers_to_the_same_run() {
        let relay = relay(true).await;
        relay.enqueue_as("hold", "o-mark").await;
        relay
            .sql(
                "CREATE TRIGGER fail_mark BEFORE UPDATE ON harvest_workflow_outbox \
                 BEGIN SELECT RAISE(ABORT, 'injected'); END",
            )
            .await;
        let error = relay.outbox.flush().await.unwrap_err().to_string();
        assert!(error.contains("injected"), "{error}");
        let first = relay.runs_of("hold", "o-mark").await;
        assert_eq!(first.len(), 1, "the start committed before the mark");
        assert_eq!(relay.row("o-mark").await, (0, None));

        let exec = first[0].parse().unwrap();
        let ack = relay.harvest.terminate(exec, "operator").await.unwrap();
        assert_eq!(ack.state, "TERMINATED");
        relay.sql("DROP TRIGGER fail_mark").await;
        assert_eq!(relay.outbox.flush().await.unwrap(), 1);
        assert_eq!(relay.row("o-mark").await, (1, Some(first[0].clone())));
        assert_eq!(relay.runs_of("hold", "o-mark").await, first);
        relay.driver.shutdown().await;
    }
}
