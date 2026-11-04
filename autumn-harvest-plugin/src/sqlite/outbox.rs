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
//!   lock on the Harvest file, so the table needs no claim column.

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
CREATE INDEX IF NOT EXISTS idx_harvest_workflow_outbox_due
    ON harvest_workflow_outbox (next_attempt_at, id)
    WHERE delivered_at IS NULL;";

/// The one queue of the SQLite backend.
const QUEUE: &str = "default";

/// The refusal of a write to a database with no outbox table.
const NO_TABLE: &str = "this database has no harvest_workflow_outbox table. Write the start on \
     the app's primary database. HarvestPlugin creates the table there at boot \
     (autumn-harvest#2470)";

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
/// - an empty `workflow_id`, as on Postgres;
/// - a `queue_name` other than `"default"`;
/// - a `search_attrs` that is not an object of JSON primitives.
///
/// A database with no outbox table, such as a fleet tenant database, is a
/// `QueryBuilderError` that names this issue.
pub async fn enqueue_workflow_start_outbox(
    conn: &mut RuntimeConnection,
    request: &WorkflowStartRequest,
) -> Result<(), diesel::result::Error> {
    check_request(request)
        .map_err(|message| diesel::result::Error::QueryBuilderError(message.into()))?;
    let now = now_ms();
    diesel::sql_query(
        "INSERT INTO harvest_workflow_outbox \
         (workflow_name, workflow_id, queue_name, input, memo, search_attrs, next_attempt_at, \
         created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT (workflow_name, workflow_id) DO NOTHING",
    )
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
    if request.workflow_id.is_empty() {
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

/// A missing table as the refusal that names this issue.
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

/// Create the outbox table on the app's primary database.
///
/// # Errors
///
/// Returns a message when no connection is available or the DDL fails.
pub async fn ensure_schema(pool: &Pool<RuntimeConnection>) -> Result<(), String> {
    let mut conn = pool
        .get()
        .await
        .map_err(|error| format!("outbox schema: no connection: {error}"))?;
    conn.batch_execute(SCHEMA_SQL)
        .await
        .map_err(|error| format!("outbox schema: {error}"))
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
    /// fails.
    pub async fn flush(&self) -> Result<usize, AutumnError> {
        Ok(self.flush_inner().await?)
    }

    /// Drain one batch of at most `limit` due rows. Return the number
    /// delivered.
    ///
    /// # Errors
    ///
    /// See [`flush`](Self::flush).
    pub async fn drain_once(&self, limit: i64) -> Result<usize, AutumnError> {
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
        diesel::sql_query(
            "SELECT id, workflow_name, workflow_id, input, memo, search_attrs, \
             delivery_attempts FROM harvest_workflow_outbox \
             WHERE delivered_at IS NULL AND next_attempt_at <= ? ORDER BY id LIMIT ?",
        )
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
        diesel::sql_query(
            "UPDATE harvest_workflow_outbox SET delivery_attempts = delivery_attempts + 1, \
             last_error = NULL, delivered_execution_id = ?, delivered_at = ? \
             WHERE id = ? AND delivered_at IS NULL",
        )
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
        diesel::sql_query(
            "UPDATE harvest_workflow_outbox SET delivery_attempts = delivery_attempts + 1, \
             last_error = ?, next_attempt_at = ? WHERE id = ? AND delivered_at IS NULL",
        )
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
    /// A drain that `stop` interrupts loses nothing. A row that started and
    /// has no mark goes to the driver again at the next boot, and
    /// `StartOnce` returns the run that exists.
    pub fn spawn(self, stop: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            if !self.inner.config.enabled {
                tracing::debug!("the Harvest SQLite outbox relay is disabled");
                return;
            }
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
                        tracing::debug!(
                            "the Harvest SQLite driver stopped, so the outbox relay stops"
                        );
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
        for (request, want) in [
            (empty, "workflow_id must not be empty"),
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
}
