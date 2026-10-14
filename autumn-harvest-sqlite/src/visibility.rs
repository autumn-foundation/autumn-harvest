//! List and describe executions (issue #2027).
//!
//! [`SqliteRuntime`](crate::SqliteRuntime) and [`SqliteReader`] both expose
//! these reads. The reader opens the file `READ_ONLY` and takes no writer lock.

use std::path::Path;

use autumn_harvest::ExecutionId;
use chrono::{DateTime, Utc};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OpenFlags, Row, ToSql};
use serde_json::{Map, Value};

use crate::error::{SqliteError, SqliteResult};

/// The page size when [`ExecutionFilter::limit`] is `None`.
pub const DEFAULT_LIST_LIMIT: u32 = 100;

/// The largest page. A larger [`ExecutionFilter::limit`] is cut to this.
pub const MAX_LIST_LIMIT: u32 = 1000;

/// How long a reader waits for the writer's transaction to commit.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// The columns a file can lack. An older file reads each one as NULL.
///
/// `exec_id`, `workflow_name` and `state` are not here. Every file has them,
/// so a query fails on a table without them.
const OPTIONAL_COLUMNS: [&str; 8] = [
    "workflow_id",
    "started_at",
    "closed_at",
    "search_attrs",
    "memo",
    "input_json",
    "output_json",
    "error",
];

/// One stored field, read without failing its row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Stored<T> {
    /// The field holds a value of the expected type.
    Value(T),
    /// The field is NULL, or the file has no such column.
    Null,
    /// The field holds a value this backend cannot read.
    ///
    /// A value in another storage class is unreadable. So is text that is not
    /// UTF-8, or JSON that does not parse.
    Unreadable,
}

impl<T> Stored<T> {
    /// The value, or `None` for [`Stored::Null`] and [`Stored::Unreadable`].
    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Null | Self::Unreadable => None,
        }
    }

    /// The owned value, or `None` for [`Stored::Null`] and
    /// [`Stored::Unreadable`].
    pub fn into_value(self) -> Option<T> {
        match self {
            Self::Value(value) => Some(value),
            Self::Null | Self::Unreadable => None,
        }
    }

    /// Is the field [`Stored::Unreadable`]?
    pub const fn is_unreadable(&self) -> bool {
        matches!(self, Self::Unreadable)
    }
}

/// The position of one row in a listing.
///
/// [`ExecutionFilter::before`] takes it to read the next page. It is the
/// table `rowid`, which grows in insert order. A `VACUUM` can renumber it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ListCursor(i64);

impl ListCursor {
    /// A cursor from its raw value, as [`ListCursor::raw`] gave it.
    #[must_use]
    pub const fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// The raw value, to carry the cursor outside this process.
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

/// Which executions a listing returns. All set filters combine with AND.
///
/// A name, a state and a workflow id are matched by their bytes. A value
/// stored in the wrong storage class therefore still matches.
#[derive(Debug, Clone, Default)]
pub struct ExecutionFilter {
    /// Only runs of this workflow.
    pub workflow_name: Option<String>,
    /// Only the run with this execution id.
    pub exec_id: Option<String>,
    /// Only runs whose business workflow id starts with these bytes.
    pub workflow_id_prefix: Option<String>,
    /// Only runs in one of these states. An empty list matches every state.
    pub states: Vec<String>,
    /// Only runs that started at or after this time.
    pub started_after: Option<DateTime<Utc>>,
    /// Only runs that started before this time.
    pub started_before: Option<DateTime<Utc>>,
    /// Only runs whose search attribute equals each value.
    ///
    /// A value must be a JSON string, number, boolean or `null`. Numbers
    /// compare as numbers, so `1` matches `1.0`.
    pub search_attributes: Vec<(String, Value)>,
    /// Only rows before this cursor. `None` reads from the newest row.
    pub before: Option<ListCursor>,
    /// The page size. `None` is [`DEFAULT_LIST_LIMIT`].
    pub limit: Option<u32>,
}

impl ExecutionFilter {
    /// A filter that matches every execution.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Only runs of `name`.
    #[must_use]
    pub fn workflow_name(mut self, name: impl Into<String>) -> Self {
        self.workflow_name = Some(name.into());
        self
    }

    /// Only the run with the execution id `exec_id`.
    #[must_use]
    pub fn exec_id(mut self, exec_id: impl Into<String>) -> Self {
        self.exec_id = Some(exec_id.into());
        self
    }

    /// Only runs whose business workflow id starts with `prefix`.
    #[must_use]
    pub fn workflow_id_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.workflow_id_prefix = Some(prefix.into());
        self
    }

    /// Also match runs in `state`.
    #[must_use]
    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.states.push(state.into());
        self
    }

    /// Only runs that started at or after `at`.
    #[must_use]
    pub const fn started_after(mut self, at: DateTime<Utc>) -> Self {
        self.started_after = Some(at);
        self
    }

    /// Only runs that started before `at`.
    #[must_use]
    pub const fn started_before(mut self, at: DateTime<Utc>) -> Self {
        self.started_before = Some(at);
        self
    }

    /// Only runs whose search attribute `key` equals `value`.
    #[must_use]
    pub fn search_attribute(mut self, key: impl Into<String>, value: Value) -> Self {
        self.search_attributes.push((key.into(), value));
        self
    }

    /// Only rows before `cursor`.
    #[must_use]
    pub const fn before(mut self, cursor: ListCursor) -> Self {
        self.before = Some(cursor);
        self
    }

    /// At most `limit` rows. The limit is cut to [`MAX_LIST_LIMIT`].
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// One execution as a listing shows it. It carries no payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSummary {
    /// The position of this row. Pass it to [`ExecutionFilter::before`].
    pub cursor: ListCursor,
    /// The execution id.
    pub exec_id: Stored<String>,
    /// The workflow name.
    pub workflow_name: Stored<String>,
    /// The business workflow id.
    pub workflow_id: Stored<String>,
    /// The state, such as `RUNNING` or `COMPLETED`.
    pub state: Stored<String>,
    /// The `WorkflowStarted` time. A row written before issue #2027 has none.
    pub started_at: Stored<DateTime<Utc>>,
    /// The time of the terminal write. A running run has none.
    pub closed_at: Stored<DateTime<Utc>>,
    /// The search attributes, as a JSON object.
    pub search_attributes: Stored<Map<String, Value>>,
    /// The memo that the start set.
    pub memo: Stored<Value>,
}

impl ExecutionSummary {
    /// The execution id, when it is readable and parses.
    #[must_use]
    pub fn execution_id(&self) -> Option<ExecutionId> {
        self.exec_id.value().and_then(|id| id.parse().ok())
    }
}

/// One execution with its payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionDescription {
    /// The fields a listing shows.
    pub summary: ExecutionSummary,
    /// The workflow input, as the stored JSON text.
    pub input_json: Stored<String>,
    /// The workflow output, as the stored JSON text. A run that did not
    /// complete has none.
    pub output_json: Stored<String>,
    /// The failure, cancel or terminate reason.
    pub error: Stored<String>,
}

/// One page of a listing, newest first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionPage {
    /// The rows of this page.
    pub executions: Vec<ExecutionSummary>,
    /// The cursor of the next page, or `None` when no row is left.
    pub next: Option<ListCursor>,
}

/// A read-only handle on a database file (issue #2027).
///
/// It opens the file `READ_ONLY` and takes no writer lock, so it works beside
/// a live [`SqliteRuntime`](crate::SqliteRuntime). It cannot migrate a file.
/// A column that an older file does not have reads as [`Stored::Null`].
pub struct SqliteReader {
    conn: Connection,
}

impl SqliteReader {
    /// Open the database at `path` for reading.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        // A read waits this long for the writer to commit, as the runtime's
        // own connection does.
        conn.busy_timeout(BUSY_TIMEOUT)?;
        Ok(Self { conn })
    }

    /// One page of executions, newest first, with no payloads.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::InvalidSearchAttribute`] for a filter value that
    /// is not a JSON primitive. Returns [`SqliteError::Sqlite`] if the query
    /// fails.
    pub fn list_executions(&self, filter: &ExecutionFilter) -> SqliteResult<ExecutionPage> {
        list(&self.conn, filter)
    }

    /// Call `visit` once for each row of one page, newest first.
    ///
    /// Each row carries its payloads. Only one row is in memory at a time.
    /// Returns the cursor of the next page.
    ///
    /// # Errors
    ///
    /// The errors of [`list_executions`](Self::list_executions).
    pub fn visit_executions(
        &self,
        filter: &ExecutionFilter,
        visit: impl FnMut(ExecutionDescription),
    ) -> SqliteResult<Option<ListCursor>> {
        visit_rows(&self.conn, filter, visit)
    }

    /// One execution with its payloads.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::ExecutionNotFound`] for an unknown id. Returns
    /// [`SqliteError::Sqlite`] if the query fails.
    pub fn describe(&self, exec: ExecutionId) -> SqliteResult<ExecutionDescription> {
        describe(&self.conn, exec)
    }
}

/// See [`SqliteReader::list_executions`].
pub fn list(conn: &Connection, filter: &ExecutionFilter) -> SqliteResult<ExecutionPage> {
    let mut executions = Vec::new();
    let next = query(conn, filter, false, |row| {
        executions.push(summary(row)?);
        Ok(())
    })?;
    Ok(ExecutionPage { executions, next })
}

/// See [`SqliteReader::visit_executions`].
pub fn visit_rows(
    conn: &Connection,
    filter: &ExecutionFilter,
    mut visit: impl FnMut(ExecutionDescription),
) -> SqliteResult<Option<ListCursor>> {
    query(conn, filter, true, |row| {
        visit(description(row)?);
        Ok(())
    })
}

/// See [`SqliteReader::describe`].
pub fn describe(conn: &Connection, exec: ExecutionId) -> SqliteResult<ExecutionDescription> {
    let filter = ExecutionFilter::new().exec_id(exec.to_string()).limit(1);
    let mut found = None;
    visit_rows(conn, &filter, |row| found = Some(row))?;
    found.ok_or(SqliteError::ExecutionNotFound(exec))
}

/// The page size `filter` asks for, cut to the range 1 to [`MAX_LIST_LIMIT`].
fn page_size(filter: &ExecutionFilter) -> u32 {
    filter
        .limit
        .unwrap_or(DEFAULT_LIST_LIMIT)
        .clamp(1, MAX_LIST_LIMIT)
}

/// Run one page of the listing query, and call `each` for each row.
///
/// The query reads one row past the page. That row is never passed on. It
/// only says that a next page exists. The payload columns are selected only
/// when `payloads` is set.
fn query(
    conn: &Connection,
    filter: &ExecutionFilter,
    payloads: bool,
    mut each: impl FnMut(&Row<'_>) -> SqliteResult<()>,
) -> SqliteResult<Option<ListCursor>> {
    let (sql, params) = build_query(&present_columns(conn)?, filter, payloads)?;
    let mut statement = conn.prepare(&sql)?;
    let bound: Vec<&dyn ToSql> = params.iter().map(|p| p as &dyn ToSql).collect();
    let mut rows = statement.query(bound.as_slice())?;
    let size = page_size(filter);
    let mut passed = 0_u32;
    let mut last = None;
    while let Some(row) = rows.next()? {
        if passed == size {
            return Ok(last);
        }
        last = Some(ListCursor(row.get(0)?));
        each(row)?;
        passed += 1;
    }
    Ok(None)
}

/// The optional columns that the file has.
///
/// A reader cannot migrate a file. It therefore asks the file which columns
/// exist, and selects NULL for the rest. The question is asked on each call,
/// because a runtime can migrate the file while a reader is open.
fn present_columns(conn: &Connection) -> SqliteResult<Vec<&'static str>> {
    let mut statement = conn.prepare("SELECT name FROM pragma_table_info('harvest_executions')")?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(OPTIONAL_COLUMNS
        .into_iter()
        .filter(|column| names.iter().any(|name| name == column))
        .collect())
}

/// One bound parameter of the listing query.
enum Param {
    Int(i64),
    Real(f64),
    Text(String),
}

impl ToSql for Param {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Self::Int(v) => v.to_sql(),
            Self::Real(v) => v.to_sql(),
            Self::Text(v) => v.to_sql(),
        }
    }
}

/// The SQL and the parameters of one page.
///
/// Every value is a bound parameter. Only column names from
/// [`OPTIONAL_COLUMNS`] and fixed SQL reach the text.
///
/// The name, the state and the workflow id are compared as bytes. A `BLOB`
/// that holds the same letters as a `TEXT` value never compares equal to it,
/// and a damaged row would then drop out of the listing. The cast keeps the
/// name out of the index choice, so the query walks the `rowid` backwards.
/// That is the order of the listing, so the walk stops at the page size. A
/// filter that matches few rows walks further.
fn build_query(
    present: &[&str],
    filter: &ExecutionFilter,
    payloads: bool,
) -> SqliteResult<(String, Vec<Param>)> {
    let column = |name: &str| {
        if present.contains(&name) {
            name.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let mut select = vec![
        "rowid".to_string(),
        "exec_id".to_string(),
        "workflow_name".to_string(),
        column("workflow_id"),
        "state".to_string(),
        column("started_at"),
        column("closed_at"),
        column("search_attrs"),
        column("memo"),
    ];
    if payloads {
        select.extend(["input_json", "output_json", "error"].map(column));
    }

    let mut params = vec![Param::Int(filter.before.map_or(i64::MAX, ListCursor::raw))];
    // The cursor is a bound, and not `(? IS NULL OR rowid < ?)`. The planner
    // cannot seek on an `OR`. The largest `rowid` stands in for no cursor,
    // and no insert counter reaches it.
    let mut terms = vec!["rowid < ?1".to_string()];
    if let Some(name) = &filter.workflow_name {
        let p = bind(&mut params, Param::Text(name.clone()));
        terms.push(format!("cast(workflow_name as blob) = cast({p} as blob)"));
    }
    if let Some(exec_id) = &filter.exec_id {
        // A plain compare, so the primary key answers it.
        let p = bind(&mut params, Param::Text(exec_id.clone()));
        terms.push(format!("exec_id = {p}"));
    }
    if let Some(prefix) = &filter.workflow_id_prefix {
        let len = i64::try_from(prefix.len()).unwrap_or(i64::MAX);
        let n = bind(&mut params, Param::Int(len));
        let p = bind(&mut params, Param::Text(prefix.clone()));
        terms.push(format!(
            "substr(cast({} as blob), 1, {n}) = cast({p} as blob)",
            column("workflow_id")
        ));
    }
    if !filter.states.is_empty() {
        let any: Vec<String> = filter
            .states
            .iter()
            .map(|state| {
                let p = bind(&mut params, Param::Text(state.clone()));
                format!("cast({p} as blob)")
            })
            .collect();
        terms.push(format!("cast(state as blob) IN ({})", any.join(", ")));
    }
    let started = column("started_at");
    if let Some(at) = filter.started_after {
        let p = bind(&mut params, Param::Int(at.timestamp_millis()));
        terms.push(format!(
            "typeof({started}) = 'integer' AND {started} >= {p}"
        ));
    }
    if let Some(at) = filter.started_before {
        let p = bind(&mut params, Param::Int(at.timestamp_millis()));
        terms.push(format!("typeof({started}) = 'integer' AND {started} < {p}"));
    }
    let attrs = column("search_attrs");
    for (key, value) in &filter.search_attributes {
        terms.push(search_attr_term(&mut params, &attrs, key, value)?);
    }
    let limit = bind(&mut params, Param::Int(i64::from(page_size(filter)) + 1));
    let sql = format!(
        "SELECT {} FROM harvest_executions WHERE {} ORDER BY rowid DESC LIMIT {limit}",
        select.join(", "),
        terms.join(" AND ")
    );
    Ok((sql, params))
}

/// The term that matches rows whose search attribute `key` equals `value`.
///
/// `json_each` raises on text that is not JSON, and that would fail the whole
/// page. The guard gives such a row no attributes.
fn search_attr_term(
    params: &mut Vec<Param>,
    attrs: &str,
    key: &str,
    value: &Value,
) -> SqliteResult<String> {
    let k = bind(params, Param::Text(key.to_string()));
    let test = match value {
        Value::Null => "type = 'null'".to_string(),
        Value::Bool(true) => "type = 'true'".to_string(),
        Value::Bool(false) => "type = 'false'".to_string(),
        Value::Number(number) => {
            let v = number.as_i64().map_or_else(
                || Param::Real(number.as_f64().unwrap_or(f64::NAN)),
                Param::Int,
            );
            let p = bind(params, v);
            format!("type IN ('integer', 'real') AND atom = {p}")
        }
        Value::String(text) => {
            let p = bind(params, Param::Text(text.clone()));
            format!("type = 'text' AND atom = {p}")
        }
        Value::Array(_) | Value::Object(_) => {
            return Err(SqliteError::InvalidSearchAttribute(format!(
                "the filter on `{key}` must be a JSON string, number, boolean or null"
            )));
        }
    };
    Ok(format!(
        "EXISTS (SELECT 1 FROM json_each(CASE WHEN typeof({attrs}) = 'text' \
         AND json_valid({attrs}) AND json_type({attrs}) = 'object' \
         THEN {attrs} END) WHERE key = {k} AND {test})"
    ))
}

/// Push `value` and return its placeholder.
fn bind(params: &mut Vec<Param>, value: Param) -> String {
    params.push(value);
    format!("?{}", params.len())
}

/// Read one listed row. The column order is the one [`build_query`] selects.
fn summary(row: &Row<'_>) -> SqliteResult<ExecutionSummary> {
    Ok(ExecutionSummary {
        cursor: ListCursor(row.get(0)?),
        exec_id: text(row.get_ref(1)?),
        workflow_name: text(row.get_ref(2)?),
        workflow_id: text(row.get_ref(3)?),
        state: text(row.get_ref(4)?),
        started_at: time(row.get_ref(5)?),
        closed_at: time(row.get_ref(6)?),
        search_attributes: match json(row.get_ref(7)?) {
            Stored::Value(Value::Object(map)) => Stored::Value(map),
            Stored::Value(_) | Stored::Unreadable => Stored::Unreadable,
            Stored::Null => Stored::Null,
        },
        memo: json(row.get_ref(8)?),
    })
}

/// Read one visited row, with its payloads.
fn description(row: &Row<'_>) -> SqliteResult<ExecutionDescription> {
    Ok(ExecutionDescription {
        summary: summary(row)?,
        input_json: text(row.get_ref(9)?),
        output_json: text(row.get_ref(10)?),
        error: text(row.get_ref(11)?),
    })
}

/// A text field. A value of another class, or bytes that are not UTF-8, is
/// unreadable. The engine reads these columns as `String`, so it would refuse
/// the same value.
fn text(value: ValueRef<'_>) -> Stored<String> {
    match value {
        ValueRef::Null => Stored::Null,
        ValueRef::Text(bytes) => std::str::from_utf8(bytes)
            .map_or(Stored::Unreadable, |text| Stored::Value(text.to_string())),
        ValueRef::Integer(_) | ValueRef::Real(_) | ValueRef::Blob(_) => Stored::Unreadable,
    }
}

/// An epoch-millisecond field.
fn time(value: ValueRef<'_>) -> Stored<DateTime<Utc>> {
    match value {
        ValueRef::Null => Stored::Null,
        ValueRef::Integer(ms) => {
            DateTime::from_timestamp_millis(ms).map_or(Stored::Unreadable, Stored::Value)
        }
        ValueRef::Real(_) | ValueRef::Text(_) | ValueRef::Blob(_) => Stored::Unreadable,
    }
}

/// A JSON text field.
fn json(value: ValueRef<'_>) -> Stored<Value> {
    match value {
        ValueRef::Null => Stored::Null,
        ValueRef::Text(bytes) => {
            serde_json::from_slice(bytes).map_or(Stored::Unreadable, Stored::Value)
        }
        ValueRef::Integer(_) | ValueRef::Real(_) | ValueRef::Blob(_) => Stored::Unreadable,
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::Connection;

    use super::{ExecutionFilter, build_query, present_columns};
    use crate::schema::SCHEMA;

    /// The page query seeks on the cursor and never sorts in a temporary
    /// B-tree. A sort would read every matching row before the limit.
    #[test]
    fn the_page_query_walks_the_rowid_without_a_sort() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA).unwrap();
        let filter = ExecutionFilter::new()
            .workflow_name("wf")
            .state("RUNNING")
            .workflow_id_prefix("order-")
            .started_after(chrono::Utc::now())
            .search_attribute("stage", serde_json::json!("done"));
        for payloads in [false, true] {
            let (sql, params) =
                build_query(&present_columns(&conn).unwrap(), &filter, payloads).unwrap();
            let mut statement = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let bound: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
            let plan: Vec<String> = statement
                .query_map(bound.as_slice(), |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
            let plan = plan.join("\n");
            assert!(!plan.contains("TEMP B-TREE"), "{plan}");
            assert!(plan.contains("rowid<?"), "{plan}");
        }
    }
}
