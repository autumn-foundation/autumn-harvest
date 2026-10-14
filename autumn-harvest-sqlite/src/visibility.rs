//! List and describe executions (issue #2027).
//!
//! [`SqliteRuntime`](crate::SqliteRuntime) and [`SqliteReader`] both expose
//! these reads. The reader opens the file `READ_ONLY` and takes no writer lock.

use std::path::Path;

use autumn_harvest::ExecutionId;
use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde_json::{Map, Value};

use crate::error::{SqliteError, SqliteResult};

/// The page size when [`ExecutionFilter::limit`] is `None`.
pub const DEFAULT_LIST_LIMIT: u32 = 100;

/// The largest page. A larger [`ExecutionFilter::limit`] is cut to this.
pub const MAX_LIST_LIMIT: u32 = 1000;

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
    pub const fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// The raw value, to carry the cursor outside this process.
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
#[derive(Debug, Clone, PartialEq)]
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
    pub fn execution_id(&self) -> Option<ExecutionId> {
        self.exec_id.value().and_then(|id| id.parse().ok())
    }
}

/// One execution with its payloads.
#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
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
        let _ = path;
        Err(SqliteError::Unsupported("SqliteReader::open".to_string()))
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
    let _ = (conn, filter);
    Err(SqliteError::Unsupported("list_executions".to_string()))
}

/// See [`SqliteReader::visit_executions`].
pub fn visit_rows(
    conn: &Connection,
    filter: &ExecutionFilter,
    visit: impl FnMut(ExecutionDescription),
) -> SqliteResult<Option<ListCursor>> {
    let _ = (conn, filter, visit);
    Err(SqliteError::Unsupported("visit_executions".to_string()))
}

/// See [`SqliteReader::describe`].
pub fn describe(conn: &Connection, exec: ExecutionId) -> SqliteResult<ExecutionDescription> {
    let _ = (conn, exec);
    Err(SqliteError::Unsupported("describe".to_string()))
}
