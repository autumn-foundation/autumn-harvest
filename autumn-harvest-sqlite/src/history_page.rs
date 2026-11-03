//! One page of a run's history (issue #2489).
//!
//! The management API pages `GET /workflows/{id}/history` with the Postgres
//! cursor contract. [`SqliteReader::history_page`](crate::SqliteReader::history_page)
//! serves it from the file.
//!
//! A Postgres event row has a global `id`, and a client reads it as an opaque
//! cursor. This backend keys an event by `(exec_id, seq)`, where `seq` starts
//! at 0. The page reports `seq + 1` as the `id`. Each `id` is then positive, as
//! on Postgres, so `after=0` reads from the first event.

use autumn_harvest::{ExecutionId, WorkflowEvent};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};

use crate::error::{SqliteError, SqliteResult};

/// The page size when [`HistoryFilter::limit`] is `None`. Postgres uses the
/// same default.
pub const DEFAULT_HISTORY_PAGE: u32 = 100;

/// The largest page. A larger [`HistoryFilter::limit`] is cut to this, as on
/// Postgres.
pub const MAX_HISTORY_PAGE: u32 = 1000;

/// What one history page reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct HistoryFilter {
    /// Read only the events after this `id`. `None` reads from the start.
    pub after: Option<i64>,
    /// The page size. `None` is [`DEFAULT_HISTORY_PAGE`]. The page size is
    /// cut to the range 1 to [`MAX_HISTORY_PAGE`].
    pub limit: Option<u32>,
    /// Read only events of these types, such as `"SignalReceived"`. Empty
    /// reads every type.
    pub event_types: Vec<String>,
}

impl HistoryFilter {
    /// A filter that reads the first page of every event type.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read only the events after `after`.
    #[must_use]
    pub const fn after(mut self, after: Option<i64>) -> Self {
        self.after = after;
        self
    }

    /// Set the page size.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Add one event type to the filter.
    #[must_use]
    pub fn event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_types.push(event_type.into());
        self
    }

    fn page_size(&self) -> u32 {
        self.limit
            .unwrap_or(DEFAULT_HISTORY_PAGE)
            .clamp(1, MAX_HISTORY_PAGE)
    }
}

/// One event of a [`HistoryPage`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PagedEvent {
    /// The cursor anchor: the stored `seq` plus one.
    pub id: i64,
    /// The 0-based index of the event in its run, as on Postgres.
    pub event_id: i64,
    /// When the event was appended. `None` for an event written before
    /// issue #2489, or by a file that has no such column.
    pub recorded_at: Option<DateTime<Utc>>,
    /// The event.
    pub event: WorkflowEvent,
}

/// One page of a run's history, oldest first.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HistoryPage {
    /// The events of this page.
    pub events: Vec<PagedEvent>,
    /// The `after` value of the next page, or `None` on the last page.
    pub next: Option<i64>,
    /// The number of events of the run. The filter does not change it.
    pub total_events: u64,
    /// The `id` of the last event of the run, or 0 for an empty history. The
    /// filter does not change it.
    pub last_event_id: i64,
}

/// See [`SqliteReader::history_page`](crate::SqliteReader::history_page).
pub fn history_page(
    conn: &Connection,
    exec_id: ExecutionId,
    filter: &HistoryFilter,
) -> SqliteResult<HistoryPage> {
    if !crate::store::execution_exists(conn, exec_id)? {
        return Err(SqliteError::ExecutionNotFound(exec_id));
    }
    let exec = exec_id.to_string();
    let (total, last_seq): (i64, Option<i64>) = conn.query_row(
        "SELECT count(*), max(seq) FROM harvest_events WHERE exec_id = ?1",
        params![exec],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let size = filter.page_size();
    // The cursor `id` is `seq + 1`, so `seq >= after` reads past it.
    let from_seq = filter.after.unwrap_or(0).max(0);
    // One JSON array parameter holds every type, so a long filter binds no
    // more parameters.
    let types = serde_json::to_string(&filter.event_types)?;
    let mut statement = conn.prepare(&history_page_query(has_recorded_at(conn)?))?;
    let mut rows = statement.query(params![
        exec,
        from_seq,
        filter.event_types.is_empty(),
        types,
        i64::from(size) + 1,
    ])?;
    let mut events = Vec::new();
    let mut next = None;
    while let Some(row) = rows.next()? {
        if events.len() == size as usize {
            next = events.last().map(|event: &PagedEvent| event.id);
            break;
        }
        let seq: i64 = row.get(0)?;
        let json: String = row.get(1)?;
        let recorded_at: Option<i64> = row.get(2)?;
        events.push(PagedEvent {
            id: seq + 1,
            event_id: seq,
            recorded_at: recorded_at.and_then(DateTime::from_timestamp_micros),
            event: serde_json::from_str(&json)?,
        });
    }
    Ok(HistoryPage {
        events,
        next,
        total_events: u64::try_from(total).unwrap_or_default(),
        last_event_id: last_seq.map_or(0, |seq| seq + 1),
    })
}

/// The page query. A file without `recorded_at` reads it as NULL. A reader
/// cannot add the column, so it asks the file.
fn history_page_query(recorded_at: bool) -> String {
    let recorded_at = if recorded_at { "recorded_at" } else { "NULL" };
    format!(
        "SELECT seq, event_json, {recorded_at} FROM harvest_events \
         WHERE exec_id = ?1 AND seq >= ?2 \
           AND (?3 OR json_extract(event_json, '$.type') \
                IN (SELECT value FROM json_each(?4))) \
         ORDER BY seq LIMIT ?5"
    )
}

/// Does the file have `harvest_events.recorded_at`?
fn has_recorded_at(conn: &Connection) -> SqliteResult<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM pragma_table_info('harvest_events') \
           WHERE name = 'recorded_at')",
        [],
        |row| row.get(0),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_page_size_is_cut_to_the_bounds() {
        assert_eq!(HistoryFilter::new().page_size(), DEFAULT_HISTORY_PAGE);
        assert_eq!(HistoryFilter::new().limit(0).page_size(), 1);
        assert_eq!(
            HistoryFilter::new().limit(u32::MAX).page_size(),
            MAX_HISTORY_PAGE
        );
    }

    #[test]
    fn an_old_file_reads_no_append_time() {
        assert!(history_page_query(true).contains("recorded_at FROM"));
        assert!(history_page_query(false).contains("NULL FROM"));
    }
}
