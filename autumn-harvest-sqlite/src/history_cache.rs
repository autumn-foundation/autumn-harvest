//! The warm history of the runs that one runtime drives.
//!
//! A decision cycle needs the whole history of its run. Without this cache
//! each cycle reads and parses every stored event again, so a run of `n`
//! events costs `n * n / 2` parses. With it, a cycle parses only the events
//! that the last cycle added. The Postgres worker has the same cache
//! (`autumn_harvest::cache`, issue #235).
//!
//! # Why a cached history is still the stored history
//!
//! An entry holds the events of one run up to its last cached row, the
//! *tail*. Three rules keep that prefix equal to the stored rows.
//!
//! 1. **The runtime connection only appends.** The guard triggers on
//!    `harvest_events` refuse each `UPDATE` and each replace of a row (issue
//!    #2031). No code outside this crate can use the runtime connection. So
//!    a stored row below the tail never changes through it.
//! 2. **A write of another connection drops every entry.**
//!    `PRAGMA data_version` changes when another connection commits. A load
//!    reads it after the delta rows. A changed value drops the cache, and the
//!    load reads the whole history again. A test fixture that damages a
//!    stored event uses its own connection, so this rule covers it.
//! 3. **A load checks the tail row.** The delta read starts at the tail. Its
//!    first row must be the cached tail, with the same length and hash. A
//!    purge removes every event of a run, so a purged run has no tail row,
//!    and the load reads the whole history again.
//!
//! The version is always read before the rows that it guards. A version read
//! after them could include a commit that those rows do not show, and a
//! later check would then miss that commit.
//!
//! # Memory
//!
//! The cache holds at most a byte budget of stored event JSON. The parsed
//! events take a few times that in memory. When a new entry passes the
//! budget, the entries that were used least recently go first. A run that
//! ends leaves the cache. A lost entry costs one full load and nothing else.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use autumn_harvest::{ExecutionId, WorkflowEvent};
use rusqlite::{Connection, params};

use crate::error::SqliteResult;

/// The default byte budget of the cache: 16 MiB of stored event JSON.
pub const DEFAULT_HISTORY_CACHE_BYTES: u64 = 16 * 1024 * 1024;

/// The rows of one run from `seq` on, in order. A cold load passes the lowest
/// `seq`.
const ROWS_FROM_SQL: &str = "SELECT seq, event_json FROM harvest_events \
     WHERE exec_id = ?1 AND seq >= ?2 ORDER BY seq";

/// The identity of the last cached row of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tail {
    seq: i64,
    len: usize,
    hash: u64,
}

impl Tail {
    fn of(seq: i64, json: &str) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        json.hash(&mut hasher);
        Self {
            seq,
            len: json.len(),
            hash: hasher.finish(),
        }
    }
}

/// One cached history.
struct Entry {
    events: Vec<WorkflowEvent>,
    bytes: u64,
    tail: Tail,
    /// The value of the cache clock at the last use.
    used: u64,
}

/// The history of one run, as a decision cycle reads it.
///
/// The cycle can add planned events to [`events`](Self::events).
/// [`HistoryCache::keep`] removes them again, so the cache holds only events
/// that a load read from the database.
pub struct LoadedHistory {
    /// The stored events, in order.
    pub events: Vec<WorkflowEvent>,
    /// The sum of the stored JSON lengths (issue #2023).
    pub bytes: u64,
    /// The last stored row. `None` for an empty history.
    tail: Option<Tail>,
    /// How many of `events` came from the database.
    stored: usize,
}

/// The warm histories of one runtime. See the module docs.
pub struct HistoryCache {
    entries: HashMap<ExecutionId, Entry>,
    /// The most stored bytes that the cache holds. Zero turns the cache off.
    budget: u64,
    held: u64,
    clock: u64,
    /// The `PRAGMA data_version` that every entry was read under.
    data_version: Option<i64>,
}

impl HistoryCache {
    /// A cache that holds at most `budget` bytes of stored event JSON.
    pub fn new(budget: u64) -> Self {
        Self {
            entries: HashMap::new(),
            budget,
            held: 0,
            clock: 0,
            data_version: None,
        }
    }

    /// Set the byte budget. A lower budget drops entries at once.
    pub fn set_budget(&mut self, budget: u64) {
        self.budget = budget;
        self.evict_to_budget();
    }

    /// Drop every entry.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.held = 0;
    }

    /// Drop the entry of `exec_id`, if any.
    pub fn forget(&mut self, exec_id: ExecutionId) {
        if let Some(entry) = self.entries.remove(&exec_id) {
            self.held = self.held.saturating_sub(entry.bytes);
        }
    }

    /// Load the full ordered history of `exec_id` and its stored size.
    ///
    /// A warm entry leaves the cache here. Give the result back with
    /// [`keep`](Self::keep) to cache it again.
    ///
    /// Call this outside a transaction. A row that a rollback removes must
    /// not reach the cache.
    pub fn load(&mut self, conn: &Connection, exec_id: ExecutionId) -> SqliteResult<LoadedHistory> {
        if self.budget == 0 {
            return read_from(conn, exec_id, None);
        }
        if let Some(entry) = self.entries.remove(&exec_id) {
            self.held = self.held.saturating_sub(entry.bytes);
            let warm = read_from(conn, exec_id, Some(entry))?;
            // Read the version after the delta rows. An equal value proves
            // that no other connection changed the cached prefix.
            if self.same_data_version(conn)? && warm.tail.is_some() {
                return Ok(warm);
            }
        } else {
            self.same_data_version(conn)?;
        }
        // The version is now older than this read, as each entry needs.
        read_from(conn, exec_id, None)
    }

    /// Cache `loaded` for the next cycle of `exec_id`.
    ///
    /// A history over the whole budget is not cached, so it cannot push every
    /// other run out.
    pub fn keep(&mut self, exec_id: ExecutionId, mut loaded: LoadedHistory) {
        let Some(tail) = loaded.tail else {
            return;
        };
        if loaded.bytes > self.budget {
            return;
        }
        loaded.events.truncate(loaded.stored);
        self.forget(exec_id);
        self.clock += 1;
        self.held = self.held.saturating_add(loaded.bytes);
        self.entries.insert(
            exec_id,
            Entry {
                events: loaded.events,
                bytes: loaded.bytes,
                tail,
                used: self.clock,
            },
        );
        self.evict_to_budget();
    }

    /// Read `PRAGMA data_version`. A value that differs from the stored one
    /// drops every entry and returns `false`.
    fn same_data_version(&mut self, conn: &Connection) -> SqliteResult<bool> {
        let version: i64 = conn
            .prepare_cached("PRAGMA data_version")?
            .query_row([], |row| row.get(0))?;
        if self.data_version == Some(version) {
            return Ok(true);
        }
        self.clear();
        self.data_version = Some(version);
        Ok(false)
    }

    /// Drop the least recently used entries until the cache fits its budget.
    fn evict_to_budget(&mut self) {
        while self.held > self.budget {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(exec_id, _)| *exec_id)
            else {
                self.held = 0;
                return;
            };
            self.forget(oldest);
        }
    }
}

/// Read the history of `exec_id` on top of `warm`, or all of it.
///
/// With `warm`, the read starts at the cached tail row. A missing or changed
/// tail row returns a history with no tail. The caller then reads the whole
/// history.
fn read_from(
    conn: &Connection,
    exec_id: ExecutionId,
    warm: Option<Entry>,
) -> SqliteResult<LoadedHistory> {
    let (mut events, mut bytes, cached_tail) = match warm {
        Some(entry) => (entry.events, entry.bytes, Some(entry.tail)),
        None => (Vec::new(), 0, None),
    };
    let from = cached_tail.map_or(i64::MIN, |tail| tail.seq);
    let mut tail = None;
    let mut stmt = conn.prepare_cached(ROWS_FROM_SQL)?;
    let mut rows = stmt.query(params![exec_id.to_string(), from])?;
    if let Some(cached) = cached_tail {
        let found = match rows.next()? {
            Some(row) => Some(Tail::of(row.get(0)?, event_text(row)?)),
            None => None,
        };
        if found != Some(cached) {
            return Ok(LoadedHistory {
                events: Vec::new(),
                bytes: 0,
                tail: None,
                stored: 0,
            });
        }
        tail = found;
    }
    while let Some(row) = rows.next()? {
        // Parse the borrowed text. An owned copy of each row costs one
        // allocation and buys nothing.
        let json = event_text(row)?;
        bytes = bytes.saturating_add(json.len() as u64);
        events.push(serde_json::from_str(json)?);
        tail = Some(Tail::of(row.get(0)?, json));
    }
    let stored = events.len();
    Ok(LoadedHistory {
        events,
        bytes,
        tail,
        stored,
    })
}

/// The `event_json` text of a row of [`ROWS_FROM_SQL`], borrowed.
fn event_text<'row>(row: &'row rusqlite::Row<'_>) -> rusqlite::Result<&'row str> {
    row.get_ref(1)?.as_str().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(error))
    })
}

#[cfg(test)]
mod tests {
    use autumn_harvest::{ExecutionId, WorkflowEvent};
    use rusqlite::Connection;
    use serde_json::json;

    use super::{DEFAULT_HISTORY_CACHE_BYTES, HistoryCache};
    use crate::{schema, store};

    fn marker(n: usize) -> WorkflowEvent {
        WorkflowEvent::MarkerRecorded {
            name: format!("m{n}"),
            details: json!({ "n": n, "pad": "x".repeat(20) }),
        }
    }

    /// A WAL file with the schema and the append-only guard.
    fn open_file() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let conn = Connection::open(dir.path().join("history.db")).unwrap();
        conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        conn.execute_batch(schema::SCHEMA).unwrap();
        conn.execute_batch(schema::APPEND_ONLY_GUARD).unwrap();
        (dir, conn)
    }

    fn second_connection(dir: &tempfile::TempDir) -> Connection {
        Connection::open(dir.path().join("history.db")).unwrap()
    }

    /// The stored form of each event. `WorkflowEvent` has no `PartialEq`.
    fn stored_form(events: &[WorkflowEvent]) -> Vec<String> {
        events
            .iter()
            .map(|event| serde_json::to_string(event).unwrap())
            .collect()
    }

    fn append(conn: &Connection, exec: ExecutionId, range: std::ops::Range<usize>) {
        for n in range {
            store::append_event(conn, exec, &marker(n)).unwrap();
        }
    }

    /// Load through the cache, check the result against a full load, and
    /// cache it again. Returns the number of events.
    fn load_and_keep(cache: &mut HistoryCache, conn: &Connection, exec: ExecutionId) -> usize {
        let loaded = cache.load(conn, exec).unwrap();
        let (events, bytes) = store::load_history_sized(conn, exec).unwrap();
        assert_eq!(
            stored_form(&loaded.events),
            stored_form(&events),
            "the cache returns the stored history",
        );
        assert_eq!(loaded.bytes, bytes, "and its stored size");
        let count = loaded.events.len();
        cache.keep(exec, loaded);
        count
    }

    #[test]
    fn a_warm_load_adds_only_the_new_events() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();

        append(&conn, exec, 0..3);
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 3);
        assert!(cache.entries.contains_key(&exec), "the history is warm");

        append(&conn, exec, 3..5);
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 5);
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 5, "no new event");
    }

    /// A warm load reads no row below the tail. This test removes such a row
    /// through the runtime connection, which rule 1 of the module docs
    /// forbids, to show that the cached prefix is what the load returns.
    #[test]
    fn a_warm_load_does_not_read_the_cached_prefix_again() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..3);
        load_and_keep(&mut cache, &conn, exec);

        conn.execute("DELETE FROM harvest_events WHERE seq = 0", [])
            .unwrap();

        let warm = cache.load(&conn, exec).unwrap();
        assert_eq!(warm.events.len(), 3, "the prefix came from the cache");
        assert_eq!(store::load_history(&conn, exec).unwrap().len(), 2);
    }

    #[test]
    fn a_load_takes_the_entry_until_it_is_kept_again() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..2);
        load_and_keep(&mut cache, &conn, exec);

        let loaded = cache.load(&conn, exec).unwrap();
        assert!(cache.entries.is_empty(), "the entry left the cache");
        assert_eq!(cache.held, 0);
        drop(loaded);

        assert_eq!(load_and_keep(&mut cache, &conn, exec), 2, "a cold load");
    }

    #[test]
    fn keep_drops_the_events_that_the_cycle_planned() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..2);

        let mut loaded = cache.load(&conn, exec).unwrap();
        loaded.events.push(marker(99));
        cache.keep(exec, loaded);

        assert_eq!(cache.entries[&exec].events.len(), 2);
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 2);
    }

    /// A fixture that damages a stored event uses another connection. The
    /// next load must return the damaged row, not the cached one.
    #[test]
    fn a_rewrite_on_another_connection_drops_the_cache() {
        let (dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..3);
        load_and_keep(&mut cache, &conn, exec);

        let mut other = second_connection(&dir);
        let rewritten = serde_json::to_string(&marker(77)).unwrap();
        crate::append_only::with_guard_off(&mut other, |tx| {
            tx.execute(
                "UPDATE harvest_events SET event_json = ?1 WHERE seq = 0",
                [&rewritten],
            )
        })
        .unwrap();

        let loaded = cache.load(&conn, exec).unwrap();
        assert_eq!(
            stored_form(&loaded.events[..1]),
            stored_form(&[marker(77)]),
            "the rewritten row is read",
        );
        assert_eq!(loaded.events.len(), 3);
    }

    #[test]
    fn an_append_on_another_connection_is_read() {
        let (dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..2);
        load_and_keep(&mut cache, &conn, exec);

        append(&second_connection(&dir), exec, 2..4);

        assert_eq!(load_and_keep(&mut cache, &conn, exec), 4);
    }

    /// A purge removes every event of a run on the runtime connection. The
    /// data version does not change, so the tail check must see it.
    #[test]
    fn a_purged_run_is_read_again() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..3);
        load_and_keep(&mut cache, &conn, exec);

        conn.execute("DELETE FROM harvest_events", []).unwrap();
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 0);

        append(&conn, exec, 10..12);
        assert_eq!(load_and_keep(&mut cache, &conn, exec), 2);
    }

    #[test]
    fn a_zero_budget_turns_the_cache_off() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(0);
        let exec = ExecutionId::new();
        append(&conn, exec, 0..3);

        assert_eq!(load_and_keep(&mut cache, &conn, exec), 3);

        assert!(cache.entries.is_empty());
        assert_eq!(cache.data_version, None, "the cache reads no version");
    }

    #[test]
    fn the_budget_evicts_the_least_recently_used_run() {
        let (_dir, conn) = open_file();
        let runs: Vec<ExecutionId> = (0..3).map(|_| ExecutionId::new()).collect();
        for exec in &runs {
            append(&conn, *exec, 0..4);
        }
        let (_, one_run) = store::load_history_sized(&conn, runs[0]).unwrap();
        // Room for two runs, not three.
        let mut cache = HistoryCache::new(one_run * 2 + one_run / 2);

        load_and_keep(&mut cache, &conn, runs[0]);
        load_and_keep(&mut cache, &conn, runs[1]);
        load_and_keep(&mut cache, &conn, runs[0]);
        load_and_keep(&mut cache, &conn, runs[2]);

        assert!(cache.entries.contains_key(&runs[0]), "used after run 1");
        assert!(!cache.entries.contains_key(&runs[1]), "the oldest use");
        assert!(cache.entries.contains_key(&runs[2]));
        assert_eq!(cache.held, one_run * 2);

        cache.set_budget(one_run);
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key(&runs[2]));
    }

    #[test]
    fn a_history_over_the_budget_is_not_cached() {
        let (_dir, conn) = open_file();
        let small = ExecutionId::new();
        let large = ExecutionId::new();
        append(&conn, small, 0..1);
        append(&conn, large, 0..50);
        let (_, small_bytes) = store::load_history_sized(&conn, small).unwrap();
        let mut cache = HistoryCache::new(small_bytes);

        load_and_keep(&mut cache, &conn, small);
        load_and_keep(&mut cache, &conn, large);

        assert!(cache.entries.contains_key(&small), "the small run stays");
        assert!(!cache.entries.contains_key(&large));
    }

    #[test]
    fn forget_and_clear_release_the_held_bytes() {
        let (_dir, conn) = open_file();
        let mut cache = HistoryCache::new(DEFAULT_HISTORY_CACHE_BYTES);
        let first = ExecutionId::new();
        let second = ExecutionId::new();
        append(&conn, first, 0..2);
        append(&conn, second, 0..2);
        load_and_keep(&mut cache, &conn, first);
        load_and_keep(&mut cache, &conn, second);
        assert!(cache.held > 0);

        cache.forget(first);
        assert!(!cache.entries.contains_key(&first));
        cache.clear();
        assert_eq!((cache.entries.len(), cache.held), (0, 0));
    }

    /// The next step of a fixed pseudo-random sequence.
    fn next(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *state >> 33
    }

    /// Any order of appends, foreign writes, purges, evictions and loads
    /// leaves each load equal to a full load.
    #[test]
    fn every_load_equals_a_full_load_under_mixed_writes() {
        let (dir, conn) = open_file();
        let mut other = second_connection(&dir);
        let runs: Vec<ExecutionId> = (0..4).map(|_| ExecutionId::new()).collect();
        let mut cache = HistoryCache::new(4_000);
        let mut state = 7_u64;
        let mut appended = 0_usize;

        for _ in 0..600 {
            let exec = runs[usize::try_from(next(&mut state) % 4).unwrap()];
            match next(&mut state) % 10 {
                0..=3 => {
                    load_and_keep(&mut cache, &conn, exec);
                }
                4..=6 => {
                    appended += 1;
                    append(&conn, exec, appended..appended + 1);
                }
                7 => {
                    appended += 1;
                    append(&other, exec, appended..appended + 1);
                }
                8 => {
                    let rewritten = serde_json::to_string(&marker(appended + 1_000)).unwrap();
                    crate::append_only::with_guard_off(&mut other, |tx| {
                        tx.execute(
                            "UPDATE harvest_events SET event_json = ?1 \
                             WHERE exec_id = ?2 AND seq = 0",
                            [&rewritten, &exec.to_string()],
                        )
                    })
                    .unwrap();
                }
                _ => {
                    conn.execute(
                        "DELETE FROM harvest_events WHERE exec_id = ?1",
                        [exec.to_string()],
                    )
                    .unwrap();
                }
            }
        }
        for exec in runs {
            load_and_keep(&mut cache, &conn, exec);
        }
    }
}
