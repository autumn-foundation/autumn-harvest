//! The board of an agent swarm on `SQLite` (issue #2814).
//!
//! The tables and rules are those of `autumn_harvest::swarm_board` on
//! Postgres. The typed board is `SqliteSwarmStore` in
//! `autumn-harvest-agent`.
//!
//! Each write runs in one `IMMEDIATE` transaction. `SQLite` has one writer.
//! So the posts and verdicts of one swarm commit in `seq` order. A merge
//! sees no other write between its compare and its swap.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use autumn_harvest::swarm_board::{
    CommitOutcome, ItemQuery, ItemRow, MainRow, MergeCommit, MergeRow, NewPost, PostRow, bind_u64,
    bounded, stored_u64,
};
use rusqlite::config::DbConfig;
use rusqlite::{Connection, OptionalExtension as _, Row, TransactionBehavior, params};

use crate::error::{SqliteError, SqliteResult};
use crate::overlay_log::SqliteConnector;

/// The tables and the guards, applied on each open.
///
/// - The `UPDATE` triggers refuse each change of a post or a verdict.
/// - The `INSERT` triggers refuse `INSERT OR REPLACE` over a stored post or
///   verdict.
const SWARM_SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS harvest_swarm_items (
    swarm          TEXT    NOT NULL CHECK (swarm <> ''),
    item           TEXT    NOT NULL CHECK (item <> ''),
    lane           TEXT    NOT NULL CHECK (lane <> ''),
    title          TEXT    NOT NULL,
    body           TEXT    NOT NULL,
    status         TEXT    NOT NULL CHECK (status IN ('open', 'claimed', 'merged', 'dropped')),
    holder         TEXT,
    epoch          INTEGER NOT NULL CHECK (epoch >= 0),
    lease_until_ms INTEGER NOT NULL,
    base_version   INTEGER NOT NULL CHECK (base_version >= 0),
    base_snapshot  TEXT    NOT NULL,
    rev            INTEGER NOT NULL CHECK (rev >= 1),
    created_by     TEXT    NOT NULL CHECK (created_by <> ''),
    created_at_ms  INTEGER NOT NULL,
    updated_at_ms  INTEGER NOT NULL,
    PRIMARY KEY (swarm, item),
    CHECK (status <> 'claimed' OR holder IS NOT NULL),
    CHECK (status <> 'open' OR holder IS NULL)
);
CREATE INDEX IF NOT EXISTS harvest_swarm_items_board_idx
    ON harvest_swarm_items (swarm, status, lane, item);
CREATE INDEX IF NOT EXISTS harvest_swarm_items_holder_idx
    ON harvest_swarm_items (swarm, holder) WHERE status = 'claimed';
CREATE TABLE IF NOT EXISTS harvest_swarm_counters (
    swarm  TEXT    PRIMARY KEY CHECK (swarm <> ''),
    posts  INTEGER NOT NULL DEFAULT 0 CHECK (posts >= 0),
    merges INTEGER NOT NULL DEFAULT 0 CHECK (merges >= 0)
);
CREATE TABLE IF NOT EXISTS harvest_swarm_posts (
    swarm         TEXT    NOT NULL CHECK (swarm <> ''),
    seq           INTEGER NOT NULL CHECK (seq >= 1),
    post_key      TEXT    NOT NULL CHECK (post_key <> ''),
    channel       TEXT    NOT NULL CHECK (channel <> ''),
    author        TEXT    NOT NULL CHECK (author <> ''),
    body          TEXT    NOT NULL,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (swarm, seq),
    UNIQUE (swarm, post_key)
);
CREATE INDEX IF NOT EXISTS harvest_swarm_posts_channel_idx
    ON harvest_swarm_posts (swarm, channel, seq);
CREATE TABLE IF NOT EXISTS harvest_swarm_mains (
    swarm         TEXT    PRIMARY KEY CHECK (swarm <> ''),
    version       INTEGER NOT NULL CHECK (version >= 1),
    snapshot      TEXT    NOT NULL,
    score         INTEGER NOT NULL,
    item          TEXT,
    author        TEXT,
    updated_at_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS harvest_swarm_merges (
    swarm           TEXT    NOT NULL CHECK (swarm <> ''),
    seq             INTEGER NOT NULL CHECK (seq >= 1),
    submit_key      TEXT    NOT NULL CHECK (submit_key <> ''),
    item            TEXT    NOT NULL,
    holder          TEXT    NOT NULL,
    epoch           INTEGER NOT NULL CHECK (epoch >= 0),
    base_version    INTEGER NOT NULL CHECK (base_version >= 0),
    main_version    INTEGER NOT NULL CHECK (main_version >= 0),
    verdict         TEXT    NOT NULL CHECK (verdict IN (
                        'merged', 'rejected', 'conflict', 'fenced', 'judge_failed', 'contended')),
    candidate_score INTEGER,
    main_score      INTEGER,
    merged_version  INTEGER,
    snapshot        TEXT,
    detail          TEXT    NOT NULL,
    created_at_ms   INTEGER NOT NULL,
    PRIMARY KEY (swarm, seq),
    UNIQUE (swarm, submit_key),
    CHECK ((verdict = 'merged') = (merged_version IS NOT NULL)),
    CHECK ((verdict = 'merged') = (snapshot IS NOT NULL))
);
CREATE TRIGGER IF NOT EXISTS harvest_swarm_posts_immutable_upd
    BEFORE UPDATE ON harvest_swarm_posts
BEGIN
    SELECT RAISE(ABORT, 'harvest_swarm_posts is append-only (issue #2814)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_swarm_posts_immutable_ins
    BEFORE INSERT ON harvest_swarm_posts
    WHEN EXISTS (SELECT 1 FROM harvest_swarm_posts
                 WHERE swarm = NEW.swarm AND (seq = NEW.seq OR post_key = NEW.post_key))
BEGIN
    SELECT RAISE(ABORT, 'harvest_swarm_posts is append-only (issue #2814)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_swarm_merges_immutable_upd
    BEFORE UPDATE ON harvest_swarm_merges
BEGIN
    SELECT RAISE(ABORT, 'harvest_swarm_merges is append-only (issue #2814)');
END;
CREATE TRIGGER IF NOT EXISTS harvest_swarm_merges_immutable_ins
    BEFORE INSERT ON harvest_swarm_merges
    WHEN EXISTS (SELECT 1 FROM harvest_swarm_merges
                 WHERE swarm = NEW.swarm AND (seq = NEW.seq OR submit_key = NEW.submit_key))
BEGIN
    SELECT RAISE(ABORT, 'harvest_swarm_merges is append-only (issue #2814)');
END;
";

const ITEM_COLUMNS: &str = "item, lane, title, body, status, holder, epoch, lease_until_ms, \
     base_version, base_snapshot, rev, created_by, created_at_ms, updated_at_ms";

const POST_COLUMNS: &str = "seq, post_key, channel, author, body, created_at_ms";

const MAIN_COLUMNS: &str = "version, snapshot, score, item, author, updated_at_ms";

const MERGE_COLUMNS: &str = "seq, submit_key, item, holder, epoch, base_version, main_version, \
     verdict, candidate_score, main_score, merged_version, snapshot, detail, created_at_ms";

/// The board of agent swarms in one `SQLite` file.
///
/// Call it through `SqliteSwarmStore` in `autumn-harvest-agent`, which
/// checks each input.
#[doc(hidden)]
pub struct SqliteSwarmBoard {
    source: Mutex<Source>,
}

/// Where a call of the board gets its connection.
enum Source {
    /// One connection for the life of the board.
    Held(Connection),
    /// A new connection for each call.
    Factory(SqliteConnector),
}

impl std::fmt::Debug for SqliteSwarmBoard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSwarmBoard").finish_non_exhaustive()
    }
}

/// A value that does not fit its column.
const fn range(message: String) -> SqliteError {
    SqliteError::Corrupt(message)
}

fn to_u64(value: i64, what: &str) -> rusqlite::Result<u64> {
    stored_u64(value, what).map_err(|message| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Integer, message.into())
    })
}

fn bind(value: u64, what: &str) -> SqliteResult<i64> {
    bind_u64(value, what).map_err(range)
}

fn opt_bind(value: Option<u64>, what: &str) -> SqliteResult<Option<i64>> {
    value.map(|value| bind(value, what)).transpose()
}

fn item_row(row: &Row<'_>) -> rusqlite::Result<ItemRow> {
    Ok(ItemRow {
        item: row.get(0)?,
        lane: row.get(1)?,
        title: row.get(2)?,
        body: row.get(3)?,
        status: row.get(4)?,
        holder: row.get(5)?,
        epoch: to_u64(row.get(6)?, "epoch")?,
        lease_until_ms: row.get(7)?,
        base_version: to_u64(row.get(8)?, "base version")?,
        base_snapshot: row.get(9)?,
        rev: to_u64(row.get(10)?, "rev")?,
        created_by: row.get(11)?,
        created_at_ms: row.get(12)?,
        updated_at_ms: row.get(13)?,
    })
}

fn post_row(row: &Row<'_>) -> rusqlite::Result<PostRow> {
    Ok(PostRow {
        seq: to_u64(row.get(0)?, "post seq")?,
        post_key: row.get(1)?,
        channel: row.get(2)?,
        author: row.get(3)?,
        body: row.get(4)?,
        created_at_ms: row.get(5)?,
    })
}

fn main_row(row: &Row<'_>) -> rusqlite::Result<MainRow> {
    Ok(MainRow {
        version: to_u64(row.get(0)?, "main version")?,
        snapshot: row.get(1)?,
        score: row.get(2)?,
        item: row.get(3)?,
        author: row.get(4)?,
        updated_at_ms: row.get(5)?,
    })
}

fn merge_row(row: &Row<'_>) -> rusqlite::Result<MergeRow> {
    let merged: Option<i64> = row.get(10)?;
    Ok(MergeRow {
        seq: to_u64(row.get(0)?, "merge seq")?,
        submit_key: row.get(1)?,
        item: row.get(2)?,
        holder: row.get(3)?,
        epoch: to_u64(row.get(4)?, "epoch")?,
        base_version: to_u64(row.get(5)?, "base version")?,
        main_version: to_u64(row.get(6)?, "main version")?,
        verdict: row.get(7)?,
        candidate_score: row.get(8)?,
        main_score: row.get(9)?,
        merged_version: merged
            .map(|value| to_u64(value, "merged version"))
            .transpose()?,
        snapshot: row.get(11)?,
        detail: row.get(12)?,
        created_at_ms: row.get(13)?,
    })
}

impl SqliteSwarmBoard {
    /// Open the board in the database at `path`, and create its tables.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the file cannot be opened or the
    /// schema cannot be applied.
    pub fn open(path: impl AsRef<Path>) -> SqliteResult<Self> {
        Self::prepare(Connection::open(path)?)
    }

    /// Open a board in a new, private in-memory database.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] if the schema cannot be applied.
    pub fn open_in_memory() -> SqliteResult<Self> {
        Self::prepare(Connection::open_in_memory()?)
    }

    /// Open a board that gets a new connection from `connect` for each
    /// call. The rules are those of `SqliteOverlayLog::with_connector`.
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
        // As in `SqliteRuntime::open`: wait for another writer, and keep a
        // commit across a crash.
        conn.execute_batch(
            "PRAGMA busy_timeout = 5000;\n\
             PRAGMA journal_mode = WAL;\n\
             PRAGMA synchronous = FULL;",
        )?;
        apply_schema(&mut conn)?;
        Ok(Self {
            source: Mutex::new(Source::Held(conn)),
        })
    }

    /// Run `f` on a connection under the lock of the board.
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

    /// Insert the first main head of `swarm`. Returns `false` when the
    /// swarm already has one.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn seed_main(&self, swarm: &str, main: &MainRow) -> SqliteResult<bool> {
        let version = bind(main.version, "main version")?;
        self.with_conn(|conn| {
            let inserted = conn.execute(
                "INSERT INTO harvest_swarm_mains
                     (swarm, version, snapshot, score, item, author, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT (swarm) DO NOTHING",
                params![
                    swarm,
                    version,
                    main.snapshot,
                    main.score,
                    main.item,
                    main.author,
                    main.updated_at_ms
                ],
            )?;
            Ok(inserted == 1)
        })
    }

    /// The main head of `swarm`, if it has one.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn main(&self, swarm: &str) -> SqliteResult<Option<MainRow>> {
        self.with_conn(|conn| main_in(conn, swarm))
    }

    /// Insert `row` into `swarm`. Returns `false` when the id is taken.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn insert_item(&self, swarm: &str, row: &ItemRow) -> SqliteResult<bool> {
        let epoch = bind(row.epoch, "epoch")?;
        let base = bind(row.base_version, "base version")?;
        let rev = bind(row.rev, "rev")?;
        self.with_conn(|conn| {
            let inserted = conn.execute(
                &format!(
                    "INSERT INTO harvest_swarm_items (swarm, {ITEM_COLUMNS})
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
                     ON CONFLICT (swarm, item) DO NOTHING"
                ),
                params![
                    swarm,
                    row.item,
                    row.lane,
                    row.title,
                    row.body,
                    row.status,
                    row.holder,
                    epoch,
                    row.lease_until_ms,
                    base,
                    row.base_snapshot,
                    rev,
                    row.created_by,
                    row.created_at_ms,
                    row.updated_at_ms
                ],
            )?;
            Ok(inserted == 1)
        })
    }

    /// The item `item` of `swarm`, if it exists.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn item(&self, swarm: &str, item: &str) -> SqliteResult<Option<ItemRow>> {
        self.with_conn(|conn| {
            Ok(conn
                .query_row(
                    &format!(
                        "SELECT {ITEM_COLUMNS} FROM harvest_swarm_items
                         WHERE swarm = ?1 AND item = ?2"
                    ),
                    params![swarm, item],
                    item_row,
                )
                .optional()?)
        })
    }

    /// The items of `swarm` that match `query`, in id order.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn items(&self, swarm: &str, query: &ItemQuery<'_>) -> SqliteResult<Vec<ItemRow>> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {ITEM_COLUMNS} FROM harvest_swarm_items
                 WHERE swarm = ?1
                   AND (?2 IS NULL OR lane = ?2)
                   AND (?3 IS NULL OR status = ?3)
                   AND (?4 IS NULL OR holder = ?4)
                   AND (?5 IS NULL OR item > ?5)
                 ORDER BY item
                 LIMIT ?6"
            ))?;
            let rows = stmt.query_map(
                params![
                    swarm,
                    query.lane,
                    query.status,
                    query.holder,
                    query.after,
                    query.bounded_limit()
                ],
                item_row,
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// The number of items of `swarm` that `holder` holds.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn count_held(&self, swarm: &str, holder: &str) -> SqliteResult<u64> {
        self.with_conn(|conn| {
            let n: i64 = conn.query_row(
                "SELECT COUNT(*) FROM harvest_swarm_items
                 WHERE swarm = ?1 AND holder = ?2 AND status = 'claimed'",
                params![swarm, holder],
                |row| row.get(0),
            )?;
            stored_u64(n, "count").map_err(range)
        })
    }

    /// Write `next` over its item when the stored `rev` is still
    /// `expected_rev`. Returns `false` when another write landed first.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn swap_item(&self, swarm: &str, expected_rev: u64, next: &ItemRow) -> SqliteResult<bool> {
        let Ok(expected) = i64::try_from(expected_rev) else {
            return Ok(false);
        };
        self.with_conn(|conn| swap_in(conn, swarm, expected, next))
    }

    /// Add `post` to `swarm`, or return the post that its key already
    /// added.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn append_post(&self, swarm: &str, post: &NewPost<'_>) -> SqliteResult<PostRow> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if let Some(found) = post_by_key(&tx, swarm, post.post_key)? {
                return Ok(found);
            }
            let seq = bump(&tx, swarm, "posts")?;
            let row = tx.query_row(
                &format!(
                    "INSERT INTO harvest_swarm_posts
                         (swarm, seq, post_key, channel, author, body, created_at_ms)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     RETURNING {POST_COLUMNS}"
                ),
                params![
                    swarm,
                    seq,
                    post.post_key,
                    post.channel,
                    post.author,
                    post.body,
                    post.at_ms
                ],
                post_row,
            )?;
            tx.commit()?;
            Ok(row)
        })
    }

    /// The posts of `swarm` after `after`, in `seq` order.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn posts(
        &self,
        swarm: &str,
        channel: Option<&str>,
        after: u64,
        limit: u32,
    ) -> SqliteResult<Vec<PostRow>> {
        let Ok(after) = i64::try_from(after) else {
            return Ok(Vec::new());
        };
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {POST_COLUMNS} FROM harvest_swarm_posts
                 WHERE swarm = ?1 AND (?2 IS NULL OR channel = ?2) AND seq > ?3
                 ORDER BY seq
                 LIMIT ?4"
            ))?;
            let rows = stmt.query_map(params![swarm, channel, after, bounded(limit)], post_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// The verdict of `swarm` that the submit `key` logged, if any.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn merge_by_key(&self, swarm: &str, key: &str) -> SqliteResult<Option<MergeRow>> {
        self.with_conn(|conn| merge_by_key_in(conn, swarm, key))
    }

    /// The verdicts of `swarm` after `after`, in `seq` order.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when the statement fails.
    pub fn merges(&self, swarm: &str, after: u64, limit: u32) -> SqliteResult<Vec<MergeRow>> {
        let Ok(after) = i64::try_from(after) else {
            return Ok(Vec::new());
        };
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(&format!(
                "SELECT {MERGE_COLUMNS} FROM harvest_swarm_merges
                 WHERE swarm = ?1 AND seq > ?2
                 ORDER BY seq
                 LIMIT ?3"
            ))?;
            let rows = stmt.query_map(params![swarm, after, bounded(limit)], merge_row)?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        })
    }

    /// Log a verdict that merges nothing, or return the verdict that its
    /// key already logged.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn record_verdict(&self, swarm: &str, record: &MergeRow) -> SqliteResult<MergeRow> {
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if let Some(found) = merge_by_key_in(&tx, swarm, &record.submit_key)? {
                return Ok(found);
            }
            let row = insert_merge_in(&tx, swarm, record)?;
            tx.commit()?;
            Ok(row)
        })
    }

    /// Commit one merge: move main, close the item and log the verdict.
    ///
    /// One `IMMEDIATE` transaction compares the main version and the item
    /// `rev`, then writes all three. A mismatch changes nothing.
    ///
    /// # Errors
    ///
    /// Returns [`SqliteError::Sqlite`] when a statement fails.
    pub fn commit_merge(&self, swarm: &str, commit: &MergeCommit) -> SqliteResult<CommitOutcome> {
        let expected_main = bind(commit.expected_main, "main version")?;
        let next_version = bind(commit.next_main.version, "main version")?;
        let item_rev = bind(commit.item_rev, "rev")?;
        self.with_conn(|conn| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if let Some(found) = merge_by_key_in(&tx, swarm, &commit.record.submit_key)? {
                return Ok(CommitOutcome::Recorded(found));
            }
            let actual = main_in(&tx, swarm)?.map(|main| main.version);
            if actual != Some(commit.expected_main) {
                return Ok(CommitOutcome::MainMoved { actual });
            }
            if !swap_in(&tx, swarm, item_rev, &commit.next_item)? {
                return Ok(CommitOutcome::ItemMoved);
            }
            let main = &commit.next_main;
            let moved = tx.execute(
                "UPDATE harvest_swarm_mains
                 SET version = ?2, snapshot = ?3, score = ?4, item = ?5, author = ?6,
                     updated_at_ms = ?7
                 WHERE swarm = ?1 AND version = ?8",
                params![
                    swarm,
                    next_version,
                    main.snapshot,
                    main.score,
                    main.item,
                    main.author,
                    main.updated_at_ms,
                    expected_main
                ],
            )?;
            if moved != 1 {
                return Err(range(format!(
                    "the main head of swarm {swarm:?} moved inside its transaction"
                )));
            }
            let row = insert_merge_in(&tx, swarm, &commit.record)?;
            tx.commit()?;
            Ok(CommitOutcome::Committed(row))
        })
    }
}

/// One connection from `connect`. It does not checkpoint when it closes.
fn connect_once(connect: &SqliteConnector) -> SqliteResult<Connection> {
    let conn = connect()?;
    conn.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    Ok(conn)
}

/// Apply [`SWARM_SCHEMA`] in one `DEFERRED` transaction.
fn apply_schema(conn: &mut Connection) -> SqliteResult<()> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Deferred)?;
    tx.execute_batch(SWARM_SCHEMA)?;
    tx.commit()?;
    Ok(())
}

/// The body of [`SqliteSwarmBoard::main`].
fn main_in(conn: &Connection, swarm: &str) -> SqliteResult<Option<MainRow>> {
    Ok(conn
        .query_row(
            &format!("SELECT {MAIN_COLUMNS} FROM harvest_swarm_mains WHERE swarm = ?1"),
            params![swarm],
            main_row,
        )
        .optional()?)
}

/// The body of [`SqliteSwarmBoard::swap_item`].
fn swap_in(conn: &Connection, swarm: &str, expected: i64, next: &ItemRow) -> SqliteResult<bool> {
    let epoch = bind(next.epoch, "epoch")?;
    let base = bind(next.base_version, "base version")?;
    let updated = conn.execute(
        "UPDATE harvest_swarm_items
         SET lane = ?3, title = ?4, body = ?5, status = ?6, holder = ?7, epoch = ?8,
             lease_until_ms = ?9, base_version = ?10, base_snapshot = ?11, rev = rev + 1,
             updated_at_ms = ?12
         WHERE swarm = ?1 AND item = ?2 AND rev = ?13",
        params![
            swarm,
            next.item,
            next.lane,
            next.title,
            next.body,
            next.status,
            next.holder,
            epoch,
            next.lease_until_ms,
            base,
            next.base_snapshot,
            next.updated_at_ms,
            expected
        ],
    )?;
    Ok(updated == 1)
}

/// The post of `swarm` that `key` added, if any.
fn post_by_key(conn: &Connection, swarm: &str, key: &str) -> SqliteResult<Option<PostRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {POST_COLUMNS} FROM harvest_swarm_posts
                 WHERE swarm = ?1 AND post_key = ?2"
            ),
            params![swarm, key],
            post_row,
        )
        .optional()?)
}

/// The body of [`SqliteSwarmBoard::merge_by_key`].
fn merge_by_key_in(conn: &Connection, swarm: &str, key: &str) -> SqliteResult<Option<MergeRow>> {
    Ok(conn
        .query_row(
            &format!(
                "SELECT {MERGE_COLUMNS} FROM harvest_swarm_merges
                 WHERE swarm = ?1 AND submit_key = ?2"
            ),
            params![swarm, key],
            merge_row,
        )
        .optional()?)
}

/// Step the counter `column` of `swarm`, and return the new value.
fn bump(conn: &Connection, swarm: &str, column: &'static str) -> SqliteResult<i64> {
    let sql = match column {
        "posts" => {
            "INSERT INTO harvest_swarm_counters (swarm, posts) VALUES (?1, 1)
             ON CONFLICT (swarm) DO UPDATE SET posts = posts + 1
             RETURNING posts"
        }
        _ => {
            "INSERT INTO harvest_swarm_counters (swarm, merges) VALUES (?1, 1)
             ON CONFLICT (swarm) DO UPDATE SET merges = merges + 1
             RETURNING merges"
        }
    };
    Ok(conn.query_row(sql, params![swarm], |row| row.get(0))?)
}

/// Log `record` in `swarm` with the next verdict `seq`.
fn insert_merge_in(conn: &Connection, swarm: &str, record: &MergeRow) -> SqliteResult<MergeRow> {
    let seq = bump(conn, swarm, "merges")?;
    let epoch = bind(record.epoch, "epoch")?;
    let base = bind(record.base_version, "base version")?;
    let main = bind(record.main_version, "main version")?;
    let merged = opt_bind(record.merged_version, "merged version")?;
    Ok(conn.query_row(
        &format!(
            "INSERT INTO harvest_swarm_merges
                 (swarm, seq, submit_key, item, holder, epoch, base_version, main_version,
                  verdict, candidate_score, main_score, merged_version, snapshot, detail,
                  created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
             RETURNING {MERGE_COLUMNS}"
        ),
        params![
            swarm,
            seq,
            record.submit_key,
            record.item,
            record.holder,
            epoch,
            base,
            main,
            record.verdict,
            record.candidate_score,
            record.main_score,
            merged,
            record.snapshot,
            record.detail,
            record.created_at_ms
        ],
        merge_row,
    )?)
}
