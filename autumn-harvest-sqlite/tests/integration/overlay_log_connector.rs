//! The overlay log on a connection factory.
//!
//! A fleet database replicates its WAL. Only its replicator checkpoints,
//! and the replicator connection closes last. These tests show that a
//! factory log keeps both rules. `DESIGN-2656.md` in `docs/design/` has the
//! plan.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use autumn_harvest::overlay_log::{ActivateOutcome, MoveRequest};
use autumn_harvest_sqlite::{SqliteConnector, SqliteError, SqliteOverlayLog};
use rusqlite::Connection;

/// The page size of a new `SQLite` file.
const PAGE: u64 = 4096;

/// The size of the WAL header and of the header of one frame.
const WAL_HEADER: u64 = 32;
const FRAME_HEADER: u64 = 24;

/// One fleet database: a temporary WAL file and its replicator connection.
struct Fleet {
    _dir: tempfile::TempDir,
    path: PathBuf,
    replicator: Option<Connection>,
}

impl Fleet {
    /// A new WAL file. Its replicator connection never checkpoints by itself.
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("tenant.db");
        let replicator = Connection::open(&path).expect("replicator");
        replicator
            .execute_batch(
                "PRAGMA journal_mode = WAL;\n\
                 PRAGMA wal_autocheckpoint = 0;\n\
                 CREATE TABLE IF NOT EXISTS app_rows (id INTEGER PRIMARY KEY);",
            )
            .expect("replicator pragmas");
        Self {
            _dir: dir,
            path,
            replicator: Some(replicator),
        }
    }

    /// A factory with `autocheckpoint` pages, that counts its calls.
    fn connector(&self, autocheckpoint: u32, calls: &Arc<AtomicUsize>) -> SqliteConnector {
        let path = self.path.clone();
        let calls = Arc::clone(calls);
        Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            let conn = Connection::open(&path)?;
            conn.execute_batch(&format!(
                "PRAGMA busy_timeout = 5000;\n\
                 PRAGMA wal_autocheckpoint = {autocheckpoint};"
            ))?;
            Ok(conn)
        })
    }

    fn wal(&self) -> PathBuf {
        wal_of(&self.path)
    }

    /// The bytes of the main file.
    fn main_bytes(&self) -> Vec<u8> {
        std::fs::read(&self.path).expect("main file")
    }

    /// The number of frames in the WAL file, from its size.
    fn wal_frames(&self) -> u64 {
        let len = std::fs::metadata(self.wal()).expect("wal file").len();
        len.saturating_sub(WAL_HEADER) / (PAGE + FRAME_HEADER)
    }

    /// `PRAGMA wal_checkpoint` on the replicator: busy, log frames and
    /// checkpointed frames.
    fn checkpoint(&self) -> (i64, i64, i64) {
        self.replicator
            .as_ref()
            .expect("replicator")
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .expect("checkpoint")
    }

    /// Close the replicator connection.
    fn close_replicator(&mut self) {
        if let Some(conn) = self.replicator.take() {
            conn.close().expect("close");
        }
    }
}

fn wal_of(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push("-wal");
    PathBuf::from(name)
}

/// A plain activation by the operator `ops`.
const fn mv(version: u64, expected: Option<u64>) -> MoveRequest<'static> {
    MoveRequest {
        version,
        expected,
        actor_kind: "operator",
        actor: "ops",
        after_seq: None,
        head: None,
    }
}

#[test]
fn the_schema_runs_through_the_factory() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let _log = SqliteOverlayLog::with_connector(fleet.connector(0, &calls)).expect("open");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "one connection for the schema"
    );
    let held = tempfile::tempdir().expect("tempdir");
    let held_path = held.path().join("held.db");
    drop(SqliteOverlayLog::open(&held_path).expect("open"));
    let expected = schema_of(&held_path);
    assert_eq!(expected.len(), 12, "four tables and eight guards");
    assert_eq!(schema_of(&fleet.path), expected, "every table and guard");
}

/// The overlay tables and triggers in the file at `path`, with their SQL.
fn schema_of(path: &Path) -> Vec<(String, String)> {
    let raw = Connection::open(path).expect("raw");
    let mut stmt = raw
        .prepare(
            "SELECT name, sql FROM sqlite_master
             WHERE name LIKE 'harvest_agent_%' AND sql IS NOT NULL ORDER BY name",
        )
        .expect("prepare");
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

#[test]
fn each_call_opens_one_connection() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let log = SqliteOverlayLog::with_connector(fleet.connector(0, &calls)).expect("open");
    let before = calls.load(Ordering::SeqCst);
    log.append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v1");
    assert_eq!(calls.load(Ordering::SeqCst), before + 1);
    log.activate("support", &mv(1, None)).expect("activate");
    assert_eq!(calls.load(Ordering::SeqCst), before + 2);
    log.active("support").expect("active").expect("v1");
    log.get("support", 1).expect("get").expect("v1");
    log.list("support").expect("list");
    log.last_activation("support")
        .expect("last")
        .expect("a move");
    assert_eq!(calls.load(Ordering::SeqCst), before + 6);
}

#[test]
fn the_log_keeps_no_connection_between_calls() {
    let mut fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let log = SqliteOverlayLog::with_connector(fleet.connector(0, &calls)).expect("open");
    log.append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v1");
    log.activate("support", &mv(1, None)).expect("activate");
    assert!(fleet.wal().exists(), "the frames are in the WAL");
    // The log is still open. If it held a connection, the replicator
    // connection would not be the last one, and its close would keep the
    // WAL file.
    fleet.close_replicator();
    assert!(
        !fleet.wal().exists(),
        "the replicator connection closed last, and SQLite removed the WAL"
    );
    let active = log.active("support").expect("active").expect("v1");
    assert_eq!(active.version, 1);
}

#[test]
fn the_log_never_checkpoints_the_wal() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let main_before = fleet.main_bytes();
    let log = SqliteOverlayLog::with_connector(fleet.connector(0, &calls)).expect("open");
    // More than the 1000 pages of the default `wal_autocheckpoint`.
    let body = vec![b'x'; 64 * 1024];
    for _ in 0..80 {
        log.append("support", &body, "operator", "ops")
            .expect("append")
            .expect("a version");
    }
    let frames = fleet.wal_frames();
    assert!(frames > 1000, "the test wrote {frames} frames");
    assert_eq!(
        fleet.main_bytes(),
        main_before,
        "no frame reached the main file"
    );
    let (busy, log_frames, done) = fleet.checkpoint();
    assert_eq!(busy, 0);
    assert_eq!(
        u64::try_from(log_frames).expect("frames"),
        frames,
        "every frame was still in the WAL"
    );
    assert_eq!(done, log_frames, "the replicator checkpointed them all");
    assert_ne!(fleet.main_bytes(), main_before);
}

/// The control of the test above: the same check sees a checkpoint.
#[test]
fn the_wal_check_sees_a_checkpoint_by_a_store_connection() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let main_before = fleet.main_bytes();
    let log = SqliteOverlayLog::with_connector(fleet.connector(1, &calls)).expect("open");
    log.append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v1");
    assert_ne!(
        fleet.main_bytes(),
        main_before,
        "a connection with wal_autocheckpoint = 1 folds its frames"
    );
}

#[test]
fn a_failed_connect_is_an_error() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let inner = fleet.connector(0, &calls);
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let gate = Arc::clone(&fail);
    let connect: SqliteConnector = Arc::new(move || {
        if gate.load(Ordering::SeqCst) {
            return Err(rusqlite::Error::InvalidPath(
                "the fleet lease is gone".into(),
            ));
        }
        inner()
    });
    let log = SqliteOverlayLog::with_connector(connect).expect("open");
    fail.store(true, Ordering::SeqCst);
    let err = log
        .append("support", b"{}", "operator", "ops")
        .expect_err("no connection");
    assert!(matches!(err, SqliteError::Sqlite(_)), "{err}");
    fail.store(false, Ordering::SeqCst);
    let row = log
        .append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v1");
    assert_eq!(row.version, 1, "the failed call wrote nothing");
}

#[test]
fn the_open_fails_when_the_first_connect_fails() {
    let connect: SqliteConnector =
        Arc::new(|| Err(rusqlite::Error::InvalidPath("no lease".into())));
    let err = SqliteOverlayLog::with_connector(connect).expect_err("no connection");
    assert!(matches!(err, SqliteError::Sqlite(_)), "{err}");
}

#[test]
fn the_guards_hold_on_a_factory_log() {
    let fleet = Fleet::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let log = SqliteOverlayLog::with_connector(fleet.connector(0, &calls)).expect("open");
    log.append("support", b"{}", "operator", "ops")
        .expect("append")
        .expect("v1");
    assert_eq!(
        log.activate("support", &mv(1, None)).expect("activate"),
        ActivateOutcome::Activated
    );
    let raw = Connection::open(&fleet.path).expect("raw");
    for (statement, guard) in [
        (
            "UPDATE harvest_agent_overlays SET body = x'7b7d'",
            "harvest_agent_overlays is append-only",
        ),
        (
            "DELETE FROM harvest_agent_overlays",
            "harvest_agent_overlays keeps its active version",
        ),
        (
            "DELETE FROM harvest_agent_overlay_heads",
            "harvest_agent_overlay_heads keeps its rows",
        ),
        (
            "UPDATE harvest_agent_overlay_activations SET actor = 'mallory'",
            "harvest_agent_overlay_activations is append-only",
        ),
    ] {
        let err = raw
            .execute(statement, [])
            .expect_err("the guard refuses it");
        assert!(err.to_string().contains(guard), "{statement}: {err}");
    }
}
