//! Issue #1834: the single-writer contract is enforced, not only documented.
//!
//! `SqliteRuntime::open` takes an exclusive OS lock on `<db>.lock`. A second
//! opener fails fast with `SqliteError::DatabaseLocked`. It fails before any
//! pragma, schema step or orphan reclaim, so it cannot steal a `RUNNING` task.
//! The kernel releases the lock when the holder exits. A crash therefore never
//! leaves a stale lock behind.
//!
//! The two-process cases run this test binary again as a child. The child runs
//! only `lock_probe_child`, which acts on the environment below.

// `noop_wf` holds no `.await`; it completes in one cycle by design.
#![allow(clippy::unused_async)]
// The `#[workflow]` macro references its input param through expansion.
#![allow(clippy::used_underscore_binding)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use autumn_harvest::prelude::*;
use autumn_harvest_sqlite::{RunState, SqliteError, SqliteRuntime};
use serde_json::json;

/// The database path the child opens. The child does nothing without it.
const PROBE_PATH_VAR: &str = "HARVEST_SQLITE_LOCK_PROBE_PATH";
/// `open`: try to open once and report. `hold`: open, report, then wait.
const PROBE_MODE_VAR: &str = "HARVEST_SQLITE_LOCK_PROBE_MODE";

const LOCKED: &str = "PROBE:LOCKED";
const OPENED: &str = "PROBE:OPENED";
const HOLDING: &str = "PROBE:HOLDING";

#[workflow]
async fn noop_wf(_ctx: &WorkflowContext, n: i64) -> Result<i64, String> {
    Ok(n + 1)
}

fn temp_db() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("harvest.sqlite3");
    (dir, path)
}

/// The child half of the two-process cases. It is a no-op in a normal run.
#[test]
fn lock_probe_child() {
    let Ok(path) = std::env::var(PROBE_PATH_VAR) else {
        return;
    };
    let mode = std::env::var(PROBE_MODE_VAR).unwrap_or_default();
    match SqliteRuntime::open(&path) {
        Ok(rt) => {
            if mode == "hold" {
                println!("{HOLDING}");
                // The parent kills this process. The lock must die with it.
                loop {
                    std::thread::park();
                    let _ = &rt;
                }
            }
            println!("{OPENED}");
        }
        Err(SqliteError::DatabaseLocked { .. }) => println!("{LOCKED}"),
        Err(other) => println!("PROBE:ERROR:{other}"),
    }
}

/// Start this test binary again, running only `lock_probe_child`.
fn spawn_probe(path: &Path, mode: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "single_writer_lock::lock_probe_child",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(PROBE_PATH_VAR, path)
        .env(PROBE_MODE_VAR, mode)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

/// Kills the child on drop, so a failed assertion never leaks a parked child.
struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Read the child's stdout until it prints a `PROBE:` token.
///
/// libtest prints `test <name> ... ` first, on the same line, so the token
/// does not start the line.
fn probe_line(child: &mut Child) -> String {
    let stdout = child.stdout.take().unwrap();
    for line in BufReader::new(stdout).lines() {
        let line = line.unwrap();
        if let Some(at) = line.find("PROBE:") {
            return line[at..].trim_end().to_string();
        }
    }
    panic!("the probe child exited without a PROBE token");
}

/// Open `path` in a child process and return what it reported.
fn open_in_child(path: &Path) -> String {
    let mut child = spawn_probe(path, "open");
    let line = probe_line(&mut child);
    child.wait().unwrap();
    line
}

#[test]
fn a_second_process_cannot_open_a_file_the_first_holds() {
    let (_dir, path) = temp_db();
    let _holder = SqliteRuntime::open(&path).unwrap();

    assert_eq!(
        open_in_child(&path),
        LOCKED,
        "a second process must fail fast on a held database file"
    );
}

#[tokio::test]
async fn the_first_process_keeps_working_after_a_second_open_fails() {
    let (_dir, path) = temp_db();
    let mut holder = SqliteRuntime::open(&path).unwrap();
    holder.register_workflow(&noop_wf_info());
    let exec = holder.start_workflow("noop_wf", json!(1)).unwrap();

    assert_eq!(open_in_child(&path), LOCKED);

    let state = holder.run_until_blocked(exec).await.unwrap();
    assert!(matches!(state, RunState::Completed(ref v) if v.as_i64() == Some(2)));
}

#[test]
fn a_killed_holder_does_not_leave_a_stale_lock() {
    let (_dir, path) = temp_db();
    let mut holder = ChildGuard(spawn_probe(&path, "hold"));
    assert_eq!(probe_line(&mut holder.0), HOLDING);

    let err = SqliteRuntime::open(&path)
        .err()
        .expect("the file is held by the child");
    assert!(matches!(err, SqliteError::DatabaseLocked { .. }), "{err}");

    // A crash, not a clean shutdown: no destructor runs in the child.
    holder.0.kill().unwrap();
    holder.0.wait().unwrap();

    SqliteRuntime::open(&path).expect("the kernel must release a dead process's lock");
}

#[test]
fn a_second_open_in_the_same_process_fails_until_the_first_drops() {
    let (_dir, path) = temp_db();
    let first = SqliteRuntime::open(&path).unwrap();

    let err = SqliteRuntime::open(&path)
        .err()
        .expect("a second runtime on one file is a second writer");
    match &err {
        SqliteError::DatabaseLocked { path: lock_path } => {
            assert!(
                lock_path
                    .to_string_lossy()
                    .ends_with("harvest.sqlite3.lock"),
                "the error must name the lock file: {}",
                lock_path.display()
            );
        }
        other => panic!("expected DatabaseLocked, got {other:?}"),
    }
    assert!(
        err.to_string().contains("another process"),
        "the message must say why: {err}"
    );

    drop(first);
    SqliteRuntime::open(&path).expect("a dropped runtime releases its lock");
}

#[cfg(unix)]
#[test]
fn a_symlinked_path_shares_the_lock_of_its_target() {
    let (dir, path) = temp_db();
    let _holder = SqliteRuntime::open(&path).unwrap();
    let link = dir.path().join("alias.sqlite3");
    std::os::unix::fs::symlink(&path, &link).unwrap();

    let err = SqliteRuntime::open(&link)
        .err()
        .expect("a second path to one file is still one file");
    assert!(matches!(err, SqliteError::DatabaseLocked { .. }), "{err}");
}

#[test]
fn a_read_only_inspector_still_reads_while_the_lock_is_held() {
    let (_dir, path) = temp_db();
    let mut holder = SqliteRuntime::open(&path).unwrap();
    holder.register_workflow(&noop_wf_info());
    holder.start_workflow("noop_wf", json!(1)).unwrap();

    let inspector =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let count: i64 = inspector
        .query_row("SELECT COUNT(*) FROM harvest_executions", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn in_memory_runtimes_take_no_lock() {
    let _a = SqliteRuntime::open_in_memory().unwrap();
    let _b = SqliteRuntime::open_in_memory().unwrap();
    let _c = SqliteRuntime::open(":memory:").unwrap();
    let _d = SqliteRuntime::open(":memory:").unwrap();
}

/// `SQLite` reports no path for a non-UTF-8 file name. The lock must still
/// apply, so `open` falls back to the path the caller gave.
#[cfg(target_os = "linux")]
#[test]
fn a_non_utf8_path_still_takes_the_lock() {
    use std::os::unix::ffi::OsStrExt;
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join(std::ffi::OsStr::from_bytes(b"harvest-\xff.sqlite3"));
    let _holder = SqliteRuntime::open(&path).unwrap();

    let err = SqliteRuntime::open(&path)
        .err()
        .expect("a non-UTF-8 path is still one file");
    assert!(matches!(err, SqliteError::DatabaseLocked { .. }), "{err}");
}

/// The lock file grants no access that the database file does not grant.
#[cfg(unix)]
#[test]
fn the_lock_file_takes_the_database_file_mode() {
    use std::os::unix::fs::PermissionsExt;
    let (_dir, path) = temp_db();
    drop(rusqlite::Connection::open(&path).unwrap());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();

    let _holder = SqliteRuntime::open(&path).unwrap();
    let mut lock = path.into_os_string();
    lock.push(".lock");
    let mode = std::fs::metadata(&lock).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o077,
        0,
        "lock mode {mode:o} must not open to others"
    );
}

/// A shared-cache in-memory URI names one database that every connection in
/// the process can open. `SQLite` reports no file for it, so it needs its own
/// in-process lock.
#[test]
fn a_shared_cache_memory_database_takes_an_in_process_lock() {
    let uri = "file:harvest_lock_1834?mode=memory&cache=shared";
    let first = SqliteRuntime::open(uri).unwrap();

    let err = SqliteRuntime::open(uri)
        .err()
        .expect("two runtimes on one shared-cache database are two writers");
    assert!(matches!(err, SqliteError::DatabaseLocked { .. }), "{err}");

    // A different shared name is a different database.
    let _other = SqliteRuntime::open("file:harvest_lock_1834_b?mode=memory&cache=shared").unwrap();

    drop(first);
    SqliteRuntime::open(uri).expect("a dropped runtime releases its in-process lock");
}
