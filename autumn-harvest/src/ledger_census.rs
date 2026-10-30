//! Record which function sends each SQL statement (issues #2366, #2414 and #2429).
//!
//! The Ledger census measures every profiled query in `docs/ledger/`. A plan
//! needs the statement text, and a profile is keyed by the function that sends
//! it. Diesel can report each statement to a default instrumentation. This
//! module installs one when `HARVEST_LEDGER_CENSUS` names an output file.
//!
//! SQLite has no default instrumentation. An auto-extension runs for each
//! connection that the process opens. It installs a `trace_v2` callback that
//! reports each statement when it starts to run.
//!
//! For each statement, the hook writes one JSON line with the SQL text and the
//! source frames of the call stack. `docs/audits/ledger-census.py` maps the
//! frames to profiles. The hook samples a few stacks for each distinct
//! statement, because symbol resolution is slow.
//!
//! The module exists only with the `ledger-census` feature, which no release
//! build enables. Even then, the hook stays inert until the variable is set.
//!
//! # Linking
//!
//! A function in `.init_array` installs the hook before `main`. The linker
//! drops an archive member that no symbol pulls in, so the census run passes
//! `-u harvest_ledger_census_anchor` to the linker. That pulls this module
//! into every test binary, the plugin's binaries included, with no call site.
//! `docs/ledger/README.md` gives the command.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::sync::{Mutex, OnceLock};

use diesel::connection::{Instrumentation, InstrumentationEvent};

/// The variable that names the census output file.
pub const CENSUS_ENV: &str = "HARVEST_LEDGER_CENSUS";

/// Stacks recorded for each distinct statement text.
const SAMPLES_PER_STATEMENT: u32 = 8;

/// Source frames kept from each stack.
const MAX_FRAMES: usize = 48;

/// Diesel appends the bind values after this marker.
const BINDS_MARKER: &str = " -- binds: ";

struct Census {
    out: Mutex<File>,
    seen: Mutex<HashMap<String, u32>>,
}

fn census() -> Option<&'static Census> {
    static CENSUS: OnceLock<Option<Census>> = OnceLock::new();
    CENSUS
        .get_or_init(|| {
            let path = std::env::var_os(CENSUS_ENV)?;
            let out = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()?;
            Some(Census {
                out: Mutex::new(out),
                seen: Mutex::new(HashMap::new()),
            })
        })
        .as_ref()
}

/// Strip the bind values, so one statement has one text.
#[must_use]
pub fn statement_text(debug: &str) -> &str {
    debug
        .split_once(BINDS_MARKER)
        .map_or(debug, |(sql, _)| sql)
        .trim()
}

/// Return the `Debug` text of the bind values, or an empty string.
///
/// A generic plan cannot prove a partial-index predicate such as `NOT fired`
/// from a bound `fired = $3`. The script puts the bool and string values in
/// place of their parameters, as Postgres does for each custom plan.
#[must_use]
pub fn bind_text(debug: &str) -> &str {
    debug
        .split_once(BINDS_MARKER)
        .map_or("", |(_, binds)| binds.trim())
}

/// Keep the `crate/src/path:line` of each frame in a workspace crate's `src/` tree.
///
/// The hook's own frames are dropped, because no profile covers them.
///
/// Rustc records the package under test as `./src/...`, relative to its own
/// directory. `package` names that directory, so its frames keep their crate.
#[must_use]
pub fn source_frames(backtrace: &str, package: &str) -> Vec<String> {
    backtrace
        .lines()
        .filter_map(|line| line.trim().strip_prefix("at "))
        .filter_map(|at| {
            let rel = if let Some(local) = at.strip_prefix("./src/") {
                format!("{package}/src/{local}")
            } else {
                let src = at.rfind("/src/")?;
                let crate_start = at[..src].rfind('/').map_or(0, |slash| slash + 1);
                at[crate_start..].to_owned()
            };
            let own = rel.starts_with("autumn-harvest/src/ledger_census.rs");
            (rel.starts_with("autumn-harvest") && !own).then(|| {
                let mut parts = rel.rsplitn(3, ':');
                let _column = parts.next();
                let line = parts.next().unwrap_or_default();
                let path = parts.next().unwrap_or_default();
                format!("{path}:{line}")
            })
        })
        .take(MAX_FRAMES)
        .collect()
}

/// The directory name of the package under test, from Cargo's environment.
fn package_dir() -> &'static str {
    static PACKAGE: OnceLock<String> = OnceLock::new();
    PACKAGE.get_or_init(|| {
        std::env::var("CARGO_MANIFEST_DIR")
            .ok()
            .and_then(|dir| {
                std::path::Path::new(&dir)
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_default()
    })
}

/// Write one census line for a statement, unless it has enough samples.
///
/// `binds` is Diesel's `Debug` text of the bind values. A SQLite statement
/// has none, and its line names its backend.
fn record(sql: &str, binds: &str, sqlite: bool) {
    let Some(census) = census() else {
        return;
    };
    {
        let Ok(mut seen) = census.seen.lock() else {
            return;
        };
        let count = seen.entry(sql.to_owned()).or_insert(0);
        if *count >= SAMPLES_PER_STATEMENT {
            return;
        }
        *count += 1;
    }
    let frames = source_frames(
        &std::backtrace::Backtrace::force_capture().to_string(),
        package_dir(),
    );
    // One `write` call for each line. A test that runs this binary as a
    // child process appends to the same file, and one append is atomic.
    let mut entry = serde_json::json!({ "sql": sql, "binds": binds, "frames": frames });
    if sqlite {
        entry["backend"] = "sqlite".into();
    }
    let line = format!("{entry}\n");
    if let Ok(mut out) = census.out.lock() {
        let _ = out.write_all(line.as_bytes());
    }
}

struct Hook;

impl Instrumentation for Hook {
    fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
        let InstrumentationEvent::StartQuery { query, .. } = event else {
            return;
        };
        let debug = query.to_string();
        record(statement_text(&debug), bind_text(&debug), false);
    }
}

/// Report each SQLite statement when it starts to run.
///
/// SQLite also reports each trigger program that a statement starts. Its text
/// is then a `-- TRIGGER name` comment, not the statement's own text, and the
/// statement that fired it is already recorded. A statement may itself start
/// with a comment, so only the mismatch tells the two apart.
/// The SQLite schema has no partial index on a bound value, so the plan
/// needs no bind values.
fn sqlite_trace(event: rusqlite::trace::TraceEvent<'_>) {
    let rusqlite::trace::TraceEvent::Stmt(stmt, text) = event else {
        return;
    };
    let sql = stmt.sql();
    if *sql != *text || sql.trim().is_empty() {
        return;
    }
    record(sql.trim(), "", true);
}

/// Install the statement callback on a connection that SQLite just opened.
fn sqlite_connection(conn: rusqlite::Connection) -> rusqlite::Result<()> {
    conn.trace_v2(
        rusqlite::trace::TraceEventCodes::SQLITE_TRACE_STMT,
        Some(sqlite_trace),
    );
    Ok(())
}

/// The entry point that SQLite calls for each new connection.
///
/// # Safety
///
/// SQLite calls it with a live connection handle. The handle stays open
/// for the whole call, and the bridge does not close it.
unsafe extern "C" fn sqlite_auto_extension(
    db: *mut rusqlite::ffi::sqlite3,
    err: *mut *mut std::ffi::c_char,
    _api: *const rusqlite::ffi::sqlite3_api_routines,
) -> std::ffi::c_int {
    // SAFETY: the arguments come from SQLite, as the bridge requires.
    unsafe { rusqlite::auto_extension::init_auto_extension(db, err, sqlite_connection) }
}

fn make_hook() -> Option<Box<dyn Instrumentation>> {
    census().map(|_| Box::new(Hook) as Box<dyn Instrumentation>)
}

/// Install the hook before any test opens a connection.
///
/// The test harness has no global set-up step. A function in `.init_array`
/// runs before `main`, so every connection that a test opens gets the hook.
extern "C" fn install() {
    if std::env::var_os(CENSUS_ENV).is_some() {
        let _ = diesel::connection::set_default_instrumentation(make_hook);
        // SAFETY: the extension opens no connection, and it neither closes
        // its connection nor changes the list of extensions.
        let _ = unsafe { rusqlite::auto_extension::register_auto_extension(sqlite_auto_extension) };
    }
}

#[cfg(target_os = "linux")]
#[used]
#[unsafe(link_section = ".init_array")]
static INSTALL: extern "C" fn() = install;

/// The symbol that the census run asks the linker to keep.
///
/// It does nothing. It sits in this module so that `-u` on its name pulls
/// the module, and with it `INSTALL`, into the binary.
#[unsafe(no_mangle)]
pub const extern "C" fn harvest_ledger_census_anchor() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_text_drops_the_binds() {
        assert_eq!(
            statement_text("SELECT 1 WHERE id = $1 -- binds: [7]"),
            "SELECT 1 WHERE id = $1"
        );
        assert_eq!(statement_text("  BEGIN  "), "BEGIN");
    }

    #[test]
    fn bind_text_keeps_the_values_only() {
        assert_eq!(
            bind_text("SELECT 1 WHERE id = $1 -- binds: [7, false]"),
            "[7, false]"
        );
        assert_eq!(bind_text("BEGIN"), "");
    }

    #[test]
    fn source_frames_keeps_workspace_sources_only() {
        let trace = "\
   0: std::backtrace::Backtrace::force_capture
             at /rustc/abc/library/std/src/backtrace.rs:312:13
   1: autumn_harvest::ledger_census::Hook::on_connection_event
             at /home/u/autumn-harvest/autumn-harvest/src/ledger_census.rs:129:20
   1: autumn_harvest::queue::claim_task_one_statement::{{closure}}
             at /home/u/autumn-harvest/autumn-harvest/src/queue.rs:2440:20
   2: autumn_harvest_plugin::api::list_workflows::{{closure}}
             at ./src/api.rs:120:9
   2: integration::queue_tests::claims
             at /home/u/autumn-harvest/autumn-harvest/tests/integration/queue_tests.rs:10:5";
        assert_eq!(
            source_frames(trace, "autumn-harvest-plugin"),
            vec![
                "autumn-harvest/src/queue.rs:2440".to_owned(),
                "autumn-harvest-plugin/src/api.rs:120".to_owned(),
            ]
        );
    }
}
