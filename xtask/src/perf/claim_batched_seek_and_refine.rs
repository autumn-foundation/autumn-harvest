//! `cargo xtask perf claim-batched-seek-and-refine`: the port of
//! `autumn-harvest/scripts/claim_batched_seek_and_refine_perf_repro.sh`.
//!
//! The reproduction harness for
//! `docs/performance-claim-batched-seek-and-refine.md` (issue #1340).
//!
//! Unlike the other claim captures, this one compares two queries that BOTH
//! exist in the tree today. `queue::claim_task_query()` is the control, and
//! `queue::claim_task_batched_candidates_query()` is the candidate. So it
//! needs no git-stash dance to reconstruct a "before" state.
//!
//! Usage:
//!
//! ```text
//! HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo xtask perf claim-batched-seek-and-refine
//! ```
//!
//! It writes into `docs/perf-artifacts/claim-batched-seek-and-refine/`:
//!
//! - `control_idle.explain.txt`: `claim_task_query()` at 256 keys, 0 RUNNING.
//! - `batch_idle.explain.txt`: `claim_task_batched_candidates_query()`, same.
//! - `control_hot.explain.txt`: `claim_task_query()` at 256 keys, 2,000 RUNNING.
//! - `batch_hot.explain.txt`: `claim_task_batched_candidates_query()`, same.
//! - `end_to_end_latency.txt`: real `claim_task` against `claim_task_batched`
//!   calls on the same hot-contention fixture. It is the source of the
//!   headline milliseconds on the doc page.
//!
//! Preconditions: a reachable Postgres 16 that `HARVEST_TEST_DATABASE_URL`
//! names, migrated with the full bundle (`autumn_harvest::test_init_sql()`),
//! and a Rust toolchain that can build this crate.

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use smop::prelude::*;

use super::broken_session_scan::support::{self, Ctx};

/// The example that prints both query texts.
const DUMP_QUERIES_RS: &str = r#"fn main() {
    println!("=== CONTROL ===");
    println!("{}", autumn_harvest::queue::claim_task_query());
    println!("=== BATCH ===");
    println!("{}", autumn_harvest::queue::claim_task_batched_candidates_query());
}
"#;

/// Where the example goes for the length of the dump.
const EXAMPLE: &str = "autumn-harvest/examples/dump_queries.rs";

/// The idle fixture: a 10,000-row backlog over 4 queues and 256 keys.
const SEED_IDLE_SQL: &str = "TRUNCATE harvest_task_queue;
INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, attempt, max_attempts, scheduled_at, concurrency_key,
   concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'PENDING', (i % 100), 0, 3,
       NOW() - INTERVAL '1 second', 'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 10000) AS i;
ANALYZE harvest_task_queue;
";

/// The hot-contention addition: 2,000 RUNNING rows on the same 256 keys.
const SEED_HOT_SQL: &str = "INSERT INTO harvest_task_queue
  (id, queue_name, task_type, activity_name, activity_id, input, state,
   priority, worker_id, attempt, max_attempts, scheduled_at, started_at,
   concurrency_key, concurrency_cap, crash_strikes, wake_requested)
SELECT gen_random_uuid(), 'bench-q-' || (i % 4), 'activity', 'noop',
       gen_random_uuid(), '{}'::jsonb, 'RUNNING', 0, 'holder-' || i, 1, 3,
       NOW() - INTERVAL '10 second', NOW() - INTERVAL '5 second',
       'ckey-' || (i % 256), 1000000, 0, FALSE
FROM generate_series(1, 2000) AS i;
ANALYZE harvest_task_queue;
";

/// The query texts that the example printed.
#[derive(Debug, PartialEq, Eq)]
pub struct Queries {
    /// The line after `=== CONTROL ===`.
    pub control: String,
    /// The line after `=== BATCH ===`.
    pub batch: String,
}

/// Picks the line after each marker, as the inline Python of the script did.
///
/// A marker that appears twice resolves to its last line, because the Python
/// built a dict of marker lines.
pub fn split_queries(dump: &str) -> Result<Queries> {
    let lines: Vec<&str> = dump.lines().collect();
    let after = |marker: &str| -> Result<String> {
        let index = lines
            .iter()
            .rposition(|line| *line == marker)
            .with_context(|| format!("the query dump has no {marker:?} line"))?;
        lines
            .get(index + 1)
            .map(ToString::to_string)
            .with_context(|| format!("the query dump ends at {marker:?}"))
    };
    Ok(Queries {
        control: after("=== CONTROL ===")?,
        batch: after("=== BATCH ===")?,
    })
}

/// Writes the psql script that captures both plans of one scenario.
///
/// The lines join with `\n` and no trailing newline, as `"\n".join(lines)` did.
pub fn explain_script(queries: &Queries, scenario: &str) -> String {
    let control = &queries.control;
    let batch = &queries.batch;
    let queues = "ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3']";
    [
        "SET jit = off;".to_string(),
        "DEALLOCATE ALL;".to_string(),
        format!("PREPARE control_q (text, text[], text, int8, text[], text[]) AS {control};"),
        format!(
            "PREPARE batch_q (text, text[], text, int8, text[], text[], bool, int4, int4, \
             timestamptz, uuid, int8) AS {batch};"
        ),
        "BEGIN;".to_string(),
        format!(r"\o control_{scenario}.explain.txt"),
        format!(
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE control_q('w1', {queues}, '', NULL, \
             ARRAY[]::text[], ARRAY[]::text[]);"
        ),
        r"\o".to_string(),
        "ROLLBACK;".to_string(),
        "BEGIN;".to_string(),
        format!(r"\o batch_{scenario}.explain.txt"),
        format!(
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE batch_q('w1', {queues}, '', NULL, \
             ARRAY[]::text[], ARRAY[]::text[], false, 0, 0, NULL, NULL, 50);"
        ),
        r"\o".to_string(),
        "ROLLBACK;".to_string(),
    ]
    .join("\n")
}

/// Removes the example and the scratch directory on exit, as the `EXIT` trap did.
struct Cleanup {
    example: PathBuf,
    _work: fs::TempDir,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.example);
    }
}

/// Returns `psql "$URL" -v ON_ERROR_STOP=1`.
fn psql(url: &str, dir: &Path) -> Command {
    let mut command = Command::new("psql");
    command
        .args([url, "-v", "ON_ERROR_STOP=1"])
        .current_dir(dir);
    command
}

/// Arguments of `cargo xtask perf claim-batched-seek-and-refine`.
#[derive(Debug, Args)]
pub struct Args {}

/// Runs `cargo xtask perf claim-batched-seek-and-refine`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let Some(url) = support::non_empty_var(support::DB_URL_VAR) else {
        eprintln!(
            "HARVEST_TEST_DATABASE_URL: set HARVEST_TEST_DATABASE_URL to a migrated Postgres 16 \
             admin connection string"
        );
        return Ok(ExitCode::FAILURE);
    };
    run_in(&Ctx::from_env(), &url)
}

fn run_in(ctx: &Ctx, url: &str) -> Result<ExitCode> {
    let out_dir = ctx.path("docs/perf-artifacts/claim-batched-seek-and-refine");
    let work = fs::temp_dir()?;
    let work_dir = work.path().to_path_buf();
    let example = ctx.path(EXAMPLE);

    println!("== dumping claim_task_query() and claim_task_batched_candidates_query() ==");
    fs::write_string(work_dir.join("dump_queries.rs"), DUMP_QUERIES_RS)?;
    fs::create_dir_all(ctx.path("autumn-harvest/examples"))?;
    fs::copy(work_dir.join("dump_queries.rs"), &example)?;
    let cleanup = Cleanup {
        example,
        _work: work,
    };
    let queries_txt = work_dir.join("queries.txt");
    let status = support::status_to(
        &mut ctx.cargo([
            "run",
            "-p",
            "autumn-harvest",
            "--features",
            "db",
            "--example",
            "dump_queries",
        ]),
        support::open_out(&queries_txt, false)?,
    )?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }
    std::fs::remove_file(&cleanup.example)
        .with_context(|| format!("removing {}", cleanup.example.display()))?;

    let queries = match split_queries(&fs::read_string(&queries_txt)?) {
        Ok(queries) => queries,
        Err(error) => {
            eprintln!("{error:#}");
            return Ok(ExitCode::FAILURE);
        }
    };
    fs::write_string(work_dir.join("control.sql"), &queries.control)?;
    fs::write_string(work_dir.join("batch.sql"), &queries.batch)?;

    let scenarios = [
        (
            "== seeding fixture (10,000-row backlog, 4 queues, 256 keys) ==",
            SEED_IDLE_SQL,
            "idle",
            "== capturing idle scenario ==",
        ),
        (
            "== seeding hot-contention addition (2,000 RUNNING rows, same 256 keys) ==",
            SEED_HOT_SQL,
            "hot",
            "== capturing hot-contention scenario ==",
        ),
    ];
    for (seeding, seed_sql, scenario, capturing) in scenarios {
        println!("{seeding}");
        let status = support::status_with_stdin(&mut psql(url, &ctx.root), seed_sql)?;
        if !status.success() {
            return Ok(support::exit_code(status));
        }
        let script = format!("explain_{scenario}.sql");
        fs::write_string(work_dir.join(&script), explain_script(&queries, scenario))?;

        println!("{capturing}");
        // The shell ran this psql in a subshell that changed into the scratch
        // directory, so the `\o` files land there.
        let status = support::status_to(
            psql(url, &work_dir).args(["-f", &script]),
            std::process::Stdio::null(),
        )?;
        if !status.success() {
            return Ok(support::exit_code(status));
        }
    }

    fs::create_dir_all(&out_dir)?;
    for name in [
        "control_idle.explain.txt",
        "batch_idle.explain.txt",
        "control_hot.explain.txt",
        "batch_hot.explain.txt",
    ] {
        fs::copy(work_dir.join(name), out_dir.join(name))?;
    }

    println!("== capturing real end-to-end latency (claim_task vs claim_task_batched) ==");
    // The test seeds its own fixture, of the same shape as above. It writes
    // `end_to_end_latency.txt` straight into the artifact directory, so there
    // is nothing to copy here.
    let status = support::status(&mut ctx.cargo([
        "test",
        "-p",
        "autumn-harvest",
        "--features",
        "db,testing",
        "--test",
        "integration",
        "--",
        "--ignored",
        "--nocapture",
        "zz_capture_claim_batched_end_to_end_latency",
    ]))?;
    if !status.success() {
        return Ok(support::exit_code(status));
    }
    drop(cleanup);

    support::report_artifacts(&out_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_queries_takes_the_line_after_each_marker() {
        let dump = "=== CONTROL ===\nSELECT 1\nmore\n=== BATCH ===\nSELECT 2\n";
        let queries = split_queries(dump).unwrap();
        assert_eq!(queries.control, "SELECT 1");
        assert_eq!(queries.batch, "SELECT 2");
    }

    #[test]
    fn split_queries_fails_without_a_marker_or_a_following_line() {
        assert!(split_queries("=== CONTROL ===\nSELECT 1\n").is_err());
        assert!(split_queries("=== CONTROL ===\nSELECT 1\n=== BATCH ===\n").is_err());
    }

    #[test]
    fn explain_script_matches_the_python_generator() {
        let queries = Queries {
            control: "SELECT c".to_string(),
            batch: "SELECT b".to_string(),
        };
        let expected = [
            "SET jit = off;",
            "DEALLOCATE ALL;",
            "PREPARE control_q (text, text[], text, int8, text[], text[]) AS SELECT c;",
            "PREPARE batch_q (text, text[], text, int8, text[], text[], bool, int4, int4, \
             timestamptz, uuid, int8) AS SELECT b;",
            "BEGIN;",
            r"\o control_hot.explain.txt",
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE control_q('w1', \
             ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, \
             ARRAY[]::text[], ARRAY[]::text[]);",
            r"\o",
            "ROLLBACK;",
            "BEGIN;",
            r"\o batch_hot.explain.txt",
            "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF) EXECUTE batch_q('w1', \
             ARRAY['bench-q-0','bench-q-1','bench-q-2','bench-q-3'], '', NULL, \
             ARRAY[]::text[], ARRAY[]::text[], false, 0, 0, NULL, NULL, 50);",
            r"\o",
            "ROLLBACK;",
        ]
        .join("\n");
        assert_eq!(explain_script(&queries, "hot"), expected);
    }
}
