//! Deterministic (non-criterion) harness for profiling `ReplayVerifier`'s
//! batch fixture-verification cost -- issue #251's realistic budget
//! ("verifying 1,000 fixtures averaging 1k events each completes in under
//! 30 seconds on a 4-core laptop, in-memory user code, no DB").
//!
//! Unlike `replay_profile.rs` (which constructs `Vec<WorkflowEvent>` directly
//! in memory and calls `WorkflowReplayer::replay_from_events`),
//! `ReplayVerifier::verify_dir` exercises a *different* boundary that no
//! existing profiling harness in this repo touches: real filesystem I/O
//! (`std::fs::read_to_string` over a directory walk) and JSON *deserialize*
//! of `HistorySnapshot`/`WorkflowEvent` from a string -- the same
//! `serde_json::from_str` boundary a production worker crosses every time it
//! loads a recorded history from `harvest_events`. `replay_profile.rs`
//! constructs events as Rust struct literals and never exercises this path
//! at all.
//!
//! Mirrors `replay_verifier_bench.rs`'s exact fixture shape (`Value::Null`
//! payloads, `activity_count` activities per fixture) so this harness
//! measures the *same* documented issue #251 workload, not a bespoke shape
//! invented to flatter a particular change. The one difference is the
//! *fixture count*, which is reduced by default (see
//! `VERIFY_PROFILE_FIXTURES` below) purely to keep a single valgrind run
//! tractable -- callgrind emulation is roughly one to two orders of
//! magnitude slower than native execution, and issue #251's full 1,000
//! fixtures is calibrated against a 30-second *native* wall-clock budget.
//! Fixture count and total instructions scale linearly (see
//! `docs/performance-verify.md`), so a reduced run is representative; set
//! `VERIFY_PROFILE_FIXTURES=1000` to reproduce the exact documented shape
//! given enough wall-clock headroom.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is not measured with `cargo bench` / criterion timing. It is driven
//! directly under `valgrind --tool=callgrind` (instruction counts) and
//! `valgrind --tool=dhat` (allocation counts/bytes), which are deterministic
//! across runs. See `docs/performance-verify.md` for the numbers this
//! produces and how to reproduce them.
//!
//! `harness = false`, own `main()` -- same shape as `replay_profile.rs` -- so
//! the compiled artifact is a plain executable a profiler can be pointed at
//! directly, with no criterion wall-clock loop diluting the measured work.
//!
//! # Two-phase mode (`prepare` / `run`) -- the profiling-correct entry point
//!
//! `verify_dir`'s runtime spawns one `tokio::task` per fixture (see
//! `ReplayVerifier::verify_dir`'s implementation) and drives them on a
//! multi-threaded runtime. Under callgrind, cost incurred while a *worker
//! thread* executes a spawned task's poll is **not** attributed as a
//! call-graph descendant of the `block_on` frame on the *spawning* thread --
//! callgrind's flat/self-cost view (`callgrind_annotate`) still sums it
//! correctly across all threads, but the `block_on` call site's own
//! "inclusive cost" in the call-graph view undercounts. Do not use
//! `block_on`'s call-graph inclusive cost as a proxy for `verify_dir`'s share
//! of the profile; use `callgrind_annotate`'s flat totals as this crate's
//! `docs/performance-verify.md` does.
//!
//! Fixture generation (`build_fixture_json` + `serde_json::to_string` + N
//! `std::fs::write` calls) is real, non-trivial cost that has nothing to do
//! with `ReplayVerifier` and must not be included in a `verify_dir`
//! measurement. `VERIFY_PROFILE_MODE=prepare` runs *only* that fixture-writing
//! step (no tokio runtime, no `ReplayVerifier`) against a directory named by
//! `VERIFY_PROFILE_DIR`, so it can be run **unprofiled** as a setup step.
//! `VERIFY_PROFILE_MODE=run` then does the reverse -- no fixture generation at
//! all, just: build a tokio runtime (~60K instructions; negligible against
//! `verify_dir`'s cost -- see `docs/performance-verify.md`'s reconciliation
//! section) and call `verify_dir` on the pre-populated `VERIFY_PROFILE_DIR`.
//! Point a profiler at the `run`-mode invocation only:
//!
//! ```text
//! export VERIFY_PROFILE_DIR=/tmp/verify-fixtures
//! export VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500
//! mkdir -p "$VERIFY_PROFILE_DIR"
//!
//! # Unprofiled setup -- writes fixtures, does not touch ReplayVerifier:
//! VERIFY_PROFILE_MODE=prepare <path-to-binary>
//!
//! # Profiled measurement -- ONLY tokio-runtime-build + verify_dir:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
//!   --callgrind-out-file=callgrind.out \
//!   env VERIFY_PROFILE_MODE=run <path-to-binary>
//! callgrind_annotate callgrind.out
//! ```
//!
//! `VERIFY_PROFILE_MODE` (default `full`, when unset) selects the mode:
//! `prepare` (write fixtures only, no runtime), `run` (verify a pre-populated
//! directory only, no fixture generation -- both `prepare` and `run` require
//! `VERIFY_PROFILE_DIR` to be set to the same path), or the default `full`
//! (build + write fixtures to a fresh tempdir + verify, all in one process --
//! a convenience mode for a quick smoke run; NOT the mode to point a profiler
//! at when isolating `verify_dir`'s own cost, since it also measures fixture
//! generation). Any other value panics rather than silently falling back to
//! `full` -- a typo in `VERIFY_PROFILE_MODE` (e.g. `ru` instead of `run`)
//! must not produce a plausible-looking but methodologically invalid
//! measurement.
//!
//! # Running (`full` convenience mode)
//!
//! ```text
//! # Locate the compiled binary (no criterion timing loop runs; this just
//! # resolves the path cargo built):
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench verify_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable'
//!
//! # Instruction counts:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=callgrind.out <path>
//! callgrind_annotate callgrind.out
//!
//! # Allocation counts/bytes:
//! valgrind --tool=dhat --dhat-out-file=dhat.json <path>
//! ```
//!
//! `VERIFY_PROFILE_FIXTURES` (default `20`) sets the number of fixture files
//! written to a directory and verified. `VERIFY_PROFILE_ACTIVITIES` (default
//! `500`, matching `replay_verifier_bench.rs`'s `bench_1000_fixtures` -- `500`
//! activities = `1_001` events per fixture, issue #251's exact per-fixture
//! shape) sets activities per fixture. Both env vars must match between a
//! `prepare` invocation and its paired `run` invocation.

use std::future::Future;
use std::pin::Pin;

use autumn_harvest::context::WorkflowContext;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::testing::{HistorySnapshot, ReplayVerifier};
use autumn_harvest::types::{ActivityExecId, ExecutionId};
use chrono::Utc;
use serde_json::Value;

/// Workflow that executes N sequential activities. Structurally identical to
/// `replay_verifier_bench.rs`'s `sequential_workflow`.
fn sequential_workflow<'a>(
    ctx: &'a WorkflowContext,
    input: Value,
) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>> {
    Box::pin(async move {
        let n = usize::try_from(input.as_u64().unwrap_or(0)).unwrap_or(0);
        for i in 0..n {
            ctx.execute_activity_raw(&format!("activity_{i}"), Value::Null, "default")
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(Value::Null)
    })
}

/// Build a `HistorySnapshot` JSON string with `activity_count` completed
/// activities. Byte-for-byte the same shape `replay_verifier_bench.rs`'s
/// `build_fixture_json` produces.
fn build_fixture_json(activity_count: usize) -> String {
    let exec_id = ExecutionId::new();
    let mut events = Vec::with_capacity(activity_count * 2 + 1);
    events.push(WorkflowEvent::WorkflowStarted {
        input: Value::from(activity_count as u64),
        timestamp: Utc::now(),
        last_completion_result: None,
        last_error: None,
        scheduled_time: None,
    });
    for i in 0..activity_count {
        let activity_id = ActivityExecId::new();
        events.push(WorkflowEvent::ActivityScheduled {
            activity_id,
            name: format!("activity_{i}"),
            input: Value::Null,
            queue: "default".into(),
        });
        events.push(WorkflowEvent::ActivityCompleted {
            activity_id,
            output: Value::Null,
        });
    }
    let snapshot = HistorySnapshot {
        workflow_name: "sequential".to_string(),
        execution_id: exec_id,
        events,
        context_headers: None,
        execution_timeout: None,
        deadline_at: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// Read `key` as a `usize`. An *absent* variable uses `default`; a
/// *present but unparseable* value panics rather than silently falling back
/// to `default` -- a typo like `VERIFY_PROFILE_FIXTURES=2O` (letter O) or
/// `VERIFY_PROFILE_ACTIVITIES=1000x` must not silently select a different
/// workload and produce a plausible-looking but methodologically invalid
/// measurement (the same principle `VERIFY_PROFILE_MODE`'s dispatch below
/// applies to a malformed mode string). Runs at the very top of `main()`,
/// before the `prepare`/`run` mode split, so it executes in the profiled
/// `run`-mode process too -- but the success-path work (one env lookup, one
/// `parse()` call) is byte-for-byte what the previous
/// `.ok().and_then(...).unwrap_or(default)` chain already did; only the
/// error-handling branches differ, and those don't execute for a
/// correctly-configured measurement run, so this needed no re-measurement.
fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|e| panic!("{key}={value:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key} is not valid UTF-8: {}", raw.to_string_lossy())
        }
    }
}

/// Filename for the tiny sidecar `prepare_fixtures` writes alongside the
/// fixture files, recording the exact `fixtures`/`activities` values used to
/// generate them (`key=value` lines -- deliberately not JSON: `verify_dir`'s
/// directory walk globs every `*.json` file in the directory as a fixture to
/// replay, and a stray non-fixture `.json` file there would fail as a
/// spurious harness error). `run` mode reads it via `read_prepared_meta` to
/// derive the ground-truth workload shape *cheaply*, in preference to
/// `activities_per_fixture_from_disk`'s full-fixture parse below -- see that
/// function's doc comment for why a *some* ground-truth check is needed at
/// all, and its measured cost (~3.9M instructions / ~3% of the isolated
/// `verify_dir` total measured elsewhere in this file's history -- see
/// `docs/performance-verify.md` -- non-negligible, and exactly the kind of
/// unrelated cost the two-phase split exists to keep out of the profiled
/// `run`-mode region). The sidecar read costs a few thousand instructions
/// for a few dozen bytes, not millions for an entire ~1001-event fixture.
const PROFILE_META_FILENAME: &str = "verify_profile_meta.txt";

/// Read the `fixtures`/`activities` values `prepare_fixtures` persisted to
/// `PROFILE_META_FILENAME`. `None` when the sidecar is missing (a directory
/// populated without going through `VERIFY_PROFILE_MODE=prepare` at all),
/// in which case the caller falls back to `activities_per_fixture_from_disk`.
fn read_prepared_meta(dir: &std::path::Path) -> Option<(usize, usize)> {
    let text = std::fs::read_to_string(dir.join(PROFILE_META_FILENAME)).ok()?;
    let mut fixtures = None;
    let mut activities = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("fixtures=") {
            fixtures = value.trim().parse().ok();
        } else if let Some(value) = line.strip_prefix("activities=") {
            activities = value.trim().parse().ok();
        }
    }
    Some((fixtures?, activities?))
}

/// Remove any pre-existing `fixture_*.json` files from `dir` before
/// `prepare_fixtures` writes a fresh set. Without this, re-running `prepare`
/// with a *smaller* `VERIFY_PROFILE_FIXTURES` than a previous run against the
/// same `VERIFY_PROFILE_DIR` leaves higher-numbered files behind (e.g. a
/// prior `fixtures=50` run's `fixture_00020.json`..`fixture_00049.json`
/// survive a later `fixtures=20` run, which only rewrites indices 0..19).
/// `verify_dir` globs every `*.json` file in the directory, so those stale
/// leftovers -- possibly written with a *different* `VERIFY_PROFILE_ACTIVITIES`
/// value -- would silently be included in the next `run`-mode measurement,
/// producing a non-uniform, partially-stale workload instead of the
/// requested one; `report.fixtures_total`'s equality assert only catches
/// this when the stale count happens to disagree with the *new* run's
/// `VERIFY_PROFILE_FIXTURES`, not when it happens to coincide. Only removes
/// files matching the exact naming pattern `prepare_fixtures` itself writes
/// -- the sidecar (non-`.json` extension) and any unrelated file are left
/// alone. Runs only from `prepare`/`full` mode, never from `run` mode, so
/// this adds zero cost to the profiled measurement region.
fn remove_stale_fixtures(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        let starts_with_fixture = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("fixture_"));
        let is_json = path.extension().and_then(|ext| ext.to_str()) == Some("json");
        if starts_with_fixture && is_json {
            std::fs::remove_file(&path).expect("remove stale fixture");
        }
    }
}

/// Write `fixtures` copies of the same fixture JSON into `dir`, plus the
/// `PROFILE_META_FILENAME` sidecar recording the exact values used. Pure
/// filesystem I/O plus one one-time `serde_json::to_string` -- intentionally
/// isolable so it can run **unprofiled** (`VERIFY_PROFILE_MODE=prepare`)
/// ahead of a profiled `VERIFY_PROFILE_MODE=run` pass that measures only
/// `ReplayVerifier::verify_dir`. Idempotent regardless of growing or
/// shrinking `fixtures`/`activities` between runs: `remove_stale_fixtures`
/// clears any pre-existing `fixture_*.json` files first, so re-running
/// `prepare` against an existing directory always leaves it in exactly the
/// state a single `prepare` call with the current arguments would produce.
fn prepare_fixtures(dir: &std::path::Path, fixtures: usize, activities: usize) {
    std::fs::create_dir_all(dir).expect("create fixture dir");
    remove_stale_fixtures(dir);
    let fixture_json = build_fixture_json(activities);
    for i in 0..fixtures {
        std::fs::write(dir.join(format!("fixture_{i:05}.json")), &fixture_json)
            .expect("write fixture");
    }
    std::fs::write(
        dir.join(PROFILE_META_FILENAME),
        format!("fixtures={fixtures}\nactivities={activities}\n"),
    )
    .expect("write profile meta sidecar");
}

fn required_profile_dir(mode: &str) -> std::path::PathBuf {
    std::env::var("VERIFY_PROFILE_DIR")
        .unwrap_or_else(|_| panic!("VERIFY_PROFILE_DIR must be set in `{mode}` mode"))
        .into()
}

/// Fallback for `read_prepared_meta` returning `None`: read one fixture file
/// already on disk in `dir` and derive its per-fixture activity count from
/// the actual recorded event count (`WorkflowStarted` + two events per
/// activity). Every fixture written by `prepare_fixtures` is an identical
/// copy, so sampling exactly one is fully representative, not merely
/// convenient. Costs a full JSON parse of a realistic-size fixture (see
/// `PROFILE_META_FILENAME`'s doc comment for the measured cost) -- correct
/// as a fallback for a directory not populated via `prepare_fixtures`, but
/// not the path a normal two-phase `prepare`/`run` invocation should take.
fn activities_per_fixture_from_disk(dir: &std::path::Path) -> usize {
    let mut fixture_paths: Vec<_> = std::fs::read_dir(dir)
        .expect("read fixture dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    fixture_paths.sort();
    let sample_path = fixture_paths
        .first()
        .expect("at least one *.json fixture file in the profile dir");
    let json = std::fs::read_to_string(sample_path).expect("read sample fixture");
    let snapshot: HistorySnapshot = serde_json::from_str(&json).expect("parse sample fixture");
    // WorkflowStarted (1 event) + ActivityScheduled/ActivityCompleted per
    // activity (2 events each) -- the exact shape `build_fixture_json` emits.
    (snapshot.events.len() - 1) / 2
}

/// Build the tokio runtime + call `verify_dir` on `dir`. This is the ONLY
/// work `VERIFY_PROFILE_MODE=run` performs -- no fixture generation, no
/// directory creation -- so it is the region a profiler should be pointed
/// at when isolating `verify_dir`'s own cost. See the module doc comment.
fn run_verify(dir: &std::path::Path, fixtures: usize, activities: usize) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("tokio runtime");

    let report = rt.block_on(async {
        ReplayVerifier::new()
            .register_fn("sequential", sequential_workflow)
            .verify_dir(dir)
            .await
    });

    assert_eq!(report.fixtures_total, fixtures, "fixture count mismatch");
    assert_eq!(
        report.failed + report.harness_errors,
        0,
        "unexpected fixture failure(s): {report:?}"
    );

    // Derive the reported workload shape from what `prepare` actually wrote
    // -- see `PROFILE_META_FILENAME`'s doc comment for why this must not
    // simply trust the `activities` env-var argument, and why the cheap
    // sidecar read is preferred over the full-fixture-parse fallback inside
    // this profiled region.
    let actual_activities = read_prepared_meta(dir).map_or_else(
        || activities_per_fixture_from_disk(dir),
        |(_prepared_fixtures, prepared_activities)| prepared_activities,
    );
    if actual_activities != activities {
        eprintln!(
            "verify_profile: WARNING -- VERIFY_PROFILE_ACTIVITIES={activities} does not \
             match the {actual_activities} activities/fixture actually found on disk in \
             {} (prepare/run env drift?); the line below reports the on-disk value.",
            dir.display()
        );
    }

    println!(
        "verify_profile: fixtures={fixtures} activities_per_fixture={actual_activities} \
         total_events_verified={} succeeded={}",
        fixtures * (actual_activities * 2 + 1),
        report.succeeded,
    );
}

/// Read `VERIFY_PROFILE_MODE`, defaulting an *absent* variable to `"full"`.
/// A *non-UTF-8* value panics rather than being silently folded into the
/// same default as "absent" -- `std::env::var`'s `Err` covers both
/// `VarError::NotPresent` and `VarError::NotUnicode`, and collapsing both
/// into `.unwrap_or_else(|_| "full")` would let a non-UTF-8 mode value (a
/// launcher passing a raw, invalid-UTF-8 `OsString`, reachable on Unix)
/// silently run the un-isolated `full` mode inside what the caller believes
/// is a profiled, isolated `run`-mode invocation -- exactly the failure this
/// file's other two workload-shape checks (`env_usize` and the `match
/// mode.as_str()` catch-all just below) already guard against. Mirrors
/// `env_usize`'s `NotPresent`/`NotUnicode` split.
fn resolve_mode() -> String {
    match std::env::var("VERIFY_PROFILE_MODE") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "full".to_string(),
        Err(std::env::VarError::NotUnicode(raw)) => panic!(
            "VERIFY_PROFILE_MODE is not valid UTF-8: {}",
            raw.to_string_lossy()
        ),
    }
}

fn main() {
    let mode = resolve_mode();
    let fixtures = env_usize("VERIFY_PROFILE_FIXTURES", 20);
    let activities = env_usize("VERIFY_PROFILE_ACTIVITIES", 500);

    match mode.as_str() {
        "prepare" => {
            let dir = required_profile_dir("prepare");
            prepare_fixtures(&dir, fixtures, activities);
            println!(
                "verify_profile[prepare]: wrote {fixtures} fixtures to {}",
                dir.display()
            );
        }
        "run" => {
            let dir = required_profile_dir("run");
            run_verify(&dir, fixtures, activities);
        }
        "full" => {
            // Convenience mode: prepare + verify in one process against a
            // fresh tempdir. NOT the mode to profile in isolation -- it also
            // measures fixture generation. Use `prepare` + `run` for that.
            let dir = tempfile::tempdir().expect("tempdir");
            prepare_fixtures(dir.path(), fixtures, activities);
            run_verify(dir.path(), fixtures, activities);
        }
        other => panic!(
            "unrecognized VERIFY_PROFILE_MODE={other:?}; expected one of \
             \"prepare\", \"run\", \"full\" (the default when the env var is unset). \
             A typo here would otherwise silently fall back to `full` and reintroduce \
             the setup cost the two-phase mode exists to exclude."
        ),
    }
}
