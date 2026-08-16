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
use autumn_harvest::replay_sample::SampleManifest;
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
/// `PROFILE_META_FILENAME`. `None` only when the sidecar is genuinely
/// *missing* (a directory populated without going through
/// `VERIFY_PROFILE_MODE=prepare` at all), in which case the caller falls
/// back to `activities_per_fixture_from_disk`. A sidecar that *exists* but
/// is malformed or truncated (a bad number, or a missing `fixtures=`/
/// `activities=` line -- e.g. from a `prepare` invocation that crashed or
/// was killed mid-write) panics rather than being silently treated the same
/// as "missing": collapsing both cases to the same `None` would route a
/// *damaged* sidecar into the same expensive full-fixture-parse fallback
/// this sidecar mechanism exists to avoid inside the profiled `run`-mode
/// region (`activities_per_fixture_from_disk`'s doc comment measures that
/// fallback at ~3.9M instructions / ~3% of the isolated total) -- silently,
/// with no diagnostic that the sidecar was ever damaged in the first place.
fn read_prepared_meta(dir: &std::path::Path) -> Option<(usize, usize)> {
    let path = dir.join(PROFILE_META_FILENAME);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
        Err(e) => panic!("failed to read {}: {e}", path.display()),
    };
    let mut fixtures = None;
    let mut activities = None;
    for line in text.lines() {
        if let Some(value) = line.strip_prefix("fixtures=") {
            fixtures = Some(value.trim().parse::<usize>().unwrap_or_else(|e| {
                panic!("malformed `fixtures=` line in {}: {e}", path.display())
            }));
        } else if let Some(value) = line.strip_prefix("activities=") {
            activities = Some(value.trim().parse::<usize>().unwrap_or_else(|e| {
                panic!("malformed `activities=` line in {}: {e}", path.display())
            }));
        }
    }
    Some((
        fixtures.unwrap_or_else(|| panic!("{} is missing a `fixtures=` line", path.display())),
        activities.unwrap_or_else(|| panic!("{} is missing an `activities=` line", path.display())),
    ))
}

/// Delete any pre-existing `PROFILE_META_FILENAME` sidecar from `dir` before
/// `prepare_fixtures` starts rewriting fixture files -- run first, ahead of
/// `remove_stale_fixtures` and the fixture-write loop below, so the sidecar
/// is gone for the *entire* window during which the fixtures on disk are
/// changing. Without this, a `prepare` invocation interrupted (crash, kill,
/// OOM) anywhere between finishing the fixture-write loop and finishing its
/// own fresh sidecar write leaves the *previous* invocation's sidecar
/// behind: well-formed, but describing a workload shape (e.g. a different
/// `activities` value) that no longer matches the now-rewritten fixtures.
/// `read_prepared_meta`'s malformed-sidecar panic cannot catch this -- a
/// leftover-from-a-previous-run sidecar is not malformed, just stale --  so
/// `run_verify` would trust it and silently report the wrong shape. Deleting
/// it first turns that whole interruption window into the already-supported
/// "sidecar genuinely absent" case (`read_prepared_meta` returns `None`,
/// `run_verify` falls back to `activities_per_fixture_from_disk`, which now
/// derives the true shape from what's actually on disk), instead of a
/// stale-but-well-formed sidecar silently lying about it.
fn remove_stale_meta(dir: &std::path::Path) {
    let path = dir.join(PROFILE_META_FILENAME);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("failed to remove stale {}: {e}", path.display()),
    }
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
/// shrinking `fixtures`/`activities` between runs: `remove_stale_meta` and
/// `remove_stale_fixtures` clear any pre-existing sidecar/`fixture_*.json`
/// files first, so re-running `prepare` against an existing directory always
/// leaves it in exactly the state a single `prepare` call with the current
/// arguments would produce -- and never in a state where a *stale* sidecar
/// survives alongside freshly (or partially) rewritten fixtures.
fn prepare_fixtures(dir: &std::path::Path, fixtures: usize, activities: usize) {
    std::fs::create_dir_all(dir).expect("create fixture dir");
    remove_stale_meta(dir);
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

/// Recursively collect every `*.json` fixture file under `dir`, mirroring
/// `ReplayVerifier::verify_dir`'s own directory-walk semantics (its private
/// `collect_json_files` helper -- not a `pub` item this external bench
/// binary can call, so this mirrors rather than reuses it) so a nested
/// layout (e.g. `suite/fixture.json`) that `verify_dir` itself accepts is
/// not silently invisible to this fallback. Also excludes the same
/// replay-drift-bundle manifest filename `verify_dir` excludes, so pointing
/// this fallback at a `replay_bundle`-produced directory (issue #798) does
/// not misparse the manifest file as a fixture.
fn collect_json_fixtures(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut dirs_to_visit = vec![dir.to_path_buf()];
    while let Some(current_dir) = dirs_to_visit.pop() {
        let entries = std::fs::read_dir(&current_dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", current_dir.display()));
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                dirs_to_visit.push(path);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("json")
                && path.file_name().and_then(|name| name.to_str())
                    != Some(SampleManifest::FILE_NAME)
            {
                files.push(path);
            }
        }
    }
    files
}

/// Fallback for `read_prepared_meta` returning `None`: parse every `*.json`
/// fixture found under `dir` (recursively, via `collect_json_fixtures`
/// above -- matching `verify_dir`'s own discovery, so a nested fixture
/// layout is not missed here), derive each one's per-fixture activity count
/// by counting its `WorkflowEvent::ActivityScheduled` events (validating
/// that every fixture agrees), and sum every fixture's *actual* recorded
/// event count along the way.
///
/// Returns `(activities_per_fixture, total_events_across_all_fixtures)`.
/// The second field exists because `run_verify`'s reported
/// `total_events_verified` cannot be reconstructed from
/// `activities_per_fixture` alone here the way it can on the sidecar-present
/// path: a `prepare`-populated fixture is *guaranteed* to be exactly
/// `build_fixture_json`'s synthetic `WorkflowStarted` + two events per
/// activity shape, so `fixtures * (activities * 2 + 1)` is exact there --
/// but a hand-populated directory reaching this fallback carries no such
/// guarantee (retried activities' extra `ActivityStarted` events, timers,
/// signals, or any other event `verify_dir` itself accepts can all inflate
/// a fixture's real event count well past that synthetic formula), so the
/// only way to report a truthful total is to sum what was actually parsed.
/// This reuses the parse this function already pays for the heterogeneity
/// check -- no extra pass over the fixtures is needed for it.
///
/// Counts `ActivityScheduled` directly rather than inferring a count from
/// the total event length -- `(events.len() - 1) / 2` only holds for this
/// file's own synthetic shape (`build_fixture_json`'s exact
/// `WorkflowStarted` + one `ActivityScheduled`/`ActivityCompleted` pair per
/// activity), which this fallback's own doc comment below says is *not*
/// what reaches it. A real worker history instead emits one `ActivityStarted`
/// per attempt: a *retryable* failure appends no event at all (`
/// queue::requeue_for_retry` only updates the `harvest_task_queue` row --
/// see its doc comment and `WorkflowTestEnv::mock_activity_retries`'s at
/// `src/testing.rs:5278-5286`); the activity's only other history event is
/// its single terminal `ActivityCompleted` or `ActivityFailed`. So a single
/// activity retried a few times still inflates the raw event count -- via
/// the repeated `ActivityStarted`s, which `HistoryMatcher::scan_activity_terminal`
/// (`src/replay.rs`) skips as transparent noise while scanning for that one
/// terminal event -- well past two per activity; halving it would badly
/// overcount that fixture's activities, and two fixtures with the *same*
/// logical activity count but *different* retry counts would (wrongly)
/// disagree under the halved formula and trip the heterogeneity panic below
/// on otherwise-valid, uniform input. `ActivityScheduled` is recorded
/// exactly once per logical activity dispatch regardless of how many
/// attempts it took, so counting it is robust to any event shape
/// `verify_dir` itself accepts.
///
/// A directory populated by `prepare_fixtures` writes identical fixture
/// copies -- but it also *always* writes the `PROFILE_META_FILENAME`
/// sidecar this fallback exists to substitute for, so that path never
/// actually reaches this function. The only directories that do are
/// hand-populated (no sidecar at all), where nothing guarantees every
/// fixture shares one activity count: such a directory can legitimately mix
/// e.g. 500- and 1,000-activity snapshots that each verify successfully on
/// their own. `run_verify` reports the returned activity count across the
/// *entire* fixture set, so reporting only one sampled fixture's count would
/// silently mislabel every fixture that disagrees with it. Every fixture
/// found is therefore parsed and checked to agree with the first; a
/// heterogeneous set panics naming the two disagreeing files rather than
/// reporting one arbitrary shape as if it applied to all of them.
///
/// Full JSON parse of every fixture is real, non-trivial cost (see
/// `PROFILE_META_FILENAME`'s doc comment: ~3.9M instructions for *one*
/// realistic-size fixture) -- acceptable here because, as above, this whole
/// function is already documented as unreachable from a normal two-phase
/// `prepare`/`run` invocation (which always carries the sidecar); it is not
/// the golden profiled path this harness exists to keep clean.
fn activities_per_fixture_from_disk(dir: &std::path::Path) -> (usize, usize) {
    let mut fixture_paths = collect_json_fixtures(dir);
    fixture_paths.sort();
    assert!(
        !fixture_paths.is_empty(),
        "at least one *.json fixture file (searched recursively) in the profile dir {}",
        dir.display()
    );
    let mut first: Option<(std::path::PathBuf, usize)> = None;
    let mut total_events = 0usize;
    for path in &fixture_paths {
        let json = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read fixture {}: {e}", path.display()));
        let snapshot: HistorySnapshot = serde_json::from_str(&json)
            .unwrap_or_else(|e| panic!("parse fixture {}: {e}", path.display()));
        // The fixture's real, actually-recorded event count -- never a
        // formula derived from `activities_per_fixture`, since a fixture
        // reaching this fallback is not guaranteed to have the synthetic
        // "WorkflowStarted + 2 events per activity" shape (see the doc
        // comment above).
        total_events += snapshot.events.len();
        // One `ActivityScheduled` per logical activity dispatch, regardless
        // of how many `ActivityStarted`/`ActivityFailed` retry-attempt
        // events surround it -- see the doc comment above for why this must
        // not be inferred from the raw event count instead.
        let count = snapshot
            .events
            .iter()
            .filter(|event| matches!(event, WorkflowEvent::ActivityScheduled { .. }))
            .count();
        match &first {
            None => first = Some((path.clone(), count)),
            Some((first_path, first_count)) => {
                assert_eq!(
                    count,
                    *first_count,
                    "heterogeneous fixture activity counts under {}: {} has {first_count} \
                     activities but {} has {count}; this fallback (used only when no \
                     `{PROFILE_META_FILENAME}` sidecar is present) requires every fixture to \
                     share one activity count so a single workload shape can be reported",
                    dir.display(),
                    first_path.display(),
                    path.display()
                );
            }
        }
    }
    (first.expect("checked non-empty above").1, total_events)
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
    //
    // The two paths disagree on how `total_events_verified` below must be
    // derived, so both `actual_activities` and (on the fallback path only)
    // the fixtures' real summed event count travel together:
    // sidecar-present -> `None` here; every `prepare`-written fixture is
    // *guaranteed* to be `build_fixture_json`'s exact synthetic
    // `WorkflowStarted` + 2-events-per-activity shape, so the cheap
    // `fixtures * (actual_activities * 2 + 1)` formula below is exact, and
    // computing it costs nothing beyond the sidecar read already required.
    // sidecar-absent -> `Some(sum of every parsed fixture's real event
    // count)`; a hand-populated fixture reaching this fallback carries no
    // such shape guarantee (retried activities' extra `ActivityStarted`
    // events, timers, signals, or any other event `verify_dir` itself
    // accepts can all inflate a fixture's true event count past that
    // synthetic formula), so the true total must be read from what was
    // actually parsed rather than reconstructed from `actual_activities`
    // alone -- see `activities_per_fixture_from_disk`'s doc comment.
    let (actual_activities, actual_total_events) =
        if let Some((_prepared_fixtures, prepared_activities)) = read_prepared_meta(dir) {
            (prepared_activities, None)
        } else {
            let (activities, total_events) = activities_per_fixture_from_disk(dir);
            (activities, Some(total_events))
        };
    if actual_activities != activities {
        eprintln!(
            "verify_profile: WARNING -- VERIFY_PROFILE_ACTIVITIES={activities} does not \
             match the {actual_activities} activities/fixture actually found on disk in \
             {} (prepare/run env drift?); the line below reports the on-disk value.",
            dir.display()
        );
    }

    let total_events_verified =
        actual_total_events.unwrap_or(fixtures * (actual_activities * 2 + 1));

    println!(
        "verify_profile: fixtures={fixtures} activities_per_fixture={actual_activities} \
         total_events_verified={total_events_verified} succeeded={}",
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
