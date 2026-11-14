//! Deterministic harness for profiling the batch fixture-verification cost of
//! `ReplayVerifier`.
//!
//! The workload is the issue #251 budget. 1,000 fixtures that average 1k events
//! each must verify in under 30 seconds on a 4-core laptop, with in-memory user
//! code and no DB.
//!
//! `replay_profile.rs` builds `Vec<WorkflowEvent>` in memory and calls
//! `WorkflowReplayer::replay_from_events`. `ReplayVerifier::verify_dir` crosses a
//! boundary that no other profiling harness here touches. It does real
//! filesystem I/O (`std::fs::read_to_string` over a directory walk). It
//! deserializes `HistorySnapshot`/`WorkflowEvent` from a string with
//! `serde_json::from_str`. A production worker crosses the same boundary each
//! time it loads a recorded history from `harvest_events`.
//!
//! The fixture shape mirrors `replay_verifier_bench.rs`: `Value::Null` payloads
//! and `activity_count` activities per fixture. So this harness measures the
//! documented issue #251 workload, not a shape that flatters a change. Only the
//! default *fixture count* is smaller (see `VERIFY_PROFILE_FIXTURES` below), to
//! keep one valgrind run tractable. Callgrind emulation is one to two orders of
//! magnitude slower than native. The 1,000 fixtures of issue #251 target a
//! 30-second *native* budget. Fixture count and total instructions scale
//! linearly (see `docs/performance-verify.md`), so a reduced run is
//! representative. Set `VERIFY_PROFILE_FIXTURES=1000` for the exact documented
//! shape, given enough wall-clock headroom.
//!
//! Wall-clock timing is unreliable on this shared-vCPU machine, so this binary
//! does not use `cargo bench` / criterion timing. It runs under
//! `valgrind --tool=callgrind` (instruction counts) and `valgrind --tool=dhat`
//! (allocation counts/bytes), which are deterministic across runs. See
//! `docs/performance-verify.md` for the numbers and how to reproduce them.
//!
//! `harness = false`, own `main()`, the same shape as `replay_profile.rs`. A
//! profiler can point at the plain executable, with no criterion wall-clock
//! loop to dilute the measured work.
//!
//! # Two-phase mode (`prepare` / `run`) -- the profiling-correct entry point
//!
//! The `verify_dir` runtime spawns one `tokio::task` per fixture and drives them
//! on a multi-threaded runtime. Under callgrind, a worker thread's poll of a
//! spawned task is **not** a call-graph descendant of the `block_on` frame on
//! the *spawning* thread. The flat/self-cost view
//! (`callgrind_annotate`) still sums it across all threads. But the inclusive
//! cost of the `block_on` call site undercounts, so it is no proxy for the
//! share of `verify_dir`. Use the flat totals of `callgrind_annotate`, as
//! `docs/performance-verify.md` does.
//!
//! Fixture generation (`build_fixture_json` + `serde_json::to_string` + N
//! `std::fs::write` calls) is real cost unrelated to `ReplayVerifier`. A
//! `verify_dir` measurement must exclude it. `VERIFY_PROFILE_MODE=prepare` runs
//! *only* that step, with no tokio runtime and no `ReplayVerifier`. It writes to
//! the directory that `VERIFY_PROFILE_DIR` names, so it can run **unprofiled**.
//! `VERIFY_PROFILE_MODE=run` does the reverse, with no fixture generation. It
//! builds a tokio runtime and calls `verify_dir` on the pre-populated
//! `VERIFY_PROFILE_DIR`. The runtime build (~60K instructions) is negligible
//! (see the reconciliation section of `docs/performance-verify.md`). Point a
//! profiler at the `run`-mode invocation only:
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
//!
//! - `prepare`: write fixtures only, with no runtime.
//! - `run`: verify a pre-populated directory only, with no fixture generation.
//!   `prepare` and `run` both need `VERIFY_PROFILE_DIR` set to the same path.
//! - `full`: build, write fixtures to a fresh tempdir and verify, in one
//!   process. It is a convenience mode for a quick smoke run. It also measures
//!   fixture generation, so do NOT profile it to isolate `verify_dir`.
//!
//! Any other value panics and never falls back to `full`. A typo such as `ru`
//! for `run` must not produce a plausible but invalid measurement.
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
//! written and verified. `VERIFY_PROFILE_ACTIVITIES` (default `500`) sets the
//! activities per fixture. `500` matches `bench_1000_fixtures` in
//! `replay_verifier_bench.rs`: `1_001` events per fixture, the exact issue #251
//! shape. Both env vars must match between a `prepare` run and its paired `run`.

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
        start_provenance: None,
        parent_execution_id: None,
        workflow_id: None,
        queue_name: None,
    };
    serde_json::to_string(&snapshot).unwrap()
}

/// Read `key` as a `usize`. An *absent* variable uses `default`. A
/// *present but unparseable* value panics and does not fall back to
/// `default`. A typo like `VERIFY_PROFILE_FIXTURES=2O` (letter O) or
/// `VERIFY_PROFILE_ACTIVITIES=1000x` must not silently select a different
/// workload. That gives a plausible-looking but methodologically invalid
/// measurement. `VERIFY_PROFILE_MODE`'s dispatch below applies the same
/// principle to a malformed mode string.
///
/// This runs at the very top of `main()`, before the `prepare`/`run` mode
/// split. So it executes in the profiled `run`-mode process too. The
/// success-path work (one env lookup, one `parse()` call) is byte-for-byte
/// what the previous `.ok().and_then(...).unwrap_or(default)` chain did.
/// Only the error-handling branches differ. Those do not execute for a
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

/// Filename of the small sidecar that `prepare_fixtures` writes beside the
/// fixtures.
///
/// It records the `fixtures`/`activities` values used, as `key=value` lines.
/// It is deliberately not JSON. `verify_dir` replays each `*.json` file in the
/// directory, so a stray non-fixture `.json` file fails as a spurious harness
/// error. `run` mode reads it with `read_prepared_meta` to get the ground-truth
/// workload shape *cheaply*. The alternative is the full-fixture parse of
/// `activities_per_fixture_from_disk`, whose doc says why a ground-truth check
/// is needed at all. That parse costs ~3.9M instructions, ~3% of the isolated
/// `verify_dir` total (see `docs/performance-verify.md`). The two-phase split
/// exists to keep such unrelated cost out of the profiled `run` region. The
/// sidecar read costs a few thousand instructions, not millions.
const PROFILE_META_FILENAME: &str = "verify_profile_meta.txt";

/// Read the `fixtures`/`activities` values that `prepare_fixtures` persisted to
/// `PROFILE_META_FILENAME`.
///
/// Returns `None` only when the sidecar is *missing*, because the directory was
/// not populated by `VERIFY_PROFILE_MODE=prepare`. The caller then falls back
/// to `activities_per_fixture_from_disk`. A sidecar that exists but is
/// malformed or truncated panics. Examples are a bad number, or a missing
/// `fixtures=`/`activities=` line from a `prepare` that crashed mid-write. A
/// `None` there would silently route a *damaged* sidecar into the expensive
/// full-fixture-parse fallback (~3.9M instructions, ~3% of the isolated total).
/// That cost lands in the profiled `run` region, with no diagnostic.
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
/// `prepare_fixtures` starts rewriting fixture files. This runs first, ahead
/// of `remove_stale_fixtures` and the fixture-write loop below. So the sidecar
/// is gone for the *entire* window in which the fixtures on disk change.
///
/// Without this, an interrupted `prepare` invocation (crash, kill, OOM) can
/// leave the *previous* invocation's sidecar behind. The window runs from the
/// end of the fixture-write loop to the end of the fresh sidecar write. The
/// old sidecar is well-formed, but it describes a workload shape (e.g. a
/// different `activities` value) that no longer matches the rewritten
/// fixtures. `read_prepared_meta`'s malformed-sidecar panic cannot catch this.
/// A leftover sidecar is not malformed, just stale. So `run_verify` would
/// trust it and silently report the wrong shape.
///
/// Deleting it first turns that whole interruption window into the supported
/// "sidecar genuinely absent" case. `read_prepared_meta` returns `None`, and
/// `run_verify` falls back to `activities_per_fixture_from_disk`. That
/// function derives the true shape from what is actually on disk. A
/// stale-but-well-formed sidecar cannot then lie about it.
fn remove_stale_meta(dir: &std::path::Path) {
    for path in [
        dir.join(PROFILE_META_FILENAME),
        dir.join(profile_meta_tmp_filename()),
    ] {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("failed to remove stale {}: {e}", path.display()),
        }
    }
}

/// Filename of the temp sidecar that `write_meta_atomically` writes and syncs
/// before it renames it to `PROFILE_META_FILENAME`. `remove_stale_meta` also
/// removes it. So a temp file from a `prepare` that crashed before the rename
/// does not linger into a later `prepare`.
fn profile_meta_tmp_filename() -> String {
    format!("{PROFILE_META_FILENAME}.tmp")
}

/// Write `PROFILE_META_FILENAME` atomically.
///
/// The contents go to a temp file in the same directory, which is `sync_all`'d
/// and then published with `std::fs::rename`. The rename is atomic on one
/// filesystem, and `dir` and the temp file always share one. So
/// `read_prepared_meta` sees the fully-old sidecar or the fully-new one, never
/// a partial write.
///
/// Otherwise an interrupted `prepare` (crash, kill, OOM, or a short write) can
/// leave a truncated prefix that is *syntactically valid but wrong*. For
/// example, `activities=500` truncated to `activities=50` still parses as a
/// `usize`. The malformed-sidecar panic of `read_prepared_meta` cannot catch
/// that, because the content is not malformed. So `run` would silently report
/// the wrong workload shape.
fn write_meta_atomically(dir: &std::path::Path, fixtures: usize, activities: usize) {
    let tmp_path = dir.join(profile_meta_tmp_filename());
    let final_path = dir.join(PROFILE_META_FILENAME);
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_path).expect("create temp profile meta sidecar");
        file.write_all(format!("fixtures={fixtures}\nactivities={activities}\n").as_bytes())
            .expect("write temp profile meta sidecar");
        file.sync_all().expect("sync temp profile meta sidecar");
    }
    std::fs::rename(&tmp_path, &final_path).expect("publish profile meta sidecar");
}

/// Remove any `fixture_*.json` files from `dir` before `prepare_fixtures`
/// writes a fresh set.
///
/// Otherwise a re-run of `prepare` with a *smaller* `VERIFY_PROFILE_FIXTURES`
/// against the same `VERIFY_PROFILE_DIR` leaves higher-numbered files behind.
/// For example, a `fixtures=20` run rewrites only indices 0..19, so
/// `fixture_00020.json`..`fixture_00049.json` from an earlier `fixtures=50` run
/// survive. `verify_dir` globs every `*.json` file, so the stale files join the
/// next `run` measurement. They can have a *different*
/// `VERIFY_PROFILE_ACTIVITIES`, which gives a non-uniform workload. The
/// equality assert on `report.fixtures_total` catches this only when the stale
/// count differs from the new `VERIFY_PROFILE_FIXTURES`. This removes only
/// files with the exact name pattern of `prepare_fixtures`. The sidecar and
/// other files stay. It runs only in `prepare`/`full` mode, so it adds no cost
/// to the profiled region.
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
/// `PROFILE_META_FILENAME` sidecar with the exact values used.
///
/// It is pure filesystem I/O plus one `serde_json::to_string`. It runs
/// **unprofiled** (`VERIFY_PROFILE_MODE=prepare`) ahead of a profiled
/// `VERIFY_PROFILE_MODE=run` pass that measures only
/// `ReplayVerifier::verify_dir`. It is idempotent when `fixtures`/`activities`
/// grow or shrink between runs. `remove_stale_meta` and `remove_stale_fixtures`
/// first clear any old sidecar and `fixture_*.json` files. So a re-run leaves
/// the directory as one `prepare` call with the current arguments would. A
/// *stale* sidecar never survives beside rewritten fixtures.
fn prepare_fixtures(dir: &std::path::Path, fixtures: usize, activities: usize) {
    std::fs::create_dir_all(dir).expect("create fixture dir");
    remove_stale_meta(dir);
    remove_stale_fixtures(dir);
    let fixture_json = build_fixture_json(activities);
    for i in 0..fixtures {
        std::fs::write(dir.join(format!("fixture_{i:05}.json")), &fixture_json)
            .expect("write fixture");
    }
    write_meta_atomically(dir, fixtures, activities);
}

/// A *genuinely absent* `VERIFY_PROFILE_DIR` panics unconditionally.
///
/// A *present but empty-string* value (`VERIFY_PROFILE_DIR=`) is rejected too.
/// That is a common shell misconfiguration: the variable is exported but never
/// assigned. `PathBuf::from("")` constructs without error, and
/// `std::fs::create_dir_all("")` is a silent no-op.
/// `PathBuf::from("").join("fixture_00000.json")` resolves to the bare relative
/// filename `"fixture_00000.json"`. So `prepare`/`run` would silently use the
/// current working directory, with no diagnostic of the misconfiguration.
fn required_profile_dir(mode: &str) -> std::path::PathBuf {
    let value = std::env::var("VERIFY_PROFILE_DIR")
        .unwrap_or_else(|_| panic!("VERIFY_PROFILE_DIR must be set in `{mode}` mode"));
    assert!(
        !value.is_empty(),
        "VERIFY_PROFILE_DIR is set but empty in `{mode}` mode; an empty value resolves to the \
         process's current working directory rather than a real fixture directory (a bare \
         relative filename, not an error, when joined with a fixture name) -- set it to an \
         actual path"
    );
    value.into()
}

/// Recursively collect every `*.json` fixture file under `dir`. This mirrors
/// the directory walk of `ReplayVerifier::verify_dir`. Its private
/// `collect_json_files` helper is not a `pub` item this external bench binary
/// can call, so this mirrors it and does not reuse it. A nested layout (e.g.
/// `suite/fixture.json`) that `verify_dir` accepts is then visible to this
/// fallback too. It also excludes the replay-drift-bundle manifest filename
/// that `verify_dir` excludes. So a `replay_bundle`-produced directory (issue
/// #798) does not make this fallback misparse the manifest as a fixture.
///
/// Classifies each entry via `DirEntry::file_type()`, not `path.is_dir()`.
/// The latter follows symlinks (`Path::is_dir()` is backed by
/// `fs::metadata`). `verify_dir`'s own `collect_json_files`
/// (`src/testing.rs`) classifies via `entry.file_type()`, which does *not*
/// follow symlinks. `path.is_dir()` here would recurse into a directory that
/// only a symlink reaches, which `verify_dir` itself never traverses. This
/// fallback could then report metadata for fixtures that `verify_dir` never
/// measured. It could also spin on a symlink cycle that `verify_dir` skips.
/// `entry.file_type()` makes this traversal match what was verified, entry
/// for entry. An entry whose type cannot be determined (a race, a broken
/// symlink) is silently skipped and does not panic. That matches
/// `collect_json_files`'s own tolerant `if let Ok(file_type) = ...` handling.
fn collect_json_fixtures(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    let mut dirs_to_visit = vec![dir.to_path_buf()];
    while let Some(current_dir) = dirs_to_visit.pop() {
        let entries = std::fs::read_dir(&current_dir)
            .unwrap_or_else(|e| panic!("read {}: {e}", current_dir.display()));
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if let Ok(file_type) = entry.file_type() {
                if file_type.is_dir() {
                    dirs_to_visit.push(path);
                } else if path.extension().and_then(|ext| ext.to_str()) == Some("json")
                    && path.file_name().and_then(|name| name.to_str())
                        != Some(SampleManifest::FILE_NAME)
                {
                    files.push(path);
                }
            }
        }
    }
    files
}

/// Fallback when `read_prepared_meta` returns `None`.
///
/// It parses every `*.json` fixture under `dir`. The walk is recursive, through
/// `collect_json_fixtures`, to match the discovery of `verify_dir`. It counts
/// the `WorkflowEvent::ActivityScheduled` events of each fixture, checks that
/// every fixture agrees, and sums the *actual* recorded event counts.
///
/// Returns `(activities_per_fixture, total_events_across_all_fixtures)`. The
/// second field exists because this path cannot rebuild the reported
/// `total_events_verified` of `run_verify` from `activities_per_fixture`. A
/// `prepare`-written fixture is *guaranteed* to have the synthetic shape of
/// `build_fixture_json`: `WorkflowStarted` plus two events per activity. So
/// `fixtures * (activities * 2 + 1)` is exact on the sidecar path. A
/// hand-populated directory has no such guarantee. Retried activities' extra
/// `ActivityStarted` events, timers, signals, or any other event that
/// `verify_dir` accepts can inflate the real count. So the only truthful total
/// is the sum of what was parsed. It reuses the parse of the heterogeneity
/// check, with no extra pass.
///
/// It counts `ActivityScheduled` and does not infer a count from the event
/// length. `(events.len() - 1) / 2` holds only for the synthetic shape of
/// `build_fixture_json`. As the next paragraph says, that shape never reaches
/// this fallback. A real worker history emits one `ActivityStarted` per attempt.
/// A *retryable* failure appends no event at all: `queue::requeue_for_retry`
/// updates only the `harvest_task_queue` row. See its doc comment, and
/// `WorkflowTestEnv::mock_activity_retries` at `src/testing.rs:5278-5286`. The
/// only other history event of the activity is its single terminal
/// `ActivityCompleted` or `ActivityFailed`. So retries inflate the raw count
/// with repeated `ActivityStarted` events, well past two per activity.
/// `HistoryMatcher::scan_activity_terminal` (`src/replay.rs`) skips them as
/// noise while it scans for the terminal event. Halving would overcount that
/// fixture's activities. Two fixtures with the *same* activity count but
/// *different* retry counts would then disagree. That trips the heterogeneity
/// panic below on valid, uniform input. `ActivityScheduled` is recorded once
/// per logical dispatch, however many attempts it took. So counting it is
/// robust to any event shape that `verify_dir` accepts.
///
/// `prepare_fixtures` writes identical fixture copies. But it *always* writes
/// the `PROFILE_META_FILENAME` sidecar too, so that path never reaches this
/// function. Only hand-populated directories do, and there nothing guarantees
/// one activity count. Such a directory can mix 500- and 1,000-activity
/// snapshots that each verify on their own. `run_verify` reports the returned
/// count for the *entire* fixture set. One sampled fixture's count would
/// silently mislabel each fixture that disagrees with it. So each fixture is
/// parsed and checked against the first. A heterogeneous set panics and names
/// the two disagreeing files.
///
/// A full JSON parse of each fixture is real cost: ~3.9M instructions for *one*
/// realistic fixture (see the `PROFILE_META_FILENAME` doc). That is acceptable,
/// because a normal two-phase `prepare`/`run` invocation always has the sidecar
/// and never reaches this function. It is not the profiled path this harness
/// keeps clean.
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
        // The real recorded event count, never a formula from
        // `activities_per_fixture`. A fixture on this fallback path need not
        // have the synthetic "WorkflowStarted + 2 events per activity" shape
        // (see the doc comment above).
        total_events += snapshot.events.len();
        // One `ActivityScheduled` per logical activity dispatch, however many
        // `ActivityStarted`/`ActivityFailed` retry events surround it. The doc
        // comment above says why the raw event count must not be used.
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

/// Build the tokio runtime and call `verify_dir` on `dir`.
///
/// This is the ONLY work of `VERIFY_PROFILE_MODE=run`, with no fixture
/// generation and no directory creation. So point a profiler at it to isolate
/// the cost of `verify_dir`. See the module doc comment.
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

    // Derive the reported workload shape from what `prepare` wrote. The
    // `PROFILE_META_FILENAME` doc says why this must not trust the
    // `activities` env-var argument. It also says why the cheap sidecar read
    // beats the full-fixture-parse fallback inside this profiled region.
    //
    // The two paths derive `total_events_verified` below differently. So
    // `actual_activities` travels with the real summed event count, which
    // only the fallback path has:
    // - Sidecar present: `None`. Each `prepare`-written fixture is
    //   *guaranteed* to have the synthetic shape of `build_fixture_json`. So
    //   `fixtures * (actual_activities * 2 + 1)` below is exact and costs
    //   nothing beyond the sidecar read.
    // - Sidecar absent: `Some(sum of each parsed fixture's real event count)`.
    //   A hand-populated fixture has no shape guarantee. Retries, timers,
    //   signals or any other accepted event can inflate its count. So the
    //   true total comes from the parse (see the doc comment of
    //   `activities_per_fixture_from_disk`).
    let (actual_activities, actual_total_events) =
        if let Some((_prepared_fixtures, prepared_activities)) = read_prepared_meta(dir) {
            (prepared_activities, None)
        } else {
            let (activities, total_events) = activities_per_fixture_from_disk(dir);
            (activities, Some(total_events))
        };
    // `actual_activities` is the authoritative value, not the `activities`
    // env-var argument above. It comes from what is *actually on disk*: the
    // sidecar that `prepare` wrote, or, on the no-sidecar fallback path, the
    // fixtures parsed there. That is the exact workload `verify_dir` profiles
    // on the next line.
    //
    // The `activities > 0` assert in `main()` only rejects a misconfigured env
    // var of the *current invocation*. It cannot see that this `run`
    // invocation's `VERIFY_PROFILE_DIR` was populated separately. Examples
    // are a directory that an older binary `prepare`d before that assert, one
    // reused across sessions without a new `prepare`, or a hand-populated one.
    // Such a directory may hold zero-activity fixtures, whatever the current
    // `VERIFY_PROFILE_ACTIVITIES` says.
    //
    // Without this check, such a directory falls into the mismatch warning
    // just below. A mismatch between a *positive* actual count and the
    // requested one is intentionally only a warning (see that branch). The
    // run then silently profiles the same degenerate workload, one
    // `WorkflowStarted` event per fixture. The `VERIFY_PROFILE_ACTIVITIES=0`
    // check in `main()` exists to reject that workload. The run prints a
    // plausible `activities_per_fixture=0 ... succeeded=N` line and exits 0.
    assert!(
        actual_activities > 0,
        "the fixtures actually found in {} have 0 activities each -- \
         VERIFY_PROFILE_ACTIVITIES={activities} for this invocation only governs a fresh \
         `prepare` run, not fixtures already on disk, so it cannot retroactively fix up a stale \
         or hand-populated directory; `verify_dir` would profile the same degenerate, \
         near-zero-cost workload the `activities > 0` check in main() exists to reject -- \
         re-run `VERIFY_PROFILE_MODE=prepare` against this directory, or point \
         VERIFY_PROFILE_DIR at one that was",
        dir.display()
    );
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

/// Read `VERIFY_PROFILE_MODE`, and default an *absent* variable to `"full"`.
///
/// A *non-UTF-8* value panics and does not fold into the "absent" default. The
/// `Err` of `std::env::var` covers both `VarError::NotPresent` and
/// `VarError::NotUnicode`. `.unwrap_or_else(|_| "full")` on both would let a
/// non-UTF-8 value run the un-isolated `full` mode. That value can come from a
/// launcher that passes a raw, invalid-UTF-8 `OsString` on Unix. The caller
/// then believes it runs an isolated, profiled `run` mode. The other two
/// workload-shape checks guard against the same failure: `env_usize` and the
/// `match mode.as_str()` catch-all below. This mirrors the
/// `NotPresent`/`NotUnicode` split of `env_usize`.
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
    // `fixtures == 0` would silently "succeed" instead of signalling a
    // misconfiguration. `prepare_fixtures` writes zero `fixture_*.json` files,
    // and still writes a sidecar that claims `fixtures=0`.
    // `ReplayVerifier::replay_dir` deliberately reports an empty fixture
    // directory as a successful *empty* report, not an error. See the
    // `if files.is_empty()` branch in `replay_dir`, `src/testing.rs`. So
    // `run`/`full` mode would print a plausible `fixtures=0 ... succeeded=0`
    // line and exit 0. Callgrind/DHAT would record only runtime-build and
    // harness overhead, none of the `verify_dir` workload. The check runs
    // before the `prepare`/`run`/`full` dispatch below, so it covers all three
    // modes. It turns a vacuous "measurement" into a loud configuration error.
    assert!(
        fixtures > 0,
        "VERIFY_PROFILE_FIXTURES=0 verifies nothing: `verify_dir` reports a trivially \
         successful empty batch for a fixture directory with zero `*.json` files, so this \
         would silently produce a plausible-looking but methodologically invalid measurement \
         instead of exercising the documented verification workload -- set it to a positive \
         value (the default is 20)"
    );
    // `activities == 0` is a distinct but analogous methodological trap.
    // Unlike `fixtures == 0`, `verify_dir` does real work here. It still
    // spawns one task per fixture, reads and parses each `*.json` file, and
    // replays each one. So this is not a literal empty-batch no-op.
    //
    // But `build_fixture_json(0)` produces a fixture with only a single
    // `WorkflowStarted` event (the `for i in 0..activity_count` loop above
    // runs zero times). So every fixture collapses to the cheapest possible
    // replay. There is no `ActivityScheduled`/`ActivityCompleted` pair and no
    // `HistoryMatcher` event-by-event matching. There is only one trivial
    // deserialize and an immediate `Ok` return. That skips the
    // event-processing/replay-matching cost this harness exists to measure.
    // The module doc names issue #251's "1k events each" workload. The doc
    // of `VERIFY_PROFILE_ACTIVITIES` says "500 activities = 1,001 events per
    // fixture, issue #251's exact per-fixture shape".
    //
    // An accidental zero still prints a plausible, honestly-labeled
    // `activities_per_fixture=0` line. But fixed per-fixture overhead (task
    // spawn, file I/O, a near-empty JSON parse) dominates the resulting Ir
    // count. A change under review is meant to move the per-event cost. So
    // this case can give a misleading "5% floor" verdict, like the
    // `fixtures == 0` case above.
    assert!(
        activities > 0,
        "VERIFY_PROFILE_ACTIVITIES=0 bypasses the event-heavy workload this harness exists to \
         measure: every generated fixture would contain only a single `WorkflowStarted` event \
         (no activities to schedule or complete), so `verify_dir` would still run but the \
         measurement would be dominated by fixed per-fixture overhead rather than the \
         per-event replay cost issue #251's workload is defined by -- set it to a positive \
         value (the default is 500)"
    );

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
