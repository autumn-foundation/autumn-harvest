//! Non-criterion instruction/allocation-count profiling harness for
//! `autumn_harvest_verify::verify` -- the MIR parse + call-graph resolution +
//! taint-analysis pipeline behind `cargo harvest-verify` (issue #962). This is
//! the crate's own real public entry point: the exact function
//! `cargo-harvest-verify` calls, not a hand-picked internal helper.
//!
//! Wall-clock timing is not admissible evidence on this (shared-vCPU) machine.
//! This binary is driven directly under `valgrind --tool=callgrind`
//! (instruction counts) and `valgrind --tool=dhat` (allocation counts/bytes)
//! instead, mirroring `autumn-harvest`'s own `verify_profile.rs` /
//! `timeline_profile.rs` two-phase convention.
//!
//! # Workload
//!
//! The realistic workload this crate's own CI gate names is `harvest-verify
//! -p autumn-harvest --all-examples`. See `.github/workflows/ci.yml`'s
//! `harvest-verify` job, documented at
//! `docs/rnd/determinism-static-analysis.md`'s "Row 6": 43 example targets,
//! 57 `#[workflow]` functions, a warm-cache gate measured at 16.9s wall
//! clock. This harness reproduces exactly that shape rather than a
//! synthetic MIR fixture invented to flatter a particular change.
//!
//! `pipeline::run` (via the public `verify` entry point) does two things
//! that must not be conflated. First, it asks cargo to build the target and
//! emit MIR: a `rustc` subprocess, real work, but not this crate's own
//! code. That step is wildly non-deterministic under callgrind, since it
//! depends on the installed toolchain and cargo's own cache state. Second,
//! it parses, resolves and analyzes that MIR -- this crate's entire reason
//! to exist. Only the second half is admissible evidence for a change to
//! *this* crate.
//!
//! # Two-phase mode (`prepare` / `run`)
//!
//! `ANALYZE_PROFILE_MODE=prepare` runs the cargo/rustc emission step once,
//! unprofiled, and leaves the `.mir` files on disk under
//! `ANALYZE_PROFILE_MIR_DIR`. `ANALYZE_PROFILE_MODE=run` then skips cargo
//! entirely: `Options.mir_paths` points `verify` at the pre-emitted
//! directory, so `pipeline::run` takes the `--mir`-only branch and never
//! spawns cargo. It then calls `verify` `ANALYZE_PROFILE_REPS` times,
//! mirroring a long-lived CI runner re-analyzing a fixed MIR shape
//! repeatedly across separate gate runs.
//!
//! ```text
//! export ANALYZE_PROFILE_MIR_DIR=/tmp/analyze-profile-mir
//! rm -rf "$ANALYZE_PROFILE_MIR_DIR"
//!
//! # Unprofiled setup -- builds autumn-harvest's examples and emits MIR:
//! BIN=$(cargo bench -p autumn-harvest-verify --bench analyze_profile \
//!   --no-run --message-format=json 2>/dev/null \
//!   | jq -r 'select(.executable != null) | .executable')
//! ANALYZE_PROFILE_MODE=prepare "$BIN"
//!
//! # Profiled measurement -- ONLY parse + resolve + analyze, no cargo/rustc:
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no \
//!   --callgrind-out-file=cg.out \
//!   env ANALYZE_PROFILE_MODE=run "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json env ANALYZE_PROFILE_MODE=run "$BIN"
//! ```
//!
//! `ANALYZE_PROFILE_MODE` (default `full`, when unset) selects the mode.
//! `prepare` emits MIR only. `run` analyzes a pre-populated
//! `ANALYZE_PROFILE_MIR_DIR` only, with no cargo invocation. The default,
//! `full`, emits and analyzes once, in one process. Both `prepare` and
//! `run` require `ANALYZE_PROFILE_MIR_DIR` to name the same path. `full` is
//! a convenience smoke-test mode, not the mode to point a profiler at. Any
//! other value panics rather than silently falling back to `full`.
//!
//! `ANALYZE_PROFILE_REPS` (default `20`) sets how many times the fixed,
//! pre-emitted MIR is analyzed in `run` mode.

use std::path::PathBuf;

use autumn_harvest_verify::driver::{self, BuildRequest};
use autumn_harvest_verify::{Options, verify};

fn env_usize(key: &str, default: usize) -> usize {
    match std::env::var(key) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("{key}={raw:?} is not a valid usize: {e}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            panic!("{key}={} is not valid Unicode", raw.to_string_lossy())
        }
    }
}

fn mir_dir() -> PathBuf {
    std::env::var_os("ANALYZE_PROFILE_MIR_DIR").map_or_else(
        || std::env::temp_dir().join("analyze-profile-mir"),
        PathBuf::from,
    )
}

/// The exact real-crate shape `docs/rnd/determinism-static-analysis.md`'s
/// "Row 6" names as this tool's headline CI gate.
fn emit_build(target_dir: PathBuf) -> BuildRequest {
    BuildRequest {
        packages: vec!["autumn-harvest".to_string()],
        all_examples: true,
        no_default_features: true,
        features: vec!["testing".to_string()],
        target_dir: Some(target_dir),
        ..Default::default()
    }
}

fn prepare() {
    let dir = mir_dir();
    let build = emit_build(dir.clone());
    let (emitted, warnings) = driver::emit_mir_with_warnings(&build)
        .unwrap_or_else(|e| panic!("MIR emission failed: {e}"));
    for w in &warnings {
        eprintln!("warning: {w}");
    }
    eprintln!(
        "prepared {} .mir file(s) under {}",
        emitted.len(),
        dir.display()
    );
    assert!(!emitted.is_empty(), "emission produced no .mir files");
}

fn run() {
    let dir = mir_dir();
    assert!(
        dir.is_dir(),
        "{} does not exist -- run ANALYZE_PROFILE_MODE=prepare first",
        dir.display()
    );
    let reps = env_usize("ANALYZE_PROFILE_REPS", 20);
    let build = BuildRequest::default();
    let opts = Options {
        mir_paths: vec![dir],
        ..Default::default()
    };

    let mut total_workflows = 0usize;
    for _ in 0..reps {
        let report = verify(&build, &opts).unwrap_or_else(|e| panic!("verify() failed: {e}"));
        // A stale or emptied ANALYZE_PROFILE_MIR_DIR does not error here.
        // `verify` just reports zero workflows, so a profile run against it
        // would silently measure nothing instead of the intended workload.
        assert!(
            !report.discovery_failed && !report.workflows.is_empty(),
            "analyzed 0 workflows -- ANALYZE_PROFILE_MIR_DIR does not hold the \
             expected fixture; re-run ANALYZE_PROFILE_MODE=prepare"
        );
        total_workflows = std::hint::black_box(report.workflows.len());
    }
    eprintln!("analyzed {total_workflows} workflow(s) per rep, {reps} rep(s)");
}

fn full() {
    let dir = mir_dir();
    let build = emit_build(dir.clone());
    driver::emit_mir_with_warnings(&build).unwrap_or_else(|e| panic!("MIR emission failed: {e}"));
    let opts = Options {
        mir_paths: vec![dir],
        ..Default::default()
    };
    let report =
        verify(&BuildRequest::default(), &opts).unwrap_or_else(|e| panic!("verify() failed: {e}"));
    eprintln!("analyzed {} workflow(s)", report.workflows.len());
}

fn main() {
    let mode = std::env::var("ANALYZE_PROFILE_MODE").unwrap_or_else(|_| "full".to_string());
    match mode.as_str() {
        "prepare" => prepare(),
        "run" => run(),
        "full" => full(),
        other => panic!("ANALYZE_PROFILE_MODE={other:?} is not one of prepare/run/full"),
    }
}
