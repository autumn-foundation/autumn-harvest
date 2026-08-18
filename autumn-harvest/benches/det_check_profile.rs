//! Deterministic (non-criterion) harness for profiling `det_check::check_paths`
//! -- the source-scanning engine behind `harvest det-check` (issue #778), the
//! CI governance gate for the determinism guardrails.
//!
//! Wall-clock timing is unreliable on this (shared-vCPU) machine, so this
//! binary is driven directly under `valgrind --tool=callgrind` (instruction
//! counts) and `valgrind --tool=dhat` (allocation counts/bytes), which are
//! deterministic across runs.
//!
//! # Running
//!
//! ```text
//! BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench det_check_profile --no-run --message-format=json \
//!   | jq -r 'select(.executable != null) | .executable')
//! valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
//! callgrind_annotate --threshold=98 cg.out
//! valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
//! ```
//!
//! `DET_CHECK_PROFILE_DIR` (default: this crate's own `src/`, i.e. the exact
//! "self-scan" workload issue #778 already documents as clean) selects the
//! directory to scan. `DET_CHECK_PROFILE_REPS` (default 1) repeats the whole
//! `check_paths` call.

use std::path::{Path, PathBuf};

use autumn_harvest::det_check::check_paths;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let dir: PathBuf = std::env::var("DET_CHECK_PROFILE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("src"));
    let reps = env_usize("DET_CHECK_PROFILE_REPS", 1);

    let mut total_findings = 0usize;
    let mut total_suppressions = 0usize;
    for _ in 0..reps {
        let paths = [dir.as_path()];
        let report = check_paths(&paths).expect("check_paths must succeed on a real source tree");
        total_findings += report.findings.len();
        total_suppressions += report.suppressions.len();
        std::hint::black_box(&report);
    }

    println!(
        "det_check_profile: dir={} reps={reps} total_findings={total_findings} total_suppressions={total_suppressions}",
        dir.display()
    );
}
