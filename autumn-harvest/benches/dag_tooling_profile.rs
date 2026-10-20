//! Non-criterion instruction/allocation-count harness for the static DAG
//! tooling authors run in CI: `DagLinter::analyze`, `DagProfiler::profile`
//! and `export_chrome_trace`.
//!
//! Wall-clock timing is unreliable on the shared vCPU, so run the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A layered DAG of `DAG_TOOL_LAYERS` layers (default 40) by `DAG_TOOL_WIDTH`
//! tasks (default 50, at most 50). Each task has a distinct activity name, as a
//! real pipeline does, and each task past layer 0 depends on three tasks of the
//! previous layer. Only half the tasks carry a retry policy, so the linter
//! rules produce warnings. `DAG_TOOL_REPS` (default 1) repeats the three
//! stages.
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench dag_tooling_profile --no-run
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
//! callgrind_annotate --inclusive=yes cg.out
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Duration;

use autumn_harvest::dag::{DagBuilder, DagDefinition, DagTaskRef};
use autumn_harvest::dag_linter::{
    DagLinter, DuplicateNodeNameRule, ExcessiveParallelismRule, MissingRetryPolicyRule,
    MissingTimeoutRule,
};
use autumn_harvest::dag_profiler::DagProfiler;
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::trace_export::export_chrome_trace;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn a0() {}
fn a1() {}
fn a2() {}
fn a3() {}
fn a4() {}
fn a5() {}
fn a6() {}
fn a7() {}
fn a8() {}
fn a9() {}
fn a10() {}
fn a11() {}
fn a12() {}
fn a13() {}
fn a14() {}
fn a15() {}
fn a16() {}
fn a17() {}
fn a18() {}
fn a19() {}
fn a20() {}
fn a21() {}
fn a22() {}
fn a23() {}
fn a24() {}
fn a25() {}
fn a26() {}
fn a27() {}
fn a28() {}
fn a29() {}
fn a30() {}
fn a31() {}
fn a32() {}
fn a33() {}
fn a34() {}
fn a35() {}
fn a36() {}
fn a37() {}
fn a38() {}
fn a39() {}
fn a40() {}
fn a41() {}
fn a42() {}
fn a43() {}
fn a44() {}
fn a45() {}
fn a46() {}
fn a47() {}
fn a48() {}
fn a49() {}

fn activity(b: &mut DagBuilder, i: usize) -> DagTaskRef {
    match i {
        0 => b.activity(a0),
        1 => b.activity(a1),
        2 => b.activity(a2),
        3 => b.activity(a3),
        4 => b.activity(a4),
        5 => b.activity(a5),
        6 => b.activity(a6),
        7 => b.activity(a7),
        8 => b.activity(a8),
        9 => b.activity(a9),
        10 => b.activity(a10),
        11 => b.activity(a11),
        12 => b.activity(a12),
        13 => b.activity(a13),
        14 => b.activity(a14),
        15 => b.activity(a15),
        16 => b.activity(a16),
        17 => b.activity(a17),
        18 => b.activity(a18),
        19 => b.activity(a19),
        20 => b.activity(a20),
        21 => b.activity(a21),
        22 => b.activity(a22),
        23 => b.activity(a23),
        24 => b.activity(a24),
        25 => b.activity(a25),
        26 => b.activity(a26),
        27 => b.activity(a27),
        28 => b.activity(a28),
        29 => b.activity(a29),
        30 => b.activity(a30),
        31 => b.activity(a31),
        32 => b.activity(a32),
        33 => b.activity(a33),
        34 => b.activity(a34),
        35 => b.activity(a35),
        36 => b.activity(a36),
        37 => b.activity(a37),
        38 => b.activity(a38),
        39 => b.activity(a39),
        40 => b.activity(a40),
        41 => b.activity(a41),
        42 => b.activity(a42),
        43 => b.activity(a43),
        44 => b.activity(a44),
        45 => b.activity(a45),
        46 => b.activity(a46),
        47 => b.activity(a47),
        48 => b.activity(a48),
        49 => b.activity(a49),
        _ => unreachable!("width is capped at 50"),
    }
}

fn dag(layers: usize, width: usize) -> DagDefinition {
    let mut b = DagBuilder::new();
    let mut prev: Vec<DagTaskRef> = Vec::new();
    for layer in 0..layers {
        let mut cur = Vec::with_capacity(width);
        for i in 0..width {
            let mut t = activity(&mut b, i);
            if i % 2 == 0 {
                t = t.retry(RetryPolicy::default());
            }
            if i % 3 == 0 {
                t = t.start_to_close(Duration::from_secs(30 + (i as u64)));
            }
            if layer > 0 {
                for k in 0..3 {
                    t = t.upstream(&prev[(i + k) % width]);
                }
            }
            cur.push(t);
        }
        prev = cur;
    }
    b.build().expect("acyclic")
}

#[inline(never)]
fn lint(dag: &DagDefinition) -> usize {
    let linter = DagLinter::new()
        .with_rule(MissingRetryPolicyRule::new())
        .with_rule(MissingTimeoutRule::new())
        .with_rule(ExcessiveParallelismRule::new(20))
        .with_rule(DuplicateNodeNameRule::new());
    linter.analyze(dag, &HashMap::new()).len()
}

#[inline(never)]
fn profile(dag: DagDefinition) -> autumn_harvest::dag_profiler::DagProfile {
    DagProfiler::new(dag).profile()
}

#[inline(never)]
fn export(p: &autumn_harvest::dag_profiler::DagProfile) -> usize {
    export_chrome_trace(p).as_array().map_or(0, Vec::len)
}

fn main() {
    let layers = env_usize("DAG_TOOL_LAYERS", 40);
    let width = env_usize("DAG_TOOL_WIDTH", 50).min(50);
    let reps = env_usize("DAG_TOOL_REPS", 1);
    let d = dag(layers, width);
    for _ in 0..reps {
        black_box(lint(black_box(&d)));
        let p = profile(d.clone());
        black_box(export(black_box(&p)));
    }
}
