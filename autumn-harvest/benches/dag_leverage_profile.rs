//! Deterministic instruction/allocation-count harness for
//! `DagLeverage::rank`, the what-if ranking of DAG tasks.
//!
//! Wall-clock timing is unreliable on the shared vCPU, so run the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A layered DAG of `DAG_LEV_LAYERS` layers (default 10) by `DAG_LEV_WIDTH`
//! tasks (default 50, at most 50). Each task past layer 0 depends on three tasks
//! of the previous layer. Durations differ per activity name. The harness runs
//! the slack analysis, then ranks every task, as an operator tool does.
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench dag_leverage_profile --no-run
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
//! ```

use std::hint::black_box;
use std::time::Duration;

use autumn_harvest::dag::{DagBuilder, DagDefinition, DagTaskRef};
use autumn_harvest::dag_leverage::DagLeverage;
use autumn_harvest::dag_slack::DagSlackAnalyzer;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const fn a0() {}
const fn a1() {}
const fn a2() {}
const fn a3() {}
const fn a4() {}
const fn a5() {}
const fn a6() {}
const fn a7() {}
const fn a8() {}
const fn a9() {}
const fn a10() {}
const fn a11() {}
const fn a12() {}
const fn a13() {}
const fn a14() {}
const fn a15() {}
const fn a16() {}
const fn a17() {}
const fn a18() {}
const fn a19() {}
const fn a20() {}
const fn a21() {}
const fn a22() {}
const fn a23() {}
const fn a24() {}
const fn a25() {}
const fn a26() {}
const fn a27() {}
const fn a28() {}
const fn a29() {}
const fn a30() {}
const fn a31() {}
const fn a32() {}
const fn a33() {}
const fn a34() {}
const fn a35() {}
const fn a36() {}
const fn a37() {}
const fn a38() {}
const fn a39() {}
const fn a40() {}
const fn a41() {}
const fn a42() {}
const fn a43() {}
const fn a44() {}
const fn a45() {}
const fn a46() {}
const fn a47() {}
const fn a48() {}
const fn a49() {}

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

fn main() {
    let layers = env_usize("DAG_LEV_LAYERS", 10);
    let width = env_usize("DAG_LEV_WIDTH", 50).min(50);
    let d = dag(layers, width);
    let mut analyzer = DagSlackAnalyzer::new(d.clone());
    for i in 0..width {
        analyzer = analyzer.mock_duration(
            &format!("a{i}"),
            Duration::from_millis(100 + 37 * (i as u64 % 11)),
        );
    }
    let report = analyzer.analyze();
    let ranking = rank(&d, &report);
    black_box(ranking.len());
}

#[inline(never)]
fn rank(
    d: &DagDefinition,
    report: &autumn_harvest::dag_slack::DagSlackReport,
) -> Vec<autumn_harvest::dag_leverage::TaskLeverage> {
    DagLeverage::new(black_box(d), black_box(report)).rank()
}
