//! Non-criterion instruction/allocation-count harness for
//! `DagRetryEnvelope::analyze`, the worst-case makespan analysis of a DAG.
//!
//! Wall-clock timing is unreliable on the shared vCPU, so run the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A layered DAG of `RETRY_ENV_LAYERS` layers (default 40) by `RETRY_ENV_WIDTH`
//! tasks (default 50, at most 50). Each task past layer 0 depends on three
//! tasks of the previous layer. Most tasks carry a retry policy. The policies
//! cycle through every jitter mode, with 3 to 10 attempts. A third of the
//! tasks set `start_to_close`. `RETRY_ENV_REPS` (default 1) repeats the call.
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench dag_retry_envelope_profile --no-run
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
//! callgrind_annotate --inclusive=yes cg.out
//! ```

use std::hint::black_box;
use std::time::Duration;

use autumn_harvest::dag::{DagBuilder, DagDefinition, DagTaskRef};
use autumn_harvest::dag_retry_envelope::DagRetryEnvelope;
use autumn_harvest::policy::{JitterPolicy, RetryPolicy};

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

fn policy(i: usize) -> RetryPolicy {
    let jitter = match i % 4 {
        0 => JitterPolicy::None,
        1 => JitterPolicy::Full,
        2 => JitterPolicy::Equal,
        _ => JitterPolicy::Decorrelated,
    };
    let attempts = 3 + u32::try_from(i % 8).unwrap_or(0);
    RetryPolicy::exponential(attempts, Duration::from_secs(1 + (i as u64 % 5))).with_jitter(jitter)
}

fn dag(layers: usize, width: usize) -> DagDefinition {
    let mut b = DagBuilder::new();
    let mut prev: Vec<DagTaskRef> = Vec::new();
    for layer in 0..layers {
        let mut cur = Vec::with_capacity(width);
        for i in 0..width {
            let mut t = activity(&mut b, i);
            if i % 5 != 4 {
                t = t.retry(policy(i + layer));
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
fn analyze(dag: DagDefinition) -> usize {
    let report = DagRetryEnvelope::new(dag)
        .with_default_duration(Duration::from_secs(2))
        .analyze();
    report.tasks.len() + report.worst_case_path.len()
}

fn main() {
    let layers = env_usize("RETRY_ENV_LAYERS", 40);
    let width = env_usize("RETRY_ENV_WIDTH", 50).min(50);
    let reps = env_usize("RETRY_ENV_REPS", 1);
    let dag = dag(layers, width);
    let mut total = 0;
    for _ in 0..reps {
        total += black_box(analyze(black_box(dag.clone())));
    }
    println!("tasks={} checksum={total}", layers * width);
}
