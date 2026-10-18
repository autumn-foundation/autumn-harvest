//! Non-criterion instruction/allocation-count harness for `DagBuilder::build`
//! and `DagSimulator::run`, the in-memory DAG compile and dry-run that
//! workflow authors drive from tests and CI.
//!
//! Wall-clock timing is unreliable on the shared vCPU, so run the binary under
//! `valgrind --tool=callgrind` or `valgrind --tool=dhat`.
//!
//! # Workload
//!
//! A layered DAG: `DAG_SIM_LAYERS` layers (default 40) of `DAG_SIM_WIDTH`
//! tasks (default 50). Each task past layer 0 depends on three tasks of the
//! previous layer, so every layer is a real fan-in. `DAG_SIM_REPS` (default 1)
//! repeats the `build` and `run` calls.
//!
//! ```text
//! cargo bench -p autumn-harvest --no-default-features --features testing \
//!   --bench dag_simulator_profile --no-run
//! valgrind --tool=callgrind --callgrind-out-file=cg.out <binary>
//! callgrind_annotate --inclusive=yes cg.out
//! ```

use std::hint::black_box;

use autumn_harvest::dag::DagBuilder;
use autumn_harvest::dag_simulator::DagSimulator;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn noop() {}

fn builder(layers: usize, width: usize) -> DagBuilder {
    let mut b = DagBuilder::new();
    let mut prev = Vec::new();
    for layer in 0..layers {
        let mut cur = Vec::with_capacity(width);
        for i in 0..width {
            let mut t = b.activity(noop);
            if layer > 0 {
                for k in 0..3 {
                    t = t.upstream(&prev[(i + k) % width]);
                }
            }
            cur.push(t);
        }
        prev = cur;
    }
    b
}

#[inline(never)]
fn build(b: &DagBuilder) -> autumn_harvest::dag::DagDefinition {
    b.build().expect("acyclic")
}

#[inline(never)]
fn simulate(dag: autumn_harvest::dag::DagDefinition) -> usize {
    DagSimulator::new(dag).run().task_states.len()
}

fn main() {
    let layers = env_usize("DAG_SIM_LAYERS", 40);
    let width = env_usize("DAG_SIM_WIDTH", 50);
    let reps = env_usize("DAG_SIM_REPS", 1);
    let b = builder(layers, width);
    for _ in 0..reps {
        let dag = black_box(build(&b));
        black_box(simulate(dag));
    }
}
