use autumn_harvest::prelude::*;

// A malformed sla duration string must be rejected at compile time by the
// `#[dag]` macro (issue #743 AC10), mirroring how the `#[workflow]` macro
// validates its own debounce/batch/throttle duration attributes rather than
// deferring to a runtime `.expect(...)` panic.
#[dag(sla = "three hours")]
fn bad_sla_dag(dag: &mut DagBuilder) {
    let _ = dag;
}

fn main() {}
