use autumn_harvest::prelude::*;

// The `#[dag]` macro rejects a malformed `sla` duration at compile time
// (issue #743 AC10). The `#[workflow]` macro validates its debounce, batch
// and throttle durations the same way. Neither defers to a runtime
// `.expect(...)` panic.
#[dag(sla = "three hours")]
fn bad_sla_dag(dag: &mut DagBuilder) {
    let _ = dag;
}

fn main() {}
