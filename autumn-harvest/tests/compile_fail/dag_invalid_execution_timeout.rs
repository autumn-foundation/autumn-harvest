use autumn_harvest::prelude::*;

// The `#[dag]` macro rejects a malformed `execution_timeout` duration at
// compile time (issue #743 AC10). The `#[workflow]` macro validates its
// debounce, batch and throttle durations the same way. Neither defers to a
// runtime `.expect(...)` panic.
#[dag(execution_timeout = "4 hours")]
fn bad_execution_timeout_dag(dag: &mut DagBuilder) {
    let _ = dag;
}

fn main() {}
