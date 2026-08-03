use autumn_harvest::prelude::*;

// A malformed execution_timeout duration string must be rejected at compile
// time by the `#[dag]` macro (issue #743 AC10), mirroring how the
// `#[workflow]` macro validates its own debounce/batch/throttle duration
// attributes rather than deferring to a runtime `.expect(...)` panic.
#[dag(execution_timeout = "4 hours")]
fn bad_execution_timeout_dag(dag: &mut DagBuilder) {
    let _ = dag;
}

fn main() {}
