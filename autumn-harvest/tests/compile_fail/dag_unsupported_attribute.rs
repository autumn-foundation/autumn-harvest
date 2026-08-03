use autumn_harvest::prelude::*;

// AC8 (issue #743): the `#[dag]` macro's "unsupported attribute" error must
// list `execution_timeout` and `sla` alongside the other recognized names.
#[dag(bogus_attr = "nope")]
fn bogus_attr_dag(dag: &mut DagBuilder) {
    let _ = dag;
}

fn main() {}
