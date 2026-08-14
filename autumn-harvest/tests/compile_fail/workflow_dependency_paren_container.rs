use autumn_harvest::prelude::*;

// Issue #802: `activities`/`children` use `=` with a bracketed list, not a
// parenthesised group. An author who pattern-matched off `concurrency(..)` /
// `throttle(..)` gets an actionable message naming the attribute and the
// correct shape, rather than syn's bare "expected `=`".
#[workflow(children(generate_report))]
async fn paren_container_declared_children(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
