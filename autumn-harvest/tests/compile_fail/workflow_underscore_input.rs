use autumn_harvest::prelude::*;

// A `_` input pattern has no binding for the generated dispatch code. The
// `#[workflow]` macro must say so, not fail with an arity error on the
// attribute line.
#[workflow]
async fn bad_input(ctx: &WorkflowContext, _: ()) -> HarvestResult<()> {
    let _ = ctx;
    Ok(())
}

fn main() {}
