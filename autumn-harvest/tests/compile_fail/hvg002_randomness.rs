use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    let _ = ctx.side_effect(&rand::random::<u64>().to_string(), || 42);
    Ok(())
}

fn main() {}
