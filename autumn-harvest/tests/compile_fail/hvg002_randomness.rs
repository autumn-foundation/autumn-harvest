use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = rand::random::<u64>();
    Ok(())
}

fn main() {}
