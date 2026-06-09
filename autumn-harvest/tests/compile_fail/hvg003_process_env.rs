use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::env::var("PORT");
    Ok(())
}

fn main() {}
