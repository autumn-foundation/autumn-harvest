use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = chrono::Utc::now();
    Ok(())
}

fn main() {}
