use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    Ok(())
}

fn main() {}
