use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    tokio::spawn(async {});
    Ok(())
}

fn main() {}
