use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::fs::read_to_string("config.json");
    Ok(())
}

fn main() {}
