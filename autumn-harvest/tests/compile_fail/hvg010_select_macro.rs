use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    tokio::select! {
        _ = ctx.timer("t1", 60) => {}
        _ = ctx.wait_for_signal("approve") => {}
    }
    Ok(())
}

fn main() {}
