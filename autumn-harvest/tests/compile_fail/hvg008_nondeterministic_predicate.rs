use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    ctx.await_condition(move || {
        let _ = std::time::Instant::now();
        true
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

fn main() {}
