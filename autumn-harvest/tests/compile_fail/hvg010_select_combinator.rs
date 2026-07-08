use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    // The `futures::future::select` combinator FUNCTION is the function-call
    // sibling of `tokio::select!` — HVG010 flags it too (issue #799).
    let a = Box::pin(async {});
    let b = Box::pin(async {});
    let _ = futures::future::select(a, b).await;
    Ok(())
}

fn main() {}
