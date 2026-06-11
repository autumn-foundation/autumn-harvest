use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = std::env::var("PORT");
    let _ = std::env::var_os("PORT");
    let _ = std::env::args();
    let _ = std::env::args_os();
    Ok(())
}

fn main() {}
