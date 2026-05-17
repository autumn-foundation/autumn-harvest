use autumn_harvest::prelude::*;

// Non-async fn is not allowed for #[update] — should fail to compile.
#[update(workflow = "my_workflow")]
fn approve(_ctx: &WorkflowContext) -> Result<bool, String> {
    Ok(true)
}

fn main() {}
