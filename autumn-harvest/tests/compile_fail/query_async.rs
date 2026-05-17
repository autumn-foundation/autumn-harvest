use autumn_harvest::prelude::*;

// async fn is not allowed for #[query] — should fail to compile.
#[query(workflow = "my_workflow")]
async fn get_status(_ctx: &WorkflowContext) -> Result<u64, String> {
    Ok(42)
}

fn main() {}
