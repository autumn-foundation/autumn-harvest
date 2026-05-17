use autumn_harvest::prelude::*;

// Missing ctx as first argument — should fail to compile.
#[update(workflow = "my_workflow")]
async fn approve() -> Result<bool, String> {
    Ok(true)
}

fn main() {}
