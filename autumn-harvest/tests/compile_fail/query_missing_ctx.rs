use autumn_harvest::prelude::*;

// Missing ctx as first argument — should fail to compile.
#[query(workflow = "my_workflow")]
fn get_count() -> Result<u64, String> {
    Ok(42)
}

fn main() {}
