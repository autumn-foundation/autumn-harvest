use autumn_harvest::prelude::*;

// Return type must be Result<T, E> — should fail to compile.
#[query(workflow = "my_workflow")]
fn get_count(_ctx: &WorkflowContext) -> u64 {
    42
}

fn main() {}
