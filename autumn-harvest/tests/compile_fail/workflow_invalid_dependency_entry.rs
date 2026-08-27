use autumn_harvest::prelude::*;

// Issue #802: a dependency list entry must be a name — a bare identifier, a
// path, or a string literal. Anything else (here an integer literal) is
// rejected with the actionable "expected an activity/workflow name" message
// rather than a raw syn parse error.
#[workflow(activities = [42])]
async fn non_name_declared_activity(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
