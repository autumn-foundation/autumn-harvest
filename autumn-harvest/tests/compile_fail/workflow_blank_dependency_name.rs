use autumn_harvest::prelude::*;

// Issue #802: a blank declared dependency name can never resolve against any
// catalog, so the macro rejects it at compile time rather than letting it reach
// preflight as a confusing "references unregistered activity ''".
#[workflow(activities = [""])]
async fn blank_declared_activity(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
