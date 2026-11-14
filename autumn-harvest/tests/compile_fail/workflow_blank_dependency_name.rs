use autumn_harvest::prelude::*;

// Issue #802: a blank declared dependency name never resolves against any
// catalog. So the macro rejects it at compile time. Otherwise preflight reports
// a confusing "references unregistered activity ''".
#[workflow(activities = [""])]
async fn blank_declared_activity(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
