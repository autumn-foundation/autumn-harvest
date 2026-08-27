use autumn_harvest::prelude::*;

// Issue #802: a repeated `activities` / `children` key is rejected rather than
// silently last-wins. Left unguarded, `activities = [missing_handler],
// activities = []` would record only the empty list and preflight would pass
// despite the first declaration naming an unregistered handler — silently
// weakening the very check the attribute exists to provide.
#[workflow(activities = [missing_handler], activities = [])]
async fn duplicate_declared_activities(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
