use autumn_harvest::prelude::*;

// Issue #802: a repeated `activities` / `children` key is rejected, not
// silently last-wins. Unguarded, `activities = [missing_handler], activities = []`
// records only the empty list. Preflight then passes, although the first list
// names an unregistered handler. That silently weakens the check the attribute
// exists to provide.
#[workflow(activities = [missing_handler], activities = [])]
async fn duplicate_declared_activities(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
