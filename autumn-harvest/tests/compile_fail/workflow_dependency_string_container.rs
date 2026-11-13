use autumn_harvest::prelude::*;

// Issue #802: `activities`/`children` take a bracketed list, not a string.
// An author may copy the `owner = "..."` / `runbook = "..."` shape. The error
// names the attribute and the correct shape, not syn's bare "expected square
// brackets".
#[workflow(activities = "send_email, charge_card")]
async fn string_container_declared_activities(_ctx: &WorkflowContext) -> Result<(), String> {
    Ok(())
}

fn main() {}
