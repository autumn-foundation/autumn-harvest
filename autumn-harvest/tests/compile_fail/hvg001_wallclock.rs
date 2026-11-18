use autumn_harvest::prelude::*;

#[workflow]
async fn bad_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    let _ = jiff::Timestamp::now();
    let _ = jiff::ZonedDateTime::now();
    let _ = jiff::ZonedDateTime::now();
    Ok(())
}

fn main() {}
