use autumn_harvest::prelude::*;
use chrono::Local;

#[workflow]
async fn bad_workflow(_ctx: &WorkflowContext) -> Result<(), String> {
    let _ = chrono::Utc::now();
    let _ = chrono::Local::now();
    let _ = Local::now();
    Ok(())
}

fn main() {}
