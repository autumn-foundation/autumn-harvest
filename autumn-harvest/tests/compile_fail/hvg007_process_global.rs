use autumn_harvest::prelude::*;
use std::sync::Mutex;

static GLOBAL_MUTEX: Mutex<u32> = Mutex::new(0);

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _guard = GLOBAL_MUTEX.lock().unwrap();
    Ok(())
}

fn main() {}
