use autumn_harvest::prelude::*;
use std::sync::Mutex;

static GLOBAL_MUTEX: Mutex<u32> = Mutex::new(0);

#[workflow(allow_nondeterministic_apis)]
async fn bypass_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    let _ = chrono::Utc::now();
    let _ = rand::random::<u64>();
    let _ = std::env::var("PORT");
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    tokio::spawn(async {});
    let _ = std::fs::read_to_string("config.json");
    let _guard = GLOBAL_MUTEX.lock().unwrap();
    tracing::info!("this is bare log which is a warning but allowed or warning only");
    Ok(())
}

fn main() {}
