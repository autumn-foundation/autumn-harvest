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
    drop(_guard);
    tokio::select! {
        _ = ctx.timer("t1", 60) => {}
        _ = ctx.wait_for_signal("approve") => {}
    }
    Ok(())
}

#[workflow]
async fn renamed_context_workflow(context: &WorkflowContext) -> Result<(), String> {
    let _ = context.side_effect("test_id", || rand::random::<u64>());

    // Unrelated gen() call (allowed)
    struct InvoiceIdGenerator;
    impl InvoiceIdGenerator {
        fn r#gen(&self) -> String {
            "invoice_123".to_string()
        }
    }
    let generator = InvoiceIdGenerator;
    let _id = generator.r#gen();

    // Deterministic tonic status / metadata (allowed)
    let _status = tonic::Status::new(tonic::Code::Ok, "ok");
    let _meta = tonic::metadata::MetadataMap::new();

    Ok(())
}

mod tonic {
    pub enum Code { Ok }
    pub struct Status;
    impl Status {
        pub fn new(_: Code, _: &str) -> Self { Self }
    }
    pub mod metadata {
        pub struct MetadataMap;
        impl MetadataMap {
            pub fn new() -> Self { Self }
        }
    }
}

fn main() {}

