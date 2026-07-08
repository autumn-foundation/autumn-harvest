use autumn_harvest::prelude::*;
use std::collections::{BTreeMap, HashMap};
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
    // HVG011 bypass proof: a command-emitting HashMap iteration is allowed
    // under allow_nondeterministic_apis (the flag gates the whole visitor).
    let mut amounts: HashMap<String, u64> = HashMap::new();
    amounts.insert("acct".to_string(), 1);
    for (account, _amount) in &amounts {
        ctx.execute_activity_raw(
            "debit",
            autumn_harvest::serde_json::Value::String(account.clone()),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// HVG011 negative controls: ordered/sorted iteration with commands must
// compile clean in a fully-linted workflow (no allow_nondeterministic_apis).
#[workflow]
async fn ordered_iteration_workflow(ctx: &WorkflowContext) -> Result<(), String> {
    // BTreeMap iteration with a command — deterministic order, never flagged.
    let mut btree: BTreeMap<String, u64> = BTreeMap::new();
    btree.insert("a".to_string(), 1);
    for (key, _value) in &btree {
        ctx.execute_activity_raw(
            "process",
            autumn_harvest::serde_json::Value::String(key.clone()),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    // Vec iteration with a command — never flagged.
    let items: Vec<String> = vec!["x".to_string(), "y".to_string()];
    for item in &items {
        ctx.execute_activity_raw(
            "process",
            autumn_harvest::serde_json::Value::String(item.clone()),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
    }

    // HashMap keys collected + sorted into a Vec, then iterated with a
    // command — the recommended remediation itself must never be flagged.
    let mut map: HashMap<String, u64> = HashMap::new();
    map.insert("k".to_string(), 1);
    let mut keys: Vec<String> = map.keys().cloned().collect();
    keys.sort();
    for key in keys {
        ctx.execute_activity_raw(
            "process",
            autumn_harvest::serde_json::Value::String(key),
            "default",
        )
        .await
        .map_err(|e| e.to_string())?;
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

