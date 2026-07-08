use autumn_harvest::prelude::*;
use std::collections::HashMap;

#[workflow]
async fn bad_workflow(ctx: &WorkflowContext) -> Result<(), String> {
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

fn main() {}
