use autumn_harvest::prelude::*;

// Neither `starts` nor `signals` given — should fail to compile.
#[webhook(path = "/hooks/orders")]
fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
