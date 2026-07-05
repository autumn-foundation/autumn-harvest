use autumn_harvest::prelude::*;

// Both `starts` and `signals` given — should fail to compile.
#[webhook(path = "/hooks/orders", starts = "order_flow", signals = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
