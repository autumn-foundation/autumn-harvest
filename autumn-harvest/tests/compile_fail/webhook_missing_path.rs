use autumn_harvest::prelude::*;

// Missing required `path` attribute — should fail to compile.
#[webhook(starts = "order_flow")]
fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
