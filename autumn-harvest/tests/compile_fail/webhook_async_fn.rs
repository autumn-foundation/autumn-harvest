use autumn_harvest::prelude::*;

// async fn is not allowed for #[webhook] mapping functions — should fail to
// compile. Do I/O inside the target workflow, not the mapping function.
#[webhook(path = "/hooks/orders", starts = "order_flow")]
async fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
