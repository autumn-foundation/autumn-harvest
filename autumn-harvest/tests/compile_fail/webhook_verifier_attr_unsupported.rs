use autumn_harvest::prelude::*;

// `verifier = ...` from the original issue #344 spec is superseded by
// autumn-web 0.5's [security.webhooks] signed-webhook endpoints — should
// fail to compile with a pointer to the real configuration surface.
#[webhook(path = "/hooks/orders", starts = "order_flow", verifier = my_verifier)]
fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
