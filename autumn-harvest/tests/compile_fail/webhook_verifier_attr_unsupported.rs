use autumn_harvest::prelude::*;

// `verifier = ...` from the original issue #344 spec is superseded. The
// `[security.webhooks]` signed-webhook endpoints of autumn-web 0.5 replace it.
// The build fails with a pointer to that configuration surface.
#[webhook(path = "/hooks/orders", starts = "order_flow", verifier = my_verifier)]
fn map_order(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("order-1"))
}

fn main() {}
