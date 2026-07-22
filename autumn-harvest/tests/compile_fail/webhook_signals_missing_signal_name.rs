use autumn_harvest::prelude::*;

// `signals` given without required `signal_name` — should fail to compile.
#[webhook(path = "/hooks/stripe", signals = "subscription_flow")]
fn map_payment(_ctx: &WebhookCtx, evt: serde_json::Value) -> Result<WorkflowId, String> {
    let _ = evt;
    Ok(WorkflowId::new("subscription-1"))
}

fn main() {}
