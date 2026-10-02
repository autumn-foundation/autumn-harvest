use autumn_harvest::prelude::*;

use crate::domain::StandaloneOrder;

/// The path of the order webhook. `#[webhook]` needs it as a literal too.
/// `build_webhook_router` panics when the two differ.
pub const ORDER_WEBHOOK_PATH: &str = "/hooks/orders";

pub fn webhooks() -> Vec<WebhookTriggerInfo> {
    webhooks![order_placed]
}

/// Start one `standalone_order` per order. The workflow id comes from the
/// order id, so a redelivery maps to the run that already exists.
#[webhook(path = "/hooks/orders", starts = "standalone_order")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    reason = "#[webhook] fixes this signature"
)]
pub fn order_placed(_ctx: &WebhookCtx, order: StandaloneOrder) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("order-{}", order.order_id)))
}
