use std::time::Duration;

use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::domain::StandaloneOrder;

pub fn activities() -> Vec<ActivityInfo> {
    activities![reserve_inventory, release_inventory, buy_shipping_label,]
}

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)), queue = "standalone")]
pub async fn reserve_inventory(
    _ctx: &ActivityContext,
    order: StandaloneOrder,
) -> HarvestResult<Value> {
    Ok(json!({
        "reservation_id": format!("res_{}", order.order_id),
        "sku": order.sku,
        "quantity": order.quantity,
    }))
}

#[activity(start_to_close = "30s", queue = "standalone")]
pub async fn release_inventory(_ctx: &ActivityContext, reservation: Value) -> HarvestResult<Value> {
    tracing::info!(reservation = ?reservation, "released inventory reservation");
    Ok(json!({ "released": true }))
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)), queue = "standalone")]
pub async fn buy_shipping_label(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "label_id": format!("lbl_{}", input["order_id"].as_str().unwrap_or("order")),
        "carrier": input["carrier"],
    }))
}
