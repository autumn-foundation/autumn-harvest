use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::domain::{RUNNER_QUEUE, StandaloneOrder};

pub fn workflows() -> Vec<WorkflowInfo> {
    workflows![standalone_order, standalone_shipping]
}

#[workflow]
pub async fn standalone_order(
    ctx: &WorkflowContext,
    order: StandaloneOrder,
) -> HarvestResult<Value> {
    let version = ctx.version("standalone_order_shipping_v2", 1, 2);
    let mut saga = Saga::new(ctx);
    let reservation = saga
        .step(
            || async {
                ctx.execute_activity_raw("reserve_inventory", json!(order), RUNNER_QUEUE)
                    .await
            },
            |reservation| async move {
                ctx.execute_activity_raw("release_inventory", reservation, RUNNER_QUEUE)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let shipment = ctx
        .spawn_child_workflow_raw(
            "standalone_shipping",
            json!({
                "order_id": order.order_id,
                "reservation": reservation,
                "carrier": if version >= 2 { "ground" } else { "postal" },
            }),
        )
        .await?;

    Ok(json!({
        "order_id": order.order_id,
        "shipment": shipment,
        "version": version,
    }))
}

#[workflow]
pub async fn standalone_shipping(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    ctx.execute_activity_raw("buy_shipping_label", input, RUNNER_QUEUE)
        .await
}
