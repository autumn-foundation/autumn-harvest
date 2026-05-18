use autumn_harvest::prelude::*;
use serde_json::{Value, json};

use crate::activities::{buy_shipping_label_info, release_inventory_info, reserve_inventory_info};
use crate::domain::StandaloneOrder;

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
    let reservation: Value = saga
        .step(
            || async {
                ctx.execute_activity(&reserve_inventory_info(), &order)
                    .await
            },
            |reservation| async move {
                ctx.execute_activity::<_, Value>(&release_inventory_info(), reservation)
                    .await
                    .map(|_| ())
            },
        )
        .await?;

    let shipment: Value = ctx
        .spawn_child_workflow(
            &standalone_shipping_info(),
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
    ctx.execute_activity(&buy_shipping_label_info(), input)
        .await
}
