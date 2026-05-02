use autumn_harvest::{WorkflowEvent, WorkflowSimulator};
use autumn_harvest_plugin::prelude::HarvestMode;
use serde_json::json;

use crate::domain::{RUNNER_QUEUE, StandaloneOrder};
use crate::runtime::{standalone_builder, standalone_runtime_config};
use crate::workflows;

#[test]
fn runtime_config_uses_external_runner_mode_without_outbox() {
    let config = standalone_runtime_config("postgres://runner:runner@localhost/runner".to_owned());

    assert_eq!(config.mode, HarvestMode::External);
    assert!(config.worker_enabled);
    assert!(config.scheduler_enabled);
    assert!(!config.outbox.enabled);
}

#[test]
fn builder_registers_runner_owned_workflows_and_activities() {
    let built = standalone_builder()
        .try_build()
        .expect("standalone runner registrations should build");

    assert_eq!(built.workflow_count(), 2);
    assert_eq!(built.activity_count(), 3);
    assert_eq!(built.worker_config().queues, vec![RUNNER_QUEUE.to_owned()]);
}

#[tokio::test]
async fn standalone_order_uses_version_gate_saga_and_child_workflow() {
    let result =
        WorkflowSimulator::new(workflows::__autumn_workflow_info_standalone_order().handler)
            .mock_activity("reserve_inventory", |input| {
                let order: StandaloneOrder =
                    serde_json::from_value(input).expect("order should deserialize");
                Ok(json!({
                    "reservation_id": format!("res_{}", order.order_id),
                    "sku": order.sku,
                    "quantity": order.quantity,
                }))
            })
            .mock_child_workflow("standalone_shipping", |input| {
                Ok(json!({
                    "label_id": format!("lbl_{}", input["order_id"].as_str().unwrap_or("order")),
                    "carrier": input["carrier"],
                }))
            })
            .run(json!(StandaloneOrder {
                order_id: "order-1001".to_owned(),
                sku: "sku-book".to_owned(),
                quantity: 2,
            }))
            .await;

    assert_eq!(
        result
            .final_output
            .expect("standalone order should complete"),
        json!({
            "order_id": "order-1001",
            "shipment": {
                "label_id": "lbl_order-1001",
                "carrier": "ground",
            },
            "version": 2,
        })
    );
    assert!(result.history.iter().any(|event| matches!(
        event,
        WorkflowEvent::MarkerRecorded { name, details }
            if name == "version:standalone_order_shipping_v2" && details == &json!(2)
    )));
    assert!(result.history.iter().any(|event| matches!(
        event,
        WorkflowEvent::ChildWorkflowStarted { workflow_name, .. }
            if workflow_name == "standalone_shipping"
    )));
}
