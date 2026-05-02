#![allow(clippy::missing_errors_doc, clippy::unused_async)]

use std::net::SocketAddr;
use std::time::Duration;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::prelude::*;
use autumn_web::config::DatabaseConfig;
use autumn_web::reexports::axum::{self, Json, routing::get};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const RUNNER_QUEUE: &str = "standalone";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct StandaloneOrder {
    order_id: String,
    sku: String,
    quantity: u32,
}

fn standalone_runtime_config(database_url: String) -> HarvestRuntimeConfig {
    HarvestRuntimeConfig {
        mode: HarvestMode::External,
        worker_enabled: true,
        scheduler_enabled: true,
        database: HarvestDatabaseConfig {
            url: Some(database_url),
        },
        outbox: HarvestOutboxConfig {
            enabled: false,
            ..HarvestOutboxConfig::default()
        },
        batch: autumn_harvest_plugin::config::HarvestBatchConfig::default(),
    }
}

fn standalone_builder() -> HarvestBuilder {
    HarvestBuilder::default()
        .workflows(workflows![standalone_order, standalone_shipping])
        .activities(activities![
            reserve_inventory,
            release_inventory,
            buy_shipping_label,
        ])
        .worker(WorkerConfig::default().with_queues([RUNNER_QUEUE]))
}

#[workflow]
async fn standalone_order(ctx: &WorkflowContext, order: StandaloneOrder) -> HarvestResult<Value> {
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
async fn standalone_shipping(ctx: &WorkflowContext, input: Value) -> HarvestResult<Value> {
    ctx.execute_activity_raw("buy_shipping_label", input, RUNNER_QUEUE)
        .await
}

#[activity(start_to_close = "30s", retry = RetryPolicy::fixed(3, Duration::from_secs(1)), queue = "standalone")]
async fn reserve_inventory(_ctx: &ActivityContext, order: StandaloneOrder) -> HarvestResult<Value> {
    Ok(json!({
        "reservation_id": format!("res_{}", order.order_id),
        "sku": order.sku,
        "quantity": order.quantity,
    }))
}

#[activity(start_to_close = "30s", queue = "standalone")]
async fn release_inventory(_ctx: &ActivityContext, reservation: Value) -> HarvestResult<Value> {
    tracing::info!(reservation = ?reservation, "released inventory reservation");
    Ok(json!({ "released": true }))
}

#[activity(start_to_close = "30s", retry = RetryPolicy::exponential(3, Duration::from_secs(1)), queue = "standalone")]
async fn buy_shipping_label(_ctx: &ActivityContext, input: Value) -> HarvestResult<Value> {
    Ok(json!({
        "label_id": format!("lbl_{}", input["order_id"].as_str().unwrap_or("order")),
        "carrier": input["carrier"],
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://runner:runner@localhost:5434/runner".to_owned());
    if std::env::var("AUTUMN_PROFILE").as_deref() == Ok("dev") {
        autumn_web::migrate::run_pending(&database_url, autumn_harvest::MIGRATIONS)?;
    }

    let pool = autumn_web::db::create_pool(&DatabaseConfig {
        url: Some(database_url.clone()),
        ..DatabaseConfig::default()
    })?
    .ok_or("DATABASE_URL must create a Postgres pool")?;

    let config = standalone_runtime_config(database_url);
    let built = standalone_builder().try_build()?;
    let runner = HarvestRunner::start(built, &config, HarvestRunnerResources::new(pool.clone()))
        .map_err(|error| format!("failed to start Harvest runner: {error}"))?;

    let api_state = HarvestApiState::new();
    api_state.install_storage_pool(runner.storage_pool());
    api_state.install(runner.api_runtime());

    let web_state = autumn_web::AppState::for_test().with_pool(pool);
    let app = axum::Router::new()
        .route(
            "/",
            get(|| async { Json(json!({ "service": "standalone-runner" })) }),
        )
        .nest("/api/harvest", harvest_api_router(api_state))
        .with_state(web_state);

    let address = SocketAddr::from(([127, 0, 0, 1], 8082));
    tracing::info!(%address, "standalone Harvest runner listening");
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    runner.stop().await;
    Ok(())
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::warn!(%error, "failed to listen for shutdown signal");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::{WorkflowEvent, WorkflowSimulator};

    #[test]
    fn runtime_config_uses_external_runner_mode_without_outbox() {
        let config =
            standalone_runtime_config("postgres://runner:runner@localhost/runner".to_owned());

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
        let result = WorkflowSimulator::new(__autumn_workflow_info_standalone_order().handler)
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
}
