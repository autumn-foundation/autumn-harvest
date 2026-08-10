//! Trigger a durable workflow from a Kafka topic (issue #944).
//!
//! Run with:
//! ```bash
//! export AUTUMN_DATABASE__URL=postgres://localhost/harvest_demo
//! cargo run -p autumn-harvest-plugin --example kafka_connector_quickstart \
//!     --features kafka
//! ```
//!
//! Then produce a message:
//! ```bash
//! echo '{"order_id":"A-1001","total_cents":4999}' \
//!   | kcat -P -b localhost:9092 -t orders.placed
//! ```
//!
//! What you get for the ~20 lines between the two `── connector ──` markers:
//!
//! * **Idempotent redelivery** — the dedupe key is derived from the message's
//!   own Kafka coordinates (`topic:partition:offset`), namespaced by the
//!   binding. A rebalance or an un-acked crash replays the message; harvest
//!   returns the *same* execution instead of starting a second one.
//! * **At-least-once ack ordering** — the offset is committed only after the
//!   start durably committed (or was recognised as a replay), and only once
//!   every lower in-flight offset has also settled. Kill the process
//!   mid-dispatch and the message is redelivered, not lost.
//! * **Poison isolation** — a message that cannot be decoded, or that the
//!   mapping function rejects `poison_threshold` times running, goes to
//!   `harvest_connector_dead_letters` and is acked. One bad message never
//!   wedges the partition.
//! * **Backpressure** — at most `max_in_flight` dispatches are in flight, and
//!   a start deferred by a throttle (issue #607) counts as success.
//!
//! See `docs/getting-started/13-broker-connectors.md` for the failure-mode
//! table and the ordering caveat.

#![allow(clippy::unnecessary_wraps)]

use std::sync::Arc;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::connector::{
    KafkaSource, KafkaSourceConfig, MappedMessage, SourceBinding,
};

#[derive(serde::Deserialize, serde::Serialize)]
struct OrderPlaced {
    order_id: String,
    total_cents: i64,
}

#[workflow]
async fn fulfil_order(_ctx: &WorkflowContext, order: OrderPlaced) -> Result<String, String> {
    Ok(format!(
        "fulfilled {} for {}c",
        order.order_id, order.total_cents
    ))
}

#[autumn_web::main]
async fn main() {
    // ───────────────────────────── connector ─────────────────────────────
    let source = Arc::new(
        KafkaSource::connect(&KafkaSourceConfig::new(
            "localhost:9092",
            "harvest-orders",
            "orders.placed",
        ))
        .expect("kafka consumer should connect"),
    );

    let binding = SourceBinding::starts("orders", "orders.placed", "fulfil_order")
        .map_json(|_ctx, order: OrderPlaced| {
            // The mapping function chooses the workflow id. Deriving it from a
            // business key (rather than the offset) makes the run addressable
            // and keeps redeliveries collapsing onto one execution even if the
            // broker replays under a different offset.
            let payload = serde_json::to_value(&order).map_err(|e| e.to_string())?;
            Ok::<_, String>(MappedMessage::new(
                format!("order-{}", order.order_id),
                payload,
            ))
        })
        .max_in_flight(32);
    // ─────────────────────────── end connector ───────────────────────────

    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![fulfil_order])
                .connector(binding, source)
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
