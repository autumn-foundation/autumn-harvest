//! Trigger a durable workflow from an SQS queue (issue #944).
//!
//! Run with:
//! ```bash
//! export AUTUMN_DATABASE__URL=postgres://localhost/harvest_demo
//! export AWS_REGION=us-east-1
//! cargo run -p autumn-harvest-plugin --example sqs_connector_quickstart \
//!     --features sqs
//! ```
//!
//! Then send a message:
//! ```bash
//! aws sqs send-message \
//!   --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/device-telemetry \
//!   --message-body '{"device_id":"dev-7","reading":21.5}'
//! ```
//!
//! This example uses a **`signals_with_start`** binding rather than a plain
//! start, which is the recommended shape whenever messages carry an entity key:
//!
//! * The first message for `device-dev-7` starts the entity workflow; every
//!   later message for that device is delivered to the *same* run as a signal
//!   (issue #244's atomic start-or-attach). That is also how you get **per-key
//!   ordering** — the entity workflow serializes its own signals — since the
//!   connector itself dispatches up to `max_in_flight` messages concurrently
//!   and does not preserve queue order across them.
//! * The dedupe key is the message's own SQS coordinates: a FIFO queue's
//!   `MessageDeduplicationId` when present (producer-controlled, so it survives
//!   a *re-publish* too), otherwise `MessageId` (stable across redeliveries of
//!   the same message — the honest limit of a standard queue).
//!
//! Deletion happens only after the signal durably committed or was recognised
//! as a replay; a harvest-side failure leaves the message on the queue for its
//! visibility timeout to expire. Poison messages are handed to the queue's own
//! redrive policy via `broker_native_dead_letter()` — the visibility timeout is
//! reset to 0 so SQS re-delivers, counts the receive, and moves the message to
//! the configured DLQ once `maxReceiveCount` is hit.
//!
//! See `docs/getting-started/13-broker-connectors.md`.

#![allow(clippy::unnecessary_wraps)]

use std::sync::Arc;

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;
use autumn_harvest_plugin::connector::{MappedMessage, SourceBinding, SqsSource, SqsSourceConfig};

const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/device-telemetry";

#[derive(serde::Deserialize, serde::Serialize)]
struct Reading {
    device_id: String,
    reading: f64,
}

/// One long-lived run per device, fed by signals.
#[workflow]
async fn device_session(ctx: &WorkflowContext, first: Reading) -> Result<String, String> {
    let mut count = 1;
    let mut latest = first.reading;
    // Drain whatever has already arrived, then wait for the next one.
    loop {
        for r in ctx
            .drain_signals::<Reading>("reading")
            .map_err(|e| e.to_string())?
        {
            latest = r.reading;
            count += 1;
        }
        if count >= 100 {
            return Ok(format!("{count} readings, last {latest}"));
        }
        match ctx
            .receive_signal_timeout::<Reading>("reading", std::time::Duration::from_secs(3600))
            .await
            .map_err(|e| e.to_string())?
        {
            Some(r) => {
                latest = r.reading;
                count += 1;
            }
            // Idle for an hour: seal the session rather than run forever.
            None => return Ok(format!("idle after {count} readings, last {latest}")),
        }
    }
}

#[autumn_web::main]
async fn main() {
    // ───────────────────────────── connector ─────────────────────────────
    let source = Arc::new(
        SqsSource::connect(
            SqsSourceConfig::new(QUEUE_URL)
                .stream("device-telemetry")
                // Above the worst-case dispatch latency, so a message is not
                // redelivered while it is still in flight.
                .visibility_timeout_secs(60),
        )
        .await
        .expect("sqs client should build"),
    );

    let binding = SourceBinding::signals_with_start(
        "telemetry",
        "device-telemetry",
        "device_session",
        "reading",
    )
    .map_json(|_ctx, r: Reading| {
        let payload = serde_json::to_value(&r).map_err(|e| e.to_string())?;
        Ok::<_, String>(MappedMessage::new(
            format!("device-{}", r.device_id),
            payload,
        ))
    })
    // Let SQS's own redrive policy own poison messages.
    .broker_native_dead_letter()
    .max_in_flight(16);
    // ─────────────────────────── end connector ───────────────────────────

    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![device_session])
                .connector(binding, source)
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
