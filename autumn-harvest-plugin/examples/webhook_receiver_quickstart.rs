//! Trigger a durable workflow from a signed inbound webhook (issue #344).
//!
//! Run with:
//! ```bash
//! export AUTUMN_DATABASE__URL=postgres://localhost/harvest_demo
//! export STRIPE_WEBHOOK_SECRET=whsec_demo_secret_at_least_16_bytes
//! cargo run -p autumn-harvest-plugin --example webhook_receiver_quickstart --features webhooks
//! ```
//!
//! and an `autumn.toml` alongside it:
//! ```toml
//! [[security.webhooks.endpoints]]
//! name = "stripe"
//! path = "/hooks/stripe"
//! provider = "stripe"
//! secret_env = "STRIPE_WEBHOOK_SECRET"
//! replay_protection = false  # Harvest's own dedup is durable; see chapter 12
//! ```
//!
//! Autumn's `[security.webhooks]` layer verifies the Stripe signature (and,
//! per-endpoint, timestamp tolerance and replay protection) before any of
//! this crate's code runs. `#[webhook]` only maps the already-verified
//! payload to a deterministic workflow trigger. See
//! `docs/getting-started/12-webhooks.md` for the full walkthrough, including
//! curl examples with a computed `Stripe-Signature` header.

#![allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::unused_async
)]

use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

#[derive(serde::Deserialize)]
struct StripeEvent {
    id: String,
}

/// Starts a fresh subscription-flow run per Stripe event id -- redelivering
/// the same event resolves to the same execution (`AllowDuplicate` reuse
/// policy, keyed by this deterministic `WorkflowId`). The workflow's input is
/// always the verified JSON body itself -- the mapping function's return
/// value is only the `WorkflowId`.
#[webhook(path = "/hooks/stripe", starts = "subscription_flow")]
fn map_stripe_event(_ctx: &WebhookCtx, evt: StripeEvent) -> Result<WorkflowId, String> {
    Ok(WorkflowId::new(format!("stripe-{}", evt.id)))
}

#[workflow]
async fn subscription_flow(
    _ctx: &WorkflowContext,
    event: StripeWorkflowInput,
) -> Result<String, String> {
    Ok(format!("processed {}", event.event_type))
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StripeWorkflowInput {
    #[serde(rename = "type")]
    event_type: String,
}

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![subscription_flow])
                .webhooks(webhooks![map_stripe_event])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
