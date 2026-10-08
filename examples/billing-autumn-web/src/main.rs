#![allow(clippy::missing_errors_doc, clippy::unused_async)]

mod activities;
mod dags;
mod domain;
mod routes;
mod workflows;

#[cfg(test)]
mod tests;

use autumn_harvest::prelude::WorkerConfig;
use autumn_harvest_plugin::prelude::HarvestPlugin;

use crate::domain::{CHECKOUT_QUEUE, INVOICE_QUEUE, OPS_QUEUE, PAYMENT_QUEUE};

#[autumn_web::main]
async fn main() {
    // The `run` future is large in autumn-web 0.8, so it is boxed.
    // That keeps it off the stack and satisfies `clippy::large_futures`.
    Box::pin(
        autumn_web::app()
            .routes(routes::routes())
            .plugin(
                HarvestPlugin::new()
                    .workflows(workflows::workflows())
                    .activities(activities::activities())
                    .dags(dags::dags())
                    .worker(WorkerConfig::default().with_queues([
                        CHECKOUT_QUEUE,
                        PAYMENT_QUEUE,
                        INVOICE_QUEUE,
                        OPS_QUEUE,
                    ]))
                    .api("/api/harvest"),
            )
            .run(),
    )
    .await;
}
