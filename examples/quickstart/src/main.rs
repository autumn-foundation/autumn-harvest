use autumn_harvest::prelude::*;
use autumn_harvest_plugin::HarvestPlugin;

// RED: greeting and send_greeting are not defined yet.
// This crate will not compile until the workflow and activity bodies are added.

#[autumn_web::main]
async fn main() {
    autumn_web::app()
        .plugin(
            HarvestPlugin::new()
                .workflows(workflows![greeting])
                .activities(activities![send_greeting])
                .worker(WorkerConfig::default())
                .api("/api/harvest"),
        )
        .run()
        .await;
}
