//! The upgrade-check command of a candidate build (issue #1995).
//!
//! The check must run inside the candidate build. Only that build holds its
//! workflow types and codec keys. So each deployment adds a small binary like
//! this one, next to its worker binary, with the same workflows and codecs.
//!
//! ```console
//! $ cargo harvest-verify -p my-workflows --lib --emit-structure new.structure.json
//! $ cargo run --example upgrade_check --features db,testing -- \
//!     --database-url-env HARVEST_DATABASE_URL \
//!     --baseline-structure old.structure.json \
//!     --candidate-structure new.structure.json
//! ```
//!
//! The command prints one verdict per in-flight run: migrate, review or pin.
//! See `docs/upgrade-check.md`.

use autumn_harvest::prelude::*;
use autumn_harvest::upgrade_check::{UpgradeCheck, run_command};

#[activity]
async fn reserve_stock(_ctx: &ActivityContext, sku: String) -> Result<u32, String> {
    Ok(u32::try_from(sku.len()).unwrap_or(u32::MAX))
}

/// One step: a helper body that emits one command.
async fn reserve(ctx: &WorkflowContext, sku: String) -> Result<u32, String> {
    ctx.execute_activity(&reserve_stock_info(), sku)
        .await
        .map_err(|e| e.to_string())
}

#[workflow]
async fn place_order(ctx: &WorkflowContext, sku: String) -> Result<u32, String> {
    let quantity = reserve(ctx, sku).await?;
    ctx.timer("settle", 60).await.map_err(|e| e.to_string())?;
    Ok(quantity)
}

#[tokio::main]
async fn main() {
    // Register the same workflows, signals, updates and codecs as the worker.
    let check = UpgradeCheck::new().register(workflows![place_order]);
    std::process::exit(run_command(check, std::env::args().collect()).await);
}
