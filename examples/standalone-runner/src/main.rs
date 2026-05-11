//! Standalone runner example for the Autumn Harvest framework.
//!
//! **Why does this exist?**
//! This binary demonstrates how to run Autumn Harvest entirely independent of a web
//! framework. It spins up a worker pool, registers sample workflows and activities,
//! and executes them.
//!
//! ## Examples
//!
//! To run this example:
//! ```sh
//! cargo run -p standalone-runner
//! ```

#![allow(clippy::missing_errors_doc, clippy::unused_async)]

mod activities;
mod domain;
mod runtime;
mod server;
mod workflows;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    server::run().await
}
