#![allow(clippy::missing_errors_doc, clippy::unused_async)]

use std::io::IsTerminal as _;

mod activities;
mod db;
mod domain;
mod runtime;
mod server;
mod webhooks;
mod workflows;

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .init();
    server::run().await
}
