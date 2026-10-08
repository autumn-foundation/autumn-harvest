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
