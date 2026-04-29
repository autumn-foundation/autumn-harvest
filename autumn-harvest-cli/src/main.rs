//! The autumn-harvest-cli main entry point.
//!
//! Provides the `harvest` binary to interact with the Harvest management API.

use clap::Parser;

use autumn_harvest_cli::{Cli, run_cli};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = run_cli(cli).await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
