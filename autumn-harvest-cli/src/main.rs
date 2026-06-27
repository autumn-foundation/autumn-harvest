//! CLI tool.
use clap::Parser;

use autumn_harvest_cli::{Cli, run_cli};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(error) = run_cli(cli).await {
        let exit_code = error.exit_code();
        eprintln!("error: {error}");
        std::process::exit(exit_code);
    }
}
