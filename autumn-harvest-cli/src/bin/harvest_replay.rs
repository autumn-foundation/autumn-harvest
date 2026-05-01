//! `harvest-replay` — offline replay-safety validator for `#[workflow]` functions.
//!
//! Loads a recorded event history from a JSON file (or, with the `db` feature,
//! directly from Postgres) and replays it against registered workflow handlers
//! to detect non-determinism before deploying a code change.
//!
//! # Usage
//!
//! ```text
//! cargo run --bin harvest-replay -- \
//!   --workflow onboarding \
//!   --history-source json \
//!   --json-path ./fixtures/onboarding_history.json
//! ```
//!
//! # Extending with your own workflows
//!
//! The binary must know your workflow handler functions to replay against them.
//! Copy this file into your application, add your handlers to `build_replayer()`,
//! and run with `cargo run --bin harvest-replay`.
//!
//! ```rust,no_run
//! # use autumn_harvest::testing::WorkflowReplayer;
//! fn build_replayer() -> WorkflowReplayer {
//!     WorkflowReplayer::new()
//!         // .register_fn("onboarding", onboarding_handler)
//!         // .register_fn("refund_saga", refund_saga_handler)
//! }
//! ```

use std::path::PathBuf;
use std::process;

use autumn_harvest::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use clap::{Parser, ValueEnum};

/// Offline replay-safety validator for harvest workflow functions.
///
/// Loads a recorded event history and replays it against the registered
/// workflow handlers to detect non-determinism before deploying.
#[derive(Debug, Parser)]
#[command(
    name = "harvest-replay",
    version,
    about = "Replay a recorded workflow history to verify replay safety"
)]
struct Args {
    /// The registered workflow name to replay against.
    #[arg(long)]
    workflow: Option<String>,

    /// Where to load the event history from.
    #[arg(long, value_enum, default_value = "json")]
    history_source: HistorySource,

    /// Path to a JSON `HistorySnapshot` file (required when --history-source json).
    #[arg(long)]
    json_path: Option<PathBuf>,

    /// The execution ID to replay (required when --history-source db).
    #[arg(long)]
    exec_id: Option<String>,

    /// Exit with code 1 on non-determinism or workflow failure.
    #[arg(long, default_value = "true")]
    fail_on_error: bool,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum HistorySource {
    /// Load history from a JSON `HistorySnapshot` file.
    Json,
    /// Load history directly from the Postgres event store (requires db feature).
    Db,
}

/// Register workflow handlers here before running a replay.
///
/// Add your `#[workflow]`-annotated functions with `.register_fn("name", handler)`
/// or use the `workflows![]` macro with `.register(workflows![...])`.
fn build_replayer() -> WorkflowReplayer {
    WorkflowReplayer::new()
    // Example: .register_fn("onboarding", my_crate::workflows::onboarding)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let replayer = build_replayer();

    let result = match args.history_source {
        HistorySource::Json => run_json_replay(&replayer, &args).await,
        HistorySource::Db => {
            eprintln!(
                "error: --history-source db requires the `db` feature and a Postgres \
                 connection. Use --history-source json with an exported history file instead."
            );
            process::exit(2);
        }
    };

    match result {
        Ok(()) => {}
        Err(msg) => {
            eprintln!("error: {msg}");
            process::exit(2);
        }
    }
}

async fn run_json_replay(replayer: &WorkflowReplayer, args: &Args) -> Result<(), String> {
    let path = args
        .json_path
        .as_ref()
        .ok_or("--json-path is required when --history-source json")?;

    let json = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

    // Optionally override the workflow name embedded in the snapshot.
    let report = if let Some(name) = &args.workflow {
        // Deserialise snapshot, override workflow name, then replay.
        let mut snapshot: HistorySnapshot = serde_json::from_str(&json)
            .map_err(|e| format!("failed to parse history JSON: {e}"))?;
        snapshot.workflow_name = name.clone();
        replayer
            .replay_from_events(&snapshot.workflow_name, snapshot.execution_id, snapshot.events)
            .await
    } else {
        replayer
            .replay_from_json(&json)
            .await
            .map_err(|e| format!("failed to parse history JSON: {e}"))?
    };

    // Print the report.
    println!("{report}");

    if args.fail_on_error {
        match &report.status {
            ReplayStatus::ReplaySucceeded => {}
            ReplayStatus::NonDeterminismDetected { .. } | ReplayStatus::WorkflowFailed { .. } => {
                process::exit(1);
            }
        }
    }

    Ok(())
}
