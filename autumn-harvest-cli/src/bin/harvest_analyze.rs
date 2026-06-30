//! `harvest-analyze` — offline history analyzer for `#[workflow]` functions.
//!
//! Loads a recorded event history from a JSON file and runs the workflow history
//! analyzer to identify anti-patterns such as excessive retries, large payloads,
//! suspicious zero-second timers, and sequential activity execution.
//!
//! # Usage
//!
//! ```text
//! cargo run --bin harvest-analyze -- \
//!   --json-path ./fixtures/onboarding_history.json
//! ```

use std::path::PathBuf;
use std::process;

use autumn_harvest::analyzer::HistoryAnalyzer;
use autumn_harvest::event::WorkflowEvent;
use autumn_harvest::history_export::HistoryExportDocument;
use autumn_harvest::testing::HistorySnapshot;
use clap::Parser;

/// Offline history analyzer for harvest workflow functions.
///
/// Loads a recorded event history and analyzes it against anti-pattern rules.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the JSON fixture file containing a `HistorySnapshot` or `HistoryExportDocument`.
    #[arg(long)]
    json_path: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let json = std::fs::read_to_string(&args.json_path)?;
    let events: Vec<WorkflowEvent> = if json.contains("\"schema\":") {
        // Try parsing as HistoryExportDocument
        let doc: HistoryExportDocument = serde_json::from_str(&json)?;
        doc.events
            .into_iter()
            .map(serde_json::from_value)
            .collect::<Result<Vec<_>, _>>()?
    } else if json.contains("\"events\":") {
        // Try parsing as HistorySnapshot
        let snapshot: HistorySnapshot = serde_json::from_str(&json)?;
        snapshot.events
    } else {
        // Assume it's just a raw JSON array of events
        serde_json::from_str(&json)?
    };

    println!(
        "Loaded {} events from {:?}",
        events.len(),
        args.json_path.display()
    );

    let analyzer = HistoryAnalyzer::with_default_rules();
    let warnings = analyzer.analyze(&events);

    if warnings.is_empty() {
        println!("✅ No warnings found. Workflow history looks healthy!");
        process::exit(0);
    }

    println!("⚠️ Found {} warning(s):", warnings.len());
    for warning in &warnings {
        println!("  - [{}] {}", warning.rule_name, warning.message);
    }

    process::exit(0);
}
