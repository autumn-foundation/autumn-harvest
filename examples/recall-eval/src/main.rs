//! Run the long-horizon recall eval of issue #2455 and print its table.
//!
//! With `--scripted`, the eval runs on `ReaderModel`, which needs no key.
//! Without it, the eval runs on Claude over the Messages API, with the key
//! in `ANTHROPIC_API_KEY`. Each call costs money. The output is a Markdown
//! section for `docs/performance-agent-recall.md`.

use std::path::PathBuf;
use std::sync::Arc;

use autoresearch::claude::{ClaudeModel, DEFAULT_MODEL};
use autumn_harvest_agent::recall::{
    self, EPISODES, MEMORY, NOTES, ReaderModel, RecallFixture, WINDOW,
};
use autumn_harvest_agent::{AgentModel, sqlite};
use autumn_harvest_sqlite::SqliteRuntime;
use chrono::{DateTime, Utc};
use clap::Parser;

/// The command line.
#[derive(Debug, Parser)]
#[command(about = "Measure what a durable agent forgets across ops (issue #2455)")]
struct Args {
    /// Run on the scripted `ReaderModel`. It needs no key and no network.
    #[arg(long)]
    scripted: bool,
    /// The Claude model.
    #[arg(long, default_value = DEFAULT_MODEL)]
    model: String,
    /// The API key. Only a real-model run reads it.
    #[arg(long, env = "ANTHROPIC_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
    /// The window of each config, in messages.
    #[arg(long, default_value_t = 2)]
    window: u32,
    /// The SQLite file. The default is a new temporary file. A rerun with
    /// the same file and inputs calls no model for a finished item.
    #[arg(long)]
    db: Option<PathBuf>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let (model, name): (Arc<dyn AgentModel>, String) = if args.scripted {
        (ReaderModel::new(), "ReaderModel (scripted)".to_owned())
    } else {
        let key = args
            .api_key
            .clone()
            .ok_or("a real-model run needs ANTHROPIC_API_KEY, or pass --scripted")?;
        let model = ClaudeModel::new(key).model(args.model.clone());
        (Arc::new(model), args.model.clone())
    };
    let temp = tempfile::tempdir()?;
    let db = args.db.unwrap_or_else(|| temp.path().join("recall.db"));
    let section = run(model, &name, args.window, &db, Utc::now()).await?;
    println!("{section}");
    Ok(())
}

/// Run the sweep of the four configs on `model` and render its section.
async fn run(
    model: Arc<dyn AgentModel>,
    name: &str,
    window: u32,
    db: &std::path::Path,
    at: DateTime<Utc>,
) -> Result<String, Box<dyn std::error::Error>> {
    let fixture = RecallFixture::builtin()?;
    let harness = Arc::new(recall::harness(model, window));
    let mut rt = SqliteRuntime::open(db)?;
    sqlite::register(&mut rt, Arc::clone(&harness));
    let configs = [WINDOW, NOTES, MEMORY, EPISODES].map(str::to_owned);
    let report = recall::sweep(&mut rt, &harness, &fixture, &configs).await;
    Ok(render(name, window, at, &report))
}

/// The Markdown section of one run: the model, the date, the config and
/// the table.
fn render(name: &str, window: u32, at: DateTime<Utc>, report: &recall::RecallReport) -> String {
    format!(
        "### {name}, {date}\n\n\
         - Model: `{name}`\n\
         - Date: {date}\n\
         - Config: window {window} messages. `notes` adds `ContextReset::new(1, 90)`. \
         `memory` adds store memory and the `memory` tool. `episodes` adds store memory \
         and `EpisodeSettings::default().every(1)`.\n\
         - Fixture: the built-in fixture, {items} items. Scorer: `ExactMatch`.\n\n\
         {table}",
        date = at.format("%Y-%m-%d"),
        items = report.items.len(),
        table = report.to_markdown(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn a_scripted_run_renders_the_model_the_date_the_config_and_the_table() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let at = DateTime::parse_from_rfc3339("2026-11-04T00:00:00Z")
            .expect("a date")
            .with_timezone(&Utc);
        let section = run(
            ReaderModel::new(),
            "reader",
            2,
            &dir.path().join("r.db"),
            at,
        )
        .await
        .expect("the run ends");
        assert!(section.starts_with("### reader, 2026-11-04"), "{section}");
        assert!(section.contains("window 2 messages"), "{section}");
        assert!(
            section.contains("| item | window | notes | memory | episodes |"),
            "{section}"
        );
        assert!(
            section.contains("| mean | 0.17 (6/6) | 1.00 (6/6) | 0.17 (6/6) | 1.00 (6/6) |"),
            "{section}"
        );
    }
}
