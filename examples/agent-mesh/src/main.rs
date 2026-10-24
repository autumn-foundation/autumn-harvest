//! The `agent-mesh` command line. See README.md.

use std::path::PathBuf;
use std::sync::Arc;

use agent_mesh::desk::Desk;
use agent_mesh::mesh::{self, DeskDelivery, MESH, MeshConfig, PrintSink};
use agent_mesh::scripted::ScriptedDesk;
use autoresearch::claude::{ClaudeModel, DEFAULT_MODEL};
use autumn_harvest::chrono::{DateTime, Duration, Utc};
use autumn_harvest_agent::{AgentModel, AgentOp, Approval, Control, sqlite};
use autumn_harvest_sqlite::SqliteRuntime;
use clap::{Parser, Subcommand, ValueEnum};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// An always-on support desk run by a durable agent mesh.
#[derive(Debug, Parser)]
#[command(name = "agent-mesh")]
struct Cli {
    /// The desk folder.
    #[arg(long, default_value = "desk", global = true)]
    dir: PathBuf,
    /// The model of every agent.
    #[arg(long, value_enum, default_value_t = ModelKind::Scripted, global = true)]
    model: ModelKind,
    /// The Claude model, with `--model claude`.
    #[arg(long, default_value = DEFAULT_MODEL, global = true)]
    claude_model: String,
    /// The most tokens of the mesh in one day.
    #[arg(long, default_value_t = 200_000, global = true)]
    daily_tokens: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ModelKind {
    /// The offline model. It needs no key.
    Scripted,
    /// Claude over the Messages API. Set `ANTHROPIC_API_KEY`.
    Claude,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Make the desk folder with sample tickets and notes.
    Init,
    /// Start the three agents.
    Start,
    /// Move the desk clock on, and run each wake that is due.
    Tick {
        /// How far to move the clock, in minutes.
        #[arg(long, default_value_t = 16)]
        minutes: i64,
    },
    /// Ask the triage agent to answer a ticket.
    Answer {
        /// The ticket id, such as `T-1`.
        ticket: String,
    },
    /// Decide the reply that waits for approval.
    Approve {
        /// Reject the reply, with this reason for the agent.
        #[arg(long)]
        reject: Option<String>,
    },
    /// Show the agents, the spend of the mesh and the replies.
    Status,
    /// Pause every agent of the mesh.
    Pause {
        /// Why, for the audit record.
        #[arg(long)]
        reason: String,
    },
    /// Resume every agent of the mesh.
    Resume,
    /// Run a whole session: init, start, tick, answer T-1, approve, status.
    Demo,
}

/// The desk clock: the wall clock, moved on by each `tick`. It never runs
/// back, so a timer that a tick fired stays fired.
fn now(desk: &Desk) -> DateTime<Utc> {
    let offset = std::fs::read_to_string(desk.root().join("clock.txt"))
        .ok()
        .and_then(|text| text.trim().parse::<i64>().ok())
        .unwrap_or(0);
    Utc::now() + Duration::minutes(offset)
}

fn move_clock(desk: &Desk, minutes: i64) -> std::io::Result<()> {
    let path = desk.root().join("clock.txt");
    let offset = std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| text.trim().parse::<i64>().ok())
        .unwrap_or(0);
    std::fs::write(path, (offset + minutes.max(0)).to_string())
}

fn model(cli: &Cli) -> Result<Arc<dyn AgentModel>, Error> {
    Ok(match cli.model {
        ModelKind::Scripted => Arc::new(ScriptedDesk::default()),
        ModelKind::Claude => {
            let key = std::env::var("ANTHROPIC_API_KEY")
                .map_err(|_| "set ANTHROPIC_API_KEY to use --model claude")?;
            Arc::new(ClaudeModel::new(key).model(&cli.claude_model))
        }
    })
}

fn runtime(cli: &Cli, desk: &Desk) -> Result<SqliteRuntime, Error> {
    let config = MeshConfig {
        daily_tokens: Some(cli.daily_tokens),
        ..MeshConfig::default()
    };
    let harness = mesh::harness(
        desk,
        model(cli)?,
        &config,
        Some(Arc::new(PrintSink)),
        Arc::new(DeskDelivery::new(desk.clone(), true)),
    );
    Ok(mesh::open(desk, harness)?)
}

async fn answer(cli: &Cli, desk: &Desk, id: &str) -> Result<(), Error> {
    let ticket = desk.ticket(id)?.ok_or_else(|| format!("no ticket {id}"))?;
    let mut rt = runtime(cli, desk)?;
    mesh::start(&mut rt, now(desk)).await?;
    // The op key is the ticket id, so a second ask of the same ticket sends
    // nothing new.
    mesh::ask_answer(&mut rt, &ticket, &format!("answer:{id}"))?;
    let settled = mesh::settle(&mut rt, now(desk)).await?;
    println!();
    if settled.approval.is_some() {
        println!("The reply waits for approval. Run `agent-mesh approve`, or `--reject <reason>`.");
    }
    Ok(())
}

async fn approve(cli: &Cli, desk: &Desk, reject: Option<String>) -> Result<(), Error> {
    let mut rt = runtime(cli, desk)?;
    let settled = mesh::settle(&mut rt, now(desk)).await?;
    let Some((exec, signal)) = settled.approval else {
        println!("Nothing waits for approval.");
        return Ok(());
    };
    let decision = reject.map_or(Approval::Approve, |reason| Approval::Reject { reason });
    mesh::decide(&mut rt, exec, &signal, &decision)?;
    mesh::settle(&mut rt, now(desk)).await?;
    println!();
    if let Some(state) = sqlite::state(&rt, &mesh::triage()).await? {
        println!("triage: {}", state.last_answer.unwrap_or_default());
    }
    Ok(())
}

async fn status(cli: &Cli, desk: &Desk) -> Result<(), Error> {
    let rt = runtime(cli, desk)?;
    for (id, state) in mesh::states(&rt).await? {
        let last: String = state
            .last_answer
            .unwrap_or_default()
            .chars()
            .take(70)
            .collect();
        // An agent id does not pad itself, so pad its text.
        println!(
            "{:<14} {:<8} ops={:<3} ticks={:<3} tokens={:<6} {last}",
            id.to_string(),
            state.status.name(),
            state.segments,
            state.ticks,
            state.tokens_used,
        );
    }
    let spent = mesh::spent_today(desk, now(desk)).await?;
    println!(
        "mesh {MESH}: {} tokens today, cap {}",
        spent.tokens, cli.daily_tokens
    );
    for ticket in desk.tickets()? {
        let state = if desk.reply(&ticket.id).is_some() {
            "replied"
        } else {
            "open"
        };
        println!("{} {state:<8} {}", ticket.id, ticket.subject);
    }
    Ok(())
}

async fn run(cli: &Cli) -> Result<(), Error> {
    let desk = Desk::new(&cli.dir);
    match &cli.command {
        Command::Init => {
            desk.init()?;
            println!("Made the desk in {}.", desk.root().display());
        }
        Command::Start => {
            let mut rt = runtime(cli, &desk)?;
            mesh::start(&mut rt, now(&desk)).await?;
            println!("The agents run.");
        }
        Command::Tick { minutes } => {
            move_clock(&desk, *minutes)?;
            let mut rt = runtime(cli, &desk)?;
            mesh::settle(&mut rt, now(&desk)).await?;
            println!();
        }
        Command::Answer { ticket } => answer(cli, &desk, ticket).await?,
        Command::Approve { reject } => approve(cli, &desk, reject.clone()).await?,
        Command::Status => status(cli, &desk).await?,
        Command::Pause { reason } => {
            let mut rt = runtime(cli, &desk)?;
            let pause = sqlite::pause_mesh(&mut rt, MESH, reason)?;
            mesh::settle(&mut rt, now(&desk)).await?;
            println!("Paused {} agents of {}.", pause.agents.len(), pause.mesh);
        }
        Command::Resume => {
            let mut rt = runtime(cli, &desk)?;
            for agent in mesh::agents() {
                sqlite::send(&mut rt, &agent, &AgentOp::Control(Control::Resume), None)?;
            }
            mesh::settle(&mut rt, now(&desk)).await?;
            println!("Resumed the mesh.");
        }
        Command::Demo => {
            desk.init()?;
            let mut rt = runtime(cli, &desk)?;
            mesh::start(&mut rt, now(&desk)).await?;
            drop(rt);
            println!("== A scheduled check");
            move_clock(&desk, 16)?;
            let mut rt = runtime(cli, &desk)?;
            mesh::settle(&mut rt, now(&desk)).await?;
            drop(rt);
            println!("\n== Answer T-1");
            answer(cli, &desk, "T-1").await?;
            println!("\n== Approve the reply");
            approve(cli, &desk, None).await?;
            println!("\n== Status");
            status(cli, &desk).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    // The future of a whole command is large, so it runs on the heap.
    match Box::pin(run(&cli)).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::ExitCode::FAILURE
        }
    }
}
