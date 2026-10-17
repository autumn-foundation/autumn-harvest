//! `autoresearch`: run Karpathy's research loop as durable Harvest workflows.
//!
//! ```text
//! autoresearch init lab                       # an offline lab with one commit
//! autoresearch run --lab lab --experiments 8  # a night of research, offline
//! autoresearch status --lab lab               # sessions, best result, rows
//! ```
//!
//! See `README.md` for every command.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use autoresearch::agent::{self, HarnessParts};
use autoresearch::claude::ClaudeModel;
use autoresearch::inbox::{Command, Inbox};
use autoresearch::lab::Lab;
use autoresearch::results::ResultRow;
use autoresearch::runtime::{self, Step, session_id};
use autoresearch::scripted::ScriptedResearcher;
use autoresearch::trainer::TrainerSpec;
use autoresearch::workflows::{SESSION, SessionConfig, SessionReport};
use autumn_harvest_agent::{AgentHarness, AgentModel};
use autumn_harvest_sqlite::{ExecutionFilter, ExecutionId, SqliteReader, SqliteRuntime};
use clap::{Parser, Subcommand, ValueEnum};

/// The trainer to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TrainerKind {
    /// The offline loss landscape. No GPU.
    Sim,
    /// `uv run train.py` in the lab, killed after ten minutes.
    Command,
}

/// The model of the agents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ModelKind {
    /// The offline scripted researcher. No key, no network.
    Scripted,
    /// Claude over the Messages API. Needs `ANTHROPIC_API_KEY`.
    Claude,
}

#[derive(Debug, Parser)]
#[command(
    name = "autoresearch",
    about = "Autonomous research as durable Harvest workflows"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

/// The options that every command that opens a lab takes.
#[derive(Debug, clap::Args)]
struct LabArgs {
    /// The lab: a git work tree with `train.py`.
    #[arg(long, env = "AUTORESEARCH_LAB")]
    lab: PathBuf,
    /// The database. The default is `<lab>/.autoresearch/harvest.db`.
    #[arg(long, env = "AUTORESEARCH_DB")]
    db: Option<PathBuf>,
}

/// The model of the agents.
#[derive(Debug, clap::Args)]
struct ModelArgs {
    /// The model of the agents.
    #[arg(long, value_enum, default_value_t = ModelKind::Scripted)]
    model: ModelKind,
    /// The Claude model id.
    #[arg(long, default_value = autoresearch::claude::DEFAULT_MODEL)]
    claude_model: String,
}

/// The options of a session.
#[derive(Debug, clap::Args)]
struct SessionArgs {
    /// The run tag. The default is the date, such as `oct17`.
    #[arg(long)]
    tag: Option<String>,
    /// Experiments after the baseline. Zero runs until `stop`.
    #[arg(long, default_value_t = 8)]
    experiments: u32,
    /// Continue as new after this many experiments.
    #[arg(long, default_value_t = 20)]
    per_run: u32,
    /// The trainer.
    #[arg(long, value_enum, default_value_t = TrainerKind::Sim)]
    trainer: TrainerKind,
    /// Seconds of wall time per simulated run.
    #[arg(long, default_value_t = 0)]
    train_seconds: u64,
    #[command(flatten)]
    model: ModelArgs,
    /// The GPU. It names the training mutex.
    #[arg(long, default_value = "gpu0")]
    gpu: String,
    /// The chance, in percent, of a bolder idea.
    #[arg(long, default_value_t = 10)]
    explore: u8,
    /// A data directory that must exist, such as `~/.cache/autoresearch`.
    #[arg(long)]
    data_dir: Option<String>,
    /// Seconds that a gated call waits for a person.
    #[arg(long, default_value_t = 900)]
    approval_timeout: u64,
    /// Do not start the morning-report agent at the end.
    #[arg(long)]
    no_report: bool,
    /// The poll interval while the session waits, in milliseconds.
    #[arg(long, default_value_t = 200)]
    tick_ms: u64,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Create an offline lab from the bundled template.
    Init {
        /// The directory to create.
        path: PathBuf,
    },
    /// Start or resume a session and drive it to its end.
    Run {
        #[command(flatten)]
        lab: LabArgs,
        #[command(flatten)]
        session: SessionArgs,
    },
    /// Serve the nightly schedule and the heartbeat until Ctrl-C.
    Serve {
        #[command(flatten)]
        lab: LabArgs,
        #[command(flatten)]
        session: SessionArgs,
    },
    /// Print the sessions, their best result and the results.
    Status {
        #[command(flatten)]
        lab: LabArgs,
    },
    /// Send an idea to a session.
    Hint {
        #[command(flatten)]
        lab: LabArgs,
        /// The session tag.
        #[arg(long)]
        tag: String,
        /// The idea, such as `paper:muon` or `rewind:<commit>`.
        text: String,
    },
    /// End a session after the experiment in flight.
    Stop {
        #[command(flatten)]
        lab: LabArgs,
        /// The session tag.
        #[arg(long)]
        tag: String,
        /// Why.
        #[arg(long, default_value = "stopped by the operator")]
        reason: String,
    },
    /// Release a gated tool call.
    Approve {
        #[command(flatten)]
        lab: LabArgs,
        /// The token that `run` printed.
        token: String,
    },
    /// Refuse a gated tool call.
    Deny {
        #[command(flatten)]
        lab: LabArgs,
        /// The token that `run` printed.
        token: String,
        /// Why. The model reads it.
        #[arg(long, default_value = "denied by the operator")]
        reason: String,
    },
    /// Re-drive a recorded proposal with a candidate `program.md`.
    Eval {
        #[command(flatten)]
        lab: LabArgs,
        #[command(flatten)]
        model: ModelArgs,
        /// The candidate `program.md`.
        #[arg(long)]
        candidate: PathBuf,
        /// The execution id of a recorded proposal. The default is the newest.
        #[arg(long)]
        run: Option<String>,
    },
    /// Replay every stored history with this build. Run it before a deploy.
    Verify {
        #[command(flatten)]
        lab: LabArgs,
    },
    /// Remove terminal runs older than a number of days.
    Purge {
        #[command(flatten)]
        lab: LabArgs,
        /// The age in days.
        #[arg(long, default_value_t = 30)]
        older_than_days: u64,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    // The agent glue runs each activity body with `block_in_place`, which
    // needs a multi-thread runtime.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: cannot start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(dispatch(cli.command)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(command: Cmd) -> Result<(), String> {
    match command {
        Cmd::Init { path } => init(&path),
        Cmd::Run { lab, session } => run(&lab, &session).await,
        Cmd::Serve { lab, session } => serve(&lab, &session).await,
        Cmd::Status { lab } => status(&lab),
        Cmd::Hint { lab, tag, text } => post(
            &lab,
            &Command::Hint {
                session: session_id(&tag),
                text,
            },
        ),
        Cmd::Stop { lab, tag, reason } => post(
            &lab,
            &Command::Stop {
                session: session_id(&tag),
                reason,
            },
        ),
        Cmd::Approve { lab, token } => post(&lab, &Command::Approve { token }),
        Cmd::Deny { lab, token, reason } => post(&lab, &Command::Deny { token, reason }),
        Cmd::Eval {
            lab,
            model,
            candidate,
            run,
        } => eval(&lab, &model, &candidate, run.as_deref()).await,
        Cmd::Verify { lab } => verify(&lab).await,
        Cmd::Purge {
            lab,
            older_than_days,
        } => purge(&lab, older_than_days),
    }
}

fn init(path: &Path) -> Result<(), String> {
    let lab = Lab::init(path)?;
    println!("Created an offline lab at {}.", lab.root().display());
    println!("Next: autoresearch run --lab {}", lab.root().display());
    Ok(())
}

/// The lab, its state directory and the database path.
fn open_lab(args: &LabArgs) -> Result<(Lab, PathBuf, PathBuf), String> {
    let lab = Lab::open(&args.lab)?;
    let state = lab.state_dir()?;
    let db = args.db.clone().unwrap_or_else(|| state.join("harvest.db"));
    Ok((lab, state, db))
}

fn model(args: &ModelArgs) -> Result<Arc<dyn AgentModel>, String> {
    match args.model {
        ModelKind::Scripted => Ok(Arc::new(ScriptedResearcher::default())),
        ModelKind::Claude => {
            // The key comes from the environment only: a process's arguments
            // are readable by every user of the host.
            let key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_default();
            let key = key.trim();
            if key.is_empty() {
                return Err("--model claude needs ANTHROPIC_API_KEY in the environment".into());
            }
            Ok(Arc::new(ClaudeModel::new(key).model(&args.claude_model)))
        }
    }
}

fn harness(lab: &Lab, state: &Path, args: &ModelArgs) -> Result<AgentHarness, String> {
    let parts = HarnessParts {
        labs: vec![lab.clone()],
        state_dir: state.to_path_buf(),
    };
    agent::harness(model(args)?, &parts)
}

/// Open the database as the single writer and register everything.
fn open_runtime(db: &Path, harness: AgentHarness) -> Result<SqliteRuntime, String> {
    let mut rt =
        SqliteRuntime::open(db).map_err(|e| format!("cannot open {}: {e}", db.display()))?;
    runtime::register(&mut rt, Arc::new(harness));
    Ok(rt)
}

fn config(lab: &Lab, args: &SessionArgs, tag: String) -> SessionConfig {
    let mut config = SessionConfig::new(lab.root().display().to_string(), tag);
    config.max_experiments = args.experiments;
    config.experiments_per_run = args.per_run;
    config.trainer = match args.trainer {
        TrainerKind::Sim => TrainerSpec::Simulated {
            seconds: args.train_seconds,
        },
        TrainerKind::Command => TrainerSpec::upstream(),
    };
    config.gpu.clone_from(&args.gpu);
    config.explore_percent = args.explore.min(100);
    config.data_dir.clone_from(&args.data_dir);
    config.agent.approval_timeout_secs = args.approval_timeout;
    config.report = !args.no_report;
    config
}

async fn run(args: &LabArgs, session: &SessionArgs) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let mut rt = open_runtime(&db, harness(&lab, &state, &session.model)?)?;
    let tag = session
        .tag
        .clone()
        .unwrap_or_else(|| chrono::Utc::now().format("%b%d").to_string().to_lowercase());
    let mut exec = if let Some(exec) = runtime::find_session(&rt, &tag)? {
        println!("resuming {}", session_id(&tag));
        exec
    } else {
        let exec = runtime::start(&mut rt, &config(&lab, session, tag.clone()))?;
        println!("started {} on branch autoresearch/{tag}", session_id(&tag));
        exec
    };
    let inbox = Inbox::new(&state)?;
    let mut printed = 0;
    let mut asked = HashSet::new();
    let tick = Duration::from_millis(session.tick_ms.max(10));
    loop {
        deliver(&mut rt, &inbox)?;
        let step = tokio::select! {
            step = runtime::drive(&mut rt, exec) => step?,
            _ = tokio::signal::ctrl_c() => {
                println!("stopped driving. The session is durable: run the same command to resume.");
                return Ok(());
            }
        };
        printed = print_rows(&lab, printed);
        match step {
            Step::Done(report) => {
                print_report(&report);
                if report.report_run.is_some() {
                    rt.run_until_idle().await.map_err(|e| e.to_string())?;
                    println!(
                        "report: {}",
                        autoresearch::stores::FileDelivery::dir(&lab)?.display()
                    );
                }
                return Ok(());
            }
            Step::Failed(error) => return Err(format!("the session failed: {error}")),
            Step::Continued(next) => {
                println!("continued as new: {next}");
                exec = next;
            }
            Step::Approval(pending) => {
                if asked.insert(pending.token.clone()) {
                    let lab_arg = lab.root().display();
                    println!(
                        "waiting for a person: {} {}",
                        pending.tool, pending.arguments
                    );
                    println!(
                        "  approve: autoresearch approve --lab {lab_arg} '{}'",
                        pending.token
                    );
                    println!(
                        "  deny:    autoresearch deny --lab {lab_arg} '{}'",
                        pending.token
                    );
                }
                tokio::time::sleep(tick).await;
            }
            Step::Waiting => tokio::time::sleep(tick).await,
        }
    }
}

/// Deliver every inbox command, then remove it. A command that cannot be
/// delivered is reported and removed: it names a run that is gone.
fn deliver(rt: &mut SqliteRuntime, inbox: &Inbox) -> Result<(), String> {
    for (id, command) in inbox.pending()? {
        match runtime::apply(rt, &id, &command) {
            Ok(line) => println!("inbox: {line}"),
            Err(error) => eprintln!("inbox: {error}"),
        }
        inbox.done(&id)?;
    }
    Ok(())
}

/// Print the rows of `results.tsv` after the first `printed`. Returns the
/// new count.
fn print_rows(lab: &Lab, printed: usize) -> usize {
    let rows = lab.rows().unwrap_or_default();
    for (n, row) in rows.iter().enumerate().skip(printed) {
        print_row(n, row);
    }
    rows.len().max(printed)
}

fn print_row(n: usize, row: &ResultRow) {
    println!(
        "  #{n:<3} {:<7} {:.6}  {:>5.1} GB  {}  {}",
        row.status.as_str(),
        row.val_bpb,
        row.memory_gb,
        row.commit,
        row.description
    );
}

fn print_report(report: &SessionReport) {
    let counts = report.counts;
    println!(
        "done: autoresearch/{}: {} experiments, best val_bpb {:.6} at {} ({} kept, {} discarded, {} crashed)",
        report.tag,
        report.experiments,
        report.best.val_bpb,
        report.best.commit,
        counts.kept,
        counts.discarded,
        counts.crashed
    );
    if let Some(reason) = &report.stopped {
        println!("stopped: {reason}");
    }
}

async fn serve(args: &LabArgs, session: &SessionArgs) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let mut rt = open_runtime(&db, harness(&lab, &state, &session.model)?)?;
    let base = config(&lab, session, String::new());
    rt.upsert_schedule(&runtime::nightly(&base)?)
        .map_err(|e| e.to_string())?;
    rt.upsert_schedule(&runtime::heartbeat(lab.root())?)
        .map_err(|e| e.to_string())?;
    println!(
        "serving {}: a session at 23:00 UTC, a heartbeat every 30 minutes",
        lab.root().display()
    );
    let inbox = Inbox::new(&state)?;
    let mut asked = HashSet::new();
    let mut printed = lab.rows().unwrap_or_default().len();
    let tick = Duration::from_millis(session.tick_ms.max(10)).max(Duration::from_secs(1));
    loop {
        deliver(&mut rt, &inbox)?;
        if let Err(error) = rt.poll_once().await {
            eprintln!("poll: {error}");
        }
        for exec in running_sessions(&rt)? {
            if let Step::Approval(pending) = runtime::drive(&mut rt, exec).await?
                && asked.insert(pending.token.clone())
            {
                println!(
                    "waiting for a person: {} {}",
                    pending.tool, pending.arguments
                );
                println!(
                    "  approve: autoresearch approve --lab {} '{}'",
                    lab.root().display(),
                    pending.token
                );
            }
        }
        printed = print_rows(&lab, printed);
        tokio::select! {
            () = tokio::time::sleep(tick) => {}
            _ = tokio::signal::ctrl_c() => {
                println!("stopped serving. Every session is durable.");
                return Ok(());
            }
        }
    }
}

fn running_sessions(rt: &SqliteRuntime) -> Result<Vec<ExecutionId>, String> {
    let filter = ExecutionFilter::new()
        .workflow_name(SESSION)
        .state("RUNNING");
    let page = rt.list_executions(&filter).map_err(|e| e.to_string())?;
    Ok(page
        .executions
        .iter()
        .filter_map(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .collect())
}

/// Read the sessions beside a running writer, with a read-only handle.
fn status(args: &LabArgs) -> Result<(), String> {
    let (lab, _, db) = open_lab(args)?;
    if db.exists() {
        let reader = SqliteReader::open(&db).map_err(|e| e.to_string())?;
        let page = reader
            .list_executions(&ExecutionFilter::new().workflow_name(SESSION).limit(20))
            .map_err(|e| e.to_string())?;
        for run in &page.executions {
            let id = run.workflow_id.clone().into_value().unwrap_or_default();
            let state = run.state.clone().into_value().unwrap_or_default();
            let attrs = run
                .search_attributes
                .clone()
                .into_value()
                .unwrap_or_default();
            let field = |key: &str| {
                attrs
                    .get(key)
                    .map_or_else(|| "-".to_string(), ToString::to_string)
            };
            println!(
                "{id}  {state}  phase {}  experiments {}  best_bpb {}  best {}",
                field("phase"),
                field("experiments"),
                field("best_bpb"),
                field("best_commit")
            );
        }
        if page.executions.is_empty() {
            println!("no sessions yet");
        }
    } else {
        println!("no sessions yet");
    }
    let rows = lab.rows()?;
    let skip = rows.len().saturating_sub(10);
    for (n, row) in rows.iter().enumerate().skip(skip) {
        print_row(n, row);
    }
    Ok(())
}

fn post(args: &LabArgs, command: &Command) -> Result<(), String> {
    let (_, state, _) = open_lab(args)?;
    let id = Inbox::new(&state)?.post(command)?;
    println!("queued {id}: the runner delivers it on its next tick");
    Ok(())
}

async fn eval(
    args: &LabArgs,
    model: &ModelArgs,
    candidate: &Path,
    run: Option<&str>,
) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let program = std::fs::read_to_string(candidate)
        .map_err(|e| format!("cannot read {}: {e}", candidate.display()))?;
    let rt = open_runtime(&db, harness(&lab, &state, model)?)?;
    let run = match run {
        Some(id) => id
            .parse::<ExecutionId>()
            .map_err(|e| format!("{id} is not a run id: {e}"))?,
        None => *runtime::proposals(&rt)?
            .first()
            .ok_or("no recorded proposal yet: run a session first")?,
    };
    let history = rt.load_history(run).map_err(|e| e.to_string())?;
    drop(rt);
    let evaluation = runtime::eval(&history, harness(&lab, &state, model)?, &program).await?;
    println!("run: {run}");
    println!("turns: {}", evaluation.turns.len());
    match evaluation.first_divergence {
        Some(turn) => println!("first divergence: turn {turn}"),
        None => println!("first divergence: none"),
    }
    println!("the ends differ: {}", evaluation.end_diverged);
    Ok(())
}

async fn verify(args: &LabArgs) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let scripted = ModelArgs {
        model: ModelKind::Scripted,
        claude_model: String::new(),
    };
    let rt = open_runtime(&db, harness(&lab, &state, &scripted)?)?;
    let runs = runtime::stored_runs(&rt)?;
    drop(rt);
    let report = runtime::verify(runs).await;
    for failure in &report.failures {
        println!("FAILED {failure}");
    }
    println!(
        "replayed {} histories, {} failed",
        report.replayed,
        report.failures.len()
    );
    if report.failures.is_empty() {
        Ok(())
    } else {
        Err("some histories do not replay with this build".into())
    }
}

fn purge(args: &LabArgs, days: u64) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let scripted = ModelArgs {
        model: ModelKind::Scripted,
        claude_model: String::new(),
    };
    let mut rt = open_runtime(&db, harness(&lab, &state, &scripted)?)?;
    let removed = rt
        .purge_terminal(Duration::from_secs(days.saturating_mul(86_400)))
        .map_err(|e| e.to_string())?;
    println!("removed {removed} runs older than {days} days");
    Ok(())
}
