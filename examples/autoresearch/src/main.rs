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
use autoresearch::rules::KeepPolicy;
use autoresearch::runtime::{self, Refused, Step, session_id};
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
    /// Replace the keep rule of a running session.
    Policy {
        #[command(flatten)]
        lab: LabArgs,
        /// The session tag.
        #[arg(long)]
        tag: String,
        /// The adjusted gain must be larger than this.
        #[arg(long, default_value_t = KeepPolicy::default().min_improvement)]
        min_improvement: f64,
        /// The cost of each added line, in `val_bpb`.
        #[arg(long, default_value_t = KeepPolicy::default().line_cost)]
        line_cost: f64,
        /// A run above this peak memory is a discard.
        #[arg(long, default_value_t = KeepPolicy::default().max_memory_gb)]
        max_memory_gb: f64,
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
        Cmd::Hint { lab, tag, text } => post(&lab, &Command::Hint { tag, text }),
        Cmd::Stop { lab, tag, reason } => post(&lab, &Command::Stop { tag, reason }),
        Cmd::Policy {
            lab,
            tag,
            min_improvement,
            line_cost,
            max_memory_gb,
        } => {
            let policy = KeepPolicy {
                min_improvement,
                line_cost,
                max_memory_gb,
            };
            policy.validate()?;
            post(&lab, &Command::Policy { tag, policy })
        }
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

fn model(args: &ModelArgs, state: &Path) -> Result<Arc<dyn AgentModel>, String> {
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
            let model = ClaudeModel::new(key)
                .model(&args.claude_model)
                .turn_dir(state.join("claude-turns"));
            Ok(Arc::new(model))
        }
    }
}

fn harness(lab: &Lab, state: &Path, args: &ModelArgs) -> Result<AgentHarness, String> {
    let parts = HarnessParts {
        labs: vec![lab.clone()],
        state_dir: state.to_path_buf(),
    };
    agent::harness(model(args, state)?, &parts)
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

/// The session that `run` drives: the one that `--tag` names, the one
/// running session, or a new one.
fn pick_session(
    rt: &mut SqliteRuntime,
    lab: &Lab,
    session: &SessionArgs,
) -> Result<ExecutionId, String> {
    if let Some(tag) = &session.tag {
        if let Some((exec, id)) = runtime::find_session(rt, tag)? {
            println!("resuming {id}");
            return Ok(exec);
        }
        return start_session(rt, lab, session, tag);
    }
    // No tag: resume the one running session, so the same command resumes
    // a night that crossed midnight.
    match runtime::running_sessions(rt)?.as_slice() {
        [] => {
            let tag = chrono::Utc::now().format("%b%d").to_string().to_lowercase();
            start_session(rt, lab, session, &tag)
        }
        [(exec, tag)] => {
            println!("resuming the session with the tag {tag:?}");
            Ok(*exec)
        }
        several => {
            let tags: Vec<&str> = several.iter().map(|(_, tag)| tag.as_str()).collect();
            Err(format!(
                "several sessions are running: name one with --tag ({})",
                tags.join(", ")
            ))
        }
    }
}

fn start_session(
    rt: &mut SqliteRuntime,
    lab: &Lab,
    session: &SessionArgs,
    tag: &str,
) -> Result<ExecutionId, String> {
    let exec = runtime::start(rt, &config(lab, session, tag.to_string()))?;
    println!("started {} on branch {}", session_id(tag), session_id(tag));
    Ok(exec)
}

/// Resolves on Ctrl-C, or on SIGTERM on Unix.
async fn shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

async fn run(args: &LabArgs, session: &SessionArgs) -> Result<(), String> {
    let (lab, state, db) = open_lab(args)?;
    let mut rt = open_runtime(&db, harness(&lab, &state, &session.model)?)?;
    let mut exec = pick_session(&mut rt, &lab, session)?;
    let inbox = Inbox::new(&state)?;
    let mut rows = Rows::from_now(&lab);
    let mut asked = HashSet::new();
    let tick = Duration::from_millis(session.tick_ms.max(10));
    // The signal handlers stay in place for the whole run.
    let quit = shutdown();
    tokio::pin!(quit);
    loop {
        deliver(&mut rt, &inbox)?;
        let step = tokio::select! {
            step = runtime::drive(&mut rt, exec) => step?,
            () = &mut quit => {
                println!("stopped driving. The session is durable: run the same command to resume.");
                return Ok(());
            }
        };
        rows.print_new(&lab);
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
                    ask(&lab, &pending);
                }
                tokio::time::sleep(tick).await;
            }
            Step::Waiting => tokio::time::sleep(tick).await,
        }
    }
}

/// Print a gated call and the two commands that decide it.
fn ask(lab: &Lab, pending: &runtime::Pending) {
    let lab_arg = lab.root().display();
    println!(
        "waiting for a person: {} {}",
        pending.tool,
        printable(&pending.arguments.to_string())
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

/// `text` with each control, format and bidirectional character shown as
/// its code point. Model text then cannot move the cursor, hide a line, or
/// show code in an order other than its byte order.
fn printable(text: &str) -> String {
    text.chars()
        .map(|c| {
            if autoresearch::results::hidden(c) {
                format!("\\u{{{:04x}}}", u32::from(c))
            } else {
                c.to_string()
            }
        })
        .collect()
}

/// Deliver every inbox command. A delivered command is removed. A command
/// that can never land moves to `inbox/rejected`. A command that can land
/// later stays for the next tick.
fn deliver(rt: &mut SqliteRuntime, inbox: &Inbox) -> Result<(), String> {
    for (id, command) in inbox.pending()? {
        match runtime::apply(rt, &id, &command) {
            Ok(line) => {
                println!("inbox: {line}");
                inbox.done(&id)?;
            }
            Err(Refused::Permanent(why)) => {
                eprintln!("inbox: {why}. The command is set aside.");
                inbox.reject(&id)?;
            }
            Err(Refused::Transient(why)) => eprintln!("inbox: {why}. It is tried again."),
        }
    }
    Ok(())
}

/// The rows of `results.tsv` that the runner has printed.
struct Rows {
    printed: usize,
    bytes: u64,
}

impl Rows {
    /// Start after the rows that the file holds now.
    fn from_now(lab: &Lab) -> Self {
        Self {
            printed: lab.rows().map_or(0, |rows| rows.len()),
            bytes: Self::size(lab),
        }
    }

    fn size(lab: &Lab) -> u64 {
        std::fs::metadata(lab.root().join(autoresearch::lab::RESULTS)).map_or(0, |m| m.len())
    }

    /// Print the rows that landed since the last call. The file is read
    /// only when its size changed.
    fn print_new(&mut self, lab: &Lab) {
        let bytes = Self::size(lab);
        if bytes == self.bytes {
            return;
        }
        self.bytes = bytes;
        let rows = lab.rows().unwrap_or_default();
        for (n, row) in rows.iter().enumerate().skip(self.printed) {
            print_row(n, row);
        }
        self.printed = rows.len().max(self.printed);
    }
}

fn print_row(n: usize, row: &ResultRow) {
    println!(
        "  #{n:<3} {:<7} {:.6}  {:>5.1} GB  {}  {}",
        row.status.as_str(),
        row.val_bpb,
        row.memory_gb,
        row.commit,
        printable(&row.description)
    );
}

fn print_report(report: &SessionReport) {
    let counts = report.counts;
    println!(
        "done: {}: {} experiments, best val_bpb {:.6} at {} ({} kept, {} discarded, {} crashed)",
        report.branch,
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
    let mut rows = Rows::from_now(&lab);
    let tick = Duration::from_millis(session.tick_ms.max(10)).max(Duration::from_secs(1));
    let serving = async {
        loop {
            deliver(&mut rt, &inbox)?;
            if let Err(error) = rt.poll_once().await {
                eprintln!("poll: {error}");
            }
            for (exec, _) in runtime::running_sessions(&rt)? {
                if let Step::Approval(pending) = runtime::drive(&mut rt, exec).await?
                    && asked.insert(pending.token.clone())
                {
                    ask(&lab, &pending);
                }
            }
            rows.print_new(&lab);
            tokio::time::sleep(tick).await;
        }
    };
    // A stop drops the drive in flight. The trainer kills its process group.
    tokio::select! {
        result = serving => result,
        () = shutdown() => {
            println!("stopped serving. Every session is durable.");
            Ok(())
        }
    }
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
                "{id}  {state}  tag {}  phase {}  experiments {}  best_bpb {}  best {}",
                field("tag"),
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
        None => {
            runtime::latest_proposal(&rt)?.ok_or("no recorded proposal yet: run a session first")?
        }
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
