//! The `harvest-rhai` command line.
//!
//! Each command opens the run database, does one thing and exits. A run that
//! waits for a signal exits with [`WAITING`]. A later `approve` sends the
//! signal and drives the run on. The database allows one writer, so `status`
//! and `history` read through a read-only handle beside it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use autumn_harvest::{ExecutionId, WorkflowEvent};
use autumn_harvest_sqlite::{
    ExecutionFilter, LogFilter, RunState, SqliteError, SqliteReader, SqliteRuntime, StartOptions,
};
use chrono::Utc;
use serde_json::{Value, json};
use smop::prelude::{Parser, Subcommand, clap};
use std::io::Write as _;

use crate::{Echo, ScriptInput, WORKFLOW, bridge};

/// The exit code of a run that completed.
pub const COMPLETED: u8 = 0;
/// The exit code of a run that failed, timed out, or was cancelled.
pub const FAILED: u8 = 1;
/// The exit code of a usage or database error.
pub const ERROR: u8 = 2;
/// The exit code of a run that waits for a signal, or for a timer with
/// `--no-wait`.
pub const WAITING: u8 = 3;

/// The most log lines that `history` prints.
const LOG_PAGE: u32 = 1_000;

/// Why a command stopped early.
#[derive(Debug)]
enum Stop {
    /// Standard output closed, as with `| head`. The command ends quietly.
    Closed,
    /// A usage or database error.
    Error(String),
}

impl From<String> for Stop {
    fn from(message: String) -> Self {
        Self::Error(message)
    }
}

/// Write one line to standard output.
///
/// # Errors
///
/// Returns [`Stop::Closed`] for a closed pipe, and [`Stop::Error`] for any
/// other write error.
fn emit(line: std::fmt::Arguments<'_>) -> Result<(), Stop> {
    let mut out = std::io::stdout().lock();
    match out.write_fmt(line).and_then(|()| out.write_all(b"\n")) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::BrokenPipe => Err(Stop::Closed),
        Err(err) => Err(Stop::Error(format!("standard output: {err}"))),
    }
}

/// `println!` that returns [`Stop`] from the calling function on a failed write.
macro_rules! say {
    ($($arg:tt)*) => {
        emit(format_args!($($arg)*))?
    };
}

/// Run Rhai scripts that survive a crash.
#[derive(Debug, Parser)]
#[command(name = "harvest-rhai", version)]
pub struct Cli {
    /// The run database. Its directory is created when it is missing.
    #[arg(long, global = true, default_value = ".harvest/scripts.db")]
    pub db: PathBuf,
    /// The command to run.
    #[command(subcommand)]
    pub command: Command,
}

/// The `harvest-rhai` commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start a script and drive it until it ends or waits.
    ///
    /// With `--id`, a second `run` of the same id resumes the first run.
    Run {
        /// The script file.
        script: PathBuf,
        /// A name for the run. `approve`, `status` and `history` accept it.
        #[arg(long)]
        id: Option<String>,
        /// Exit at a timer, instead of sleeping until it fires.
        #[arg(long)]
        no_wait: bool,
        /// The script arguments, read as the `ARGS` array.
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Drive a run on from its last saved call.
    Resume {
        /// The run id or name.
        run: String,
        /// Exit at a timer, instead of sleeping until it fires.
        #[arg(long)]
        no_wait: bool,
    },
    /// Send a signal to a run, then drive it on.
    Approve {
        /// The run id or name.
        run: String,
        /// The signal name that the script waits for.
        #[arg(long, default_value = "approve")]
        signal: String,
        /// The signal payload, as JSON. `wait_for` returns it.
        #[arg(long, default_value = "true")]
        payload: String,
        /// Exit at a timer, instead of sleeping until it fires.
        #[arg(long)]
        no_wait: bool,
    },
    /// List the runs, or show one run and what it waits for.
    Status {
        /// The run id or name. Omit it to list every run.
        run: Option<String>,
    },
    /// Print the saved calls and the log lines of one run.
    History {
        /// The run id or name.
        run: String,
    },
}

/// Run the command line and return the process exit code.
pub async fn main(cli: Cli) -> ExitCode {
    match dispatch(cli).await {
        Ok(code) => ExitCode::from(code),
        Err(Stop::Closed) => ExitCode::from(COMPLETED),
        Err(Stop::Error(error)) => {
            eprintln!("harvest-rhai: {error}");
            ExitCode::from(ERROR)
        }
    }
}

/// Run one command.
async fn dispatch(cli: Cli) -> Result<u8, Stop> {
    match cli.command {
        Command::Run {
            script,
            id,
            no_wait,
            args,
        } => run(&cli.db, &script, id, args, !no_wait).await,
        Command::Resume { run, no_wait } => {
            let mut rt = open_live(&cli.db)?;
            let exec = resolve(&cli.db, &run)?;
            drive(&mut rt, &cli.db, exec, !no_wait).await
        }
        Command::Approve {
            run,
            signal,
            payload,
            no_wait,
        } => {
            let payload: Value =
                serde_json::from_str(&payload).map_err(|e| format!("--payload: {e}"))?;
            let mut rt = open_live(&cli.db)?;
            let exec = resolve(&cli.db, &run)?;
            rt.send_signal(exec, &signal, payload).map_err(text)?;
            drive(&mut rt, &cli.db, exec, !no_wait).await
        }
        Command::Status { run: None } => list(&cli.db).map(|()| COMPLETED),
        Command::Status { run: Some(run) } => show(&cli.db, &run).await.map(|()| COMPLETED),
        Command::History { run } => history(&cli.db, &run).map(|()| COMPLETED),
    }
}

/// `run`: start the script, or find the run with the same `--id`, then drive.
async fn run(
    db: &Path,
    script: &Path,
    id: Option<String>,
    args: Vec<String>,
    wait: bool,
) -> Result<u8, Stop> {
    let source = smop::fs::read_string(script).map_err(|e| format!("{e:#}"))?;
    bridge::check(&source).map_err(|e| format!("{}: {e}", script.display()))?;
    let cwd = std::env::current_dir().map_err(|e| format!("current directory: {e}"))?;
    let input = ScriptInput {
        path: script.display().to_string(),
        source,
        args,
        cwd: cwd.display().to_string(),
        max_operations: bridge::MAX_OPERATIONS,
    };
    let mut options = StartOptions::new().memo(json!({ "script": input.path }));
    if let Some(id) = id {
        options = options.workflow_id(id);
    }
    let input = serde_json::to_value(&input).map_err(|e| e.to_string())?;

    let mut rt = open_live(db)?;
    let started = rt
        .start_workflow_with_options(WORKFLOW, input, options)
        .map_err(text)?;
    if started.created {
        eprintln!("run {}", started.exec_id);
    } else {
        eprintln!("run {} exists; resuming it", started.exec_id);
    }
    drive(&mut rt, db, started.exec_id, wait).await
}

/// Drive `exec` until it ends, or until it waits for input from outside.
async fn drive(
    rt: &mut SqliteRuntime,
    db: &Path,
    exec: ExecutionId,
    wait: bool,
) -> Result<u8, Stop> {
    loop {
        match rt.run_until_blocked(exec).await.map_err(text)? {
            RunState::Completed(value) => {
                if !value.is_null() {
                    say!("{}", render(&value));
                }
                return Ok(COMPLETED);
            }
            RunState::Failed(error) => {
                eprintln!("run {exec} failed: {error}");
                return Ok(FAILED);
            }
            RunState::WaitingSignal(signal) => {
                eprintln!("run {exec} waits for signal `{signal}`. Send it with:");
                eprintln!("  harvest-rhai approve {exec} --signal {signal}");
                return Ok(WAITING);
            }
            RunState::WaitingTimer if !wait => {
                eprintln!("run {exec} waits on a timer. Continue it with:");
                eprintln!("  harvest-rhai resume {exec}");
                return Ok(WAITING);
            }
            RunState::WaitingTimer => sleep_to_next_timer(db, exec).await?,
            RunState::InProgress => {}
            RunState::TimedOut(reason)
            | RunState::Cancelled(reason)
            | RunState::Terminated(reason) => {
                eprintln!("run {exec} ended: {reason}");
                return Ok(FAILED);
            }
            other => return Err(format!("run {exec} is in an unexpected state: {other:?}").into()),
        }
    }
}

/// Sleep until the next unfired timer of `exec` is due.
async fn sleep_to_next_timer(db: &Path, exec: ExecutionId) -> Result<(), String> {
    let pending = reader(db)?.pending_work(exec).map_err(text)?;
    let Some(due) = pending.timers.iter().map(|t| t.fire_at).min() else {
        return Ok(());
    };
    let left = (due - Utc::now()).to_std().unwrap_or_default();
    eprintln!("run {exec} sleeps for {:.1}s", left.as_secs_f64());
    tokio::time::sleep(left).await;
    Ok(())
}

/// `status` with no run: one line per run, newest first.
///
/// A database that does not exist yet holds no run, so it lists nothing.
fn list(db: &Path) -> Result<(), Stop> {
    if !db.exists() {
        return Ok(());
    }
    let page = reader(db)?
        .list_executions(&ExecutionFilter::new().workflow_name(WORKFLOW))
        .map_err(text)?;
    for run in page.executions {
        let script = run
            .memo
            .into_value()
            .and_then(|memo| {
                memo.get("script")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .unwrap_or_default();
        say!(
            "{}  {:<10}  {:<20}  {}",
            run.exec_id.into_value().unwrap_or_default(),
            run.state.into_value().unwrap_or_default(),
            run.workflow_id.into_value().unwrap_or_default(),
            script,
        );
    }
    Ok(())
}

/// `status` with a run: its state, what it waits for, and its progress.
async fn show(db: &Path, run: &str) -> Result<(), Stop> {
    let exec = resolve(db, run)?;
    let reader = reader(db)?;
    let described = reader.describe(exec).map_err(text)?;
    say!("run       {exec}");
    say!(
        "state     {}",
        described.summary.state.into_value().unwrap_or_default()
    );
    if let Some(error) = described.error.into_value() {
        say!("error     {error}");
    }
    let pending = reader.pending_work(exec).map_err(text)?;
    for timer in &pending.timers {
        say!("timer     {} fires at {}", timer.timer_id, timer.fire_at);
    }
    for task in &pending.tasks {
        say!("call      {task:?}");
    }
    match crate::status(db, exec).await {
        Ok(status) => {
            say!("calls     {}", status["calls"]);
            if let Some(progress) = status["progress"].as_str().filter(|p| !p.is_empty()) {
                say!("progress  {progress}");
            }
        }
        Err(note) => say!("progress  unavailable: {note}"),
    }
    Ok(())
}

/// `history`: the saved calls of one run, then its log lines.
fn history(db: &Path, run: &str) -> Result<(), Stop> {
    let exec = resolve(db, run)?;
    let reader = reader(db)?;
    let mut calls: HashMap<String, (String, Value)> = HashMap::new();
    let mut step = 0_u32;
    for event in reader.load_history(exec).map_err(text)? {
        match event {
            WorkflowEvent::ActivityScheduled {
                activity_id,
                name,
                input,
                ..
            } => {
                calls.insert(activity_id.to_string(), (name, input));
            }
            WorkflowEvent::ActivityCompleted {
                activity_id,
                output,
            } => {
                step += 1;
                let (name, input) = calls.remove(&activity_id.to_string()).unwrap_or_default();
                say!(
                    "{step:>3}. {}  ->  {}",
                    call_line(&name, &input),
                    render(&output)
                );
            }
            WorkflowEvent::ActivityFailed {
                activity_id, error, ..
            } => {
                step += 1;
                let (name, input) = calls.remove(&activity_id.to_string()).unwrap_or_default();
                say!("{step:>3}. {}  FAILED  {error}", call_line(&name, &input));
            }
            WorkflowEvent::TimerStarted {
                timer_id,
                duration_secs,
            } => {
                step += 1;
                say!("{step:>3}. sleep({duration_secs})  [{timer_id}]");
            }
            WorkflowEvent::SignalReceived {
                signal_name,
                payload,
            } => {
                step += 1;
                say!(
                    "{step:>3}. signal `{signal_name}`  ->  {}",
                    render(&payload)
                );
            }
            WorkflowEvent::WorkflowCompleted { output } => {
                say!("     completed  ->  {}", render(&output));
            }
            WorkflowEvent::WorkflowFailed { error, .. } => say!("     failed  {error}"),
            _ => {}
        }
    }
    for line in reader
        .workflow_logs(exec, &LogFilter::new(LOG_PAGE))
        .map_err(text)?
    {
        say!("log  {}", line.message);
    }
    Ok(())
}

/// One saved call, as a script line.
fn call_line(name: &str, input: &Value) -> String {
    let arg = |key: &str| {
        input
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    match name {
        crate::host::SH => format!("sh({:?})", arg("cmd")),
        crate::host::READ_FILE => format!("read_file({:?})", arg("path")),
        crate::host::WRITE_FILE => format!("write_file({:?}, ..)", arg("path")),
        crate::host::ENV => format!("env({:?})", arg("name")),
        other => format!("{other}({input})"),
    }
}

/// A value on one line. A string prints without quotes, cut to 120 chars.
fn render(value: &Value) -> String {
    let text = match value {
        Value::String(text) => text.replace('\n', "\\n"),
        other => other.to_string(),
    };
    if text.chars().count() > 120 {
        format!("{}...", text.chars().take(117).collect::<String>())
    } else {
        text
    }
}

/// Open the writer with the `print` sink installed.
fn open_live(db: &Path) -> Result<SqliteRuntime, String> {
    if let Some(dir) = db.parent().filter(|d| !d.as_os_str().is_empty()) {
        smop::fs::create_dir_all(dir).map_err(|e| format!("{e:#}"))?;
    }
    let mut rt = crate::open(db).map_err(text)?;
    crate::set_echo(
        &mut rt,
        Echo(Arc::new(|line| {
            // A closed pipe drops the line. The run log keeps it.
            let _ = writeln!(std::io::stdout(), "{line}");
        })),
    );
    Ok(rt)
}

/// A read-only handle beside the writer.
fn reader(db: &Path) -> Result<SqliteReader, String> {
    SqliteReader::open(db).map_err(text)
}

/// The execution that `run` names: an execution id, or a `--id` name.
fn resolve(db: &Path, run: &str) -> Result<ExecutionId, String> {
    if let Ok(exec) = run.parse::<ExecutionId>() {
        return Ok(exec);
    }
    reader(db)?
        .list_executions(
            &ExecutionFilter::new()
                .workflow_name(WORKFLOW)
                .workflow_id(run),
        )
        .map_err(text)?
        .executions
        .first()
        .and_then(autumn_harvest_sqlite::ExecutionSummary::execution_id)
        .ok_or_else(|| format!("no run is named `{run}`"))
}

/// A runtime error as one line of text.
// `map_err` hands the error over by value.
#[allow(clippy::needless_pass_by_value)]
fn text(error: SqliteError) -> String {
    error.to_string()
}
