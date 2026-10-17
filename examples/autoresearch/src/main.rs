//! `autoresearch`: run Karpathy's research loop as durable Harvest workflows.
//!
//! ```text
//! autoresearch init lab                       # an offline lab with one commit
//! autoresearch run --lab lab --experiments 8  # a night of research, offline
//! autoresearch status --lab lab               # sessions, best result, rows
//! ```
//!
//! See `README.md` for every command.

use std::path::PathBuf;
use std::process::ExitCode;

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
#[command(name = "autoresearch", about = "Autonomous research as durable Harvest workflows")]
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
    /// The model of the agents.
    #[arg(long, value_enum, default_value_t = ModelKind::Scripted)]
    model: ModelKind,
    /// The Claude model id.
    #[arg(long, default_value = autoresearch::claude::DEFAULT_MODEL)]
    claude_model: String,
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
    let _ = cli;
    eprintln!("green phase");
    ExitCode::FAILURE
}
