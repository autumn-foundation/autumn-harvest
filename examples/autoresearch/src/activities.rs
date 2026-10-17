//! The activities: every step that touches git, the trainer or a file.
//!
//! Each input carries the lab path, so an activity is a pure function of its
//! input and the file system. It needs no process state, so the same bodies
//! run on SQLite and on a Postgres worker. Each body is idempotent, because
//! execution is at least once.

use std::time::Duration;

use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::lab::CommitInfo;
use crate::results::ResultRow;
use crate::rules::Best;
use crate::summary::Summary;
use crate::trainer::{TrainRun, TrainerSpec};

/// The error type of a setup that cannot succeed on a retry.
pub const DATA_MISSING: &str = "DataMissing";
/// The error type of a train call whose branch moved under it.
pub const BRANCH_MOVED: &str = "BranchMoved";

/// The input of [`setup`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupInput {
    /// The lab path.
    pub lab: String,
    /// The run tag. The branch is `autoresearch/<tag>`.
    pub tag: String,
    /// A directory that must exist, such as `~/.cache/autoresearch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
}

/// What [`setup`] found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupReport {
    /// The research branch.
    pub branch: String,
    /// The short hash of its head.
    pub head: String,
    /// The text of `program.md` at the start of the session.
    pub program: String,
}

/// The input of the activities that need the lab path only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabInput {
    /// The lab path.
    pub lab: String,
}

/// The input of [`commit`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitInput {
    /// The lab path.
    pub lab: String,
    /// The commit message.
    pub message: String,
}

/// The input of [`reset`] and [`find_kept`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRef {
    /// The lab path.
    pub lab: String,
    /// A short commit hash.
    pub commit: String,
}

/// The input of [`train`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainInput {
    /// The lab path.
    pub lab: String,
    /// How to train.
    pub trainer: TrainerSpec,
    /// The commit that must be at `HEAD`.
    pub commit: String,
}

/// The input of [`record`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecordInput {
    /// The lab path.
    pub lab: String,
    /// The row to append.
    pub row: ResultRow,
}

/// Check out the research branch, check the data and read `program.md`.
///
/// # Errors
///
/// Fails with [`DATA_MISSING`], not retryable, when `data_dir` is missing.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn setup(_ctx: &ActivityContext, input: SetupInput) -> Result<SetupReport, String> {
    let _ = input;
    Err("green phase".into())
}

/// The short hash of `HEAD`.
///
/// # Errors
///
/// Returns an error when git fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn head(_ctx: &ActivityContext, input: LabInput) -> Result<String, String> {
    let _ = input;
    Err("green phase".into())
}

/// Commit `train.py`. The activity idempotency key is the commit key, so a
/// retry finds the first commit.
///
/// # Errors
///
/// Returns an error when git fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn commit(ctx: &ActivityContext, input: CommitInput) -> Result<CommitInfo, String> {
    let _ = (ctx, input);
    Err("green phase".into())
}

/// Reset the branch to a commit. The saga compensation of [`commit`].
///
/// # Errors
///
/// Returns an error when git fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn reset(_ctx: &ActivityContext, input: CommitRef) -> Result<String, String> {
    let _ = input;
    Err("green phase".into())
}

/// The kept row of a commit in `results.tsv`, as a [`Best`].
///
/// # Errors
///
/// Returns an error when `results.tsv` does not parse.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn find_kept(_ctx: &ActivityContext, input: CommitRef) -> Result<Option<Best>, String> {
    let _ = input;
    Err("green phase".into())
}

/// Train the commit at `HEAD`. Each progress line is a heartbeat.
///
/// A crash or a timeout is a result. Only a run that cannot start is an
/// error, and that error retries once.
///
/// # Errors
///
/// Fails with [`BRANCH_MOVED`], not retryable, when `HEAD` is not the
/// commit. Returns an error when the trainer cannot start.
#[activity(start_to_close = "15m", retry = RetryPolicy::exponential(2, Duration::from_secs(5)))]
pub async fn train(ctx: &ActivityContext, input: TrainInput) -> Result<TrainRun, String> {
    let _ = (ctx, input);
    Err("green phase".into())
}

/// Parse the summary from the end of `run.log`. A local activity: it is cheap
/// and it runs inline, after the decision that schedules it commits.
///
/// # Errors
///
/// None in practice. The signature matches the activity contract.
#[activity(local = true)]
pub async fn parse_summary(_ctx: &ActivityContext, log_tail: String) -> Result<Option<Summary>, String> {
    let _ = log_tail;
    Err("green phase".into())
}

/// Append one row to `results.tsv`. Returns `false` for a row it holds.
///
/// # Errors
///
/// Returns an error when the file cannot be written.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn record(_ctx: &ActivityContext, input: RecordInput) -> Result<bool, String> {
    let _ = input;
    Err("green phase".into())
}

/// Every activity of the example, for registration.
#[must_use]
pub fn all() -> Vec<ActivityInfo> {
    vec![
        setup_info(),
        head_info(),
        commit_info(),
        reset_info(),
        find_kept_info(),
        train_info(),
        parse_summary_info(),
        record_info(),
    ]
}
