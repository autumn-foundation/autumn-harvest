//! The activities: every step that touches git, the trainer or a file.
//!
//! Each input carries the lab path, so an activity is a pure function of its
//! input and the file system. It needs no process state, so the same bodies
//! run on SQLite and on a Postgres worker. Each body is idempotent, because
//! execution is at least once.
//!
//! Each step that writes the lab names the session branch. It refuses to
//! run on another branch, so a second session in the lab cannot make it
//! commit or reset on the wrong branch.

use std::path::Path;
use std::time::Duration;

use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};
use autumn_harvest::policy::RetryPolicy;
use autumn_harvest::prelude::*;
use serde::{Deserialize, Serialize};

use crate::lab::{CommitInfo, Lab, PROGRAM, check_tag};
use crate::results::ResultRow;
use crate::rules::Best;
use crate::summary::Summary;
use crate::trainer::{TrainRun, TrainerSpec};

/// The error type of a setup that cannot succeed on a retry.
pub const DATA_MISSING: &str = "DataMissing";
/// The error type of a step whose branch or `HEAD` moved under it.
pub const BRANCH_MOVED: &str = "BranchMoved";
/// The error type of a tag that cannot name a branch.
pub const BAD_TAG: &str = "BadTag";
/// The error type of a `train.py` change that no commit holds.
pub const DIRTY_TREE: &str = "DirtyTree";
/// The directory in the state directory with one finished run per train call.
const RUNS: &str = "runs";

/// A failure that no retry can fix.
fn fatal(error_type: &str, message: impl Into<String>) -> String {
    ActivityFailure::non_retryable(error_type, message).into_error_payload()
}

/// Run blocking lab work, such as git and file calls, off the async workers.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| format!("the lab call did not finish: {e}"))?
}

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

/// The input of [`head`]: a lab and its session branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabInput {
    /// The lab path.
    pub lab: String,
    /// The session branch.
    pub branch: String,
}

/// The input of [`commit`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitInput {
    /// The lab path.
    pub lab: String,
    /// The session branch.
    pub branch: String,
    /// The commit that must be at `HEAD`: the parent of the new commit.
    pub parent: String,
    /// The commit message.
    pub message: String,
}

/// The input of [`reset`] and [`find_kept`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitRef {
    /// The lab path.
    pub lab: String,
    /// The session branch. [`find_kept`] does not read it.
    pub branch: String,
    /// A short commit hash.
    pub commit: String,
}

/// The input of [`train`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrainInput {
    /// The lab path.
    pub lab: String,
    /// The session branch.
    pub branch: String,
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
/// Each of these fails and does not retry:
///
/// - [`DATA_MISSING`]: `data_dir` is missing.
/// - [`BAD_TAG`]: the tag cannot name a branch.
/// - [`DIRTY_TREE`]: `train.py` has a change that no commit holds.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn setup(_ctx: &ActivityContext, input: SetupInput) -> Result<SetupReport, String> {
    blocking(move || setup_lab(&input)).await
}

fn setup_lab(input: &SetupInput) -> Result<SetupReport, String> {
    let lab = Lab::open(&input.lab)?;
    if let Some(dir) = &input.data_dir
        && !Path::new(dir).is_dir()
    {
        return Err(fatal(
            DATA_MISSING,
            format!("the data directory {dir} does not exist: run `uv run prepare.py` first"),
        ));
    }
    check_tag(&input.tag).map_err(|e| fatal(BAD_TAG, e))?;
    let branch = format!("{}{}", crate::lab::BRANCH_PREFIX, input.tag);
    if lab.require_branch(&branch).is_err() {
        lab.require_clean_train()
            .map_err(|e| fatal(DIRTY_TREE, e))?;
    }
    Ok(SetupReport {
        branch: lab.start_branch(&input.tag)?,
        head: lab.head()?,
        program: lab.read(PROGRAM)?,
    })
}

/// The short hash of the head of the session branch.
///
/// The session releases the lab between experiments, so another session in
/// the lab can check out its own branch. This step checks the session
/// branch out again first.
///
/// # Errors
///
/// Fails with [`DIRTY_TREE`], not retryable, when the branch must change and
/// `train.py` has a change that no commit holds. Returns an error when git
/// fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn head(_ctx: &ActivityContext, input: LabInput) -> Result<String, String> {
    blocking(move || {
        let lab = Lab::open(&input.lab)?;
        if lab.require_branch(&input.branch).is_err() {
            lab.require_clean_train()
                .map_err(|e| fatal(DIRTY_TREE, e))?;
            lab.checkout(&input.branch)?;
        }
        lab.head()
    })
    .await
}

/// Commit `train.py` on `parent`. The activity idempotency key is the
/// commit key, so a retry finds the first commit.
///
/// # Errors
///
/// Fails with [`BRANCH_MOVED`], not retryable, when the branch or `HEAD`
/// moved. Returns an error when git fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn commit(ctx: &ActivityContext, input: CommitInput) -> Result<CommitInfo, String> {
    let key = ctx
        .idempotency_key()
        .map_err(|e| e.to_string())?
        .to_string();
    blocking(move || {
        let lab = Lab::open(&input.lab)?;
        if let Some(info) = lab.committed(&key)? {
            return Ok(info);
        }
        lab.require_branch(&input.branch)
            .and_then(|()| lab.require_head(&input.parent))
            .map_err(|e| fatal(BRANCH_MOVED, e))?;
        lab.commit_train(&input.message, &key, &input.parent)
    })
    .await
}

/// Reset the session branch to a commit. The saga compensation of
/// [`commit`].
///
/// # Errors
///
/// Fails with [`BRANCH_MOVED`], not retryable, when another branch is
/// checked out. Returns an error when git fails.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn reset(_ctx: &ActivityContext, input: CommitRef) -> Result<String, String> {
    blocking(move || {
        let lab = Lab::open(&input.lab)?;
        lab.require_branch(&input.branch)
            .map_err(|e| fatal(BRANCH_MOVED, e))?;
        lab.reset_to(&input.commit)?;
        Ok(input.commit)
    })
    .await
}

/// The kept row of a commit in `results.tsv`, as a [`Best`].
///
/// # Errors
///
/// Returns an error when `results.tsv` cannot be read.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn find_kept(_ctx: &ActivityContext, input: CommitRef) -> Result<Option<Best>, String> {
    let row = blocking(move || Lab::open(&input.lab)?.kept(&input.commit)).await?;
    Ok(row.map(|row| Best {
        commit: row.commit,
        val_bpb: row.val_bpb,
        memory_gb: row.memory_gb,
    }))
}

/// Train the commit at `HEAD`. Each progress line is a heartbeat.
///
/// A crash or a timeout is a result. Only a run that cannot start is an
/// error, and that error retries once.
///
/// A finished run is saved under the activity idempotency key. When the
/// process dies after a run and before its result lands, the redelivery
/// returns the saved run. It does not train again.
///
/// # Errors
///
/// Fails, not retryable, with [`BRANCH_MOVED`] when the branch or `HEAD`
/// moved, and with [`DIRTY_TREE`] when `train.py` has a change that no
/// commit holds. Returns an error when the trainer cannot start.
#[activity(start_to_close = "15m", retry = RetryPolicy::exponential(2, Duration::from_secs(5)))]
pub async fn train(ctx: &ActivityContext, input: TrainInput) -> Result<TrainRun, String> {
    let key = ctx
        .idempotency_key()
        .map_err(|e| e.to_string())?
        .to_string();
    let path = input.lab.clone();
    let (branch, commit) = (input.branch.clone(), input.commit.clone());
    let (lab, state, saved) = blocking(move || {
        let lab = Lab::open(&path)?;
        let state = lab.state_dir()?;
        let runs = state.join(RUNS);
        std::fs::create_dir_all(&runs)
            .map_err(|e| format!("cannot create {}: {e}", runs.display()))?;
        let saved = runs.join(format!("{}.json", crate::stores::hashed(&key)));
        lab.require_branch(&branch)
            .and_then(|()| lab.require_head(&commit))
            .map_err(|e| fatal(BRANCH_MOVED, e))?;
        lab.require_clean_train()
            .map_err(|e| fatal(DIRTY_TREE, e))?;
        Ok((lab, state, saved))
    })
    .await?;
    if let Some(run) = std::fs::read(&saved)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<TrainRun>(&bytes).ok())
    {
        return Ok(run);
    }
    let (lines, mut progress) = tokio::sync::mpsc::unbounded_channel::<String>();
    let run = crate::trainer::run(&input.trainer, lab.root(), &state, move |line| {
        let _ = lines.send(line.to_string());
    });
    // Each progress line is a heartbeat. The loop ends when the run drops
    // the sender.
    let beat = async {
        while let Some(line) = progress.recv().await {
            let _ = ctx.heartbeat(serde_json::json!({ "line": line })).await;
        }
    };
    let (run, ()) = tokio::join!(run, beat);
    let run = run?;
    let bytes = serde_json::to_vec(&run).map_err(|e| e.to_string())?;
    crate::stores::write_atomic(&saved, &bytes)?;
    Ok(run)
}

/// Parse the summary from the end of `run.log`. A local activity: it is cheap
/// and it runs inline, after the decision that schedules it commits.
///
/// # Errors
///
/// None in practice. The signature matches the activity contract.
#[activity(local = true)]
#[allow(clippy::unused_async)] // The activity macro needs an async fn.
pub async fn parse_summary(
    _ctx: &ActivityContext,
    log_tail: String,
) -> Result<Option<Summary>, String> {
    Ok(crate::summary::parse(&log_tail))
}

/// Append one row to `results.tsv`, keyed on the activity idempotency key.
/// Returns `false` when a retry finds its row.
///
/// # Errors
///
/// Returns an error when a file cannot be written.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn record(ctx: &ActivityContext, input: RecordInput) -> Result<bool, String> {
    let key = ctx
        .idempotency_key()
        .map_err(|e| e.to_string())?
        .to_string();
    blocking(move || Lab::open(&input.lab)?.record(&input.row, &key)).await
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn lab_with_counter() -> (tempfile::TempDir, TrainInput, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let branch = lab.start_branch("t").unwrap();
        let counter = dir.path().join("runs");
        let script = format!("echo run >> '{}'; echo 'val_bpb: 0.99'", counter.display());
        let input = TrainInput {
            lab: lab.root().display().to_string(),
            branch,
            trainer: TrainerSpec::Command {
                program: "sh".into(),
                args: vec!["-c".into(), script],
                timeout_secs: 30,
            },
            commit: lab.head().unwrap(),
        };
        (dir, input, counter)
    }

    #[tokio::test]
    async fn a_train_call_that_runs_again_returns_its_saved_run() {
        let (_dir, input, counter) = lab_with_counter();
        let ctx = ActivityContext::new_test();
        let first = train(&ctx, input.clone()).await.unwrap();
        // The same call after a crash: the run is not trained again.
        let again = train(&ctx, input.clone()).await.unwrap();
        assert_eq!(first, again);
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            1
        );
        // A new call trains.
        train(&ActivityContext::new_test(), input).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            2
        );
    }

    #[tokio::test]
    async fn train_refuses_a_moved_branch_and_a_change_no_commit_holds() {
        let (_dir, input, counter) = lab_with_counter();
        let lab = Lab::open(&input.lab).unwrap();
        lab.edit_train("DEPTH = 8", "DEPTH = 9").unwrap();
        let dirty = train(&ActivityContext::new_test(), input.clone()).await;
        assert!(dirty.unwrap_err().contains(DIRTY_TREE));
        lab.git(&["checkout", "--", "train.py"]).unwrap();
        lab.start_branch("other").unwrap();
        let moved = train(&ActivityContext::new_test(), input).await;
        assert!(moved.unwrap_err().contains(BRANCH_MOVED));
        assert!(!counter.exists(), "nothing trained");
    }
}
