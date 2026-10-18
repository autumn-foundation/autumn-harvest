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

use std::collections::BTreeMap;
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
/// The error type of a tag whose branch another session started.
pub const TAG_USED: &str = "TagUsed";
/// The error type of a lab path that is not canonical.
pub const LAB_PATH: &str = "LabPath";
/// The error type of a train call on a worker that cannot serialize it.
pub const UNSUPPORTED: &str = "Unsupported";
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
    /// The full id of its head.
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
    /// The full id of a commit.
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
/// - [`TAG_USED`]: another session started the branch of the tag.
/// - [`LAB_PATH`]: the lab path is not canonical.
///
/// The activity idempotency key marks the start commit of the branch, so a
/// retry finds its own branch.
#[activity(start_to_close = "1m", retry = RetryPolicy::exponential(3, Duration::from_secs(1)))]
pub async fn setup(ctx: &ActivityContext, input: SetupInput) -> Result<SetupReport, String> {
    let key = ctx
        .idempotency_key()
        .map_err(|e| e.to_string())?
        .to_string();
    blocking(move || setup_lab(&input, &key)).await
}

fn setup_lab(input: &SetupInput, key: &str) -> Result<SetupReport, String> {
    let lab = Lab::open(&input.lab)?;
    // The lab mutex and the agent runs find the lab by its canonical path.
    // Another path for the same lab would get its own mutex.
    if lab.root().to_str() != Some(input.lab.as_str()) {
        return Err(fatal(
            LAB_PATH,
            format!(
                "the lab {} is not a canonical path. Start the session with {}",
                input.lab,
                lab.root().display()
            ),
        ));
    }
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
    let used = format!("{branch} exists");
    let branch = lab.start_branch(&input.tag, key).map_err(|e| {
        if e.contains(&used) {
            fatal(TAG_USED, e)
        } else {
            e
        }
    })?;
    Ok(SetupReport {
        branch,
        head: lab.head()?,
        program: lab.read(PROGRAM)?,
    })
}

/// The full id of the head of the session branch.
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
    let commit = input.commit.clone();
    let row = blocking(move || Lab::open(&input.lab)?.kept(&input.commit)).await?;
    Ok(row.map(|row| Best {
        commit,
        val_bpb: row.val_bpb,
        memory_gb: row.memory_gb,
    }))
}

/// The time that the runner needs around one run: to start it, to kill it,
/// and to save its log.
const TRAIN_MARGIN: Duration = Duration::from_secs(60);

/// Fail unless a run of `trainer` ends well inside the budget of [`train`].
/// A longer run would time out the activity, and its retry too. Off Unix
/// it fails too, because the train activity refuses there.
///
/// # Errors
///
/// Returns an error that names the run time and the budget.
pub fn check_trainer(trainer: &TrainerSpec) -> Result<(), String> {
    // Off Unix the train activity refuses. Say so before the session starts.
    if cfg!(not(unix)) {
        return Err(
            "training needs a Unix worker, because two deliveries of a train call take turns on flock".into(),
        );
    }
    let budget = train_info().default_start_to_close.unwrap_or(Duration::MAX);
    let run = Duration::from_secs(trainer.longest_secs());
    if run.saturating_add(TRAIN_MARGIN) <= budget {
        Ok(())
    } else {
        Err(format!(
            "a run of {}s does not fit the {}s budget of a train activity with {}s to spare. Use a shorter run",
            run.as_secs(),
            budget.as_secs(),
            TRAIN_MARGIN.as_secs()
        ))
    }
}

/// How long a call waits for its lock. Another delivery of the call holds
/// it for one run at most, and a run ends inside the budget of [`train`].
const CALL_LOCK_WAIT: Duration = Duration::from_secs(16 * 60);

/// Hold an exclusive lock on the lock file beside `saved` until the file
/// drops. Two deliveries of one call that run at once take turns. A run can
/// plant a link or a pipe at the lock file, and the open refuses both.
async fn call_lock(saved: &Path) -> Result<std::fs::File, String> {
    call_lock_within(saved, CALL_LOCK_WAIT).await
}

/// [`call_lock`] that waits at most `wait`. A process that a run left can
/// hold the lock, so the call fails in time and parks no thread for good.
async fn call_lock_within(saved: &Path, wait: Duration) -> Result<std::fs::File, String> {
    let path = saved.with_extension("lock");
    let opened = path.clone();
    let file = blocking(move || crate::stores::open_lock(&opened)).await?;
    let deadline = tokio::time::Instant::now() + wait;
    while !try_lock_exclusive(&file, &path)? {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{} stays locked. Another delivery of the call runs, or a process that a run left holds it",
                path.display()
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(file)
}

/// Take an exclusive `flock` on `file` when it is free.
#[cfg(unix)]
fn try_lock_exclusive(file: &std::fs::File, path: &Path) -> Result<bool, String> {
    match rustix::fs::flock(file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(true),
        Err(rustix::io::Errno::WOULDBLOCK) => Ok(false),
        Err(e) => Err(format!("cannot lock {}: {e}", path.display())),
    }
}

/// Off Unix no lock serializes two deliveries of one call. Both could
/// train, so the call refuses to train.
#[cfg(not(unix))]
fn try_lock_exclusive(_file: &std::fs::File, _path: &Path) -> Result<bool, String> {
    Err(fatal(
        UNSUPPORTED,
        "the train activity needs a Unix worker, because it serializes the deliveries of a call with flock",
    ))
}

/// The run is code that the model wrote, and the checks before it are stale
/// once it ends. A run can change the branch, `HEAD` or a tracked file, such
/// as `train.py` or `prepare.py`. Then put the lab back and count the run as
/// a crash. `before` is the [`Lab::tracked_snapshot`] from before the run.
fn after_run(
    lab: &Lab,
    target: (&str, &str),
    before: &BTreeMap<String, String>,
    run: TrainRun,
) -> Result<TrainRun, String> {
    let (branch, commit) = target;
    let changed = lab
        .require_branch(branch)
        .and_then(|()| lab.require_head(commit))
        .and_then(|()| lab.require_clean_train())
        .and_then(|()| {
            let after = lab.tracked_snapshot()?;
            let moved: Vec<&str> = before
                .keys()
                .chain(after.keys())
                .filter(|path| before.get(*path) != after.get(*path))
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            if moved.is_empty() {
                Ok(())
            } else {
                Err(format!("it changed {}", moved.join(", ")))
            }
        });
    let Err(why) = changed else {
        return Ok(run);
    };
    lab.restore(branch, commit)?;
    Ok(TrainRun {
        exit_code: None,
        timed_out: run.timed_out,
        log_tail: format!(
            "{}\nautoresearch: the run changed the lab, so it counts as a crash: {why}\n",
            run.log_tail
        ),
    })
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
    let (lab, state, saved) = blocking(move || {
        let lab = Lab::open(&path)?;
        let state = lab.state_dir()?;
        let runs = state.join(RUNS);
        std::fs::create_dir_all(&runs)
            .map_err(|e| format!("cannot create {}: {e}", runs.display()))?;
        let saved = runs.join(format!("{}.json", crate::stores::hashed(&key)));
        Ok((lab, state, saved))
    })
    .await?;
    // A worker that lost its lease can run on while the call is delivered
    // again. The second delivery waits here, then finds the saved run. The
    // checks come after the lock, because a run can change the lab.
    let _call = call_lock(&saved).await?;
    if let Some(run) = crate::stores::read_regular(&saved)
        .ok()
        .flatten()
        .and_then(|bytes| serde_json::from_slice::<TrainRun>(&bytes).ok())
    {
        return Ok(run);
    }
    // The mark says that a delivery of this call began a run. With no saved
    // run, its runner stopped. The checks passed before that run, so a
    // change since then is the run's, or a person's while the runner was
    // stopped. Stop what is left, keep the changes in a stash, and put the
    // lab back.
    let started = saved.with_extension("started");
    let marked = crate::stores::open_regular(&started)
        .map_err(|e| format!("cannot read {}: {e}", started.display()))?
        .is_some();
    if marked {
        crate::trainer::reap(&state).await?;
        let (back, branch, commit) = (lab.clone(), input.branch.clone(), input.commit.clone());
        blocking(move || {
            // The stash is only a backup. Git cannot stash some states, such
            // as an unborn branch or a conflicted index, and the lab must
            // still go back.
            let message = format!("autoresearch: the lab before a resume of {branch}");
            if let Err(error) = back.keep_changes(&message) {
                tracing::warn!(%error, "the lab before a resume is not kept");
            }
            back.restore(&branch, &commit)
        })
        .await?;
    }
    let (checked, branch, commit) = (lab.clone(), input.branch.clone(), input.commit.clone());
    blocking(move || {
        checked
            .require_branch(&branch)
            .and_then(|()| checked.require_head(&commit))
            .map_err(|e| fatal(BRANCH_MOVED, e))?;
        checked
            .require_clean_train()
            .and_then(|()| checked.require_fixed_files(&commit))
            .map_err(|e| fatal(DIRTY_TREE, e))
    })
    .await?;
    let snapshot = lab.clone();
    let before = blocking(move || snapshot.tracked_snapshot()).await?;
    crate::stores::write_atomic(&started, b"")?;
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
    let run =
        blocking(move || after_run(&lab, (&input.branch, &input.commit), &before, run)).await?;
    let bytes = serde_json::to_vec(&run).map_err(|e| e.to_string())?;
    crate::stores::write_atomic(&saved, &bytes)?;
    // The saved run answers each later delivery, so the mark has no use.
    let _ = std::fs::remove_file(&started);
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
        let branch = lab.start_branch("t", "s1").unwrap();
        let counter = dir.path().join("runs");
        let script = format!("echo run >> '{}'; echo 'val_bpb: 0.99'", counter.display());
        let input = TrainInput {
            lab: lab.root().display().to_string(),
            branch,
            trainer: TrainerSpec::Command {
                program: "sh".into(),
                args: vec!["-c".into(), script],
                timeout_secs: 30,
                pass_env: Vec::new(),
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
        lab.start_branch("other", "s2").unwrap();
        let moved = train(&ActivityContext::new_test(), input).await;
        assert!(moved.unwrap_err().contains(BRANCH_MOVED));
        assert!(!counter.exists(), "nothing trained");
    }

    /// A change to the fixed evaluation that no commit holds, such as a hand
    /// edit of `prepare.py` between two experiments, is refused before the
    /// run. A score must belong to the files of its commit. Prose, such as
    /// `program.md`, may change.
    #[tokio::test]
    async fn train_refuses_a_change_to_a_fixed_file_that_no_commit_holds() {
        let (_dir, input, counter) = lab_with_counter();
        let lab = Lab::open(&input.lab).unwrap();
        let prepare = lab.root().join(crate::lab::PREPARE);
        let fixed = std::fs::read_to_string(&prepare).unwrap();
        std::fs::write(&prepare, format!("{fixed}TAMPERED = 1\n")).unwrap();
        let dirty = train(&ActivityContext::new_test(), input.clone()).await;
        let why = dirty.unwrap_err();
        assert!(
            why.contains(DIRTY_TREE) && why.contains("prepare.py"),
            "{why}"
        );
        assert!(!counter.exists(), "nothing trained");
        std::fs::write(&prepare, fixed).unwrap();
        for prose in [PROGRAM, "README.md"] {
            let path = lab.root().join(prose);
            let text = std::fs::read_to_string(&path).unwrap();
            std::fs::write(&path, format!("{text}\nA note.\n")).unwrap();
        }
        train(&ActivityContext::new_test(), input).await.unwrap();
    }

    #[tokio::test]
    async fn setup_refuses_a_tag_that_another_session_used() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let input = SetupInput {
            lab: lab.root().display().to_string(),
            tag: "oct17".into(),
            data_dir: None,
        };
        let ctx = ActivityContext::new_test();
        let first = setup(&ctx, input.clone()).await.unwrap();
        // The same call after a crash: the same branch and start commit.
        assert_eq!(setup(&ctx, input.clone()).await.unwrap(), first);
        // A new session with the tag, such as after `purge`.
        let used = setup(&ActivityContext::new_test(), input).await;
        assert!(used.unwrap_err().contains(TAG_USED));
    }

    /// A link to the lab would give the session another lab mutex, and no
    /// agent run would find the lab.
    #[cfg(unix)]
    #[tokio::test]
    async fn setup_refuses_a_lab_path_that_is_not_canonical() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(lab.root(), &link).unwrap();
        let input = SetupInput {
            lab: link.display().to_string(),
            tag: "oct17".into(),
            data_dir: None,
        };
        let why = setup(&ActivityContext::new_test(), input)
            .await
            .unwrap_err();
        assert!(why.contains(LAB_PATH), "{why}");
        assert!(why.contains(&lab.root().display().to_string()), "{why}");
    }

    /// The row shows the short form. The base keeps the full id, so a
    /// later reset never meets an ambiguous prefix.
    #[tokio::test]
    async fn find_kept_returns_the_full_id_of_the_commit() {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        let head = lab.head().unwrap();
        let row = ResultRow {
            commit: crate::results::short(&head),
            val_bpb: 0.99,
            memory_gb: 44.0,
            status: crate::rules::Status::Keep,
            description: "baseline".into(),
        };
        lab.record(&row, "k1").unwrap();
        let input = CommitRef {
            lab: lab.root().display().to_string(),
            branch: "master".into(),
            commit: head.clone(),
        };
        let best = find_kept(&ActivityContext::new_test(), input)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(best.commit, head);
        assert_eq!(best.val_bpb, 0.99);
    }

    #[test]
    fn a_trainer_must_fit_the_train_budget() {
        assert!(check_trainer(&TrainerSpec::upstream()).is_ok());
        assert!(check_trainer(&TrainerSpec::Simulated { seconds: 840 }).is_ok());
        let long = check_trainer(&TrainerSpec::Simulated { seconds: 841 }).unwrap_err();
        assert!(long.contains("budget"), "{long}");
        let command = TrainerSpec::Command {
            program: "uv".into(),
            args: Vec::new(),
            timeout_secs: 3600,
            pass_env: Vec::new(),
        };
        assert!(check_trainer(&command).is_err());
    }

    /// The run is code that the model wrote. A run that changes the branch,
    /// `HEAD` or `train.py` is a crash, and the lab goes back as it was.
    #[tokio::test]
    async fn a_run_that_changes_the_lab_is_a_crash_and_the_lab_is_restored() {
        let changes = [
            "git checkout -q -b elsewhere",
            "echo '# x' >> train.py; git -c user.name=r -c user.email=r@r commit -qam x",
            "echo 'DEPTH = 99' >> train.py",
            // The fixed evaluation. A keep runs no reset, so the check must
            // catch it.
            "echo 'TAMPERED = 1' >> prepare.py",
            "rm prepare.py",
        ];
        for change in changes {
            let (_dir, mut input, _counter) = lab_with_counter();
            input.trainer = TrainerSpec::Command {
                program: "sh".into(),
                args: vec![
                    "-c".into(),
                    format!("{change}; echo ---; echo 'val_bpb: 0.5'"),
                ],
                timeout_secs: 30,
                pass_env: Vec::new(),
            };
            let run = train(&ActivityContext::new_test(), input.clone())
                .await
                .unwrap();
            assert_eq!(run.exit_code, None, "{change}: {run:?}");
            assert!(
                run.log_tail.contains("changed the lab"),
                "{change}: {run:?}"
            );
            if change.contains("prepare.py") {
                // Each file is named once.
                assert!(
                    run.log_tail.ends_with("it changed prepare.py\n"),
                    "{change}: {run:?}"
                );
            }
            let lab = Lab::open(&input.lab).unwrap();
            lab.require_branch(&input.branch).unwrap();
            lab.require_head(&input.commit).unwrap();
            lab.require_clean_train().unwrap();
            assert_eq!(
                lab.git(&["status", "--porcelain", "--untracked-files=no"])
                    .unwrap(),
                "",
                "{change}"
            );
        }
    }

    /// A worker that lost its lease can run on while the call is delivered
    /// again. The two deliveries train once. The second delivery comes while
    /// the first run has a change in `train.py`, and it does not fail.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_deliveries_at_once_train_once() {
        let (_dir, mut input, counter) = lab_with_counter();
        let script = format!(
            "echo run >> '{}'; echo '# scratch' >> train.py; sleep 1; \
             git checkout -q train.py; echo 'val_bpb: 0.99'",
            counter.display()
        );
        input.trainer = TrainerSpec::Command {
            program: "sh".into(),
            args: vec!["-c".into(), script],
            timeout_secs: 30,
            pass_env: Vec::new(),
        };
        let ctx = ActivityContext::new_test();
        let again = async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            train(&ctx, input.clone()).await
        };
        let (first, second) = tokio::join!(train(&ctx, input.clone()), again);
        assert_eq!(first.unwrap(), second.unwrap());
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            1
        );
    }

    /// A runner that stops mid-run, as on Ctrl-C, drops the run before the
    /// checks after it. The call comes again on resume. It finds the lab as
    /// the run left it, puts it back, and trains again.
    #[tokio::test]
    async fn a_call_that_comes_again_after_a_stopped_run_puts_the_lab_back() {
        let (_dir, input, counter) = lab_with_counter();
        let ctx = ActivityContext::new_test();
        let mut stopped = input.clone();
        stopped.trainer = TrainerSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "git checkout -q -b elsewhere; echo '# scratch' >> train.py; sleep 30".into(),
            ],
            timeout_secs: 60,
            pass_env: Vec::new(),
        };
        let cut = tokio::time::timeout(Duration::from_millis(1500), train(&ctx, stopped)).await;
        assert!(cut.is_err(), "the runner stopped mid-run");
        let lab = Lab::open(&input.lab).unwrap();
        assert!(
            lab.require_branch(&input.branch).is_err(),
            "the run moved it"
        );
        // A person changes the lab while the runner is stopped.
        let prepare = lab.root().join(crate::lab::PREPARE);
        let text = std::fs::read_to_string(&prepare).unwrap();
        std::fs::write(&prepare, format!("{text}PERSON = 1\n")).unwrap();
        let run = train(&ctx, input.clone()).await.unwrap();
        assert_eq!(run.exit_code, Some(0), "{run:?}");
        // What the resume put back stays in a stash.
        let kept = lab.git(&["stash", "list"]).unwrap();
        assert!(kept.contains("autoresearch"), "{kept}");
        let shown = lab.git(&["stash", "show", "-p", "stash@{0}"]).unwrap();
        assert!(shown.contains("PERSON = 1"), "{shown}");
        lab.require_branch(&input.branch).unwrap();
        lab.require_head(&input.commit).unwrap();
        lab.require_clean_train().unwrap();
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            1
        );
    }

    /// A run can plant a pipe at the lock file of a call. The call must not
    /// hang on it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_planted_pipe_at_a_call_lock_is_refused() {
        use rustix::fs::{FileType, Mode, OFlags};
        let dir = tempfile::tempdir().unwrap();
        let saved = dir.path().join("call.json");
        let lock = saved.with_extension("lock");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &lock,
            FileType::Fifo,
            Mode::RUSR | Mode::WUSR,
            0,
        )
        .unwrap();
        let locked = tokio::time::timeout(Duration::from_secs(5), call_lock(&saved)).await;
        // A reader frees an open that blocks, so a failure cannot hang the test.
        let _reader = rustix::fs::open(&lock, OFlags::RDONLY | OFlags::NONBLOCK, Mode::empty());
        assert!(matches!(locked, Ok(Err(_))), "{locked:?}");
    }

    /// A process that a run left can hold the lock of a call. The call must
    /// fail in time, not park the runner for good.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_held_call_lock_fails_the_call_in_time() {
        let dir = tempfile::tempdir().unwrap();
        let saved = dir.path().join("call.json");
        let held = crate::stores::open_lock(&saved.with_extension("lock")).unwrap();
        rustix::fs::flock(&held, rustix::fs::FlockOperation::LockExclusive).unwrap();
        let locked = tokio::time::timeout(
            Duration::from_secs(5),
            call_lock_within(&saved, Duration::from_millis(300)),
        )
        .await;
        assert!(matches!(locked, Ok(Err(_))), "{locked:?}");
        drop(held);
        call_lock_within(&saved, Duration::from_millis(300))
            .await
            .unwrap();
    }

    /// The stash is only a backup. A lab that git cannot stash, such as one
    /// on an unborn branch, still goes back on resume.
    #[tokio::test]
    async fn a_resume_puts_back_a_lab_that_git_cannot_stash() {
        let (_dir, input, _counter) = lab_with_counter();
        let ctx = ActivityContext::new_test();
        let mut stopped = input.clone();
        stopped.trainer = TrainerSpec::Command {
            program: "sh".into(),
            args: vec![
                "-c".into(),
                "git checkout -q --orphan elsewhere; sleep 30".into(),
            ],
            timeout_secs: 60,
            pass_env: Vec::new(),
        };
        let cut = tokio::time::timeout(Duration::from_millis(1500), train(&ctx, stopped)).await;
        assert!(cut.is_err(), "the runner stopped mid-run");
        let lab = Lab::open(&input.lab).unwrap();
        assert!(
            lab.git(&["stash", "create"]).is_err(),
            "git cannot stash it"
        );
        let run = train(&ctx, input.clone()).await.unwrap();
        assert_eq!(run.exit_code, Some(0), "{run:?}");
        lab.require_branch(&input.branch).unwrap();
        lab.require_head(&input.commit).unwrap();
        lab.require_clean_train().unwrap();
    }
}
