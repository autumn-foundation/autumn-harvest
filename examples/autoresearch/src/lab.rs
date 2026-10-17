//! The lab: a git work tree with `train.py`, `prepare.py` and `program.md`.
//!
//! Every git call goes through [`Lab::git`]. Each write is idempotent, because
//! an activity can run again after a crash:
//!
//! - [`Lab::commit_train`] finds its own commit by an `Experiment-Key`
//!   trailer and does not commit twice.
//! - [`Lab::record`] keys each row on the activity idempotency key, so a
//!   retry does not append its row twice.
//! - [`Lab::edit_train_once`] keys each edit on its tool call, so a re-run
//!   does not apply it twice.
//! - [`Lab::reset_to`] is idempotent.
//!
//! The agent tools read only [`READABLE`] files and write only `train.py`.
//! A read refuses a symlink and returns at most [`MAX_READ`] bytes.

use std::fmt::Write as _;
use std::io::{Read as _, Seek as _, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, PoisonError};

use serde::{Deserialize, Serialize};

use crate::results::{HEADER, ResultRow};

/// The file that the agent edits.
pub const TRAIN: &str = "train.py";
/// The fixed file that the agent must not edit.
pub const PREPARE: &str = "prepare.py";
/// The instructions that the human edits.
pub const PROGRAM: &str = "program.md";
/// The untracked results log.
pub const RESULTS: &str = "results.tsv";
/// The directory of example state: database, inbox, memory, cache, reports.
pub const STATE_DIR: &str = ".autoresearch";
/// The directory of papers that `read_paper` serves.
pub const PAPERS_DIR: &str = "papers";
/// The files that `read_file` serves. Every other path is refused.
pub const READABLE: &[&str] = &[TRAIN, PREPARE, PROGRAM, RESULTS, "README.md", "run.log"];
/// The trailer that names the experiment of a commit.
pub const KEY_TRAILER: &str = "Experiment-Key";
/// The trailer of the start commit of a session. It holds the key of the
/// setup step.
pub const SESSION_TRAILER: &str = "Session-Key";
/// The prefix of every research branch.
pub const BRANCH_PREFIX: &str = "autoresearch/";
/// The most bytes that one agent read returns. A longer file returns its end.
pub const MAX_READ: u64 = 64 * 1024;
/// The newest rows of `results.tsv` that one agent read returns.
pub const READ_ROWS: usize = 50;
/// The file in the state directory that maps each recorded key to its row.
const RECORDED: &str = "recorded.tsv";
/// The directory in the state directory with one marker per applied edit.
const EDITS: &str = "edits";

/// One process writes `results.tsv` and `train.py` from several threads.
/// This lock makes each read-modify-write whole.
static WRITE_LOCK: Mutex<()> = Mutex::new(());
/// The author of every commit that the example makes.
const AUTHOR: [&str; 4] = [
    "-c",
    "user.name=autoresearch",
    "-c",
    "user.email=autoresearch@localhost",
];
/// Git settings for every call. Hooks and the file system monitor are off:
/// they close two common ways for code in the lab to run through git. They
/// do not make git safe from code that can write `.git/config`. Signing is
/// off, so a signer in the global config cannot prompt and hang a night.
const SAFE_GIT: [&str; 6] = [
    "-c",
    "core.hooksPath=/dev/null",
    "-c",
    "core.fsmonitor=false",
    "-c",
    "commit.gpgsign=false",
];

/// The bundled lab template. `autoresearch init` writes these files.
pub const TEMPLATE: &[(&str, &str)] = &[
    (TRAIN, include_str!("../lab/train.py")),
    (PREPARE, include_str!("../lab/prepare.py")),
    (PROGRAM, include_str!("../program.md")),
    ("README.md", include_str!("../lab/README.md")),
    ("papers/muon.md", include_str!("../lab/papers/muon.md")),
    (
        "papers/warmdown.md",
        include_str!("../lab/papers/warmdown.md"),
    ),
];

/// One commit of `train.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitInfo {
    /// The full id of the commit, or of `HEAD` when nothing changed.
    pub commit: String,
    /// The full id of the parent: the reset target.
    pub parent: String,
    /// `false` when `train.py` had no change, so no commit was made.
    pub changed: bool,
    /// Lines added to `train.py`.
    pub lines_added: u32,
    /// Lines removed from `train.py`.
    pub lines_removed: u32,
}

/// A git work tree that holds one research branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lab {
    root: PathBuf,
}

impl Lab {
    /// Open the lab at `path`. It must be the top of a git work tree.
    ///
    /// # Errors
    ///
    /// Returns an error when `path` is not a git work tree.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        let root = path
            .canonicalize()
            .map_err(|e| format!("cannot open the lab {}: {e}", path.display()))?;
        let lab = Self { root };
        let top = lab.git(&["rev-parse", "--show-toplevel"])?;
        let top = PathBuf::from(top)
            .canonicalize()
            .map_err(|e| format!("cannot resolve the work tree: {e}"))?;
        if top != lab.root {
            return Err(format!(
                "{} is inside a git work tree, not its top ({})",
                path.display(),
                top.display()
            ));
        }
        Ok(lab)
    }

    /// Create a lab at `path` from [`TEMPLATE`], with one commit on `master`.
    ///
    /// # Errors
    ///
    /// Returns an error when `path` holds files, or git fails.
    pub fn init(path: impl AsRef<Path>) -> Result<Self, String> {
        let path = path.as_ref();
        if path.exists()
            && std::fs::read_dir(path)
                .map_err(|e| format!("cannot read {}: {e}", path.display()))?
                .next()
                .is_some()
        {
            return Err(format!("{} is not empty", path.display()));
        }
        for (name, text) in TEMPLATE {
            let file = path.join(name);
            if let Some(parent) = file.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
            }
            std::fs::write(&file, text).map_err(|e| format!("cannot write {name}: {e}"))?;
        }
        git_in(path, &["init", "--quiet", "--initial-branch=master"])?;
        git_in(path, &["add", "--all"])?;
        git_in(
            path,
            &[
                "commit",
                "--quiet",
                "-m",
                "baseline lab from the autoresearch template",
            ],
        )?;
        let lab = Self::open(path)?;
        lab.exclude_local_files()?;
        Ok(lab)
    }

    /// The lab of `labs` whose root is `session`, the session id of an
    /// agent run.
    #[must_use]
    pub fn serving<'a>(labs: &'a [Self], session: Option<&str>) -> Option<&'a Self> {
        let session = session?;
        labs.iter().find(|lab| lab.root.to_str() == Some(session))
    }

    /// The canonical root of the lab.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The state directory, created on first use. Only its owner can read
    /// it, because it holds prompts, transcripts and the database.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created.
    pub fn state_dir(&self) -> Result<PathBuf, String> {
        let dir = self.root.join(STATE_DIR);
        private_dir(&dir)?;
        Ok(dir)
    }

    /// Run git in the lab and return its trimmed stdout.
    ///
    /// # Errors
    ///
    /// Returns git's stderr when git exits non-zero.
    pub fn git(&self, args: &[&str]) -> Result<String, String> {
        git_in(&self.root, args)
    }

    /// Fail unless `branch` is checked out.
    ///
    /// # Errors
    ///
    /// Returns an error that names both branches when they differ.
    pub fn require_branch(&self, branch: &str) -> Result<(), String> {
        let current = self.git(&["branch", "--show-current"])?;
        if current == branch {
            Ok(())
        } else {
            Err(format!(
                "the lab is on {current:?}, not {branch:?}: another session moved it"
            ))
        }
    }

    /// Check out `branch` when another branch is checked out. Refuse when
    /// `train.py` has a change that no commit holds.
    ///
    /// # Errors
    ///
    /// Returns an error when `train.py` has a change, or git fails.
    pub fn checkout(&self, branch: &str) -> Result<(), String> {
        if self.require_branch(branch).is_ok() {
            return Ok(());
        }
        self.require_clean_train()?;
        self.git(&["checkout", "--quiet", branch])?;
        Ok(())
    }

    /// Fail when `train.py` has a change that no commit holds.
    ///
    /// # Errors
    ///
    /// Returns an error that says how to fix the work tree.
    pub fn require_clean_train(&self) -> Result<(), String> {
        if self
            .git(&["status", "--porcelain", "--", TRAIN])?
            .is_empty()
        {
            Ok(())
        } else {
            Err(format!(
                "{TRAIN} has a change that no commit holds. \
                 Commit it, or discard it with `git checkout -- {TRAIN}`"
            ))
        }
    }

    /// Check out `autoresearch/<tag>`, the branch of the session whose setup
    /// step has `key`. Hide `results.tsv`, `run.log` and the state directory
    /// from git.
    ///
    /// A new branch starts on `HEAD` with an empty commit that holds `key`.
    /// So two sessions in one lab never share a commit, and a row of one
    /// session cannot name the commit of another. A retried setup finds its
    /// start commit. Any other branch with the name belongs to another
    /// session, such as one that `purge` removed, and is refused.
    ///
    /// # Errors
    ///
    /// Returns an error when the tag is not a valid branch name, the branch
    /// exists with no start commit of `key`, or git fails.
    pub fn start_branch(&self, tag: &str, key: &str) -> Result<String, String> {
        check_tag(tag)?;
        let branch = format!("{BRANCH_PREFIX}{tag}");
        let reference = format!("refs/heads/{branch}");
        let trailer = format!("{SESSION_TRAILER}: {key}");
        self.exclude_local_files()?;
        if self
            .git(&["rev-parse", "--verify", "--quiet", &reference])
            .is_ok()
        {
            let grep = format!("--grep={trailer}");
            let bodies = self.git(&[
                "log",
                "--fixed-strings",
                &grep,
                "--format=%B%x00",
                &reference,
            ])?;
            let started = bodies
                .split('\0')
                .any(|body| body.lines().any(|line| line.trim() == trailer));
            if !started {
                return Err(format!(
                    "the branch {branch} exists: another session used the tag {tag:?}. Use a new tag"
                ));
            }
            self.checkout(&branch)?;
            return Ok(branch);
        }
        // A change that no commit holds would move to the new branch, and
        // the baseline would train it under the wrong commit.
        self.require_clean_train()?;
        let message = format!("autoresearch: start {tag}\n\n{trailer}");
        // `HEAD:` is the tree of `HEAD`, so the start commit changes no file.
        let start = self.git(&["commit-tree", "HEAD:", "-p", "HEAD", "-m", &message])?;
        self.git(&["branch", &branch, &start])?;
        self.git(&["checkout", "--quiet", &branch])?;
        Ok(branch)
    }

    /// Add the local files to `.git/info/exclude`, once. The lines are
    /// appended, so the patterns of the user stay as they are.
    fn exclude_local_files(&self) -> Result<(), String> {
        use std::io::Write as _;
        let path = self
            .root
            .join(self.git(&["rev-parse", "--git-path", "info/exclude"])?);
        let text = read_or_empty(&path)?;
        let missing: Vec<String> = [RESULTS, "run.log", STATE_DIR]
            .iter()
            .map(|name| format!("/{name}"))
            .filter(|line| !text.lines().any(|l| l == line))
            .collect();
        if missing.is_empty() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("cannot create {}: {e}", parent.display()))?;
        }
        let mut lines = missing.join("\n") + "\n";
        if !text.is_empty() && !text.ends_with('\n') {
            lines.insert(0, '\n');
        }
        std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .and_then(|mut file| file.write_all(lines.as_bytes()))
            .map_err(|e| format!("cannot write {}: {e}", path.display()))
    }

    /// The full id of `HEAD`.
    ///
    /// # Errors
    ///
    /// Returns an error when git fails.
    pub fn head(&self) -> Result<String, String> {
        self.full("HEAD")
    }

    /// The full id of `rev`.
    fn full(&self, rev: &str) -> Result<String, String> {
        self.git(&["rev-parse", "--verify", rev])
    }

    /// The full id of the one commit of the branch whose id starts with
    /// `prefix`. Only `HEAD` and its ancestors count.
    ///
    /// Git looks for the prefix in the whole object store, with each reset
    /// commit and each other branch. The branch holds far fewer commits.
    ///
    /// # Errors
    ///
    /// Returns an error when `prefix` is not 4 or more hex digits, or when
    /// it names no commit of the branch, or more than one.
    pub fn on_branch(&self, prefix: &str) -> Result<String, String> {
        let prefix = prefix.trim().to_ascii_lowercase();
        if prefix.len() < 4 || !prefix.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("{prefix:?} is not a commit id"));
        }
        let ids = self.git(&["rev-list", "HEAD"])?;
        let mut found = ids.lines().filter(|id| id.starts_with(&prefix));
        match (found.next(), found.next()) {
            (Some(id), None) => Ok(id.to_string()),
            (None, _) => Err(format!("{prefix} is not a commit of this branch")),
            (Some(_), Some(_)) => Err(format!(
                "{prefix} names more than one commit of this branch"
            )),
        }
    }

    /// Commit `train.py` on `parent` with `message` and a [`KEY_TRAILER`] of
    /// `key`.
    ///
    /// When `HEAD` already carries `key`, return it and commit nothing. When
    /// `train.py` has no change, return `HEAD` with `changed: false`.
    ///
    /// # Errors
    ///
    /// Returns an error when `HEAD` is not `parent`, or git fails.
    pub fn commit_train(
        &self,
        message: &str,
        key: &str,
        parent: &str,
    ) -> Result<CommitInfo, String> {
        if let Some(info) = self.committed(key)? {
            return Ok(info);
        }
        self.require_head(parent)?;
        let trailer = format!("{KEY_TRAILER}: {key}");
        let status = self.git(&["status", "--porcelain", "--", TRAIN])?;
        if status.is_empty() {
            return self.describe_head(false);
        }
        let message = format!("{}\n\n{trailer}", message.trim());
        self.git(&["commit", "--quiet", "-m", &message, "--", TRAIN])?;
        self.describe_head(true)
    }

    /// `HEAD` as a [`CommitInfo`] when it carries the trailer of `key`.
    ///
    /// # Errors
    ///
    /// Returns an error when git fails.
    pub fn committed(&self, key: &str) -> Result<Option<CommitInfo>, String> {
        let trailer = format!("{KEY_TRAILER}: {key}");
        let message = self.git(&["log", "-1", "--format=%B"])?;
        if message.lines().any(|line| line.trim() == trailer) {
            self.describe_head(true).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Fail unless `HEAD` is `commit`.
    ///
    /// # Errors
    ///
    /// Returns an error that names both commits when they differ.
    pub fn require_head(&self, commit: &str) -> Result<(), String> {
        let head = self.head()?;
        if head == commit {
            Ok(())
        } else {
            Err(format!("HEAD is {head}, not {commit}: the branch moved"))
        }
    }

    /// `HEAD` as a [`CommitInfo`]. With `changed: false`, the parent is
    /// `HEAD` itself and no lines changed.
    fn describe_head(&self, changed: bool) -> Result<CommitInfo, String> {
        let commit = self.head()?;
        if !changed {
            return Ok(CommitInfo {
                parent: commit.clone(),
                commit,
                changed,
                lines_added: 0,
                lines_removed: 0,
            });
        }
        let parent = self.full("HEAD^")?;
        // `--numstat` prints "<added> <removed> <path>".
        let stat = self.git(&["diff", "--numstat", "HEAD^", "HEAD", "--", TRAIN])?;
        let mut numbers = stat
            .split_whitespace()
            .take(2)
            .map(|n| n.parse::<u32>().unwrap_or(0));
        Ok(CommitInfo {
            commit,
            parent,
            changed,
            lines_added: numbers.next().unwrap_or(0),
            lines_removed: numbers.next().unwrap_or(0),
        })
    }

    /// Reset the branch and the work tree to `commit`.
    ///
    /// # Errors
    ///
    /// Returns an error when `commit` is unknown or git fails.
    pub fn reset_to(&self, commit: &str) -> Result<(), String> {
        let target = self.git(&[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{commit}^{{commit}}"),
        ])?;
        self.git(&["reset", "--quiet", "--hard", &target])?;
        Ok(())
    }

    /// The diff of `train.py` against `HEAD`.
    ///
    /// # Errors
    ///
    /// Returns an error when git fails.
    pub fn diff_train(&self) -> Result<String, String> {
        self.git(&["diff", "HEAD", "--", TRAIN])
    }

    /// Read one of the [`READABLE`] files for an agent. A missing file reads
    /// as empty. `results.tsv` reads as its header and the newest
    /// [`READ_ROWS`] rows.
    ///
    /// # Errors
    ///
    /// Returns an error for any other path, or a file that is not a regular
    /// file.
    pub fn read(&self, name: &str) -> Result<String, String> {
        if !READABLE.contains(&name) {
            return Err(format!(
                "{name:?} is not readable; read one of {}",
                READABLE.join(", ")
            ));
        }
        let Some(text) = read_capped(&self.root.join(name))? else {
            return Ok(String::new());
        };
        if name != RESULTS {
            return Ok(text);
        }
        let rows: Vec<&str> = text
            .lines()
            .filter(|line| !line.trim().is_empty() && *line != HEADER)
            .collect();
        let newest = &rows[rows.len().saturating_sub(READ_ROWS)..];
        if newest.is_empty() {
            return Ok(String::new());
        }
        Ok(format!("{HEADER}\n{}\n", newest.join("\n")))
    }

    /// Read `papers/<name>.md`. The name is letters, digits, `-` and `_`.
    ///
    /// # Errors
    ///
    /// Returns an error for any other name, or a paper that does not exist.
    pub fn read_paper(&self, name: &str) -> Result<String, String> {
        let valid = !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
        if !valid {
            return Err(format!(
                "{name:?} is not a paper name: use letters, digits, '-' and '_'"
            ));
        }
        read_capped(&self.root.join(PAPERS_DIR).join(format!("{name}.md")))?
            .ok_or_else(|| format!("no paper named {name:?}"))
    }

    /// Replace the one occurrence of `old` in `train.py` with `new`.
    ///
    /// # Errors
    ///
    /// Returns an error when `old` is empty, absent, or not unique.
    pub fn edit_train(&self, old: &str, new: &str) -> Result<(), String> {
        let _guard = WRITE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let path = self.root.join(TRAIN);
        let text = read_train(&path)?;
        crate::stores::write_atomic(&path, replace_once(&text, old, new)?.as_bytes())
    }

    /// [`Lab::edit_train`] once per `call`, a key of the tool call.
    ///
    /// A marker with the hashes of the file before and after the edit is
    /// written before the file. A re-run of `call` reads it. When the file
    /// is no longer at the "before" hash, the edit ran, so the re-run
    /// changes nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the edit does not apply, or a write fails.
    pub fn edit_train_once(&self, old: &str, new: &str, call: &str) -> Result<(), String> {
        let _guard = WRITE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let path = self.root.join(TRAIN);
        let text = read_train(&path)?;
        let dir = self.state_dir()?.join(EDITS);
        private_dir(&dir)?;
        let marker = dir.join(crate::stores::hashed(call));
        let before = crate::stores::hashed(&text);
        if let Ok(saved) = std::fs::read_to_string(&marker)
            && saved.lines().next() != Some(before.as_str())
        {
            return Ok(());
        }
        let edited = replace_once(&text, old, new)?;
        let note = format!("{before}\n{}\n", crate::stores::hashed(&edited));
        crate::stores::write_atomic(&marker, note.as_bytes())?;
        crate::stores::write_atomic(&path, edited.as_bytes())
    }

    /// Append `row` to `results.tsv`, with the header on a new file.
    ///
    /// `key` names the call, such as the activity idempotency key. The key
    /// and the index of its row go to the state directory before the row
    /// goes to `results.tsv`. A retry with the same key finds its row and
    /// appends nothing. Two experiments with equal rows both land.
    ///
    /// Returns `true` when the row was appended.
    ///
    /// # Errors
    ///
    /// Returns an error when a file cannot be read or written.
    pub fn record(&self, row: &ResultRow, key: &str) -> Result<bool, String> {
        let _guard = WRITE_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
        let keys_path = self.state_dir()?.join(RECORDED);
        let mut keys = read_or_empty(&keys_path)?;
        let path = self.root.join(RESULTS);
        let mut text = read_or_empty(&path)?;
        let held = data_lines(&text).count();
        let key = crate::stores::hashed(key);
        let index = keys
            .lines()
            .filter_map(|line| line.split_once('\t'))
            .find(|(k, _)| *k == key)
            .and_then(|(_, index)| index.parse::<usize>().ok());
        match index {
            // The row landed before a crash or a lost result.
            Some(index) if index < held => return Ok(false),
            // The key landed, and the row did not.
            Some(_) => {}
            None => {
                let _ = writeln!(keys, "{key}\t{held}");
                crate::stores::write_atomic(&keys_path, keys.as_bytes())?;
            }
        }
        if text.trim().is_empty() {
            text = format!("{HEADER}\n");
        } else if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&row.to_line());
        text.push('\n');
        crate::stores::write_atomic(&path, text.as_bytes())?;
        Ok(true)
    }

    /// Every row of `results.tsv`. A missing file has no rows. A line that
    /// does not parse, such as a hand edit, is skipped.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read.
    pub fn rows(&self) -> Result<Vec<ResultRow>, String> {
        let text = read_or_empty(&self.root.join(RESULTS))?;
        Ok(data_lines(&text)
            .filter_map(|line| ResultRow::parse_line(line).ok())
            .collect())
    }

    /// The newest kept row of `commit`, a full id or its short form.
    ///
    /// A row names its commit by a prefix: the first 7 hex digits, as
    /// upstream. When another commit in the lab has that prefix too, the row
    /// cannot tell the two apart. The lookup then fails, so it never gives
    /// the score of the wrong commit.
    ///
    /// # Errors
    ///
    /// Returns an error when `results.tsv` cannot be read, git fails, or
    /// the prefix of the row names more than one commit.
    pub fn kept(&self, commit: &str) -> Result<Option<ResultRow>, String> {
        let Some(row) = self.rows()?.into_iter().rev().find(|row| {
            row.status == crate::rules::Status::Keep
                && row.commit.len() >= 4
                && commit.starts_with(&row.commit)
        }) else {
            return Ok(None);
        };
        let named = self.commits_named(&row.commit)?;
        if named > 1 {
            return Err(format!(
                "{named} commits have the id {}, so results.tsv cannot tell their rows apart",
                row.commit
            ));
        }
        Ok(Some(row))
    }

    /// The number of commits in the object store whose id starts with
    /// `prefix`, which has 4 or more hex digits.
    fn commits_named(&self, prefix: &str) -> Result<usize, String> {
        let objects = self.git(&["rev-parse", &format!("--disambiguate={prefix}")])?;
        let mut commits = 0;
        for id in objects.lines() {
            if self.git(&["cat-file", "-t", id])? == "commit" {
                commits += 1;
            }
        }
        Ok(commits)
    }
}

/// Check that `tag` can name the branch `autoresearch/<tag>`.
///
/// # Errors
///
/// Returns an error that states the rule.
pub fn check_tag(tag: &str) -> Result<(), String> {
    let valid = !tag.is_empty()
        && tag.len() <= 64
        && tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
        && !tag.starts_with(['.', '-'])
        && !tag.ends_with('.')
        && !Path::new(tag)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("lock"))
        && !tag.contains("..");
    if valid {
        Ok(())
    } else {
        Err(format!(
            "the tag {tag:?} is not valid. Use 1 to 64 letters, digits, '.', '-' and '_'. \
             Do not start with '.' or '-', end with '.' or \".lock\", or use \"..\""
        ))
    }
}

/// Run git in `dir` with the example's author and [`SAFE_GIT`].
fn git_in(dir: &Path, args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .args(AUTHOR)
        .args(SAFE_GIT)
        .args(args)
        .current_dir(dir)
        .output()
        .map_err(|e| format!("cannot run git: {e}"))?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

/// Create `dir`, and let only its owner read it.
fn private_dir(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| format!("cannot protect {}: {e}", dir.display()))?;
    }
    Ok(())
}

/// The lines of a `results.tsv` text that can hold a row.
fn data_lines(text: &str) -> impl Iterator<Item = &str> {
    text.lines()
        .filter(|line| !line.trim().is_empty() && *line != HEADER)
}

/// The text of `path`, or an empty text when it does not exist.
fn read_or_empty(path: &Path) -> Result<String, String> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(String::from_utf8_lossy(&bytes).into_owned()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(format!("cannot read {}: {e}", path.display())),
    }
}

/// The text of `train.py`.
fn read_train(path: &Path) -> Result<String, String> {
    std::fs::read_to_string(path).map_err(|e| format!("cannot read {TRAIN}: {e}"))
}

/// `text` with the one occurrence of `old` replaced by `new`.
fn replace_once(text: &str, old: &str, new: &str) -> Result<String, String> {
    if old.is_empty() {
        return Err("old must not be empty".into());
    }
    match text.matches(old).count() {
        0 => Err(format!("{old:?} does not occur in {TRAIN}")),
        1 => Ok(text.replacen(old, new, 1)),
        n => Err(format!(
            "{old:?} occurs {n} times in {TRAIN}; quote more of the line"
        )),
    }
}

/// Read at most [`MAX_READ`] bytes from the end of `path`, as lossy UTF-8.
/// Returns `None` when the file does not exist.
///
/// A symlink is refused, so a link that the agent's code planted cannot
/// send a file from outside the lab to the model.
fn read_capped(path: &Path) -> Result<Option<String>, String> {
    let name = path.display();
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("cannot read {name}: {e}")),
    };
    if !meta.is_file() {
        return Err(format!("{name} is not a regular file"));
    }
    let mut file = std::fs::File::open(path).map_err(|e| format!("cannot read {name}: {e}"))?;
    let skip = meta.len().saturating_sub(MAX_READ);
    file.seek(SeekFrom::Start(skip))
        .map_err(|e| format!("cannot read {name}: {e}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_READ)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("cannot read {name}: {e}"))?;
    let text = String::from_utf8_lossy(&bytes);
    if skip == 0 {
        return Ok(Some(text.into_owned()));
    }
    // Start at a whole line, and say what was cut.
    let rest = text.split_once('\n').map_or("", |(_, rest)| rest);
    Ok(Some(format!(
        "[the first {skip} bytes are not shown]\n{rest}"
    )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use crate::results::short;
    use crate::rules::Status;

    fn lab() -> (tempfile::TempDir, Lab) {
        let dir = tempfile::tempdir().unwrap();
        let lab = Lab::init(dir.path().join("lab")).unwrap();
        (dir, lab)
    }

    fn row(commit: &str) -> ResultRow {
        ResultRow {
            commit: commit.into(),
            val_bpb: 0.9979,
            memory_gb: 44.0,
            status: Status::Keep,
            description: "baseline".into(),
        }
    }

    #[test]
    fn init_writes_the_template_and_one_commit() {
        let (_dir, lab) = lab();
        for (name, _) in TEMPLATE {
            assert!(lab.root().join(name).is_file(), "{name}");
        }
        assert_eq!(lab.git(&["rev-list", "--count", "HEAD"]).unwrap(), "1");
        let head = lab.head().unwrap();
        assert!(
            head.len() >= 40 && head.bytes().all(|b| b.is_ascii_hexdigit()),
            "HEAD is a full object id: {head}"
        );
        assert!(
            Lab::init(lab.root()).is_err(),
            "init refuses a used directory"
        );
    }

    #[test]
    fn open_refuses_a_directory_that_is_not_a_work_tree() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Lab::open(dir.path()).is_err());
    }

    #[test]
    fn start_branch_creates_or_resumes_the_tag_branch() {
        let (_dir, lab) = lab();
        assert_eq!(
            lab.start_branch("oct17", "s1").unwrap(),
            "autoresearch/oct17"
        );
        assert_eq!(
            lab.git(&["branch", "--show-current"]).unwrap(),
            "autoresearch/oct17"
        );
        // The same setup again, after a crash: it resumes the branch.
        lab.git(&["checkout", "--quiet", "master"]).unwrap();
        assert_eq!(
            lab.start_branch("oct17", "s1").unwrap(),
            "autoresearch/oct17"
        );
        lab.require_branch("autoresearch/oct17").unwrap();
        for bad in [
            "bad tag",
            "../escape",
            "x.lock",
            "x.",
            ".x",
            "-x",
            "a..b",
            "",
        ] {
            assert!(lab.start_branch(bad, "s2").is_err(), "{bad}");
        }
        assert!(check_tag("Oct17-a_1.b").is_ok());
    }

    #[test]
    fn a_tag_that_another_session_used_is_refused() {
        let (_dir, lab) = lab();
        lab.start_branch("oct17", "s1").unwrap();
        let used = lab.start_branch("oct17", "s2").unwrap_err();
        assert!(used.contains("exists"), "{used}");
        // A branch that a person made has no start commit either.
        lab.git(&["branch", "autoresearch/hand", "master"]).unwrap();
        assert!(lab.start_branch("hand", "s3").is_err());
    }

    /// Each session starts on its own commit, so a row of one session never
    /// names the commit of another.
    #[test]
    fn two_sessions_never_share_a_commit() {
        let (_dir, lab) = lab();
        let base = lab.head().unwrap();
        lab.start_branch("a", "s1").unwrap();
        let a = lab.head().unwrap();
        lab.git(&["checkout", "--quiet", "master"]).unwrap();
        lab.start_branch("b", "s2").unwrap();
        let b = lab.head().unwrap();
        assert_ne!(a, b);
        assert_ne!(a, base);
        assert_eq!(lab.git(&["rev-parse", &format!("{a}^")]).unwrap(), base);
        assert_eq!(lab.git(&["diff", "--stat", &base, &a]).unwrap(), "");
    }

    #[test]
    fn a_new_branch_refuses_a_train_py_that_no_commit_holds() {
        let (_dir, lab) = lab();
        lab.edit_train("DEPTH = 8", "DEPTH = 9").unwrap();
        let refused = lab.start_branch("t1", "s1").unwrap_err();
        assert!(refused.contains("no commit holds"), "{refused}");
        assert_eq!(lab.git(&["branch", "--show-current"]).unwrap(), "master");
    }

    #[test]
    fn checkout_moves_back_to_the_session_branch_only_when_clean() {
        let (_dir, lab) = lab();
        lab.start_branch("a", "s1").unwrap();
        lab.start_branch("b", "s2").unwrap();
        assert!(lab.require_branch("autoresearch/a").is_err());
        lab.checkout("autoresearch/a").unwrap();
        lab.require_branch("autoresearch/a").unwrap();
        lab.edit_train("DEPTH = 8", "DEPTH = 9").unwrap();
        assert!(lab.checkout("autoresearch/b").is_err());
        lab.require_branch("autoresearch/a").unwrap();
    }

    #[test]
    fn the_exclude_file_keeps_the_patterns_of_the_user() {
        let (_dir, lab) = lab();
        let exclude = lab.root().join(".git/info/exclude");
        let mut user = b"/build/\n/caf".to_vec();
        user.push(0xe9);
        user.extend_from_slice(b"/\n*.ckpt");
        std::fs::write(&exclude, &user).unwrap();
        lab.start_branch("t1", "s1").unwrap();
        let after = std::fs::read(&exclude).unwrap();
        assert!(
            after.starts_with(&user),
            "the user lines stay byte for byte"
        );
        let text = String::from_utf8_lossy(&after);
        assert!(text.contains("\n/results.tsv\n") && text.contains("/.autoresearch"));
    }

    #[test]
    fn local_files_stay_untracked() {
        let (_dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        lab.record(&row("aaaaaaa"), "k").unwrap();
        std::fs::write(lab.root().join("run.log"), "x").unwrap();
        lab.state_dir().unwrap();
        assert_eq!(lab.git(&["status", "--porcelain"]).unwrap(), "");
    }

    #[test]
    fn a_commit_is_made_once_per_key() {
        let (_dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        let base = lab.head().unwrap();
        lab.edit_train("MATRIX_LR = 0.04", "MATRIX_LR = 0.05")
            .unwrap();
        let first = lab.commit_train("raise matrix LR", "k-1", &base).unwrap();
        assert!(first.changed);
        assert_eq!(first.parent, base);
        assert_eq!((first.lines_added, first.lines_removed), (1, 1));
        // A re-run after a crash finds the same commit.
        let again = lab.commit_train("raise matrix LR", "k-1", &base).unwrap();
        assert_eq!(again, first);
        assert_eq!(
            lab.git(&["rev-list", "--count", "master..HEAD"]).unwrap(),
            "2"
        );
    }

    #[test]
    fn a_commit_refuses_a_head_that_moved() {
        let (_dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        lab.edit_train("DEPTH = 8", "DEPTH = 9").unwrap();
        let refused = lab.commit_train("deeper", "k-1", "0000000").unwrap_err();
        assert!(refused.contains("the branch moved"), "{refused}");
    }

    #[test]
    fn a_commit_with_no_change_reports_head() {
        let (_dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        let head = lab.head().unwrap();
        let info = lab.commit_train("nothing", "k-2", &head).unwrap();
        assert!(!info.changed);
        assert_eq!(info.commit, lab.head().unwrap());
    }

    #[test]
    fn reset_restores_the_base_and_is_idempotent() {
        let (_dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        let base = lab.head().unwrap();
        lab.edit_train("DEPTH = 8", "DEPTH = 10").unwrap();
        lab.commit_train("deeper", "k-3", &base).unwrap();
        lab.reset_to(&base).unwrap();
        lab.reset_to(&base).unwrap();
        assert_eq!(lab.head().unwrap(), base);
        assert!(lab.read(TRAIN).unwrap().contains("DEPTH = 8"));
        assert!(lab.reset_to("0000000").is_err());
    }

    #[test]
    fn the_agent_reads_only_known_files_and_papers() {
        let (_dir, lab) = lab();
        assert!(lab.read(TRAIN).unwrap().contains("MATRIX_LR"));
        assert!(lab.read(PREPARE).unwrap().contains("TIME_BUDGET"));
        assert_eq!(lab.read(RESULTS).unwrap(), "");
        for bad in [
            ".git/config",
            "../etc/passwd",
            "/etc/passwd",
            "lab/train.py",
        ] {
            assert!(lab.read(bad).is_err(), "{bad}");
        }
        assert!(lab.read_paper("muon").unwrap().contains("Muon"));
        for bad in ["../train", "muon.md", "", "nope"] {
            assert!(lab.read_paper(bad).is_err(), "{bad}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_read_refuses_a_symlink_and_returns_the_end_of_a_long_file() {
        let (dir, lab) = lab();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, "key").unwrap();
        std::os::unix::fs::symlink(&secret, lab.root().join("run.log")).unwrap();
        assert!(lab.read("run.log").is_err());
        std::fs::remove_file(lab.root().join("run.log")).unwrap();

        let long = format!(
            "{}\nthe end\n",
            "x".repeat(usize::try_from(MAX_READ).unwrap() * 2)
        );
        std::fs::write(lab.root().join("run.log"), long).unwrap();
        let text = lab.read("run.log").unwrap();
        assert!(text.len() < usize::try_from(MAX_READ).unwrap() + 100);
        assert!(text.starts_with("[the first") && text.ends_with("the end\n"));
    }

    #[test]
    fn the_agent_reads_the_newest_rows_of_results() {
        let (_dir, lab) = lab();
        for n in 0..READ_ROWS + 5 {
            lab.record(&row(&format!("{n:07}")), &format!("k{n}"))
                .unwrap();
        }
        let text = lab.read(RESULTS).unwrap();
        assert!(text.starts_with(HEADER));
        assert_eq!(text.lines().count(), READ_ROWS + 1);
        assert!(!text.contains("0000004\t") && text.contains("0000005\t"));
        assert_eq!(lab.rows().unwrap().len(), READ_ROWS + 5);
    }

    #[cfg(unix)]
    #[test]
    fn only_the_owner_can_read_the_state_directory() {
        use std::os::unix::fs::PermissionsExt as _;
        let (_dir, lab) = lab();
        let mode = std::fs::metadata(lab.state_dir().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    #[test]
    fn an_edit_needs_one_exact_match() {
        let (_dir, lab) = lab();
        assert!(lab.edit_train("", "x").is_err());
        assert!(lab.edit_train("NO SUCH TEXT", "x").is_err());
        assert!(lab.edit_train("LR", "x").is_err(), "LR occurs many times");
        lab.edit_train("WEIGHT_DECAY = 0.2", "WEIGHT_DECAY = 0.1")
            .unwrap();
        assert!(lab.read(TRAIN).unwrap().contains("WEIGHT_DECAY = 0.1"));
    }

    #[test]
    fn an_edit_runs_once_per_call() {
        let (_dir, lab) = lab();
        let insert = "DEPTH = 8\nEXTRA_KNOB = 1";
        lab.edit_train_once("DEPTH = 8", insert, "run/0/call_1")
            .unwrap();
        // The same call again, after a crash: the insertion is not doubled.
        lab.edit_train_once("DEPTH = 8", insert, "run/0/call_1")
            .unwrap();
        assert_eq!(lab.read(TRAIN).unwrap().matches("EXTRA_KNOB").count(), 1);
        // A new call is a new edit.
        assert!(
            lab.edit_train_once("DEPTH = 8", insert, "run/1/call_1")
                .is_ok()
        );
        assert_eq!(lab.read(TRAIN).unwrap().matches("EXTRA_KNOB").count(), 2);
    }

    #[test]
    fn an_edit_whose_write_was_lost_runs_on_its_retry() {
        let (_dir, lab) = lab();
        let before = lab.read(TRAIN).unwrap();
        lab.edit_train_once("DEPTH = 8", "DEPTH = 9", "c").unwrap();
        // The crash came after the marker and before the file.
        std::fs::write(lab.root().join(TRAIN), &before).unwrap();
        lab.edit_train_once("DEPTH = 8", "DEPTH = 9", "c").unwrap();
        assert!(lab.read(TRAIN).unwrap().contains("DEPTH = 9"));
    }

    #[test]
    fn a_row_is_recorded_once_per_key() {
        let (_dir, lab) = lab();
        assert!(lab.record(&row("aaaaaaa"), "k1").unwrap());
        assert!(!lab.record(&row("aaaaaaa"), "k1").unwrap());
        // An equal row from another experiment lands too.
        assert!(lab.record(&row("aaaaaaa"), "k2").unwrap());
        assert!(lab.record(&row("bbbbbbb"), "k3").unwrap());
        let text = lab.read(RESULTS).unwrap();
        assert!(text.starts_with(HEADER));
        assert_eq!(lab.rows().unwrap().len(), 3);
    }

    #[test]
    fn a_row_whose_write_was_lost_lands_on_its_retry() {
        let (_dir, lab) = lab();
        lab.record(&row("aaaaaaa"), "k1").unwrap();
        let rows = lab.root().join(RESULTS);
        let before = std::fs::read_to_string(&rows).unwrap();
        lab.record(&row("bbbbbbb"), "k2").unwrap();
        // The crash came after the key and before the row.
        std::fs::write(&rows, before).unwrap();
        assert!(lab.record(&row("bbbbbbb"), "k2").unwrap());
        assert_eq!(lab.rows().unwrap().len(), 2);
    }

    #[test]
    fn a_line_that_does_not_parse_is_skipped() {
        let (_dir, lab) = lab();
        lab.record(&row("aaaaaaa"), "k1").unwrap();
        let path = lab.root().join(RESULTS);
        let mut text = std::fs::read_to_string(&path).unwrap();
        text.push_str("a hand edit\n");
        std::fs::write(&path, text).unwrap();
        assert!(lab.record(&row("bbbbbbb"), "k2").unwrap());
        assert_eq!(lab.rows().unwrap().len(), 2);
    }

    #[test]
    fn kept_finds_a_row_by_the_full_id_of_its_commit() {
        let (_dir, lab) = lab();
        let head = lab.head().unwrap();
        lab.record(&row(&short(&head)), "k1").unwrap();
        assert_eq!(lab.kept(&head).unwrap(), Some(row(&short(&head))));
        assert_eq!(lab.kept(&short(&head)).unwrap(), Some(row(&short(&head))));
    }

    /// Two commits on `HEAD` whose ids share their first 4 hex digits.
    fn colliding(dir: &Path, lab: &Lab) -> (String, String) {
        let base = lab.head().unwrap();
        let tree = lab.git(&["rev-parse", "HEAD:"]).unwrap();
        let objects = dir.join("objects");
        std::fs::create_dir(&objects).unwrap();
        let paths: Vec<String> = (0..2000)
            .map(|n| {
                let path = objects.join(n.to_string());
                let body = format!(
                    "tree {tree}\nparent {base}\nauthor a <a@b> 0 +0000\n\
                     committer a <a@b> 0 +0000\n\ncandidate {n}\n"
                );
                std::fs::write(&path, body).unwrap();
                path.display().to_string()
            })
            .collect();
        let mut args = vec!["hash-object", "-t", "commit", "-w"];
        args.extend(paths.iter().map(String::as_str));
        let ids = lab.git(&args).unwrap();
        let mut seen = std::collections::HashMap::new();
        ids.lines()
            .find_map(|id| {
                seen.insert(&id[..4], id)
                    .map(|other| (id.to_string(), other.to_string()))
            })
            .unwrap()
    }

    /// Two commits whose ids share a prefix: one on the branch, one not.
    /// Git cannot resolve the prefix. The branch can.
    #[test]
    fn a_short_id_names_one_commit_of_the_branch() {
        let (dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        let base = lab.head().unwrap();
        let (on, off) = colliding(dir.path(), &lab);
        lab.git(&["reset", "--quiet", "--hard", &on]).unwrap();
        let prefix = &on[..4];
        assert!(
            lab.git(&["rev-parse", "--verify", &format!("{prefix}^{{commit}}")])
                .is_err(),
            "git finds two commits for {prefix}"
        );
        assert_eq!(lab.on_branch(prefix).unwrap(), on);
        assert_eq!(lab.on_branch(&short(&on)).unwrap(), on);
        assert_eq!(lab.on_branch(&base).unwrap(), base);
        assert!(lab.on_branch(&short(&off)).is_err(), "not on the branch");
        assert!(lab.on_branch("ab").is_err(), "too short");
        assert!(lab.on_branch("HEAD~1").is_err(), "not hex");
    }

    /// A row names its commit by a prefix. When two commits share it, the
    /// row cannot tell them apart, so the lookup fails. It never gives the
    /// score of the wrong commit.
    #[test]
    fn kept_refuses_a_row_whose_id_names_two_commits() {
        let (dir, lab) = lab();
        lab.start_branch("t1", "s1").unwrap();
        let (on, _off) = colliding(dir.path(), &lab);
        lab.git(&["reset", "--quiet", "--hard", &on]).unwrap();
        lab.record(&row(&on[..4]), "k1").unwrap();
        assert!(
            lab.kept(&on).is_err(),
            "two commits share the id of the row"
        );
        lab.record(&row(&short(&on)), "k2").unwrap();
        assert_eq!(lab.kept(&on).unwrap(), Some(row(&short(&on))));
    }
}
