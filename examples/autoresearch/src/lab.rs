//! The lab: a git work tree with `train.py`, `prepare.py` and `program.md`.
//!
//! Every git call goes through [`Lab::git`]. Each write is idempotent, because
//! an activity can run again after a crash:
//!
//! - [`Lab::commit_train`] finds its own commit by an `Experiment-Key`
//!   trailer and does not commit twice.
//! - [`Lab::record`] does not append a row that `results.tsv` already holds.
//! - [`Lab::reset_to`] is a no-op at its target.
//!
//! The agent tools read only [`READABLE`] files and write only `train.py`.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::results::ResultRow;

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
/// The prefix of every research branch.
pub const BRANCH_PREFIX: &str = "autoresearch/";
/// The author of every commit that the example makes.
const AUTHOR: [&str; 4] = [
    "-c",
    "user.name=autoresearch",
    "-c",
    "user.email=autoresearch@localhost",
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
    /// The short hash of the commit, or of `HEAD` when nothing changed.
    pub commit: String,
    /// The short hash of the parent: the reset target.
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
        let run = |args: &[&str]| {
            let out = Command::new("git")
                .args(AUTHOR)
                .args(args)
                .current_dir(path)
                .output()
                .map_err(|e| format!("cannot run git: {e}"))?;
            if out.status.success() {
                Ok(())
            } else {
                Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
            }
        };
        run(&["init", "--quiet", "--initial-branch=master"])?;
        run(&["add", "--all"])?;
        run(&[
            "commit",
            "--quiet",
            "-m",
            "baseline lab from the autoresearch template",
        ])?;
        let lab = Self::open(path)?;
        lab.exclude_local_files()?;
        Ok(lab)
    }

    /// The canonical root of the lab.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The state directory, created on first use.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory cannot be created.
    pub fn state_dir(&self) -> Result<PathBuf, String> {
        let dir = self.root.join(STATE_DIR);
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        Ok(dir)
    }

    /// Run git in the lab and return its trimmed stdout.
    ///
    /// # Errors
    ///
    /// Returns git's stderr when git exits non-zero.
    pub fn git(&self, args: &[&str]) -> Result<String, String> {
        let out = Command::new("git")
            .args(AUTHOR)
            .args(args)
            .current_dir(&self.root)
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

    /// Check out `autoresearch/<tag>`. Create it from `HEAD` when it does not
    /// exist. Hide `results.tsv`, `run.log` and the state directory from git.
    ///
    /// # Errors
    ///
    /// Returns an error when the tag is not a valid branch name, or git fails.
    pub fn start_branch(&self, tag: &str) -> Result<String, String> {
        let valid = !tag.is_empty()
            && tag.len() <= 64
            && tag
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.')
            && !tag.starts_with(['.', '-'])
            && !tag.contains("..");
        if !valid {
            return Err(format!(
                "the tag {tag:?} must be 1 to 64 of a-z, 0-9, '.', '-' and '_'"
            ));
        }
        let branch = format!("{BRANCH_PREFIX}{tag}");
        self.exclude_local_files()?;
        if self.git(&["branch", "--show-current"])? == branch {
            return Ok(branch);
        }
        let exists = self
            .git(&[
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ])
            .is_ok();
        if exists {
            self.git(&["checkout", "--quiet", &branch])?;
        } else {
            self.git(&["checkout", "--quiet", "-b", &branch])?;
        }
        Ok(branch)
    }

    /// Add the local files to `.git/info/exclude`, once.
    fn exclude_local_files(&self) -> Result<(), String> {
        let path = self.git(&["rev-parse", "--git-path", "info/exclude"])?;
        let path = self.root.join(path);
        let text = std::fs::read_to_string(&path).unwrap_or_default();
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
        let mut text = text;
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        for line in missing {
            text.push_str(&line);
            text.push('\n');
        }
        std::fs::write(&path, text).map_err(|e| format!("cannot write {}: {e}", path.display()))
    }

    /// The short hash of `HEAD`.
    ///
    /// # Errors
    ///
    /// Returns an error when git fails.
    pub fn head(&self) -> Result<String, String> {
        self.git(&["rev-parse", "--short=7", "HEAD"])
    }

    /// Commit `train.py` with `message` and an [`KEY_TRAILER`] of `key`.
    ///
    /// When `HEAD` already carries `key`, return it and commit nothing. When
    /// `train.py` has no change, return `HEAD` with `changed: false`.
    ///
    /// # Errors
    ///
    /// Returns an error when git fails.
    pub fn commit_train(&self, message: &str, key: &str) -> Result<CommitInfo, String> {
        let trailer = format!("{KEY_TRAILER}: {key}");
        let head_message = self.git(&["log", "-1", "--format=%B"])?;
        if head_message.lines().any(|line| line.trim() == trailer) {
            return self.describe_head(true);
        }
        let status = self.git(&["status", "--porcelain", "--", TRAIN])?;
        if status.is_empty() {
            return self.describe_head(false);
        }
        let message = format!("{}\n\n{trailer}", message.trim());
        self.git(&["commit", "--quiet", "-m", &message, "--", TRAIN])?;
        self.describe_head(true)
    }

    /// `HEAD` as a [`CommitInfo`]. `changed: false` has no parent or lines.
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
        let parent = self.git(&["rev-parse", "--short=7", "HEAD^"])?;
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

    /// Read one of the [`READABLE`] files. A missing file reads as empty.
    ///
    /// # Errors
    ///
    /// Returns an error for any other path.
    pub fn read(&self, name: &str) -> Result<String, String> {
        if !READABLE.contains(&name) {
            return Err(format!(
                "{name:?} is not readable; read one of {}",
                READABLE.join(", ")
            ));
        }
        match std::fs::read_to_string(self.root.join(name)) {
            Ok(text) => Ok(text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
            Err(e) => Err(format!("cannot read {name}: {e}")),
        }
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
        std::fs::read_to_string(self.root.join(PAPERS_DIR).join(format!("{name}.md")))
            .map_err(|_| format!("no paper named {name:?}"))
    }

    /// Replace the one occurrence of `old` in `train.py` with `new`.
    ///
    /// # Errors
    ///
    /// Returns an error when `old` is empty, absent, or not unique.
    pub fn edit_train(&self, old: &str, new: &str) -> Result<(), String> {
        if old.is_empty() {
            return Err("old must not be empty".into());
        }
        let path = self.root.join(TRAIN);
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("cannot read {TRAIN}: {e}"))?;
        match text.matches(old).count() {
            0 => Err(format!("{old:?} does not occur in {TRAIN}")),
            1 => {
                let edited = text.replacen(old, new, 1);
                crate::stores::write_atomic(&path, edited.as_bytes())
            }
            n => Err(format!(
                "{old:?} occurs {n} times in {TRAIN}; quote more of the line"
            )),
        }
    }

    /// Append `row` to `results.tsv`, with the header on a new file. A row
    /// with the same commit and description is not appended twice.
    ///
    /// Returns `true` when the row was appended.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read or written.
    pub fn record(&self, row: &ResultRow) -> Result<bool, String> {
        let rows = self.rows()?;
        let held = rows.iter().any(|r| {
            r.commit == row.commit
                && r.description == row.description.trim()
                && r.status == row.status
        });
        if held {
            return Ok(false);
        }
        let path = self.root.join(RESULTS);
        let mut text = std::fs::read_to_string(&path).unwrap_or_default();
        if text.trim().is_empty() {
            text = format!("{}\n", crate::results::HEADER);
        } else if !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str(&row.to_line());
        text.push('\n');
        crate::stores::write_atomic(&path, text.as_bytes())?;
        Ok(true)
    }

    /// Every row of `results.tsv`. A missing file has no rows.
    ///
    /// # Errors
    ///
    /// Returns an error when the file does not parse.
    pub fn rows(&self) -> Result<Vec<ResultRow>, String> {
        crate::results::parse(&self.read(RESULTS)?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
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
        assert_eq!(lab.head().unwrap().len(), 7);
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
        assert_eq!(lab.start_branch("oct17").unwrap(), "autoresearch/oct17");
        assert_eq!(
            lab.git(&["branch", "--show-current"]).unwrap(),
            "autoresearch/oct17"
        );
        // Again: it resumes the branch.
        assert_eq!(lab.start_branch("oct17").unwrap(), "autoresearch/oct17");
        assert!(lab.start_branch("bad tag").is_err());
        assert!(lab.start_branch("../escape").is_err());
    }

    #[test]
    fn local_files_stay_untracked() {
        let (_dir, lab) = lab();
        lab.start_branch("t1").unwrap();
        lab.record(&row("aaaaaaa")).unwrap();
        std::fs::write(lab.root().join("run.log"), "x").unwrap();
        lab.state_dir().unwrap();
        assert_eq!(lab.git(&["status", "--porcelain"]).unwrap(), "");
    }

    #[test]
    fn a_commit_is_made_once_per_key() {
        let (_dir, lab) = lab();
        lab.start_branch("t1").unwrap();
        let base = lab.head().unwrap();
        lab.edit_train("MATRIX_LR = 0.04", "MATRIX_LR = 0.05")
            .unwrap();
        let first = lab.commit_train("raise matrix LR", "k-1").unwrap();
        assert!(first.changed);
        assert_eq!(first.parent, base);
        assert_eq!((first.lines_added, first.lines_removed), (1, 1));
        // A re-run after a crash finds the same commit.
        let again = lab.commit_train("raise matrix LR", "k-1").unwrap();
        assert_eq!(again, first);
        assert_eq!(lab.git(&["rev-list", "--count", "HEAD"]).unwrap(), "2");
    }

    #[test]
    fn a_commit_with_no_change_reports_head() {
        let (_dir, lab) = lab();
        lab.start_branch("t1").unwrap();
        let info = lab.commit_train("nothing", "k-2").unwrap();
        assert!(!info.changed);
        assert_eq!(info.commit, lab.head().unwrap());
    }

    #[test]
    fn reset_restores_the_base_and_is_idempotent() {
        let (_dir, lab) = lab();
        lab.start_branch("t1").unwrap();
        let base = lab.head().unwrap();
        lab.edit_train("DEPTH = 8", "DEPTH = 10").unwrap();
        lab.commit_train("deeper", "k-3").unwrap();
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
    fn a_row_is_recorded_once() {
        let (_dir, lab) = lab();
        assert!(lab.record(&row("aaaaaaa")).unwrap());
        assert!(!lab.record(&row("aaaaaaa")).unwrap());
        assert!(lab.record(&row("bbbbbbb")).unwrap());
        let text = lab.read(RESULTS).unwrap();
        assert!(text.starts_with(crate::results::HEADER));
        assert_eq!(lab.rows().unwrap().len(), 2);
    }
}
