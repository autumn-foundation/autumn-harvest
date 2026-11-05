//! `cargo xtask audit cli-flag-coverage`: the CLI-flag coverage matrix for `docs/`.
//!
//! This is the Folio corpus harness, ported from `docs/audits/cli-flag-coverage.py`.
//! It is deterministic and reproducible on any checkout. It needs no network access.
//! It is the inverse of `cargo xtask audit config-cli-drift`. That audit
//! catches a doc that cites a `--flag` that no longer exists, which is drift.
//! This one catches the other direction: a real `harvest` CLI flag that no
//! page of the corpus mentions at all. That is a coverage gap. See the
//! "Coverage matrix" entry of the Tier-1 list in `docs/audits/README.md`.
//!
//! The ground truth is the extraction that `config-cli-drift` performs over
//! the CLI sources: each `#[arg(long...)]` field. This module calls that code
//! and does not copy it. So the two audits can never disagree about what a
//! real flag is.
//!
//! **This audit is report-only by design and never fails CI.** Many flags of a
//! mature CLI are narrow, internal or self-explanatory, such as
//! `--limit-groups`, `--conf-file` and `--refill-per-sec`. A gate at zero for
//! each of them is the "comprehensiveness worship" that the doctrine of this
//! corpus rejects. Undocumented and stable beats documented and wrong. A blind
//! coverage gate pushes someone to pad a page just to turn a number green.
//! The number here is triage input for a human or Folio pass, not a build
//! gate. Sometimes a page walks a reader through the exact command of a flag
//! and stops short of the flag. Only then is the flag worth a fix. That judgment needs
//! a person who reads the prose around it, not a script.
//!
//! # Usage
//!
//! ```sh
//! cargo xtask audit cli-flag-coverage [--json]
//! ```
//!
//! The exit code is always 0.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

use super::config_cli_drift::{self, INVOCATION_LANGS};
use super::corpus_link_check;
use super::corpus_link_check::pycompat::{self, Json};
use crate::util;

/// Arguments of `cargo xtask audit cli-flag-coverage`.
#[derive(Debug, Args)]
pub struct Args {
    /// Print the result as JSON.
    #[arg(long)]
    pub json: bool,
}

/// Returns the file set that the orphan scan of `corpus-link-check` grades.
///
/// These are the pages under `docs/` that the real corpus reaches. A process
/// page, such as one in `docs/plans/` or `docs/rnd/`, counts only if a link
/// reaches it. The set is narrower than `corpus_files`, which also returns
/// `README.md`, `CHANGELOG.md` and `RELEASE_NOTES.md` as link sources. Those
/// are the front door, the territory of Onramp, not part of the reference
/// corpus that this coverage question is about. A flag that only the CLI
/// walkthrough of `README.md` documents is a real answer for a reader of the
/// README. `--to-event` was such a flag. But a reader who searches `docs/`
/// cannot find it. A credit for that mention hides the gap instead of
/// showing it, so the mention does not count.
fn graded_corpus_files(root: &Path) -> Result<Vec<PathBuf>> {
    let files = corpus_link_check::corpus_files(root);
    let reachable = corpus_link_check::compute_corpus_reachable(root, &files)?;
    let docs = root.join("docs");
    Ok(files
        .into_iter()
        .filter(|p| p.starts_with(&docs) && reachable.contains(p))
        .collect())
}

/// Returns the real `--flag` tokens that one page documents.
///
/// Outside fenced code, in prose and reference tables, a bare `--flag` mention
/// counts on its own. The runbooks of this corpus often document a flag in a
/// sentence or a table row, such as "`--dry-run` previews without writing".
/// They do not repeat a full runnable example each time, and the mention
/// answers the question of a reader just as well. Fenced code has no such
/// single-subject guarantee, so it gets the stricter rule below.
///
/// Inside a fenced block, a `--flag` counts only on the same logical command
/// line as a real `harvest` invocation. Backslash continuations are joined,
/// as in the drift scan. A mention elsewhere in the same block does not
/// count. Review found two false credits before this rule:
///
/// - A match on the raw page text, with no fence rules, credited `diesel
///   migration generate --version` in `docs/upgrading/0.5.0.md` as the
///   `--version` of harvest.
/// - A rule that the block merely contains the word `harvest` credited
///   `--depth` from an unrelated `git fetch --depth=1` line. That line shared
///   a block with a real `cargo run ... --bin harvest -- schema check ...`
///   invocation in `docs/workflow-schema-contract-guide.md`. `--depth` is a
///   real `workflow children` flag, so the credit hid a real gap.
///
/// The per-line invocation match of the drift scan closes both. It never
/// matches the own flags of git, diesel or cargo. So a flag counts only as an
/// argument of the harvest invocation itself.
fn flags_documented_in(pats: &config_cli_drift::Patterns, page: &Path) -> Result<HashSet<String>> {
    let text = pycompat::read_text_lossy(page)?;
    let (masked, blocks) = config_cli_drift::mask_fenced(pats, &pycompat::splitlines(&text));
    let mut found: HashSet<String> = pats
        .flag_tokens(&masked.join("\n"))
        .into_iter()
        .map(str::to_owned)
        .collect();
    for (lang, block_lines) in &blocks {
        if !INVOCATION_LANGS.contains(&lang.as_str()) {
            continue;
        }
        for line in config_cli_drift::join_shell_continuations(block_lines) {
            for flag in config_cli_drift::invocation_flags(pats, &line) {
                found.insert(flag.to_owned());
            }
        }
    }
    Ok(found)
}

/// Builds the report of the repository at `root`.
fn coverage(root: &Path, json: bool) -> Result<String> {
    let pats = config_cli_drift::Patterns::new()?;
    let (cli_flags, _cli_env_vars) = config_cli_drift::extract_cli_ground_truth(&pats, root)?;
    let files = graded_corpus_files(root)?;
    let mut documented = HashSet::new();
    for p in &files {
        documented.extend(flags_documented_in(&pats, p)?);
    }
    let undocumented: BTreeSet<&String> = cli_flags.difference(&documented).collect();
    if json {
        let known: BTreeSet<&String> = cli_flags.iter().collect();
        let value = Json::Obj(vec![
            ("files_scanned".into(), Json::Int(files.len())),
            (
                "cli_flags_known".into(),
                Json::strings(known.into_iter().cloned()),
            ),
            (
                "undocumented_flags".into(),
                Json::strings(undocumented.into_iter().cloned()),
            ),
        ]);
        return Ok(value.render() + "\n");
    }
    let mut out = format!(
        "Folio CLI-flag coverage matrix — {} files scanned, {} CLI flags known\n\n",
        files.len(),
        cli_flags.len()
    );
    let _ = writeln!(
        out,
        "Undocumented CLI flags (reported only, never fails CI): {}",
        undocumented.len()
    );
    for f in undocumented {
        let _ = writeln!(out, "  --{f}");
    }
    Ok(out)
}

/// Runs `cargo xtask audit cli-flag-coverage`.
pub fn run(args: Args) -> Result<ExitCode> {
    let root = pycompat::resolve(&util::repo_root());
    print!("{}", coverage(&root, args.json)?);
    Ok(ExitCode::SUCCESS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coverage_credits_prose_and_harvest_lines_only() {
        let dir = smop::fs::temp_dir().unwrap();
        let root = pycompat::resolve(dir.path());
        let write = |path: &str, body: &str| {
            let full = root.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, body).unwrap();
        };
        write(
            "autumn-harvest-cli/src/lib.rs",
            "#[arg(long)]\npub wait: bool,\n#[arg(long)]\npub depth: u8,\n#[arg(long = \"dry-run\")]\nx: bool,\n#[arg(long)]\npub to_event: u8,\n#[arg(long)]\npub hidden: u8,\n",
        );
        write(
            "docs/a.md",
            "Use `--dry-run` to preview.\n```sh\ngit fetch --depth=1\nharvest run \\\n  --wait\n# harvest --hidden\n```\n```rust\nharvest --hidden\n```\n",
        );
        write("README.md", "[a](docs/a.md) `--to-event`\n");
        write("docs/plans/p.md", "--hidden\n");
        let out = coverage(&root, false).unwrap();
        assert_eq!(
            out,
            "Folio CLI-flag coverage matrix — 1 files scanned, 7 CLI flags known\n\n\
             Undocumented CLI flags (reported only, never fails CI): 5\n\
             \x20 --depth\n  --help\n  --hidden\n  --to-event\n  --version\n"
        );
        let json = coverage(&root, true).unwrap();
        assert!(
            json.starts_with(
                "{\n  \"files_scanned\": 1,\n  \"cli_flags_known\": [\n    \"depth\","
            )
        );
    }
}
