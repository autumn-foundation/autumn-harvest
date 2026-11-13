//! `cargo xtask audit comment-hygiene`: comment hygiene across the Rust
//! sources.
//!
//! This is the Folio corpus harness for comments. It is deterministic and
//! reproducible on any checkout. It reads the file system and git only, with
//! no network and no build. So it runs in CI on each PR. See
//! `docs/audits/README.md`.
//!
//! # What this does not do
//!
//! It does not shorten rationale. This codebase carries long "why"
//! comments on purpose, and they are load-bearing. One example is the ABBA
//! lock-order argument at `materialize_due_child_timeout_deadlines`.
//! Another is the `cohort` key argument in `partition.rs`. A third is the
//! codec-rotation scope guarantee, which `CLAUDE.md` cites as the proof that
//! exception #3 is safe.
//! A reader who deletes those to reach a word budget destroys the only
//! record of why the code has its shape. Length is not the defect. The
//! defects below are.
//!
//! So the rules split into two tiers, and the tier is the whole design.
//!
//! ## Tier A: absolute gates, which must stay at zero
//!
//! These are defects under any house style. They are mechanical to detect,
//! and they were few enough to drive to zero when this audit landed. A new
//! one fails the build.
//!
//! - CH001, commented-out code: Rust that was commented out, not deleted.
//!   Version control already remembers it. A commented-out block rots in
//!   silence, because no compiler or test ever reads it.
//! - CH002, a TODO with no issue: a TODO, FIXME, XXX or HACK with no
//!   `#<issue>` or URL. A marker with no reference is a wish, not a tracked
//!   commitment. Nothing ever routes it to a person.
//! - CH003, a narrative aside: deliberation left in the tree, such as
//!   `actually, let us ...`, `we will ...` and `not sure why`. It records
//!   the author in thought, not the decision. It often contradicts the code that shipped.
//!   This audit found an aside about `ApiMethod::Delete` in the CLI that
//!   argued against the variant that the code beside it already used.
//! - CH004, a blank comment edge: a comment block that opens or closes on
//!   an empty `//` line. That is an editing artifact.
//! - CH005, review archaeology: the name of the round of review, or of the
//!   priority tag, that produced a change. That is process trivia that a
//!   future reader cannot look up. The issue number is the durable handle,
//!   so cite that instead.
//! - CH006, a contraction: ASD-STE100 forbids contractions such as "is not"
//!   written short. They hurt readers whose first language is not English,
//!   and machine translation, for no gain in brevity.
//!
//! CH005 and CH006 started in Tier B. They moved here once the corpus
//! reached zero.
//!
//! ## Tier B: ratcheted against the merge base
//!
//! This is a real style defect with a legacy population far too large to
//! fix in one change: about 17k sentences. The rule is simple. A change may
//! not add one to a file that it touches.
//! Existing findings stay until someone chooses to fix them.
//!
//! The comparison reads the merge base out of git. CI passes `--base <ref>`.
//! The audit scans each changed file again as it was there. There is no
//! baseline in the tree, on purpose. A stored baseline goes stale the moment
//! the base branch moves. One merge that adds a long comment anywhere turns
//! each open PR red for a file its author never opened. A stored baseline
//! also invites a regeneration that hides a new violation, breaks on
//! renames, and costs half a megabyte of generated fingerprints.
//!
//! Findings are matched by fingerprint (rule and normalized text), not
//! counted. A count only answers "how many". So a change that removes one
//! legacy violation and adds a different one in the same file nets to zero
//! and passes. An identity answers "which", so the new one is caught,
//! although the total never moved. A renamed file is compared against its
//! own previous path. A file that the change adds has no merge-base
//! version, so each Tier B finding in it is from the change.
//!
//! Without `--base`, or when the diff cannot be computed, Tier B reports but
//! never fails. The diff fails with no git, an unknown ref, or a shallow
//! clone with no reachable merge base. To gate the whole corpus at the
//! moment when the tool cannot tell what changed is the worst option
//! available. Tier A has no scope. It gates everywhere, always.
//!
//! - CH007, a long sentence: over 25 words, the ASD-STE100 limit for
//!   descriptive text. It applies per sentence. So a long rationale block
//!   with a good structure passes cleanly once it is written as several
//!   sentences.
//!
//! # What counts as a comment
//!
//! Each Rust comment counts, as a real lexer finds it (`extract_comments`),
//! not a line regex. That is leading and trailing `//`, `///`, `//!`, and
//! `/* */`, with the nested and doc forms. Text inside a string is not a
//! comment, and that matters in both directions. This corpus embeds Rust
//! and SQL snippets in raw strings. The fixtures of `det_check_tests.rs`
//! have `// harvest-suppress:` lines, and `chaos_catalogue_drift.rs` has a
//! literal commented-out call as test data. To flag one fails CI on an
//! innocent fixture.
//!
//! Whole constructs are exempt, because a flag on them means nothing, not
//! because they are above the rules. Fenced code blocks (``` and ~~~) are
//! sample code. Markdown tables and headings are not prose.
//!
//! # Usage
//!
//! ```text
//! cargo xtask audit comment-hygiene [--json] [--tier-a-only]
//! cargo xtask audit comment-hygiene --self-test
//! cargo xtask audit comment-hygiene --base origin/trunk-dev
//! cargo xtask audit comment-hygiene --paths a.rs b.rs
//! ```
//!
//! The exit status is 1 on any Tier A finding or any Tier B regression, and
//! 0 otherwise.
//!
//! # Known limitations
//!
//! - The sentence split is at the level of a regex: `[.!?]` and white space.
//!   "e.g." and "i.e." are excluded by name. Neither ever ends a sentence,
//!   and this corpus writes both constantly. A split there cut sentences in
//!   two and let a long one past CH007. Each other abbreviation still
//!   splits, on purpose. "etc." and "vs." do end sentences, so to exclude
//!   them would merge two real ones and report too much. A general fix,
//!   which requires a capital after the stop, merges "... the row. Postgres
//!   ..." and is worse than the problem. Both directions are wrong.
//!   Under-reporting is the safe one for a gate.
//!
//! - A rule about what Rustdoc renders applies to `///`, `//!`, `/**` and
//!   `/*!` only, because Rustdoc renders nothing else. Indented code, a
//!   Setext underline and an HTML block all have that limit. A ``` fence
//!   does not. An author writes one to mean "this is an example", whatever
//!   the marker. Each comment in this tree that holds an example holds a
//!   fence. An error here is silent, because it removes findings and does not
//!   add them. The corpus diff catches it, not the self-test.
//!
//! - The Markdown block layer is written by hand and is partial on purpose.
//!   One known gap remains, recorded in issue #1383 and not fixed here. It
//!   under-reports, which is the safe direction for a gate. The scan can
//!   read a delimiter row from a fragment beside its header, on the same
//!   source line. That happens when an empty nested comment sits between
//!   the two. A fix needs
//!   `next_row` to require the delimiter on a later line. That is a small
//!   change, deferred anyway. The issue holds the reasons, not the size of
//!   the patch.
//!
//! - CH001 is high-precision and incomplete on purpose, and it should stay
//!   so. It recognizes commented-out Rust by the shape of the line. The
//!   shapes are item headers, `let`, `use` and assignment, attributes and
//!   closing braces. Macro and call statements count too, and so does
//!   control flow that opens a block. It does not
//!   recognize an expression with no terminator (`// foo(bar)`). That shape
//!   cannot be told apart from prose that names a call, which this corpus
//!   writes on thousands of lines.
//!
//!   The asymmetry is intentional. A false positive fails CI on ordinary
//!   English, which is worse than one missed commented-out line. So each
//!   branch is anchored and has a terminator. Each widening is measured
//!   against the corpus and the prose sweep of `--self-test` first. A gain
//!   in recall from a relaxed anchor has given a false positive each time
//!   it was tried here. Add a new alternative with a narrow anchor instead.
//!
//! - CH002, CH005 and CH006 skip inline code spans (`` `like this` ``).
//!   They are absolute. A comment that documents a literal ("Parse the
//!   `TODO:` prefix", "Match the `can't` token") must not fail the build.
//!
//! - CH004 judges only runs of leading `//`. A trailing comment has no block
//!   to have edges. A blank first line inside `/* */` is a conventional
//!   format, not the editing artifact that the rule is about.
//!
//! - The lexer is a lexer, not a parser. It tracks strings, raw strings,
//!   char literals and nested block comments, but it knows nothing of macros
//!   or `cfg`. That is enough here, because each construct that can hide a
//!   `//` from a reader is lexical. `--self-test` pins the cases that it
//!   must get right. That includes the backslash-newline continuation,
//!   whose newline must be counted, or each later line number drifts.

// The comments of this audit discuss Markdown syntax as their subject. They
// hold literal fences, lone backticks and the names of CommonMark constructs,
// and `doc_markdown` reads those as broken Markdown.
//
// The `.rs` tests are case-sensitive on purpose. The Python original used
// `str.endswith(".rs")`, and the corpus and the merge-base scope depend on
// that exact test.
#![allow(
    clippy::doc_markdown,
    clippy::case_sensitive_file_extension_comparisons
)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::process::ExitCode;

use smop::prelude::*;

use crate::util::repo_root;

mod fixtures;
mod json;
mod lexer;
mod markdown;
mod markers;
mod patterns;
mod pyre;
mod pysem;
mod ratchet;
mod rules;
mod runs;
mod selftest;
mod sha256;
mod shapes;
mod spans;

use ratchet::{
    Tally, baseline_from_merge_base, compare_tier_b, diff_context, index_findings, tally,
    tier_b_gate,
};
use rules::{Finding, findings_for_source};

/// Directories that hold no first-party source.
const SKIP_DIRS: [&str; 4] = [".git", "target", "node_modules", ".cargo"];

/// The absolute rules.
pub const TIER_A: [&str; 6] = ["CH001", "CH002", "CH003", "CH004", "CH005", "CH006"];

/// The ratcheted rules.
pub const TIER_B: [&str; 1] = ["CH007"];

/// The title of each rule.
pub const RULE_TITLES: [(&str, &str); 7] = [
    ("CH001", "commented-out code"),
    ("CH002", "TODO/FIXME without an issue reference"),
    ("CH003", "narrative aside"),
    ("CH004", "blank comment line at a block edge"),
    ("CH005", "review-round archaeology"),
    ("CH006", "contraction (ASD-STE100)"),
    ("CH007", "sentence over 25 words (ASD-STE100)"),
];

/// The fix hint of each rule.
const RULE_HINTS: [(&str, &str); 7] = [
    (
        "CH001",
        "Delete it -- git remembers. Keep it only inside a ``` fence as a documented example.",
    ),
    ("CH002", "Add the tracking issue: TODO(#1234): ..."),
    ("CH003", "State the decision, not the deliberation."),
    (
        "CH004",
        "Drop the empty `//` line at the edge of the block.",
    ),
    (
        "CH005",
        "Cite the issue number (#1234), not the review round that found it.",
    ),
    (
        "CH006",
        "Expand it: \"isn't\" -> \"is not\", \"let's\" -> \"To ... ,\".",
    ),
    (
        "CH007",
        "Split it. One idea per sentence, 25 words or fewer.",
    ),
];

/// The title of `rule`.
pub fn rule_title(rule: &str) -> &'static str {
    RULE_TITLES
        .iter()
        .find(|(name, _)| *name == rule)
        .map_or("", |(_, title)| title)
}

fn rule_hint(rule: &str) -> &'static str {
    RULE_HINTS
        .iter()
        .find(|(name, _)| *name == rule)
        .map_or("", |(_, hint)| hint)
}

/// Arguments of `cargo xtask audit comment-hygiene`.
#[derive(Debug, Args)]
// Python `argparse` accepts a unique prefix of a long flag, such as
// `--tier`. So this command accepts one too.
#[command(
    about = "Folio corpus harness: comment hygiene across the Rust sources.",
    infer_long_args = true
)]
pub struct Args {
    /// Emit the findings as JSON.
    #[arg(long)]
    pub json: bool,
    /// Check only the absolute gates.
    #[arg(long)]
    pub tier_a_only: bool,
    /// Check the lexer and the rules against their fixtures.
    #[arg(long)]
    pub self_test: bool,
    /// Gate Tier B only on the files changed since the merge base with REF.
    ///
    /// An example is origin/trunk-dev. Without it, Tier B reports over the
    /// whole corpus but never fails. A count over the whole corpus is not for
    /// this change to answer.
    #[arg(long, value_name = "REF")]
    pub base: Option<String>,
    /// Limit the scan to these .rs files.
    #[arg(long, num_args = 0..)]
    pub paths: Option<Vec<String>>,
}

/// Runs `cargo xtask audit comment-hygiene`.
pub fn run(args: Args) -> Result<ExitCode> {
    patterns::check_all().map_err(|error| anyhow!("a pattern does not compile: {error}"))?;
    if args.self_test {
        let code = selftest::self_test();
        ensure!(
            !pyre::search_failed(),
            "a regex search exceeded its backtrack limit"
        );
        return Ok(ExitCode::from(code));
    }
    let root = normalize(&repo_root());
    let code = gate(&root, args)?;
    ensure!(
        !pyre::search_failed(),
        "a regex search exceeded its backtrack limit"
    );
    Ok(ExitCode::from(code))
}

/// Scans the tree and applies both tiers. Returns the exit status.
fn gate(root: &Path, args: Args) -> Result<u8> {
    let Args {
        json,
        tier_a_only,
        base,
        paths,
        ..
    } = args;
    let findings = scan(root, paths.as_deref())?;
    let base = base.as_deref().filter(|base| !base.is_empty());

    // Tier B gates only what this change touched. With no scope it reports
    // and never fails. The alternative fails a PR for a file that it never
    // opened.
    let mut renames: HashMap<String, String> = HashMap::new();
    let mut merge_base = String::new();
    let scope: HashSet<String>;
    let scope_note: String;
    if let Some(base) = base {
        if let Some((sha, changed, moved)) = diff_context(root, base) {
            let rust = changed.iter().filter(|path| path.ends_with(".rs")).count();
            let moved_rust = moved.keys().filter(|path| path.ends_with(".rs")).count();
            scope_note = format!(
                "gating {rust} changed .rs file(s) vs {base}{}",
                if moved_rust > 0 {
                    format!(", {moved_rust} renamed")
                } else {
                    String::new()
                }
            );
            merge_base = sha;
            scope = changed;
            renames = moved;
        } else {
            eprintln!(
                "comment-hygiene: cannot diff against {} (unknown ref, no git, or a shallow clone). Tier B is report-only for this run.",
                pyre::repr(base)
            );
            // An empty scope gates nothing. To gate the whole corpus instead
            // is the worst option available. It fails the build over legacy
            // debt that the change never touched. And it does so when the
            // tool has already admitted that it cannot tell what changed.
            scope = HashSet::new();
            scope_note = format!("could not diff against {base}; report-only");
        }
    } else {
        scope = HashSet::new();
        scope_note = "no --base given; report-only".to_string();
    }

    // The baseline is not computed when only the absolute gates apply.
    // Nothing reads it on that path, and to build it walks the merge base for
    // each file in scope.
    let baseline = if base.is_some() && !scope.is_empty() && !tier_a_only {
        baseline_from_merge_base(root, &merge_base, &scope, &renames)
    } else {
        Tally::new()
    };

    if json {
        let current = tally(&findings);
        let regressions = tier_b_gate(
            &current,
            &baseline,
            Some(&scope),
            &renames,
            &findings,
            tier_a_only,
        );
        let mut sorted_scope: Vec<&String> = scope.iter().collect();
        sorted_scope.sort();
        println!(
            "{}",
            json::report(
                &findings,
                &current,
                &sorted_scope,
                &merge_base,
                &regressions
            )
        );
        let tier_a = findings
            .iter()
            .any(|finding| TIER_A.contains(&finding.rule));
        return Ok(u8::from(tier_a || !regressions.is_empty()));
    }

    Ok(report(
        &findings,
        &baseline,
        tier_a_only,
        Some(&scope),
        &scope_note,
        &renames,
    ))
}

/// Lexical normalization of an absolute path, as Python `os.path.normpath`.
fn normalize(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `path` relative to `root`, as Python `os.path.relpath` computes it.
fn relpath(path: &Path, root: &Path) -> PathBuf {
    let path: Vec<Component<'_>> = path.components().collect();
    let root: Vec<Component<'_>> = root.components().collect();
    let common = path
        .iter()
        .zip(&root)
        .take_while(|(left, right)| left == right)
        .count();
    let mut out = PathBuf::new();
    for _ in common..root.len() {
        out.push("..");
    }
    for component in &path[common..] {
        out.push(component.as_os_str());
    }
    out
}

/// Each first-party `.rs` file, relative to the root and sorted.
fn rust_sources(root: &Path, paths: Option<&[String]>) -> Result<Vec<String>> {
    if let Some(paths) = paths.filter(|paths| !paths.is_empty()) {
        let cwd = std::env::current_dir().context("reading the current directory")?;
        let mut out = Vec::new();
        for path in paths {
            let absolute = normalize(&cwd.join(path));
            let rel = relpath(&absolute, root).to_string_lossy().into_owned();
            if rel.ends_with(".rs") && root.join(&rel).is_file() {
                out.push(rel);
            }
        }
        out.sort();
        return Ok(out);
    }

    let mut found = Vec::new();
    let walker = walkdir::WalkDir::new(root).follow_links(false).into_iter();
    let walker = walker.filter_entry(|entry| {
        entry.depth() == 0
            || !entry.file_type().is_dir()
            || !SKIP_DIRS.contains(&entry.file_name().to_string_lossy().as_ref())
    });
    for entry in walker.flatten() {
        if entry.depth() == 0 {
            continue;
        }
        // A symbolic link to a directory is a directory to Python
        // `os.walk`, which does not descend into it. A link to anything else
        // is a file name.
        let is_dir =
            entry.file_type().is_dir() || (entry.file_type().is_symlink() && entry.path().is_dir());
        if is_dir {
            continue;
        }
        if !entry.file_name().to_string_lossy().ends_with(".rs") {
            continue;
        }
        if let Ok(rel) = entry.path().strip_prefix(root) {
            found.push(rel.to_string_lossy().into_owned());
        }
    }
    found.sort();
    Ok(found)
}

/// Reads a file as Python text mode reads it.
///
/// Bad UTF-8 becomes U+FFFD, and each line end becomes `\n`.
fn read_source(path: &Path) -> std::io::Result<String> {
    let bytes = std::fs::read(path)?;
    let text = String::from_utf8_lossy(&bytes);
    Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
}

/// Each finding in the tree, sorted by rule, path and line.
///
/// The files are read on several threads. Each file keeps its own order,
/// and the sort is stable, so the result is the same as a serial scan.
fn scan(root: &Path, paths: Option<&[String]>) -> Result<Vec<Finding>> {
    let sources = rust_sources(root, paths)?;
    let workers = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .clamp(1, 4);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let mut per_file: Vec<Option<std::io::Result<Vec<Finding>>>> = Vec::new();
    per_file.resize_with(sources.len(), || None);
    let results = std::sync::Mutex::new(per_file);
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let Some(rel) = sources.get(index) else {
                        break;
                    };
                    let found = read_source(&root.join(rel))
                        .map(|source| findings_for_source(rel, &source));
                    if let Ok(mut results) = results.lock() {
                        results[index] = Some(found);
                    }
                }
            });
        }
    });
    let per_file = results
        .into_inner()
        .map_err(|_| anyhow!("a scan thread panicked"))?;
    // The read errors print in file order, as the serial scan printed them.
    let mut findings: Vec<Finding> = Vec::new();
    for (rel, found) in sources.iter().zip(per_file) {
        match found {
            Some(Ok(found)) => findings.extend(found),
            Some(Err(error)) => eprintln!(
                "comment-hygiene: cannot read {rel}: {}",
                os_error(&error, &root.join(rel))
            ),
            None => bail!("the scan skipped {rel}"),
        }
    }
    findings.sort_by(|left, right| {
        (left.rule, &left.path, left.line).cmp(&(right.rule, &right.path, right.line))
    });
    Ok(findings)
}

/// The text of a read error, as Python prints an `OSError`.
///
/// Python prints `[Errno 2] No such file or directory: '/path'`. Rust
/// prints the same reason with ` (os error 2)` after it and no path. An
/// error with no OS code keeps the Rust text.
fn os_error(error: &std::io::Error, path: &Path) -> String {
    let Some(code) = error.raw_os_error() else {
        return error.to_string();
    };
    let text = error.to_string();
    let reason = text
        .strip_suffix(&format!(" (os error {code})"))
        .unwrap_or(&text);
    format!(
        "[Errno {code}] {reason}: {}",
        pyre::repr(&path.to_string_lossy())
    )
}

/// Prints the text report. Returns the exit status.
fn report(
    findings: &[Finding],
    baseline: &Tally,
    tier_a_only: bool,
    scope: Option<&HashSet<String>>,
    scope_note: &str,
    renames: &HashMap<String, String>,
) -> u8 {
    let mut by_rule: BTreeMap<&str, Vec<&Finding>> = BTreeMap::new();
    for finding in findings {
        by_rule.entry(finding.rule).or_default().push(finding);
    }

    let mut failed = false;

    println!("Tier A -- absolute gates");
    for rule in TIER_A {
        let hits = by_rule.get(rule).map_or(&[][..], Vec::as_slice);
        let status = if hits.is_empty() { "OK  " } else { "FAIL" };
        println!("  [{status}] {rule} {}: {}", rule_title(rule), hits.len());
        if !hits.is_empty() {
            failed = true;
            for finding in hits.iter().take(20) {
                println!(
                    "         {}:{}: {}",
                    finding.path,
                    finding.line,
                    pyre::truncate(&finding.text, 100)
                );
            }
            if hits.len() > 20 {
                println!("         ... and {} more", hits.len() - 20);
            }
            println!("         fix: {}", rule_hint(rule));
        }
    }

    if tier_a_only {
        return u8::from(failed);
    }

    let current = tally(findings);
    println!("\nTier B -- ratcheted against the merge base ({scope_note})");
    for rule in TIER_B {
        let corpus: usize = current
            .get(rule)
            .map_or(0, |files| files.values().map(Vec::len).sum());
        let allowed: usize = baseline
            .get(rule)
            .map_or(0, |files| files.values().map(Vec::len).sum());
        let in_scope: usize = current.get(rule).map_or(0, |files| {
            files
                .iter()
                .filter(|(path, _)| scope.is_none_or(|scope| scope.contains(*path)))
                .map(|(_, list)| list.len())
                .sum()
        });
        println!(
            "  {rule} {}: {corpus} in the corpus; {in_scope} in changed files ({allowed} at the merge base)",
            rule_title(rule)
        );
    }

    let regressions = compare_tier_b(
        &current,
        baseline,
        scope,
        renames,
        &index_findings(findings),
    );
    if !regressions.is_empty() {
        failed = true;
        // Indented entries are located lines under a summary, not extra
        // regressions.
        let count = regressions
            .iter()
            .filter(|line| !line.starts_with(' '))
            .count();
        println!("\n{count} Tier B regression(s) -- these files gained violations:");
        for line in regressions.iter().take(40) {
            println!("    {line}");
        }
        if regressions.len() > 40 {
            println!("    ... and {} more line(s)", regressions.len() - 40);
        }
        println!(
            "\n  New and edited comments must satisfy the rule. Fix the flagged\n  lines rather than regenerating the baseline -- the baseline exists\n  to freeze the legacy corpus, not to absorb new debt."
        );
    }

    if !failed {
        println!("\nOK: no Tier A findings and no Tier B regressions.");
    }
    u8::from(failed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_is_lexical() {
        assert_eq!(normalize(Path::new("/a/./b/../c")), PathBuf::from("/a/c"));
    }

    #[test]
    fn relpath_climbs_out_of_the_root() {
        assert_eq!(
            relpath(Path::new("/r/a/b.rs"), Path::new("/r")),
            PathBuf::from("a/b.rs")
        );
        assert_eq!(
            relpath(Path::new("/x/b.rs"), Path::new("/r")),
            PathBuf::from("../x/b.rs")
        );
    }

    #[test]
    fn a_read_error_reads_as_python_prints_it() {
        let error = std::io::Error::from_raw_os_error(2);
        assert_eq!(
            os_error(&error, Path::new("/r/a.rs")),
            "[Errno 2] No such file or directory: '/r/a.rs'"
        );
        let other = std::io::Error::other("odd");
        assert_eq!(os_error(&other, Path::new("/r/a.rs")), "odd");
    }

    #[test]
    fn each_rule_has_a_title_and_a_hint() {
        for rule in TIER_A.iter().chain(TIER_B.iter()) {
            assert_ne!(rule_title(rule), "");
            assert_ne!(rule_hint(rule), "");
        }
    }
}
