//! `cargo xtask audit action-sha-pin`: every action `uses:` pins a commit SHA.
//!
//! This is the port of `docs/audits/action-sha-pin.py`, a supply-chain harness
//! (issue #1826).
//!
//! A tag or a branch is a mutable ref. Its owner can move it to new code, and
//! the next run executes that code with the token of the workflow. A 40-hex
//! commit SHA cannot move.
//!
//! Each `uses` key in `.github/workflows/*.y*ml` and
//! `.github/actions/**/action.y*ml` must take one of three forms:
//!
//! - `owner/repo[/path]@<40 lowercase hex> # <tag>`. The comment names the
//!   source tag of the SHA. Dependabot updates the SHA and the comment together.
//! - `./path`, a local action in this repository.
//! - `docker://image@sha256:<64 hex>`, an image pinned by digest.
//!
//! The command finds each `uses` key in the parsed YAML tree. So a flow mapping,
//! an escaped key or a folded value cannot hide one. A parser drops comments,
//! so the command then reads the raw line of each key for the `# <tag>` comment.
//! A key that is not on one line in the plain form above is a finding. Text in
//! a `run:` block is not a key, so it is never a finding.
//!
//! The command does not check that the SHA matches the tag. That needs network
//! access. Dependabot rewrites each pair that it bumps. A hand edit is not
//! checked.
//!
//! `--self-test` runs the fixtures. The command needs no network. The Python
//! script needed `PyYAML`. The port compiles its YAML reader in, so the check
//! never skips silently.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::workflow_yaml_parse::pytext;
use super::workflow_yaml_parse::pyyaml::{self, Graph, Kind};
use crate::util;

/// Arguments of `cargo xtask audit action-sha-pin`.
#[derive(Debug, Args)]
pub struct Args {
    /// Run the fixtures instead of the repository files.
    #[arg(long)]
    pub self_test: bool,
}

/// Compiles one of the fixed patterns of this module.
fn pattern(source: &str) -> Regex {
    Regex::new(source).unwrap_or_else(|error| unreachable!("pattern {source}: {error}"))
}

/// The one line form that the command reads: an optional list dash, the plain
/// key, an optionally quoted value, and an optional comment.
///
/// Python wrote the quote as a group and a backreference. The Rust engine has
/// no backreference, so each quote form is its own branch. The value excludes
/// both quotes, so the branches match the same lines.
static PLAIN: LazyLock<Regex> = LazyLock::new(|| {
    pattern(
        r#"^\s*(?:-\s+)?uses:\s*(?:"(?P<dq>[^\s"'#]+)"|'(?P<sq>[^\s"'#]+)'|(?P<bare>[^\s"'#]+))(?:\s+#\s*(?P<comment>\S.*?))?\s*$"#,
    )
});
static REMOTE: LazyLock<Regex> = LazyLock::new(|| {
    pattern(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_./-]+)?@(?P<ref>.+)$")
});
static SHA: LazyLock<Regex> = LazyLock::new(|| pattern(r"^[0-9a-f]{40}$"));
/// A version tag: `v1`, `v4.4.0`, `2.0.1`. Dependabot cannot update other text.
static TAG: LazyLock<Regex> = LazyLock::new(|| pattern(r"^v?\d[\w.+-]*(?:\s|$)"));
static DOCKER: LazyLock<Regex> =
    LazyLock::new(|| pattern(r"^docker://[^@\s]+@sha256:[0-9a-f]{64}$"));

/// The tree of a file holds an alias cycle, so the walk has no end.
///
/// Python recursed until `RecursionError` stopped the script with exit 1. The
/// port stops the run with exit 1 as well, so the gate still fails.
#[derive(Debug, PartialEq, Eq)]
struct Recursive;

/// The composed graph, and the (line number, value node) of each `uses` key.
type UsesNodes = (Graph, Vec<(usize, usize)>);

/// Returns (line number, node) for each `uses` key in the YAML tree.
///
/// A `uses` key directly under `with:` is an action input, not an action
/// reference, so the walk skips it.
fn uses_nodes(text: &str) -> Result<Result<UsesNodes, Recursive>, pyyaml::YamlError> {
    let (graph, roots) = pyyaml::compose_all(text)?;
    let mut out = Vec::new();
    for root in roots {
        let mut path = HashSet::new();
        if walk(&graph, root, None, &mut path, &mut out).is_err() {
            return Ok(Err(Recursive));
        }
    }
    Ok(Ok((graph, out)))
}

/// Walks one node. `path` holds the nodes above it, so the walk detects an
/// alias cycle instead of recursing forever.
fn walk<'g>(
    graph: &'g Graph,
    node: usize,
    parent_key: Option<&'g str>,
    path: &mut HashSet<usize>,
    out: &mut Vec<(usize, usize)>,
) -> Result<(), Recursive> {
    if !path.insert(node) {
        return Err(Recursive);
    }
    match &graph.nodes[node].kind {
        Kind::Mapping(pairs) => {
            for &(key, value) in pairs {
                let name = match &graph.nodes[key].kind {
                    Kind::Scalar(name) => Some(name.as_str()),
                    _ => None,
                };
                if name == Some("uses") && parent_key != Some("with") {
                    out.push((graph.nodes[key].start.line + 1, value));
                }
                walk(graph, value, name, path, out)?;
            }
        }
        Kind::Sequence(items) => {
            for &item in items {
                walk(graph, item, parent_key, path, out)?;
            }
        }
        Kind::Scalar(_) => {}
    }
    path.remove(&node);
    Ok(())
}

/// Returns a finding message for one plain `uses:` line, or `None`.
fn check_pin(value: &str, comment: Option<&str>) -> Option<String> {
    if value.starts_with("./") {
        return None;
    }
    if value.starts_with("docker://") {
        if DOCKER.is_match(value) {
            return None;
        }
        return Some(format!(
            "`{value}` must pin the image by `@sha256:<digest>`"
        ));
    }
    let Some(remote) = REMOTE.captures(value) else {
        return Some(format!("`{value}` is not `owner/repo[/path]@<ref>`"));
    };
    if !SHA.is_match(&remote["ref"]) {
        return Some(format!(
            "`{value}` must pin a 40-hex commit SHA, not a tag or branch"
        ));
    }
    if !comment.is_some_and(|comment| TAG.is_match(comment)) {
        return Some(format!(
            "`{value}` needs a `# <tag>` comment, such as `# v4.4.0`"
        ));
    }
    None
}

/// Returns (line number, message) for each finding in `text`.
fn scan_text(text: &str) -> Result<Vec<(usize, String)>, Recursive> {
    let (graph, nodes) = match uses_nodes(text) {
        Ok(found) => found?,
        Err(error) => {
            return Ok(vec![(
                0,
                format!("does not parse as YAML: {}", error.render(text)),
            )]);
        }
    };
    let lines = pytext::splitlines(text);
    let mut out = Vec::new();
    for (number, value) in nodes {
        let line = lines.get(number - 1).copied().unwrap_or("");
        let found = PLAIN.captures(line).and_then(|m| {
            let quoted = m.name("dq").or_else(|| m.name("sq"));
            let plain_value = quoted.or_else(|| m.name("bare"))?.as_str();
            Some((plain_value, m.name("comment").map(|c| c.as_str())))
        });
        let node = &graph.nodes[value];
        let plain = match (&node.kind, found) {
            (Kind::Scalar(scalar), Some((plain_value, _))) => {
                plain_value == scalar && node.start.line + 1 == number
            }
            _ => false,
        };
        let Some((plain_value, comment)) = found.filter(|_| plain) else {
            out.push((
                number,
                "write this `uses:` on one line: `uses: owner/repo@<sha> # <tag>`".to_owned(),
            ));
            continue;
        };
        if let Some(message) = check_pin(plain_value, comment) {
            out.push((number, message));
        }
    }
    Ok(out)
}

/// Returns the files to scan under `root`, in the order of the Python script.
fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let github = root.join(".github");
    let glob = |pattern: PathBuf| -> Result<Vec<PathBuf>> {
        let pattern = pattern
            .to_str()
            .context("the repository path is not valid UTF-8")?
            .to_owned();
        fs::glob(pattern)
    };
    let mut found = glob(github.join("workflows/*.y*ml"))?;
    found.extend(glob(github.join("actions/**/action.y*ml"))?);
    Ok(found)
}

const SHA_A: &str = "11d5960a326750d5838078e36cf38b85af677262";

/// A one-step workflow around `line`, which sits at step-item indent.
fn step(line: &str) -> String {
    format!("jobs:\n  a:\n    steps:\n{line}\n")
}

/// Returns (YAML text, finding count). Each fixture names one form that the
/// scan decides.
#[allow(clippy::too_many_lines)]
fn fixtures() -> Vec<(String, usize)> {
    let digest = "0".repeat(64);
    vec![
        (
            step(&format!("      - uses: actions/checkout@{SHA_A} # v4.4.0")),
            0,
        ),
        (
            step(&format!(
                "      - uses: actions/checkout@{SHA_A}  #  v4.4.0  "
            )),
            0,
        ),
        (
            step(&format!(
                "      - uses: 'actions/checkout@{SHA_A}' # v4.4.0"
            )),
            0,
        ),
        (
            step(&format!(
                "      - uses: \"github/codeql-action/init@{SHA_A}\" # v3.29.0"
            )),
            0,
        ),
        (
            step(&format!(
                "      - uses: dtolnay/rust-toolchain@{SHA_A} # v1"
            )),
            0,
        ),
        (
            step(&format!(
                "      - uses: actions/checkout@{SHA_A} # v4 # trailing"
            )),
            0,
        ),
        (
            step(&format!(
                "      - name: x\n        uses: actions/checkout@{SHA_A} # v4.4.0"
            )),
            0,
        ),
        (step("      - uses: ./.github/actions/setup"), 0),
        (
            step(&format!("      - uses: docker://alpine@sha256:{digest}")),
            0,
        ),
        (
            step("      # uses: actions/checkout@v4 is only a comment\n      - run: true"),
            0,
        ),
        (
            step("      - name: 'cache uses: none'\n        run: true"),
            0,
        ),
        (
            step("      - run: |\n          cat <<EOF\n          uses: foo\n          EOF"),
            0,
        ),
        (
            step("      - uses: ./a\n        with:\n          uses: an-input-named-uses"),
            0,
        ),
        (
            format!("jobs:\n  call:\n    uses: o/r/.github/workflows/w.yml@{SHA_A} # v1.2.0\n"),
            0,
        ),
        (step("      - uses: actions/checkout@v4"), 1),
        (step("      - uses: actions/checkout@v4 # v4"), 1),
        (step("      - uses: dtolnay/rust-toolchain@stable"), 1),
        (step(&format!("      - uses: actions/checkout@{SHA_A}")), 1),
        (
            step(&format!("      - uses: actions/checkout@{SHA_A} #")),
            1,
        ),
        (
            step(&format!("      - uses: actions/checkout@{SHA_A} # latest")),
            1,
        ),
        (
            step(&format!(
                "      - uses: actions/checkout@{} # v4.4.0",
                SHA_A.to_uppercase()
            )),
            1,
        ),
        (
            step(&format!(
                "      - uses: actions/checkout@{} # v4.4.0",
                &SHA_A[..7]
            )),
            1,
        ),
        (
            step(&format!("      - uses: actions/checkout@{SHA_A}0 # v4.4.0")),
            1,
        ),
        (step("      - uses: docker://alpine:3.20"), 1),
        (step("      - uses: actions/checkout"), 1),
        (
            "jobs:\n  call:\n    uses: o/r/.github/workflows/w.yml@main\n".to_owned(),
            1,
        ),
        // Forms that a line scan alone misses. The parsed tree finds each one.
        (
            step(&format!("      - {{ uses: actions/checkout@{SHA_A} }}")),
            1,
        ),
        (
            step("      - {name: x, with: {a: b}, uses: actions/checkout@v4}"),
            1,
        ),
        (
            step("      - {\n        name: x, uses: actions/checkout@v4\n        }"),
            1,
        ),
        (
            "jobs:\n  a:\n    steps: [uses: actions/checkout@v4]\n".to_owned(),
            1,
        ),
        (step("      - \"u\\x73es\": actions/checkout@v4"), 1),
        (
            step(&format!(
                "      - \"uses\": actions/checkout@{SHA_A} # v4.4.0"
            )),
            1,
        ),
        (step("      - ? uses\n        : actions/checkout@v4"), 1),
        (step("      - &k uses: actions/checkout@v4"), 1),
        (step("      - uses: >-\n          actions/checkout@v4"), 1),
        (
            step(&format!(
                "      - uses: actions/checkout@{SHA_A} # v4.4.0\n      - uses: a/b@v1"
            )),
            1,
        ),
        ("jobs: [unclosed\n".to_owned(), 1),
    ]
}

/// Renders findings as Python `repr` of a list of `(int, str)` tuples.
fn repr_findings(found: &[(usize, String)]) -> String {
    let items: Vec<String> = found
        .iter()
        .map(|(number, message)| format!("({number}, {})", pytext::repr_str(message)))
        .collect();
    format!("[{}]", items.join(", "))
}

/// Runs the fixtures. Returns the output lines and the count of wrong ones.
fn self_test() -> (Vec<String>, usize) {
    let fixtures = fixtures();
    let mut lines = Vec::new();
    let mut bad = 0;
    for (text, expected) in &fixtures {
        let got = scan_text(text).unwrap_or_default();
        if got.len() != *expected {
            lines.push(format!(
                "self-test: expected {expected} finding(s), got {}:\n{text}",
                repr_findings(&got)
            ));
            bad += 1;
        }
    }
    lines.push(format!(
        "action-sha-pin self-test: {} fixtures, {bad} wrong",
        fixtures.len()
    ));
    (lines, bad)
}

/// The result of a scan of the repository files.
struct Audit {
    /// The finding lines, then the summary line when the scan ends.
    lines: Vec<String>,
    total: usize,
    /// The file whose alias cycle stopped the scan, if any.
    recursive: Option<String>,
}

/// Scans each file under `root`. A file with an alias cycle stops the scan
/// with no summary line, as the Python traceback did.
fn audit(root: &Path) -> Result<Audit> {
    let paths = files(root)?;
    let mut lines = Vec::new();
    let mut total = 0;
    for path in &paths {
        let text = pytext::read_text(path)?.unwrap_or_default();
        let rel = path.strip_prefix(root).unwrap_or(path).display();
        let Ok(findings) = scan_text(&text) else {
            return Ok(Audit {
                lines,
                total,
                recursive: Some(rel.to_string()),
            });
        };
        for (number, message) in findings {
            lines.push(format!("{rel}:{number}: {message}"));
            total += 1;
        }
    }
    lines.push(format!(
        "action-sha-pin: {} files, {total} unpinned",
        paths.len()
    ));
    Ok(Audit {
        lines,
        total,
        recursive: None,
    })
}

/// Runs `cargo xtask audit action-sha-pin`.
pub fn run(args: Args) -> Result<ExitCode> {
    let (lines, bad, recursive) = if args.self_test {
        let (lines, bad) = self_test();
        (lines, bad, None)
    } else {
        let audit = audit(&util::repo_root())?;
        (audit.lines, audit.total, audit.recursive)
    };
    for line in lines {
        println!("{line}");
    }
    if let Some(rel) = recursive {
        eprintln!("action-sha-pin: {rel}: an alias cycle makes the YAML tree infinite");
        return Ok(ExitCode::FAILURE);
    }
    Ok(util::gate_exit(bad == 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fixture_decides_as_expected() {
        let (lines, bad) = self_test();
        assert_eq!(bad, 0, "{lines:#?}");
        assert_eq!(
            lines.last().map(String::as_str),
            Some("action-sha-pin self-test: 37 fixtures, 0 wrong")
        );
    }

    #[test]
    fn a_tag_pin_names_its_line() {
        let text = step("      - uses: actions/checkout@v4");
        assert_eq!(
            scan_text(&text).unwrap_or_default(),
            [(
                4,
                "`actions/checkout@v4` must pin a 40-hex commit SHA, not a tag or branch"
                    .to_owned()
            )]
        );
    }

    #[test]
    fn an_unparsable_file_is_one_finding_on_line_zero() {
        let found = scan_text("jobs: [unclosed\n").unwrap_or_default();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].0, 0);
        assert!(
            found[0]
                .1
                .starts_with("does not parse as YAML: while parsing a flow sequence")
        );
    }

    #[test]
    fn an_alias_cycle_stops_the_scan() {
        assert_eq!(scan_text("a: &x [*x]\n"), Err(Recursive));
        // A shared alias that is not a cycle is walked twice, as in Python.
        let text = format!("x: &s\n  uses: a/b@{SHA_A} # v1\njobs: [*s, *s]\n");
        assert_eq!(scan_text(&text).map(|found| found.len()), Ok(0));
        let text = "x: &s {uses: a/b@v1}\njobs: [*s, *s]\n";
        assert_eq!(scan_text(text).map(|found| found.len()), Ok(3));
    }

    #[test]
    fn a_wrong_fixture_reports_its_findings() {
        let got = vec![(4, "it's".to_owned())];
        assert_eq!(repr_findings(&got), "[(4, \"it's\")]");
        assert_eq!(repr_findings(&[]), "[]");
    }

    #[test]
    fn the_real_workflows_are_pinned() {
        let audit = audit(&util::repo_root()).unwrap_or_else(|error| panic!("{error:#}"));
        assert_eq!(audit.total, 0, "{:#?}", audit.lines);
        assert_eq!(audit.recursive, None);
    }
}
