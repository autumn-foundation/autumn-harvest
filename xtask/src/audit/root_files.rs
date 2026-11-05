//! `cargo xtask audit root-files`: the port of `docs/audits/root-files.py`.
//!
//! It fails on a tracked file at the repository root that is not in
//! `ALLOWED`.
//!
//! The root holds build config, licences and the few documents that GitHub
//! and tooling expect there. Everything else has a home under a directory. A
//! design record goes in `docs/design/`, a plan in `docs/plans/`, an R&D note
//! in `docs/rnd/`, and an ADR in `docs/adr/`.
//!
//! Agents copy the layout that they see. One design record at the root became
//! seventy-five, so this gate keeps the root to a fixed list.
//!
//! A root file that the repository needs goes in `ALLOWED`, with a reason. An
//! `ALLOWED` entry that is not tracked is also a finding, so the list stays
//! exact. The gate checks files only. A directory at the root, such as
//! `xtask/`, holds tracked paths with a `/`, so the gate never sees it.
//!
//! Usage: `cargo xtask audit root-files [--self-test]`.
//!
//! This module also holds the `git ls-files` reader that
//! `cargo xtask audit tracked-artifacts` shares.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::process::ExitCode;

use smop::prelude::*;

use crate::util::repo_root;

/// Arguments of `cargo xtask audit root-files`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real tree.
    #[arg(long)]
    pub self_test: bool,
}

/// Each file that may sit at the root, with the reason it sits there.
const ALLOWED: &[(&str, &str)] = &[
    (".dockerignore", "Docker build context filter"),
    (".gitattributes", "git attributes"),
    (".gitignore", "git ignore rules"),
    (
        "AGENTS.md",
        "agent instructions; agents read it from the root",
    ),
    (
        "CHANGELOG.md",
        "release changelog, collated by hand from docs/changelog.d/",
    ),
    (
        "CLAUDE.md",
        "agent instructions; agents read it from the root",
    ),
    ("CONTRIBUTING.md", "GitHub links it from the root"),
    ("Cargo.lock", "workspace lockfile"),
    ("Cargo.toml", "workspace manifest"),
    ("Dockerfile", "worker image build"),
    ("LICENSE-APACHE", "licence"),
    ("LICENSE-MIT", "licence"),
    ("README.md", "GitHub renders it from the root"),
    (
        "RELEASE_NOTES.md",
        "release notes; `cargo xtask audit doc-claim-drift` reads it here",
    ),
    ("SECURITY.md", "GitHub links it from the root"),
    ("cliff.toml", "git-cliff config"),
    ("clippy.toml", "clippy config"),
    ("deny.toml", "cargo-deny config"),
    ("harvest-verify.allow.toml", "harvest-verify allow list"),
    ("rust-toolchain.toml", "toolchain pin"),
    ("rustfmt.toml", "rustfmt config"),
];

/// The likely home of a stray root file, by its upper-case name prefix.
const HOMES: &[(&str, &str)] = &[
    ("DESIGN-", "docs/design/"),
    ("PLAN", "docs/plans/"),
    ("ADR", "docs/adr/"),
];

/// One tracked path, as `git ls-files -z` prints it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TrackedPath {
    /// The raw path bytes, which name the file on disk.
    pub raw: Vec<u8>,
    /// The path as Python decodes it with `surrogateescape`, then prints it
    /// with `backslashreplace`. Each byte that is not UTF-8 shows as `\udcXX`.
    pub text: String,
    /// The code points of the Python string. Python sorts paths by these.
    pub key: Vec<u32>,
}

impl TrackedPath {
    /// Decodes raw path bytes the way the Python original did.
    pub(super) fn from_bytes(raw: &[u8]) -> Self {
        let mut text = String::new();
        let mut key = Vec::new();
        for chunk in raw.utf8_chunks() {
            text.push_str(chunk.valid());
            key.extend(chunk.valid().chars().map(u32::from));
            for byte in chunk.invalid() {
                let _ = write!(text, "\\udc{byte:02x}");
                key.push(0xdc00 + u32::from(*byte));
            }
        }
        Self {
            raw: raw.to_vec(),
            text,
            key,
        }
    }

    /// Builds a path from UTF-8 text, for fixtures.
    #[cfg(test)]
    pub(super) fn from_text(text: &str) -> Self {
        Self::from_bytes(text.as_bytes())
    }
}

/// Returns every tracked path of the repository, in `git ls-files` order.
///
/// Git stores path bytes. A path that is not UTF-8 must still reach a report.
pub(super) fn tracked_paths() -> Result<Vec<TrackedPath>> {
    let output = sh::cmd("git")
        .args(["ls-files", "-z"])
        .dir(repo_root())
        .output_raw()?;
    ensure!(
        output.status.success(),
        "git ls-files -z failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(output
        .stdout
        .split(|b| *b == 0)
        .filter(|p| !p.is_empty())
        .map(TrackedPath::from_bytes)
        .collect())
}

/// Returns the directory where a stray root file most likely belongs.
fn hint(path: &str) -> &'static str {
    let upper = path.to_uppercase();
    for (prefix, home) in HOMES {
        if upper.starts_with(prefix) {
            return home;
        }
    }
    // Python's `endswith(".md")` is case-sensitive, so the port is too.
    #[allow(clippy::case_sensitive_file_extension_comparisons)]
    let is_markdown = path.ends_with(".md");
    if is_markdown {
        "a directory under docs/"
    } else {
        "a subdirectory"
    }
}

/// Returns one message for each root path that breaks the rule.
fn findings(paths: &[TrackedPath], allowed: &[&str]) -> Vec<String> {
    let mut root: Vec<&TrackedPath> = paths.iter().filter(|p| !p.raw.contains(&b'/')).collect();
    root.sort_by(|a, b| a.key.cmp(&b.key));
    let allowed_set: BTreeSet<&str> = allowed.iter().copied().collect();
    let mut found: Vec<String> = root
        .iter()
        .filter(|p| !allowed_set.contains(p.text.as_str()))
        .map(|p| {
            format!(
                "{}: not allowed at the root; move it to {}",
                p.text,
                hint(&p.text)
            )
        })
        .collect();
    let tracked: BTreeSet<&str> = root.iter().map(|p| p.text.as_str()).collect();
    for path in &allowed_set {
        if !tracked.contains(path) {
            found.push(format!("{path}: in ALLOWED, but not tracked"));
        }
    }
    found
}

/// Checks the real tree and prints the findings.
fn check() -> Result<ExitCode> {
    let paths = tracked_paths()?;
    let allowed: Vec<&str> = ALLOWED.iter().map(|(path, _)| *path).collect();
    let found = findings(&paths, &allowed);
    for message in &found {
        println!("{message}");
    }
    println!(
        "root-files: {} allowed, {} findings",
        ALLOWED.len(),
        found.len()
    );
    if !found.is_empty() {
        println!(
            "Fix: `git mv` the file into its home. If the root must hold it, \
             add it to ALLOWED in xtask/src/audit/root_files.rs with a reason."
        );
    }
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let allow = ["README.md", "Cargo.toml"];
    let paths = |names: &[&str]| -> Vec<TrackedPath> {
        names
            .iter()
            .map(|n| TrackedPath::from_bytes(n.as_bytes()))
            .collect()
    };

    let found = findings(
        &paths(&["README.md", "Cargo.toml", "docs/design/DESIGN-1.md"]),
        &allow,
    );
    ensure!(found.is_empty(), "{found:?}");

    let found = findings(
        &paths(&["README.md", "Cargo.toml", "DESIGN-2308.md"]),
        &allow,
    );
    ensure!(
        found == ["DESIGN-2308.md: not allowed at the root; move it to docs/design/"],
        "{found:?}"
    );

    let found = findings(
        &paths(&["README.md", "Cargo.toml", "PLAN-x.md", "notes.md", "a.sh"]),
        &allow,
    );
    ensure!(
        found
            == [
                "PLAN-x.md: not allowed at the root; move it to docs/plans/",
                "a.sh: not allowed at the root; move it to a subdirectory",
                "notes.md: not allowed at the root; move it to a directory under docs/",
            ],
        "{found:?}"
    );

    let found = findings(&paths(&["README.md"]), &allow);
    ensure!(
        found == ["Cargo.toml: in ALLOWED, but not tracked"],
        "{found:?}"
    );
    Ok(())
}

/// Runs `cargo xtask audit root-files`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("root-files self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("root-files self-test failed: {error:#}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_test_passes() -> Result<()> {
        self_test()
    }

    #[test]
    fn a_root_directory_is_not_a_root_file() {
        let paths = [
            TrackedPath::from_text("README.md"),
            TrackedPath::from_text("xtask/Cargo.toml"),
        ];
        assert_eq!(findings(&paths, &["README.md"]), Vec::<String>::new());
    }

    #[test]
    fn a_non_utf8_name_is_escaped_like_python() {
        let path = TrackedPath::from_bytes(b"bad\xffname");
        assert_eq!(path.text, "bad\\udcffname");
        let found = findings(&[path], &[]);
        assert_eq!(
            found,
            ["bad\\udcffname: not allowed at the root; move it to a subdirectory"]
        );
    }

    #[test]
    fn every_allowed_entry_is_a_root_file() {
        assert!(ALLOWED.iter().all(|(path, _)| !path.contains('/')));
    }
}
