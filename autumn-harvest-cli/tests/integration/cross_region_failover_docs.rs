//! Guards that the `harvest ...` command lines published in
//! `docs/runbooks/cross-region-failover.md` actually parse against the real
//! CLI.
//!
//! A runbook is read under pressure during a live failover, and a flag that
//! only exists on `dr status`/`dr fence`/`dr promote`/`partition ...` (which
//! each declare their own local `-o` short alias) does not carry over to
//! `backup verify` or `worker health`, which accept only the long
//! `--format`/`--output` form. Extracted from the doc's own fenced examples so
//! a future edit that reintroduces a bogus short flag fails here instead of
//! during an incident.

use clap::Parser as _;
use std::path::{Path, PathBuf};

use autumn_harvest_cli::Cli;

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate directory must have a parent")
        .to_path_buf()
}

fn runbook() -> String {
    let path = repo_root().join("docs/runbooks/cross-region-failover.md");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// Every `harvest ...` command line inside a fenced ` ```bash ` block.
/// Backslash line-continuations are joined into their one command; comment
/// lines and blank lines are dropped; a block with several independent
/// command lines (no continuation between them) yields several commands, not
/// one concatenated string.
fn harvest_invocations(doc: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut lines = doc.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "```bash" {
            continue;
        }
        let mut current = String::new();
        for body in lines.by_ref() {
            if body.trim() == "```" {
                break;
            }
            let trimmed = body.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let continues = trimmed.ends_with('\\');
            current.push_str(trimmed.trim_end_matches('\\').trim_end());
            if continues {
                current.push(' ');
            } else {
                let cmd = std::mem::take(&mut current);
                if cmd.starts_with("harvest ") {
                    out.push(cmd);
                }
            }
        }
    }
    out
}

/// Whitespace tokenizer that keeps a double-quoted span (e.g. a `--reason`
/// value with spaces) as one token, matching how a shell would split the
/// line. No escaping is needed: the runbook's examples never nest quotes.
fn tokenize(cmd: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for c in cmd.chars() {
        match c {
            '"' => in_quotes = !in_quotes,
            c if c.is_whitespace() && !in_quotes => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => current.push(c),
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[test]
fn every_harvest_invocation_in_the_failover_runbook_parses() {
    let doc = runbook();
    let invocations = harvest_invocations(&doc);
    assert!(
        invocations.len() >= 5,
        "expected at least 5 `harvest` command examples in the runbook, found {}: {invocations:?}",
        invocations.len()
    );
    for cmd in &invocations {
        let tokens = tokenize(cmd);
        Cli::try_parse_from(&tokens).unwrap_or_else(|e| {
            panic!("runbook command does not parse against the real CLI: `{cmd}`\n{e}")
        });
    }
}
