//! `cargo xtask check search-attr-query-syntax`: the port of
//! `scripts/check-search-attr-query-syntax.sh`.
//!
//! The guard fails if any documented `search_attr=` query string uses
//! `key=value` instead of the `key:value` separator of the API.
//!
//! Mechanism this guards against: `parse_workflow_filters` in
//! `autumn-harvest-plugin/src/api.rs` parses each `search_attr` query value
//! with `value.split_once(':')`. With no colon it answers 400 with
//! `invalid search_attr '...'; expected 'key:value'`.
//! `strict_query::decode_or_autumn_error` percent-decodes that value before `parse_workflow_filters` sees it. So a
//! literal `:` and its encoded forms `%3A` and `%3a` are equally valid on the
//! wire, and this guard accepts all three (PR #1432).
//!
//! `README.md` documents the correct `?search_attr=tenant:acme` shape. But
//! `examples/billing-autumn-web/README.md` gave `search_attr=tenant_id=acme`.
//! That README is the "reference example for the less tiny path" of the
//! repository. It is the first thing that a reader who builds a real
//! integration sees. The `=` matched the `key=value` pairs of the URL around
//! it, not the `key:value` shape of the API. The reader runs that curl right
//! after starting a checkout. A live run of that exact command against a
//! running billing-autumn-web returns `400 Bad Request`. No CI job runs this
//! example, so nothing else catches this class of drift.

use std::path::Path;
use std::process::ExitCode;

use regex::Regex;
use smop::prelude::*;

use super::default_run::support::{self, Report};
use crate::util;

/// Arguments of `cargo xtask check search-attr-query-syntax`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

/// Runs `cargo xtask check search-attr-query-syntax`.
pub fn run(_args: Args) -> Result<ExitCode> {
    Ok(evaluate(&util::repo_root())?.emit())
}

/// The patterns of the guard, compiled once.
struct Patterns {
    occurrence: Regex,
    colon: Regex,
    encoded_colon: Regex,
}

impl Patterns {
    fn new() -> Result<Self> {
        Ok(Self {
            occurrence: Regex::new(r#"search_attr=[^&'"` ]*"#)?,
            colon: Regex::new(r"^[^:=]+:")?,
            encoded_colon: Regex::new(r"^[^:=]+%3[Aa]")?,
        })
    }

    /// Returns the value of each bad `search_attr=<value>` in `line`.
    ///
    /// `key:value` is correct. Anything else is the defect class of this
    /// guard: a value that starts with `=`, or has an `=` before its first
    /// `:`.
    fn bad_values<'a>(&self, line: &'a str) -> Vec<&'a str> {
        self.occurrence
            .find_iter(line)
            // `while read -r match` trims the whitespace at each end.
            .map(|found| found.as_str().trim_matches([' ', '\t', '\n']))
            .map(|found| found.strip_prefix("search_attr=").unwrap_or(found))
            // A bare `search_attr=` with no runnable value after it is prose
            // about the parameter. It is not a copy-pasteable example, so the
            // guard has nothing to check.
            .filter(|value| !value.is_empty())
            .filter(|value| !self.colon.is_match(value) && !self.encoded_colon.is_match(value))
            .collect()
    }
}

/// Scans every markdown file under `root`.
fn evaluate(root: &Path) -> Result<Report> {
    let patterns = Patterns::new()?;
    let mut report = Report::new();
    let mut count = 0_usize;
    for (shown, path) in support::markdown_files(root) {
        let text = support::read_or_empty(&path);
        for (index, line) in support::lines(&text).into_iter().enumerate() {
            if !line.contains("search_attr=") {
                continue;
            }
            // The old guard read `file:line:text` back with `IFS=: read`,
            // which drops a lone trailing colon from the text.
            let line = support::bash_read_colon_rest(line);
            for value in patterns.bad_values(line) {
                report.err(format!(
                    "{shown}:{}: 'search_attr={value}' does not match the API's 'key:value' shape (parse_workflow_filters splits on ':', not '=').",
                    index + 1
                ));
                count += 1;
            }
        }
    }

    if count > 0 {
        report.err("");
        report.fail(
            "Fix: use ':' to separate the search-attr key from its value, e.g. 'search_attr=tenant_id:acme'.",
        );
        return Ok(report);
    }
    report.out("OK: every documented search_attr= query string uses 'key:value'.");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(files: &[(&str, &str)]) -> smop::fs::TempDir {
        let dir = fs::temp_dir().expect("temp dir");
        for (path, body) in files {
            let full = dir.path().join(path);
            std::fs::create_dir_all(full.parent().expect("parent")).expect("mkdir");
            std::fs::write(full, body).expect("write");
        }
        dir
    }

    #[test]
    fn colon_and_encoded_colon_values_pass() {
        let dir = fixture(&[(
            "README.md",
            "curl '/workflows?search_attr=tenant:acme&x=1'\n`search_attr=` takes key:value\n?search_attr=tenant%3aacme\n",
        )]);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 0, "{}", report.stderr());
    }

    #[test]
    fn an_equals_separator_fails() {
        let dir = fixture(&[(
            "examples/b/README.md",
            "intro\ncurl \"http://x/api?status=running&search_attr=tenant_id=acme\"\n",
        )]);
        let report = evaluate(dir.path()).expect("evaluate");
        assert_eq!(report.code(), 1);
        assert_eq!(
            report.stderr(),
            "./examples/b/README.md:2: 'search_attr=tenant_id=acme' does not match the API's 'key:value' shape (parse_workflow_filters splits on ':', not '=').\n\nFix: use ':' to separate the search-attr key from its value, e.g. 'search_attr=tenant_id:acme'.\n"
        );
    }

    #[test]
    fn bad_values_checks_each_occurrence() {
        let patterns = Patterns::new().expect("patterns");
        assert_eq!(
            patterns.bad_values("search_attr==x search_attr=a:b search_attr=k=v:w"),
            vec!["=x", "k=v:w"]
        );
    }
}
