//! `cargo xtask audit libpq-keyword-sync`: the port of
//! `docs/audits/libpq-keyword-sync.py`.
//!
//! It keeps the libpq-keyword-list clone class in step (issue #2521).
//!
//! Two files each define `is_connection_keyword(key: &str) -> bool`:
//! `autumn-harvest-cli/src/lib.rs` and `autumn-harvest-plugin/src/dev/dsn.rs`.
//! Each has a `const KEYWORDS: &[&str]` that lists the 46 libpq
//! connection-string parameter names, with the same case-insensitive lookup.
//! Issue #1322 found that the `dev::dsn` copy lacked the existing guard of
//! the CLI. PR #1601 copied the function, with its `KEYWORDS` list, across to
//! close it. The two copies are below the rule-of-three merge bar. A shared
//! home would be a public-API change to the `autumn-harvest` core crate, and
//! that needs a human decision (see issue #2521). So for now they stay
//! linked, not merged.
//!
//! This command extracts the body of each function by brace matching. It
//! fails in four cases:
//! - The bodies differ once the one allowed difference is normalized away.
//!   That difference is the `pub(super) ` visibility prefix and the wording
//!   of line comments.
//! - A copy is missing, not unique or unclosed.
//! - A body has a block comment or a raw string. The brace matcher cannot
//!   read them, so the command refuses them.
//! - The sibling comment that names this issue is gone.
//!
//! Usage: `cargo xtask audit libpq-keyword-sync [--self-test]`.
//!
//! This module also holds the brace matcher that
//! `cargo xtask audit required-migration-versions-sync` shares.

use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::vantage_flash_live_region::compile;
use crate::util::read_repo_file;

/// Arguments of `cargo xtask audit libpq-keyword-sync`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real files.
    #[arg(long)]
    pub self_test: bool,
}

const ISSUE: &str = "#2521";
const FN_NAME: &str = "is_connection_keyword";
static ISSUE_RE: LazyLock<Regex> =
    LazyLock::new(|| compile(&format!(r"{}\b", regex::escape(ISSUE))));
static SIGNATURE: LazyLock<Regex> = LazyLock::new(|| {
    compile(&format!(
        r"(?m)^(?:pub\(super\) )?fn {FN_NAME}\(key: &str\) -> bool \{{"
    ))
});

// A char literal, a string literal or a line comment. Braces inside these
// do not count toward brace matching. The leading `^` anchors the pattern
// at the scan position, as Python's `re.match` does.
static SKIP: LazyLock<Regex> =
    LazyLock::new(|| compile(r#"^(?:'(?:\\.|[^\\'])'|"(?:\\.|[^\\"])*"|//[^\n]*)"#));
// Constructs that `SKIP` cannot read. A body with one fails closed.
pub(super) static UNSUPPORTED: LazyLock<Regex> = LazyLock::new(|| compile(r#"/\*|\br#*""#));
// A line comment's text is the one place where the two copies may differ in
// wording. Each one collapses to a fixed marker before the comparison.
static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| compile(r"//[^\n]*"));

const TARGETS: [&str; 2] = [
    "autumn-harvest-cli/src/lib.rs",
    "autumn-harvest-plugin/src/dev/dsn.rs",
];

/// Returns the start and the text of the one function that `signature` opens.
///
/// The text runs from the signature to the closing brace. Returns `None`
/// when the function is missing, is not unique, or has no closing brace.
pub(super) fn find_fn<'s>(source: &'s str, signature: &Regex) -> Option<(usize, &'s str)> {
    let mut matches = signature.find_iter(source);
    let first = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    let start = first.start();
    let mut pos = first.end();
    let mut depth = 1;
    while depth > 0 {
        if let Some(skipped) = SKIP.find(&source[pos..]) {
            pos += skipped.end();
            continue;
        }
        let ch = source[pos..].chars().next()?;
        if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth -= 1;
        }
        pos += ch.len_utf8();
    }
    Some((start, &source[start..pos]))
}

/// Splits `text` into lines the way Python's `str.splitlines` does.
///
/// Python also ends a line at a vertical tab, a form feed, the separators
/// U+001C to U+001E, U+0085, U+2028 and U+2029. A line comment search over
/// the lines must see the same lines.
pub(super) fn py_splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let is_break = matches!(
            c,
            '\n' | '\r'
                | '\u{0b}'
                | '\u{0c}'
                | '\u{1c}'
                | '\u{1d}'
                | '\u{1e}'
                | '\u{85}'
                | '\u{2028}'
                | '\u{2029}'
        );
        if !is_break {
            continue;
        }
        lines.push(&text[start..i]);
        let mut next = i + c.len_utf8();
        if c == '\r' && chars.peek().is_some_and(|&(_, n)| n == '\n') {
            chars.next();
            next += 1;
        }
        start = next;
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// Translates line endings the way Python's text mode reads a file.
///
/// Python's `read_text` turns each `\r\n` and each lone `\r` into `\n`.
/// Without this, a file with CRLF endings breaks a pattern that ends on `\n`.
/// So a gate would fail where the Python original passed.
pub(super) fn py_universal_newlines(text: String) -> String {
    if text.contains('\r') {
        text.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        text
    }
}

/// Reads a file under the repository root as Python's `read_text` does.
pub(super) fn py_read_repo_file(rel: &str) -> Result<String> {
    Ok(py_universal_newlines(read_repo_file(rel)?))
}

/// Reads a file at `path` as Python's `read_text` does.
pub(super) fn py_read_text(path: &std::path::Path) -> Result<String> {
    Ok(py_universal_newlines(fs::read_string(path)?))
}

/// Collapses the one allowed difference so the rest compares exactly.
fn normalize(body: &str) -> String {
    let body = body.replacen("pub(super) fn", "fn", 1);
    LINE_COMMENT.replace_all(&body, "// <comment>").into_owned()
}

/// Returns true when the doc comment just above `start` names this issue.
///
/// The block must also name the `counterpart` file.
fn has_sibling_comment(source: &str, start: usize, counterpart: &str) -> bool {
    let comment: Vec<&str> = py_splitlines(&source[..start])
        .into_iter()
        .rev()
        .take_while(|line| line.starts_with("///") || line.starts_with("//!"))
        .collect();
    let text = comment.join("\n");
    ISSUE_RE.is_match(&text) && text.contains(counterpart)
}

/// Returns the file name of `path`, as Python's `Path.name` does.
pub(super) fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Returns one message per finding over `sources`, a list of path and text.
fn findings(sources: &[(&str, String)]) -> Vec<String> {
    let mut found = Vec::new();
    let mut bodies: Vec<(&str, String)> = Vec::new();
    for (path, source) in sources {
        let Some((start, body)) = find_fn(source, &SIGNATURE) else {
            found.push(format!(
                "{path}: fn {FN_NAME} is missing, not unique or unclosed."
            ));
            continue;
        };
        if UNSUPPORTED.is_match(body) {
            found.push(format!(
                "{path}: fn {FN_NAME} has a block comment or a raw string. \
                 Use line comments and plain strings."
            ));
            continue;
        }
        bodies.push((path, normalize(body)));
        let counterpart = TARGETS
            .iter()
            .find(|t| *t != path)
            .map_or("", |t| file_name(t));
        if !has_sibling_comment(source, start, counterpart) {
            found.push(format!(
                "{path}: the doc comment above fn {FN_NAME} must name issue {ISSUE} and {counterpart}."
            ));
        }
    }
    let differ = bodies.windows(2).any(|w| w[0].1 != w[1].1);
    if differ {
        let names: Vec<&str> = bodies.iter().map(|(p, _)| *p).collect();
        found.push(format!(
            "{} differ beyond the visibility prefix and comment wording. \
             Change both the same way, or merge them (issue {ISSUE}).",
            names.join(" and ")
        ));
    }
    found
}

/// Checks the real files and prints the findings.
fn check() -> Result<ExitCode> {
    let mut sources = Vec::new();
    for rel in TARGETS {
        sources.push((rel, py_read_repo_file(rel)?));
    }
    let found = findings(&sources);
    for message in &found {
        println!("{message}");
    }
    println!("libpq-keyword-sync: {} findings", found.len());
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let doc_a = "/// Sibling of `dsn.rs` (#2521).\n";
    let body_a = concat!(
        "fn is_connection_keyword(key: &str) -> bool {\n",
        "    const KEYWORDS: &[&str] = &[\"host\", \"port\"];\n",
        "    // comment A wording\n",
        "    KEYWORDS.iter().any(|k| key.eq_ignore_ascii_case(k))\n",
        "}\n",
    );
    let doc_b = "/// Sibling of `lib.rs` (#2521).\n";
    let body_b = concat!(
        "pub(super) fn is_connection_keyword(key: &str) -> bool {\n",
        "    const KEYWORDS: &[&str] = &[\"host\", \"port\"];\n",
        "    // comment B, worded differently\n",
        "    KEYWORDS.iter().any(|k| key.eq_ignore_ascii_case(k))\n",
        "}\n",
    );
    let good_a = format!("{doc_a}{body_a}\nfn other() {{}}\n");
    let good_b = format!("{doc_b}{body_b}\nfn other() {{}}\n");

    let (start_a, found_a) = find_fn(&good_a, &SIGNATURE).context("good_a has no fn")?;
    let (start_b, found_b) = find_fn(&good_b, &SIGNATURE).context("good_b has no fn")?;
    ensure!(found_a == body_a.trim_end_matches('\n'), "{found_a:?}");
    ensure!(found_b == body_b.trim_end_matches('\n'), "{found_b:?}");
    ensure!(has_sibling_comment(&good_a, start_a, "dsn.rs"));
    ensure!(has_sibling_comment(&good_b, start_b, "lib.rs"));
    ensure!(!has_sibling_comment(&good_a, start_a, "third.rs"));
    ensure!(
        normalize(found_a) == normalize(found_b),
        "{:?} != {:?}",
        normalize(found_a),
        normalize(found_b)
    );

    ensure!(find_fn("fn other() {}\n", &SIGNATURE).is_none());
    ensure!(find_fn(&format!("{body_a}\n{body_a}"), &SIGNATURE).is_none());
    let unclosed = &body_a[..body_a.rfind('}').context("no brace")?];
    ensure!(find_fn(unclosed, &SIGNATURE).is_none());

    for text in [
        body_a.to_string(),
        format!("/// dsn.rs #25211.\n{body_a}"),
        format!("/// dsn.rs #1.\n{body_a}"),
    ] {
        let (start, _) = find_fn(&text, &SIGNATURE).context("fixture has no fn")?;
        ensure!(!has_sibling_comment(&text, start, "dsn.rs"), "{text}");
    }

    let sources = [(TARGETS[0], good_a.clone()), (TARGETS[1], good_b.clone())];
    ensure!(findings(&sources).is_empty(), "{:?}", findings(&sources));

    let drifted_b = good_b.replace("&[\"host\", \"port\"]", "&[\"host\"]");
    let drifted = [(TARGETS[0], good_a.clone()), (TARGETS[1], drifted_b)];
    ensure!(
        findings(&drifted).iter().any(|m| m.contains("differ")),
        "{:?}",
        findings(&drifted)
    );

    let gone = [
        (TARGETS[0], good_a.clone()),
        (TARGETS[1], format!("{body_b}\nfn other() {{}}\n")),
    ];
    ensure!(
        findings(&gone).iter().any(|m| m.contains("must name")),
        "{:?}",
        findings(&gone)
    );

    let absent = [
        (TARGETS[0], good_a.clone()),
        (TARGETS[1], "fn other() {}\n".to_string()),
    ];
    ensure!(
        findings(&absent).iter().any(|m| m.contains("missing")),
        "{:?}",
        findings(&absent)
    );

    for risky in ["/* } */", "/* /* } */ */", "let s = r#\"a\"}\"#;"] {
        let text = good_a.replace("KEYWORDS.iter()", &format!("{risky}\n    KEYWORDS.iter()"));
        let refused = findings(&[(TARGETS[0], text), (TARGETS[1], good_b.clone())]);
        ensure!(
            refused.iter().any(|m| m.contains("raw string")),
            "{risky}: {refused:?}"
        );
    }
    Ok(())
}

/// Runs `cargo xtask audit libpq-keyword-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("libpq-keyword-sync self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("libpq-keyword-sync self-test failed: {error:#}");
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
    fn line_endings_translate_like_python() {
        assert_eq!(
            py_universal_newlines("a\r\nb\rc\n".to_string()),
            "a\nb\nc\n"
        );
        assert_eq!(py_universal_newlines("plain\n".to_string()), "plain\n");
        let crlf = "/// Sibling (#2521) of `dsn.rs`.\r\nfn is_connection_keyword(key: &str) -> bool {\r\n    true\r\n}\r\n";
        let lf = py_universal_newlines(crlf.to_string());
        let (start, body) = find_fn(&lf, &SIGNATURE).unwrap();
        assert!(!body.contains('\r'));
        assert!(has_sibling_comment(&lf, start, "dsn.rs"));
    }

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(
            py_splitlines("a\nb\r\nc\rd\u{0c}e\n"),
            ["a", "b", "c", "d", "e"]
        );
        assert_eq!(py_splitlines("a\n\nb"), ["a", "", "b"]);
        assert_eq!(py_splitlines(""), Vec::<&str>::new());
    }

    #[test]
    fn a_brace_in_a_string_or_comment_does_not_close_the_body() {
        let src = "fn is_connection_keyword(key: &str) -> bool {\n    let s = \"}\"; // }\n    '}' == 'x'\n}\nrest";
        let (_, body) = find_fn(src, &SIGNATURE).unwrap();
        assert!(body.ends_with("'}' == 'x'\n}"), "{body}");
    }
}
