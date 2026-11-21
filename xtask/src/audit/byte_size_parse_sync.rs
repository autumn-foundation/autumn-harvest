//! `cargo xtask audit byte-size-parse-sync`.
//!
//! It keeps the byte-size-parsing clone class in step (issue #2625).
//!
//! Two crates each parse a human-readable byte-size string (`KiB`, `KB`,
//! `MiB`, `MB`, `GiB`, `GB`, or plain bytes): `autumn_harvest::parse_byte_size`
//! in `autumn-harvest/src/lib.rs`, and
//! `autumn_harvest_macros::parse_byte_size_macro` in
//! `autumn-harvest-macros/src/lib.rs`. `autumn-harvest-macros` is a
//! `proc-macro` crate. Its manifest depends on `syn`, `quote` and
//! `proc-macro2` only, never on `autumn-harvest`. A proc-macro crate
//! expands into the very crate that would then need to depend on it.
//! No crate exists that both call sites could share a function from.
//!
//! This command extracts each body by brace matching. It then normalizes
//! the two allowed differences: the signature line's function name and
//! visibility, and the wording of line comments. It fails in four cases:
//! - The bodies differ once normalized.
//! - A copy is missing, not unique or unclosed.
//! - A body has a block comment or a raw string. The brace matcher cannot
//!   read them, so the command refuses them.
//! - The sibling comment that names this issue is gone.
//!
//! Usage: `cargo xtask audit byte-size-parse-sync [--self-test]`.
//! The brace matcher lives in `libpq_keyword_sync.rs`.

use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::libpq_keyword_sync::{UNSUPPORTED, find_fn, py_read_repo_file, py_splitlines};
use super::vantage_flash_live_region::compile;

/// Arguments of `cargo xtask audit byte-size-parse-sync`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {
    /// Run the fixtures instead of the real files.
    #[arg(long)]
    pub self_test: bool,
}

const ISSUE: &str = "#2625";
static ISSUE_RE: LazyLock<Regex> =
    LazyLock::new(|| compile(&format!(r"{}\b", regex::escape(ISSUE))));

const TARGET_RUNTIME: &str = "autumn-harvest/src/lib.rs";
const TARGET_MACRO: &str = "autumn-harvest-macros/src/lib.rs";
const TARGETS: [&str; 2] = [TARGET_RUNTIME, TARGET_MACRO];

// Both targets are named `lib.rs`, so the sibling comment names the
// crate-qualified function instead of a bare file name.
const REF_RUNTIME: &str = "autumn_harvest::parse_byte_size";
const REF_MACRO: &str = "autumn_harvest_macros::parse_byte_size_macro";

static SIGNATURE_RUNTIME: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?m)^pub fn parse_byte_size\(s: &str\) -> Option<u64> \{"));
static SIGNATURE_MACRO: LazyLock<Regex> = LazyLock::new(|| {
    compile(r"(?m)^pub\(crate\) fn parse_byte_size_macro\(s: &str\) -> Option<u64> \{")
});

// A comment's text is the one place the two copies may differ in wording.
// Each one collapses to a fixed marker before the comparison.
static LINE_COMMENT: LazyLock<Regex> = LazyLock::new(|| compile(r"//[^\n]*"));
// An attribute line between the doc comment and the signature (`#[must_use]`
// on the runtime copy only). The sibling-comment search steps over it.
static ATTRIBUTE_LINE: LazyLock<Regex> = LazyLock::new(|| compile(r"^\s*#\["));

/// Returns the signature pattern that opens the copy at `path`.
fn signature_for(path: &str) -> &'static LazyLock<Regex> {
    if path == TARGET_RUNTIME {
        &SIGNATURE_RUNTIME
    } else {
        &SIGNATURE_MACRO
    }
}

/// Returns the crate-qualified name the sibling comment must name for the
/// copy at `path` — the *other* copy, not this one.
fn counterpart_ref(path: &str) -> &'static str {
    if path == TARGET_RUNTIME {
        REF_MACRO
    } else {
        REF_RUNTIME
    }
}

/// Collapses the two allowed differences so the rest compares exactly.
///
/// The signature line (function name and visibility) is dropped outright;
/// every remaining line comment's wording collapses to one fixed marker.
fn normalize(body: &str) -> String {
    let after_signature = body.find('\n').map_or("", |i| &body[i..]);
    LINE_COMMENT
        .replace_all(after_signature, "// <comment>")
        .into_owned()
}

/// Returns true when the doc comment just above `start` names this issue.
///
/// The block must also name the `counterpart` file. Any number of
/// attribute lines are allowed to sit between the doc comment and the
/// signature at `start`.
fn has_sibling_comment(source: &str, start: usize, counterpart: &str) -> bool {
    let mut lines = py_splitlines(&source[..start]).into_iter().rev();
    let mut next = lines.next();
    while next.is_some_and(|line| ATTRIBUTE_LINE.is_match(line)) {
        next = lines.next();
    }
    let mut comment = Vec::new();
    while let Some(line) = next {
        if !(line.starts_with("///") || line.starts_with("//!")) {
            break;
        }
        comment.push(line);
        next = lines.next();
    }
    let text = comment.join("\n");
    ISSUE_RE.is_match(&text) && text.contains(counterpart)
}

/// Returns one message per finding over `sources`, a list of path and text.
fn findings(sources: &[(&str, String)]) -> Vec<String> {
    let mut found = Vec::new();
    let mut bodies: Vec<(&str, String)> = Vec::new();
    for (path, source) in sources {
        let signature = signature_for(path);
        let Some((start, body)) = find_fn(source, signature) else {
            found.push(format!(
                "{path}: the parse_byte_size function is missing, not unique or unclosed."
            ));
            continue;
        };
        if UNSUPPORTED.is_match(body) {
            found.push(format!(
                "{path}: the parse_byte_size function has a block comment or a \
                 raw string. Use line comments and plain strings."
            ));
            continue;
        }
        bodies.push((path, normalize(body)));
        let counterpart = counterpart_ref(path);
        if !has_sibling_comment(source, start, counterpart) {
            found.push(format!(
                "{path}: the doc comment above the parse_byte_size function must \
                 name issue {ISSUE} and {counterpart}."
            ));
        }
    }
    let differ = bodies.windows(2).any(|w| w[0].1 != w[1].1);
    if differ {
        let names: Vec<&str> = bodies.iter().map(|(p, _)| *p).collect();
        found.push(format!(
            "{} differ beyond the signature line and comment wording. \
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
    println!("byte-size-parse-sync: {} findings", found.len());
    Ok(crate::util::gate_exit(found.is_empty()))
}

/// Exercises each failure case on fixtures.
fn self_test() -> Result<()> {
    let doc_a = "/// Sibling of autumn_harvest_macros::parse_byte_size_macro (#2625).\n";
    let body_a = concat!(
        "pub fn parse_byte_size(s: &str) -> Option<u64> {\n",
        "    let s = s.trim();\n",
        "    // comment A wording\n",
        "    s.parse().ok()\n",
        "}",
    );
    let attr_a = "#[must_use]\n";
    let doc_b = "/// Sibling of autumn_harvest::parse_byte_size (#2625).\n";
    let body_b = concat!(
        "pub(crate) fn parse_byte_size_macro(s: &str) -> Option<u64> {\n",
        "    let s = s.trim();\n",
        "    // comment B, worded differently\n",
        "    s.parse().ok()\n",
        "}",
    );
    let good_a = format!("{doc_a}{attr_a}{body_a}\n\nfn other() {{}}\n");
    let good_b = format!("{doc_b}{body_b}\n\nfn other() {{}}\n");

    let (start_a, found_a) = find_fn(&good_a, &SIGNATURE_RUNTIME).context("good_a has no fn")?;
    let (start_b, found_b) = find_fn(&good_b, &SIGNATURE_MACRO).context("good_b has no fn")?;
    ensure!(found_a == body_a, "{found_a:?}");
    ensure!(found_b == body_b, "{found_b:?}");
    ensure!(has_sibling_comment(&good_a, start_a, REF_MACRO));
    ensure!(has_sibling_comment(&good_b, start_b, REF_RUNTIME));
    ensure!(!has_sibling_comment(&good_a, start_a, "third.rs"));
    ensure!(
        normalize(found_a) == normalize(found_b),
        "{:?} != {:?}",
        normalize(found_a),
        normalize(found_b)
    );

    ensure!(find_fn("fn other() {}\n", &SIGNATURE_RUNTIME).is_none());
    ensure!(find_fn(&format!("{body_a}\n{body_a}"), &SIGNATURE_RUNTIME).is_none());
    let unclosed = &body_a[..body_a.rfind('}').context("no brace")?];
    ensure!(find_fn(unclosed, &SIGNATURE_RUNTIME).is_none());

    for text in [
        body_a.to_string(),
        format!("/// macro.rs #26251.\n{attr_a}{body_a}"),
        format!("/// macro.rs #1.\n{attr_a}{body_a}"),
    ] {
        let (start, _) = find_fn(&text, &SIGNATURE_RUNTIME).context("fixture has no fn")?;
        ensure!(!has_sibling_comment(&text, start, "macro.rs"), "{text}");
    }

    let sources = [(TARGETS[0], good_a.clone()), (TARGETS[1], good_b.clone())];
    ensure!(findings(&sources).is_empty(), "{:?}", findings(&sources));

    let drifted_b = good_b.replace("s.parse().ok()", "s.trim().parse().ok()");
    let drifted = [(TARGETS[0], good_a.clone()), (TARGETS[1], drifted_b)];
    ensure!(
        findings(&drifted).iter().any(|m| m.contains("differ")),
        "{:?}",
        findings(&drifted)
    );

    let gone = [
        (TARGETS[0], good_a.clone()),
        (TARGETS[1], format!("{body_b}\n\nfn other() {{}}\n")),
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
        let text = good_a.replace("s.parse().ok()", &format!("{risky}\n    s.parse().ok()"));
        let refused = findings(&[(TARGETS[0], text), (TARGETS[1], good_b.clone())]);
        ensure!(
            refused.iter().any(|m| m.contains("raw string")),
            "{risky}: {refused:?}"
        );
    }
    Ok(())
}

/// Runs `cargo xtask audit byte-size-parse-sync`.
pub fn run(args: Args) -> Result<ExitCode> {
    if !args.self_test {
        return check();
    }
    match self_test() {
        Ok(()) => {
            println!("byte-size-parse-sync self-test: OK");
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("byte-size-parse-sync self-test failed: {error:#}");
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
    fn an_attribute_line_does_not_block_the_sibling_comment() {
        let text = "/// names #2625 and a.rs\n#[must_use]\nfn x() {}\n";
        assert!(has_sibling_comment(text, text.rfind("fn").unwrap(), "a.rs"));
    }

    #[test]
    fn a_blank_line_breaks_the_sibling_comment() {
        let text = "/// names #2625 and a.rs\n\nfn x() {}\n";
        assert!(!has_sibling_comment(
            text,
            text.rfind("fn").unwrap(),
            "a.rs"
        ));
        let text = "/// names #2625 and a.rs\nfn x() {}\n";
        assert!(has_sibling_comment(text, text.rfind("fn").unwrap(), "a.rs"));
    }
}
