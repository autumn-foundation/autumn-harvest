//! `cargo xtask audit vantage-label-association`: the port of
//! `docs/audits/vantage-label-association.py`.
//!
//! This is the form-label association audit for the Vantage dashboard
//! (`autumn-harvest-plugin/src/ui.rs`).
//!
//! The audit is deterministic and reproducible on any checkout. It needs no
//! server, no browser and no network access.
//!
//! Vantage has no client-side JavaScript for this. An `<input>`, `<select>`
//! or `<textarea>` gets an accessible name only through a real HTML
//! mechanism. One is a `for="…"` attribute that points at the `id` of the
//! control. The other is a `<label>` that wraps the control as a descendant.
//! That implicit form is valid per the HTML spec without any `for` and `id`
//! pair at all. Every `label { … }` block in this file uses the wrapping form
//! today, and `for="` does not appear anywhere in `ui.rs`. The audit still
//! checks both paths, and it checks the `for` path for real. A `for="typo"`
//! next to an `id="actual"` resolves to nothing in a browser. So a bare
//! "the attribute is present" check would pass a label that is still broken.
//! The audit collects each literal `id="…"` on a labelable control in the
//! file. It counts a `for` as valid only when its value is one of them.
//!
//! A `label` that only sits *next to* its control associates with nothing.
//! Siblings under the same `form` are not parent and child. A screen reader
//! then announces the control with no name: "edit text, blank" instead of
//! "Jump to event, edit text, blank". Every sibling field on the same page
//! still announces correctly. That gap is silent. The field still submits,
//! and the sighted operator still sees the adjacent text. No error, contrast
//! or flash-message audit here would ever catch it.
//!
//! To recognize a "real control" is not just a word search. First
//! `strip_comments` removes `//` and `/* */` comments. Then
//! `mask_text_strings` blanks every rendered-text string. So neither a
//! comment nor the text node of a label can supply the word or the terminator
//! that `CONTROL_OPEN` looks for. Only the own string of an attribute value
//! (after `=`) survives masking. That keeps the `for` and `id` resolution
//! working.
//!
//! Run `cargo xtask audit vantage-label-association`. The command exits 1 if
//! a `label` block has no descendant control and no `for="…"` attribute, and
//! 0 otherwise. So it is safe as a CI gate, or as a standalone report.
//!
//! The scan works on characters, not bytes, wherever the Python original
//! counts characters. So each window and each blanked span has the same
//! extent as in the original.

use std::collections::BTreeSet;
use std::process::ExitCode;
use std::sync::LazyLock;

use regex::Regex;
use smop::prelude::*;

use super::vantage_flash_live_region::{
    captures_unpreceded, compile, is_word_dot_or_hyphen, is_word_or_hyphen, line_of,
    search_unpreceded, ui_rs,
};

/// Arguments of `cargo xtask audit vantage-label-association`.
#[derive(Debug, Clone, Copy, Args)]
pub struct Args {}

// One maud attribute: `ident="literal"` or `ident=(expr)`.
const ATTR: &str = r#"\s+[\w-]+\s*=\s*(?:"[^"]*"|\([^()]*\))"#;

// The opening tag of a `label` element, from the `label` keyword to the `{`
// that starts its maud block. The Python lookbehind `(?<![\w-])` keeps this
// from matching inside another identifier that ends in "label". Examples are
// `event_label` and `dead_letter_bulk_action_label`. The attrs segment only
// accepts real maud attribute syntax, repeated. So a plain Rust binding like
// `let label = String::from("Paused");` can never satisfy it. There "label"
// is followed by a bare `=`, which matches neither alternative. So the attrs
// group matches zero times, and the mandatory `{` never follows. Without this
// restriction, `[^{]*` would skip past unrelated `;`, `=` and `(` characters.
// It would reach the next `{` anywhere later in the function. It would then
// misread an ordinary variable as an HTML element.
static LABEL_OPEN: LazyLock<Regex> =
    LazyLock::new(|| compile(&format!(r"label\b((?:{ATTR})*)\s*\{{")));

// An explicit association: `for="some-id"` on the own opening tag of the
// label. It captures the target id, so the audit can check it against a real
// control. A `for` that names no control on the page associates with
// nothing, exactly like no `for` at all. A typo or a stale rename causes that.
static FOR_ATTR: LazyLock<Regex> = LazyLock::new(|| compile(r#"for\s*=\s*"([^"]+)""#));

// The own opening tag of a labelable control. It runs from the tag name to
// the `;` that self-closes it (`input`), or to the `{` that starts its block
// (`select`, `textarea`). It mirrors the brace-based scan of `LABEL_OPEN`.
// A control can also self-close, so it accepts both terminators. The Python
// pattern starts with the lookbehind `(?<![\w.-])`.
static CONTROL_OPEN: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?:input|select|textarea)\b[^;{]*?[;{]"));

// A literal `id="…"` on the own opening tag of a control. Only a literal can
// be resolved statically. A computed `id=(expr)` cannot. So a `for` that
// points at one never counts as resolved. Unverifiable is not the same as
// verified.
static ID_ATTR: LazyLock<Regex> = LazyLock::new(|| compile(r#"id\s*=\s*"([^"]+)""#));

// A window that ends in `let` or `@let`, whitespace, an identifier and `=` is
// the right-hand side of a Rust or maud binding. An example is
// `@let hint = "select one;";`. The string goes to `hint`, and `(hint)`
// renders it later. It is never an attribute of an element, however closely
// `ident\s*=\s*$` alone resembles one. The audit checks this pattern before
// the generic attribute-tail pattern below. So a let-binding never counts as
// an attribute value, even though its tail also matches that pattern.
static LET_BINDING_TAIL: LazyLock<Regex> =
    LazyLock::new(|| compile(r"(?:^|[^\w])(?:@let|let)\s+[\w-]+\s*=\s*\z"));

// A window that ends in an identifier followed by `=`. That is maud
// attribute syntax: `ident="…"` or `ident = "…"`, with any whitespace on
// either side, as in `ATTR` above. The identifier must start at a real
// boundary: `^`, or a character that is neither a word character nor `=`.
// So this cannot match the tail of a longer identifier or a chained `==`.
static ATTR_TAIL: LazyLock<Regex> = LazyLock::new(|| compile(r"(?:^|[^\w=])[\w-]+\s*=\s*\z"));

// The inline CSS of the dashboard lives in a Rust raw string. It can contain
// a `label` *selector* (`.filters label{...}`, `.stat .label{...}`) that is
// not markup at all. At the regex level it looks exactly like a real maud
// element. Raw CSS `label{` matches "label" followed by `{` just as cleanly
// as maud syntax does. So the audit blanks the block out before the scan. It
// uses the extraction span of `cargo xtask audit vantage-dashboard-contrast`.
// Line numbers elsewhere stay the same, and nothing inside can match.
static STYLE_BLOCK: LazyLock<Regex> =
    LazyLock::new(|| compile(r##"(?s)const STYLE: &str = r#"\n(.*?)\n"#;"##));

/// Blanks out comments while it keeps string literals intact.
///
/// It blanks `//` line comments, which covers `///` and `//!` too, and
/// `/* … */` block comments. It tracks string literals, so it leaves a
/// comment marker inside a quoted value alone. It keeps each newline, so the
/// line numbers of every other regex stay correct.
///
/// Without this, `label`, `input`, `select` or `textarea` as *words* in
/// prose would satisfy a bare word search as well as the real element does.
/// An example is a code comment that explains a `<select>` two lines below
/// it. PR #1485 found that gap. The audit strips comments first, then it
/// requires a real element-opening shape (`CONTROL_OPEN`), not a word. That
/// closes the gap, because prose can no longer stand in for markup.
fn strip_comments(src: &[char]) -> Vec<char> {
    let n = src.len();
    let mut out = Vec::with_capacity(n);
    let mut i = 0;
    let mut in_string = false;
    while i < n {
        let ch = src[i];
        if in_string {
            out.push(ch);
            if ch == '\\' && i + 1 < n {
                out.push(src[i + 1]);
                i += 2;
                continue;
            }
            if ch == '"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        if ch == '"' {
            in_string = true;
            out.push(ch);
            i += 1;
            continue;
        }
        if ch == '/' && src.get(i + 1) == Some(&'/') {
            let end = src[i..]
                .iter()
                .position(|&c| c == '\n')
                .map_or(n, |off| i + off);
            out.extend(std::iter::repeat_n(' ', end - i));
            i = end;
            continue;
        }
        if ch == '/' && src.get(i + 1) == Some(&'*') {
            let end = find_seq(src, i + 2, &['*', '/']).map_or(n, |close| close + 2);
            out.extend(
                src[i..end]
                    .iter()
                    .map(|&c| if c == '\n' { '\n' } else { ' ' }),
            );
            i = end;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}

/// Returns the index of the first `needle` in `hay` at or after `from`.
fn find_seq(hay: &[char], from: usize, needle: &[char]) -> Option<usize> {
    if from > hay.len() {
        return None;
    }
    hay[from..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|off| from + off)
}

/// Returns true when the string literal at `quote_index` is an attribute value.
///
/// An attribute value sits on the own tag of an element. The right-hand side
/// of a Rust or maud `@let` binding is not one. Neither is any other string
/// that merely sits after an `=`. PR #1485 found both cases.
fn is_attribute_value(src: &[char], quote_index: usize) -> bool {
    let window: String = src[quote_index.saturating_sub(200)..quote_index]
        .iter()
        .collect();
    if LET_BINDING_TAIL.is_match(&window) {
        return false;
    }
    ATTR_TAIL.is_match(&window)
}

/// Blanks the content of each string literal that is not an attribute value.
///
/// That covers a maud rendered-text node (`"Task kind"`), and the right-hand
/// side of a `let` or `@let` binding (see `is_attribute_value`). It also
/// covers an ordinary Rust string argument (`String::from("Paused")`). The
/// quotes and content of an attribute value stay untouched. Both `ID_ATTR`
/// and `FOR_ATTR` read real content out of those.
///
/// Without this, a rendered string could satisfy the terminator search of
/// `CONTROL_OPEN` from *inside* the string. Take a label whose real control
/// moved to a sibling, but which still renders text like `"select one;"`. It
/// can render it directly, or bind it first via `@let hint = "select one;";
/// (hint)`. Either way the quoted text supplies the trailing `;` that
/// `CONTROL_OPEN` looks for, not a real element. Call this after
/// `strip_comments`. Then a quote that was only ever inside a comment is
/// already gone, and it cannot start a string here.
fn mask_text_strings(src: &[char]) -> Vec<char> {
    let mut out = src.to_vec();
    let n = src.len();
    let mut i = 0;
    while i < n {
        if src[i] != '"' {
            i += 1;
            continue;
        }
        let is_attr_value = is_attribute_value(src, i);
        let mut k = i + 1;
        while k < n && src[k] != '"' {
            k += if src[k] == '\\' { 2 } else { 1 };
        }
        let end = k.min(n);
        if !is_attr_value {
            for slot in &mut out[i + 1..end] {
                if *slot != '\n' {
                    *slot = ' ';
                }
            }
        }
        i = end + 1;
    }
    out
}

/// Returns each literal `id` on a labelable control in `src`.
fn control_ids(src: &str) -> BTreeSet<String> {
    captures_unpreceded(&CONTROL_OPEN, src, is_word_dot_or_hyphen)
        .iter()
        .filter_map(|m| {
            let tag = m.get(0)?.as_str();
            let id = search_unpreceded(&ID_ATTR, tag, is_word_or_hyphen)?;
            Some(id[1].to_string())
        })
        .collect()
}

/// Returns the index just past the `}` that matches the `{` at `open`.
///
/// It skips brace characters inside `"..."` string literals. Maud attribute
/// values and rendered text can hold `{` or `}` only inside quotes. A raw `{`
/// or `}` outside quotes is always maud syntax.
fn find_block_end(src: &str, open: usize) -> Result<usize> {
    let bytes = src.as_bytes();
    let mut depth = 0i64;
    let mut in_string = false;
    let mut i = open;
    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if ch == b'\\' {
                // Skip the escaped character too.
                i += 1;
            } else if ch == b'"' {
                in_string = false;
            }
        } else if ch == b'"' {
            in_string = true;
        } else if ch == b'{' {
            depth += 1;
        } else if ch == b'}' {
            depth -= 1;
            if depth == 0 {
                return Ok(i + 1);
            }
        }
        i += 1;
    }
    bail!("unbalanced braces starting at index {open}")
}

/// Blanks the `STYLE` block, or returns `None` when it is absent.
fn blank_style_block(src: &str) -> Option<String> {
    let span = STYLE_BLOCK.captures(src)?.get(1)?;
    let blanked: String = span
        .as_str()
        .chars()
        .map(|c| if c == '\n' { '\n' } else { ' ' })
        .collect();
    Some(format!(
        "{}{blanked}{}",
        &src[..span.start()],
        &src[span.end()..]
    ))
}

/// One `label` render site and its association.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Site {
    line: usize,
    has_for: bool,
    has_control: bool,
}

/// Returns each `label` render site of the prepared source `src`.
fn sites(src: &str) -> Result<Vec<Site>> {
    let ids = control_ids(src);
    let mut found = Vec::new();
    for m in captures_unpreceded(&LABEL_OPEN, src, is_word_or_hyphen) {
        let Some(whole) = m.get(0) else { continue };
        let attrs = m.get(1).map_or("", |a| a.as_str());
        let open_brace = whole.end() - 1;
        let block_end = find_block_end(src, open_brace)?;
        let block = &src[open_brace..block_end];
        let has_for = search_unpreceded(&FOR_ATTR, attrs, is_word_or_hyphen)
            .is_some_and(|f| ids.contains(&f[1]));
        let has_control = search_unpreceded(&CONTROL_OPEN, block, is_word_dot_or_hyphen).is_some();
        found.push(Site {
            line: line_of(src, whole.start()),
            has_for,
            has_control,
        });
    }
    found.sort();
    Ok(found)
}

/// Builds the report over the raw source of `ui.rs`.
///
/// Returns the report lines and whether the audit passed. An absent `STYLE`
/// block is the inner `Err`, whose text the caller prints.
fn report(raw: &str, path: &str) -> Result<std::result::Result<(Vec<String>, bool), String>> {
    let Some(blanked) = blank_style_block(raw) else {
        return Ok(Err(
            "could not find `const STYLE` block in ui.rs".to_string()
        ));
    };
    let chars: Vec<char> = blanked.chars().collect();
    let src: String = mask_text_strings(&strip_comments(&chars))
        .into_iter()
        .collect();
    let found = sites(&src)?;

    let mut out = vec![
        format!("Form-label association audit — {path}"),
        format!("{} `label` render sites checked.\n", found.len()),
    ];
    if found.is_empty() {
        out.extend(
            [
                "ERROR: found no `label` render sites at all — either the markup",
                "was rewritten in a spelling this audit doesn't recognize (a silent",
                "pass would hide that), or every form lost its labels outright.",
            ]
            .map(String::from),
        );
        return Ok(Ok((out, false)));
    }
    let missing: Vec<usize> = found
        .iter()
        .filter(|s| !(s.has_for || s.has_control))
        .map(|s| s.line)
        .collect();
    let ok_count = found.len() - missing.len();
    if missing.is_empty() {
        out.push(
            "Every `label` render site wraps a control or names one via `for=\"…\"`.".to_string(),
        );
    } else {
        out.push(format!("BROKEN — {}:", missing.len()));
        for line in &missing {
            out.push(format!(
                "  ui.rs:{line}  label has no `for=\"…\"` and wraps no input/select/textarea"
            ));
        }
    }
    out.push(format!(
        "\nCorrect — {ok_count} of {} render sites carry a real association.",
        found.len()
    ));
    Ok(Ok((out, missing.is_empty())))
}

/// Runs `cargo xtask audit vantage-label-association`.
pub fn run(args: Args) -> Result<ExitCode> {
    let Args {} = args;
    let path = ui_rs();
    let raw = fs::read_string(&path)?;
    match report(&raw, &path.display().to_string())? {
        Ok((lines, passed)) => {
            for line in lines {
                println!("{line}");
            }
            Ok(crate::util::gate_exit(passed))
        }
        Err(message) => {
            // Python's `SystemExit(message)` prints to stderr and exits 1.
            eprintln!("{message}");
            Ok(ExitCode::FAILURE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audit(markup: &str) -> (Vec<String>, bool) {
        let raw = format!("const STYLE: &str = r#\"\n.filters label{{color:#fff}}\n\"#;\n{markup}");
        report(&raw, "ui.rs").unwrap().unwrap()
    }

    #[test]
    fn a_wrapping_label_passes() {
        let (lines, passed) = audit("label { \"Kind\" input name=\"kind\"; }\n");
        assert!(passed, "{lines:?}");
        assert_eq!(lines[1], "1 `label` render sites checked.\n");
    }

    #[test]
    fn a_for_must_name_a_real_id() {
        let good = "label for=\"k\" { \"Kind\" }\ninput id=\"k\";\n";
        assert!(audit(good).1);
        let typo = "label for=\"typo\" { \"Kind\" }\ninput id=\"k\";\n";
        let (lines, passed) = audit(typo);
        assert!(!passed);
        assert!(lines[3].starts_with("  ui.rs:4  label has no"), "{lines:?}");
    }

    #[test]
    fn prose_and_text_do_not_count_as_a_control() {
        let markup = concat!(
            "label {\n",
            "    // an input; lives below\n",
            "    \"select one;\"\n",
            "    @let hint = \"textarea;\";\n",
            "    (hint)\n",
            "}\n",
            "input name=\"sibling\";\n",
        );
        let (lines, passed) = audit(markup);
        assert!(!passed, "{lines:?}");
        assert_eq!(lines[2], "BROKEN — 1:");
    }

    #[test]
    fn a_binding_or_identifier_is_not_a_label() {
        let markup = "let label = String::from(\"Paused\");\nlet event_label { }\n";
        let (lines, passed) = audit(markup);
        assert!(!passed);
        assert_eq!(lines[1], "0 `label` render sites checked.\n");
    }

    #[test]
    fn an_absent_style_block_is_reported() {
        assert!(report("fn main() {}", "ui.rs").unwrap().is_err());
    }

    #[test]
    fn comment_markers_inside_strings_survive() {
        let chars: Vec<char> = "a \"//x\" // gone\n/* b\n */c".chars().collect();
        let out: String = strip_comments(&chars).into_iter().collect();
        assert_eq!(out, "a \"//x\"        \n    \n   c");
    }

    #[test]
    fn an_unbalanced_block_is_an_error() {
        assert!(find_block_end("{ {", 0).is_err());
        assert_eq!(find_block_end("{ \"}\" }", 0).unwrap(), 7);
    }
}
