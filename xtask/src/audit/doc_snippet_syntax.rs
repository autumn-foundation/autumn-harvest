//! `cargo xtask audit doc-snippet-syntax`: Rust syntax check for the getting-started code blocks.
//!
//! This is the Folio corpus harness, ported from `docs/audits/doc-snippet-syntax.py`.
//! It is deterministic and reproducible on any checkout. It needs no network access.
//! It uses only `rustfmt`, which ships with the toolchain of this workspace. It
//! never runs `cargo`, never uses the network and never touches `target/`.
//!
//! # What this checks, and why
//!
//! `docs/getting-started/*.md` and `README.md` are the front door. Chapter 1
//! promises "no database, no Docker, and nothing to configure" for the fastest
//! path. Each later chapter has rust fences, and a reader on that path copies
//! them into an editor. Before this audit, nothing in the corpus checked that
//! those fences are even valid Rust. The other audits check links, config and
//! CLI drift, and comment hygiene. None of them looks inside a rust fence (see
//! the table of `docs/audits/README.md`). A snippet that does not parse breaks
//! the first thing a reader tries: pasting it.
//!
//! This is a SYNTAX check, not a compile check. It does not resolve `use`
//! imports. It does not know whether a type exists. It does not run the borrow
//! checker. That needs a real `cargo build` against a scratch crate for each
//! combination of chapter-local context. Examples are imports from two
//! chapters earlier, and `autumn-web` against connector-feature dependencies
//! for chapters 1, 12 and 13. The audits of this directory are deliberately
//! not network-heavy or compile-heavy (see "no network access required" in
//! `docs/audits/README.md`). A bare parse failure is still a real, mechanical
//! defect class. The lexer or parser of `rustc` rejects the snippet before any
//! question of imports or types. So it is a strict subset of "fails to
//! compile". Missing `use` lines and undefined example types give no false
//! positive. The cost is that type errors go uncaught.
//!
//! # How a block is checked
//!
//! The extraction reads lines, not one multi-line regex. A fence is a line
//! whose content opens with three backticks and `rust`, however far a list
//! item indents it. The close is the next line whose content is exactly three
//! backticks. An opener with no close before the end of the file is a hard
//! error. An unmatched fence that drops silently shrinks the corpus count
//! without a signal. A `rust,ignore` info string does NOT exempt a block. Only
//! the entries of `KNOWN_FRAGMENT_EXCEPTIONS` do. `ignore` marks a block that
//! `rustdoc` skips, not a block whose syntax stopped mattering.
//!
//! Each rust block becomes the body of its own function,
//! `fn __snippet() { <block content> }`. It goes to `rustfmt --emit stdout`
//! **on its own**: one process for each block, not one for the whole corpus.
//! A single shared buffer was tried first and dropped. `rustfmt` reports a
//! parse error where its recovery lands. For an unclosed brace or string, that
//! can be past the block that caused it, even past the end of the buffer.
//! The error then went to the block whose header line was closest before the
//! error line. So the fault went to a later block, even an excepted one, and
//! was dropped. One block per process is the simplest fix that cannot
//! misattribute. Only one block is in scope, so each diagnostic is its own.
//!
//! The wrapper is a function body, not a sequence of top-level items, on
//! purpose. A function body accepts local items, such as `#[workflow] async fn
//! ...`, `struct ...` and `use ...`. It also accepts bare statements and
//! expressions in the same scope. So one wrapper takes a complete workflow
//! definition and a short expression, with no need to classify the block
//! first. `rustfmt` fails on a real parse error, such as `error: expected
//! expression, found ...`. It exits 0 on valid syntax, formatted or not. This
//! audit only cares about the former.
//!
//! # Known, documented exceptions
//!
//! A few blocks are deliberately not complete, parseable Rust. Prose such as
//! "add this line to your existing builder chain" shows a bare `.method(...)`
//! continuation with no receiver. That is correct technical writing, not a
//! defect. Each one is in `KNOWN_FRAGMENT_EXCEPTIONS` with its reason. The key
//! is a stable prefix of the first content line of the block, not a line
//! number. So an edit elsewhere in the file does not silently end the cover.
//! An excepted block is still counted and shown in the summary as excluded,
//! never dropped silently.
//!
//! # Usage
//!
//! ```sh
//! cargo xtask audit doc-snippet-syntax
//! ```
//!
//! The exit code is 1 if a block that is not excepted fails to parse, and 0
//! otherwise. It is safe as a CI gate, or as a standalone report.

use std::fmt::Write as _;
use std::path::Path;
use std::process::ExitCode;

use regex::{Captures, Regex};
use smop::prelude::*;

use super::corpus_link_check::pycompat;
use crate::util;

/// Arguments of `cargo xtask audit doc-snippet-syntax`.
#[derive(Debug, Args)]
pub struct Args {}

/// The files that the audit reads: glob patterns relative to the repository root.
const TARGET_GLOBS: [(&str, &str); 2] = [("docs/getting-started", "*.md"), ("", "README.md")];

// LEAD_INDENT is the plain indent of the fence. It is consumed exactly ONCE,
// with or without a list marker after it. Two separate 3-character budgets,
// one before an optional marker and one after, stack into 6 characters for a
// plain line. That is past the real CommonMark cap for one container.
//
// BQ is one level of blockquote marker (">") and its trailing whitespace. It
// repeats for each "> " of a nested blockquote. It is captured with
// LEAD_INDENT, so the same text can be stripped from the content lines of the
// block. The "> " of a blockquote is Markdown syntax, not Rust source. Left in
// place, it breaks the parse of each blockquoted snippet, valid or not.
//
// The trailing run of BQ is capped at 4 characters, not 3. CommonMark consumes
// at most ONE character right after ">" as the optional padding of the marker.
// That is deterministic when present, not a choice. Only what follows it is the
// inner content, with the usual 3-column fence budget from there. So 1 padding
// and 3 budget give up to 4 columns after ">" for a valid fence. A cap of 3
// rejects a legitimate "> " followed by 3 more columns of indent.
// `valid_bq_padding` does the real budget check. It peels off the 1 padding
// character, then checks the rest against 3 columns. The regex only needs to
// admit enough characters for that check.
//
// LEAD_INDENT is capped at 3 COLUMNS, not 3 characters. So is what
// `valid_bq_padding` checks after the padding. A single leading tab expands to
// a 4-column stop and exceeds the budget alone. `{0,3}` counts characters, so
// it does not catch that. `cargo xtask audit comment-hygiene` applies the same
// CommonMark rule. Its fence delimiter check expands tabs to 4 columns. One
// of its fixtures is "four spaces is an indented code line, not a fence
// opener". Another is "an over-indented fence marker inside a fence is
// content, not a closer".
// An indent past the budget of the container makes a delimiter-like line
// indented CONTENT, not a fence opener or closer. The regexes bound each run
// to a character count only. A plain run without tabs in that count is always
// within budget. `valid_indent` and `valid_bq_padding` also reject a run whose
// tab-expanded width is over budget, because a regex cannot do that arithmetic.
const LEAD_INDENT: &str = r"[ \t]{0,3}";
const BQ: &str = r">[ \t]{0,4}";

// LIST_MARKER lets a fence be the first block of a list item. It opens on the
// line of the marker ("- ```rust", "1. ```rust"), not on a line under it. That
// is valid CommonMark. The marker text does not repeat on later lines, so
// there is nothing to strip from content lines. The plain indent stripping
// covers those lines. A marker is never legal on a CLOSING fence line. A
// closer holds only the indent and blockquote markers of its container and
// the delimiter. A line like "- ```" inside an open block is literal content,
// not a closer. An ordered marker is 1 to 9 ASCII digits, the CommonMark
// limit, then "." or ")". A ten-digit run like "1234567890." is no marker. It
// is prose that starts with digits. The class is `[0-9]`, not `\d`, so a
// Unicode digit does not qualify.
//
// This is not a full CommonMark container parser on purpose. The Python
// original stayed dependency-free, because the audits ran with no network
// access and could install no markdown package. A hand-written line scanner
// stays within that. The trade has three known gaps.
//
// 1. A fence on a CONTINUATION line of a list item needs the content column of
//    that item from the line of its marker. This scanner has no container stack
//    across lines. So it knows the width of a marker only when the marker is on
//    the opening line of the fence. That needs state from lines OUTSIDE the
//    span of the fence.
// 2. Whether an ordered marker other than "1." opens a list depends on whether
//    it interrupts an open paragraph. This scanner tracks no paragraph state.
//    So it accepts each marker that matches LIST_MARKER, whatever comes before.
//    That also needs state from lines outside the span of the fence.
// 3. Containers can nest in ANY order. "> - ```rust", a list inside a
//    blockquote, is as valid as "- > ```rust", a blockquote inside a list.
//    This scanner handles the second one. The opener pattern puts list markers
//    before blockquote markers (group 2, then group 3). That is local to the
//    opening line, unlike gaps 1 and 2. But any interleaving needs an ordered
//    token sequence instead of "all list markers, then all blockquote
//    markers". Each later piece that assumes that fixed shape must then walk
//    the same sequence: `blockquote_depth`, `fence_indent_valid` and the
//    content-line stripping in `find_rust_blocks`. That rewrites the core
//    model of this scanner. It is not a local fix.
//
// All three are absent from the corpus (grepped). One search was for a wide
// marker before an indented fence. One was for an ordered marker other than
// "1." near a fence. One was for a blockquote marker, then a list marker, then
// a fence. All
// three came up empty. If one shows up for real, extend the scanner here. The
// likely way is a container stack, as the fence delimiter and container
// stripping of `comment-hygiene` use for Rust doc comments.
//
// The list marker is group 2, apart from the indent and blockquote text in
// groups 1 and 3. Only the latter repeats on each content line of the
// container. "- " never appears after the opening line, but "> " does, on
// each line of a blockquote. A strip of the marker from content lines is
// wrong. Without a strip of the indent and blockquote text, Markdown syntax
// stays in the Rust source that goes to `rustfmt`.
//
// The trailing whitespace of a marker is unbounded in the regex. CommonMark
// counts only 1 to 4 columns of it as padding, absorbed into the content
// column. With 5 or more, only ONE column separates the marker from the
// content, as `comment-hygiene` computes it. That can only make a same-line
// fence invalid, never valid. What is left after that column is the indent of
// the delimiter from the real content start. 5 padding columns minus the 1
// that counts is already 4, past the 3-column budget. So
// `list_marker_padding_valid` requires the whole run to be 1 to 4 columns.
// Nothing past 4 can pass the indent budget anyway.
//
// The delimiter (group 4) is 3 or more backticks or tildes, as in CommonMark.
// A closing fence uses the same character, with a run at least as long as the
// opener. So a run of 4 is captured and compared, not matched as exactly 3.
// Group 5 is the raw info string. The code checks it, so the regex does not
// hold a literal "rust". A fence of another language is still a fence: its
// span is tracked and skipped whole. An invisible non-Rust fence can hold a
// line that looks like a Rust fence opener, as Markdown-about-Markdown does.
// That line then reads as a real opener.
//
// A fence can open inside SEVERAL list containers on one line. "- 1. ```rust"
// is a bullet item that holds an ordered item that holds the fence. Both
// containers open together, as a `comment-hygiene` fixture says: "two list
// markers on one line both open containers for the fence". MARKER_GLYPH and
// LIST_MARKER are separate, so the repeated group can match any number of
// them. Each padding is still checked on its own afterwards. This needs no
// state beyond the current line, unlike the gaps above, so it is in scope.
const MARKER_GLYPH: &str = r"(?:[-*+]|[0-9]{1,9}[.)])";

/// The only edition that this corpus tells a reader to use.
///
/// The `Cargo.toml` block of chapter 1 pins `edition = "2021"` for the tutorial
/// project. Each later chapter, and `README.md`, builds on it. A check against
/// another edition risks false positives and false negatives. Valid 2021 code
/// that uses a word reserved in 2024, such as `gen`, is rejected. A snippet
/// valid only in 2024 passes for readers on the documented 2021 setup.
const RUST_EDITION: &str = "2021";

/// Blocks that are deliberately not complete Rust, as `(file, first line, reason)`.
///
/// The second field matches the first non-blank, non-comment line of the block,
/// stripped. Each entry was checked against a baseline run before it was added.
const KNOWN_FRAGMENT_EXCEPTIONS: [(&str, &str, &str); 3] = [
    (
        "docs/getting-started/05-child-workflows.md",
        ".workflows(workflows![checkout, issue_invoice])",
        "deliberate partial excerpt: \"add this line to your existing builder chain\", not a standalone statement",
    ),
    (
        "docs/getting-started/10-operations.md",
        ".workflows(vec![",
        "deliberate partial excerpt: \"the same builder is available without the macro\", a continuation line, not a standalone statement",
    ),
    (
        "docs/getting-started/13-broker-connectors.md",
        ".map_json(|ctx, event: Reading| {",
        "deliberate partial excerpt: \"the key is on the mapping context\", a continuation line, not a standalone statement",
    ),
];

/// The compiled patterns of the scanner.
struct Patterns {
    fence_open: Regex,
    fence_close: Regex,
    marker_padding: Regex,
    quote_prefix: Vec<Regex>,
}

impl Patterns {
    fn new() -> Result<Self> {
        let list_marker = format!(r"{MARKER_GLYPH}[ \t]+");
        Ok(Self {
            fence_open: pycompat::regex(&format!(
                r"^({LEAD_INDENT})((?:{list_marker})*)((?:{BQ})*)(`{{3,}}|~{{3,}})(.*)$"
            ))?,
            fence_close: pycompat::regex(&format!(
                r"^({LEAD_INDENT}(?:{BQ})*)(`{{3,}}|~{{3,}})\s*$"
            ))?,
            marker_padding: pycompat::regex(&format!(r"({MARKER_GLYPH})(\s+)"))?,
            quote_prefix: Vec::new(),
        })
    }

    /// Returns the pattern for at least `depth` levels of blockquote marker.
    fn quote_prefix(&mut self, depth: usize) -> Result<&Regex> {
        while self.quote_prefix.len() <= depth {
            let n = self.quote_prefix.len();
            self.quote_prefix.push(pycompat::regex(&format!(
                r"^{LEAD_INDENT}(?:{BQ}){{{n},}}"
            ))?);
        }
        Ok(&self.quote_prefix[depth])
    }
}

/// Returns the absolute column after `text`, from `start_col`, with tab stops of 4.
///
/// Python `str.expandtabs` always measures as if its input starts at column 0.
/// The width of a tab depends on its real column. A tab at column 3 reaches
/// column 4, and one at column 4 reaches column 8. So a substring expanded on
/// its own is measured wrong unless it truly begins the line.
fn expand_column(text: &str, start_col: usize) -> usize {
    text.chars().fold(start_col, |col, ch| {
        if ch == '\t' {
            col + 4 - (col % 4)
        } else {
            col + 1
        }
    })
}

/// Reports whether pure indent `text` fits the 3-column fence budget.
///
/// Tabs expand to 4-column stops. A character count, which is all that `{0,3}`
/// gives the regex, undercounts a tab. One tab reaches the next stop and can
/// exceed the budget alone.
fn valid_indent(text: &str) -> bool {
    expand_column(text, 0) <= 3
}

/// Reports whether the whitespace right after one ">" is within budget.
///
/// Up to 4 characters can be valid here, unlike a bare lead indent. The first
/// one, if present, is the optional padding of the blockquote marker.
/// `CommonMark` consumes it as part of the marker, not as indent. Only the rest
/// counts against the usual 3-column budget.
fn valid_bq_padding(text: &str) -> bool {
    let mut chars = text.chars();
    chars.next().is_none() || valid_indent(chars.as_str())
}

/// Reports whether each indent run of a captured fence prefix is within budget.
///
/// The lead indent before any ">" has a flat 3-column budget. The trailing
/// indent of each ">" has up to 4, one of which can be the marker padding. A
/// split on ">" finds each run, for the lead and trail of an opener and for
/// the single group of a closer.
fn fence_indent_valid(prefix: &str) -> bool {
    let mut chunks = prefix.split('>');
    chunks.next().is_none_or(valid_indent) && chunks.all(valid_bq_padding)
}

/// Reports whether each list marker of a marker run has 1 to 4 columns of padding.
///
/// Several markers can stack on one line, as in "- 1. ". `CommonMark` checks the
/// allowance of each container on its own, because each one opens on its own.
/// `start_col` is the real column where the marker text starts, after any lead
/// indent. A glyph is never a tab, so it advances the column by its length.
/// The padding after it is measured from that running position. A padding
/// substring measured alone resets the tab math to column 0 and can
/// undercount a tab. An empty `marker` is trivially fine.
fn list_marker_padding_valid(pats: &Patterns, marker: &str, start_col: usize) -> bool {
    let mut col = start_col;
    for caps in pats.marker_padding.captures_iter(marker) {
        col += group(&caps, 1).chars().count();
        let end_col = expand_column(group(&caps, 2), col);
        if !(1..=4).contains(&(end_col - col)) {
            return false;
        }
        col = end_col;
    }
    true
}

/// Returns the text of capture group `i`, or an empty string.
fn group<'h>(caps: &Captures<'h>, i: usize) -> &'h str {
    caps.get(i).map_or("", |m| m.as_str())
}

/// Counts the ">" markers of a captured fence prefix.
///
/// The count tells a closing fence at the depth of the opener from one deeper
/// or shallower. An example is a plain `` ``` `` after a `` > ```rust `` opener,
/// which does not close it. A list marker needs no such count. Unlike ">", it
/// does not repeat on each line of the container, so it never appears on a
/// closing line.
fn blockquote_depth(prefix: &str) -> usize {
    prefix.matches('>').count()
}

/// Reports whether `line` still has at least `depth` levels of blockquote marker.
///
/// `CommonMark` has no lazy continuation inside a fenced code block, unlike a
/// paragraph. A line that drops the ">" ends the blockquote there, and each
/// fence open inside it. It does not merely leave the closer unfound many
/// lines later. A depth of 0, with no blockquote, is always present.
fn quote_prefix_present(pats: &mut Patterns, line: &str, depth: usize) -> Result<bool> {
    if depth == 0 {
        return Ok(true);
    }
    Ok(pats.quote_prefix(depth)?.is_match(line))
}

/// Reports whether `line` still reaches the content column of the list item.
///
/// `marker_width` is that column. It is 0 when the fence did not open on the
/// line of a marker, and then the check always passes. A blank line passes
/// too, unlike in the blockquote check. The `comment-hygiene` fixtures give
/// each container its own answer. One is "an unclosed fence ends when its
/// block quote does". Another is "a blank line does not end the list item a
/// fence sits in". `CommonMark` separates the blocks of a list item with blank lines, as
/// at the top level. So a blank line inside an open fence is fence content,
/// not a dedent out of the item.
fn list_prefix_present(line: &str, marker_width: usize) -> bool {
    if pycompat::strip(line).is_empty() {
        return true;
    }
    let mut col = 0;
    for ch in line.chars() {
        if !pycompat::is_space(ch) {
            break;
        }
        col = expand_column(ch.encode_utf8(&mut [0u8; 4]), col);
    }
    col >= marker_width
}

/// Strips `depth` levels of blockquote marker from the start of `line`.
///
/// The strip follows the structure, not the text. Each level is leading
/// spaces or tabs, a ">", and at most one space or tab after it. That is the
/// `CommonMark` grammar of a marker, and it allows any spelling. A comparison
/// with the exact spelling of the opener misses that. `` > > ```rust `` opens the
/// same depth-2 blockquote as `` >> ```rust ``. A body line spelled the other way
/// shares no leading text with its opener.
fn strip_blockquote(line: &str, depth: usize) -> &str {
    let mut line = line;
    for _ in 0..depth {
        let stripped = line.trim_start_matches([' ', '\t']);
        let Some(rest) = stripped.strip_prefix('>') else {
            break;
        };
        line = rest
            .strip_prefix(' ')
            .or_else(|| rest.strip_prefix('\t'))
            .unwrap_or(rest);
    }
    line
}

/// Strips leading whitespace from `line`, up to `width` COLUMNS, not characters.
///
/// A fence on the line of its list marker (`` - ```rust ``) leaves no marker text
/// on later lines. `CommonMark` still indents those lines by the display width
/// of the marker ("- " is 2 columns) to keep them in the list item. That is
/// alignment, not Rust source. A character count strips too little when the
/// marker holds a tab, because its width in columns is more than its length.
/// The strip stops at the first non-whitespace character, or once `width`
/// columns are consumed. So a shorter line or a blank line stays intact.
fn strip_marker_width(line: &str, width: usize) -> &str {
    let mut col = 0;
    let mut cut = 0;
    for (i, ch) in line.char_indices() {
        if col >= width || !pycompat::is_space(ch) {
            break;
        }
        col = expand_column(ch.encode_utf8(&mut [0u8; 4]), col);
        cut = i + ch.len_utf8();
    }
    &line[cut..]
}

/// Returns the first non-blank line of `code` that is not a `//` comment, stripped.
fn first_content_line(code: &str) -> &str {
    code.split('\n')
        .map(pycompat::strip)
        .find(|s| !s.is_empty() && !s.starts_with("//"))
        .unwrap_or("")
}

/// Returns the code of each rust fenced block in `text`, in order.
///
/// The scan reads lines, so an indented or blockquoted fence is still found.
/// That is a code block under a list item or a "> " note. A single-regex scan
/// anchored on an unindented close drops those silently.
///
/// The display width of a list marker ("10. " is 4 columns, "- " is 2) is
/// stripped from each line of its container, the closer too. That happens
/// before the line meets the 3-column budget or the blockquote depth match.
/// The budget is relative to the content column of the item, not to column 0,
/// per `CommonMark`. Then `depth` levels of blockquote marker are stripped from
/// each content line, by structure. A nested `` > > ```rust `` and a body written
/// as ">> let x = 1;" are the same depth-2 blockquote with no common prefix.
///
/// A closing fence ends the block only at the same blockquote depth. It must
/// use the same delimiter character, with a run at least as long as the opener.
/// Otherwise an unrelated fence closes this one and swallows each line up to
/// it as content. That fence can be at a different depth, or a bare `` ``` ``
/// that closes some other block with a longer delimiter.
///
/// Each fence is tracked this way, Rust or not. Once a fence is open, only its
/// own closer ends it. No other fence opens inside it. So a line that looks
/// like a nested rust opener, inside an outer fence that shows Markdown as a
/// literal example, is inert content. A backtick opener whose info string
/// holds a backtick is not a fence at all, per `CommonMark`. It is ambiguous
/// with an inline code span. It is skipped as plain content and opens nothing.
/// So a real rust fence a few lines down is not read as its content.
///
/// The result is an error message if a fence has no close before the end of
/// the file. A block dropped that way shrinks the corpus count with no signal.
/// So does one whose real close was skipped for a depth or delimiter mismatch
/// with nothing compatible after it.
fn find_rust_blocks(
    pats: &mut Patterns,
    text: &str,
    relpath: &str,
) -> Result<std::result::Result<Vec<String>, String>> {
    let mut lines: Vec<&str> = text.split('\n').collect();
    if lines.last() == Some(&"") {
        // A file that ends in "\n" splits into an empty last element that is
        // not a real line. Left in, the container-end check reads it as a
        // blank line that leaves an open blockquote. That closes a fence that
        // was never closed, right at the end of the file. It defeats the
        // unmatched-fence error that this function exists to raise.
        lines.pop();
    }
    let mut blocks = Vec::new();
    let n = lines.len();
    let mut i = 0;
    while i < n {
        let Some(caps) = pats.fence_open.captures(lines[i]) else {
            i += 1;
            continue;
        };
        let (lead, marker, trail, delim, info) = (
            group(&caps, 1),
            group(&caps, 2),
            group(&caps, 3),
            group(&caps, 4),
            group(&caps, 5),
        );
        if !fence_indent_valid(&format!("{lead}{trail}"))
            || !list_marker_padding_valid(pats, marker, expand_column(lead, 0))
        {
            i += 1;
            continue;
        }
        let delim_char = delim.chars().next().unwrap_or('`');
        let delim_len = delim.len();
        if delim_char == '`' && info.contains('`') {
            i += 1;
            continue;
        }
        // The language is the first comma-separated word of the info string,
        // as in the rustdoc annotations "rust,ignore" and "rust,no_run". It is
        // an exact match, not a prefix. "rustfmt" and "rustic" start with
        // "rust" but are not the Rust tag. A block in one of those is not Rust
        // source that this audit checks.
        let is_rust =
            pycompat::strip(pycompat::strip(info).split(',').next().unwrap_or("")) == "rust";
        let depth = blockquote_depth(&format!("{lead}{trail}"));
        let marker_start_col = expand_column(lead, 0);
        // The absolute column of the marker content, not the marker width
        // alone. An indented marker ("   - ```rust") uses `marker_start_col`
        // columns before its own text. A later line must reach the lead indent
        // PLUS the marker width to stay in the item. The value is 0 with no
        // marker at all, because it also gates the list-specific checks.
        let marker_width = if marker.is_empty() {
            0
        } else {
            expand_column(marker, marker_start_col)
        };
        let start_line = i + 1;
        i += 1;
        let mut code_lines: Vec<&str> = Vec::new();
        let mut closed = false;
        while i < n {
            let raw = lines[i];
            if marker_width > 0 && !list_prefix_present(raw, marker_width) {
                // This is the idea of the blockquote check below, done first
                // and on the RAW line. A line below the content column of the
                // item ends the item, and each fence inside it. The strip of
                // the marker width makes no sense before that check.
                closed = true;
                break;
            }
            let line = strip_marker_width(raw, marker_width);
            if let Some(cm) = pats.fence_close.captures(line) {
                let (prefix, close) = (group(&cm, 1), group(&cm, 2));
                if fence_indent_valid(prefix)
                    && blockquote_depth(prefix) == depth
                    && close.starts_with(delim_char)
                    && close.len() >= delim_len
                {
                    closed = true;
                    i += 1;
                    break;
                }
            }
            if depth > 0 && !quote_prefix_present(pats, line, depth)? {
                // No lazy continuation inside a fenced code block. A line that
                // leaves the blockquote ends it, and the fence inside it, here.
                // `i` stays, so the next pass reads this line as a fresh opener.
                closed = true;
                break;
            }
            if is_rust {
                code_lines.push(strip_blockquote(line, depth));
            }
            i += 1;
        }
        if !closed {
            return Ok(Err(format!(
                "{relpath}: fence opened at line {start_line} has no closing delimiter before end of file"
            )));
        }
        if is_rust {
            blocks.push(code_lines.join("\n"));
        }
    }
    Ok(Ok(blocks))
}

/// Runs `rustfmt` on one wrapped block.
///
/// The result is `None` if the block parses. Otherwise it is the location and
/// the message of the first diagnostic.
fn check_block(code: &str) -> Result<Option<(String, String)>> {
    let wrapped = format!("fn __snippet() {{\n{code}\n}}\n");
    let dir = smop::fs::temp_dir()?;
    let path = dir.path().join("snippet.rs");
    smop::fs::write_string(&path, &wrapped)?;
    let output = sh::cmd("rustfmt")
        .args(["--edition", RUST_EDITION, "--emit", "stdout"])
        .arg(&path)
        .output_raw()?;
    if output.status.success() {
        return Ok(None);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let (mut msg, mut loc) = ("?".to_owned(), "?".to_owned());
    for line in pycompat::splitlines(&stderr) {
        let s = pycompat::strip(line);
        if s.starts_with("error") {
            s.clone_into(&mut msg);
        } else if s.starts_with("-->") {
            s.clone_into(&mut loc);
            break;
        }
    }
    Ok(Some((loc, msg)))
}

/// Returns the target files as sorted repository-relative paths.
///
/// A Python glob `*` skips names that start with a dot, so this does too.
fn collect_files(root: &Path) -> Vec<String> {
    let mut files = std::collections::BTreeSet::new();
    for (dir, pattern) in TARGET_GLOBS {
        let suffix = pattern.trim_start_matches('*');
        if pattern.starts_with('*') {
            let Ok(entries) = std::fs::read_dir(root.join(dir)) else {
                continue;
            };
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !name.starts_with('.') && name.ends_with(suffix) {
                    files.insert(format!("{dir}/{name}"));
                }
            }
        } else if std::fs::symlink_metadata(root.join(pattern)).is_ok() {
            files.insert(pattern.to_owned());
        }
    }
    files.into_iter().collect()
}

/// One failing block: the file, its first content line, the location and the message.
type Failure = (String, String, String, String);

/// The totals of one run.
struct Summary {
    files: usize,
    blocks: usize,
    excepted: usize,
    failures: Vec<Failure>,
}

/// A parse check: `None` for a clean block, else the location and the message.
type BlockCheck = dyn FnMut(&str) -> Result<Option<(String, String)>>;

/// Checks each target file under `root`, with `check` as the parse check.
fn check_files(
    root: &Path,
    check: &mut BlockCheck,
) -> Result<std::result::Result<Summary, String>> {
    let mut pats = Patterns::new()?;
    let files = collect_files(root);
    let mut summary = Summary {
        files: files.len(),
        blocks: 0,
        excepted: 0,
        failures: Vec::new(),
    };
    for relpath in &files {
        let text = pycompat::read_text(&root.join(relpath))?;
        let blocks = match find_rust_blocks(&mut pats, &text, relpath)? {
            Ok(blocks) => blocks,
            Err(message) => return Ok(Err(message)),
        };
        summary.blocks += blocks.len();
        for code in &blocks {
            let first = first_content_line(code);
            let excepted = KNOWN_FRAGMENT_EXCEPTIONS
                .iter()
                .any(|(file, line, _)| file == relpath && *line == first);
            if excepted {
                summary.excepted += 1;
                continue;
            }
            // Each block goes to its own `rustfmt` process. That guarantees
            // that a diagnostic never goes to another block than its own.
            if let Some((loc, msg)) = check(code)? {
                summary
                    .failures
                    .push((relpath.clone(), first.to_owned(), loc, msg));
            }
        }
    }
    Ok(Ok(summary))
}

/// Renders the summary as the Python script printed it.
fn render(summary: &Summary) -> String {
    let mut out = format!(
        "Folio doc-snippet syntax check — {} ```rust blocks across {} files ({} documented fragment exceptions, not parse-checked)\n\n",
        summary.blocks, summary.files, summary.excepted
    );
    if summary.failures.is_empty() {
        out += "All non-excepted blocks parse cleanly.\n";
        return out;
    }
    let _ = write!(
        out,
        "{} block(s) fail to parse as valid Rust syntax:\n\n",
        summary.failures.len()
    );
    for (relpath, first, loc, msg) in &summary.failures {
        let _ = writeln!(
            out,
            "  {relpath}  ({})\n    {msg}\n    {loc}",
            pycompat::repr(first)
        );
    }
    out
}

/// Runs `cargo xtask audit doc-snippet-syntax`.
pub fn run(_args: Args) -> Result<ExitCode> {
    let root = util::repo_root();
    match check_files(&root, &mut check_block)? {
        Err(message) => {
            eprintln!("{message}");
            Ok(ExitCode::FAILURE)
        }
        Ok(summary) => {
            print!("{}", render(&summary));
            Ok(util::gate_exit(summary.failures.is_empty()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocks(text: &str) -> std::result::Result<Vec<String>, String> {
        let mut pats = Patterns::new().unwrap();
        find_rust_blocks(&mut pats, text, "x.md").unwrap()
    }

    #[test]
    fn plain_and_annotated_rust_fences_are_found() {
        let text = "```rust\nlet a = 1;\n```\n```rust,ignore\nlet b = 2;\n```\n```rustfmt\nno\n```\n````rust\n```\nlet c;\n````\n";
        assert_eq!(
            blocks(text).unwrap(),
            ["let a = 1;", "let b = 2;", "```\nlet c;"]
        );
    }

    #[test]
    fn list_items_and_blockquotes_are_stripped() {
        let text = "- ```rust\n  let a = 1;\n  ```\n> > ```rust\n>> let b = 2;\n> > ```\n10. ```rust\n    let c = 3;\n    ```\n";
        assert_eq!(
            blocks(text).unwrap(),
            ["let a = 1;", "let b = 2;", "let c = 3;"]
        );
    }

    #[test]
    fn indent_budgets_use_tab_columns() {
        assert!(valid_indent("   "));
        assert!(!valid_indent("\t"));
        assert!(!valid_indent("    "));
        assert!(!valid_bq_padding(" \t"));
        assert!(valid_bq_padding("    "));
        assert!(!valid_bq_padding("     "));
        assert_eq!(expand_column("\t", 3), 4);
        assert_eq!(expand_column("a\t", 0), 4);
        let text = "\t```rust\nlet a;\n";
        assert_eq!(blocks(text).unwrap(), [] as [std::string::String; 0]);
    }

    #[test]
    fn marker_padding_is_one_to_four_columns() {
        let pats = Patterns::new().unwrap();
        assert!(list_marker_padding_valid(&pats, "- 1. ", 0));
        assert!(!list_marker_padding_valid(&pats, "-     ", 0));
        assert!(list_marker_padding_valid(&pats, "", 0));
        assert_eq!(
            blocks("-     ```rust\nx\n").unwrap(),
            [] as [std::string::String; 0]
        );
    }

    #[test]
    fn an_unclosed_fence_is_an_error() {
        let err = blocks("text\n```rust\nlet a;\n").unwrap_err();
        assert_eq!(
            err,
            "x.md: fence opened at line 2 has no closing delimiter before end of file"
        );
    }

    #[test]
    fn a_dedent_or_a_dropped_quote_ends_the_fence() {
        assert_eq!(blocks("- ```rust\n  a\nb\n").unwrap(), ["a"]);
        assert_eq!(blocks("> ```rust\n> a\nb\n").unwrap(), ["a"]);
        // A blank line stays inside the list item.
        assert_eq!(
            blocks("- ```rust\n  a\n\n  b\n  ```\n").unwrap(),
            ["a\n\nb"]
        );
    }

    #[test]
    fn a_backtick_info_string_is_no_fence() {
        assert_eq!(blocks("``` a`b\n```rust\nok\n```\n").unwrap(), ["ok"]);
    }

    #[test]
    fn the_first_content_line_skips_comments() {
        assert_eq!(first_content_line("\n  // note\n  .x(1)\n"), ".x(1)");
        assert_eq!(first_content_line("// only"), "");
    }

    #[test]
    fn a_fixture_run_counts_exceptions_and_failures() {
        let dir = smop::fs::temp_dir().unwrap();
        let gs = dir.path().join("docs/getting-started");
        std::fs::create_dir_all(&gs).unwrap();
        std::fs::write(
            gs.join("05-child-workflows.md"),
            "```rust\n.workflows(workflows![checkout, issue_invoice])\n```\n```rust\nlet 'x = ;\n```\n",
        )
        .unwrap();
        std::fs::write(gs.join(".hidden.md"), "```rust\nbad(\n```\n").unwrap();
        std::fs::write(dir.path().join("README.md"), "```rust\nlet ok = 1;\n```\n").unwrap();
        let mut fake = |code: &str| -> Result<Option<(String, String)>> {
            Ok(code
                .contains("'x")
                .then(|| ("--> f.rs:2:5".to_owned(), "error: bad".to_owned())))
        };
        let summary = check_files(dir.path(), &mut fake).unwrap().unwrap();
        assert_eq!(
            render(&summary),
            "Folio doc-snippet syntax check — 3 ```rust blocks across 2 files (1 documented fragment exceptions, not parse-checked)\n\n\
             1 block(s) fail to parse as valid Rust syntax:\n\n\
             \x20 docs/getting-started/05-child-workflows.md  (\"let 'x = ;\")\n    error: bad\n    --> f.rs:2:5\n"
        );
    }

    #[test]
    fn rustfmt_reports_a_parse_error() {
        if sh::cmd("rustfmt")
            .arg("--version")
            .success()
            .unwrap_or(false)
        {
            assert_eq!(check_block("let a = 1;").unwrap(), None);
            let (loc, msg) = check_block("let a = ;").unwrap().unwrap();
            assert!(msg.starts_with("error"), "{msg}");
            assert!(loc.starts_with("-->"), "{loc}");
        }
    }
}
