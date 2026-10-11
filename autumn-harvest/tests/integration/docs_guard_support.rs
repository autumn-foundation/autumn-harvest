//! Helpers shared by the docs guard suites.
//!
//! A docs guard reads a Markdown page and checks it as data. These helpers
//! cut a page into sections, split its prose into sentences, find issue
//! references, and find the CI step that runs a guard suite.
//!
//! Pure: no database, no async. The suites that use these helpers test them.

use std::collections::BTreeSet;

/// Words that end in a full stop but do not end a sentence.
const ABBREVIATIONS: &[&str] = &["e.g.", "i.e.", "etc.", "vs.", "cf."];

/// The text from `start` to the next `end` after it, or `None`.
pub fn section<'a>(doc: &'a str, start: &str, end: &str) -> Option<&'a str> {
    let body = &doc[doc.find(start)? + start.len()..];
    Some(&body[..body.find(end).unwrap_or(body.len())])
}

/// The workflow step whose text contains `needle`.
///
/// The stanza runs from the step's `- ` line to the next step or job. A key
/// written below `run:` is part of the stanza.
pub fn step_stanza<'a>(block: &'a str, needle: &str) -> Option<&'a str> {
    let at = block.find(needle)?;
    let start = block[..at].rfind("\n      - ")?;
    let end = [
        block[at..].find("\n      - "),
        block[at..]
            .find("\n  ")
            .filter(|i| !block[at + i..].starts_with("\n   ")),
    ]
    .into_iter()
    .flatten()
    .min()
    .map_or(block.len(), |i| at + i + 1);
    Some(&block[start..end])
}

/// Each way a step can stop running the full guard set.
///
/// The `run:` line must end with `filter`, so a narrower filter fails.
pub fn check_stanza(stanza: &str, filter: &str) -> Result<(), String> {
    if stanza.contains("\n        if:") {
        return Err("the guard step must not have an `if:` condition".into());
    }
    if stanza.contains("continue-on-error") {
        return Err("the guard step must not set `continue-on-error`".into());
    }
    let run = stanza
        .lines()
        .find(|line| line.trim_start().starts_with("run:"))
        .ok_or("the guard step must have a `run:` line")?;
    if !run.trim_end().ends_with(&format!("{filter}\"")) {
        return Err(format!("the `run:` line must end with `{filter}\"`"));
    }
    Ok(())
}

/// Issue numbers cited as `#NNN` or as a GitHub `issues/NNN` link.
///
/// An anchor such as `page.md#5-title` or `(#12-title)` is not a citation.
/// Neither is an HTML entity such as `&#8212;`.
pub fn issue_refs(text: &str) -> BTreeSet<u32> {
    let mut refs = BTreeSet::new();
    for (marker, after_marker_ok) in [("#", false), ("issues/", true)] {
        for (at, _) in text.match_indices(marker) {
            let before = text[..at].chars().next_back();
            let bad_before =
                before.is_some_and(|c| c.is_alphanumeric() || matches!(c, '&' | '#' | '/'));
            if bad_before && !after_marker_ok {
                continue;
            }
            let rest = &text[at + marker.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            let next = rest[digits.len()..].chars().next();
            let anchor = next.is_some_and(|c| c.is_alphanumeric() || matches!(c, '-' | '_' | ';'));
            if (1..=6).contains(&digits.len())
                && !anchor
                && let Ok(number) = digits.parse()
            {
                refs.insert(number);
            }
        }
    }
    refs
}

/// Prose sentences from paragraphs, list items, blockquotes and table cells.
///
/// Headings, code fences, HTML comments and table separator rows are not
/// prose. A link keeps its text and loses its target, so a URL does not count
/// as words. An inline code span counts as words but cannot end a sentence.
pub fn prose_sentences(text: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    let mut paragraph = String::new();
    let mut in_fence = false;
    let mut in_comment = false;
    for raw in text.lines() {
        let raw = raw.trim();
        if raw.starts_with("```") || raw.starts_with("~~~") {
            in_fence = !in_fence;
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        let line = mask_code_spans(raw);
        let mut trimmed = line.as_str();
        if trimmed.starts_with("<!--") {
            in_comment = true;
        }
        if in_fence || in_comment || is_heading(trimmed) || trimmed.starts_with("|-") {
            in_comment &= !trimmed.contains("-->");
            blocks.push(std::mem::take(&mut paragraph));
            continue;
        }
        if trimmed.starts_with('|') {
            blocks.push(std::mem::take(&mut paragraph));
            blocks.extend(trimmed.trim_matches('|').split('|').map(str::to_owned));
            continue;
        }
        while let Some(rest) = trimmed.strip_prefix('>') {
            trimmed = rest.trim_start();
        }
        let item = ["- ", "* ", "+ "]
            .iter()
            .find_map(|bullet| trimmed.strip_prefix(bullet))
            .or_else(|| list_number_body(trimmed));
        if trimmed.is_empty() || item.is_some() {
            blocks.push(std::mem::take(&mut paragraph));
        }
        paragraph.push(' ');
        paragraph.push_str(item.unwrap_or(trimmed));
    }
    blocks.push(paragraph);

    blocks
        .iter()
        .map(|block| strip_link_targets(block))
        .flat_map(|block| split_sentences(&block))
        .collect()
}

/// A Markdown heading starts with one to six `#` and a space.
fn is_heading(line: &str) -> bool {
    let hashes = line.bytes().take_while(|&b| b == b'#').count();
    (1..=6).contains(&hashes) && matches!(line.as_bytes().get(hashes), None | Some(b' '))
}

/// Replace marks inside each inline code span with `_`.
///
/// A masked span keeps its word count. Its full stops, brackets and pipes no
/// longer end a sentence, start a link or split a table cell.
fn mask_code_spans(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_code = false;
    for c in line.chars() {
        if c == '`' {
            in_code = !in_code;
        } else if in_code && matches!(c, '.' | '?' | '!' | '[' | ']' | '(' | ')' | '|') {
            out.push('_');
        } else {
            out.push(c);
        }
    }
    out
}

/// The body of a `1. item` line, or `None`.
fn list_number_body(line: &str) -> Option<&str> {
    let (number, body) = line.split_once(". ")?;
    (!number.is_empty() && number.bytes().all(|b| b.is_ascii_digit())).then_some(body)
}

/// Replace `[text](target)` with `text`. An unclosed target stays as text.
fn strip_link_targets(block: &str) -> String {
    let mut out = String::new();
    let mut rest = block;
    while let Some(open) = rest.find("](") {
        out.push_str(&rest[..open]);
        let after = &rest[open + 2..];
        let Some(close) = after.find(')') else {
            out.push(' ');
            rest = after;
            break;
        };
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    out.replace('[', "")
}

/// Split after a word that ends in `.`, `?` or `!`.
///
/// Closing emphasis, quote and bracket marks after the stop still end the
/// sentence, so `**Lead-in.**` is one sentence. An abbreviation does not end
/// a sentence.
fn split_sentences(block: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    for word in block.split_whitespace() {
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
        let bare = word.trim_end_matches(['*', '_', ')', '"', '\'']);
        let abbreviation = ABBREVIATIONS.contains(&bare.to_ascii_lowercase().as_str());
        if bare.ends_with(['.', '?', '!']) && !abbreviation {
            sentences.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        sentences.push(current);
    }
    sentences
}
