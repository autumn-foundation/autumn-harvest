//! The rules, applied to the comments of one file.

use std::fmt::Write as _;

use super::lexer::{Piece, extract_comments, is_doc};
use super::markdown::strip_containers;
use super::markers::untracked_marker;
use super::patterns::{
    ARCHAEOLOGY_RE, COMMENTED_CODE_RE, CONTRACTION_RE, MAX_SENTENCE_WORDS, NARRATIVE_RE, TODO_RE,
};
use super::pyre::{normalize_ws, split_ws, strip, truncate};
use super::runs::{comment_lines, comment_runs, prose_units};
use super::shapes::sentence_splits;
use super::spans::{
    blank_cells, blank_code_spans, blank_inline_code, blank_spans_across, mask_code_spans,
    mask_inline_code,
};

/// One finding of one rule.
#[derive(Debug, Clone)]
pub struct Finding {
    pub rule: &'static str,
    pub path: String,
    pub line: usize,
    pub text: String,
    /// What identifies this finding across edits.
    ///
    /// This is not the line number on purpose. The line number moves each
    /// time a line above it changes.
    pub key: String,
}

impl Finding {
    /// A finding whose key is its text.
    pub fn new(rule: &'static str, path: &str, line: usize, text: &str) -> Self {
        Self::keyed(rule, path, line, text, text)
    }

    /// A finding with a key of its own.
    pub fn keyed(rule: &'static str, path: &str, line: usize, text: &str, key: &str) -> Self {
        Self {
            rule,
            path: path.to_string(),
            line,
            text: text.to_string(),
            key: normalize_ws(key),
        }
    }

    /// The first twelve hex digits of SHA-256 over the rule and the key.
    pub fn fingerprint(&self) -> String {
        let mut input = Vec::with_capacity(self.rule.len() + 1 + self.key.len());
        input.extend_from_slice(self.rule.as_bytes());
        input.push(0);
        input.extend_from_slice(self.key.as_bytes());
        let digest = super::sha256::digest(&input);
        digest.iter().take(6).fold(String::new(), |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        })
    }
}

/// Blanks the code spans of one block, and its inline code in a doc comment.
///
/// The blanking reads the whole block, like the spans beside it. An
/// unclosed "<code>" runs to the end of the paragraph. So a marker on the
/// line under it renders as code too. A line-by-line blank left that marker
/// exposed and failed the build on it.
///
/// A table row is the one exception. Its cells are separate inline
/// contexts, so a backtick in one cannot pair with a backtick in the next.
/// Each row is its own block, so such a block is one line.
fn flush_block(lines: &[String], cells: &[Option<Vec<(usize, usize)>>], doc: bool) -> Vec<String> {
    let blanked = match (lines, cells) {
        ([line], [Some(cell_spans)]) if !cell_spans.is_empty() => {
            vec![blank_cells(line, cell_spans)]
        }
        _ => blank_spans_across(lines),
    };
    if !doc {
        return blanked;
    }
    blank_inline_code(&blanked.join("\n"))
        .split('\n')
        .map(str::to_string)
        .collect()
}

/// CH001, CH002, CH005 and CH006: the judgements of a single line.
#[allow(clippy::too_many_lines)]
fn check_line_rules(path: &str, pieces: &[&Piece]) -> Vec<Finding> {
    let mut findings = Vec::new();
    let mut lines = Vec::new();
    let mut spanless: Vec<String> = Vec::new();
    let mut follow: Vec<String> = Vec::new();
    let mut lead: Vec<String> = Vec::new();
    // Per run. A code span belongs to one comment. A join of the whole file
    // let an unmatched backtick in one comment pair with a backtick in an
    // unrelated one further down. That blanked everything between,
    // including the comments of the code between them, and an absolute rule
    // with them.
    for run in comment_runs(pieces) {
        let run_lines = comment_lines(&run);
        // Fenced lines become empty, and they are not dropped. So their
        // backticks cannot open a span over the prose after them, and each
        // index still lines up.
        //
        // And the run is cut where a block begins. A span belongs to one
        // block, not only to one comment. Two list items are two blocks. A
        // backtick paired across them blanked a marker that Rustdoc leaves
        // as ordinary text.
        let doc = is_doc(&run[0].marker);
        let mut block: Vec<String> = Vec::new();
        let mut block_cells: Vec<Option<Vec<(usize, usize)>>> = Vec::new();
        for line in &run_lines {
            if line.opens && !block.is_empty() {
                spanless.extend(flush_block(&block, &block_cells, doc));
                block.clear();
                block_cells.clear();
            }
            block.push(if line.in_fence {
                String::new()
            } else {
                line.text.clone()
            });
            block_cells.push(if line.in_fence {
                None
            } else {
                line.cells.clone()
            });
        }
        spanless.extend(flush_block(&block, &block_cells, doc));
        // The text of a marker can wrap, in either direction.
        // `TODO: implement the retry described in` over `#123` is one
        // tracked commitment. So is `#123` over
        // `TODO: remove the fallback`. That is the association that this tree
        // writes as `#123 - TODO: remove the fallback` on one line. A read of the line of the marker alone
        // failed the build on both.
        //
        // Neither carry decides anything. They supply the rest of the
        // comment, and the patterns that already read a line decide. The
        // forward `marker_span` cuts the text back to the sentence of the
        // marker. The backward `ADJACENT_REF_RE` still requires the
        // reference to touch the marker. That is why a line of prose before
        // the marker is safe to carry. "See #123 for the parser." over a
        // marker leaves "for the parser." between the two, and the reference
        // is still not the marker's own.
        //
        // Each carry stops where the text stops being the same comment: at a
        // block boundary, a fenced line, a blank line or another marker. The
        // last bound is not about sentences either. Each marker is reported
        // on its own line, so to carry past one would report it twice.
        let base = follow.len();
        for offset in 0..run_lines.len() {
            let mut carried: Vec<&str> = Vec::new();
            for probe in offset + 1..run_lines.len() {
                if run_lines[probe].in_fence || run_lines[probe].opens {
                    break;
                }
                let text = strip(&spanless[base + probe]);
                if text.is_empty() || TODO_RE.is_match(text) {
                    break;
                }
                carried.push(text);
            }
            follow.push(if carried.is_empty() {
                String::new()
            } else {
                format!(" {}", carried.join(" "))
            });
        }

        // The backward carry, built in one pass, not walked again for each
        // line. The bounds are the ones where the forward carry stops, read
        // from the other side. A line that opens a block has nothing above
        // it in that block. A fenced line, a blank line or a marker ends
        // what came before it.
        let mut carry = String::new();
        for (offset, line) in run_lines.iter().enumerate() {
            if line.opens {
                carry.clear();
            }
            lead.push(carry.clone());
            let text = strip(&spanless[base + offset]);
            if line.in_fence || text.is_empty() || TODO_RE.is_match(text) {
                carry.clear();
            } else {
                carry.push_str(text);
                carry.push(' ');
            }
        }
        lines.extend(run_lines);
    }

    for (index, line) in lines.iter().enumerate() {
        if line.in_fence {
            continue;
        }
        let stripped = strip(&line.text);
        if stripped.is_empty() {
            continue;
        }

        // CH001 is anchored, so it must see past any container marker. A
        // list or a quote is not a code fence, and the documented exemption
        // is the fence. "- let stale = compute();" is commented-out code
        // with a bullet in front of it. The removal has no container, so
        // only markers that CommonMark accepts at the left margin go.
        //
        // The text is blanked first, as for CH002 beside it. A code span can
        // wrap. So "Demonstrates `" over "let x = compute();" renders the
        // statement as <code>. That is an example of Rust, not Rust that was
        // commented out. To fail the build on it stops the example.
        let spanless_body = strip(&spanless[index]);
        let peeled = strip(strip_containers(spanless_body, &[], 0, false).0);
        let code_line = if peeled.is_empty() {
            spanless_body
        } else {
            peeled
        };
        if COMMENTED_CODE_RE.match_start(code_line).is_some() {
            findings.push(Finding::new("CH001", path, line.line, stripped));
        }

        // CH002 is anchored at the start of the line for its form with no
        // punctuation. So it needs the same removal as CH001. "- TODO fix
        // this" is a commitment with a bullet in front of it, and only a
        // fence exempts. The text is blanked first. A marker inside an
        // inline code span is the syntax under description, not a
        // commitment. CH005 and CH006 read the raw text on purpose, as the
        // known limitations say. But those are ratcheted, and this rule
        // fails the build outright.
        //
        // The reference is read from the blanked text too, not the raw line.
        // "The `#123` syntax" documents the shape of a marker. It does not
        // track the commitment beside it. A read of the raw line let it stand
        // in for one.
        let spanless_line = peeled;
        let marker_text = if spanless_line.is_empty() {
            spanless_body
        } else {
            spanless_line
        };
        if untracked_marker(&format!("{marker_text}{}", follow[index]), &lead[index]) {
            findings.push(Finding::new("CH002", path, line.line, stripped));
        }

        if ARCHAEOLOGY_RE.is_match(stripped) {
            findings.push(Finding::new("CH005", path, line.line, stripped));
        }

        if CONTRACTION_RE.is_match(stripped) {
            findings.push(Finding::new("CH006", path, line.line, stripped));
        }
    }
    findings
}

/// CH004: a comment run that opens or closes on an empty comment line.
///
/// Only leading `//` runs are judged. A trailing comment has no block to
/// have edges. A blank first line in the body of a `/* */` is conventional
/// format, not the editing artifact that this rule is about.
fn check_block_edges(path: &str, pieces: &[&Piece]) -> Vec<Finding> {
    let mut findings = Vec::new();
    for run in comment_runs(pieces) {
        if run[0].block || run[0].trailing {
            continue;
        }
        // An explicit `#[doc = ...]` is part of the same document. So a run
        // that meets one has not reached its edge. The blank doc line beside
        // it is the paragraph break between the two, and to delete it merges
        // them. A render with rustdoc 1.94.1 confirms this, both ways round.
        //
        // The kinds must agree. `//! One.` over a blank `//!` over
        // `#[doc = "Two."]` renders the module document as "One." alone,
        // because the outer attribute documents the next item. That blank
        // really is a trailing edge, and it stays reported.
        let kind = if run[0].marker == "//!" {
            "inner"
        } else {
            "outer"
        };
        if strip(&run[0].text).is_empty() && run[0].doc_above != kind {
            findings.push(Finding::new(
                "CH004",
                path,
                run[0].line,
                "block opens on an empty comment line",
            ));
        }
        let last = run[run.len() - 1];
        if run.len() > 1 && strip(&last.text).is_empty() && last.doc_below != kind {
            findings.push(Finding::new(
                "CH004",
                path,
                last.line,
                "block closes on an empty comment line",
            ));
        }
    }
    findings
}

/// The sentences of `unit`, each with its offset.
///
/// A plain split drops the offsets, and the offset maps a sentence back to
/// the line where it was written.
///
/// `boundaries` is where the full stops are looked for. A period inside a
/// code span is not a sentence end. Rustdoc renders "`foo. not sure why`"
/// as one span. A split there left a fragment that CH003 read as
/// deliberation. The sentence always comes from `unit`, so the text
/// reported and the words counted are the words of the author.
fn split_sentences<'u>(unit: &'u str, boundaries: &str) -> Vec<(usize, &'u str)> {
    let mut sentences = Vec::new();
    let mut start = 0;
    for (found_start, found_end) in sentence_splits(boundaries) {
        sentences.push((start, unit.get(start..found_start).unwrap_or("")));
        start = found_end;
    }
    sentences.push((start, unit.get(start..).unwrap_or("")));
    sentences
}

/// The source line of the fragment that covers `offset` in a joined unit.
fn line_of(spans: &[(usize, usize)], offset: usize) -> usize {
    let mut line = spans[0].1;
    for &(position, candidate) in spans {
        if position > offset {
            break;
        }
        line = candidate;
    }
    line
}

/// CH003 and CH007: both judge a whole sentence, not a wrapped line.
///
/// A match per line is wrong for these. Comment prose wraps in the middle
/// of a sentence. So a continuation line often begins with a word that is
/// a defect only at the start of a sentence. "... proves the append" over
/// "actually landed" is ordinary prose, not deliberation.
fn check_prose_rules(path: &str, pieces: &[&Piece]) -> Vec<Finding> {
    let mut findings = Vec::new();
    for unit in prose_units(pieces) {
        let mut boundaries = mask_code_spans(&unit.text);
        if unit.doc {
            boundaries = mask_inline_code(&boundaries);
        }
        for (offset, sentence) in split_sentences(&unit.text, &boundaries) {
            let sentence = strip(sentence);
            if sentence.is_empty() {
                continue;
            }
            let lineno = line_of(&unit.spans, offset);
            // Blanked for CH003, as CH002 blanks them, for the same reason.
            // Both are absolute. A comment that documents a contraction
            // token documents a literal. To fail the build on it stops the
            // documentation.
            // CH006 still reads the raw text. It is ratcheted, and the known
            // limitations record that choice.
            let mut spanless = blank_code_spans(sentence);
            if unit.doc {
                spanless = blank_inline_code(&spanless);
            }
            if NARRATIVE_RE.is_match(&spanless) {
                findings.push(Finding::new("CH003", path, lineno, truncate(sentence, 100)));
            }
            // Counted over the masked copy, so a code span is one token, not
            // as many tokens as its spaces make. "/// Use `<a 26-token
            // command>` here." is a two-word sentence with a literal in it.
            // A count of the tokens of the literal reported it as 28 words.
            // Then it failed the Tier B ratchet on documentation that says
            // almost nothing.
            //
            // The mask is the one that finds sentence boundaries. It fills a
            // span to its own width. So the span stays as one token, and it
            // does not vanish as it does in the blanked copy.
            let masked = if unit.doc {
                mask_inline_code(&mask_code_spans(sentence))
            } else {
                mask_code_spans(sentence)
            };
            let words = split_ws(&masked).len();
            if words > MAX_SENTENCE_WORDS {
                findings.push(Finding::keyed(
                    "CH007",
                    path,
                    lineno,
                    &format!("{words} words: {}", truncate(sentence, 90)),
                    sentence,
                ));
            }
        }
    }
    findings
}

/// Each finding in `source`, which is the file at `path`.
pub fn findings_for_source(path: &str, source: &str) -> Vec<Finding> {
    let pieces = extract_comments(source);
    let refs: Vec<&Piece> = pieces.iter().collect();
    let mut findings = check_line_rules(path, &refs);
    findings.extend(check_block_edges(path, &refs));
    findings.extend(check_prose_rules(path, &refs));
    findings
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(source: &str) -> Vec<(&'static str, usize)> {
        let mut found: Vec<_> = findings_for_source("t.rs", source)
            .iter()
            .map(|finding| (finding.rule, finding.line))
            .collect();
        found.sort_unstable();
        found.dedup();
        found
    }

    #[test]
    fn a_trailing_todo_is_gated() {
        assert_eq!(rules("let n = 1; // TODO: fix\n"), vec![("CH002", 1)]);
    }

    #[test]
    fn a_long_sentence_is_counted_per_sentence() {
        let words: Vec<String> = (1..=26).map(|n| format!("word{n}")).collect();
        let long = format!("// {}.\n", words.join(" "));
        assert_eq!(rules(&long), vec![("CH007", 1)]);
        let split = format!("// {}. {}.\n", words[..13].join(" "), words[13..].join(" "));
        assert_eq!(rules(&split), Vec::new());
    }

    #[test]
    fn the_fingerprint_is_the_rule_and_the_normalized_key() {
        let one = Finding::new("CH006", "a.rs", 1, "it isn't  so");
        let two = Finding::new("CH006", "b.rs", 9, "it isn't so");
        assert_eq!(one.fingerprint(), two.fingerprint());
        assert_eq!(one.fingerprint().len(), 12);
    }

    #[test]
    fn sentences_keep_their_offsets() {
        let unit = "One two. Three four.";
        let found = split_sentences(unit, unit);
        assert_eq!(found, vec![(0, "One two."), (9, "Three four.")]);
    }
}
