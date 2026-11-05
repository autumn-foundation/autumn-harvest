//! Inline code spans and inline `<code>` elements, found and blanked.
//!
//! CH002 is absolute and at zero. So a comment that documents the marker
//! syntax, such as "Parse the `TODO:` prefix", must not fail the build. A
//! span is blanked, not deleted, so each later offset still points at the
//! same column.
//!
//! A blank and a mask fill a span in two different ways, on purpose.
//!
//! A blank fills each character with one space, as the Python original did.
//! The block layer reads the blanked copy again, and it counts columns in
//! characters. So a span of "`\u{e9}`" must leave three columns, not four.
//! Otherwise a container after the span moves, and CH001 misses a line.
//!
//! A mask fills each byte with one `x`. A sentence boundary found in the
//! masked copy then cuts the real text at the same byte offset. A run of
//! `x` is one word whatever its length, so the count of words is the same.

use super::patterns::{BLANK_LINE_RE, CODE_CLOSE_RE, CODE_OPEN_RE};

/// Offsets of the characters that an odd run of backslashes escapes.
///
/// The parity is counted from left to right, because a look-behind cannot
/// count it. One backslash escapes the character after it. Two escape each
/// other, and the character after them acts as usual. Rustdoc renders
/// "\\\\`marker`" with the marker as code, and a one-character look-behind
/// refused that opener.
///
/// This ignores which character is escaped, on purpose. A backtick and an
/// inline "<code>" tag obey the same parity. Two scanners, one for each,
/// drifted apart.
pub fn escaped_offsets(text: &str) -> Vec<bool> {
    let bytes = text.as_bytes();
    let mut escaped = vec![false; bytes.len() + 1];
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            index += 1;
            continue;
        }
        let run = index;
        while index < bytes.len() && bytes[index] == b'\\' {
            index += 1;
        }
        if (index - run) % 2 == 1 && index < bytes.len() {
            escaped[index] = true;
            index += 1;
        }
    }
    escaped
}

/// The spans of each run of backticks.
fn backtick_runs(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut runs = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'`' {
            let start = index;
            while index < bytes.len() && bytes[index] == b'`' {
                index += 1;
            }
            runs.push((start, index));
        } else {
            index += 1;
        }
    }
    runs
}

/// Where the inline code spans of `text` are.
///
/// This is a scan, not one pattern, because the rule has three parts that
/// a single pattern got wrong. The delimiter runs of a span must be exactly
/// the same length. An escaped backtick cannot open a span. But inside an
/// open span there are no escapes at all. So "`\\`" is a span that holds
/// one backslash, as Rustdoc renders it. And a blank line ends the
/// paragraph, so no span reaches across one.
///
/// The escapes are resolved before the spans are found. The mask never
/// spans a delimiter that it did not consume, so the parity works out.
pub fn code_span_ranges(text: &str) -> Vec<(usize, usize)> {
    let escaped = escaped_offsets(text);
    let runs = backtick_runs(text);
    let mut ranges = Vec::new();
    let mut index = 0;
    while index < runs.len() {
        let (mut start, end) = runs[index];
        // An escape consumes exactly one backtick, not the run that it
        // opens. Rustdoc renders an escaped run of two backticks as one
        // literal tick and the opener of a one-tick span. To discard the
        // whole run left a marker inside that span open to an absolute rule.
        if escaped[start] {
            start += 1;
        }
        if start < end {
            for (probe, &closer) in runs.iter().enumerate().skip(index + 1) {
                if closer.1 - closer.0 != end - start {
                    continue;
                }
                if BLANK_LINE_RE.find_range(text, end, closer.0).is_some() {
                    break;
                }
                ranges.push((start, closer.1));
                index = probe;
                break;
            }
        }
        index += 1;
    }
    ranges
}

/// How a fill replaces one character of a span.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fill {
    /// One space for each character, so each column still lines up.
    Blank,
    /// One `x` for each byte, so each byte offset still lines up.
    Mask,
}

/// `text` with each character in `ranges` replaced by filler.
///
/// Newlines survive, so a text with several lines splits back into the
/// same lines.
fn fill_ranges(text: &str, ranges: &[(usize, usize)], fill: Fill) -> String {
    if ranges.is_empty() {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut next = 0;
    for (index, c) in text.char_indices() {
        while next < ranges.len() && ranges[next].1 <= index {
            next += 1;
        }
        let inside = next < ranges.len() && ranges[next].0 <= index && index < ranges[next].1;
        if inside && c != '\n' {
            match fill {
                Fill::Blank => out.push(' '),
                Fill::Mask => out.extend(std::iter::repeat_n('x', c.len_utf8())),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `text` with each code span replaced by `filler`.
fn replace_spans(text: &str, fill: Fill) -> String {
    let mut ranges = code_span_ranges(text);
    ranges.sort_unstable();
    fill_ranges(text, &ranges, fill)
}

/// Where the inline `<code>` elements of `text` are.
///
/// Rustdoc renders "<code>not sure why</code>" in a `<code>` element, as it
/// does a backtick span. So the absolute rules must not read a narrative
/// phrase or a marker inside one.
///
/// This is a scan, not one pattern, for two reasons. An escaped tag is not
/// a tag. Rustdoc renders "\\<code>TODO: x" as literal characters, so the
/// marker inside it is a real commitment.
///
/// And an element that does not close runs to the end of the block. That
/// is the one place where this differs from a backtick span. An unmatched
/// backtick opens nothing. An unmatched "<code>" opens an element that the
/// close tag of the paragraph ends. So "<code>a" renders each later word of
/// that paragraph as code. Callers pass one block, so the end of `text` is
/// the end of the block.
pub fn inline_code_ranges(text: &str) -> Vec<(usize, usize)> {
    let escaped = escaped_offsets(text);
    let unescaped = |pattern: &super::pyre::Re, start: usize| {
        let mut found = pattern.find_from(text, start);
        while let Some((at, _)) = found {
            if !escaped[at] {
                break;
            }
            found = pattern.find_from(text, at + 1);
        }
        found
    };
    let mut ranges = Vec::new();
    let mut index = 0;
    loop {
        let Some(opener) = unescaped(&CODE_OPEN_RE, index) else {
            return ranges;
        };
        // HTML nests, so the elements nest. Rustdoc renders
        // `<code>outer <code>inner</code> TODO: x</code>` with the marker
        // still inside the outer element. To pair the outer opener with the
        // inner closer exposed the marker and failed the build. The element
        // ends at the closer that brings the depth back to zero.
        let mut depth = 1;
        let mut cursor = opener.1;
        let mut end = text.len();
        loop {
            let closer = unescaped(&CODE_CLOSE_RE, cursor);
            let nested = unescaped(&CODE_OPEN_RE, cursor);
            if let Some(nested) = nested
                && closer.is_none_or(|closer| nested.0 < closer.0)
            {
                depth += 1;
                cursor = nested.1;
                continue;
            }
            let Some(closer) = closer else {
                break;
            };
            depth -= 1;
            cursor = closer.1;
            if depth == 0 {
                end = closer.1;
                break;
            }
        }
        ranges.push((opener.0, end));
        if end == text.len() {
            return ranges;
        }
        index = end;
    }
}

/// `text` with each inline `<code>` element replaced by `filler`.
fn replace_inline_code(text: &str, fill: Fill) -> String {
    let mut ranges = inline_code_ranges(text);
    ranges.sort_unstable();
    merge_overlaps(&mut ranges);
    fill_ranges(text, &ranges, fill)
}

/// Joins ranges that overlap, so the fill walks them in one pass.
fn merge_overlaps(ranges: &mut Vec<(usize, usize)>) {
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(ranges.len());
    for &(start, end) in ranges.iter() {
        if let Some(last) = merged.last_mut()
            && start <= last.1
        {
            last.1 = last.1.max(end);
            continue;
        }
        merged.push((start, end));
    }
    *ranges = merged;
}

/// `text` with each inline `<code>` element replaced by spaces.
pub fn blank_inline_code(text: &str) -> String {
    replace_inline_code(text, Fill::Blank)
}

/// `text` with each inline `<code>` element replaced by filler.
///
/// Filler finds sentence boundaries, and spaces are for matches, as for the
/// backtick spans. A full stop inside "<code>foo. not sure why</code>" is
/// not a sentence end. A split there left a fragment that the narrative
/// rule read as deliberation.
pub fn mask_inline_code(text: &str) -> String {
    replace_inline_code(text, Fill::Mask)
}

/// `text` with each inline code span replaced by spaces of equal width.
pub fn blank_code_spans(text: &str) -> String {
    replace_spans(text, Fill::Blank)
}

/// `text` with each code span replaced by filler of the same width.
///
/// Filler, not spaces. This copy only finds sentence boundaries. A span
/// blanked to white space lets the `\s+` of the separator swallow it. Then
/// a sentence that opens with a code span loses it, and it reports one word
/// short with the span missing from its text.
pub fn mask_code_spans(text: &str) -> String {
    replace_spans(text, Fill::Mask)
}

/// `lines` with code spans blanked, including spans that wrap.
///
/// The decision reads the joined text, so a span that opens on one line
/// can close on the next. Spans become spaces, so each column still lines
/// up. Newlines survive, so the result splits back into the same lines.
/// An unmatched backtick blanks nothing, as in CommonMark.
pub fn blank_spans_across(lines: &[String]) -> Vec<String> {
    blank_code_spans(&lines.join("\n"))
        .split('\n')
        .map(str::to_string)
        .collect()
}

/// `text` with code spans blanked inside each cell, never across two.
///
/// A blank can be shorter than its cell in bytes. So the cells are blanked
/// from the right, and each earlier offset still points into `text`.
pub fn blank_cells(text: &str, spans: &[(usize, usize)]) -> String {
    let mut out = text.to_string();
    let mut ordered = spans.to_vec();
    ordered.sort_unstable();
    for &(start, end) in ordered.iter().rev() {
        if start >= end || end > text.len() {
            continue;
        }
        let blanked = blank_code_spans(&text[start..end]);
        out.replace_range(start..end, &blanked);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_span_needs_runs_of_equal_length() {
        assert_eq!(blank_code_spans("a `b` c"), "a     c");
        assert_eq!(blank_code_spans("a ``b` c"), "a ``b` c");
        assert_eq!(blank_code_spans("a \\`b` c `d`"), "a \\`b     d`");
    }

    #[test]
    fn a_blank_line_ends_a_span() {
        assert_eq!(blank_code_spans("`a\n\nb`"), "`a\n\nb`");
        assert_eq!(blank_code_spans("`a\nb`"), "  \n  ");
    }

    #[test]
    fn backtick_runs_match_their_pattern() {
        use super::super::patterns::BACKTICK_RUN_RE;
        for text in ["", "a", "`", "a``b`", "```", "x `y` ``z``"] {
            let mut want = Vec::new();
            let mut pos = 0;
            while let Some(found) = BACKTICK_RUN_RE.find_from(text, pos) {
                want.push(found);
                pos = found.1;
            }
            assert_eq!(backtick_runs(text), want, "{text:?}");
        }
    }

    #[test]
    fn the_mask_keeps_byte_offsets() {
        let text = "x `\u{e9}` y";
        assert_eq!(mask_code_spans(text).len(), text.len());
    }

    #[test]
    fn the_blank_keeps_columns() {
        let text = "x `\u{e9}` y";
        assert_eq!(blank_code_spans(text), "x     y");
        assert_eq!(blank_code_spans(text).chars().count(), text.chars().count());
    }

    #[test]
    fn cells_blank_from_the_right() {
        let text = "| `\u{e9}` | `b` |";
        assert_eq!(blank_cells(text, &[(1, 7), (8, 13)]), "|     |     |");
    }

    #[test]
    fn an_unclosed_code_element_runs_to_the_end() {
        assert_eq!(
            blank_inline_code("a <code>b c"),
            format!("a{}", " ".repeat(10))
        );
        assert_eq!(blank_inline_code("\\<code>b"), "\\<code>b");
    }
}
