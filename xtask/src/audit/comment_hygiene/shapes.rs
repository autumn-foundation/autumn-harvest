//! Matchers written by hand for the hot line-shape patterns.
//!
//! The block layer asks these questions many times for each comment line.
//! Each pattern below needs a look-around or a back-reference, so the regex
//! engine runs it on its slow backtracking path. Each matcher here gives the
//! same answer as its pattern. The tests at the end check that on each short
//! string over the characters that matter.

/// Is `byte` a space or a tab?
const fn blank(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

/// The end of the leading run of spaces and tabs.
fn indent_end(bytes: &[u8]) -> usize {
    bytes.iter().take_while(|&&byte| blank(byte)).count()
}

/// `^([ \t]*)((?:>(?!=)[ \t]?)+)`: one or more block-quote markers.
///
/// It returns the end of the indent and the end of the match. A `>` that a
/// `=` follows is an operator, not a marker, so the run stops before it.
pub fn blockquote(text: &str) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let start = indent_end(bytes);
    let mut end = None;
    let mut cursor = start;
    while bytes.get(cursor) == Some(&b'>') && bytes.get(cursor + 1) != Some(&b'=') {
        cursor += 1;
        if bytes.get(cursor).is_some_and(|&byte| blank(byte)) {
            cursor += 1;
        }
        end = Some(cursor);
    }
    end.map(|end| (start, end))
}

/// `^([ \t]*)>(?!=)[ \t]?`: exactly one block-quote marker.
pub fn one_quote(text: &str) -> Option<(usize, usize)> {
    let bytes = text.as_bytes();
    let start = indent_end(bytes);
    if bytes.get(start) != Some(&b'>') || bytes.get(start + 1) == Some(&b'=') {
        return None;
    }
    let end = if bytes.get(start + 1).is_some_and(|&byte| blank(byte)) {
        start + 2
    } else {
        start + 1
    };
    Some((start, end))
}

/// `^([ \t]*)([-_*])(?:[ \t]*\2){2,}[ \t]*$`: a thematic break.
///
/// It returns the end of the indent. The rest of the line holds only
/// spaces, tabs and the rule character, and that character appears at
/// least three times.
pub fn thematic_break(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let start = indent_end(bytes);
    let rule = *bytes.get(start)?;
    if !matches!(rule, b'-' | b'_' | b'*') {
        return None;
    }
    let rest = &bytes[start + 1..];
    // A comment line is short, so a plain count is fast enough here.
    #[allow(clippy::naive_bytecount)]
    let rules = rest.iter().filter(|&&byte| byte == rule).count();
    (rules >= 2 && rest.iter().all(|&byte| byte == rule || blank(byte))).then_some(start)
}

/// `^[ \t]*([-_*=\u2500\u2501\u2550\u00b7])(?:[ \t]*\1){2,}[ \t]*$`: a
/// section rule, in the broad set of characters.
pub fn separator(text: &str) -> bool {
    let rest = text.trim_start_matches([' ', '\t']);
    let mut chars = rest.chars();
    let Some(rule) = chars.next() else {
        return false;
    };
    if !matches!(
        rule,
        '-' | '_' | '*' | '=' | '\u{2500}' | '\u{2501}' | '\u{2550}' | '\u{b7}'
    ) {
        return false;
    }
    let tail = chars.as_str();
    tail.chars().filter(|&c| c == rule).count() >= 2
        && tail.chars().all(|c| c == rule || c == ' ' || c == '\t')
}

/// `^\[[ xX]\](?=[ \t]|$)`: a task-list marker. It returns its end.
pub const fn task_marker(text: &str) -> Option<usize> {
    match text.as_bytes() {
        [b'[', b' ' | b'x' | b'X', b']'] | [b'[', b' ' | b'x' | b'X', b']', b' ' | b'\t', ..] => {
            Some(3)
        }
        _ => None,
    }
}

/// Each match of the sentence separator, from left to right.
///
/// The pattern is `(?<=[.!?])(?<!e\.g\.)(?<!E\.g\.)(?<!i\.e\.)(?<!I\.e\.)
/// [*_]*\s+`. A separator starts right after terminal punctuation that does
/// not end "e.g." or "i.e.". It takes any emphasis markers, then at least
/// one white-space character, then the rest of that white space.
pub fn sentence_splits(text: &str) -> Vec<(usize, usize)> {
    let bytes = text.as_bytes();
    let mut found = Vec::new();
    let mut pos = 1;
    while pos <= bytes.len() {
        if !matches!(bytes[pos - 1], b'.' | b'!' | b'?') {
            pos += 1;
            continue;
        }
        let before = &bytes[..pos];
        if [b"e.g.", b"E.g.", b"i.e.", b"I.e."]
            .iter()
            .any(|abbreviation| before.ends_with(*abbreviation))
        {
            pos += 1;
            continue;
        }
        let mut cursor = pos;
        while cursor < bytes.len() && matches!(bytes[cursor], b'*' | b'_') {
            cursor += 1;
        }
        let space: usize = text[cursor..]
            .chars()
            .take_while(|c| c.is_whitespace())
            .map(char::len_utf8)
            .sum();
        if space == 0 {
            pos += 1;
            continue;
        }
        let end = cursor + space;
        found.push((pos, end));
        pos = end + 1;
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::comment_hygiene::patterns::{
        BLOCKQUOTE_RE, ONE_QUOTE_RE, SENTENCE_SPLIT_RE, SEPARATOR_RE, TASK_MARKER_RE,
        THEMATIC_BREAK_RE,
    };

    /// Each string of up to `max` characters over `alphabet`.
    fn strings(alphabet: &[char], max: usize) -> Vec<String> {
        let mut all = vec![String::new()];
        let mut last = vec![String::new()];
        for _ in 0..max {
            let mut next = Vec::new();
            for prefix in &last {
                for &c in alphabet {
                    let mut s = prefix.clone();
                    s.push(c);
                    next.push(s);
                }
            }
            all.extend(next.iter().cloned());
            last = next;
        }
        all
    }

    #[test]
    fn blockquote_matches_its_pattern() {
        let re = &*BLOCKQUOTE_RE;
        let one = &*ONE_QUOTE_RE;
        for text in strings(&[' ', '\t', '>', '=', 'a'], 6) {
            let want = re
                .match_start(&text)
                .map(|caps| (caps.span(1).map_or(0, |s| s.1), caps.end()));
            assert_eq!(blockquote(&text), want, "{text:?}");
            let want = one
                .match_start(&text)
                .map(|caps| (caps.span(1).map_or(0, |s| s.1), caps.end()));
            assert_eq!(one_quote(&text), want, "{text:?}");
        }
    }

    #[test]
    fn rules_match_their_patterns() {
        let thematic = &*THEMATIC_BREAK_RE;
        let broad = &*SEPARATOR_RE;
        for text in strings(&[' ', '\t', '-', '*', '=', '\u{2500}', 'a'], 6) {
            let want = thematic
                .match_start(&text)
                .map(|caps| caps.span(1).map_or(0, |s| s.1));
            assert_eq!(thematic_break(&text), want, "{text:?}");
            assert_eq!(
                separator(&text),
                broad.match_start(&text).is_some(),
                "{text:?}"
            );
        }
    }

    #[test]
    fn task_marker_matches_its_pattern() {
        let re = &*TASK_MARKER_RE;
        for text in strings(&['[', ']', ' ', 'x', '\t', 'y'], 5) {
            assert_eq!(
                task_marker(&text),
                re.match_start(&text).map(|caps| caps.end()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn sentence_splits_match_the_pattern() {
        let re = &*SENTENCE_SPLIT_RE;
        for text in strings(&['.', '?', ' ', '*', 'e', 'g', '\u{a0}'], 6) {
            let mut want = Vec::new();
            let mut pos = 0;
            while let Some(found) = re.find_from(&text, pos) {
                want.push(found);
                pos = found.1.max(found.0 + 1);
            }
            assert_eq!(sentence_splits(&text), want, "{text:?}");
        }
    }
}
