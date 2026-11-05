//! The Python meaning of a pattern, spelled out for the Rust engine.
//!
//! The patterns keep their exact Python text. The Rust engine reads most of
//! that text the same way, but not all of it. Four constructs differ, and
//! each only on a few characters.
//!
//! - `\s`: Python also counts U+001C to U+001F as white space.
//! - `\w`: Python counts a letter, a number and `_`. Rust also counts a
//!   combining mark and other connector punctuation. It does not count a
//!   number such as U+00B2.
//! - `\b`: it follows `\w`, so it differs on the same characters.
//! - A case-insensitive `i`: Python also matches U+0130 and U+0131 with it.
//!   Rust folds case by the simple Unicode folds, and those two have none.
//!
//! A text with none of those characters gets the same answer from both
//! readings. So the fast Rust reading runs on almost each text. A text with
//! one of them runs on the spelled-out copy that this module writes.

use std::fmt::Write as _;

/// The class body of a Python `\w` in Unicode mode.
const PY_WORD: &str = r"\p{L}\p{N}_";

/// The extra class body of a Python `\s`.
const PY_SPACE_EXTRA: &str = r"\x1c-\x1f";

/// The two characters that a Python case-insensitive `i` adds.
const PY_I_EXTRA: &str = "\u{131}\u{130}";

/// A pattern rewritten to the Python meaning, with what the rewrite changed.
#[derive(Debug, PartialEq, Eq)]
pub struct Translation {
    /// The rewritten pattern.
    pub pattern: String,
    /// The rewrite changed a `\s`, a `\w` or a `\b`.
    pub classes: bool,
    /// The rewrite changed a case-insensitive `i`.
    pub folds: bool,
    /// The pattern has a `\w` inside a case-insensitive class.
    ///
    /// The rewrite cannot make that exact. Rust folds the case of a whole
    /// class, and so it adds U+0345 to the Python word class. No pattern of
    /// this audit has one, and a test keeps it so.
    pub unsupported: bool,
}

/// The value of a class item, when the item is one character.
fn escape_value(chars: &[char], at: usize) -> (Option<char>, usize) {
    let Some(&next) = chars.get(at + 1) else {
        return (Some('\\'), 1);
    };
    let hex = |len: usize| {
        let digits: String = chars.iter().skip(at + 2).take(len).collect();
        (digits.chars().count() == len)
            .then(|| {
                u32::from_str_radix(&digits, 16)
                    .ok()
                    .and_then(char::from_u32)
            })
            .flatten()
    };
    match next {
        'x' => (hex(2), 4),
        'u' => (hex(4), 6),
        'U' => (hex(8), 10),
        't' => (Some('\t'), 2),
        'n' => (Some('\n'), 2),
        'r' => (Some('\r'), 2),
        'f' => (Some('\u{c}'), 2),
        'v' => (Some('\u{b}'), 2),
        's' | 'S' | 'w' | 'W' | 'd' | 'D' => (None, 2),
        other => (Some(other), 2),
    }
}

/// Does the range from `low` to `high` hold an `i` of either case?
fn holds_i(low: char, high: char) -> bool {
    (low..=high).contains(&'i') || (low..=high).contains(&'I')
}

/// Rewrites the class that opens at `at`. Returns where the class ends.
fn rewrite_class(chars: &[char], at: usize, fold: bool, out: &mut Translation) -> usize {
    let mut index = at + 1;
    out.pattern.push('[');
    if chars.get(index) == Some(&'^') {
        out.pattern.push('^');
        index += 1;
    }
    let first = index;
    let mut has_i = false;
    while index < chars.len() {
        let c = chars[index];
        if c == ']' && index > first {
            break;
        }
        let (value, width) = if c == '\\' {
            escape_value(chars, index)
        } else {
            (Some(c), 1)
        };
        match (c, chars.get(index + 1)) {
            ('\\', Some('s')) => {
                out.pattern.push_str(r"\s");
                out.pattern.push_str(PY_SPACE_EXTRA);
                out.classes = true;
            }
            ('\\', Some('w')) => {
                out.pattern.push_str(PY_WORD);
                out.classes = true;
                out.unsupported |= fold;
            }
            _ => out
                .pattern
                .extend(&chars[index..(index + width).min(chars.len())]),
        }
        index += width;
        // A range is a `-` between two items, and never the last item.
        if let Some(low) = value
            && chars.get(index) == Some(&'-')
            && chars.get(index + 1).is_some_and(|&next| next != ']')
        {
            let (high, high_width) = if chars[index + 1] == '\\' {
                escape_value(chars, index + 1)
            } else {
                (Some(chars[index + 1]), 1)
            };
            out.pattern
                .extend(&chars[index..(index + 1 + high_width).min(chars.len())]);
            index += 1 + high_width;
            has_i |= high.is_some_and(|high| holds_i(low, high));
            continue;
        }
        has_i |= value.is_some_and(|value| holds_i(value, value));
    }
    if fold && has_i {
        out.pattern.push_str(PY_I_EXTRA);
        out.folds = true;
    }
    if index < chars.len() {
        out.pattern.push(']');
        index += 1;
    }
    index
}

/// Rewrites the group syntax that opens at `at`, `(?` included.
///
/// It returns where the group body starts, and the case-insensitive state
/// for that body. A flag group with no body, such as `(?i)`, changes the
/// state of the enclosing group, so the caller handles it.
fn rewrite_group(chars: &[char], at: usize, fold: bool, out: &mut String) -> (usize, bool, bool) {
    let mut index = at + 2;
    out.push_str("(?");
    match chars.get(index) {
        Some(':' | '=' | '!') => {
            out.push(chars[index]);
            return (index + 1, fold, false);
        }
        Some('<') if matches!(chars.get(index + 1), Some('=' | '!')) => {
            out.extend(&chars[index..index + 2]);
            return (index + 2, fold, false);
        }
        Some('P') => {
            let close = if chars.get(index + 1) == Some(&'=') {
                ')'
            } else {
                '>'
            };
            while index < chars.len() {
                out.push(chars[index]);
                index += 1;
                if chars[index - 1] == close {
                    break;
                }
            }
            return (index, fold, close == ')');
        }
        _ => {}
    }
    let mut state = fold;
    let mut on = true;
    while let Some(&c) = chars.get(index) {
        match c {
            '-' => on = false,
            'i' => state = on,
            ':' | ')' => break,
            _ => {}
        }
        out.push(c);
        index += 1;
    }
    if chars.get(index) == Some(&')') {
        out.push(')');
        return (index + 1, state, true);
    }
    if chars.get(index) == Some(&':') {
        out.push(':');
        index += 1;
    }
    (index, state, false)
}

/// Rewrites `pattern` so that the Rust engine reads it as Python does.
pub fn python_semantics(pattern: &str) -> Translation {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = Translation {
        pattern: String::with_capacity(pattern.len() * 2),
        classes: false,
        folds: false,
        unsupported: false,
    };
    let mut fold = false;
    let mut saved: Vec<bool> = Vec::new();
    let mut index = 0;
    while index < chars.len() {
        let c = chars[index];
        match c {
            '\\' => {
                let next = chars.get(index + 1).copied();
                match next {
                    Some('s') => {
                        let _ = write!(out.pattern, r"[\s{PY_SPACE_EXTRA}]");
                        out.classes = true;
                    }
                    Some('w') => {
                        let _ = write!(out.pattern, "(?-i:[{PY_WORD}])");
                        out.classes = true;
                    }
                    Some('b') => {
                        let _ = write!(
                            out.pattern,
                            "(?-i:(?:(?<=[{PY_WORD}])(?![{PY_WORD}])|(?<![{PY_WORD}])(?=[{PY_WORD}])))"
                        );
                        out.classes = true;
                    }
                    Some(next) => {
                        out.pattern.push(c);
                        out.pattern.push(next);
                    }
                    None => out.pattern.push(c),
                }
                index += 2;
            }
            '[' => index = rewrite_class(&chars, index, fold, &mut out),
            '(' if chars.get(index + 1) == Some(&'?') => {
                let (body, state, closed) = rewrite_group(&chars, index, fold, &mut out.pattern);
                // A flag group with no body sets the rest of the enclosing
                // group, so it saves no state. A back-reference changes
                // nothing. Each other group restores the state at its end.
                if !closed {
                    saved.push(fold);
                }
                fold = state;
                index = body;
            }
            '(' => {
                saved.push(fold);
                out.pattern.push(c);
                index += 1;
            }
            ')' => {
                fold = saved.pop().unwrap_or(false);
                out.pattern.push(c);
                index += 1;
            }
            'i' | 'I' if fold => {
                let _ = write!(out.pattern, "[iI{PY_I_EXTRA}]");
                out.folds = true;
                index += 1;
            }
            _ => {
                out.pattern.push(c);
                index += 1;
            }
        }
    }
    out
}

/// Does `text` hold a character on which `\s`, `\w` or `\b` disagree?
///
/// The set is a superset of the real disagreement, which is safe. A text in
/// it only runs on the slower copy, and that copy is exact for each text.
pub fn diverges_on_classes(text: &str) -> bool {
    static DIVERGENT: std::sync::LazyLock<Option<regex::Regex>> = std::sync::LazyLock::new(|| {
        regex::Regex::new(
            r"[\x1c-\x1f\p{M}\p{Join_Control}\p{Nl}\p{No}[\p{Pc}&&[^_]][\p{Alphabetic}&&\P{L}]]",
        )
        .ok()
    });
    if text.is_ascii() {
        return text.bytes().any(|byte| (0x1c..=0x1f).contains(&byte));
    }
    DIVERGENT
        .as_ref()
        .is_none_or(|divergent| divergent.is_match(text))
}

/// Does `text` hold U+0130 or U+0131?
///
/// Both start with the byte 0xC4 in UTF-8. A search for one byte is fast,
/// and some callers ask this of a whole block many times.
pub fn diverges_on_folds(text: &str) -> bool {
    text.as_bytes().contains(&0xc4) && text.contains(['\u{130}', '\u{131}'])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rewrite(pattern: &str) -> String {
        python_semantics(pattern).pattern
    }

    #[test]
    fn classes_are_rewritten_outside_and_inside_a_class() {
        assert_eq!(rewrite(r"a\sb"), r"a[\s\x1c-\x1f]b");
        assert_eq!(rewrite(r"[^\s.]"), r"[^\s\x1c-\x1f.]");
        assert_eq!(rewrite(r"[\w]\w"), r"[\p{L}\p{N}_](?-i:[\p{L}\p{N}_])");
        assert_eq!(rewrite(r"[]\s]"), r"[]\s\x1c-\x1f]");
        assert_eq!(rewrite(r"\\s\d"), r"\\s\d");
        assert!(!python_semantics(r"<code[ \t]*>").classes);
    }

    #[test]
    fn a_case_insensitive_i_gains_two_characters() {
        assert_eq!(rewrite("(?i)it"), "(?i)[iI\u{131}\u{130}]t");
        assert_eq!(rewrite("(?i)[a-z]"), "(?i)[a-z\u{131}\u{130}]");
        assert_eq!(rewrite("(?i)[a-h]"), "(?i)[a-h]");
        assert_eq!(rewrite("(?i:i)i"), "(?i:[iI\u{131}\u{130}])i");
        assert_eq!(rewrite("(?i)(?-i:Ei)i"), "(?i)(?-i:Ei)[iI\u{131}\u{130}]");
        assert_eq!(rewrite("(?<=i)(?P<x>i)(?P=x)"), "(?<=i)(?P<x>i)(?P=x)");
        assert!(!python_semantics("(?i)<code").folds);
    }

    #[test]
    fn the_rewrite_compiles_and_matches_as_python_does() {
        let compile = |pattern: &str| {
            fancy_regex::Regex::new(&rewrite(pattern)).map_err(|error| error.to_string())
        };
        let matches = |pattern: &str, text: &str| {
            compile(pattern).is_ok_and(|re| re.is_match(text).unwrap_or(false))
        };
        // Python reads U+001F as space, U+00B2 as a word character, and a
        // combining mark as a word boundary.
        assert!(matches(r"a\sb", "a\u{1f}b"));
        assert!(matches(r"^\w+$", "x\u{b2}"));
        assert!(!matches(r"^\w+$", "e\u{301}"));
        assert!(matches(r"\b22\b", "#22\u{301}65"));
        assert!(!matches(r"\b22\b", "#2265"));
        assert!(!matches(r"(?<!\bDr)\.", "x\u{301}Dr."));
        assert!(matches(r"(?<!\bDr)\.", "xDr."));
        // A case fold does not widen the Python word class.
        assert!(matches(r"(?i)round 3\b", "round 3\u{345}"));
        assert!(matches(r"(?i)\bit's", "\u{130}t's"));
        assert!(matches(r"(?i)[a-z]+", "\u{131}"));
        assert!(!matches(r"(?i)[^a-z]", "\u{131}"));
    }

    #[test]
    fn divergence_is_rare() {
        assert!(!diverges_on_classes(
            "plain text, caf\u{e9} \u{2019}quoted\u{2019}"
        ));
        assert!(diverges_on_classes("x\u{1c}y"));
        assert!(diverges_on_classes("n\u{b2}"));
        assert!(diverges_on_classes("e\u{301}"));
        assert!(!diverges_on_classes("snake_case"));
        assert!(diverges_on_folds("\u{131}"));
        assert!(!diverges_on_folds("\u{100}"));
    }
}
