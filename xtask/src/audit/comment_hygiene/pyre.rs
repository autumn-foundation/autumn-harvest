//! Helpers that keep the port faithful to the Python original.
//!
//! The audit began as a Python script, and its findings are fingerprinted.
//! A finding that changes shape between the two implementations is a false
//! Tier B regression. So this module reproduces the few Python behaviors
//! that the rules depend on. They are `str.isspace`, `str.strip`,
//! `str.split`, `str.expandtabs`, `str.splitlines` and `repr`. The others are
//! the `re` search forms that take a start and an end position.
//!
//! Offsets are byte offsets into UTF-8 text. Python counts characters, so
//! every place that reports or truncates text counts characters here too.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};

/// Is set when a regex search fails at run time.
///
/// A search fails only when it exceeds the backtrack limit. The limit is
/// set far above any real input, so this flag stays clear. If it is set,
/// the run reports an error instead of a result that it cannot trust.
static SEARCH_FAILED: AtomicBool = AtomicBool::new(false);

/// Returns true when a regex search failed during this process.
pub fn search_failed() -> bool {
    SEARCH_FAILED.load(Ordering::Relaxed)
}

/// Python `str.isspace` for one character.
///
/// Python also counts the four information separators U+001C to U+001F.
/// Rust `char::is_whitespace` does not.
pub fn py_isspace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Python `str.strip()` with no argument.
pub fn strip(s: &str) -> &str {
    s.trim_matches(py_isspace)
}

/// Python `str.lstrip()` with no argument.
pub fn lstrip(s: &str) -> &str {
    s.trim_start_matches(py_isspace)
}

/// Python `str.split()` with no argument.
pub fn split_ws(s: &str) -> Vec<&str> {
    s.split(py_isspace)
        .filter(|word| !word.is_empty())
        .collect()
}

/// Python `" ".join(s.split())`: every whitespace run becomes one space.
pub fn normalize_ws(s: &str) -> String {
    split_ws(s).join(" ")
}

/// The length of `s.expandtabs(4)`, in characters.
///
/// A tab moves to the next multiple of four columns. A newline or a
/// carriage return starts the column count again, as Python does.
pub fn expanded_len(s: &str) -> usize {
    let mut total = 0;
    let mut column = 0;
    for c in s.chars() {
        match c {
            '\t' => {
                let width = 4 - column % 4;
                total += width;
                column += width;
            }
            '\n' | '\r' => {
                total += 1;
                column = 0;
            }
            _ => {
                total += 1;
                column += 1;
            }
        }
    }
    total
}

/// Python `str.splitlines()`, without the line ends.
///
/// Python splits on more than a newline. A form feed or a line separator
/// also ends a line. The bracket counter numbers lines this way, so the
/// port must number them the same way.
pub fn splitlines(s: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = s.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        let boundary = matches!(
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
        if !boundary {
            continue;
        }
        lines.push(&s[start..index]);
        let mut next = index + c.len_utf8();
        if c == '\r' && chars.peek().map(|&(_, n)| n) == Some('\n') {
            chars.next();
            next += 1;
        }
        start = next;
    }
    if start < s.len() {
        lines.push(&s[start..]);
    }
    lines
}

/// The first `count` characters of `s`, as Python `s[:count]` gives them.
pub fn truncate(s: &str, count: usize) -> &str {
    s.char_indices()
        .nth(count)
        .map_or(s, |(index, _)| &s[..index])
}

/// `s[index..]`, or an empty string when `index` is past the end.
///
/// Python slicing never fails, and the block layer relies on that.
pub fn tail(s: &str, index: usize) -> &str {
    s.get(index..).unwrap_or("")
}

/// Python `repr` of a string.
///
/// Clone-class note: the non-printable escape tail below (everything past
/// the shared [`crate::py_repr`] prefix) repeats verbatim in
/// [`crate::audit::ledger_profiles::pyrepr::str_repr`]. Apply any change to
/// the `0x20`/`0x7f` cutoff or the `is_control` branch to both copies.
///
/// Two instances only, both from the same `#2574` port. No missed-fix has
/// occurred on either copy since. The merge-evidence bar (rule of three, or
/// a missed-fix) is not met yet, so the duplication stays. Two other ports
/// (`corpus_link_check::pycompat::repr`,
/// `workflow_yaml_parse::pytext::repr_str`) use a stricter `is_printable`
/// check here instead of `is_control`. They are not a third instance.
pub fn repr(s: &str) -> String {
    let quote = crate::py_repr::repr_quote(s);
    let mut out = String::new();
    out.push(quote);
    for c in s.chars() {
        if crate::py_repr::push_common_repr_escape(&mut out, c, quote) {
            continue;
        }
        if (c as u32) < 0x20 || c as u32 == 0x7f {
            let _ = write!(out, "\\x{:02x}", c as u32);
        } else if c.is_control() {
            let code = c as u32;
            if code <= 0xff {
                let _ = write!(out, "\\x{code:02x}");
            } else {
                let _ = write!(out, "\\u{code:04x}");
            }
        } else {
            out.push(c);
        }
    }
    out.push(quote);
    out
}

/// Python `repr` of a list of strings.
pub fn repr_list(items: &[String]) -> String {
    let parts: Vec<String> = items.iter().map(|item| repr(item)).collect();
    format!("[{}]", parts.join(", "))
}

/// The spans of one match and its groups.
#[derive(Debug, Clone)]
pub struct Caps {
    spans: Vec<Option<(usize, usize)>>,
}

impl Caps {
    /// The start of the whole match.
    pub fn start(&self) -> usize {
        self.span(0).map_or(0, |span| span.0)
    }

    /// The end of the whole match.
    pub fn end(&self) -> usize {
        self.span(0).map_or(0, |span| span.1)
    }

    /// The span of group `index`, if it took part in the match.
    pub fn span(&self, index: usize) -> Option<(usize, usize)> {
        self.spans.get(index).copied().flatten()
    }

    /// The text of group `index`, or an empty string.
    pub fn group<'t>(&self, text: &'t str, index: usize) -> &'t str {
        self.span(index)
            .map_or("", |(start, end)| &text[start..end])
    }
}

/// A compiled pattern with the Python search forms.
///
/// The engine is `fancy_regex`, because the rules use look-around and
/// back-references. A prefilter, when set, is a plain regex that every
/// match must also contain. It only skips texts that cannot match, so it
/// never changes a result.
///
/// Python and Rust read a few constructs differently on a few characters.
/// `pysem` lists them. So a pattern that uses one of them compiles twice.
/// The second copy spells out the Python meaning. A text with one of those
/// characters
/// runs on that copy. All other text runs on the first copy, which is
/// faster and gives the same answer there. A pattern with none of them
/// has no second copy and needs no check. Some of those patterns search a
/// whole block many times, and a check of the block each time is slow.
pub struct Re {
    inner: Result<fancy_regex::Regex, String>,
    exact: Option<Result<fancy_regex::Regex, String>>,
    /// The exact copy differs in `\s`, `\w` or `\b`.
    classes: bool,
    /// The exact copy differs in a case-insensitive `i`.
    folds: bool,
    prefilter: Option<regex::Regex>,
    pattern: String,
}

/// Compiles `pattern` with no limit on backtracking.
fn compile(pattern: &str) -> Result<fancy_regex::Regex, String> {
    fancy_regex::RegexBuilder::new(pattern)
        .backtrack_limit(usize::MAX)
        .build()
        .map_err(|error| error.to_string())
}

impl Re {
    /// Compiles `pattern`. A compile error is kept and reported by `check`.
    pub fn new(pattern: &str) -> Self {
        let python = super::pysem::python_semantics(pattern);
        Self {
            inner: compile(pattern),
            exact: (python.pattern != pattern).then(|| compile(&python.pattern)),
            classes: python.classes,
            folds: python.folds,
            prefilter: None,
            pattern: pattern.to_string(),
        }
    }

    /// Compiles `pattern` with a prefilter that every match contains.
    pub fn with_prefilter(pattern: &str, prefilter: &str) -> Self {
        let mut re = Self::new(pattern);
        match regex::Regex::new(prefilter) {
            Ok(filter) => re.prefilter = Some(filter),
            Err(error) => re.inner = Err(format!("prefilter: {error}")),
        }
        re
    }

    /// The pattern text, as it was written.
    #[cfg(test)]
    pub fn source(&self) -> &str {
        &self.pattern
    }

    /// The compile error, if there is one.
    pub fn check(&self) -> Result<(), String> {
        let exact = self
            .exact
            .as_ref()
            .map_or(Ok(()), |exact| exact.as_ref().map(|_| ()));
        self.inner
            .as_ref()
            .map(|_| ())
            .and(exact)
            .map_err(|error| format!("{error}: {}", self.pattern))
    }

    /// The engine for `text`, or `None` when `text` cannot match.
    ///
    /// The prefilter uses the Rust classes. So it only applies to a text on
    /// which the two sets of classes agree.
    fn engine(&self, text: &str) -> Option<&fancy_regex::Regex> {
        if let Some(exact) = &self.exact
            && ((self.classes && super::pysem::diverges_on_classes(text))
                || (self.folds && super::pysem::diverges_on_folds(text)))
        {
            return exact.as_ref().ok();
        }
        let engine = self.inner.as_ref().ok()?;
        if let Some(filter) = &self.prefilter
            && !filter.is_match(text)
        {
            return None;
        }
        Some(engine)
    }

    /// Python `pattern.search(text, pos)`.
    ///
    /// A look-behind may read the text before `pos`, as in Python.
    pub fn search_from(&self, text: &str, pos: usize) -> Option<Caps> {
        if pos > text.len() {
            return None;
        }
        let engine = self.engine(text)?;
        engine.captures_from_pos(text, pos).map_or_else(
            |_| {
                SEARCH_FAILED.store(true, Ordering::Relaxed);
                None
            },
            |found| {
                found.map(|caps| Caps {
                    spans: (0..caps.len())
                        .map(|index| caps.get(index).map(|m| (m.start(), m.end())))
                        .collect(),
                })
            },
        )
    }

    /// Python `pattern.search(text)`.
    pub fn search(&self, text: &str) -> Option<Caps> {
        self.search_from(text, 0)
    }

    /// The span of `pattern.search(text, pos)`, with no groups.
    pub fn find_from(&self, text: &str, pos: usize) -> Option<(usize, usize)> {
        if pos > text.len() {
            return None;
        }
        let engine = self.engine(text)?;
        engine.find_from_pos(text, pos).map_or_else(
            |_| {
                SEARCH_FAILED.store(true, Ordering::Relaxed);
                None
            },
            |found| found.map(|m| (m.start(), m.end())),
        )
    }

    /// The span of `pattern.search(text, pos, endpos)`, with no groups.
    pub fn find_range(&self, text: &str, pos: usize, endpos: usize) -> Option<(usize, usize)> {
        let end = endpos.min(text.len());
        self.find_from(&text[..end], pos)
    }

    /// Python `pattern.match(text, pos)`.
    ///
    /// The engine returns the leftmost match. A match that starts at `pos`
    /// is the leftmost one when it exists, so this test is exact.
    pub fn match_at(&self, text: &str, pos: usize) -> Option<Caps> {
        self.search_from(text, pos)
            .filter(|caps| caps.start() == pos)
    }

    /// The span of `pattern.match(text, pos, endpos)`, with no groups.
    pub fn match_range(&self, text: &str, pos: usize, endpos: usize) -> Option<(usize, usize)> {
        self.find_range(text, pos, endpos)
            .filter(|&(start, _)| start == pos)
    }

    /// Python `pattern.match(text)`.
    pub fn match_start(&self, text: &str) -> Option<Caps> {
        self.match_at(text, 0)
    }

    /// Python `bool(pattern.search(text))`.
    pub fn is_match(&self, text: &str) -> bool {
        let Some(engine) = self.engine(text) else {
            return false;
        };
        engine.is_match(text).unwrap_or_else(|_| {
            SEARCH_FAILED.store(true, Ordering::Relaxed);
            false
        })
    }
}

/// Removes the white space and comments of a Python `re.VERBOSE` pattern.
///
/// The large CH001 pattern is written in verbose form, as the original
/// was. Python ignores white space outside a character class, and treats
/// `#` outside a class as the start of a comment. An escaped character
/// stays as it is, so `\#` and `\ ` keep their meaning.
pub fn strip_verbose(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    let mut chars = pattern.chars().peekable();
    let mut in_class = false;
    let mut class_start = false;
    while let Some(c) = chars.next() {
        if c == '\\' {
            out.push(c);
            if let Some(next) = chars.next() {
                out.push(next);
            }
            class_start = false;
            continue;
        }
        if in_class {
            if c == ']' && !class_start {
                in_class = false;
            }
            out.push(c);
            class_start = false;
            continue;
        }
        match c {
            '[' => {
                in_class = true;
                class_start = true;
                out.push(c);
                if chars.peek() == Some(&'^') {
                    chars.next();
                    out.push('^');
                }
            }
            ' ' | '\t' | '\n' | '\r' | '\u{0b}' | '\u{0c}' => {}
            '#' => {
                for skipped in chars.by_ref() {
                    if skipped == '\n' {
                        break;
                    }
                }
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expanded_len_counts_tab_stops() {
        assert_eq!(expanded_len(""), 0);
        assert_eq!(expanded_len(" \t"), 4);
        assert_eq!(expanded_len(" \t\t"), 8);
        assert_eq!(expanded_len("\u{a0}\t"), 4);
    }

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(splitlines("a\nb\r\nc\x0cd\n"), vec!["a", "b", "c", "d"]);
        assert_eq!(splitlines("a\n\nb"), vec!["a", "", "b"]);
        assert_eq!(splitlines(""), Vec::<&str>::new());
    }

    #[test]
    fn strip_and_split_use_python_white_space() {
        assert_eq!(strip("\u{1c} a \u{a0}"), "a");
        assert_eq!(split_ws(" a\u{3000}b  c "), vec!["a", "b", "c"]);
    }

    #[test]
    fn truncate_counts_characters() {
        assert_eq!(truncate("h\u{e9}llo", 2), "h\u{e9}");
        assert_eq!(truncate("ab", 5), "ab");
    }

    #[test]
    fn repr_picks_the_python_quote() {
        assert_eq!(repr("it's"), "\"it's\"");
        assert_eq!(repr("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(repr("x\ny"), "'x\\ny'");
    }

    #[test]
    fn repr_escapes_cr_tab_and_a_bare_backslash() {
        assert_eq!(repr("a\rb"), "'a\\rb'");
        assert_eq!(repr("a\tb"), "'a\\tb'");
        assert_eq!(repr("a\\b"), "'a\\\\b'");
    }

    #[test]
    fn strip_verbose_keeps_classes_and_escapes() {
        assert_eq!(strip_verbose("a b # note\n [ #] \\# c"), "ab[ #]\\#c");
        assert_eq!(strip_verbose("[]] x"), "[]]x");
    }

    #[test]
    fn match_at_requires_the_start() {
        let re = Re::new(r"b+");
        assert!(re.match_at("abb", 0).is_none());
        assert_eq!(re.match_at("abb", 1).map(|caps| caps.end()), Some(3));
    }

    #[test]
    fn search_range_ends_the_text() {
        let re = Re::new(r"b$");
        assert!(re.search("abc").is_none());
        assert!(re.find_range("abc", 0, 2).is_some());
    }
}
