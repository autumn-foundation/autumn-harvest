//! The string and regex steps that the port reads as Python reads them.
//!
//! The audit was written against Python's `re` module and `str` methods.
//! Each helper here keeps those semantics, so the rest of the port reads
//! like the original. Positions are byte offsets. The masked source is
//! almost all ASCII, so a byte offset there is a character offset too.
//!
//! A regex runs on `fancy_regex`, since many patterns use a lookbehind or a
//! lookahead. Python's `$` matches at the end or before a final newline.
//! So each pattern compiles in two forms: one for a text that ends in `\n`
//! and one for any other text. A leading negative lookbehind of one class is
//! checked by hand. The rest of the pattern then often runs on the plain
//! `regex` engine, which is much faster.

use std::cell::{OnceCell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use fancy_regex::{Regex, RegexBuilder};

/// The backtracking limit. Python's `re` has none, so this is high.
const BACKTRACK_LIMIT: usize = 1 << 32;

/// The marker that stands for a Python `$` in a translated pattern.
const DOLLAR: char = '\u{1}';

thread_local! {
    static CACHE: RefCell<HashMap<String, Rc<Compiled>>> = RefCell::new(HashMap::new());
    static FAILURE: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Records the first internal error, such as a pattern that fails to compile.
pub fn fail(message: String) {
    FAILURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            *slot = Some(message);
        }
    });
}

/// Takes the recorded internal error, if any.
pub fn take_failure() -> Option<String> {
    FAILURE.with(|slot| slot.borrow_mut().take())
}

/// One Python pattern, compiled lazily in each form that a call needs.
struct Compiled {
    /// The translated body, with `DOLLAR` where Python had `$`.
    body: String,
    /// The class of a leading negative lookbehind, cut from `body`.
    guard: Option<regex::Regex>,
    /// Search forms: for a text without a final newline, then with one.
    search: [OnceCell<Option<Regex>>; 2],
    /// Forms anchored at the start.
    anchored: [OnceCell<Option<Regex>>; 2],
    /// Forms anchored at both ends.
    full: [OnceCell<Option<Regex>>; 2],
    /// Plain prefilters of the anchored forms: anchored at the start, then
    /// at both ends, each for a text without and with a final newline.
    prefilter: [OnceCell<Option<regex::Regex>>; 4],
    /// The body with its lookarounds cut, when it has one.
    prefilter_source: Option<String>,
    /// The plain form of a literal name with one trailing lookahead.
    literal: Option<Literal>,
}

/// A body that is a literal name, an optional `\b`, then one lookahead.
///
/// The alias layer builds many such patterns, such as `Q\b(?=\s*::)`. The
/// name has one fixed length, so the match ends right after it. The plain
/// engine finds the name, and the lookahead is then tested where the name
/// ends. That gives the same match as the backtracking machine, faster.
struct Literal {
    /// The name with its optional `\b`.
    head: regex::Regex,
    /// The lookahead body, anchored at the start.
    ahead: regex::Regex,
    /// Whether the lookahead must match (`(?=`) or must not (`(?!`).
    positive: bool,
}

impl Literal {
    fn parse(body: &str) -> Option<Self> {
        let name_end = body
            .find(|char: char| !(char.is_ascii_alphanumeric() || char == '_'))
            .filter(|end| *end > 0)?;
        let (name, rest) = body.split_at(name_end);
        let (boundary, rest) = rest
            .strip_prefix(r"\b")
            .map_or(("", rest), |rest| (r"\b", rest));
        let positive = if rest.starts_with("(?=") {
            true
        } else if rest.starts_with("(?!") {
            false
        } else {
            return None;
        };
        if group_end(rest.as_bytes(), 0)? != rest.len() {
            return None;
        }
        let inner = &rest[3..rest.len() - 1];
        let looks = ["(?=", "(?!", "(?<", "\\b", "\\B", "^", "\\A"];
        if inner.contains(DOLLAR) || looks.iter().any(|look| inner.contains(look)) {
            return None;
        }
        // A capture group inside the lookahead would report a span, so such
        // a body keeps the full machine.
        let bytes = inner.as_bytes();
        let captures = (0..bytes.len()).any(|index| {
            bytes[index] == b'('
                && bytes.get(index + 1) != Some(&b'?')
                && (index == 0 || bytes[index - 1] != b'\\')
        });
        if captures {
            return None;
        }
        Some(Self {
            head: regex::Regex::new(&format!("{name}{boundary}")).ok()?,
            ahead: regex::Regex::new(&format!(r"\A(?:{inner})")).ok()?,
            positive,
        })
    }
}

impl Compiled {
    fn new(pattern: &str) -> Self {
        let translated = translate(pattern);
        let (guard, body) = split_guard(&translated);
        let prefilter_source = prefilter_body(&body);
        let literal = Literal::parse(&body);
        Self {
            body,
            guard,
            search: [OnceCell::new(), OnceCell::new()],
            anchored: [OnceCell::new(), OnceCell::new()],
            full: [OnceCell::new(), OnceCell::new()],
            prefilter: [
                OnceCell::new(),
                OnceCell::new(),
                OnceCell::new(),
                OnceCell::new(),
            ],
            prefilter_source,
            literal,
        }
    }

    /// The plain prefilter of an anchored form, or `None` when it has none.
    fn prefilter(&self, kind: Form, newline: bool) -> Option<&regex::Regex> {
        let source = self.prefilter_source.as_ref()?;
        let (slot, suffix) = match kind {
            Form::Anchored => (0, ""),
            Form::Full => (2, r"\z"),
            Form::Search => return None,
        };
        self.prefilter[slot + usize::from(newline)]
            .get_or_init(|| {
                let dollar = if newline { r"\n?\z" } else { r"\z" };
                let body = source.replace(DOLLAR, dollar);
                regex::Regex::new(&format!(r"\A(?:{body}){suffix}")).ok()
            })
            .as_ref()
    }

    /// Whether the plain prefilter of an anchored form rules out a match.
    fn ruled_out(&self, kind: Form, text: &str) -> bool {
        self.prefilter(kind, ends_in_newline(text))
            .is_some_and(|prefilter| !prefilter.is_match(text))
    }

    fn form(&self, kind: Form, newline: bool) -> Option<&Regex> {
        let index = usize::from(newline);
        let (cell, wrap): (&OnceCell<Option<Regex>>, fn(&str) -> String) = match kind {
            Form::Search => (&self.search[index], |body| body.to_string()),
            Form::Anchored => (&self.anchored[index], |body| format!(r"\A(?:{body})")),
            Form::Full => (&self.full[index], |body| format!(r"\A(?:{body})\z")),
        };
        cell.get_or_init(|| {
            let dollar = if newline { r"(?:\z|(?=\n\z))" } else { r"\z" };
            let body = self.body.replace(DOLLAR, dollar);
            let source = wrap(&body);
            match RegexBuilder::new(&source)
                .backtrack_limit(BACKTRACK_LIMIT)
                .build()
            {
                Ok(compiled) => Some(compiled),
                Err(error) => {
                    fail(format!("cannot compile the pattern {source:?}: {error}"));
                    None
                }
            }
        })
        .as_ref()
    }
}

#[derive(Clone, Copy)]
enum Form {
    Search,
    Anchored,
    Full,
}

/// Returns the compiled form of a Python pattern, from the cache.
fn compiled(pattern: &str) -> Rc<Compiled> {
    CACHE.with(|cache| {
        if let Some(found) = cache.borrow().get(pattern) {
            return Rc::clone(found);
        }
        let made = Rc::new(Compiled::new(pattern));
        cache
            .borrow_mut()
            .insert(pattern.to_string(), Rc::clone(&made));
        made
    })
}

/// Translates Python regex syntax to the syntax that `fancy_regex` reads.
///
/// A `$` outside a class becomes `DOLLAR`, unless the pattern sets `(?m)`.
/// A `{` that opens no quantifier is escaped, since Rust rejects it bare. A
/// `[` inside a class is escaped, since Rust reads it as a nested class. So
/// are `&`, `-` and `~` pairs, which Rust reads as set operations.
fn translate(pattern: &str) -> String {
    let multiline = pattern.starts_with("(?m)") || pattern.starts_with("(?sm)");
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len() + 8);
    let mut index = 0;
    let mut in_class = false;
    let mut class_start = 0;
    while index < chars.len() {
        let char = chars[index];
        if char == '\\' && index + 1 < chars.len() {
            let next = chars[index + 1];
            if next == 'Z' && !in_class {
                out.push_str(r"\z");
            } else {
                out.push(char);
                out.push(next);
            }
            index += 2;
            continue;
        }
        if in_class {
            let first =
                index == class_start || (index == class_start + 1 && chars[class_start] == '^');
            match char {
                ']' if !first => {
                    in_class = false;
                    out.push(']');
                }
                '[' => out.push_str(r"\["),
                '&' | '~' => {
                    out.push('\\');
                    out.push(char);
                }
                '-' if chars.get(index + 1) == Some(&'-') => out.push_str(r"\-"),
                _ => out.push(char),
            }
            index += 1;
            continue;
        }
        match char {
            '[' => {
                in_class = true;
                out.push('[');
                class_start = index + 1;
            }
            '$' if !multiline => out.push(DOLLAR),
            '{' => {
                if quantifier_at(&chars, index) {
                    while index < chars.len() && chars[index] != '}' {
                        out.push(chars[index]);
                        index += 1;
                    }
                    out.push('}');
                } else {
                    out.push_str(r"\{");
                }
            }
            '}' => out.push_str(r"\}"),
            _ => out.push(char),
        }
        index += 1;
    }
    out
}

/// Whether a `{` at `index` opens a quantifier such as `{2}` or `{1,3}`.
fn quantifier_at(chars: &[char], index: usize) -> bool {
    let mut at = index + 1;
    let digits = at;
    while at < chars.len() && chars[at].is_ascii_digit() {
        at += 1;
    }
    if at == digits {
        return false;
    }
    if chars.get(at) == Some(&',') {
        at += 1;
        while at < chars.len() && chars[at].is_ascii_digit() {
            at += 1;
        }
    }
    chars.get(at) == Some(&'}')
}

/// Cuts a leading `(?<![..])` of one class, so the rest can run unguarded.
///
/// The cut applies only when the rest has no top-level `|`. A top-level
/// alternative would otherwise lose its own lookbehind.
fn split_guard(pattern: &str) -> (Option<regex::Regex>, String) {
    let Some(rest) = pattern.strip_prefix("(?<![") else {
        return (None, pattern.to_string());
    };
    let bytes = rest.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 2,
            b'[' => return (None, pattern.to_string()),
            b']' if index > 0 => break,
            _ => index += 1,
        }
    }
    if index >= bytes.len() || bytes.get(index + 1) != Some(&b')') {
        return (None, pattern.to_string());
    }
    let class = &rest[..index];
    let body = &rest[index + 2..];
    if has_top_level_bar(body) {
        return (None, pattern.to_string());
    }
    match regex::Regex::new(&format!(r"\A[{class}]\z")) {
        Ok(guard) => (Some(guard), body.to_string()),
        Err(_) => (None, pattern.to_string()),
    }
}

/// The body with its lookarounds cut, when it has one.
///
/// `fancy_regex` always searches. An anchored form that fails at the start
/// still runs its backtracking machine at each later position, which is slow
/// on a long text. A lookaround is a zero-width test, so the body without
/// its lookarounds matches every text that the body matches, and more. The
/// plain engine tests that cut body at the start at once. When it fails,
/// the full body fails too, and the slow run is skipped.
fn prefilter_body(body: &str) -> Option<String> {
    let looks = ["(?=", "(?!", "(?<=", "(?<!"];
    if !looks.iter().any(|look| body.contains(look)) {
        return None;
    }
    strip_lookarounds(body)
}

/// `pattern` with each lookahead and lookbehind group removed.
fn strip_lookarounds(pattern: &str) -> Option<String> {
    let bytes = pattern.as_bytes();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    let mut in_class = false;
    while index < bytes.len() {
        let char = bytes[index];
        if char == b'\\' {
            out.push_str(pattern.get(index..index + 2)?);
            index += 2;
            continue;
        }
        if in_class {
            in_class = char != b']';
        } else if char == b'[' {
            in_class = true;
        } else if [&b"(?="[..], b"(?!", b"(?<=", b"(?<!"]
            .iter()
            .any(|look| bytes[index..].starts_with(look))
        {
            index = group_end(bytes, index)?;
            continue;
        }
        let width = pattern[index..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&pattern[index..index + width]);
        index += width;
    }
    Some(out)
}

/// The index after the group that opens at `start`.
fn group_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut index = start;
    let mut in_class = false;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 1,
            b'[' if !in_class => in_class = true,
            b']' if in_class => in_class = false,
            b'(' if !in_class => depth += 1,
            b')' if !in_class => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return Some(index + 1);
                }
            }
            _ => {}
        }
        index += 1;
    }
    None
}

/// Whether a pattern has a `|` outside every group and class.
const fn has_top_level_bar(pattern: &str) -> bool {
    let bytes = pattern.as_bytes();
    let mut depth = 0usize;
    let mut in_class = false;
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index += 1,
            b'[' if !in_class => in_class = true,
            b']' if in_class => in_class = false,
            b'(' if !in_class => depth += 1,
            b')' if !in_class => depth = depth.saturating_sub(1),
            b'|' if !in_class && depth == 0 => return true,
            _ => {}
        }
        index += 1;
    }
    false
}

/// One match: the span of each group, with group 0 the whole match.
#[derive(Clone, Debug)]
pub struct M<'t> {
    text: &'t str,
    spans: Rc<[Option<(usize, usize)>]>,
}

impl<'t> M<'t> {
    /// Rebuilds a match over `text` from stored spans.
    pub const fn from_spans(text: &'t str, spans: Rc<[Option<(usize, usize)>]>) -> Self {
        Self { text, spans }
    }

    /// The stored spans of the groups.
    pub fn spans(&self) -> Rc<[Option<(usize, usize)>]> {
        Rc::clone(&self.spans)
    }

    /// Where the whole match starts.
    pub fn start(&self) -> usize {
        self.spans[0].map_or(0, |span| span.0)
    }

    /// Where the whole match ends.
    pub fn end(&self) -> usize {
        self.spans[0].map_or(0, |span| span.1)
    }

    /// The whole match.
    pub fn whole(&self) -> &'t str {
        &self.text[self.start()..self.end()]
    }

    /// Group `index`, or `None` when it did not take part.
    pub fn get(&self, index: usize) -> Option<&'t str> {
        self.spans
            .get(index)
            .copied()
            .flatten()
            .map(|(start, end)| &self.text[start..end])
    }

    /// Group `index`, or the empty text when it did not take part.
    pub fn g(&self, index: usize) -> &'t str {
        self.get(index).unwrap_or("")
    }

    /// Where group `index` starts, or `-1` as Python gives it.
    pub fn start_of(&self, index: usize) -> isize {
        self.spans
            .get(index)
            .copied()
            .flatten()
            .map_or(-1, |(start, _)| to_isize(start))
    }
}

/// A position as a signed index.
pub fn to_isize(value: usize) -> isize {
    isize::try_from(value).unwrap_or(isize::MAX)
}

/// A signed index as a position, with a negative one as zero.
pub fn to_usize(value: isize) -> usize {
    usize::try_from(value).unwrap_or(0)
}

fn spans_of(captures: &fancy_regex::Captures<'_>) -> Rc<[Option<(usize, usize)>]> {
    (0..captures.len())
        .map(|index| {
            captures
                .get(index)
                .map(|found| (found.start(), found.end()))
        })
        .collect()
}

fn ends_in_newline(text: &str) -> bool {
    text.ends_with('\n')
}

fn run(regex: &Regex, text: &str, pos: usize) -> Option<Rc<[Option<(usize, usize)>]>> {
    match regex.captures_from_pos(text, pos) {
        Ok(found) => found.map(|captures| spans_of(&captures)),
        Err(error) => {
            fail(format!("the pattern {:?} failed: {error}", regex.as_str()));
            None
        }
    }
}

/// Python's `pattern.search(text, pos)`.
pub fn search_from<'t>(pattern: &str, text: &'t str, pos: usize) -> Option<M<'t>> {
    let compiled = compiled(pattern);
    if let Some(literal) = &compiled.literal {
        return literal_search(&compiled, literal, text, pos);
    }
    let regex = compiled.form(Form::Search, ends_in_newline(text))?;
    let mut at = pos;
    loop {
        if at > text.len() {
            return None;
        }
        let spans = run(regex, text, at)?;
        let start = spans[0].map_or(0, |span| span.0);
        let Some(guard) = &compiled.guard else {
            return Some(M { text, spans });
        };
        let previous = text[..start].chars().next_back();
        let blocked = previous.is_some_and(|char| {
            let mut buffer = [0u8; 4];
            guard.is_match(char.encode_utf8(&mut buffer))
        });
        if !blocked {
            return Some(M { text, spans });
        }
        at = start + text[start..].chars().next().map_or(1, char::len_utf8);
    }
}

/// `search_from` for a `Literal` body, as `Literal` explains it.
fn literal_search<'t>(
    compiled: &Compiled,
    literal: &Literal,
    text: &'t str,
    pos: usize,
) -> Option<M<'t>> {
    let mut at = pos;
    while at <= text.len() {
        let found = literal.head.find_at(text, at)?;
        let (start, end) = (found.start(), found.end());
        at = start + text[start..].chars().next().map_or(1, char::len_utf8);
        if blocked(compiled, text, start) {
            continue;
        }
        if literal.ahead.is_match(&text[end..]) == literal.positive {
            let spans: Rc<[Option<(usize, usize)>]> = Rc::from(vec![Some((start, end))]);
            return Some(M { text, spans });
        }
    }
    None
}

/// Whether the cut lookbehind of `compiled` forbids a match at `start`.
fn blocked(compiled: &Compiled, text: &str, start: usize) -> bool {
    let Some(guard) = &compiled.guard else {
        return false;
    };
    text[..start].chars().next_back().is_some_and(|char| {
        let mut buffer = [0u8; 4];
        guard.is_match(char.encode_utf8(&mut buffer))
    })
}

/// Python's `re.search(pattern, text)`.
pub fn search<'t>(pattern: &str, text: &'t str) -> Option<M<'t>> {
    search_from(pattern, text, 0)
}

/// Whether `re.search(pattern, text)` finds a match.
pub fn found(pattern: &str, text: &str) -> bool {
    search(pattern, text).is_some()
}

/// Python's `re.match(pattern, text)`, anchored at the start.
///
/// A leading lookbehind always holds at the start of a text, so the cut
/// guard needs no check here.
pub fn rmatch<'t>(pattern: &str, text: &'t str) -> Option<M<'t>> {
    let compiled = compiled(pattern);
    if compiled.ruled_out(Form::Anchored, text) {
        return None;
    }
    let regex = compiled.form(Form::Anchored, ends_in_newline(text))?;
    let spans = run(regex, text, 0)?;
    Some(M { text, spans })
}

/// Whether `re.match(pattern, text)` matches.
pub fn matches(pattern: &str, text: &str) -> bool {
    rmatch(pattern, text).is_some()
}

/// Python's `pattern.match(text, pos)` for a pattern with no lookbehind.
///
/// The spans count from the start of `text`.
pub fn rmatch_at<'t>(pattern: &str, text: &'t str, pos: usize) -> Option<M<'t>> {
    let rest = text.get(pos..)?;
    let found = rmatch(pattern, rest)?;
    let spans = found
        .spans
        .iter()
        .map(|span| span.map(|(start, end)| (start + pos, end + pos)))
        .collect();
    Some(M { text, spans })
}

/// Python's `re.fullmatch(pattern, text)`.
pub fn fullmatch<'t>(pattern: &str, text: &'t str) -> Option<M<'t>> {
    let compiled = compiled(pattern);
    if compiled.ruled_out(Form::Full, text) {
        return None;
    }
    let regex = compiled.form(Form::Full, ends_in_newline(text))?;
    let spans = run(regex, text, 0)?;
    Some(M { text, spans })
}

/// Whether `re.fullmatch(pattern, text)` matches.
pub fn full(pattern: &str, text: &str) -> bool {
    fullmatch(pattern, text).is_some()
}

/// Python's `re.finditer(pattern, text)`.
///
/// An empty match right after an empty match at the same place is skipped,
/// as Python does.
pub fn finditer<'t>(pattern: &str, text: &'t str) -> Vec<M<'t>> {
    let mut found = Vec::new();
    let mut pos = 0;
    let mut must_advance = false;
    while pos <= text.len() {
        let Some(mut hit) = search_from(pattern, text, pos) else {
            break;
        };
        if must_advance && hit.start() == pos && hit.end() == pos {
            let next = pos + text[pos..].chars().next().map_or(1, char::len_utf8);
            match search_from(pattern, text, next) {
                Some(later) => hit = later,
                None => break,
            }
        }
        must_advance = hit.start() == hit.end();
        pos = hit.end();
        found.push(hit);
    }
    found
}

/// Python's `re.findall` for a pattern with one group: each group 1.
pub fn findall1<'t>(pattern: &str, text: &'t str) -> Vec<&'t str> {
    finditer(pattern, text).iter().map(|hit| hit.g(1)).collect()
}

/// Python's `re.sub` with a function that builds each replacement.
pub fn sub_with(pattern: &str, text: &str, mut replace: impl FnMut(&M<'_>) -> String) -> String {
    let mut out = String::with_capacity(text.len());
    let mut last = 0;
    for hit in finditer(pattern, text) {
        out.push_str(&text[last..hit.start()]);
        out.push_str(&replace(&hit));
        last = hit.end();
    }
    out.push_str(&text[last..]);
    out
}

/// Python's `re.sub` with a template, where `\1` names group 1.
pub fn sub(pattern: &str, replacement: &str, text: &str) -> String {
    sub_with(pattern, text, |hit| expand(replacement, hit))
}

/// Expands a Python replacement template for one match.
fn expand(template: &str, hit: &M<'_>) -> String {
    let bytes = template.as_bytes();
    let mut out = String::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 1 < bytes.len() && bytes[index + 1].is_ascii_digit() {
            out.push_str(hit.g(usize::from(bytes[index + 1] - b'0')));
            index += 2;
            continue;
        }
        let char_len = template[index..].chars().next().map_or(1, char::len_utf8);
        out.push_str(&template[index..index + char_len]);
        index += char_len;
    }
    out
}

/// Python's `re.escape` for the names this audit escapes.
pub fn esc(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for char in text.chars() {
        if r"\.+*?()|[]{}^$#&-~".contains(char) || char.is_whitespace() {
            out.push('\\');
        }
        out.push(char);
    }
    out
}

/// `text` with every whitespace character removed, as `re.sub(r"\s", "", ..)`.
pub fn no_space(text: &str) -> String {
    text.chars().filter(|char| !char.is_whitespace()).collect()
}

/// Python's `text[start:end]` for positions that do not exceed the length.
///
/// A position past the end is cut to the end. A position inside a
/// multi-byte character moves down to its start.
pub fn sl(text: &str, start: usize, end: usize) -> &str {
    let end = floor(text, end.min(text.len()));
    let start = floor(text, start.min(end));
    &text[start..end]
}

/// Python's `text[start:end]` for signed positions, where `-1` is the last
/// byte.
pub fn ps(text: &str, start: isize, end: isize) -> &str {
    let length = to_isize(text.len());
    let resolve = |at: isize| {
        let at = if at < 0 { at + length } else { at };
        to_usize(at.clamp(0, length))
    };
    let (start, end) = (resolve(start), resolve(end));
    if start >= end {
        return "";
    }
    sl(text, start, end)
}

/// Whether a byte is whitespace, as Python's `str.isspace()` reads ASCII.
pub const fn is_space(char: u8) -> bool {
    matches!(
        char,
        b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c | 0x1c..=0x1f
    )
}

/// Python's `text[start:]`.
pub fn from(text: &str, start: usize) -> &str {
    sl(text, start, text.len())
}

/// Python's `text[:end]`.
pub fn upto(text: &str, end: usize) -> &str {
    sl(text, 0, end)
}

/// The nearest character boundary at or below `at`.
const fn floor(text: &str, mut at: usize) -> usize {
    while at > 0 && !text.is_char_boundary(at) {
        at -= 1;
    }
    at
}

/// The byte at `index`, or zero past the end.
pub fn byte(text: &str, index: usize) -> u8 {
    text.as_bytes().get(index).copied().unwrap_or(0)
}

/// The byte at a Python index, where `-1` is the last byte.
pub fn byte_at(text: &str, index: isize) -> u8 {
    let length = to_isize(text.len());
    let at = if index < 0 { index + length } else { index };
    if at < 0 || at >= length {
        return 0;
    }
    byte(text, to_usize(at))
}

/// Python's `text.find(needle, start)`, or `-1`.
pub fn find(text: &str, needle: &str, start: usize) -> isize {
    if start > text.len() {
        return -1;
    }
    from(text, start)
        .find(needle)
        .map_or(-1, |at| to_isize(at + floor(text, start)))
}

/// Python's `text.rfind(needle, 0, end)`, or `-1`.
pub fn rfind_before(text: &str, needle: &str, end: usize) -> isize {
    upto(text, end).rfind(needle).map_or(-1, to_isize)
}

/// Python's `text.rfind(needle)`, or `-1`.
pub fn rfind(text: &str, needle: &str) -> isize {
    text.rfind(needle).map_or(-1, to_isize)
}

/// The balanced group that starts at the first byte of `text`.
///
/// It counts `opener` and `closer` from the first byte on. With no match,
/// the whole text is the group, as in the Python original.
pub fn balanced(text: &str, opener: u8, closer: u8) -> &str {
    let mut depth = 0i64;
    for (index, &char) in text.as_bytes().iter().enumerate() {
        if char == opener {
            depth += 1;
        } else if char == closer {
            depth -= 1;
            if depth == 0 {
                return &text[..=index];
            }
        }
    }
    text
}

/// `balanced` for parentheses.
pub fn paren(text: &str) -> &str {
    balanced(text, b'(', b')')
}

/// `balanced` for braces.
pub fn brace(text: &str) -> &str {
    balanced(text, b'{', b'}')
}

/// Whether a byte is a word byte, as `str.isalnum()` or `_` reads ASCII.
pub const fn is_word(char: u8) -> bool {
    char.is_ascii_alphanumeric() || char == b'_'
}

/// How many times `needle` occurs in `text`.
pub fn count(text: &str, needle: &str) -> usize {
    text.matches(needle).count()
}

/// How many times the byte `needle` occurs in `text`.
pub fn count_byte(text: &str, needle: u8) -> i64 {
    let total = text.bytes().filter(|&char| char == needle).count();
    i64::try_from(total).unwrap_or(i64::MAX)
}

/// The length of the leading whitespace of `text`.
pub fn lead_len(text: &str) -> usize {
    text.len() - text.trim_start().len()
}

/// A file text as Python's `Path.read_text()` returns it.
///
/// Python reads a file with universal newlines. So `\r\n` and a lone `\r`
/// each become `\n`, and a line number counts the same on every platform.
pub fn universal_newlines(text: String) -> String {
    if text.contains('\r') {
        text.replace("\r\n", "\n").replace('\r', "\n")
    } else {
        text
    }
}

/// Python's `repr` of a string, as `str()` of a tuple prints it.
pub fn repr(text: &str) -> String {
    let quote = crate::py_repr::repr_quote(text);
    let mut out = String::new();
    out.push(quote);
    for char in text.chars() {
        if !crate::py_repr::push_common_repr_escape(&mut out, char, quote) {
            out.push(char);
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_translated_construct_compiles() {
        for pattern in [
            r"[\&|]",
            r"[([{]",
            r"(?<![=!<>+\-*/%&|^:])=(?![=>])",
            r"\s*\{",
            r"\bfn\s+%s\s*<[^(]*>",
            r"(?:^|[,{}])\s*([^,{}=]*?)\s*=>",
            r"(?m)^// crate ([a-z_]+)$",
        ] {
            assert!(search(pattern, "x").is_none() || search(pattern, "x").is_some());
        }
        assert!(take_failure().is_none());
    }

    #[test]
    fn a_dollar_matches_before_a_final_newline() {
        assert_eq!(search(r"a$", "ba\n").map(|hit| hit.start()), Some(1));
        assert_eq!(search(r"a$", "ba").map(|hit| hit.start()), Some(1));
        assert!(search(r"a$", "ba\n\n").is_none());
    }

    #[test]
    fn a_leading_lookbehind_is_checked() {
        let hits: Vec<usize> = finditer(r"(?<![\w:])Json\b", "x::Json Json aJson")
            .iter()
            .map(M::start)
            .collect();
        assert_eq!(hits, vec![8]);
    }

    #[test]
    fn a_top_level_alternative_keeps_its_own_lookbehind() {
        let hits: Vec<usize> = finditer(r"(?<![\w:])ab|cd", "xab cd")
            .iter()
            .map(M::start)
            .collect();
        assert_eq!(hits, vec![4]);
    }

    #[test]
    fn empty_matches_follow_python() {
        let hits: Vec<&str> = finditer(r"a*", "baa").iter().map(M::whole).collect();
        assert_eq!(hits, vec!["", "aa", ""]);
    }

    #[test]
    fn a_template_expands_its_groups() {
        assert_eq!(sub(r"([<(])\s+", r"\1", "Vec< u8 >( a)"), "Vec<u8 >(a)");
    }

    #[test]
    fn balanced_returns_the_whole_text_without_a_match() {
        assert_eq!(paren("(a(b)c)d"), "(a(b)c)");
        assert_eq!(paren("(a"), "(a");
    }

    #[test]
    fn a_file_text_reads_with_universal_newlines() {
        assert_eq!(universal_newlines("a\r\nb\rc\n".to_string()), "a\nb\nc\n");
        assert_eq!(universal_newlines("a\nb".to_string()), "a\nb");
    }

    #[test]
    fn repr_quotes_as_python_does() {
        assert_eq!(repr("a"), "'a'");
        assert_eq!(repr("it's"), "\"it's\"");
    }

    #[test]
    fn repr_escapes_newline_cr_tab_and_a_bare_backslash() {
        assert_eq!(repr("a\nb"), "'a\\nb'");
        assert_eq!(repr("a\rb"), "'a\\rb'");
        assert_eq!(repr("a\tb"), "'a\\tb'");
        assert_eq!(repr("a\\b"), "'a\\\\b'");
    }

    #[test]
    fn repr_passes_other_control_characters_through_unescaped() {
        assert_eq!(repr("a\u{7}b"), "'a\u{7}b'");
    }

    #[test]
    fn a_literal_with_a_lookahead_matches_as_the_full_machine_does() {
        let text = "Q<T> x::Q(1) Q (2) aQ(3) Q::<u8>(4)";
        let pattern = r"(?<![\w:.])Q\b(?=\s*(?:::<|\())";
        assert!(compiled(pattern).literal.is_some());
        let fast: Vec<usize> = finditer(pattern, text).iter().map(M::start).collect();
        assert_eq!(fast, vec![13, 25]);
        let negative = r"(?<![\w:])Q\b(?!\s*<)";
        let found: Vec<usize> = finditer(negative, text).iter().map(M::start).collect();
        assert_eq!(found, vec![13, 25]);
    }

    #[test]
    fn a_literal_with_a_capture_in_its_lookahead_keeps_the_full_machine() {
        assert!(compiled(r"Q(?=(x))").literal.is_none());
        assert_eq!(search(r"Q(?=(x))", "Qy Qx").map(|hit| hit.start()), Some(3));
    }

    #[test]
    fn an_anchored_prefilter_rules_out_without_changing_a_match() {
        assert!(rmatch(r"\s*:(?!:)", "  :: x").is_none());
        assert_eq!(rmatch(r"\s*:(?!:)", "  : x").map(|hit| hit.end()), Some(3));
        assert!(rmatch(r"\s*:(?!:)", "x :").is_none());
        assert!(full(r"a(?=b)b", "ab"));
        assert!(!full(r"a(?=b)b", "ac"));
    }

    #[test]
    fn strip_lookarounds_removes_each_lookaround_group() {
        assert_eq!(
            strip_lookarounds(r"a(?=b(c))d(?<![x)])e(?!f)").as_deref(),
            Some("ade")
        );
    }
}
