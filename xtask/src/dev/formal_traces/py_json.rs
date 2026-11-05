//! The `json.loads` of `CPython` 3.12 for one trace line, and Python `repr`.
//!
//! `scripts/formal_traces.py` parsed each line with `json.loads`. `CPython`
//! 3.12 is the `python3` of the `ubuntu-24.04` runner that ran it. This
//! decoder follows the C scanner of that release. So each trace gets the
//! same verdict, and each error names the same defect at the same place.
//!
//! The decoder keeps these Python behaviors:
//!
//! - `NaN`, `Infinity` and `-Infinity` are numbers.
//! - An integer has no size bound, and `-0` is the integer 0.
//! - A lone surrogate escape such as `\ud800` stays in the string.
//! - A repeated key keeps its first place and its last value.
//! - An error message holds the text and the position of the Python one.
//!
//! `CPython` 3.13 changed some messages, for example for a trailing comma.
//! This module follows 3.12.

use std::fmt::Write as _;
use std::sync::LazyLock;

use regex::Regex;

/// A JSON value as Python's `json.loads` returns it.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    /// `None`.
    Null,
    /// `True` or `False`.
    Bool(bool),
    /// An `int` that fits in an `i128`.
    Int(i128),
    /// An `int` that does not fit in an `i128`, as its decimal text.
    BigInt(String),
    /// A `float`.
    Float(f64),
    /// A `str`.
    Str(PyStr),
    /// A `list`.
    List(Vec<Self>),
    /// A `dict`, in the order of its keys.
    Dict(Vec<(PyStr, Self)>),
}

impl Json {
    /// The value under `key` of a dict, or `None`.
    pub fn get(&self, key: &str) -> Option<&Self> {
        match self {
            Self::Dict(items) => items
                .iter()
                .find(|(k, _)| k.as_str() == Some(key))
                .map(|(_, v)| v),
            _ => None,
        }
    }

    /// The value under `key`, with `Null` for a missing key or a non-dict.
    pub fn field(&self, key: &str) -> &Self {
        self.get(key).unwrap_or(&Self::Null)
    }

    /// The text of a `Str` that holds no lone surrogate.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(text) => text.as_str(),
            _ => None,
        }
    }

    /// Python's `repr` of the value.
    pub fn repr(&self) -> String {
        match self {
            Self::Null => "None".to_string(),
            Self::Bool(true) => "True".to_string(),
            Self::Bool(false) => "False".to_string(),
            Self::Int(n) => n.to_string(),
            Self::BigInt(text) => text.clone(),
            Self::Float(x) => float_repr(*x),
            Self::Str(text) => text.repr(),
            Self::List(items) => {
                let items: Vec<String> = items.iter().map(Self::repr).collect();
                format!("[{}]", items.join(", "))
            }
            Self::Dict(items) => {
                let items: Vec<String> = items
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k.repr(), v.repr()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}

/// A Python `str`, which can hold a lone surrogate.
///
/// The bytes are WTF-8: UTF-8 that also encodes a lone surrogate. The
/// decoder joins each surrogate pair, so equal strings have equal bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PyStr(Vec<u8>);

impl PyStr {
    /// The string of these code points.
    fn from_code_points(points: &[u32]) -> Self {
        let mut bytes = Vec::with_capacity(points.len());
        for &point in points {
            if let Some(c) = char::from_u32(point) {
                let mut buffer = [0_u8; 4];
                bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
            } else {
                // A surrogate. Encode it as UTF-8 would encode any code
                // point of the same size.
                bytes.push(0xe0 | ((point >> 12) & 0x0f) as u8);
                bytes.push(0x80 | ((point >> 6) & 0x3f) as u8);
                bytes.push(0x80 | (point & 0x3f) as u8);
            }
        }
        Self(bytes)
    }

    /// The text, or `None` when the string holds a lone surrogate.
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.0).ok()
    }

    /// The code points of the string.
    fn code_points(&self) -> Vec<u32> {
        let mut points = Vec::new();
        let mut at = 0;
        while let Some(&lead) = self.0.get(at) {
            let (size, bits) = match lead {
                0x00..=0x7f => (1, u32::from(lead)),
                0xc0..=0xdf => (2, u32::from(lead & 0x1f)),
                0xe0..=0xef => (3, u32::from(lead & 0x0f)),
                _ => (4, u32::from(lead & 0x07)),
            };
            let tail = self.0.get(at + 1..at + size).unwrap_or_default();
            let point = tail
                .iter()
                .fold(bits, |acc, b| (acc << 6) | u32::from(b & 0x3f));
            points.push(point);
            at += size;
        }
        points
    }

    /// Python's `repr` of the string.
    pub fn repr(&self) -> String {
        repr_code_points(&self.code_points())
    }
}

/// Python's `repr` of a `str` with no lone surrogate.
pub fn str_repr(text: &str) -> String {
    let points: Vec<u32> = text.chars().map(u32::from).collect();
    repr_code_points(&points)
}

/// Python's `repr` of a string of code points.
///
/// The quote is `'` unless the text holds `'` and no `"`. A character that
/// `str.isprintable` rejects goes out as an escape.
fn repr_code_points(points: &[u32]) -> String {
    let single = u32::from('\'');
    let double = u32::from('"');
    let quote = if points.contains(&single) && !points.contains(&double) {
        '"'
    } else {
        '\''
    };
    let mut out = String::new();
    out.push(quote);
    for &point in points {
        match char::from_u32(point) {
            Some('\\') => out.push_str("\\\\"),
            Some('\t') => out.push_str("\\t"),
            Some('\n') => out.push_str("\\n"),
            Some('\r') => out.push_str("\\r"),
            Some(c) if c == quote => {
                out.push('\\');
                out.push(c);
            }
            Some(c) if is_printable(c) => out.push(c),
            _ => {
                if point < 0x100 {
                    let _ = write!(out, "\\x{point:02x}");
                } else if point < 0x1_0000 {
                    let _ = write!(out, "\\u{point:04x}");
                } else {
                    let _ = write!(out, "\\U{point:08x}");
                }
            }
        }
    }
    out.push(quote);
    out
}

/// The characters that Python's `str.isprintable` rejects.
///
/// They are the categories Other and Separator, except the space. A `char`
/// is never a surrogate, so the class leaves out `Cs`. The caller escapes a
/// surrogate itself. The Unicode tables of `regex` can be newer than those
/// of Python 3.12. So a code point that a later Unicode release assigns can
/// differ.
static NOT_PRINTABLE: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"[\p{Cc}\p{Cf}\p{Co}\p{Cn}\p{Zl}\p{Zp}\p{Zs}]").ok());

/// True for a character that Python's `repr` prints as it is.
fn is_printable(c: char) -> bool {
    if c == ' ' {
        return true;
    }
    let mut buffer = [0_u8; 4];
    NOT_PRINTABLE
        .as_ref()
        .is_none_or(|re| !re.is_match(c.encode_utf8(&mut buffer)))
}

/// Python's `repr` of a float.
///
/// Python prints the shortest digits that round trip. It uses an exponent
/// when the exponent is below -4 or at least 16.
pub fn float_repr(x: f64) -> String {
    if x.is_nan() {
        return "nan".to_string();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    let sci = format!("{x:e}");
    let (mantissa, exponent) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exponent: i32 = exponent.parse().unwrap_or(0);
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if (-4..16).contains(&exponent) {
        let point = exponent + 1;
        let text = if point <= 0 {
            let zeros = "0".repeat(usize::try_from(-point).unwrap_or(0));
            format!("0.{zeros}{digits}")
        } else {
            let point = usize::try_from(point).unwrap_or(0);
            if digits.len() <= point {
                format!("{digits}{}.0", "0".repeat(point - digits.len()))
            } else {
                format!("{}.{}", &digits[..point], &digits[point..])
            }
        };
        return format!("{sign}{text}");
    }
    let (head, tail) = digits.split_at(1);
    let body = if tail.is_empty() {
        head.to_string()
    } else {
        format!("{head}.{tail}")
    };
    let mark = if exponent < 0 { '-' } else { '+' };
    format!("{sign}{body}e{mark}{:02}", exponent.abs())
}

/// Why a line does not decode.
#[derive(Debug, PartialEq, Eq)]
pub enum Decode {
    /// A `JSONDecodeError`, with the text that Python gives it.
    Invalid(String),
    /// Another exception of Python. The script stopped with a traceback.
    Crash(String),
}

/// The deepest nesting that the decoder takes.
///
/// `CPython` stops deep nesting with a `RecursionError`. Its bound depends on
/// the stack, so this bound is close to it, not equal.
const MAX_DEPTH: usize = 1000;

/// The most digits that `CPython` 3.12 converts to an `int`.
const MAX_INT_DIGITS: usize = 4300;

/// A failure inside the scanner.
enum Fail {
    /// Python's `StopIteration`, which becomes "Expecting value" here.
    Stop(usize),
    /// A `JSONDecodeError` with this message at this position.
    At(&'static str, usize),
    /// Another exception.
    Crash(String),
}

/// Decodes one line as Python's `json.loads` does.
pub fn loads(text: &str) -> Result<Json, Decode> {
    let s: Vec<char> = text.chars().collect();
    if s.first() == Some(&'\u{feff}') {
        return Err(Decode::Invalid(message(
            "Unexpected UTF-8 BOM (decode using utf-8-sig)",
            &s,
            0,
        )));
    }
    let scanner = Scanner { s: &s };
    let start = scanner.skip_space(0);
    let fail = |fail: Fail| match fail {
        Fail::Stop(at) => Decode::Invalid(message("Expecting value", &s, at)),
        Fail::At(text, at) => Decode::Invalid(message(text, &s, at)),
        Fail::Crash(text) => Decode::Crash(text),
    };
    let (value, end) = scanner.scan_once(start, 0).map_err(fail)?;
    let end = scanner.skip_space(end);
    if end != s.len() {
        return Err(Decode::Invalid(message("Extra data", &s, end)));
    }
    Ok(value)
}

/// The text of a `JSONDecodeError`: the message and its line and column.
fn message(text: &str, s: &[char], at: usize) -> String {
    let before = &s[..at.min(s.len())];
    let line = before.iter().filter(|c| **c == '\n').count() + 1;
    let column = before
        .iter()
        .rposition(|c| *c == '\n')
        .map_or(at + 1, |newline| at - newline);
    format!("{text}: line {line} column {column} (char {at})")
}

/// The scanner of one decoded line.
struct Scanner<'a> {
    s: &'a [char],
}

impl Scanner<'_> {
    /// The first position at or after `at` that is not JSON white space.
    fn skip_space(&self, mut at: usize) -> usize {
        while self
            .s
            .get(at)
            .is_some_and(|c| matches!(c, ' ' | '\t' | '\n' | '\r'))
        {
            at += 1;
        }
        at
    }

    /// True when the text at `at` starts with `word`.
    fn has(&self, at: usize, word: &str) -> bool {
        word.chars()
            .enumerate()
            .all(|(i, c)| self.s.get(at + i) == Some(&c))
    }

    /// Scans one value at `idx`, and returns it with the position after it.
    fn scan_once(&self, idx: usize, depth: usize) -> Result<(Json, usize), Fail> {
        let len = self.s.len();
        let Some(&c) = self.s.get(idx) else {
            return Err(Fail::Stop(idx));
        };
        match c {
            '"' => {
                let (text, end) = self.scan_string(idx + 1)?;
                Ok((Json::Str(text), end))
            }
            '{' => Self::nested(depth, "object", |d| self.object(idx + 1, d)),
            '[' => Self::nested(depth, "array", |d| self.array(idx + 1, d)),
            'n' if idx + 3 < len && self.has(idx, "null") => Ok((Json::Null, idx + 4)),
            't' if idx + 3 < len && self.has(idx, "true") => Ok((Json::Bool(true), idx + 4)),
            'f' if idx + 4 < len && self.has(idx, "false") => Ok((Json::Bool(false), idx + 5)),
            'N' if idx + 2 < len && self.has(idx, "NaN") => Ok((Json::Float(f64::NAN), idx + 3)),
            'I' if idx + 7 < len && self.has(idx, "Infinity") => {
                Ok((Json::Float(f64::INFINITY), idx + 8))
            }
            '-' if idx + 8 < len && self.has(idx, "-Infinity") => {
                Ok((Json::Float(f64::NEG_INFINITY), idx + 9))
            }
            _ => self.number(idx),
        }
    }

    /// Runs `scan` one level deeper, or fails as Python's recursion bound.
    fn nested(
        depth: usize,
        what: &str,
        scan: impl FnOnce(usize) -> Result<(Json, usize), Fail>,
    ) -> Result<(Json, usize), Fail> {
        if depth >= MAX_DEPTH {
            return Err(Fail::Crash(format!(
                "RecursionError: maximum recursion depth exceeded while decoding a JSON {what} \
                 from a unicode string"
            )));
        }
        scan(depth + 1)
    }

    /// Scans a number at `start`, as `_match_number_unicode` does.
    fn number(&self, start: usize) -> Result<(Json, usize), Fail> {
        let s = self.s;
        let digit = |at: usize| s.get(at).is_some_and(char::is_ascii_digit);
        let end_idx = s.len().saturating_sub(1);
        let mut idx = start;
        if s.get(idx) == Some(&'-') {
            idx += 1;
            if idx > end_idx {
                return Err(Fail::Stop(start));
            }
        }
        match s.get(idx) {
            Some('1'..='9') => {
                idx += 1;
                while idx <= end_idx && digit(idx) {
                    idx += 1;
                }
            }
            Some('0') => idx += 1,
            _ => return Err(Fail::Stop(start)),
        }
        let mut is_float = false;
        if idx < end_idx && s[idx] == '.' && digit(idx + 1) {
            is_float = true;
            idx += 2;
            while idx <= end_idx && digit(idx) {
                idx += 1;
            }
        }
        if idx < end_idx && matches!(s[idx], 'e' | 'E') {
            let e_start = idx;
            idx += 1;
            if idx < end_idx && matches!(s[idx], '-' | '+') {
                idx += 1;
            }
            while idx <= end_idx && digit(idx) {
                idx += 1;
            }
            if digit(idx - 1) {
                is_float = true;
            } else {
                idx = e_start;
            }
        }
        let text: String = s[start..idx].iter().collect();
        if is_float {
            return Ok((Json::Float(text.parse().unwrap_or(f64::NAN)), idx));
        }
        let digits = text.trim_start_matches('-').len();
        if digits > MAX_INT_DIGITS {
            return Err(Fail::Crash(format!(
                "ValueError: Exceeds the limit ({MAX_INT_DIGITS} digits) for integer string \
                 conversion: value has {digits} digits; use sys.set_int_max_str_digits() to \
                 increase the limit"
            )));
        }
        let value = text
            .parse::<i128>()
            .map_or_else(|_| Json::BigInt(text.clone()), Json::Int);
        Ok((value, idx))
    }

    /// Scans an object whose `{` comes just before `idx`.
    fn object(&self, mut idx: usize, depth: usize) -> Result<(Json, usize), Fail> {
        let s = self.s;
        let mut items: Vec<(PyStr, Json)> = Vec::new();
        idx = self.skip_space(idx);
        if s.get(idx) != Some(&'}') {
            loop {
                if s.get(idx) != Some(&'"') {
                    return Err(Fail::At(
                        "Expecting property name enclosed in double quotes",
                        idx,
                    ));
                }
                let (key, next) = self.scan_string(idx + 1)?;
                idx = self.skip_space(next);
                if s.get(idx) != Some(&':') {
                    return Err(Fail::At("Expecting ':' delimiter", idx));
                }
                idx = self.skip_space(idx + 1);
                let (value, next) = self.scan_once(idx, depth)?;
                // A Python dict keeps the first place of a key and its last
                // value.
                if let Some(slot) = items.iter_mut().find(|(k, _)| *k == key) {
                    slot.1 = value;
                } else {
                    items.push((key, value));
                }
                idx = self.skip_space(next);
                if s.get(idx) == Some(&'}') {
                    break;
                }
                if s.get(idx) != Some(&',') {
                    return Err(Fail::At("Expecting ',' delimiter", idx));
                }
                idx = self.skip_space(idx + 1);
            }
        }
        Ok((Json::Dict(items), idx + 1))
    }

    /// Scans an array whose `[` comes just before `idx`.
    fn array(&self, mut idx: usize, depth: usize) -> Result<(Json, usize), Fail> {
        let s = self.s;
        let mut items = Vec::new();
        idx = self.skip_space(idx);
        if s.get(idx) != Some(&']') {
            loop {
                let (value, next) = self.scan_once(idx, depth)?;
                items.push(value);
                idx = self.skip_space(next);
                if s.get(idx) == Some(&']') {
                    break;
                }
                if s.get(idx) != Some(&',') {
                    return Err(Fail::At("Expecting ',' delimiter", idx));
                }
                idx = self.skip_space(idx + 1);
            }
        }
        Ok((Json::List(items), idx + 1))
    }

    /// Scans a string whose quote comes just before `end`.
    ///
    /// This follows `scanstring_unicode` of `CPython` 3.12 in strict mode.
    fn scan_string(&self, mut end: usize) -> Result<(PyStr, usize), Fail> {
        let s = self.s;
        let len = s.len();
        let begin = end - 1;
        let mut points: Vec<u32> = Vec::new();
        loop {
            let mut next = end;
            let mut c = '\0';
            while next < len {
                c = s[next];
                if c == '"' || c == '\\' {
                    break;
                }
                if u32::from(c) <= 0x1f {
                    return Err(Fail::At("Invalid control character at", next));
                }
                next += 1;
            }
            if c != '"' && c != '\\' {
                return Err(Fail::At("Unterminated string starting at", begin));
            }
            points.extend(s[end..next].iter().map(|c| u32::from(*c)));
            next += 1;
            if c == '"' {
                end = next;
                break;
            }
            if next == len {
                return Err(Fail::At("Unterminated string starting at", begin));
            }
            if s[next] == 'u' {
                next += 1;
                end = next + 4;
                if end >= len {
                    return Err(Fail::At("Invalid \\uXXXX escape", next - 1));
                }
                let mut point =
                    hex4(&s[next..end]).ok_or(Fail::At("Invalid \\uXXXX escape", end - 5))?;
                next = end;
                // A high surrogate joins the low surrogate of the next escape.
                if (0xd800..=0xdbff).contains(&point)
                    && end + 6 < len
                    && s[next] == '\\'
                    && s[next + 1] == 'u'
                {
                    next += 2;
                    end += 6;
                    let low =
                        hex4(&s[next..end]).ok_or(Fail::At("Invalid \\uXXXX escape", end - 5))?;
                    if (0xdc00..=0xdfff).contains(&low) {
                        point = 0x1_0000 + (((point - 0xd800) << 10) | (low - 0xdc00));
                    } else {
                        end -= 6;
                    }
                }
                points.push(point);
            } else {
                end = next + 1;
                let point = match s[next] {
                    '"' => '"',
                    '\\' => '\\',
                    '/' => '/',
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    _ => return Err(Fail::At("Invalid \\escape", end - 2)),
                };
                points.push(u32::from(point));
            }
        }
        Ok((PyStr::from_code_points(&points), end))
    }
}

/// The value of four hex digits, or `None`.
fn hex4(digits: &[char]) -> Option<u32> {
    digits
        .iter()
        .try_fold(0, |acc, c| c.to_digit(16).map(|d| (acc << 4) | d))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `str(err)` of `CPython` 3.12, or the `repr` of the value.
    fn outcome(text: &str) -> String {
        match loads(text) {
            Ok(value) => format!("OK {}", value.repr()),
            Err(Decode::Invalid(message)) => message,
            Err(Decode::Crash(message)) => format!("CRASH {message}"),
        }
    }

    #[test]
    fn errors_match_python_3_12() {
        // Each expected text comes from `/usr/bin/python3.12`.
        let cases = [
            ("[", "Expecting value: line 1 column 2 (char 1)"),
            (
                "{",
                "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
            ),
            ("[1,]", "Expecting value: line 1 column 4 (char 3)"),
            (
                "{\"a\":1,}",
                "Expecting property name enclosed in double quotes: line 1 column 8 (char 7)",
            ),
            (
                "\"\\u0041",
                "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
            ),
            (
                "\"\\u00",
                "Invalid \\uXXXX escape: line 1 column 3 (char 2)",
            ),
            (
                "{\"a\" 1}",
                "Expecting ':' delimiter: line 1 column 6 (char 5)",
            ),
            ("[1 2]", "Expecting ',' delimiter: line 1 column 4 (char 3)"),
            ("\"\\x\"", "Invalid \\escape: line 1 column 2 (char 1)"),
            (
                "\"a\u{1}\"",
                "Invalid control character at: line 1 column 3 (char 2)",
            ),
            ("-", "Expecting value: line 1 column 1 (char 0)"),
            ("01", "Extra data: line 1 column 2 (char 1)"),
            ("1.", "Extra data: line 1 column 2 (char 1)"),
            (" ", "Expecting value: line 1 column 2 (char 1)"),
            ("", "Expecting value: line 1 column 1 (char 0)"),
            (
                "\u{feff}1",
                "Unexpected UTF-8 BOM (decode using utf-8-sig): line 1 column 1 (char 0)",
            ),
            ("nul", "Expecting value: line 1 column 1 (char 0)"),
            ("{\"a\":", "Expecting value: line 1 column 6 (char 5)"),
            (
                "\"\\ud800\\u",
                "Invalid \\uXXXX escape: line 1 column 9 (char 8)",
            ),
            (
                "\"\\ud83d\\u00\"",
                "Invalid \\uXXXX escape: line 1 column 9 (char 8)",
            ),
            (
                "\"abc",
                "Unterminated string starting at: line 1 column 1 (char 0)",
            ),
            ("-NaN", "Expecting value: line 1 column 1 (char 0)"),
            (
                "{\"é\":1 \"b\":2}",
                "Expecting ',' delimiter: line 1 column 8 (char 7)",
            ),
        ];
        for (text, want) in cases {
            assert_eq!(outcome(text), want, "{text:?}");
        }
    }

    #[test]
    fn values_match_python_3_12() {
        let cases = [
            ("-0", "OK 0"),
            ("-0.0", "OK -0.0"),
            ("1E+2", "OK 100.0"),
            ("NaN", "OK nan"),
            ("-Infinity", "OK -inf"),
            (
                "123456789012345678901234567890123456789012",
                "OK 123456789012345678901234567890123456789012",
            ),
            ("\"\\ud800\"", "OK '\\ud800'"),
            ("\"\\ud83d\\ude00\"", "OK '\u{1f600}'"),
            ("\"\\ud83d\"", "OK '\\ud83d'"),
            ("{\"b\":1,\"a\":2,\"b\":3}", "OK {'b': 3, 'a': 2}"),
            (
                "[true, false, null, \"it's\", \"\u{a0}\"]",
                "OK [True, False, None, \"it's\", '\\xa0']",
            ),
        ];
        for (text, want) in cases {
            assert_eq!(outcome(text), want, "{text:?}");
        }
    }

    #[test]
    fn a_lone_surrogate_is_not_text() {
        let Ok(Json::Str(text)) = loads("\"a\\udc00\"") else {
            panic!("want a string");
        };
        assert_eq!(text.as_str(), None);
        assert_ne!(Ok(Json::Str(text)), loads("\"a\\udc01\""));
    }

    #[test]
    fn deep_nesting_and_long_ints_crash_as_python_does() {
        let deep = "[".repeat(MAX_DEPTH + 1);
        assert!(outcome(&deep).starts_with("CRASH RecursionError"));
        let long = "7".repeat(MAX_INT_DIGITS + 1);
        assert!(outcome(&long).starts_with("CRASH ValueError"));
    }

    #[test]
    fn the_printable_class_compiles() {
        assert!(NOT_PRINTABLE.is_some());
    }

    #[test]
    fn repr_matches_python() {
        assert_eq!(str_repr("tab\there\u{1}\u{a0}é"), r"'tab\there\x01\xa0é'");
        assert_eq!(
            str_repr("\u{378}\u{e000}\u{f0000}\u{200b}"),
            r"'\u0378\ue000\U000f0000\u200b'"
        );
        assert_eq!(float_repr(1.0), "1.0");
        assert_eq!(float_repr(0.1), "0.1");
        assert_eq!(float_repr(1e16), "1e+16");
        assert_eq!(float_repr(1e15), "1000000000000000.0");
        assert_eq!(float_repr(0.0001), "0.0001");
        assert_eq!(float_repr(0.00001), "1e-05");
        assert_eq!(float_repr(-1.5e-7), "-1.5e-07");
        assert_eq!(float_repr(123.456), "123.456");
    }
}
