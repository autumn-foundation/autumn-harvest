//! Python text semantics that the ported audits keep.
//!
//! The audits of this unit were Python scripts. Their findings depend on a few
//! details of Python text handling. This module reproduces those details, so
//! each port reads a file as its script did.
//!
//! - `Path.read_text` translates `\r\n` and `\r` to `\n`.
//! - `str.splitlines` splits on more characters than `\n`.
//! - `str.rstrip` strips the Unicode separators `\x1c` to `\x1f` as well.
//! - `repr` of a `str` picks its quote character from the content.

use std::fmt::Write as _;
use std::path::Path;

use smop::prelude::*;

/// Translates line ends as Python universal newlines do.
///
/// `Path.read_text` opens a file in text mode, so Python sees `\n` only.
pub fn universal_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

/// Reads a UTF-8 file as `Path.read_text`, or returns `None` for a non-file.
pub fn read_text(path: &Path) -> Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Some(universal_newlines(&text)))
}

/// Returns true for a character that ends a line in `str.splitlines`.
const fn is_line_boundary(c: char) -> bool {
    matches!(
        c,
        '\n' | '\r'
            | '\x0b'
            | '\x0c'
            | '\x1c'
            | '\x1d'
            | '\x1e'
            | '\u{85}'
            | '\u{2028}'
            | '\u{2029}'
    )
}

/// Splits `text` into lines as `str.splitlines` does.
///
/// A `\r\n` pair is one boundary. A trailing boundary adds no empty line.
pub fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = text.char_indices().peekable();
    while let Some((index, c)) = chars.next() {
        if !is_line_boundary(c) {
            continue;
        }
        lines.push(&text[start..index]);
        start = index + c.len_utf8();
        if c == '\r'
            && let Some(&(next, '\n')) = chars.peek()
        {
            chars.next();
            start = next + 1;
        }
    }
    if start < text.len() {
        lines.push(&text[start..]);
    }
    lines
}

/// Returns true for a character that `str.isspace` accepts.
fn is_py_space(c: char) -> bool {
    c.is_whitespace() || ('\x1c'..='\x1f').contains(&c)
}

/// Strips trailing whitespace as `str.rstrip()` does.
pub fn rstrip(text: &str) -> &str {
    text.trim_end_matches(is_py_space)
}

/// Strips both ends as `str.strip()` does.
pub fn strip(text: &str) -> &str {
    text.trim_matches(is_py_space)
}

/// Returns true for a character that `str.isprintable` accepts.
///
/// The test covers the controls, the separators and the format characters that
/// occur in practice. Rust has no Unicode category table in `std`, so a rare
/// unassigned code point counts as printable here.
fn is_py_printable(c: char) -> bool {
    let code = u32::from(c);
    !(code < 0x20
        || (0x7f..=0xa0).contains(&code)
        || code == 0xad
        || code == 0x1680
        || (0x2000..=0x200f).contains(&code)
        || (0x2028..=0x202f).contains(&code)
        || (0x205f..=0x2064).contains(&code)
        || code == 0x3000
        || code == 0xfeff
        || (0xd800..=0xdfff).contains(&code)
        || (0xe000..=0xf8ff).contains(&code))
}

/// Renders `text` as Python `repr` of a `str`.
pub fn repr_str(text: &str) -> String {
    let quote = if text.contains('\'') && !text.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(text.len() + 2);
    out.push(quote);
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if is_py_printable(c) => out.push(c),
            c => {
                let code = u32::from(c);
                if code < 0x100 {
                    let _ = write!(out, "\\x{code:02x}");
                } else if code < 0x1_0000 {
                    let _ = write!(out, "\\u{code:04x}");
                } else {
                    let _ = write!(out, "\\U{code:08x}");
                }
            }
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitlines_matches_python() {
        assert_eq!(splitlines("a\nb\n"), ["a", "b"]);
        assert_eq!(splitlines("a\r\nb\rc"), ["a", "b", "c"]);
        assert_eq!(splitlines("a\n\nb"), ["a", "", "b"]);
        assert_eq!(splitlines("a\x0cb\u{2028}c"), ["a", "b", "c"]);
        assert_eq!(splitlines(""), Vec::<&str>::new());
        assert_eq!(splitlines("\n"), [""]);
    }

    #[test]
    fn rstrip_strips_the_python_space_set() {
        assert_eq!(rstrip("## A \t\x1f"), "## A");
        assert_eq!(strip("  a  "), "a");
    }

    #[test]
    fn repr_str_matches_python() {
        assert_eq!(repr_str("on"), "'on'");
        assert_eq!(repr_str("it's"), "\"it's\"");
        assert_eq!(repr_str("a'\"b"), "'a\\'\"b'");
        assert_eq!(repr_str("a\nb\\"), "'a\\nb\\\\'");
        assert_eq!(repr_str("\x07\u{85}"), "'\\x07\\x85'");
        assert_eq!(repr_str("é"), "'é'");
        assert_eq!(repr_str("a\rb"), "'a\\rb'");
        assert_eq!(repr_str("a\tb"), "'a\\tb'");
    }

    #[test]
    fn universal_newlines_translates_every_line_end() {
        assert_eq!(universal_newlines("a\r\nb\rc\n"), "a\nb\nc\n");
    }
}
