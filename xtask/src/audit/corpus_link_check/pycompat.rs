//! Text helpers that keep the docs-corpus audits byte-compatible with their Python originals.
//!
//! The audits were Python scripts. Their findings depend on Python string and
//! regex semantics, so a port with Rust defaults reports different results.
//! Each helper here reproduces one Python behaviour:
//!
//! - `\s` in Python also matches the four separators U+001C to U+001F.
//! - `\w` in Python matches letters, numbers and `_`, but not combining marks.
//! - `\b` in Python uses that `\w` class, not the Rust one.
//! - `str.splitlines` breaks on more than `\n`.
//! - `Path.read_text` translates `\r\n` and `\r` to `\n`.
//! - `Path.resolve` resolves symbolic links before it applies `..`.
//! - `json.dumps(indent=2)` escapes each non-ASCII character.
//! - `repr(str)` picks its quote and escapes from the content.
//!
//! The `audit` group shares this module through `corpus_link_check`.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::path::{Component, Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;
use smop::prelude::*;

/// Reports whether `c` is whitespace for Python `str.isspace` and regex `\s`.
pub fn is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Reports whether `c` is a word character for Python regex `\w`.
pub fn is_word(c: char) -> bool {
    static WORD: OnceLock<Option<Regex>> = OnceLock::new();
    if c == '_' || c.is_ascii_alphanumeric() {
        return true;
    }
    if c.is_ascii() {
        return false;
    }
    WORD.get_or_init(|| Regex::new(r"^[\p{L}\p{N}]$").ok())
        .as_ref()
        .is_some_and(|re| {
            let mut buf = [0u8; 4];
            re.is_match(c.encode_utf8(&mut buf))
        })
}

/// Returns `s` without leading and trailing whitespace, as Python `str.strip()`.
pub fn strip(s: &str) -> &str {
    s.trim_matches(is_space)
}

/// Returns `s` without leading whitespace, as Python `str.lstrip()`.
pub fn lstrip(s: &str) -> &str {
    s.trim_start_matches(is_space)
}

/// Returns `s` without trailing whitespace, as Python `str.rstrip()`.
pub fn rstrip(s: &str) -> &str {
    s.trim_end_matches(is_space)
}

/// Splits `s` at each line boundary that Python `str.splitlines` knows.
///
/// A final line break does not add an empty last line.
pub fn splitlines(s: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        let is_break = matches!(
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
        if !is_break {
            continue;
        }
        lines.push(&s[start..i]);
        let mut end = i + c.len_utf8();
        if c == '\r'
            && let Some(&(j, '\n')) = chars.peek()
        {
            end = j + 1;
            chars.next();
        }
        start = end;
    }
    if start < s.len() {
        lines.push(&s[start..]);
    }
    lines
}

/// Applies the universal-newline translation of Python text mode.
fn universal_newlines(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

/// Reads a file as Python `read_text(encoding="utf-8")` does.
///
/// Invalid UTF-8 is an error, as it is in Python.
pub fn read_text(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let text = String::from_utf8(bytes).with_context(|| format!("decoding {}", path.display()))?;
    Ok(universal_newlines(&text))
}

/// Reads a file as Python `read_text(encoding="utf-8", errors="replace")` does.
pub fn read_text_lossy(path: &Path) -> Result<String> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(universal_newlines(&String::from_utf8_lossy(&bytes)))
}

/// Compiles a Python regex pattern with Python meanings for `\s`, `\S`, `\w` and `\W`.
///
/// The replacement is a bracketed class. Rust regex accepts a nested class,
/// so the replacement also works inside `[...]`.
pub fn regex(pattern: &str) -> Result<Regex> {
    let mut out = String::with_capacity(pattern.len() + 16);
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('s') => out.push_str(r"[\s\x1C-\x1F]"),
            Some('S') => out.push_str(r"[^\s\x1C-\x1F]"),
            Some('w') => out.push_str(r"[\p{L}\p{N}_]"),
            Some('W') => out.push_str(r"[^\p{L}\p{N}_]"),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    Regex::new(&out).with_context(|| format!("compiling regex {pattern}"))
}

/// Reports whether `c` is a word character for the `\w` class of the Rust regex crate.
fn is_rust_word(c: char) -> bool {
    static WORD: OnceLock<Option<Regex>> = OnceLock::new();
    WORD.get_or_init(|| Regex::new(r"^\w$").ok())
        .as_ref()
        .is_some_and(|re| {
            let mut buf = [0u8; 4];
            re.is_match(c.encode_utf8(&mut buf))
        })
}

/// Returns each match of `re` in `text`, with the `\b` word boundary of Python.
///
/// A Rust `\b` uses the Rust `\w` class. That class also holds combining
/// marks, connector punctuation and joiners. The Python class excludes them,
/// but it holds other numbers, such as `²`. So `50 variants` and a combining
/// mark after it match in Python only.
///
/// The search runs on a copy of `text`. In the copy, each character whose
/// word status differs is replaced. The substitute has the Python status. A
/// Python word character becomes `ꙮ`, and any other character becomes
/// U+FFFC. The pattern must not hold either substitute.
pub fn find_iter_python_words<'h>(re: &Regex, text: &'h str) -> Vec<&'h str> {
    let mut shadow = String::with_capacity(text.len());
    // `starts[k]` is the original offset of the copy offset `k`. A substitute
    // takes 3 bytes, so the copy is at most 3 times as long as `text`.
    let mut starts = vec![usize::MAX; text.len() * 3 + 1];
    for (i, c) in text.char_indices() {
        let python = is_word(c);
        let sub = if python == is_rust_word(c) {
            c
        } else if python {
            '\u{a66e}'
        } else {
            '\u{fffc}'
        };
        starts[shadow.len()] = i;
        shadow.push(sub);
    }
    starts[shadow.len()] = text.len();
    re.find_iter(&shadow)
        .filter_map(|m| {
            let (s, e) = (starts[m.start()], starts[m.end()]);
            (s != usize::MAX && e != usize::MAX).then(|| &text[s..e])
        })
        .collect()
}

/// Resolves `path` as Python `Path.resolve()` does in its non-strict mode.
///
/// The walk follows `posixpath.realpath`. Each symbolic link is read before a
/// later `..` applies. A missing component stays as written.
pub fn resolve(path: &Path) -> PathBuf {
    let base = if path.is_absolute() {
        PathBuf::from("/")
    } else {
        std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"))
    };
    let mut seen = HashMap::new();
    join_realpath(base, path, &mut seen, 0).0
}

/// Joins `rest` onto the resolved `base`, resolving each link on the way.
fn join_realpath(
    mut base: PathBuf,
    rest: &Path,
    seen: &mut HashMap<PathBuf, Option<PathBuf>>,
    depth: usize,
) -> (PathBuf, bool) {
    if rest.is_absolute() {
        base = PathBuf::from("/");
    }
    let names: Vec<Component<'_>> = rest
        .components()
        .filter(|c| {
            !matches!(
                c,
                Component::RootDir | Component::CurDir | Component::Prefix(_)
            )
        })
        .collect();
    for (i, component) in names.iter().enumerate() {
        let name: OsString = match component {
            Component::ParentDir => {
                base.pop();
                continue;
            }
            Component::Normal(name) => (*name).to_owned(),
            _ => continue,
        };
        let newpath = base.join(&name);
        let is_link = std::fs::symlink_metadata(&newpath).is_ok_and(|m| m.file_type().is_symlink());
        if !is_link {
            base = newpath;
            continue;
        }
        let remaining: PathBuf = names[i + 1..].iter().collect();
        match seen.get(&newpath) {
            Some(Some(done)) => {
                base.clone_from(done);
                continue;
            }
            Some(None) => return (newpath.join(remaining), false),
            None => {}
        }
        let Ok(target) = std::fs::read_link(&newpath) else {
            base = newpath;
            continue;
        };
        if depth > 64 {
            return (newpath.join(remaining), false);
        }
        seen.insert(newpath.clone(), None);
        let (resolved, ok) = join_realpath(base.clone(), &target, seen, depth + 1);
        if !ok {
            return (resolved.join(remaining), false);
        }
        seen.insert(newpath, Some(resolved.clone()));
        base = resolved;
    }
    (base, true)
}

/// Returns the repository root as Python `Path(__file__).resolve()` gave it.
pub fn resolved_root(root: &Path) -> PathBuf {
    resolve(root)
}

/// Returns `path` relative to `root` as a POSIX string, as `str(p.relative_to(root))`.
pub fn rel(path: &Path, root: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let parts: Vec<String> = relative
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

/// A JSON value printed as Python `json.dumps(value, indent=2)` prints it.
#[derive(Debug, Clone)]
pub enum Json {
    /// A non-negative integer.
    Int(usize),
    /// A string.
    Str(String),
    /// An array.
    Arr(Vec<Self>),
    /// An object, with its keys in insertion order.
    Obj(Vec<(String, Self)>),
}

impl Json {
    /// Builds an array of strings.
    pub fn strings<I, S>(items: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::Arr(items.into_iter().map(|s| Self::Str(s.into())).collect())
    }

    /// Renders the value with a two-space indent and ASCII-only escapes.
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out
    }

    fn write(&self, out: &mut String, level: usize) {
        match self {
            Self::Int(n) => {
                let _ = write!(out, "{n}");
            }
            Self::Str(s) => write_json_string(out, s),
            Self::Arr(items) if items.is_empty() => out.push_str("[]"),
            Self::Obj(items) if items.is_empty() => out.push_str("{}"),
            Self::Arr(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    push_indent(out, level + 1);
                    item.write(out, level + 1);
                }
                out.push('\n');
                push_indent(out, level);
                out.push(']');
            }
            Self::Obj(items) => {
                out.push('{');
                for (i, (key, value)) in items.iter().enumerate() {
                    out.push_str(if i == 0 { "\n" } else { ",\n" });
                    push_indent(out, level + 1);
                    write_json_string(out, key);
                    out.push_str(": ");
                    value.write(out, level + 1);
                }
                out.push('\n');
                push_indent(out, level);
                out.push('}');
            }
        }
    }
}

fn push_indent(out: &mut String, level: usize) {
    for _ in 0..level {
        out.push_str("  ");
    }
}

/// Writes a JSON string with the escapes of Python `ensure_ascii=True`.
fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

/// Reports whether Python `str.isprintable` holds for one character.
fn is_printable(c: char) -> bool {
    static NON_PRINTABLE: OnceLock<Option<Regex>> = OnceLock::new();
    if c == ' ' {
        return true;
    }
    if c.is_ascii() {
        return c.is_ascii_graphic();
    }
    NON_PRINTABLE
        .get_or_init(|| Regex::new(r"^[\p{Cc}\p{Cf}\p{Co}\p{Cn}\p{Zl}\p{Zp}\p{Zs}]$").ok())
        .as_ref()
        .is_some_and(|re| {
            let mut buf = [0u8; 4];
            !re.is_match(c.encode_utf8(&mut buf))
        })
}

/// Returns the Python `repr()` of a string.
pub fn repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if c == quote => {
                out.push('\\');
                out.push(c);
            }
            _ if is_printable(c) => out.push(c),
            _ => {
                let code = u32::from(c);
                let _ = if code < 0x100 {
                    write!(out, "\\x{code:02x}")
                } else if code < 0x1_0000 {
                    write!(out, "\\u{code:04x}")
                } else {
                    write!(out, "\\U{code:08x}")
                };
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
        assert_eq!(
            splitlines("a\nb\r\nc\rd\u{2028}e\n"),
            ["a", "b", "c", "d", "e"]
        );
        assert_eq!(splitlines("a\n\nb"), ["a", "", "b"]);
        assert_eq!(splitlines(""), [] as [&str; 0]);
        assert_eq!(splitlines("\n"), [""]);
    }

    #[test]
    fn whitespace_includes_the_python_separators() {
        assert!(is_space('\u{1c}'));
        assert!(is_space('\u{a0}'));
        assert!(!is_space('x'));
        assert_eq!(strip("\u{1f} a \t"), "a");
    }

    #[test]
    fn word_class_follows_python() {
        assert!(is_word('é'));
        assert!(is_word('٣'));
        assert!(!is_word('\u{301}'));
        assert!(!is_word('—'));
    }

    #[test]
    fn regex_translation_keeps_python_classes() {
        let re = regex(r"^[\w-]+\s$").unwrap();
        assert!(re.is_match("a-b\u{1c}"));
        assert!(!re.is_match("a\u{301}\u{1c}"));
        let re = regex(r"\\s").unwrap();
        assert!(re.is_match("\\s"));
    }

    #[test]
    fn json_matches_python_dumps() {
        let value = Json::Obj(vec![
            ("n".into(), Json::Int(3)),
            ("e".into(), Json::Arr(vec![])),
            ("s".into(), Json::strings(["a—\"b\"", "😀"])),
        ]);
        assert_eq!(
            value.render(),
            "{\n  \"n\": 3,\n  \"e\": [],\n  \"s\": [\n    \"a\\u2014\\\"b\\\"\",\n    \"\\ud83d\\ude00\"\n  ]\n}"
        );
    }

    #[test]
    fn repr_matches_python() {
        assert_eq!(repr("a"), "'a'");
        assert_eq!(repr("it's"), "\"it's\"");
        assert_eq!(repr("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(repr("tab\there\\"), "'tab\\there\\\\'");
        assert_eq!(repr("é\u{7f}\u{200b}"), "'é\\x7f\\u200b'");
    }

    #[test]
    fn word_boundaries_follow_python() {
        let re = regex(r"\b\d+ x\b").unwrap();
        // A combining mark is no word character in Python, so `\b` holds.
        assert_eq!(
            find_iter_python_words(&re, "5 x\u{301} 7 x"),
            ["5 x", "7 x"]
        );
        // `²` is a word character in Python, so no `\b` precedes the 4.
        assert_eq!(find_iter_python_words(&re, "\u{b2}4 x"), [] as [&str; 0]);
        assert_eq!(find_iter_python_words(&re, "é4 x 9 xa"), [] as [&str; 0]);
    }

    #[test]
    fn resolve_applies_parent_components() {
        let p = resolve(Path::new("/a/b/../c/./d/"));
        assert_eq!(p, PathBuf::from("/a/c/d"));
        assert_eq!(resolve(Path::new("/../x")), PathBuf::from("/x"));
    }
}
