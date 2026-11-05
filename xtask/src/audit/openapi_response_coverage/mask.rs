//! Comment and literal masking: `code_only` and `masked_source`.

use super::state::{self, Text};

orc_cache!(MASKED: usize => Text);

/// `block` with comments and literals blanked, at the same length.
///
/// A literal keeps its opening and closing delimiters, such as `b"` and `"`
/// or `r#"` and `"#`. So a second pass leaves masked text unchanged. With
/// `literals` false, only comments are blanked. Newlines stay, so offsets
/// and line numbers do not change. A block comment can nest, as in Rust.
pub fn code_only(block: &str, literals: bool) -> String {
    let bytes = block.as_bytes();
    let length = bytes.len();
    let mut out = bytes.to_vec();
    let mut blank = |start: usize, stop: usize, head: usize, tail: usize| -> usize {
        if head > 0 && !literals {
            return stop;
        }
        let end = stop.saturating_sub(tail);
        for byte in out.iter_mut().take(end).skip(start + head) {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
        stop
    };
    let mut index = 0;
    while index < length {
        let char = bytes[index];
        let prefix = string_prefix(bytes, index);
        if bytes[index..].starts_with(b"//") {
            let stop = block[index..].find('\n').map_or(length, |at| at + index);
            index = blank(index, stop, 0, 0);
        } else if bytes[index..].starts_with(b"/*") {
            let mut depth = 0;
            let mut stop = index;
            while stop < length {
                if bytes[stop..].starts_with(b"/*") {
                    depth += 1;
                    stop += 2;
                } else if bytes[stop..].starts_with(b"*/") {
                    depth -= 1;
                    stop += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    stop += 1;
                }
            }
            index = blank(index, stop.min(length), 0, 0);
        } else if let Some(Prefix::Raw { end, hashes }) = prefix {
            let mut closer = String::from("\"");
            closer.push_str(&"#".repeat(hashes));
            let close = block.get(end..).and_then(|rest| rest.find(&closer));
            let tail = 1 + hashes;
            let stop = close.map_or(length, |at| at + end + tail);
            let kept_tail = if close.is_some() { tail } else { 0 };
            index = blank(index, stop, end - index, kept_tail);
        } else if char == b'"' || prefix.is_some() {
            let opener = block[index..].find('"').map_or(length, |at| at + index) + 1;
            let mut stop = opener;
            while stop < length && bytes[stop] != b'"' {
                stop += if bytes[stop] == b'\\' { 2 } else { 1 };
            }
            let closed = stop < length;
            index = blank(
                index,
                (stop + 1).min(length),
                opener - index,
                usize::from(closed),
            );
        } else if char == b'\'' {
            if let Some(end) = char_literal(block, index) {
                index = blank(index, end, 1, 1);
            } else {
                index += 1;
            }
        } else if let Some(word) = word_char(block, index) {
            // A whole word, so `r"` or `b"` inside a name is no literal.
            let mut at = index + word;
            while let Some(step) = word_char(block, at) {
                at += step;
            }
            index = at;
        } else {
            index += 1;
        }
    }
    String::from_utf8(out)
        .unwrap_or_else(|error| String::from_utf8_lossy(error.as_bytes()).into_owned())
}

/// The byte length of a word character at `index`, as Python's
/// `isalnum()` or `_` reads it, or `None`.
fn word_char(text: &str, index: usize) -> Option<usize> {
    let char = text.get(index..)?.chars().next()?;
    (char.is_alphanumeric() || char == '_').then(|| char.len_utf8())
}

/// A string prefix at `index`, as `STRING_PREFIX` reads it.
enum Prefix {
    /// A raw string prefix such as `r#"`: where it ends, and its hashes.
    Raw { end: usize, hashes: usize },
    /// A byte string prefix, `b"`.
    Byte,
}

/// `b?r(#*)"|b"` matched at `index`.
fn string_prefix(bytes: &[u8], index: usize) -> Option<Prefix> {
    let mut at = index;
    if bytes.get(at) == Some(&b'b') {
        at += 1;
    }
    if bytes.get(at) == Some(&b'r') {
        let mut end = at + 1;
        while bytes.get(end) == Some(&b'#') {
            end += 1;
        }
        if bytes.get(end) == Some(&b'"') {
            return Some(Prefix::Raw {
                end: end + 1,
                hashes: end - at - 1,
            });
        }
    }
    if bytes.get(index) == Some(&b'b') && bytes.get(index + 1) == Some(&b'"') {
        return Some(Prefix::Byte);
    }
    None
}

/// The end of a char literal at `index`, as `'(?:\\.|[^\\'])'` reads it.
///
/// A lifetime such as `'a` has no closing quote, so it stays.
fn char_literal(text: &str, index: usize) -> Option<usize> {
    let mut chars = text.get(index + 1..)?.char_indices();
    let (_, first) = chars.next()?;
    let after = if first == '\\' {
        let (at, escaped) = chars.next()?;
        if escaped == '\n' {
            return None;
        }
        index + 1 + at + escaped.len_utf8()
    } else if first == '\'' {
        return None;
    } else {
        index + 1 + first.len_utf8()
    };
    (text.as_bytes().get(after) == Some(&b'\'')).then_some(after + 1)
}

/// `source` through `code_only`, computed once per source.
pub fn masked_source(source: &Text) -> Text {
    state::cached(&MASKED, state::key(source), || {
        state::intern(&code_only(source, true))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comments_and_literals_are_blanked_at_the_same_length() {
        let text = "let a = \"x//y\"; // note\nlet b = r#\"q\"#; /* a /* b */ c */ 'c'";
        let masked = code_only(text, true);
        assert_eq!(masked.len(), text.len());
        assert_eq!(
            masked,
            "let a = \"    \";        \nlet b = r#\" \"#;                   ' '"
        );
    }

    #[test]
    fn a_lifetime_is_no_char_literal() {
        assert_eq!(
            code_only("fn f<'a>(x: &'a str)", true),
            "fn f<'a>(x: &'a str)"
        );
    }

    #[test]
    fn literals_stay_when_only_comments_are_blanked() {
        assert_eq!(code_only("\"a\" // b", false), "\"a\"     ");
    }

    #[test]
    fn a_prefix_inside_a_word_is_no_literal() {
        assert_eq!(code_only("bar\"x\"", true), "bar\" \"");
    }
}
