//! Balanced-delimiter scanning over textual MIR.
//!
//! `rustc --emit=mir` prints item paths, types, places and operands that are
//! full of `:`, `,`, `(` and `<` *inside* nested delimiters
//! (`<impl at f.rs:9:1: 9:9>::emit`, `{closure@f.rs:16:21: 16:24}`,
//! `Result::<u64, E>::map_err::<String, {closure@f.rs:104:32: 104:35}>`), so
//! nothing here may split on a bare byte. Every helper is tolerant: unbalanced
//! or truncated input yields `None` or a best-effort answer, never a panic.

/// Calls `f(index, byte, depth)` for every byte outside a string literal,
/// stopping early when `f` returns `false`.
///
/// `depth` is the nesting depth *outside* the delimiter at `index`, so an
/// opening bracket and its matching closer are both reported at the same
/// depth. `->` and `=>` are recognised as arrows, so their `>` does not close
/// a generic argument list; double-quoted strings are skipped wholesale
/// because MIR assert messages contain `{}` and quotes.
pub fn walk(s: &str, mut f: impl FnMut(usize, u8, u32) -> bool) {
    let bytes = s.as_bytes();
    let mut depth: u32 = 0;
    let mut in_string = false;
    let mut prev = 0_u8;
    let mut i = 0_usize;
    while let Some(&c) = bytes.get(i) {
        if in_string {
            if c == b'\\' {
                i += 2;
                prev = 0;
                continue;
            }
            if c == b'"' {
                in_string = false;
            }
            prev = c;
            i += 1;
            continue;
        }
        if c == b'"' {
            in_string = true;
            prev = c;
            i += 1;
            continue;
        }
        let reported = match c {
            b'(' | b'[' | b'{' | b'<' => {
                let outer = depth;
                depth = depth.saturating_add(1);
                outer
            }
            b')' | b']' | b'}' => {
                depth = depth.saturating_sub(1);
                depth
            }
            b'>' if prev != b'-' && prev != b'=' => {
                depth = depth.saturating_sub(1);
                depth
            }
            _ => depth,
        };
        if !f(i, c, reported) {
            return;
        }
        prev = c;
        i += 1;
    }
}

/// Byte index of the first occurrence of `pat` at nesting depth 0.
pub fn find_top(s: &str, pat: &str) -> Option<usize> {
    let first = *pat.as_bytes().first()?;
    let mut found = None;
    walk(s, |i, c, d| {
        if d == 0 && c == first && s.get(i..).is_some_and(|tail| tail.starts_with(pat)) {
            found = Some(i);
            return false;
        }
        true
    });
    found
}

/// Byte index of the last occurrence of `pat` at nesting depth 0.
pub fn rfind_top(s: &str, pat: &str) -> Option<usize> {
    let first = *pat.as_bytes().first()?;
    let mut found = None;
    walk(s, |i, c, d| {
        if d == 0 && c == first && s.get(i..).is_some_and(|tail| tail.starts_with(pat)) {
            found = Some(i);
        }
        true
    });
    found
}

/// Splits `s` on every `sep` at nesting depth 0.
pub fn split_top(s: &str, sep: u8) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0_usize;
    walk(s, |i, c, d| {
        if d == 0 && c == sep {
            if let Some(part) = s.get(start..i) {
                parts.push(part);
            }
            start = i + 1;
        }
        true
    });
    if let Some(part) = s.get(start..) {
        parts.push(part);
    }
    parts
}

/// Byte index of the closer matching the opening delimiter at `open`.
pub fn match_at(s: &str, open: usize) -> Option<usize> {
    let mut base: Option<u32> = None;
    let mut close = None;
    walk(s, |i, c, d| {
        if i < open {
            return true;
        }
        if i == open {
            base = Some(d);
            return true;
        }
        if base == Some(d) && matches!(c, b')' | b']' | b'}' | b'>') {
            close = Some(i);
            return false;
        }
        true
    });
    close
}

/// Splits `HEAD<open> INNER <close>` when the group closes exactly at the end
/// of `s`, returning `(head, inner)`.
pub fn trailing_group(s: &str, open: u8, close: u8) -> Option<(&str, &str)> {
    if s.as_bytes().last() != Some(&close) {
        return None;
    }
    let mut candidate = None;
    walk(s, |i, c, d| {
        if d == 0 && c == open {
            candidate = Some(i);
        }
        true
    });
    let start = candidate?;
    let end = s.len().checked_sub(1)?;
    if match_at(s, start) != Some(end) {
        return None;
    }
    Some((s.get(..start)?, s.get(start.checked_add(1)?..end)?))
}

/// `true` when `s` is one parenthesised group covering the whole string.
pub fn is_wrapped(s: &str) -> bool {
    s.len() >= 2
        && s.starts_with('(')
        && s.ends_with(')')
        && match_at(s, 0) == s.len().checked_sub(1)
}

/// Byte index of the first depth-0 `:` that is not part of a `::` path
/// separator — the `PATH: TYPE` split point of a decl, a parameter or a
/// `static`/`const` header.
pub fn find_type_colon(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut found = None;
    walk(s, |i, c, d| {
        if d == 0
            && c == b':'
            && bytes.get(i.wrapping_add(1)) != Some(&b':')
            && bytes.get(i.wrapping_sub(1)) != Some(&b':')
        {
            found = Some(i);
            return false;
        }
        true
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrows_do_not_close_generics() {
        let s = "get_or_init::<fn() -> RefCell<u64> {init}>(copy _3) -> [return: bb1]";
        assert_eq!(find_top(s, "("), Some(s.find("(copy").unwrap()));
        assert!(rfind_top(s, "->").unwrap() > s.find("[return").unwrap() - 4);
    }

    #[test]
    fn strings_hide_braces_and_commas() {
        let s = r#"assert(!move (_3.1: bool), "compute `{} + {}`, overflow", copy _1)"#;
        let (head, inner) = trailing_group(s, b'(', b')').unwrap();
        assert_eq!(head, "assert");
        assert_eq!(split_top(inner, b',').len(), 3);
    }

    #[test]
    fn type_colon_skips_path_separators() {
        let s = "TL::{constant#0}: for<'a> fn(Option<&'a mut u8>) -> *const u8";
        assert_eq!(
            s.get(..find_type_colon(s).unwrap()),
            Some("TL::{constant#0}")
        );
        let d = "_2: <T as std::iter::IntoIterator>::IntoIter";
        assert_eq!(d.get(..find_type_colon(d).unwrap()), Some("_2"));
    }

    #[test]
    fn impl_span_colons_are_nested() {
        let s = "<impl at spike.rs:5:1: 5:15>::next(_1: &A)";
        assert_eq!(find_type_colon(s), None);
        assert_eq!(
            s.get(..find_top(s, "(").unwrap()),
            Some("<impl at spike.rs:5:1: 5:15>::next")
        );
    }

    #[test]
    fn degenerate_input_is_tolerated() {
        assert_eq!(find_top("", "("), None);
        assert_eq!(match_at("(((", 0), None);
        assert_eq!(trailing_group(")))", b'(', b')'), None);
        assert!(!is_wrapped("("));
        assert!(is_wrapped("((a))"));
        assert!(!is_wrapped("(a)(b)"));
    }
}
