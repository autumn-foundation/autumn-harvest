//! The one part of Python's `str` `repr()` every script-port shares.
//!
//! Issue tracker: see the clone-class note on each caller. Several xtask
//! modules port a Python script that called `repr()` on a string, and each
//! was hand-translated independently (`#2574`). Python's string literal
//! escape rules fix five cases with no room for interpretation: a
//! backslash, `\n`, `\r`, `\t`, and the quote character `repr()` chose for
//! this string. The Python Language Reference lists them as the literal
//! escape sequences every string repr uses (`\\`, `\n`, `\r`, `\t`, and the
//! active quote), so every port agrees on them byte for byte. What each
//! caller does for every *other* character is where they deliberately
//! differ (some escape the non-printable set, one escapes nothing, one
//! must handle a lone surrogate that cannot be a Rust `char` at all), so
//! that part stays with the caller.

/// Returns Python's quote character for `repr()` of `s`.
///
/// `repr()` quotes with `'`, unless `s` holds a `'` and no `"`.
#[must_use]
pub fn repr_quote(s: &str) -> char {
    if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    }
}

/// Pushes `c`'s escape onto `out` for the five cases every `repr()` port
/// shares, and returns `true` when it did.
///
/// Returns `false` for every other character, so the caller can apply its
/// own rule for the rest of `repr()` — escape the non-printable set, pass
/// everything through, or decode a surrogate-aware code point first.
#[must_use]
pub fn push_common_repr_escape(out: &mut String, c: char, quote: char) -> bool {
    match c {
        '\\' => out.push_str("\\\\"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        '\t' => out.push_str("\\t"),
        _ if c == quote => {
            out.push('\\');
            out.push(c);
        }
        _ => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repr_quote_switches_only_for_a_lone_apostrophe() {
        assert_eq!(repr_quote("plain"), '\'');
        assert_eq!(repr_quote("it's"), '"');
        assert_eq!(repr_quote("a'b\"c"), '\'');
    }

    #[test]
    fn push_common_repr_escape_covers_the_five_shared_cases() {
        let quote = '\'';
        for (c, expected) in [
            ('\\', "\\\\"),
            ('\n', "\\n"),
            ('\r', "\\r"),
            ('\t', "\\t"),
            ('\'', "\\'"),
        ] {
            let mut out = String::new();
            assert!(push_common_repr_escape(&mut out, c, quote));
            assert_eq!(out, expected);
        }
    }

    #[test]
    fn push_common_repr_escape_declines_everything_else() {
        let mut out = String::new();
        assert!(!push_common_repr_escape(&mut out, 'a', '\''));
        assert!(!push_common_repr_escape(&mut out, '\u{7f}', '\''));
        assert_eq!(out, "");
    }
}
