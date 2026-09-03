//! How a DSN is *spelled*, shared by everything in the dev runtime that has to
//! read one without connecting (issue #1286).
//!
//! Two modules here look at a connection string the developer typed rather than
//! at a parsed [`tokio_postgres::Config`]: [`banner`](super::banner) has to
//! blank out passwords before printing, and [`safety`](super::safety) has to
//! name the `sslmode` values the client cannot model. Both need the same three
//! facts — which syntax the string is in, where its options are, and what a
//! percent-encoded query key really says — and when each answered them for
//! itself they disagreed.
//!
//! That disagreement was the bug. `safety` searched for the literal text
//! `sslmode=` anywhere in the string, so `password='sslmode=require'` refused a
//! perfectly legal loopback database as TLS-requiring, while `banner`'s scanner
//! two modules over already knew that span was a password value. One scanner,
//! two callers.
//!
//! # What this is not
//!
//! It is **not** a DSN parser, and nothing here decides what gets connected to.
//! `tokio_postgres::Config::from_str` remains the only thing that answers "what
//! will the client dial" — see [`safety`](super::safety)'s module docs for why
//! a second opinion on that question is a bypass waiting to happen. These
//! helpers only locate the bytes of a string; a span they miss costs a clearer
//! message, never a connection the gate meant to refuse.

use std::ops::Range;

/// Whether libpq — and `tokio_postgres::Config::from_str` — would read this as
/// a URI rather than a keyword/value string.
///
/// The test is the literal prefix, case-sensitively, and nothing else: that is
/// the client's own rule, and matching it exactly is the point. Leading
/// whitespace is ignored for the *decision* only; callers keep the string
/// itself unchanged, so what the developer pastes still looks like what they
/// typed.
pub(super) fn is_uri_dsn(dsn: &str) -> bool {
    let trimmed = dsn.trim_start();
    trimmed.starts_with("postgresql://") || trimmed.starts_with("postgres://")
}

/// One `keyword = value` option of a libpq keyword/value connection string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct KeywordOption<'a> {
    /// The keyword exactly as written, case included.
    pub key: &'a str,
    /// The value with its quotes removed and its `\` escapes resolved — what
    /// libpq would hand the driver.
    pub value: String,
    /// Byte range of the value *as written* — quotes and escapes included —
    /// within the DSN the option was read from. Rewriting a value means
    /// splicing over this range, which leaves every other byte untouched by
    /// construction.
    pub value_span: Range<usize>,
}

/// Read a libpq **keyword/value** connection string the way libpq reads it.
///
/// A real scanner, not `split_whitespace()`. Options are separated by
/// whitespace, `=` may have whitespace on either side, and a value may be
/// single-quoted and contain spaces; `\` escapes the next character in both the
/// quoted and the bare form. A whitespace split cannot see any of that, so
/// `password='foo hunter2'` tokenizes as `password='foo` and leaves `hunter2'`
/// standing — in a banner whose entire purpose is to be safe to paste into an
/// issue.
///
/// Lazy, so a caller looking for one option ([`safety`](super::safety) wants
/// `sslmode`) stops at it rather than decoding every later value — including
/// the password — into a `String` it never reads.
///
/// Deliberately **lenient**: a token with no `=` is skipped, and an
/// unterminated quote or a trailing `\` ends the scan where it ends the string,
/// rather than failing the whole read. Both callers already have an authority
/// for malformed input — `Config::from_str` refuses it, and the banner must
/// still print something — so bailing out would only lose the options that were
/// legible.
pub(super) const fn keyword_options(dsn: &str) -> KeywordOptions<'_> {
    KeywordOptions { dsn, index: 0 }
}

/// The options of a keyword/value connection string, in the order written.
///
/// Yields non-overlapping [`KeywordOption::value_span`]s that only ever move
/// forward, which is what makes a span-splice rewrite safe.
pub(super) struct KeywordOptions<'a> {
    /// The string being scanned.
    dsn: &'a str,
    /// How far into `dsn` the scan has reached, always a character boundary.
    index: usize,
}

impl<'a> Iterator for KeywordOptions<'a> {
    type Item = KeywordOption<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self.dsn.as_bytes();

        // A token with no `=` is not an option, so keep going until one is —
        // every pass consumes at least one byte, so this terminates.
        while self.index < bytes.len() {
            // Whitespace between options.
            while self.index < bytes.len() && bytes[self.index].is_ascii_whitespace() {
                self.index += 1;
            }
            if self.index >= bytes.len() {
                return None;
            }

            // Keyword: up to the `=` or the whitespace that ends it.
            let key_start = self.index;
            while self.index < bytes.len()
                && bytes[self.index] != b'='
                && !bytes[self.index].is_ascii_whitespace()
            {
                self.index += 1;
            }
            let key = &self.dsn[key_start..self.index];

            // libpq permits whitespace on either side of the `=`.
            while self.index < bytes.len() && bytes[self.index].is_ascii_whitespace() {
                self.index += 1;
            }
            if self.index >= bytes.len() || bytes[self.index] != b'=' {
                // A bare token with no value. `self.index` is past `key_start`
                // here — the loop above only stops on `=` (handled) or on
                // whitespace it then consumed — so the scan still advances.
                continue;
            }
            self.index += 1;
            while self.index < bytes.len() && bytes[self.index].is_ascii_whitespace() {
                self.index += 1;
            }

            let value_start = self.index;
            let value = self.take_value();
            return Some(KeywordOption {
                key,
                value,
                value_span: value_start..self.index,
            });
        }

        None
    }
}

impl KeywordOptions<'_> {
    /// Consume one value from the cursor: a single-quoted span, or everything
    /// up to the next unescaped whitespace. Returns it unquoted and unescaped.
    fn take_value(&mut self) -> String {
        let bytes = self.dsn.as_bytes();
        let mut value = String::new();

        if self.index < bytes.len() && bytes[self.index] == b'\'' {
            self.index += 1;
            while self.index < bytes.len() {
                match bytes[self.index] {
                    b'\\' if self.index + 1 < bytes.len() => {
                        self.index += 1 + push_char(self.dsn, self.index + 1, &mut value);
                    }
                    b'\'' => {
                        self.index += 1;
                        break;
                    }
                    _ => self.index += push_char(self.dsn, self.index, &mut value),
                }
            }
        } else {
            while self.index < bytes.len() && !bytes[self.index].is_ascii_whitespace() {
                if bytes[self.index] == b'\\' && self.index + 1 < bytes.len() {
                    self.index += 1 + push_char(self.dsn, self.index + 1, &mut value);
                } else {
                    self.index += push_char(self.dsn, self.index, &mut value);
                }
            }
        }

        value
    }
}

/// Append the character at byte offset `at` to `out`, returning its width.
///
/// The width is the whole character, not one byte: `\é` is three bytes, and
/// stepping two would leave the cursor *inside* it — so the next `dsn[index..]`
/// slice panics on a non-character boundary instead of scanning. A DSN can
/// carry any UTF-8 the developer's password does.
fn push_char(dsn: &str, at: usize, out: &mut String) -> usize {
    // `at` is always a character boundary: every step here advances by a whole
    // character. An empty tail cannot happen (callers check the bound first),
    // but the default keeps a width of 1 so the walk still advances.
    dsn[at..].chars().next().map_or(1, |ch| {
        out.push(ch);
        ch.len_utf8()
    })
}

/// The query-string parameters of a **URI** DSN, keys and values percent-decoded.
///
/// Splitting on `&` and then on the first `=` is exactly what the client's URI
/// reader does, so a parameter's *value* can never be mistaken for the query
/// string's own key — which is how `?application_name=sslmode=require` came to
/// be read as a TLS demand.
///
/// The query string starts at the first `?`, matching
/// `Config::from_str`; a `?` anywhere earlier in a URI has to be
/// percent-encoded, so there is nothing before it to protect.
pub(super) fn uri_query_parameters(dsn: &str) -> impl Iterator<Item = (String, String)> + '_ {
    dsn.split_once('?')
        .map_or("", |(_, query)| query)
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (percent_decoded(key), percent_decoded(value)))
}

/// Percent-decode a URI query component, for comparison only.
///
/// `tokio_postgres`'s URI reader decodes query keys and values before it
/// matches them, so `%73slmode=verify-full` **is** the `sslmode` parameter as
/// far as the code that dials is concerned — and comparing the raw bytes both
/// printed a whole credential in the banner and missed a TLS demand in the
/// safety gate.
///
/// Deliberately not a general URI decoder: nothing is re-encoded or reshaped
/// from the result, and a malformed escape is left alone exactly as
/// `percent_decode` leaves it.
pub(super) fn percent_decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && index + 2 < bytes.len()
            && let Some(high) = char::from(bytes[index + 1]).to_digit(16)
            && let Some(low) = char::from(bytes[index + 2]).to_digit(16)
        {
            // Both digits are < 16, so the sum is < 256 and the cast is exact.
            #[allow(clippy::cast_possible_truncation)]
            out.push((high * 16 + low) as u8);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::{is_uri_dsn, keyword_options, percent_decoded, uri_query_parameters};

    /// `(key, unescaped value, the value exactly as written)` for every option.
    fn options(dsn: &str) -> Vec<(&str, String, &str)> {
        keyword_options(dsn)
            .map(|option| (option.key, option.value, &dsn[option.value_span]))
            .collect()
    }

    #[test]
    fn the_syntax_test_is_the_clients_own_prefix_rule() {
        for uri in [
            "postgres://u@localhost/app",
            "postgresql://u@localhost/app",
            "  postgres://u@localhost/app",
        ] {
            assert!(is_uri_dsn(uri), "{uri}");
        }
        for keyword_value in [
            "host=localhost dbname=app",
            // Not the client's spelling, so not a URI to the client either.
            "POSTGRES://u@localhost/app",
            "postgres:/u@localhost/app",
            "dbname=postgres://x",
            "",
        ] {
            assert!(!is_uri_dsn(keyword_value), "{keyword_value}");
        }
    }

    #[test]
    fn a_quoted_value_is_consumed_whole_and_reported_as_written() {
        assert_eq!(
            options("host=localhost password='foo hunter2' dbname=app"),
            [
                ("host", "localhost".to_owned(), "localhost"),
                ("password", "foo hunter2".to_owned(), "'foo hunter2'"),
                ("dbname", "app".to_owned(), "app"),
            ]
        );
    }

    #[test]
    fn an_escape_is_resolved_in_both_the_quoted_and_the_bare_form() {
        assert_eq!(
            options(r"password='a\'b' user=c\ d"),
            [
                ("password", "a'b".to_owned(), r"'a\'b'"),
                // The escaped space does not end the bare value.
                ("user", "c d".to_owned(), r"c\ d"),
            ]
        );
    }

    #[test]
    fn whitespace_around_the_equals_sign_is_permitted_as_libpq_permits_it() {
        assert_eq!(
            options("sslmode = verify-full"),
            [("sslmode", "verify-full".to_owned(), "verify-full")]
        );
    }

    #[test]
    fn a_value_containing_an_option_is_a_value_and_not_an_option() {
        // The whole point of #1286: this string has exactly one `sslmode`-shaped
        // run of text in it, and it is not a key.
        assert_eq!(
            options("host=localhost password='sslmode=require'"),
            [
                ("host", "localhost".to_owned(), "localhost"),
                (
                    "password",
                    "sslmode=require".to_owned(),
                    "'sslmode=require'"
                ),
            ]
        );
    }

    #[test]
    fn a_bare_token_is_skipped_without_swallowing_the_option_after_it() {
        assert_eq!(
            options("bareword host=localhost"),
            [("host", "localhost".to_owned(), "localhost")]
        );
    }

    #[test]
    fn a_multibyte_escape_advances_by_the_whole_character() {
        // Stepping two bytes past a `\` would leave the cursor inside `é` and
        // the next slice would panic on a non-character boundary.
        assert_eq!(
            options(r"password='\é' host=localhost"),
            [
                ("password", "é".to_owned(), r"'\é'"),
                ("host", "localhost".to_owned(), "localhost"),
            ]
        );
    }

    #[test]
    fn malformed_syntax_ends_the_scan_rather_than_failing_or_hanging() {
        // Every one of these is something a developer can type. None may
        // diverge, and each must still surrender the options it could read.
        for dsn in [
            "host=localhost password='unterminated",
            r"host=localhost password=trailing\",
            r"password='\",
            "host=",
            "=",
            "'",
            "   ",
            "",
            "sslmode",
        ] {
            let scanned = options(dsn);
            assert!(
                scanned.len() <= 2,
                "{dsn:?} should not invent options: {scanned:?}"
            );
        }
        assert_eq!(
            options("host=localhost password='unterminated")
                .first()
                .map(|(key, _, _)| *key),
            Some("host"),
            "an option before the malformed one is still readable"
        );
    }

    #[test]
    fn spans_are_forward_only_and_never_overlap() {
        // What makes the banner's span-splice rewrite safe.
        let dsn = r"a=1 b='2 3' c=4\ 5 d= e='' f=6";
        let mut previous_end = 0;
        for option in keyword_options(dsn) {
            assert!(
                option.value_span.start >= previous_end,
                "{option:?} overlaps or precedes the span before it"
            );
            assert!(
                option.value_span.start <= option.value_span.end,
                "{option:?}"
            );
            previous_end = option.value_span.end;
        }
        assert!(previous_end <= dsn.len());
    }

    #[test]
    fn a_query_parameters_value_is_never_read_as_the_next_key() {
        assert_eq!(
            uri_query_parameters("postgres://u:pw@localhost/app?application_name=sslmode=require")
                .collect::<Vec<_>>(),
            [("application_name".to_owned(), "sslmode=require".to_owned())]
        );
    }

    #[test]
    fn query_keys_and_values_are_decoded_the_way_the_client_decodes_them() {
        assert_eq!(
            uri_query_parameters("postgres://u@localhost/app?%73slmode=verify%2Dfull")
                .collect::<Vec<_>>(),
            [("sslmode".to_owned(), "verify-full".to_owned())]
        );
    }

    #[test]
    fn a_dsn_with_no_query_string_has_no_parameters() {
        for dsn in [
            "postgres://u:pw@localhost/sslmode=require",
            "postgres://u@localhost/app",
            "postgres://u@localhost/app?",
        ] {
            assert_eq!(
                uri_query_parameters(dsn).count(),
                0,
                "{dsn} carries no `key=value` query parameter"
            );
        }
    }

    #[test]
    fn a_malformed_escape_is_left_exactly_as_written() {
        assert_eq!(percent_decoded("%zz"), "%zz");
        assert_eq!(percent_decoded("%4"), "%4");
        assert_eq!(percent_decoded("%"), "%");
        assert_eq!(percent_decoded("plain"), "plain");
    }
}
