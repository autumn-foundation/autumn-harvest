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
/// Deliberately **lenient**: a token with no `=` is skipped, and an
/// unterminated quote or a trailing `\` ends the scan where it ends the string,
/// rather than failing the whole read. Both callers already have an authority
/// for malformed input — `Config::from_str` refuses it, and the banner must
/// still print something — so bailing out would only lose the options that were
/// legible. The walk advances on every iteration, so it terminates on any
/// input.
pub(super) fn keyword_options(dsn: &str) -> Vec<KeywordOption<'_>> {
    let bytes = dsn.as_bytes();
    let mut options = Vec::new();
    let mut index = 0;

    while index < bytes.len() {
        // Whitespace between options.
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }

        // Keyword: up to the `=` or the whitespace that ends it.
        let key_start = index;
        while index < bytes.len() && bytes[index] != b'=' && !bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key = &dsn[key_start..index];

        // libpq permits whitespace on either side of the `=`.
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b'=' {
            // A bare token with no value: not an option. `index` is past
            // `key_start` here — the loop above only stops on `=` (handled) or
            // on whitespace it consumed — so the walk still advances.
            continue;
        }
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }

        let value_start = index;
        let mut value = String::new();
        if index < bytes.len() && bytes[index] == b'\'' {
            index += 1;
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if index + 1 < bytes.len() => {
                        index += 1 + push_char(dsn, index + 1, &mut value);
                    }
                    b'\'' => {
                        index += 1;
                        break;
                    }
                    _ => index += push_char(dsn, index, &mut value),
                }
            }
        } else {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                if bytes[index] == b'\\' && index + 1 < bytes.len() {
                    index += 1 + push_char(dsn, index + 1, &mut value);
                } else {
                    index += push_char(dsn, index, &mut value);
                }
            }
        }

        options.push(KeywordOption {
            key,
            value,
            value_span: value_start..index,
        });
    }

    options
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
