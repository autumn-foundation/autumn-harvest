//! Strict percent-decoding for raw HTTP query strings.
//!
//! Extracted from the issue #774 fix for `GET /admin/queue-coverage` and
//! shared by every other route swept in issue #1151.
//!
//! axum's built-in `Query<T>` extractor is backed by
//! `serde_urlencoded`/`form_urlencoded`. Both *always* succeed. They silently
//! substitute `U+FFFD` (the Unicode replacement character) for a malformed
//! percent-encoded byte sequence, and never reject the request.
//! A caller that sends `?queue_name=%FF` (an invalid UTF-8 byte) gets no error.
//! The query decodes to a *different*, legitimate-looking value. A route that
//! filters or scopes its response by such a param can then return a
//! false-clean result instead of the documented `400`.
//!
//! So every route in [`crate::api`] that consumes a raw `(key, value)` pair
//! list must read the raw query string via `axum::extract::RawQuery`. (A
//! single-field derive-based `Query<T>` is exempt.) It must decode the string
//! with [`parse_raw_query_pairs_strict`], not with axum's lossy
//! `Query<Vec<(String, String)>>` extractor. It must map
//! [`InvalidQueryEncoding`] to the `400` JSON error shape that the route
//! already uses for other invalid params. See
//! [`bad_request_response`] for the shared shape.

use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// The message of every strict-query `400` for [`InvalidQueryEncoding`].
///
/// Shared so the wording can never drift between call sites (issue #1151).
pub const MALFORMED_QUERY_MESSAGE: &str = "malformed query string: invalid percent-encoded UTF-8";

/// A query string component's percent-decoded bytes are not valid UTF-8, or
/// contain a syntactically malformed `%` escape.
///
/// Returned by [`parse_raw_query_pairs_strict`]. Callers map this to a `400`
/// JSON response (see [`bad_request_response`]). They never silently
/// substitute `U+FFFD`, as axum's built-in `Query<T>` extractor does via
/// `serde_urlencoded`/`form_urlencoded` (issue #774 review).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidQueryEncoding;

impl std::fmt::Display for InvalidQueryEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "query string contains an invalid percent-encoded UTF-8 byte sequence"
        )
    }
}

impl std::error::Error for InvalidQueryEncoding {}

/// The `400` JSON response body `GET /admin/queue-coverage` returns for
/// [`InvalidQueryEncoding`].
///
/// This is the original `{"error": "..."}` shape that issue #774 shipped and
/// tested. Every *other* strict-query route uses the RFC-7807-flavored
/// `{"detail": "...", "status": 400, ...}` shape of [`AutumnError`].
/// The malformed-encoding tests in `queue_coverage_integration.rs` assert on
/// this literal shape. So it is not folded into
/// [`decode_or_autumn_error_response`] for the issue #1151 sweep. A change to
/// the error contract of a shipped, tested route is out of scope here.
#[must_use]
pub fn bad_request_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": MALFORMED_QUERY_MESSAGE })),
    )
        .into_response()
}

/// Decode an optional raw query string into `(key, value)` pairs for a
/// handler whose return type is `Result<_, AutumnError>`.
///
/// The majority shape across [`crate::api`]'s read routes. Takes the value
/// `axum::extract::RawQuery` returns; a missing query string (`None`) is an
/// empty pair list, matching axum's own `Query<T>` behavior for an absent
/// `?`.
///
/// # Errors
///
/// Returns the same [`AutumnError::bad_request_msg`]-shaped `400` every
/// other invalid-param case in these routes already returns, carrying
/// [`MALFORMED_QUERY_MESSAGE`].
pub fn decode_or_autumn_error(
    raw_query: Option<&str>,
) -> Result<Vec<(String, String)>, AutumnError> {
    match raw_query.map(parse_raw_query_pairs_strict) {
        None => Ok(Vec::new()),
        Some(Ok(pairs)) => Ok(pairs),
        Some(Err(InvalidQueryEncoding)) => {
            Err(AutumnError::bad_request_msg(MALFORMED_QUERY_MESSAGE))
        }
    }
}

/// Decode an optional raw query string into `(key, value)` pairs for
/// `GET /admin/queue-coverage` specifically.
///
/// The one route whose malformed-query `400` predates this shared module
/// (issue #774) and is proven, by name, in `queue_coverage_integration.rs`.
/// See [`bad_request_response`] for why its shape is not [`AutumnError`]'s.
///
/// # Errors
///
/// Returns a ready-made [`bad_request_response`] on [`InvalidQueryEncoding`]
/// for the caller to `return` directly.
///
/// `Response` is a large `Err` variant. Every strict-query call site
/// `match`es and returns it at once, and never stores or propagates it. So a
/// box would only add an allocation.
#[allow(clippy::result_large_err)]
pub fn decode_or_queue_coverage_bad_request(
    raw_query: Option<&str>,
) -> Result<Vec<(String, String)>, Response> {
    match raw_query.map(parse_raw_query_pairs_strict) {
        None => Ok(Vec::new()),
        Some(Ok(pairs)) => Ok(pairs),
        Some(Err(InvalidQueryEncoding)) => Err(bad_request_response()),
    }
}

/// Decode an optional raw query string into `(key, value)` pairs for a
/// handler whose return type is `axum::response::Response`.
///
/// For an infallible-signature handler that converts errors internally via
/// `.into_response()`. Its *other* invalid-param `400`s are
/// [`AutumnError`]-shaped: the `Err(error) => return error.into_response()`
/// idiom used throughout [`crate::api`]. This function, not
/// [`decode_or_queue_coverage_bad_request`], keeps the malformed-query `400`
/// in the SAME body shape as the other `400`s of the route. So one route
/// never has two inconsistent error shapes (issue #1151 review).
///
/// # Errors
///
/// Returns a ready-made `400` [`Response`] built from
/// [`AutumnError::bad_request_msg`] on [`InvalidQueryEncoding`], for the
/// caller to `return` directly.
///
/// See the doc comment of [`decode_or_queue_coverage_bad_request`] for why the
/// large `Err` variant is allowed, not boxed.
#[allow(clippy::result_large_err)]
pub fn decode_or_autumn_error_response(
    raw_query: Option<&str>,
) -> Result<Vec<(String, String)>, Response> {
    match raw_query.map(parse_raw_query_pairs_strict) {
        None => Ok(Vec::new()),
        Some(Ok(pairs)) => Ok(pairs),
        Some(Err(InvalidQueryEncoding)) => {
            Err(AutumnError::bad_request_msg(MALFORMED_QUERY_MESSAGE).into_response())
        }
    }
}

/// Strictly percent-decodes a raw query string into `(key, value)` pairs.
///
/// Axum's lossy `Query<T>` extractor would silently decode a malformed value
/// like `?queue_name=%FF` to `queue_name=<U+FFFD>`. That filter looks
/// legitimate but is wrong. A scoped read route could then return a
/// false-clean result instead of the documented `400` (issue #774 review).
/// Issue #1151 swept the fix to every other raw-pairs route.
///
/// Mirrors the grammar of `form_urlencoded::parse` exactly:
/// - split on `&`;
/// - split each segment on the first `=` (a segment with no `=` is a key with
///   an empty value);
/// - decode `+` to a literal space *before* percent-decoding.
///
/// An empty segment (from a leading/trailing/doubled `&`, or an entirely
/// empty query string) is skipped.
///
/// # Errors
///
/// Returns [`InvalidQueryEncoding`] on the first key or value that is
/// malformed in either of two ways:
/// - a syntactically invalid `%` escape, not followed by exactly two hex
///   digits (e.g. `%`, `%2`, `%GG`);
/// - a syntactically valid escape whose decoded bytes are not valid UTF-8
///   (e.g. `%FF`).
///
/// This is the one place in a strict-query request path that *can* reject an
/// input outright. The route-local `XxxQuery::from_query_pairs` helper that
/// consumes this output stays infallible by construction.
pub fn parse_raw_query_pairs_strict(
    raw_query: &str,
) -> Result<Vec<(String, String)>, InvalidQueryEncoding> {
    let mut pairs = Vec::new();
    for segment in raw_query.split('&') {
        if segment.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = segment.split_once('=').unwrap_or((segment, ""));
        pairs.push((
            decode_form_component_strict(raw_key)?,
            decode_form_component_strict(raw_value)?,
        ));
    }
    Ok(pairs)
}

/// Strictly percent-decodes one `application/x-www-form-urlencoded`
/// key/value component: `+` -> space first, then `%XX` percent-decoding
/// with strict (non-lossy) UTF-8 validation of the resulting bytes.
fn decode_form_component_strict(raw: &str) -> Result<String, InvalidQueryEncoding> {
    if !has_only_well_formed_percent_escapes(raw) {
        return Err(InvalidQueryEncoding);
    }
    let space_decoded = raw.replace('+', " ");
    percent_encoding::percent_decode_str(&space_decoded)
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|_| InvalidQueryEncoding)
}

/// Whether every `%` in `s` is immediately followed by exactly two ASCII
/// hex digits.
///
/// `percent_encoding::percent_decode_str` does **not** reject a malformed
/// escape on its own. It leaves an incomplete (`%`, `%2`) or non-hex (`%GG`)
/// sequence as a **literal, undecoded** run of bytes, not an error. Those
/// bytes (`%`, `G`, digits, ...) are valid ASCII, so `decode_utf8()` still
/// succeeds trivially. Without this pre-check, `?queue_name=orders%GG` would
/// silently decode to the literal string `"orders%GG"`. The route would query
/// that (almost certainly nonexistent) value instead of rejecting it. This is
/// a second malformed-encoding shape, distinct from the `%FF` case that
/// decodes to invalid UTF-8 (issue #774 review).
fn has_only_well_formed_percent_escapes(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let well_formed = bytes.get(i + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(i + 2).is_some_and(u8::is_ascii_hexdigit);
            if !well_formed {
                return false;
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_raw_query_pairs_strict_empty_input_is_empty_pairs() {
        assert_eq!(parse_raw_query_pairs_strict(""), Ok(Vec::new()));
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_normal_pairs() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=email"),
            Ok(vec![("queue_name".to_string(), "email".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_percent_encoded_values() {
        // %20 must decode to a literal space, matching form_urlencoded.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=hello%20world"),
            Ok(vec![("queue_name".to_string(), "hello world".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_decodes_plus_as_space() {
        // application/x-www-form-urlencoded: unescaped `+` means space.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=a+b"),
            Ok(vec![("queue_name".to_string(), "a b".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_a_literal_plus_is_encoded_as_percent_2b() {
        // %2B is a percent-encoded literal '+', distinct from bare '+'
        // (which means space) -- the two must decode differently.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=a%2Bb"),
            Ok(vec![("queue_name".to_string(), "a+b".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_invalid_utf8_in_value() {
        // 0xFF is never a valid standalone UTF-8 byte -- this is the exact
        // review-flagged repro (`?queue_name=%FF`).
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=%FF"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_invalid_utf8_in_key() {
        assert_eq!(
            parse_raw_query_pairs_strict("%FF=value"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_a_lone_trailing_percent() {
        // `%` with nothing after it. percent_decode_str leaves it as a
        // literal `%`, which is valid UTF-8 on its own. Only an explicit
        // hex-escape well-formedness check catches this.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_a_percent_with_one_hex_digit() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%2"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_non_hex_percent_escape() {
        // `%GG` -- the exact review-flagged repro. `G` is not a hex digit,
        // so percent_decode_str leaves `%GG` undecoded rather than erroring;
        // the caller must not silently query the literal "orders%GG".
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=orders%GG"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_rejects_non_hex_percent_escape_in_key() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue%ZZname=value"),
            Err(InvalidQueryEncoding)
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_accepts_well_formed_lowercase_hex_escape() {
        // Lowercase hex digits are just as well-formed as uppercase.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=hello%2fworld"),
            Ok(vec![("queue_name".to_string(), "hello/world".to_string())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_skips_empty_segments() {
        // A leading/trailing/doubled `&` must not produce a spurious
        // empty-key pair.
        assert_eq!(
            parse_raw_query_pairs_strict("&a=1&&b=2&"),
            Ok(vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_key_without_equals_has_empty_value() {
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name"),
            Ok(vec![("queue_name".to_string(), String::new())])
        );
    }

    #[test]
    fn parse_raw_query_pairs_strict_preserves_percent_encoded_whitespace_padding() {
        // Regression tie-in: a caller who genuinely percent-encodes
        // surrounding whitespace must still get it back verbatim through
        // the strict decoder.
        assert_eq!(
            parse_raw_query_pairs_strict("queue_name=%20email%20"),
            Ok(vec![("queue_name".to_string(), " email ".to_string())])
        );
    }

    #[test]
    fn bad_request_response_has_400_status_and_documented_error_body() {
        let response = bad_request_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn decode_or_autumn_error_treats_absent_query_as_empty_pairs() {
        // AutumnError does not implement PartialEq, so the Ok side is
        // asserted directly rather than via assert_eq! on the whole Result.
        assert_eq!(
            decode_or_autumn_error(None).expect("absent query must decode"),
            Vec::new()
        );
    }

    #[test]
    fn decode_or_autumn_error_decodes_well_formed_pairs() {
        assert_eq!(
            decode_or_autumn_error(Some("a=1&b=2")).expect("well-formed pairs must decode"),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn decode_or_autumn_error_rejects_malformed_percent_encoding() {
        assert!(decode_or_autumn_error(Some("queue_name=%FF")).is_err());
    }

    #[test]
    fn decode_or_queue_coverage_bad_request_treats_absent_query_as_empty_pairs() {
        assert_eq!(
            decode_or_queue_coverage_bad_request(None).expect("absent query must decode"),
            Vec::new()
        );
    }

    #[test]
    fn decode_or_queue_coverage_bad_request_returns_400_response_on_malformed_encoding() {
        let response = decode_or_queue_coverage_bad_request(Some("queue_name=%FF"))
            .expect_err("malformed encoding must be rejected");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn decode_or_autumn_error_response_treats_absent_query_as_empty_pairs() {
        assert_eq!(
            decode_or_autumn_error_response(None).expect("absent query must decode"),
            Vec::new()
        );
    }

    #[test]
    fn decode_or_autumn_error_response_returns_400_on_malformed_encoding() {
        let response = decode_or_autumn_error_response(Some("queue_name=%FF"))
            .expect_err("malformed encoding must be rejected");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
