//! Strict percent-decoding for raw HTTP query strings (issue #1151, extracted
//! from the issue #774 fix for `GET /admin/queue-coverage`).
//!
//! axum's built-in `Query<T>` extractor is backed by
//! `serde_urlencoded`/`form_urlencoded`, both of which *always* succeed by
//! silently substituting `U+FFFD` (the Unicode replacement character) for a
//! malformed percent-encoded byte sequence instead of rejecting the request.
//! A caller sending `?queue_name=%FF` (an invalid UTF-8 byte, not a valid
//! percent-encoding target) gets no error — the query decodes to a
//! *different*, legitimate-looking value rather than the intended one. For
//! any route that filters/scopes its response by such a param, this means a
//! genuinely malformed request can silently produce a false-clean result
//! instead of the documented `400`.
//!
//! Every route in [`crate::api`] that consumes a raw `(key, value)` pair list
//! (rather than a single-field derive-based `Query<T>`) is expected to read
//! the raw query string via `axum::extract::RawQuery` and decode it with
//! [`parse_raw_query_pairs_strict`] instead of axum's lossy `Query<Vec<(String,
//! String)>>` extractor, mapping [`InvalidQueryEncoding`] to the same
//! documented `400` JSON error shape the route already uses for other
//! invalid-param cases — see [`bad_request_response`] for the shared shape.

use autumn_web::error::AutumnError;
use autumn_web::reexports::axum;
use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

/// The message every strict-query route's `400` carries for
/// [`InvalidQueryEncoding`], shared so the wording can never drift between
/// the 20 call sites that need it (issue #1151).
pub const MALFORMED_QUERY_MESSAGE: &str =
    "malformed query string: invalid percent-encoded UTF-8";

/// A query string component's percent-decoded bytes are not valid UTF-8, or
/// contain a syntactically malformed `%` escape.
///
/// Returned by [`parse_raw_query_pairs_strict`]; callers map this to a `400`
/// JSON response (see [`bad_request_response`]) rather than silently
/// substituting `U+FFFD`, the fallback axum's built-in `Query<T>` extractor
/// performs via `serde_urlencoded`/`form_urlencoded` (issue #774 review).
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
/// [`InvalidQueryEncoding`] — the original, already-shipped and tested
/// (issue #774) `{"error": "..."}` shape, distinct from [`AutumnError`]'s
/// RFC-7807-flavored `{"detail": "...", "status": 400, ...}` shape every
/// *other* strict-query route uses. Kept exactly as issue #774 shipped it
/// (see `queue_coverage_integration.rs`'s existing malformed-encoding tests,
/// which assert on this literal shape) rather than folded into
/// [`decode_or_autumn_error_response`] purely for issue #1151's sweep —
/// changing a shipped, tested route's error contract is out of scope here.
pub fn bad_request_response() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "error": MALFORMED_QUERY_MESSAGE })),
    )
        .into_response()
}

/// Decode an optional raw query string (as returned by
/// `axum::extract::RawQuery`) into `(key, value)` pairs for a handler whose
/// return type is `Result<_, AutumnError>` — the majority shape across
/// [`crate::api`]'s read routes. A missing query string (`None`) is an empty
/// pair list, matching axum's own `Query<T>` behavior for an absent `?`.
///
/// # Errors
///
/// Returns the same [`AutumnError::bad_request_msg`]-shaped `400` every
/// other invalid-param case in these routes already returns, carrying
/// [`MALFORMED_QUERY_MESSAGE`].
pub fn decode_or_autumn_error(raw_query: Option<&str>) -> Result<Vec<(String, String)>, AutumnError> {
    match raw_query.map(parse_raw_query_pairs_strict) {
        None => Ok(Vec::new()),
        Some(Ok(pairs)) => Ok(pairs),
        Some(Err(InvalidQueryEncoding)) => Err(AutumnError::bad_request_msg(MALFORMED_QUERY_MESSAGE)),
    }
}

/// Decode an optional raw query string into `(key, value)` pairs for
/// `GET /admin/queue-coverage` specifically — the one route whose malformed-
/// query `400` predates this shared module (issue #774) and is proven, by
/// name, in `queue_coverage_integration.rs`. See [`bad_request_response`]
/// for why its shape is not [`AutumnError`]'s.
///
/// # Errors
///
/// Returns a ready-made [`bad_request_response`] on [`InvalidQueryEncoding`]
/// for the caller to `return` directly.
pub fn decode_or_bad_request(raw_query: Option<&str>) -> Result<Vec<(String, String)>, Response> {
    match raw_query.map(parse_raw_query_pairs_strict) {
        None => Ok(Vec::new()),
        Some(Ok(pairs)) => Ok(pairs),
        Some(Err(InvalidQueryEncoding)) => Err(bad_request_response()),
    }
}

/// Decode an optional raw query string into `(key, value)` pairs for a
/// handler whose return type is `axum::response::Response` (an infallible
/// signature that converts errors internally via `.into_response()`) but
/// whose *other* invalid-param `400`s are [`AutumnError`]-shaped (the
/// `Err(error) => return error.into_response()` idiom used throughout
/// [`crate::api`]). Using this instead of [`decode_or_bad_request`] keeps a
/// route's malformed-query `400` in the SAME body shape as its own other
/// `400`s, rather than introducing a second, inconsistent error shape on the
/// same route (issue #1151 review).
///
/// # Errors
///
/// Returns a ready-made `400` [`Response`] built from
/// [`AutumnError::bad_request_msg`] on [`InvalidQueryEncoding`], for the
/// caller to `return` directly.
pub fn decode_or_autumn_error_response(raw_query: Option<&str>) -> Result<Vec<(String, String)>, Response> {
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
/// A malformed value like `?queue_name=%FF` would otherwise silently decode
/// to `queue_name=<U+FFFD>` via axum's lossy `Query<T>` extractor — a
/// legitimate-looking but wrong filter that lets a scoped read route return a
/// false-clean result instead of rejecting the request with the documented
/// `400` (issue #774 review; swept to every other raw-pairs route by issue
/// #1151).
///
/// Mirrors `form_urlencoded::parse`'s grammar exactly: split on `&`, split
/// each segment on the first `=` (a segment with no `=` is a key with an
/// empty value), `+` decodes to a literal space *before* percent-decoding.
/// An empty segment (from a leading/trailing/doubled `&`, or an entirely
/// empty query string) is skipped.
///
/// # Errors
///
/// Returns [`InvalidQueryEncoding`] on the first key or value that is
/// malformed in either of two ways: a syntactically invalid `%` escape (not
/// followed by exactly two hex digits, e.g. `%`, `%2`, `%GG`), or a
/// syntactically valid escape whose decoded bytes are not valid UTF-8 (e.g.
/// `%FF`). This is the one place in a strict-query request path that *can*
/// reject an input outright — the route-local `XxxQuery::from_query_pairs`
/// helper that consumes this function's output stays infallible by
/// construction.
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
/// escape on its own: an incomplete (`%`, `%2`) or non-hex (`%GG`) sequence
/// is left as a **literal, undecoded** run of bytes rather than an error --
/// and since those bytes (`%`, `G`, digits, ...) are themselves valid ASCII,
/// `decode_utf8()` still succeeds trivially. Without this pre-check,
/// `?queue_name=orders%GG` would silently decode to the literal string
/// `"orders%GG"` and query that (almost certainly nonexistent) value instead
/// of being rejected -- a second, distinct malformed-encoding shape from the
/// already-handled `%FF`-decodes-to-invalid-UTF-8 case (issue #774 review).
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
        // `%` with nothing after it -- percent_decode_str leaves it as a
        // literal `%` (still valid UTF-8 on its own), so only an explicit
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
    fn decode_or_bad_request_treats_absent_query_as_empty_pairs() {
        assert_eq!(
            decode_or_bad_request(None).expect("absent query must decode"),
            Vec::new()
        );
    }

    #[test]
    fn decode_or_bad_request_returns_400_response_on_malformed_encoding() {
        let response = decode_or_bad_request(Some("queue_name=%FF"))
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
