//! Regression test for issue #1579.
//!
//! `autumn-web` renders a styled HTML error page for a request whose
//! `Accept` header resolves to a browser preference. A bare wildcard
//! `Accept: */*` resolves that way. A genuinely missing header does
//! not: autumn-web treats an absent header as unspecified, and
//! unspecified is not a browser preference. `curl` sends `Accept:
//! */*` by default. A validation error, an unknown `--state` filter
//! for example, then came back as an HTML page. It did not come back
//! as the documented `application/problem+json` body.
//!
//! These tests pin the real behavior for both request shapes. They
//! also confirm an explicit `Accept: application/json` header avoids
//! the HTML page regardless of a client's own default. That header
//! is what the fixed `harvest` CLI now sends on every request. No
//! database is needed; the route below never touches storage.

use autumn_web::error::AutumnError;
use autumn_web::prelude::*;
use autumn_web::test::TestApp;

#[get("/boom")]
async fn boom() -> Result<&'static str, AutumnError> {
    Err(AutumnError::bad_request_msg("bad input"))
}

#[tokio::test]
async fn bare_wildcard_accept_gets_an_html_error_page() {
    // `curl`'s own default when no `-H 'Accept: ...'` flag is given.
    let client = TestApp::new().routes(routes![boom]).build();

    let response = client.get("/boom").header("accept", "*/*").send().await;

    response
        .assert_status(400)
        .assert_header_contains("content-type", "text/html");
}

#[tokio::test]
async fn missing_accept_header_is_not_rewritten_to_html() {
    // A genuinely absent `Accept` header, distinct from a bare `*/*`.
    let client = TestApp::new().routes(routes![boom]).build();

    let response = client.get("/boom").send().await;

    response
        .assert_status(400)
        .assert_header_contains("content-type", "application/problem+json");
}

#[tokio::test]
async fn explicit_json_accept_gets_the_documented_problem_json_body() {
    // This is what the fixed CLI now sends on every request.
    let client = TestApp::new().routes(routes![boom]).build();

    let response = client
        .get("/boom")
        .header("accept", "application/json")
        .send()
        .await;

    response
        .assert_status(400)
        .assert_header_contains("content-type", "application/problem+json")
        .assert_body_contains("bad input");
}
