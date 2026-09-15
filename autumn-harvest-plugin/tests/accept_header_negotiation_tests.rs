//! Regression test for issue #1579.
//!
//! `autumn-web` renders a styled HTML error page for a request whose
//! `Accept` header prefers HTML. A missing header counts too: the
//! default resolution favors browser navigation. The `harvest` CLI
//! sent no `Accept` header at all. A validation error, an unknown
//! `--state` filter for example, then came back as an HTML page. It
//! did not come back as the documented `application/problem+json`
//! body.
//!
//! These tests pin the real, documented behavior. An explicit
//! `Accept: application/json` header now buys this for the CLI, and
//! for any other direct API client: the same route, only the request
//! header differs. No database is needed; the route below never
//! touches storage.

use autumn_web::error::AutumnError;
use autumn_web::prelude::*;
use autumn_web::test::TestApp;

#[get("/boom")]
async fn boom() -> Result<&'static str, AutumnError> {
    Err(AutumnError::bad_request_msg("bad input"))
}

#[tokio::test]
async fn missing_accept_header_gets_an_html_error_page() {
    // This is the `harvest` CLI's old behavior, and still `curl`'s: no
    // `Accept` header at all. autumn-web resolves this to HTML.
    let client = TestApp::new().routes(routes![boom]).build();

    let response = client.get("/boom").send().await;

    response
        .assert_status(400)
        .assert_header_contains("content-type", "text/html");
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
