//! Fuzz target: SSRF callback-URL validation is total.
//!
//! `completion_callback::validate_target_url` is the security boundary for
//! operator-registered completion-callback targets (issue #605): it parses an
//! arbitrary URL and applies scheme/host/IP-literal rules. A panic here is a
//! denial-of-service on the registration path. Raw-bytes fuzzing drives the
//! `url` crate's parser plus the IPv4/IPv6 literal classification over
//! byte-level malformed URL input.
//!
//! We use the *most permissive* policy (allow http + IP literals) so the fuzzer
//! reaches the deepest branches — the parse, the userinfo/host extraction, and
//! the routable-IP classification — rather than short-circuiting on scheme.
//!
//! Invariant: `validate_target_url` returns `Ok`/`Err` and never panics.
#![no_main]

use autumn_harvest::completion_callback::{SsrfPolicy, validate_target_url};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let raw = String::from_utf8_lossy(data);
    let policy = SsrfPolicy::default()
        .with_allow_http(true)
        .with_allow_ip_literals(true);
    let _ = validate_target_url(&raw, &policy);
});
