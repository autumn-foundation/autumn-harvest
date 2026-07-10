//! Property tests for [`autumn_harvest::completion_callback::validate_target_url`]
//! SSRF guarding (Tier B target 10).
//!
//! Contract under test:
//! - a non-routable IP-literal target (loopback / private / link-local / CGNAT /
//!   ULA / unspecified / multicast / broadcast) is **never** accepted — not even
//!   under the maximally permissive policy (`allow_ip_literals = true`,
//!   `allow_http = true`);
//! - `validate_target_url` is total (never panics) on arbitrary input.

use autumn_harvest::completion_callback::{HostAllowlist, SsrfPolicy, validate_target_url};
use proptest::prelude::*;

use super::prop_config::config;

/// The most permissive policy that still enforces IP-literal routability. If a
/// URL is rejected under *this* policy, it is rejected under every policy.
fn permissive_policy() -> SsrfPolicy {
    SsrfPolicy::new(HostAllowlist::new())
        .with_allow_ip_literals(true)
        .with_allow_http(true)
}

/// Strategy producing a URL string whose host is a guaranteed-non-routable
/// IPv4 literal. (Note: `0.x.y.z` other than `0.0.0.0` is deliberately excluded
/// — only `0.0.0.0` is `is_unspecified`, so a general `0.a.b.c` is not blocked
/// by the guard and would be a false expectation.)
fn internal_ipv4_url() -> impl Strategy<Value = String> {
    let octet = 0u8..=255;
    prop_oneof![
        (octet.clone(), octet.clone(), octet.clone())
            .prop_map(|(a, b, c)| format!("127.{a}.{b}.{c}")),
        (octet.clone(), octet.clone(), octet.clone())
            .prop_map(|(a, b, c)| format!("10.{a}.{b}.{c}")),
        (16u8..=31, octet.clone(), octet.clone()).prop_map(|(a, b, c)| format!("172.{a}.{b}.{c}")),
        (octet.clone(), octet.clone()).prop_map(|(b, c)| format!("192.168.{b}.{c}")),
        (octet.clone(), octet.clone()).prop_map(|(b, c)| format!("169.254.{b}.{c}")),
        (64u8..=127, octet.clone(), octet.clone()).prop_map(|(a, b, c)| format!("100.{a}.{b}.{c}")),
        Just("0.0.0.0".to_string()),
        Just("255.255.255.255".to_string()),
        (224u8..=239, octet.clone(), octet.clone(), octet)
            .prop_map(|(a, b, c, d)| format!("{a}.{b}.{c}.{d}")),
    ]
    .prop_map(|ip| format!("https://{ip}/callback"))
}

/// Strategy producing a URL string whose host is a non-routable IPv6 literal.
fn internal_ipv6_url() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("::1".to_string()),
        Just("::".to_string()),
        any::<u16>().prop_map(|n| format!("fc00::{n:x}")),
        any::<u16>().prop_map(|n| format!("fd00::{n:x}")),
        any::<u16>().prop_map(|n| format!("fe80::{n:x}")),
    ]
    .prop_map(|ip| format!("https://[{ip}]/callback"))
}

proptest! {
    #![proptest_config(config())]

    /// Internal IPv4 targets are never accepted, even by the permissive policy.
    #[test]
    fn internal_ipv4_never_validates(url in internal_ipv4_url()) {
        let policy = permissive_policy();
        prop_assert!(
            validate_target_url(&url, &policy).is_err(),
            "internal IPv4 URL was accepted: {url}"
        );
    }

    /// Internal IPv6 targets are never accepted, even by the permissive policy.
    #[test]
    fn internal_ipv6_never_validates(url in internal_ipv6_url()) {
        let policy = permissive_policy();
        prop_assert!(
            validate_target_url(&url, &policy).is_err(),
            "internal IPv6 URL was accepted: {url}"
        );
    }

    /// Total function: never panics on arbitrary input.
    #[test]
    fn validate_is_total(s in any::<String>()) {
        let policy = permissive_policy();
        let _ = validate_target_url(&s, &policy);
    }
}
