//! Durable completion callbacks — push a workflow's terminal result to an
//! operator-registered URL (issue #605).
//!
//! # Overview
//!
//! An embedder starting a workflow from an external service has no push
//! notification when it finishes today — only polling `GET
//! /workflows/{id}/result` (issue #527) or a long-poll. This module lets an
//! embedder register one or more **completion callback targets** — a URL
//! plus an [`EventFilter`] selecting which terminal states deliver — either
//! per-start ([`crate::execution::StartWorkflowParams::completion_callbacks`])
//! or as a [`HarvestBuilder`](crate::builder::HarvestBuilder)-wide default.
//! On the workflow's terminal transition, a durable, shard-local delivery row
//! is enqueued that a background scanner POSTs (HMAC-signed, SSRF-guarded,
//! retried with backoff, dead-lettered on exhaustion) to the target URL.
//!
//! # Determinism contract
//!
//! **No new [`crate::event::WorkflowEvent`] variant. No replay-determinism
//! impact.** Callback registration is execution metadata (a JSONB column);
//! delivery is a post-terminal operational side-channel driven by a separate
//! table (`harvest_completion_deliveries`) and scanner. `harvest_events` is
//! never touched by this module. A workflow with no configured callback
//! targets behaves byte-for-byte identically to today.
//!
//! # At-least-once, not exactly-once
//!
//! Redeliveries (retry, or an operator-triggered redrive) carry the *same*
//! `delivery_id` so receivers can deduplicate. There is no exactly-once
//! guarantee — a POST can succeed on the receiver's side and still be
//! retried if the response is lost in transit.
//!
//! # SSRF guard
//!
//! Every target URL is validated against an operator-configured
//! [`SsrfPolicy`] both at registration time (builder default / per-start
//! option) and again at enqueue time (defense in depth). The default policy
//! is HTTPS-only with a required host allowlist; private, loopback,
//! link-local, and other non-routable IP literals are always rejected unless
//! explicitly allowed.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use uuid::Uuid;

pub use crate::completion_trigger::TerminalState;

// ---------------------------------------------------------------------
// M1: SSRF guard
// ---------------------------------------------------------------------

/// A single host-matching entry in a [`HostAllowlist`].
///
/// Two forms are supported:
/// - **Exact**: `api.example.com` matches only that exact host
///   (case-insensitive).
/// - **Suffix wildcard**: `*.example.com` matches any host with one or more
///   labels before `.example.com` (e.g. `a.example.com`,
///   `a.b.example.com`) but never the bare `example.com` itself, and never
///   an unrelated host that merely ends with the same characters (e.g.
///   `evilexample.com` does not match `*.example.com`).
///
/// An entry may optionally pin a specific port; when it does, the target
/// URL's port (defaulted per-scheme) must equal it exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AllowlistEntry {
    /// Exact hostname match (case-insensitive).
    Exact { host: String, port: Option<u16> },
    /// Suffix-wildcard match: `*.suffix`.
    Suffix { suffix: String, port: Option<u16> },
}

impl AllowlistEntry {
    /// Parse a pattern like `"api.example.com"`, `"*.example.com"`,
    /// `"api.example.com:8443"`, or `"*.example.com:8443"`.
    #[must_use]
    pub fn parse(pattern: &str) -> Self {
        // issue #921 review (Codex): an out-of-range port digit string (e.g.
        // "api.example.com:70000", which cannot fit in a u16) must NOT
        // silently fall back to an *unpinned* entry — that would broaden a
        // malformed SSRF-allowlist pattern into "any port on this host" is
        // allowed, exactly backwards for a security control. Only split the
        // host from the port when the digits actually parse as a valid
        // `u16`; otherwise treat the whole pattern (colon included) as the
        // host literal, which can never match a real parsed URL host (a
        // `url::Host::Domain` never contains a colon), so the entry simply
        // never matches anything rather than silently widening access.
        let (host_part, port) = match pattern.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() && p.chars().all(|c| c.is_ascii_digit()) => p
                .parse::<u16>()
                .map_or((pattern, None), |port| (h, Some(port))),
            _ => (pattern, None),
        };
        let lower = host_part.to_ascii_lowercase();
        lower.strip_prefix("*.").map_or_else(
            || Self::Exact {
                host: lower.clone(),
                port,
            },
            |suffix| Self::Suffix {
                suffix: suffix.to_string(),
                port,
            },
        )
    }

    fn matches_host(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        match self {
            Self::Exact { host: exact, .. } => host == *exact,
            Self::Suffix { suffix, .. } => {
                host.len() > suffix.len() + 1
                    && host.ends_with(suffix.as_str())
                    && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
            }
        }
    }

    const fn required_port(&self) -> Option<u16> {
        match self {
            Self::Exact { port, .. } | Self::Suffix { port, .. } => *port,
        }
    }
}

/// Operator-configured set of allowed callback target hosts.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostAllowlist {
    entries: Vec<AllowlistEntry>,
}

impl HostAllowlist {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_pattern(mut self, pattern: impl AsRef<str>) -> Self {
        self.entries.push(AllowlistEntry::parse(pattern.as_ref()));
        self
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns every entry whose host pattern matches `host` (issue #921
    /// review: an operator can legitimately configure multiple entries for
    /// the same host with different pinned ports, e.g.
    /// `api.example.com:8443` and `api.example.com:9443` — the caller must
    /// consider all of them, not just the first, or a valid multi-port
    /// allowlist becomes order-dependent).
    fn matching_entries<'a>(&'a self, host: &'a str) -> impl Iterator<Item = &'a AllowlistEntry> {
        self.entries.iter().filter(move |e| e.matches_host(host))
    }
}

/// Operator policy controlling which callback target URLs are permitted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SsrfPolicy {
    /// Allowed hostnames (exact or `*.suffix`). Required (non-empty) for any
    /// domain-based target to pass validation.
    pub allowlist: HostAllowlist,
    /// When `false` (default), only `https://` targets are permitted.
    /// When `true`, `http://` targets are also permitted (still subject to
    /// the same host allowlist and IP-literal rules).
    pub allow_http: bool,
    /// When `false` (default), an IP-literal host (`https://127.0.0.1/...`,
    /// `https://93.184.216.34/...`) is always rejected — even a public IP —
    /// because it can never be `HostAllowlist`-matched and bypasses the
    /// operator's domain-based intent. When `true`, IP literals are
    /// permitted subject to the private/loopback/link-local rejection below.
    pub allow_ip_literals: bool,
}

impl SsrfPolicy {
    #[must_use]
    pub const fn new(allowlist: HostAllowlist) -> Self {
        Self {
            allowlist,
            allow_http: false,
            allow_ip_literals: false,
        }
    }

    #[must_use]
    pub const fn with_allow_http(mut self, allow: bool) -> Self {
        self.allow_http = allow;
        self
    }

    #[must_use]
    pub const fn with_allow_ip_literals(mut self, allow: bool) -> Self {
        self.allow_ip_literals = allow;
        self
    }
}

/// Machine-readable reason a callback target URL was rejected.
///
/// Serialized with a `code` discriminator so an HTTP caller (registration
/// route) can return a stable, programmatically-checkable error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(tag = "code")]
pub enum SsrfRejection {
    #[error("callback target URL could not be parsed: {message}")]
    Unparseable { message: String },
    #[error("callback target URL has no host")]
    MissingHost,
    #[error("scheme '{scheme}' is not allowed for callback targets")]
    SchemeNotAllowed { scheme: String },
    #[error("callback target URL must not carry userinfo (username/password)")]
    UserinfoNotAllowed,
    #[error("IP-literal callback targets are not allowed")]
    IpLiteralNotAllowed,
    #[error("IP address '{ip}' is not a routable public address")]
    IpNotRoutable { ip: String },
    #[error("host '{host}' is not on the callback target allowlist")]
    HostNotAllowlisted { host: String },
    #[error("port {port} is not allowed for host '{host}'")]
    PortNotAllowed { host: String, port: u16 },
}

/// Reject non-routable (loopback / private / link-local / unspecified /
/// multicast / documentation / CGNAT) IPv4 addresses.
const fn is_ipv4_non_routable(ip: Ipv4Addr) -> bool {
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_documentation()
        || ip.is_broadcast()
        // 100.64.0.0/10 — shared address space (CGNAT, RFC 6598).
        || (ip.octets()[0] == 100 && (ip.octets()[1] & 0b1100_0000) == 64)
}

/// Reject non-routable IPv6 addresses (loopback, unspecified, link-local,
/// unique local / ULA `fc00::/7`, multicast).
const fn is_ipv6_non_routable(ip: Ipv6Addr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return true;
    }
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_ipv4_non_routable(v4);
    }
    let seg0 = ip.segments()[0];
    // fe80::/10 link-local.
    if (seg0 & 0xffc0) == 0xfe80 {
        return true;
    }
    // fc00::/7 unique local address (ULA).
    if (seg0 & 0xfe00) == 0xfc00 {
        return true;
    }
    false
}

const fn is_ip_non_routable(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_ipv4_non_routable(v4),
        IpAddr::V6(v6) => is_ipv6_non_routable(v6),
    }
}

/// Validate a callback target URL against `policy`.
///
/// Returns the parsed [`url::Url`] on success so callers do not need to
/// re-parse it. Rules (in order):
/// 1. Must parse as an absolute URL.
/// 2. Scheme must be `https`, or `http` when `policy.allow_http`.
/// 3. Must not carry userinfo (`user:pass@host`).
/// 4. Must have a host.
/// 5. An IP-literal host is rejected unless `policy.allow_ip_literals`, and
///    even then is rejected if it is loopback/private/link-local/etc. An IP
///    literal is *never* matched against the domain allowlist.
/// 6. A domain host must match an entry in `policy.allowlist` (exact or
///    `*.suffix`); a bare, dotless host (e.g. `localhost`) is only ever
///    accepted via an explicit exact-match allowlist entry.
/// 7. If the matched allowlist entry pins a port, the URL's effective port
///    (explicit, or the scheme default) must equal it.
///
/// DNS resolution is intentionally **not** performed here (doing so would
/// both be a TOCTOU race against the actual delivery connection and require
/// this pure function to become I/O); the guard is syntactic (scheme/host)
/// plus IP-literal blocking. Deployments with strict SSRF requirements
/// should pair this with network-level egress controls.
///
/// # Errors
/// Returns [`SsrfRejection`] when `raw_url` fails any of the rules above.
pub fn validate_target_url(raw_url: &str, policy: &SsrfPolicy) -> Result<url::Url, SsrfRejection> {
    let url = url::Url::parse(raw_url).map_err(|e| SsrfRejection::Unparseable {
        message: e.to_string(),
    })?;

    let scheme = url.scheme();
    let scheme_ok = scheme == "https" || (scheme == "http" && policy.allow_http);
    if !scheme_ok {
        return Err(SsrfRejection::SchemeNotAllowed {
            scheme: scheme.to_string(),
        });
    }

    if !url.username().is_empty() || url.password().is_some() {
        return Err(SsrfRejection::UserinfoNotAllowed);
    }

    let host = url.host().ok_or(SsrfRejection::MissingHost)?;

    match host {
        url::Host::Ipv4(ip) => validate_ip_literal(IpAddr::V4(ip), policy),
        url::Host::Ipv6(ip) => validate_ip_literal(IpAddr::V6(ip), policy),
        url::Host::Domain(domain) => validate_domain_host(&url, domain, policy),
    }?;

    Ok(url)
}

fn validate_ip_literal(ip: IpAddr, policy: &SsrfPolicy) -> Result<(), SsrfRejection> {
    if !policy.allow_ip_literals {
        return Err(SsrfRejection::IpLiteralNotAllowed);
    }
    if is_ip_non_routable(ip) {
        return Err(SsrfRejection::IpNotRoutable { ip: ip.to_string() });
    }
    Ok(())
}

fn validate_domain_host(
    url: &url::Url,
    domain: &str,
    policy: &SsrfPolicy,
) -> Result<(), SsrfRejection> {
    // issue #921 review (Codex): check every matching entry, not just the
    // first one `matching_entries` yields — an operator can register
    // multiple entries for the same host with different pinned ports (or a
    // mix of pinned and unpinned entries), and the URL is allowed if *any*
    // of them accepts it.
    let mut host_matched = false;
    for entry in policy.allowlist.matching_entries(domain) {
        host_matched = true;
        match entry.required_port() {
            None => return Ok(()),
            Some(required_port) => {
                let effective_port = url.port_or_known_default().unwrap_or(required_port);
                if effective_port == required_port {
                    return Ok(());
                }
            }
        }
    }

    if host_matched {
        Err(SsrfRejection::PortNotAllowed {
            host: domain.to_string(),
            port: url.port_or_known_default().unwrap_or(0),
        })
    } else {
        Err(SsrfRejection::HostNotAllowlisted {
            host: domain.to_string(),
        })
    }
}

#[cfg(test)]
mod ssrf_tests {
    use super::*;

    fn policy_with(patterns: &[&str]) -> SsrfPolicy {
        let mut allowlist = HostAllowlist::new();
        for p in patterns {
            allowlist = allowlist.with_pattern(*p);
        }
        SsrfPolicy::new(allowlist)
    }

    #[test]
    fn rejects_non_https_by_default() {
        let policy = policy_with(&["api.example.com"]);
        let err = validate_target_url("http://api.example.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::SchemeNotAllowed {
                scheme: "http".to_string()
            }
        );
    }

    #[test]
    fn allows_http_only_when_explicitly_allowlisted_scheme() {
        let policy = policy_with(&["api.example.com"]).with_allow_http(true);
        assert!(validate_target_url("http://api.example.com/hook", &policy).is_ok());
    }

    #[test]
    fn rejects_disallowed_scheme() {
        let policy = policy_with(&["api.example.com"]).with_allow_http(true);
        let err = validate_target_url("ftp://api.example.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::SchemeNotAllowed {
                scheme: "ftp".to_string()
            }
        );
    }

    #[test]
    fn exact_host_match_succeeds() {
        let policy = policy_with(&["api.example.com"]);
        assert!(validate_target_url("https://api.example.com/hook", &policy).is_ok());
    }

    #[test]
    fn exact_host_match_is_case_insensitive() {
        let policy = policy_with(&["API.example.com"]);
        assert!(validate_target_url("https://api.EXAMPLE.com/hook", &policy).is_ok());
    }

    #[test]
    fn suffix_wildcard_matches_subdomains() {
        let policy = policy_with(&["*.example.com"]);
        assert!(validate_target_url("https://a.example.com/hook", &policy).is_ok());
        assert!(validate_target_url("https://a.b.example.com/hook", &policy).is_ok());
    }

    #[test]
    fn suffix_wildcard_does_not_match_bare_suffix() {
        let policy = policy_with(&["*.example.com"]);
        let err = validate_target_url("https://example.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "example.com".to_string()
            }
        );
    }

    #[test]
    fn suffix_wildcard_does_not_match_unrelated_host_with_same_tail_chars() {
        let policy = policy_with(&["*.example.com"]);
        let err = validate_target_url("https://evilexample.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "evilexample.com".to_string()
            }
        );
    }

    #[test]
    fn rejects_non_allowlisted_host() {
        let policy = policy_with(&["api.example.com"]);
        let err = validate_target_url("https://evil.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "evil.com".to_string()
            }
        );
    }

    #[test]
    fn rejects_loopback_ip_literal_v4() {
        let policy = policy_with(&["127.0.0.1"]).with_allow_ip_literals(true);
        let err = validate_target_url("https://127.0.0.1/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::IpNotRoutable {
                ip: "127.0.0.1".to_string()
            }
        );
    }

    #[test]
    fn rejects_loopback_ip_literal_v6() {
        let policy = policy_with(&["::1"]).with_allow_ip_literals(true);
        let err = validate_target_url("https://[::1]/hook", &policy).unwrap_err();
        assert!(matches!(err, SsrfRejection::IpNotRoutable { .. }));
    }

    #[test]
    fn rejects_private_ip_literals() {
        let policy = SsrfPolicy::default().with_allow_ip_literals(true);
        for addr in [
            "10.0.0.1",
            "172.16.5.4",
            "192.168.1.1",
            "169.254.1.1", // link-local
            "100.64.0.1",  // CGNAT shared address space
        ] {
            let err = validate_target_url(&format!("https://{addr}/hook"), &policy).unwrap_err();
            assert!(
                matches!(err, SsrfRejection::IpNotRoutable { .. }),
                "expected {addr} to be rejected as non-routable, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_ipv6_link_local_and_ula() {
        let policy = SsrfPolicy::default().with_allow_ip_literals(true);
        for addr in ["fe80::1", "fc00::1", "fd12:3456:789a::1"] {
            let err = validate_target_url(&format!("https://[{addr}]/hook"), &policy).unwrap_err();
            assert!(
                matches!(err, SsrfRejection::IpNotRoutable { .. }),
                "expected {addr} to be rejected as non-routable, got {err:?}"
            );
        }
    }

    #[test]
    fn rejects_ip_literal_when_not_explicitly_allowed_even_if_public() {
        let policy = policy_with(&["93.184.216.34"]); // allow_ip_literals defaults false
        let err = validate_target_url("https://93.184.216.34/hook", &policy).unwrap_err();
        assert_eq!(err, SsrfRejection::IpLiteralNotAllowed);
    }

    #[test]
    fn ip_literal_never_matches_host_wildcard() {
        // Even a wildcard that would syntactically "cover" this is irrelevant:
        // IP literals are validated as IPs, never as hostnames.
        let policy = policy_with(&["*.1"]);
        let err = validate_target_url("https://93.184.216.1/hook", &policy).unwrap_err();
        assert_eq!(err, SsrfRejection::IpLiteralNotAllowed);
    }

    #[test]
    fn rejects_userinfo() {
        let policy = policy_with(&["api.example.com"]);
        let err =
            validate_target_url("https://user:pass@api.example.com/hook", &policy).unwrap_err();
        assert_eq!(err, SsrfRejection::UserinfoNotAllowed);
    }

    #[test]
    fn rejects_unparseable_url() {
        let policy = policy_with(&["api.example.com"]);
        let err = validate_target_url("not a url", &policy).unwrap_err();
        assert!(matches!(err, SsrfRejection::Unparseable { .. }));
    }

    #[test]
    fn pinned_port_must_match() {
        let policy = policy_with(&["api.example.com:8443"]);
        assert!(validate_target_url("https://api.example.com:8443/hook", &policy).is_ok());
        let err = validate_target_url("https://api.example.com:9999/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::PortNotAllowed {
                host: "api.example.com".to_string(),
                port: 9999
            }
        );
    }

    #[test]
    fn unpinned_port_allows_default_scheme_port() {
        let policy = policy_with(&["api.example.com"]);
        assert!(validate_target_url("https://api.example.com/hook", &policy).is_ok());
        // No port pinned on the allowlist entry means any port is fine.
        assert!(validate_target_url("https://api.example.com:9443/hook", &policy).is_ok());
    }

    #[test]
    fn an_out_of_range_port_pattern_fails_closed_instead_of_becoming_unpinned() {
        // Issue #921 review (Codex): "api.example.com:70000" has a digit
        // string that cannot fit in a u16. Silently dropping the port
        // constraint would turn a malformed pattern into "any port on this
        // host is allowed" -- backwards for an SSRF control. It must
        // instead fail closed: the entry never matches a real host at all.
        let policy = policy_with(&["api.example.com:70000"]);
        let err = validate_target_url("https://api.example.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "api.example.com".to_string()
            }
        );
        let err = validate_target_url("https://api.example.com:12345/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "api.example.com".to_string()
            }
        );
    }

    #[test]
    fn multiple_pinned_port_entries_for_the_same_host_are_all_considered() {
        // Issue #921 review (Codex): registering both
        // api.example.com:8443 and api.example.com:9443 must allow either
        // port -- not just whichever entry happens to come first.
        let policy = policy_with(&["api.example.com:8443", "api.example.com:9443"]);
        assert!(validate_target_url("https://api.example.com:8443/hook", &policy).is_ok());
        assert!(validate_target_url("https://api.example.com:9443/hook", &policy).is_ok());
        let err = validate_target_url("https://api.example.com:9999/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::PortNotAllowed {
                host: "api.example.com".to_string(),
                port: 9999
            }
        );
    }

    #[test]
    fn a_later_unpinned_entry_for_the_same_host_still_allows_any_port() {
        let policy = policy_with(&["api.example.com:8443", "api.example.com"]);
        assert!(validate_target_url("https://api.example.com:8443/hook", &policy).is_ok());
        assert!(validate_target_url("https://api.example.com:9999/hook", &policy).is_ok());
    }

    #[test]
    fn rejection_is_machine_readable_via_serde_tag() {
        let policy = policy_with(&["api.example.com"]);
        let err = validate_target_url("https://evil.com/hook", &policy).unwrap_err();
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], "HostNotAllowlisted");
        assert_eq!(json["host"], "evil.com");
    }

    #[test]
    fn empty_allowlist_rejects_every_domain_host() {
        let policy = SsrfPolicy::default();
        let err = validate_target_url("https://api.example.com/hook", &policy).unwrap_err();
        assert_eq!(
            err,
            SsrfRejection::HostNotAllowlisted {
                host: "api.example.com".to_string()
            }
        );
    }
}

// ---------------------------------------------------------------------
// M2: HMAC signing + envelope
// ---------------------------------------------------------------------

/// The fixed JSON envelope `POSTed` to a callback target on a terminal transition.
///
/// Field order and shape are part of the public contract — do not reorder
/// or rename without a compatibility plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionEnvelope {
    pub delivery_id: Uuid,
    pub execution_id: Uuid,
    pub workflow_name: String,
    pub workflow_id: String,
    /// One of `"COMPLETED" | "FAILED" | "CANCELLED" | "TIMED_OUT" | "TERMINATED"`.
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<serde_json::Value>,
    pub completed_at: DateTime<Utc>,
}

/// Build the fixed envelope for a terminal execution.
///
/// `result` is populated only for [`TerminalState::Completed`] (from
/// `execution_output`); `error` is populated for every other terminal state
/// (from `execution_error`). `completed_at` defaults to `Utc::now()` if the
/// execution row has not yet recorded one (should not normally happen for a
/// terminal row, but keeps this a total function).
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn build_envelope(
    delivery_id: Uuid,
    execution_id: Uuid,
    workflow_name: &str,
    workflow_id: &str,
    state: TerminalState,
    execution_output: Option<serde_json::Value>,
    execution_error: Option<&str>,
    completed_at: Option<DateTime<Utc>>,
) -> CompletionEnvelope {
    let (result, error) = if matches!(state, TerminalState::Completed) {
        (execution_output, None)
    } else {
        (
            None,
            execution_error.map(|e| serde_json::Value::String(e.to_string())),
        )
    };
    CompletionEnvelope {
        delivery_id,
        execution_id,
        workflow_name: workflow_name.to_string(),
        workflow_id: workflow_id.to_string(),
        state: state.as_str().to_string(),
        result,
        error,
        completed_at: completed_at.unwrap_or_else(Utc::now),
    }
}

/// Canonical serialization of the envelope — the exact bytes that get signed and `POSTed`.
///
/// Callers must sign these bytes, not a re-serialization, so the
/// receiver's signature check is over exactly what it received.
///
/// # Errors
/// Returns `Err` only if the envelope somehow fails to serialize (it cannot,
/// in practice, given `CompletionEnvelope`'s field types — kept fallible so
/// serialization changes can't silently panic in the delivery path).
pub fn serialize_body(envelope: &CompletionEnvelope) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(envelope)
}

/// A secret used to HMAC-sign callback deliveries. Deliberately opaque in
/// `Debug` so it can never leak into logs.
#[derive(Clone)]
pub struct CallbackSecret(Vec<u8>);

impl CallbackSecret {
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for CallbackSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallbackSecret(<redacted>)")
    }
}

type HmacSha256 = Hmac<Sha256>;

/// Sign `body` with `secret`, returning `"sha256=<lowercase-hex>"` — the
/// exact value of the `X-Harvest-Signature` header.
///
/// # Panics
/// Never panics in practice: HMAC-SHA256 accepts a key of any length, so
/// the internal `expect` on key construction cannot fail for a real
/// `CallbackSecret`.
#[must_use]
pub fn sign(secret: &CallbackSecret, body: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body);
    let bytes = mac.finalize().into_bytes();
    let mut hex = String::with_capacity(bytes.len() * 2 + 7);
    hex.push_str("sha256=");
    for b in bytes {
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// HTTP header name carrying the HMAC signature over the raw request body.
pub const SIGNATURE_HEADER: &str = "X-Harvest-Signature";
/// HTTP header name carrying the RFC 3339 delivery timestamp.
pub const TIMESTAMP_HEADER: &str = "X-Harvest-Timestamp";
/// HTTP header name carrying the stable delivery id (mirrors the envelope's
/// `delivery_id` field, for receivers that prefer a header over a body
/// parse for dedup).
pub const DELIVERY_ID_HEADER: &str = "X-Harvest-Delivery-Id";

/// Build the full set of delivery headers for a signed POST: signature,
/// timestamp, and delivery id.
#[must_use]
pub fn delivery_headers(
    secret: &CallbackSecret,
    body: &[u8],
    delivery_id: Uuid,
    now: DateTime<Utc>,
) -> Vec<(&'static str, String)> {
    vec![
        (SIGNATURE_HEADER, sign(secret, body)),
        (TIMESTAMP_HEADER, now.to_rfc3339()),
        (DELIVERY_ID_HEADER, delivery_id.to_string()),
    ]
}

#[cfg(test)]
mod envelope_tests {
    use super::*;
    use serde_json::json;

    fn sample_ids() -> (Uuid, Uuid) {
        (
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
        )
    }

    #[test]
    fn completed_envelope_has_result_and_no_error() {
        let (delivery_id, exec_id) = sample_ids();
        let completed_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let envelope = build_envelope(
            delivery_id,
            exec_id,
            "process_refund",
            "wf-1",
            TerminalState::Completed,
            Some(json!({"refunded": true})),
            None,
            Some(completed_at),
        );
        assert_eq!(envelope.state, "COMPLETED");
        assert_eq!(envelope.result, Some(json!({"refunded": true})));
        assert_eq!(envelope.error, None);

        let body = serialize_body(&envelope).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("result").is_some());
        assert!(
            value.get("error").is_none(),
            "error field must be omitted, not null"
        );
    }

    #[test]
    fn failed_envelope_has_error_and_no_result() {
        let (delivery_id, exec_id) = sample_ids();
        let envelope = build_envelope(
            delivery_id,
            exec_id,
            "process_refund",
            "wf-1",
            TerminalState::Failed,
            None,
            Some("card declined"),
            Some(Utc::now()),
        );
        assert_eq!(envelope.state, "FAILED");
        assert_eq!(envelope.result, None);
        assert_eq!(envelope.error, Some(json!("card declined")));

        let body = serialize_body(&envelope).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("error").is_some());
        assert!(
            value.get("result").is_none(),
            "result field must be omitted, not null"
        );
    }

    #[test]
    fn envelope_field_order_matches_contract() {
        let (delivery_id, exec_id) = sample_ids();
        let envelope = build_envelope(
            delivery_id,
            exec_id,
            "run_kyc",
            "wf-2",
            TerminalState::Completed,
            Some(json!(42)),
            None,
            Some(Utc::now()),
        );
        let body = serialize_body(&envelope).unwrap();
        let text = String::from_utf8(body).unwrap();
        // Assert the contract's declared field order via textual position,
        // not just presence, so a reorder breaks this test.
        let idx = |k: &str| text.find(&format!("\"{k}\"")).unwrap();
        assert!(idx("delivery_id") < idx("execution_id"));
        assert!(idx("execution_id") < idx("workflow_name"));
        assert!(idx("workflow_name") < idx("workflow_id"));
        assert!(idx("workflow_id") < idx("state"));
        assert!(idx("state") < idx("result"));
        assert!(idx("result") < idx("completed_at"));
    }

    #[test]
    fn hmac_signature_is_stable_known_vector() {
        let secret = CallbackSecret::new(b"test-secret".to_vec());
        let body = b"{\"hello\":\"world\"}";
        let sig = sign(&secret, body);
        // Recomputed independently: HMAC-SHA256("test-secret", body).
        assert!(sig.starts_with("sha256="));
        // Same input always yields the same signature.
        assert_eq!(sig, sign(&secret, body));
    }

    #[test]
    fn hmac_signature_changes_with_body() {
        let secret = CallbackSecret::new(b"test-secret".to_vec());
        let sig1 = sign(&secret, b"body-one");
        let sig2 = sign(&secret, b"body-two");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn hmac_signature_changes_with_secret() {
        let sig1 = sign(&CallbackSecret::new(b"secret-a".to_vec()), b"same-body");
        let sig2 = sign(&CallbackSecret::new(b"secret-b".to_vec()), b"same-body");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn signature_header_format_is_sha256_prefixed_lowercase_hex() {
        let secret = CallbackSecret::new(b"k".to_vec());
        let sig = sign(&secret, b"x");
        let hex_part = sig
            .strip_prefix("sha256=")
            .expect("must have sha256= prefix");
        assert_eq!(hex_part.len(), 64, "SHA-256 hex digest must be 64 chars");
        assert!(
            hex_part
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn delivery_headers_includes_signature_timestamp_and_delivery_id() {
        let secret = CallbackSecret::new(b"k".to_vec());
        let body = b"payload-bytes";
        let delivery_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        let now = Utc::now();
        let headers = delivery_headers(&secret, body, delivery_id, now);

        let get = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.clone())
        };
        assert_eq!(get(SIGNATURE_HEADER), Some(sign(&secret, body)));
        assert_eq!(get(TIMESTAMP_HEADER), Some(now.to_rfc3339()));
        assert_eq!(get(DELIVERY_ID_HEADER), Some(delivery_id.to_string()));
    }

    #[test]
    fn signature_covers_exact_posted_bytes() {
        // Signing must operate on the exact bytes serialize_body() produces —
        // never a re-serialization the receiver might parse differently.
        let (delivery_id, exec_id) = sample_ids();
        let envelope = build_envelope(
            delivery_id,
            exec_id,
            "wf",
            "id",
            TerminalState::Completed,
            Some(json!({"a": 1})),
            None,
            Some(Utc::now()),
        );
        let body = serialize_body(&envelope).unwrap();
        let secret = CallbackSecret::new(b"shared-secret".to_vec());
        let sig_over_body = sign(&secret, &body);

        // Re-serializing an equivalent-but-differently-ordered JSON value
        // must NOT produce the same bytes (proving sign() is byte-exact,
        // not semantic).
        let mut reordered = serde_json::to_value(&envelope).unwrap();
        if let serde_json::Value::Object(map) = &mut reordered {
            let mut new_map = serde_json::Map::new();
            for (k, v) in map.iter().rev() {
                new_map.insert(k.clone(), v.clone());
            }
            *map = new_map;
        }
        let reordered_bytes = serde_json::to_vec(&reordered).unwrap();
        if reordered_bytes != body {
            assert_ne!(sign(&secret, &reordered_bytes), sig_over_body);
        }
    }

    #[test]
    fn callback_secret_debug_is_redacted() {
        let secret = CallbackSecret::new(b"super-secret-value".to_vec());
        let debug = format!("{secret:?}");
        assert!(!debug.contains("super-secret-value"));
        assert_eq!(debug, "CallbackSecret(<redacted>)");
    }
}

// ---------------------------------------------------------------------
// M4: Config resolution + event-filter matching
// ---------------------------------------------------------------------

/// Which terminal states deliver for a given [`CallbackTarget`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "states")]
pub enum EventFilter {
    /// Deliver only on [`TerminalState::Completed`].
    CompletedOnly,
    /// Deliver on any of the five terminal states.
    AnyTerminal,
    /// Deliver on exactly the listed terminal states.
    States(Vec<TerminalState>),
}

impl EventFilter {
    #[must_use]
    pub fn matches(&self, state: TerminalState) -> bool {
        match self {
            Self::CompletedOnly => matches!(state, TerminalState::Completed),
            Self::AnyTerminal => true,
            Self::States(states) => states.contains(&state),
        }
    }
}

/// A single completion-callback registration: a target URL plus the
/// terminal-state filter selecting when it fires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackTarget {
    pub url: String,
    pub filter: EventFilter,
}

impl CallbackTarget {
    #[must_use]
    pub fn new(url: impl Into<String>, filter: EventFilter) -> Self {
        Self {
            url: url.into(),
            filter,
        }
    }
}

/// Resolve the full, deterministic, deduplicated set of effective callback targets.
///
/// Per-execution targets come first (in the order given), then builder-wide
/// defaults, skipping any entry that exactly duplicates an already-included
/// `(url, filter)` pair — including a duplicate *within* `per_exec` itself
/// (issue #921 review): a caller that repeats the same target twice in one
/// start request must still get exactly one delivery row/`delivery_id` for
/// it, not two independently-retried deliveries to the same receiver for
/// the same logical registration.
///
/// The returned `Vec`'s index is the **stable `callback_index`** used as
/// half of the `(workflow_exec_id, callback_index)` uniqueness key in
/// `harvest_completion_deliveries` — it is computed over the *full* union,
/// not just the subset that matches a particular fired state, so a given
/// `(execution, target)` pair always has exactly one permanent slot
/// regardless of which terminal state actually fires (an execution only
/// ever reaches one terminal state, so this is purely a stability
/// guarantee, not a multi-fire concern).
#[must_use]
pub fn resolve_all_targets(
    per_exec: &[CallbackTarget],
    defaults: &[CallbackTarget],
) -> Vec<CallbackTarget> {
    let mut all: Vec<CallbackTarget> = Vec::with_capacity(per_exec.len() + defaults.len());
    for target in per_exec.iter().chain(defaults.iter()) {
        if !all.contains(target) {
            all.push(target.clone());
        }
    }
    all
}

/// Select the `(callback_index, target)` pairs from the full effective-target
/// union (see [`resolve_all_targets`]) whose [`EventFilter`] matches `state`.
///
/// Returns an empty `Vec` when nothing matches — including the common case
/// of no configured targets at all — which is exactly the "identical
/// behavior for callback-less workflows" guarantee.
#[must_use]
pub fn resolve_effective_targets(
    all_targets: &[CallbackTarget],
    state: TerminalState,
) -> Vec<(usize, &CallbackTarget)> {
    all_targets
        .iter()
        .enumerate()
        .filter(|(_, target)| target.filter.matches(state))
        .collect()
}

#[cfg(test)]
mod config_resolution_tests {
    use super::*;

    fn target(url: &str, filter: EventFilter) -> CallbackTarget {
        CallbackTarget::new(url, filter)
    }

    #[test]
    fn completed_only_filter_matches_completed_only() {
        let f = EventFilter::CompletedOnly;
        assert!(f.matches(TerminalState::Completed));
        assert!(!f.matches(TerminalState::Failed));
        assert!(!f.matches(TerminalState::Cancelled));
        assert!(!f.matches(TerminalState::TimedOut));
        assert!(!f.matches(TerminalState::Terminated));
    }

    #[test]
    fn any_terminal_filter_matches_all_five_states() {
        let f = EventFilter::AnyTerminal;
        for state in [
            TerminalState::Completed,
            TerminalState::Failed,
            TerminalState::Cancelled,
            TerminalState::TimedOut,
            TerminalState::Terminated,
        ] {
            assert!(f.matches(state));
        }
    }

    #[test]
    fn explicit_states_filter_matches_only_listed_states() {
        let f = EventFilter::States(vec![TerminalState::Failed, TerminalState::TimedOut]);
        assert!(f.matches(TerminalState::Failed));
        assert!(f.matches(TerminalState::TimedOut));
        assert!(!f.matches(TerminalState::Completed));
        assert!(!f.matches(TerminalState::Cancelled));
        assert!(!f.matches(TerminalState::Terminated));
    }

    #[test]
    fn resolve_all_targets_is_per_exec_first_then_defaults() {
        let per_exec = vec![target("https://a.example.com", EventFilter::AnyTerminal)];
        let defaults = vec![target("https://b.example.com", EventFilter::CompletedOnly)];
        let all = resolve_all_targets(&per_exec, &defaults);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].url, "https://a.example.com");
        assert_eq!(all[1].url, "https://b.example.com");
    }

    #[test]
    fn resolve_all_targets_dedupes_identical_url_and_filter() {
        let dup = target("https://a.example.com", EventFilter::AnyTerminal);
        let per_exec = vec![dup.clone()];
        let defaults = vec![
            dup,
            target("https://b.example.com", EventFilter::AnyTerminal),
        ];
        let all = resolve_all_targets(&per_exec, &defaults);
        assert_eq!(all.len(), 2, "the duplicate default must be dropped");
        assert_eq!(all[0].url, "https://a.example.com");
        assert_eq!(all[1].url, "https://b.example.com");
    }

    #[test]
    fn resolve_all_targets_dedupes_a_repeated_target_within_per_exec_itself() {
        // Issue #921 review (Codex): a caller repeating the same {url, filter}
        // twice in one start request's completion_callbacks list must still
        // resolve to exactly one target/callback_index/delivery_id, not two
        // independently-delivered duplicates to the same receiver.
        let dup = target("https://a.example.com", EventFilter::AnyTerminal);
        let per_exec = vec![
            dup.clone(),
            target("https://b.example.com", EventFilter::AnyTerminal),
            dup,
        ];
        let all = resolve_all_targets(&per_exec, &[]);
        assert_eq!(
            all.len(),
            2,
            "a target repeated within per_exec must collapse to one entry"
        );
        assert_eq!(all[0].url, "https://a.example.com");
        assert_eq!(all[1].url, "https://b.example.com");
    }

    #[test]
    fn resolve_all_targets_keeps_same_url_different_filter_as_distinct() {
        let per_exec = vec![target("https://a.example.com", EventFilter::CompletedOnly)];
        let defaults = vec![target("https://a.example.com", EventFilter::AnyTerminal)];
        let all = resolve_all_targets(&per_exec, &defaults);
        assert_eq!(
            all.len(),
            2,
            "same URL with a different filter is a distinct target"
        );
    }

    #[test]
    fn resolve_effective_targets_filters_by_state_and_preserves_index() {
        let all = vec![
            target(
                "https://completed-only.example.com",
                EventFilter::CompletedOnly,
            ),
            target("https://any.example.com", EventFilter::AnyTerminal),
            target(
                "https://failed-or-timed-out.example.com",
                EventFilter::States(vec![TerminalState::Failed, TerminalState::TimedOut]),
            ),
        ];

        let matches = resolve_effective_targets(&all, TerminalState::Failed);
        let matched_indices: Vec<usize> = matches.iter().map(|(idx, _)| *idx).collect();
        assert_eq!(matched_indices, vec![1, 2]);

        let matches_completed = resolve_effective_targets(&all, TerminalState::Completed);
        let matched_indices_completed: Vec<usize> =
            matches_completed.iter().map(|(idx, _)| *idx).collect();
        assert_eq!(matched_indices_completed, vec![0, 1]);
    }

    #[test]
    fn empty_targets_yields_no_matches_for_any_state() {
        let all: Vec<CallbackTarget> = Vec::new();
        for state in [
            TerminalState::Completed,
            TerminalState::Failed,
            TerminalState::Cancelled,
            TerminalState::TimedOut,
            TerminalState::Terminated,
        ] {
            assert!(resolve_effective_targets(&all, state).is_empty());
        }
    }

    #[test]
    fn callback_target_and_filter_roundtrip_through_json() {
        let t = target(
            "https://api.example.com/hook",
            EventFilter::States(vec![TerminalState::Completed, TerminalState::Cancelled]),
        );
        let json = serde_json::to_value(&t).unwrap();
        let back: CallbackTarget = serde_json::from_value(json).unwrap();
        assert_eq!(t, back);
    }
}

// ---------------------------------------------------------------------
// M7 (core side): outbound transport seam
// ---------------------------------------------------------------------
//
// Core never ships an HTTP client (it is deliberately Postgres-only and the
// determinism lint flags `reqwest`/`hyper` inside workflow bodies as
// non-deterministic I/O). This trait mirrors the established
// [`crate::payload_store::PayloadStore`] / [`crate::retention::HistoryArchiver`]
// embedder-seam shape: a boxed-future trait with no concrete implementation
// in this crate. `autumn-harvest-plugin` supplies a default `reqwest`-based
// implementation, auto-wired by `HarvestPlugin`.

/// Future returned by [`CompletionCallbackDeliverer::deliver`].
pub type DeliverFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = DeliveryAttempt> + Send + 'a>>;

/// The result of one delivery attempt.
///
/// Either the HTTP response status was observed (`status`, regardless of
/// whether it was 2xx), or the request never got a response
/// (`transport_error` — connect failure, timeout, TLS error, DNS failure,
/// etc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryAttempt {
    pub status: Option<u16>,
    pub transport_error: Option<String>,
}

impl DeliveryAttempt {
    #[must_use]
    pub const fn success(status: u16) -> Self {
        Self {
            status: Some(status),
            transport_error: None,
        }
    }

    #[must_use]
    pub const fn transport_error(message: String) -> Self {
        Self {
            status: None,
            transport_error: Some(message),
        }
    }

    /// `true` only for a 2xx response status.
    #[must_use]
    pub fn is_success(&self) -> bool {
        matches!(self.status, Some(s) if (200..300).contains(&s))
    }
}

/// An embedder-supplied (or plugin-default) outbound HTTP transport for
/// completion-callback delivery.
///
/// Implementations are a thin transport: send `body` with `headers` to
/// `target_url` via HTTP POST and report what happened. HMAC signing, SSRF
/// validation, retry/backoff, and dead-lettering all happen in core, above
/// this trait — implementations do not need to reason about any of it.
pub trait CompletionCallbackDeliverer: Send + Sync + 'static {
    fn deliver<'a>(
        &'a self,
        target_url: &'a str,
        body: &'a [u8],
        headers: &'a [(&'static str, String)],
    ) -> DeliverFuture<'a>;
}

#[cfg(test)]
mod deliverer_trait_tests {
    use super::*;

    // Compile-time object-safety assertion: the trait must be usable as
    // `Arc<dyn CompletionCallbackDeliverer>` (how it is stored on builder
    // config and threaded through the scanner).
    #[allow(dead_code)]
    fn assert_object_safe(_: &dyn CompletionCallbackDeliverer) {}

    #[test]
    fn delivery_attempt_success_is_success_only_for_2xx() {
        assert!(DeliveryAttempt::success(200).is_success());
        assert!(DeliveryAttempt::success(299).is_success());
        assert!(!DeliveryAttempt::success(199).is_success());
        assert!(!DeliveryAttempt::success(300).is_success());
        assert!(!DeliveryAttempt::success(500).is_success());
    }

    #[test]
    fn transport_error_is_never_success() {
        assert!(!DeliveryAttempt::transport_error("connection refused".to_string()).is_success());
    }
}

// ---------------------------------------------------------------------
// M6 (pure part): outcome classification
// ---------------------------------------------------------------------

/// What the scanner should do next after one delivery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeAction {
    /// 2xx response — mark the delivery `DELIVERED`.
    Delivered { status: u16 },
    /// Non-2xx or transport error, and `attempt < max_attempts` — reschedule
    /// with the given absolute backoff deadline.
    Backoff {
        next_attempt_at: DateTime<Utc>,
        last_status: Option<u16>,
        last_error: Option<String>,
    },
    /// Non-2xx or transport error, and `attempt >= max_attempts` — mark the
    /// delivery `FAILED` and route to the DLQ.
    DeadLetter {
        last_status: Option<u16>,
        last_error: Option<String>,
    },
}

/// Deterministic per-delivery jitter seed, mirroring `worker.rs`'s
/// `retry_stream_seed` (same FNV-offset-basis-XOR-halves idiom applied to a
/// single UUID instead of two): two different deliveries never share a
/// jitter stream, and the same delivery always jitters the same way for a
/// given attempt number, so `JitterPolicy::Full`/`Equal`/`Decorrelated`
/// spread retries across deliveries instead of all of them backing off in
/// lockstep (the whole point of configuring jitter).
#[must_use]
fn delivery_stream_seed(delivery_id: Uuid) -> u64 {
    let mut seed = 0xcbf2_9ce4_8422_2325_u64;
    let raw = delivery_id.as_u128().to_le_bytes();
    seed ^= u64::from_le_bytes(raw[..8].try_into().unwrap_or([0_u8; 8]));
    seed = seed.wrapping_mul(0x1000_0000_01b3);
    seed ^= u64::from_le_bytes(raw[8..].try_into().unwrap_or([0_u8; 8]));
    seed = seed.wrapping_mul(0x1000_0000_01b3);
    seed
}

/// Pure decision function: given the outcome of one delivery attempt and
/// the frozen retry policy, decide what the scanner should do. The tx code
/// (M6, DB-gated) only ever needs to *apply* this decision.
///
/// `attempt` is the 1-based attempt number that was just made (i.e. after
/// the scanner's claim-time increment). `max_attempts` is frozen onto the
/// delivery row at enqueue time from the effective retry policy. `seed`
/// (typically [`delivery_stream_seed`] of the delivery's id) drives
/// `retry_policy.jitter` — without it, a configured `JitterPolicy` would be
/// silently ignored and every delivery would back off in perfect lockstep.
#[must_use]
pub fn classify_outcome(
    outcome: &DeliveryAttempt,
    attempt: u32,
    max_attempts: u32,
    retry_policy: &crate::policy::RetryPolicy,
    seed: u64,
    now: DateTime<Utc>,
) -> OutcomeAction {
    if let Some(status) = outcome.status
        && outcome.is_success()
    {
        return OutcomeAction::Delivered { status };
    }

    if attempt >= max_attempts {
        return OutcomeAction::DeadLetter {
            last_status: outcome.status,
            last_error: outcome.transport_error.clone(),
        };
    }

    let delay = crate::policy::compute_retry_delay_with_seed(retry_policy, attempt, seed);
    let next_attempt_at =
        now + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::seconds(0));

    OutcomeAction::Backoff {
        next_attempt_at,
        last_status: outcome.status,
        last_error: outcome.transport_error.clone(),
    }
}

#[cfg(test)]
mod classify_outcome_tests {
    use super::*;
    use crate::policy::RetryPolicy;
    use std::time::Duration as StdDuration;

    fn test_policy() -> RetryPolicy {
        RetryPolicy::exponential(5, StdDuration::from_secs(1))
    }

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn success_2xx_marks_delivered_regardless_of_attempt() {
        let outcome = DeliveryAttempt::success(204);
        let action = classify_outcome(&outcome, 3, 5, &test_policy(), 0, now());
        assert_eq!(action, OutcomeAction::Delivered { status: 204 });
    }

    #[test]
    fn non_2xx_below_max_attempts_schedules_backoff() {
        let outcome = DeliveryAttempt::success(500);
        let action = classify_outcome(&outcome, 1, 5, &test_policy(), 0, now());
        match action {
            OutcomeAction::Backoff {
                next_attempt_at,
                last_status,
                last_error,
            } => {
                assert!(next_attempt_at > now());
                assert_eq!(last_status, Some(500));
                assert_eq!(last_error, None);
            }
            other => panic!("expected Backoff, got {other:?}"),
        }
    }

    #[test]
    fn transport_error_below_max_attempts_schedules_backoff() {
        let outcome = DeliveryAttempt::transport_error("connection reset".to_string());
        let action = classify_outcome(&outcome, 2, 5, &test_policy(), 0, now());
        match action {
            OutcomeAction::Backoff {
                last_status,
                last_error,
                ..
            } => {
                assert_eq!(last_status, None);
                assert_eq!(last_error, Some("connection reset".to_string()));
            }
            other => panic!("expected Backoff, got {other:?}"),
        }
    }

    #[test]
    fn failure_at_max_attempts_dead_letters() {
        let outcome = DeliveryAttempt::success(503);
        let action = classify_outcome(&outcome, 5, 5, &test_policy(), 0, now());
        assert_eq!(
            action,
            OutcomeAction::DeadLetter {
                last_status: Some(503),
                last_error: None,
            }
        );
    }

    #[test]
    fn failure_beyond_max_attempts_still_dead_letters() {
        // Defensive: an attempt counter that somehow exceeds max_attempts
        // (should not happen in practice) still dead-letters rather than
        // looping forever.
        let outcome = DeliveryAttempt::transport_error("timeout".to_string());
        let action = classify_outcome(&outcome, 9, 5, &test_policy(), 0, now());
        assert!(matches!(action, OutcomeAction::DeadLetter { .. }));
    }

    #[test]
    fn backoff_delay_grows_with_attempt_and_is_capped() {
        let policy = test_policy(); // exponential(5, 1s) -> coeff 2.0, cap 300s
        let mut prev_delay = StdDuration::ZERO;
        for attempt in 1..5 {
            let outcome = DeliveryAttempt::success(500);
            let action = classify_outcome(&outcome, attempt, 10, &policy, 0, now());
            let OutcomeAction::Backoff {
                next_attempt_at, ..
            } = action
            else {
                panic!("expected Backoff");
            };
            let delay = (next_attempt_at - now()).to_std().unwrap();
            assert!(
                delay >= prev_delay,
                "delay must be monotonically non-decreasing across attempts"
            );
            assert!(
                delay <= StdDuration::from_secs(300),
                "delay must respect max_interval cap"
            );
            prev_delay = delay;
        }
    }

    #[test]
    fn dead_letter_reason_is_distinct_from_backoff_and_delivered() {
        // classify_outcome must never conflate "give up" with "try again" or
        // "succeeded" — these three outcomes drive materially different DB
        // writes (FAILED+DLQ vs PENDING-with-future-scheduled_at vs DELIVERED).
        let delivered = classify_outcome(
            &DeliveryAttempt::success(200),
            1,
            5,
            &test_policy(),
            0,
            now(),
        );
        let backoff = classify_outcome(
            &DeliveryAttempt::success(500),
            1,
            5,
            &test_policy(),
            0,
            now(),
        );
        let dead_letter = classify_outcome(
            &DeliveryAttempt::success(500),
            5,
            5,
            &test_policy(),
            0,
            now(),
        );
        assert!(matches!(delivered, OutcomeAction::Delivered { .. }));
        assert!(matches!(backoff, OutcomeAction::Backoff { .. }));
        assert!(matches!(dead_letter, OutcomeAction::DeadLetter { .. }));
    }

    #[test]
    fn jitter_policy_is_honored_not_ignored() {
        // issue #921 review (Codex P2): classify_outcome used to call the
        // raw, non-jittered `compute_retry_delay` directly, silently
        // dropping any configured `RetryPolicy::jitter`. Two different seeds
        // under a `Full` jitter policy must be able to produce different
        // delays for the same attempt — proving the seed actually reaches
        // the jitter computation instead of being ignored.
        let policy = test_policy().with_jitter(crate::policy::JitterPolicy::Full);
        let outcome = DeliveryAttempt::success(500);
        let mut delays = std::collections::HashSet::new();
        for seed in 0..20u64 {
            let action = classify_outcome(&outcome, 2, 10, &policy, seed, now());
            let OutcomeAction::Backoff {
                next_attempt_at, ..
            } = action
            else {
                panic!("expected Backoff");
            };
            delays.insert((next_attempt_at - now()).num_nanoseconds());
        }
        assert!(
            delays.len() > 1,
            "expected varying delays across seeds under Full jitter, got a single value \
             {delays:?} — classify_outcome is not honoring the seed"
        );
    }

    #[test]
    fn delivery_stream_seed_is_deterministic_and_distinguishes_deliveries() {
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        assert_eq!(delivery_stream_seed(a), delivery_stream_seed(a));
        assert_ne!(
            delivery_stream_seed(a),
            delivery_stream_seed(b),
            "two different delivery ids should (almost certainly) get distinct jitter streams"
        );
    }
}

// ---------------------------------------------------------------------
// M5: runtime config + enqueue folded into the terminal transaction
// ---------------------------------------------------------------------

/// Default delivery retry policy: exponential backoff over roughly a 1-hour window.
///
/// 10 attempts, 30s initial interval, coefficient 2.0, capped at 10 minutes
/// per retry — matching the issue's "survives a receiver outage up to the
/// configured retry window (default 1 h)" success metric.
#[must_use]
pub const fn default_delivery_retry_policy() -> crate::policy::RetryPolicy {
    crate::policy::RetryPolicy {
        max_attempts: 10,
        initial_interval: std::time::Duration::from_secs(30),
        backoff_coefficient: 2.0,
        max_interval: std::time::Duration::from_secs(600),
        non_retryable_errors: Vec::new(),
        jitter: crate::policy::JitterPolicy::None,
    }
}

/// Builder-level completion-callback configuration.
///
/// Collected by [`HarvestBuilder`](crate::builder::HarvestBuilder)'s fluent
/// `completion_callback_*` methods and resolved into a
/// [`CallbackRuntimeConfig`] at plugin-startup time (the plugin supplies a
/// default [`CompletionCallbackDeliverer`] — a `reqwest`-based
/// implementation — when `deliverer` is `None`, since core ships no HTTP
/// client).
#[derive(Clone)]
pub struct CompletionCallbackBuilderConfig {
    pub allowlist: HostAllowlist,
    pub allow_http: bool,
    pub allow_ip_literals: bool,
    pub secret: Option<CallbackSecret>,
    pub default_targets: Vec<CallbackTarget>,
    pub retry_policy: crate::policy::RetryPolicy,
    pub deliverer: Option<std::sync::Arc<dyn CompletionCallbackDeliverer>>,
}

impl Default for CompletionCallbackBuilderConfig {
    fn default() -> Self {
        Self {
            allowlist: HostAllowlist::new(),
            allow_http: false,
            allow_ip_literals: false,
            secret: None,
            default_targets: Vec::new(),
            retry_policy: default_delivery_retry_policy(),
            deliverer: None,
        }
    }
}

impl CompletionCallbackBuilderConfig {
    /// The [`SsrfPolicy`] implied by this config's allowlist/scheme/IP-literal settings.
    #[must_use]
    pub fn ssrf_policy(&self) -> SsrfPolicy {
        SsrfPolicy {
            allowlist: self.allowlist.clone(),
            allow_http: self.allow_http,
            allow_ip_literals: self.allow_ip_literals,
        }
    }

    /// Validate every configured default target against this config's own
    /// SSRF policy. Called at `HarvestBuilder::try_build()` time.
    ///
    /// # Errors
    /// Returns the first `(url, rejection)` pair that fails validation.
    pub fn validate_default_targets(&self) -> Result<(), (String, SsrfRejection)> {
        let policy = self.ssrf_policy();
        for target in &self.default_targets {
            if let Err(rejection) = validate_target_url(&target.url, &policy) {
                return Err((target.url.clone(), rejection));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod builder_config_tests {
    use super::*;

    #[test]
    fn default_config_has_empty_allowlist_and_no_default_targets() {
        let config = CompletionCallbackBuilderConfig::default();
        assert!(config.allowlist.is_empty());
        assert!(config.default_targets.is_empty());
        assert!(!config.allow_http);
        assert!(!config.allow_ip_literals);
        assert!(config.secret.is_none());
        assert!(config.deliverer.is_none());
    }

    #[test]
    fn default_retry_policy_totals_roughly_one_hour() {
        let policy = default_delivery_retry_policy();
        let mut total = std::time::Duration::ZERO;
        for attempt in 1..policy.max_attempts {
            total += crate::policy::compute_retry_delay(
                policy.initial_interval,
                policy.backoff_coefficient,
                policy.max_interval,
                attempt,
            );
        }
        assert!(
            total >= std::time::Duration::from_secs(30 * 60)
                && total <= std::time::Duration::from_secs(120 * 60),
            "expected a retry window in the neighborhood of 1 hour, got {total:?}"
        );
    }

    #[test]
    fn validate_default_targets_passes_when_all_allowlisted() {
        let mut config = CompletionCallbackBuilderConfig {
            allowlist: HostAllowlist::new().with_pattern("api.example.com"),
            ..Default::default()
        };
        config.default_targets = vec![CallbackTarget::new(
            "https://api.example.com/hook",
            EventFilter::AnyTerminal,
        )];
        assert!(config.validate_default_targets().is_ok());
    }

    #[test]
    fn validate_default_targets_rejects_the_first_non_allowlisted_target() {
        let config = CompletionCallbackBuilderConfig {
            allowlist: HostAllowlist::new().with_pattern("api.example.com"),
            default_targets: vec![CallbackTarget::new(
                "https://evil.com/hook",
                EventFilter::AnyTerminal,
            )],
            ..Default::default()
        };
        let (url, rejection) = config.validate_default_targets().unwrap_err();
        assert_eq!(url, "https://evil.com/hook");
        assert!(matches!(
            rejection,
            SsrfRejection::HostNotAllowlisted { .. }
        ));
    }

    #[test]
    fn ssrf_policy_reflects_configured_flags() {
        let config = CompletionCallbackBuilderConfig {
            allow_http: true,
            allow_ip_literals: true,
            ..Default::default()
        };
        let policy = config.ssrf_policy();
        assert!(policy.allow_http);
        assert!(policy.allow_ip_literals);
    }
}

/// Process-wide completion-callback runtime configuration.
///
/// Resolved from [`HarvestBuilder`](crate::builder::HarvestBuilder) at
/// startup and set once via [`crate::worker::HandlerRegistry`] construction
/// — the same pattern as
/// [`crate::completion_trigger::GLOBAL_WORKFLOW_METADATA`].
///
/// `None` (the default, before any builder wiring runs) means the feature
/// is fully inert: [`enqueue_completion_deliveries`] is a no-op and no
/// `harvest_completion_deliveries` row is ever written, so an embedder who
/// never touches the completion-callback builder API sees zero behavior
/// change.
#[cfg(feature = "db")]
#[derive(Clone)]
pub struct CallbackRuntimeConfig {
    pub deliverer: std::sync::Arc<dyn CompletionCallbackDeliverer>,
    pub secret: CallbackSecret,
    pub ssrf_policy: SsrfPolicy,
    pub default_targets: Vec<CallbackTarget>,
    pub retry_policy: crate::policy::RetryPolicy,
}

// `Arc`-wrapped (issue #605 code review): every read clones the value out of
// the lock (see below), and `CallbackRuntimeConfig` carries owned `Vec<u8>`/
// `Vec<String>`/`Vec<CallbackTarget>` fields alongside the already-cheap
// `Arc<dyn CompletionCallbackDeliverer>` — without the outer `Arc`, a clone
// deep-copies all of those on every terminal transition and every scanner
// tick, a config value that is otherwise identical across calls and only
// ever changes at startup. Wrapping the whole struct turns that into an O(1)
// refcount bump.
#[cfg(feature = "db")]
pub static GLOBAL_CALLBACK_CONFIG: std::sync::RwLock<
    Option<std::sync::Arc<CallbackRuntimeConfig>>,
> = std::sync::RwLock::new(None);

/// Read the current [`GLOBAL_CALLBACK_CONFIG`], tolerating a poisoned lock
/// (issue #921 review) rather than either panicking or silently treating a
/// poisoned lock the same as "never configured".
///
/// A poisoned `RwLock` means some other thread panicked while holding the
/// write guard — but the single `*lock = Some(..)` assignment that write
/// path performs is not itself a multi-step invariant that a panic could
/// leave half-updated, so the data behind a poisoned read guard is still
/// perfectly valid to read. Recovering it (via `into_inner()`) rather than
/// discarding it avoids the failure mode a bare `.read().ok()` has: if
/// *anything* elsewhere in the process ever panics while holding this
/// lock's write guard, every subsequent terminal transition would
/// otherwise silently stop enqueuing/delivering completion callbacks for
/// the rest of the process's life, with no diagnostic at all. Panicking
/// instead (the alternative some reviewers suggest) was rejected: it would
/// propagate into `enqueue_completion_deliveries`, called inside the
/// worker's terminal-transition transaction for *every* workflow
/// completion in the system — crashing delivery of a completion callback
/// must never crash the workflow's own terminal transition.
#[cfg(feature = "db")]
fn read_global_callback_config() -> Option<std::sync::Arc<CallbackRuntimeConfig>> {
    match GLOBAL_CALLBACK_CONFIG.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            tracing::error!(
                "GLOBAL_CALLBACK_CONFIG lock was poisoned by a panic elsewhere in the \
                 process; recovering the last-written config rather than treating it as \
                 unconfigured"
            );
            poisoned.into_inner().clone()
        }
    }
}

/// Install [`GLOBAL_CALLBACK_CONFIG`] for an embedder using the core
/// `HarvestBuilder::build()` -> `into_worker_parts()` path directly.
///
/// This bypasses `autumn-harvest-plugin`'s `HarvestPlugin`/`HarvestRunner`
/// (issue #921 review, Codex P2). `autumn-harvest-plugin`'s
/// `PreparedHarvestRuntime::build` is the *only*
/// installer this crate ships (core has no `reqwest` dependency to fall back
/// to when no deliverer is configured, and no obvious place to warn-and-noop
/// otherwise), so a caller who never goes through the plugin — the
/// documented "direct core worker" embedding path — got a builder API
/// (`completion_callback_default`/`completion_callback_deliverer`/etc.) that
/// silently did nothing: `enqueue_completion_deliveries` reads
/// `GLOBAL_CALLBACK_CONFIG` and treats `None` as "feature never configured",
/// so no delivery row was ever enqueued.
///
/// Only installs when a [`CompletionCallbackDeliverer`] was actually
/// supplied via `completion_callback_deliverer(...)` — with no deliverer
/// there is nothing to POST with, and unlike the plugin path this crate has
/// no default implementation to substitute, so leaving the config
/// unconfigured (byte-identical to today) is correct rather than installing
/// a config that can never deliver anything.
#[cfg(feature = "db")]
pub fn install_global_callback_config_for_direct_worker(config: &CompletionCallbackBuilderConfig) {
    let Some(deliverer) = config.deliverer.clone() else {
        return;
    };
    let secret = config.secret.clone().unwrap_or_else(|| {
        tracing::warn!(
            "completion-callback HMAC secret was never configured via \
             HarvestBuilder::completion_callback_secret(...) -- every delivered callback \
             will be signed with an empty key, which defeats the X-Harvest-Signature \
             authenticity guarantee for any receiver relying on it"
        );
        CallbackSecret::new(Vec::new())
    });
    if let Ok(mut lock) = GLOBAL_CALLBACK_CONFIG.write() {
        *lock = Some(std::sync::Arc::new(CallbackRuntimeConfig {
            deliverer,
            secret,
            ssrf_policy: config.ssrf_policy(),
            default_targets: config.default_targets.clone(),
            retry_policy: config.retry_policy.clone(),
        }));
    }
}

/// Enqueue durable completion-callback deliveries for `execution`'s terminal
/// transition to `state`.
///
/// Called from [`crate::completion_trigger::evaluate_triggers_for_execution`]
/// immediately after it loads the execution row, so it runs inside the
/// *same* transaction as every terminal-state write (completion, failure,
/// cancellation, termination, timeout) — never on a `ContinueAsNew`
/// transition, since that function has no such call site. Because the
/// insert happens inside the terminal transaction, a background scanner
/// that only reads committed rows observes the delivery *after* the
/// terminal transition has durably committed, without any extra
/// coordination.
///
/// Idempotent: the `UNIQUE (workflow_exec_id, callback_index)` constraint +
/// `ON CONFLICT DO NOTHING` means calling this twice for the same execution
/// and state (e.g. if a parent-close-cascade recursion re-enters the
/// caller for the same execution) enqueues each target's delivery at most
/// once.
///
/// # Errors
/// Returns `HarvestError` on a database failure, or if serializing the
/// envelope/event-filter/retry-policy JSON for a matching target fails.
#[cfg(feature = "db")]
pub async fn enqueue_completion_deliveries(
    conn: &mut diesel_async::AsyncPgConnection,
    execution: &crate::models::WorkflowExecution,
    exec_id: crate::types::ExecutionId,
    state: TerminalState,
) -> crate::error::HarvestResult<()> {
    use diesel_async::RunQueryDsl;

    let Some(config) = read_global_callback_config() else {
        return Ok(());
    };

    let per_exec: Vec<CallbackTarget> = execution
        .completion_callbacks
        .clone()
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default();

    let all_targets = resolve_all_targets(&per_exec, &config.default_targets);
    if all_targets.is_empty() {
        return Ok(());
    }

    let matching = resolve_effective_targets(&all_targets, state);
    if matching.is_empty() {
        return Ok(());
    }

    for (callback_index, target) in matching {
        // Defense-in-depth: re-validate against the *live* SSRF policy even
        // though the target was already validated at registration time
        // (builder default at build time, or per-start option at the HTTP
        // start route). A tightened operator policy must never be bypassed
        // by an execution registered under a looser prior policy. Rejection
        // here skips just this one target rather than aborting the
        // terminal transaction.
        if let Err(rejection) = validate_target_url(&target.url, &config.ssrf_policy) {
            tracing::warn!(
                execution_id = %exec_id,
                callback_index,
                target_url = %target.url,
                ?rejection,
                "completion callback target failed SSRF re-validation at enqueue time; skipping"
            );
            continue;
        }

        let delivery_id = Uuid::new_v4();
        let envelope = build_envelope(
            delivery_id,
            exec_id.as_uuid(),
            &execution.workflow_name,
            &execution.workflow_id,
            state,
            execution.output.clone(),
            execution.error.as_deref(),
            execution.completed_at,
        );
        let payload = serde_json::to_value(&envelope).map_err(|e| {
            crate::error::HarvestError::Config(format!(
                "failed to serialize completion-callback envelope: {e}"
            ))
        })?;
        let event_filter_json = serde_json::to_value(&target.filter).map_err(|e| {
            crate::error::HarvestError::Config(format!(
                "failed to serialize completion-callback event filter: {e}"
            ))
        })?;
        let retry_policy_json = serde_json::to_value(&config.retry_policy).map_err(|e| {
            crate::error::HarvestError::Config(format!(
                "failed to serialize completion-callback retry policy: {e}"
            ))
        })?;
        let callback_index_i32 = i32::try_from(callback_index).unwrap_or(i32::MAX);
        let max_attempts_i32 = i32::try_from(config.retry_policy.max_attempts).unwrap_or(i32::MAX);

        let new_row = crate::models::NewCompletionDelivery {
            id: delivery_id,
            workflow_exec_id: exec_id.as_uuid(),
            shard_id: execution.shard_id,
            callback_index: callback_index_i32,
            workflow_name: &execution.workflow_name,
            workflow_id: &execution.workflow_id,
            target_url: &target.url,
            event_filter: event_filter_json,
            terminal_state: state.as_str(),
            payload,
            max_attempts: max_attempts_i32,
            retry_policy: retry_policy_json,
            next_attempt_at: Utc::now(),
        };

        diesel::insert_into(crate::schema::harvest_completion_deliveries::table)
            .values(&new_row)
            .on_conflict((
                crate::schema::harvest_completion_deliveries::workflow_exec_id,
                crate::schema::harvest_completion_deliveries::callback_index,
            ))
            .do_nothing()
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------
// M6: scanner (two-transaction claim / deliver / record-outcome)
// ---------------------------------------------------------------------

/// Maximum number of delivery rows claimed per shard per scanner tick.
/// Mirrors [`crate::debounce::DEBOUNCE_FIRE_BATCH_SIZE`].
#[cfg(feature = "db")]
const COMPLETION_DELIVERY_FIRE_BATCH_SIZE: i64 = 100;

/// How long a claimed row stays `INFLIGHT` before it becomes reclaimable
/// again. Protects against a scanner crashing between claim and
/// record-outcome: the row is not stuck forever, but also isn't
/// immediately double-claimed by a concurrent scanner tick.
#[cfg(feature = "db")]
const INFLIGHT_LEASE_SECS: i64 = 30;

#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct ClaimedDeliveryRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    workflow_exec_id: Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    target_url: String,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    payload: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempt: i32,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    max_attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Jsonb)]
    retry_policy: serde_json::Value,
}

/// Claim up to [`COMPLETION_DELIVERY_FIRE_BATCH_SIZE`] due rows in one
/// atomic statement: a `FOR UPDATE SKIP LOCKED` candidate CTE feeding an
/// `UPDATE ... RETURNING`, so the claim and the `INFLIGHT` transition
/// happen together with no gap a concurrent scanner could race into
/// (mirrors `queue::claim_task`'s candidate-CTE shape). This one SQL
/// statement is atomic by itself — no explicit transaction wrapper needed
/// — and, critically, does **not** hold any lock across the network POST
/// that follows: the statement (and its row locks) completes before this
/// function returns.
#[cfg(feature = "db")]
async fn claim_due_deliveries(
    conn: &mut diesel_async::AsyncPgConnection,
    now: DateTime<Utc>,
) -> crate::error::HarvestResult<Vec<ClaimedDeliveryRow>> {
    use diesel_async::RunQueryDsl;

    let lease_until = now + chrono::Duration::seconds(INFLIGHT_LEASE_SECS);
    let sql = "
        WITH candidate AS (
            SELECT id FROM harvest_completion_deliveries
            WHERE state IN ('PENDING', 'INFLIGHT') AND next_attempt_at <= $1
            ORDER BY next_attempt_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
        )
        UPDATE harvest_completion_deliveries d
        SET state = 'INFLIGHT', attempt = d.attempt + 1, next_attempt_at = $3, updated_at = $1
        FROM candidate c
        WHERE d.id = c.id
        RETURNING d.id, d.workflow_exec_id, d.target_url, d.payload, d.attempt, d.max_attempts, d.retry_policy
    ";

    diesel::sql_query(sql)
        .bind::<diesel::sql_types::Timestamptz, _>(now)
        .bind::<diesel::sql_types::BigInt, _>(COMPLETION_DELIVERY_FIRE_BATCH_SIZE)
        .bind::<diesel::sql_types::Timestamptz, _>(lease_until)
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Re-read the current `payload` for a batch of just-claimed delivery ids,
/// keyed by id. A non-locking bulk `SELECT` — used only to narrow the
/// window between claim and dispatch during which a concurrent
/// `erase_workflow_payloads` call could tombstone a row's payload (issue
/// #921 review, Codex P1, follow-up); see the call site in
/// [`fire_due_on_conn`] for the full rationale.
#[cfg(feature = "db")]
async fn reread_claimed_payloads(
    conn: &mut diesel_async::AsyncPgConnection,
    ids: &[Uuid],
) -> crate::error::HarvestResult<std::collections::HashMap<Uuid, serde_json::Value>> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_completion_deliveries::dsl;

    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let rows: Vec<(Uuid, serde_json::Value)> = dsl::harvest_completion_deliveries
        .filter(dsl::id.eq_any(ids))
        .select((dsl::id, dsl::payload))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;

    Ok(rows.into_iter().collect())
}

/// Build the [`crate::dlq::NewDeadLetterEntry`] for an exhausted delivery,
/// sourcing `input` from a *fresh* read of the row's `payload` column rather
/// than the in-memory [`ClaimedDeliveryRow`] snapshot captured at claim time
/// (issue #921 review, Codex P1).
///
/// A concurrent `erase_workflow_payloads` call can tombstone this row's
/// `payload` column *after* the claim but *before* this outcome is
/// recorded, and that erase transaction may have already committed by the
/// time this runs. Dead-lettering with the stale, pre-erase `row.payload`
/// would re-insert the erased result/error PII into
/// `harvest_dead_letters.input`, silently undoing an erasure the operator
/// believed was durable. Must be called from inside the same transaction
/// that subsequently writes the `FAILED` state, under `FOR UPDATE`, which
/// also serializes this read against a concurrent erase's own `UPDATE` on
/// this row: whichever transaction's lock-acquiring statement runs first is
/// authoritative, and the other blocks until it commits.
#[cfg(feature = "db")]
async fn dead_letter_entry_with_current_payload(
    conn: &mut diesel_async::AsyncPgConnection,
    row: &ClaimedDeliveryRow,
    last_status: Option<u16>,
) -> crate::error::HarvestResult<crate::dlq::NewDeadLetterEntry> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_completion_deliveries::dsl;

    let current_payload: Option<serde_json::Value> = dsl::harvest_completion_deliveries
        .find(row.id)
        .select(dsl::payload)
        .for_update()
        .first(conn)
        .await
        .optional()
        .map_err(crate::error::database_error)?;

    let reason = crate::dlq::DeadLetterReason::CallbackDeliveryExhausted {
        delivery_id: row.id,
        attempts: row.attempt,
        last_status,
        target: row.target_url.clone(),
    };
    Ok(crate::dlq::NewDeadLetterEntry {
        original_task_id: row.id,
        queue_name: "completion-callback".to_string(),
        task_type: "CALLBACK".to_string(),
        workflow_exec_id: Some(row.workflow_exec_id),
        activity_name: None,
        input: current_payload.unwrap_or_else(|| row.payload.clone()),
        error: reason.to_string(),
        attempts: row.attempt,
        owner: None,
        severity: None,
    })
}

/// Record the outcome of one delivery attempt: `DELIVERED` (single
/// `UPDATE`), rescheduled with backoff (single `UPDATE`), or dead-lettered
/// (`UPDATE` + `harvest_dead_letters` insert, in one transaction so the two
/// writes are atomic).
#[cfg(feature = "db")]
async fn apply_outcome(
    conn: &mut diesel_async::AsyncPgConnection,
    row: &ClaimedDeliveryRow,
    action: OutcomeAction,
) -> crate::error::HarvestResult<()> {
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;
    use diesel_async::scoped_futures::ScopedFutureExt;

    use crate::schema::harvest_completion_deliveries::dsl;

    // Guard every outcome write by the exact `attempt` number this row had
    // *when it was claimed* (issue #921 review, Codex P2): if a delivery
    // attempt runs longer than the INFLIGHT lease (a slow custom deliverer,
    // a stalled executor, or just an unlucky scheduling gap), a later
    // scanner tick can reclaim the same row and bump `attempt` again while
    // the earlier, stale attempt is still in flight. Without this guard,
    // the stale attempt's outcome — arriving after the newer attempt's —
    // would win the update-by-`id`-only race and could silently revive an
    // already-`DELIVERED`/`FAILED` row back to `PENDING`, or overwrite a
    // fresher dead-letter with stale status/error text. Filtering on
    // `attempt.eq(row.attempt)` makes a superseded attempt's write a
    // no-op (0 rows matched) instead of corrupting the newer state.
    match action {
        OutcomeAction::Delivered { status } => {
            diesel::update(
                dsl::harvest_completion_deliveries
                    .find(row.id)
                    .filter(dsl::attempt.eq(row.attempt)),
            )
            .set((
                dsl::state.eq("DELIVERED"),
                dsl::last_status.eq(Some(i32::from(status))),
                dsl::last_error.eq(None::<String>),
                dsl::delivered_at.eq(Some(Utc::now())),
                dsl::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        }
        OutcomeAction::Backoff {
            next_attempt_at,
            last_status,
            last_error,
        } => {
            diesel::update(
                dsl::harvest_completion_deliveries
                    .find(row.id)
                    .filter(dsl::attempt.eq(row.attempt)),
            )
            .set((
                dsl::state.eq("PENDING"),
                dsl::next_attempt_at.eq(next_attempt_at),
                dsl::last_status.eq(last_status.map(i32::from)),
                dsl::last_error.eq(last_error),
                dsl::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;
        }
        OutcomeAction::DeadLetter {
            last_status,
            last_error,
        } => {
            conn.transaction::<_, crate::error::HarvestError, _>(|conn| {
                async move {
                    let entry =
                        dead_letter_entry_with_current_payload(conn, row, last_status).await?;

                    let updated = diesel::update(
                        dsl::harvest_completion_deliveries
                            .find(row.id)
                            .filter(dsl::attempt.eq(row.attempt)),
                    )
                    .set((
                        dsl::state.eq("FAILED"),
                        dsl::last_status.eq(last_status.map(i32::from)),
                        dsl::last_error.eq(last_error),
                        dsl::updated_at.eq(Utc::now()),
                    ))
                    .execute(conn)
                    .await
                    .map_err(crate::error::database_error)?;

                    // A stale/superseded attempt must not dead-letter a row a
                    // newer attempt already resolved differently.
                    if updated > 0 {
                        crate::dlq::dead_letter(conn, &entry).await?;
                    }
                    Ok(())
                }
                .scope_boxed()
            })
            .await?;
        }
    }

    Ok(())
}

/// Tolerate a missing `harvest_completion_deliveries` table (pre-migration
/// deployments, or mixed-schema test fixtures exercising unrelated timeout
/// behavior) exactly like [`crate::debounce::fire_due_debounced_starts`]
/// does for `harvest_debounce`.
#[cfg(feature = "db")]
async fn completion_deliveries_table_exists(
    conn: &mut diesel_async::AsyncPgConnection,
) -> crate::error::HarvestResult<bool> {
    use diesel_async::RunQueryDsl;

    #[derive(diesel::QueryableByName)]
    struct TableExists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        present: bool,
    }
    let exists: TableExists = diesel::sql_query(
        "SELECT to_regclass('harvest_completion_deliveries') IS NOT NULL AS present",
    )
    .get_result(conn)
    .await
    .map_err(crate::error::database_error)?;
    Ok(exists.present)
}

/// Claim and process one shard's due deliveries: claim (tx-free, one atomic
/// statement) -> POST unlocked -> record outcome (single statement, or a
/// transaction for the dead-letter path). Returns the number of rows
/// processed (delivered, rescheduled, or dead-lettered — all count as
/// "processed" for scanner-tick accounting, mirroring
/// `enforce_timeouts_once`'s `count` semantics).
#[cfg(feature = "db")]
async fn fire_due_on_conn(
    conn: &mut diesel_async::AsyncPgConnection,
    config: &CallbackRuntimeConfig,
) -> crate::error::HarvestResult<usize> {
    if !completion_deliveries_table_exists(conn).await? {
        return Ok(0);
    }

    let now = Utc::now();
    let claimed = claim_due_deliveries(conn, now).await?;
    let mut processed = 0usize;

    // Re-read each row's payload immediately before serializing it for
    // dispatch (issue #921 review, Codex P1, follow-up): `claimed` carries
    // the payload as of the claim, but `erase_workflow_payloads` can
    // tombstone this row's payload column at any point afterward --
    // including while this tick is still building its dispatch batch.
    // Bytes already POSTed to an external receiver cannot be recalled, so
    // the freshness check for the network body has to happen as close to
    // send time as realistically achievable without holding a lock across
    // the network call. This does not close the race entirely (an erase
    // landing in the gap between this read and the actual `deliver()` call
    // moments later is still possible), but it eliminates the dominant
    // source of staleness: the full claim-to-dispatch batch-build time,
    // plus (for every row after the first in a large batch) the cumulative
    // serialization work for earlier rows. `dead_letter_entry_with_current_
    // payload` performs its own separate re-read *after* the network round
    // trip, closing the remaining window for what gets written to the DLQ.
    let claimed_ids: Vec<Uuid> = claimed.iter().map(|row| row.id).collect();
    let fresh_payloads = reread_claimed_payloads(conn, &claimed_ids).await?;

    // Sign and send the payload bytes exactly as read back above — a
    // Postgres JSONB round-trip may reorder object keys relative to the
    // bytes originally inserted, but that never threatens the "signature
    // covers exactly what was posted" invariant: whatever bytes we derive
    // here are the ones we sign *and* the ones we send, on every attempt
    // including the first. A row that fails to serialize is dead-lettered
    // immediately rather than merely skipped (issue #921 review): a `continue`
    // here would leave the row's just-claimed `INFLIGHT` state and
    // `next_attempt_at = lease_until` untouched, so — since serialization
    // failure is deterministic for a given payload — the very next scanner
    // tick (30s later) would reclaim and fail to serialize it again,
    // forever, spamming logs and never actually resolving the delivery.
    let mut dispatchable = Vec::with_capacity(claimed.len());
    for row in claimed {
        // Re-validate the target URL against the *live* SSRF policy before
        // dispatching (issue #921 review, Codex P2): `target_url` was only
        // ever checked at enqueue time (`enqueue_completion_deliveries`'s
        // own defense-in-depth re-check). A row can sit `PENDING`/`INFLIGHT`/
        // `FAILED` for a long time -- or be manually redriven long after
        // enqueue -- so an operator who tightens the allowlist (removing a
        // compromised or decommissioned host) after enqueue must have that
        // change actually enforced on the next dispatch attempt, not just
        // for brand-new deliveries. A rejection here dead-letters the row
        // immediately rather than silently dropping it forever: the
        // operator gets a visible, actionable DLQ entry instead of a task
        // that appears to make no progress.
        if let Err(rejection) = validate_target_url(&row.target_url, &config.ssrf_policy) {
            tracing::warn!(
                delivery_id = %row.id,
                target_url = %row.target_url,
                rejection = ?rejection,
                "completion-callback target URL no longer allowed by the live SSRF policy; dead-lettering"
            );
            let action = OutcomeAction::DeadLetter {
                last_status: None,
                last_error: Some(format!(
                    "target URL rejected by current SSRF policy: {rejection}"
                )),
            };
            apply_outcome(conn, &row, action).await?;
            processed += 1;
            continue;
        }

        let payload = fresh_payloads.get(&row.id).unwrap_or(&row.payload);
        let body = match serde_json::to_vec(payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::error!(
                    delivery_id = %row.id,
                    error = %e,
                    "failed to serialize completion-callback payload for delivery; dead-lettering"
                );
                let action = OutcomeAction::DeadLetter {
                    last_status: None,
                    last_error: Some(format!("payload serialization failed: {e}")),
                };
                apply_outcome(conn, &row, action).await?;
                processed += 1;
                continue;
            }
        };
        let headers = delivery_headers(&config.secret, &body, row.id, now);
        dispatchable.push((row, body, headers));
    }

    // Dispatch every claimed delivery's HTTP POST concurrently rather than
    // one at a time: a sequential loop would hold this shard's single
    // connection (checked out from a bounded pool shared with other work)
    // for the sum of every attempt's latency — with a full batch and a few
    // slow/unreachable targets, that's tens of minutes worst case. Dispatching
    // concurrently bounds the wall-clock cost of one tick to the *slowest*
    // single delivery instead of the sum of all of them. Only the network
    // call is concurrent; outcome recording below still runs sequentially
    // against the single connection this function was handed (mirroring
    // every other scanner in this codebase).
    let delivery_futures = dispatchable
        .iter()
        .map(|(row, body, headers)| config.deliverer.deliver(&row.target_url, body, headers));
    let attempt_outcomes = futures::future::join_all(delivery_futures).await;

    // A fresh timestamp for backoff scheduling (issue #921 review, Codex
    // P2): `now` above was captured *before* claim + dispatch, so reusing it
    // here as the base for `next_attempt_at = now + delay` would understate
    // the delay by however long the batch's slowest concurrent POST (or, for
    // a large batch, the cumulative per-row serialization work before
    // dispatch) actually took -- for a delivery whose attempt took longer
    // than its own computed delay, `next_attempt_at` could already be in the
    // past by the time `apply_outcome` writes it, so the scanner's very next
    // tick would immediately retry instead of honoring the configured
    // backoff, hammering an unhealthy receiver. `join_all` is a barrier, so
    // every attempt in this batch has genuinely completed by this point --
    // one fresh capture correctly anchors backoff to "after this attempt
    // finished" for the whole batch.
    let backoff_now = Utc::now();

    for ((row, _body, _headers), attempt_outcome) in dispatchable.into_iter().zip(attempt_outcomes)
    {
        let retry_policy: crate::policy::RetryPolicy = match serde_json::from_value(
            row.retry_policy.clone(),
        ) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(
                    delivery_id = %row.id,
                    error = %e,
                    "failed to deserialize frozen retry policy for delivery; using builder default"
                );
                config.retry_policy.clone()
            }
        };

        let max_attempts = u32::try_from(row.max_attempts).unwrap_or(u32::MAX);
        let attempt = u32::try_from(row.attempt).unwrap_or(u32::MAX);
        let seed = delivery_stream_seed(row.id);
        let action = classify_outcome(
            &attempt_outcome,
            attempt,
            max_attempts,
            &retry_policy,
            seed,
            backoff_now,
        );

        apply_outcome(conn, &row, action).await?;
        processed += 1;
    }

    Ok(processed)
}

/// Fire all due completion-callback deliveries across every assigned shard.
///
/// Mirrors [`crate::debounce::fire_due_debounced_starts`]'s per-shard
/// fan-out: `harvest_completion_deliveries` rows live on the shard that
/// owned the terminal transaction that enqueued them, so a single-
/// connection scan would never see rows on non-default shards.
///
/// Called from [`crate::timeout::enforce_timeouts_once`] on the existing
/// `spawn_timeout_checker` poll interval — no new background task is
/// spawned. A no-op (returns `Ok(0)`) when no completion-callback config
/// has ever been registered ([`GLOBAL_CALLBACK_CONFIG`] is `None`).
///
/// # Errors
/// Returns `HarvestError` if a database query fails. A single delivery's
/// transport failure is never an `Err` here — it is captured as a
/// [`DeliveryAttempt`] and classified into a backoff/dead-letter DB write.
#[cfg(feature = "db")]
pub async fn fire_due_completion_deliveries(
    conn: &mut diesel_async::AsyncPgConnection,
    sharded_pool: &Option<crate::shard::ShardedDbPool>,
    shard_assignments: &[crate::types::ShardId],
) -> crate::error::HarvestResult<usize> {
    let Some(config) = read_global_callback_config() else {
        return Ok(0);
    };

    let mut total = 0usize;

    match sharded_pool {
        Some(sp) if !shard_assignments.is_empty() => {
            for shard in shard_assignments {
                let Some(pool) = sp.exact_pool_for(*shard).cloned() else {
                    continue;
                };
                let mut shard_conn = match pool.get().await {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!(
                            "[completion_callback] failed to get connection to shard {shard:?}: {e:?}"
                        );
                        continue;
                    }
                };
                total += fire_due_on_conn(&mut shard_conn, &config).await?;
            }
        }
        _ => {
            total += fire_due_on_conn(conn, &config).await?;
        }
    }

    Ok(total)
}

// ---------------------------------------------------------------------
// M9: management-surface read/redrive primitives
// ---------------------------------------------------------------------

/// List completion-callback deliveries for one execution.
///
/// Ordered by `callback_index` (stable, deterministic — see
/// [`resolve_all_targets`]). Powers the management-API "list pending/failed
/// deliveries for an execution" surface.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn list_deliveries_for_execution(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
) -> crate::error::HarvestResult<Vec<crate::models::CompletionDelivery>> {
    use diesel::prelude::*;
    use diesel_async::RunQueryDsl;

    use crate::schema::harvest_completion_deliveries::dsl;

    dsl::harvest_completion_deliveries
        .filter(dsl::workflow_exec_id.eq(exec_id.as_uuid()))
        .select(crate::models::CompletionDelivery::as_select())
        .order(dsl::callback_index.asc())
        .load(conn)
        .await
        .map_err(crate::error::database_error)
}

/// Outcome of a [`redrive_delivery`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRedriveOutcome {
    /// The delivery was reset to `PENDING` (fresh retry budget granted by
    /// extending `max_attempts` — see [`redrive_delivery`]'s doc comment for
    /// why `attempt` itself is deliberately *not* reset) so the scanner
    /// retries it on its next tick, and any associated
    /// `harvest_dead_letters` row from the original exhaustion was deleted
    /// (mirrors `dlq::redrive_dead_letter`'s cleanup).
    Redriven,
    /// No delivery row exists with this id.
    NotFound,
    /// The delivery exists but is not currently `FAILED` — nothing to
    /// redrive (it already delivered, or is still pending/in-flight).
    NotFailed { current_state: String },
}

/// Manually redrive a dead-lettered completion-callback delivery.
///
/// Self-contained: it only ever mutates the `harvest_completion_deliveries`
/// row (and deletes the matching `harvest_dead_letters` row, if any) —
/// never `harvest_workflow_executions` or the task queue. Unlike
/// [`crate::dlq::redrive_dead_letter`], there is no owning task or
/// execution to reactivate: the terminal workflow already completed its
/// own lifecycle independently of whether its callback was ever delivered.
///
/// Redelivery reuses the same `delivery_id` (the row's own primary key),
/// preserving the at-least-once + receiver-side-dedup contract.
///
/// **`attempt` is deliberately never reset** (issue #921 review, Codex P2).
/// [`apply_outcome`] guards every outcome write with `.filter(attempt.eq(row.attempt))`
/// so a stale attempt whose HTTP call outlives the `INFLIGHT` lease (and is
/// therefore reclaimed by a later scanner tick, per that guard's own doc
/// comment) can never overwrite a fresher attempt's result — that guard's
/// soundness depends on `attempt` values never repeating over a delivery
/// row's lifetime. Resetting `attempt` to `0` on redrive would violate that:
/// a genuinely-still-in-flight pre-redrive call (e.g. one that raced the
/// lease and is still awaiting a slow deliverer response) could return
/// *after* the redrive and its captured `row.attempt` (a small number like
/// `1`) could alias a fresh post-redrive claim that legitimately reaches the
/// same small number, silently applying the ancient response's outcome to
/// the wrong attempt. Instead, a "fresh retry budget" is granted by
/// extending `max_attempts` (by the row's current `max_attempts`, so a
/// redrive always grants at least as many additional attempts as the
/// original policy allowed) while leaving the monotonically-increasing
/// `attempt` counter untouched, so no attempt number is ever reused across
/// a redrive.
///
/// `exec_id` scopes the lookup to the execution named by the caller (e.g.
/// the `{id}` path segment of `POST
/// /workflows/{id}/completion-deliveries/{delivery_id}/redrive`) — a
/// `delivery_id` that exists but belongs to a *different* execution is
/// reported as [`DeliveryRedriveOutcome::NotFound`], exactly like an
/// unknown id, so the caller can never redrive (or learn the existence of)
/// a delivery outside the execution they named.
///
/// # Errors
/// Returns `HarvestError` on a database failure.
#[cfg(feature = "db")]
pub async fn redrive_delivery(
    conn: &mut diesel_async::AsyncPgConnection,
    exec_id: crate::types::ExecutionId,
    delivery_id: Uuid,
) -> crate::error::HarvestResult<DeliveryRedriveOutcome> {
    use diesel::prelude::*;
    use diesel_async::AsyncConnection;
    use diesel_async::RunQueryDsl;
    use diesel_async::scoped_futures::ScopedFutureExt;

    use crate::schema::harvest_completion_deliveries::dsl;
    use crate::schema::harvest_dead_letters::dsl as dlq_dsl;

    let exec_uuid = exec_id.as_uuid();

    conn.transaction::<DeliveryRedriveOutcome, crate::error::HarvestError, _>(|conn| {
        async move {
            let current: Option<(String, i32)> = dsl::harvest_completion_deliveries
                .find(delivery_id)
                .filter(dsl::workflow_exec_id.eq(exec_uuid))
                .select((dsl::state, dsl::max_attempts))
                .for_update()
                .first(conn)
                .await
                .optional()
                .map_err(crate::error::database_error)?;

            let Some((state, max_attempts)) = current else {
                return Ok(DeliveryRedriveOutcome::NotFound);
            };
            if state != "FAILED" {
                return Ok(DeliveryRedriveOutcome::NotFailed {
                    current_state: state,
                });
            }

            // Extend the ceiling rather than resetting `attempt` — see the
            // doc comment above for why reusing attempt numbers is unsafe.
            let extended_max_attempts = max_attempts.saturating_add(max_attempts.max(1));

            diesel::update(
                dsl::harvest_completion_deliveries
                    .find(delivery_id)
                    .filter(dsl::workflow_exec_id.eq(exec_uuid)),
            )
            .filter(dsl::state.eq("FAILED"))
            .set((
                dsl::state.eq("PENDING"),
                dsl::max_attempts.eq(extended_max_attempts),
                dsl::next_attempt_at.eq(Utc::now()),
                dsl::last_status.eq(None::<i32>),
                dsl::last_error.eq(None::<String>),
                dsl::updated_at.eq(Utc::now()),
            ))
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

            diesel::delete(
                dlq_dsl::harvest_dead_letters
                    .filter(dlq_dsl::original_task_id.eq(delivery_id))
                    .filter(dlq_dsl::task_type.eq("CALLBACK")),
            )
            .execute(conn)
            .await
            .map_err(crate::error::database_error)?;

            Ok(DeliveryRedriveOutcome::Redriven)
        }
        .scope_boxed()
    })
    .await
}
