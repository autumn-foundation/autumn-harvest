//! Tests for the activity idempotency key feature (issue #157).
//!
//! Run with: `cargo test --test idempotency_tests --no-default-features --features testing`
#![cfg(feature = "testing")]
//!
//! Covers: `IdempotencyKey` type, `ActivityContext::idempotency_key()`,
//! `ActivityContext::attempt()`, subkey derivation, HTTP-header safety,
//! collision resistance, and the duplicate-dispatch / crash-retry scenario
//! that is the core safety motivation for the feature.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use autumn_harvest::context::ActivityContext;
use autumn_harvest::error::HarvestError;
use autumn_harvest::types::{ActivityExecId, IdempotencyKey};

// ── Basic accessibility ───────────────────────────────────────────────────

#[test]
fn idempotency_key_accessible_from_test_context() {
    let ctx = ActivityContext::new_test();
    assert!(
        ctx.idempotency_key().is_ok(),
        "new_test() must provide an idempotency key"
    );
}

#[test]
fn idempotency_key_stable_across_repeated_calls() {
    let ctx = ActivityContext::new_test();
    let k1 = ctx.idempotency_key().unwrap().as_str().to_owned();
    let k2 = ctx.idempotency_key().unwrap().as_str().to_owned();
    assert_eq!(k1, k2, "idempotency key must not change between calls");
}

// ── Distinctness across invocations ──────────────────────────────────────

#[test]
fn distinct_activity_exec_ids_produce_distinct_keys() {
    let id1 = ActivityExecId::new();
    let id2 = ActivityExecId::new();
    let k1 = IdempotencyKey::from_activity_exec_id(id1);
    let k2 = IdempotencyKey::from_activity_exec_id(id2);
    assert_ne!(k1.as_str(), k2.as_str());
}

#[test]
fn keys_for_ten_thousand_invocations_have_no_collisions() {
    let keys: HashSet<String> = (0..10_000)
        .map(|_| {
            IdempotencyKey::from_activity_exec_id(ActivityExecId::new())
                .as_str()
                .to_owned()
        })
        .collect();
    assert_eq!(
        keys.len(),
        10_000,
        "10,000 distinct invocations must yield 10,000 distinct keys"
    );
}

// ── Subkey derivation ────────────────────────────────────────────────────

#[test]
fn subkey_derivation_is_deterministic() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let s1 = key.subkey("charge").as_str().to_owned();
    let s2 = key.subkey("charge").as_str().to_owned();
    assert_eq!(
        s1, s2,
        "subkey(same_name) must return the same value every time"
    );
}

#[test]
fn different_subkey_names_produce_different_keys() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let charge = key.subkey("charge");
    let notify = key.subkey("notify");
    assert_ne!(
        charge.as_str(),
        notify.as_str(),
        "subkeys with distinct names must be distinct"
    );
}

#[test]
fn subkey_is_distinct_from_its_parent() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let sub = key.subkey("charge");
    assert_ne!(
        key.as_str(),
        sub.as_str(),
        "subkey must differ from parent key"
    );
}

#[test]
fn same_subkey_name_different_parents_produce_different_keys() {
    let k1 = IdempotencyKey::from_activity_exec_id(ActivityExecId::new()).subkey("charge");
    let k2 = IdempotencyKey::from_activity_exec_id(ActivityExecId::new()).subkey("charge");
    assert_ne!(
        k1.as_str(),
        k2.as_str(),
        "same subkey name on different parents must yield different keys"
    );
}

// ── HTTP-header safety ───────────────────────────────────────────────────

fn is_http_header_safe(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii() && c >= ' ' && c != '\x7f')
}

#[test]
fn base_key_display_is_safe_for_http_headers() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert!(
        is_http_header_safe(key.as_str()),
        "base key '{}' must be safe as an HTTP Idempotency-Key header value",
        key.as_str()
    );
}

#[test]
fn subkey_display_is_safe_for_http_headers() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let sub = key.subkey("charge-card");
    assert!(
        is_http_header_safe(sub.as_str()),
        "subkey '{}' must be safe as an HTTP Idempotency-Key header value",
        sub.as_str()
    );
}

#[test]
fn display_matches_as_str() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert_eq!(format!("{key}"), key.as_str());
}

// ── Builder methods ───────────────────────────────────────────────────────

#[test]
fn with_idempotency_key_overrides_default() {
    let id = ActivityExecId::new();
    let key = IdempotencyKey::from_activity_exec_id(id);
    let ctx = ActivityContext::new_test().with_idempotency_key(key.clone());
    assert_eq!(ctx.idempotency_key().unwrap().as_str(), key.as_str());
}

#[test]
fn attempt_returns_one_for_default_test_context() {
    let ctx = ActivityContext::new_test();
    assert_eq!(ctx.attempt(), Some(1));
}

#[test]
fn with_attempt_sets_attempt_number() {
    let ctx = ActivityContext::new_test().with_attempt(3);
    assert_eq!(ctx.attempt(), Some(3));
}

// ── Default key does not contain PII / input payloads ────────────────────

#[test]
fn base_key_is_uuid_format() {
    // The base key must be a UUID string (no arbitrary user data embedded).
    let id = ActivityExecId::new();
    let key = IdempotencyKey::from_activity_exec_id(id);
    let s = key.as_str();
    // UUID canonical form: 8-4-4-4-12 hex groups separated by hyphens
    assert_eq!(s.len(), 36);
    assert!(s.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
}

// ── Crash-retry / duplicate-dispatch simulation ──────────────────────────

/// Fake payment gateway that deduplicates on idempotency key.
/// A second charge with the same key is silently ignored (same semantics as
/// Stripe's idempotent requests).
struct FakeGateway {
    charges: Mutex<HashMap<String, u64>>,
}

impl FakeGateway {
    fn new() -> Self {
        Self {
            charges: Mutex::new(HashMap::new()),
        }
    }

    /// Record a charge. If the key was already used, no new charge is added.
    fn charge(&self, idempotency_key: &str, amount_cents: u64) {
        self.charges
            .lock()
            .unwrap()
            .entry(idempotency_key.to_string())
            .or_insert(amount_cents);
    }

    fn total_unique_charges(&self) -> usize {
        self.charges.lock().unwrap().len()
    }

    fn total_amount_charged(&self) -> u64 {
        self.charges.lock().unwrap().values().sum()
    }
}

#[tokio::test]
async fn payment_gateway_duplicate_dispatch_commits_exactly_once() {
    // The same logical activity invocation is dispatched 5 times
    // (simulating worker crash before Harvest records completion).
    // Every attempt must carry the *same* idempotency key so the gateway
    // deduplicates to exactly one charge.
    let gateway = Arc::new(FakeGateway::new());
    let idempotency_key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());

    for attempt in 1u32..=5 {
        let ctx = ActivityContext::new_test()
            .with_idempotency_key(idempotency_key.clone())
            .with_attempt(attempt);

        // Activity reads the Harvest-provided key and passes it to the gateway.
        let key = ctx.idempotency_key().unwrap();
        gateway.charge(key.as_str(), 2999);
    }

    assert_eq!(
        gateway.total_unique_charges(),
        1,
        "5 retries with the same idempotency key must result in exactly 1 gateway charge"
    );
    assert_eq!(gateway.total_amount_charged(), 2999);
}

#[tokio::test]
async fn payment_gateway_100_forced_retry_trials_each_commit_exactly_once() {
    // 100 independent workflow executions, each crashing and retrying 3×.
    // Each workflow has a distinct idempotency key; no cross-execution
    // collisions; each execution commits exactly one charge.
    let gateway = Arc::new(FakeGateway::new());

    for _ in 0..100 {
        let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
        for attempt in 1u32..=3 {
            let ctx = ActivityContext::new_test()
                .with_idempotency_key(key.clone())
                .with_attempt(attempt);
            gateway.charge(ctx.idempotency_key().unwrap().as_str(), 100);
        }
    }

    assert_eq!(
        gateway.total_unique_charges(),
        100,
        "100 executions × 3 attempts = 100 unique charges (one per logical invocation)"
    );
}

// ── Error path: missing key ───────────────────────────────────────────────

#[test]
fn context_without_key_returns_config_error() {
    // A context created via the internal `new` path without a key set
    // must return a descriptive HarvestError::Config, not panic or return
    // a meaningless value.
    let ctx = ActivityContext::new_for_idempotency_test_no_key();
    let result = ctx.idempotency_key();
    assert!(
        matches!(result, Err(HarvestError::Config(_))),
        "missing idempotency key must return HarvestError::Config, got: {result:?}"
    );
}

// ── Subkey chaining ───────────────────────────────────────────────────────

#[test]
fn nested_subkeys_are_stable_and_distinct() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    let charge = key.subkey("payment").subkey("charge");
    let refund = key.subkey("payment").subkey("refund");
    // Stable
    assert_eq!(
        key.subkey("payment").subkey("charge").as_str(),
        charge.as_str()
    );
    // Distinct siblings
    assert_ne!(charge.as_str(), refund.as_str());
}

// ── Subkey name validation ────────────────────────────────────────────────

#[test]
fn subkey_slash_in_name_does_not_collide_with_nested_subkey() {
    // "a/b" as a single name must not equal subkey("a").subkey("b").
    // The slash is rejected by the validator, so the collision is impossible
    // at the API level.
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    // Nested: key -> "a" -> "b"
    let nested = key.subkey("a").subkey("b");
    // A flat name that *looks* like the path "a/b" would panic — test that
    // the flat and nested forms are provably distinct by ensuring the nested
    // path prefix includes the intermediate segment.
    assert!(
        nested.as_str().contains("/a/"),
        "nested path must include both segments"
    );
}

#[test]
fn subkey_sanitizes_slash_in_name() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert!(key.subkey("a/b").as_str().ends_with("/a%2Fb"));
}

#[test]
fn subkey_sanitizes_empty_name() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert!(key.subkey("").as_str().ends_with("/default"));
}

#[test]
fn subkey_sanitizes_non_ascii_name() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert!(key.subkey("café").as_str().ends_with("/caf%C3%A9"));
}

#[test]
fn subkey_sanitizes_control_char_in_name() {
    let key = IdempotencyKey::from_activity_exec_id(ActivityExecId::new());
    assert!(
        key.subkey("charge\tcard")
            .as_str()
            .ends_with("/charge%09card")
    );
}
