//! Idempotency-key derivation for broker event-source connectors (issue #944).
//!
//! # Why the key is derived, not supplied
//!
//! Every broker in scope redelivers: a Kafka consumer-group rebalance replays
//! uncommitted offsets, an SQS visibility timeout expires and the message
//! reappears. The connector must therefore make dispatch idempotent *by
//! construction* rather than trusting the producer to send a key.
//!
//! The key is derived from [stable broker coordinates] — `{topic}:{partition}:
//! {offset}` for Kafka, `MessageDeduplicationId`/`MessageId` for SQS — and is
//! **namespaced by the binding** exactly as the inbound webhook receiver
//! namespaces `{path}:{signal_name}:{delivery_id}` (issue #344, PR #918
//! review). Without that namespace, two bindings targeting the same
//! `(workflow_name, workflow_id)` would silently swallow each other's signals
//! whenever their raw broker ids happened to coincide — `signal_with_start`'s
//! dedupe is scoped to `(workflow_name, workflow_id, idempotency_key)` and
//! knows nothing about topics.
//!
//! [stable broker coordinates]: crate::connector::message::MessageCoordinates
//!
//! # Injectivity
//!
//! Components are joined with `:`, but a topic, a queue name, or an SQS
//! `MessageDeduplicationId` may itself contain `:`. A naive join is therefore
//! **not** injective: binding `a` + id `b:c` and binding `a:b` + id `c` both
//! flatten to `conn:a:b:c` — two genuinely different messages colliding onto
//! one dedupe key.
//!
//! Each component is therefore encoded self-delimitingly, mirroring the
//! `bound_key_component` scheme the per-key rate limiter uses (issue #699):
//!
//! * `L{byte_len}:{value}` for a literal — the length prefix says exactly how
//!   many bytes follow, so an embedded `:` can never split ambiguously.
//! * `H{64 hex}` for an over-length component — the full SHA-256 of the value.
//!
//! The `L`/`H` first-byte tags are structurally disjoint, so a literal whose
//! value happens to look like a hash encoding can never collide with that
//! hash.
//!
//! # Boundedness
//!
//! `autumn_harvest::start_idempotency::MAX_START_IDEMPOTENCY_KEY_LEN` is 512
//! bytes and `validate_start_idempotency_key` rejects anything longer. A
//! broker-supplied id is caller-controlled and unbounded, so the encoding
//! hashes any component over [`MAX_KEY_COMPONENT_LEN`], which caps the whole
//! derived key well under that limit — see
//! `derived_key_is_bounded_for_pathological_components`.

use sha2::{Digest, Sha256};
use std::fmt::Write as _;

/// Prefix marking a key as connector-derived.
///
/// Reserved: it keeps the connector's dedupe namespace provably disjoint from
/// an idempotency key an application supplies through the plain HTTP start
/// route, so the two can never alias.
pub const CONNECTOR_KEY_PREFIX: &str = "conn";

/// Longest component encoded literally; anything longer is SHA-256 hashed.
///
/// Chosen so the whole derived key (prefix + three encoded components +
/// separators) stays comfortably inside the 512-byte
/// `MAX_START_IDEMPOTENCY_KEY_LEN` ceiling even when every component is
/// over-length.
pub const MAX_KEY_COMPONENT_LEN: usize = 128;

/// Encode one key component self-delimitingly and boundedly.
///
/// Returns `L{len}:{value}` for a short literal and `H{64 hex}` for an
/// over-length component (full SHA-256, so a collision is computationally
/// infeasible — a 64-bit hash would not be safe against broker-supplied ids).
#[must_use]
pub fn bound_key_component(component: &str) -> String {
    if component.len() > MAX_KEY_COMPONENT_LEN {
        let digest = Sha256::digest(component.as_bytes());
        let mut out = String::with_capacity(1 + 64);
        out.push('H');
        for b in digest {
            let _ = write!(out, "{b:02x}");
        }
        out
    } else {
        format!("L{}:{}", component.len(), component)
    }
}

/// Derive the namespaced, injective, bounded idempotency key for one message.
///
/// `signal_name` is `Some` for a `SignalsWithStart` binding and `None` for a
/// `Starts` binding; including it is defence-in-depth (the binding name alone
/// already disambiguates, since duplicate binding names are rejected at build
/// time).
#[must_use]
pub fn message_idempotency_key(
    binding: &str,
    signal_name: Option<&str>,
    coordinates_render: &str,
) -> String {
    format!(
        "{CONNECTOR_KEY_PREFIX}:{}:{}:{}",
        bound_key_component(binding),
        bound_key_component(signal_name.unwrap_or("")),
        bound_key_component(coordinates_render),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::message::MessageCoordinates;

    fn kafka(topic: &str, partition: i32, offset: i64) -> String {
        MessageCoordinates::KafkaOffset {
            topic: topic.to_string(),
            partition,
            offset,
        }
        .render()
    }

    #[test]
    fn same_message_derives_the_same_key() {
        let a = message_idempotency_key("orders", None, &kafka("orders", 0, 7));
        let b = message_idempotency_key("orders", None, &kafka("orders", 0, 7));
        assert_eq!(a, b, "redelivery must derive an identical key");
    }

    #[test]
    fn distinct_offsets_derive_distinct_keys() {
        let a = message_idempotency_key("orders", None, &kafka("orders", 0, 7));
        let b = message_idempotency_key("orders", None, &kafka("orders", 0, 8));
        assert_ne!(a, b);
    }

    #[test]
    fn distinct_partitions_derive_distinct_keys() {
        let a = message_idempotency_key("orders", None, &kafka("orders", 0, 7));
        let b = message_idempotency_key("orders", None, &kafka("orders", 1, 7));
        assert_ne!(a, b);
    }

    #[test]
    fn distinct_bindings_derive_distinct_keys_for_the_same_message() {
        // The #918 review precedent: two bindings must never alias, or one
        // binding's signal is silently dropped as the other's "replay".
        let a = message_idempotency_key("orders", Some("order_event"), &kafka("t", 0, 1));
        let b = message_idempotency_key("audit", Some("order_event"), &kafka("t", 0, 1));
        assert_ne!(a, b);
    }

    #[test]
    fn distinct_signal_names_derive_distinct_keys() {
        let a = message_idempotency_key("orders", Some("created"), &kafka("t", 0, 1));
        let b = message_idempotency_key("orders", Some("updated"), &kafka("t", 0, 1));
        assert_ne!(a, b);
    }

    #[test]
    fn starts_and_signals_bindings_do_not_alias() {
        // `None` encodes as the empty literal `L0:`, which is structurally
        // distinct from any non-empty signal name.
        let starts = message_idempotency_key("orders", None, &kafka("t", 0, 1));
        let signals = message_idempotency_key("orders", Some(""), &kafka("t", 0, 1));
        assert_eq!(
            starts, signals,
            "an explicitly-empty signal name is the same namespace as None"
        );
        let named = message_idempotency_key("orders", Some("x"), &kafka("t", 0, 1));
        assert_ne!(starts, named);
    }

    #[test]
    fn separator_bearing_components_cannot_collide() {
        // The whole point of the length-tagged encoding. A naive
        // `format!("{binding}:{sig}:{coords}")` join makes these two equal.
        let a = message_idempotency_key("a", Some("b"), "c:d");
        let b = message_idempotency_key("a", Some("b:c"), "d");
        assert_ne!(a, b, "`:`-bearing components must stay injective");

        let c = message_idempotency_key("a:b", None, "c");
        let d = message_idempotency_key("a", None, "b:c");
        assert_ne!(c, d);
    }

    #[test]
    fn literal_never_collides_with_a_hash_encoding() {
        // A short literal whose *value* is exactly a hash's string encoding
        // must not collide with that hash (issue #699's precedent bug).
        let long = "x".repeat(MAX_KEY_COMPONENT_LEN + 1);
        let hashed = bound_key_component(&long);
        assert!(hashed.starts_with('H'));
        let literal = bound_key_component(&hashed);
        assert!(literal.starts_with('L'));
        assert_ne!(hashed, literal);
    }

    #[test]
    fn derived_key_is_bounded_for_pathological_components() {
        // Every component maximally over-length: the key must still be well
        // inside MAX_START_IDEMPOTENCY_KEY_LEN (512) so
        // `validate_start_idempotency_key` can never reject a message the
        // connector itself derived.
        let huge = "z".repeat(64 * 1024);
        let key = message_idempotency_key(&huge, Some(&huge), &huge);
        assert!(
            key.len() <= 512,
            "derived key must be <= 512 bytes, got {}",
            key.len()
        );
        // And distinct over-length components still derive distinct keys.
        let other = "y".repeat(64 * 1024);
        assert_ne!(key, message_idempotency_key(&other, Some(&huge), &huge));
    }

    #[test]
    fn typical_key_is_short_and_prefixed() {
        let key = message_idempotency_key("orders", Some("order_event"), &kafka("orders", 0, 42));
        assert!(key.starts_with("conn:"));
        assert!(key.len() < 120, "typical key should stay short: {key}");
    }

    #[test]
    fn bound_key_component_is_deterministic() {
        let long = "q".repeat(500);
        assert_eq!(bound_key_component(&long), bound_key_component(&long));
        assert_eq!(bound_key_component("abc"), "L3:abc");
        assert_eq!(bound_key_component(""), "L0:");
    }
}
