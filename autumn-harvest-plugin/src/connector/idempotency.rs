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
//! {offset}` for Kafka, `MessageId` for SQS — and is
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
//! Components are joined with `:`, but a topic, a queue name, or a broker's
//! own message id may itself contain `:`. A naive join is therefore
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

/// Prefix marking a key as connector-derived, **including** its separator.
///
/// Reserved, and *enforced* rather than merely conventional: derived keys share
/// a durable uniqueness scope with caller-supplied ones — `(workflow_name,
/// idempotency_key)` on `harvest_start_idempotency` for a `Starts` binding, and
/// `(workflow_exec_id, idempotency_key)` on `harvest_signals` for a
/// `SignalsWithStart` one — and they are *predictable* (Kafka coordinates are
/// `{topic}:{partition}:{offset}`). A caller who landed a derived key first
/// would make the broker's own delivery read as an idempotent replay, acked
/// without ever dispatching its payload.
///
/// So the caller-facing routes refuse this prefix outright; see
/// [`autumn_harvest::start_idempotency::RESERVED_KEY_PREFIX`], which this
/// aliases so the deriving and the refusing side can never drift apart.
pub const CONNECTOR_KEY_PREFIX: &str = autumn_harvest::start_idempotency::RESERVED_KEY_PREFIX;

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
    message_idempotency_key_in(binding, signal_name, coordinates_render, None)
}

/// Derive the key within a named *incarnation* of the binding's stream.
///
/// # Why an incarnation is needed
///
/// Broker coordinates are only unique within one incarnation of the thing
/// they address. Kafka's `{topic}:{partition}:{offset}` restarts at zero when
/// a topic is deleted and recreated, and means nothing across a cutover to a
/// different cluster — so the same coordinates come back around while the
/// claims from their previous life are still live, and fresh records are
/// classified as replays and acknowledged without ever dispatching. Neither
/// half is observable from the consumer API: Kafka exposes no topic
/// incarnation, and a bootstrap list is not a stable cluster identity.
///
/// The binding name is already the key namespace, so renaming the binding
/// also rotates it — but that name is the `source` metric label too, so the
/// remedy would cost every dashboard and alert built on the binding. This
/// separates the two: rotate the dedupe namespace, keep the identity.
///
/// # Encoding
///
/// The component is appended, and *only* when set, so a binding that never
/// rotates derives byte-identical keys to a build without this parameter —
/// the key is persisted, and changing the derivation would invalidate every
/// live claim on upgrade. Appending stays injective for the same reason the
/// other components are: each is self-delimiting, so a key with a trailing
/// component can never read as one without.
#[must_use]
pub fn message_idempotency_key_in(
    binding: &str,
    signal_name: Option<&str>,
    coordinates_render: &str,
    incarnation: Option<&str>,
) -> String {
    let base = format!(
        "{CONNECTOR_KEY_PREFIX}{}:{}:{}",
        bound_key_component(binding),
        bound_key_component(signal_name.unwrap_or("")),
        bound_key_component(coordinates_render),
    );
    match incarnation {
        None => base,
        Some(incarnation) => format!("{base}:{}", bound_key_component(incarnation)),
    }
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

    /// Kafka coordinates are only unique *within one incarnation* of a topic.
    ///
    /// Cut a binding over to another cluster, or delete and recreate its
    /// topic, and offsets restart at zero — so `(topic, partition, offset)`
    /// repeats values whose claims are still live. Without a way to rotate
    /// the namespace those fresh records are classified as replays and acked
    /// without ever dispatching: silent loss, exactly the failure the key
    /// exists to prevent.
    #[test]
    fn a_rotated_incarnation_separates_a_recreated_topic_from_the_old_one() {
        let before = message_idempotency_key_in("orders", None, &kafka("orders", 0, 7), None);
        let after = message_idempotency_key_in(
            "orders",
            None,
            &kafka("orders", 0, 7),
            Some("2026-08-cutover"),
        );
        assert_ne!(
            before, after,
            "a rotated incarnation must not collide with the claims it is \
             rotating away from"
        );
    }

    /// Rotating is opt-in, and the default must stay byte-identical.
    ///
    /// This key is persisted: changing the derivation for bindings that set
    /// no incarnation would invalidate every live claim on upgrade and
    /// re-dispatch whatever the broker still holds. Pinned against a literal
    /// so an accidental format change cannot pass.
    #[test]
    fn an_unset_incarnation_leaves_the_key_byte_identical() {
        assert_eq!(
            message_idempotency_key_in("orders", None, &kafka("orders", 0, 7), None),
            "conn:L6:orders:L0::L10:orders:0:7",
        );
        assert_eq!(
            message_idempotency_key_in("orders", None, &kafka("orders", 0, 7), None),
            message_idempotency_key("orders", None, &kafka("orders", 0, 7)),
        );
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

    /// The namespaces must be disjoint *by enforcement*, not by convention.
    ///
    /// Derived keys are predictable — Kafka coordinates are
    /// `{topic}:{partition}:{offset}` — so a caller who could claim one first
    /// would make the broker's own delivery read as an idempotent replay and
    /// be acked without dispatching. Asserted behaviourally rather than as a
    /// string-equality pin on the two constants, so it still holds however
    /// either is spelled.
    #[test]
    fn every_derived_key_is_refused_from_a_caller() {
        for key in [
            message_idempotency_key("orders", None, &kafka("orders", 0, 7)),
            message_idempotency_key("orders", Some("order_event"), &kafka("orders", 3, 91)),
            message_idempotency_key_in("orders", None, &kafka("orders", 0, 7), Some("cutover")),
            // Pathological components take the hashed encoding; still reserved.
            message_idempotency_key(&"z".repeat(64 * 1024), Some("s"), "c"),
        ] {
            assert!(
                autumn_harvest::start_idempotency::is_reserved_key(&key),
                "derived key must be in the reserved namespace: {key}"
            );
            assert!(
                autumn_harvest::start_idempotency::validate_start_idempotency_key(Some(&key))
                    .is_err(),
                "a caller must not be able to squat the derived key: {key}"
            );
        }
    }

    /// Adding the separator to the prefix must not have changed the derived
    /// key's bytes: it is persisted, so a format change would invalidate every
    /// live claim on upgrade.
    #[test]
    fn the_prefix_carries_its_own_separator_and_the_key_is_unchanged() {
        assert_eq!(CONNECTOR_KEY_PREFIX, "conn:");
        assert_eq!(
            message_idempotency_key("orders", None, &kafka("orders", 0, 7)),
            "conn:L6:orders:L0::L10:orders:0:7",
        );
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
