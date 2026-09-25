//! Redis key naming convention.
//!
//! Centralized so all callers agree on the key shape and operators have a
//! single place to look when building `redis-cli` queries.

/// Per-queue Redis Stream that holds claimable tasks.
#[must_use]
pub fn stream_key(prefix: &str, queue_name: &str) -> String {
    format!("{prefix}:queue:{queue_name}")
}

/// Per-queue sorted set that holds delayed tasks awaiting their `scheduled_at`.
///
/// A periodic promoter moves entries whose score (unix milliseconds) is
/// `<= NOW()` into the corresponding stream via [`stream_key`].
#[must_use]
pub fn scheduled_zset_key(prefix: &str, queue_name: &str) -> String {
    format!("{prefix}:scheduled:{queue_name}")
}

/// Per-queue HASH used by the scheduled set to store the full envelope payload
/// keyed by stable `task_id`.
///
/// We can't put the payload directly on the sorted set because the score is
/// the only ordering primitive there; instead we ZADD `task_id` with the score
/// and HSET the payload alongside, then HDEL when promoting or cancelling.
#[must_use]
pub fn scheduled_payloads_key(prefix: &str, queue_name: &str) -> String {
    format!("{prefix}:scheduled:{queue_name}:payloads")
}

/// Per-queue dead-letter stream for tasks that exhausted their retries.
#[must_use]
pub fn dlq_key(prefix: &str, queue_name: &str) -> String {
    format!("{prefix}:dlq:{queue_name}")
}

/// Hash tag for one queue's dispatch key family (issue #1429).
///
/// Every dispatch key for `queue_name` nests this substring in `{...}`.
/// Redis Cluster hashes only the bytes between the first `{` and the next
/// `}` to pick a slot. Every key sharing this tag therefore lands on the
/// same slot, and the channel's multi-key scripts (`PUBLISH_LUA`,
/// `REQUEUE_LUA`, `PROMOTE_MARKED_LUA`) stay valid on a cluster.
fn dispatch_key_tag(prefix: &str, queue_name: &str) -> String {
    format!("{{{prefix}:dispatch:{queue_name}}}")
}

/// Per-queue Redis Stream that holds dispatch references (issue #1312).
///
/// The dispatch channel is a separate key family from the standalone queue
/// above. A dispatch entry holds only a reference to a `harvest_task_queue`
/// row, never the task payload.
#[must_use]
pub fn dispatch_stream_key(prefix: &str, queue_name: &str) -> String {
    dispatch_key_tag(prefix, queue_name)
}

/// Per-queue sorted set of dispatch references that are not yet due.
///
/// The score is the due time in unix milliseconds. A promoter moves due
/// members onto [`dispatch_stream_key`].
#[must_use]
pub fn dispatch_delayed_key(prefix: &str, queue_name: &str) -> String {
    format!("{}:delayed", dispatch_key_tag(prefix, queue_name))
}

/// Per-queue hash that holds the payload of each delayed dispatch reference.
///
/// The sorted set orders by score alone, so the payload lives beside it,
/// keyed by `task_id`.
#[must_use]
pub fn dispatch_payloads_key(prefix: &str, queue_name: &str) -> String {
    format!("{}:delayed:payloads", dispatch_key_tag(prefix, queue_name))
}

/// Dedupe marker for one task id on one queue.
///
/// The marker makes a publish idempotent per task id. It expires after the
/// configured dedupe TTL, so a leaked marker cannot block a republish for
/// ever. The key carries `queue_name`'s hash tag (issue #1429). A publish or
/// a release touches the marker in the same multi-key script call as the
/// queue's stream, delayed set and payload hash. It must therefore land in
/// the same Redis Cluster slot as the rest of that call's keys. A task id
/// identifies a row on exactly one queue, so this never collides across
/// queues.
#[must_use]
pub fn dispatch_marker_key(prefix: &str, queue_name: &str, task_id: &str) -> String {
    format!("{}{task_id}", dispatch_marker_prefix(prefix, queue_name))
}

/// Key prefix every dedupe marker for `queue_name` shares.
///
/// The promote script builds a marker key from a task id. It reads that id
/// out of the delayed set, so it needs the prefix rather than a finished
/// key.
#[must_use]
pub fn dispatch_marker_prefix(prefix: &str, queue_name: &str) -> String {
    format!("{}:marker:", dispatch_key_tag(prefix, queue_name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_stable() {
        assert_eq!(stream_key("harvest", "email"), "harvest:queue:email");
        assert_eq!(
            scheduled_zset_key("harvest", "email"),
            "harvest:scheduled:email"
        );
        assert_eq!(
            scheduled_payloads_key("harvest", "email"),
            "harvest:scheduled:email:payloads"
        );
        assert_eq!(dlq_key("harvest", "email"), "harvest:dlq:email");
    }

    #[test]
    fn keys_respect_custom_prefix() {
        assert_eq!(stream_key("acme", "billing"), "acme:queue:billing");
    }

    #[test]
    fn dispatch_keys_are_stable() {
        assert_eq!(
            dispatch_stream_key("harvest", "email"),
            "{harvest:dispatch:email}"
        );
        assert_eq!(
            dispatch_delayed_key("harvest", "email"),
            "{harvest:dispatch:email}:delayed"
        );
        assert_eq!(
            dispatch_payloads_key("harvest", "email"),
            "{harvest:dispatch:email}:delayed:payloads"
        );
        assert_eq!(
            dispatch_marker_key("harvest", "email", "abc"),
            "{harvest:dispatch:email}:marker:abc"
        );
    }

    #[test]
    fn dispatch_keys_never_collide_with_queue_keys() {
        // The standalone queue owns `:queue:` and `:scheduled:`. The dispatch
        // channel owns `:dispatch:`. One Redis can hold both under one prefix.
        assert_ne!(
            dispatch_stream_key("harvest", "email"),
            stream_key("harvest", "email")
        );
        assert_ne!(
            dispatch_delayed_key("harvest", "email"),
            scheduled_zset_key("harvest", "email")
        );
    }

    #[test]
    fn dispatch_keys_for_one_queue_share_one_hash_tag() {
        // Issue #1429: Redis Cluster hashes only the bytes inside `{...}`.
        // Every key in one queue's dispatch family must carry the same tag
        // so a multi-key script (PUBLISH_LUA, REQUEUE_LUA,
        // PROMOTE_MARKED_LUA) stays in one slot.
        fn tag(key: &str) -> &str {
            let start = key.find('{').expect("key carries a hash tag");
            let end = key.find('}').expect("hash tag is closed");
            &key[start + 1..end]
        }

        let stream = dispatch_stream_key("harvest", "email");
        let delayed = dispatch_delayed_key("harvest", "email");
        let payloads = dispatch_payloads_key("harvest", "email");
        let marker = dispatch_marker_key("harvest", "email", "abc");
        let marker_prefix = dispatch_marker_prefix("harvest", "email");

        let expected = tag(&stream);
        assert_eq!(tag(&delayed), expected);
        assert_eq!(tag(&payloads), expected);
        assert_eq!(tag(&marker), expected);
        assert_eq!(tag(&marker_prefix), expected);
    }

    #[test]
    fn dispatch_keys_for_different_queues_carry_different_tags() {
        // Different queues may then land on different Cluster slots, which
        // spreads the channel's load instead of pinning it to one node.
        assert_ne!(
            dispatch_stream_key("harvest", "email"),
            dispatch_stream_key("harvest", "billing")
        );
    }
}
