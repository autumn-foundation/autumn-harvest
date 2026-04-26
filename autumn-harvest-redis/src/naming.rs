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
}
