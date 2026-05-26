//! Per-key concurrency limits for tenant fair-share scheduling (issue #247).
//!
//! # Overview
//!
//! When multiple tenants share a worker fleet, a single noisy tenant can
//! saturate the pool and starve everyone else.  `ConcurrencyPolicy` lets an
//! author declare a *key expression* and a *limit*:
//!
//! ```rust
//! use autumn_harvest::concurrency::ConcurrencyPolicy;
//!
//! let policy = ConcurrencyPolicy { key_expr: "input.tenant_id", limit: 10 };
//! assert_eq!(policy.limit, 10);
//! ```
//!
//! At dispatch time the worker resolves the expression against the workflow's
//! JSON input (via [`crate::concurrency::resolve_concurrency_key`]) to get the concrete group key
//! (e.g. `"acme"`), then passes `(key, limit)` to [`crate::queue::EnqueueParams`]
//! so the `SKIP LOCKED` claim query enforces it across the whole fleet.
//!
//! # Sharding note
//!
//! Limits are enforced *within a shard*. Cross-shard global limits are out of
//! scope; embedders wanting a true global cap should route all executions for
//! a given key to a single shard via a custom [`crate::ShardRouter`].
//! See `docs/sharding.md` for details.

/// Declarative per-key concurrency constraint attached to a [`crate::info::WorkflowInfo`].
///
/// The macro `#[workflow(concurrency(key = "input.tenant_id", limit = 10))]`
/// populates this struct on the companion `WorkflowInfo`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyPolicy {
    /// JSON field path (dot-notation) resolved against the workflow input to
    /// produce the runtime group key.  The `"input."` prefix is stripped if
    /// present so `"input.tenant_id"` and `"tenant_id"` are equivalent.
    ///
    /// Nested paths like `"user.id"` walk into nested objects.
    pub key_expr: &'static str,
    /// Maximum number of RUNNING workflow tasks with the same resolved key,
    /// enforced across the whole worker fleet for this shard.
    pub limit: u32,
}

/// Resolve a dot-notation key expression against a JSON input payload.
///
/// The `"input."` prefix is stripped if present so both `"tenant_id"` and
/// `"input.tenant_id"` work identically.  Nested paths (e.g. `"user.id"`)
/// walk into nested JSON objects.
///
/// Returns `None` when:
/// - The input is not a JSON object.
/// - Any segment along the path is missing.
/// - The resolved value is JSON `null`.
///
/// Non-string values are converted to their JSON string representation
/// (`123` → `"123"`, `true` → `"true"`) so the caller always gets a
/// plain `String` usable as a concurrency group key.
///
/// # Examples
///
/// ```rust
/// use autumn_harvest::concurrency::resolve_concurrency_key;
///
/// let input = serde_json::json!({ "tenant_id": "acme" });
/// assert_eq!(
///     resolve_concurrency_key("input.tenant_id", &input),
///     Some("acme".to_string()),
/// );
///
/// let nested = serde_json::json!({ "user": { "id": 42 } });
/// assert_eq!(
///     resolve_concurrency_key("user.id", &nested),
///     Some("42".to_string()),
/// );
/// ```
#[must_use]
pub fn resolve_concurrency_key(expr: &str, input: &serde_json::Value) -> Option<String> {
    // Strip the "input." prefix so "input.tenant_id" == "tenant_id".
    let path = expr.strip_prefix("input.").unwrap_or(expr);

    let mut current = input;
    for segment in path.split('.') {
        current = current.as_object()?.get(segment)?;
    }

    match current {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_top_level_field() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_concurrency_key("tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_input_prefix_stripped() {
        let input = serde_json::json!({ "tenant_id": "acme" });
        assert_eq!(
            resolve_concurrency_key("input.tenant_id", &input),
            Some("acme".to_string())
        );
    }

    #[test]
    fn resolve_nested() {
        let input = serde_json::json!({ "user": { "id": 42 } });
        assert_eq!(
            resolve_concurrency_key("user.id", &input),
            Some("42".to_string())
        );
    }

    #[test]
    fn resolve_missing_returns_none() {
        let input = serde_json::json!({ "other": "val" });
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_null_returns_none() {
        let input = serde_json::json!({ "tenant_id": null });
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }

    #[test]
    fn resolve_integer_as_string() {
        let input = serde_json::json!({ "tenant_id": 123 });
        assert_eq!(
            resolve_concurrency_key("tenant_id", &input),
            Some("123".to_string())
        );
    }

    #[test]
    fn resolve_non_object_input() {
        let input = serde_json::json!("plain_string");
        assert_eq!(resolve_concurrency_key("tenant_id", &input), None);
    }
}
