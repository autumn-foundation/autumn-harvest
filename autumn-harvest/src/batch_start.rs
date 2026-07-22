//! Batch workflow start types (issue #357).
//!
//! Defines the hard-cap configuration, per-item request / result structs, and
//! per-item status enum consumed by `POST /workflows/batch_start`.
//!
//! This module is intentionally free of Diesel / database imports so it compiles
//! under `--no-default-features` for use in the CLI crate.

use serde::{Deserialize, Serialize};

use crate::types::Priority;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Default maximum number of items allowed in a single batch-start request.
pub const DEFAULT_BATCH_START_MAX_ITEMS: usize = 1000;

/// Default maximum serialised byte length for a batch-start request body: 10 MiB.
pub const DEFAULT_BATCH_START_MAX_BYTES: u64 = 10 * 1024 * 1024;

/// Absolute hard ceiling on the Axum-level body buffer for batch-start: 100 MiB.
///
/// Allows operators to raise `BatchStartConfig.max_total_bytes` above the
/// 10 MiB default while still bounding pre-handler memory usage. Requests
/// above this ceiling are rejected before any bytes reach the handler.
pub const BATCH_START_BODY_HARD_LIMIT: u64 = 100 * 1024 * 1024;

/// Hard caps applied to every `POST /workflows/batch_start` request (issue #357).
///
/// Both values are checked before any execution row is inserted. Requests that
/// exceed either limit are rejected with `413 Payload Too Large`.
///
/// Wire these into [`crate::builder::HarvestBuilder::batch_start_config`] during
/// application startup. The defaults are operator-friendly values that keep a
/// single management-API call from monopolising the connection pool or saturating
/// Postgres WAL bandwidth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchStartConfig {
    /// Maximum number of items in a single batch-start request.
    ///
    /// Default: **1 000**.
    pub max_items_per_batch: usize,

    /// Maximum combined byte length of the entire serialised request body.
    ///
    /// Default: **10 MiB** (`10 * 1024 * 1024`).
    pub max_total_bytes: u64,
}

impl Default for BatchStartConfig {
    fn default() -> Self {
        Self {
            max_items_per_batch: DEFAULT_BATCH_START_MAX_ITEMS,
            max_total_bytes: DEFAULT_BATCH_START_MAX_BYTES,
        }
    }
}

// ── Per-item request ──────────────────────────────────────────────────────────

/// One workflow to start, supplied inside a `POST /workflows/batch_start` body.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct BatchStartItem {
    /// Registered workflow type name (e.g. `"onboarding"`).
    pub workflow_name: String,

    /// Logical workflow instance id.
    ///
    /// When omitted the server generates a random UUID v4.
    #[serde(default)]
    pub workflow_id: Option<String>,

    /// JSON input forwarded to the workflow handler.
    ///
    /// Defaults to `null` when absent.
    #[serde(default)]
    pub input: Option<serde_json::Value>,

    /// Optional JSONB search-attribute object stored on the execution row.
    #[serde(default)]
    pub search_attributes: Option<serde_json::Value>,

    /// Caller-supplied idempotency key scoped to this item.
    ///
    /// A second request with the same `workflow_name` + `workflow_id` +
    /// `idempotency_key` triple is a no-op: the existing `execution_id` is
    /// returned without inserting a duplicate row.
    #[serde(default)]
    pub idempotency_key: Option<String>,

    /// Caller-supplied trace/correlation headers threaded onto the execution row.
    #[serde(default)]
    pub context_headers: Option<std::collections::HashMap<String, String>>,

    /// Dispatch priority for this item's tasks. Omitted defaults to `Normal`.
    #[serde(default)]
    pub priority: Option<Priority>,
}

// ── Per-item result ───────────────────────────────────────────────────────────

/// Outcome for a single item in a best-effort (`atomic = false`) batch-start response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchStartItemResult {
    /// Zero-based index of this item in the original request.
    pub index: usize,

    /// The workflow instance id this item resolved to — the caller's
    /// explicit `workflow_id`, or the server-generated UUID when omitted.
    ///
    /// `None` only for a rejection produced during pre-validation, before any
    /// id is resolved (e.g. an unregistered workflow name) — no id was ever
    /// generated or used for such an item.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,

    /// Whether the execution was successfully inserted or rejected.
    pub status: BatchStartItemStatus,

    /// Execution id of the started workflow.
    ///
    /// Present when `status` is `started`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,

    /// Human-readable rejection reason.
    ///
    /// Present when `status` is `rejected`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Whether a single item in a best-effort batch start succeeded or was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchStartItemStatus {
    /// The workflow execution row was successfully inserted.
    Started,
    /// The item was rejected (unknown workflow type, duplicate id, over-size
    /// payload, malformed input, etc.).
    Rejected,
    /// The item was deferred by a start throttle (issue #607): no token was
    /// available, so a durable pending-start row was written and the execution
    /// will be admitted later by the throttle scanner. Not a failure — the item
    /// is neither dropped nor rejected.
    Deferred,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn batch_start_config_default_values_match_spec() {
        let cfg = BatchStartConfig::default();
        assert_eq!(cfg.max_items_per_batch, 1000);
        assert_eq!(cfg.max_total_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn batch_start_config_constants_match_defaults() {
        let cfg = BatchStartConfig::default();
        assert_eq!(cfg.max_items_per_batch, DEFAULT_BATCH_START_MAX_ITEMS);
        assert_eq!(cfg.max_total_bytes, DEFAULT_BATCH_START_MAX_BYTES);
    }

    #[test]
    fn batch_start_item_minimal_deserializes() {
        let json = json!({"workflow_name": "onboarding"});
        let item: BatchStartItem = serde_json::from_value(json).unwrap();
        assert_eq!(item.workflow_name, "onboarding");
        assert!(item.workflow_id.is_none());
        assert!(item.input.is_none());
        assert!(item.search_attributes.is_none());
        assert!(item.idempotency_key.is_none());
    }

    #[test]
    fn batch_start_item_full_round_trips() {
        let item = BatchStartItem {
            workflow_name: "onboarding".to_string(),
            workflow_id: Some("user-42".to_string()),
            input: Some(json!({"user_id": 42})),
            search_attributes: Some(json!({"tenant": "acme"})),
            idempotency_key: Some("idem-key-1".to_string()),
            context_headers: Some(std::collections::HashMap::from([(
                "trace-id".to_string(),
                "abc123".to_string(),
            )])),
            priority: Some(Priority::High),
        };
        let serialised = serde_json::to_value(&item).unwrap();
        let back: BatchStartItem = serde_json::from_value(serialised).unwrap();
        assert_eq!(item, back);
    }

    #[test]
    fn batch_start_item_result_started_serialises() {
        let result = BatchStartItemResult {
            index: 0,
            workflow_id: Some("user-42".to_string()),
            status: BatchStartItemStatus::Started,
            execution_id: Some("00000000-0000-4000-8000-000000000001".to_string()),
            error: None,
        };
        let v = serde_json::to_value(&result).unwrap();
        assert_eq!(v["status"], "started");
        assert_eq!(v["workflow_id"], "user-42");
        assert_eq!(v["execution_id"], "00000000-0000-4000-8000-000000000001");
        assert!(
            v.get("error").is_none(),
            "error should be skipped when None"
        );
    }

    #[test]
    fn batch_start_item_result_workflow_id_skipped_when_none() {
        let result = BatchStartItemResult {
            index: 2,
            workflow_id: None,
            status: BatchStartItemStatus::Rejected,
            execution_id: None,
            error: Some("unregistered".to_string()),
        };
        let v = serde_json::to_value(&result).unwrap();
        assert!(
            v.get("workflow_id").is_none(),
            "workflow_id should be skipped when None"
        );
    }

    #[test]
    fn batch_start_item_result_rejected_serialises() {
        let result = BatchStartItemResult {
            index: 1,
            workflow_id: None,
            status: BatchStartItemStatus::Rejected,
            execution_id: None,
            error: Some("workflow 'unknown_wf' is not registered".to_string()),
        };
        let v = serde_json::to_value(&result).unwrap();
        assert_eq!(v["status"], "rejected");
        assert!(
            v.get("execution_id").is_none(),
            "execution_id should be skipped when None"
        );
        assert_eq!(v["error"], "workflow 'unknown_wf' is not registered");
    }

    #[test]
    fn batch_start_item_status_round_trips() {
        for status in [
            BatchStartItemStatus::Started,
            BatchStartItemStatus::Rejected,
        ] {
            let v = serde_json::to_value(status).unwrap();
            let back: BatchStartItemStatus = serde_json::from_value(v).unwrap();
            assert_eq!(back, status);
        }
    }
}
