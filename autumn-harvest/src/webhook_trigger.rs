//! Inbound HTTP webhook trigger descriptors (issue #344).
//!
//! # Who owns what
//!
//! Harvest does **not** ship its own signature-verification, replay-protection,
//! or secret-rotation code. autumn-web 0.5 has first-class signed webhook
//! intake: `autumn_web::webhook::SignedWebhook`, configured via
//! `[security.webhooks.endpoints]`. It verifies the sender's signature and
//! timestamp, rejects stale/duplicate deliveries, and rotates secrets. All of
//! that happens **before** any handler code runs. Harvest's slice sits
//! entirely downstream of that verification. It answers a narrower question:
//! *which workflow should this verified delivery start or signal, and how do
//! we dispatch that idempotently?*
//!
//! The outbound-webhook slice works the same way. The `webhooks` feature of
//! `autumn-harvest-plugin` delivers outbound webhooks through the
//! `webhook_outbound` module of autumn-web. So does the `#[workflow(mcp)]` MCP
//! tool slice (issue #597). Autumn owns the primitive, and Harvest ships a
//! thin binding layer.
//!
//! # The pieces
//!
//! - [`WebhookCtx`] — verified request metadata handed to a `#[webhook]`
//!   mapping function (never the *unverified* request).
//! - [`WebhookTarget`] — whether a verified delivery starts a fresh workflow
//!   or signals (with start-or-attach) an existing one.
//! - [`WebhookTriggerInfo`] — the registration record produced by a
//!   `#[webhook]` companion function and collected by `webhooks![]`.
//! - [`validate_webhook_triggers`] — pure fail-fast validation run at
//!   `HarvestPlugin::build` time, before any route is mounted.
//!
//! See `docs/getting-started/12-webhooks.md` for an end-to-end walkthrough.

use crate::types::WorkflowId;

/// Verified webhook request metadata handed to a `#[webhook]` mapping
/// function.
///
/// The plugin builds it from an already-verified
/// `autumn_web::webhook::SignedWebhook`. Signature, timestamp and (if
/// configured) replay verification have succeeded before a mapping function
/// receives it. A mapping function never sees an unverified request.
#[derive(Debug, Clone)]
pub struct WebhookCtx {
    /// The exact HTTP path this webhook is bound to (the `#[webhook(path =
    /// ...)]` value), useful for logging when one mapping function is shared
    /// across bindings.
    pub path: &'static str,
    /// The `security.webhooks.endpoints` configuration entry name that
    /// verified this request.
    pub endpoint: String,
    /// The provider preset that verified this request (`"stripe"`,
    /// `"github"`, `"slack"`, or `"generic"`).
    pub provider: String,
    /// The provider-supplied delivery ID, when the endpoint's
    /// `delivery_id_header` (or a top-level JSON `"id"` field) resolved one.
    ///
    /// Required for [`WebhookTarget::SignalsWithStart`] targets — it becomes
    /// the signal's idempotency key. Not required for
    /// [`WebhookTarget::Starts`] targets, whose idempotency comes from the
    /// mapping function's deterministic [`WorkflowId`].
    pub delivery_id: Option<String>,
    /// The provider-supplied event type, when the endpoint's
    /// `event_type_header` resolved one.
    pub event_type: Option<String>,
    /// The exact verified request body bytes.
    pub raw_body: Vec<u8>,
}

impl WebhookCtx {
    /// Construct a `WebhookCtx` from already-verified request metadata.
    #[must_use]
    pub fn new(
        path: &'static str,
        endpoint: impl Into<String>,
        provider: impl Into<String>,
        delivery_id: Option<String>,
        event_type: Option<String>,
        raw_body: Vec<u8>,
    ) -> Self {
        Self {
            path,
            endpoint: endpoint.into(),
            provider: provider.into(),
            delivery_id,
            event_type,
            raw_body,
        }
    }
}

/// Why a `#[webhook]` mapping function's dispatch shim failed to produce a
/// workflow trigger.
///
/// The plugin surfaces both variants as `400 Bad Request`, never a `500`,
/// because the request has already passed signature verification. The
/// `error_code` is `"parse_failed"` for [`Self::Deserialize`] and
/// `"mapping_rejected"` for [`Self::Rejected`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebhookHandlerError {
    /// The verified body could not be deserialized into the mapping
    /// function's typed payload parameter.
    Deserialize(String),
    /// The mapping function itself returned `Err`.
    Rejected(String),
}

impl std::fmt::Display for WebhookHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deserialize(msg) => write!(f, "webhook payload deserialize failed: {msg}"),
            Self::Rejected(msg) => write!(f, "webhook mapping function rejected payload: {msg}"),
        }
    }
}

impl std::error::Error for WebhookHandlerError {}

/// Type-erased `#[webhook]` mapping shim.
///
/// A plain `fn` pointer, like [`crate::info::WorkflowHandlerFn`] (design
/// decision #6: fn pointers keep registration `Sync` without `Arc`). The macro
/// casts a monomorphized closure to this type. The closure deserializes
/// `payload` into the user's typed parameter and calls the mapping function.
/// The mapping function is synchronous by design: it never does I/O.
///
/// `payload` is borrowed, not owned. The dispatch path uses the same verified
/// body twice: for this deserialization and as the workflow input or signal
/// payload of the delegate start call. `&Value` lets the caller keep its own
/// owned copy without an extra clone per dispatch.
pub type WebhookHandlerFn =
    fn(&WebhookCtx, &serde_json::Value) -> Result<WorkflowId, WebhookHandlerError>;

/// Where a verified webhook delivery is routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookTarget {
    /// Start a fresh workflow execution (`reuse_policy: AllowDuplicate`). The
    /// mapping function's returned [`WorkflowId`] is the idempotency
    /// mechanism: redelivering the same logical event should map to the same
    /// `WorkflowId`.
    Starts {
        /// The target workflow's registered name.
        workflow: &'static str,
    },
    /// Atomically start-or-attach and signal an existing (or fresh) workflow
    /// execution, keyed by the verified delivery ID (issue #244's
    /// `signal_with_start`).
    SignalsWithStart {
        /// The target workflow's registered name.
        workflow: &'static str,
        /// The signal name delivered to the target workflow.
        signal_name: &'static str,
    },
}

impl WebhookTarget {
    /// The target workflow's registered name, regardless of variant.
    #[must_use]
    pub const fn workflow(&self) -> &'static str {
        match self {
            Self::Starts { workflow } | Self::SignalsWithStart { workflow, .. } => workflow,
        }
    }
}

/// Registration produced by a `#[webhook]` companion function, collected by
/// `webhooks![]` and consumed by `HarvestPlugin::webhooks(...)`.
#[derive(Debug, Clone)]
pub struct WebhookTriggerInfo {
    /// The annotated function's name.
    pub name: &'static str,
    /// The annotated function's module path (`module_path!()`), for
    /// diagnostics when two mapping functions share a name.
    pub module: &'static str,
    /// The exact HTTP path this webhook binds to. Must start with `/` and
    /// must not be `/`. Must match a `security.webhooks.endpoints[].path` in
    /// the embedding app's configuration exactly — that is the endpoint
    /// whose verification governs this binding.
    pub path: &'static str,
    /// Where a verified delivery is routed.
    pub target: WebhookTarget,
    /// The type-erased mapping shim.
    pub handler: WebhookHandlerFn,
    /// Optional task-queue override for the dispatched workflow start. `None`
    /// defers to the target workflow's own registered default queue.
    pub queue: Option<&'static str>,
}

/// Pure validation for a set of registered webhook triggers.
///
/// Rejects duplicate binding paths (which would silently shadow each other
/// at the HTTP router level) and malformed paths. It runs at
/// `HarvestPlugin::build` time, so a mount conflict fails at process startup.
/// Otherwise it shows as a confusing 404 or wrong handler on the first live
/// request.
///
/// # Errors
///
/// Returns a human-readable message naming the offending path and the
/// trigger name(s) involved.
pub fn validate_webhook_triggers(triggers: &[WebhookTriggerInfo]) -> Result<(), String> {
    let mut seen: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
    for trigger in triggers {
        if !trigger.path.starts_with('/') || trigger.path == "/" {
            return Err(format!(
                "webhook trigger '{}' has invalid path '{}': path must start with '/' and must \
                 not be the root path",
                trigger.name, trigger.path
            ));
        }
        if let Some(first_name) = seen.insert(trigger.path, trigger.name) {
            return Err(format!(
                "duplicate webhook binding path '{}': triggers '{first_name}' and '{}' would \
                 shadow each other -- each #[webhook] path must be unique",
                trigger.path, trigger.name
            ));
        }
    }
    Ok(())
}
