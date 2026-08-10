//! Declarative source→workflow binding descriptors (issue #944).
//!
//! A [`SourceBinding`] is the broker-agnostic analogue of
//! [`autumn_harvest::WebhookTriggerInfo`] (issue #344): it maps one
//! topic/queue onto one target workflow, with a synchronous mapping function
//! that turns raw message bytes plus broker metadata into a
//! [`autumn_harvest::WorkflowId`] and a JSON payload.
//!
//! The descriptor is deliberately builder-shaped rather than macro-shaped —
//! a binding needs runtime configuration (broker coordinates, poison
//! threshold, concurrency) that an attribute macro cannot express.

use autumn_harvest::WorkflowId;
use autumn_harvest::info::WorkflowInfo;
use std::collections::HashMap;
use std::sync::Arc;

use super::disposition::DeadLetterMode;
use super::message::MessageCtx;

/// Default consecutive mapping-rejection strikes before a message is
/// dead-lettered, mirroring `WorkerConfig::poison_pill_threshold` (issue
/// #367).
pub const DEFAULT_POISON_THRESHOLD: u32 = 3;

/// Default bound on messages dispatched concurrently by one binding.
///
/// Bounds the connector's own fan-in so a topic backlog cannot stampede the
/// admission path (the per-workflow throttle, issue #607, bounds the *start*
/// rate; this bounds the *dispatch* rate).
pub const DEFAULT_MAX_IN_FLIGHT: usize = 16;

/// What a mapping function can fail with.
///
/// Mirrors [`autumn_harvest::WebhookHandlerError`]'s two-way classification
/// exactly, because the two carry different retry semantics: a decode failure
/// is deterministic and dead-letters on sight, while a rejection is
/// strike-counted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MappingError {
    /// The raw bytes could not be decoded into the expected shape.
    Deserialize(String),
    /// The mapping function ran but declined to map this message.
    Rejected(String),
}

impl std::fmt::Display for MappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deserialize(m) => write!(f, "message deserialize failed: {m}"),
            Self::Rejected(m) => write!(f, "mapping function rejected message: {m}"),
        }
    }
}

impl std::error::Error for MappingError {}

/// What a mapping function produces for a successfully-mapped message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedMessage {
    /// The business workflow id this message belongs to. For a
    /// `SignalsWithStart` binding this is the entity key, and messages sharing
    /// it are delivered to the same execution in order (see the ordering
    /// caveat in `docs/getting-started/13-broker-connectors.md`).
    pub workflow_id: WorkflowId,
    /// The JSON payload handed to the workflow as its start input (and, for a
    /// `SignalsWithStart` binding, as the signal payload).
    pub payload: serde_json::Value,
}

impl MappedMessage {
    /// The mapping function's return value: which workflow id this message
    /// belongs to, and the JSON payload to hand the workflow.
    ///
    /// ```
    /// # use autumn_harvest_plugin::connector::MappedMessage;
    /// let m = MappedMessage::new("order-A-1001", serde_json::json!({"total": 4999}));
    /// assert_eq!(m.workflow_id.as_str(), "order-A-1001");
    /// ```
    #[must_use]
    pub fn new(workflow_id: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            workflow_id: WorkflowId::new(workflow_id),
            payload,
        }
    }
}

/// The type-erased mapping function stored on a binding.
pub type MessageMapper =
    Arc<dyn Fn(&MessageCtx) -> Result<MappedMessage, MappingError> + Send + Sync>;

/// What a binding does with a mapped message.
///
/// Deliberately mirrors [`autumn_harvest::WebhookTarget`] so both inbound
/// paths carry identical semantics; the connector reuses the very same
/// `signal_with_start_workflow_execution` primitive (issue #244).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorTarget {
    /// Start the workflow, deduping so a redelivery attaches instead of
    /// creating a second run.
    Starts {
        /// The registered workflow type name.
        workflow: &'static str,
    },
    /// Atomically start-or-attach and deliver a signal — the entity-workflow
    /// pattern, and the *ordered* path for per-key message streams.
    SignalsWithStart {
        /// The registered workflow type name.
        workflow: &'static str,
        /// The signal delivered on every message.
        signal_name: &'static str,
    },
}

impl ConnectorTarget {
    /// The target workflow type name.
    #[must_use]
    pub const fn workflow(&self) -> &'static str {
        match self {
            Self::Starts { workflow } | Self::SignalsWithStart { workflow, .. } => workflow,
        }
    }

    /// The signal name, for a `SignalsWithStart` target.
    #[must_use]
    pub const fn signal_name(&self) -> Option<&'static str> {
        match self {
            Self::Starts { .. } => None,
            Self::SignalsWithStart { signal_name, .. } => Some(signal_name),
        }
    }
}

/// How a `Starts` binding deduplicates redeliveries.
///
/// `SignalsWithStart` bindings always dedupe on the derived broker-coordinate
/// key (it is the only mechanism `signal_with_start` offers), so this only
/// applies to `Starts`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyMode {
    /// Pass the derived broker-coordinate key as the start's idempotency key
    /// (issue #808). Strongest: dedupe is independent of whatever
    /// `workflow_id` the mapping function chose.
    ///
    /// **Mutually exclusive** with a target carrying a throttle, debounce or
    /// batch policy — the start route rejects that combination with `400`,
    /// because a deferred admission has no synchronous execution to dedupe
    /// against.
    BrokerCoordinates,
    /// Dedupe on the mapping function's deterministic `workflow_id` (the
    /// inbound-webhook model, issue #344). Slightly weaker — it relies on the
    /// mapping function deriving a stable id — but it composes with
    /// throttle/debounce/batch admission.
    WorkflowId,
}

/// One declarative topic/queue → workflow binding.
pub struct SourceBinding {
    /// Operator-declared binding name. Used as the `source` metric label and
    /// as the idempotency-key namespace, so it must be unique and is required
    /// to be low-cardinality by construction.
    pub name: &'static str,
    /// The logical stream (topic or queue) this binding consumes.
    pub stream: String,
    /// What to do with a mapped message.
    pub target: ConnectorTarget,
    /// Raw bytes + metadata → workflow id + payload.
    pub mapper: MessageMapper,
    /// Task queue override; `None` uses the target workflow's default.
    pub queue: Option<&'static str>,
    /// Consecutive mapping-rejection strikes before dead-lettering.
    pub poison_threshold: u32,
    /// Bound on concurrently-dispatched messages for this binding.
    pub max_in_flight: usize,
    /// Where poison messages go.
    pub dead_letter_mode: DeadLetterMode,
    /// Explicit dedupe mode for a `Starts` target; `None` resolves
    /// automatically (see [`resolve_idempotency_mode`]).
    pub idempotency_mode: Option<IdempotencyMode>,
    /// Set by `map_raw`/`map_json`. Private so `validate_bindings` can tell
    /// "never configured a mapper" from "configured one that rejects", and so
    /// a binding can only be built through the constructors.
    mapper_configured: bool,
}

impl std::fmt::Debug for SourceBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceBinding")
            .field("name", &self.name)
            .field("stream", &self.stream)
            .field("target", &self.target)
            .field("queue", &self.queue)
            .field("poison_threshold", &self.poison_threshold)
            .field("max_in_flight", &self.max_in_flight)
            .field("dead_letter_mode", &self.dead_letter_mode)
            .field("idempotency_mode", &self.idempotency_mode)
            .finish_non_exhaustive()
    }
}

impl SourceBinding {
    /// Bind `stream` to a workflow *start*.
    #[must_use]
    pub fn starts(name: &'static str, stream: impl Into<String>, workflow: &'static str) -> Self {
        Self::new(name, stream, ConnectorTarget::Starts { workflow })
    }

    /// Bind `stream` to an atomic start-or-attach + signal (issue #244).
    #[must_use]
    pub fn signals_with_start(
        name: &'static str,
        stream: impl Into<String>,
        workflow: &'static str,
        signal_name: &'static str,
    ) -> Self {
        Self::new(
            name,
            stream,
            ConnectorTarget::SignalsWithStart {
                workflow,
                signal_name,
            },
        )
    }

    fn new(name: &'static str, stream: impl Into<String>, target: ConnectorTarget) -> Self {
        Self {
            name,
            stream: stream.into(),
            target,
            // Default mapper: reject everything, with a message naming the fix.
            // Replaced by `map_json`/`map_raw`; `validate_bindings` rejects a
            // binding that never set one.
            mapper: Arc::new(|_| {
                Err(MappingError::Rejected(
                    "no mapping function configured for this binding".to_string(),
                ))
            }),
            queue: None,
            poison_threshold: DEFAULT_POISON_THRESHOLD,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            dead_letter_mode: DeadLetterMode::HarvestSink,
            idempotency_mode: None,
            mapper_configured: false,
        }
    }

    /// Map raw message bytes directly — the primary contract (issue #944
    /// scopes schema-registry decoding out; typed decoding is the mapping
    /// function's concern).
    #[must_use]
    pub fn map_raw<F>(mut self, f: F) -> Self
    where
        F: Fn(&MessageCtx) -> Result<MappedMessage, MappingError> + Send + Sync + 'static,
    {
        self.mapper = Arc::new(f);
        self.mapper_configured = true;
        self
    }

    /// Convenience: decode the body as JSON into `T`, then map.
    ///
    /// A decode failure classifies as [`MappingError::Deserialize`] and a
    /// mapping-function error as [`MappingError::Rejected`], mirroring the
    /// `#[webhook]` macro's dispatch shim exactly.
    #[must_use]
    pub fn map_json<T, E, F>(self, f: F) -> Self
    where
        T: serde::de::DeserializeOwned,
        E: std::fmt::Display,
        F: Fn(&MessageCtx, T) -> Result<MappedMessage, E> + Send + Sync + 'static,
    {
        self.map_raw(move |ctx| {
            let typed: T = serde_json::from_slice(&ctx.raw_body)
                .map_err(|e| MappingError::Deserialize(e.to_string()))?;
            f(ctx, typed).map_err(|e| MappingError::Rejected(e.to_string()))
        })
    }

    /// Route dispatched work to a specific task queue.
    #[must_use]
    pub const fn queue(mut self, queue: &'static str) -> Self {
        self.queue = Some(queue);
        self
    }

    /// Consecutive mapping-rejection strikes before dead-lettering (`0`
    /// disables strike-based quarantine; deterministic failures still
    /// dead-letter).
    #[must_use]
    pub const fn poison_threshold(mut self, threshold: u32) -> Self {
        self.poison_threshold = threshold;
        self
    }

    /// Bound on concurrently-dispatched messages for this binding.
    #[must_use]
    pub const fn max_in_flight(mut self, max: usize) -> Self {
        self.max_in_flight = max;
        self
    }

    /// Use the broker's own dead-letter machinery (SQS redrive, a Kafka DLQ
    /// topic) instead of a harvest-side record.
    #[must_use]
    pub const fn broker_native_dead_letter(mut self) -> Self {
        self.dead_letter_mode = DeadLetterMode::BrokerNative;
        self
    }

    /// Force a dedupe mode for a `Starts` target instead of resolving it from
    /// the target workflow's admission policies.
    #[must_use]
    pub const fn idempotency_mode(mut self, mode: IdempotencyMode) -> Self {
        self.idempotency_mode = Some(mode);
        self
    }
}

// Private field, set by `map_raw`/`map_json`, so `validate_bindings` can tell
// "never configured a mapper" from "configured one that happens to reject".
impl SourceBinding {
    pub(crate) const fn has_mapper(&self) -> bool {
        self.mapper_configured
    }
}

/// Resolve the effective dedupe mode for a binding.
///
/// * An explicit [`SourceBinding::idempotency_mode`] always wins.
/// * Otherwise [`IdempotencyMode::BrokerCoordinates`] is preferred — it is the
///   strongest guarantee and does not depend on the mapping function's id
///   choice — **unless** the target workflow carries a throttle, debounce or
///   batch policy, in which case a keyed start would be rejected `400` by the
///   start route's mutual-exclusion rule. Then it falls back to
///   [`IdempotencyMode::WorkflowId`], which composes with deferred admission.
///
/// `SignalsWithStart` targets always resolve to
/// [`IdempotencyMode::BrokerCoordinates`]: the signal path's key is a body
/// field with no such mutual exclusion.
#[must_use]
pub fn resolve_idempotency_mode(
    binding_target: ConnectorTarget,
    configured: Option<IdempotencyMode>,
    info: Option<&WorkflowInfo>,
) -> IdempotencyMode {
    if matches!(binding_target, ConnectorTarget::SignalsWithStart { .. }) {
        return IdempotencyMode::BrokerCoordinates;
    }
    if let Some(mode) = configured {
        return mode;
    }
    if info.is_some_and(has_deferred_admission) {
        IdempotencyMode::WorkflowId
    } else {
        IdempotencyMode::BrokerCoordinates
    }
}

/// Whether a workflow's admission is deferred (throttle #607, debounce #499,
/// or batch #518), which makes a keyed start a `400`.
#[must_use]
pub const fn has_deferred_admission(info: &WorkflowInfo) -> bool {
    info.throttle.is_some() || info.debounce.is_some() || info.batch.is_some()
}

/// Validate a binding set at build time.
///
/// Fails fast — like `validate_webhook_triggers` (issue #344) — so a
/// misconfiguration surfaces at `HarvestPlugin::build` rather than on the
/// first message, when the message would otherwise be silently retried
/// forever.
///
/// # Errors
///
/// Returns a human-readable message when a binding has an empty name, an
/// empty stream, no mapping function, a duplicate name, a duplicate
/// `(stream, target)` pair, a target that is not a registered workflow, a
/// target that is a registered DAG, or an explicit
/// [`IdempotencyMode::BrokerCoordinates`] on a workflow whose admission is
/// deferred.
pub fn validate_bindings(
    bindings: &[SourceBinding],
    registered_workflows: &[WorkflowInfo],
    registered_dag_names: &[String],
) -> Result<(), String> {
    let refs: Vec<&SourceBinding> = bindings.iter().collect();
    validate_bindings_refs(&refs, registered_workflows, registered_dag_names)
}

/// [`validate_bindings`] over borrowed bindings.
///
/// The plugin stores each registration behind an `Arc` (its mapping function
/// is not `Clone`), so it cannot hand over a `&[SourceBinding]`.
///
/// # Errors
///
/// Same as [`validate_bindings`].
pub fn validate_bindings_refs(
    bindings: &[&SourceBinding],
    registered_workflows: &[WorkflowInfo],
    registered_dag_names: &[String],
) -> Result<(), String> {
    let by_name: HashMap<&str, &WorkflowInfo> = registered_workflows
        .iter()
        .map(|w| (w.name, w))
        .collect::<HashMap<_, _>>();

    let mut seen_names: std::collections::HashSet<&str> = std::collections::HashSet::new();
    let mut seen_stream_targets: HashMap<(String, &'static str, Option<&'static str>), &str> =
        HashMap::new();

    for b in bindings {
        if b.name.trim().is_empty() {
            return Err(
                "connector binding has an empty name: every binding needs a unique, \
                        non-empty name (it is the metrics `source` label and the \
                        idempotency-key namespace)"
                    .to_string(),
            );
        }
        if b.stream.trim().is_empty() {
            return Err(format!(
                "connector binding '{}' has an empty stream: name the topic or queue to consume",
                b.name
            ));
        }
        if !b.has_mapper() {
            return Err(format!(
                "connector binding '{}' has no mapping function: add `.map_json(...)` or \
                 `.map_raw(...)`",
                b.name
            ));
        }
        if !seen_names.insert(b.name) {
            return Err(format!(
                "duplicate connector binding name '{}': names must be unique -- they namespace \
                 the derived idempotency key, so two bindings sharing a name would silently \
                 deduplicate each other's messages",
                b.name
            ));
        }

        let key = (
            b.stream.clone(),
            b.target.workflow(),
            b.target.signal_name(),
        );
        if let Some(first) = seen_stream_targets.insert(key, b.name) {
            return Err(format!(
                "connector bindings '{first}' and '{}' both consume stream '{}' into the same \
                 target: consolidate them, or give one a different target",
                b.name, b.stream
            ));
        }

        let workflow = b.target.workflow();
        if registered_dag_names.iter().any(|d| d == workflow) {
            return Err(format!(
                "connector binding '{}' targets '{workflow}', which is a registered DAG: \
                 trigger DAGs via POST /dags/{workflow}/trigger, not a broker binding",
                b.name
            ));
        }
        let Some(info) = by_name.get(workflow) else {
            return Err(format!(
                "connector binding '{}' targets unregistered workflow '{workflow}': register it \
                 via .workflows(workflows![...]) before .connector(...)",
                b.name
            ));
        };

        // A `SignalsWithStart` binding cannot compose with a deferred
        // admission policy, and every failure mode is silent:
        //
        // * **debounce** — `signal-with-start` refuses a *fresh* start on a
        //   debounced workflow with a `400` (it can only attach). The
        //   connector classifies that as a deterministic refusal, so the
        //   FIRST message for every entity is dead-lettered while later ones
        //   attach fine. That reads as "some messages vanish".
        // * **batch** — same shape: no signal-with-start batch admission path
        //   exists, so a fresh start is refused.
        // * **throttle** — worse, because it *succeeds*: signal-with-start
        //   does not consult the start throttle at all, so the binding would
        //   silently bypass the very backpressure the operator configured.
        //
        // Fail at build time rather than let any of those reach production.
        if matches!(b.target, ConnectorTarget::SignalsWithStart { .. })
            && has_deferred_admission(info)
        {
            return Err(format!(
                "connector binding '{}' is a signals_with_start binding, but target workflow \
                 '{workflow}' carries a throttle/debounce/batch policy. signal-with-start \
                 refuses a fresh start on a debounced/batched workflow (so the first message \
                 for each entity would be dead-lettered) and bypasses a throttle entirely. \
                 Use a `starts` binding, or remove the deferred-admission policy",
                b.name
            ));
        }

        if b.idempotency_mode == Some(IdempotencyMode::BrokerCoordinates)
            && has_deferred_admission(info)
        {
            return Err(format!(
                "connector binding '{}' sets IdempotencyMode::BrokerCoordinates but target \
                 workflow '{workflow}' carries a throttle/debounce/batch policy: a keyed start \
                 is rejected for a deferred admission. Drop the explicit mode (it will resolve \
                 to WorkflowId automatically) or remove the deferred-admission policy",
                b.name
            ));
        }

        // A `broker_native_dead_letter()` binding hands poison quarantine to
        // the broker's own redrive policy — which only works if the adapter
        // has a real per-message nack that increments a receive count. Kafka
        // has none (`abandon` is a no-op; the message simply is not
        // committed), so a poison message would be re-read forever and never
        // reach any dead-letter destination: the exact partition wedge the
        // feature exists to prevent. The binding cannot see its own adapter
        // here, so this is caught adapter-side in `spawn_connectors`; what we
        // can check is that the mode was set deliberately.
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use autumn_harvest::WorkflowId;

    fn ok_mapped() -> MappedMessage {
        MappedMessage {
            workflow_id: WorkflowId::new("w-1"),
            payload: serde_json::json!({"ok": true}),
        }
    }

    fn ctx(body: &[u8]) -> MessageCtx {
        MessageCtx {
            binding: "orders",
            coordinates: super::super::message::MessageCoordinates::Opaque {
                stream: "orders".to_string(),
                id: "1".to_string(),
            },
            key: None,
            headers: std::collections::BTreeMap::new(),
            raw_body: body.to_vec(),
        }
    }

    fn wf(name: &'static str) -> WorkflowInfo {
        WorkflowInfo {
            name,
            module: "tests",
            handler: |_ctx, input| Box::pin(async move { Ok(input) }),
            execution_timeout: None,
            chain_execution_timeout: None,
            sla: None,
            concurrency: None,
            debounce: None,
            batch: None,
            throttle: None,
            max_input_bytes: None,
            owner: None,
            runbook_url: None,
            severity: None,
            description: None,
            input_schema: None,
            output_schema: None,
            error_schema: None,
            retry_policy: None,
            mcp: false,
        }
    }

    #[test]
    fn starts_and_signals_targets_expose_workflow_and_signal() {
        let s = ConnectorTarget::Starts { workflow: "wf" };
        assert_eq!(s.workflow(), "wf");
        assert_eq!(s.signal_name(), None);

        let sw = ConnectorTarget::SignalsWithStart {
            workflow: "wf",
            signal_name: "evt",
        };
        assert_eq!(sw.workflow(), "wf");
        assert_eq!(sw.signal_name(), Some("evt"));
    }

    #[test]
    fn map_json_classifies_decode_and_rejection_distinctly() {
        #[derive(serde::Deserialize)]
        struct Order {
            id: String,
        }
        let b =
            SourceBinding::starts("orders", "orders", "order_flow").map_json(|_ctx, o: Order| {
                if o.id.is_empty() {
                    return Err("empty id");
                }
                Ok(MappedMessage {
                    workflow_id: WorkflowId::new(format!("order-{}", o.id)),
                    payload: serde_json::json!({"id": o.id}),
                })
            });

        // Bad JSON -> Deserialize (dead-letters immediately).
        let err = (b.mapper)(&ctx(b"not json")).unwrap_err();
        assert!(matches!(err, MappingError::Deserialize(_)), "{err:?}");

        // Valid JSON the mapper declines -> Rejected (strike-counted).
        let err = (b.mapper)(&ctx(br#"{"id":""}"#)).unwrap_err();
        assert!(matches!(err, MappingError::Rejected(_)), "{err:?}");

        // Happy path.
        let m = (b.mapper)(&ctx(br#"{"id":"7"}"#)).unwrap();
        assert_eq!(m.workflow_id.as_str(), "order-7");
    }

    #[test]
    fn map_raw_receives_the_raw_bytes() {
        let b = SourceBinding::starts("raw", "raw", "wf").map_raw(|c| {
            Ok(MappedMessage {
                workflow_id: WorkflowId::new(format!("len-{}", c.raw_body.len())),
                payload: serde_json::json!({}),
            })
        });
        let m = (b.mapper)(&ctx(b"\x00\x01\x02")).unwrap();
        assert_eq!(m.workflow_id.as_str(), "len-3");
    }

    #[test]
    fn builder_defaults_mirror_poison_pill_and_bound_concurrency() {
        let b = SourceBinding::starts("orders", "orders", "wf");
        assert_eq!(b.poison_threshold, DEFAULT_POISON_THRESHOLD);
        assert_eq!(b.poison_threshold, 3, "mirrors poison_pill_threshold");
        assert_eq!(b.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert_eq!(b.dead_letter_mode, DeadLetterMode::HarvestSink);
        assert_eq!(b.idempotency_mode, None);
        assert_eq!(b.queue, None);
    }

    #[test]
    fn builder_overrides_apply() {
        let b = SourceBinding::starts("orders", "orders", "wf")
            .map_raw(|_| Ok(ok_mapped()))
            .queue("ingest")
            .poison_threshold(7)
            .max_in_flight(2)
            .broker_native_dead_letter()
            .idempotency_mode(IdempotencyMode::WorkflowId);
        assert_eq!(b.queue, Some("ingest"));
        assert_eq!(b.poison_threshold, 7);
        assert_eq!(b.max_in_flight, 2);
        assert_eq!(b.dead_letter_mode, DeadLetterMode::BrokerNative);
        assert_eq!(b.idempotency_mode, Some(IdempotencyMode::WorkflowId));
    }

    #[test]
    fn validate_accepts_a_well_formed_binding() {
        let bindings = vec![
            SourceBinding::starts("orders", "orders", "order_flow").map_raw(|_| Ok(ok_mapped())),
        ];
        assert!(validate_bindings(&bindings, &[wf("order_flow")], &[]).is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_binding_names() {
        let bindings = vec![
            SourceBinding::starts("orders", "a", "order_flow").map_raw(|_| Ok(ok_mapped())),
            SourceBinding::starts("orders", "b", "order_flow").map_raw(|_| Ok(ok_mapped())),
        ];
        let err = validate_bindings(&bindings, &[wf("order_flow")], &[]).unwrap_err();
        assert!(err.contains("duplicate connector binding name"), "{err}");
    }

    #[test]
    fn validate_rejects_duplicate_stream_target_pairs() {
        let bindings = vec![
            SourceBinding::starts("a", "orders", "order_flow").map_raw(|_| Ok(ok_mapped())),
            SourceBinding::starts("b", "orders", "order_flow").map_raw(|_| Ok(ok_mapped())),
        ];
        let err = validate_bindings(&bindings, &[wf("order_flow")], &[]).unwrap_err();
        assert!(err.contains("both consume stream"), "{err}");
    }

    #[test]
    fn validate_rejects_unregistered_workflow() {
        let bindings =
            vec![SourceBinding::starts("orders", "orders", "nope").map_raw(|_| Ok(ok_mapped()))];
        let err = validate_bindings(&bindings, &[wf("order_flow")], &[]).unwrap_err();
        assert!(err.contains("unregistered workflow 'nope'"), "{err}");
        assert!(err.contains(".workflows(workflows![...])"), "{err}");
    }

    #[test]
    fn validate_rejects_a_dag_target() {
        let bindings =
            vec![SourceBinding::starts("etl", "etl", "nightly_etl").map_raw(|_| Ok(ok_mapped()))];
        let err = validate_bindings(
            &bindings,
            &[wf("nightly_etl")],
            &["nightly_etl".to_string()],
        )
        .unwrap_err();
        assert!(err.contains("is a registered DAG"), "{err}");
    }

    #[test]
    fn validate_rejects_a_binding_with_no_mapper() {
        let bindings = vec![SourceBinding::starts("orders", "orders", "order_flow")];
        let err = validate_bindings(&bindings, &[wf("order_flow")], &[]).unwrap_err();
        assert!(err.contains("no mapping function"), "{err}");
    }

    #[test]
    fn validate_rejects_empty_name_or_stream() {
        let b = vec![SourceBinding::starts("", "orders", "wf").map_raw(|_| Ok(ok_mapped()))];
        assert!(
            validate_bindings(&b, &[wf("wf")], &[])
                .unwrap_err()
                .contains("empty name")
        );

        let b = vec![SourceBinding::starts("n", "  ", "wf").map_raw(|_| Ok(ok_mapped()))];
        assert!(
            validate_bindings(&b, &[wf("wf")], &[])
                .unwrap_err()
                .contains("empty stream")
        );
    }

    #[test]
    fn signals_with_start_always_uses_broker_coordinates() {
        let target = ConnectorTarget::SignalsWithStart {
            workflow: "wf",
            signal_name: "evt",
        };
        // Even an explicit WorkflowId request is overridden: the signal path
        // has no other dedupe mechanism.
        assert_eq!(
            resolve_idempotency_mode(target, Some(IdempotencyMode::WorkflowId), None),
            IdempotencyMode::BrokerCoordinates,
        );
    }

    #[test]
    fn starts_defaults_to_broker_coordinates_without_deferred_admission() {
        let target = ConnectorTarget::Starts { workflow: "wf" };
        assert_eq!(
            resolve_idempotency_mode(target, None, Some(&wf("wf"))),
            IdempotencyMode::BrokerCoordinates,
        );
        assert_eq!(
            resolve_idempotency_mode(target, None, None),
            IdempotencyMode::BrokerCoordinates,
        );
    }

    #[test]
    fn starts_falls_back_to_workflow_id_under_a_throttle_policy() {
        // A keyed start against a throttled workflow is a 400; falling back
        // keeps AC "throttle composes" and AC "redelivery dedupes" both true.
        let throttled = wf("wf").with_throttle(
            autumn_harvest::throttle::ThrottlePolicy::from_rate_str("100/m", None, None, None)
                .expect("valid rate"),
        );
        let target = ConnectorTarget::Starts { workflow: "wf" };
        assert_eq!(
            resolve_idempotency_mode(target, None, Some(&throttled)),
            IdempotencyMode::WorkflowId,
        );
    }

    #[test]
    fn explicit_mode_wins_for_starts() {
        let target = ConnectorTarget::Starts { workflow: "wf" };
        assert_eq!(
            resolve_idempotency_mode(target, Some(IdempotencyMode::WorkflowId), Some(&wf("wf"))),
            IdempotencyMode::WorkflowId,
        );
    }

    #[test]
    fn validate_rejects_explicit_broker_coordinates_on_a_throttled_target() {
        let throttled = wf("wf").with_throttle(
            autumn_harvest::throttle::ThrottlePolicy::from_rate_str("100/m", None, None, None)
                .expect("valid rate"),
        );
        let bindings = vec![
            SourceBinding::starts("orders", "orders", "wf")
                .map_raw(|_| Ok(ok_mapped()))
                .idempotency_mode(IdempotencyMode::BrokerCoordinates),
        ];
        let err = validate_bindings(&bindings, &[throttled], &[]).unwrap_err();
        assert!(err.contains("throttle/debounce/batch"), "{err}");
    }

    #[test]
    fn mapping_error_displays_distinctly() {
        assert!(
            MappingError::Deserialize("bad".to_string())
                .to_string()
                .contains("deserialize failed")
        );
        assert!(
            MappingError::Rejected("nope".to_string())
                .to_string()
                .contains("rejected message")
        );
    }
}
