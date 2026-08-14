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
    /// `SignalsWithStart` binding this is the entity key: every message sharing
    /// it is delivered to the **same execution** — affinity, *not* ordering.
    ///
    /// The connector spawns up to `max_in_flight` dispatches concurrently,
    /// including two messages for the same key in one batch, so a later broker
    /// record can persist its signal first and the workflow then observes them
    /// in database-recorded order rather than broker order. Every message still
    /// lands in exactly one run; only the sequence is unguaranteed. Set
    /// `.max_in_flight(1)` on the binding to make dispatch strictly sequential
    /// — the one knob that does buy you order, at the cost of throughput. See
    /// the ordering caveat in `docs/getting-started/13-broker-connectors.md`.
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
    /// pattern, and the per-key *affinity* path for keyed message streams.
    ///
    /// Affinity, not ordering: every message for a key lands in one execution,
    /// but concurrent dispatch means the run may observe them out of broker
    /// order unless the binding sets `.max_in_flight(1)`. See
    /// [`MappedMessage::workflow_id`].
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
    /// (issue #808). The stronger of the two: the key is derived from stable
    /// broker coordinates rather than from whatever the mapping function
    /// computed.
    ///
    /// **Scope: shard-local.** The claim this writes
    /// (`harvest_start_idempotency`) is per-shard, and the connector always
    /// dispatches with an explicit `workflow_id`, which issue #808 routes by
    /// (a keyed start routes by the *key* only when `workflow_id` was
    /// omitted). So on a **single-shard deployment — the default — dedupe is
    /// fully independent of the mapper's id**, but on a **multi-shard**
    /// deployment a redelivery whose mapper produces a *different*
    /// `workflow_id` routes to a different shard, cannot see the original
    /// claim, and starts a second execution. Keep the mapping function's id
    /// deterministic on a multi-shard deployment; the connector logs a warning
    /// at startup when it detects that combination. This is the same
    /// shard-local scope every sibling dedupe primitive carries (#808 start
    /// idempotency, #521 signal idempotency, #247 concurrency, #607 throttle,
    /// #691 mutex) — cross-shard coordination is out of scope engine-wide.
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

/// Whether this binding's dedupe promise is narrowed on this deployment.
///
/// [`IdempotencyMode::BrokerCoordinates`] advertises dedupe independent of the
/// mapper's `workflow_id`. That holds unconditionally on a single-shard
/// deployment, but the claim it writes is shard-local while the dispatch is
/// routed by the explicit `workflow_id` (issue #808), so on a multi-shard
/// deployment the promise degrades to "holds while the mapper's id is stable"
/// — i.e. to what [`IdempotencyMode::WorkflowId`] already documents.
///
/// `true` means the operator should be told; the runtime logs it once at
/// startup rather than failing, because a stable mapper (the overwhelmingly
/// common case, and what every example ships) is perfectly safe.
pub(crate) const fn coordinate_dedupe_is_shard_local_only(
    mode: IdempotencyMode,
    readable_shards: usize,
) -> bool {
    matches!(mode, IdempotencyMode::BrokerCoordinates) && readable_shards > 1
}

/// One declarative topic/queue → workflow binding.
pub struct SourceBinding {
    /// Operator-declared binding name. Used as the `source` metric label and
    /// as the idempotency-key namespace, so it must be unique and is required
    /// to be low-cardinality by construction.
    ///
    /// Because it is both, renaming it to rotate the dedupe namespace also
    /// breaks every dashboard built on the label — use
    /// [`Self::key_incarnation`] to rotate one without the other.
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
    /// Rotates the idempotency-key namespace without renaming the binding.
    ///
    /// Set this when the binding is cut over to a different cluster, or its
    /// topic is deleted and recreated: broker coordinates are only unique
    /// within one incarnation of the thing they address, so the old ones come
    /// back around while their claims are still live. See
    /// [`SourceBinding::key_incarnation`] for the full rationale.
    pub key_incarnation: Option<&'static str>,
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
            .field("key_incarnation", &self.key_incarnation)
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
            key_incarnation: None,
            mapper_configured: false,
        }
    }

    /// Rotate the idempotency-key namespace for this binding.
    ///
    /// # When you need it
    ///
    /// The connector derives its dedupe key from broker coordinates, and
    /// those are only unique within one *incarnation* of the stream they
    /// address. Kafka's `{topic}:{partition}:{offset}` restarts at zero when
    /// a topic is deleted and recreated, and means nothing at all across a
    /// cutover to a different cluster. Point an existing binding at either
    /// and the same coordinates come back around while the claims from their
    /// previous life are still live, so genuinely new records are classified
    /// as replays and acknowledged **without being dispatched**.
    ///
    /// Nothing detects this for you: Kafka exposes no topic incarnation to a
    /// consumer, and a bootstrap broker list is not a stable cluster
    /// identity. Set any value that changes when the stream underneath does —
    /// a date, a cluster name, a ticket id.
    ///
    /// # Why not just rename the binding
    ///
    /// You can — [`SourceBinding::name`] is already the key namespace. But it
    /// is also the `source` metric label, so renaming rotates the dedupe
    /// namespace and breaks every dashboard, alert and runbook built on the
    /// binding at the same time. This separates the two.
    ///
    /// # Cost of setting it
    ///
    /// Rotating deliberately invalidates the binding's live claims, so
    /// anything the broker still holds unacknowledged is dispatched again.
    /// That is the point on a genuine cutover — the records are new — but it
    /// makes this a cutover knob, not something to churn.
    #[must_use]
    pub const fn key_incarnation(mut self, incarnation: &'static str) -> Self {
        self.key_incarnation = Some(incarnation);
        self
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

    /// Use the broker's own dead-letter machinery instead of a harvest-side
    /// record.
    ///
    /// Only for sources that report a dead-letter destination *abandoning a
    /// message actually feeds* — in practice an SQS queue carrying a redrive
    /// policy, or a custom adapter that overrides
    /// [`EventSource::has_native_dead_letter`][hn]. The pairing is rejected at
    /// build time on any other source rather than silently re-reading the
    /// poison message forever.
    ///
    /// **Not available on Kafka.** Kafka has no per-message nack, so there is
    /// nothing for abandoning to hand the message to — routing a DLQ topic is a
    /// *producer* action the consumer cannot perform. A Kafka binding should
    /// leave this unset and let poison messages land in
    /// `harvest_connector_dead_letters` (the default
    /// [`DeadLetterMode::HarvestSink`]), which is also what keeps the partition
    /// moving: a dead-lettered message is acked, whereas one left to retry
    /// blocks its prefix.
    ///
    /// [hn]: super::source::EventSource::has_native_dead_letter
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

        // NOTE: a duplicate `(stream, target)` pair is deliberately NOT an
        // error here. Two independent brokers can expose the same stream name
        // and legitimately feed one workflow (active/active, or a cluster
        // migration), and nothing visible from a binding distinguishes that
        // from an accidental duplicate: a Kafka source's `stream()` is fixed
        // to the topic and the plugin separately requires the binding's stream
        // to match it, so an operator has no way to alias around a rejection.
        // The real hazard -- two bindings on one *physical* subscription
        // splitting the stream between them -- is caught in
        // `HarvestPlugin::build`, which can compare source identities; the
        // same place warns about a duplicate pair on distinct subscriptions,
        // since that double-dispatches every message.
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

        validate_target_compatibility(b, info, workflow)?;

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

/// Reject binding/target pairings whose dedupe or admission semantics do not
/// compose, at build time rather than on the first message.
fn validate_target_compatibility(
    b: &SourceBinding,
    info: &WorkflowInfo,
    workflow: &str,
) -> Result<(), String> {
    // A `SignalsWithStart` binding cannot compose with a deferred admission
    // policy, and every failure mode is silent:
    //
    // * **debounce** — `signal-with-start` refuses a *fresh* start on a
    //   debounced workflow with a `400` (it can only attach). The connector
    //   classifies that as a deterministic refusal, so the FIRST message for
    //   every entity is dead-lettered while later ones attach fine. That
    //   reads as "some messages vanish".
    // * **batch** — same shape: no signal-with-start batch admission path
    //   exists, so a fresh start is refused.
    // * **throttle** — worse, because it *succeeds*: signal-with-start does
    //   not consult the start throttle at all, so the binding would silently
    //   bypass the very backpressure the operator configured.
    if matches!(b.target, ConnectorTarget::SignalsWithStart { .. }) && has_deferred_admission(info)
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

    // A `Starts` binding onto a *batched* workflow cannot dedupe a
    // redelivery, and the duplicate is visible to the workflow.
    //
    // `resolve_idempotency_mode` falls back to `WorkflowId` for every
    // deferred-admission target, because a keyed start is a `400` there. That
    // fallback leans on workflow-id reuse — which only arbitrates when an
    // execution is *created*. Batch admission mutates a pending aggregate
    // long before that: `admit_batched_start` upserts with
    // `buffered_payloads = existing || EXCLUDED`, so a redelivered message is
    // appended a second time and the collapsed run receives it twice. It also
    // counts toward `max_size`, so it can flush the batch early.
    //
    // The other two deferred policies are genuinely safe and stay allowed:
    //
    // * **debounce** collapses on `(workflow_name, debounce_key)`, so a
    //   redelivery lands on the same pending row. Exactly one run still
    //   results; the cost is a trailing-edge deadline extension (bounded by
    //   `max_wait`) and a `pending_count` that counts admissions rather than
    //   distinct messages. Documented, not rejected.
    // * **throttle** keeps one row per admission, but each fires through
    //   `start_or_load_workflow_execution`, where workflow-id reuse collapses
    //   the duplicate onto the original run (and refunds its token).
    if matches!(b.target, ConnectorTarget::Starts { .. }) && info.batch.is_some() {
        return Err(format!(
            "connector binding '{}' starts '{workflow}', which carries a batch policy: a \
             keyed start is rejected for a deferred admission, so dedupe would fall back to \
             workflow-id reuse -- and that only arbitrates once an execution is created. A \
             broker redelivery would append the same message to the pending batch twice \
             (counting toward max_size, so it can flush early), and the collapsed run would \
             see one message duplicated. Remove the batch policy from '{workflow}', or map \
             this binding to an unbatched workflow that starts the batched one itself",
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
            declared_activities: None,
            declared_children: None,
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
    fn validate_allows_independent_sources_to_feed_one_stream_target_pair() {
        // Two independent Kafka clusters both exposing `orders` and both
        // feeding `order_flow` is legitimate fan-in (active/active, or a
        // cluster migration). Nothing in the binding can distinguish them:
        // `KafkaSource::stream()` is fixed to the topic and the plugin
        // separately asserts the binding's stream matches it, so an operator
        // cannot alias either one to get past a logical duplicate check.
        //
        // The genuine hazard -- two consumers on ONE physical subscription
        // splitting the stream -- is caught by the source-identity check in
        // `HarvestPlugin::build`, which can see what this cannot.
        let bindings = vec![
            SourceBinding::starts("a", "orders", "order_flow").map_raw(|_| Ok(ok_mapped())),
            SourceBinding::starts("b", "orders", "order_flow").map_raw(|_| Ok(ok_mapped())),
        ];
        assert!(
            validate_bindings(&bindings, &[wf("order_flow")], &[]).is_ok(),
            "a logical stream+target pair is not evidence of a physical clash"
        );
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
    fn known_limitation_coordinate_dedupe_scope_is_shard_local() {
        // Issue #944 / Codex P1. `BrokerCoordinates` derives the dedupe key
        // from stable broker coordinates, but the CLAIM it writes
        // (`harvest_start_idempotency`) is shard-local, and the connector
        // always dispatches with an explicit `workflow_id`, so issue #808
        // routes the start by `workflow_id` rather than by the key. On a
        // MULTI-SHARD deployment a mapper whose id drifts therefore lands the
        // redelivery on a different shard, where the original claim is not
        // visible -- so it starts a second execution.
        //
        // This pins the honest scope: the dedupe promise holds
        // unconditionally on a single-shard deployment (the default), and on a
        // multi-shard one only while the mapper's id is stable. It is the same
        // shard-local scope every sibling dedupe primitive carries (#808 start
        // idempotency, #521 signal idempotency, #247 concurrency, #607
        // throttle, #691 mutex).
        assert!(
            !coordinate_dedupe_is_shard_local_only(IdempotencyMode::BrokerCoordinates, 1),
            "a single-shard deployment (the default) carries the full promise",
        );
        assert!(
            coordinate_dedupe_is_shard_local_only(IdempotencyMode::BrokerCoordinates, 4),
            "a multi-shard deployment narrows the promise to a stable mapper id",
        );
        // `WorkflowId` mode never made the stronger promise, so a multi-shard
        // deployment does not narrow anything for it -- warning there would be
        // pure noise.
        assert!(
            !coordinate_dedupe_is_shard_local_only(IdempotencyMode::WorkflowId, 4),
            "WorkflowId mode already documents that it relies on a stable id",
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

    fn batch_policy() -> autumn_harvest::event_batch::BatchPolicy {
        autumn_harvest::event_batch::BatchPolicy {
            key_expr: "tenant_id".to_string(),
            max_size: 10,
            max_wait: std::time::Duration::from_secs(30),
        }
    }

    fn debounce_policy() -> autumn_harvest::debounce::DebouncePolicy {
        autumn_harvest::debounce::DebouncePolicy {
            key_expr: "tenant_id",
            window: std::time::Duration::from_secs(30),
            max_wait: None,
        }
    }

    #[test]
    fn validate_rejects_a_starts_binding_onto_a_batched_workflow() {
        // A batch target is the one deferred-admission policy where a broker
        // redelivery is *visible to the workflow*: the redelivery appends the
        // same payload to `buffered_payloads` a second time, so the collapsed
        // run receives one message twice — and it counts toward `max_size`,
        // so it can flush the batch early too. Workflow-id reuse cannot save
        // it, because no execution exists yet when the append happens.
        let batched = wf("wf").with_batch(batch_policy());
        let bindings =
            vec![SourceBinding::starts("orders", "orders", "wf").map_raw(|_| Ok(ok_mapped()))];
        let err = validate_bindings(&bindings, &[batched], &[]).unwrap_err();
        assert!(err.contains("batch"), "{err}");
        assert!(
            err.contains("twice") || err.contains("duplicate"),
            "the error must say what actually goes wrong: {err}"
        );
    }

    #[test]
    fn a_starts_binding_onto_a_debounced_workflow_is_allowed() {
        // Debounce collapses on `(workflow_name, debounce_key)`, so a
        // redelivery lands on the SAME pending row and still yields exactly
        // one run. It costs a bounded deadline extension, not a duplicate.
        let debounced = wf("wf").with_debounce(debounce_policy());
        let bindings =
            vec![SourceBinding::starts("orders", "orders", "wf").map_raw(|_| Ok(ok_mapped()))];
        assert!(validate_bindings(&bindings, &[debounced], &[]).is_ok());
    }

    #[test]
    fn a_starts_binding_onto_a_throttled_workflow_is_allowed() {
        // Throttle keeps one pending row per admission, but every row fires
        // through `start_or_load_workflow_execution`, where workflow-id reuse
        // collapses the redelivery onto the original run.
        let throttled = wf("wf").with_throttle(
            autumn_harvest::throttle::ThrottlePolicy::from_rate_str("100/m", None, None, None)
                .expect("valid rate"),
        );
        let bindings =
            vec![SourceBinding::starts("orders", "orders", "wf").map_raw(|_| Ok(ok_mapped()))];
        assert!(validate_bindings(&bindings, &[throttled], &[]).is_ok());
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
