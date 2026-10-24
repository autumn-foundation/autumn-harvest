//! The activity bodies: model calls, tool calls, and the always-on hooks.
//!
//! [`AgentHarness`] holds the parts that do real work: the
//! [`AgentModel`], the [`Tool`]s, the [`ToolPolicy`], the memory store, the
//! delivery and the precheck. The engine runs its methods as activities, so
//! each result is recorded once and replayed after that.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use tracing::Instrument;

use crate::agent::AgentProfile;
use crate::bounds::{exceeds_bytes, json_len};
use crate::delivery::{Delivery, LogDelivery, Report};
use crate::error::{AgentError, ErrorKind};
use crate::flow::{FlowRules, Released, call_labels};
use crate::heartbeat::{HeartbeatTask, Precheck};
use crate::live::{Coalescer, LiveEvent, LiveSink};
use crate::memory::{MEMORY_TOOL, MemoryScope, MemorySnapshot, MemoryStore, MemoryTool};
use crate::mesh::{DELEGATE_TOOL, Peer, delegate_definition, valid_name};
use crate::mesh_control::{MESH_BUDGET_EXCEEDED, MeshBudget, MeshBudgetExceeded, MeshSpend};
use crate::message::{ChatMessage, ContentPart, RunId, TokenUsage, ToolDefinition};
use crate::model::{AgentModel, BoxFuture, ChatRequest, ChatResponse, UNKNOWN_MODEL_ID};
use crate::policy::{AllowAll, RunInfo, Strictest, ToolDecision, ToolPolicy, ToolRules};
use crate::response_cache::{CacheKey, KeyTenant, ResponseCache};
use crate::tool::{Tool, ToolContext, ToolEffect};
use autumn_harvest::builder::DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
use autumn_harvest::failure::{ActivityFailure, IntoActivityErrorString};

use crate::transcript::{InMemoryTranscriptStore, PREFIX_MISSING, TranscriptStore, chain};
use crate::types::{ModelTurn, ModelTurnRequest, ToolCallRequest, ToolOutcome};
use crate::workspace::{WorkspaceHandle, WorkspaceMount, WorkspaceRef, WorkspaceStore};

/// The default cap on one tool result, in characters.
pub const DEFAULT_TOOL_OUTPUT_LIMIT: usize = 8_000;

/// The name of the model that [`AgentHarness::new`] takes (issue #2271).
///
/// A task, a peer or a profile with no model name uses it. A mandate
/// allowlist names it as any other model.
pub const DEFAULT_MODEL: &str = "default";

/// The default time budget of one model call: 13 minutes.
///
/// With [`DEFAULT_POLICY_TIMEOUT`], three [`DEFAULT_CACHE_TIMEOUT`] budgets
/// and one [`DEFAULT_BUDGET_TIMEOUT`], it stays below the 15-minute `start_to_close` of
/// `agent_model_turn`. A slow call therefore ends as a
/// retryable failure that the harness reports. It does not end as an engine
/// timeout, which SQLite reports only after the body returns.
pub const DEFAULT_MODEL_TIMEOUT: Duration = Duration::from_secs(13 * 60);

/// The default time budget of one tool call: 9 minutes.
///
/// It is below the 10-minute `start_to_close` of `agent_tool_call`. A slow
/// tool therefore gives the model an error result, and the run goes on.
pub const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(9 * 60);

/// The default time budget of the policy for all tool calls of one turn: one
/// minute.
///
/// A policy can wait on I/O. Without a budget, a stalled policy would hang
/// the model turn, and SQLite cannot end the turn from outside.
pub const DEFAULT_POLICY_TIMEOUT: Duration = Duration::from_secs(60);

/// The default time budget of one memory read, delivery or precheck: 30
/// seconds.
///
/// It is below the one-minute `start_to_close` of the shortest of those
/// activities. SQLite cannot end a stalled call from outside.
pub const DEFAULT_HOOK_TIMEOUT: Duration = Duration::from_secs(30);

/// The default time budget of one response-cache call: 5 seconds.
///
/// One turn makes at most three cache calls: the tenant read, the lookup and
/// the store. Each call has this budget. A cache call over it is a miss.
pub const DEFAULT_CACHE_TIMEOUT: Duration = Duration::from_secs(5);

/// The default time budget of the LLM budget check of one turn: 10 seconds
/// (issue #1997).
///
/// The check reads the spend of the run and of its tenant before the model
/// call. Without a budget, a slow pool or a slow read could use up the
/// `start_to_close` of the turn.
pub const DEFAULT_BUDGET_TIMEOUT: Duration = Duration::from_secs(10);

/// The bytes that the `ToolOutcome` JSON adds around its content.
const OUTCOME_ENVELOPE_BYTES: u64 = 64;

/// The mark at the end of a cut result.
const TRUNCATED: &str = "…[truncated]";

/// The model, the tools, and the policy behind the two agent activities.
///
/// Install it once per worker. On the Postgres engine, pass it to
/// `HarvestBuilder::state`. On SQLite, pass it to `sqlite::register`.
#[derive(Debug, Clone)]
pub struct AgentHarness {
    client: Arc<dyn AgentModel>,
    models: BTreeMap<String, Arc<dyn AgentModel>>,
    tools: Vec<Arc<dyn Tool>>,
    policy: Arc<dyn ToolPolicy>,
    temperature: Option<f32>,
    tool_output_limit: usize,
    max_result_bytes: u64,
    model_timeout: Duration,
    tool_timeout: Duration,
    policy_timeout: Duration,
    hook_timeout: Duration,
    cache_timeout: Duration,
    budget_timeout: Duration,
    memory: Option<Arc<dyn MemoryStore>>,
    delivery: Arc<dyn Delivery>,
    precheck: Option<Arc<dyn Precheck>>,
    cache: Option<ResponseCache>,
    roster: Vec<Peer>,
    flow: FlowRules,
    profiles: Vec<AgentProfile>,
    transcripts: Arc<dyn TranscriptStore>,
    mesh_budgets: Vec<MeshBudget>,
    mesh_spend: Option<Arc<dyn MeshSpend>>,
    live: Option<Arc<dyn LiveSink>>,
    workspace: Option<WorkspaceStore>,
}

impl AgentHarness {
    /// A harness with no tools and the [`AllowAll`] policy.
    #[must_use]
    pub fn new(client: Arc<dyn AgentModel>) -> Self {
        Self {
            client,
            models: BTreeMap::new(),
            transcripts: Arc::new(InMemoryTranscriptStore::default()),
            mesh_budgets: Vec::new(),
            mesh_spend: None,
            live: None,
            workspace: None,
            tools: Vec::new(),
            policy: Arc::new(AllowAll),
            temperature: None,
            tool_output_limit: DEFAULT_TOOL_OUTPUT_LIMIT,
            max_result_bytes: DEFAULT_MAX_ACTIVITY_RESULT_BYTES,
            model_timeout: DEFAULT_MODEL_TIMEOUT,
            tool_timeout: DEFAULT_TOOL_TIMEOUT,
            policy_timeout: DEFAULT_POLICY_TIMEOUT,
            hook_timeout: DEFAULT_HOOK_TIMEOUT,
            cache_timeout: DEFAULT_CACHE_TIMEOUT,
            budget_timeout: DEFAULT_BUDGET_TIMEOUT,
            memory: None,
            delivery: Arc::new(LogDelivery),
            precheck: None,
            cache: None,
            roster: Vec::new(),
            flow: FlowRules::new(),
            profiles: Vec::new(),
        }
    }

    /// Add one tool.
    #[must_use]
    pub fn tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.push(tool);
        self
    }

    /// Add several tools.
    #[must_use]
    pub fn tools(mut self, tools: impl IntoIterator<Item = Arc<dyn Tool>>) -> Self {
        self.tools.extend(tools);
        self
    }

    /// Set the policy that decides on each tool call.
    #[must_use]
    pub fn policy(mut self, policy: Arc<dyn ToolPolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Set the sampling temperature of every model call.
    #[must_use]
    pub const fn temperature(mut self, temperature: f32) -> Self {
        self.temperature = Some(temperature);
        self
    }

    /// Set the cap on one tool result, in characters.
    #[must_use]
    pub const fn tool_output_limit(mut self, limit: usize) -> Self {
        self.tool_output_limit = limit;
        self
    }

    /// Set the activity-result cap of the workers, in bytes. Set it when the
    /// workers use a cap other than the engine default.
    ///
    /// A larger tool result is cut to fit. A model reply that does not fit
    /// ends the run as `reply_too_large`. A cap of zero is no cap, as in the
    /// engine. A cap under 256 bytes cannot hold the `oversized` turn, so the
    /// activity can still fail.
    ///
    /// A worker with a payload store offloads a large result, and the engine
    /// skips its cap for it. The harness cannot see the store, so set zero for
    /// such a worker.
    #[must_use]
    pub const fn max_result_bytes(mut self, bytes: u64) -> Self {
        self.max_result_bytes = bytes;
        self
    }

    /// The activity-result cap of the workers, in bytes.
    #[cfg(feature = "eval")]
    pub(crate) const fn result_cap(&self) -> u64 {
        self.max_result_bytes
    }

    /// Set the time budget of one model call.
    #[must_use]
    pub const fn model_timeout(mut self, timeout: Duration) -> Self {
        self.model_timeout = timeout;
        self
    }

    /// Set the time budget of one tool call.
    #[must_use]
    pub const fn tool_timeout(mut self, timeout: Duration) -> Self {
        self.tool_timeout = timeout;
        self
    }

    /// Set the time budget of the policy for all tool calls of one turn. A
    /// call that has no decision when the budget ends is denied.
    #[must_use]
    pub const fn policy_timeout(mut self, timeout: Duration) -> Self {
        self.policy_timeout = timeout;
        self
    }

    /// Set the time budget of one memory read, delivery or precheck.
    #[must_use]
    pub const fn hook_timeout(mut self, timeout: Duration) -> Self {
        self.hook_timeout = timeout;
        self
    }

    /// Set the memory store. A run with a memory scope reads a snapshot from
    /// it and gets the `memory` tool.
    #[must_use]
    pub fn memory(mut self, store: Arc<dyn MemoryStore>) -> Self {
        self.memory = Some(store);
        self
    }

    /// Set where reports go. The default is [`LogDelivery`].
    #[must_use]
    pub fn delivery(mut self, delivery: Arc<dyn Delivery>) -> Self {
        self.delivery = delivery;
        self
    }

    /// Set the cheap check that can skip a heartbeat before any model call.
    #[must_use]
    pub fn precheck(mut self, precheck: Arc<dyn Precheck>) -> Self {
        self.precheck = Some(precheck);
        self
    }

    /// Serve an identical model call from `cache`, across runs.
    ///
    /// The model must name itself through [`AgentModel::model_id`]. A model
    /// with no id is never cached.
    #[must_use]
    pub fn response_cache(mut self, cache: ResponseCache) -> Self {
        if self.client.model_id().is_none() {
            tracing::warn!("the model has no model_id, so the response cache serves nothing");
        }
        self.cache = Some(cache);
        self
    }

    /// Add the model `name` to the registry (issue #2271).
    ///
    /// A task, a peer, a profile or a heartbeat names it. The model-turn
    /// input records the name, so replay asks the same model. The name
    /// [`DEFAULT_MODEL`] replaces the model of [`AgentHarness::new`].
    #[must_use]
    pub fn model_named(mut self, name: impl Into<String>, model: Arc<dyn AgentModel>) -> Self {
        let name = name.into();
        if name == DEFAULT_MODEL {
            self.client = model;
        } else {
            self.models.insert(name, model);
        }
        self
    }

    /// The model `name`. `None` is the [`DEFAULT_MODEL`].
    ///
    /// # Errors
    ///
    /// Returns a non-retryable activity error payload when the registry has
    /// no such model. A retry cannot add it.
    pub(crate) fn model(&self, name: Option<&str>) -> Result<&Arc<dyn AgentModel>, String> {
        match name {
            None | Some(DEFAULT_MODEL) => Ok(&self.client),
            Some(name) => self.models.get(name).ok_or_else(|| {
                ActivityFailure::non_retryable(
                    "UnknownModel",
                    format!(
                        "the harness has no model named {:?}; add it with AgentHarness::model_named",
                        truncate(name, crate::mesh::MAX_NAME_LEN)
                    ),
                )
                .into_error_payload()
            }),
        }
    }

    /// Keep the workspaces of durable agents in `store` (issue #2274). A
    /// profile turns a workspace on with `AgentProfile::workspace`.
    #[must_use]
    pub fn workspace(mut self, store: WorkspaceStore) -> Self {
        self.workspace = Some(store);
        self
    }

    /// Send the live text of every model turn to `sink`, on every backend
    /// (issue #2268). Pass a model that implements
    /// [`AgentModel::chat_stream`] to get text while the model writes.
    #[must_use]
    pub fn live_sink(mut self, sink: Arc<dyn LiveSink>) -> Self {
        self.live = Some(sink);
        self
    }

    /// Cap the spend of a mesh in each period (issue #2272). A spent budget
    /// refuses the model turns of the mesh. Set the spend source with
    /// [`mesh_spend`](Self::mesh_spend).
    #[must_use]
    pub fn mesh_budget(mut self, budget: MeshBudget) -> Self {
        self.mesh_budgets.push(budget);
        self
    }

    /// Read the spend of a mesh from `source`, such as the cost ledger.
    #[must_use]
    pub fn mesh_spend(mut self, source: Arc<dyn MeshSpend>) -> Self {
        self.mesh_spend = Some(source);
        self
    }

    /// Refuse a turn of `mesh` when a budget of the mesh is spent.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable [`MESH_BUDGET_EXCEEDED`] payload with the
    /// period end. A budget with no spend source fails closed as a config
    /// error. A failed read is retryable.
    async fn check_mesh_budget(
        &self,
        mesh: Option<&str>,
        now: autumn_harvest::chrono::DateTime<autumn_harvest::chrono::Utc>,
    ) -> Result<(), String> {
        let Some(mesh) = mesh else {
            return Ok(());
        };
        let budgets = self
            .mesh_budgets
            .iter()
            .filter(|budget| budget.mesh == mesh);
        for budget in budgets {
            let Some(source) = &self.mesh_spend else {
                return Err(ActivityFailure::non_retryable(
                    "MeshSpendMissing",
                    "a mesh budget needs a spend source: call AgentHarness::mesh_spend",
                )
                .into_error_payload());
            };
            let (since, until) = budget.period.window(now);
            let spend = tokio::time::timeout(self.budget_timeout, source.spent(mesh, since))
                .await
                .map_err(|_| "the read took longer than the budget timeout".to_owned())
                .and_then(|read| read.map_err(|err| err.to_string()))
                .map_err(|reason| {
                    autumn_harvest::llm_budget::check_failed(&reason).into_error_payload()
                })?;
            if budget.is_spent(&spend) {
                let details = MeshBudgetExceeded {
                    mesh: mesh.to_owned(),
                    until_ms: until.timestamp_millis(),
                };
                return Err(ActivityFailure::non_retryable(
                    MESH_BUDGET_EXCEEDED,
                    format!("the mesh {mesh:?} has spent its budget for this period"),
                )
                .with_details(serde_json::to_value(details).unwrap_or_default())
                .into_error_payload());
            }
        }
        Ok(())
    }

    /// Keep the windows of model turns in `store`, so a later turn records
    /// only the messages it adds (issue #2264). The default is an
    /// [`InMemoryTranscriptStore`]. A shared store lets a delta hit on every
    /// worker.
    #[must_use]
    pub fn transcript_store(mut self, store: Arc<dyn TranscriptStore>) -> Self {
        self.transcripts = store;
        self
    }

    /// The whole window of `request`: its prefix from the transcript store,
    /// then its messages. A turn with `remember` keeps the window in the
    /// store.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable [`PREFIX_MISSING`] payload when the store does
    /// not hold the prefix. The workflow then sends the full form.
    fn window(&self, request: &mut ModelTurnRequest) -> Result<Vec<ChatMessage>, String> {
        let added = std::mem::take(&mut request.messages);
        let (digest, window) = match &request.prefix {
            None => (None, added.clone()),
            Some(prefix) => {
                // The digest check makes a changed window a miss, so a
                // shared store cannot put words into a turn.
                let held = self
                    .transcripts
                    .get(&prefix.digest)
                    .filter(|held| u32::try_from(held.len()).is_ok_and(|len| len == prefix.len))
                    .filter(|held| chain(None, held) == prefix.digest)
                    .ok_or_else(|| {
                        ActivityFailure::non_retryable(
                            PREFIX_MISSING,
                            "the transcript store does not hold the prefix of this turn",
                        )
                        .into_error_payload()
                    })?;
                let mut window = held;
                window.extend(added.iter().cloned());
                (Some(prefix.digest.as_str()), window)
            }
        };
        if request.remember {
            // A chain digest of the prefix and the added messages equals the
            // digest of the whole window, so one hash pass is enough.
            let digest = digest.map_or_else(
                || chain(None, &window),
                |prefix| chain(Some(prefix), &added),
            );
            self.transcripts.put(&digest, &window);
        }
        Ok(window)
    }

    /// Add one peer to the roster. A run whose mandate names the peer can
    /// delegate to it. When two peers share a name, the later one wins at
    /// lookup.
    #[must_use]
    pub fn peer(mut self, peer: Peer) -> Self {
        self.roster.push(peer);
        self
    }

    /// Add the profile of one kind of durable agent. When two profiles share
    /// a kind, the later one wins.
    #[must_use]
    pub fn agent(mut self, profile: AgentProfile) -> Self {
        self.profiles.push(profile);
        self
    }

    /// The profile of the agent kind `kind`, if the harness has one.
    #[must_use]
    pub fn agent_profile(&self, kind: &str) -> Option<AgentProfile> {
        self.profiles
            .iter()
            .rev()
            .find(|profile| profile.kind == kind)
            .cloned()
    }

    /// Install flow rules. They gate each tool call against the labels that
    /// the run tree has exercised.
    #[must_use]
    pub fn flow_rules(mut self, rules: FlowRules) -> Self {
        self.flow = rules;
        self
    }

    /// The peers of the roster that `names` lists, in name order.
    ///
    /// The result skips a name with no peer and a peer with an invalid name.
    #[must_use]
    pub fn discover_peers(&self, names: &[String]) -> Vec<Peer> {
        let wanted: BTreeSet<&String> = names.iter().collect();
        let mut found = BTreeMap::new();
        for peer in self
            .roster
            .iter()
            .filter(|peer| wanted.contains(&peer.name))
        {
            if valid_name(&peer.name) {
                found.insert(peer.name.clone(), peer.clone());
            } else {
                tracing::warn!(peer = %peer.name, "a peer with an invalid name is left out");
            }
        }
        found.into_values().collect()
    }

    /// Set the time budget of one response-cache call. One turn makes at
    /// most three. Keep the sum of the model, policy, cache and budget-check
    /// budgets below the 15-minute `start_to_close` of `agent_model_turn`.
    #[must_use]
    pub const fn cache_timeout(mut self, timeout: Duration) -> Self {
        self.cache_timeout = timeout;
        self
    }

    /// Set the time budget of the LLM budget check of one turn (issue
    /// #1997). Keep the sum of the model, policy, cache and budget-check
    /// budgets below the 15-minute `start_to_close` of `agent_model_turn`.
    #[must_use]
    pub const fn budget_timeout(mut self, timeout: Duration) -> Self {
        self.budget_timeout = timeout;
        self
    }

    /// Run the LLM budget check of one turn (issue #1997).
    ///
    /// A refusal returns the refusal. A check over the budget time fails as
    /// a retryable `LlmBudgetCheckFailed`. Either way, the turn calls no
    /// model, so a retry pays nothing.
    pub(crate) async fn check_budget<F>(&self, check: impl FnOnce() -> F) -> Result<(), String>
    where
        F: std::future::Future<Output = Result<(), String>>,
    {
        let budget = self.budget_timeout;
        let Ok(checked) = tokio::time::timeout(budget, check()).await else {
            let reason = format!("the read took longer than {budget:?}");
            return Err(autumn_harvest::llm_budget::check_failed(&reason).into_error_payload());
        };
        checked
    }

    /// Whether a turn can use the response cache: the harness has a cache,
    /// and a model has an id.
    pub(crate) fn cache_active(&self) -> bool {
        self.cache.is_some()
            && std::iter::once(&self.client)
                .chain(self.models.values())
                .any(|model| model.model_id().is_some())
    }

    /// The time budget of one response-cache call, the tenant read included.
    pub(crate) const fn cache_budget(&self) -> Duration {
        self.cache_timeout
    }

    /// Run one model call, then ask the policy about each tool call.
    ///
    /// The decisions are part of the result. Replay reads them back, so a
    /// policy that reads changing state cannot change a recorded run.
    ///
    /// With a response cache, `request.tenant` scopes the key. This method
    /// does not read the verified tenant of the run. The `agent_model_turn`
    /// activity does, so register [`activities`](crate::activities) on
    /// Postgres. A hit skips the model call. The policy still decides each
    /// tool call.
    ///
    /// A turn that exceeds the result cap does not fail. The method cuts each
    /// decision reason. When the turn still does not fit, the method returns a
    /// turn with [`ModelTurn::oversized`] set.
    ///
    /// # Errors
    ///
    /// Returns an activity error payload. A failure that a retry can fix is
    /// retryable: a rate limit, a transport fault, a provider outage, or the
    /// time budget. Every other failure is not.
    pub async fn model_turn(&self, request: ModelTurnRequest) -> Result<ModelTurn, String> {
        let tenant = CacheTenant::declared(request.tenant.as_deref());
        self.model_turn_in(request, tenant, |_content| {}).await
    }

    /// Run one model call, let `prepare` edit the model content, then ask
    /// the policy about each tool call.
    ///
    /// The evaluation harness uses `prepare` to set the recorded call ids
    /// before the policy reads them. It skips the response cache, because an
    /// evaluation measures the live answer of the candidate.
    ///
    /// # Errors
    ///
    /// The errors of [`model_turn`](Self::model_turn).
    #[cfg(feature = "eval")]
    pub(crate) async fn model_turn_with(
        &self,
        request: ModelTurnRequest,
        prepare: impl FnMut(&mut Vec<ContentPart>),
    ) -> Result<ModelTurn, String> {
        self.model_turn_in(request, CacheTenant::Skip, prepare)
            .await
    }

    /// [`model_turn_with`](Self::model_turn_with), with the tenant that
    /// scopes the response cache.
    pub(crate) async fn model_turn_in(
        &self,
        request: ModelTurnRequest,
        tenant: CacheTenant,
        prepare: impl FnMut(&mut Vec<ContentPart>),
    ) -> Result<ModelTurn, String> {
        self.model_turn_timed_in(
            request,
            tenant,
            prepare,
            None,
            autumn_harvest::chrono::Utc::now(),
        )
        .await
        .map(|(turn, _latency)| turn)
    }

    /// [`model_turn_in`](Self::model_turn_in), with the latency of the model
    /// call (issue #1996).
    ///
    /// The latency covers the model call only, not the cache or the policy.
    /// It is `None` when a cache hit answers the turn, because no model call
    /// ran.
    ///
    /// `publish` gets the live events of a turn whose request sets `stream`
    /// (issue #2268). The live sink of the harness gets them for every turn.
    ///
    /// `now` is the clock of the runtime. The mesh budget window uses it, so
    /// it agrees with the clock of the ledger rows (issue #2272).
    pub(crate) async fn model_turn_timed_in(
        &self,
        request: ModelTurnRequest,
        tenant: CacheTenant,
        prepare: impl FnMut(&mut Vec<ContentPart>),
        publish: Option<&(dyn Fn(&LiveEvent) + Send + Sync)>,
        now: autumn_harvest::chrono::DateTime<autumn_harvest::chrono::Utc>,
    ) -> Result<(ModelTurn, Option<Duration>), String> {
        let span = tracing::info_span!(
            "agent_model_turn",
            gen_ai.agent.name = tracing::field::Empty
        );
        if let Some(agent) = &request.agent {
            span.record("gen_ai.agent.name", agent.as_str());
        }
        self.model_turn_spanned(request, tenant, prepare, publish, now)
            .instrument(span)
            .await
    }

    /// The tools of one turn: the built-in tools that the decision checks,
    /// and the definitions that the model sees.
    fn turn_tools(&self, request: &ModelTurnRequest) -> (Vec<Arc<dyn Tool>>, Vec<ToolDefinition>) {
        let mut builtins = self.builtins(
            request.memory_scope.as_ref(),
            request.extra_tools.iter().cloned(),
        );
        // Only a run with a mandate names its acting agent.
        let mandate = request.agent.is_some();
        // Under a mandate with no peer to offer, a `delegate` call is still a
        // delegation. A hidden stand-in lets the decision treat it as the
        // workflow tool, so the workflow refuses it with a named verdict.
        let hidden_delegate = mandate && !builtins.iter().any(|tool| tool.name() == DELEGATE_TOOL);
        if hidden_delegate {
            builtins.push(Arc::new(WorkflowTool(delegate_definition(&[]))));
        }
        let tools = self
            .permitted(&builtins, request.allowed_tools.as_ref())
            .filter(|tool| !(hidden_delegate && tool.name() == DELEGATE_TOOL))
            .map(Tool::definition)
            .collect();
        (builtins, tools)
    }

    /// The body of [`model_turn_timed_in`](Self::model_turn_timed_in).
    async fn model_turn_spanned(
        &self,
        request: ModelTurnRequest,
        tenant: CacheTenant,
        mut prepare: impl FnMut(&mut Vec<ContentPart>),
        publish: Option<&(dyn Fn(&LiveEvent) + Send + Sync)>,
        now: autumn_harvest::chrono::DateTime<autumn_harvest::chrono::Utc>,
    ) -> Result<(ModelTurn, Option<Duration>), String> {
        let live = Live {
            run_id: request.run_id.clone(),
            step: request.steps_used,
            publish: publish.filter(|_| request.stream),
            sink: self.live.as_deref(),
        };
        let client = self.model(request.model.as_deref())?;
        self.check_mesh_budget(request.mesh.as_deref(), now).await?;
        let mut request = request;
        let messages = self.window(&mut request)?;
        let (builtins, tools) = self.turn_tools(&request);
        let allowed = request.allowed_tools.as_ref();
        let chat = ChatRequest {
            messages,
            tools,
            max_tokens: request.max_output_tokens,
            temperature: self.temperature,
        };
        // The key holds the model id, so one model never reads the answer
        // of another.
        let key = self.cache_key(client.as_ref(), &tenant, &chat);
        let mut cached = match &key {
            Some(key) => self.cache_lookup(key).await,
            None => None,
        };
        // At most two passes. A hit whose turn is over the result cap is a
        // miss, so the second pass asks the model. Both passes share one
        // policy budget, so the turn stays inside `start_to_close`.
        let mut policy_budget = self.policy_timeout;
        let mut latency = None;
        loop {
            let cache_hit = cached.is_some();
            // A fresh answer waits for its decisions before it reaches the cache.
            let mut fresh = None;
            let response = if let Some(response) = cached.take() {
                // No model call ran, so the turn spent no tokens.
                ChatResponse {
                    usage: TokenUsage::default(),
                    ..response
                }
            } else {
                let started = std::time::Instant::now();
                let response = if live.is_on() {
                    self.stream_model(client.as_ref(), &chat, &live).await?
                } else {
                    self.call_model(client.as_ref(), &chat).await?
                };
                latency = Some(started.elapsed());
                fresh = key.as_ref().map(|key| (key, response.clone()));
                response
            };
            let mut content = response.content;
            prepare(&mut content);
            let mut turn = ModelTurn {
                content,
                stop: response.stop_reason,
                usage: response.usage,
                decisions: Vec::new(),
                cache_hit,
                labels: Vec::new(),
                releases: Vec::new(),
                oversized: false,
                interruptible: Vec::new(),
            };
            let info = RunInfo {
                run_id: RunId::new(request.run_id.clone()),
                session_id: request.session_id.clone(),
                steps_used: request.steps_used,
                max_steps: request.max_steps,
                usage: request.usage.saturating_add(response.usage),
            };
            self.decide_calls(
                &mut turn,
                &info,
                Scope {
                    read_only: request.read_only,
                    builtins: &builtins,
                    allowed,
                    exercised: &request.flow_labels,
                    released: &request.flow_released,
                },
                &mut policy_budget,
            )
            .await;
            if request.interruptible {
                turn.interruptible = self.interruptible_calls(&turn, &builtins);
            }
            // The cache serves and keeps only an answer whose whole turn fits.
            // The check uses the turn before the cut below. A cached turn that
            // exceeds the cap would end every identical run as
            // `reply_too_large`.
            let fits = self.fits_result_cap(&turn);
            if cache_hit && !fits {
                tracing::warn!("a cached answer does not fit the result cap; calling the model");
                continue;
            }
            if let Some((key, response)) = fresh
                && fits
            {
                self.cache_store(key, &response).await;
            }
            return Ok((fit_turn(turn, self.max_result_bytes), latency));
        }
    }

    /// Record one model call in the agent cost ledger (issue #1996).
    ///
    /// A ledger refusal does not fail the turn, because the reply is already
    /// paid for. [`ledger_call`](Self::ledger_call) builds the call.
    pub(crate) fn record_turn(
        &self,
        ctx: &autumn_harvest::context::ActivityContext,
        model: Option<&str>,
        usage: &TokenUsage,
        latency: Duration,
    ) {
        if let Err(err) = ctx.record_llm_call(self.ledger_call(model, usage, latency)) {
            tracing::warn!(error = %err, "the agent cost ledger refused a model turn");
        }
    }

    /// The cost ledger call of one model turn (issues #1996 and #2275).
    ///
    /// Both backends record this call: Postgres through the activity
    /// context, SQLite through its ledger closure. So one script gives the
    /// same rows on both. The call keeps its tokens even when the model
    /// metadata is bad:
    /// - no model id, or one that the ledger refuses, becomes
    ///   [`UNKNOWN_MODEL_ID`];
    /// - a cost that the ledger refuses is dropped, so the call is unpriced.
    ///
    /// Each refusal logs a warning.
    pub(crate) fn ledger_call(
        &self,
        model: Option<&str>,
        usage: &TokenUsage,
        latency: Duration,
    ) -> autumn_harvest::llm_ledger::LlmCall {
        use autumn_harvest::llm_ledger::LlmCall;

        // A turn on an unknown model fails before its call, so the default
        // is never priced for it.
        let client = self.model(model).unwrap_or(&self.client);
        let mut model = client.model_id().unwrap_or(UNKNOWN_MODEL_ID);
        if let Err(err) = LlmCall::new(model, 0, 0).validate() {
            tracing::warn!(
                error = %err,
                "the agent cost ledger refused the model id; recording the turn as {UNKNOWN_MODEL_ID:?}"
            );
            model = UNKNOWN_MODEL_ID;
        }
        let cost = client.cost_usd_micros(usage).filter(|&cost| {
            let check = LlmCall::new(UNKNOWN_MODEL_ID, 0, 0)
                .with_cost_usd_micros(cost)
                .validate();
            if let Err(err) = &check {
                tracing::warn!(
                    error = %err,
                    "the agent cost ledger refused the cost; recording the turn as unpriced"
                );
            }
            check.is_ok()
        });
        let mut call = LlmCall::new(
            model,
            u64::from(usage.input_tokens),
            u64::from(usage.output_tokens),
        )
        .with_latency(latency);
        if let Some(cost) = cost {
            call = call.with_cost_usd_micros(cost);
        }
        call
    }

    /// Ask the policy about each tool call of `turn`, in order.
    ///
    /// One deadline bounds the whole decision phase. A per-call budget would
    /// let many stalled calls add up past `start_to_close`. The phase spends
    /// from `budget` and leaves the rest in it for a second pass. A stalled
    /// policy denies the call. That fails closed, and the model reads why.
    async fn decide_calls(
        &self,
        turn: &mut ModelTurn,
        info: &RunInfo,
        scope: Scope<'_>,
        budget: &mut Duration,
    ) {
        let Scope {
            read_only,
            builtins,
            allowed,
            exercised,
            released,
        } = scope;
        let started = tokio::time::Instant::now();
        let deadline = started + *budget;
        let read_only_policy: Arc<dyn ToolPolicy>;
        let policy: &dyn ToolPolicy = if read_only {
            // The read-only rules go first. Their deny ends the decision, so
            // the app policy is not asked about a call that cannot run.
            read_only_policy = Arc::new(Strictest::new(vec![
                Arc::new(ToolRules::read_only()),
                Arc::clone(&self.policy),
            ]));
            read_only_policy.as_ref()
        } else {
            self.policy.as_ref()
        };
        // Each call that is not denied may run. Its labels count for the
        // calls after it in this turn before it runs, so the check fails
        // closed. A release counts only after the approval, so a call that
        // waits releases nothing for the calls after it.
        let mut seen = exercised.clone();
        let can_release = self.flow.can_release();
        for call in turn.calls() {
            if can_release {
                turn.releases.push(Released::new());
            }
            if !permits(builtins, allowed, &call.name) {
                turn.decisions.push(ToolDecision::Deny {
                    reason: format!("the tool {:?} is not in this agent's mandate", call.name),
                });
                if !self.flow.is_empty() {
                    turn.labels.push(BTreeSet::new());
                }
                continue;
            }
            let tool = self.visible(builtins).find(|tool| tool.name() == call.name);
            if tool.is_none() && !self.flow.is_empty() {
                // This worker cannot label the call. Another worker with
                // another tool list could run it with no labels.
                turn.decisions.push(ToolDecision::Deny {
                    reason: format!("unknown tool {:?}: flow rules need its labels", call.name),
                });
                turn.labels.push(BTreeSet::new());
                continue;
            }
            let decide = policy.decide(&call, tool, info);
            let mut decision = tokio::time::timeout_at(deadline, decide)
                .await
                .unwrap_or_else(|_| ToolDecision::Deny {
                    reason: format!("the policy did not decide within {:?}", self.policy_timeout),
                });
            if !self.flow.is_empty() {
                let labels = call_labels(tool);
                let (flow, freed) = self.flow.decide(&seen, released, &labels);
                decision = strictest(decision, flow);
                if !matches!(decision, ToolDecision::Deny { .. }) {
                    seen.extend(labels.iter().cloned());
                    if let Some(slot) = turn.releases.last_mut() {
                        *slot = freed;
                    }
                }
                turn.labels.push(labels);
            }
            turn.decisions.push(decision);
        }
        *budget = budget.saturating_sub(started.elapsed());
    }

    /// For each tool call of `turn`, whether its tool opts in to an
    /// interrupt (issue #2265). An unknown tool does not.
    fn interruptible_calls(&self, turn: &ModelTurn, builtins: &[Arc<dyn Tool>]) -> Vec<bool> {
        turn.calls()
            .iter()
            .map(|call| {
                self.visible(builtins)
                    .find(|tool| tool.name() == call.name)
                    .is_some_and(Tool::interruptible)
            })
            .collect()
    }

    /// Send `chat` to `client` within the model time budget, and send each
    /// piece of answer text to `live` while the model writes (issue #2268).
    async fn stream_model(
        &self,
        client: &dyn AgentModel,
        chat: &ChatRequest,
        live: &Live<'_>,
    ) -> Result<ChatResponse, String> {
        live.send(&LiveEvent::Start {
            run_id: live.run_id.clone(),
            step: live.step,
        });
        let joined = std::sync::Mutex::new(Coalescer::new());
        let on_delta = |text: &str| {
            let ready = joined
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(text);
            if let Some(text) = ready {
                live.delta(text);
            }
        };
        let result =
            tokio::time::timeout(self.model_timeout, client.chat_stream(chat, &on_delta)).await;
        let rest = joined
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .flush();
        if let Some(text) = rest {
            live.delta(text);
        }
        result
            .map_err(|_| {
                ActivityFailure::retryable(
                    "ModelTimeout",
                    format!("the model did not answer within {:?}", self.model_timeout),
                )
                .into_error_payload()
            })?
            .map_err(|err| model_failure(&err))
    }

    /// Send `chat` to `client` within the model time budget.
    async fn call_model(
        &self,
        client: &dyn AgentModel,
        chat: &ChatRequest,
    ) -> Result<ChatResponse, String> {
        tokio::time::timeout(self.model_timeout, client.chat(chat))
            .await
            .map_err(|_| {
                ActivityFailure::retryable(
                    "ModelTimeout",
                    format!("the model did not answer within {:?}", self.model_timeout),
                )
                .into_error_payload()
            })?
            .map_err(|err| model_failure(&err))
    }

    /// The cache key of `chat`, or `None` when this turn skips the cache.
    ///
    /// The turn skips the cache when the harness has no cache, the model has
    /// no id, or the tenant says to skip.
    fn cache_key(
        &self,
        client: &dyn AgentModel,
        tenant: &CacheTenant,
        chat: &ChatRequest,
    ) -> Option<CacheKey> {
        let cache = self.cache.as_ref()?;
        let model_id = client.model_id()?;
        let tenant = match tenant {
            CacheTenant::None => KeyTenant::None,
            CacheTenant::Verified(name) => KeyTenant::Verified(name),
            CacheTenant::Declared(name) => KeyTenant::Declared(name),
            CacheTenant::Skip => return None,
        };
        cache
            .key(tenant, model_id, chat)
            .inspect_err(|err| tracing::warn!(error = %err, "the response cache skips this turn"))
            .ok()
    }

    /// The cached answer for `key`. A fault or a slow cache is a miss.
    async fn cache_lookup(&self, key: &CacheKey) -> Option<ChatResponse> {
        let cache = self.cache.as_ref()?;
        match tokio::time::timeout(self.cache_timeout, cache.lookup(key)).await {
            Ok(Ok(found)) => found,
            Ok(Err(err)) => {
                tracing::warn!(error = %err, "response cache read failed; calling the model");
                None
            }
            Err(_) => {
                tracing::warn!(budget = ?self.cache_timeout, "response cache read timed out; calling the model");
                None
            }
        }
    }

    /// Whether a later hit on the answer of `turn` fits the result cap.
    ///
    /// The check measures the turn as a hit records it: the same content
    /// and decisions, no usage, and `cache_hit`. A zero cap is no cap, as on
    /// a worker.
    fn fits_result_cap(&self, turn: &ModelTurn) -> bool {
        let hit = ModelTurn {
            usage: TokenUsage::default(),
            cache_hit: true,
            ..turn.clone()
        };
        !exceeds_bytes(&hit, self.max_result_bytes)
    }

    /// Store a fresh answer. A fault or a slow cache is only a warning.
    async fn cache_store(&self, key: &CacheKey, response: &ChatResponse) {
        let Some(cache) = &self.cache else {
            return;
        };
        match tokio::time::timeout(self.cache_timeout, cache.store(key, response)).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::warn!(error = %err, "response cache write failed"),
            Err(_) => {
                tracing::warn!(budget = ?self.cache_timeout, "response cache write timed out");
            }
        }
    }

    /// Run one tool call.
    ///
    /// A tool failure, an unknown tool name, or a call over its time budget
    /// is a result the model reads, not an activity error. The run goes on.
    ///
    /// # Errors
    ///
    /// This method returns `Ok` for every tool outcome. The `Result` matches
    /// the activity signature.
    pub async fn tool_call(&self, request: ToolCallRequest) -> Result<ToolOutcome, String> {
        let call = request.call;
        let builtins = self.builtins_labelled(
            request.memory_scope.as_ref(),
            std::iter::empty(),
            &request.flow_labels,
        );
        // A second check of the mandate. The decision and the call can run on
        // workers with different tool lists.
        if !permits(&builtins, request.allowed_tools.as_ref(), &call.name) {
            return Ok(self.error_outcome(&format!(
                "the tool {:?} is not in this agent's mandate",
                call.name
            )));
        }
        let Some(tool) = self
            .visible(&builtins)
            .find(|tool| tool.name() == call.name)
        else {
            return Ok(self.error_outcome(&format!("unknown tool {:?}", call.name)));
        };
        // A second check of the read-only rule. The decision and the call can
        // run on workers with different tool lists, for example in a deploy.
        if request.read_only && tool.effect() >= ToolEffect::Write {
            return Ok(self.error_outcome("this run may only read data"));
        }
        // The same reason applies to the flow labels. A tool that gained a
        // label after the decision did not pass the flow rules with it.
        if let Some(recorded) = &request.labels
            && !call_labels(Some(tool)).is_subset(recorded)
        {
            return Ok(self
                .error_outcome("the tool labels changed after the decision; call the tool again"));
        }
        // A read-only run can read the files but never moves the head.
        let keep_files = !request.read_only;
        // Only a tool that opts in loads the workspace (issue #2274).
        let mount = request.workspace.filter(|_| tool.uses_workspace());
        let handle = match &mount {
            Some(mount) => Some(self.open_workspace(mount).await?),
            None => None,
        };
        let ctx = ToolContext {
            run_id: RunId::new(request.run_id),
            call_id: call.id,
            session_id: request.session_id,
            step: request.step,
            workspace: handle.clone(),
        };
        let execution = tool.execute(call.arguments, &ctx);
        Ok(
            match tokio::time::timeout(self.tool_timeout, execution).await {
                Ok(Ok(output)) => {
                    let mut outcome = ToolOutcome::ok(fit_result(
                        truncate(&output.to_string(), self.tool_output_limit),
                        self.max_result_bytes,
                    ));
                    if let (Some(mount), Some(handle), true) = (&mount, &handle, keep_files) {
                        match self.save_workspace(mount, handle).await? {
                            Ok(head) => outcome.workspace = head,
                            Err(cap) => outcome = self.error_outcome(&cap),
                        }
                    }
                    outcome
                }
                Ok(Err(err)) => {
                    tracing::warn!(tool = %call.name, error = %err, "agent tool call failed");
                    self.error_outcome(err.message())
                }
                Err(_) => self.error_outcome(&format!(
                    "the tool did not finish within {:?}",
                    self.tool_timeout
                )),
            },
        )
    }

    /// The workspace store, or a non-retryable error.
    fn workspace_store(&self) -> Result<&WorkspaceStore, String> {
        self.workspace.as_ref().ok_or_else(|| {
            ActivityFailure::non_retryable(
                "WorkspaceStoreMissing",
                "the agent has a workspace, but no workspace store is installed: call AgentHarness::workspace",
            )
            .into_error_payload()
        })
    }

    /// Load the head of `mount` for one tool call (issue #2274).
    ///
    /// A store failure retries the activity. A missing or changed snapshot
    /// fails it, because the head is lost.
    async fn open_workspace(&self, mount: &WorkspaceMount) -> Result<WorkspaceHandle, String> {
        let store = self.workspace_store()?;
        let workspace = tokio::time::timeout(self.tool_timeout, store.load(mount))
            .await
            .map_err(|_| workspace_timeout())?
            .map_err(|err| workspace_failure(&err))?;
        Ok(WorkspaceHandle::new(workspace))
    }

    /// Save the workspace of a call that changed it (issue #2274).
    ///
    /// `Ok(Ok(None))` means no change. `Ok(Err(text))` is a cap, which the
    /// model reads. `Err` retries the activity: a new attempt runs the tool
    /// again from the same head.
    async fn save_workspace(
        &self,
        mount: &WorkspaceMount,
        handle: &WorkspaceHandle,
    ) -> Result<Result<Option<WorkspaceRef>, String>, String> {
        let workspace = handle.snapshot();
        if !workspace.is_dirty() {
            return Ok(Ok(None));
        }
        let store = self.workspace_store()?;
        let save = store.save(&mount.scope, mount.generation, &workspace);
        match tokio::time::timeout(self.tool_timeout, save).await {
            Err(_) => Err(workspace_timeout()),
            Ok(Ok(head)) => Ok(Ok(Some(head))),
            Ok(Err(err)) if err.kind() == ErrorKind::Tool => Ok(Err(err.message().to_owned())),
            Ok(Err(err)) => Err(workspace_failure(&err)),
        }
    }

    /// Delete the snapshots of the scope of `mount` that are older than its
    /// head (issue #2274). With no head, delete those below the generation
    /// of `mount`. Returns the number deleted.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error when no store is installed, and the
    /// store error otherwise.
    pub async fn workspace_retain(&self, mount: WorkspaceMount) -> Result<u64, String> {
        let store = self.workspace_store()?;
        let deleted = match &mount.head {
            Some(head) => store.retain(&mount.scope, head).await,
            None => store.reset(&mount.scope, mount.generation).await,
        };
        deleted.map_err(|err| model_failure(&err))
    }

    /// Delete every snapshot of the scope of `mount` below its generation
    /// (issue #2274). Returns the number deleted. With no store installed
    /// it deletes nothing, so an erase of an agent with no workspace
    /// succeeds.
    ///
    /// # Errors
    ///
    /// Returns the store error.
    pub async fn workspace_reset(&self, mount: WorkspaceMount) -> Result<u64, String> {
        let Some(store) = &self.workspace else {
            return Ok(0);
        };
        store
            .reset(&mount.scope, mount.generation)
            .await
            .map_err(|err| model_failure(&err))
    }

    /// An error result whose recorded outcome fits the result cap.
    ///
    /// The message is escaped twice: once into the error JSON, and once as
    /// the outcome content. One byte can grow to twelve, so the message keeps
    /// a twelfth of the cap.
    fn error_outcome(&self, message: &str) -> ToolOutcome {
        let message = truncate(message, self.tool_output_limit);
        let max = max_error_bytes(self.max_result_bytes);
        ToolOutcome::error(&cut_bytes(message, max))
    }

    /// Load the memory of `scope` and render it as the frozen snapshot.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error when no store is installed, and the
    /// store error otherwise.
    pub async fn memory_snapshot(&self, scope: MemoryScope) -> Result<MemorySnapshot, String> {
        let Some(store) = &self.memory else {
            return Err(ActivityFailure::non_retryable(
                "MemoryStoreMissing",
                "the run has a memory scope, but no memory store is installed: call AgentHarness::memory",
            )
            .into_error_payload());
        };
        let blocks = tokio::time::timeout(self.hook_timeout, store.load(&scope))
            .await
            .map_err(|_| self.hook_timed_out("MemoryTimeout", "the memory store"))?
            .map_err(|err| model_failure(&err))?;
        // The labels go with the text, so a cut text keeps every label.
        Ok(match MemorySnapshot::of(&blocks) {
            MemorySnapshot::Text(text) => {
                MemorySnapshot::Text(fit_result(text, self.max_result_bytes))
            }
            MemorySnapshot::Labelled { text, labels } => MemorySnapshot::Labelled {
                text: fit_result(text, self.max_result_bytes),
                labels,
            },
        })
    }

    /// Clear the memory of `scope`.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable error when no store is installed, and the
    /// store error otherwise.
    pub async fn memory_reset(&self, scope: MemoryScope) -> Result<(), String> {
        let Some(store) = &self.memory else {
            return Err(ActivityFailure::non_retryable(
                "MemoryStoreMissing",
                "the agent has a memory scope, but no memory store is installed: call AgentHarness::memory",
            )
            .into_error_payload());
        };
        tokio::time::timeout(self.hook_timeout, store.clear(&scope))
            .await
            .map_err(|_| self.hook_timed_out("MemoryTimeout", "the memory store"))?
            .map_err(|err| model_failure(&err))
    }

    /// Send one report to the delivery.
    ///
    /// # Errors
    ///
    /// Returns the delivery error. A retryable kind retries, and so does a
    /// send over its time budget.
    pub async fn deliver(&self, report: Report) -> Result<(), String> {
        tokio::time::timeout(self.hook_timeout, self.delivery.deliver(&report))
            .await
            .map_err(|_| self.hook_timed_out("DeliveryTimeout", "the delivery"))?
            .map_err(|err| model_failure(&err))
    }

    /// Ask the precheck whether a heartbeat tick should run. With no
    /// precheck, every tick runs. A precheck over its time budget skips the
    /// tick.
    ///
    /// # Errors
    ///
    /// This method returns `Ok` for every answer. The `Result` matches the
    /// activity signature.
    pub async fn precheck_tick(&self, task: HeartbeatTask) -> Result<bool, String> {
        let Some(precheck) = &self.precheck else {
            return Ok(true);
        };
        let answer = tokio::time::timeout(self.hook_timeout, precheck.should_run(&task)).await;
        Ok(answer.unwrap_or_else(|_| {
            tracing::warn!(
                budget = ?self.hook_timeout,
                "heartbeat precheck timed out; the tick is skipped"
            );
            false
        }))
    }

    fn hook_timed_out(&self, kind: &str, what: &str) -> String {
        ActivityFailure::retryable(
            kind,
            format!("{what} did not answer within {:?}", self.hook_timeout),
        )
        .into_error_payload()
    }

    /// The tools that the crate provides for one call: the memory tool, and
    /// a stand-in for each tool the workflow handles itself.
    fn builtins(
        &self,
        scope: Option<&MemoryScope>,
        extra: impl Iterator<Item = ToolDefinition>,
    ) -> Vec<Arc<dyn Tool>> {
        self.builtins_labelled(scope, extra, &BTreeSet::new())
    }

    /// [`builtins`](Self::builtins), with the flow labels that a memory
    /// write keeps (issue #2272).
    fn builtins_labelled(
        &self,
        scope: Option<&MemoryScope>,
        extra: impl Iterator<Item = ToolDefinition>,
        labels: &BTreeSet<String>,
    ) -> Vec<Arc<dyn Tool>> {
        let memory = self.memory.as_ref().zip(scope).map(|(store, scope)| {
            Arc::new(MemoryTool::new(Arc::clone(store), scope.clone()).with_labels(labels.clone()))
                as _
        });
        memory
            .into_iter()
            .chain(extra.map(|definition| Arc::new(WorkflowTool(definition)) as _))
            .collect()
    }

    /// The visible tools that `allowed` permits.
    fn permitted<'a>(
        &'a self,
        builtins: &'a [Arc<dyn Tool>],
        allowed: Option<&'a BTreeSet<String>>,
    ) -> impl Iterator<Item = &'a dyn Tool> {
        self.visible(builtins)
            .filter(move |tool| permits(builtins, allowed, tool.name()))
    }

    /// The built-in tools, then each app tool whose name no built-in takes.
    ///
    /// A built-in wins a name clash. Two tools with one name would make many
    /// providers refuse the request. Under a mandate, the built-ins always
    /// hold `delegate`, so the name stays reserved.
    fn visible<'a>(&'a self, builtins: &'a [Arc<dyn Tool>]) -> impl Iterator<Item = &'a dyn Tool> {
        // With a store installed, `memory` stays reserved even when this run
        // gets no memory tool. An app tool of that name could otherwise slip
        // past the memory-write opt-in.
        let reserved = self.memory.is_some();
        let taken = move |name: &str| {
            (reserved && name == MEMORY_TOOL) || builtins.iter().any(|tool| tool.name() == name)
        };
        builtins.iter().map(AsRef::as_ref).chain(
            self.tools
                .iter()
                .map(AsRef::as_ref)
                .filter(move |tool| !taken(tool.name())),
        )
    }
}

/// What bounds the decisions of one turn.
struct Scope<'a> {
    /// Apply the read-only rules on top of the policy.
    read_only: bool,
    /// The built-in tools of the turn.
    builtins: &'a [Arc<dyn Tool>],
    /// The harness tools that the mandate permits. `None` means every tool.
    allowed: Option<&'a BTreeSet<String>>,
    /// The flow labels that the run tree exercised before the turn.
    exercised: &'a BTreeSet<String>,
    /// The flow rules that a person released before the turn.
    released: &'a Released,
}

/// Whether the mandate permits the tool `name`.
///
/// The mandate never bounds a workflow tool, such as `delegate`. Its own
/// switch governs it. The `memory` tool writes state that later runs read,
/// so an allowlist must name it.
fn permits(builtins: &[Arc<dyn Tool>], allowed: Option<&BTreeSet<String>>, name: &str) -> bool {
    allowed.is_none_or(|allowed| allowed.contains(name))
        || (name != MEMORY_TOOL && builtins.iter().any(|tool| tool.name() == name))
}

/// The stricter of two decisions: a deny, then an approval, then an allow.
/// Two approvals keep the first reason.
fn strictest(first: ToolDecision, second: ToolDecision) -> ToolDecision {
    match (&first, &second) {
        (ToolDecision::Deny { .. }, _) => first,
        (_, ToolDecision::Deny { .. }) => second,
        (ToolDecision::RequireApproval { .. }, _) => first,
        (_, ToolDecision::RequireApproval { .. }) => second,
        _ => first,
    }
}

/// The tenant that scopes the response cache for one turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CacheTenant {
    /// The run has no tenant.
    None,
    /// The engine verified this tenant for the run.
    Verified(String),
    /// The task names this tenant.
    Declared(String),
    /// The turn skips the cache. For example, the activity cannot read the
    /// verified tenant.
    Skip,
}

impl CacheTenant {
    /// The tenant that a task field names, if any.
    pub(crate) fn declared(task: Option<&str>) -> Self {
        task.map_or(Self::None, |name| Self::Declared(name.to_owned()))
    }
}

/// A tool that the workflow runs itself, such as `schedule_followup`.
///
/// The policy sees it as a known tool with the `Internal` effect. The harness
/// never runs it.
#[derive(Debug)]
struct WorkflowTool(ToolDefinition);

impl Tool for WorkflowTool {
    fn name(&self) -> &str {
        &self.0.name
    }

    fn description(&self) -> &str {
        &self.0.description
    }

    fn input_schema(&self) -> serde_json::Value {
        self.0.input_schema.clone()
    }

    fn effect(&self) -> ToolEffect {
        ToolEffect::Internal
    }

    fn execute<'a>(
        &'a self,
        _input: serde_json::Value,
        _ctx: &'a ToolContext,
    ) -> BoxFuture<'a, Result<serde_json::Value, AgentError>> {
        let err = AgentError::new(
            crate::error::ErrorKind::Tool,
            format!("{} runs in the workflow, not in the harness", self.0.name),
        );
        Box::pin(std::future::ready(Err(err)))
    }

    fn definition(&self) -> ToolDefinition {
        self.0.clone()
    }
}

/// The receivers of the live events of one model call (issue #2268).
struct Live<'a> {
    run_id: String,
    step: u32,
    /// The publisher of the activity, for a request that streams.
    publish: Option<&'a (dyn Fn(&LiveEvent) + Send + Sync)>,
    /// The live sink of the harness.
    sink: Option<&'a dyn LiveSink>,
}

impl Live<'_> {
    const fn is_on(&self) -> bool {
        self.publish.is_some() || self.sink.is_some()
    }

    fn send(&self, event: &LiveEvent) {
        if let Some(publish) = self.publish {
            publish(event);
        }
        if let Some(sink) = self.sink {
            sink.send(event);
        }
    }

    fn delta(&self, text: String) {
        self.send(&LiveEvent::Delta {
            run_id: self.run_id.clone(),
            step: self.step,
            text,
        });
    }
}

/// Map a provider failure to an activity error payload.
///
/// A rate limit, a transport fault, and a provider outage produced no answer.
/// A retry can succeed, so they retry. Every other kind fails at once.
/// The retryable failure of a workspace load or save over its time budget.
/// The error type of a tool call whose workspace store timed out (issue
/// #2274). The workflow runs the call again from the same head.
pub(crate) const WORKSPACE_TIMEOUT: &str = "WorkspaceTimeout";

/// The error type of a tool call whose workspace store failed with an error
/// that a new try can fix (issue #2274). The workflow runs the call again.
pub(crate) const WORKSPACE_STORE_FAILED: &str = "WorkspaceStoreFailed";

/// The failure of a workspace store call. A retryable store error gets
/// [`WORKSPACE_STORE_FAILED`]. Any other keeps its own kind, so the call
/// does not run again.
fn workspace_failure(err: &AgentError) -> String {
    if err.kind().is_retryable() {
        ActivityFailure::retryable(WORKSPACE_STORE_FAILED, err.to_string()).into_error_payload()
    } else {
        model_failure(err)
    }
}

fn workspace_timeout() -> String {
    ActivityFailure::retryable(
        WORKSPACE_TIMEOUT,
        "the workspace store did not answer within the tool time budget",
    )
    .into_error_payload()
}

fn model_failure(err: &AgentError) -> String {
    let kind = format!("{:?}", err.kind());
    let message = err.to_string();
    let failure = if err.kind().is_retryable() {
        ActivityFailure::retryable(kind, message)
    } else {
        ActivityFailure::non_retryable(kind, message)
    };
    failure.into_error_payload()
}

/// The most bytes of tool output one result keeps under `cap`.
///
/// JSON escapes one byte into at most six (`\u0000`). A sixth of the cap,
/// less the outcome envelope, therefore always fits. A cap of zero is no cap,
/// so the whole output stays.
#[must_use]
pub fn max_tool_result_bytes(cap: u64) -> usize {
    share_of(cap, 6)
}

/// The most bytes of error text one result keeps under `cap`.
///
/// The text is escaped twice, so a byte can grow to twelve.
fn max_error_bytes(cap: u64) -> usize {
    share_of(cap, 12)
}

/// The share of `cap` for one result: (`cap` - envelope) / `divisor`. A cap
/// of zero is no cap, as in the engine.
fn share_of(cap: u64, divisor: u64) -> usize {
    if cap == 0 {
        return usize::MAX;
    }
    usize::try_from(cap.saturating_sub(OUTCOME_ENVELOPE_BYTES) / divisor).unwrap_or(usize::MAX)
}

/// Cut `text` so that its recorded outcome fits the result cap.
fn fit_result(text: String, cap: u64) -> String {
    cut_bytes(text, max_tool_result_bytes(cap))
}

/// Cut `text` to at most `max` bytes on a char boundary, and mark the cut.
fn cut_bytes(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    let mut end = max.saturating_sub(TRUNCATED.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATED}", &text[..end])
}

/// The most bytes that one decision reason keeps when a turn exceeds the cap.
///
/// A policy writes a reason as free text. 256 bytes still tell the reviewer or
/// the model why. A turn with many calls then stays small.
const REASON_CUT_BYTES: usize = 256;

/// Make `turn` fit the result cap, or replace it with a typed outcome.
///
/// A turn that exceeds the cap would fail the activity with a `PayloadTooLarge`
/// that is not retryable, after the model call was paid for. So the decision
/// reasons are cut first, because a policy writes them as free text. A turn
/// that still does not fit becomes [`ModelTurn::into_oversized`]. The loop
/// then ends the run under [`AgentStop::ReplyTooLarge`](crate::AgentStop). A
/// cap of zero is no cap.
///
/// The activity does not fail, so no failed-workflow alert fires. A warning
/// with the sizes marks the drop. It holds no text of the reply.
pub(crate) fn fit_turn(mut turn: ModelTurn, cap: u64) -> ModelTurn {
    if !exceeds_bytes(&turn, cap) {
        return turn;
    }
    for decision in &mut turn.decisions {
        if let ToolDecision::Deny { reason } | ToolDecision::RequireApproval { reason } = decision {
            *reason = cut_bytes(std::mem::take(reason), REASON_CUT_BYTES);
        }
    }
    let size = json_len(&turn);
    if size > cap {
        tracing::warn!(
            observed_bytes = size,
            cap_bytes = cap,
            "the model reply exceeds the result cap; the run ends as reply_too_large"
        );
        return turn.into_oversized();
    }
    turn
}

/// Cut `text` to `limit` characters and mark the cut.
pub(crate) fn truncate(text: &str, limit: usize) -> String {
    match text.char_indices().nth(limit) {
        None => text.to_owned(),
        Some((end, _)) => format!("{}{TRUNCATED}", &text[..end]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounds::json_len;
    use crate::error::ErrorKind;

    #[test]
    fn retryable_kinds_are_marked_retryable() {
        for (kind, retryable) in [
            (ErrorKind::RateLimited, true),
            (ErrorKind::Transport, true),
            (ErrorKind::Unavailable, true),
            (ErrorKind::Authentication, false),
            (ErrorKind::Provider, false),
            (ErrorKind::Config, false),
            (ErrorKind::Decode, false),
            (ErrorKind::Budget, false),
        ] {
            let payload = model_failure(&AgentError::new(kind, "boom"));
            let failure = autumn_harvest::failure::parse_typed_payload(&payload).unwrap();
            assert_eq!(failure.non_retryable, !retryable, "{kind:?}");
            assert!(failure.message.contains("boom"));
        }
    }

    #[test]
    fn a_result_of_any_content_fits_the_result_cap() {
        let cap = DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
        let max = max_tool_result_bytes(cap);
        for unit in ["x", "\u{0}", "é", "\""] {
            // At the limit, one byte under it, and far over it.
            for len in [max - 1, max, max + 1, 2 * max] {
                let text: String = unit.repeat(len).chars().take(len).collect();
                let outcome = ToolOutcome::ok(fit_result(text, cap));
                assert!(json_len(&outcome) <= cap, "{unit:?} {len}");
            }
        }
        assert_eq!(fit_result("short".into(), cap), "short");
        assert!(fit_result("x".repeat(100), 300).ends_with(TRUNCATED));
    }

    #[test]
    fn a_zero_result_cap_is_no_cap() {
        // The engine reads a result cap of zero as no cap.
        assert_eq!(max_tool_result_bytes(0), usize::MAX);
        let text = "x".repeat(10_000);
        assert_eq!(fit_result(text.clone(), 0), text);
    }

    #[test]
    fn a_zero_result_cap_keeps_a_whole_error() {
        let harness = AgentHarness::new(Arc::new(NoModel)).max_result_bytes(0);
        let message = "m".repeat(5_000);
        let outcome = harness.error_outcome(&message);
        assert!(outcome.is_error);
        assert!(!outcome.content.contains(TRUNCATED), "{}", outcome.content);
        assert!(outcome.content.contains(&message));
    }

    /// A turn with `content` and `decisions`, and fixed usage for checks.
    fn turn_of(content: Vec<ContentPart>, decisions: Vec<ToolDecision>) -> ModelTurn {
        ModelTurn {
            content,
            stop: crate::message::StopReason::EndTurn,
            usage: TokenUsage::new(7, 3),
            decisions,
            cache_hit: false,
            labels: Vec::new(),
            releases: Vec::new(),
            oversized: false,
            interruptible: Vec::new(),
        }
    }

    fn call(id: &str) -> ContentPart {
        ContentPart::ToolCall {
            id: id.to_owned(),
            name: "t".to_owned(),
            arguments: serde_json::json!({}),
        }
    }

    fn deny(reason: &str) -> ToolDecision {
        ToolDecision::Deny {
            reason: reason.to_owned(),
        }
    }

    #[test]
    fn a_turn_that_fits_is_left_alone() {
        let turn = turn_of(vec![call("c")], vec![deny("no")]);
        assert_eq!(fit_turn(turn.clone(), 10_000), turn);
    }

    #[test]
    fn a_long_reason_under_a_roomy_cap_stays_whole() {
        let turn = turn_of(vec![call("a")], vec![deny(&"r".repeat(5_000))]);
        assert_eq!(fit_turn(turn.clone(), 1_000_000), turn);
    }

    #[test]
    fn an_error_of_any_content_fits_the_result_cap() {
        // A NUL grows to seven bytes: six in the error JSON, and one more for
        // the backslash when the outcome escapes the text again.
        let cap = DEFAULT_MAX_ACTIVITY_RESULT_BYTES;
        let harness = AgentHarness::new(Arc::new(NoModel)).tool_output_limit(usize::MAX);
        for unit in ["\u{0}", "\"", "é"] {
            let message = unit.repeat(usize::try_from(cap).unwrap());
            let outcome = harness.error_outcome(&message);
            assert!(
                json_len(&outcome) <= cap,
                "{unit:?}: {}",
                json_len(&outcome)
            );
        }
    }

    #[test]
    fn a_zero_turn_cap_is_no_cap() {
        let turn = turn_of(vec![ContentPart::Text("y".repeat(10_000))], Vec::new());
        assert_eq!(fit_turn(turn.clone(), 0), turn);
    }

    #[test]
    fn long_reasons_are_cut_before_the_reply_is_dropped() {
        let approval = ToolDecision::RequireApproval {
            reason: "q".repeat(50_000),
        };
        let turn = turn_of(
            vec![call("a"), call("b")],
            vec![deny(&"r".repeat(50_000)), approval],
        );
        let fitted = fit_turn(turn.clone(), 4_096);

        assert!(!fitted.oversized);
        assert!(json_len(&fitted) <= 4_096, "{}", json_len(&fitted));
        assert_eq!(fitted.content, turn.content, "the calls stay as they were");
        assert_eq!(fitted.usage, turn.usage);
        let [first, second] = fitted.decisions.as_slice() else {
            panic!("one decision for each call: {:?}", fitted.decisions);
        };
        assert!(matches!(first, ToolDecision::Deny { .. }));
        assert!(matches!(second, ToolDecision::RequireApproval { .. }));
        for decision in [first, second] {
            let (ToolDecision::Deny { reason } | ToolDecision::RequireApproval { reason }) =
                decision
            else {
                panic!("a decision with a reason: {decision:?}");
            };
            assert!(reason.ends_with(TRUNCATED), "{reason}");
            // The documented limit, not the constant: a change must show here.
            assert!(reason.len() <= 256, "{}", reason.len());
        }
    }

    #[test]
    fn a_reason_is_cut_on_a_char_boundary() {
        // Three bytes per char. 256 less the marker is 242, which is not a
        // multiple of three, so the cut must step back to a boundary.
        let turn = turn_of(vec![call("a")], vec![deny(&"€".repeat(5_000))]);
        let fitted = fit_turn(turn, 2_048);
        let [ToolDecision::Deny { reason }] = fitted.decisions.as_slice() else {
            panic!("one cut reason: {:?}", fitted.decisions);
        };
        assert!(reason.ends_with(TRUNCATED));
        assert!(reason.len() <= 256, "{}", reason.len());
        assert!(reason.len() > 200, "the cut keeps most of the limit");
    }

    #[test]
    fn a_reply_that_still_does_not_fit_becomes_a_typed_outcome() {
        let turn = turn_of(
            vec![ContentPart::Text("y".repeat(10_000)), call("c")],
            vec![deny("no")],
        );
        let fitted = fit_turn(turn.clone(), 1_024);

        assert!(fitted.oversized);
        assert_eq!(fitted.content, Vec::new());
        assert_eq!(fitted.decisions, Vec::new());
        assert!(fitted.labels.is_empty() && fitted.releases.is_empty());
        assert_eq!(fitted.usage, turn.usage, "the paid call is still counted");
        assert_eq!(fitted.stop, turn.stop);
        assert!(json_len(&fitted) <= 1_024);
    }

    proptest::proptest! {
        #[test]
        fn a_fitted_turn_fits_or_is_the_typed_outcome(
            text in "(\\PC|[\\x00-\\x1f]){0,2000}",
            reasons in proptest::collection::vec("(\\PC|[\\x00-\\x1f]){0,2000}", 0..4),
            cap in 300_u64..6_000,
        ) {
            let content = std::iter::once(ContentPart::Text(text))
                .chain((0..reasons.len()).map(|index| call(&format!("c{index}"))))
                .collect();
            let decisions = reasons.iter().map(|reason| deny(reason)).collect();
            let turn = turn_of(content, decisions);

            proptest::prop_assert_eq!(&fit_turn(turn.clone(), 0), &turn);
            let fitted = fit_turn(turn.clone(), cap);
            if fitted.oversized {
                proptest::prop_assert_eq!(&fitted.content, &Vec::<ContentPart>::new());
                proptest::prop_assert_eq!(&fitted.decisions, &Vec::<ToolDecision>::new());
                proptest::prop_assert_eq!(fitted.usage, turn.usage);
            } else {
                proptest::prop_assert!(json_len(&fitted) <= cap);
                proptest::prop_assert_eq!(&fitted.content, &turn.content);
                proptest::prop_assert_eq!(fitted.decisions.len(), turn.decisions.len());
            }
            proptest::prop_assert_eq!(fit_turn(fitted.clone(), cap), fitted);
        }
    }

    #[test]
    fn the_typed_outcome_fits_a_small_cap() {
        // The builder docs say a cap of 256 bytes or more holds the outcome.
        // Check it with the widest usage numbers.
        let mut turn = turn_of(vec![call("c")], vec![deny("no")]);
        turn.usage = TokenUsage {
            input_tokens: u32::MAX,
            output_tokens: u32::MAX,
            cache_read_tokens: u32::MAX,
            cache_write_tokens: u32::MAX,
        };
        assert!(json_len(&turn.into_oversized()) <= 256);
    }

    #[test]
    fn the_default_budgets_fit_the_model_turn_start_to_close() {
        let turn = DEFAULT_MODEL_TIMEOUT
            + DEFAULT_POLICY_TIMEOUT
            + 3 * DEFAULT_CACHE_TIMEOUT
            + DEFAULT_BUDGET_TIMEOUT;
        assert!(turn < Duration::from_secs(15 * 60), "{turn:?}");
    }

    #[tokio::test]
    async fn a_stalled_budget_check_fails_retryable() {
        let harness =
            AgentHarness::new(Arc::new(NoModel)).budget_timeout(Duration::from_millis(20));
        let outcome = harness
            .check_budget(std::future::pending::<Result<(), String>>)
            .await;
        let failure = autumn_harvest::failure::parse_typed_payload(&outcome.unwrap_err()).unwrap();
        assert_eq!(
            failure.error_type,
            autumn_harvest::llm_budget::ERROR_TYPE_LLM_BUDGET_CHECK_FAILED
        );
        assert!(!failure.non_retryable);
    }

    #[tokio::test]
    async fn a_budget_refusal_passes_through() {
        let harness = AgentHarness::new(Arc::new(NoModel));
        let outcome = harness
            .check_budget(|| async { Err("refused".to_owned()) })
            .await;
        assert_eq!(outcome, Err("refused".to_owned()));
        assert_eq!(harness.check_budget(|| async { Ok(()) }).await, Ok(()));
    }

    /// A model that the budget tests never call.
    #[derive(Debug)]
    struct NoModel;

    impl AgentModel for NoModel {
        fn chat<'a>(
            &'a self,
            _request: &'a ChatRequest,
        ) -> crate::model::BoxFuture<'a, Result<crate::model::ChatResponse, AgentError>> {
            Box::pin(async { Err(AgentError::new(ErrorKind::Provider, "unused")) })
        }
    }

    #[test]
    fn truncate_cuts_on_a_char_boundary() {
        assert_eq!(truncate("abc", 3), "abc");
        assert_eq!(truncate("ééé", 2), "éé…[truncated]");
        assert_eq!(truncate("", 0), "");
    }
}
