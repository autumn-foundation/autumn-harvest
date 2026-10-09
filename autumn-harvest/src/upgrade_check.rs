//! Pre-deploy upgrade verdict for each in-flight run (issue #1995).
//!
//! The check runs inside the candidate build, because only that build holds
//! its own workflow types and codec keys. It gives each in-flight run one
//! [`Verdict`]:
//!
//! - [`Verdict::Migrate`]: the candidate build can take the run over.
//! - [`Verdict::Review`]: a person must look first.
//! - [`Verdict::Pin`]: the run must stay on its current build.
//!
//! Three checks give the verdict, one per failure mode:
//!
//! 1. **Determinism.** A canary replay of the recorded prefix under the
//!    candidate code. A divergence pins the run.
//! 2. **Rehydration.** The replay decodes each consumed payload into the
//!    candidate types. A candidate schema also checks each recorded signal
//!    and update payload. A signal that waits in `harvest_signals` is not in
//!    history yet, so it needs a schema. A failure pins the run.
//! 3. **Structural drift.** Two structure manifests from
//!    `cargo harvest-verify --emit-structure`, one per build, are diffed body
//!    by body. A changed body that the run may still execute needs review.
//!
//! The verdict is the worst finding: pin, then review, then migrate.
//! `DESIGN-1995.md` holds the full rules and the proof that a changed helper
//! is "passed".
//!
//! **Trust boundary.** The check decodes payloads in memory with the
//! candidate codecs. A [`RunVerdict`] holds ids, names, finding kinds and
//! event indexes. It never holds a payload or an error text, because a serde
//! error quotes the value it rejects. The database path reads only, inside a
//! read-only transaction.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::event::WorkflowEvent;
use crate::info::{
    QueryHandlerInfo, SignalHandlerInfo, UpdateHandlerInfo, WorkflowHandlerFn, WorkflowInfo,
};
use crate::payload_codec::PayloadCodecs;
use crate::testing::{HistorySnapshot, ReplayStatus, WorkflowReplayer};
use crate::types::{ExecutionId, ShardId};

/// The structure manifest format this module reads.
pub const STRUCTURE_FORMAT: &str = "harvest-structure/1";

/// The default time limit for the replay of one run.
pub const DEFAULT_REPLAY_TIMEOUT: Duration = Duration::from_secs(30);

/// The default limit of in-flight runs read from one shard.
pub const DEFAULT_LIMIT_PER_SHARD: usize = 10_000;

/// Step kinds that a run's history can prove complete.
const PROVABLE_KINDS: [&str; 7] = [
    "activity",
    "local-activity",
    "timer",
    "child",
    "side-effect",
    "version",
    "patch",
];

// ── errors ──────────────────────────────────────────────────────────────────

/// A bad input to the check.
#[derive(Debug, thiserror::Error)]
pub enum UpgradeCheckError {
    /// A structure manifest that does not parse or has another format.
    #[error("structure manifest: {0}")]
    Manifest(String),
    /// A file the check cannot read.
    #[error("cannot read {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// A bad command line.
    #[error("usage: {0}")]
    Usage(String),
}

// ── structure manifest ──────────────────────────────────────────────────────

/// The call graph of each workflow in one build.
///
/// `cargo harvest-verify --emit-structure FILE` writes it. These types read
/// the same JSON, so this crate needs no dependency on the verifier.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StructureManifest {
    pub format: String,
    pub model_version: String,
    pub rustc_version: String,
    pub workflows: Vec<WorkflowStructure>,
}

/// The call graph of one workflow.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowStructure {
    /// `crate::module::fn`.
    pub workflow: String,
    /// The registered workflow name.
    pub name: String,
    /// The id of the workflow's own body.
    pub root: String,
    /// Each `unknown` boundary, as `kind: detail`.
    #[serde(default)]
    pub boundaries: Vec<String>,
    pub bodies: Vec<BodyNode>,
}

/// One body in a workflow graph.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BodyNode {
    pub id: String,
    pub digest: String,
    #[serde(default)]
    pub calls: Vec<CallSite>,
    #[serde(default)]
    pub steps: Vec<StepSite>,
}

/// One call from a body to another body.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallSite {
    pub callee: String,
    /// The call sits in a loop of the caller.
    pub in_loop: bool,
    /// The call handles a future that another call site built. It does not
    /// start the callee.
    #[serde(default)]
    pub resume: bool,
}

/// One command a body emits.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSite {
    pub sink: String,
    /// The history record, such as `activity`.
    pub kind: String,
    /// The step name, when the manifest knows it.
    pub key: Option<String>,
    #[serde(default)]
    pub in_loop: bool,
}

impl StructureManifest {
    /// Parse a manifest and check its format.
    ///
    /// # Errors
    ///
    /// [`UpgradeCheckError::Manifest`] for bad JSON or another format.
    pub fn parse(json: &str) -> Result<Self, UpgradeCheckError> {
        let manifest: Self =
            serde_json::from_str(json).map_err(|e| UpgradeCheckError::Manifest(e.to_string()))?;
        if manifest.format != STRUCTURE_FORMAT {
            return Err(UpgradeCheckError::Manifest(format!(
                "format `{}` is not `{STRUCTURE_FORMAT}`",
                manifest.format
            )));
        }
        Ok(manifest)
    }

    /// Read and parse a manifest file.
    ///
    /// # Errors
    ///
    /// [`UpgradeCheckError::Io`] when the file cannot be read, and the
    /// errors of [`StructureManifest::parse`].
    pub fn load(path: &std::path::Path) -> Result<Self, UpgradeCheckError> {
        let text = std::fs::read_to_string(path).map_err(|source| UpgradeCheckError::Io {
            path: path.display().to_string(),
            source,
        })?;
        Self::parse(&text)
    }

    /// The graph of the workflow registered as `name`. Two graphs with one
    /// name are ambiguous, so the answer is then `None`.
    #[must_use]
    pub fn workflow(&self, name: &str) -> Option<&WorkflowStructure> {
        let mut found = self.workflows.iter().filter(|w| w.name == name);
        let first = found.next()?;
        found.next().is_none().then_some(first)
    }
}

impl WorkflowStructure {
    fn body(&self, id: &str) -> Option<&BodyNode> {
        self.bodies.iter().find(|b| b.id == id)
    }
}

// ── verdicts ────────────────────────────────────────────────────────────────

/// The verdict for one run. The order is the order of severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Verdict {
    /// The candidate build can take the run over.
    Migrate,
    /// A person must look first.
    Review,
    /// The run must stay on its current build.
    Pin,
}

impl Verdict {
    /// The lowercase name the report prints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Migrate => "migrate",
            Self::Review => "review",
            Self::Pin => "pin",
        }
    }
}

/// Why a run does not get migrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FindingKind {
    /// The candidate build registers no handler for the workflow type.
    WorkflowNotRegistered,
    /// The replay diverges inside the recorded prefix.
    Nondeterminism,
    /// The replay fails where the recorded run did not.
    ReplayFailed,
    /// A recorded payload breaks a candidate schema.
    PayloadSchemaViolation,
    /// The candidate codecs cannot decode the history.
    HistoryUndecodable,
    /// The replay did not finish in time.
    ReplayTimedOut,
    /// A signal or an open update has no candidate schema.
    PayloadUnchecked,
    /// A payload is offloaded, and the check has no offloader to read it.
    PayloadOffloaded,
    /// A manifest, or the workflow in a manifest, is missing.
    StructureUnavailable,
    /// The workflow graph has an `unknown` boundary.
    UnknownBoundary,
    /// The workflow's own body changed.
    RootChanged,
    /// A changed helper may still run for this run.
    StepNotPassed,
}

impl FindingKind {
    /// The verdict this finding forces.
    #[must_use]
    pub const fn verdict(self) -> Verdict {
        match self {
            Self::WorkflowNotRegistered
            | Self::Nondeterminism
            | Self::ReplayFailed
            | Self::PayloadSchemaViolation
            | Self::HistoryUndecodable => Verdict::Pin,
            Self::ReplayTimedOut
            | Self::PayloadUnchecked
            | Self::PayloadOffloaded
            | Self::StructureUnavailable
            | Self::UnknownBoundary
            | Self::RootChanged
            | Self::StepNotPassed => Verdict::Review,
        }
    }

    /// The kebab-case name the report prints.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkflowNotRegistered => "workflow-not-registered",
            Self::Nondeterminism => "nondeterminism",
            Self::ReplayFailed => "replay-failed",
            Self::PayloadSchemaViolation => "payload-schema-violation",
            Self::HistoryUndecodable => "history-undecodable",
            Self::ReplayTimedOut => "replay-timed-out",
            Self::PayloadUnchecked => "payload-unchecked",
            Self::PayloadOffloaded => "payload-offloaded",
            Self::StructureUnavailable => "structure-unavailable",
            Self::UnknownBoundary => "unknown-boundary",
            Self::RootChanged => "root-changed",
            Self::StepNotPassed => "step-not-passed",
        }
    }
}

/// One reason for a verdict.
///
/// `detail` names code and history positions only: body ids, step names,
/// signal names. It never holds a payload value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    pub kind: FindingKind,
    pub detail: String,
    /// The index of the history event the finding is about, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_index: Option<usize>,
}

impl Finding {
    fn new(kind: FindingKind, detail: impl Into<String>) -> Self {
        Self {
            kind,
            detail: detail.into(),
            event_index: None,
        }
    }

    const fn at(mut self, index: usize) -> Self {
        self.event_index = Some(index);
        self
    }
}

/// The verdict for one in-flight run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunVerdict {
    pub execution_id: ExecutionId,
    pub workflow_name: String,
    /// The shard the run lives on, when the check read it from a database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<ShardId>,
    /// The build the run is assigned to, when the check read it from a
    /// database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    pub verdict: Verdict,
    pub findings: Vec<Finding>,
}

impl RunVerdict {
    /// Add a finding, and raise the verdict to match it.
    #[cfg(feature = "db")]
    fn push(&mut self, finding: Finding) {
        self.verdict = self.verdict.max(finding.kind.verdict());
        self.findings.push(finding);
    }

    fn new(execution_id: ExecutionId, workflow_name: String, findings: Vec<Finding>) -> Self {
        let verdict = findings
            .iter()
            .map(|f| f.kind.verdict())
            .max()
            .unwrap_or(Verdict::Migrate);
        Self {
            execution_id,
            workflow_name,
            shard_id: None,
            build_id: None,
            verdict,
            findings,
        }
    }
}

/// The verdicts of one check.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeReport {
    pub runs: Vec<RunVerdict>,
    pub migrate: usize,
    pub review: usize,
    pub pin: usize,
    /// Why the check did not see every in-flight run. Empty when it did.
    #[serde(default)]
    pub incomplete: Vec<String>,
}

impl UpgradeReport {
    /// A report over `runs`, with the counts filled in.
    #[must_use]
    pub fn from_runs(runs: Vec<RunVerdict>) -> Self {
        let count = |v: Verdict| runs.iter().filter(|r| r.verdict == v).count();
        Self {
            migrate: count(Verdict::Migrate),
            review: count(Verdict::Review),
            pin: count(Verdict::Pin),
            runs,
            incomplete: Vec::new(),
        }
    }

    /// `0` when every run gets migrate, `1` when a run gets review or pin,
    /// and `2` when the check is incomplete.
    #[must_use]
    pub const fn exit_code(&self) -> i32 {
        if !self.incomplete.is_empty() {
            2
        } else if self.review > 0 || self.pin > 0 {
            1
        } else {
            0
        }
    }

    /// The report as pretty JSON.
    #[must_use]
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// The report as text: one line per run, then the findings, then a
    /// summary.
    #[must_use]
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        for run in &self.runs {
            let shard = run
                .shard_id
                .map(|s| format!(" shard={}", s.as_i32()))
                .unwrap_or_default();
            let _ = writeln!(
                out,
                "{:<7}  {}  {}{shard}",
                run.verdict.as_str(),
                run.execution_id,
                run.workflow_name
            );
            for finding in &run.findings {
                let at = finding
                    .event_index
                    .map(|i| format!(" (event {i})"))
                    .unwrap_or_default();
                let _ = writeln!(
                    out,
                    "           {}: {}{at}",
                    finding.kind.as_str(),
                    finding.detail
                );
            }
        }
        for reason in &self.incomplete {
            let _ = writeln!(out, "incomplete: {reason}");
        }
        let _ = writeln!(
            out,
            "checked {}: migrate {}, review {}, pin {}{}",
            self.runs.len(),
            self.migrate,
            self.review,
            self.pin,
            if self.incomplete.is_empty() {
                ""
            } else {
                "; the check is incomplete"
            }
        );
        out
    }
}

/// A signal that waits in `harvest_signals`. History holds no event for it
/// yet, so no replay consumes it.
///
/// `Debug` does not print the payload, because it can be plaintext.
#[derive(Clone, PartialEq, Eq)]
pub struct PendingSignal {
    pub signal_name: String,
    pub payload: serde_json::Value,
}

impl std::fmt::Debug for PendingSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSignal")
            .field("signal_name", &self.signal_name)
            .finish_non_exhaustive()
    }
}

/// A run as the database stores it: each payload still codec-encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedHistory {
    pub workflow_name: String,
    pub execution_id: ExecutionId,
    /// Each `harvest_events.event_data` value, in event order.
    pub event_data: Vec<serde_json::Value>,
    /// The run's pending signals, with encoded payloads.
    pub pending_signals: Vec<PendingSignal>,
}

// ── the check ───────────────────────────────────────────────────────────────

/// The pre-deploy check, configured with the candidate build.
pub struct UpgradeCheck {
    replayer: WorkflowReplayer,
    input_schemas: HashMap<String, fn() -> serde_json::Value>,
    signal_schemas: HashMap<(String, String), fn() -> serde_json::Value>,
    update_schemas: HashMap<(String, String), fn() -> serde_json::Value>,
    updates: Vec<UpdateHandlerInfo>,
    queries: Vec<QueryHandlerInfo>,
    structure: Option<(StructureManifest, StructureManifest)>,
    codecs: Arc<PayloadCodecs>,
    offloader: Option<Arc<crate::payload_store::PayloadOffloader>>,
    replay_timeout: Duration,
}

impl Default for UpgradeCheck {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for UpgradeCheck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpgradeCheck")
            .field("workflows", &self.replayer.registered_workflow_names())
            .field("has_structure", &self.structure.is_some())
            .field("replay_timeout", &self.replay_timeout)
            .finish_non_exhaustive()
    }
}

impl UpgradeCheck {
    /// A check with no workflow, no manifest and the default codecs.
    #[must_use]
    pub fn new() -> Self {
        Self {
            replayer: WorkflowReplayer::new(),
            input_schemas: HashMap::new(),
            signal_schemas: HashMap::new(),
            update_schemas: HashMap::new(),
            updates: Vec::new(),
            queries: Vec::new(),
            structure: None,
            codecs: Arc::new(PayloadCodecs::default()),
            offloader: None,
            replay_timeout: DEFAULT_REPLAY_TIMEOUT,
        }
    }

    /// Register the candidate build's workflows, with their input schemas.
    #[must_use]
    pub fn register(mut self, workflows: Vec<WorkflowInfo>) -> Self {
        for info in &workflows {
            if let Some(schema) = info.input_schema {
                self.input_schemas.insert(info.name.to_string(), schema);
            }
        }
        self.replayer = self.replayer.register(workflows);
        self
    }

    /// Register one candidate handler by name.
    #[must_use]
    pub fn register_fn(mut self, name: impl Into<String>, handler: WorkflowHandlerFn) -> Self {
        self.replayer = self.replayer.register_fn(name, handler);
        self
    }

    /// The candidate build's signal declarations. Each schema validates the
    /// recorded payloads of its signal.
    #[must_use]
    pub fn signals(mut self, signals: Vec<SignalHandlerInfo>) -> Self {
        for signal in signals {
            if let Some(schema) = signal.arg_schema {
                self.signal_schemas.insert(
                    (signal.workflow.to_string(), signal.name.to_string()),
                    schema,
                );
            }
        }
        self
    }

    /// The candidate build's update handlers. The replay registers them, and
    /// each schema validates the recorded inputs of its update.
    #[must_use]
    pub fn updates(mut self, updates: Vec<UpdateHandlerInfo>) -> Self {
        for update in &updates {
            if let Some(schema) = update.arg_schema {
                self.update_schemas.insert(
                    (update.workflow.to_string(), update.name.to_string()),
                    schema,
                );
            }
        }
        self.updates.extend(updates);
        self.replayer = self.replayer.updates(self.updates.clone());
        self
    }

    /// The candidate build's query handlers. The worker registers them before
    /// workflow code runs, so the replay registers them too. A workflow can
    /// branch on `ctx.list_query_names()`.
    #[must_use]
    pub fn queries(mut self, queries: Vec<QueryHandlerInfo>) -> Self {
        self.queries.extend(queries);
        self.replayer = self.replayer.queries(self.queries.clone());
        self
    }

    /// The candidate worker's payload caps, in bytes: activity input, signal
    /// payload and workflow input. `0` means no cap. The replay applies them
    /// to the commands a run sends next. Pass the values of the candidate
    /// `HarvestBuilder`.
    #[must_use]
    pub fn with_payload_caps(
        mut self,
        max_activity_input: u64,
        max_signal_payload: u64,
        max_workflow_input: u64,
    ) -> Self {
        self.replayer = self.replayer.with_payload_caps(
            max_activity_input,
            max_signal_payload,
            max_workflow_input,
        );
        self
    }

    /// Configure the replayer further, for example with shared state or the
    /// candidate build id.
    #[must_use]
    pub fn map_replayer(mut self, f: impl FnOnce(WorkflowReplayer) -> WorkflowReplayer) -> Self {
        self.replayer = f(self.replayer);
        self
    }

    /// The structure manifests of the current build and the candidate build.
    #[must_use]
    pub fn with_structure(
        mut self,
        baseline: StructureManifest,
        candidate: StructureManifest,
    ) -> Self {
        self.structure = Some((baseline, candidate));
        self
    }

    /// The candidate build's codecs. They decode each history in memory.
    #[must_use]
    pub fn with_codecs(mut self, codecs: Arc<PayloadCodecs>) -> Self {
        self.codecs = codecs;
        self
    }

    /// The candidate build's payload offloader. The check reads offloaded
    /// payloads back through it, in memory.
    #[must_use]
    pub fn with_offloader(
        mut self,
        offloader: Arc<crate::payload_store::PayloadOffloader>,
    ) -> Self {
        // The check inflates at its input, before it decodes. The replayer
        // gets no offloader, so it never inflates a payload twice: a value
        // that itself looks like a claim check stays as it is.
        // The replayer still gets the threshold. The worker offloads a payload
        // above it, so the payload cap does not reject that payload.
        self.replayer = self
            .replayer
            .with_payload_offload_threshold(Some(offloader.threshold()));
        self.offloader = Some(offloader);
        self
    }

    /// The time limit for the replay of one run.
    #[must_use]
    pub const fn with_replay_timeout(mut self, timeout: Duration) -> Self {
        self.replay_timeout = timeout;
        self
    }

    /// Give one run its verdict.
    pub async fn check_snapshot(&self, snapshot: HistorySnapshot) -> RunVerdict {
        self.check_snapshot_with(snapshot, &[]).await
    }

    /// Give one run its verdict, with the signals that wait for it.
    pub async fn check_snapshot_with(
        &self,
        snapshot: HistorySnapshot,
        pending_signals: &[PendingSignal],
    ) -> RunVerdict {
        let name = snapshot.workflow_name.clone();
        let execution_id = snapshot.execution_id;
        let mut findings = Vec::new();
        if !self.replayer.is_workflow_registered(&name) {
            findings.push(Finding::new(
                FindingKind::WorkflowNotRegistered,
                format!("the candidate build has no handler for `{name}`"),
            ));
        } else if self.offloader.is_none() && has_offloaded_payload(&snapshot.events) {
            // A claim-check stub is not the payload. Replay and schema checks
            // over it would pin a run that may well fit, so neither runs.
            findings.push(Finding::new(
                FindingKind::PayloadOffloaded,
                "the run holds offloaded payloads; pass the offloader to check them",
            ));
        } else {
            findings.extend(self.replay_findings(&snapshot).await);
            findings.extend(self.payload_findings(&name, &snapshot.events, pending_signals));
        }
        findings.extend(self.structure_findings(&name, &snapshot.events));
        RunVerdict::new(execution_id, name, findings)
    }

    /// Decode a stored history with the candidate codecs, then give it its
    /// verdict. The plaintext stays in memory.
    pub async fn check_encoded(&self, history: EncodedHistory) -> RunVerdict {
        let mut events = Vec::with_capacity(history.event_data.len());
        for (index, mut value) in history.event_data.into_iter().enumerate() {
            // The worker encodes, then offloads. So the check inflates first,
            // then decodes, as `load_history_inflated` does.
            if let Some(offloader) = &self.offloader
                && offloader.inflate_event_value(&mut value).await.is_err()
            {
                return undecodable(history.execution_id, history.workflow_name, Some(index));
            }
            match self.codecs.decode_event(value) {
                Ok(event) => events.push(event),
                Err(_) => {
                    return undecodable(history.execution_id, history.workflow_name, Some(index));
                }
            }
        }
        let mut pending = Vec::with_capacity(history.pending_signals.len());
        for signal in history.pending_signals {
            match self.codecs.decode_column(&signal.payload) {
                Ok(payload) => pending.push(PendingSignal {
                    signal_name: signal.signal_name,
                    payload,
                }),
                Err(_) => {
                    return undecodable(history.execution_id, history.workflow_name, None);
                }
            }
        }
        let snapshot = HistorySnapshot {
            workflow_name: history.workflow_name,
            execution_id: history.execution_id,
            events,
            context_headers: None,
            execution_timeout: None,
            deadline_at: None,
            parent_execution_id: None,
            workflow_id: None,
            queue_name: None,
        };
        self.check_snapshot_with(snapshot, &pending).await
    }

    async fn replay_findings(&self, snapshot: &HistorySnapshot) -> Vec<Finding> {
        let replay = self.replayer.replay_canary_snapshot(snapshot.clone());
        let Ok(report) = tokio::time::timeout(self.replay_timeout, replay).await else {
            return vec![Finding::new(
                FindingKind::ReplayTimedOut,
                format!(
                    "the replay ran longer than {} s",
                    self.replay_timeout.as_secs()
                ),
            )];
        };
        match report.status {
            ReplayStatus::ReplaySucceeded => Vec::new(),
            ReplayStatus::NonDeterminismDetected {
                kind, event_index, ..
            } => vec![
                Finding::new(
                    FindingKind::Nondeterminism,
                    format!("{kind:?}: the candidate code diverges from the recorded history"),
                )
                .at(event_index),
            ],
            ReplayStatus::WorkflowFailed { event_index, .. } => vec![
                Finding::new(
                    FindingKind::ReplayFailed,
                    "the candidate code fails the run on replay; a recorded payload may not fit \
                     the candidate types",
                )
                .at(event_index),
            ],
        }
    }

    /// Schema findings for recorded and pending payloads.
    ///
    /// The replay consumes each recorded payload, except an open update and a
    /// buffered signal. A candidate schema, when there is one, still checks
    /// it. A pending signal is not in history yet, so no replay reads it. So
    /// each signal, recorded or pending, and each open update need a schema.
    fn payload_findings(
        &self,
        workflow: &str,
        events: &[WorkflowEvent],
        pending_signals: &[PendingSignal],
    ) -> Vec<Finding> {
        let finished: BTreeSet<String> = events
            .iter()
            .filter_map(|e| match e {
                WorkflowEvent::UpdateCompleted { update_id, .. }
                | WorkflowEvent::UpdateFailed { update_id, .. } => Some(update_id.to_string()),
                _ => None,
            })
            .collect();
        let mut findings = Vec::new();
        for (index, event) in events.iter().enumerate() {
            let checked = match event {
                WorkflowEvent::WorkflowStarted { input, .. } => self
                    .input_schemas
                    .get(workflow)
                    .and_then(|schema| schema_finding("workflow input", workflow, *schema, input)),
                WorkflowEvent::SignalReceived {
                    signal_name,
                    payload,
                } => {
                    // A recorded signal can wait in the workflow's buffer while
                    // the run waits on another step. Replay then passes it with
                    // no decode, so only a schema can check it.
                    let key = (workflow.to_string(), signal_name.clone());
                    self.signal_schemas.get(&key).map_or_else(
                        || {
                            Some(Finding::new(
                                FindingKind::PayloadUnchecked,
                                format!(
                                    "recorded signal `{signal_name}` has no schema in the candidate build"
                                ),
                            ))
                        },
                        |schema| schema_finding("signal", signal_name, *schema, payload),
                    )
                }
                WorkflowEvent::UpdateAdmitted {
                    update_id,
                    name,
                    input,
                    ..
                } => match self
                    .update_schemas
                    .get(&(workflow.to_string(), name.clone()))
                {
                    Some(schema) => schema_finding("update", name, *schema, input),
                    // The canary does not consume an open update, so only a
                    // schema can check it.
                    None if !finished.contains(&update_id.to_string()) => Some(Finding::new(
                        FindingKind::PayloadUnchecked,
                        format!("open update `{name}` has no schema in the candidate build"),
                    )),
                    None => None,
                },
                _ => None,
            };
            findings.extend(checked.map(|f| f.at(index)));
        }
        for signal in pending_signals {
            let name = signal.signal_name.as_str();
            let key = (workflow.to_string(), name.to_string());
            findings.extend(self.signal_schemas.get(&key).map_or_else(
                || {
                    Some(Finding::new(
                        FindingKind::PayloadUnchecked,
                        format!("pending signal `{name}` has no schema in the candidate build"),
                    ))
                },
                |schema| schema_finding("pending signal", name, *schema, &signal.payload),
            ));
        }
        findings
    }

    fn structure_findings(&self, workflow: &str, events: &[WorkflowEvent]) -> Vec<Finding> {
        let Some((baseline, candidate)) = &self.structure else {
            return vec![Finding::new(
                FindingKind::StructureUnavailable,
                "no structure manifests were given",
            )];
        };
        // Digests from two toolchains or two models are not comparable.
        if baseline.rustc_version != candidate.rustc_version
            || baseline.model_version != candidate.model_version
        {
            return vec![Finding::new(
                FindingKind::StructureUnavailable,
                "the two manifests come from different toolchains or models",
            )];
        }
        let (Some(old), Some(new)) = (baseline.workflow(workflow), candidate.workflow(workflow))
        else {
            return vec![Finding::new(
                FindingKind::StructureUnavailable,
                format!("`{workflow}` is missing from a manifest, or two workflows share its name"),
            )];
        };
        let mut findings = Vec::new();
        // A declarative update handler is not a workflow entry, so the
        // manifest does not show its code.
        if self.updates.iter().any(|u| u.workflow == workflow) {
            findings.push(Finding::new(
                FindingKind::UnknownBoundary,
                "declarative update handlers are not in the structure manifest",
            ));
        }
        let boundaries: BTreeSet<&str> = old
            .boundaries
            .iter()
            .chain(&new.boundaries)
            .map(String::as_str)
            .collect();
        if let Some(first) = boundaries.first() {
            findings.push(Finding::new(
                FindingKind::UnknownBoundary,
                format!(
                    "{} boundar{}, first: {first}",
                    boundaries.len(),
                    plural_y(boundaries.len())
                ),
            ));
        }
        let facts = HistoryFacts::from_events(events);
        for id in changed_bodies(old, new) {
            if id == old.root || id == new.root {
                findings.push(Finding::new(
                    FindingKind::RootChanged,
                    format!("the workflow body `{id}` changed"),
                ));
                continue;
            }
            // The run ran the baseline code and the candidate takes it over.
            // So the proof must hold in each graph that has the body. A wait
            // that only the baseline helper holds is still open for this run.
            let passed = [old, new]
                .into_iter()
                .filter(|graph| graph.body(&id).is_some())
                .all(|graph| is_passed(graph, &id, &facts));
            if !passed {
                findings.push(Finding::new(
                    FindingKind::StepNotPassed,
                    format!("`{id}` changed, and this run may still execute it"),
                ));
            }
        }
        findings
    }
}

/// A violation finding when `payload` breaks `schema`. The detail counts the
/// violations and quotes none of them, because a violation can quote the
/// value.
fn schema_finding(
    role: &str,
    name: &str,
    schema: fn() -> serde_json::Value,
    payload: &serde_json::Value,
) -> Option<Finding> {
    let violations = crate::info::validate_against_schema(&schema(), payload).err()?;
    Some(Finding::new(
        FindingKind::PayloadSchemaViolation,
        format!(
            "{role} `{name}` breaks the candidate schema ({} violation{})",
            violations.len(),
            if violations.len() == 1 { "" } else { "s" }
        ),
    ))
}

/// A finding when the baseline manifest may not describe the code the run ran.
///
/// A run with no assigned build ran whatever the fleet ran, so the operator
/// vouches for it. A run assigned to another build than the baseline ran
/// other code. With no baseline build named, the check cannot tell.
#[cfg(feature = "db")]
fn build_finding(run_build: Option<&str>, baseline_build: Option<&str>) -> Option<Finding> {
    let run_build = run_build?;
    match baseline_build {
        Some(baseline) if baseline == run_build => None,
        Some(baseline) => Some(Finding::new(
            FindingKind::StructureUnavailable,
            format!("the run is assigned to build `{run_build}`, not the baseline `{baseline}`"),
        )),
        None => Some(Finding::new(
            FindingKind::StructureUnavailable,
            format!("the run is assigned to build `{run_build}`; name the baseline build"),
        )),
    }
}

/// The shard a run belongs to, among the shard ids that alias its database.
///
/// The id encodes the shard the run started on. A rebalanced run keeps that
/// id but lives elsewhere. So an encoded shard outside the group, or no
/// encoded shard, gives the group's first shard id.
#[cfg(feature = "db")]
fn shard_of(execution_id: ExecutionId, shards: &[ShardId]) -> ShardId {
    let encoded = execution_id.shard();
    if !encoded.is_unencoded() && shards.contains(&encoded) {
        return encoded;
    }
    shards.first().copied().unwrap_or(encoded)
}

/// A payload field of `events` is a claim-check reference.
///
/// The offloader replaces only the direct payload fields of an event's
/// `data`, and never a pending signal. So only those fields count. A nested
/// value with the same shape is business data.
fn has_offloaded_payload(events: &[WorkflowEvent]) -> bool {
    events
        .iter()
        .filter_map(|e| serde_json::to_value(e).ok())
        .any(|v| {
            v.get("data")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|data| {
                    crate::payload_store::PAYLOAD_FIELD_KEYS
                        .iter()
                        .filter_map(|key| data.get(*key))
                        .any(crate::payload_store::is_offload_envelope)
                })
        })
}

const fn plural_y(n: usize) -> &'static str {
    if n == 1 { "y" } else { "ies" }
}

fn undecodable(
    execution_id: ExecutionId,
    workflow_name: String,
    index: Option<usize>,
) -> RunVerdict {
    let mut finding = Finding::new(
        FindingKind::HistoryUndecodable,
        "the candidate codecs cannot decode this history",
    );
    finding.event_index = index;
    RunVerdict::new(execution_id, workflow_name, vec![finding])
}

// ── structural rules ────────────────────────────────────────────────────────

/// Body ids whose digest differs, or that only one build has. Sorted.
fn changed_bodies(old: &WorkflowStructure, new: &WorkflowStructure) -> Vec<String> {
    let digests = |w: &WorkflowStructure| -> BTreeMap<String, String> {
        w.bodies
            .iter()
            .map(|b| (b.id.clone(), b.digest.clone()))
            .collect()
    };
    let (a, b) = (digests(old), digests(new));
    let ids: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    let mut changed: Vec<String> = ids
        .into_iter()
        .filter(|id| a.get(*id) != b.get(*id))
        .cloned()
        .collect();
    if old.root != new.root {
        changed.push(new.root.clone());
    }
    changed.sort();
    changed.dedup();
    changed
}

/// Whether the changed body `id` is behind the run for good.
///
/// `DESIGN-1995.md` §3 lists the conditions and why they suffice.
fn is_passed(graph: &WorkflowStructure, id: &str, facts: &HistoryFacts) -> bool {
    if id == graph.root || !runs_at_most_once(graph, id, &mut BTreeSet::new()) {
        return false;
    }
    let subtree = subtree(graph, id);
    if !is_closed_subtree(graph, id, &subtree) {
        return false;
    }
    // How many step sites the subtree has for each key.
    let mut sites: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    for body in graph
        .bodies
        .iter()
        .filter(|b| subtree.contains(b.id.as_str()))
    {
        for step in &body.steps {
            let Some(key) = step.key.as_deref() else {
                return false;
            };
            if step.in_loop || !PROVABLE_KINDS.contains(&step.kind.as_str()) {
                return false;
            }
            let count = sites.entry((step.kind.as_str(), key)).or_insert(0);
            *count = count.saturating_add(1);
        }
    }
    if sites.is_empty() {
        return false;
    }
    // A step of the same kind outside the subtree can complete the same key,
    // when it has that key or an unknown one.
    let kinds: BTreeSet<&str> = sites.keys().map(|(kind, _)| *kind).collect();
    let shared = graph
        .bodies
        .iter()
        .filter(|b| !subtree.contains(b.id.as_str()))
        .flat_map(|b| &b.steps)
        .any(|s| {
            kinds.contains(s.kind.as_str())
                && s.key
                    .as_deref()
                    .is_none_or(|key| sites.contains_key(&(s.kind.as_str(), key)))
        });
    !shared
        && sites.iter().all(|(&(kind, key), &n)| {
            facts.completed_count(kind, key) >= n && !facts.is_pending(kind, key)
        })
        && facts.decided_after(sites.keys().copied())
}

/// No body in the subtree, other than `id`, is entered from outside it, and
/// no call inside it can repeat.
///
/// So the subtree runs only as part of one run of `id`, and each body in it
/// starts at most once per run of `id`. A future built outside and awaited
/// inside fails the first rule.
fn is_closed_subtree(graph: &WorkflowStructure, id: &str, subtree: &BTreeSet<&str>) -> bool {
    graph.bodies.iter().all(|body| {
        let from_inside = subtree.contains(body.id.as_str());
        body.calls.iter().all(|call| {
            let target_inside = subtree.contains(call.callee.as_str());
            if !target_inside || call.callee == id {
                return true;
            }
            from_inside && !call.in_loop
        })
    })
}

/// The body starts at most once per run: one starting call site, outside
/// any loop, on each body up to the root.
fn runs_at_most_once<'g>(
    graph: &'g WorkflowStructure,
    id: &'g str,
    seen: &mut BTreeSet<&'g str>,
) -> bool {
    if id == graph.root {
        return true;
    }
    if !seen.insert(id) {
        return false;
    }
    let starts: Vec<(&str, &CallSite)> = graph
        .bodies
        .iter()
        .flat_map(|b| b.calls.iter().map(move |c| (b.id.as_str(), c)))
        .filter(|(_, c)| c.callee == id && !c.resume)
        .collect();
    match starts.as_slice() {
        [(caller, site)] => !site.in_loop && runs_at_most_once(graph, caller, seen),
        _ => false,
    }
}

/// `id` and every body reachable from it.
fn subtree<'g>(graph: &'g WorkflowStructure, id: &'g str) -> BTreeSet<&'g str> {
    let mut out = BTreeSet::new();
    let mut stack = vec![id];
    while let Some(next) = stack.pop() {
        if !out.insert(next) {
            continue;
        }
        if let Some(body) = graph.body(next) {
            stack.extend(body.calls.iter().map(|c| c.callee.as_str()));
        }
    }
    out
}

/// What a run's history says about each step key.
#[derive(Debug, Default)]
struct HistoryFacts {
    /// How many times the run completed each key.
    completed: BTreeMap<(String, String), usize>,
    /// Keys with an instance still open.
    pending: BTreeSet<(String, String)>,
    /// The last event index that completed each key from outside a decision:
    /// an activity, local activity, timer or child result.
    resolved_at: BTreeMap<(String, String), usize>,
    /// The last event index that a decision wrote.
    last_decision: Option<usize>,
}

impl HistoryFacts {
    fn from_events(events: &[WorkflowEvent]) -> Self {
        let mut open: HashMap<String, (&'static str, String)> = HashMap::new();
        let mut timers: HashMap<String, usize> = HashMap::new();
        let mut facts = Self::default();
        for (index, event) in events.iter().enumerate() {
            if is_decision_event(event) {
                facts.last_decision = Some(index);
            }
            match event {
                WorkflowEvent::ActivityScheduled {
                    activity_id, name, ..
                }
                | WorkflowEvent::ActivityAwaitingExternal {
                    activity_id, name, ..
                } => {
                    open.insert(activity_id.to_string(), ("activity", name.clone()));
                }
                WorkflowEvent::LocalActivityScheduled {
                    activity_id, name, ..
                } => {
                    open.insert(activity_id.to_string(), ("local-activity", name.clone()));
                }
                WorkflowEvent::ActivityCompleted { activity_id, .. }
                | WorkflowEvent::ActivityCompletedExternally { activity_id, .. }
                | WorkflowEvent::LocalActivityCompleted { activity_id, .. } => {
                    if let Some((kind, name)) = open.remove(&activity_id.to_string()) {
                        facts.resolve(kind, &name, index);
                    }
                }
                WorkflowEvent::ChildWorkflowStarted {
                    child_id,
                    workflow_name,
                    ..
                } => {
                    open.insert(child_id.to_string(), ("child", workflow_name.clone()));
                }
                WorkflowEvent::ChildWorkflowCompleted { child_id, .. }
                | WorkflowEvent::ChildWorkflowFailed { child_id, .. } => {
                    if let Some((kind, name)) = open.remove(&child_id.to_string()) {
                        facts.resolve(kind, &name, index);
                    }
                }
                WorkflowEvent::ChildWorkflowSpawnedDetached { workflow_name, .. } => {
                    facts.done("child", workflow_name);
                }
                WorkflowEvent::TimerStarted { timer_id, .. } => {
                    let count = timers.entry(timer_id.to_string()).or_insert(0);
                    *count = count.saturating_add(1);
                }
                WorkflowEvent::TimerFired { timer_id }
                | WorkflowEvent::TimerCancelled { timer_id } => {
                    let key = timer_id.to_string();
                    if let Some(count) = timers.get_mut(&key)
                        && *count > 0
                    {
                        *count = count.saturating_sub(1);
                        facts.resolve("timer", &key, index);
                    }
                }
                WorkflowEvent::SideEffectRecorded {
                    name: Some(name), ..
                } => facts.done("side-effect", name),
                WorkflowEvent::MarkerRecorded { name, .. } => {
                    if let Some(change) = name.strip_prefix("version:") {
                        facts.done("version", change);
                    } else if let Some(patch) = name.strip_prefix("patch:") {
                        facts.done("patch", patch);
                    }
                }
                _ => {}
            }
        }
        for (kind, name) in open.into_values() {
            facts.pending.insert((kind.to_string(), name));
        }
        for (key, count) in timers {
            if count > 0 {
                facts.pending.insert(("timer".to_string(), key));
            }
        }
        facts
    }

    /// A decision wrote this completion itself, such as a side effect.
    fn done(&mut self, kind: &str, key: &str) {
        let count = self
            .completed
            .entry((kind.to_string(), key.to_string()))
            .or_insert(0);
        *count = count.saturating_add(1);
    }

    /// The engine wrote this completion at `index`, outside a decision.
    fn resolve(&mut self, kind: &str, key: &str, index: usize) {
        self.done(kind, key);
        self.resolved_at
            .insert((kind.to_string(), key.to_string()), index);
    }

    fn completed_count(&self, kind: &str, key: &str) -> usize {
        self.completed
            .get(&(kind.to_string(), key.to_string()))
            .copied()
            .unwrap_or(0)
    }

    fn is_pending(&self, kind: &str, key: &str) -> bool {
        self.pending.contains(&(kind.to_string(), key.to_string()))
    }

    /// A decision ran after the last outside completion of `keys`.
    ///
    /// The engine appends an activity, timer or child result outside any
    /// decision. The code after that await runs only in the next decision.
    /// A decision event later in history proves that it ran, because a
    /// decision writes at the history position it loaded.
    fn decided_after<'k>(&self, keys: impl IntoIterator<Item = (&'k str, &'k str)>) -> bool {
        let last = keys
            .into_iter()
            .filter_map(|(kind, key)| {
                self.resolved_at
                    .get(&(kind.to_string(), key.to_string()))
                    .copied()
            })
            .max();
        last.is_none_or(|resolved| self.last_decision.is_some_and(|d| d > resolved))
    }
}

/// An event that only a workflow decision writes.
const fn is_decision_event(event: &WorkflowEvent) -> bool {
    matches!(
        event,
        WorkflowEvent::ActivityScheduled { .. }
            | WorkflowEvent::LocalActivityScheduled { .. }
            | WorkflowEvent::ActivityAwaitingExternal { .. }
            | WorkflowEvent::TimerStarted { .. }
            | WorkflowEvent::TimerCancelled { .. }
            | WorkflowEvent::ChildWorkflowStarted { .. }
            | WorkflowEvent::ChildWorkflowSpawnedDetached { .. }
            | WorkflowEvent::MarkerRecorded { .. }
            | WorkflowEvent::SideEffectRecorded { .. }
            | WorkflowEvent::ExternalSignalRequested { .. }
            | WorkflowEvent::ExternalCancelRequested { .. }
            | WorkflowEvent::ExternalAwaitRequested { .. }
            | WorkflowEvent::DecisionCommitted { .. }
    )
}

// ── the database path ───────────────────────────────────────────────────────

/// Which in-flight runs [`UpgradeCheck::run`] reads.
#[cfg(feature = "db")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeCheckOptions {
    /// Only runs of this workflow type.
    pub workflow_name: Option<String>,
    /// The most runs read from one shard. More runs make the check
    /// incomplete.
    pub limit_per_shard: usize,
    /// The build that the baseline manifest describes. The check then reads
    /// only runs assigned to it, or to no build: compat from the candidate
    /// covers no other run. With no baseline build, each run with an
    /// assigned build needs review.
    pub baseline_build_id: Option<String>,
}

#[cfg(feature = "db")]
impl Default for UpgradeCheckOptions {
    fn default() -> Self {
        Self {
            workflow_name: None,
            limit_per_shard: DEFAULT_LIMIT_PER_SHARD,
            baseline_build_id: None,
        }
    }
}

/// One in-flight execution row.
#[cfg(feature = "db")]
#[derive(diesel::QueryableByName)]
struct InFlightRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Jsonb>)]
    context_headers: Option<serde_json::Value>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Interval>)]
    execution_timeout: Option<chrono::Duration>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
    deadline_at: Option<chrono::DateTime<chrono::Utc>>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Uuid>)]
    parent_id: Option<uuid::Uuid>,
    #[diesel(sql_type = diesel::sql_types::Text)]
    workflow_id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    queue_name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    assigned_build_id: Option<String>,
}

/// The in-flight runs of one shard, oldest first. `$1` is the state list,
/// `$2` the optional workflow name, `$3` the row limit and `$4` the optional
/// baseline build.
///
/// Compat from the candidate build covers only runs assigned to the baseline
/// build, or to no build. With a baseline build named, the scan reads those
/// runs only. A run on the candidate build, or on an older build, is outside
/// the deploy decision.
#[cfg(feature = "db")]
const IN_FLIGHT_SQL: &str = "SELECT id, workflow_name, context_headers, execution_timeout, \
     deadline_at, parent_id, workflow_id, queue_name, assigned_build_id \
     FROM harvest_workflow_executions \
     WHERE state = ANY($1) AND ($2::text IS NULL OR workflow_name = $2) \
     AND ($4::text IS NULL OR assigned_build_id IS NULL OR assigned_build_id = $4) \
     ORDER BY created_at, id LIMIT $3";

#[cfg(feature = "db")]
impl UpgradeCheck {
    /// Give each in-flight run on every shard its verdict.
    ///
    /// The check reads inside read-only transactions and writes nothing. A
    /// shard that fails, or that holds more runs than the limit, makes the
    /// report incomplete.
    pub async fn run(
        &self,
        pool: &crate::shard::ShardedDbPool,
        options: &UpgradeCheckOptions,
    ) -> UpgradeReport {
        let mut runs = Vec::new();
        let mut incomplete = Vec::new();
        // Two shard ids can share one physical database (#1266). Read each
        // database once, under its first shard id.
        for (shard_pool, shards) in pool.pool_groups() {
            let Some(&shard) = shards.first() else {
                continue;
            };
            match self.run_shard(&shards, shard_pool, options).await {
                Ok((shard_runs, truncated)) => {
                    runs.extend(shard_runs);
                    if truncated {
                        incomplete.push(format!(
                            "shard {}: more than {} in-flight runs; raise the limit",
                            shard.as_i32(),
                            options.limit_per_shard
                        ));
                    }
                }
                Err(e) => incomplete.push(format!("shard {}: {e}", shard.as_i32())),
            }
        }
        let mut report = UpgradeReport::from_runs(runs);
        report.incomplete = incomplete;
        report
    }

    /// Check the in-flight runs of one physical database. `shards` are the
    /// shard ids that alias it, first one first.
    async fn run_shard(
        &self,
        shards: &[ShardId],
        pool: &crate::worker::DbPool,
        options: &UpgradeCheckOptions,
    ) -> crate::error::HarvestResult<(Vec<RunVerdict>, bool)> {
        use diesel_async::RunQueryDsl as _;

        let mut conn = pool
            .get()
            .await
            .map_err(|e| crate::error::HarvestError::Database(e.to_string()))?;
        let states: Vec<String> = crate::replay_sample::IN_FLIGHT_STATES
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        // Aliased shards share this database, so each one adds its own limit.
        let group_limit = options.limit_per_shard.saturating_mul(shards.len().max(1));
        let limit = i64::try_from(group_limit.saturating_add(1)).unwrap_or(i64::MAX);
        let workflow_name = options.workflow_name.clone();
        let baseline_build = options.baseline_build_id.clone();
        let mut rows: Vec<InFlightRow> = conn
            .build_transaction()
            .read_only()
            .run(async |conn| {
                diesel::sql_query(IN_FLIGHT_SQL)
                    .bind::<diesel::sql_types::Array<diesel::sql_types::Text>, _>(&states)
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                        workflow_name.as_deref(),
                    )
                    .bind::<diesel::sql_types::BigInt, _>(limit)
                    .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(
                        baseline_build.as_deref(),
                    )
                    .load(conn)
                    .await
                    .map_err(crate::error::database_error)
            })
            .await?;
        let mut truncated = rows.len() > group_limit;
        rows.truncate(group_limit);
        // The limit holds for each shard id on its own, not only for the
        // group. A shard over its limit makes the check incomplete.
        let mut per_shard: HashMap<ShardId, usize> = HashMap::new();
        rows.retain(|row| {
            let count = per_shard
                .entry(shard_of(ExecutionId::from_uuid(row.id), shards))
                .or_insert(0);
            *count = count.saturating_add(1);
            let keep = *count <= options.limit_per_shard;
            truncated |= !keep;
            keep
        });

        let mut verdicts = Vec::with_capacity(rows.len());
        for row in rows {
            let execution_id = ExecutionId::from_uuid(row.id);
            let codecs = Arc::clone(&self.codecs);
            let offloader = self.offloader.clone();
            // One snapshot for both reads (`REPEATABLE READ`). A worker that
            // ingests a pending signal between them would otherwise move it out
            // of `harvest_signals` and into history unseen by either read.
            let loaded = conn
                .build_transaction()
                .repeatable_read()
                .read_only()
                .run(async |conn| {
                    let history = crate::store::load_history_inflated(
                        conn,
                        execution_id,
                        &codecs,
                        offloader.as_deref(),
                    )
                    .await?;
                    let pending = load_pending_signals(conn, execution_id, &codecs).await?;
                    Ok::<_, crate::error::HarvestError>((history, pending))
                })
                .await;
            let mut verdict = match loaded {
                Ok((history, pending)) => {
                    match self.snapshot_for(&row, execution_id, history.events) {
                        Some(snapshot) => self.check_snapshot_with(snapshot, &pending).await,
                        None => undecodable(execution_id, row.workflow_name.clone(), None),
                    }
                }
                Err(e) if is_codec_error(&e) => {
                    undecodable(execution_id, row.workflow_name.clone(), None)
                }
                Err(e) => return Err(e),
            };
            verdict.shard_id = Some(shard_of(execution_id, shards));
            if self.structure.is_some()
                && let Some(finding) = build_finding(
                    row.assigned_build_id.as_deref(),
                    options.baseline_build_id.as_deref(),
                )
            {
                verdict.push(finding);
            }
            verdict.build_id = row.assigned_build_id;
            verdicts.push(verdict);
        }
        Ok((verdicts, truncated))
    }

    /// The replay input for `row`. `None` when the candidate codecs cannot
    /// decode its context headers.
    fn snapshot_for(
        &self,
        row: &InFlightRow,
        execution_id: ExecutionId,
        events: Vec<WorkflowEvent>,
    ) -> Option<HistorySnapshot> {
        let context_headers = match &row.context_headers {
            None => None,
            Some(stored) => {
                let decoded = self.codecs.decode_column(stored).ok()?;
                Some(serde_json::from_value::<HashMap<String, String>>(decoded).ok()?)
            }
        };
        Some(HistorySnapshot {
            workflow_name: row.workflow_name.clone(),
            execution_id,
            events,
            context_headers,
            execution_timeout: row.execution_timeout,
            deadline_at: row.deadline_at,
            parent_execution_id: row.parent_id.map(ExecutionId::from_uuid),
            workflow_id: Some(row.workflow_id.clone()),
            queue_name: Some(row.queue_name.clone()),
        })
    }
}

/// The signals that wait for `execution_id`, decoded, oldest first.
#[cfg(feature = "db")]
async fn load_pending_signals(
    conn: &mut diesel_async::AsyncPgConnection,
    execution_id: ExecutionId,
    codecs: &PayloadCodecs,
) -> crate::error::HarvestResult<Vec<PendingSignal>> {
    use crate::schema::harvest_signals::dsl;
    use diesel::{ExpressionMethods as _, QueryDsl as _};
    use diesel_async::RunQueryDsl as _;

    let rows: Vec<(String, serde_json::Value)> = dsl::harvest_signals
        .filter(dsl::workflow_exec_id.eq(execution_id.as_uuid()))
        .filter(dsl::consumed.eq(false))
        .order((dsl::received_at, dsl::id))
        .select((dsl::signal_name, dsl::payload))
        .load(conn)
        .await
        .map_err(crate::error::database_error)?;
    rows.into_iter()
        .map(|(signal_name, payload)| {
            Ok(PendingSignal {
                signal_name,
                payload: codecs.decode_column(&payload)?,
            })
        })
        .collect()
}

/// A history read failed because the codecs cannot decode it.
///
/// A missing key or codec has its own variant. A failed decrypt is a
/// `Config` error, and plaintext that is not JSON is a `Serialization` error.
#[cfg(feature = "db")]
const fn is_codec_error(e: &crate::error::HarvestError) -> bool {
    matches!(
        e,
        crate::error::HarvestError::UnknownCodecKey { .. }
            | crate::error::HarvestError::UnknownPayloadCodec { .. }
            | crate::error::HarvestError::Config(_)
            | crate::error::HarvestError::Serialization(_)
    )
}

// ── the command ─────────────────────────────────────────────────────────────

/// The parsed command line of [`run_command`].
#[cfg(feature = "db")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CommandArgs {
    database_urls: Vec<String>,
    baseline: Option<std::path::PathBuf>,
    baseline_build: Option<String>,
    candidate: Option<std::path::PathBuf>,
    workflow_name: Option<String>,
    limit: Option<usize>,
    json: bool,
}

/// The usage text of [`run_command`].
#[cfg(feature = "db")]
pub const USAGE: &str = "\
usage: <binary> [--database-url-env NAME]... [--database-url URL]... \
[--baseline-structure FILE] [--candidate-structure FILE] [--baseline-build ID] [--workflow-name NAME] \
[--limit N] [--format text|json]

Gives each in-flight run a verdict against this build: migrate, review or pin.
Name one database per shard, in shard order. --database-url-env reads the URL
from an environment variable, so the URL stays out of the process list.
Exit codes: 0 every run migrates, 1 a run needs review or pin, 2 incomplete or bad input.";

#[cfg(feature = "db")]
impl CommandArgs {
    fn parse(args: Vec<String>) -> Result<Self, UpgradeCheckError> {
        let mut out = Self::default();
        let mut args = args.into_iter().skip(1);
        while let Some(arg) = args.next() {
            // `--flag=value` and `--flag value` mean the same.
            let (flag, inline) = match arg.split_once('=') {
                Some((flag, value)) if flag.starts_with("--") => {
                    (flag.to_string(), Some(value.to_string()))
                }
                _ => (arg, None),
            };
            let mut value = || {
                inline
                    .clone()
                    .or_else(|| args.next())
                    .ok_or_else(|| UpgradeCheckError::Usage(format!("{flag} needs a value")))
            };
            match flag.as_str() {
                "--database-url" => out.database_urls.push(value()?),
                "--database-url-env" => {
                    let name = value()?;
                    let url = std::env::var(&name).map_err(|_| {
                        UpgradeCheckError::Usage(format!(
                            "environment variable `{name}` is not set"
                        ))
                    })?;
                    out.database_urls.push(url);
                }
                "--baseline-structure" => out.baseline = Some(value()?.into()),
                "--candidate-structure" => out.candidate = Some(value()?.into()),
                "--workflow-name" => out.workflow_name = Some(value()?),
                "--baseline-build" => out.baseline_build = Some(value()?),
                "--limit" => {
                    let raw = value()?;
                    out.limit = Some(raw.parse().map_err(|_| {
                        UpgradeCheckError::Usage(format!("--limit `{raw}` is not a number"))
                    })?);
                }
                "--format" => match value()?.as_str() {
                    "text" => out.json = false,
                    "json" => out.json = true,
                    other => {
                        return Err(UpgradeCheckError::Usage(format!(
                            "--format `{other}` is not text or json"
                        )));
                    }
                },
                // The flag name only: a value can hold a password.
                other => {
                    return Err(UpgradeCheckError::Usage(format!("unknown flag `{other}`")));
                }
            }
        }
        if out.database_urls.is_empty()
            && let Ok(url) = std::env::var("HARVEST_DATABASE_URL")
        {
            out.database_urls.push(url);
        }
        if out.database_urls.is_empty() {
            return Err(UpgradeCheckError::Usage(
                "pass --database-url-env or --database-url, or set HARVEST_DATABASE_URL"
                    .to_string(),
            ));
        }
        if out.baseline.is_some() != out.candidate.is_some() {
            return Err(UpgradeCheckError::Usage(
                "pass both --baseline-structure and --candidate-structure, or neither".to_string(),
            ));
        }
        Ok(out)
    }
}

/// Run the check from a command line. The report goes to stdout, and an
/// error goes to stderr.
///
/// `args` starts with the program name. The candidate binary calls this from
/// its own `main`, with `check` holding its workflows and codecs. The return
/// value is the process exit code: see [`UpgradeReport::exit_code`].
#[cfg(feature = "db")]
pub async fn run_command(check: UpgradeCheck, args: Vec<String>) -> i32 {
    let mut out = std::io::stdout();
    let mut err = std::io::stderr();
    run_command_with_output(check, args, &mut out, &mut err).await
}

/// [`run_command`], with the report written to `out` and errors to `err`.
#[cfg(feature = "db")]
pub async fn run_command_with_output(
    check: UpgradeCheck,
    args: Vec<String>,
    out: &mut (dyn std::io::Write + Send),
    err: &mut (dyn std::io::Write + Send),
) -> i32 {
    let args = match CommandArgs::parse(args) {
        Ok(args) => args,
        Err(e) => {
            let _ = writeln!(err, "error: {e}\n\n{USAGE}");
            return 2;
        }
    };
    let check = match (&args.baseline, &args.candidate) {
        (Some(baseline), Some(candidate)) => {
            match (
                StructureManifest::load(baseline),
                StructureManifest::load(candidate),
            ) {
                (Ok(b), Ok(c)) => check.with_structure(b, c),
                (Err(e), _) | (_, Err(e)) => {
                    let _ = writeln!(err, "error: {e}");
                    return 2;
                }
            }
        }
        _ => check,
    };
    let entries = args.database_urls.iter().enumerate().map(|(i, url)| {
        (
            ShardId::new(i32::try_from(i).unwrap_or(i32::MAX)),
            url.clone(),
        )
    });
    let pool = match crate::shard::ShardedDbPool::from_dsns(entries, ShardId::new(0), 2) {
        Ok(pool) => pool,
        Err(e) => {
            let _ = writeln!(err, "error: {e}");
            return 2;
        }
    };
    let options = UpgradeCheckOptions {
        workflow_name: args.workflow_name.clone(),
        limit_per_shard: args.limit.unwrap_or(DEFAULT_LIMIT_PER_SHARD),
        baseline_build_id: args.baseline_build.clone(),
    };
    let report = check.run(&pool, &options).await;
    let rendered = if args.json {
        report.to_json()
    } else {
        report.render_text()
    };
    let _ = writeln!(out, "{rendered}");
    report.exit_code()
}

#[cfg(all(test, feature = "db"))]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<CommandArgs, UpgradeCheckError> {
        CommandArgs::parse(
            std::iter::once("bin")
                .chain(args.iter().copied())
                .map(String::from)
                .collect(),
        )
    }

    #[test]
    fn the_command_line_parses_every_flag() {
        let args = parse(&[
            "--database-url",
            "postgres://a",
            "--database-url",
            "postgres://b",
            "--baseline-structure",
            "old.json",
            "--candidate-structure",
            "new.json",
            "--workflow-name",
            "order",
            "--limit",
            "5",
            "--format",
            "json",
        ])
        .expect("parses");
        assert_eq!(args.database_urls, ["postgres://a", "postgres://b"]);
        assert_eq!(args.workflow_name.as_deref(), Some("order"));
        assert_eq!(args.limit, Some(5));
        assert!(args.json);
    }

    #[test]
    fn one_manifest_alone_is_a_usage_error() {
        let err = parse(&["--database-url", "x", "--baseline-structure", "old.json"]);
        assert!(matches!(err, Err(UpgradeCheckError::Usage(_))));
    }

    #[test]
    fn a_verdict_names_the_aliased_shard_its_run_belongs_to() {
        let (zero, one) = (ShardId::new(0), ShardId::new(1));
        let on_one = ExecutionId::new_for_shard(one);
        assert_eq!(super::shard_of(on_one, &[zero, one]), one);
        // A rebalanced run keeps its id but lives in this group.
        assert_eq!(super::shard_of(on_one, &[zero]), zero);
        let unencoded = ExecutionId::from_uuid(uuid::Uuid::new_v4());
        assert_eq!(super::shard_of(unencoded, &[one, zero]), one);
    }

    #[test]
    fn a_run_on_another_build_than_the_baseline_needs_review() {
        assert!(super::build_finding(None, None).is_none());
        assert!(super::build_finding(Some("v2"), Some("v2")).is_none());
        let other = super::build_finding(Some("v1"), Some("v2")).expect("finding");
        assert_eq!(other.kind, FindingKind::StructureUnavailable);
        assert!(super::build_finding(Some("v1"), None).is_some());
    }

    #[test]
    fn a_flag_takes_its_value_after_an_equals_sign() {
        let args = parse(&["--database-url=postgres://a", "--format=json"]).expect("parses");
        assert_eq!(args.database_urls, ["postgres://a"]);
        assert!(args.json);
    }

    #[test]
    fn an_unknown_flag_error_does_not_echo_its_value() {
        let err = parse(&["--database-url", "x", "--db=postgres://u:secret@h"])
            .expect_err("unknown flag");
        assert!(!err.to_string().contains("secret"), "{err}");
    }

    #[test]
    fn a_flag_without_a_value_is_a_usage_error() {
        assert!(matches!(
            parse(&["--database-url"]),
            Err(UpgradeCheckError::Usage(_))
        ));
        assert!(matches!(
            parse(&["--database-url", "x", "--format", "yaml"]),
            Err(UpgradeCheckError::Usage(_))
        ));
    }
}
