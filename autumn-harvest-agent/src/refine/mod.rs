//! The refine loop of an agent kind (epic #2369).
//!
//! [`propose`] turns evidence into a candidate overlay (issue #2379).
//! [`policy`] holds the operator refine policy of each kind, and
//! [`adopt`] decides whether a candidate goes live without a person
//! (issue #2394).
//! [`bundle`] holds the manifest of an evidence bundle (issue #2377).
//! [`ModelProposer`] runs a proposer over an [`AgentModel`](crate::AgentModel)
//! with the default templates of [`prompts`]. The workflow `agent_refine`
//! of `run` runs Propose, Probe and Compose with a fixed budget (issue
//! #2380). [`ledger`] counts that budget. The designs are in
//! `docs/design/DESIGN-2379.md`, `docs/design/DESIGN-2380.md` and
//! `docs/design/DESIGN-2394.md`.

pub mod adopt;
pub mod bundle;
#[cfg(feature = "eval")]
pub mod compose;
pub mod ledger;
pub mod meter;
pub mod model_proposer;
pub mod policy;
pub mod policy_read;
pub mod prompts;
pub mod propose;
#[cfg(feature = "eval")]
pub mod run;
pub mod sources;

pub use adopt::{Adoption, RefineStatus, WaitReason, adopt};
pub use bundle::{BundleFile, BundleManifest};
#[cfg(feature = "eval")]
pub use compose::{
    ComposeCandidate, ComposeContext, ComposeRequest, Composition, OverlayComposer,
    OverlaySelector, ProbedCandidate, SelectContext, SelectRequest, SelectedBy, Selection,
    agent_refine_compose, agent_refine_compose_info, agent_refine_select, agent_refine_select_info,
};
pub use ledger::{MAX_REFINE_K, MAX_SOURCE_RUNS, RefineError, RefineLedger, RefineMode};
pub use meter::{Charge, Meter};
pub use model_proposer::{
    DEFAULT_PROPOSER_OUTPUT_TOKENS, KEEP_BASE, ModelProposer, OUTPUT_CONTRACT,
};
pub use policy::{
    AdoptDecision, ApprovalTimeout, AutoAdopt, DailyCap, GateState, LessonEntries, Lessons,
    OverlayPart, PolicyCeilings, PolicyChange, PolicyError, PolicyModels, RefinePolicy,
};
pub use policy_read::{agent_refine_policy, agent_refine_policy_info, read_refine_policy};
pub use propose::{
    DEFAULT_PROPOSAL_ATTEMPTS, DEFAULT_PROPOSER_TIMEOUT, Dimension, Evidence,
    MAX_PROPOSAL_ATTEMPTS, MAX_REJECTION_CHARS, OverlayProposer, ProposalAttempts,
    ProposalAttemptsError, ProposeContext, ProposeRequest, ProposerReply, Stage, StageOutcome,
    agent_refine_propose, agent_refine_propose_info,
};
#[cfg(feature = "eval")]
pub use run::{
    LiveReprobe, ProbeRecord, ProbeRequest, ProbeResult, REFINE_WORKFLOW_NAME, RefineInput,
    RefineReport, RefineStart, SelectionRecord, SourceBrief, StageRecord, StageRole, StartRequest,
    agent_refine, agent_refine_info, agent_refine_probe, agent_refine_probe_info,
    agent_refine_start, agent_refine_start_info,
};
pub use sources::{LiveEnd, LiveProbeError, LiveRequest, RefineSources, SourceRecord};
