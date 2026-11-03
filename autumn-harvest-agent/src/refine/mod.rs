//! The refine loop of an agent kind (epic #2369).
//!
//! [`propose`] turns evidence into a candidate overlay (issue #2379).
//! [`policy`] holds the operator refine policy of each kind, and
//! [`adopt`] decides whether a candidate goes live without a person
//! (issue #2394).
//! [`ModelProposer`] runs a proposer over an [`AgentModel`](crate::AgentModel)
//! with the default templates of [`prompts`]. The designs are in
//! `docs/design/DESIGN-2379.md` and `docs/design/DESIGN-2394.md`.

pub mod adopt;
pub mod model_proposer;
pub mod policy;
pub mod policy_read;
pub mod prompts;
pub mod propose;

pub use adopt::{Adoption, RefineStatus, WaitReason, adopt};
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
