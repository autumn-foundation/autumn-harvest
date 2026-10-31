//! The refine loop of an agent kind (epic #2369).
//!
//! [`propose`] turns evidence into a candidate overlay (issue #2379).
//! [`ModelProposer`] runs a proposer over an [`AgentModel`](crate::AgentModel)
//! with the default templates of [`prompts`]. The design is in
//! `docs/design/DESIGN-2379.md`.

pub mod model_proposer;
pub mod prompts;
pub mod propose;

pub use model_proposer::{KEEP_BASE, ModelProposer, OUTPUT_CONTRACT};
pub use propose::{
    DEFAULT_PROPOSER_TIMEOUT, Dimension, Evidence, MAX_PROPOSAL_ATTEMPTS, MAX_REJECTION_CHARS,
    OverlayProposer, ProposalAttempts, ProposalAttemptsError, ProposeContext, ProposeRequest,
    ProposerReply, Stage, StageOutcome, agent_refine_propose, agent_refine_propose_info,
};
