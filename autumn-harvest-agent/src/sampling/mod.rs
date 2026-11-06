//! Operator sampling policy per agent kind (issue #2439, epic #2438).
//!
//! Every strategy of the epic runs more than one draw. The operator decides
//! the draw count and the concurrency. The operator also decides the tool
//! effects of a draw, the scorers and the judge models. Each setting has a
//! safe default, and a change needs no restart.
//!
//! - [`SamplingPolicy`] holds the settings of one kind.
//! - [`SamplingPolicyStore`] keeps one policy per kind and audits each
//!   change with its old and new values.
//! - [`admit`] compares a strategy request with the policy, before the
//!   first model call.
//! - [`agent_draws`] reads the policy once, admits the request and runs the
//!   draws.
//! - [`agent_sample`] runs best-of-N, rejection sampling or a model
//!   cascade. It scores each draw and selects one. It also runs a vote: it
//!   keys each answer with an [`AnswerKey`], and the most common key wins.
//! - [`agent_sample`] also runs step-level beam search. It scores each model
//!   turn and keeps the best beams with [`beam_rank`].
//! - [`admit_answer_check`] caps the answer check of a run (issue #2442).
//!
//! A run never carries a policy field. [`AgentTask`](crate::AgentTask),
//! [`AgentOp`](crate::AgentOp), [`DrawsInput`] and [`SampleInput`] refuse
//! each name in [`SAMPLING_POLICY_FIELDS`]. The design is in
//! `docs/design/DESIGN-2439.md`, `docs/design/DESIGN-2443.md`,
//! `docs/design/DESIGN-2444.md` and `docs/design/DESIGN-2446.md`.

mod admit;
mod draws;
mod policy;
mod sample;
mod store;
mod vote;

pub use admit::{
    Admission, Ask, CascadeStep, DrawPlan, REFUSED_PREFIX, Refusal, SampleStep, admit,
    admit_answer_check, admit_tiers, answer_retry_cap, beam_draws, beam_slots, cascade_step,
    draw_plan, retry_after_reject, sample_step, wave_end,
};
pub use draws::{
    DRAWS_WORKFLOW_NAME, DrawsInput, DrawsReport, agent_draws, agent_draws_info,
    agent_sampling_policy, agent_sampling_policy_info,
};
pub(crate) use policy::refuse_policy_fields;
pub use policy::{
    MAX_ANSWER_RETRIES, MAX_DRAWS, PolicyError, SAMPLING_POLICY_FIELDS, SamplingPolicy, ScorerId,
    ScorerRef, Scorers, Strategy,
};
pub use sample::{
    BeamFate, BeamStep, SAMPLE_WORKFLOW_NAME, SampleDraw, SampleInput, SampleOutcome, SampleReport,
    SampleStop, SampleStrategy, SkippedTier, Tier, agent_sample, agent_sample_info, beam_rank,
};
#[cfg(feature = "postgres")]
pub use store::PostgresSamplingPolicyStore;
#[cfg(feature = "sqlite")]
pub use store::SqliteSamplingPolicyStore;
pub use store::{
    InMemorySamplingPolicyStore, MAX_AUDIT_ROWS, PolicyAction, PolicyAudit, PolicyChange,
    PolicyRead, PolicyStoreError, SamplingPolicyStore,
};
pub use vote::{
    AnswerKey, AnswerKeyFn, MAX_KEY_BYTES, Tally, VoteCount, VoteKeyReply, VoteKeyRequest,
    agent_vote_key, agent_vote_key_info, run_vote_key, vote_settled,
};
