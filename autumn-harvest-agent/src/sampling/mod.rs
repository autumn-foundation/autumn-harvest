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
//! - [`admit_answer_check`] caps the answer check of a run (issue #2442).
//!
//! A run never carries a policy field. [`AgentTask`](crate::AgentTask),
//! [`AgentOp`](crate::AgentOp) and [`DrawsInput`] refuse each name in
//! [`SAMPLING_POLICY_FIELDS`]. The design is in
//! `docs/design/DESIGN-2439.md`.

mod admit;
mod draws;
mod policy;
mod store;

pub use admit::{
    Admission, Ask, DrawPlan, REFUSED_PREFIX, Refusal, admit, admit_answer_check, answer_retry_cap,
    draw_plan, retry_after_reject, wave_end,
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
#[cfg(feature = "postgres")]
pub use store::PostgresSamplingPolicyStore;
#[cfg(feature = "sqlite")]
pub use store::SqliteSamplingPolicyStore;
pub use store::{
    InMemorySamplingPolicyStore, MAX_AUDIT_ROWS, PolicyAction, PolicyAudit, PolicyChange,
    PolicyRead, PolicyStoreError, SamplingPolicyStore,
};
