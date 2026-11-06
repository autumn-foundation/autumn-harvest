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
//! - [`agent_sample`] runs best-of-N or rejection sampling. It scores each
//!   draw and selects one (issue #2443).
//! - [`agent_sample`] also runs step-level beam search. It scores each model
//!   turn and keeps the best beams with [`beam_rank`].
//! - [`admit_answer_check`] caps the answer check of a run (issue #2442).
//! - [`agent_sweep`] runs one strategy over a fixed set of inputs, and
//!   keeps one row for each item.
//!
//! A run never carries a policy field. [`AgentTask`](crate::AgentTask),
//! [`AgentOp`](crate::AgentOp), [`DrawsInput`] and [`SampleInput`] refuse
//! each name in [`SAMPLING_POLICY_FIELDS`]. The design is in
//! `docs/design/DESIGN-2439.md`, `docs/design/DESIGN-2443.md`,
//! `docs/design/DESIGN-2446.md` and `docs/design/DESIGN-2448.md`.

mod admit;
mod draws;
mod policy;
mod sample;
mod store;
mod sweep;

pub use admit::{
    Admission, Ask, DrawPlan, REFUSED_PREFIX, Refusal, SampleStep, SweepPlan, admit,
    admit_answer_check, answer_retry_cap, beam_draws, beam_slots, draw_plan, retry_after_reject,
    sample_step, sweep_plan, wave_end,
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
    SampleStop, SampleStrategy, agent_sample, agent_sample_info, beam_rank,
};
#[cfg(feature = "postgres")]
pub use store::PostgresSamplingPolicyStore;
#[cfg(feature = "sqlite")]
pub use store::SqliteSamplingPolicyStore;
pub use store::{
    InMemorySamplingPolicyStore, MAX_AUDIT_ROWS, PolicyAction, PolicyAudit, PolicyChange,
    PolicyRead, PolicyStoreError, SamplingPolicyStore,
};
pub use sweep::{
    MAX_SWEEP_ANSWER_BYTES, MAX_SWEEP_ITEM_BYTES, MAX_SWEEP_ITEMS, MAX_SWEEP_LIST_BYTES,
    MAX_SWEEP_PAGE_BYTES, MAX_SWEEP_REASON_BYTES, SWEEP_WORKFLOW_NAME, SweepInput, SweepItem,
    SweepItemStatus, SweepItemStore, SweepItems, SweepItemsRef, SweepPage, SweepReport, SweepRow,
    SweepStop, SweepTotals, agent_sweep, agent_sweep_info, agent_sweep_items,
    agent_sweep_items_info,
};
