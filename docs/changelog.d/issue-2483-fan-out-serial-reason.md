## Feature — a fan-out names why a delegation ran alone (issue #2483)

`AgentTask::parallel_children(n)` batches only allowed `delegate` calls in
a row, each to an inline peer. Before this change, any other delegation ran
inline with no trace. Now its `DelegationRecord::serial` holds a
`SerialReason`: `single_call`, `not_consecutive`, `gated`, `durable_peer` or
`bound_reached`. A `debug` event logs the same reason once, not on replay.
A bound under 2 names no reason, and a refused delegation keeps its
refusal only.

**Design.** The new kernel `fan_out_plan` gives each call of a turn a step.
A step is the lead of a batch, a member, a lone call with its reason, or no
step. The
batches are the same as before. The proptest `the_plan_batches_as_before`
compares them with the old code. The reason comes from the recorded turn,
the recorded roster and the task, so replay is not affected. The field has
a serde default, so a report from before decodes. It is skipped when empty,
so a run with no lone delegation writes the same report as before. No `WorkflowEvent`
variant or migration is added.

**Proof.** `formal/verus/src/agent_fan_out.rs` mirrors `can_join` and
`fan_out_plan`. It proves that no step overflows. It proves that a bound
under 2 names no reason. It proves that each delegation that can run leads
a batch, joins one, or names a reason that is true of the turn. A batch
stops short of the bound only at the end of a run.

**Docs.** The mesh guide §15 lists the batching preconditions next to the
knob, with a table of the reasons. ADR 0013 records the decision.

**Tests.** `fan_out.rs` covers each reason end to end, a nested record that
reaches the root, replay, and a run with no bound. `durable_peers.rs`
covers `durable_peer`. The unit tests check each step against the spec of
the proof.
