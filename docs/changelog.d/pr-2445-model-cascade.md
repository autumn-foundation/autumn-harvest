## Model cascade strategy (issue #2445)

`agent_sample` adds a model cascade. It asks a cheap model first, scores
the answer, and asks a stronger model only on a low score. The design is
in `docs/design/DESIGN-2445.md`.

- `SampleStrategy::Cascade { tiers }`. A `Tier` is a harness model and an
  accept threshold. Tier `i` is draw `i`, one child `agent_loop` run per
  tier. The run stops at the first tier whose score is at or above its
  threshold.
- When no tier accepts, `select` picks the best verdict, and the outcome is
  `SampleOutcome::Escalated { draw }`.
- `SamplingPolicy::cascade_models` lists the models that a tier may name.
  It is empty by default and skipped on encode. A tier model outside it
  fails the run with `tier_model_not_allowed` before any model call. A
  cascade with a `concurrency` above 1 is refused.
- A tier whose model the mandate of the profile does not allow is skipped.
  `SampleReport::skipped` names it with `Refusal::NotPermitted`. A failed
  tier gets a reject, and the next tier runs.
- `SampleDraw::model` records the harness model of each draw. Each tier
  asks its own model, so the cost ledger has one row per tier with the
  model id of that tier.
- `cascade_step` gives the next tier. Verus proves it, and a spec model
  shows that each tier runs at most once, in order.
- CLI: `harvest agents sampling policy <kind> set --cascade-models a,b`.
- No migration. No new `WorkflowEvent` variant. No new activity.

Tests: `tests/sample_cascade.rs` (each acceptance line, the admission, a
failed tier, the budget, the wire form and a clean replay) and
`tests/sample_cascade_crash.rs` (a crash after tier 1 resumes at tier 2).
