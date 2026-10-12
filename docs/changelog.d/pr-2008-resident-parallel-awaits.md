## Feature — Resident state for parallel activity awaits (issue #2008)

**Measurement first (issue #2007, counter part).** A new counter,
`harvest.workflow.resident{workflow, queue, outcome}`, counts each
decision attempt on a worker with resident state on. `outcome` is `hit`
when the decision resumed a resident workflow. Each other value names why
it replayed: `cold`, `declined`, `multi_await`, `race`, `mutex`,
`hot_swap`, `blocked` or `unsupported`. The `ResidentOutcome` enum bounds
the label. A cache entry keeps the miss reason of its suspension, and the
next decision reports it. The context now counts each `ctx.race()` in
flight. A suspension inside a race reports `race`, even after a cold
replay. The starter dashboard gains a panel for the outcome shares.

**Behaviour change (issue #2008).** A suspension that awaits two or more
activities now stays resident. This covers `futures::join!`,
`try_join_all` and the fan-out helper, the shape of parallel tool calls.
A delta can resolve any non-empty subset of the parked activities. Each
other activity gets a `WaitForActivity` command again, with its live
sender. A cold replay emits the same command, and it writes no event.

A cycle that re-parks a sibling is a speculation. It must only wait, and
it must not read history state, such as the replay position. It fires no
side effect that replay suppresses. Otherwise the resume declines with
`ResumeDeclined::SiblingStillParked`, and the worker drops the future and
replays cold. A delta with several results runs as one cycle per result,
in history order, because a cold replay matches them in that order.

Joins with a timer or a signal next to another await still replay, and
report `multi_await`. Races still replay, and report `race`.

**Measurement.** The same workloads ran before and after the change:

- An agent loop ran on a real worker. It had three rounds of one model
  call and four parallel tool calls. It hit 3 of 10 decisions before and
  10 of 11 after. This is one run, and the tool results arrive in batches
  that vary.
- The differential test of the same loop is deterministic. Every
  decision after the first now resumes, in twelve arrival modes.
- The DST world sweep, seeds 0 to 11: 33 warm decisions and 30 cache hits
  that replayed before, 44 and 19 after. Every invariant holds.

`DESIGN-2008.md` records the method and the outcome sequences.

**Known limit.** A join branch that runs a command after its own await
fails a cold replay when a later branch has a command event. When the
earlier branch's result arrives last, a warm decision cannot tell the
branch code from code after the join. Issue #1798 has the same limit for
a single wait. `docs/sticky-routing.md` names it.

**No migration. No new `WorkflowEvent` variant.** `ResumeDeclined` gains
`SiblingStillParked`. `MetricsRecorder` gains `record_workflow_resident`,
with a no-op default. The crate root exports the new `#[non_exhaustive]`
enum `ResidentOutcome`. `telemetry` gains `METRIC_WORKFLOW_RESIDENT`.

**Tests.** Differential tests in `resident.rs` compare each warm decision
with a cold replay of the same history. They cover each join shape in
twelve arrival modes, with and without progress events. Mutation runs
confirm that the stepping and the position check are both needed.
`resident_outcome_tests` runs the real worker.
`dst_world_tests::a_join_resumes_warm_through_the_worker` pins a warm
partial resume in the DST world, and the seeded sweep now runs a join in
every scheduled run.
