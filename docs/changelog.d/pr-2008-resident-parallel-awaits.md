## Feature — Resident state for parallel activity awaits (issues #2008 and #2007)

**Measurement first (issue #2007, counter part).** A new counter,
`harvest.workflow.resident{workflow, queue, outcome}`, counts each
decision on a worker with resident state on. `outcome` is `hit` when the
decision resumed a resident workflow. Each other value names why it
replayed: `cold`, `declined`, `multi_await`, `race`, `mutex`, `hot_swap`,
`blocked` or `unsupported`. The `ResidentOutcome` enum bounds the label.
A cache entry keeps the miss reason of its suspension, and the next
decision reports it. `ctx.race()` now counts itself as open while it runs,
so a race in flight reports `race`, also after a cold replay. The starter
dashboard gains a panel for the outcome shares.

**Behaviour change (issue #2008).** A suspension that awaits two or more
activities now stays resident. This covers `futures::join!`,
`try_join_all` and the fan-out helper, the shape of parallel tool calls.
A delta can resolve any non-empty subset of the parked activities. Each
other activity gets a `WaitForActivity` command again, with its live
sender. A cold replay emits the same command, and it writes no event.

A cycle that re-parks a sibling must only wait. After the poll, the
resume checks that the cycle suspends with only its re-parked waits. A
branch that runs a command, fails, or drops a sibling makes the resume
decline with `ResumeDeclined::SiblingStillParked`. The worker then drops
the future and replays cold.

Joins that mix a timer or a signal with another await still replay, and
report `multi_await`. Races still replay, and report `race`.

**Measurement.** The same workloads ran before and after the change:

- An agent loop of three rounds, each with one model call and four
  parallel tool calls, on a real worker: 3 hits of 10 decisions before,
  10 hits of 11 after. Every decision after the first now hits.
- The DST world sweep, seeds 0 to 11: 33 warm decisions and 30 cache hits
  that replayed before, 44 and 19 after. Every invariant holds.

`DESIGN-2008.md` records the method and the outcome sequences.

**Known limit.** A join branch that runs a command after its own await
fails a cold replay when a later branch has a command event. When the
last parked activity of such a join resolves, a warm decision cannot tell
the branch code from code after the join. Issue #1798 has the same limit
for a single wait. `docs/sticky-routing.md` names it.

**No migration. No new `WorkflowEvent` variant.** `ResumeDeclined` gains
`SiblingStillParked`. `MetricsRecorder` gains
`record_workflow_resident`, with a no-op default.

**Tests.** Differential tests in `resident.rs` compare each warm decision
with a cold replay of the same history, for each join shape and three
arrival orders. `resident_outcome_tests` runs the real worker.
`dst_world_tests::a_join_resumes_warm_through_the_worker` pins a warm
partial resume in the DST world, and the seeded sweep now runs a join in
every scheduled run.
