## Testing — A fair drain phase for the world simulation (issue #2217)

The DST nightly failed world seed 52000000302 on `Converges`. The engine was
not at fault. Worker `w1.1` abandoned the first decision task of `s2`, and
only the reclaimer requeues the claim of a dead worker. The planner draws
each action by weight, and the reclaimer has the lowest weight. In 400
drain steps, the draw never picked it.

`Converges` is a liveness check, so it holds only under a fair scheduler.
The drain phase of `dst::world` is now weakly fair. An action that stays
enabled for `FAIR_STEPS` (60) steps without a run runs next. The longest
wait goes first. The rule takes no random draw, so a run with no overdue
action keeps its trace. The fault phase does not change, so planted seed 0
and the golden toy traces are unchanged.

A `Converges` failure now names the work that remains, such as
`s2 RUNNING`. The triage of this seed needed a kept database to find it.

The branch also fixes the `trunk-dev` build. PR #2071 added
`WorkflowCancelRequested` after #2205 and #2209 made both payload slot
matches in `event.rs` name every variant. The library failed with E0004.
The variant holds no payload, so both matches return `None` for it. The
duplicate `PayloadSlot` is removed as in #2220.

No migration. No `WorkflowEvent` change.

Tests:

- `the_drain_phase_runs_each_enabled_action_within_the_fair_bound` (the
  `dst` target): over 32 toy seeds, no always-enabled action waits more
  than the bound in the drain. It failed before the fix: a sweeper pass
  waited 87 steps.
- `the_drain_phase_reclaims_an_abandoned_decision`
  (`dst_world_tests`): seed 52000000302 converges, and a reclaimer pass in
  the drain requeues the abandoned claim.
