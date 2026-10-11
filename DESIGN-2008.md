# Design — Issue #2008: Resident state for parallel activity awaits

Issue #2008 asks to extend the resident path to the miss reason that
dominates. The resident path (issue #1798) keeps only cycles with one
awaited command. A join of activities, such as parallel tool calls, falls
back to a full replay at each decision.

**No migration. No new `WorkflowEvent` variant. No route change.** One new
counter, `harvest.workflow.resident`. `ResumeDeclined` gains one variant.

---

## 0. Planning record

### 0.1 Facts found before the plan

- The sibling issue #2007 (hit-rate counters) is open. No resident counter
  exists. Only `harvest.workflow.cache_hit` and `cache_miss` exist, and
  they do not tell a resume from a replay.
- `plan_suspension` refuses a second awaited command. A
  `futures::join!` of two activities therefore never stays resident.
- `join!` and `try_join!` are the blessed wait-all forms (HVG005).
  `select!` is a hard blocker (HVG010). `ctx.race()` is the blessed
  wait-first form. It records a `race:{seq}` marker.
- A cold replay of an activity that has no terminal event yet emits
  `WaitForActivity`. That command writes no event.
- The executor suspends a cycle only when a Harvest future is parked:
  a held park token, or a queued command that awaits a result. A parked
  future with no command is a foreign wait. It deadlocks the task.
- A cold replay matches commands in program order. A terminal found past
  an unread sibling command rewinds the cursor to that command.
- A join branch that runs a command after its own await therefore fails
  a cold replay whenever a later branch has a command event. Section 0.5
  shows the experiment.
- The DST world (issue #2002) runs the real worker loop. Its workload has
  no join. Its stats count cold, warm and declined decisions.

### 0.2 Brainstorm — how can a join stay resident?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Measure first. Add a per-decision counter with a bounded miss reason. | **Adopted.** It is the dependency of this issue, and the measurement in section 4 needs it. |
| B2 | Keep a join resident, but resume it only when one delta resolves every parked await. | Rejected. Activities finish at different times. Each finish wakes a decision, so this path almost never runs. |
| B3 | Resume a join with any non-empty subset of its parked activities. Re-park the rest. | **Adopted.** Each finish becomes a warm decision. |
| B4 | Re-park a sibling by a new `WaitForActivity` command that holds the live sender. | **Adopted.** A cold replay emits the same command. The executor then sees a parked future and suspends. |
| B5 | Also cover joins that mix timers or signals. | Deferred. A cold replay of a started timer re-emits `StartTimer`, and signals have other scan rules. The miss stays visible as `multi_await`. |
| B6 | Also cover `ctx.race()`. | Rejected for now. A race records a winner marker and cancels losers. A context counter marks an open race, so the miss shows as `race`. |
| B7 | Check the warm cycle after the poll. Decline when a branch ran a command while a sibling stayed parked. | **Adopted.** The worker then replays cold, which is always correct. |
| B8 | Add a run-time switch for the new path. | Rejected. `with_resident_workflows(false)` already turns off all resident state. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | A warm decision emits commands in another order than a cold replay. | Differential tests compare each warm decision with a cold replay of the same history, for each shape and arrival order. |
| R2 | A branch runs a command after its await while a sibling is parked. A cold replay reads that history another way. | The post-poll check declines. The worker drops the future and replays cold. |
| R3 | A re-park command stays in a batch after its receiver dropped, for example after a `try_join!` error. | The post-poll check declines any re-park whose receiver dropped, and any terminal outcome with a re-park. |
| R4 | A resolving event for a sibling is lost, so the sibling waits forever. | Each parked activity resolves at most once. A second terminal declines. The DST `Converges` invariant fails a stuck run. |
| R5 | A race in flight is taken for a join. | A guard counts open races in the context. Capture refuses with `race`. |
| R6 | The counter gets a high-cardinality label. | The `outcome` label takes nine fixed values. `execution.id` stays span-only. |
| R7 | The counter double-counts a decision. | The worker records once per decision, on the first cycle only. |
| R8 | The extended path is never exercised under faults. | The DST world workload gains a join. A seeded test pins a warm partial resume through the real worker. |
| R9 | The change lowers the hit rate of today's shapes. | All existing resident tests stay green. The measurement covers a single-await workload too. |
| R10 | Two results land in one delta, and the earlier branch runs on. A cold replay fails, but the warm cycle accepts. Review found it. | A delta with several results runs as one cycle per result, in history order. Each cycle but the last must only wait. |
| R11 | A branch reads `ctx.info()`, `replay_position()` or `is_replaying()` while a sibling is parked. A cold replay can stop its cursor at the sibling command and read another value. Review found it. | Such a cycle is a speculation. A position read declines it. |
| R12 | A post-poll decline repeats a log line or a business metric, because the cold replay fires it again. Review found it. | A speculative cycle fires no side effect that replay suppresses. |

### 0.4 Six thinking hats

- **White (facts).** Section 0.1. Today a join of n activities costs n
  replays. Only the last decision can be warm, after a cold replay leaves
  one wait.
- **Red (feelings).** A partial resume feels risky, because the parked
  future now spans siblings that resolve over several decisions.
  Differential tests and the DST world answer that.
- **Black (risks).** Sections 0.3 and 0.5. Joins with branch
  continuations are subtle. A warm decision cannot see every shape that a
  cold replay rejects.
- **Yellow (benefits).** Parallel tool calls stop paying a replay per
  finished tool. The counter makes every miss reason visible to operators.
- **Green (ideas).** Mixed joins and races next, each behind its own
  differential suite. A per-reason alert when `multi_await` grows.
- **Blue (process).** Instrument first, then measure. Red tests, then green
  code, then refactor. DST world last. Then a multi-angle review.

### 0.5 Experiments run before the plan

A throwaway test replayed hand-built histories cold. `B` is a decision
boundary. Each row is the cold result.

| # | Shape | History after the start | Cold result |
|---|-------|-------------------------|-------------|
| E1 | `join!(a, b)` then `c` | `Sa Sb B Cb` | Suspends with `WaitForActivity(a)` |
| E2 | `join!(a, b)` then `c` | `Sa Sb B Ca` | Suspends with `WaitForActivity(b)` |
| E3 | `join!(a, b)` then `c` | `Sa Sb B Cb B Ca` | Suspends with `ScheduleActivity(c)` |
| E4 | `join!(a, b)` then `c` | `Sa Sb B Ca Started(b)` | Suspends with `WaitForActivity(b)` |
| E5 | `join!(a, async { b; c })` | `Sa Sb B Cb` | Suspends with `WaitForActivity(a)`, `ScheduleActivity(c)` |
| E6 | `join!(async { a; c }, b)` | `Sa Sb B Ca` | Suspends with `WaitForActivity(b)` and a non-determinism record |
| E7 | `join!(async { a; c }, b)` | `Sa Sb B Cb B Ca` | Fails: non-deterministic replay |
| E8 | `execute_activity_fan_out_raw` | first decision | `RecordMarker(fan_out:1)`, then two `ScheduleActivity` |
| E9 | `ctx.race()` of two activities | first decision | `RecordMarker(race:1)`, then two `ScheduleActivity` |

Two more facts came out of the experiment:

- E1, E2 and E4 show that a re-park by `WaitForActivity` equals the cold
  result, also with a progress event of the parked sibling.
- **A gap in issue #1798.** After `Sa Sb B Cb`, the single-await path of
  issue #1798
  keeps `join!(async { a; c }, b)` resident with one wait for `a`. When
  `a` finishes, the warm decision schedules `c`, but a cold replay of that
  history fails (E7). A warm decision cannot tell this shape from E3.
  Section 1.3 names the limit.

---

## 1. Change

### 1.1 Measurement (dependency of this issue)

`ResidentOutcome` names the result of one decision for the resident path:

| Label | Meaning |
|-------|---------|
| `hit` | The decision resumed the resident workflow. |
| `cold` | The worker held no resident state for the run, for example after a cache miss or on the first decision. |
| `declined` | The resident workflow did not resume. See `ResumeDeclined`. |
| `multi_await` | The last suspension awaited more than one command, in a shape the path does not cover. |
| `race` | The last suspension was inside `ctx.race()`. |
| `mutex` | The last suspension held or acquired a durable mutex. |
| `hot_swap` | The workflow runs in a hot-swap module. |
| `blocked` | Another context state blocked the capture (see `resident_blocker`), or a probe reached the signal of the wait at the frontier. |
| `unsupported` | The last suspension had a command that the path does not cover. |

The worker records `harvest.workflow.resident{workflow, queue, outcome}`
once per decision attempt, when resident state is on. A decision that
does not commit runs again and records again. A cache entry keeps the
reason why its suspension did not stay resident. The next decision reads
it.

### 1.2 Parallel activity awaits

- **Capture.** A suspension can await one command of any covered kind, or
  two or more activities. Each awaited activity keeps its own live channel.
- **Resume.** The delta starts with the events of the last suspension.
  Then it holds progress events and at least one resolving event. Each
  resolving event completes a distinct parked activity. A single parked
  timer or signal resolves as before.
- **Re-park.** Before the poll, each unresolved activity gets a
  `WaitForActivity` command that holds its live sender. A cold replay
  emits the same command. It writes no event.
- **Post-poll check.** When a re-park exists, the cycle must suspend with
  only its re-parks, and each must still have a live receiver. Otherwise
  the resume declines with `ResumeDeclined::SiblingStillParked`. The worker
  drops the future and replays cold.
- **Speculation.** A cycle with a re-park must not read the replay
  position, and it fires no side effect that replay suppresses. A read
  declines the resume.
- **One cycle per result.** A delta with several results runs as one
  cycle per result, in history order. A cold replay matches results in
  that order, so each early result faces the check of its own cycle.

### 1.3 Known limit

A join branch that runs a command after its own await fails a cold
replay when a later branch has a command event (E6, E7). The checks
catch this shape while a sibling is still parked, also when both results
land in one delta in branch order. When the earlier branch's result
arrives last, the warm decision cannot tell the branch code from code
after the join (E3). It runs on, and the failure shows at the next cold
replay. Issue #1798 has the same limit for a single wait.
`docs/sticky-routing.md` names the limit, and `ReplayVerifier` finds
such a history before a deploy.

## 2. Tests

Red first, then green. Each row names the test that pins it.

| Behaviour | Test |
|-----------|------|
| Capture names each miss reason. | `resident::tests::capture_names_why_a_suspension_is_not_resident` |
| A race in flight is a `race` miss on a cold replay. | `resident::tests::a_race_in_flight_is_a_race_miss_on_a_cold_replay` |
| A cache entry keeps its miss reason. | `cache::tests::an_entry_keeps_the_miss_reason_of_its_suspension` |
| The labels are stable. | `telemetry::tests::resident_outcome_labels_are_stable_and_distinct` |
| The worker records one outcome per decision. | `resident_outcome_tests::a_sequential_run_is_cold_once_then_hits` |
| A mixed join is a `multi_await` miss. | `resident_outcome_tests::a_join_of_an_activity_and_a_signal_is_a_multi_await_miss` |
| A join of activities stays resident. | `resident::tests::an_activity_join_stays_resident` |
| Warm equals cold for a join, in twelve arrival modes with and without progress events. | `resident::tests::warm_activity_join_matches_cold_replay_in_every_arrival_order` |
| An agent loop with parallel tool calls resumes every decision. | `resident::tests::warm_tool_loop_resumes_every_decision_in_every_arrival_order` |
| A partial result does not replay the body. | `resident::tests::warm_tool_loop_runs_the_body_once` |
| The fan-out helper matches cold. | `resident::tests::warm_fan_out_matches_cold_replay_in_every_arrival_order` |
| A branch that runs on, fails or reads the position while a sibling is parked declines. Two results in one delta run in history order. | `resident::tests::a_branch_that_runs_on_while_a_sibling_is_parked_replays_cold` |
| A join declines when the context inputs change. | `resident::tests::a_join_declines_when_its_context_inputs_change` |
| A worker with resident state off records nothing. | `resident_outcome_tests::a_worker_with_resident_state_off_records_no_outcome` |
| A partial delta re-parks the sibling. | `resident::tests::a_partial_delta_re_parks_the_sibling` |
| Join deltas that a replay could read another way decline. | `resident::tests::join_deltas_that_replay_could_read_differently_decline` |
| A failed join with a parked sibling declines. | `resident::tests::a_sibling_left_parked_by_a_failed_branch_declines` |
| A join hits on each tool result through the worker. | `resident_outcome_tests::a_join_of_activities_hits_on_every_tool_result` |
| The agent-loop hit rate through the worker. | `resident_outcome_tests::an_agent_loop_with_parallel_tool_calls_hits_after_the_first_decision` |
| A join resumes warm in the DST world. | `dst_world_tests::a_join_resumes_warm_through_the_worker` |
| The seeded sweep holds every invariant with the join workload. | `dst_world_tests::a_world_sweep_covers_the_scope`, `world_seed_sweep` |

## 3. Acceptance criteria

| Criterion | Evidence |
|-----------|----------|
| Depends on the hit-rate sibling (#2007). | The counter part of #2007 ships first, in its own commit: `harvest.workflow.resident` with a bounded miss reason, listed in `docs/telemetry.md`. Section 4 measures with it. The e2e bench run of #2007 stays open there. |
| The path covers the dominant miss reason, and the hit rate on the same workload rises. | Before the change, the agent loop misses with `multi_await`, and then with `blocked` on the wait that the cold replay leaves. A join of activities now stays resident. Section 4: 3 of 10 hits before, 10 of 11 after, on the same workload. |
| The extended path runs under DST. | The DST world's scheduled workflow joins two activities. `a_join_resumes_warm_through_the_worker` pins a warm partial resume through the real worker. The seeded sweep passes every invariant, with more warm decisions than before. |

## 4. Measurement

Both runs use the same code for the workload and the measurement. The
"before" run is the red commit, which has the counter and the workload
but not the extension.

**Agent loop on a real worker.**
`resident_outcome_tests::an_agent_loop_with_parallel_tool_calls_hits_after_the_first_decision`
runs three rounds of one model call and four parallel tool calls. The
test prints the outcome of each decision.

| Run | Hits | Decisions | Outcomes |
|-----|------|-----------|----------|
| Before | 3 | 10 | `cold, hit, multi_await, blocked, hit, multi_await, blocked, hit, multi_await, blocked` |
| After | 10 | 11 | `cold`, then 10 `hit` |

The before run shows a second miss. A cold replay of a partial join
leaves the `ActivityStarted` event of a running tool unread. Capture then
refuses the last wait as `blocked`, so the next decision replays too. A
cold capture of `Sa Sb Started(a) Ca` stays resident, and one of
`Sa Sb Started(a) Ca Started(b)` reports `blocked`. A resident join does
not replay, so that miss goes away as well.

This is one run of each. The number of decisions depends on how the tool
results arrive. The deterministic evidence is the differential test
`warm_tool_loop_resumes_every_decision_in_every_arrival_order`: before
the change, the oldest-first order resumed 4 of 8 decisions; now it
resumes all 8, and every other mode resumes every decision too.

**DST world sweep, seeds 0 to 11.**
`HARVEST_DST_SEEDS=12 cargo test -p autumn-harvest --test integration
dst_world_tests::world_seed_sweep -- --nocapture --test-threads=1`

| Run | Warm | Cold | Declined (a cache hit that replayed) |
|-----|------|------|--------------------------------------|
| Before | 33 | 170 | 30 |
| After | 44 | 170 | 19 |

A cold decision follows a crash, a restart or a first decision, so its
count does not change. Every invariant holds in both runs. With the new
workload, `a_planted_failure_replays_from_its_seed_alone` still fails
seed 0 with the plant, and the seed still passes without it.

**Reproduce.** Point `HARVEST_TEST_DATABASE_URL` at a migrated Postgres
and run the two commands above, at the red commit and at the head.
