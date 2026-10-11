# Design — claim concurrency: more than one claim in flight per worker

Assay #14 found that one worker runs one claim at a time. Claims per second
times the mean claim time reads 0.91 to 0.99 in every harvest run. That fits
one claim in flight. Throughput then follows
`1 / (claims per workflow × claim latency)`. The #1971 fix cut the claim
latency, but it did not add claim concurrency. The assay names this as the
next lever (re-charter item 1).

This change lets one worker run up to `max_concurrent_claims` claims at
once. **No SQL change. No migration. No new `WorkflowEvent` variant.**

---

## 0. Planning record

### 0.1 Research summary

| Finding | Effect on this design |
|---|---|
| Assay #14: claim-loop occupancy 0.94; 7 claims per workflow; claim mean 5.7 to 5.9 ms on `7bf3789`; 22.7 workflows/s against Temporal's 28.7. | A second claim in flight is the lever. The claim itself is already flat with depth. |
| Assay #14: mean pool use 2 to 4 of 32 connections; pool-wait p99 near 2 ms. | The pool has room for more claims in flight. |
| Temporal SDKs run several pollers per task kind. The Go SDK default is 2 per kind. Newer SDKs scale pollers with demand. | Several claim loops per worker are normal. Demand-driven scaling avoids idle load. |
| `poll_once` takes one real permit of each admitted kind before it claims (#1787). `try_acquire_owned` is atomic. | Each loop can call `poll_once` on its own. No loop can claim past the local permits. |
| `FOR UPDATE SKIP LOCKED` already lets many workers claim at once. | More loops in one worker add no new claim race. |
| The leader loop owns the only `LISTEN` connection, and the `capacity_freed` wake. | Extra loops cannot share them. They need their own wake. |

### 0.2 Brainstorm — how can one worker claim more per second?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Run N copies of today's poll loop. | Rejected. Each copy polls on its own timer, so idle claim load grows N times. Only one copy can own the listener. |
| B2 | Keep today's loop as the leader. Add N − 1 follower loops. A follower claims only after a sibling claim succeeds, and stops at the first empty claim. | **Adopted.** Idle cost does not change. Concurrency grows only while claims return work. The leader alone still guarantees progress. |
| B3 | Claim K rows in one statement (`claim_task_batched`). | Deferred. Its candidate scan is the full scan. Each poll holds one permit per kind. It changes claim order (assay 0005). |
| B4 | Start the next claim before the dispatch of the last one. | Rejected. `dispatch_task` already spawns. Nothing waits between claims except the claim. |
| B5 | Eager activity dispatch: a workflow task claims its own new activity rows. | Deferred. It changes claim ownership and needs its own fence review. |
| B6 | Scale the loop count with load, as Temporal does. | Partly adopted. B2 is demand-driven. The cap stays a static setting. |
| B7 | Cut the claim latency again. | Out of scope. See DESIGN-1971.md §4. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Loops claim more tasks than the local permits allow. | Each loop takes its own `PollPermits` before it claims. Tests: `concurrent_claims_never_exceed_the_activity_permits` and `concurrent_claims_never_exceed_the_workflow_permits`. |
| R2 | Two loops claim one row. | The claim SQL is unchanged. `FOR UPDATE SKIP LOCKED` decides. Test: every task of a backlog runs exactly once. |
| R3 | Idle claim load grows with the loop count. | A follower has no timer and no listener. Test: `an_idle_worker_claims_at_the_single_loop_rate`. |
| R4 | A follower claims after shutdown starts, so a task misses the drain. | The followers run in the same future as the leader. The drain starts only after every loop returns. Test: `no_claim_starts_after_a_concurrent_worker_stops`. |
| R5 | A burst of NOTIFY wakes every loop at once. | A follower wakes on a sibling success only, one wake per success. |
| R6 | A wake is lost and work waits. | `Notify::notify_one` stores one wake. The leader polls on its own timer in any case. |
| R7 | Followers use the Redis dispatch channel, or a shard with an unverified registration. | A follower polls Postgres only where the leader would. It skips a shard with a channel or with a pending registration. Test: `a_follower_polls_postgres_only_where_the_leader_does`. Known limit: in degraded dispatch mode the leader drains Postgres alone (§4). |
| R8 | Followers take every pool connection. | Each loop holds one connection for one claim. The default cap is small. The worker warns at startup when the claim loops can fill a pool. The upgrade guide and the pool runbook say to size the pool for it. |
| R9 | Tests that need a strict claim order become flaky. | Existing worker tests keep the default, so they run followers. The affected suites pass. A test that needs a strict order can set `max_concurrent_claims = 1`. |
| R10 | `harvest.worker.pollers` changes meaning. | It counts running claim loops, as before. Followers are claim loops. The docs say so. |
| R11 | Claim order changes. | Two loops can take rows slightly out of order, as two workers can today. `docs/performance.md` already documents this. |
| R12 | Each follower run ends on an empty claim. That claim also runs the throttle check, and a stored wake adds one more empty claim. | A follower skips the throttle check, so the leader alone emits `harvest.rate_limit.throttled`. A follower drops a stored wake when its run ends. Test: `an_idle_worker_claims_at_the_single_loop_rate` counts claims after a burst. |
| R13 | A worker's own loops contend for one hot concurrency key or one rate-limit bucket. | The loser of `pg_try_advisory_xact_lock` gets an empty claim, as with two workers. The upgrade guide advises 1 claim loop for such a queue. |
| R14 | The outlier cohort mixes workers with other claim caps. | `ExecutionPolicy` keys the cohort on `claim_loops`. |

### 0.4 Six thinking hats — B2

| Hat | Notes |
|-----|-------|
| White | Assay #14 numbers above. Measured after the change on assay #14's workload: 1 loop keeps 0.94 claims in flight, 2 loops 1.86 and 4 loops 3.3. Throughput rises 21% to 35% with 2 loops, and 39% to 59% with 4. See `docs/performance.md` § "Claim concurrency". |
| Red | The leader loop does not change, so the old behaviour stays as a floor. That is easy to trust. |
| Black | (1) On a 4-core box, Postgres and the worker share the CPUs. Two loops may give less than 2×. Measured: the mean claim time rises from 4 ms to 6 ms with 2 loops and to 9 ms with 4. (2) Followers add claim statements under load. (3) One more connection per extra loop. (4) A hot concurrency key makes loops lose `pg_try_advisory_xact_lock` more often. They return an empty claim, as two workers do today. |
| Yellow | No SQL, schema or event change. The exactly-once, `SKIP LOCKED`, permit and fence arguments carry over unchanged. |
| Green | B3 and B5 stay open. An adaptive cap can come later. |
| Blue | Red: a slow-claim test that measures overlapping claims fails. Green: the setting and the follower loops. Refactor: shared helpers, docs and measurement. Then a multi-angle review. |

---

## 1. Design

### 1.1 The setting

`WorkerConfig::max_concurrent_claims` caps the claims one worker runs at
once. The value `1` is the old serial loop. The value `0` is invalid.

### 1.2 The loops

- The **leader** is today's loop: `run_poll_loop` or `run_poll_loop_multi`.
  It owns the listener, the timer and the `capacity_freed` wake. After a
  Postgres claim succeeds, it wakes one follower.
- A **follower** waits for a wake. It then claims until a claim returns
  nothing, and wakes one more follower after each success. It has no timer
  and no listener.
- All loops run in one `join`. The drain starts after every loop returns.

### 1.3 Where a follower claims

A follower claims through `poll_once` only where the leader would poll
Postgres. It skips a shard with an installed dispatch channel, and a shard
whose registration is not verified. On a multi-shard worker it rotates its
start shard, as the leader does.

## 2. Acceptance criteria

No issue tracks this change. These criteria come from assay #14's
re-charter item 1.

| AC | Statement |
|---|---|
| AC1 | Red first: a DB test shows that one worker never runs two claims at once. |
| AC2 | A worker runs up to `max_concurrent_claims` claims at once. The default is above 1. The value 1 restores the serial loop. |
| AC3 | Safety: no claim past the local permits, each task runs once, no claim after shutdown starts, idle claim rate unchanged. Followers respect the dispatch channel and the registration gate on both loop shapes. |
| AC4 | Measured: assay #14's harvest arms before and after, with claim-loop occupancy. The result is in `docs/performance.md`. |
| AC5 | The setting is in the builder, the runtime config, the effective config and the docs, with a changelog fragment. |

## 3. Test plan

| Phase | Test | Expected in red | Expected in green |
|---|---|---|---|
| Red | `a_worker_overlaps_claims_up_to_its_cap` | Fail: the largest overlap is 1. | Pass. |
| Red | `concurrent_claims_never_exceed_the_activity_permits` | Fail: the largest overlap is 1. | Pass. |
| Red | `no_claim_starts_after_a_concurrent_worker_stops` | Fail: the largest overlap is 1. | Pass. |
| Red | `max_concurrent_claims_of_one_keeps_claims_serial` | Pass. | Pass. |
| Red | `every_task_runs_once_under_concurrent_claims` | Pass. | Pass. |
| Red | `an_idle_worker_claims_at_the_single_loop_rate` | Pass. | Pass. |
| Green | `a_multi_shard_worker_overlaps_claims_up_to_its_cap`, `concurrent_claims_never_exceed_the_workflow_permits` | — | Pass. |
| Green | Unit tests: validation, builder default, effective config, follower predicate, pool check, cohort key | — | Pass. |
| Both | Existing claim and worker suites | Pass. | Pass. |

## 4. Follow-ups (not in this change)

1. Batched claims (B3), once the batched path reads the seek window.
2. Eager activity dispatch (B5).
3. An adaptive cap from claim-loop occupancy.
4. Followers in degraded dispatch mode. While a channel fails, the leader
   drains Postgres alone in `drain_postgres`. A shared per-shard flag could
   let followers help.
