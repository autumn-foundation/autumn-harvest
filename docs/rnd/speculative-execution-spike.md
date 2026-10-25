# Speculative durable execution — R&D spike report (issue #2011)

> **Status: R&D spike, closed with a no-go.** The engine does not change.
> The model lives in the DST harness, in `autumn_harvest::dst::speculate`.

**Audit date:** 2026-10-11. **Audited revision:** `8a40df5`.

The engine code is the `trunk-dev` revision `8a40df5`. The bench ran that
engine with the probe of this change added.

The spike asks one question. Can a worker run the next decision on
resident state while the previous commit is in flight? The worker
discards that decision when the commit or the claim fence fails.

The prototype files are:

- `autumn-harvest/src/dst/speculate.rs`, the model.
- `autumn-harvest/tests/dst/speculate.rs`, its tests.
- `HARVEST_BENCH_COMMIT_PROBE` in
  `autumn-harvest/tests/integration/e2e_bench_support.rs`, the probe.
- `docs/design/DESIGN-2011.md`, the plan and the go criteria.

---

## Decision summary

**Verdict: no-go.** Do not build speculative decisions into the engine.

| Question | Answer |
|---|---|
| What share of end-to-end latency is commit time? | About 2.4 % unloaded and 1.6 % under load, as a ratio of medians. With every commit at the persist p99, it is 5.9 % unloaded. |
| How much does gated speculation save? | Nothing measurable. Paired runs give equal mean latency. |
| Is gated speculation safe? | Yes, in the model. It keeps every invariant over 1,000 seeds under faults. |
| Does early release of effects save more? | About 3 % in the model, or 1.8 % at the measured commit p50. |
| What does early release break? | The commit gate of issue #1796. An effect can run twice, and some runs never complete. |
| Does speculation need the claim fence? | Not for the safety invariants of the model. A prefix check alone keeps them. |
| Is asymmetric logging worth it? | Not now. It saves 30 % to 37 % of log rows. The engine needs those rows. |
| Where is the latency? | In the wake hop, from the end of an activity to the next decision. It takes about 88 % of unloaded latency. |

## The question

Issue #2011 cites two results. libDSE reports up to 10x lower end-to-end
latency than Temporal and Durable Functions baselines. Halfmoon shows that
logging only reads, or only writes, is enough for exactly-once execution.
That cuts logging overhead by 1.5x to 4.0x.

Harvest puts every durable fact of a decision in one Postgres commit. It
releases outside effects only after that commit. Post-commit `NOTIFY`
(issue #1796) and the transactional outbox enforce that gate.

In Harvest, a decision is speculative until its commit lands. The risks
that the issue names are fencing, replay determinism of speculative
branches and repair semantics.

## Method

The issue limits the prototype to the DST harness. The spike thus has
two parts.

1. **Measure** the e2e bench with an opt-in probe. A persist is the
   transaction that writes one decision, `COMMIT` included. The probe
   records the duration of each persist and of each claim attempt. It
   also reads the start-to-completion time of each run.
2. **Model** the worker in `dst::speculate`. The model is a discrete-event
   simulation with no database. It takes its durations from the bench.

The probe starts no metric sampler. A sampler issues its own queries and
would load Postgres during the measurement.

### The model

| Part | Meaning |
|---|---|
| Execution | A number of rounds. A round schedules activities and waits for all results. |
| Decision | Reads each durable input that resident state has not seen. Schedules the next round or completes. |
| Commit | Appends one decision record. It must follow the decision prefix that it read. |
| Effect | One activity run. Its result becomes a durable input only if its schedule is durable. |
| Worker | Holds claims, resident state and a commit chain. |
| Reclaimer | Moves the claim of a dead or stalled worker. The epoch goes up, and the new owner loads cold. |

A commit chain is the queue of decisions that wait to commit. It holds at
most 4 decisions (`MAX_CHAIN`). One commit of an execution is in flight
at a time.

The modes are:

- `Serial`: decide, commit, wait, then decide again. This is the engine
  today.
- `Gated`: decide again while a commit is in flight. Release effects only
  after their own commit lands.
- `Eager`: as `Gated`, but release effects at the decision, before the
  commit. It shows what the commit gate costs.

The fences are `Epoch` (prefix and claim) and `PrefixOnly`. Most
suspended branches of the engine append from the event id read at the
decision start. A concurrent append then makes the commit fail on
`UNIQUE (workflow_exec_id, event_id)`. Branches that start child
workflows read the event id again under the row lock, so they have no
prefix check. No suspended branch checks the claim epoch, unless the run
is paused. `PrefixOnly` models the common branches.

Two more engine rules apply in every mode:

- A commit fails when an activity result was appended after its decision
  read the history. The result took the event id that the commit uses.
- A commit holds the row lock of its execution while it is in flight. An
  activity result waits for that lock before it appends. A signal does
  not wait: it goes to a separate table until a decision reads it.

At each commit send, the seed can inject three faults:

- a crash while the commit is in flight;
- a stall just before the send;
- a failed commit, which applies nothing.

A crash loses or lands its in-flight commit with equal chance. A seed has
at most 3 crashes and stalls in total, over its 4 executions. Failed
commits have no cap. A stall delays only the worker. The store applies a
commit that is already in flight at its landing time.

Each draw depends only on the seed and a key: the decision position, the
effect or the epoch. So two modes draw equal durations and faults for
equal decisions, and their runs pair seed by seed.

The workloads are `Chain` and `FanOut`. `Chain` is the bench workflow:
three activities in sequence. `FanOut` has two rounds of four parallel
activities, and two signals. It completes only after both signals, and
its output adds their values. So a wrong signal state in a speculative
decision breaks `ReplayEquivalent` or `ExpectedOutput`.

### Invariants

| Invariant | Meaning |
|---|---|
| `CommitByOwner` | An applied commit comes from the current claim holder. |
| `ReplayEquivalent` | Replay of the durable log gives each decision that the log holds. |
| `EffectAfterCommit` | An effect leaves the worker only after the commit that schedules it. |
| `EffectOnce` | An effect runs at most once. |
| `ExpectedOutput` | A completed execution returns the value that its workload defines. |
| `Converges` | Every execution completes within the event limit. |

Under `Gated` with the `Epoch` fence, three of these hold by
construction. The fence is the `CommitByOwner` predicate. `Gated`
releases an effect only when its schedule applies. An applied decision
always moves to a new round, so no effect runs twice.

The evidence for `Gated` is thus in `ReplayEquivalent`, `ExpectedOutput`
and `Converges`. Those three fail when a repair keeps bad speculative
state. Two planted defects prove it.

## Measured latency

The host has 4 logical CPUs, an Intel Xeon at 2.1 GHz. It runs PostgreSQL
16.15 with `max_connections = 200`. The bench ran at 1 shard, release
profile, with `HARVEST_BENCH_COMMIT_PROBE=1`. Unloaded means 1 workflow
in flight. Loaded means the bench default of 32.

| Run | `fsync` | Workflows/s | Start to completion p50 / p99 | Persist p50 / p99 | Persists per run | Commit share of p50 |
|---|---|--:|--:|--:|--:|--:|
| Unloaded, 200 runs | on | 2.04 | 473.26 / 519.50 ms | 2.82 / 6.95 ms | 4.00 | 2.4 % |
| Loaded, 600 runs | on | 32.84 | 911.50 / 1227.70 ms | 3.60 / 9.81 ms | 4.00 | 1.6 % |
| Unloaded, 200 runs | off | 2.07 | 467.35 / 518.41 ms | 2.37 / 5.06 ms | 4.00 | 2.0 % |
| Loaded, 600 runs | off | 37.43 | 823.01 / 969.57 ms | 3.26 / 9.07 ms | 4.00 | 1.6 % |

The commit share is four persist p50 values divided by the run p50. It is
a ratio of medians, not a bound. The probe does not tie a persist to its
run, so it cannot give the share of each run.

A pessimistic case gives a firmer limit. If all four commits of a run took
the persist p99, they would be 4 x 6.95 ms = 27.8 ms. That is 5.9 % of the
unloaded run p50, and 4.3 % of the loaded one at 9.81 ms each. The `fsync`
setting moves the persist p50 by less than 0.5 ms on this host.

A loaded run includes the time that a run waits in the closed loop before
its first claim. So its latency is mostly queueing, not engine hops.

The run start is the database clock at the start row. The run end is the
worker host clock. The host-to-database offset was under 0.1 ms here.

Other numbers from the same `fsync`-on session:

| Scenario | Value |
|---|--:|
| `dispatch_latency` p50 / p99 | 12.58 / 90.11 ms |
| `replay_throughput` | 9,851,862 events/s |
| Claim attempt p50 / p99, unloaded, empty polls included | 2.77 / 5.93 ms |

### Calibration

`Timing::BENCH` holds the model durations:

```rust
decide_us: 2,
commit_us: (2_820, 6_950),
dispatch_us: 12_580,
activity_us: (50, 150),
wake_us: 138_560,
```

- The commit range is the persist p50 to p99. Its mean is 4.885 ms, above
  the p50.
- The dispatch hop is the `dispatch_latency` p50.
- The decide time follows from replay throughput. The bench history has
  fewer than 20 events, at 0.1 µs each.
- The bench activity is a no-op, so its body takes microseconds.
- The wake hop is not measured directly. It is the measured p50 minus the
  other model hops.

The wake hop runs from the end of an activity to the start of the next
decision. It contains the commit of the result and the post-commit
`NOTIFY` gate of up to 25 ms. It also contains the claim settle delay of
50 to 75 ms and the claim. No speculation over a commit can reach it.

`the_calibrated_model_matches_the_measured_bench` checks that the model
`Serial` mean is within 2 % of the measured 473.26 ms. It is 473.25 ms.
The wake hop was fitted to that p50, so the check proves the closed form,
not an independent prediction.

## DST results

Each row is a sweep of 1,000 seeds, 4 executions each. Each seed runs
twice, and the two traces must be equal. The default faults are on
unless a row says otherwise.

"Busy" faults are the default faults, but one commit in five fails.
"Spread" timing is bench timing with activities from 1 ms to 100 ms.

### Safety

| Config | Result |
|---|---|
| `Serial` and `Gated`, `Chain` and `FanOut`, `Full` and `ReadsOnly` | Pass. Every invariant holds in all 8 configs. |
| `Gated`, `FanOut`, spread timing, busy faults | Pass. 297 speculative decisions were discarded and repaired. |
| `Gated` with the `keep-tail` plant, the config above | Fail on seed 41: `ReplayEquivalent`. |
| `Serial` with the `keep-tail` plant, the same config | Pass. `Serial` has no speculative tail. |
| `Gated` with the `keep-on-failure` plant | Fail on seed 0: `Converges` on `Chain`, `ReplayEquivalent` on `FanOut`. |
| `Gated`, `PrefixOnly` fence, all checks | Fail on seed 18 (`Chain`) and seed 24 (`FanOut`): `CommitByOwner`. |
| `Gated`, `PrefixOnly` fence, all other checks | Pass. 50 stale commits on `Chain` and 74 on `FanOut` do no harm. |
| `Eager`, `EffectAfterCommit` | Fail on seed 0. An effect leaves before its commit, by design. |
| `Eager`, `EffectOnce` | Fail on seed 0. |
| `Eager`, `Converges` | Fail on seed 3. |
| `Eager`, `ReplayEquivalent`, `ExpectedOutput`, `CommitByOwner` | Pass. |

In the `Serial` `Chain` sweep, the faults gave 767 crashes, 839 stalls,
828 failed commits and 2,453 reclaims. The claim fence rejected 574
commits. Crashes lost 465 in-flight commits, and 427 others applied.

The `keep-tail` plant only affects speculation. After a failed commit, it
drops the failed decision but sends the speculative decisions behind it.
Those decisions were built on the failed one, so replay disagrees.

The `keep-on-failure` plant drops the commit chain but keeps the state
that the chain built. On `Chain`, that state waits for a result that no
commit scheduled. On `FanOut`, its next commit disagrees with replay.

The `Eager` failures have two causes:

- A failed commit discards a decision whose effect already left. The
  repaired decision schedules the same effect, so it runs twice.
- A stall before the send lets the effect finish first. The store rejects
  its result, because no commit scheduled it. The commit then lands, and
  nothing runs the effect again. The run never completes.

Over 1,000 seeds, `Eager` stranded 219 of 4,000 `Chain` runs (5.5 %) and
151 of 4,000 `FanOut` runs (3.8 %). It ran 1,189 more effects than
`Serial` on `Chain`, and 3,536 more on `FanOut`.

### Latency

Mean latency over 4,000 runs, in milliseconds.

| Timing | Workload | Faults | `Serial` | `Gated` | `Eager` |
|---|---|---|--:|--:|--:|
| Bench | `Chain` | none | 473.254 | 473.254 | 458.614 |
| Bench | `Chain` | default | 607.281 | 607.281 | 576.264 |
| Bench | `FanOut` | none | 527.961 | 527.961 | 527.744 |
| Bench | `FanOut` | default | 589.908 | 589.913 | 585.475 |
| Spread | `FanOut` | none | 811.571 | 811.571 | 811.660 |
| Spread | `FanOut` | default | 927.384 | 927.386 | 922.480 |

A `FanOut` run waits for its last signal, so signal times set most of its
latency.

The `Eager` rows with faults average only the runs that completed.

`Eager` saves 3.1 % on bench `Chain`. That is three model commit means
of 4.885 ms. At the measured commit p50 of 2.82 ms, three commits are
1.8 % of 473.26 ms.

`Gated` runs few speculative decisions, and they save nothing. With no
faults, it made none on bench `FanOut` and 20 on spread `FanOut`. Paired
runs gave equal latency on all 1,000 `Chain` seeds and on 996 of 1,000
spread `FanOut` seeds. With faults, the `Gated` mean is at most 0.005 ms
above the `Serial` mean. Four facts explain this:

- On the `Chain` workload, the next input needs an activity. The activity
  needs the commit. So no input is ready while a commit is in flight.
- An activity result waits for the row lock of a commit in flight. So no
  new result becomes durable during the commit. Only a signal can feed a
  speculative decision. On spread `FanOut`, 881 result appends waited.
- With bench timing, the four results of a round land within about
  100 µs. One 2 µs decision reads them all.
- A speculative decision overlaps only an in-flight commit. It can save
  one decide time, here 2 µs. It can also split a batch into two commits,
  which costs one commit.

### Cited tests

The cited tests run 64 or 256 seeds. The 1,000-seed numbers come from
`speculation_sweep` and from the same configs.

| Test | What it shows |
|---|---|
| `serial_chain_latency_has_a_closed_form` | The model latency is the sum of its hops. |
| `the_calibrated_model_matches_the_measured_bench` | The model matches the bench within 2 %. |
| `gated_speculation_has_nothing_to_run_on_a_chain` | `Gated` equals `Serial` on the `Chain` workload. |
| `bench_timing_gives_fan_out_no_speculation` | Bench timing gives fan-out no speculation over seeds 0 to 63. |
| `gated_speculation_leaves_paired_fan_out_latency_unchanged` | Paired runs agree on at least 95 % of seeds. The sums differ by under 0.1 %. |
| `gated_discards_speculative_work_and_keeps_every_invariant` | A busy sweep discards speculative work and keeps every invariant. |
| `a_speculation_only_defect_is_found_and_replays_from_its_seed` | The `keep-tail` plant fails `Gated` only, and its seed replays it. |
| `eager_release_hides_one_commit_per_hop_on_a_chain` | `Eager` saves exactly one commit per hop. |
| `eager_release_breaks_the_commit_gate` | `Eager` breaks `EffectAfterCommit`. |
| `eager_release_runs_an_effect_twice_after_a_failed_commit` | `Eager` breaks `EffectOnce`. |
| `eager_release_can_strand_an_execution` | `Eager` breaks `Converges`. |
| `a_prefix_only_fence_lets_a_stale_owner_commit` | `PrefixOnly` breaks `CommitByOwner`. |
| `a_prefix_only_fence_keeps_every_other_invariant` | `PrefixOnly` keeps every other invariant. |
| `serial_and_gated_keep_every_invariant_under_faults` | `Serial` and `Gated` pass under each fault. |
| `a_result_waits_for_the_row_lock_and_a_stale_read_loses` | A result waits for the row lock, and a stale read fails its commit. |
| `a_planted_repair_defect_is_found_and_replays_from_its_seed` | The `keep-on-failure` plant fails, and its seed replays it. |
| `reads_only_logging_drops_exactly_the_write_rows` | `ReadsOnly` drops the write rows and nothing else. |
| `golden_speculation_traces_are_equal_on_every_platform` | Fixed seeds give fixed traces on each platform. |

## Asymmetric logging

`ReadsOnly` stores only the inputs that each decision read. Replay derives
the scheduled effects. Every invariant holds, with the same latency.

| Workload | `Full` rows | `ReadsOnly` rows | Saved |
|---|--:|--:|--:|
| `Chain`, 1,000 seeds with faults | 40,000 | 28,000 | 30 % |
| `FanOut`, 1,000 seeds with faults | 86,919 | 54,919 | 37 % |

The saving does not carry over to Harvest for three reasons:

- An `ActivityScheduled` event carries the activity input and id. Replay
  matches each new command to its recorded event by type and name. It
  needs the recorded `activity_id` to find each result. The strict
  replayer also compares the input.
- The task row that releases an effect is a write in the same commit. It
  stays.
- Persist is about 1.6 % to 2.4 % of median latency. Fewer rows in that commit cannot
  move the end-to-end number much.

## Go / no-go

**Verdict: no-go** for speculative decisions and for asymmetric logging.

`docs/design/DESIGN-2011.md` fixed three go criteria before the measurement.

| Criterion | Result |
|---|---|
| 1. `Gated` cuts unloaded latency of the bench workflow by 20 % or more. | **Not met.** It cuts 0 %. All four commits at the persist p99 would still be only 5.9 %. |
| 2. `Gated` keeps every invariant under faults. | **Met.** Every invariant holds in 8 configs and a busy sweep, at 1,000 seeds each. |
| 3. The gain does not need `Eager`. | **Not met.** Only `Eager` gains, and it breaks three invariants. |

**Why Harvest gains so little.** Harvest already puts each decision in one
commit. The commit is a small part of a hop. Most of a hop is the wake
path after the commit.

**What is worth keeping.** The model and its fault sweeps stay as DST
tests. In the model, a prefix check alone keeps the safety invariants.
The model can test a later idea, such as decision batching.

**Where to look instead.** The wake hop is about 88 % of the unloaded
latency of the bench workflow. The post-commit `NOTIFY` gate and the claim
settle delay are in that hop. A follow-up can measure each part.

## Known limits

- The model is not the engine. It has no delta load, no timers, no child
  workflows and no local activities.
- The wake hop is fitted, not measured directly.
- The decide time comes from replay throughput. A warm resume does less
  work than a cold replay. A real decision also loads its new events.
- The bench ran on one host with one shard. The numbers are for this
  revision and host, not for every deployment.
- The model commit range has a mean above the measured p50. So the model
  overstates what hiding a commit saves.
- `Eager` has no result buffer and no rollback. A result buffer would fix
  `Converges`, but not `EffectOnce`.
- Faults come only at a commit send. The model has no network partition
  and no clock skew.
- A live worker always learns the true outcome of its commit. The model
  has no ambiguous commit outcome.

## Reproduce

The model sweeps need no database:

```sh
cargo test -p autumn-harvest --no-default-features --test dst speculate::
HARVEST_DST_SEEDS=1000 HARVEST_DST_SPEC_MODE=gated HARVEST_DST_SPEC_WORKLOAD=fan-out \
  cargo test --release -p autumn-harvest --no-default-features --test dst \
  speculate::speculation_sweep -- --nocapture
```

The bench needs Postgres:

```sh
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
HARVEST_BENCH_COMMIT_PROBE=1 HARVEST_BENCH_SHARDS=1 \
HARVEST_BENCH_INFLIGHT=1 HARVEST_BENCH_WORKFLOWS=200 \
HARVEST_BENCH_SCENARIOS=throughput,dispatch_latency,replay_throughput \
  cargo bench -p autumn-harvest --features db,testing --bench e2e_bench
```

Unset `HARVEST_BENCH_INFLIGHT` and set `HARVEST_BENCH_WORKFLOWS=600` for
the loaded run. The guards in `speculative_execution_docs` keep this
report in step with the model.
