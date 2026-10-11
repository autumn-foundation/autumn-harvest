# Speculative durable execution — R&D spike report (issue #2011)

> **Status: R&D spike, closed with a no-go.** The engine does not change.
> The model lives in the DST harness, in `autumn_harvest::dst::speculate`.

**Audit date:** 2026-10-11. **Audited revision:** `8a40df5`.

The spike asks one question. Can a worker run the next decision on
resident state while the previous commit flushes? The worker discards
that decision when the commit or the claim fence fails.

The prototype files are:

- `autumn-harvest/src/dst/speculate.rs`, the model.
- `autumn-harvest/tests/dst/speculate.rs`, its tests.
- `HARVEST_BENCH_COMMIT_PROBE` in
  `autumn-harvest/tests/integration/e2e_bench_support.rs`, the probe.
- `DESIGN-2011.md`, the plan and the go criteria.

---

## Decision summary

**Verdict: no-go.** Do not build speculative decisions into the engine.

| Question | Answer |
|---|---|
| How much end-to-end latency is commit time? | 2.6 % unloaded and 1.6 % under load, on the bench workflow. |
| How much does gated speculation save? | About 0 %. The largest model gain is 5 µs on a 340 ms mean. |
| Is gated speculation safe? | Yes, in the model. It keeps every invariant over 1,000 seeds under faults. |
| Does early release (libDSE) save more? | 2.95 %, one commit per hop. It breaks the commit gate of issue #1796. |
| What does early release break? | An effect runs twice after a failed commit. 6.1 % of chain runs never complete. |
| Does speculation need the claim fence? | No. The prefix check alone keeps every safety invariant except owner identity. |
| Is asymmetric logging worth it? | Not now. It saves 30 % to 39 % of log rows, but the engine needs those rows. |
| Where is the latency? | The hop from an activity result to the next decision: about 136 ms of each 155 ms hop. |

## The question

libDSE reports up to 10x lower latency than Temporal and Durable
Functions. It runs the next step before the previous log write is
durable. It also releases messages to other components before that write.

Halfmoon shows that logging only reads, or only writes, is enough for
exactly-once. That cuts logging overhead by 1.5x to 4.0x.

Harvest puts every durable fact of a decision in one Postgres commit. It
releases outside effects only after that commit. Post-commit `NOTIFY`
(issue #1796) and the transactional outbox enforce that gate.

So the speculation boundary in Harvest is "the commit has not happened
yet". The risks that the issue names are fencing, replay determinism of
speculative branches and repair semantics.

## Method

The issue limits the prototype to the DST harness. The spike thus has
two parts.

1. **Measure** the e2e bench with an opt-in probe. The probe records the
   persist and claim durations of each workflow task. It also reads the
   start-to-completion time of each run from the database clock.
2. **Model** the worker in `dst::speculate`. The model is a discrete-event
   simulation with no database. It takes its durations from the bench.

### The model

| Part | Meaning |
|---|---|
| Execution | A number of rounds. A round schedules activities and waits for all results. |
| Decision | Reads each durable input that resident state has not seen. Schedules the next round or completes. |
| Commit | Appends one decision record. It must follow the decision prefix that it read. |
| Effect | One activity run. Its result becomes a durable input only if its schedule is durable. |
| Worker | Holds claims, resident state and a commit chain of up to 4 decisions. |
| Reclaimer | Moves the claim of a dead or stalled worker. The epoch goes up, and the new owner loads cold. |

The modes are:

- `Serial`: decide, commit, wait, then decide again. This is the engine
  today.
- `Gated`: decide again while a commit flushes. Release effects only after
  their own commit lands.
- `Eager`: as `Gated`, but release effects at the decision, before the
  commit. This is the libDSE design without its rollback of other
  components.

The fences are `Epoch` (prefix and claim) and `PrefixOnly`. Today the
suspended commit path appends from the event id read at the decision
start. `UNIQUE (workflow_exec_id, event_id)` rejects it when the history
moved. That path checks no claim epoch. `PrefixOnly` models it.

Each commit send draws three faults: a crash while the commit is in
flight, a stall just before the send, and a failed commit. A crash loses
or lands its in-flight commit with equal chance. A run has at most 3
crashes and stalls.

The workloads are `Chain` and `FanOut`. `Chain` is the bench workflow:
three activities in sequence. `FanOut` has two rounds of four parallel
activities, and two signals.

### Invariants

| Invariant | Meaning |
|---|---|
| `CommitByOwner` | An applied commit comes from the current claim holder. |
| `ReplayEquivalent` | Replay of the durable log gives each decision that the log holds. |
| `EffectAfterCommit` | An effect leaves the worker only after the commit that schedules it. |
| `EffectOnce` | An effect runs at most once. |
| `ExpectedOutput` | A completed execution returns the value that its workload defines. |
| `Converges` | Every execution completes within the event limit. |

## Measured latency

The host has 4 logical CPUs, an Intel Xeon at 2.1 GHz. It runs PostgreSQL
16.15 with `max_connections = 200`. The bench ran at 1 shard, release
profile, with `HARVEST_BENCH_COMMIT_PROBE=1`. Unloaded means 1 workflow
in flight. Loaded means the bench default of 32.

| Run | `fsync` | Workflows/s | Start to completion p50 / p99 | Persist p50 / p99 | Persists per run | Commit share of p50 |
|---|---|--:|--:|--:|--:|--:|
| Unloaded, 200 runs | on | 2.05 | 470.50 / 516.21 ms | 3.05 / 6.19 ms | 4.00 | 2.6 % |
| Loaded, 600 runs | on | 30.77 | 1012.80 / 1111.80 ms | 4.07 / 10.98 ms | 4.00 | 1.6 % |
| Unloaded, 200 runs | off | 2.07 | 464.50 / 518.00 ms | 2.57 / 5.72 ms | 4.00 | 2.2 % |
| Loaded, 600 runs | off | 33.37 | 917.96 / 1109.56 ms | 3.60 / 10.51 ms | 4.00 | 1.6 % |

The commit share is four persist p50s over the run p50. It is the most
that hiding every commit could save. The `fsync` setting moves the
persist p50 by only 0.5 ms on this host.

Other numbers from the same `fsync`-on session:

| Scenario | Value |
|---|--:|
| `dispatch_latency` p50 / p99 | 14.75 / 91.61 ms |
| `replay_throughput` | 10,032,854 events/s |
| Claim p50 / p99, unloaded | 2.71 / 6.19 ms |

### Calibration

`Timing::BENCH` holds the model durations:

```rust
decide_us: 2,
commit_us: (3_050, 6_190),
dispatch_us: 14_750,
activity_us: (50, 150),
wake_us: 135_800,
```

- The commit range is the persist p50 to p99.
- The dispatch hop is the `dispatch_latency` p50.
- The decide time follows from replay throughput. The bench history has
  fewer than 20 events, at 0.1 µs each.
- The bench activity is a no-op, so its body takes microseconds.
- The wake hop is the rest of the measured p50. It runs from a durable
  result to the start of the next decision.

`the_calibrated_model_matches_the_measured_bench` checks that the model
`Serial` mean is within 2 % of the measured 470.5 ms. It is 470.46 ms.

The wake hop is about 136 ms of each 155 ms hop. It holds the commit of
the result and the post-commit `NOTIFY` gate of up to 25 ms. It also
holds the claim settle delay of 50 to 75 ms and the claim. No speculation
over a commit can reach it.

## DST results

Each row is a sweep of 1,000 seeds, 4 executions each. Each seed runs
twice, and the two traces must be equal. The default faults are on
unless a row says otherwise.

### Safety

| Config | Result |
|---|---|
| `Serial` and `Gated`, `Chain` and `FanOut`, `Full` and `ReadsOnly` | Pass. Every invariant holds in all 8 configs. |
| `Gated`, `PrefixOnly` fence, all checks | Fail on seed 35 (`Chain`) and seed 62 (`FanOut`): `CommitByOwner`. |
| `Gated`, `PrefixOnly` fence, all other checks | Pass. 59 stale commits on `Chain` and 29 on `FanOut` do no harm. |
| `Eager`, `EffectAfterCommit` | Fail on seed 0. An effect leaves before its commit, by design. |
| `Eager`, `EffectOnce` | Fail on seed 0 (`Chain`) and seed 1 (`FanOut`). |
| `Eager`, `Converges` | Fail on seed 0 (`Chain`) and seed 3 (`FanOut`). |
| `Eager`, `ReplayEquivalent`, `ExpectedOutput`, `CommitByOwner` | Pass. |
| `Gated` with the `keep-on-failure` plant | Fail on seed 0 (`Chain`): `Converges`. Fail on seed 3 (`FanOut`): `ReplayEquivalent`. |

In the `Serial` `Chain` sweep the faults gave 764 crashes, 852 stalls,
845 failed commits and 2,471 reclaims. The claim fence rejected 389
commits. Crashes lost 463 in-flight commits and landed 444.

The `Eager` failures have two causes:

- A failed commit discards a decision whose effect already left. The
  repaired decision schedules the same effect, so it runs twice.
- A stall before the send lets the effect finish first. The store rejects
  its result, because no commit scheduled it. The commit then lands, and
  nothing runs the effect again. The run never completes.

Over 1,000 seeds, `Eager` stranded 244 of 4,000 `Chain` runs and 155 of
4,000 `FanOut` runs. It also ran 1,221 more effects than `Serial` on
`Chain`.

The plant drops the commit chain after a failure but keeps the state that
the chain built. On `Chain`, that state waits for a result that no commit
scheduled, so the run never completes. On `FanOut`, its next commit
disagrees with replay.

### Latency

Mean latency over 4,000 runs, in milliseconds.

| Timing | Workload | Faults | `Serial` | `Gated` | `Eager` |
|---|---|---|--:|--:|--:|
| Bench | `Chain` | none | 470.463 | 470.463 | 456.575 |
| Bench | `Chain` | default | 614.773 | 614.773 | 580.580 |
| Bench | `FanOut` | none | 286.925 | 286.925 | 278.325 |
| Bench | `FanOut` | default | 389.289 | 389.290 | 368.332 |
| Spread | `FanOut` | none | 340.511 | 340.506 | 330.092 |
| Spread | `FanOut` | default | 587.208 | 587.306 | 564.947 |

"Spread" is bench timing with activities from 1 ms to 100 ms. The `Eager`
rows with faults average only the runs that completed.

`Gated` runs almost no speculative decisions. Over 4,000 runs with no
faults, it made 1 on bench `FanOut` and 56 on spread `FanOut`. Three
facts explain this:

- On a chain, the next input needs an activity. The activity needs the
  commit. So no input is ready while a commit flushes.
- With bench timing, the four results of a round land within about
  100 µs. One 2 µs decision reads them all.
- A speculative decision overlaps only a commit flight. It saves at most
  one decide time, here 2 µs.

With faults, `Gated` is up to 0.1 ms slower on a mean. A failed commit
discards more decisions when the chain is longer.

### Cited tests

| Test | What it shows |
|---|---|
| `serial_chain_latency_has_a_closed_form` | The model latency is the sum of its hops. |
| `the_calibrated_model_matches_the_measured_bench` | The model matches the bench within 2 %. |
| `gated_speculation_has_nothing_to_run_on_a_chain` | `Gated` equals `Serial` on a chain. |
| `bench_timing_gives_fan_out_no_speculation` | Bench timing gives fan-out no speculation. |
| `gated_speculation_moves_fan_out_latency_by_under_one_percent` | The gain stays under 1 % and under one decide time per speculative decision. |
| `eager_release_hides_one_commit_per_hop_on_a_chain` | `Eager` saves exactly one commit per hop. |
| `eager_release_breaks_the_commit_gate` | `Eager` breaks `EffectAfterCommit`. |
| `eager_release_runs_an_effect_twice_after_a_failed_commit` | `Eager` breaks `EffectOnce`. |
| `eager_release_can_strand_an_execution` | `Eager` breaks `Converges`. |
| `a_prefix_only_fence_lets_a_stale_owner_commit` | `PrefixOnly` breaks `CommitByOwner`. |
| `a_prefix_only_fence_keeps_every_other_invariant` | `PrefixOnly` keeps every other invariant. |
| `serial_and_gated_keep_every_invariant_under_faults` | `Serial` and `Gated` pass under each fault. |
| `a_planted_repair_defect_is_found_and_replays_from_its_seed` | The harness finds a repair defect, and its seed replays it. |
| `reads_only_logging_drops_exactly_the_write_rows` | `ReadsOnly` drops the write rows and nothing else. |
| `golden_speculation_traces_are_equal_on_every_platform` | Fixed seeds give fixed traces on each platform. |

## Asymmetric logging

`ReadsOnly` stores only the inputs that each decision read. Replay derives
the scheduled effects. Every invariant holds, with the same latency.

| Workload | `Full` rows | `ReadsOnly` rows | Saved |
|---|--:|--:|--:|
| `Chain`, 1,000 seeds with faults | 40,000 | 28,000 | 30 % |
| `FanOut`, 1,000 seeds with faults | 81,108 | 49,108 | 39 % |

The saving does not carry over to Harvest for three reasons:

- An `ActivityScheduled` event carries the activity input. Replay checks
  the recorded commands against the new ones. That check finds
  non-determinism, and it needs the write rows.
- The task row that releases an effect is a write in the same commit. It
  stays.
- Persist is 1.6 % to 2.6 % of latency. Fewer rows in that commit cannot
  move the end-to-end number much.

## Go / no-go

**Verdict: no-go** for speculative decisions and for asymmetric logging.

`DESIGN-2011.md` fixed three go criteria before the measurement.

| Criterion | Result |
|---|---|
| 1. `Gated` cuts unloaded latency of the bench workflow by 20 % or more. | **Not met.** It cuts 0 %. The ceiling is 2.6 %. |
| 2. `Gated` keeps every invariant under faults. | **Met.** 1,000 seeds per config, 8 configs. |
| 3. The gain does not need `Eager`. | **Not met.** Only `Eager` gains, 2.95 %, and it breaks three invariants. |

**Why libDSE gains and Harvest does not.** libDSE moves the log write off
the path of every step and every message. Harvest already batches a
decision into one commit. The commit is a small part of a hop. The hop is
mostly the wake path after the commit.

**What is worth keeping.** The model and its fault sweep stay as DST
tests. They show that the prefix check of the suspended path keeps the
safety invariants. They can test a later idea, such as batching.

**Where to look instead.** The wake hop is about 87 % of the unloaded
latency of the bench workflow. The post-commit `NOTIFY` gate and the
claim settle delay are in that hop. A follow-up can measure each part.

## Known limits

- The model is not the engine. It has no delta load, no timers, no child
  workflows and no local activities.
- The wake hop comes from the closed form, not from a direct measurement.
- The decide time comes from replay throughput. A warm resume does less
  work than a cold replay, but a real decision also loads its delta.
- The bench ran on one host with one shard. The numbers are for this
  revision and host, not for every deployment.
- `Eager` has no result buffer and no rollback of other components.
  libDSE has both. A buffer would fix `Converges`, but not `EffectOnce`.
- Faults come only at a commit send. The model has no network partition
  and no clock skew.

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
