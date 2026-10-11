# Typed state snapshots — R&D spike report (issue #2013)

**Status: R&D spike. No engine code.** The prototype lives in
`autumn-harvest/tests/integration/typed_snapshot_spike_tests.rs`. The plan
and its brainstorms are in `DESIGN-2013.md`.

## 1. The question

A run resumes by replay. Replay runs the workflow code from the first event,
so all of that code must stay deterministic for the life of the run.

Issue #2013 asks a different model. A run resumes from a typed state
snapshot. The code before the snapshot point then does not run again, so it
does not need to stay deterministic. Is that worth building?

The issue names three gains:

- **Versioning.** New code can change the steps before the snapshot freely.
- **History caps.** A run does not need its full history to resume.
- **Agent recovery.** AstronOS gave a fresh model session a versioned state.
  That passed 14 of 15 runs, against 2 of 15 for a full-history replay. The
  benchmark is small.

The issue sets one hard constraint. Rust cannot serialize an arbitrary
future. A snapshot therefore needs an explicit state type, Golem-style save
and load functions, and a machine check that no completed side effect is
lost or repeated.

## 2. The gate: resident hit rate

The issue gates the spike on the resident-state hit rate of issue #2007.
That data did not exist, so this change builds it first.

**The counter.** `harvest.workflow.resident` counts one outcome for each
decision. `outcome=hit` means the decision resumed a resident workflow and
replayed nothing. `outcome=miss` means it replayed. A miss names its
`reason` from a closed set. `docs/telemetry.md` lists every reason.

**Measurement 1: e2e bench.** The `throughput` and `signal_roundtrip`
scenarios of `benches/e2e_bench.rs` ran at 1 shard. The setup was a local
PostgreSQL 16 server, a shared-server topology, a debug build and 4 vCPUs.
The hit rate depends on the workflow shape, not on speed.

| Scenario | Workflow shape | Decisions | Hits | Hit rate | Misses |
|---|---|--:|--:|--:|---|
| `throughput` | Three activities in sequence | 5,760 | 4,320 | 75.0% | `cold` = 1,440 |
| `signal_roundtrip` | One signal wait | 960 | 480 | 50.0% | `cold` = 480 |

Each miss is the first decision of a run. One worker served each shard, so
every later decision was warm. Each rate is the ceiling for its shape: n − 1
of n decisions.

**Measurement 2: agent loop.** `resident_hit_rate_db_tests.rs` runs the real
`agent_loop` on one Postgres worker. A scripted model asks for two allowed
tool calls, then a gated call, then answers.

| Decision | What it ran | Outcome |
|--:|---|---|
| 1 | Start, first model turn | `miss/cold` |
| 2–6 | Model turns and allowed tool calls | `hit/resumed` × 5 |
| 7 | After the approval signal | `miss/race` |
| 8–9 | Gated tool call, last model turn | `miss/race_teardown` × 2 |

The agent loop resumed 5 of 9 decisions, or 5 of 6 before the gate.

**A finding for #2008.** A won signal-with-deadline race pushes
`CancelRaceLosers` on each cold replay, because that command writes no event.
A suspension with that command cannot stay resident. So after one approval
gate, every later decision of the run replays. The test pins this behavior.
It is a narrow fix for #2008: a resumed future does not re-run the race, so
the teardown is safe to skip on a warm decision.

**What the data says.** On a warm worker the resident path already removes
replay from sequential workflows. Replay cost stays only on cold decisions:
the first decision, a restart, a failover, an eviction or another worker.
Snapshots therefore cannot buy much decision speed on a warm worker. Their
case must rest on versioning, caps and cold recovery.

## 3. Design sketch

Two designs fit the constraint. Both checkpoint only at a quiescent point:
no activity, timer, child or update is open.

### 3.1 In-place snapshot (sketched, not built)

1. The author declares a state type with a version, a save and a load.
   For example, `#[workflow(state = Order)]`.
2. At `ctx.checkpoint(&state)` the worker appends a new `StateSnapshot`
   event. It holds the state type, the version, the state, the effect
   ledger and the context counters.
3. A cold decision loads history from the last `StateSnapshot`. It seeds the
   counters, loads the state, and calls the resume entry with it.
4. A checkpoint with an open effect fails the decision. Nothing is lost or
   repeated, because nothing is in flight.

The cost is high:

- **A new event variant.** Build N−1 must read it, so it ships off by
  default first (`docs/upgrading/README.md`).
- **A new resume path** in the worker, the executor and the replay matcher.
- **Saved context counters.** A new context restarts every reserved name,
  such as `__signal_timeout:{seq}:{name}`, `race:{seq}` and
  `saga_compensated:{seq}`. A resume that does not restore the counters
  reuses old names and matches old timers.
- **Every history tool needs a snapshot mode.** These are the replay
  debugger, the drift gate, reset, the upgrade verdict, PII erasure, codec
  rotation and the TLA+ trace check. Each one reads from event 0 today.
- **Caps gain nothing until the prefix goes.** A history cap needs the old
  prefix deleted. That is a new delete path next to retention.

### 3.2 Typed checkpoint over continue-as-new (prototyped)

1. The workflow calls `continue_as_new` with `{version, state}` at a
   quiescent point.
2. The engine side stamps an effect ledger from the history when it persists
   the checkpoint. The author never writes the ledger.
3. The successor run loads the state. An older version passes through the
   `upgrade` step of the state type. An unknown newer version is refused.
4. The successor has a new, empty history. So the code before the checkpoint
   never runs again, and it does not need to stay deterministic.

Continue-as-new already carries unconsumed signals, memo, search attributes,
headers and the assigned build. The keyed entity (#1975) already uses it to
checkpoint typed state. The gap is small: a version, an upgrade step and the
ledger check.

Its limits:

- The successor has a new execution id. The workflow id stays.
- Continue-as-new works only in a root workflow. A child cannot checkpoint.
- The state must fit the workflow input cap, or use payload offload.

## 4. What the prototype shows

Each claim names its test in `typed_snapshot_spike_tests.rs`.

| Claim | Test |
|---|---|
| A quiescent history gives a ledger of its completed effects. | `a_quiescent_history_gives_a_ledger_of_its_completed_effects` |
| A checkpoint with an open activity, timer, child or update is refused. | `a_checkpoint_with_an_open_effect_is_refused` |
| A v1 snapshot loads under v2 code through its upgrade step. | `a_v1_snapshot_loads_under_v2_code` |
| A snapshot from newer code is refused. | `a_snapshot_from_newer_code_is_refused` |
| A ledger that differs from its source history is refused. | `a_ledger_that_differs_from_its_source_history_is_refused` |
| Changed code before the checkpoint fails a full replay, but resumes from the snapshot. The resume runs no completed charge again. | `changed_code_before_the_checkpoint_resumes_from_the_snapshot` |
| A new context restarts reserved names, so an in-place resume must restore the counters. | `reserved_names_restart_in_each_new_context` |

The machine check is the ledger. It refuses a checkpoint with an open
effect. It also lets the loader compare the stored ledger with the source
history. A forged or stale snapshot then fails to load.

What the prototype does not cover:

- Local activities, external awaits, external signals and cancels.
- A held durable mutex. History has no release event, so the ledger cannot
  see it. The context can.
- The value of a completed effect. The ledger proves that an effect
  completed. It does not prove that the state reflects its result. That stays
  the author's job, as in Golem.

## 5. Verdict

**Verdict: no-go on an in-place snapshot now; go on typed checkpoints over continue-as-new.**

Why no-go on the in-place design:

- The gate data shows that the resident path already removes replay from
  warm sequential decisions. Snapshots would speed up only cold decisions.
- Continue-as-new already gives the main gain. The prototype shows that code
  before a checkpoint can change with no patch marker.
- The in-place design costs a new event variant, a new resume path, saved
  counters and a snapshot mode in each history tool. Its only extra gains
  are a stable execution id and checkpoints in child workflows.

Why go on the narrow design:

- It needs no new event variant and no migration.
- It closes the real gaps: a schema version, an upgrade step and a machine
  check of completed effects.
- It feeds the upgrade verdict (#1995). A stored snapshot that does not load
  under the candidate build is a rehydration failure.

Agent recovery is an app concern. The agent loop already carries its
transcript as explicit state, and the memory snapshot activity covers the
rest. A typed checkpoint gives a fresh model session the versioned state
that AstronOS used.

**Follow-up work, in order:**

1. #2008: let a warm decision skip `CancelRaceLosers` of a race that ended.
   The agent loop then resumes past an approval gate.
2. A versioned state and an upgrade step for the keyed entity checkpoint.
3. The effect ledger check at the continue-as-new persist step.
4. The snapshot load check in the upgrade verdict.

**What would change the verdict:**

- Cold decisions dominate a production fleet, for example after frequent
  failovers. `harvest.workflow.resident{reason="cold"}` shows this.
- Child workflows need checkpoints. Continue-as-new cannot serve them.
- A stable execution id across checkpoints becomes a hard requirement.

## See also

- `docs/sticky-routing.md`: the resident path and its counter.
- `docs/why-deterministic-replay.md`: replay against checkpoint-only models.
- `docs/adr/0006-keyed-entity.md`: typed state over continue-as-new.
- `docs/upgrade-check.md`: the pre-deploy upgrade verdict.
