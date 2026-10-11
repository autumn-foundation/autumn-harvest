# Typed state snapshots — R&D spike report (issue #2013)

**Status: R&D spike. No engine code.** The prototype lives in
`autumn-harvest/tests/integration/typed_snapshot_spike_tests.rs`. The plan
and its brainstorms are in `DESIGN-2013.md`.

## 1. The question

A run resumes by replay. Replay runs the workflow code from the first event.
So all of that code must stay deterministic for the life of the run.

Issue #2013 asks about a different model. A run resumes from a typed state
snapshot. The code before the snapshot point then does not run again. So it
does not need to stay deterministic. Is that worth building?

The issue names three gains:

- **Versioning.** New code can change the steps before the snapshot freely.
- **History caps.** A run does not need its full history to resume.
- **Agent recovery.** AstronOS gave a fresh model session a versioned state.
  That passed 14 of 15 runs, against 2 of 15 for a full-history replay. The
  benchmark is small.

The issue sets one hard constraint. Rust cannot serialize an arbitrary
future. A snapshot therefore needs an explicit state type and Golem-style
save and load functions. A machine check must also show that the snapshot
loses or repeats no completed side effect.

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
One worker served the shard, and the cache evicted nothing. In that setup
the hit rate depends on the workflow shape, not on speed.

| Scenario | Workflow shape | Decisions | Hits | Hit rate | Misses |
|---|---|--:|--:|--:|---|
| `throughput` | Three activities in sequence | 5,760 | 4,320 | 75.0% | `cold` = 1,440 |
| `signal_roundtrip` | One signal wait | 960 | 480 | 50.0% | `cold` = 480 |

Each miss is the first decision of a run. Each rate is the ceiling for its
shape: n − 1 of n decisions.

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
`CancelRaceLosers` on each cold replay. That command writes no event. A
suspension with that command cannot stay resident. So after one approval
gate, every later decision of the run replays. The test pins this behavior.

A fix can let the resident path keep such a suspension. That is a
hypothesis, not a proven change. A cold replay re-issues the teardown today.
That heals a teardown that a panic or a hard-cap seal dropped. A fix must
show three things:

- The decision that resolved the race persisted its teardown.
- A warm decision that skips the teardown decides as a cold replay does.
- The DST world (#2002) finds no divergence with the change.

**What the data says.**

- On a warm worker, the resident path already removes replay from
  sequential decisions. Replay cost stays on cold decisions: the first
  decision, a restart, a failover, an eviction or another worker.
- A cold decision is cheap at today's caps. `decision_cost` in
  `benches/replay_bench.rs` measures 1.25 ms for a 10,000-event history.
  The default hard cap is 50,000 events, so a cold replay stays near 6 ms.
- These rates are ceilings. The bench and the test run one worker with no
  eviction. A fleet with restarts and many workers has more `cold` misses.
  No fleet data exists yet.

Snapshots therefore cannot buy much decision speed. Their case must rest on
versioning and agent recovery.

## 3. Design sketch

Two designs fit the constraint. Both checkpoint only at a quiescent point.
At that point no activity, timer, child or update is open, and the body has
taken each signal in history.

### 3.1 In-place snapshot (sketched, not built)

1. The author declares a state type with a version, a save and a load.
   For example, `#[workflow(state = Order)]`.
2. At `ctx.checkpoint(&state)` the worker appends a new `StateSnapshot`
   event. It holds the state type, the version, the state, the effect
   ledger and the context counters.
3. A cold decision loads history from the last `StateSnapshot`. It seeds the
   counters, loads the state, and calls the resume entry with it.
4. `ctx.checkpoint` returns an error when an effect is open. A failed
   decision would retry forever, because the open effect does not change.

The cost is high:

- **A new event variant.** Build N−1 must read it, so it ships off by
  default first (`docs/upgrading/README.md`).
- **A new resume path** in the worker, the executor and the replay matcher.
- **Saved context counters.** A new context restarts every reserved name,
  such as `__signal_timeout:{seq}:{name}`, `race:{seq}` and
  `saga_compensated:{seq}`. A resume that does not restore the counters
  reuses old names and matches old timers.
- **Each replay tool needs a snapshot mode.** The replay debugger, the drift
  gate, reset, the upgrade verdict and the TLA+ trace check read a history
  from event 0 today.
- **Two more payload writers.** PII erasure and codec rotation sweep payload
  fields. Each one must cover the new state field.
- **Caps gain nothing until the prefix goes.** A history cap needs the old
  prefix deleted. That is a new delete path next to retention.

### 3.2 Typed checkpoint over continue-as-new (prototyped)

1. The workflow calls `continue_as_new` with `{version, state}` at a
   quiescent point.
2. The worker stamps an effect ledger from the history when it persists the
   checkpoint. The ledger goes into the successor input, so no event
   changes. The author never writes the ledger.
3. The stamp refuses an open effect. It also refuses a signal in history
   that the body has not taken. Continue-as-new moves only signals that the
   run did not ingest, so it would drop that signal. The worker reads this
   from the replay matcher, because history alone cannot show it.
4. The successor loads the state. An older version passes through the
   `upgrade` step of the state type. The loader refuses a newer version.
   It then asks the state type whether the state covers the ledger.
5. The successor has a new, empty history. So the code before the
   checkpoint never runs again, and it does not need to stay deterministic.

Continue-as-new already carries the workflow id, unconsumed signals, memo,
search attributes and headers. The keyed entity (#1975) already uses it to
checkpoint typed state.

**One gap blocks the versioning gain.** Continue-as-new also carries
`assigned_build_id` (#171). Under build routing, the successor therefore
stays on the old build. New code runs only when the checkpoint re-resolves
the build. Temporal ships this as upgrade-on-continue-as-new, in public
preview. See `research_notes/Durable workflow engine R&D
opportunities/correctness_dx_verification.md`. Without build
routing, a deploy replaces the code, and the successor runs the new code.

Its other limits:

- The successor has a new execution id. The workflow id stays.
- Continue-as-new works only in a root workflow. A child cannot checkpoint.
- The state and the ledger must fit the workflow input cap, or use payload
  offload. The ledger grows with each effect, so it needs counts or a digest
  at scale.

## 4. What the prototype shows

Each claim names its test in `typed_snapshot_spike_tests.rs`.

| Claim | Test |
|---|---|
| A quiescent history gives a ledger of its completed effects. | `a_quiescent_history_gives_a_ledger_of_its_completed_effects` |
| The stamp refuses an open activity, timer, child or update. | `a_checkpoint_with_an_open_effect_is_refused` |
| The stamp refuses a signal that the body has not taken. | `a_checkpoint_with_an_unread_signal_is_refused` |
| A v1 snapshot loads under v2 code through its upgrade step. | `a_v1_snapshot_loads_under_v2_code` |
| The loader refuses a snapshot from newer code. | `a_snapshot_from_newer_code_is_refused` |
| The loader refuses a ledger that differs from its source history. | `a_ledger_that_differs_from_its_source_history_is_refused` |
| The loader refuses a state that does not cover its ledger. | `a_state_that_does_not_cover_its_ledger_is_refused` |
| Changed code before the checkpoint fails a full replay as non-deterministic. The same code resumes from the snapshot and runs no completed charge again. | `changed_code_before_the_checkpoint_resumes_from_the_snapshot` |
| A new context restarts reserved names. An in-place resume must restore the counters. | `reserved_names_restart_in_each_new_context` |

The machine check has three parts: quiescence, a ledger that matches its
source history, and a state that covers its ledger. The state type writes
the cover rule. The loader runs it on each load, so a wrong upgrade step
fails to load.

What the prototype does not prove or cover:

- The value of a completed effect. The ledger proves that an effect
  completed. The cover rule checks only what the author writes into it.
- Which effect completed. The ledger keys activities by name, so two
  charges look the same.
- Local activities, detached children, external awaits, external signals,
  cancels and a held mutex. History has no release event for a mutex, so
  the context must report a hold.
- The real persist path. The tests stamp the ledger outside the worker.
- A source history that is gone. Retention or PII erasure can remove the
  predecessor before a loader re-checks the ledger. The successor must then
  trust the stored ledger.

## 5. Verdict

**Verdict: no-go on an in-place snapshot now; go on typed checkpoints over continue-as-new.**

The verdict is provisional. It rests on bench and test ceilings, not on
fleet data.

Why no-go on the in-place design:

- The resident path already removes replay from warm sequential decisions.
  A cold replay costs about 6 ms at the default cap. Snapshots would buy
  little speed.
- Continue-as-new already gives the main gain. The prototype shows that code
  before a checkpoint can change with no patch marker.
- The in-place design costs a new event variant, a new resume path, saved
  counters and a snapshot mode in each replay tool. Its only extra gains
  are a stable execution id and checkpoints in child workflows.

Why go on the narrow design:

- It needs no new event variant. The ledger rides in the successor input.
- It closes the real gaps: a schema version, an upgrade step, a cover rule
  and a machine check of completed effects.
- It gives the AstronOS shape: a fresh model session gets a versioned,
  authoritative state.

Agent recovery is mostly an app concern. The agent loop already carries its
transcript as explicit state, and the memory snapshot activity covers the
rest.

**Follow-up work, in order:**

1. #2008: let the resident path keep a suspension with the teardown of a
   finished race, with the proof in section 2.
2. Re-resolve the build at a checkpoint, as an opt-in. Without it, the
   versioning gain does not reach a pinned run.
3. A versioned state, an upgrade step and a cover rule for the keyed entity
   checkpoint. Its state type gets a schema through #1994.
4. The quiescence and unread-signal check at the continue-as-new persist
   step, with the ledger stamped into the successor input.
5. The upgrade verdict (#1995) already decodes stored payloads against the
   candidate schemas. Add the upgrade step and the cover rule to that check.

**What would change the verdict:**

- `harvest.workflow.resident{reason="cold"}` is more than 20% of decisions
  on a production fleet for a week.
- The p99 replay time of a cold decision is above 50 ms.
- Agent runs keep replaying after a gate, because the #2008 fix does not
  land.
- Child workflows need checkpoints, or a stable execution id across
  checkpoints becomes a hard requirement.

## See also

- `docs/sticky-routing.md`: the resident path and its counter.
- `docs/why-deterministic-replay.md`: replay against checkpoint-only models.
- `docs/adr/0006-keyed-entity.md`: typed state over continue-as-new.
- `docs/upgrade-check.md`: the pre-deploy upgrade verdict.
