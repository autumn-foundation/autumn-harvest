# Design: issue #2250, model the owner lease before the code

Epic #2247 adds a fenced decision owner per partition. Stage 1a (#2251)
adds the code. This issue writes the spec first: a TLA+ model and a
seeded simulator, each with the four nemeses of the issue.

**No engine change. No migration. No new `WorkflowEvent` variant.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- `formal/tla/ActivityClaim.tla` models the claim epoch. It uses a ghost
  `seq`, a `Fenced` switch and a `...Reach.cfg` witness.
- `scripts/check-formal-models.sh` runs each row of `formal/tla/models.txt`
  in the `formal-models` CI job. `formal_models_coverage.rs` guards it.
- `autumn_harvest::dst` holds three seeded harnesses. The `dst` test target
  runs in the `Test (<os>)` job with `--no-default-features`.
- Stage 1a code does not exist. No owner table, no ADR for #2249.
- The research report (#2242) gives the protocol: epoch +1 on each
  acquisition, renewal never bumps, the commit takes the owner row
  `FOR SHARE`, a takeover takes it exclusively, acks follow a durable write.

### 0.2 Brainstorm: what the model can cover

- One partition row with holder, epoch and a live flag for `lease_until`.
- A decision in two commit steps: the fence read, then the append. A lock
  can then matter between them.
- A resident cache per owner, so a re-acquisition can reuse stale state.
- An in-memory inbox mutant for Restate #4566.
- A ghost `tenure` per acquisition, so a renewal that bumps the epoch is
  visible as a self-fence.
- One witness invariant that one behaviour violates only when it shows all
  four nemeses.
- A store trait in the simulator, so stage 1a plugs its Postgres store in.
- A guard test that fails when the owner table lands without a simulator
  run against it.

### 0.3 Reverse brainstorm: ways to make the model useless

| Way to fail | Counter-measure |
|---|---|
| The model passes because it never reaches the race. | `OwnerLeaseReach.cfg` must violate `SomeNemesisUnseen`. |
| The invariants restate the guard. | Ghost fields: `pinned`, `at`, `base`, `tenure`. Invariants compare them, not the guard. |
| A backstop hides a missing fence. | The model has no event-id backstop. The fence alone must hold. |
| A mutant fails for an unrelated reason. | Each mutant config lists only the invariant it must break. |
| A dead owner cannot act, so no stale write. | An unloaded worker keeps its decision in flight, as a paused thread does. |
| The simulator drifts from the spec. | Same defects and invariants. Tests map each TLA+ defect and the invariant list to the simulator. |
| The simulator never runs against real code. | `OwnerStore` trait, `sweep_with`, and a guard test that trips when `harvest_decision_owner` appears. |
| A worker has one decision slot, so a pipeline bug is out of reach. | Decisions in flight are a set. A `pipeline` mutant shows the rule. |
| The docs rot. | The coverage guards name every spec, defect and witness. |

### 0.4 Six hats

- **White (facts).** The issue asks for 3 invariants, 4 nemeses, 1 mutant
  without the commit fence, a simulator in CI and an ADR update.
- **Red (gut).** The renewal nemesis feels weak. A renewal cannot break
  safety when the commit holds the fence. The real risk is that a renewal
  fences the owner itself, or waits on the commit lock.
- **Black (risks).** State explosion in TLC. A long CI job. Overlap with
  #2249, which owns the full ADR. A trait that does not fit Postgres.
- **Yellow (value).** Each 2026 bug of the survey gets a mutant that TLC
  rejects. Stage 1a gets a spec and a ready harness.
- **Green (ideas).** Model the lock mode. A fence with `FOR SHARE` blocks a
  renewal `UPDATE`. `FOR KEY SHARE` plus a non-key renewal `UPDATE` does
  not, and a takeover `FOR UPDATE` still waits.
- **Blue (process).** Spec, then red mutants, then green fixed config, then
  the simulator tests (red), the oracle (green), then refactor and docs.

### 0.5 Decisions

1. New spec `formal/tla/OwnerLease.tla` with a `Defect` constant. Seven
   mutant configs, one design config, one reach config.
2. New harness `autumn_harvest::dst::owner` with an `OwnerStore` trait and
   an in-memory oracle. Same defects and invariants.
3. New ADR `docs/adr/0010-fenced-owner-lease.md`, status *Proposed*. It
   holds only the rules the model forces. #2249 adds the partition rules.
4. A guard test makes AC 2 bind: when a migration adds the owner table, the
   repo must hold a simulator run against the real store.

## 1. The model-forced rules

The rules R1 to R7 and their mutants are in the table of
[ADR 0010](../adr/0010-fenced-owner-lease.md#decision).

## 2. Review findings

Four review agents checked the model, the simulator, the docs and the CI
wiring. The fixes:

- The guard test of AC 2 matched its own file. It now skips `tests/dst`,
  needs a store other than the oracle, and has a self-test.
- The model now has a set of decisions in flight, a `pipeline` mutant (R7),
  a graceful `Release`, and an `owners` ghost for `OneOwnerPerEpoch`.
- The store reports only durable facts. A renewal can be `Busy`.
- Each nemesis has a fired counter. A test checks its fault against a run
  with no nemesis.
- The ADR names the takeover `SELECT ... FOR UPDATE`, the load order, the
  liveness timeouts, and what the model assumes.

## 3. Acceptance criteria and evidence

The PR description lists each criterion of #2250 with its evidence.
