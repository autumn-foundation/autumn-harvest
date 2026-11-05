# DESIGN-2380: the `agent_refine` workflow

Issue #2380. Parent: #2369. Needs #2375 and #2379. Uses #2376, #2377 and
#2378. Policy: #2394. Budget model: `harness2/core/ledger.py`.

## Problem

Harness² runs Propose, Probe and Compose in a Python thread pool. It
resumes from marker files. The engine needs the same loop as a durable
workflow. A crash must cost one step, not the sweep. The run must fix its
cost before it starts.

## Decision

`autumn-harvest-agent/src/refine/run.rs` holds the workflow
`agent_refine`. It needs the `eval` feature, because each probe is an
evaluation.

### The input

```rust
pub struct RefineInput {
    pub kind: String,
    pub source_runs: Vec<ExecutionId>, // 1 to 16, no repeat
    pub k: u32,                        // 1 to 8
    pub mode: RefineMode,              // Parallel or Sequential
    pub seed: u64,
    pub budget: Option<MeshBudget>,
}
```

### The steps

1. `agent_refine_policy` reads the policy of the kind once (#2394).
2. `agent_refine_start` reads each source run through the installed
   `RefineSources`. It checks each run in this order:
   1. The run exists.
   2. It is an `agent_loop` or `agent` run.
   3. It ended.
   4. It is of `kind`. An `agent_loop` run names its kind in
      `AgentTask::kind`. A run with no kind fails this check.
   5. The replay probe can read it.
   6. The active overlay applies to the profile of the recorded task,
      under the policy ceilings. Otherwise each probe of the source would
      fail. The error is `RefineSourceOverCeilings`.

   A failed check fails the run with a named `RefineError`. No model call
   runs before this step ends. The step also reads the base profile, the
   active overlay and, for each source, its profile and evidence text.
   Each stage of a source checks its replies against the source profile,
   as its probe does. A reply must also apply to the profile of each other
   source, so the run can carry it to any later source. It must apply to
   the registered profile of the kind too, so the kind can take the chosen
   overlay.
3. The loop runs each source in input order. The overlay that a source
   chooses is the current overlay of the next source.
4. The run checks the ledger before each step that can spend. A spend at
   a cap stops the run as `RefineStatus::BudgetExhausted`, with no
   `chosen` overlay. After the whole plan, only a spend above a cap gives
   that status.
5. At the end, `RefineLedger::check` compares the counts with the
   identity. A mismatch fails the run with `RefineError::BudgetIdentity`.

**Parallel mode.** For each source:

1. k INV and k DOM stages, each from the current overlay. Each stage gets
   its sample number, so a proposer can vary its answer.
2. One probe for each candidate stage.
3. k compositions over the shared evidence. Each one reads the anonymised
   pool of the 2k probes (#2376). With a workspace store, it also reads an
   evidence bundle (#2377).
4. One probe for each candidate composition.
5. `agent_refine_select` picks one composition by its opaque id. The
   selector reads the overlays and the pool under those ids. With no
   candidate, the source keeps the current overlay.

The stages of one round are independent. The workflow still runs them
one after another, so the history order is fixed and the budget check
runs between any two steps.

**Sequential mode.** For each source, k steps. Each step runs INV, DOM,
their probes, one composition over those two probes, and its probe. A
candidate composition becomes the current overlay of the next step.

### The probe

`agent_refine_probe` runs `eval::evaluate` of the overlay against the
recorded source. The base rollout is the recorded source, so it costs no
call. When `stubbed_tool_calls > 0`, the probe asks the host for a live
re-probe (#2378) with `RefinePolicy::probe_effects`. The Postgres host
forks the source and waits for the fork. The SQLite host has no fork. The
probe then records `LiveReprobe::Skipped` with the reason, and the stage
notes say so.

### The budget identity

For each source run, both modes run exactly 3k sessions and plan 3k
probes. A session is a proposer or composer stage. A `KeptBase` or
`FellBack` stage counts as a session and adds no probe. So:

```text
sessions = 3k × sources
probes   = sessions − (kept_base + fell_back)
```

`RefineLedger` counts sessions, probes, live re-probes, selections, kept
and fell-back stages, tokens and cost. The selection and the live re-probe
are not in the identity. `formal/verus/src/agent_refine.rs` proves that
each plan meets it for each k and each run of stage outcomes.

### The spend

- Each stage meters its proposer or composer calls through
  `ProposeContext::charge`. `ModelProposer` charges each model call.
- Each probe sums the usage and cost of its candidate turns. A live
  re-probe adds the spend from the report of a completed fork.
- A failed stage reports the spend of its last attempt in its failure
  details, and the run counts it.
- Each activity writes its calls to the cost ledger. The rows key on the
  execution of the activity, which is the refine run.
- A run with a `budget` caps its own recorded spend with
  `MeshBudget::is_spent`. The run sets the search attribute `agent_mesh`
  to the budget mesh, so the mesh spend of `LedgerSpend` holds the refine
  rows too.

### The report

`RefineReport` holds the status, the chosen overlay, one record per stage
and the ledger. It holds no execution id of the refine run and no time. So
one seed and one script give byte-identical reports.

### Crash safety

Each stage, each probe and each selection is one recorded activity. A crash
re-runs the step in flight. Replay reads every completed step, so the run
asks no proposer again for it.

A stage that fails falls back only when the engine reports a failed or
timed-out activity. A cancel or an input over the cap fails the run. The
fallback text holds the error type and the message, with no attempt
number, so a replay gives the same text.

## Brainstorm

| Question | Options | Choice |
|---|---|---|
| A probe | `eval::compare` of base and candidate. `eval::evaluate` of the candidate. | `evaluate`. The base is the recorded source, so a compare would spend a second run on a known answer. |
| How a composer reads the pool | Re-run `compare_anonymised`. Anonymise the recorded probes. | Anonymise the recorded probes. A re-run would spend each probe twice. |
| The composer | A new `Dimension`. A new trait. | A new trait `OverlayComposer`. `Dimension` is exhaustive, and a composer reads a pool. |
| The selector | A model only. A trait with a default rule. | A trait with a default rule. The default picks by the probe results and costs no call. |
| Many sources | One loop over all. One loop per source. | One loop per source. The choice of a source is the current overlay of the next. |
| The budget | The mesh spend of the period. The spend of the run. | The spend of the run. It is in history, so a replay and a second run agree. |
| Proposer spend | Not metered. Metered by the context. | Metered. `ModelProposer` used to call its model with no ledger row. |
| Kind of an `agent_loop` run | A search attribute. A task field. | A task field `AgentTask::kind`. History records it. |
| Concurrency | One stage at a time. An activity fan-out. | One stage at a time. The budget check then runs between any two steps, and a crash re-runs one step. A fan-out can come later. |

## Reverse brainstorm

How can the run cost more, or report a false result? Each failure has a
guard.

| Failure | Guard |
|---|---|
| A bad source spends model calls first. | `agent_refine_start` checks every source before any stage. |
| A source of another kind changes the overlay of this kind. | The start step compares the recorded kind. |
| A crash re-asks a completed stage. | One activity per stage. Replay reads the result. |
| The run counts a lost stage twice. | The run rebuilds the ledger from recorded results only. |
| A count drifts from the plan. | `RefineLedger::check` at the end. The Verus proof of the plan. |
| A run keeps going past its budget. | The run checks the ledger before each step. |
| A spent run still names a winner. | `BudgetExhausted` has no `chosen`. |
| The composer learns the order of a candidate. | The pool lists opaque ids in a seeded order. No reader context holds the seed. |
| A failure text names a model. | The pool redacts each digest, each model route and its provider id. |
| A carried overlay does not fit a later source, such as a read-only one. | Each stage reply must apply to the profile of every source of the run. |
| The selector names an id outside the pool. | The step ignores the id, takes the rule pick and records a note. |
| A failed attempt hides its spend. | The failure details hold the spend of the last attempt. |
| A live fork runs on with no end. | The probe cancels a fork that does not end in 60 minutes. |
| A live re-probe on SQLite fails the run. | The SQLite host returns `Unsupported`, and the probe records a skip. |
| A live re-probe writes data. | It runs only `probe_effects`, default `[read_only]` (#2378). |
| Two runs differ in their report. | The report holds no run id and no time. A test compares bytes. |
| `k` or the source list is huge. | `k` is 1 to 8. The list holds 1 to 16 runs. |
| A proposer failure on every retry fails the sweep. | The stage falls back to the current overlay. A configuration error fails the run. |

## Six hats

- **White.** `agent_refine_propose` already runs one stage as one
  activity. `eval::evaluate` reports `stubbed_tool_calls`. The anonymised
  pool of #2376 and the bundle of #2377 exist. SQLite has no fork.
- **Red.** A loop that spends without a bound is the worst outcome. The
  plan must be fixed, and the counts must prove it.
- **Black.** Evidence text rides in each stage input, so history grows with
  k. An `agent` run has no replay probe yet, so the start step refuses it
  with `SourceNotReplayable`. A live re-probe records its rows under the
  fork, not the refine run.
- **Yellow.** An operator gets a fixed cost, a crash-safe sweep and a full
  record of each stage.
- **Green.** A later change can give an `agent` run a replay probe, and
  can let a proposer read a bundle.
- **Blue.** Spec and proof of the identity first. Then red tests with a
  scripted proposer, composer and model. Then the workflow. Then the
  gates.

## Limits

- The start step refuses an `agent` run of the kind with
  `SourceNotReplayable`. `eval` replays `agent_loop` runs only.
- A live re-probe records its cost ledger rows under the fork execution.
  The refine ledger counts the spend of a completed fork from its report.
  A fork that fails, or that the probe cancels, adds none.
- A worker crash during the wait of a live re-probe starts a second fork
  on the retry.
- The engine keeps no ledger row of a failed attempt. An attempt that
  fails before a later attempt succeeds is in neither ledger.
- A probe writes one cost ledger row under the unknown model id. An
  `Evaluation` holds the sum of its turns, not the model of each turn.
- On Postgres, a live re-probe names its fork in the report. Two runs then
  differ in that id.
- An older worker cannot decode a `ProposeRequest` with a `sample` above
  1. Start a refine run only after every worker runs this release.
- The budget check runs between steps. The last step can pass the cap.
  The run then ends as `BudgetExhausted`.
- Each compose and select input holds the probes of its round. A long
  source with a large k can pass the activity input cap.
- `ModelProposer` has no composer or selector form yet. An app brings its
  own `OverlayComposer`. With no `OverlaySelector`, a rule picks.

## Tests

`autumn-harvest-agent/tests/refine_run.rs` covers each criterion on
SQLite. `refine/ledger.rs` unit tests and proptests cover the kernel.
