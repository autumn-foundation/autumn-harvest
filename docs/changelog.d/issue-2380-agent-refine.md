## `agent_refine` workflow: parallel and sequential modes, a fixed budget, crash-safe (issue #2380)

The refine loop of an agent kind now runs as one durable workflow. Parent:
#2369. It uses the proposer of #2379, the overlay probe of #2375, the
anonymised pool of #2376, the evidence bundle of #2377, the live probe of
#2378 and the policy of #2394. The design is in
`docs/design/DESIGN-2380.md`.

- **Workflow.** `agent_refine` takes a `RefineInput`: `kind`,
  `source_runs` (1 to 16), `k` (1 to 8), `mode` (`parallel` or
  `sequential`), `seed` and an optional `MeshBudget`. It needs the `eval`
  feature. `sqlite::start_refine` starts it. `workflows()` and
  `activities()` hold it and its steps.
- **Start.** The run reads its policy once. `agent_refine_start` then
  checks every source before any model call. A bad source fails the run
  with `RefineSourceNotFound`, `RefineSourceNotAgentRun`,
  `RefineSourceInFlight`, `RefineSourceOtherKind`,
  `RefineSourceNotReplayable` or `RefineSourceOverCeilings`. An
  `agent_loop` run names its kind with `AgentTask::kind`. A durable
  `agent` run has no replay probe yet, so it gets
  `RefineSourceNotReplayable`. Each stage of a source checks its replies
  against the profile of that source, as its probe does. A reply must
  also apply to each other source and to the registered profile of the
  kind. So a carried overlay fits each later source, and the kind can take
  the chosen overlay.
- **Steps.** `agent_refine_probe` runs `eval::evaluate` and, when it stubs
  a tool call, a live re-probe through `RefineSources::live_probe`.
  `agent_refine_compose` runs an `OverlayComposer` over the probes under
  opaque ids. The pool redacts each model route that a probe asked for,
  with its model id, so an inherited or a peer route is redacted too.
  `agent_refine_select` runs an `OverlaySelector`. A rule
  picks when no selector is installed, or when the selector fails, times
  out or names an id outside the pool.
- **Budget identity.** Each source runs 3k sessions. Each candidate stage
  adds one probe. `RefineLedger::check` fails a run that breaks it with
  `RefineBudgetIdentity`. `formal/verus/src/agent_refine.rs` proves the
  kernel `identity_holds` and that each plan meets the identity.
- **Spend.** `ProposeContext::charge` and the `charge` method of each new
  context meter a model call. `ModelProposer` charges each call, so its
  calls now reach the cost ledger. Each step writes its calls under the
  refine run. A failed stage keeps the spend of its last attempt. A spent
  budget ends the run as `RefineStatus::BudgetExhausted`, with no chosen
  overlay.
- **Failures.** A failed stage falls back only on a failed or timed-out
  activity. A failed select step takes the rule pick. A cancel or an
  input over the cap fails the run with `RefineStageFailed`.
- **Additions.** `AgentTask::kind`, `RefineStatus::Chosen` and
  `RefineStatus::BudgetExhausted`, `Evaluation::candidate_usage` and
  `Evaluation::candidate_cost_usd_micros`, `ProposeRequest::sample`,
  `Stage::usage` and `Stage::cost_usd_micros`, and `ProbeResult::routes`
  and `ProbedCandidate::routes`. Each new field has a serde
  default, so a recorded value decodes as before. An older worker cannot
  decode a `ProposeRequest` with a `sample` above 1, so start a refine
  run only after every worker runs this release.
- **Backends.** `sqlite::Sources` and `postgres::Sources` read the source
  runs. Only Postgres runs a live re-probe, with the new
  `eval::live_probe_with` under the policy ceilings of the run, as the
  stages and the replay probe use. It waits through approval
  waits, and it cancels a fork that does not end in 60 minutes or whose
  wait fails. SQLite records `live re-probe skipped: SQLite has no fork`
  in the stage notes.
- **Tests.** `autumn-harvest-agent/tests/refine_run.rs` covers each
  criterion on SQLite, with a scripted proposer, composer and model. One
  test aborts a child process after the third probe and resumes the run
  to the same report as an uninterrupted run.
