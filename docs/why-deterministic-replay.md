# Why Harvest keeps deterministic replay

Some durable-execution engines sell "no deterministic replay" as a feature. If
you compare Harvest with one of them, you see the constraint first. This page
states what replay gives you and what it costs. It also shows how Harvest
lowers that cost with compile-time checks, deploy gates and a runtime backstop.

Read the [engine comparison](comparison.md) for the full picture. This page
answers one question from it: why replay, and not checkpoint-only steps?

> **Market facts accurate as of 2026-10-08.** Each claim about another engine
> links its own source. If a claim is stale, open an issue.

## Two models

| Model | How a run resumes | Examples |
|---|---|---|
| Deterministic replay | The engine re-runs the workflow code against its recorded event history. Recorded results replace completed work. | Harvest, Temporal |
| Checkpoint-only steps | The engine loads the saved result of each completed step. Code between steps re-runs and need not be deterministic. | Sayiir, Absurd |
| Process snapshot | The engine restores a saved process or container image. | Trigger.dev |

- [Sayiir](https://github.com/sayiir/sayiir) checkpoints after each task and
  states "no deterministic replay".
- [Absurd](https://lucumr.pocoo.org/2026/4/4/absurd-in-production/) treats each
  step as a checkpoint. Code between steps can read the clock or draw random
  numbers.
- [Trigger.dev](https://indepth.dev/posts/1020/en/how-trigger-dev-checkpoints-containers)
  checkpoints the task container with CRIU and restores it later.

A checkpoint-only engine is easier to write code for. That is a real benefit.
The cost does not go away, though. It moves to the questions the engine can no
longer answer.

## What replay buys

Replay records every decision the workflow makes, not only the results of its
steps. Four capabilities follow from that record.

| Capability | What it gives you | Read more |
|---|---|---|
| Full history | Each command and each result is an ordered event. You can see why a run took a path, not only where it stopped. | [History export](runbooks/replay-fixture-export.md), [timeline (#739)](management-api.md) |
| Reset | An operator forks a run from an earlier completed event boundary. The fork runs under fixed code. | [Reset (#148)](../README.md#resetting-a-workflow-after-a-bad-deploy) |
| Replay debugging | A debugger replays an exported production history offline, one step at a time. It also diffs two builds. | [Replay debugger (#949)](replay-debugger.md) |
| Drift detection | New code replays against the recorded commands. A changed decision fails as a divergence, not as a wrong result. | [Drift gate (#798)](replay-drift-gate.md) |

Drift detection is the capability that checkpoint-only engines cannot copy. They
save step results, not the decisions between steps. If new code takes a
different branch for a running workflow, nothing detects it. The run continues
on the new path.

## What replay costs

These costs are real. Harvest does not hide them.

- **Workflow code must be deterministic.** The workflow body must not read the
  clock, draw random numbers, do I/O or depend on hash iteration order.
- **A change to a running workflow needs a gate.** Without a patch marker, new
  code can diverge from the history of a run that started under old code.
- **Resume cost grows with history.** Replay re-runs the body from the first
  event. A long history costs CPU on each cold resume.
- **A divergence needs an operator.** When drift reaches production, someone
  must roll back, patch or reset the affected runs.

## How Harvest lowers the cost

Harvest checks determinism at four points: when you write the code, when you
build it, before you deploy it and while it runs. Each row below pairs a cost
with the shipped tool that lowers it.

| Cost | Tool | When it acts |
|---|---|---|
| Non-deterministic code in a workflow body | Compile-time guardrails HVG001–HVG011 in the `#[workflow]` macro (#386). A hard blocker fails the build. | Build |
| The same hazards in helper functions | The `det_check` scanner, run as `harvest det-check` (#778) | CI or pre-commit |
| Hazards that cross closures, traits or crates | [`harvest-verify`](harvest-verify.md), an opt-in taint analysis on compiled code | CI |
| A need for time, UUIDs or random numbers | Deterministic primitives such as `ctx.system_now()` and `ctx.new_uuid()` (#384). Each value is recorded once. | Authoring |
| A change to a running workflow | `ctx.patched` and `ctx.deprecate_patch` (#687) | Authoring |
| A regression against saved histories | The [`WorkflowReplayer` harness](replay-verify.md) replays fixture histories in CI (#251). | CI |
| A regression against runs in flight now | The [in-flight drift gate](replay-drift-gate.md) replays a sample of live runs against the candidate build (#798). | Before promotion |
| A regression that only live data shows | A [replay canary](runbooks/safe-deploy.md#runbook-pre-deploy-replay-canary) replays sampled running workflows in memory and writes nothing (#512). | Before deploy |
| Drift that still reaches production | [ND-blocking](runbooks/nondeterminism-block.md) (#603). The run is parked, not failed. A rollback lets it resume. | Run time |
| Finding the cause of a divergence | Replay diagnosis (#614) and the [replay debugger](replay-debugger.md) (#949) | Incident |
| Long histories | Sticky routing with warm-cache delta loading (#235) and a history ceiling (#493) | Run time |

The guardrails and `det_check` catch most hazards before the code runs. The
replay canary and the drift gate catch the rest before a deploy promotes. The
park state is the backstop. A divergent run stays `RUNNING` and alerts, so no
work is lost while you roll back. The [determinism guide](workflow-determinism-guide.md)
lists each rule and the safe alternative.

## When checkpoint-only is the better choice

Replay is not the right trade for every team. A checkpoint-only engine can be
the better choice in these cases:

- **Your runs are short.** If each run ends in minutes, full history and reset
  add little.
- **All logic lives in steps.** If the code between steps makes no decisions,
  there is little drift to detect.
- **You cannot accept rules on workflow code.** For example, the code between
  steps must call a library that reads the clock.
- **You accept silent path changes.** Your team prefers that a changed branch
  runs without warning over a deploy gate.

If none of these apply, the tooling above makes replay cheap to keep. You get
the history, reset, debugging and drift detection that checkpoints cannot give.

## Related

- [Engine comparison](comparison.md): Harvest against five other engines.
- [Workflow determinism guide](workflow-determinism-guide.md): each HVG rule
  and `det_check`.
- [`harvest-verify`](harvest-verify.md): semantic determinism verification.
- [In-flight replay-drift gate](replay-drift-gate.md): the pre-promotion gate.
- [Replay debugger](replay-debugger.md): offline step-through and build diff.
- [Non-determinism block runbook](runbooks/nondeterminism-block.md): what to do
  when a run is parked.
