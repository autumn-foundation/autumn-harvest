# Why Harvest keeps deterministic replay

Some durable-execution engines advertise "no deterministic replay" as a
feature. If you compare Harvest with one of them, you see the constraint first.
This page states what replay gives you and what it costs. It also shows how
Harvest lowers that cost with compile-time checks, deploy gates and a runtime
safeguard.

The [engine comparison](comparison.md) covers all axes. This page answers one
question from it: why replay, and not checkpoint-only steps?

> **The market facts on this page are correct on 2026-10-08.** Each claim about
> another engine links a source. If a claim is stale, open an issue.

## Two models

Two models resume a run from a durable record. A third model restores a process
image. The difference is what the engine checks when it resumes.

| Model | What the engine does on resume | What it checks | Examples |
|---|---|---|---|
| Deterministic replay | Re-runs the workflow code. Recorded results replace completed work. | Each new command matches the recorded command. | Harvest, Temporal |
| Checkpoint-only steps | Loads the saved result of each completed step and skips that step. | Nothing between steps. That code does not have to be deterministic. | Sayiir, Absurd |
| Process snapshot | Restores a saved process or container image. | Nothing. | Trigger.dev |

- [Sayiir](https://github.com/sayiir/sayiir) uses checkpoint-based recovery
  and states "no deterministic replay".
- [Absurd](https://lucumr.pocoo.org/2026/4/4/absurd-in-production/) treats each
  step as a checkpoint. Code between steps can read the clock or draw random
  numbers.
- [Trigger.dev](https://trigger.dev/docs/how-it-works) uses CRIU to checkpoint
  the task process and restore it later.

Some checkpoint engines are hybrids. DBOS resumes from step checkpoints, but its
docs require deterministic workflow code. See the
[comparison](comparison.md#5-determinism-guarantees--tooling).

A checkpoint-only engine is easier to write code for. That is a real benefit.
The cost does not disappear. It moves to questions that the engine cannot
answer: which path a run took, and whether new code takes the same path.

## What replay buys

Replay records every command that the workflow issues, not only the results of
its steps. Drift is new code that issues a different command than the history
records. Four capabilities follow from the record.

| Capability | What it gives you | Read more |
|---|---|---|
| Full history | Each command and each result is an ordered event. You can see which path a run took and the results that led there. | [History export](runbooks/replay-fixture-export.md), [timeline (#739)](management-api.md) |
| Reset | An operator forks a running top-level workflow at an earlier event. The event must be a completed boundary, with no activity, timer or child still open. The fork runs under the current code, and the original run ends. | [Reset (#148)](../README.md#resetting-a-workflow-after-a-bad-deploy) |
| Replay debugging | A debugger replays an exported production history offline, one step at a time. It also diffs two builds. | [Replay debugger (#949)](replay-debugger.md) |
| Drift detection | New code replays against the recorded commands. If the command sequence changes, replay reports a divergence. The run does not continue with a wrong result. | [Drift gate (#798)](replay-drift-gate.md) |

An engine that lets code between steps run freely stores step results only. It
cannot compare new code with the path that a running workflow took. If new code
takes a different branch, the run continues on the new path, and the engine
reports no error.

## What replay costs

- **Workflow code must be deterministic.** The workflow body must not read the
  clock, draw random numbers, do I/O or depend on hash iteration order.
- **Authors must learn the rules.** The team must know what may run in a
  workflow body and what belongs in an activity.
- **A change to a running workflow needs a patch marker.** Without one, new
  code can diverge from the history of a run that started under old code.
- **Patch markers stay in the code.** Each `ctx.patched` branch stays until
  every run that started before it ends. Then `ctx.deprecate_patch` retires it.
- **History stores every payload.** Each activity input and result is an event
  in Postgres. Replay fixtures with full payloads are production data.
- **A history has a size limit.** An optional ceiling fails a run past a set
  event count (#493). A long loop must use continue-as-new.
- **Cold resume cost grows with history.** A cold resume re-runs the body from
  the first event. A long history costs CPU on each cold resume.
- **The gates need upkeep.** The drift gate needs a replay binary in your crate.
  Fixture directories need export and rotation.
- **A divergence needs an operator.** When drift reaches production, someone
  must roll back, patch or reset the affected runs.

## How Harvest lowers the cost

Harvest acts at each stage: when you write the code, build it, test it in CI,
deploy it and run it. Each row pairs a problem with the tool that addresses it.

| Problem | Tool | Stage |
|---|---|---|
| Non-deterministic code in a workflow body | Compile-time guardrails HVG001–HVG011 in the `#[workflow]` macro (#386). A hard blocker fails the build. | Build |
| The same hazards in a helper that the body calls directly | The `det_check` scanner, run as `harvest det-check` (#778). It follows one first-party call. | CI |
| Hazards that cross closures, traits or crates | [`harvest-verify`](harvest-verify.md), an opt-in taint analysis on compiled code. It is a prototype. | CI |
| A need for time, UUIDs or random numbers | Deterministic primitives such as `ctx.system_now()` and `ctx.new_uuid()` (#384). The engine records each value once. | Write |
| A change to a running workflow | `ctx.patched` and `ctx.deprecate_patch` (#687) | Write |
| A regression against saved histories | The [`ReplayVerifier` gate](replay-verify.md) replays fixture histories in CI (#251). | CI |
| A regression against runs in flight now | The [in-flight drift gate](replay-drift-gate.md) replays a sample of live runs against the candidate build (#798). | CI, before promotion |
| A regression that only live data shows | A [replay canary](runbooks/safe-deploy.md#runbook-pre-deploy-replay-canary) replays sampled running workflows in memory and writes nothing (#512). | Deploy |
| Drift that still reaches production | [ND-blocking](runbooks/nondeterminism-block.md) (#603). The engine parks the run. It does not fail it. | Run |
| Finding the cause of a divergence | Replay diagnosis (#614) and the [replay debugger](replay-debugger.md) (#949) | Incident |
| Long histories | Warm-cache delta loading (#235) cuts history reads. Continue-as-new keeps each history short. | Run |

The guardrails and `det_check` catch common hazards at build time. The drift
gate and the canary replay a sample of live runs before a build goes out.
ND-blocking keeps a divergent run in `RUNNING` and raises an alert. In most
cases, a rollback then lets the run resume. The
[runbook](runbooks/nondeterminism-block.md) lists the cases that need a reset.
The [determinism guide](workflow-determinism-guide.md) lists each rule and its
safe alternative.

## When checkpoint-only is the better choice

Replay is not the right trade for every team. A checkpoint-only engine can be
the better choice in these cases:

- **Your runs are short.** Few runs are in flight during a deploy, so drift is
  rare.
- **All logic lives in steps.** If the code between steps makes no decisions,
  there is little drift to detect.
- **You cannot accept rules on workflow code.** For example, your orchestration
  code calls a library that reads the clock.
- **You want a fix to reach running workflows at once.** A checkpoint engine
  runs the new path with no patch marker. Harvest needs a patch marker or a
  reset.
- **You cannot maintain replay fixtures in CI.** Without them, the drift gates
  have nothing to replay.

If none of these cases apply, the tooling above lowers the cost of replay. In
return, you get a full command history, reset to a completed boundary, replay
debugging and drift detection.

## Related

- [Engine comparison](comparison.md): Harvest against five other engines.
- [Workflow determinism guide](workflow-determinism-guide.md): each HVG rule
  and `det_check`.
- [`harvest-verify`](harvest-verify.md): semantic determinism verification.
- [In-flight replay-drift gate](replay-drift-gate.md): the pre-promotion gate.
- [Replay debugger](replay-debugger.md): offline step-through and build diff.
- [Non-determinism block runbook](runbooks/nondeterminism-block.md): what to do
  when a run is parked.
