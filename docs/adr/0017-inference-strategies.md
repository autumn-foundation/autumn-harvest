# ADR 0017: Inference strategies are workflows over durable child runs

## Status

Accepted (issue #2450, epic #2438).

## Context

- [Cascades](https://github.com/google-research/cascades) treats a chain
  of model calls as a probabilistic program. A generator yields named
  random choices, and effect handlers record, replay and score the trace.
  Its draws run in a thread pool, so a crash loses every draw.
- Harvest already has a trace and a replay. History records each model
  call as an activity, and replay reads it back.
- `ChatResponse` has no log-probabilities. Several providers do not
  return them.
- A draw can call tools. N draws of a writing agent can make N writes.
- The strategies of the epic are best-of-N, rejection sampling, a vote, a
  model cascade and step-level beam search.

## Decision

1. **Strategies are workflows over child runs.** `agent_sample` runs each
   draw as a child `agent_loop` run with its own history. History is the
   trace, and replay is the replay handler. Harvest adds no generator and
   no effect-handler interpreter. A second interpreter would duplicate
   both, and a crash would still cost every draw.
2. **Scores come from verifiers.** A `Scorer` is a pure check, a test run
   or a judge model. The run records each verdict. Harvest does not weight
   draws by log-probabilities. `ChatResponse` carries none, and several
   providers do not return them.
3. **A draw that the run does not select only reads.** By default, a draw runs
   `ReadOnly` tools only. An operator widens `draw_effects` per kind in
   the `SamplingPolicy`. A run can never widen it.
4. **What is not adopted.** Likelihood weighting and importance sampling
   need log-probabilities. TRICE trains model weights, and Harvest keeps
   weights fixed. The parameter store of Cascades maps to the versioned
   overlays of #2370. `Choose` for few-shot selection waits for the lesson
   library (#2382).

## Consequences

- A crash costs one draw. Each draw shares the budget tree and the cost
  ledger, and in a mesh each draw counts against the mandate.
- A strategy needs a check. A task with no scorer can still vote on a
  short answer key.
- Strategies that need log-probabilities stay out until providers supply
  them.
- A strategy that writes needs an operator to widen `draw_effects`. The
  audit of the policy store names who did it.
- The guide is [section 28](../agent-adapter.md#28-inference-strategies).
  The plan is in [DESIGN-2450](../design/DESIGN-2450.md).
