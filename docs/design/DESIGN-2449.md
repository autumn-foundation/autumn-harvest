# DESIGN-2449: strategies in a durable agent and a mesh

Issue #2449. Parent epic #2438. Needs #2439 (policy) and #2443
(`agent_sample`). Related: #2263 (durable agent), #2269 (durable peers)
and #2272 (mesh budget).

## 1. Problem

`agent_sample` starts from a kind and an input text. A durable `Agent`
handles ops from its inbox. A mesh peer gets work through a peer message.
Neither can use a strategy.

A `Mandate` bounds depth, fan-out, peers, tools and models. It does not
bound draws. A peer that runs best-of-8 for each message spends 8 times
the tokens, and its caller cannot see or cap that.

## 2. Brainstorming

The chosen option is in bold.

| Question | Options |
|---|---|
| Where an op carries the strategy | a new `AgentOp` variant · **an optional `sample` field on `UserMessage` and `PeerEnvelope`** |
| The strategy type | the policy `Strategy` · **`OpSample`: a `SampleStrategy`, a `ScorerRef` and an optional concurrency** |
| How the agent runs it | spawn `agent_sample` as a child workflow · **call the core of `agent_sample` inline, with the task that the agent builds** |
| What a draw sees | the input text only · **the op task: the transcript as history, the flow labels and the narrowed mandate** |
| The draw cap of a mandate | a new budget type · **`Mandate::max_draws: Option<u32>`, met like every other field** |
| The meet of two caps | the parent value · **the smaller value; `None` only when both sides are `None`** |
| Draws and fan-out | ignore fan-out · **each draw is one delegation unit; the draws split the rest with `sibling_cap`** |
| A beam and fan-out | each step is one unit · **`max_draws` only; a beam counts delegations per path** |
| A cascade and fan-out | `max_draws` only · **each tier is one unit, as each tier is a run of its own** |
| The refusal | a sampling refusal · **`mesh::Refusal::DrawsExceeded`, so a peer reply names it** |
| The mesh budget | a new check · **each draw carries the mesh of the profile, so the model-turn check applies** |
| Old histories | no gate · **a patch marker gates the mandate check in `agent_sample`** |

Why inline, and not a child workflow? `agent_sample` takes no task. So
a caller cannot borrow the policy of a kind for a task of its own. A child
workflow would need a task or a mandate in its input, which reopens that
hole. Inline, the agent builds the task, and no new input reaches the
public workflow.

## 3. Reverse brainstorming

"How could a strategy op spend, widen or hide more than it may?" Each
answer gives a rule.

| Failure | Rule |
|---|---|
| A peer runs best-of-8 for each message, out of sight. | `max_draws` bounds the draws. Each draw also counts as one delegation unit. |
| A child mandate gets a larger cap than its parent. | `meet_cap` returns the smaller value. Verus proves it, and a property test checks `delegate`, `outbound` and `accept`. |
| A receiver drops `max_draws` it does not know. | `from_meta` refuses each unknown key. The field is skipped when `None`, so an old mandate still decodes. |
| Draws run under the wide profile mandate, not the narrowed one. | The draws take the mandate of the op. A peer op takes the accepted mandate. |
| N draws each get the whole fan-out. | Draw `i` gets `sibling_cap(rest, n, i)` delegations. The shares sum to the rest. |
| A refused op spends tokens. | The mandate check runs first and needs no read. The policy check runs before any model call. |
| A refused op kills the agent. | A refusal is a failed op: a count and an error text. The next op runs. |
| A mesh budget does not see the draws. | Each draw carries the mesh, so each model turn checks the budget. |
| A draw that the run does not select hides its taint. | The report holds the union of the labels of each draw. The agent keeps it. |
| A steer message with a strategy joins a running op. | A message with a strategy never sends a steer signal. It runs as its own op. |
| A judge calls a model outside the mandate. | The mandate must allow the judge model, else `model_forbidden`. |
| A spent mesh budget drops a message. | A mesh refusal parks the agent. An op whose draws spent no token waits first in line. |
| A large transcript fails the spawn late. | The op checks the draw input against the workflow input cap first. |
| The scorer reads the old transcript. | The score input holds only the messages after the history. |
| An old `agent_sample` run replays into the new check. | A patch marker gates the check. An old run reads no marker and skips it. |

## 4. Six thinking hats

- **White (facts).** `run_draw_tasks` already records a UUID root per
  wave, and it adds the agent id to `waiting` when it runs in an agent.
  `sibling_cap` splits a rest, and Verus proves the shares sum to it. The
  model-turn activity checks the mesh budget before each call.
- **Red (feelings).** An operator fears a silent multiplier of cost in a
  mesh most. The caller must be able to cap it, and the cap must only
  narrow.
- **Black (risks).** A new mandate key fails an old receiver. That is the
  intended fail-closed path, and a `None` cap sends no key. The agent
  workflow is under strict `harvest-verify`, so the inline call must stay
  in code that the analyzer can follow.
- **Yellow (benefits).** A durable agent gets best-of-N with its own
  transcript. A mesh caller bounds the draws of the whole subtree.
- **Green (ideas).** A later issue can let the `delegate` tool ask for a
  strategy. The vote (#2444) can reuse `OpSample`.
- **Blue (process).** Red tests first, one per acceptance line. Then the
  kernels and their Verus proof, the mandate field, the inline core, the
  agent op, the docs. Then a review from several angles.

## 5. Decision

```rust
pub struct OpSample {
    pub strategy: SampleStrategy,
    pub scorer: ScorerRef,
    pub concurrency: Option<NonZeroU32>,
}

pub struct Mandate {
    // ...
    pub max_draws: Option<u32>,
}

pub const fn meet_cap(left: Option<u32>, right: Option<u32>) -> Option<u32>;
pub const fn draw_verdict(max_draws: Option<u32>, room: u32, draws: u32) -> DrawVerdict;
```

The op path:

1. `open_op` reads the op profile and builds the segment task, as for a
   plain op. A peer op takes the accepted mandate.
2. The task gets the transcript as history and the labels of the agent.
3. `Mandate::admit_draws` refuses `DrawsExceeded`, then
   `FanOutExhausted`.
4. The sample core reads the policy, admits the request, and runs the
   draws in waves.
5. The selected draw gives the answer and the new transcript. The tokens
   and the labels of each draw go into the state.

A refused or failed op changes no transcript. A peer gets the refusal in
`PeerReply::refusal`, or the error in `PeerReply::error`.

## 6. Limits

- A mesh refusal of a draw parks the agent. Only an op whose draws spent
  no token waits for the next period. Any other op answers with the
  draws that ran, or fails with `no_accepted_draw`.
- A judge call does not check the mesh budget before the call.
- An interrupt does not stop the draws of a running op.
- A draw reads the progress notes of the agent, but saves none.
- A worker that predates `max_draws` drops the field from a peer message.
- The `delegate` tool cannot ask for a strategy. Only an op can. Issue
  #2610 tracks it.
- A draw has no memory tool, no workspace and no follow-ups, as in
  `agent_sample`.
