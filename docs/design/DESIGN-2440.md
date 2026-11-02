# DESIGN-2440: Draw identity

Issue #2440. Parent epic #2438. Related: #1998 (response cache), #2270
(concurrent children).

## Problem

`ResponseCache::key` hashes the model id, the output cap, the temperature,
the tools, the messages, the tenant and the namespace. N draws of one
request at a temperature above 0 therefore share one key. With the cache
on, the first answer serves every draw. Best-of-N or a vote then collapses
to one sample.

`ChatRequest` carries no seed. Two draws at one step also share the tool
idempotency key `(run_id, step, call_id)`.

## Facts

- `ModelTurnRequest` is the recorded input of `agent_model_turn`.
  `ChatRequest` is not recorded. The harness builds it from the input.
- `AgentTask` is the recorded input of `agent_loop`.
- An inline delegation runs with the run id `{root}:d{n}`. A fan-out child
  (#2270) is a child `agent_loop` run with its own execution id.
- The key is the SHA-256 hash of one JSON document, `KeyMaterial`, at
  format version 1.
- `shares_run_id` skips the cache when the run id is a workflow id that a
  later run can reuse.

## Brainstorm

1. Put a `DrawId` in `ChatRequest` and in the key. This is the direction of
   the issue.
2. Draw a random seed per draw with `ctx.random`. A fork or a retry then
   never shares an entry.
3. Put the run id in the key. Every cross-run hit is then lost.
4. Give each draw its own cache namespace. That hides the draw from the
   model, so no provider gets a seed.
5. Run each draw as a child `agent_loop` run, as a fan-out child does.
6. Add a registered `agent_draws` workflow, or a helper that any workflow
   calls.

Decision: 1, 5 and the helper of 6. The registered workflow belongs to
`agent_sample` (#2443).

## Reverse brainstorm: how to break it

| Way to break it | Guard |
|---|---|
| Change the key bytes of a request with no draw. Every stored entry then misses. | The draw is not written when it is `None`. A golden test pins one key. |
| Write `"draw": null` into a recorded input. | `skip_serializing_if` on each new field. `a_run_with_no_draw_records_no_draw_field` checks it. A test also replays a history recorded before the change. |
| Give two draws one run id. | Run id `{root}:s{n}`. A test checks the run id that tools see. |
| Let two draws delegate the same task. Their children share a key. | A delegated child keeps the draw id. |
| Let a fan-out child keep the root. It then reuses the run id of its parent. | A child keeps the draw id and drops the root. A test checks a fan-out child. |
| Take the root from a workflow id that a later run reuses. | `run_draws` records a UUID. `drive` refuses a root that is not a UUID. `shares_run_id` reads a `:s{n}` suffix as it reads `:d{n}`. |
| Take the root from the execution id. A reset or a fork replays under a new id, and the child inputs differ. Two batches of one run share run ids. | Each `run_draws` call records a new UUID with `ctx.new_uuid`. Tests replay under a new execution id and run two batches. |
| A reset or a fork starts new draws under a recorded root. | Not reachable. `validate_reset_point` refuses a point with an unfinished child, and fork uses it too. The UUID and the child starts share one decision. |
| Check the root against the parent execution id. The replayer has no parent, so replay fails where the live run passed. | The check reads the input only. |
| Let each draw deliver its answer. | `run_draws` turns delivery off. |
| Let a draw that loses write memory, a workspace head or a follow-up. | `run_draws` clears them, as `child_task` does. |
| Let an older worker run a draw. It drops the field. | A Limits note in section 11 of `agent-adapter.md`. |
| Lose the usage of a failed draw. | `run_draws` sets `report_failure`. |
| Start an unbounded batch. | A batch over `MAX_PARALLEL_CHILDREN` is refused. |

## Six hats

- **White.** The facts above. `ChatRequest` has no `non_exhaustive`, so a
  new field breaks each struct literal outside the crate.
- **Red.** A root in the task is a risk. Derive it, do not trust the caller.
  `Draw::root` is private, and the loop refuses a root that is not a UUID.
- **Black.** The breaking change on `ChatRequest`. A provider that maps the
  draw to a seed gives draw 0 of two runs the same seed. The cache already
  shares that answer, so the behaviour agrees.
- **Yellow.** Best-of-N, vote and beam search (#2443, #2444, #2446) build on
  this. A provider with a seed parameter gets one.
- **Green.** `DrawId::run_id(root)` gives `{root}:s{n}` in one place.
- **Blue.** Red tests first, then the smallest code, then the docs and the
  gates.

## Decision

- `DrawId` is a `u32` newtype, serialized as a bare number.
- `ChatRequest::draw: Option<DrawId>`. `None` is one draw, as today.
- `ResponseCache::key` adds `draw` to `KeyMaterial` only when it is `Some`.
- `ModelTurnRequest::draw` carries the draw into the activity. A recorded
  input with no field decodes as `None`.
- `AgentTask::draw: Option<Draw>`. `Draw { id, root }`. With a root, the run
  id is `{root}:s{id}`. A delegated child keeps `id` and has no root.
- `run_draws(ctx, task, ids)` starts one child `agent_loop` run per draw id
  and returns the reports in order. Each call records a new UUID as the
  root. A draw has no memory, no workspace, no follow-ups and no delivery.
- Budgets are not split here. Each draw gets the whole cap. `agent_sample`
  (#2443) puts the draws in one budget tree.
- `AgentModel` documents that a model may pass the draw as a seed. The
  default ignores it.

## Acceptance criteria

See issue #2440. Each test names the criterion it proves.
