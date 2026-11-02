## Phase 2438.2440 — Draw identity: run id, request and cache key (issue #2440)

`DrawId` is a new `u32` newtype. `ChatRequest::draw` carries it to the
model, and `ResponseCache::key` puts it in the key. Two draws of one request
now get two cache entries. Before, best-of-N with the cache on collapsed to
one sample. `run_draws` runs each draw as a child `agent_loop` run with the
run id `{root}:s{n}`, so tools get one idempotency key per draw.

Design decisions (`docs/design/DESIGN-2440.md`):

- The key document holds the draw only when it is `Some`. A request with no
  draw keeps its key bytes. A golden test pins one key from `trunk-dev`.
- `ModelTurnRequest::draw` and `AgentTask::draw` are not written when
  `None`. A history recorded before the change replays. A test replays a
  fixture history from `trunk-dev`.
- Each `run_draws` call records a new UUID as its root. Two batches never
  share a run id, and a reset or a fork replays the same root. `drive`
  refuses a root that is not a UUID. `shares_run_id` reads a `:s{n}`
  suffix as it reads `:d{n}`.
- A delegated child keeps the draw id and drops the root.
- A draw never delivers, and it reports a failure, so the caller keeps its
  usage. It has no memory, no workspace and no follow-ups. Each draw gets
  the whole token cap. `agent_sample` (#2443) will split it.
- A batch over `MAX_PARALLEL_CHILDREN`, or with one id twice, starts no
  draw.

Breaking change: `ChatRequest` has a new public field. A struct literal
outside the crate adds `draw: None`. See `docs/upgrading/0.8.0.md`.

Fleet rule: call `run_draws` only after every worker runs this release. An
older binary drops the field.

No new `WorkflowEvent` variant. No migration. No new query.

Tests: `autumn-harvest-agent/tests/draws.rs`, the key tests in
`response_cache.rs`, the root tests in `draw.rs` and
`a_draw_of_a_workflow_id_run_skips_the_cache` in `workflow.rs`.
