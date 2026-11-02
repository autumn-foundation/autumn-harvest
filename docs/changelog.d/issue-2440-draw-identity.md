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
- The root of a draw is the execution id of the caller, so no later run
  reuses a draw run id. `shares_run_id` reads a `:s{n}` suffix as it reads
  `:d{n}`.
- A delegated child keeps the draw id and drops the root.
- A draw never delivers, and it reports a failure, so the caller keeps its
  usage.
- A batch over `MAX_PARALLEL_CHILDREN`, or with one id twice, starts no
  draw.

Breaking change: `ChatRequest` has a new public field. A struct literal
outside the crate adds `draw: None`.

No new `WorkflowEvent` variant. No migration. No new query.

Tests: `autumn-harvest-agent/tests/draws.rs`, and the key tests in
`response_cache.rs`.
