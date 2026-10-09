## Feature — A cross-run, content-addressed LLM response cache (issue #1998)

**What shipped.** `autumn-harvest-agent` gets an opt-in `ResponseCache`.
Install it with `AgentHarness::response_cache`. A model call that matches an
earlier call is served from the cache, in any later run. Retries, forks and
evaluation runs stop paying for identical calls. `docs/agent-adapter.md`
section 11 shows the pattern.

**Design.**

- The key is a SHA-256 hash of the model id, the output cap, the
  temperature, the tools, the messages, the tenant and its source, and a
  namespace. `AgentModel::model_id` is new, with a `None` default. A model
  with no id is never cached.
- The answer goes to the embedder's `PayloadStore`, inside a
  `PayloadCodecs` envelope. An active codec encrypts it, and a read refuses
  a plain entry. The entry holds no prompt. A `CacheIndex` maps each key to
  its blob. `InMemoryCacheIndex` has a capacity and deletes the blob of each
  entry it drops.
- The default scope is the tenant (`CacheScope::Tenant`).
  `CacheScope::Shared` is opt-in. `AgentTask::tenant` and
  `HeartbeatTask::tenant` are new. On Postgres, the new
  `ActivityContext::run_tenant` reads the verified tenant of the run (issue
  #1977). It wins over the task field. A declared tenant never shares an
  entry with a verified tenant. A failed read skips the cache.
- A hit is the `agent_model_turn` result. History records it with
  `ModelTurn::cache_hit` and zero token usage. The policy still decides each
  tool call. Replay reads history and never reads the cache.
- A cache fault, a slow cache or an entry that does not decode is a miss
  with a warning. An out-of-date entry is a quiet miss. Each cache call has
  the new `cache_timeout` budget, 5 seconds by default.
- On Postgres, a turn whose run id is the workflow id skips the cache. A hit
  repeats tool-call ids, so it could repeat a tool idempotency key.
- `docs/agent-adapter.md` now cites #1996, not the epic #1970, for the cost
  ledger.

**Invariants.** No new `WorkflowEvent` variant. No migration. No route
change. `AgentTask::tenant`, `HeartbeatTask::tenant`,
`ModelTurnRequest::tenant` and `ModelTurn::cache_hit` skip serialization at
their defaults, so recorded payloads keep their shape. An older binary
cannot strictly replay a run that sets `tenant`. Set it only after the whole
fleet runs this release.

**Tests.** `tests/response_cache.rs` serves a second SQLite run from the
cache and checks the recorded turn. A strict replay of that history
succeeds. Other tests prove five more rules. Tenants do not share an entry.
A model with no id is not cached. A broken store falls back to the model.
The shared scope serves every tenant. A hit still asks the policy. Unit
tests cover each key part, both scopes, the tenant source, the entry check,
`max_age`, eviction, and `AeadCodec` encryption of the stored blob. They
also cover the tenant rules of the activity. On Postgres,
`an_activity_reads_the_verified_tenant` proves `run_tenant`.
