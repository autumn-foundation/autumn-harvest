# Design — Issue #1998: a cross-run LLM response cache

Replay reads a model answer back from one run's history. A second run that
sends the same request pays for it again. Issue #1998 asks for an opt-in cache
that serves an identical call across runs.

**No migration. No new `WorkflowEvent` variant. No route change.**

---

## 0. Planning record

### 0.1 Brainstorm — where does the cache live, and what is its key?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Cache in the engine, below every activity. | Rejected. The engine does not know which activity is a model call. |
| B2 | Cache in `AgentHarness::model_turn`, the one place that calls the model. | **Adopted.** The hit is the activity result, so history records it as usual. |
| B3 | Store the answer through `PayloadStore`. | **Adopted.** The issue asks for it. The embedder owns the backend. |
| B4 | Use the `PayloadStore` key as the cache key. | Rejected. The store picks the key, so a reader cannot compute it. |
| B5 | A small `CacheIndex` trait maps a cache key to a blob key. Ship `InMemoryCacheIndex`. | **Adopted.** It follows `MemoryStore` and `InMemoryMemoryStore`. |
| B6 | Key on the model id, the parameters, the tools and the messages. | **Adopted.** A SHA-256 hash of one JSON document. |
| B7 | Key on the prompt text only. | Rejected. A different model or tool list gives a different answer. |
| B8 | Take the model id from a new `AgentModel::model_id`. No id means no cache. | **Adopted.** The client knows its model. A guess would mix models. |
| B9 | Encrypt the stored entry with `PayloadCodecs::encode_payload`. | **Adopted.** The same envelope and key rotation as event payloads. |
| B10 | Take the tenant from a new `AgentTask::tenant` field only. | Rejected alone. A tenant-bound caller writes the start body (issue #1977, design risk R8). |
| B11 | Add `WorkflowContext::tenant` and put it in each `ModelTurnRequest`. | Rejected. Strict replay compares activity inputs. Offline replay paths do not know the tenant. |
| B12 | Read the verified tenant inside the activity: `ActivityContext::run_tenant`. | **Adopted.** One primary-key read on the run's own shard. Replay never sees it. |
| B13 | Report the original token usage on a hit. | Rejected. A hit spends no tokens. Usage is zero, and `ModelTurn::cache_hit` is `true`. |

### 0.2 Reverse brainstorm — how can this cache do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Serve one tenant's answer to another tenant. | The tenant is in the key. The default scope is `CacheScope::Tenant`. The entry repeats its key, and a mismatch is a miss. Red test first. |
| R2 | Let a tenant-bound caller name another tenant in `AgentTask::tenant`. | On Postgres the verified tenant of the run replaces it. A failed lookup skips the cache. |
| R3 | Change replay. | A hit is the activity result. Replay reads history and never reads the cache. A replay test proves it. |
| R4 | Store a prompt or an answer in clear. | The entry holds the answer only, codec-encoded. The index holds two opaque keys. A test reads the raw blob. |
| R5 | Fail a run because the cache is down. | Every cache fault is a miss with a warning. Each call has the `cache_timeout` budget. |
| R6 | Serve an answer from another model. | No model id means no cache. The id is in the key. |
| R7 | Serve a stale answer for ever. | `ResponseCache::max_age`. A new `namespace` makes every old entry a miss. |
| R8 | Grow the in-memory index without bound. | It has a capacity. It evicts the oldest entry and deletes its blob. |
| R9 | Test a guessed prompt against the index. | A secret `namespace` goes into the hash. Document it. |
| R10 | Count a hit against the token budget. | Usage is zero on a hit. |
| R11 | Keep a sampled answer for ever at a high temperature. | The cache is opt-in. Document that a hit repeats one sample. |
| R12 | Lose an entry after a codec key retires. | The entry fails to decode. That is a miss, and the next answer replaces it. |
| R13 | Keep erased data in the cache. | Erasure does not reach the cache. Document it, and a store expiry. |
| R14 | Let a declared tenant name a verified tenant. | The tenant source is in the key. |
| R15 | Repeat a tool idempotency key in another run. | On Postgres, a turn whose run id is the workflow id skips the cache. |
| R16 | Overrun the 15-minute `start_to_close` with cache calls. | A 5-second `cache_timeout` per call. A test checks the sum. |
| R17 | Log a decrypted answer in a decode error. | Decode errors use fixed text. |

### 0.3 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | `PayloadStore` is put, get and delete by an opaque key. `PayloadCodecs` is not `db`-gated. `harvest_workflow_executions.tenant` holds the verified tenant (#1977). The activity context has the pool and the execution id of a regular activity. |
| Red | "The same question, the same tenant, no second bill" is easy to explain. A cache that can answer across tenants is frightening. |
| Black | Unkeyed hashes let an index reader test a guess. A hit repeats one sample. The engine tenant read costs one query per turn. A rebalance in flight can hide the row. |
| Yellow | Retries, forks and evaluation runs stop paying for identical calls. Replay does not change. The cost ledger (#1996) can count hits from `cache_hit`. |
| Green | A durable `CacheIndex` on Postgres or SQLite can come later. Request coalescing can come later. |
| Blue | Red: key, scope, codec, cross-run and replay tests. Green: the cache, the harness hook and `run_tenant`. Refactor: docs, changelog, review. |

---

## 1. Design

### 1.1 Key

`ResponseCache::key` hashes one JSON document with SHA-256:

```text
{ v, namespace, scope, tenant_source, tenant, model, max_tokens, temperature_bits, tools, messages }
```

- `scope` is `tenant` or `shared`. A shared key has no tenant.
- `tenant_source` is `none`, `verified` or `declared`.
- `temperature_bits` is `f32::to_bits`, so the value is exact.
- `serde_json` sorts object keys, so key order in a tool argument does not
  change the key.

### 1.2 Entry

The blob is the codec envelope of `{ v, key, stored_at, content, stop_reason, usage }`.
A read checks `v` and `key`. A mismatch, a decode fault or an old entry is a
miss.

### 1.3 Tenant

| Backend | Tenant of the key |
|---------|-------------------|
| Postgres, run with a verified tenant | The verified tenant. `AgentTask::tenant` is ignored, with a warning when it differs. |
| Postgres, run with no tenant | `AgentTask::tenant`, declared. |
| Postgres, lookup failed | No cache for the turn. |
| SQLite | `AgentTask::tenant`, declared. |

`ActivityContext::run_tenant` reads the tenant. It returns `None` for a test
context and a build without `db`. It refuses a local activity.

### 1.4 Turn

1. No cache, no model id, a failed tenant read, or a run id that is the
   workflow id: call the model.
2. Look up. A hit returns the cached answer with zero usage and
   `cache_hit: true`. The policy still decides each tool call.
3. A miss calls the model, then stores the answer. A store fault is a warning.

---

## 2. Test plan

| Test | Proves |
|------|--------|
| `response_cache::tests` | The key changes with each part. The scope rules. The entry check. The age limit. Eviction. |
| `response_cache::tests::an_active_codec_encrypts_the_entry` | The raw blob has no plaintext under `AeadCodec`. |
| `tests/response_cache.rs` (SQLite) | A second run is served from the cache and records the turn. Strict replay of that history succeeds. Tenants do not share entries. Faults fall back to the model. |
| `tests/engine_activities.rs` | The engine-path activity uses the cache. |
| `workflow::tests` | The verified tenant wins. A failed read and a shared run id skip the cache. |
| `context::tests::run_tenant_is_none_for_a_test_context_and_refused_for_a_local_one` | `run_tenant` outside a regular activity. |
| `tenant_propagation_tests::an_activity_reads_the_verified_tenant` | `run_tenant` returns the run's tenant on Postgres. |

---

## 3. Revision after review

Four review agents read the change: correctness, security, replay and
documentation. The design took these changes:

- The tenant source is in the key (R14). Before, a declared `acme` and a
  verified `acme` shared a key.
- `run_tenant` refuses a local activity. Before, it returned `None`, and the
  task field then applied.
- A turn whose run id is the workflow id skips the cache (R15).
- Cache calls have their own 5-second budget (R16). Three 30-second hook
  budgets could overrun `start_to_close`.
- Decode errors use fixed text (R17). An active codec refuses a plain entry.
- `max_age` checks both directions, so a fast clock cannot extend an entry.
- The docs no longer say that `max_age` or `namespace` removes data.
