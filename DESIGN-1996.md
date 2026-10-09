# Design — Issue #1996: agent cost ledger

Issue #1996 asks Harvest to record the model id, the tokens, the cost and
the latency of each LLM step. Usage reports must roll the figures up per
workflow type and per tenant. The figures must not ride inside the
completion `output`, because `AeadCodec` encrypts that field.

**One migration (a new side table). No new `WorkflowEvent` variant. No
change to `harvest_events`. No replay impact. No new route.**

---

## 0. Planning record

### 0.1 Facts found before the plan

- An activity handler returns a bare `serde_json::Value`
  (`ActivityHandlerFn`, `info.rs`). It has no side channel today.
- Three Postgres paths append a completion event. The worker path is
  `finalize_activity_completion_write` (`worker.rs`). The transactional
  path is `ActivityContext::run_transactional` (`context.rs`). The local
  path is `append_frontier_resolution` (`worker.rs`). Each one knows the
  new `event_id` before it inserts the row.
- `harvest_events.id` is not a stable key. The insert does not return it.
  The partitioned layout keys rows by `(id, cohort)`. A shard move does not
  carry `id`. The stable key is `(workflow_exec_id, event_id)`.
- The codec encrypts six named fields under `event_data.data`. A column
  outside `harvest_events` is in clear unless the codec covers it.
- The usage report (issue #596) has no tenant column. A tenant is
  `group_by=search_attr:<key>`, by convention `search_attr:tenant_id`.
- Retention deletes the execution row. Side tables follow through
  `ON DELETE CASCADE`. `harvest_workflow_logs` is the precedent.
- A shard move copies a fixed list of child tables
  (`shard_rebalance::COPIED_RELATIONS`). A table not on the list loses its
  rows on a move.
- The agent adapter (#1973) already reads `TokenUsage` from each model
  reply. It stores the usage inside the encrypted `output` only. Its guide
  defers per-step cost to this ledger.

### 0.2 Brainstorm — where does the metadata live, and who writes it?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Put the figures in the completion `output`. | Rejected. Under `AeadCodec` the output is ciphertext. SQL cannot sum it. |
| B2 | Add an `LlmStepCompleted` `WorkflowEvent` variant. | Rejected. The issue forbids it. A new variant changes history and replay. |
| B3 | Add clear fields beside `output` in `ActivityCompleted.data`. | Rejected. That changes the stored event shape. Old readers and the replay matcher see it. |
| B4 | A side table keyed by `(workflow_exec_id, event_id, call_index)`, written in the completion transaction. | **Adopted.** History does not change. The row commits with the event or not at all. |
| B5 | A typed `LlmActivity` trait that wraps the handler. | Rejected. It fixes one call shape. A plain context method also serves interceptors and hand-written handlers. |
| B6 | `ActivityContext::record_llm_call(LlmCall)`, read by the engine after the handler returns. | **Adopted.** It works for `#[activity]`, raw handlers and interceptors. It needs no `db` feature, so the agent crate can call it. |
| B7 | One ledger row per activity. | Rejected. One step can call two models. `call_index` allows many calls per step. |
| B8 | Cost as `NUMERIC` or `f64`. | Rejected. Diesel has no `numeric` feature here, and a float sum drifts. Cost is `BIGINT` millionths of a US dollar (`cost_usd_micros`). |
| B9 | Roll up through the existing `GET /admin/usage` query, with new fields per group. | **Adopted.** `group_by=workflow_name` gives the per-type view. `group_by=search_attr:tenant_id` gives the per-tenant view. |
| B10 | Record the agent adapter's model turns. | **Adopted.** Two default methods on `AgentModel` name the model and price the call. Existing implementations still compile. |
| B11 | Gate quota checks on the ledger. | Deferred. The issue does not ask for it. The clear table makes it possible later. |

### 0.3 Reverse brainstorm — how can this change do harm?

| # | How to make it harmful | Mitigation |
|---|------------------------|------------|
| R1 | Write the ledger in its own transaction, so a crash keeps a cost for a step that never completed, or loses it. | Each path inserts the rows in the completion transaction, after the event. Tests read both in one run. |
| R2 | Count a cost twice when a lost lease drops the completion. | A lost lease returns before the insert. The later attempt records its own calls. |
| R3 | Put the prompt or PII in the model id, in clear. | The model id is capped at 200 bytes and must not be empty. The security posture page says it is in clear and must not hold PII. |
| R4 | Let a handler record without bound and flood the table. | At most 256 calls per attempt. The 257th call returns an error. |
| R5 | Store a value that breaks a `CHECK` and rolls back the completion. | `record_llm_call` checks every value before it accepts the call. The table checks again. |
| R6 | Lose the rows on a shard move. | The table joins `COPIED_RELATIONS`, the parity list, the copy and the staged-copy cleanup. Test. |
| R7 | Keep rows after retention deletes the run. | `ON DELETE CASCADE` from `harvest_workflow_executions`. Test. |
| R8 | Copy rows into a reset fork, so the report counts a cost twice. | Reset copies events only. The ledger stays with the source run. |
| R9 | Take a long lock on `harvest_workflow_executions` in the migration. | The `REFERENCES` clause runs after `SET LOCAL lock_timeout = '5s'`. The `down.sql` does the same. The lock lint checks both. |
| R10 | Change replay. | Nothing in history changes. A replay test runs one workflow with a ledger and one without, then compares the decoded histories and replays both. |
| R11 | Break the usage report for runs with no ledger rows. | The new CTE joins with `FULL OUTER JOIN`. Absent rows read as zero. The existing usage tests stay green. |
| R12 | Name the table `harvest_events_*`. | Event-reclaim tests sum deletes over `harvest_events%`. The table is `harvest_llm_ledger`. |
| R13 | Write from a fenced DR shard. | The insert runs after the fenced event append in the same transaction. A fenced append aborts both. |
| R14 | Drop calls recorded after `run_transactional` commits. | The commit takes the calls that exist at commit time. The docs say to record before the commit. A later call logs a warning. |

### 0.4 Six thinking hats

| Hat | Notes |
|-----|-------|
| White | Three completion paths. A stable key exists before the insert. The usage query already has a CTE per metric and a `FULL OUTER JOIN` per group. The agent adapter has tokens but no model id, cost or latency. |
| Red | Operators want one number per tenant per month: "what did agents cost?". A per-call API that is hard to reach will go unused. |
| Black | A non-final failed attempt appends no event, so it gets no ledger row. A retried call that cost tokens is not counted. The SQLite backend does not write the ledger. Model ids and token counts are visible without the codec key. |
| Yellow | No replay risk by construction. The cost sum is exact integer arithmetic. The figures are queryable with plain SQL. The same API serves the agent loop and any hand-written LLM activity. |
| Green | A `group_by=model` dimension, quota hooks on cost, and failed-attempt metering are follow-ups. An interceptor can record calls for every LLM activity in one place. |
| Blue | Red: unit tests for `LlmCall` and the context, a DB test for the three paths, the usage rollup, the shard copy, retention and replay, plugin and CLI tests, and a docs guard. Green: migration, module, context, write paths, usage SQL, plugin, CLI, agent and docs. Refactor: gates, then a multi-angle review. |

### 0.5 Scope

In scope: the four "done when" items, the shard move, retention, the CLI
table and the agent adapter. Out of scope: failed-attempt metering, quota
checks, the SQLite backend and a per-model report dimension. The changelog
fragment lists them as known limits.

---

## 1. API

```rust
use autumn_harvest::llm_ledger::LlmCall;

ctx.record_llm_call(
    LlmCall::new("claude-sonnet-5-5", 1_200, 340)
        .with_cost_usd_micros(8_700)
        .with_latency(elapsed),
)?;
```

- `LlmCall::new(model, input_tokens, output_tokens)`. The cost is optional.
  A call with no cost counts as unpriced.
- When the latency is not set, the engine records the run time of the
  attempt so far.
- `record_llm_call` returns `LlmCallError` for an empty or long model id,
  a value above `i64::MAX`, or more than 256 calls. `LlmCallError`
  converts to `String`, so `?` works in an activity.
- `ActivityContext::llm_calls()` returns the calls recorded so far. Unit
  tests use it.
- The engine writes the calls only when the attempt completes. A failed
  attempt writes none.

## 2. Storage

Migration `20261009050156_harvest_llm_ledger`:

| Column | Type | Note |
|---|---|---|
| `workflow_exec_id` | `UUID` | FK to `harvest_workflow_executions`, `ON DELETE CASCADE`. |
| `event_id` | `INT` | The completion event of the step. |
| `call_index` | `INT` | Order of the call in the step. |
| `activity_name` | `TEXT` | The step's activity type. |
| `model` | `TEXT` | The model id. |
| `input_tokens`, `output_tokens` | `BIGINT` | Not negative. |
| `cost_usd_micros` | `BIGINT NULL` | Millionths of a US dollar. `NULL` is unpriced. |
| `latency_ms` | `BIGINT` | Not negative. |
| `recorded_at` | `TIMESTAMPTZ` | The completion transaction time. |

The primary key is `(workflow_exec_id, event_id, call_index)`. An index on
`recorded_at` serves the report window. The table has no `JSONB` column and
no `BIGSERIAL`.

## 3. Usage report

Each group of `GET /admin/usage` gains six fields. The window is
`recorded_at`.

| Field | Meaning |
|---|---|
| `llm_calls` | Ledger rows. |
| `llm_input_tokens` | Sum of input tokens. |
| `llm_output_tokens` | Sum of output tokens. |
| `llm_cost_usd_micros` | Sum of priced costs. |
| `llm_unpriced_calls` | Rows with no cost. |
| `llm_latency_ms` | Sum of latencies. |

The CLI `harvest usage` table adds `LLM_CALLS`, `LLM_IN`, `LLM_OUT` and
`LLM_COST_USD`.

## 4. Tests

| Test | Where |
|---|---|
| `LlmCall` checks, context record and take, error text | `llm_ledger.rs`, `context.rs` unit tests |
| Worker, local and transactional paths write the rows; the output stays ciphertext; a failed attempt writes none | `tests/integration/llm_ledger_tests.rs` |
| Replay is unchanged by the ledger | same file |
| Usage rollup per workflow type and per tenant | same file |
| Retention cascade and shard-move copy | same file |
| Plugin merge and JSON fields | `autumn-harvest-plugin/src/usage.rs` unit tests |
| CLI table columns | `autumn-harvest-cli` usage tests |
| Agent model turn records one call | `autumn-harvest-agent/tests/engine_activities.rs` |
| Security posture names the clear ledger fields | `llm_ledger.rs` docs guard |
