# Agent cost ledger

The agent cost ledger records the model id, tokens, cost and latency of
each LLM call (issue #1996). The usage report sums it per workflow type and
per tenant.

The ledger is a separate table, `harvest_llm_ledger`. It is not part of the
workflow history, so replay does not change. The payload codec does not
encrypt it, so SQL can sum it without a key. The model id and the counts are
in clear. See
[the security posture](security-posture.md#llm-cost-ledger-issue-1996).

## 1. Record a call

Call `ActivityContext::record_llm_call` once for each model call:

```rust
use std::time::Instant;
use autumn_harvest::llm_ledger::LlmCall;
use autumn_harvest::prelude::*;

#[activity(start_to_close = "5m")]
async fn summarise(ctx: &ActivityContext, prompt: String) -> Result<String, String> {
    let started = Instant::now();
    let reply = call_my_model(&prompt).await?;
    ctx.record_llm_call(
        LlmCall::new("claude-sonnet-5-5", reply.input_tokens, reply.output_tokens)
            .with_cost_usd_micros(reply.cost_usd_micros)
            .with_latency(started.elapsed()),
    )?;
    Ok(reply.text)
}
```

- `LlmCall::new(model, input_tokens, output_tokens)` makes the call.
- `with_cost_usd_micros` sets the cost in millionths of a US dollar. A call
  with no cost counts as unpriced.
- `with_latency` sets the latency. When it is not set, the context records
  the time since the previous call, or since the attempt started. Set it
  when an attempt does other slow work between calls.
- An activity can record up to 256 calls in one attempt.
- `ActivityContext::llm_calls` returns the calls recorded so far. Use it in
  a unit test.
- An interceptor gets the same context, so it can record the calls of many
  activities in one place.

`record_llm_call` checks each call:

| Check | Limit |
|---|---|
| Model id | 1 to 200 bytes of ASCII letters, digits and `._:/@+-` |
| Input or output tokens | At most 10^12 |
| Cost | At most 10^15 millionths of a US dollar |
| Latency | Saturates at 10^10 ms |

A refused call returns `LlmCallError`. The error converts to a
non-retryable activity failure. So `?` fails the attempt and does not run
the model again.

## 2. When the engine writes the rows

The engine writes one row per call when the attempt completes. The write is
in the transaction that appends the completion event, so the rows and the
event commit together. Each row holds the `event_id` of that event and the
`call_index` of the call in the attempt.

| Path | Completion event |
|---|---|
| A worker activity | `ActivityCompleted` |
| A local activity | `LocalActivityCompleted` |
| `run_transactional` | `ActivityCompleted`, in the user transaction |

In a `run_transactional` activity, record the calls before the commit. The
commit writes the calls that exist at that time. The engine logs a warning
for a call recorded after the commit and drops it.

## 3. Read the ledger

`GET /admin/usage` adds six fields to each group:

| Field | Meaning |
|---|---|
| `llm_calls` | Ledger rows in the window. |
| `llm_input_tokens` | Sum of the input tokens. |
| `llm_output_tokens` | Sum of the output tokens. |
| `llm_cost_usd_micros` | Sum of the priced costs, in millionths of a US dollar. |
| `llm_unpriced_calls` | Rows with no cost. |
| `llm_latency_ms` | Sum of the latencies, in milliseconds. |

The window applies to `recorded_at`. `recorded_at` is the time of the
completion transaction. It equals the completion event's `timestamp`, so a
row and its event fall in the same window.

Use `group_by=workflow_name` for the cost per workflow type. Use
`group_by=search_attr:tenant_id` for the cost per tenant. See
[the usage report](sharding.md#historical-per-tenant-usage-report-issue-596).

`harvest usage` shows `LLM_CALLS`, `LLM_IN`, `LLM_OUT`, `LLM_COST_USD` and
`LLM_UNPRICED` columns. `LLM_COST_USD` sums the priced calls only, so read it
with `LLM_UNPRICED`. `--json` prints every field.

The table is plain SQL, so a custom report can read it. A shard move leaves
a copy of the rows on two shards. Filter as the usage report does, so the
fleet counts each run once:

```sql
SELECT l.model, SUM(l.input_tokens), SUM(l.output_tokens), SUM(l.cost_usd_micros)
  FROM harvest_llm_ledger l
  JOIN harvest_workflow_executions w ON w.id = l.workflow_exec_id
 WHERE l.recorded_at >= NOW() - INTERVAL '30 days'
   AND (w.state NOT IN ('MIGRATING', 'MIGRATED')
        OR (w.state = 'MIGRATED' AND EXISTS (
            SELECT 1 FROM harvest_shard_migrations m
             WHERE m.execution_id = w.id AND m.phase = 'COMMITTED')))
 GROUP BY l.model;
```

## 4. The agent adapter

The `agent_model_turn` activity of `autumn-harvest-agent` records one call
per model turn. Implement `AgentModel::model_id` to name the model, and
`AgentModel::cost_usd_micros` to price a call. The defaults are `"unknown"`
and unpriced. The ledger records a refused model id as `"unknown"`, so the
tokens still count. See [the agent adapter](agent-adapter.md).

## 5. Lifecycle

- **Retention.** The rows cascade with the execution row. The history
  archive does not hold them, so retention deletes the cost data too.
- **Shard moves.** The rows move with the run. The report skips the staged
  target copy. It counts the sealed source until the move is done. So one
  shard reports each run, with one exception. Activation commits the target
  first, then marks the source done on its own database. Between the two
  commits, both shards count the run. If the second commit fails, the
  window lasts until migration recovery ends it.
- **Reset.** A fork copies events, not ledger rows. The cost counts once.
- **Erasure.** Erasure keeps the rows. They hold no payload. The model id
  is the only free-form text, and it must not hold PII.
- **Cross-region DR.** The `FOR ALL TABLES` publication copies the table to
  the standby.

## 6. Limits

- A failed attempt writes no row. So does an attempt that loses its lease,
  or that returns a result over the result cap. Tokens that such an attempt
  spent are not counted.
- A SQLite activity has no `ActivityContext`, so it cannot record a call.
  The SQLite agent path writes no ledger rows.
- The ledger has no columns for cached prompt tokens.
- The report has no per-model dimension. Query the table for that.
- Quota checks do not read the ledger.
