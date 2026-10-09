# Agent cost ledger

The agent cost ledger records the model id, the tokens, the cost and the
latency of each LLM call that an activity makes (issue #1996). The usage
report sums the ledger per workflow type and per tenant.

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
- `with_latency` sets the latency. When it is not set, the engine records
  the run time of the attempt.
- An activity can record up to 256 calls in one attempt.
- `record_llm_call` refuses an empty model id, a model id over 200 bytes, a
  value above `i64::MAX` and a call over the limit. The error converts to
  `String`, so `?` works in an activity.
- An interceptor gets the same context, so it can record the calls of many
  activities in one place.
- `ActivityContext::llm_calls` returns the calls recorded so far. Use it in
  a unit test.

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

The window applies to `recorded_at`. It is the timestamp of the completion
transaction, the same value as the completion event's `timestamp`, so a row
and its event always fall in the same window.
Use `group_by=workflow_name` for the cost per workflow type. Use
`group_by=search_attr:tenant_id` for the cost per tenant. See
[the usage report](sharding.md#historical-per-tenant-usage-report-issue-596).

`harvest usage` shows `LLM_CALLS`, `LLM_IN`, `LLM_OUT` and `LLM_COST_USD`
columns. `--json` prints every field.

The table is plain SQL, so a custom report can read it:

```sql
SELECT model, SUM(input_tokens), SUM(output_tokens), SUM(cost_usd_micros)
  FROM harvest_llm_ledger
 WHERE recorded_at >= NOW() - INTERVAL '30 days'
 GROUP BY model;
```

## 4. The agent adapter

The `agent_model_turn` activity of `autumn-harvest-agent` records one call
per model turn. Implement `AgentModel::model_id` to name the model, and
`AgentModel::cost_usd_micros` to price a call. The defaults are `"unknown"`
and unpriced. See [the agent adapter](agent-adapter.md).

## 5. Lifecycle

- **Retention.** The rows cascade with the execution row.
- **Shard moves.** The rows move with the run. The usage report skips the
  staged target copy before the cutover and the sealed source after it, so
  one shard reports each call.
- **Reset.** A fork copies events, not ledger rows. The cost counts once.
- **Erasure.** Erasure keeps the rows. They hold no payload.

## 6. Limits

- A failed attempt writes no row. A retried call that cost tokens before it
  failed is not counted.
- The SQLite backend does not write the ledger. Its context accepts a
  call and drops it.
- The report has no per-model dimension. Query the table for that.
- Quota checks do not read the ledger.
