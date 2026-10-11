## Engine — agent cost ledger (issue #1996)

An activity can now record each LLM call:

```rust
ctx.record_llm_call(LlmCall::new("claude-sonnet-5-5", 1_200, 340)
    .with_cost_usd_micros(8_700)
    .with_latency(elapsed))?;
```

The engine writes the model id, the tokens, the cost and the latency to the
new `harvest_llm_ledger` table. The write is in the transaction that appends
the completion event: `ActivityCompleted`, `LocalActivityCompleted`, or the
`run_transactional` commit. A row holds the `event_id` of that event.

**Outside the encrypted payload.** `AeadCodec` encrypts the completion
`output`, so SQL could not sum figures inside it. The ledger is a side
table in clear. History does not change, so replay does not change. A test
runs a workflow with and without a ledger call, then compares the decoded
histories and replays both. `docs/security-posture.md` names the clear
fields.

**Checks.** A model id is a token of 1 to 200 bytes: ASCII letters, digits
and `._:/@+-`. Tokens, cost and latency have upper bounds. A refused call is
a non-retryable activity failure, so `?` does not call the model again.

**Usage report.** Each group of `GET /admin/usage` gains `llm_calls`,
`llm_input_tokens`, `llm_output_tokens`, `llm_cost_usd_micros`,
`llm_unpriced_calls` and `llm_latency_ms`. `group_by=workflow_name` gives
the cost per workflow type. `group_by=search_attr:tenant_id` gives the
cost per tenant. The `harvest usage` table gains `LLM_CALLS`, `LLM_IN`,
`LLM_OUT`, `LLM_COST_USD` and `LLM_UNPRICED`.

**Agent adapter.** `agent_model_turn` records one call per model turn.
`AgentModel` gains `model_id()` and `cost_usd_micros(usage)`, with
defaults, so existing models still compile.

**Lifecycle.** The rows cascade with the run on retention. They move with
the run on a shard move. The usage report counts a moving run on one shard
in every phase. A reset fork does not copy the rows. `harvest preflight`
checks the grants on the table.

**Known limits.**

- A failed attempt writes no row. Neither does an attempt that loses its
  lease or returns a result over the cap.
- The SQLite backend does not write the ledger.
- The ledger has no columns for cached prompt tokens.
- The report has no per-model dimension.
- Quota checks do not read the ledger.

**Migration.** `20261009050156_harvest_llm_ledger` adds the table and one
index on `recorded_at`. No data migration, no `WorkflowEvent` variant, no
replay impact. See
[0.8.0 §1.5](../upgrading/0.8.0.md#15-the-usage-report-carries-the-agent-cost-ledger).

**Also.** The `GET /admin/usage` contract text said that terminal counts
use `completed_at`. It now says that they count terminal events, as the
code already does.

**Tests.**

- `llm_ledger_tests`: the worker, local and transactional paths; a failed
  attempt; a local retry; a call after the commit; an oversized result; a
  refused call; replay; the usage rollup, an aged run and shard-move phases;
  retention.
- `the_llm_ledger_moves_with_the_run` in the shard rebalance suite.
- Unit tests in `llm_ledger.rs`, the plugin usage merge, the CLI table,
  preflight and the agent model turn.
