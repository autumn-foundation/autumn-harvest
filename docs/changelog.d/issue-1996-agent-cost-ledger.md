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

**Usage report.** Each group of `GET /admin/usage` gains `llm_calls`,
`llm_input_tokens`, `llm_output_tokens`, `llm_cost_usd_micros`,
`llm_unpriced_calls` and `llm_latency_ms`. `group_by=workflow_name` gives
the cost per workflow type. `group_by=search_attr:tenant_id` gives the
cost per tenant. The `harvest usage` table gains `LLM_CALLS`, `LLM_IN`,
`LLM_OUT` and `LLM_COST_USD`.

**Agent adapter.** `agent_model_turn` records one call per model turn.
`AgentModel` gains `model_id()` and `cost_usd_micros(usage)`, with
defaults, so existing models still compile.

**Lifecycle.** The rows cascade with the run on retention and move with
the run on a shard move. The usage report skips the sealed source of a
moved run. A reset fork does not copy them.

**Known limits.** A failed attempt writes no row. The SQLite
backend does not write the ledger. The report has no per-model dimension.
Quota checks do not read the ledger.

**Migration.** `20261009050156_harvest_llm_ledger` adds the table and one
index on `recorded_at`. No data migration, no `WorkflowEvent` variant, no
replay impact. See
[0.8.0 §1.3](../upgrading/0.8.0.md#13-the-usage-report-carries-the-agent-cost-ledger).

**Tests.** `llm_ledger_tests` (worker, local and transactional paths, a
failed attempt, replay, the usage rollup, retention),
`the_llm_ledger_moves_with_the_run`, plus unit tests in `llm_ledger.rs`,
the plugin usage merge, the CLI table and the agent model turn.
