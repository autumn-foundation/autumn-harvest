## Ledger: the tenant LLM spend read reads the index only (issue #1997)

`llm_budget::load_llm_spend` runs before each LLM step of a type with a
tenant LLM cap. Its tenant sum read every ledger row of each in-window run
from the heap, through the primary key, and then dropped the rows outside
the window. The ledger takes rows in time order from many runs, so the read
touched about one heap page per row.

- **Migration `20261120174554_harvest_llm_ledger_run_recorded_index`.** One
  covering index, `harvest_llm_ledger_run_recorded`, on
  `(workflow_exec_id, recorded_at) INCLUDE (input_tokens, output_tokens,
  cost_usd_micros)`. Guarded build: an operator can prebuild it
  `CONCURRENTLY`. The plain build takes `SHARE` on `harvest_llm_ledger`.
  No query text change, no `WorkflowEvent` variant, no replay impact.
- **Measurement.** On a 6.26M-row ledger with a cold visibility map, a read
  of a large tenant takes 34,833 → 16,915 buffers (−51%), and 3,024 after a
  vacuum (−91%). Each ledger insert writes about 165 more WAL bytes (+37%).
  `docs/performance-llm-spend-tenant-window.md` has the full record.
- **Harness.** `cargo xtask perf llm-spend-tenant-window` runs
  `llm_spend_tenant_window_perf::zz_capture_llm_spend_tenant_window_evidence`.
  `llm_spend_covering_index_does_not_change_the_spend` always runs and
  proves that the index changes no spend on the window edge cases.
  `LLM_SPEND_SQL` is now public, so the harness explains the exact text.
