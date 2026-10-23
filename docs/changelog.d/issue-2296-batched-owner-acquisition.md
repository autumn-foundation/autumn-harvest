## Perf — Batched owner acquisition (issue #2296)

`OwnerLoop` now takes the partitions of a scan in one transaction,
`decision_owner::acquire_many`. Before, each partition was its own
transaction of 6 statements. A join of 512 partitions sent 3,072 statements.
Now the statement count of a scan does not grow with its picks.

- The batch takes the DR generation row first. Then it locks the takeable
  owner rows `FOR UPDATE SKIP LOCKED`, in ascending partition order.
- It adds 1 to the epoch of each locked row only. A row that a commit holds
  is skipped and listed as busy. So the drain of ADR 0009 decision 5 holds.
- A batch never waits for an owner row. One busy row does not delay the rest.
- `acquire` keeps its single-row form, which waits. Tests and
  `decision_fence_bench` use it.
- No migration. No new `WorkflowEvent` variant. No `harvest_events` write.

`pg_stat_statements` on Postgres 16.15, one fresh database per run, 512
acquisitions on one connection:

| Path | Statements | Buffers | WAL bytes |
|:--|--:|--:|--:|
| `acquire` per partition | 3,072 | 6,120 | 151,354 |
| `acquire_many`, chunks of 32 (the worker default) | 96 | 3,443 | 128,431 |
| `acquire_many`, one batch of 512 | 6 | 4,205 | 159,275 |

The issue counted 5 statements per partition. Issue #2254 later added the
idle bound, so the count is now 6.

Evidence:

- `decision_owner_db_tests`: one transaction writes each row of a batch
  (one `xmin`); held and busy rows are skipped with no wait; a stale owner is
  fenced after a batched takeover.
- `decision_owner_claim_tests`: a joiner takes each chunk in one transaction;
  a batch that names a missing row takes no row.
- `cross_region_dr_tests`: a fenced region takes no row of a batch, and its
  owner loop adopts nothing; a batch that waits out a bump is all busy.
- `decision_owner_lock_modes_db_tests`: `FOR UPDATE SKIP LOCKED` skips a row
  that a commit holds, with no wait.
- `decision_owner_sim_db_tests`: the owner simulator passes with batched
  acquisitions.
- Verus, `owner_fence.rs`: a batch moves each locked row by one acquisition
  and leaves each other row. A row that a commit holds keeps its epoch, so
  the commit still passes its fence.
