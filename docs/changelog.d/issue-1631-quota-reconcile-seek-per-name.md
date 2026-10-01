## Fix — quota reconcile scan reads rows per registered name (issue #1631)

The `quota_key` reconcile sweep fetches up to `batch_size` candidate rows
per tick. The old query filtered `workflow_name = ANY($1)` after an
`(id)` index scan. Postgres read every unrelated `quota_key IS NULL` row
to find the matches. Once a registered type had a large resolved history,
cost grew by about one page per unrelated row. The issue measured 301,129
buffers and 251 ms per tick at 300,000 such rows.

The query now seeks once per registered name. A `LATERAL` subquery reads
the first `batch_size` candidates of one name in `id` order. The outer
query merges the rows and keeps the first `batch_size`. The result equals
the old global `id` order, so the keyset cursor and its anti-starvation
guarantee do not change.

A tick now reads at most `batch_size` rows per registered name. With many
registered types that each hold a full backlog, the total is names times
`batch_size`. That total does not depend on unrelated rows. The bound
test fixture at 40,000 unrelated rows dropped from 40,019 buffers to 171.

Migration `20261001192155_harvest_quota_reconcile_name_id_index` adds
`idx_harvest_we_quota_reconcile_name_id` on `(workflow_name, id)` with the
old partial predicate. It drops `idx_harvest_we_quota_reconcile_candidates`.
Nothing else read that index. The write cost per row stays the same: one
partial index.

No `WorkflowEvent` variant, no data change, no replay impact.

Tests: `quota_reconcile_candidate_bound_tests.rs`. One test caps the pages
read at two noise sizes. One test checks that merged batches keep global
`id` order across two registered types.
