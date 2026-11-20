## Phase X.Y — One-row claims stay on a full index (issue #2751)

The by-id claim tests `state = 'PENDING'` in its `candidate` and
`concurrency_pending_keys` scans. After an idle analyze, the plan read a
`PENDING` partial index and filtered on `id`, so its cost grew with the
live backlog. The owner take had the same defect.

- `splice_one_row_predicates` adds the id test of each one-row claim. It
  wraps the state test of both scans with `row_guard_sql!`. The by-id claim
  then reads the primary key, and the owner take `idx_harvest_tq_workflow`.
- The window claim keeps the plain state test, because it needs its
  partial indexes. A unit test pins this.
- `row_guard::guarded_statements` lists the by-id claim in each form and
  the owner take. The plan test checks each one in four plan modes.
- No migration, no new event variant, no change to `harvest_events`.
- Evidence: `row_guard_plan_db_tests` fails on the parent commit for each
  variant. Its differential test proves that both forms change the same
  rows. On a 1M-row fixture with idle statistics, the by-id claim reads
  622 → 26 buffers, and the owner take 3,610 → 33. See
  `docs/performance-claim-by-id-guard.md`.
- A keyed row still pays for the two running-count scans after an idle
  analyze. They read by key, so they need their own fix.
