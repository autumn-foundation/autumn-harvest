# DESIGN-2751: Keep the one-row claims on a full index

## Problem

`queue::claim_task_by_id_query` splices `harvest_task_queue.id = $7` into
the full-scan claim. Two scans get the id test: `candidate` and
`concurrency_pending_keys`. Both scans also test
`queue_name = ANY($2) AND state = 'PENDING'`. Several partial indexes have
the predicate `state = 'PENDING'`, such as `idx_harvest_tq_poll`,
`idx_harvest_tq_claim_seek` and `idx_harvest_tq_coverage_sample`.

After an idle analyze, each live partial index looks empty. The plan can
then read such an index and filter on `id`. That path reads every live
entry of the index. A cached generic plan keeps the choice.

## Facts (white hat)

Fixture: the idle fixture of `row_guard_plan_db_tests`. 100k terminal rows,
`VACUUM ANALYZE`, then 2,000 live rows. Each statement is prepared and
explained under the claim settings and the planner defaults, with the
generic and the custom plan.

| Statement | Plan before, both scans | Plan after |
|---|---|---|
| by-id claim, plain | `idx_harvest_tq_claim_seek` | `harvest_task_queue_pkey` |
| by-id claim, fenced | `idx_harvest_tq_claim_seek` | `harvest_task_queue_pkey` |
| by-id claim, owned | `idx_harvest_tq_claim_seek` | `harvest_task_queue_pkey` |
| by-id claim, owned and fenced | `idx_harvest_tq_claim_seek` | `harvest_task_queue_pkey` |
| owner take, plain and fenced | a live-row partial index | `idx_harvest_tq_workflow` |

The hypothesis holds. Each variant leaves its full index in each of the
four plan modes.

The owner take (`owned_claim_by_execution_query`) is the same splice,
keyed by an execution id. It has the same defect, so it is in scope.

A 1M-row fixture gives the cost.
`docs/performance-claim-by-id-guard.md` records it. After an idle analyze,
the by-id claim reads 622 buffers under the generic plan and 3,077 under
the custom plan. After the fix it reads 26 and 28.

## Options (green hat, brainstorming)

1. Splice `row_guard_sql!` around the state test of both by-id scans. This
   is the form of the issue #2713 fix. One anchor covers both scans.
2. Build a second full-scan literal with the guard. This forks the hottest
   statement. Each gate then has two copies.
3. Compare the state with a scalar subquery, as the window scan does. That
   form also hides the state test. The window scan needs it for a TID scan.
   A by-id scan has an id, so the shared `row_guard_sql!` form is clearer.
4. Drop the state test from the by-id scans. Then the claim can take a row
   that is not `PENDING`. That changes the rows.
5. Set `plan_cache_mode = force_custom_plan` for the by-id claim. The custom
   plan also leaves the primary key with idle statistics.
6. Add `id` to a `PENDING` index. That needs a concurrent index build on
   the hottest table. The idle statistics still favor another index.

Decision: option 1, through one splice helper for both one-row claims.

## Reverse brainstorming: how can the fix fail?

| Way to fail | Prevention |
|---|---|
| The guard reaches the window claim, which needs its partial indexes. | Only the one-row splice adds it. A unit test pins the plain state test in the window claim and in a sample of its fenced, fair, kind and owned forms. |
| A future edit renames the anchor, and the guard silently goes away. | The splice asserts that the anchor appears twice. A unit test pins two guards in each variant. The plan test fails on a plain scan. |
| The `CASE` gives NULL where the plain test gives false. | Both are top-level `AND` terms of a `WHERE` clause. The truth-table test covers every input. A differential run of each variant matches the plain form on every seeded row. |
| The row lock rechecks a new row version and skips the `CASE`. | Postgres rechecks every qual of a `FOR UPDATE` scan. `SKIP LOCKED` does not change the quals. |
| The owner take loses its execution index. | The plan test accepts only `idx_harvest_tq_workflow` or the primary key for the take. |
| A splice that relies on the plain text breaks. | The owner and fence splices use other anchors. The shape tests compare each variant to the base with the guard unwrapped. |

## Feelings (red hat)

A guard on a claim scan looks like it hides a gate. The comment on
`BY_ID_STATE_GUARD` says why it is there. The gate list tests unwrap the
guard and find each gate.

## Risks (black hat)

- Semantics. The truth table and the differential run cover it.
- Coverage. The plan test checks six variants in four plan modes, on the
  slot-count fixture and the idle fixture.
- Residual cost. A keyed row still pays for the two running-count scans
  after an idle analyze. They read by key, not by id, and every claim form
  has them. The guard cannot help them, so they need their own issue.

## Benefits (yellow hat)

- A by-id claim reads a fixed number of buffers for any backlog size.
- A cached generic plan cannot keep a backlog-sized index scan.
- One splice helper now serves both one-row claims.

## Scope (blue hat)

In scope:

- The by-id claim in each form: plain, fenced, owned and owned fenced. The
  fair form is the same text as the plain form.
- The owner take, plain and fenced.

Out of scope:

- The window claim and the batch claim. They need their partial indexes.
- The running-count scans and the cap recheck. They read by key.
- The SQLite backend. Its planner has no generic plan cache.

## Plan

1. RED. Add the six variants to `row_guard::guarded_statements`. The plan
   test and the differential test then fail. Add the shape tests.
2. GREEN. Splice the guard into both scans of each one-row claim, through
   one helper.
3. Measure the cost before and after on a 1M-row fixture.
4. REFACTOR. Run the gates.

## Acceptance criteria

1. Each by-id claim variant is prepared and explained under the claim
   settings and `force_generic_plan`, on the idle fixture.
2. Where `candidate` leaves `harvest_task_queue_pkey`, the by-id state
   test is wrapped with `row_guard_sql!`.
3. The variants are in `row_guard::guarded_statements`, so the plan test
   checks them.
4. The guard keeps the same rows, proven by the differential test.
5. A Ledger page records the cost before and after.
