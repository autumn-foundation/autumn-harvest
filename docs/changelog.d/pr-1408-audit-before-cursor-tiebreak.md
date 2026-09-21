## Fix — `audit::list_audit`'s `before` cursor keeps tied rows (issue #1408)

`list_audit` (`autumn-harvest/src/audit.rs`) ordered strictly by
`occurred_at DESC` and filtered `before` with `occurred_at.lt(before)` — a
single-column, exclusive cursor with no tiebreaker. A caller paging
`GET /admin/audit` by setting each request's `before` to the last row's
`occurred_at` permanently dropped any other row sharing that exact
timestamp.

PR #1407 added `audit::insert_audit_batch`: every row in one batched
`INSERT` shares one `occurred_at` (single transaction, single `NOW()`), by
design. Before that PR, sequential single-row inserts almost always got
distinct microsecond timestamps, so a collision was rare. After it, any
bulk action past the endpoint's 500-row page limit produces 500+ rows tied
on one timestamp, and a client paging by cursor deterministically loses
rows at the boundary.

Fix: `list_audit` now orders by `(occurred_at, id) DESC`, and `AuditFilters`
gains `before_id: Option<Uuid>`. A caller sends `before_id` alongside
`before` — the last row's id from the prior page — and the cursor breaks
the tie by `id`: `occurred_at < before OR (occurred_at = before AND id <
before_id)`. `id` is the table's `UUID` primary key, so it is a stable,
always-unique tiebreaker; no schema change. A caller that still sends
`before` alone keeps the pre-existing single-column cursor and its known
gap — no in-repo caller (Vantage UI, CLI) chains `before` today, so nothing
changes behavior for an existing consumer.

`GET /admin/audit` gained a matching `before_id` query parameter, wired
into `AuditFilters`. `docs/api-contract.json` documents both parameters;
`docs/openapi.json` and `autumn-harvest-plugin/openapi.json` were
regenerated with `scripts/regenerate-openapi.sh`.

No new `WorkflowEvent` variant, no migration. `id` is already the table's
primary key, so the new tiebreak needs no new index.

Tests (`autumn-harvest/tests/integration/audit_tests.rs`, integration, real
Postgres):
- `audit_list_before_only_cursor_can_drop_a_tied_row` — pins the pre-existing
  gap: three rows inserted via `insert_audit_batch` share one `occurred_at`;
  paging with `before` alone loses one of them.
- `audit_list_before_id_cursor_walks_every_tied_row_once` — the same three
  tied rows, paged with `before` + `before_id`: every row is returned
  exactly once, none dropped, none duplicated.
