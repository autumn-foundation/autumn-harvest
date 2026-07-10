### Per-execution legal hold (issue #747)

**Before:** an operator could not exempt a single workflow execution's history
from the retention janitor (issue #737) or from targeted PII erasure (issue
#495). A subpoena/litigation hold on one run required disabling retention
globally or racing the janitor.

**After:** `POST /api/harvest/workflows/{id}/legal-hold` places a durable,
per-execution hold that exempts that execution's `harvest_events` history from
retention deletion **and** from PII erasure until it is released
(`POST /workflows/{id}/legal-hold/release`) or auto-expires (optional
`hold_until`). A held execution is skipped as the *first* per-candidate gate in
the retention tick, so `harvest.retention.deleted` never counts a held id; an
erase attempt on a held execution returns `409` naming the active hold.

- Additive nullable columns only (`legal_hold_set_at` / `legal_hold_until` /
  `legal_hold_reason` / `legal_hold_actor`) on `harvest_workflow_executions`
  (migration `20260709000001_harvest_legal_hold`), plus a partial discovery
  index. **No new `WorkflowEvent` variant, no replay impact, shard-local.**
- Active-hold predicate is single-sourced in `legal_hold_active(set_at, until,
  now)` (re-exported from the crate root), shared by the retention gate, the
  erase gate, the describe/list surfaces, and the set/release core functions.
- Idempotent: re-holding an actively-held execution preserves provenance
  (`newly_held: false`); releasing an unheld execution is a `200` no-op
  (`released: false`).
- `GET /workflows/{id}` surfaces a derived `legal_hold` boolean (plus the four
  raw columns); `GET /workflows?legal_hold=true` filters to actively-held runs.
- Admin-only, audited under `legal_hold.set` / `legal_hold.release`.
- CLI: `harvest legal-hold set <id> --reason <r> [--until <rfc3339>]` and
  `harvest legal-hold release <id>`.
