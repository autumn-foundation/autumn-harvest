## Phase 5.x — Vantage jump-to-event label association (issue #1485)

**Flow audited.** The Vantage operator dashboard's workflow detail page
(`autumn-harvest-plugin/src/ui.rs`), specifically the "Jump to event"
control in the event timeline pagination. It only renders once a
workflow's history exceeds `DETAIL_EVENT_PAGE_SIZE` (100 events) — a
long-running or heavily-retried workflow is exactly the kind an operator
is investigating via the runbook's five stuck-workflow scenarios
(`docs/vantage-ui.md`), so this is exactly when the control appears.

**Evidence (Tier 1, deterministic).** A new audit,
`docs/audits/vantage-label-association.py`, checks every maud `<label>`
render site in `ui.rs` for a real HTML association with its control —
either `for="…"` (never used anywhere in this file) or wrapping the
control as a descendant (the convention every other label in the file
uses). Baseline: 47 of 48 sites correct; the "Jump to event" label was
the one exception, rendered as a sibling of its `<input>` rather than a
parent, inside the same `<form>`.

**Mechanism.** A sibling `<label>` with no `for`/`id` has zero
programmatic association with its control. A screen reader announces the
input with no accessible name — "edit text, number, blank" — while every
other field on the same page, including the three panels right above it
(Send signal, Reset to event N, Trigger update), announces correctly.

**Change.** Wrapped the `input` inside its `<label>` (`ui.rs:5360-5366`),
matching the file's own 100%-consistent convention. Added
`display:inline-flex;align-items:center;gap:6px` on the label so the
visual layout is unchanged. No copy, color, or endpoint change.

**Measurement.** Deterministic, re-run in the PR:
`docs/audits/vantage-label-association.py` — 47/48 → 48/48.
`vantage-dashboard-contrast.py`, `vantage-flash-live-region.py`, and
`vantage-degraded-banner-focus.py` — all unaffected.
`cargo test -p autumn-harvest-plugin --lib ui::tests` — 233 passed, 0
failed (1 new, asserting the input is a structural descendant of its
label). No `WorkflowEvent` variant, no migration, no behavioral
instrumentation — a Tier 1 accessibility defect (missing accessible
name), which clears the impact floor on its own.
