## Phase — Vantage action forms preserve entered data on error (issue #1687)

**Flow audited.** The Vantage workflow detail page's three operator
recovery actions — Send signal, Reset to event N, Trigger update
(`autumn-harvest-plugin/src/ui.rs`). `docs/vantage-ui.md`'s "Diagnosing a
stuck workflow" runbook routes an operator through these for 3 of its 5
documented scenarios (waiting-on-signal, blocked-on-child, replay
non-determinism), so this is an incident-response path, not a peripheral
one.

**Evidence (Tier 1, error-path inventory).** All three POST handlers
redirected to `../../workflows/{id}?flash={error}` on every failure
branch — malformed JSON payload, malformed event number, or the
downstream engine call failing. Each form lives inside a `<details>`
collapsed by default, so the redisplayed page carried only a generic
top-of-page flash: the operator's signal name, JSON payload, reset event
number/reason, or update name/payload were gone, and the three panels
that might hold the mistake were closed again. Four-boolean tabulation:
error adjacent to its cause (✗ — top banner only), persists until
resolved (✓, it's a flash), says how to recover (partial — the raw
serde/parse error), entered data preserved (✗) for all three forms. This
is the same gap the existing `BackfillFormEcho` mechanism on the
schedule backfill launcher was built to close, just not applied here.

**Mechanism.** An operator mid-incident retyping a multi-line JSON
payload or an event number copied from the timeline, on one typo, lands
back on a page that looks unchanged except for a banner, and must recall
and retype everything from scratch across all three collapsed panels.

**Change.** `WorkflowActionEcho` — on a failure, each handler's redirect
now also carries the entered values (`signal_name`/`signal_payload`,
`reset_event`/`reset_reason`, `update_name`/`update_payload`) alongside
the error. `WorkflowDetailParams` reads them back; `render_workflow_detail`
keeps the relevant `<details>` open, pre-fills its inputs with exactly
what was submitted, and renders the error inline next to the field,
instead of a generic top banner. One variable moved (data preservation +
error adjacency for these three forms); no other page, form, or endpoint
touched, and the success-path flash is unchanged.

**Measurement.** Deterministic, re-run in the PR:
`cargo test -p autumn-harvest-plugin --lib ui::tests` — two new tests:
`render_workflow_detail_echoes_entered_values_on_action_form_errors`
(all three panels open, pre-filled, and show their inline error when
`WorkflowActionEcho` carries one) and
`render_workflow_detail_leaves_action_forms_collapsed_with_no_error`
(unchanged behavior — no panel forced open absent an error). No new
`WorkflowEvent` variant, no migration, no behavioral instrumentation — a
Tier 1 error-path defect (entered data lost on a documented incident-
response flow), which clears the impact floor on its own.
