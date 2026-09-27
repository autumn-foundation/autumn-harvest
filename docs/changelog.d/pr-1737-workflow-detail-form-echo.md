## Phase — Vantage workflow-detail forms preserve entered input on failure (issue #1737)

🧭 Wayfinder finding, error-path inventory (tier 1). The workflow detail
page's three operator-input forms — Send signal, Reset to event N, Trigger
update — are `<details>` disclosures, collapsed by default, each containing
a POST form (`autumn-harvest-plugin/src/ui.rs`). On any submission failure
(invalid JSON payload, non-numeric event number, unknown signal, reset-point
validation error, update rejected), the handlers redirected to
`../../workflows/{id}?flash={error}`. The redirect's flash string had no
slot for the typed values, so the fresh `GET` re-rendered the page with the
panel collapsed again and `signal_name`/`payload`/`reset_to_event_id`/
`reason`/`update_name` all empty — the same mechanism issue #1723 fixed for
the DAG retry confirm form, recurring on this page's three forms.

Baseline (four-boolean error-path table, 3 of 3 forms):

| Form | Adjacent to cause | Persists until resolved | Says how to recover | Entered data preserved |
|---|---|---|---|---|
| Send signal | No | Yes | Partial | No |
| Reset to event N | No | Yes | Partial | No |
| Trigger update | No | Yes | Partial | No |

Change: on a genuine failure, each handler (`signal_workflow_ui`,
`reset_workflow_ui`, `trigger_update_ui`) now renders the workflow detail
page directly instead of redirecting, through a new shared
`render_workflow_detail_page` (the `workflow_detail_ui` `GET` handler's body,
extracted so both call sites load the same data). A new `WorkflowFormEcho`
carries the failing form's submitted values and error message into
`render_workflow_detail`, which re-opens that one `<details>` panel
(`open[...]`), re-populates its fields (`value=[...]`/echoed textarea
content), and shows the error inline next to the field
(`span.field-error role="alert"`) rather than only in the page-top flash.
The plain `GET` passes an empty `WorkflowFormEcho`, so unaffected page loads
are byte-for-byte unchanged.

Deliberately not done: no new endpoint, no schema change, no restyle — one
mechanism (render in place with the operator's input intact) applied
uniformly to the one page's three affected forms, mirroring #1723 rather
than introducing a second pattern (e.g. round-tripping values through the
redirect query string, which #1723 rejected for putting operator-typed
`reason`/payload text in the browser history and server access logs).

After (same table, target and actual):

| Form | Adjacent to cause | Persists until resolved | Says how to recover | Entered data preserved |
|---|---|---|---|---|
| Send signal | Yes | Yes | Yes | Yes |
| Reset to event N | Yes | Yes | Yes | Yes |
| Trigger update | Yes | Yes | Yes | Yes |

Broken error paths: 3 → 0.

Test evidence: `autumn-harvest-plugin/src/ui.rs` unit tests assert that a
failed submission on each of the three forms re-opens the correct panel with
the submitted values intact and the error rendered inline
(`signal_form_echoes_submitted_values_and_error_on_failure`,
`reset_form_echoes_submitted_values_and_error_on_failure`,
`trigger_update_form_echoes_submitted_values_and_error_on_invalid_json`,
`trigger_update_form_echoes_submitted_values_and_error_on_rejection`), and
that the plain `GET` path is unaffected
(`render_detail_form_panels_collapsed_and_empty_by_default`). Manual
keyboard/screen-reader walkthrough: `open[...]` on `<details>` is a native
HTML mechanism, so no new script or ARIA wiring is needed for the reopened
panel to be reachable and announced the same way an operator-expanded panel
already was.
