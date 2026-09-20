# Vantage dashboard sweep — no new defect clears the floor (negative result)

This is a Wayfinder pass over Vantage, the embedded operator dashboard
(`docs/vantage-ui.md`) — the only UI this product ships, and the surface
every prior Wayfinder PR on this repo has targeted (#1333/#1356/#1378/#1420/
#1437/#1509/#1540/#1560/#1588/#1619/#1630/#1641). Every one of those closed
the same defect class: a `Query<..>` extractor typed directly as a number
(`page: i64`, `node: usize`, …) fails axum's query deserialization on
non-numeric text with a bare, unstyled 400 before the handler runs at all —
aborting the whole page, filters and all, instead of degrading the one bad
field. #1641's own closing note stated that class fully closed in `ui.rs`.

**Outcome: no PR.** This pass re-verified that closure, extended the same
inventory to the one dimension it didn't cover (`Path<..>` extractors), found
one outlier, and rejected it against the impact floor. No code changes ship
from this note.

## 🎯 Flow

Vantage is 100% of this product's UI surface — every operator interaction
that isn't the CLI or a raw `curl` to the management API goes through it.
This pass covers the dashboard broadly (all routes in `autumn-harvest-plugin/
src/ui.rs`) rather than one flow within it, since the defect class under
test (an extractor that aborts a page instead of degrading) is a
router-level property, not specific to one page's business logic.

## 📈 Evidence

**Tier 1, re-run, all pre-existing and unaffected:**

| Check | Before | After |
|---|---|---|
| `vantage-dashboard-contrast.py` | 135 pairs, 0 failing | 135 pairs, 0 failing |
| `vantage-label-association.py` | 48/48 compliant | 48/48 compliant |
| `vantage-flash-live-region.py` | 7/7 compliant | 7/7 compliant |
| `vantage-degraded-banner-focus.py` | 6/6 compliant | 6/6 compliant |

**Tier 1, new inventory this pass — error-path extractor audit:**

Every `#[derive(Deserialize)]` struct used as a `Query<..>` extractor in
`ui.rs` (11 call sites: `DagDetailParams`, `DagRetryConfirmParams`,
`WorkflowListParams`, `WorkflowDetailParams`, `DeadLetterListParams`,
`WorkerListParams`, `BuildRoutingListParams`, `ScheduleListParams`,
`SchedulePreviewUiParams`, `ScheduleRunsUiParams`) now types every
list/pagination/numeric field as `String` with a graceful-fallback parser,
confirmed by direct inspection — the closed class stays closed.

Extended the inventory to `Path<..>` extractors (21 call sites), a dimension
#1641's closing note didn't name. 20 of 21 take `Path<String>` (or
`Path<(String, String)>`) and resolve the id manually, surfacing
`AutumnError::bad_request_msg` on a malformed value — same contract as the
fixed `Query<..>` sites, just on the id segment instead of a filter. One
outlier: `lift_gate_ui` (`ui.rs:11709`) takes `Path<uuid::Uuid>` directly, so
a malformed id there hits axum's bare framework rejection before the handler
runs, structurally identical to the fixed defect class.

## 💡 Hypothesis considered, and rejected

Every fixed `Query<..>` site shares a mechanism stated explicitly in its own
PR: an operator bookmarks, shares, or hand-edits a URL carrying that field
(`?page=`, `?node=`, `?count=`, a jump-to-event link) — a normal, expected
way to reach a GET page in this dashboard — and later mistypes it.

`lift_gate_ui` doesn't share that mechanism. It is a `POST`-only action
reachable through exactly one path: the "Lift" button's own server-rendered
`<form action="gates/{id}/lift">`, where `{id}` is `row.id.to_string()` — a
real UUID read straight from `harvest_admission_gates`, never operator-typed,
never a bookmarkable GET, never referenced in any runbook or doc as a URL to
construct by hand (checked: no hit for `gates/{id}/lift` outside `ui.rs`
itself). There is no operator session in which a malformed value reaches
this route. Without that, "broken error path" is true only in the sense that
unreachable dead code is true — it doesn't clear this agent's hard gate #4
(a mechanism specific enough to be wrong): there's no failure story to be
wrong about.

## 🔧 Change

None. `lift_gate_ui` is unchanged. Fixing the `Path<String>` inconsistency
purely for its own sake, with no reachable failure behind it, is exactly the
"churn that moves no counter" this agent's own rules reject.

## 📊 Measurement

Impact-floor check: zero admissible evidence clears any of the five floor
conditions (axe/keyboard-trap count, ≥20% cost reduction, a reproduced
usability stumble, a ≥10%-of-losses drop-off/ticket category, or a lab
vital crossing threshold) for the one candidate this pass found. No
behavioral claim, so no review date to set.

## Verdict

**No PR.** The recurring `Query<..>` defect class stays closed; the
`Path<..>` dimension is now inventoried too, with one intentionally-unfixed
outlier recorded here so a future pass doesn't re-derive the same dead end —
or re-open it if a real entry point to `lift_gate_ui` is ever added (e.g. a
gate id surfaced in an external alert or log line an operator might paste
into a URL by hand), which would flip the mechanism argument above.

## 🔬 Reproduce

```bash
python3 docs/audits/vantage-dashboard-contrast.py
python3 docs/audits/vantage-label-association.py
python3 docs/audits/vantage-flash-live-region.py
python3 docs/audits/vantage-degraded-banner-focus.py

# Query<..> extractor inventory:
grep -n "Query<" autumn-harvest-plugin/src/ui.rs

# Path<..> extractor inventory, filtered to non-String types:
grep -n "Path<" autumn-harvest-plugin/src/ui.rs | grep -v "Path<String>\|Path<(String, String)>"

# Manual-entry-point check for the one outlier found:
grep -rn "gates/{id}/lift" --include=*.rs --include=*.md .
```
