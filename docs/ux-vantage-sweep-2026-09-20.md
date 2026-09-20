# Vantage dashboard sweep — Workers/DLQ/Schedules `refresh` (broken error paths 3→0)

This is a Wayfinder pass over Vantage, the embedded operator dashboard
(`docs/vantage-ui.md`) — the only UI this product ships, and the surface
every prior Wayfinder PR on this repo has targeted (#1333/#1356/#1378/#1420/
#1437/#1509/#1540/#1560/#1588/#1619/#1630/#1641). Every one of those closed
the same defect class: a `Query<..>` extractor typed directly as a number
(`page: i64`, `node: usize`, …) fails axum's query deserialization on
non-numeric text with a bare, unstyled 400 before the handler runs at all —
aborting the whole page, filters and all, instead of degrading the one bad
field. #1641's own closing note stated that class fully closed in `ui.rs`.

**Outcome: fix shipped, 3 pages.** This pass set out to re-verify that
closure. The first draft of this note wrongly concluded the class stayed
closed and stopped there — it checked `page`/`limit`/`count`/`node` on each
`Query<..>` struct but did not check every field, and missed that
`WorkerListParams`, `DeadLetterListParams`, and `ScheduleListParams` each
still had `refresh: Option<u64>`. Codex's review on the resulting PR (#1665)
caught the gap before it merged, naming all three fields and their line
numbers. This revision fixes them and records the correction alongside the
original candidate this pass rejected (`lift_gate_ui`'s `Path<uuid::Uuid>`,
below), so the false-negative and its cause are both on the record.

## 🎯 Flow

Vantage is 100% of this product's UI surface — every operator interaction
that isn't the CLI or a raw `curl` to the management API goes through it.
`/workers`, `/dead-letters`, and `/schedules` are three of its five list
pages, each with the auto-refresh interval documented in `docs/vantage-ui.md`
("Auto-refresh"): `?refresh=30` or `?refresh=60`, or a hand-typed custom
value, on a URL an operator bookmarks or shares while triaging.

## 📈 Evidence

**Tier 1, re-run, all pre-existing and unaffected:**

| Check | Before | After |
|---|---|---|
| `vantage-dashboard-contrast.py` | 135 pairs, 0 failing | 135 pairs, 0 failing |
| `vantage-label-association.py` | 48/48 compliant | 48/48 compliant |
| `vantage-flash-live-region.py` | 7/7 compliant | 7/7 compliant |
| `vantage-degraded-banner-focus.py` | 6/6 compliant | 6/6 compliant |

**Tier 1, error-path extractor audit — corrected:**

Re-inspected every field (not just the ones named in prior PR titles) of
every `#[derive(Deserialize)]` struct used as a `Query<..>` extractor in
`ui.rs`. `WorkerListParams::refresh`, `DeadLetterListParams::refresh`, and
`ScheduleListParams::refresh` were still `Option<u64>` — the DAG detail page
(#1630) fixed `refresh` on its own params struct, but that fix was never
carried to the three sibling list pages that also take a `refresh` query
param. A non-numeric value on any of the three (`/workers?refresh=abc`,
`/dead-letters?refresh=abc`, `/schedules?refresh=abc`) failed axum's query
deserialization with a bare 400 before the handler ran, discarding every
other filter already on the URL — 3 of 3 broken failure modes, the exact
mechanism every fixed sibling page shares.

All other `Query<..>` extractor structs (`DagDetailParams`,
`DagRetryConfirmParams`, `WorkflowListParams`, `WorkflowDetailParams`,
`BuildRoutingListParams`, `SchedulePreviewUiParams`, `ScheduleRunsUiParams`)
were re-checked field by field and hold no further instances.

## 💡 Hypothesis

Same mechanism as every fixed sibling page, stated explicitly in each of
their own PRs: an operator bookmarks, shares, or hand-edits a URL carrying
`?refresh=30`, and later mistypes it (`?refresh=3o`, a stray character from
copy-paste, or a stale link from before this field existed). The three
pages this note fixes are exactly the pages `docs/vantage-ui.md`'s
"Auto-refresh" section documents as taking this parameter — Workers, DLQ,
and Schedules — so the reachable surface is not hypothetical.

## 🔧 Change (one mechanism, three pages)

`refresh` is now `String`, not `u64`, on `WorkerListParams`,
`DeadLetterListParams`, and `ScheduleListParams`. Each handler
(`list_workers_ui`, `list_dead_letters_ui`, `list_schedules_ui`) calls
`parse_refresh_query_field` — the DAG detail page's own parser (#1630),
reused rather than duplicated — which falls back to auto-refresh disabled
on a non-numeric value and returns an error message to render inline.

- **Workers**: no form field backs `refresh` on this page (same as DAG
  detail's own `node`/`refresh`), so the error renders as a page-level
  `span.field-error role="alert"` under the `h2`.
- **DLQ and Schedules**: both already have a `<select name="refresh">`
  control (`render_dead_letter_filters`, `render_schedule_filters`); the
  error now renders next to it, matching the existing `limit_error`
  placement pattern on the same forms.

All three call sites thread the already-parsed `Option<u64>` through
unchanged to the existing render helpers that build query strings and the
`<meta http-equiv="refresh">` tag — only the top-level extractor field and
the three handlers changed; no render helper's signature needed to widen
beyond adding the one new `_error` parameter.

## 📊 Measurement

Impact-floor check: this clears "broken error paths N→0" the same way
every sibling PR in this series has — 3 reachable, bookmarkable-URL failure
modes closed, matching the mechanism and evidence bar #1333 through #1641
all used. Deterministic claim, no review date needed.

| Check | Before | After |
|---|---|---|
| `/workers?refresh=not-a-number` | bare 400, page aborts, `build_id` filter discarded | 200, renders, filter preserved, error shown |
| `/dead-letters?refresh=not-a-number` | bare 400, page aborts, `workflow_name` filter discarded | 200, renders, filter preserved, error next to Refresh field |
| `/schedules?refresh=not-a-number` | bare 400, page aborts, `target` filter discarded | 200, renders, filter preserved, error next to Refresh field |
| Broken error paths on these three pages' `refresh` field | 3 | 0 |
| `vantage-dashboard-contrast.py` / `-label-association.py` / `-flash-live-region.py` / `-degraded-banner-focus.py` | all clean | unaffected |
| `cargo check -p autumn-harvest-plugin --lib` | — | clean |
| `cargo clippy -p autumn-harvest-plugin --lib -- -D warnings` | — | clean |
| `cargo fmt --all -- --check` | — | clean |
| `cargo test -p autumn-harvest-plugin --lib ui::tests` | 289 passed | 289 passed (unaffected; no new pure-parser tests — `parse_refresh_query_field` is reused, not new) |
| `cargo check -p autumn-harvest-plugin --test ui_integration` | — | clean |

New integration tests (type-checked; not executed here — this sandbox has a
local Postgres cluster but no Docker daemon, and every `ui_integration.rs`
test spins a testcontainer directly, matching every prior PR touching this
file): `ui_workers_invalid_refresh_redisplays_page_instead_of_aborting`,
`ui_dead_letters_invalid_refresh_redisplays_page_instead_of_aborting`,
`ui_schedules_invalid_refresh_redisplays_page_instead_of_aborting`.

## Rejected candidate: `lift_gate_ui`'s `Path<uuid::Uuid>`

This pass also extended the inventory to `Path<..>` extractors (21 call
sites), a dimension #1641's closing note didn't name. 20 of 21 take
`Path<String>` (or `Path<(String, String)>`) and resolve the id manually,
surfacing `AutumnError::bad_request_msg` on a malformed value. One outlier:
`lift_gate_ui` (`ui.rs`) takes `Path<uuid::Uuid>` directly, so a malformed
id there hits axum's bare framework rejection before the handler runs,
structurally identical to the fixed defect class.

Rejected against the impact floor. Every fixed `Query<..>`/`Path<..>` site
— `refresh` included — shares a mechanism stated explicitly in its own PR:
an operator bookmarks, shares, or hand-edits a URL carrying that field, a
normal way to reach a GET page in this dashboard. `lift_gate_ui` doesn't
share that mechanism: it is a `POST`-only action reachable through exactly
one path, the "Lift" button's own server-rendered
`<form action="gates/{id}/lift">`, where `{id}` is `row.id.to_string()` — a
real UUID read straight from `harvest_admission_gates`, never
operator-typed, never a bookmarkable GET, never referenced in any runbook
or doc as a URL to construct by hand (checked: no hit for
`gates/{id}/lift` outside `ui.rs` itself). No PR ships for this one. Left
here, not silently dropped, so a future pass doesn't re-derive the same
dead end — or re-open it if a real entry point is ever added (e.g. a gate
id surfaced in an external alert an operator might paste into a URL).

## Lesson for the next pass

Checking a struct "is fixed" by grep-matching the field names a prior PR's
title mentioned (`page`, `limit`, `count`, `node`) is not the same as
checking every field of that struct. The next Wayfinder sweep over this
file should diff each `Query<..>`/`Path<..>` struct's full field list
against its own render call sites, not spot-check named fields.

## 🔬 Reproduce

```bash
cargo check -p autumn-harvest-plugin --lib
cargo clippy -p autumn-harvest-plugin --lib -- -D warnings
cargo fmt --all -- --check
cargo test -p autumn-harvest-plugin --lib ui::tests
cargo check -p autumn-harvest-plugin --test ui_integration
python3 docs/audits/vantage-dashboard-contrast.py
python3 docs/audits/vantage-label-association.py
python3 docs/audits/vantage-flash-live-region.py
python3 docs/audits/vantage-degraded-banner-focus.py
python3 docs/audits/comment-hygiene.py --base origin/trunk-dev

# Query<..>/Path<..> extractor inventory:
grep -n "Query<" autumn-harvest-plugin/src/ui.rs
grep -n "Path<" autumn-harvest-plugin/src/ui.rs | grep -v "Path<String>\|Path<(String, String)>"

# Manual-entry-point check for the rejected candidate:
grep -rn "gates/{id}/lift" --include=*.rs --include=*.md .
```
