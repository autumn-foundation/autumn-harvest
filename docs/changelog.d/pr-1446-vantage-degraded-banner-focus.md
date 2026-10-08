## Phase 5.x — Vantage degraded-banner focus-on-load (issue #1446)

**Flow audited.** The Vantage operator dashboard's `div.degraded-banner` render
sites (schedule fire-time preview, schedule run history's cross-shard
degradation banners, and the backfill launcher's error/warning banners) — the
dashboard is the product's stated no-separate-service alternative to querying
Postgres by hand for debugging failed workflows (`docs/plans/vantage-spec-dashboard-ui.md`),
so a message an operator cannot perceive defeats the page's own purpose.

**Evidence (Tier 1, deterministic).** A new audit,
`docs/audits/vantage-degraded-banner-focus.py`, modeled on the existing
`vantage-flash-live-region.py`, found all 6 `div.degraded-banner` render
sites in `autumn-harvest-plugin/src/ui.rs` carrying `role="status"` but
missing `tabindex="-1"` + `autofocus` — the baseline was 0 of 6 passing.
`div.flash` on the same file already carries both halves at all 7 of its
sites; `degraded-banner` was the one status-message class the existing
audit's `class` match does not see.

**Mechanism.** Vantage is a traditional server-rendered multi-page app: every
degraded-banner is present in the initial HTML of a full page load (a GET,
or the page a redirected POST lands on), not a client-side DOM mutation. An
ARIA live-region role only announces content that changes *after* the
accessibility tree is built; it does not announce content already present
when the page loads (WAI-ARIA APG, "Live Region Perceivable Author
Practices"). Without programmatic focus, a screen-reader operator gets no
signal that "Backfill not started", that a schedule's fire-time preview is
empty because it is paused or exhausted, or that a run-history page's shard
read came back partial or unavailable — exactly the failures this page
exists to surface (the run-history site carries its own comment: "a partial
cross-shard answer is always visible, never silently truncated data").

**Change.** Added `tabindex="-1" autofocus` to all 6 `div.degraded-banner`
render sites, matching the mechanism already used at every `div.flash` site.
No other markup, layout, or copy changed.

**Measurement.** Deterministic, re-run in the PR:
`docs/audits/vantage-degraded-banner-focus.py` — 0/6 → 6/6.
`docs/audits/vantage-flash-live-region.py` — unaffected, still 7/7.
`docs/audits/vantage-dashboard-contrast.py` — unaffected, still clean.
No `WorkflowEvent` variant, no migration, no behavioral instrumentation —
a Tier 1 accessibility defect (focus loss / missed live-region content),
which clears the impact floor on its own.
