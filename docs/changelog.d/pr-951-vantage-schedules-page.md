## Phase 5.x — Vantage schedules management page (issue #951)

Every schedule capability an operator needs already existed as an API — list, fire-time
preview (#348), backfill (#337), per-schedule run history (#534/#762), catchup policy
state (#484), bounded runs (#478/#543), jitter (#240), overlap policy (#241),
pause/resume — but at 3 a.m. the question "did the nightly billing schedule fire, and
if not, is it paused, exhausted, or wedged?" still meant a curl session against five
endpoints and a mental join. Vantage had a schedules *list*; it had none of the state
that answers "why didn't it fire?" and no drill-downs at all.

**A presentation slice, and nothing else.** No new endpoint, no new `WorkflowEvent`
variant, no migration, no new core primitive. The three drill-downs call the shipped
handlers' own bodies rather than reimplementing them — that is the load-bearing design
decision, not a convenience. `preview_schedule_firings_handler` and
`list_schedule_runs_handler` were split into a thin axum wrapper plus a reusable
`compute_schedule_preview` / `load_schedule_runs`, and the backfill launcher calls
`schedule_backfill` directly. So the UI inherits, by construction rather than by
convention: the #478/#543 bounded-run truncation, the paused/exhausted zero-entry
branches, the keyset merge and scheduled-origin-only cadence summary, the
`complete`/`partial`/`unavailable` cross-shard status, and the audit record. A UI that
re-derived any of those could tell an operator a schedule will fire when the scheduler
knows it never will — the exact failure this page exists to prevent.

Splitting the preview handler turned its ad-hoc `serde_json::json!` object into a typed
`SchedulePreview`. The key set (and the deliberate suppression of a stale `pause_reason`
on an active row) is unchanged, and the existing unit tests now assert on the serialized
value as well as the fields, so the public response shape is pinned.

**The list view.** Four field groups the page was missing: the jitter-adjusted
`effective_fire_time` under `next_run_at` (#240, via the same `effective_fire_time` the
API uses), the overlap policy with its buffered depth and cap (#241), the effective
catchup policy with its window and the drop count from the most recent recovery tick
(#484), and bounded-run state as `<remaining> of <max> left · ends <ts> · exhausted:
<reason>` (#478).

**Health is one derived function.** `schedule_health` produces four independent flags —
paused, auto-paused (#360), exhausted, catchup-dropped — and every badge, every sort
decision and the "Needs attention" summary strip goes through it, so they cannot
disagree. Unhealthy rows sort to the top; the health rank is a *prefix* on the existing
comparator, so healthy rows keep exactly the `next_run_at`-ascending order they had
before. A `health=Unhealthy` filter narrows the list, and an unrecognised value is a
`400` rather than a silent all-match.

**The backfill launcher is two-stage, and the staging is a safety property.** A dry run
writes a `harvest_backfill_log` row and an audit record, so it cannot sit on a GET
without breaking the "read path stays read-only" requirement. `GET` renders the form;
`stage=preview` runs the dry run and shows the planned count, would-dispatch and
would-skip counts and skip reasons; only an explicit `stage=commit` dispatches. A POST
that omits `stage` **falls back to the dry run**, so a bare or replayed form post can
never dispatch work. Success redirects to that schedule's run history, so the runs the
operator just launched are one click from the confirmation.

**Degraded states are designed, not defaulted.** A `partial` runs response renders a
"Some shards unreachable" banner naming each unavailable shard and its error, and flags
the cadence summary as possibly understated; an `unavailable` response says no shard
could be reached and deliberately does *not* render "No runs yet", because nothing was
read and claiming an empty history would be a lie. Zero schedules, a filter matching
nothing, a schedule with no runs, and a preview truncated to zero entries each get an
explicit message naming the cause (pause reason, `exhausted_reason`, the `end_at`
cutoff, or an expression with no future firings).

**Auth parity, per route.** Each drill-down mirrors the posture of the endpoint it
renders: the run history is admin-gated because `GET /admin/schedules/{id}/runs` is the
one schedule read route the API gates; preview and backfill are not, matching their
ungated API routes.

**An audit-trail bug found on the way through.** All six existing schedule UI mutations
(pause, resume, delete, trigger-now, bulk pause, bulk resume) wrote `source: "api"` on
their audit records while their own `route_or_command` read `POST /ui/schedules/…` —
internally inconsistent, and it meant an operator filtering the audit log by
`source = ui` to reconstruct "what was done from the dashboard" would find every
workflow, DLQ and gate action but no schedule action at all. Every other Vantage
mutation already recorded `ui`. Fixed, with the pause round-trip test asserting on it.

**Cross-site safety.** Vantage's confirmations are inline `onsubmit="return
confirm('…')"` attributes, so anything interpolated into one lands in a JavaScript
string literal. Only UUIDs and integers go in there — never a workflow or DAG name — and
the backfill confirmation passes its id through `js_escape` regardless. A test extracts
the contents of every `onsubmit` attribute in a rendered row and asserts a hostile
schedule name reaches none of them, while separately asserting a markup-bearing name is
escaped into display text rather than rendered.

**Test evidence.** 35 no-database unit tests over the pure layer: the health model and
its four flags, the unhealthy-first sort proving healthy rows keep their relative order,
health-filter parsing and matching, the catchup/overlap/bounded-run/next-fire cells
across their policy branches, the summary strip, drill-down links, the preview page's
effective-vs-original-vs-suppressed rendering and both zero-entry explanations, the runs
page's rows/origins/execution links and all three cross-shard statuses, the backfill
form's dry-run-only staging, the confirmation's planned counts and window round-trip,
the empty-window guard, window parsing, and the two inline-handler injection guards.
`ui_integration.rs` adds 12 database-backed tests: the list rendering a paused + an
exhausted + a catchup-dropped + a healthy schedule with every policy field and the
unhealthy-first ordering asserted by document position, the health filter and its `400`,
the pause round-trip asserting both the flipped row and exactly one `source = ui`
`schedule.pause` audit record, preview rendering and its exhausted explanation, `404`s
on all three drill-downs for an unknown id, run history with scheduled/backfill/manual
origins proving the cadence summary counts scheduled-origin runs only, the no-runs empty
state, the partial-shard banner against a genuinely unreachable second shard, the full
backfill form → dry-run → commit flow asserting nothing is dispatched until the commit
and that both stages are audited as UI-sourced, the malformed and inverted window
errors, and the stage-less POST defaulting to the dry run.

**Multi-angle review, and the bug that made the whole feature dangerous.** Four reviews
(security, correctness, AC compliance, UX/a11y/test-quality) ran against the first
implementation. The AC review found the one that mattered: `to_request(commit)` passed
the UI's `commit` stage straight into the API's `dry_run` field, which are *opposite*
polarity. `stage=preview` therefore sent `dry_run: false` and really dispatched the
backfill — then rendered a confirmation page reading "Nothing has been dispatched yet."
over the counts of the runs it had just launched — while `stage=commit` sent
`dry_run: true`, dispatched nothing, and redirected with "Backfill dispatched N runs".
A stage-less POST dispatched too, inverting the exact safety property the two-stage design
was built for. The integration tests were written correctly and assert precisely this, but
no Postgres was available where the change was authored, so they had never executed. The
fix is `to_request(!commit)`; the polarity is now pinned by a *pure* test that runs
without a database, because that is the layer where the mistake was reachable.

Ten more findings were fixed. Three were P1-class in their own right. **Bulk pause/resume
ignored the new health filter** while the button's count and its `confirm()` text came
from the health-filtered total — select "Unhealthy", see "Pause all matching (3)", pause
all 200. Both handlers now select whole rows and apply `ScheduleUiFilters::matches`, the
same predicate the list uses, so the acted-on set is the counted set by construction (this
also fixes the pre-existing `paused` filter being ignored). **Every nav link on all four
drill-down pages 404'd**: `layout_schedules` hard-codes depth-0 relative hrefs, correct for
`/schedules` and wrong two segments down; it now takes a `base_href` like `layout` already
did. **The run-history "Next" link** was missing the same prefix, making the history
unpageable, and dropped the `origin`/`state` filters the cursor was computed under.

The rest: the committed backfill's flash — the only report of a partial dispatch — was
URL-encoded into a redirect the runs page did not accept or render; the drill-downs mapped
an unreachable shard to `404 "schedule not found"` mid-incident, and now share the API's
`resolve_schedule_with_shard` so `503 indeterminate` survives; a UI backfill was audited
under `POST /admin/schedules/{id}/backfill`, reintroducing inverted the very
route/source mismatch this change set out to fix, so `schedule_backfill` was split into a
`schedule_backfill_inner` taking the caller's own route string (mirroring
`retry_dag_run_inner`); `max_count` was unbounded on a form that replaces the engine's
planning guard; a rejection discarded the operator's typed window; the "Needs attention"
strip counted the page rather than the filtered set; `aria-label` on the exhausted and
catchup-dropped badges *replaced* the visible text, making the reason and the drop count
inaudible; and two preview badges skipped the `role="status"` convention entirely.

One pre-existing bug was fixed alongside: `schedule_redirect` emitted a bare
`schedules?flash=…`, correct from `/schedules/bulk-pause` but resolving to
`/schedules/{id}/schedules` from `/schedules/{id}/pause|resume|delete|trigger-now` — so
every per-row action on the list page landed on a 404. Depth is now explicit.

The test review found one assertion that could not fail (`|| html.contains("disabled")`,
satisfied by a CSS rule in the inlined stylesheet) and four more satisfied by page chrome
or the stylesheet rather than by the thing under test — `contains("24")`,
`contains("COMPLETED")`, `contains("scheduled")`, `contains("backfill")`. All now assert
on the specific cell markup. Twelve pure tests and six integration tests were added for
what nothing covered: the `dry_run` polarity, the window round-trip, mount-relative links
on every drill-down (nav chrome and the next-page link), the flash actually rendering,
redirect depth, the health filter through a bulk action, the paused-DAG dead end, form
echo on rejection, filtered-vs-page summary scope, a bare "Exhausted" badge with no
reason, `max_count` validation and capping, and admin-gating parity across all three
drill-downs. The integration fixture now quote-escapes its literals so a hostile name can
be seeded at all.

**Known gap, deliberately not fixed here.** Vantage has no CSRF protection on any POST —
no token, no `Origin`/`Sec-Fetch-Site` check — and the backfill launcher is the first UI
route that dispatches unbounded scheduled work, so a logged-in operator visiting a hostile
page could be made to submit `stage=commit`. This is a repo-wide property of every Vantage
mutation (cancel, terminate, signal, reset, DLQ replay, gate lift, schedule pause/delete/
trigger), not something this page introduces, and a fix belongs at the router layer for
all of them at once. Filed as a follow-up rather than bolted onto one form.

**Codex round 1 (three P2s, all real, one of them a functional dead end).** The sharpest
was auto-pause. The scheduler's #360 auto-pause sets `auto_paused_at` and deliberately
does **not** set `is_paused`, so a row can be non-firing with `is_paused = false`. The
health model read that correctly and rendered an "Auto-paused" badge — but the row
actions, the bulk-resume candidate query and the resume UPDATE all keyed on `is_paused`
alone, so the operator was shown a schedule that had stopped firing, offered a **Pause**
button for it, and had no path in Vantage to restore it. Worse, even a resume that did
land would not have cleared `auto_paused_at` or reset `consecutive_failure_count`, so the
next tick would re-pause it. This page created the visibility and then dead-ended on it.
Resumability is now `is_paused OR auto_paused_at IS NOT NULL`, and both resume paths clear
the same fields as `set_schedule_paused(.., false, ..)`.

The second: the preview drill-down resolved its row through the new resilient
`resolve_schedule_with_shard` and then handed the *id* back to
`compute_schedule_preview`, whose own `load_schedule_by_id` stops at the first shard whose
connection fails. In a multi-shard deployment with an earlier shard down, the preview
therefore failed for a schedule the resolver had just found on a later healthy one —
undoing the very resilience the resolver was introduced for. `compute_schedule_preview`
is now split so a caller holding a resolved row passes it through.

The third: `SecondsFormat::Secs` truncated both window bounds when normalising a backfill
submission. Judged a nit in the earlier internal review ("self-consistent, so the
committed window equals the previewed one"), and that reasoning was too narrow — an
`interval:` backfill treats `from` as its first slot, so `…00.900Z` normalised to `…00Z`
shifts every slot in the plan. It changes the window, not its spelling. Now `AutoSi`,
which keeps the clean `…:00Z` rendering for whole-second windows.

Three pure regression tests (precision round-trip, auto-paused rows offering Resume,
ordinary rows unchanged) and three integration tests (per-row and bulk resume clearing
`auto_paused_at` and the failure counter, and a preview rendering across an unreachable
earlier shard).

**Codex round 2 (three more P2s, and one of them the root cause round 1 only patched).**

The instructive one: round 1 fixed the *preview* passing its id back to a lookup that
stops at the first unreachable shard. Round 2 pointed out the backfill path still did
exactly the same thing — the fix had been applied to the instance, not the cause.
`load_schedule_by_id_with_shard` now delegates to `resolve_schedule_with_shard`, so every
schedule lookup in the plugin obeys one rule: keep scanning past an unreachable shard,
return the row when any shard has it, and distinguish an authoritative `404` from a `503`
when a shard could not be checked. That also repairs `GET /admin/schedules/{id}/preview`
and `POST /admin/schedules/{id}/backfill` themselves, which had the same defect
independently of the UI.

Second: the health model read `exhausted_at` alone. That column is stamped
*asynchronously* by the tick that observes the bound, so a tick that dies first leaves a
row already terminal — `runs_started >= max_runs`, or `now >= end_at` — with the column
still NULL. The engine never trusts it alone (`schedule_backfill_inner` and
`trigger_schedule_now` both check the live fields, and the scheduler's `schedule_overdue`
derives the same thing from raw columns for precisely this reason), so a bounded-out row
rendered here as a calm `Active` schedule that `health=Unhealthy` excluded and the sort
put *below* the unhealthy rows — while this page's own preview for it correctly reported
no upcoming fire times. `schedule_is_bounded_out` now mirrors the engine's predicate, and
an unstamped exhaustion names its bound (`run budget spent` / `past end_at`) since there
is no `exhausted_reason` yet.

Third, and the reason the second fix has a `max > 0` guard: `max_runs = 0` is the engine's
*unlimited*, not a spent budget — the guard appears at five bound-check sites and is
pinned by `backfill_max_runs_zero_is_treated_as_unlimited`. The cell rendered a legacy
zero-cap row as `0 of 0 left`, telling an operator a schedule that fires forever had
stopped. No budget is rendered for a zero cap now, and it never counts as bounded out.

Three pure regression tests and three integration tests, including the backfill path
across an unreachable earlier shard and a live-bounded-out row's badge, filter and sort
position.

**Codex round 3 (three P2s — all three the *same* mistake as round 2, in the places round
2 didn't reach).** Round 2 taught the `max_runs > 0` convention and the "derive terminal
state from live fields" rule; round 3 found both applied in one place and not the other,
plus a third instance of the same class.

The `end_at` bound was compared against the wall clock. The scheduler judges it on the
**pending slot** — `schedule_overdue` tests `next_run_at >= end_at`, and the tick refuses
a fire whose `effective_fire_time >= end_at` — and `now` is wrong in both directions: a
schedule whose next slot already sits past the cutoff will never fire again while the
clock is still short of it (rendered healthy), and after downtime an overdue slot from
*before* the cutoff is still legal and will be processed once the clock passes it
(rendered exhausted). Now `next_run_at >= end_at`, falling back to the wall clock only
when there is no pending slot to judge.

`max_runs = 0` was corrected in the list cell in round 2 but not in the shared preview
computation, which mapped the raw cap through `remaining_runs_budget`, got `Some(0)`, and
short-circuited to zero entries — so the preview told an operator an *unlimited* schedule
had spent its budget. The convention now lives in one place,
`api::schedule_remaining_runs`, used by both the preview and the `GET /admin/schedules`
entry; a legacy zero-cap row reports `remaining_runs: null` rather than `0`.

And the preview short-circuited on `is_paused` only. The scheduler's due-list filters on
`auto_paused_at IS NULL` (#360), so an auto-paused schedule — `is_paused = false` —
projected future fire times for a schedule that will never fire, on the same page whose
header correctly badged it Auto-paused. The preview now treats auto-pause as terminal and
carries the reason (`auto-paused after N consecutive failures; resume to restore firing`)
so an API caller sees why the list is empty, while `is_paused` keeps mirroring the column.

All three were defects in the *shared* computation, so fixing them repaired
`GET /admin/schedules/{id}/preview` and `GET /admin/schedules` as well as the page.

Three pure regression tests, including both directions of the `end_at`/pending-slot error.
