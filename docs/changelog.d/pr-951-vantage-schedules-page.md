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
