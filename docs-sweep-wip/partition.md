# autumn-harvest 0.5.0 docs sweep — RED-PHASE PARTITION

> Companion to `plan.md`. Divides the docs+examples+README surface into 6 coherent
> review AREAS sized for one worker each. **This is the red/planning phase** — an area
> worker's job (next phase) is to produce an EVIDENCE LIST (stale claim → `file:line` →
> expected fix), NOT to edit content. Fixing is a later phase.
>
> **Persistence:** written to scratchpad + `docs-sweep-wip/partition.md` on branch
> `docs/0.5.0-sweep`. `docs-sweep-wip/` is git-rm'd before the final PR.

---

## ⛔ DEFERRED PIN SET — READ FIRST. NO WORKER MAY EDIT THESE LINES. ⛔

Resolvable Cargo-pin toml snippets + the scaffold template. They MUST stay at published
versions (`harvest new` and copy-paste quickstarts break otherwise, since 0.5.0 /
autumn-web 0.6 are not on crates.io yet). **You MAY fix OTHER content in these files;
you may NOT touch the dependency-pin lines.**

| File | Line(s) | Pinned (leave as-is) |
|---|---|---|
| `autumn-harvest-cli/templates/minimal/Cargo.toml.tmpl` | 7–9 | `autumn-harvest="0.4"`, `autumn-harvest-plugin="0.4"`, `autumn-web="0.5"` |
| `docs/autumn-workflow-architecture.md` | 77–78 | `autumn-web="0.5"`, `autumn-harvest="0.4"` |
| `docs/replay-verify.md` | 29 | `autumn-harvest={version="0.3",features=["testing"]}` (**0.3**) |
| `docs/sqlite-backend.md` | 71–72 | `autumn-harvest={version="0.4",...}`, `autumn-harvest-sqlite="0.4"` |
| `docs/telemetry.md` | 18 | `autumn-harvest-plugin={version="0.4",features=["metrics"]}` |
| `docs/getting-started/01-project-skeleton.md` | 18–20 | `autumn-harvest="0.4"`, `autumn-harvest-plugin="0.4"`, `autumn-web="0.5"` |

**Pre-PR grep-gate (must return NOTHING):**
```
grep -nE 'autumn-harvest[a-z-]* *= *[{"][^,}]*0\.5|autumn-web *= *[{"][^,}]*0\.6|autumn-harvest-sqlite *= *"0\.5"' \
  autumn-harvest-cli/templates/minimal/Cargo.toml.tmpl \
  docs/autumn-workflow-architecture.md docs/replay-verify.md docs/sqlite-backend.md \
  docs/telemetry.md docs/getting-started/01-project-skeleton.md
```
Note: README.md is NOT a deferred-pin file (it has no resolvable toml pin — see plan §6 S1).

## ⛔ HARD EXCLUSIONS — NEVER TOUCH (any area) ⛔
- `CHANGELOG.md` (owned by release PR #1125's fragment fold)
- the CLAUDE.md `### Phase Status` list (cross-PR conflict source)
- any `Cargo.toml` / `Cargo.lock` (owned by #1125; also breaks resolution)
- `docs/changelog.d/*` fragments (owned by #1125's fold — do not edit or delete)

## ⚠️ SITE-SPECIFIC FLAGS (loud) ⚠️
- **Duplicate ADR-0002** (Area 5): `docs/adr/0002-payload-codec-event-boundary.md` AND
  `docs/adr/0002-rust-native-execution-boundary.md` both numbered 0002. Renumber-with-refs
  (grep whole repo for `0002`/`ADR 0002`/both filenames) OR add a disambiguation note.
- **Missing memory file** (Area 3): `/tmp/claude/memory/team/harvest-followup-1102-composed-mode-doc.md`
  does NOT exist. There is NO dedicated scoped-API-tokens doc. Derive composed-mode /
  standalone-token auth-ordering docs from `docs/changelog.d/pr-1102-scoped-api-tokens.md`
  + the plugin `api_token` code. This is a genuine coverage gap.
- **`docs/upgrading/` does NOT exist** (Area 6): author the migration guide at
  `docs/upgrading/0.5.0.md` (new dir). Add inbound links from README + getting-started
  README (link-add only; never re-save a deferred-pin line).
- **RELEASE_NOTES.md** stale @0.4.0 (Area 5): decide extend-to-0.5.0 vs. mark-superseded.
- **api-contract.json `"version":"0.4.0"`** (Area 2): assess generated-vs-hand-edited.
- **Prose "autumn-web 0.5" vs adjacent deferred pin** (all areas, esp. 2/5): flag each as
  a REVIEW decision — do not silently create prose/pin inconsistency (plan R3/S9).
- **madmax983 → autumn-foundation org rename** (Areas 5 primarily; any file with repo
  links): correct harvest's OWN repo/badge/DeepWiki links; LEAVE genuine framework/dep
  links (autumn-web repo, diesel, croner, etc.). Maintain an external-host allow-list.

---

## AREA 1 — Getting-started guides + project skeleton + activities
**Owner focus:** the onboarding funnel. New-user-facing snippet correctness + new-feature
mentions in the tutorial chapters. Contains a deferred-pin file (01-project-skeleton).

Files:
- `docs/getting-started/README.md`
- `docs/getting-started/01-project-skeleton.md`  ⛔ **deferred pin L18-20**
- `docs/getting-started/02-first-workflow.md`
- `docs/getting-started/03-durable-timers.md`  (new: cancellable timers #768)
- `docs/getting-started/04-signals.md`  (new: signal drain #775, signal gates cross-ref)
- `docs/getting-started/05-child-workflows.md`  (new: child-or-deadline #779)
- `docs/getting-started/06-idempotency.md`  (new: WorkflowIdConflictPolicy #685)
- `docs/getting-started/07-reliability-knobs.md`  (new: panic containment #782, chain cap #617, per-key rate limit #699, deadline-aware CAN #772)
- `docs/getting-started/08-dags-and-schedules.md`  (new: DAG signal gates #746, DAG node input binding #702)
- `docs/getting-started/09-worker-routing.md`
- `docs/getting-started/10-operations.md`
- `docs/getting-started/11-testing.md`  (new: WorkflowSimulator RetryPolicy #541)
- `docs/getting-started/12-webhooks.md`  (verify #344 shipped surface)
- `docs/getting-started/activities.md`  (new: interceptors #680, auto-heartbeat #682)
- Scaffold `harvest new` (#692) prose in 01-project-skeleton (⛔ pins).

RED checklist: snippet API drift; new-feature mentions per matrix; `harvest new` prose
matches the template dir (`Cargo.toml.tmpl`,`README.md.tmpl`,`autumn.toml.tmpl`,
`compose.yaml.tmpl`,`gitignore.tmpl`,`main.rs.tmpl`); deferred pin untouched.

## AREA 2 — Core concept docs + management API + telemetry + context features
**Owner focus:** the deepest API-drift surface (routes, methods, metrics). Largest area
by risk — most new-in-0.5.0 features land here. Contains deferred pin (telemetry L18).

Files:
- `docs/management-api.md`  (new routes: summaries, run-chain, legal-hold, fail-now,
  PATCH schedules, /workflows/count, /admin/usage, /admin/tokens, completion-deliveries,
  /interface, reachability, replay-diagnosis, business-id forms #805, batch dry_run #769,
  paginated history #529, partial-availability #756)
- `docs/api-contract-guide.md`  (⚠ `"0.4.0"` example L14)
- `docs/telemetry.md`  ⛔ **deferred pin L18** (new metrics: #782 panic×2, #781 update.duration,
  #770 workflow.active, #752 summary_deleted, #618/#1053 admission, #684 6 counters, #344 webhook×2)
- `docs/typed-workflow-failures.md`  (#767 — verify current)
- `docs/completion-callbacks.md`  (#605 — verify)
- `docs/completion-triggers.md`
- `docs/mcp-tools.md`  (⚠ "autumn-web 0.5" L13 → 0.6 prose; #597 hardening)
- `docs/streaming-progress.md`  (#791 publish_progress)
- `docs/saga.md`
- `docs/sharding.md`  (#596 usage report, #756 partial reads, #544 count)
- `docs/workflow-determinism-guide.md`  (#778 det-check CLI, #767)
- `docs/replay-verify.md`  ⛔ **deferred pin L29 (0.3)**
- `docs/search-attributes.md`
- `docs/transactional-activities.md`  (interceptor interaction #680)
- `docs/sticky-routing.md`
- `docs/retry-jitter.md`
- `docs/api-contract.json`  ⚠ `"version":"0.4.0"` L2 — assess generated/hand-edited
- ctx-feature coverage: `ctx.info()` #698, `ctx.await_external_workflow` #757,
  bounded fan-out #750, **ctx.mutex #1122** (assess dedicated section), per-key rate
  limit macro #699, deadline-aware CAN #772, chain cap #617, StartSource #740,
  interface schema #610, replay-diagnosis #614.

RED checklist: every route string ↔ api-contract.json; every `ctx.*` ↔ context.rs;
metric names/labels ↔ telemetry.rs; #767 destructures need `..`;
`rate_limit_saturated`→`_exhausted` anywhere in these docs; autumn-web 0.5→0.6 prose
flags; deferred pins untouched.

## AREA 3 — Runbooks + operations + alerts/dashboards + security/operator-role
**Owner focus:** operator-facing correctness; renamed reason codes; new operator surfaces;
the scoped-tokens/composed-mode GAP.

Files:
- `docs/runbooks/*` (all 20): activity-circuit-breaker, audit-trail,
  contain-runaway-execution (#609), dag-retry-from-failed-node, external-activity-handoffs,
  ha-deployment, **harvest-alerts.md** (65 KB — new metrics/alerts), history-ceiling,
  nondeterminism-block, replay-fixture-export, safe-deploy, **safe-handler-removal.md**
  (#520/#700 reachability), schedule-backfill, schedule-preview, schedule-run-history,
  schedule-trigger-now, synthetic-incident-drills (#609 drill), **triage-pending-tasks-idle-workers.md**
  (#611 `rate_limit_saturated`→`_exhausted`, circuit codes, #765 fail-now), version-gate-retirement
- `docs/operations/adaptive-slot-tuner.md`
- `docs/operations/admission-gate-producers.md`  (#618/#1053 contract changes)
- `docs/operations/read-path-decode.md`
- `docs/operations/schedule-pause-resume.md`  (#771 PATCH, #609)
- `docs/alerts/README.md`  (new alert rules for new metrics)
- `docs/dashboards/README.md`  (new panels for new metrics)
- `docs/security-posture.md`  (**scoped API tokens #942 GAP** — composed vs standalone,
  auth ordering; legal hold #747)
- `docs/operator-role.md`  (#776 — verify)
- `docs/search-attributes.md` if usage-report tenant-key overlaps (coordinate w/ Area 2)

RED checklist: `rate_limit_saturated` grep across runbooks; new metrics ↔ alerts/dashboards;
#609 resume `409→200`; #765/#611/#771 operator surfaces; scoped-tokens gap documented
(derive from pr-1102 fragment); no fabricated auth claims.

## AREA 4 — Examples across ALL crates (compile-relevant API drift)
**Owner focus:** every example `.rs` compiles against released 0.6.0 / diesel-async 0.9.
DB examples compile-check only (no Postgres here — hedge). No deferred pins in `.rs`
bodies (their `Cargo.toml`s are HARD-EXCLUDED — do not touch).

Files (65):
- `autumn-harvest/examples/*.rs` (48) — full list in plan; new-feature examples already
  present (activity_interceptor, cancellable_timer_sla, child_with_timeout,
  signal_aggregator, dag_approval_gate, mutex_ledger, await_external_workflow,
  fanout_batch, ctx_info, typed_workflow_failure, etc.) — verify each compiles + matches
  released API.
- `autumn-harvest-plugin/examples/*.rs` (3): mcp_tools_quickstart, metrics_scrape_quickstart,
  webhook_receiver_quickstart (autumn-web 0.6 AppBuilder surface!)
- `autumn-harvest-sqlite/examples/*.rs` (2): durability, quickstart
- `examples/billing-autumn-web/src/*.rs` (7) — autumn-web 0.6 web app surface
- `examples/standalone-runner/src/*.rs` (7)
- `examples/quickstart/src/*.rs` (2)
- `examples/saga-choreography/src/main.rs`

RED checklist: `cargo build -p autumn-harvest --examples` (+ each example crate) against
0.6/0.9 baseline; note which fail and why (API drift vs. env); #767 destructure `..`;
renamed symbols; **do NOT touch any example `Cargo.toml`**. Hedge DB-only examples.

## AREA 5 — README + skills + top-level docs + sqlite + comparison + vantage + calendars + archival + ADR + plans/rnd
**Owner focus:** the "front door" + org-rename links + duplicate-ADR + RELEASE_NOTES +
historical-doc status claims. Contains deferred pins (arch-doc, sqlite-backend).

Files:
- `README.md`  (narrative `0.4.0` L912; org-rename badges/links; CLI cheat-sheet drift;
  NO toml pin present)
- `skills/SKILL.md`  (`Version: 0.4.0` L18; org links)
- `skills/references/architecture.md`  (org links; status claims)
- `RELEASE_NOTES.md`  (⚠ stale @0.4.0 — decide extend vs superseded)
- `AGENTS.md`  (932 B — quick check)
- `autumn-harvest-sqlite/README.md`  (#1068/#1080; org links)
- `docs/sqlite-backend.md`  ⛔ **deferred pins L71-72**
- `docs/comparison.md`  (#963 — verify claims current)
- `docs/vantage-ui.md`  (#957/#960 DAG-graph + timeline UI pages)
- `docs/calendars.md`
- `docs/archival.md`  (tiered/summary retention #752 cross-ref)
- `docs/autumn-workflow-architecture.md`  ⛔ **deferred pins L77-78**; narrative `(0.4.0)` L8
- `docs/adr/0001-otel-trace-contract.md`  (new metrics §7 — coordinate w/ Area 2/3)
- `docs/adr/0002-payload-codec-event-boundary.md`  ⚠ **duplicate 0002**
- `docs/adr/0002-rust-native-execution-boundary.md`  ⚠ **duplicate 0002**
- `docs/plans/*` (16 files) — historical; only fix egregious "current status" lies, prefer
  a one-line note over rewrite (plan R9)
- `docs/rnd/wasm-activities-spike.md`  (#965/#1072 — historical R&D)

RED checklist: org-rename link classification (own vs genuine-external allow-list);
duplicate-ADR resolution plan; RELEASE_NOTES decision; deferred pins untouched;
narrative-version updates (`0.4.0`→`0.5.0`) where they describe the release; plans/rnd
left frozen unless actively misleading.

## AREA 6 — Migration guide authoring + feature-presence matrix (NET-NEW)
**Owner focus:** the two green-hat deliverables. Depends on Areas 2/3 findings for the
breaking-change specifics but can be drafted from the folded CHANGELOG now.

Deliverables:
- **NEW `docs/upgrading/0.5.0.md`** (new dir). Sections per plan §7: (1) autumn-web
  0.5→0.6 / diesel-async 0.8→0.9 bump; (2) source-breaking #767 (exhaustive destructure
  `..`; child-failure `WorkflowFailed` + accessor swap); (3) behavior changes (resume
  `200`, throttle `202`, cross-shard partial reads); (4) new-feature index w/ links.
  Inbound links from README + `getting-started/README.md` (link-add only).
- **Feature-presence matrix** — promote plan §5 into a durable `docs-sweep-wip/`
  artifact (and optionally a short "new in 0.5.0" section) so reviewers see coverage.
- Coordinate the confirmed COVERAGE GAPS to closure: scoped tokens/composed-mode (w/
  Area 3), interceptors #680, cancellable timers #768, summary retention #752, legal
  hold #747, DAG signal gates #746, ctx.mutex #1122.

RED checklist: every migration-guide claim traces to the folded `## [0.5.0]` CHANGELOG
`### Upgraded`/`### Changed` (read-only); no contradiction with the excluded CHANGELOG.

---

## Sequencing & gate (recap from plan §3 Blue)
1. RED phase: each area produces an evidence list (no edits).
2. GREEN phase: fixes per area; deferred pins + hard exclusions untouched; snippets
   compile-checked; Area 6 authors the guide + matrix.
3. GATE: non-DB CI green (fmt, clippy `-D warnings` touched crates, `cargo build
   --examples`); DB suites compile-checked (hedged); deferred-pin grep-gate clean;
   org-name link classification done; `docs-sweep-wip/` git-rm'd; PR opened & marked
   **"merge AFTER #1124 and #1125."**

## Definition of done — see plan.md §4 (A–K).
