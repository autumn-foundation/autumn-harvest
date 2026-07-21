# autumn-harvest 0.5.0 documentation sweep — PLAN (planning phase output)

> **This is the PLANNING phase. No doc content is edited in this phase** — only this
> file and `partition.md` are written. Fixing happens in a later phase, by workers
> assigned the areas in `partition.md`.
>
> **Persistence note:** this file is written to BOTH the scratchpad
> (`/tmp/claude-0/.../scratchpad/`) and, on branch `docs/0.5.0-sweep`, into the repo at
> `docs-sweep-wip/plan.md`. The `docs-sweep-wip/` directory is a temporary resilience
> copy — it MUST be `git rm`'d before the final docs PR is opened. It exists only so a
> session reset does not lose the plan.

---

## 0. Environment facts (verified this session — trust these)

- Repo `/home/user/autumn-harvest`. Working branch **`docs/0.5.0-sweep`** created off
  `origin/trunk-dev` and pushed. Pushed SHA `4c1ad9f26f6cb5d89cac6e761ca8352e4b90af59`
  (the ctx.mutex merge #1122). Working tree clean.
- Toolchains: rust 1.97.0 (CI clippy) and 1.88.0 (MSRV). **Postgres is NOT running,
  `$HARVEST_TEST_DATABASE_URL` unset** → DB/testcontainers suites can only be
  *compile-checked* here, never executed. Any "DB example verified" claim must say
  "compile-checked only".
- **0.5.0 release** = `trunk-dev` + PR **#1124** (autumn-web 0.5→**0.6.0**, diesel-async
  0.8→**0.9** port) + PR **#1125** (workspace crates 0.4.0→**0.5.0**, fold of 58
  changelog fragments, metadata org → **autumn-foundation**). Both are OPEN DRAFTS;
  #1124 is dirty. **This docs PR must merge AFTER them.**
- Working-tree crate versions confirmed from a build log this session: `autumn-harvest
  v0.4.0`, `autumn-harvest-macros v0.4.0`, `autumn-web v0.5.0`, `diesel-async v0.8.0`
  — i.e. **trunk-dev is still PRE-release-bump**. The 0.5.0 versions + the folded
  CHANGELOG live only on `origin/release/v0.5.0`; the autumn-web 0.6 code port lives on
  `origin/release/autumn-web-0.6-upgrade`.
- Folded `## [0.5.0]` CHANGELOG section extracted this session (see §5 matrix). Trunk
  has **59** loose fragments in `docs/changelog.d/` (the folded set is 58; trunk adds
  `pr-1122-ctx-mutex.md`, which is the one commit trunk is ahead of the release branch).

---

## 1. Brainstorming — the full menu a "docs sweep against a release" should catch

Grouped by class. Each is a *thing to look for*; the fixing phase decides per-hit.

### A. API-signature / symbol drift (docs claim an API that no longer matches code)
1. **Renamed / removed public methods** — e.g. the changelog explicitly renames
   `rate_limit_saturated` → `rate_limit_exhausted` (#611); typed-child-failure change
   swaps `HarvestError::ActivityFailed{name:"child-workflow:.."}` → `WorkflowFailed`
   (#767). Grep docs for the OLD names.
2. **Source-breaking additive fields (#767)** — `HarvestError::WorkflowFailed`,
   `WorkflowEvent::WorkflowFailed`/`ChildWorkflowFailed`, `replay::HistoryMatch::Failed`,
   `TypedWorkflowResult` gained fields. Any doc snippet that exhaustively destructures
   these now needs `..`.
3. **New public methods missing from docs / method signatures that changed arity** —
   verify each documented `ctx.<method>(...)` against `context.rs`.
4. **Wrong return types** — e.g. `execute_child_workflow_timeout::<O>` returns
   `Ok(Option<O>)`; `await_fire()` returns `TimerOutcome::{Fired,Cancelled}`.
5. **Macro attribute drift** — `#[activity(rate_limit(key=.., rps=.., burst=..))]`
   (nested, #699), `#[workflow(mcp)]` (#597), `#[webhook(...)]` (#344),
   `#[dag] signal_gate` builder (#746). Docs showing the OLD flat/absent forms.

### B. HTTP route / management-API drift
6. **Non-existent or renamed routes** — cross-check every `GET/POST/PATCH/DELETE
   /api/harvest/...` string in docs against `docs/api-contract.json` +
   `management_api_routes()`.
7. **New 0.5.0 routes undocumented in `management-api.md` / `api-contract-guide.md`** —
   e.g. `/workflows/summaries`, `/workflows/{id}/run-chain`,
   `/workflows/{id}/legal-hold[/release]`, `/workflows/{id}/activities/{aid}/fail-now`,
   `PATCH /admin/schedules/{id}`, `/workflows/count`, `/admin/usage`, `/admin/tokens`,
   `/workflows/{id}/completion-deliveries[/{id}/redrive]`, `/workflows/{id}/interface`,
   `/workflows/registered/{name}/interface`, `/admin/workflow-types/reachability`,
   `/workflows/{id}/replay-diagnosis`, business-id (`workflow_name/workflow_id`) forms
   of act-on-existing routes (#805), the SSE `publish_progress` route (#791), the
   `dry_run` field on `/batch-operations` (#769), overdue-schedule detection (#696).
8. **Status-code / response-shape claims** — e.g. resume-non-paused now `200` not `409`
   (#609); with-start committed-replay ordering (#1105).
9. **`docs/api-contract.json` `"version": "0.4.0"`** (line 2) and
   `api-contract-guide.md:14` example `"0.4.0"` — decide whether these should read
   `0.5.0` and whether api-contract.json is hand-editable or generated (**assess, don't
   assume**).

### C. CLI flag / subcommand drift
10. **New CLI subcommands undocumented** — `harvest new`, `harvest token
    create|list|revoke|bootstrap`, `harvest legal-hold set|release`, `harvest workflow
    summaries|run-chain|fail-activity|pause|resume`, `harvest completion-delivery
    list|redrive`, `harvest usage`, `harvest schedule update`, `harvest det-check`,
    `harvest dlq bulk-replay|bulk-discard --dlq-reason`, `harvest workflow-types
    reachability`. Cross-check the README CLI cheat-sheet + `management-api.md`.
11. **Stale flag names / removed flags** in CLI examples.

### D. Dependency-version claims (prose, NOT the deferred Cargo pins — see §6/partition)
12. **"Built on autumn-web 0.5"** prose (e.g. `docs/mcp-tools.md:13`) — the released
    baseline is autumn-web **0.6**. Prose correctness claims about the released target
    are candidates to update; **resolvable Cargo-pin toml blocks are deferred** (§6).
    This tension is a REVIEW decision, flagged in reverse-brainstorming.
13. **diesel-async 0.8 → 0.9** prose mentions.
14. **Version-narrative "0.4.0"** in prose: `README.md:912`, `skills/SKILL.md:18`
    (`Version: 0.4.0`), `docs/autumn-workflow-architecture.md:8` (`Release status note
    (0.4.0)`), `RELEASE_NOTES.md` (only goes up to 0.4.0 — no 0.5.0 section).

### E. Links / cross-refs
15. **Dead internal doc links** — relative `[..](../foo.md)` that 404.
16. **Broken cross-refs to renamed sections/anchors.**
17. **Org-name drift in links** — README badges point at `madmax983/autumn-harvest`;
    the release moves metadata to **autumn-foundation**. Every repo link
    (`github.com/madmax983/...` / DeepWiki / CI badge) should be assessed against the
    autumn-foundation org — **EXCEPT genuine framework links** (autumn-web repo, autumn
    docs) which legitimately point elsewhere and must NOT be false-fixed.
18. **Skills links** (`skills/SKILL.md`, `skills/references/architecture.md`) pointing at
    the old org.

### F. Code-snippet compilability (against the RELEASED 0.6.0 / diesel-async 0.9 API)
19. **Snippets that won't compile against autumn-web 0.6 / diesel-async 0.9** — the
    single biggest correctness risk. Any `AppBuilder`/`Plugin::build`/`Diesel`
    signature that changed in 0.6/0.9. Requires reading the autumn-web-0.6-upgrade
    branch diff to know what changed. **Do NOT bump a snippet to a 0.6 API the released
    code does not actually support** (reverse-brainstorm guardrail).
20. **Example crates that no longer compile** (`examples/billing-autumn-web`,
    `examples/standalone-runner`, `examples/quickstart`, `examples/saga-choreography`)
    against 0.6/0.9 — compile-check only (no DB here).
21. **`autumn-harvest/examples/*.rs`** (48 files) + plugin/sqlite examples — API drift
    for new/renamed methods.

### G. Coverage of new features (a mention, guide, runbook, or example missing)
22. **New-in-0.5.0 features with zero user-facing docs** — see the §5 matrix. Notable
    gaps to confirm: **scoped API tokens / composed-mode auth ordering (#942)** — no
    dedicated doc exists (only the changelog fragment); typed workflow failures (#767)
    has `docs/typed-workflow-failures.md` — verify it's updated; activity interceptors
    (#680); cancellable timers (#768); tiered/summary retention (#752); legal hold
    (#747); DAG signal gates (#746); publish_progress streaming (#791, has
    `streaming-progress.md`); operator role (#776, has `operator-role.md`).
23. **New metrics / labels not in `docs/telemetry.md` / ADR-0001 §7 / dashboards /
    alerts** — `harvest.workflow.panic`, `harvest.activity.panic` (#782),
    `harvest.update.duration` (#781), `harvest.workflow.active` (#770),
    `harvest.retention.summary_deleted` (#752), `harvest.admission.bypassed` /
    `harvest.admission.blocked` (#618/#1053), six signal/update counters (#684),
    `harvest.webhook.received`/`rejected` (#344). Verify `telemetry.md` label tables.
24. **New migrations not named where migrations are listed** — `..harvest_legal_hold`,
    `..harvest_execution_summaries`, `..harvest_workflow_continue_chain`,
    `..harvest_api_tokens (20260713000000)`, `..harvest_usage_report_indexes`.

### H. Internal consistency / hygiene
25. **Changelog-vs-docs mismatches** — a feature whose changelog says "renamed X→Y" but
    docs still say X.
26. **Duplicate ADR numbering** — `docs/adr/0002-payload-codec-event-boundary.md` AND
    `docs/adr/0002-rust-native-execution-boundary.md` both numbered **0002** (confirmed).
    One should be renumbered (likely 0003) with inbound refs updated.
27. **`RELEASE_NOTES.md`** is stale at 0.4.0 — decide: add a 0.5.0 section, or note it's
    superseded by `CHANGELOG.md`. (CHANGELOG.md itself is a HARD EXCLUSION.)
28. **`docs/plans/` + `docs/rnd/`** — historical planning docs; likely out of scope for
    correctness fixes but assess for egregiously wrong "current status" claims.
29. **Wrong metric/label/migration names** already covered in G; also wrong
    `WorkflowEvent` variant claims (many 0.5.0 features are "no new variant" — a doc
    claiming otherwise is wrong).

### I. New migration/upgrade guide (green-hat, task §4)
30. **No `docs/upgrading/` directory exists** (confirmed) and no `docs/upgrading/*`
    convention. The 0.5.0 release has real source-breaking changes (#767 exhaustive
    destructure, the ActivityFailed→WorkflowFailed child change, autumn-web 0.6 /
    diesel-async 0.9 bump). **A migration guide is warranted.** Recommended path:
    `docs/upgrading/0.5.0.md` (see §7). This is authored net-new in Area 6.
31. **Feature-presence matrix** (§5) becomes a durable artifact to attach to the PR /
    check into `docs-sweep-wip/` so reviewers see coverage at a glance.

---

## 2. Reverse brainstorming — how could this sweep FAIL or do harm? (+ guardrail)

| # | Failure / harm mode | Guardrail |
|---|---|---|
| R1 | **Bump a deferred Cargo pin** (`autumn-harvest="0.4"`→`0.5`, `autumn-web="0.5"`→`0.6`) in the scaffold template or a resolvable toml snippet → `harvest new` and copy-paste quickstarts break because 0.5.0 isn't on crates.io yet. | §6 DEFERRED PIN SET listed loudly in partition.md with exact file:line. Workers may fix OTHER content in those files but must NOT touch the pin lines. A pre-PR grep gate: `grep -n 'autumn-harvest.*0\.5\|autumn-web.*0\.6' <deferred files>` must return nothing. |
| R2 | **Edit a HARD-EXCLUDED file** (`CHANGELOG.md`, CLAUDE.md `### Phase Status` list, any `Cargo.toml`/`Cargo.lock`). | Hard exclusions enumerated in partition.md; `git diff --name-only` before PR must contain none of them. |
| R3 | **Bump a prose snippet to a 0.6 API the released code doesn't actually support** (guessing at the new autumn-web surface). | Only change an API snippet after confirming the new signature against the `origin/release/autumn-web-0.6-upgrade` diff or the released example crates that compile. If unconfirmable here (no crates.io 0.6), flag as a REVIEW item rather than guess. |
| R4 | **False-fix a link that legitimately points at a framework/external repo** (autumn-web repo, autumn docs, croner, diesel) by "correcting" it to autumn-foundation/autumn-harvest. | Classify every external link: (a) harvest's own repo/badges/DeepWiki → autumn-foundation; (b) genuine framework/dependency link → LEAVE. Maintain an allow-list of "genuine external" hosts. |
| R5 | **Introduce snippets that don't compile.** | Every touched Rust snippet in a `.rs` example is compile-checked (`cargo build -p <crate> --examples` where possible; DB examples compile-check only). Inline doc snippets can't be compiled directly → keep them minimal and mirror a compiling example. |
| R6 | **Thrash / merge-conflict with the two release PRs** (#1124 autumn-web 0.6, #1125 version bump + fragment fold). | Docs PR merges AFTER both. Do NOT touch `Cargo.toml`s (which #1125 owns), `CHANGELOG.md` (which #1125 owns via the fold), or the changelog fragments themselves. Keep edits confined to `docs/**` prose, README, skills, examples' `.rs` bodies (not their `Cargo.toml`). |
| R7 | **Miss the newest feature (ctx.mutex, #1122)** because it's the one commit trunk is ahead of the release branch. | ctx.mutex IS documented in CLAUDE.md; there is no `docs/*` mutex guide yet — add coverage assessment for it explicitly (Area 2). |
| R8 | **Claim DB examples "verified" when only compile-checked** (no Postgres here). | Any verification note in the PR/docs says "compile-checked only; DB/testcontainers not executed in this environment" — matches the repo's own #543/#544/#601 precedent wording. |
| R9 | **Over-reach into `docs/plans/`, `docs/rnd/`, `docs/changelog.d/`** and rewrite historical artifacts. | Those are historical/append-only. changelog.d fragments are owned by #1125's fold — DO NOT edit. plans/rnd are frozen snapshots — only touch if a "current status" line is actively misleading, and prefer a one-line note over a rewrite. |
| R10 | **Renumber the duplicate ADR-0002 but leave dangling inbound references.** | If renumbering, grep the whole repo for the old filename/`ADR 0002`/`ADR-0002` and update every inbound ref in the same change; or (safer) leave the numbers and add a disambiguation note — decide in Area 5. |
| R11 | **Author a migration guide that contradicts the folded CHANGELOG** (which this phase can't edit). | Migration guide (Area 6) is DERIVED from the folded `## [0.5.0]` CHANGELOG (read-only) + the source-breaking `### Changed` bullets. Cross-check every claim against that section. |
| R12 | **Add a `docs/upgrading/0.5.0.md` link from a deferred-pin file and accidentally touch the pin.** | Adding an inbound link is fine; the guardrail is line-scoped edits — never re-save a pin line. |
| R13 | **Local CI gate not run / not green before PR.** | Definition of done requires the non-DB CI gate (fmt, clippy `-D warnings` on touched crates, `cargo build --examples`, no-DB doc/unit tests) green locally. DB suites compile-checked. |
| R14 | **Scoped-tokens/composed-mode doc authored from the missing memory file** (which does not exist). | Derive that doc/section from the `pr-1102-scoped-api-tokens.md` changelog fragment + the plugin auth code, NOT from the nonexistent `/tmp/claude/memory/team/harvest-followup-1102-composed-mode-doc.md`. |
| R15 | **Silent scope creep** turning a "fix stale claim" into a feature-doc rewrite. | Each area has an explicit file list; new-doc authoring is confined to Area 6 (migration guide + matrix) and explicitly-scoped coverage gaps. |

---

## 3. Six Hats

**White (facts we have).** Folded `## [0.5.0]` CHANGELOG extracted (58 fragments + ctx.mutex).
138 `docs/**/*.md` files; 65 example `.rs` files across 6 crates + top-level example
crates; README (54 KB), skills/, RELEASE_NOTES (stale @0.4.0), sqlite README. Six
deferred Cargo-pin locations + the scaffold template. Duplicate ADR-0002 confirmed. No
`docs/upgrading/`. No dedicated scoped-tokens doc. Trunk-dev is pre-version-bump; 0.5.0
= trunk + #1124 + #1125.

**Red (biggest risk / pain).** The autumn-web 0.5→0.6 / diesel-async 0.8→0.9 snippet
compilability is the scariest: we cannot fully verify 0.6 API snippets here (no 0.6 on
crates.io, #1124 dirty). Second: accidentally breaking `harvest new` by touching a
deferred pin. Third: the sheer surface (138 md + 65 examples) risks shallow coverage.

**Black (what will bite us).** (a) Prose that says "autumn-web 0.5" is *true today* but
*false for the release* — updating it risks inconsistency with an adjacent deferred toml
pin still at 0.5. (b) The org rename (madmax983→autumn-foundation) touches every badge
and many links — easy to over- or under-correct. (c) DB examples can't be executed →
"verified" claims must be hedged. (d) RELEASE_NOTES.md and api-contract.json version
strings are ambiguous scope (generated? hand-edited? superseded?). (e) Merge-order
discipline — this PR is third in line behind two open drafts.

**Yellow (highest-leverage wins).** (1) A `docs/upgrading/0.5.0.md` migration guide —
net-new, high value, derivable from the CHANGELOG, low conflict risk. (2) Fixing the
handful of renamed-symbol/route drifts (`rate_limit_saturated`, child-failure
`ActivityFailed`→`WorkflowFailed`, `409`→`200` resume) — small, high-correctness. (3)
The feature-presence matrix — cheap, makes reviewer confidence high. (4) telemetry.md
metric-catalogue additions — bounded, high operator value.

**Green (creative coverage).** Migration guide + feature-presence matrix (task §4/§6). A
"new in 0.5.0" index/landing section. A per-area RED checklist so each worker produces
evidence (file:line + claim + expected) BEFORE fixing. Consider a scripted grep-gate
for the deferred pins and the old org name as a mechanical safety net.

**Blue (process / sequence / DoD).**
1. (this phase) Branch + push ✔; extract CHANGELOG ✔; inventory ✔; write plan+partition
   ✔; persist to scratchpad + `docs-sweep-wip/` + push.
2. (red phase) Each area worker produces an evidence list (stale claim → file:line →
   expected fix) WITHOUT editing content.
3. (green phase) Fixes applied per area, deferred pins + hard exclusions untouched,
   snippets compile-checked, migration guide + matrix authored.
4. (gate) Local non-DB CI green; DB suites compile-checked; grep-gate for deferred
   pins/org name; `docs-sweep-wip/` git-rm'd; PR opened, marked "merge after #1124 &
   #1125".

---

## 4. Definition of Done / Acceptance Criteria

A. Every documented public method / macro attribute / type referenced in `docs/**`,
   README, skills, and example `.rs` bodies **exists in the released 0.5.0 API** (no
   renamed/removed-symbol drift; #767 destructures carry `..`;
   `rate_limit_saturated`→`rate_limit_exhausted`; child-failure `WorkflowFailed`).
B. Every `GET/POST/PATCH/DELETE /api/harvest/...` route string in docs **exists** in
   `docs/api-contract.json` / `management_api_routes()`; new 0.5.0 routes have at least a
   mention in `management-api.md`.
C. Every CLI subcommand/flag in docs & README exists; new 0.5.0 subcommands mentioned.
D. Every touched Rust snippet / example **compiles** against the released 0.6.0 /
   diesel-async 0.9 baseline (compile-checked; DB/testcontainers examples compile-only,
   explicitly hedged).
E. README + skills repo links resolve to **autumn-foundation/autumn-harvest** EXCEPT
   genuine framework/dependency links (flagged & left).
F. Every new-in-0.5.0 user-visible feature has **at least a mention** in docs (matrix
   §5 shows coverage); confirmed gaps (scoped tokens/composed-mode, cancellable timers,
   interceptors, summary retention, legal hold, DAG signal gates) closed to at least a
   documented paragraph or dedicated section.
G. **Migration guide exists** at `docs/upgrading/0.5.0.md`, derived from the folded
   CHANGELOG, covering: autumn-web 0.6 / diesel-async 0.9 bump, the #767 source-breaking
   destructure + child-failure behavior change, and any other `### Changed` breakers.
H. New metrics/labels/migrations added to `telemetry.md` / ADR-0001 §7 / dashboards /
   alerts where those catalogues live.
I. Duplicate ADR-0002 resolved (renumber-with-refs OR disambiguation note).
   RELEASE_NOTES.md 0.5.0 decision made & applied.
J. **Deferred pins untouched** (grep-gate clean). **Hard exclusions untouched**
   (`git diff --name-only` clean of them). `docs-sweep-wip/` removed before PR.
K. Local non-DB CI gate green (fmt, clippy `-D warnings` on touched crates,
   `cargo build --examples`). DB suites compile-checked only, hedged.

---

## 5. NEW-IN-0.5.0 FEATURE → DOCS-COVERAGE MATRIX (authoritative)

Extracted from `git show origin/release/v0.5.0:CHANGELOG.md` `## [0.5.0]`. "Needs docs?"
= plausibly warrants a guide/runbook/example/mention beyond the changelog. "Existing doc"
= where coverage likely already lives (verify in red phase). Area = partition assignment.

### Upgraded
| Feature | Issue/PR | Needs docs? | Existing doc / target | Area |
|---|---|---|---|---|
| autumn-web 0.6.0 / diesel-async 0.9 bump | #1124 | **YES (migration guide + prose)** | NEW `docs/upgrading/0.5.0.md`; prose in mcp-tools, project-skeleton, telemetry | 6 (+2,5) |

### Added (user-visible)
| Feature | Issue/PR | Needs docs? | Existing doc / target | Area |
|---|---|---|---|---|
| Typed workflow failures `WorkflowFailure` | #767 | YES | `docs/typed-workflow-failures.md`, workflow-determinism-guide, migration guide (breaking) | 2,6 |
| Activity execution interceptors | #680 | YES | none found → new section (activities/reliability) + example `activity_interceptor.rs` | 2 |
| Non-blocking signal drain (aggregator) | #775 | YES | `getting-started/04-signals.md` + `signal_aggregator.rs` | 1 |
| Declarative DAG signal/approval gate nodes | #746 | YES | `getting-started/08-dags-and-schedules.md` + `dag_approval_gate.rs` | 1 |
| Tiered / summary retention | #752 | YES | `docs/archival.md` / retention runbook + CLI | 3,5 |
| Per-execution legal hold | #747 | YES | retention/security docs + management-api + CLI | 2,3 |
| Cancellable / renewable durable timers | #768 | YES | `getting-started/03-durable-timers.md` + `cancellable_timer_sla.rs` | 1 |
| Handler-panic containment | #782 | YES | reliability-knobs + telemetry (2 new counters) | 2 |
| Continue-as-new run-chain timeline API | #701 | YES | management-api + CLI | 2 |
| Eligibility explainer circuit-breaker codes; `rate_limit_saturated`→`_exhausted` | #611 | YES (rename!) | `runbooks/triage-pending-tasks-idle-workers.md`, circuit-breaker runbook | 3 |
| proptest + cargo-fuzz harness | #1004 | maybe | `docs/testing/property-and-fuzz.md` (exists) | 4 |
| loom model checking | #1007 | maybe | `docs/testing/loom.md`, `concurrency-model-checking.md` (exist) | 4 |
| In-place schedule update `PATCH /admin/schedules/{id}` | #771 | YES | schedule runbooks + management-api + CLI | 3,2 |
| Operator force-fail activity `fail-now` | #765 | YES | triage runbook + management-api + CLI | 3,2 |
| Pause/resume CLI + `409`→`200` behavior change | #609 | YES (breaking-ish) | `runbooks/contain-runaway-execution.md` (exists), operations, migration guide | 3,6 |
| Durable completion callbacks | #605 | YES | `docs/completion-callbacks.md` (exists) — verify | 2 |
| Inbound HTTP webhook receiver `#[webhook]` | #344 | YES | `getting-started/12-webhooks.md` (exists) — verify | 1 |
| `#[workflow(mcp)]` durable MCP tools (+ many hardening) | #597 | YES | `docs/mcp-tools.md` (exists) — verify autumn-web 0.6 prose | 2 |
| Per-tenant usage report `/admin/usage` | #596 | YES | `docs/sharding.md` + management-api + CLI | 2,3 |
| Grouped workflow-count `/workflows/count` | #544 | YES | management-api + CLI | 2 |
| `WorkflowSimulator` honors RetryPolicy | #541 | YES | `getting-started/11-testing.md` | 1 |
| Paginated workflow history API | #529 | YES | management-api | 2 |
| Workflow-type reachability check (+#700 followup) | #520/#700 | YES | `runbooks/safe-handler-removal.md` (exists) + CLI | 3 |
| Deadline-aware continue-as-new | #772 | YES | reliability-knobs / can docs | 2 |
| Child-or-deadline waits `execute_child_workflow_timeout` | #779 | YES | `getting-started/05-child-workflows.md` + `child_with_timeout.rs` | 1 |
| Signal & update lifecycle metrics (6 counters) | #684 | YES | `docs/telemetry.md` | 2 |
| Business-`workflow_id` HTTP addressing | #805 | YES | management-api | 2 |
| `harvest det-check` CLI | #778 | YES | workflow-determinism-guide + CLI | 2 |
| `ctx.info()` run metadata | #698 | YES | reliability/context docs + `ctx_info.rs` | 2 |
| `autumn-harvest-sqlite` companion crate | #1068 | YES | `docs/sqlite-backend.md`, `autumn-harvest-sqlite/README.md` (exist) | 5 |
| WASM-sandboxed polyglot activities (R&D spike) | #965 | maybe | `docs/rnd/wasm-activities-spike.md` (exists) | 5 |
| Cause-targeted bulk DLQ replay/discard + facets | #613 | YES | DLQ runbook / management-api + CLI | 3,2 |
| Vantage UI DAG run-graph + timeline/Gantt pages | #957/#960 | YES | `docs/vantage-ui.md` | 5 |
| Read-only operator role | #776 | YES | `docs/operator-role.md` (exists) — verify | 3 |
| Published interaction schema + `/interface` | #610 | YES | schema docs + management-api | 2 |
| Workflow start provenance `StartSource` | #740 | YES | management-api / context docs | 2 |
| Live output streaming `ctx.publish_progress` | #791 | YES | `docs/streaming-progress.md` (exists) — verify | 2 |
| **Scoped API tokens + rotation (composed/standalone auth)** | #942 | **YES — likely a real GAP** | none dedicated → new doc/section (security-posture) + management-api + CLI | 3,2 |
| `harvest new` scaffolding CLI | #692 | YES | `getting-started/01-project-skeleton.md` (deferred pins!) | 1 |
| Auto-heartbeat guard | #682 | YES | activities/reliability | 2 |
| Active-workflow gauge `harvest.workflow.active` | #770 | YES | `docs/telemetry.md` | 2 |
| `ctx.await_external_workflow` | #757 | YES | context docs + `await_external_workflow.rs` | 2 |
| Bounded/windowed activity fan-out | #750 | YES | fan-out docs + `fanout_batch.rs` | 2 |
| Chain-scoped lifetime cap `chain_execution_timeout` | #617 | YES | reliability-knobs | 2 |
| Replay-diagnosis endpoint | #614 | YES | management-api / replay docs | 2 |
| `WorkflowIdConflictPolicy` idempotent starts | #685 | YES | `getting-started/06-idempotency.md` | 1 |
| Server-side overdue-schedule detection | #696 | YES | schedule runbooks + management-api | 3 |
| Per-key activity rate limits (nested attr) | #699 | YES | reliability-knobs / worker-routing + macro | 2 |
| DAG node input binding | #702 | YES | `getting-started/08-dags-and-schedules.md` + `dag_data_flow.rs` | 1 |
| Dry-run preview for batch operations | #769 | YES | management-api | 2 |
| Update duration histogram `harvest.update.duration` | #781 | YES | `docs/telemetry.md` | 2 |
| Synthetic liveness canary | #796 | YES | operations / telemetry | 3 |
| **ctx.mutex durable mutex (1 commit ahead of release branch)** | #1122 | YES | none dedicated → CLAUDE.md has it; assess a `docs/*` section | 2 |

### Changed (behavior/source-breaking → migration guide)
| Change | Issue | Migration-guide item? | Area |
|---|---|---|---|
| Failed child → `WorkflowFailed` (was `ActivityFailed{child-workflow:..}`) | #767 | **YES (breaking)** | 6 |
| Additive public fields on 5 variants/structs → add `..` | #767 | **YES (source-breaking)** | 6 |
| Admission gate authoritative across producers; `/admin/gates` contract | #618 | prose (admission-gate-producers.md exists) | 3 |
| Throttle scanner fire-time gate; `202` contract change | #1053 | prose (admission docs) | 3 |
| sqlite v0.1 hardening | #1068 | sqlite docs | 5 |
| Cross-shard partial-availability reads (200 + `unavailable_shards`) | #756 | management-api / sharding | 2 |

### Fixed / Internal
Mostly test/CI/no-behavior-change (SSRF #1006, /stack fold #1022, replay follow-ups
#678/#1034/#1048/#1071/#1084/#1089, gate ordering #685/#1092/#1103, comparison page #963).
Docs impact limited to: `docs/comparison.md` (exists, #963 — verify), any runbook that
described the pre-fix behavior. Assess in the owning areas; not migration-guide items.

---

## 6. DEFERRED PIN SET & HARD EXCLUSIONS (summary — full detail in partition.md §Deferred)

**DEFERRED PINS — resolvable Cargo-pin toml snippets that MUST stay at published
versions until 0.5.0 is on crates.io. Fix OTHER content in these files; NEVER edit the
pin lines.** (Verified file:line this session.)
- `autumn-harvest-cli/templates/minimal/Cargo.toml.tmpl` L7-9 (harvest 0.4 / plugin 0.4 / web 0.5)
- `docs/autumn-workflow-architecture.md` L77-78 (web 0.5 / harvest 0.4)
- `docs/replay-verify.md` L29 (harvest 0.3, `testing` feature — note: **0.3**, not 0.4)
- `docs/sqlite-backend.md` L71-72 (harvest 0.4 / sqlite 0.4)
- `docs/telemetry.md` L18 (plugin 0.4, `metrics` feature)
- `docs/getting-started/01-project-skeleton.md` L18-20 (harvest 0.4 / plugin 0.4 / web 0.5)
- **SURPRISE:** README.md has **no** resolvable toml Cargo-pin block (task listed it as
  a deferred-pin file). It uses a crates.io badge + example pointers instead. README's
  only version reference is narrative `Version 0.4.0` (L912) — that's a NARRATIVE mention
  (candidate to update), not a deferred pin.

**HARD EXCLUSIONS — never touch:** `CHANGELOG.md`; the CLAUDE.md `### Phase Status` list;
any `Cargo.toml` / `Cargo.lock`; `docs/changelog.d/*` fragments (owned by #1125's fold).

---

## 7. Recommended migration-guide path & conventions

- **No `docs/upgrading/` exists.** Recommended path: **`docs/upgrading/0.5.0.md`**
  (new directory). Rationale: SemVer-versioned upgrade notes, discoverable, doesn't
  collide with `docs/plans/` (historical) or `RELEASE_NOTES.md` (changelog-style).
- Add an inbound link from `README.md` and `getting-started/README.md`. (Link-add only;
  never re-save a deferred-pin line.)
- Content DERIVED from the folded `## [0.5.0]` CHANGELOG `### Upgraded` + `### Changed`.
  Sections: (1) Dependency bump (autumn-web 0.5→0.6, diesel-async 0.8→0.9) — what an
  embedder's `Cargo.toml`/`AppBuilder` needs; (2) Source-breaking `#767` (exhaustive
  destructure `..`; child-failure `WorkflowFailed`); (3) Behavior changes (resume
  non-paused `200`; throttle `202` contract; cross-shard partial reads); (4) New-feature
  quick index with links.

---

## 8. Surprises / open decisions (for the fixing phase / user)

- **S1.** README has no toml Cargo pin (task assumed one). No deferral concern there;
  only its narrative `0.4.0` line.
- **S2.** `docs/replay-verify.md` pins `autumn-harvest = "0.3"` (not 0.4) — still a
  deferred pin, just older. Left as-is.
- **S3.** `RELEASE_NOTES.md` stops at 0.4.0 (no 0.5.0 section). Decide: extend vs. mark
  superseded by CHANGELOG. (CHANGELOG is excluded, but RELEASE_NOTES is not.)
- **S4.** `docs/api-contract.json:2 "version":"0.4.0"` and `api-contract-guide.md:14`
  example — assess whether hand-editable/generated and whether it should read 0.5.0.
- **S5.** Duplicate ADR-0002 confirmed: `0002-payload-codec-event-boundary.md`
  ("Event payload codec boundary", Accepted) and `0002-rust-native-execution-boundary.md`
  ("Rust-Native Execution Boundary", Accepted, 2026-05-03). Resolve in Area 5.
- **S6.** The composed-mode memory file
  `/tmp/claude/memory/team/harvest-followup-1102-composed-mode-doc.md` does **NOT**
  exist. Derive scoped-tokens/composed-mode docs from `pr-1102-scoped-api-tokens.md`
  fragment + plugin auth code. Confirmed: **no dedicated scoped-API-tokens doc exists** →
  genuine coverage gap (Area 3).
- **S7.** Cannot execute DB/testcontainers here (no Postgres). All DB-example claims
  hedged "compile-checked only".
- **S8.** autumn-web 0.6 not on crates.io from this sandbox → 0.6 API snippet
  correctness must be confirmed against the `origin/release/autumn-web-0.6-upgrade`
  branch diff, else flagged REVIEW rather than guessed (R3).
- **S9.** Prose "autumn-web 0.5" vs. adjacent deferred toml pin "autumn-web 0.5":
  updating prose to 0.6 while the nearby resolvable pin stays 0.5 creates a visible
  inconsistency — flag each such site as a REVIEW decision (keep prose factual about the
  released baseline OR add a "pins update when 0.5.0 publishes" note).
