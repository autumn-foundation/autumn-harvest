## Phase 3.x — Opt-in declared workflow dependencies validated at preflight (issue #802)

**Implemented.** A `#[dag]` declares its activity references structurally, so
`check_catalog_consistency` has always been able to fail a DAG that names an
unregistered activity *before* rollout. An **imperative** `#[workflow]` has no
such structure: `ctx.execute_activity(&send_email_info(), …)` compiles cleanly
even when `send_email` was never added to `activities![…]`, so a forgotten
registration is invisible to preflight and only surfaces at runtime — one
dispatch, one retry cycle, one dead letter later, typically minutes-to-hours
into a run and *after* the workflow has already done partial work.

This closes that asymmetry with an **opt-in declaration** the author writes and
preflight verifies:

```rust
#[workflow(activities = [send_email, charge_card], children = [generate_report])]
async fn onboarding(ctx: &WorkflowContext, user_id: i64) -> Result<(), String> { … }
```

Entries are bare identifiers, paths (only the last segment is used — neither
`#[workflow]` nor `#[activity]` has a rename attribute, so the fn ident always
*is* the registered name), or string literals for a name dispatched dynamically
through `ctx.execute_activity_raw("…", …)`. A path is deliberately **not**
name-resolved by the compiler, so a typo stays a *preflight* failure — the exact
deploy-time failure mode the issue specifies — rather than becoming a compile
error that would force the author to import every dependency.

**Three states, deliberately distinct.** Two additive `Option` fields on
`WorkflowInfo` — `declared_activities: Option<&'static [&'static str]>` and
`declared_children` — mirror the existing optional-metadata pattern (`owner`,
`runbook_url`, `severity`, `description`), with `const fn` fluent builders
`with_declared_activities` / `with_declared_children` for hand-registered
workflows. `None` = did not opt in (never validated); `Some(&[])` = opted in and
asserts "I dispatch nothing" (checked, resolves trivially); `Some(&[…])` = every
listed name must resolve. The macro lowers the list to a promoted
`&'static [&'static str]` via rvalue static promotion, so `WorkflowInfo` stays
allocation-free and `Clone`-cheap.

**Preflight.** New pure helper `workflow_unregistered_dependency_failures`
(mirroring the existing `dag_unregistered_activity_failures`) is wired into
`check_catalog_consistency`, resolving `activities` against
`registry().activities` and `children` against `registry().workflows` — two
separate namespaces, so a name registered only as the *other* kind still fails.
Each unresolved reference produces its own named failure string in
`details.failures`:

```
workflow 'onboarding' references unregistered activity 'send_emial'
workflow 'onboarding' references unregistered child workflow 'generate_reprot'
```

Ordering is imposed by the helper (sorted by workflow name, activities before
children, declaration order within each) rather than inherited from
`registry().workflows`, which is a `HashMap` and would otherwise reshuffle the
operator-facing list between calls. A repeated declaration reports its miss
once. A blank name — unreachable through the macro, which rejects it at compile
time, but reachable through the fluent builder — is reported as the authoring
error it is (`declares an empty activity name`) rather than as a confusing
`unregistered ''`.

**Zero false positives, by construction.** The helper filters to opted-in
workflows *before* doing any work, so a workflow that never declared anything
contributes nothing regardless of what is (or isn't) registered — an existing
catalog's preflight verdict is unchanged, and there is no author burden for
teams that don't adopt it.

**Discovery.** `GET /workflows/registered` (Phase 3.19) surfaces the declaration
per workflow type via two additive
`#[serde(skip_serializing_if = "Option::is_none")]` fields on
`RegisteredWorkflowRecord`, so a non-opted-in workflow's record is byte-identical
to before and `Some(&[])` is distinguishable from `None` on the wire.

**Deliberate boundaries.** The declaration is **not** verified against what the
body actually dispatches — `ctx.execute_activity_raw(dynamic_name, …)` cannot be
extracted from compiled code, which is exactly why this is explicit and opt-in
rather than inferred (automatic body analysis is out of scope per the issue). A
stale declaration therefore over-reports (it can fail preflight for a dependency
no longer used) but never under-reports a name it does list. Also out of scope
and unchanged: input/output type compatibility (#373), cross-shard/worker
reachability (that remains the separate `worker_health` / `queue_coverage`
question), and the runtime dispatch error message. The `autumn-harvest-sqlite`
backend accepts both fields **inert** (they are pure deploy-time metadata the
core execution path never consults), consistent with its treatment of `owner` /
`runbook_url` / `severity` / `description`.

**No new `WorkflowEvent` variant, no migration, no replay/determinism impact, no
`::autumn_harvest::` macro-path change** — the core crate never reads either
field; it is consulted only by the preflight check and the discovery endpoint.

**Operator surface.** `harvest preflight`'s table renders only each check's
one-line `SUMMARY`, so `details.failures` — the entire payload this feature
produces — was invisible unless the operator independently knew to re-run with
`--output json | jq`. `format_preflight_table` now appends a detail block for
every non-`pass` check listing its failures and its `remediation`, making a
failing preflight (and its non-zero CI exit) actionable in-band. This also
surfaces the pre-existing DAG, schedule-resolvability, and worker-coverage
failures, which were equally hidden; a healthy fleet's output is unchanged, and
both `failures` element shapes (plain string, structured object) are rendered.
`check_catalog_consistency`'s `remediation` was widened to name **both** fixes —
preflight cannot read workflow bodies, so it cannot distinguish "you forgot to
register the handler" (a runtime hazard) from "you left a stale declaration
behind" (cosmetic); failing closed is correct, but the operator needs to know
the fix is one line either way.

**Compile-error quality.** `#[workflow]`'s existing vocabulary has exactly two
container idioms — `key = "string"` (`owner`, `runbook`, `severity`,
`description`) and `key(a = .., b = ..)` (`concurrency`, `debounce`, `batch`,
`throttle`) — and this introduces a third, `key = [..]`. Both wrong-container
first attempts (`activities = "send_email"`, `children(generate_report)`) are
caught explicitly and answered with a message that names the attribute and shows
the correct form, rather than falling through to a raw `syn` "expected square
brackets" / "expected `=`".

**Docs.** `docs/getting-started/10-operations.md` gains a "Catching a forgotten
registration before rollout" section under Preflight (the attribute and builder
forms, the entry vocabulary, the `details.failures` shape, the
opt-in/silence-is-not-verification caveat, the cross-check-against-your-
registration-lists mental model — the attribute *asserts*, it never registers —
the never-name-resolved/aliased-import note, the split-registration caveat, and
the two deliberate non-goals), with a rendered `harvest preflight` transcript
rather than a raw JSON blob. Because `children` is checked by name against the
workflow catalog it **also** covers a cross-type `continue_as_new_as` target
(#803) — the other imperative workflow-type reference preflight cannot otherwise
see — which is now stated in both `10-operations.md` and
`docs/runbooks/safe-handler-removal.md`. The authoring chapters
`02-first-workflow.md` and `05-child-workflows.md` each cross-link the section
from the exact `activities![…]` / `workflows![…]` registration line that gets
forgotten, and `CLAUDE.md`'s stale *"`#[workflow]` takes no attributes in Phase
1"* is replaced with the real key list. `docs/runbooks/harvest-alerts.md` adds
the new failure class to the `harvest_preflight_failed` likely-causes list.

**Tests.** `preflight.rs::tests::declared_deps` — 12 pure helper tests (AC6a–d,
empty-vs-absent intent pin, within-workflow and cross-workflow dedupe, blank
name, deterministic ordering, self-recursion, separate namespaces) plus 5
`check_catalog_consistency` wiring tests (Fail-and-names, Pass-when-complete,
non-opted-in unchanged, co-occurring with a pre-existing failure, and the exact
mixed-catalog failure set through the real `HashMap`-backed registry);
`macros_workflow.rs` — 5 expansion tests covering every path qualifier form
(`crate::` / `self::` / `super::` / leading `::` / bare ident), string literals,
the absent case, the explicitly-empty case, and the fluent builders;
`info.rs` — 4 record/`Debug` tests plus a DAG-shadow pin; five new trybuild
`compile_fail` fixtures pin the blank-name rejection, the non-name-entry
rejection, both wrong-container rejections, and the widened `#[workflow]`
unsupported-attribute whitelist; `interface_schema_integration.rs` — 2 route
tests proving both discovery endpoints surface the declaration end-to-end;
`autumn-harvest-cli` — 6 renderer tests pinning that failures and remediation
are rendered beneath the table, that a passing check contributes nothing, and
that structured failure objects are not dropped. All run in CI via
`cargo test -p autumn-harvest-plugin --lib`, the no-DB `--test integration` step
(which carries `macros_compile_fail`), `cargo test -p autumn-harvest-cli`, and
the Docker-backed `interface_schema_integration` manifest row.
