# RED→GREEN findings — Area 1+2 (getting-started + core concept docs)

Sweep of autumn-harvest docs against released 0.5.0 API. Ground truth = crate source
(`autumn-harvest/src`, `autumn-harvest-plugin/src`, `autumn-harvest-cli/src`,
`autumn-harvest-macros/src`) + `docs/api-contract.json`. Legend: **FIX** = applied,
source-verified; **REVIEW** = left for a later phase (0.6-dep-sensitive / ownership /
judgment); **GAP** = missing coverage (added a mention/pointer where feasible).

Environment: Postgres NOT running here — DB examples compile-checked only; autumn-web 0.6
API not verifiable from this sandbox (release PR #1124 open draft), so 0.6 code-signature
questions are REVIEW, never guessed.

Deferred pins in my area (NEVER edited): `01-project-skeleton.md` L18-20
(`autumn-harvest="0.4"`/`autumn-harvest-plugin="0.4"`/`autumn-web="0.5"`);
`telemetry.md` L18 (`autumn-harvest-plugin` `version="0.4"`); `replay-verify.md` L29
(`autumn-harvest` `version="0.3"`). Verified untouched.

---

## Confirmed source facts used throughout

New-in-0.5.0 `WorkflowContext` APIs confirmed present in `context.rs`: `start_timer`
(returns `TimerHandle<'a>`; `TimerOutcome` enum), `cancel_timer`, `reset_timer`,
`sleep_until`, `try_receive_signal`, `drain_signals`, `drain_signals_raw`,
`drain_signals_collect`, `try_wait_for_signal`, `execute_child_workflow_timeout`,
`spawn_child_workflow_timeout`, `patched`, `deprecate_patch`, `info`, `mutex`,
`publish_progress`, `await_external_workflow`. DAG signal gates confirmed in `dag.rs`
(`GateTimeoutAction`, `DagSignalGate`, `DagBuilder::signal_gate`,
`signal_gate_with_timeout`). `WorkerConfig::with_workflow_panic_max_attempts` +
`workflow_panic_max_attempts` field confirmed in `builder.rs`.

Metric catalogue extracted from `telemetry.rs` (used for telemetry.md cross-check) — see
the telemetry.md section below.

---

## docs/mcp-tools.md

- **FIX** L13: "Built on autumn-web **0.5**'s MCP layer" -> "autumn-web **0.6**'s MCP
  layer". Pure version-narrative prose, no adjacent Cargo pin in this file (verified).
  The 0.5.0 release ports to autumn-web 0.6 (#1124), and this docs PR merges after it.
  The `AppBuilder::mount_mcp("/mcp")` API name in the same paragraph is left unchanged
  (REVIEW — verify against autumn-web 0.6; not a version string).

## docs/getting-started/01-project-skeleton.md

- **GAP -> FIX**: the chapter never mentioned `harvest new` (issue #692), the shipped
  scaffolding CLI (`autumn-harvest-cli/src/lib.rs` `New` variant ~L631; template dir
  `autumn-harvest-cli/templates/minimal/`). Added a "Shortcut" callout pointing at
  `harvest new <name>`, above the manual `Cargo.toml` block. Did NOT touch the deferred
  pin lines (L18-20).
- Verified the manual `main.rs` wiring (`autumn_web::app().plugin(HarvestPlugin::new()
  .worker(...).api("/api/harvest")).run()`) matches the shipped template
  `main.rs.tmpl` pattern (`.plugin()`, `.worker()`, `.api()`). **REVIEW**: the doc's
  zero-route hello-world may need a `.routes(...)` under autumn-web (the template adds
  `.routes(routes![index])` with a comment "autumn-web requires at least one HTTP
  route") — left as-is (pre-existing, 0.6-sensitive).

## docs/api-contract-guide.md

- **REVIEW** L14: example `"version": "0.4.0"`. The guide says the contract version
  "matches the `autumn-harvest-plugin` crate version". That crate bumps to 0.5.0 via
  release PR #1125, which also owns `docs/api-contract.json`'s `version` field (L2, also
  `"0.4.0"`). Editing either here risks conflicting with #1125 (R6); the version field is
  release-process-owned, not docs-owned. Left both as-is; flagged for the release/gate
  phase to reconcile once #1125 lands.

## docs/typed-workflow-failures.md

- **OK** (no fix needed): L72-76 correctly documents the #767 change — a failed child now
  surfaces as `HarvestError::WorkflowFailed` (the `ActivityFailed{name:"child-workflow:.."}`
  mention is explicitly the *pre-#767* behavior, in the "Behavior change (upgrading)"
  section). Accessors `workflow_error_type()`/`workflow_details()`/
  `is_workflow_non_retryable()` all verified in source.

<!-- Remaining per-file findings appended below as agent evidence is integrated. -->
