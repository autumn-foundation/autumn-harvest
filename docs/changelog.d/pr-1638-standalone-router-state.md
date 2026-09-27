## Phase — Both Harvest routers return `Router<()>` (issues #1606, #1607)

First of epic #1605's two roots. `harvest_api_router` and
`harvest_ui_router` returned `Router<autumn_web::AppState>`, and that
type parameter is what forced every non-autumn-web embedder to construct
an `AppState` it had no other use for. The reference example did it by
calling `autumn_web::AppState::for_test()` in `run()` — a test-only
constructor on its real startup path — purely to satisfy the type.

**The coupling was nominal.** Across ~54k lines of `api.rs`, exactly one
handler read the state. `preflight` took a `State<AppState>` extractor to
copy the deployment profile onto `HarvestApiState`, which already carries
it. `ui.rs` never read the state at all. Every other handler in both
routers receives its state through `.layer(Extension(api_state))`.

**The copy was also dead both ways.** On the plugin path
`start_harvest_runtime` already sets the profile at startup, so the
handler's write was redundant. On a standalone mount it was unreachable:
`preflight` sits behind the admin gate that reads the very profile the
handler would have set, so the request is rejected before the handler
runs. `preflight` now reads the profile `HarvestApiState` holds, and
`start_harvest_runtime` is the only writer.

**The change.** Both router signatures become `Router<()>` and the
`autumn_web::AppState` imports leave `api.rs` and `ui.rs`. `plugin.rs`
keeps its whole internal stack state-free: `ApiMiddlewareFn` now maps
`Router<()>` to `Router<()>`, and the autumn-web state type is declared
once, at the `AppBuilder::nest` boundary, which takes a
`Router<AppState>`. The layer stack is state-agnostic, so the
load-bearing ordering at `plugin.rs` (token layer outside the
read-only-class layer, both inside the embedder's auth) is untouched.

**Issue #1607 closes with it.** The example's `build_router` loses its
`web_state` parameter and `run()` loses the `AppState::for_test()`
binding. That call site cannot compile after the router change, so the
two issues land together rather than leaving the reference example
carrying a test constructor in production for a release.

**Scope.** `AutumnError` stays, per #1606's own scope note: once the
router is `Router<()>` it is an implementation detail behind
`IntoResponse` rather than a type an embedder has to name. Reaching a
credential on a standalone mount is #1608 and is not in this change —
`/admin/preflight` standalone is still `401`, and a test now pins that.

**Call-site churn.** 295 `.with_state(AppState::…)` calls across 93 test,
bench and example files became redundant and were removed, with the
imports and helper functions they left dead. No assertion changed. One
test was renamed: `harvest_api_uses_installed_storage_pool_when_app_state_has_no_database`
is now `harvest_api_uses_the_installed_storage_pool`, since there is no
longer an `AppState` for it to name.

No new `WorkflowEvent` variant, no migration, no replay impact.

**Tests.** New no-database suite
`autumn-harvest-plugin/tests/standalone_mount.rs`, registered in
`.github/ci/integration-suites.txt` as an `allos` row. It composes the
API router with the Vantage router nested inside it onto a bare
`axum::Router`, with no `.with_state(...)` call anywhere in the file, so
a regression to `Router<AppState>` fails the suite at compile time before
any assertion runs. It also pins the new profile source (the
`admin_auth_boundary` check reports the profile set on
`HarvestApiState`) and the unchanged fail-closed admin gate. Existing
`security`, `openapi_spec`, `effective_config_http_tests` and the
`ci_run_coverage` manifest guard pass unchanged.
