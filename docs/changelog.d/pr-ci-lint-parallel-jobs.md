## Tooling — Split the CI `lint` job so clippy runs in parallel

**CI-only change** (implemented). The `lint` job ran about 35 cargo
invocations one after another: clippy on each crate and plugin feature set,
plus eight docs-snippet compile checks. A green trunk-dev run took 62 minutes,
and a cold-cache run took longer. `test`, `test-nodb` and `test-db-linux` need
`lint`, so the test matrix waited the full hour before it started.

- `lint` keeps the fast checks: the grep scripts, `cargo fmt`, the docs-only
  guard tests, the Python audits and comment hygiene. Every guard that pins a
  step to `lint` still holds.
- Clippy moves to six parallel jobs: `clippy-core`, `clippy-plugin`,
  `clippy-plugin-backends`, `clippy-plugin-cloud`, `clippy-crates` and
  `clippy-examples`. The snippet compile checks move to `doc-snippets`. No
  clippy command or feature set changes. Each group is sized from the step
  timings of the last green run.
- `lint-compile` (check name `Lint (compile)`) needs every new job and fails
  when one fails. Branch protection should require it beside `Lint`.
- The new jobs set `cache-on-failure: true`. trunk-dev runs are often red or
  cancelled, so the old `lint` cache was seldom saved, and pull requests
  started cold ("No cache found").

The test matrix now waits only for the fast `lint`, so a clippy failure no
longer skips the tests. `Lint (compile)` still reports it.

`autumn-harvest-verify/tests/ci_wiring.rs` now finds the verify clippy step in
`clippy-crates`.
