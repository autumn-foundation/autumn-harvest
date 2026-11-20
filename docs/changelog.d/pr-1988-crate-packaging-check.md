## Check crate packaging on release PRs, and state a support window (issue #1988)

Publishing stays manual. CI holds no registry token. The design is in
`docs/design/DESIGN-1988.md`.

- `cargo xtask check crate-packaging` runs `cargo package --locked` for each
  publishable crate in one run. Cargo verifies each crate against a local
  registry that holds its siblings. The command then reads each `.crate`
  file. A file over 10 MiB fails, and a file at 8 MiB or more gives a
  warning. `autumn-harvest` is above the warning size at the time of
  writing.
- The new `crate-packaging` job of `release-pr.yml` runs the full check on
  each release pull request, with a read-only token and no secret.
- `docs/releasing.md` gives the release steps and the manual publish order.
  `--plan-only` checks the order against `cargo metadata` in the `lint` job
  of each pull request. It fails on a publishable crate that the order
  omits, a listed crate that is not publishable, a crate listed twice, a
  crate before its dependency, a dependency on an unpublished crate, and a
  crate with no `description` or `license`.
- `docs/upgrading/README.md` states the support window. The newest minor
  release gets fixes. An older minor release gets none after the next
  minor release ships. `README.md` and `SECURITY.md` link to it.

Tests: unit tests in `xtask/src/check/crate_packaging.rs`. Three guard
tests in `autumn-harvest/tests/integration/release_pr_ci.rs` check the
release job, the lint step, and that no workflow holds a registry
token or publishes.
