# DESIGN-1988 — Crate packaging check and support window

Issue #1988. Parent epic #1969. Related: #955, #1616, #1826.

## 1. Problem

Nothing checks that the workspace crates package before release day. A bad
`include`, a path dependency with no `version`, or a crate over the
crates.io size limit shows only during the manual publish, after the tag.
The project is 0.x and states no support window.

The maintainer decided that publishing stays manual. CI holds no registry
token, and `release.yml` gets no `cargo publish` or `npm publish` step.

## 2. Facts

- The workspace has 29 members. Eight have no `publish` key, so they go to
  crates.io. Every other member already sets `publish = false`.
- `cargo package --workspace` also packages the `publish = false` members.
  It fails on the first unpublished path dependency. So the check names
  each publishable crate with `-p`.
- `cargo package` with several `-p` flags verifies each crate against a
  local overlay registry. A dependency on a sibling crate of the same
  version resolves with no upload. It needs no token.
- `cargo package` only warns about a missing `description` or `license`.
  crates.io refuses the upload.
- crates.io refuses a `.crate` file over 10 MiB. Today `autumn-harvest` is
  8.5 MiB. Its `tests/` tree is 16 MiB of 38 MiB before compression.
- No page holds the release steps. `SECURITY.md` says that security fixes
  go into the newest minor release only.

## 3. Brainstorming

Options for each question. The chosen option is in bold.

| Question | Options |
|---|---|
| Where the logic lives | inline shell in the workflow · **an xtask command, as the repository rules ask** |
| Which xtask group | `audit` · `ci` · **`check`, next to `check license-files`, which reads the same crate list** |
| How to find the crates | a hard-coded list · **`cargo metadata`: `publish` null or one that names `crates-io`** |
| Where the publish order lives | in the xtask source · **in `docs/releasing.md`, between two markers, so one list serves the reader and the gate** |
| How to prove the order | trust the reader · **check it against the dependency graph of `cargo metadata`** |
| What counts as a dependency | normal only · **normal, build, and a dev dependency with a version. Cargo strips a path-only dev dependency** |
| Package command | `cargo publish --dry-run` · **`cargo package --locked -p … -p …`: no registry login, one overlay for all crates** |
| Size check | none · **read each `.crate` file. Fail over 10 MiB. Warn over 8 MiB** |
| Where it runs | release PRs only · **the cheap plan check on each PR in `lint`, and the full packaging on release PRs** |
| Support policy home | `README.md` · **`docs/upgrading/README.md`, linked from `README.md` and `SECURITY.md`** |

## 4. Reverse brainstorming

"How could a bad crate still reach release day?" Each answer gives a rule.

| Failure | Rule |
|---|---|
| A new member has no `publish` key, so it is published by mistake. | The plan check fails on a publishable crate that the order does not list. Each PR runs it. |
| A crate is removed or renamed, and the order still lists it. | The plan check fails on a listed crate that is not publishable. |
| The order puts a crate before its dependency, so the manual publish fails part-way. | The plan check fails on each edge that points forward. |
| A publishable crate depends on a `publish = false` crate. | The plan check names the edge. `cargo package` would only say "no matching package". |
| A crate has no `description` or `license`. | The plan check fails. `cargo package` only warns. |
| A crate grows past the size limit. | The packaging check reads each `.crate` file. A warning at 8 MiB gives notice. |
| `cargo metadata` fails, and the gate prints OK. | The gate fails closed when it has no crate list or no order. |
| A dirty tree hides a missing file. | CI does not pass `--allow-dirty`. |
| A job gets a token, so code on a release PR can publish. | The job has `contents: read` only. A guard test checks that no step sets a `CARGO_REGISTRY_TOKEN`. |
| The old `.crate` file of an earlier run passes the size check. | The check deletes each expected `.crate` file before it packages. |

## 5. Six thinking hats

- **White (facts).** Section 2. Eight crates. Cargo orders and verifies a
  multi-crate package run. The size limit is 10 MiB.
- **Red (feeling).** A failed publish after the tag is stressful. Each
  crate on crates.io is permanent. The check must give a clear list, not a
  cargo trace.
- **Black (risks).** The full verify build is slow. The release PR job gets
  a 120-minute limit and frees runner disk first. The doc markers can drift
  from the parser. A unit test reads the real page.
- **Yellow (value).** The check costs no token and no secret. The plan
  check adds under a second to `lint`. The order is written once and
  checked on each PR.
- **Green (alternatives).** `cargo publish --workspace --dry-run` orders
  the crates by itself. It does not tell the maintainer the order for a
  manual run, and it hides the order from review. `cargo-release` adds a
  tool. A plain list that a gate checks is simpler.
- **Blue (process).** Red tests for each finding, then the code, then the
  workflow and the docs. Review, then a check of each criterion.

## 6. Decision

`cargo xtask check crate-packaging` in `xtask/src/check/crate_packaging.rs`.

1. Read `cargo metadata --no-deps`. Keep the workspace members.
2. Read the order from `docs/releasing.md`, between
   `<!-- publish-order:start -->` and `<!-- publish-order:end -->`. Each
   item is a numbered line with one crate name in backticks.
3. The plan check finds each of these:
   - a publishable crate that the order does not list;
   - a listed crate that is not a publishable workspace member;
   - a crate listed twice;
   - a crate listed before a workspace crate it depends on;
   - a publishable crate that depends on an unpublished workspace crate;
   - a publishable crate with no `description` or no `license`.
4. `--plan-only` stops here. The `lint` job runs this form.
5. Otherwise run `cargo package --locked -p <each crate>`.
   `--no-verify` and `--allow-dirty` pass through for a local run.
6. Read each `<target>/package/<name>-<version>.crate`. Fail over
   10 MiB. Warn at 8 MiB or more.

`release-pr.yml` gets a `crate-packaging` job. It runs the full form with
`contents: read` and no secret.

`docs/releasing.md` holds the release steps and the publish order.
`docs/upgrading/README.md` holds the support window.

## 7. Acceptance criteria and their evidence

| Criterion | Evidence |
|---|---|
| A release PR runs a packaging check of every publishable crate, with no registry token | The `crate-packaging` job in `release-pr.yml`. Guard tests in `autumn-harvest/tests/integration/release_pr_ci.rs`. |
| Every crate that is not published sets `publish = false` | The plan check in `lint` fails on a publishable crate that the order does not list. Unit tests in `crate_packaging.rs`. |
| The manual publish order is documented | `docs/releasing.md`. The plan check proves it against the dependency graph. |
| A support-window statement is in the README or `docs/upgrading/` | The "Support window" section of `docs/upgrading/README.md`. |
