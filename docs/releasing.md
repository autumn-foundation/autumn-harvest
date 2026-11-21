# Releasing Harvest

This page gives the release steps and the manual publish order of the
crates. [`docs/upgrading/README.md`](upgrading/README.md#support-window)
states which releases get fixes.

Publishing stays manual. CI holds no registry token, and no workflow runs
`cargo publish` or `npm publish`. A token in CI is a standing credential, and
a yank does not undo a published crate.
[`docs/design/DESIGN-1988.md`](design/DESIGN-1988.md) records the decision.

## Release steps

1. On `trunk-dev`, set the workspace `version` in `Cargo.toml`. Set the
   `version` of each workspace path dependency in each member manifest,
   the examples included. Set the `version` in
   `clients/typescript/package.json`. Run `cargo update --workspace`, and
   commit `Cargo.lock`.
2. Collate the fragments in `docs/changelog.d/` into a
   `## [X.Y.Z] - YYYY-MM-DD` section of `CHANGELOG.md`, and into
   `docs/shipped-work.md`. Then delete them, as
   [`docs/changelog.d/README.md`](changelog.d/README.md) says.
3. Add the upgrade guide `docs/upgrading/X.Y.Z.md`, and link it from
   `docs/upgrading/README.md` and `README.md`.
4. Open the release pull request from `trunk-dev` into `trunk`.
   `release-pr.yml` runs on it. Its `crate-packaging` job runs
   `cargo xtask check crate-packaging`, which packages and verifies each
   crate below with no token. Merge when every check passes.
5. After the merge, tag the head of `trunk` as `vX.Y.Z`, and push the tag. `release.yml` checks the
   tag, then builds, signs and publishes the CLI archives, the TypeScript
   client tarball and the worker image.
6. Publish the crates by hand, in the order below.

## Publish the crates

Publish from a clean checkout of the tag. Log in with `cargo login` on your
own machine. Publish one crate at a time, in this order. Each crate comes
after every workspace crate that it depends on.

<!-- publish-order:start -->
1. `autumn-harvest-macros`
2. `autumn-harvest`
3. `autumn-harvest-sqlite`
4. `autumn-harvest-redis`
5. `autumn-harvest-agent`
6. `autumn-harvest-plugin`
7. `autumn-harvest-cli`
8. `autumn-harvest-sandbox-guest`
<!-- publish-order:end -->

For each crate, run this command with the crate name:

```sh
cargo publish --locked -p <crate>
```

`cargo publish` waits until crates.io lists the new version. Thus the next
crate in the order finds it.

The `lint` job runs `cargo xtask check crate-packaging --plan-only` on each
pull request. It fails when this list omits a publishable crate, lists a
crate that is not publishable, or puts a crate before its dependency. Each
workspace crate that is not for crates.io sets `publish = false`.

To run the full check on your machine before the release pull request:

```sh
cargo xtask check crate-packaging --allow-dirty
```

### When a publish fails part-way

A crate on crates.io never changes, and a yank does not remove it. Do not
yank the crates that went up.

1. Find the cause in the `cargo publish` output.
2. If the fix does not change a published crate, merge it into
   `trunk-dev`, then into `trunk` through a release pull request. Publish
   the remaining crates from the head of `trunk`, in the same order.
3. If the fix changes a published crate, release a patch version of every
   crate.

## The TypeScript client

The TypeScript client stays a signed tarball on the GitHub Release. The
maintainer can publish that tarball to npm by hand. No workflow does it.
