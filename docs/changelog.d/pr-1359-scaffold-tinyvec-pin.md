## Fix — Pin `tinyvec` in the `harvest new` scaffold to avoid a build-breaking upstream regression (issue #1359)

`harvest new` scaffolds a project with a fresh `Cargo.toml` and no committed `Cargo.lock`, so the first `cargo build` a new user runs resolves whatever's newest on crates.io. `tinyvec` 1.13.0 shipped a build-breaking regression (`cannot find macro `vec` in this scope` in its own alloc-mode source) that's pulled in transitively via the icu4x/zerovec stack (`idna`/`url`, pulled in by `autumn-web`/`reqwest`) — breaking the documented Getting Started flow for every new user, not just CI's own "Scaffold builds and runs" job.

`autumn-harvest-cli/templates/minimal/Cargo.toml.tmpl` now declares `tinyvec = ">=1.0.0, <1.13.0"` as a direct (otherwise-unused) dependency, purely to constrain the transitive resolver. Both bounds matter: a bare `<1.13.0` is trivially satisfiable by an unrelated, SemVer-incompatible pre-1.0 `tinyvec` line (`0.4.x`) also present in the graph — the resolver picks `0.4.1` for that edge and leaves the real, transitively-needed `1.x` line completely unconstrained at whatever's newest, silently defeating the pin. `>=1.0.0` rules the `0.x` line out, forcing this edge into the same `1.x` compatibility class the real dependents use, which is what actually forces unification below `1.13.0`.

Verified by scaffolding a project and building it with the same `patch.crates-io` path overrides CI's "Scaffold builds and runs" job uses: a single `tinyvec` `1.x` entry in the resulting `Cargo.lock` (resolves to `1.12.0`), full successful build through to the scaffolded binary.

**Zero engine impact:** the change is confined to the scaffold template; `autumn-harvest`/`autumn-harvest-plugin`'s own `Cargo.toml`/`Cargo.lock` are untouched, no migration, no schema change, no public API change.

**Follow-up:** loosen the upper bound once upstream ships a `tinyvec` release that fixes the regression.
