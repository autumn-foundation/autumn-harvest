## Phase X.Y — Light test path for the sandbox unit tests (issue #2487)

`cargo sandbox-test` runs the core sandbox unit tests without a database. It
sets `--cfg harvest_light`. That cfg drops the heavy dev-dependencies of the
core crate: `testcontainers`, `criterion`, `stateright` and the agent crate.
The build has 180 crates instead of 316. It has no Diesel and no Docker client.
The largest compile step peaked at 3.3 GiB, so the build fits an 8 GiB machine.

The `tests/`, `benches/` and `examples/` targets still need the full set. The
CI job for the sandbox tests runs the alias. The sandbox docs guards check the
manifest gating, the alias, the guide and the CI step. There is no migration
and no new event.
