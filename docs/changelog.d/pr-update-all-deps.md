## Dependency update, MSRV 1.97

Updates every workspace dependency that can move. `testcontainers` stays at
0.27, because `testcontainers-modules` 0.15 needs it. `rusqlite` stays at 0.40,
because `autumn-web` 0.9 needs `libsqlite3-sys` 0.38.

**MSRV.** The MSRV rises from 1.88 to 1.97, which `wasmtime` 50 needs.

**Behavior changes for operators:**

- **Certificate trust.** `reqwest` 0.13 uses the OS trust store. It no longer
  uses the bundled Mozilla roots. An image with no CA certificates now fails TLS.
  A private CA in the OS store is now trusted.
- **Cron at the end of DST.** An hourly or shorter schedule in a time zone now
  also fires in the repeated hour. Croner 2.2 skipped the second pass, or
  returned a time before the reference. A fixed daily job still fires once.
- **Cron parsing.** `0 0 LW * *` and 7-field expressions with a year now parse.
  An expression with a `+` prefix, such as `+0 0 * * *`, no longer parses. Such a
  stored schedule stops firing, so check for one before you upgrade.
- **Seeded simulations.** `random_range` draws through `StdRng`, so a simulation
  seed can produce a different value. Replay is not affected, because it reads
  the recorded value.

**Embedders.** `reqwest` builds with `rustls-no-provider`, so `aws-lc-rs` stays
out of the default build. `autumn_harvest_plugin::http::client_builder` installs
`ring` if the process has no provider. An embedder that calls `random_range` with
a custom `SampleUniform` type must implement the `rand` 0.10 trait.

**Guards.** `HVG002` and `DET002` now also flag the `rand` 0.10 names:
`rand::rng`, `SysRng`, `random`, `random_range`, `random_bool` and `random_ratio`.

**Removed crates.** `fs4` (`std::fs::File::try_lock` replaces it) and unused
direct dependencies in the Redis crate, the sandbox guest and three examples.
