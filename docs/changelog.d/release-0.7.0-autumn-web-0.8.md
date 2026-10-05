## Release 0.7.0 — Upgrade to autumn-web 0.8

**Dependency bump.** The workspace version moves to `0.7.0`. Every
`autumn-web` requirement moves from `0.7` to `0.8`: the plugin crate, its
dev-dependency, the three Autumn examples and the `harvest new` scaffold.
The scaffold now writes `autumn-harvest = "0.7"`,
`autumn-harvest-plugin = "0.7"` and `autumn-web = "0.8"`. The MSRV stays at
1.88.0.

**No source port.** `autumn-web` 0.8 is a hardening release. Its breaking
changes are at seams that Harvest does not cross. `Route`,
`AppBuilder::plugin_migrations`, `migrate::run_pending` and the
`with_migration_connection!` thread isolation that `ensure_runtime_migrations`
relies on are unchanged. The whole workspace builds with `--all-targets` and
no source change.

**Plugin contract.** `HarvestPlugin` implements the new `Plugin::contract`.
The contract declares `autumn-web = "0.8"` through the public constant
`AUTUMN_WEB_REQUIREMENT` and the function `harvest_plugin_contract()`.
Harvest does not release in lockstep with Autumn, so the contract names an
explicit range rather than `lockstep_contract`. Three unit tests pin it:

- the constant equals the `autumn-web` requirement in the crate's
  `Cargo.toml`, so a later bump cannot leave it stale;
- `evaluate` returns `Compatible` for the `autumn-web` that the crate
  compiles against, so mounting the plugin cannot panic;
- `evaluate` rejects 0.7.0 and 0.9.0.

**`tokio-postgres-rustls` 0.14.** `autumn-web` 0.8 moved to 0.14. Harvest's
core `tls` feature and the CLI pinned 0.13, which put two copies in the
lockfile. Both pins move to 0.14. The constructor that Harvest calls is
unchanged. `pg_tls::Connector` is public, so the type change is
source-visible for an embedder that names it through its own 0.13 pin.

**Migrations.** `autumn-web` 0.8 adds five framework migrations. None
shares a version with a Harvest migration. The six versions that Harvest
shares with Autumn's job-queue set (`20260513000000`, `20260519000000`,
`20260530000000`, `20260628000000`, `20260702000000`, `20260709000000`)
predate this release. Autumn's name-keyed substitution still resolves
them, so no tracking record moves.

**Also.** The stale `circuit_forced_open` initializer in the
`stall_diagnosis_profile` bench is fixed, so `--all-targets` builds.
`aead_payload_codec_key` gets the `#[expect(clippy::expect_used)]` that its
sibling has, so `clippy --all-features` passes again. The
TypeScript client and `docs/api-contract.json` move to `0.7.0`. Stale
dependency pins in the docs move to the current release.

No new `WorkflowEvent` variant, no new migration, no replay impact.
