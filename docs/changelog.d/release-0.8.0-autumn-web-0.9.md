## Release 0.8.0 — Upgrade to autumn-web 0.9

**Dependency bump.** The workspace version moves to `0.8.0`. Every
`autumn-web` requirement moves from `0.8` to `0.9`: the plugin crate, its
dev-dependency, the three Autumn examples and the `harvest new` scaffold.
The scaffold now writes `autumn-harvest = "0.8"`,
`autumn-harvest-plugin = "0.8"` and `autumn-web = "0.9"`. The MSRV stays at
1.88.0.

**One source port.** `autumn-web` 0.9 adds `Route::criticality` for its
adaptive admission control (Autumn issue #3068). `Route` is a plain struct,
so the three hand-built route literals stop compiling: the webhook receiver,
the MCP tool routes and the MCP task route. Each sets
`Criticality::Default`, the value that a route attribute without
`criticality` gives. No other 0.9 break reaches Harvest. Harvest builds no
`ApiDoc`, `JobInfo`, `AutumnConfig` or `StaticRouteMeta` literal without
`..Default::default()`, and implements no framework store trait.

**`[harvest]` is a declared config root.** The `prod` profile of
`autumn-web` 0.9 turns on `server.strict_config`. Autumn's config schema is
closed, so the check refuses an undeclared top-level table. Without a fix,
an app with a `[harvest]` table would not boot under `AUTUMN_PROFILE=prod`.
`HarvestPlugin::build` now calls `AppBuilder::config_section` with the new
public constant `config::HARVEST_CONFIG_ROOT`. Two tests pin it:

- `harvest_plugin_build_declares_the_harvest_config_section` checks that
  mounting the plugin declares the root;
- `strict_prod_config_accepts_harvest_only_as_a_declared_root` loads a
  `prod` config with a `[harvest]` table. The load fails without the root
  and passes with it, so the test also proves why the call is needed.

**Plugin contract.** `AUTUMN_WEB_REQUIREMENT` is `"0.9"`. The
range test now rejects `0.8.0` and `0.10.0`.

**Migrations.** `autumn-web` 0.9 adds four framework migrations to its main
set and one to its commit-hook set. None shares a version with a Harvest
migration.

**Dependencies.** `autumn-web` 0.9 keeps its own `aes-gcm` 0.10.3. Harvest's
codec stays on `aes-gcm` 0.11.1 with `aes` 0.9.2, `ghash` 0.6.0 and
`polyval` 0.7.3, so the `zeroize` features still apply to the copies that
the codec uses. `autumn-web` 0.9 drops `serde_yaml`, so only Harvest's own
test guards still pull it in. `deny.toml` records that for
RUSTSEC-2024-0449, and its RUSTSEC-2026-0387 citations move to the 0.9.0
source.

**Docs.** The 0.8.0 upgrade guide gains section 2 (dependency and plugin
changes, and the `prod` defaults that reach Harvest). The README, the
getting-started index, the agent skill and the dependency pins in the docs
move to 0.8 and 0.9. The TypeScript client, `docs/api-contract.json` and the
OpenAPI documents move to `0.8.0`.

No new `WorkflowEvent` variant, no new migration, no replay impact.
