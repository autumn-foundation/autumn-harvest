## Feature — `HarvestEmbedding`: one entry point for a standalone embedding (issue #1613)

`HarvestEmbedding::new(built, config, resources).start()` runs the standalone
startup sequence and returns the runner and a mounted `Router<()>`. The
embedder used to do each step by hand, and the reference example skipped
three of them.

The entry point does these steps, in the `HarvestPlugin` order:

- It applies the operator's `[harvest.startup] orphaned_workflows` setting
  from the config files and the environment. The code value is the default.
  An invalid value refuses boot. `HarvestStartupConfig::with_operator_overrides`
  is the public seam.
- It declares the posture through `StandaloneAdminAuth`: the profile, the auth
  boundary, the session key, and the token and read-only-role layers. With no
  declared profile, it reads `AUTUMN_PROFILE` or `--profile`.
- It copies the builder limits into `HarvestApiState`. The standalone path
  used to serve the default limits.
- It loads the persisted admission gates before any worker spawns. It runs the
  orphan gate before it publishes any admission global.
- It installs the storage pool, then the API runtime, and starts the gate
  refresh loop.
- It requires one result-notification URL per shard. A multi-shard embedding
  names them with `with_notification_database_urls`.

`HarvestEmbeddingRuntime::stop` stops the gate refresh, drains the runner, and
then clears the admission globals and the API state.

The plugin and the embedding share one implementation of each step in the new
`boot.rs`. `examples/standalone-runner`'s `server.rs` now builds a pool, calls
the entry point, and serves the router.

No migration, no route change, and no `harvest_events` change.

Tests: `tests/standalone_embedding.rs` covers each step against Postgres. Unit
tests cover the startup overlay (`config.rs`) and the profile and URL
resolution (`embedding.rs`).
