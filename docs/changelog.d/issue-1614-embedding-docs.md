## Docs — `docs/embedding.md` and a getting-started fork for plain Axum (issue #1614)

`docs/embedding.md` is the reference for the standalone path. It covers what
the runtime needs, what the router gives you, and what you own instead of
`HarvestPlugin`. It also covers auth, metrics, webhooks, shutdown, multi-shard,
and what is not available off the plugin path.

`docs/getting-started/standalone-axum.md` is the on-ramp. It is a fork, not a
fourteenth chapter. It runs the Chapter 2 workflow with `HarvestEmbedding` on
a plain Axum server. Its code is the new crate `examples/standalone-quickstart`.
`README.md`, the getting-started index, Chapter 1 and Chapter 2 point at it.

The `admin_auth_boundary` preflight remediation now names
`StandaloneAdminAuth::with_admin_auth_boundary`. It also says that API tokens
alone are not a boundary. The `HarvestEmbedding` rustdoc now gives the real
startup order and the real answer of a stopped runtime.

**Fix: the `[harvest]` config files now follow the autumn-web profile order.**
Harvest picked `autumn-{profile}.toml` from `AUTUMN_PROFILE`, `--profile` or
`AUTUMN_IS_DEBUG`, and ignored `AUTUMN_ENV`. autumn-web, and
`HarvestEmbedding::with_ambient_profile`, read `AUTUMN_ENV` first. So under
`AUTUMN_ENV=prod`, Harvest skipped `autumn-prod.toml`, and a
`[harvest.startup] orphaned_workflows = "fail"` there was lost. The order is now
`AUTUMN_ENV`, `AUTUMN_PROFILE`, `--profile`, `AUTUMN_IS_DEBUG`, with the
autumn-web name normalisation (`production` becomes `prod`). This applies on
both paths.

Guards:

- `docs/audits/standalone-chapter-sync.py` runs in the `lint` job. It checks
  that the chapter shows the crate's files byte for byte, and that the crate
  holds the Chapter 2 workflow verbatim. It also checks that every shell
  block has a `chapter-run` marker, and that the CI Postgres service matches
  the chapter's `compose.yaml`.
- The new `standalone-chapter` CI job runs the chapter's bash blocks as written
  against Postgres, through `scripts/run-standalone-chapter.sh`. It then
  presses Ctrl-C and requires a clean shutdown.
- The same job compiles every Rust block in `docs/embedding.md` as a doctest
  (`EmbeddingDocSnippets`). The step fails when no doctest passes.

No migration. No `WorkflowEvent` variant. Nothing writes to `harvest_events`.
