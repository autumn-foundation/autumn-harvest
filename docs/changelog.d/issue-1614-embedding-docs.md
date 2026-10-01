## Docs — `docs/embedding.md` and a getting-started fork for plain Axum (issue #1614)

`docs/embedding.md` is the reference for the standalone path. It covers what
the runtime needs, what the router gives you, and what you own instead of
`HarvestPlugin`. It also covers auth, metrics, webhooks, shutdown, multi-shard,
and what is not available off the plugin path.

`docs/getting-started/standalone-axum.md` is the on-ramp. It is a fork, not a
fourteenth chapter. It runs the Chapter 2 workflow with `HarvestEmbedding` on
a plain Axum server. Its code is the new crate `examples/standalone-quickstart`.
`README.md`, Chapter 1 and the getting-started index point at it.

The `admin_auth_boundary` preflight remediation now names
`StandaloneAdminAuth::with_admin_auth_boundary`. It also says that API tokens
alone are not a boundary.

Guards:

- `docs/audits/standalone-chapter-sync.py` checks that the chapter shows the
  crate's files byte for byte. It also checks that the crate holds the
  Chapter 2 workflow verbatim.
- The new `standalone-chapter` CI job runs the chapter's bash blocks as written
  against Postgres, through `scripts/run-standalone-chapter.sh`.
- `EmbeddingDocSnippets` compiles every Rust block in `docs/embedding.md` as a
  doctest, in the `lint` job.

No migration. No `WorkflowEvent` variant. Nothing writes to `harvest_events`.
