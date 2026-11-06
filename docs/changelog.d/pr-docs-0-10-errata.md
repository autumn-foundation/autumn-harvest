## Documentation — 0.10.0 docs errata

- `docs/sqlite-plugin.md` "Turn it on" pinned `autumn-web = "0.9"` next to the 0.10 Harvest crates. That pair builds two copies of `autumn-web`. It now pins `"0.10"`.
- The published 0.9.0 crates declare MSRV 1.97.0, not 1.88.0. The 0.9.0 and 0.10.0 upgrade guides now say so. The `[0.9.0]` CHANGELOG entry still says 1.88.0; the next collation corrects it.
- `docs/operations/process-roles.md`, `docs/operations/run-default-overrides.md`, `docs/agent-mesh.md` and `docs/agent-adapter.md` drop tracker numbers from headings and running prose. The site vendors these pages. Links to the old `#…-issue-NNNN` anchors in the runbooks and the architecture doc now use the new anchors.
