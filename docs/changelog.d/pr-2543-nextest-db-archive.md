## CI: live-DB test binaries built once with cargo nextest archive (PR #2543)

Each of the 21 `Test DB (linux, shard N)` jobs compiled its own manifest rows.
On a green trunk-dev run, shard 15 spent 15.6 of the 22.4 minutes of its suite
step compiling.

A new `test-db-archive` job builds every `linux` and `linuxpart` test binary
into one cargo-nextest archive. It uses the union of the row features and no
debug info. Each shard unpacks the archive into `./target` and runs its slice
with `--partition hash:<k>/21`, so a large suite spreads over every shard. The
`linuxpart` pass is partitioned the same way. It no longer runs whole on
shard 0.

`run-suites.sh` gains the `archive`, `extract`, `nextest` and `filterset`
modes. The coverage nightly keeps `run linux` and its row-ordinal sharding.
See `docs/testing/ci-db-archive.md`.

No engine change, no migration, no new `WorkflowEvent` variant.

**Tests.** `ci_nextest_archive.rs` checks that the archive holds each live-DB
crate, target and feature, that each row gives one filterset term, that the
shard index maps to the partition, and that `ci.yml` wires both jobs.
