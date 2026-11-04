## CI: balance the no-db shards and merge five plugin lib-test builds (PR #2561)

The `Test (no-db, <os>, shard N)` jobs set the length of a CI run. On
trunk-dev run 41757040952, Windows shard 3 took 110 minutes, and the
lightest Windows shard took 66.

Four changes, all in `ci.yml`:

- `Run all-features lib tests` moves from shard 3 to shard 0. There it runs
  after `Compile db-backed tests`, which builds the same lib test binary. On
  shard 3 it built that binary again, for 9 to 14 minutes.
- Five plugin lib-test steps become one. Each one selected its tests by a
  name filter: `connector`, `aws_kms`, `object_store`, `vault_transit` and
  `oidc`. Each step rebuilt the plugin crate under its own feature set. One
  build with the union of the features runs the same tests. Locally, the
  five filters select 272 tests, and the union build selects the same 272.
  All of them pass.
- `Compile diagnose benchmark` had no shard condition, so all four Linux
  shards compiled it, for about 11 minutes each. It now runs on shard 3.
- Five steps move to balance the measured totals: the CLI tests, the
  saga-choreography tests, the metrics scrape example, and the Kafka and
  WASM examples.

The `redis`, `webhooks`, `dev-runtime-managed` and default plugin lib-test
steps stay apart. Each runs the whole lib under its own feature set, and a
merge would change what they test.

On the first PR run, the long poles were 96.5 minutes on Windows (from
109.8) and 58.8 on Linux (from 69.7). macOS rose from 82.2 to 91.2. Shard
0 was the long pole on Windows and macOS. Its two compile steps took 5-12 minutes
longer than on the trunk-dev run, so the agent tests moved back to shard
3, and the saga-choreography tests moved to shard 1.

No engine change, no migration, no new `WorkflowEvent` variant.
