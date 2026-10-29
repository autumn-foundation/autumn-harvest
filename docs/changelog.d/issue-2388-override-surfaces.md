## Operations — See and change runtime overrides from the CLI and Vantage (issue #2388, epic #2384)

The overrides of #2385, #2386 and #2387 were visible only one route at a
time. An operator could not tell which parameters were overridden.

- `GET /admin/overrides` lists every stored override. It reads the schedule
  overrides from every shard pool, and the run-default overrides from the
  default shard. Each entry names its actor, reason and time. A shard that
  cannot be read is named in `unavailable_shards`.
- `GET /admin/config` gains `overrides`, the count of each kind.
- The CLI gains `harvest dags set-schedule <dag>` (`--interval`, `--cron`,
  `--manual`, `--timezone`, `--catchup`, `--max-active-runs`,
  `--jitter-secs`, `--overlap-policy`, `--buffer-all-max`, `--reason`),
  `harvest dags clear-override <dag>` and `harvest overrides list`. Each
  command sends one request. `dags` is now an alias of `dag`.
- The Vantage DAG page shows each schedule field as declared in code, as
  overridden and as effective. A field with an override has a marker. A DAG
  with no override shows none.

See [`docs/operations/schedule-overrides.md`](../operations/schedule-overrides.md).

No migration, no `WorkflowEvent` variant, no replay impact.

**Tests.** `run_defaults_integration` (the list names actor, reason and time,
the config counts match, and the route is admin-only), `ui_integration` (the
DAG page shows the code value and the override, and no marker without an
override), and the CLI `request_mapping` and `contract_coverage` suites.
