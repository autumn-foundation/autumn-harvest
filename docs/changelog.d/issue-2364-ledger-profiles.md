## Tooling — Ledger profile for every SQL query (issue #2364)

Every function that sends SQL to Postgres or SQLite now has a Ledger profile.
The catalog lives in `docs/ledger/`, with one TOML file for each source file,
so two PRs conflict on it only when they change queries in the same source
file. A profile records the query's `driver` (`per_task`, `per_tick`,
`per_request`, `setup` or `test_support`), its `backend`, its `kind` (`read`,
`write`, `ddl`), the `tables` that it touches, and the Ledger page that
measures it, or `unmeasured`.

The first catalog holds 953 profiles in 96 files across `autumn-harvest`,
`autumn-harvest-plugin`, `autumn-harvest-sqlite`, `autumn-harvest-cli` and
`autumn-harvest-agent`. About one in ten names a measured Ledger page. The
rest are `unmeasured`, which makes the measurement gap countable for the
first time.

`docs/audits/ledger-profiles.py` gates the catalog in CI's `lint` job. It
scans the shipped `src/` trees for query sites outside `#[cfg(test)]`, and it
fails on a query with no profile, on a stale profile, and on a profile whose
`backend`, `kind` or `tables` omit what the scan detects. `--scaffold` prints
a stub for each missing profile. `--self-test` covers the lexer forms that it
must get right.

No engine change: no new `WorkflowEvent` variant, no migration, no schema
change and no public API change.
