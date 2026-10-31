# Pre-registration: do the engine's DB suites pass on PG 12 and PG 14? (assay ledger #16)

> Committed **before** any apparatus or measurement. Nothing in this file
> changes after the first run. The report `docs/assays/0016-postgres-12-engine-paths.md`
> appends the apparatus, the numbers and the verdict.

Registered 2026-10-26 on `trunk-dev` `38209c8`. It feeds the Keystone deferral
`docs/rnd/2026-10-12-postgres-support-floor-deferral.md`, whose first trigger
is 2026-11-12 (PG 14 end of life).

## 🎯 Question

README "Requirements" claims Postgres 12+. Assay #15 showed the 139 migrations apply on
PG 12.22 and PG 14.24. It did not run an engine query path. Do the engine's
database integration tests that can run without Docker give the same result on
PG 12.22 and PG 14.24 as on PG 16.15?

**Decision this feeds:** whether the README keeps "12+", raises the floor, or adds
a CI matrix entry. **Decider:** the owner of the PR #2144 deferral (unassigned;
`madmax983` as PR author).

## 🔍 Prior art

- The PR #2144 deferral record. Its "cheapest evidence" is this run, and it
  predicts that "a pass makes it a tested one".
- Ledger #15 covers the schema layer only. It names the engine run as its stub.
- A grep of `autumn-harvest/src`, the plugin `src` and the migrations for PG 13 and later
  syntax (`date_bin`, `MERGE`, `WITH (FORCE)`, `trim_array`, multirange, `NULLS NOT DISTINCT`)
  finds three things. `NULLS NOT DISTINCT` appears only in catalog checks in
  `partition.rs`. `unnest ... WITH ORDINALITY` is PG 9.4 syntax. One test fixture,
  `audit_chain_tests.rs:63`, runs `DROP DATABASE ... WITH (FORCE)`, which is PG 13 syntax.
  The grep predicts no engine failure. A grep is not a run, so this assay runs.

## Scope — what this result does not mean

The suites read `HARVEST_TEST_DATABASE_URL` in 150 of 329 files under
`autumn-harvest/tests/integration/` (count at registration). The rest start a
container with testcontainers, and this box has no Docker daemon. Those
modules are **not run**. A pass covers only the in-scope modules. The report states
the in-scope share of test files and test functions. It does not claim "12+ is
supported". The plugin crate, the SQLite backend and the Redis adapter are out of scope.

## Unit and classification (set now)

- **In scope:** every test in the `integration` target (`--features db`) whose module file
  contains `HARVEST_TEST_DATABASE_URL`. The module list is taken from the file list at
  `38209c8`, with no edit after the first run.
- **Differential failure on arm N:** a test that passes in all 3 PG 16 control runs and
  fails in at least 2 of 3 runs on arm N.
- **SQL-version failure:** a differential failure whose output carries a Postgres error with
  SQLSTATE class `42` (syntax, undefined function or object), class `0A`, or the text
  `syntax error`, `does not exist` or `not supported` from the server. A failure with no
  server error is **unclassified**.
- **Excluded from L1 and L2, but reported:**
  1. Failing SQL that lives in a test fixture, not in `autumn-harvest/src`. The one known
     case is `DROP DATABASE ... WITH (FORCE)` in `audit_chain_tests.rs`.
  2. On PG 12 only, the partitioned-layout modules `event_partitioning_tests`,
     `partition_archive_tests` and `partitioned_events_docs`. `docs/partitioned-events.md`
     already states a PG 14 floor for that layout.
  3. On PG 14 only, a failure whose text names `NULLS NOT DISTINCT` (PG 15 syntax in a
     partition fixture).

## ⚖️ Lines (set now, numeric)

| line | rule | verdict if met |
|:--|:--|:--|
| L4 (validity) | the PG 16 control passes at least 90% of in-scope tests in each of its 3 runs, and every run completes inside the 90 minute per-run cap | else no verdict: `undetermined`, the harness or the shared database is broken |
| L1 (kill) | at least 1 SQL-version failure on PG 12, after the exclusions above | "12+" is a measured false claim in the engine |
| L2 (kill) | at least 1 SQL-version failure on PG 14, after the exclusions above | the PG 14 floor is a measured false claim |
| L3 (pursue) | no SQL-version failure on PG 12 or PG 14, **and** unclassified differential failures at most 2% of in-scope tests on each arm | engine paths hold on the in-scope modules |
| L5 (undetermined) | no SQL-version failure, but unclassified differential failures above 2% on an arm | a difference exists that the harness cannot attribute |

Pursue needs L3 and L4 together. A miss on L1 or L2 is a kill, not an adjustment.
The thresholds do not move after data exists.

## Conditions

- Versions: PG 12.22 and PG 14.24 built from source with contrib. PG 16.15 from the
  Ubuntu package as the control. Default server config except `max_connections=300`
  on all three. Loopback only. Superuser role.
- Per run: a fresh `initdb`-ed server, a fresh database, the 139 `up.sql` files applied
  in order with `psql` (the harness of ledger #15), then
  `HARVEST_TEST_DATABASE_URL=<url> cargo test --features db --test integration -- <module filters> --test-threads=4`.
  Modules share one database inside a run. The control separates shared-database
  failures from version failures.
- Runs: 3 per version, 9 in total. All runs reported, with per-test results for every run.
- Control: PG 16, same harness, same binary, same module list.
- Incumbent claim: the README's "12+".

## Riskiest assumption first

The riskiest assumption is that the claim, sweep and partition-adjacent SQL uses nothing newer than
12. Before the full sweep, run once on PG 12 the modules whose name contains `claim`,
`queue`, `sweep` or `partition`. Record any failure before the full runs start.

## Time box and budget

One day of wall time for the apparatus, from the first build. No spend. No
production data. If the nine runs do not finish inside the box, report what finished
and set the verdict `undetermined`, with a re-charter named. Containment: branch
`claude/happy-gauss-y3yv4u`. The apparatus lives under `docs/assays/apparatus/0016-postgres-12-engine-paths/`
and never merges as engine code.
