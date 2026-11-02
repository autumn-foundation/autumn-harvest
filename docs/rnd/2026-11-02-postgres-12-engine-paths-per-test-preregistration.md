# Pre-registration: engine DB tests on PG 12 and PG 14, graded per test (assay ledger #17)

> Re-charter of assay #16, which ended `undetermined` on its validity line.
> Committed **before** any apparatus or measurement for #17. Nothing in this file
> changes after the first graded run. **Status: registered, not run.** It waits for
> a go-ahead (see "Ask before").

Registered 2026-11-02 on `trunk-dev` `c0d70ed`. It feeds the Keystone deferral
`docs/rnd/2026-10-12-postgres-support-floor-deferral.md`, whose first trigger is
2026-11-12 (PG 14 end of life). That trigger has no named owner.

## 🎯 Question

Take only the integration tests that pass on PG 16.15 in all 3 control runs and that
never start a container. Do they give the same result on PG 12.22 and PG 14.24?

**Decision this feeds:** the README "Postgres 12+" floor keeps, rises, or gains a CI
matrix entry. **Decider:** the owner of the PR #2144 deferral (unassigned). Someone
must be named before 2026-11-12, or the trigger passes with no decision.

## 🔍 Prior art

- Ledger #15: the schema layer passes on PG 12 and PG 14. Engine paths were not run.
- Ledger #16: the file-level scope failed. The PG 16 control passed 79.0% of in-scope
  tests against a 90% line. The cause is known: files that mix `HARVEST_TEST_DATABASE_URL`
  with testcontainers helpers, and modules that share one database. This assay fixes
  the scope unit. It does not re-assay #16's question with the same unit.
- Static triage of the #16 probe errors, done at `c0d70ed` for this registration
  (reading only, no run):
  - `pg_stat_force_next_flush()` (PG 15): called only from tests and benches. No
    `src` call.
  - `DROP DATABASE ... WITH (FORCE)` (PG 13): called only from tests and benches.
  - `NULLS NOT DISTINCT` (PG 15): appears in `partition.rs` only as text matched in
    catalog checks. The partitioned layout already states a PG 14 floor.
  - `gen_random_uuid()`: `autumn-harvest/src/throttle.rs:639` and the plugin
    `api.rs` use it. PG 12 has it only through `pgcrypto`. The initial migration
    creates that extension. It works only when `pgcrypto` sits on the `search_path`.
    The #16 probe error came from a fixture that migrates into an alternate schema.
    Whether an operator install can hit the same error is **not known**. This is the
    riskiest assumption, so the sweep tests it first.
- No closed pit covers this question. Assay #15 and #16 are the only entries.

## Scope — what this result does not mean

A pass covers only the tests that pass on the control and need no container. The
report states the share of test functions that this covers. It does not claim "12+ is
supported". The plugin crate, the SQLite backend and the Redis adapter stay out of
scope. Docker-backed tests stay out of scope until a Docker CI job exists. That job
is the cheaper alternative, and the deferral record proposed it first.

## Unit and classification (set now)

- **Unit:** one test function, identified by its full path.
- **In scope:** a test that passes in all 3 PG 16 control runs and whose control log
  has no line that names `/var/run/docker.sock`. The list freezes after control run 3.
- **Isolation:** every test module gets its own database, cloned from one migrated
  template. A module cannot see another module's rows.
- **Differential failure on arm N:** an in-scope test that fails in at least 2 of 3
  runs on arm N.
- **SQL-version failure:** a differential failure whose output carries a server error
  with SQLSTATE class `42` or `0A`, or the text `syntax error`, `does not exist` or
  `not supported`. A failure with no server error is **unclassified**.
- **Excluded from L1 and L2, but reported:**
  1. Failing SQL that lives in a test or bench fixture, not in `autumn-harvest/src`.
  2. On PG 12 only, the modules `event_partitioning_tests`, `partition_archive_tests`
     and `partitioned_events_docs` (PG 14 floor stated in `docs/partitioned-events.md`).
  3. On PG 14 only, a failure whose text names `NULLS NOT DISTINCT`.
- A failure of type 1 that shows a `src` path would also fail is **not** excluded.
  The report must say, per excluded failure, why `src` cannot reach the same SQL.

## ⚖️ Lines (set now, numeric)

The lines are the lines of #16. They do not move.

| line | rule | verdict if met |
|:--|:--|:--|
| L4 (validity) | at least 90% of the **candidate** tests pass in all 3 PG 16 control runs, and every run ends inside 90 minutes | else `undetermined`: the harness is broken |
| L1 (kill) | at least 1 SQL-version failure on PG 12, after the exclusions | "12+" is a measured false claim in the engine |
| L2 (kill) | at least 1 SQL-version failure on PG 14, after the exclusions | the PG 14 floor is a measured false claim |
| L3 (pursue) | no SQL-version failure on PG 12 or PG 14, **and** unclassified differential failures at most 2% of in-scope tests on each arm | engine paths hold on the in-scope tests |
| L5 (undetermined) | no SQL-version failure, but unclassified differential failures above 2% on an arm | a difference exists that the harness cannot attribute |

Pursue needs L3 and L4 together. A miss on L1 or L2 is a kill.

**Candidate set (the L4 denominator).** Every test function in the 152 module files
that #16 registered, minus the tests whose control log names `/var/run/docker.sock`.
A Docker failure does not depend on the Postgres version, so it is the only
exclusion. The candidate set does not depend on which tests pass. The in-scope set
(passes in all 3 control runs) is a subset of it. A test that passes in some control
runs but not all counts against L4 and stays out of the in-scope set. The report
states the in-scope share of the candidate set.

## Conditions

- Versions: PG 12.22 and PG 14.24 built from source with contrib. PG 16.15 from the
  Ubuntu package as the control. Default config except `max_connections=300` on all.
- Loopback only. Superuser role. Debug-profile test binary built with
  `--features db`. `--test-threads=4`. Seeds and versions go in the report.
- Three runs per arm. Every run is reported, including the discarded ones, with the
  reason for the discard.
- Run order, riskiest first: control run 1, then PG 12 run 1 and run 2 with the
  `pgcrypto` search-path question answered in the report, then the rest. A control
  run must finish before the PG 12 run that uses its list.

## Control

PG 16.15, same binary, same harness, same module set. The in-scope list comes from
the control alone.

## Time box and budget

- Time box: 2 days of wall time from the go-ahead. It does not extend without a
  written re-charter.
- Compute: about 6 hours of test time at the #16 run time of 17 minutes, less with
  per-module databases. One arm at a time.
- Disk: 20 GB per arm. Delete each arm's data directory before the next arm starts.
  This box showed 30 GB free at registration, so the budget is tight.
- Spend: none beyond the box. No production data.

## Containment

Branch `claude/happy-gauss-x4yj3s` (apparatus only under
`docs/assays/apparatus/0017-*`). The apparatus never merges to engine code.

## Ask before

- Running the sweep. It holds the box for hours and uses most of the free disk.
- Naming a decider. A verdict with no owner cannot change the README.
- Any step that needs Docker. The box has no daemon.

## Stubs expected

Docker-backed tests never run. Diesel replaced by `psql` for migrations. Debug build.
Superuser role. Default config. Per-module databases hide cross-module interference.
