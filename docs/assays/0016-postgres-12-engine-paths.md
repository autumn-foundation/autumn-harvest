# ⛏️ Prospect: do the engine DB suites pass on PG 12 and PG 14? (undetermined: PG 16 control 79.0% vs 90% validity line, ledger #16)

> Status: **measured, undetermined.** The pre-registration is
> [`docs/rnd/2026-10-26-postgres-12-engine-paths-preregistration.md`](../rnd/2026-10-26-postgres-12-engine-paths-preregistration.md),
> committed and pushed (`07cf709`) before any apparatus existed. It is unchanged.

## 🎯 Question

Do the engine's database integration tests that can run without Docker give the
same result on PG 12.22 and PG 14.24 as on PG 16.15? **Decision:** keep, raise or
test the README "Postgres 12+" floor before the 2026-11-12 PG 14 end-of-life
trigger. **Decider:** the owner of the PR #2144 deferral (unassigned).

## 🔍 Prior art

The PR #2144 deferral record and ledger #15 (schema layer passes, engine paths
not run). A grep for PG 13+ syntax predicted no engine failure. See the
pre-registration.

## 🧪 Apparatus

[`apparatus/0016-postgres-12-engine-paths/run.sh`](apparatus/0016-postgres-12-engine-paths/run.sh)
and `modules.txt` (152 module files that contain `HARVEST_TEST_DATABASE_URL` at
`38209c8`; the pre-registration said 150, which was a count of the working tree
by `grep -l`). PG 12.22 and 14.24 built from source, PG 16.15 from the Ubuntu
package. One `initdb` per arm, migrations applied with `psql`, one shared database
per run, `--test-threads=4`.

Stubs: the Docker-backed tests never run. All modules in a run share one database.
Diesel is replaced by `psql`. Default config except `max_connections=300`.
Superuser role. Debug-profile test binary.

Three apparatus defects were found and fixed before any graded run. No number from
them is reported. The first test binary was built without the `db` feature and ran
0 tests. The harness called the system `psql` with PG 12 libraries on the path. A
disk-full stop killed the next attempt, so the sweep restarted.

## 📊 Assay

| arm | run | passed | failed | ignored | pass rate (non-ignored) |
|:--|:--|--:|--:|--:|--:|
| PG 16.15 (control) | 1 | 1,796 | 477 | 31 | 79.0% |
| PG 16.15 (control) | 2, 3 | not run | | | |
| PG 14.24 | 1 to 3 | not run | | | |
| PG 12.22, full sweep | 1 to 3 | not run | | | |
| PG 12.22, registered probe (`claim\|queue\|sweep\|partition` modules) | 1 | 318 | 168 | 14 | 65.4% |

The 4,527 filtered-out tests in the probe are the other modules. Raw logs and sorted
per-test results are under `apparatus/0016-postgres-12-engine-paths/results/`.

**Why the control fails.** 200 lines of the control log name `/var/run/docker.sock`.
Those tests live in files that mix both setups: a file that reads
`HARVEST_TEST_DATABASE_URL` in one helper can still start a container in another.
The registered scope unit (the file) is too coarse. The rest of the control failures
are modules that mutate shared state in one database, mainly `event_partitioning_tests`
(86), `partition_archive_tests` (24) and `audit_export_tests`. The control also showed
that one run takes about 17 minutes and leaves 15 GB in the PG data directory.

**Probe findings, not graded.** The probe has no control run, so none of its failures
counts as a differential failure. Its server errors are worth a look in a re-charter:
`gen_random_uuid() does not exist` in a migrate-into-alternate-schema fixture,
`pg_stat_force_next_flush() does not exist` (PG 15), `syntax error at or near "NULLS"`
(PG 15, excluded in the registration) and `DROP DATABASE ... WITH (FORCE)` (PG 13,
excluded fixture). Whether any is an engine path, not a fixture, is unknown.

## 🏁 Verdict

**Undetermined.** L4 (validity) failed: the PG 16 control passed 79.0% of in-scope
tests in its first run against a 90% line. L4 needs all three control runs at or above
the line, so the miss is final and I stopped after run 1 rather than spend six more
hours on arms that cannot be graded. L1, L2, L3 and L5 are ungraded. The number that
carries the verdict is 79.0% against 90%.

This is not a "12+ holds" and not a "12+ is false". The README claim stays where
ledger #15 left it: schema verified, engine paths untested.

## Re-charter (not run)

Grade per test, not per file: take only tests that pass on the PG 16 control and never
start a container. Give every module its own database so the control passes at the
registered line. Then rerun the three arms. The line stays at 90%. Expect about
6 hours of wall time with the current 17 minute run, or less with a per-module
database and more threads. Disk: allow 20 GB per arm. The cheapest alternative is a
CI job with Docker, which removes the scope problem altogether: the deferral record
proposed it first.

## 🔬 Reproduce

```sh
# PG 12.22 and 14.24 built from source into /opt/pg<ver>; test binary from
# CARGO_INCREMENTAL=0 cargo test -p autumn-harvest --features db --test integration --no-run
runuser -u postgres -- env ROOT=$PWD TESTBIN=<binary> \
  bash docs/assays/apparatus/0016-postgres-12-engine-paths/run.sh <pg-bin-dir> <port> <outdir> <label> 3
```
