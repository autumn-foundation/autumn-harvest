# ⛏️ Prospect: does the "Postgres 12+" floor hold at the schema layer? (pursue: 139/139 on PG 12 and 14 vs 139/139 line, ledger #15)

> Status: **measured.** The pre-registration is
> [`docs/rnd/2026-10-19-postgres-12-floor-preregistration.md`](../rnd/2026-10-19-postgres-12-floor-preregistration.md),
> committed and pushed (`1a50b84`) before any apparatus existed. It is unchanged.

## 🎯 Question

The README claims Postgres 12+. CI runs PG 16 only. Do all 139 migrations apply
on fresh PG 12 and PG 14 databases, with the same tables, columns, indexes and
triggers as PG 16? **Decision:** keep, raise, or test the README floor.
**Decider:** the owner of the PR #2144 Keystone deferral (unassigned).

## 🔍 Prior art

The PR #2144 deferral record. It found no SQL above 12 by grep, and named this
run as the cheapest evidence. No earlier ledger entry covers Postgres versions.

## 🧪 Apparatus

[`apparatus/0015-postgres-12-floor/run.sh`](apparatus/0015-postgres-12-floor/run.sh).
PG 12.22 and 14.19 built from source with contrib. PG 16.15 from the Ubuntu package.
Fresh `initdb` per version, a fresh database per run, one transaction per
`up.sql` with `ON_ERROR_STOP=1`. No migration carries `metadata.toml`, and
`CONCURRENTLY` appears only in comments, so one transaction each matches Diesel.

Stubs: the engine never runs. Diesel is replaced by `psql`. Down migrations are
not run. Default server config. Loopback only. Superuser role, so the
`pgcrypto` `CREATE EXTENSION` privilege is never tested. PG 13 and 15 are not run.
The first harness draft failed on quoting (`$P`) and on the Debian `psql` path. Both
runs applied nothing and are discarded, not measurements.

## 📊 Assay

| server | run 1 | run 2 | run 3 | schema signature vs PG 16 |
|:--|:--|:--|:--|:--|
| PG 16.15 (control) | 139/139 | 139/139 | 139/139 | — |
| PG 14.19 | 139/139 | 139/139 | 139/139 | byte-identical, 3/3 |
| PG 12.22 | 139/139 | 139/139 | 139/139 | byte-identical, 3/3 |

The signature is 872 rows: 56 tables, 612 columns (name, type), 201 indexes by name,
3 triggers. All raw outputs, including all three signatures per server, are under `apparatus/0015-postgres-12-floor/results/`.
There is no worst-case axis for a deterministic DDL replay, so none was probed.

After review fixed two harness defects (an appending `summary.txt`, and a signature
query whose failure went unchecked), all three servers were rerun on the fixed
`run.sh`. Every run again applied 139/139, and every signature matched the archived
one byte for byte. The numbers above did not change.

## 🏁 Verdict

**Pursue, at the schema layer only.** L1 and L2 did not trigger. L3 (139/139 on 12
and 14), L4 (valid control) and L5 (identical signature) all passed. The number
that carries it is zero failed migrations out of 834 PG 12 and PG 14 applications (417 per version).

This does not show that 12+ is *supported*. Engine query paths (claim, replay,
sweeps, partition ops) were not exercised, because the DB suites need
testcontainers and this box has no Docker daemon. The README claim moves from
"untested" to "schema verified on 12 and 14", not to "tested".

## 💰 Cost to productionize

Item: run the core DB suite against `postgres:12` and `postgres:14`, as the
deferral record proposes. This assay does not size it. The stub that matters is
the missing engine run. The CI change touches the `.with_tag("16")` call in about
220 suites, so a matrix needs a shared tag source first. Gates: Ballast for the CI
change, Keystone for the README floor edit.

## 🔬 Reproduce

```sh
# build PG 12.22 and 14.19 (--without-readline --without-zlib, make install, contrib)
runuser -u postgres -- env PSQL=<pg>/bin/psql MIG=$PWD/autumn-harvest/migrations \
  bash docs/assays/apparatus/0015-postgres-12-floor/run.sh <pg>/bin <port> <outdir>
cmp <outdir16>/sig1.txt <outdirN>/sig1.txt
```
