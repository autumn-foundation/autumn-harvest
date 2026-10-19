# Pre-registration: does the "Postgres 12+" floor hold at the schema layer? (assay ledger #15)

> Committed **before** any apparatus or measurement. Nothing in this file
> changes after the first run. The report `docs/assays/0015-postgres-12-floor.md`
> appends the apparatus, the numbers and the verdict.

Registered 2026-10-19, for the Keystone deferral in PR #2144
(`docs/rnd/2026-10-12-postgres-support-floor-deferral.md`), which names this
as its "cheapest evidence to gather first".

## 🎯 Question

README "Requirements" claims Postgres 12+. CI runs PG 16 only. At the schema
layer, do all migrations in `autumn-harvest/migrations` (139 directories at
registration, `trunk-dev` `0351e0d`) apply without error, in version order, on
a fresh PG 12 and a fresh PG 14 database, and produce the same set of tables,
indexes and triggers as PG 16?

**Decision this feeds:** whether the README keeps "12+", raises the floor, or
adds a CI matrix entry. **Decider:** the owner of the PR #2144 deferral
(unassigned; `madmax983` as PR author). On *kill*, the README claim becomes a
measured false claim and the floor moves to the oldest version that passes.

## Scope — what this result does not mean

- It covers migration DDL only. It does **not** run engine query paths
  (claim, replay, sweeps). Those need the testcontainers suite, and no Docker
  daemon exists on this box. A pass here is therefore "the schema applies", never
  "12+ is supported".
- PG 15 is not run. Only 12, 14 and 16 are.

## ⚖️ Lines (set now, numeric)

| line | rule | verdict if met |
|:--|:--|:--|
| L1 (kill) | any migration errors on PG 12 | "12+" is false at the schema layer |
| L2 (kill) | any migration errors on PG 14 | the PG 14 floor is false |
| L3 (pursue) | 139/139 apply on PG 12 **and** PG 14, with no error and no ignored error | schema layer holds |
| L4 (validity) | the PG 16 control arm applies 139/139 | else the apparatus is broken and no verdict is printed |
| L5 (parity) | the set of (table, index, trigger, column name+type) rows is identical across 12, 14, 16, after ordering | else "undetermined": it applies but differs |

Pursue needs L3, L4 and L5 together. Any miss on L1 or L2 is a kill, not an
adjustment.

## Conditions

- Versions: PG 12.22 built from source (contrib `pgcrypto` included), PG 14 latest
  minor built from source, PG 16 from the system package as the control.
- Each: fresh `initdb`, default config, loopback only, fresh database per run.
- Apply each `up.sql` in directory-name order with `psql -v ON_ERROR_STOP=1`,
  one transaction per migration, matching how Diesel runs them. A migration
  annotated for no-transaction (`metadata.toml`) runs without one.
- Runs: 3 per version. All runs reported.
- Control: PG 16, same harness. Incumbent claim: the README's "12+".

## Riskiest assumption first

The riskiest assumption is that DDL in the newest migrations (partition-aware,
`CONCURRENTLY`, generated or covering indexes) uses nothing newer than 12.
The apparatus applies the migrations newest-first-probed: if a full run fails, the
failing version is recorded before anything else.

## Time box and budget

One day. No spend. No production data. Containment: scratchpad build, branch
`claude/happy-gauss-eyeis4`; the apparatus lives under `docs/assays/apparatus/`
and never merges as engine code.
