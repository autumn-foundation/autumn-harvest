# Ledger profiles (issue #2364)

Every function that sends SQL to Postgres or SQLite has a Ledger profile.
A profile records what the query touches, what makes it run, and whether
Ledger has measured its cost. The gate
[`docs/audits/ledger-profiles.py`](../audits/ledger-profiles.py) fails CI's
`lint` job when a query has no profile, or when a profile no longer matches
its query.

Ledger is the query-cost pass of this repository. It measures shared
buffers with `pg_stat_statements` and publishes each result in a
`docs/performance-*.md` page. See [the claim-path page](../performance.md)
and [the deep-backlog page](../performance-deep-backlog.md). Before the
catalog, only the queries that Ledger chose to measure had any record.

## Where a profile lives

The catalog has one TOML file for each source file, at the same path under
the crate name:

| Source file | Catalog file |
|---|---|
| `autumn-harvest/src/queue.rs` | `docs/ledger/autumn-harvest/queue.toml` |
| `autumn-harvest-plugin/src/durable_route/stream.rs` | `docs/ledger/autumn-harvest-plugin/durable_route/stream.toml` |

A profile is a table keyed by the function's path inside its file. A method
is `Type::method`. A function inside an inline module is `module::function`.
A `macro_rules!` macro is `name!`.

```toml
["claim_task_one_statement"]
driver = "per_task"
backend = ["postgres"]
kind = ["read", "write"]
tables = ["harvest_task_queue", "harvest_workflow_executions"]
measured = "docs/performance.md"
```

One file per source file keeps the catalog from becoming a conflict magnet.
Two PRs conflict on the catalog only when they change queries in the same
source file, and then they conflict on that source file too.

## The fields

| Field | Values | Set by |
|---|---|---|
| `driver` | `per_task`, `per_tick`, `per_request`, `setup`, `test_support` | the author |
| `backend` | `postgres`, `sqlite` | the scan, at least |
| `kind` | `read`, `write`, `ddl` | the scan, at least |
| `tables` | table names | the scan, at least |
| `measured` | a `docs/` page, or `unmeasured` | the author |

`driver` says what makes the query run, and so how often it runs:

- `per_task`: each task claim, completion, heartbeat or decision commit.
  Its cost scales with throughput.
- `per_tick`: each pass of a background loop, scanner or sweeper. Its cost
  scales with the backlog that the pass reads.
- `per_request`: each API, UI, CLI or client call.
- `setup`: startup, a migration, or an operator maintenance operation such
  as a partition layout change or a shard rebalance.
- `test_support`: a test or verification harness that ships behind a
  feature.

The scan detects `backend`, `kind` and `tables` from the function body. It
also reads a same-file builder whose name ends in `_query`, `_sql`, `_stmt`,
`_ddl` or `_statement`, and an upper-case constant. A profile must list at
least what the scan detects. It can list more, because the scan cannot
follow a query that another module builds.

`measured` names the Ledger page that measures a query of the function. The
page must exist and must name the function. Write `unmeasured` when no page
does. An `unmeasured` profile is not a defect. It is the list of queries
whose cost nobody has measured yet:

```sh
grep -rl 'measured = "unmeasured"' docs/ledger | wc -l
grep -rB5 'measured = "unmeasured"' docs/ledger | grep -B4 'per_task'
```

## When you add or change a query

1. Run the gate:

   ```sh
   python3 docs/audits/ledger-profiles.py
   ```

2. For a missing profile, print a stub and paste it into the catalog file:

   ```sh
   python3 docs/audits/ledger-profiles.py --scaffold
   ```

   The stub has `driver = "CHOOSE"`. Choose a driver. The gate refuses any
   other value.

3. When you rename or delete a function, rename or delete its profile. The
   gate reports a profile that matches no function.

4. When Ledger measures a query, set `measured` to the page.

## What the scan counts as a query

- `sql_query(...)`, `insert_into(...)`, `diesel::update(...)` and
  `diesel::delete(...)`
- the Diesel methods `load`, `load_stream`, `first`, `get_result`,
  `get_results`, `execute` and `batch_execute`, with one argument
- the `rusqlite` methods `prepare`, `prepare_cached`, `query_row`,
  `query_row_and_then`, `query_map`, `query_and_then`, `execute`,
  `execute_batch` and the `pragma_*` calls

Code under `#[cfg(test)]` or `#[test]` is out of scope, and so is a module
that a `#[cfg(test)] mod name;` declaration includes. `autumn-harvest-redis`
sends Redis commands, not SQL. `autumn-harvest-macros` generates calls into
engine functions, and those functions hold the queries.

The scan reads source text. It does not compile the code. Its self-test
covers the forms that it must get right: comments, strings, raw strings,
lifetimes, atomics, `impl` blocks, macros and test modules.

```sh
python3 docs/audits/ledger-profiles.py --self-test
```

## How the first catalog was written

The scan found the query sites and filled `backend`, `kind` and `tables`.
Each `driver` came from the role of the module, with a per-function
override where a function plays a different role. Each `measured` link
names a page that names both the function and its module. A driver is a
judgment. Correct one in a normal PR when a profile is wrong.
