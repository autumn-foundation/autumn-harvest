# Design — Issue #525: zero-setup local dev runtime (`cargo dev`)

One command brings up a **fully working local harvest runtime**: Postgres
lifecycle managed for you, migrations applied, worker polling, management API +
Vantage UI served, a sample durable workflow one copy-paste away. **No Docker,
no hand-authored `compose.yaml`, no manual `DATABASE_URL`, no manual
`diesel migration run`.**

**No new migration. No new `WorkflowEvent` variant. Storage stays Postgres.**

---

## 0. Planning record

The three techniques the work was planned with, kept because the *rejections*
are the load-bearing part of the design.

### 0.1 Brainstorm — how could "zero setup" be delivered?

| # | Idea | Verdict |
|---|------|---------|
| B1 | Ship a SQLite/in-memory backend for dev | **Rejected — out of scope by the issue.** Forks the storage contract; "dev mode lies to you" is the exact failure the issue exists to avoid. |
| B2 | Auto-`docker run` a Postgres container | **Rejected.** AC1 says *no Docker*. Docker is the tax being removed. |
| B3 | Auto-launch a Postgres the developer already has installed (`initdb` + `pg_ctl` into a scratch dir) | **Adopted as the primary source.** Zero new dependencies, works today on any machine with a Postgres install (Homebrew, apt, Postgres.app, EDB installer). |
| B4 | Download a prebuilt, platform-matched PostgreSQL archive into a user cache on first run | **Adopted as the fallback source.** This is the only thing that makes "clean machine with *only* the Rust toolchain" literally true. Opt-in feature so the default dependency graph never grows. Implemented with `postgresql_archive` — see §1.1. |
| B5 | Statically link Postgres into the binary | Rejected. Postgres is not designed to be embedded in-process; this is a research project, not an onboarding fix. |
| B6 | Reuse the host app's existing dev database (the Oban/Sidekiq answer) | **Adopted as the BYO escape hatch**, not the default — harvest has no host app to borrow from. Kept because it is the fastest path for someone who *does* already have a dev Postgres, and it is the seam the safety gate guards. |
| B7 | A hosted "try harvest" sandbox | Rejected. Doesn't solve local development, and adds an operational surface. |
| B8 | Make it a `harvest dev` CLI subcommand | Rejected. `autumn-harvest-cli` is an HTTP *client* of the management API; it deliberately does not depend on `autumn-harvest-plugin`/`autumn-web`, and the UI and API live there. Pulling a web server into the CLI to serve a dev page would invert that. |
| B9 | A binary + feature in `autumn-harvest-plugin` | **Adopted.** The plugin crate already owns every piece being wired: migrations, `HarvestRunner`, the management API router, the Vantage UI. |
| B10 | A cargo alias so the command is short | **Adopted.** `cargo dev` — see §5. |

### 0.2 Reverse brainstorm — how could this feature do *harm*?

Each row is a failure mode we deliberately engineered against; the mitigation
column is where in the code it lives.

| # | How to make this actively harmful | Mitigation |
|---|-----------------------------------|------------|
| R1 | Point the dev runtime at the production database and let it auto-migrate | `dev::safety` refuses any non-loopback host, any TLS-requiring `sslmode`, any known managed-Postgres hostname, and any production-shaped database name. Refusal is the default; there is no "force" flag for a remote host. |
| R2 | Leave a `postgres` process running after exit, forever, on every run | Explicit `shutdown()` on the normal path, a `Drop` guard for the panic path, **and** a stale-session reaper that reclaims what `SIGKILL` left behind on the next start. |
| R3 | Leave multi-GB data directories in `/tmp` | Same three paths. The data dir is removed, not just the process stopped, and the reaper removes orphans. |
| R4 | Let the dev instance be reachable from the network | `listen_addresses = '127.0.0.1'`, an ephemeral port, and `scram-sha-256` with a per-session random password. Never `trust`, never `0.0.0.0`. |
| R5 | Ship a runtime that quietly becomes someone's production deployment | Banner states it is not for production on every start; refuses non-loopback storage; the binary is behind a non-default feature so it is not in anyone's release build. |
| R6 | Diverge from real harvest behaviour, so "it worked in dev" means nothing | Same Postgres, same schema, same embedded migration sets, same `HarvestRunner`. The dev runtime provisions storage and then gets out of the way — it does not fork any engine path. |
| R7 | Break the append-only invariant or the event contract | The dev runtime writes no events and adds no migration. Guard test asserts the migration set is untouched and no new variant exists. |
| R8 | Collide with a second `cargo dev` in another terminal | Per-session data dir keyed by pid + random suffix; port 0 → kernel-assigned; the reaper only reclaims sessions whose owner pid is dead. |
| R9 | Hang forever waiting for a database that will never come up | Bounded readiness wait with a deadline, then a diagnostic that includes the postmaster log tail. |
| R10 | Silently swallow the "you have no Postgres binaries" case and fail 40 seconds later with a connection error | Binary resolution happens first and fails immediately with a platform-specific, copy-pasteable remedy. |
| R11 | Make Windows-without-OpenSSL worse | Everything on the path is pure Rust (diesel `postgres_backend`, `rustls`). Documented stance in §7. |

### 0.3 Six hats

- **White (facts).** The repo already has: embedded migration sets applied under
  `AUTUMN_PROFILE=dev`; `HarvestRunner` owning worker + scheduler; a mounted
  management API; the Vantage UI at `/api/harvest/ui`; `harvest new` scaffolding.
  What is missing is *only* storage provisioning and the wiring that hands the
  resulting DSN to the app. Nothing in the engine needs to change.
- **Red (instinct).** The thing that will actually make a new developer bounce is
  a wall of text after their one command. The banner must be short, must lead
  with the URL, and must give exactly one command to copy. Second instinct: the
  first-run download is the moment of maximum abandonment risk, so it must say
  what it is doing and roughly how big it is.
- **Black (risk).** Three real risks. (1) `initdb` on a machine with an unusual
  locale/ICU setup fails in ways that read as our bug — mitigated by passing an
  explicit encoding and a `C` locale, and by surfacing `initdb`'s own stderr
  verbatim. (2) Teardown is the AC most likely to rot silently; it needs a test
  that asserts *absence* (no process, no directory), not just a clean exit code.
  (3) Distribution packaging differences in the Postgres build we are driving —
  which is exactly what bit us: Debian/Ubuntu default the Unix socket to a
  system directory an ordinary user cannot write to. Every such default the
  cluster depends on has to be pinned explicitly in the generated config rather
  than inherited.
- **Yellow (benefit).** Every piece is already built; this is wiring plus one
  process-lifecycle module. The payoff is disproportionate: it removes the only
  hard external prerequisite in the onboarding path.
- **Green (creative).** The cargo alias — `cargo dev` — is what turns a correct
  feature into a *short* one. Also: the sample workflow should include a durable
  timer, because the thing worth showing in the first 60 seconds is durability,
  not throughput. And the reaper turns "we clean up on exit" into "we are clean
  even after `kill -9`", which is the honest version of AC5.
- **Blue (process).** Red/green/refactor throughout. The pure logic (safety
  classification, binary discovery, reaper decisions, banner rendering, DSN
  construction) is tested with no database and no processes; the lifecycle is
  covered by an integration test that skips cleanly where no Postgres binaries
  exist, so CI legs without them stay green.

---

## 0.4 Prior art found in the tree, and why it is not reused

`autumn-web` 0.7 already ships `ManagedPostgresPoolProvider` behind its
`managed-pg` feature: a `DatabasePoolProvider` that downloads Postgres, runs
`initdb`, starts a supervised child and stops it on `on_shutdown`. It was the
first candidate, and it is the wrong shape here for two independent reasons.

1. **It is persistent by design.** `settings.temporary = false`, superuser
   credentials persisted next to the data dir, a published URL file so
   `autumn task` / `autumn build` can attach to a running cluster. AC5 asks for
   the exact opposite — reclaim *all* ephemeral state — and bending a
   deliberately-persistent component into an ephemeral one would fight its
   design at every step.
2. **It resolves its URL too late.** It hands the URL to Autumn's pool *during*
   `create_pool`, but `HarvestRuntimeConfig` needs a real storage URL at
   config-load time. Wiring it would mean feeding Harvest a placeholder DSN it
   never actually connects to — precisely the kind of lie this feature exists
   to remove.

What *is* reused is the part worth reusing: the archive acquisition, via the
same upstream `postgresql_archive` crate `autumn-web`'s provider builds on.

## 1. Where it lives

`autumn-harvest-plugin`, behind two new non-default features:

- `dev-runtime` — the runtime itself. Uses Postgres binaries already on the
  machine. Adds one optional dependency, `tokio-postgres` — already in the
  lockfile, and the parser the safety gate delegates to (see §4).
- `dev-runtime-managed` — `dev-runtime` plus `postgresql_archive`, which
  downloads, verifies and extracts a platform-matched PostgreSQL build into a
  per-user cache on first run. This is the feature that makes AC2's "clean
  machine, only the Rust toolchain" true, and the one `cargo dev` enables.

### 1.1 `postgresql_archive`, not `postgresql_embedded`

`postgresql_embedded` is the better-known crate and does the whole job —
download, `initdb`, start, stop. We need only its first quarter. Taking the
whole crate pulls `sqlx` into the graph to orchestrate a cluster we already
drive ourselves, and hands over exactly the lifecycle control AC5 depends on.
`postgresql_archive` 0.19 with `theseus` + `rustls` is the acquisition half
alone, and rides on dependencies the plugin crate already has (`reqwest`,
`sha2`). 0.19 rather than 0.21: it unifies on the `reqwest` already in the
graph, and its MSRV (1.87) is below this workspace's 1.88, where 0.21's is 1.94.

New binary `harvest-dev`, `required-features = ["dev-runtime"]`.

Nothing in `autumn-harvest` (core) changes. `HarvestPlugin` / `HarvestRunner`
are used exactly as an embedder uses them — the out-of-scope list is respected
literally.

## 2. Module map

```
autumn-harvest-plugin/src/dev/
  mod.rs        DevRuntime / DevRuntimeConfig — orchestration
  safety.rs     DSN classification + the refusal gate       (pure)
  discovery.rs  Postgres binary resolution across platforms (pure over a probe fn)
  postgres.rs   EphemeralPostgres: initdb -> start -> ready -> createdb -> stop -> rm
  session.rs    on-disk session record + the stale-session reaper
  banner.rs     the start banner and the sample-trigger command   (pure)
  sample.rs     the built-in `dev_greeting` workflow + activity
```

## 3. Startup sequence

1. Resolve config (flags + env). Decide **provisioned** vs **BYO** storage.
2. **BYO** (`--database-url` / `HARVEST_DEV_DATABASE_URL`): classify the DSN
   through `safety::classify`. `Unsafe` → refuse, naming the reason. Anything
   accepted still prints the not-for-production banner.
3. **Provisioned** (default): **refuse `root` first** — before the session root
   is created or reaped, because a stale record names a `bin_dir` whose `pg_ctl`
   the reaper runs, and at uid 0 that record could be one any unprivileged local
   user pre-created at `harvest-dev-0`. Then reap stale sessions, resolve
   binaries, `initdb` into `<tmp>/harvest-dev-<uid>/session-<pid>-<rand>/data`,
   write the session record, start the postmaster on 127.0.0.1:0, wait for
   readiness, create the database and role.
3a. **Refuse a non-loopback HTTP host.** `http_host` is a public config field
   documented as loopback-only; that has to be enforced, not merely written
   down, because `run_app` mounts the management router with `.api(...)` and
   **not** `api_with_auth`. The dev runtime is unauthenticated precisely because
   it is unreachable, so the two facts are kept true together: every address the
   host resolves to must be loopback, and a host that will not resolve is
   refused rather than assumed.
3b. **Re-prove the HTTP port** once storage exists. The reservation in step 1
   cannot be *held* across provisioning — autumn-web binds the same port itself
   — so two runs that both found it free both get this far. autumn-web
   `process::exit(1)`s on a bind failure, skipping every destructor, so the
   loser would strand the cluster it just built. Checking again here, while
   teardown is still ours to run, narrows that window from seconds of
   provisioning to the microseconds before the server binds.
4. Build the Autumn app with `HarvestPlugin`, feeding it the resolved DSN,
   port and `profile = "dev"` through Autumn's own `ConfigLoader` seam —
   **not** by mutating the process environment, which is `unsafe` and unsound
   once other threads exist, and this runs inside a Tokio runtime that already
   has them. Autumn then applies the embedded migration sets on its ordinary
   startup path: the same code a real deployment runs, with no dev-only
   migration runner.
5. Serve, on a dedicated OS thread with its own Tokio runtime. `AppBuilder::run`'s
   future is not provably `Send` — rustc cannot infer the higher-ranked lifetime
   through one of autumn-web's internal closures and rejects `tokio::spawn`
   outright — while `Runtime::block_on` carries no such bound. The thread also
   isolates the dev server's runtime from the caller's, which is what lets a
   test start and stop one from inside its own `#[tokio::test]` runtime.
   Print the banner: UI URL, API URL, and one `curl` that starts the sample
   workflow.
6. On `Ctrl-C` / `SIGTERM`: stop the server, then `EphemeralPostgres::shutdown`
   — stop the postmaster, wait for the process to actually exit, remove the data
   dir and the session record. Teardown is also wired into Autumn's own
   `on_shutdown` phase; the cluster lives behind a single `Option`, so the two
   paths race harmlessly and exactly one wins.

## 4. The safety gate (AC4)

`safety::classify_database_url(dsn) -> DatabaseSafety`:

- `Allowed` — every host and hostaddr is loopback or a Unix socket, no TLS
  requirement, no production-shaped name.
- `Suspicious(reason)` — local, but the name reads as production. Needs an
  explicit opt-in.
- `Refused(reason)` — any of:
  - host is not `localhost` / `127.0.0.1` / `::1` / a Unix socket path;
  - `sslmode` is `require` / `verify-ca` / `verify-full` (a remote managed DB);
  - host matches a known managed-Postgres suffix (RDS, Cloud SQL, Neon,
    Supabase, Azure, DigitalOcean, Render, Heroku, Timescale, Aiven, …);
  - the database name or user looks production-shaped (`prod`, `production`,
    `live`, `staging`).

**The gate parses with `tokio_postgres::Config`, not a parser of its own.** The
only question that matters is what the client will actually dial, and the only
component that can answer it is the client. A hand-rolled parser is a second
opinion, and every disagreement between the two is a bypass — review found
three real ones (`hostaddr` treated as a synonym for `host`; a `host=` query
parameter that *appends* rather than replaces; `url` splitting userinfo at the
last `@` where the client splits at the first). Delegating removes the class.

There is **no override flag for a non-loopback host**. A loopback DSN whose
*name* trips the production-shaped check can be accepted with an explicit
`--allow-suspicious-database-name`, because "my local database is called
`myapp_production`" is a real and harmless situation, while "my dev runtime is
pointed at `db.prod.internal`" never is.

## 5. The command (AC1, success metric)

Workspace `.cargo/config.toml`:

```toml
[alias]
dev = ["run", "-p", "autumn-harvest-plugin",
       "--features", "dev-runtime-managed", "--bin", "harvest-dev", "--"]
```

Deliberately **not** `--release`: on a fresh clone the compile, not the run,
dominates "time from `git clone` to a running workflow".

So the whole path is **two commands**:

```bash
git clone https://github.com/autumn-foundation/autumn-harvest && cd autumn-harvest
cargo dev
```

## 6. Teardown (AC5)

Three layers, because one is not enough:

1. **Normal exit** — a `Ctrl-C`/`SIGTERM` handler → `shutdown()` →
   `pg_ctl stop -m fast`, **confirm the pid is gone**, then `remove_dir_all`.
   Confirming is load-bearing: `pg_ctl` exits 0 in several states that are not
   "stopped", and deleting a live cluster's data directory both corrupts it and
   orphans it (the reaper only ever acts on records, which go with the dir).
   Teardown has exactly one owner — an `on_shutdown` hook alongside it looked
   like belt and braces and was a race whose loser reported success for
   truncated work.
2. **Panic / early return** — `Drop` on `EphemeralPostgres` runs the same
   sequence, and `Drop` on `DevRuntime` reaches it.
3. **`SIGKILL` / power loss** — the next start reaps. Each session dir holds a
   `session.json` naming the owner pid and **start time**, the postmaster pid
   and **its** start time, written before `initdb` so a kill at any point leaves
   something to find — and rewritten **atomically** (private sibling +
   `rename`) once the postmaster is up, because a truncating rewrite killed
   half-way leaves JSON the reaper cannot parse, and an unparseable record is
   deliberately skipped: the one mechanism meant to reclaim a killed run would
   leak it permanently instead. A session whose owner is dead is reclaimed — but
   only after four checks, because reaping means signalling a pid and deleting a
   tree: the session root is per-user, **owned by us** and `0700` (re-verified
   on every use, and ownership is compared directly because `root` can `chmod`
   a foreign directory, so a successful `chmod` proves nothing at uid 0); the
   record's
   `data_dir` must be the one this layout puts inside its own session dir; the
   recorded start time must still match, so a *reused* pid is never mistaken
   for ours; and a cluster that could not be confirmed stopped is left running
   **with its directory intact** rather than deleted underneath it. During the
   start window the record carries no postmaster pid, so Postgres's own
   `postmaster.pid` is the only evidence — and *absence* of a pid counts only
   when it is confirmed absence. A file that exists but cannot be read or
   parsed, which is what a crash mid-write leaves, is uncertainty, and the
   session is left for a run that can tell.

The identity the whole thing hangs on is the **effective** uid, not the real
one: `id -u` reports the effective id, and a process with real uid 1000 and euid
0 has every privilege the root refusal exists to keep away from a planted
record.

The reaper's decision is a **pure function** over `(session record, is_alive)`
so it is exhaustively unit-testable without spawning anything.

## 7. Windows / OpenSSL stance

The whole path is pure Rust: diesel's `postgres_backend`, `diesel-async`, and
`rustls` — no libpq, no OpenSSL. `dev-runtime` therefore builds on the Windows
CI leg. The *provisioning* half needs Postgres binaries: on Windows,
`dev-runtime` finds a standard EnterpriseDB install under
`C:\Program Files\PostgreSQL\<ver>\bin`, and `dev-runtime-managed` downloads a
Windows build. Documented in the getting-started chapter rather than left to be
discovered.

## 8. Invariants (AC6)

- No new migration. No new `WorkflowEvent` variant. No change to the
  adjacently-tagged `{type, data}` contract.
- No new writer of `harvest_events` — the two sanctioned exceptions in
  `CLAUDE.md` are untouched.
- Storage stays Postgres. What is automated is the Postgres *lifecycle*.
- Guard test `dev_runtime_adds_no_migration_and_no_event_variant` pins all of
  the above, as a property of the module's own sources (comment lines stripped)
  rather than as a migration count — a count would collide with every unrelated
  migration landing in parallel.
