## Phase 5.x — zero-setup local dev runtime (issue #525)

autumn-harvest is, by primitive surface, essentially Temporal-complete — and the
first thing a new developer had to do was unrelated to workflows: install
Docker, author a `compose.yaml`, `docker compose up -d`, poll for Postgres
health, set `DATABASE_URL`. For a feature-complete-but-under-adopted engine that
prerequisite was the single biggest tax on evaluation: the moat was no longer
capability, it was the cold start. This ships the one onboarding affordance every
adjacent engine that won adoption already had.

```
git clone … && cd autumn-harvest
cargo dev
```

**What it is.** `harvest-dev`, a binary in `autumn-harvest-plugin` behind the
non-default `dev-runtime` feature, plus a workspace `cargo dev` alias. It
provisions an ephemeral PostgreSQL, lets Autumn apply the ordinary embedded
migration sets, runs a worker, serves the management API and the Vantage
dashboard, prints the dashboard URL and one copy-pasteable `curl` that starts a
durable sample workflow — then reclaims every byte of what it created on exit.

**What it deliberately is not: a second storage backend.** The issue rules out
SQLite/in-memory and this respects that literally. The cluster is real
PostgreSQL running the engine's real schema and real migrations, so a workflow
that runs under `cargo dev` is byte-for-byte the workflow it will be in
production. What is automated is the Postgres *lifecycle*. There is no "dev mode
lies to you" gap, which is the differentiator versus every peer that solved this
with a second engine. **No new migration, no new `WorkflowEvent` variant, no
change to the adjacently-tagged `{type, data}` contract, and no third writer of
`harvest_events`** — the two sanctioned exceptions in `CLAUDE.md` are untouched.
`dev_runtime_adds_no_migration_and_no_event_variant` pins all of that as a
property of the module's own sources (rather than as a migration count, which
would collide with every unrelated migration landing in parallel).

**Two tiers, and why.** `dev-runtime` drives a Postgres install already on the
machine — Homebrew, apt, Postgres.app, the EnterpriseDB installer — and adds one
small pure-Rust dependency (`url`, already in the lockfile). `dev-runtime-managed`
adds `postgresql_archive`, which downloads and extracts a platform-matched
PostgreSQL build into a per-user cache when the machine has none; that is the
tier `cargo dev` enables, and the one that makes "a clean machine with only the
Rust toolchain" literally true. `postgresql_archive` and **not**
`postgresql_embedded`: we need archive acquisition only, and the
server-orchestration half of `postgresql_embedded` would pull `sqlx` into the
graph for a cluster we already drive ourselves. Discovery is always tried first,
so a developer with Postgres installed never touches the network. The download
is SHA-256-verified upstream (`theseus` enables `postgresql_archive`'s `sha2`
hasher) and lands via extract-to-staging plus a rename, so two `cargo dev`s
racing on a cold cache cannot interleave into a half-populated install.

**Why not reuse `autumn-web`'s `ManagedPostgresPoolProvider`.** It was the first
thing considered, and it is the wrong shape here for two reasons. It is
persistent *by design* — `temporary = false`, credentials persisted next to the
data dir, a published URL file so one-off commands can attach — and AC5 wants the
exact opposite. And it hands its URL to Autumn's pool *during* boot, whereas
`HarvestRuntimeConfig` needs a real storage URL at config-load time; wiring it
would have meant feeding Harvest a placeholder DSN, which is precisely the kind
of lie this feature exists to avoid. Configuration is instead injected through
Autumn's own `ConfigLoader` seam — never by mutating the process environment,
which is `unsafe` and unsound once other threads exist, and this runs inside a
Tokio runtime that already has them.

**The safety gate is the feature's sharpest edge.** The dev runtime applies
migrations and runs a worker that claims and mutates rows; pointed at a real
database that is a destructive operation. So `classify_database_url` **fails
closed**: a DSN it cannot parse is refused, not allowed. It rejects any
non-loopback host, any TLS-requiring `sslmode` (a remote managed database in
practice), and — for a better message than "not loopback" — twenty known hosted
Postgres suffixes. Loopback matching is exact, so `localhost.attacker.example`
and `127.0.0.1.nip.io` are refused rather than passing a `starts_with` check.
Both libpq forms are understood, URI and `key=value`, because refusing to parse
the second would have made it un-classifiable rather than safe. A *local*
database whose name merely looks production-shaped is `Suspicious`, not refused,
and needs `--allow-suspicious-database-name`: "my local database is called
`myapp_production`" is ordinary, while "my dev runtime is pointed at
`db.prod.internal`" never is — so there is deliberately **no** override for a
remote host. The production-name check matches whole `-`/`_`/`.`-delimited
segments, so `reproduction_notes` and `aliveness` do not trip it.

**Teardown is three layers, because one is not enough.** A clean `Ctrl-C` goes
through Autumn's own `on_shutdown` phase (so a plain signal reclaims the cluster
even though `DevRuntime::shutdown` never gets a turn); a panic or early return
hits the `Drop` guard; and `SIGKILL` — the most ordinary thing a developer does
to a wedged process, and the one exit no dying process gets to observe — is
handled by the *next* start, which reads the session records under the session
root and reclaims any whose owning process is gone. The reap decision is a pure
function over `(record, owner_alive, postmaster_alive, self_pid)` so every branch
is tested without spawning anything, and the reaper is conservative at each step:
a directory without the session prefix is not ours, a record that will not parse
is left alone (it stops processes and deletes trees, so it acts only on records
it fully understands), and a session whose owner is alive is a concurrent
`cargo dev`, not a corpse.

**One bug only a real postmaster could find.** On Debian and Ubuntu — the most
common Linux dev platform by a wide margin — the packaged Postgres defaults
`unix_socket_directories` to `/var/run/postgresql`, which an ordinary developer
cannot write to. The postmaster starts, logs
`could not create lock file … Permission denied`, and shuts itself back down, so
`pg_ctl start` fails with nothing but "examine the log output". The generated
config now pins the socket directory *inside the session directory*, which fixes
the permission failure and means the socket is reclaimed with everything else
rather than left behind. Found by running `dev_runtime_lifecycle.rs` against a
real cluster, not by review — the whole reason that suite exists.

**Hardening carried through the details.** The cluster listens on `127.0.0.1`
only and authenticates with `scram-sha-256` against a 256-bit generated password
— never `trust`, because a loopback-only cluster is still reachable by every
other local user. The password file is `0600` and is deleted the moment `initdb`
has consumed it. The port is kernel-assigned, so two `cargo dev`s in two
terminals do not collide. `fsync`/`synchronous_commit`/`full_page_writes` are off
because the data directory is deleted on exit and there is nothing to recover —
which is also what keeps first-run start-up inside the five-minute budget. A
failed `pg_ctl start` reports the postmaster's own log tail, because "could not
start server" alone is unactionable.

**An honest note on the success metric.** The issue's budget is "≤ 5 minutes and
≤ 2 commands from `git clone`". The command count is met exactly (`git clone`,
`cargo dev`) and every *step* the baseline had — author a compose file, bring it
up, wait for health, set a DSN, run migrations — is gone. The wall-clock half is
dominated by something this change cannot remove: a cold `cargo build` of the
workspace. That is why the alias is deliberately **not** `--release` — on a
fresh clone the compile, not the run, is the dominant term, and an optimised
build costs several minutes more for a sample that does one activity and one
timer. Once built, the runtime itself reaches a completed workflow in seconds
(plus a one-off ~30 MB Postgres download on a machine that has none).

**Codex round 1 (one P1, two P2s, all real).** The P1 was the sharpest thing
anyone said about this change: the banner told a brand-new user to *"kill this
process mid-timer and start it again to watch the run resume from history"* —
a demonstration provisioned storage makes impossible, because `shutdown` deletes
the cluster and the reaper reclaims a killed run's directory. Following it would
have shown an empty run and taught a first-time reader that the engine loses
workflows, in the one place the feature exists to build confidence. The
restart-resume demonstration is now offered only where it can actually work — a
database the runtime did not create — and the provisioned banner points at
`HARVEST_DEV_DATABASE_URL` instead.

The two P2s were also genuine. Keyword/value redaction tokenised on whitespace,
so a quoted password containing a space (`password='foo hunter2'`) left half the
credential standing in the very string that exists to be safe to paste; it is now
a real scanner that consumes quoted spans whole, escapes included. And the reaper
could not stop a cluster started by *downloaded* binaries: discovery never
searches the managed cache, and on Windows `process_start_token` is always
`None`, so the identity check gating a direct `taskkill` can never pass — a
force-killed `cargo dev` on Windows would have leaked its postmaster and data
directory permanently. The session record now carries the `bin_dir` that started
the cluster (`#[serde(default)]`, so older records still parse and are still
reclaimed), and the reaper prefers it over discovery.

**Docs.** `docs/getting-started/01-project-skeleton.md` now opens with the
zero-setup path as the default first step and keeps the Docker/Compose route as
the explicit "bring your own Postgres" alternative, unchanged.
`examples/quickstart/` is untouched, as is the production embedding model
(`HarvestPlugin` / `HarvestRunner`).

**Windows / OpenSSL stance**, which the issue asked for explicitly: every crate
on this path is pure Rust — diesel's `postgres_backend`, `diesel-async`, rustls —
so there is no libpq and no OpenSSL, and `dev-runtime` builds on the Windows CI
leg unchanged. Provisioning finds a standard EnterpriseDB install under
`C:\Program Files\PostgreSQL\<version>\bin`, and the managed tier downloads a
Windows build. `postgresql_archive` is pinned to 0.19 rather than the current 0.21 for three
reasons. Its rustls feature unifies on the `reqwest` already in the graph
instead of pulling a second copy — 0.21 brought a whole second `reqwest`, plus
`jni` and `rustls-platform-verifier`. Its MSRV of 1.87 is *below* this
workspace's 1.88, where 0.21's is 1.94 — so unlike `wasm-activities`, this
feature raises no effective MSRV at all. And it is the version `autumn-web`'s
own `managed-pg` feature resolves to, so the two would unify if that feature
were ever enabled here (it is not today, so that third reason is future-proofing
rather than a present benefit).

**One claim that is compiled but not proven here.** `dev_runtime_managed.rs`
drives the download path end to end — acquire, verify the toolset, run a real
cluster on the downloaded binaries, then prove a second call reuses the cache —
but it is opt-in (`HARVEST_DEV_TEST_DOWNLOAD=1`, a ~30 MB fetch) and the
environment this was authored in blocks `api.github.com` at its egress proxy, so
the fetch itself returns `403` there. The target is compiled on every CI leg (a
`compileonly` manifest row plus both clippy legs) and the failure it produces is
the intended one — a named, actionable error rather than a hang — but the
"clean machine downloads a PostgreSQL" half of AC2 is asserted by a test that
has not yet been *run* green anywhere with network access. Recorded here rather
than papered over.

**Test evidence.** `dev_runtime_tests.rs` — 30 no-database, no-process tests over
the pure halves: the whole safety table (loopback, sockets, both DSN forms, TLS
modes, twenty managed providers, prefix-lookalike hosts, fail-closed parsing,
production-shaped names and the four ordinary names that must not trip it),
binary discovery across all three platform layouts including newest-version
preference and the Windows `.exe` probe, every branch of the reap decision,
session-record round-tripping and corrupt-record handling, `postmaster.pid`
parsing including truncated files, DSN percent-encoding, the loopback-only server
config, and the banner's four promises (leads with the UI URL, carries a
copy-pasteable trigger naming the real start route, says NOT FOR PRODUCTION, and
never promises to delete a database it did not create).
`dev_runtime_lifecycle.rs` covers the falsifiable half against a real postmaster,
skipping cleanly where no Postgres binaries exist: storage comes up as a real
loopback Postgres whose generated DSN passes our own gate; `shutdown` leaves no
process and no directory; dropping the handle without `shutdown` reclaims
anyway; the reaper stops an orphaned postmaster and removes its directory; two
concurrent runtimes get different dirs, ports and DSNs and one's teardown does
not touch the other; and end to end, a durable workflow starts, completes, and is
visible in the Vantage UI.
