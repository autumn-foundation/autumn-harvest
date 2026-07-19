## Phase 3.x — `harvest new <name>` project scaffolding (issue #692)

New local CLI subcommand `harvest new <name> [--path <DIR>] [--force] [--template minimal]`
in `autumn-harvest-cli` that scaffolds a complete, compiling, runnable
autumn-harvest project so a first-time evaluator reaches one durable workflow
execution in **≤ 3 post-scaffold commands** (`docker compose up -d` →
`AUTUMN_PROFILE=dev cargo run` → `curl … /workflows/{name}_workflow/start`). The
one metric the engine is judged on — time from `cargo add` to first executed
workflow — finally has an owning command.

**CLI-only, additive.** No new `WorkflowEvent` variant, no migration, no
event-schema or macro-path change, no library API change. `harvest new` is a
**local** command (early-return in `run_cli` before the API dispatch path,
exactly like `det-check` #778): pure local file generation, no database, no
network.

**Design decisions:**
- **Command surface**: `harvest new <name>` only this slice (AC-1's "and/or" is
  satisfied by `new`; `harvest init` and an ephemeral-Postgres `--demo` mode are
  noted follow-ups). `ScaffoldTemplate` value_enum ships one `minimal` template.
- **Embedded templates**: `include_str!` from `autumn-harvest-cli/templates/minimal/`
  — the first `include_str!` in `src/`; zero runtime fetch. `{{crate_name}}` /
  `{{ident}}` / `{{workflow_fn}}` / `{{activity_fn}}` / `{{queue}}` placeholders
  (double-brace so they never collide with Rust `{}`), substituted by a tiny
  tested `apply_substitutions` helper.
- **Generated project shape** mirrors `examples/quickstart`: `Cargo.toml`
  (crates.io **version** deps `autumn-harvest = "0.4"` + `db` feature,
  `autumn-harvest-plugin = "0.4"`, `autumn-web = "0.5"` — never path deps, since
  the generated project lives outside the workspace; `edition = "2021"` for the
  broadest floor), `src/main.rs` (one `#[workflow]` `{ident}_workflow` calling
  one `#[activity]` `{ident}_activity`, `HarvestPlugin` wiring, a tiny `#[get("/")]`
  landing route since autumn-web requires ≥1 HTTP route, worker polling the
  single `["{ident}"]` queue — a workflow start enqueues the workflow task on
  the worker's first queue and the activity (`queue = "{ident}"`) is enqueued
  there too, so one queue services both), `README.md` (the ≤3-command runbook +
  `AUTUMN_PROFILE=dev` auto-migration note, satisfying the DB-bootstrap AC
  without a manual `diesel migration run`; the result-observing step polls
  `GET /workflows/{execution_id}` for `.execution.state` and offers the real
  `harvest … workflow get <id>` subcommand), `compose.yaml` (postgres:16),
  `autumn.toml`, `.gitignore`. Every emitted identifier is a pure function of
  `<name>` — no example identifiers survive (AC-5).
- **Clean identifier derivation**: `derive_crate_ident` lowercases, maps `-` → `_`,
  collapses runs of `_`, and trims leading/trailing `_`, so any spec-valid name
  yields a warning-free `snake_case` ident with no double underscore (`my-app` →
  `my_app`, `MyApp` → `myapp`, `trail-` → `trail`). Only the derived idents are
  normalized; name **acceptance** is unchanged (keyword/reserved collision is
  checked against the raw `-` → `_` form), so an uppercase package name like
  `MyApp` stays accepted per `cargo new` parity (only cargo's own cosmetic
  uppercase warning), while the generated `main.rs` uses the clean lowercase
  ident.
- **Fail-safe** (AC-6): a non-empty target directory without `--force` returns an
  error and writes **nothing** (validation + full render happen before any file
  is written); `--force` overwrites but never removes files it did not write (no
  `rm -rf`). An existing **non-directory** at the target (a plain file) is
  rejected up front with a clear "is not a directory" message instead of an
  opaque `create_dir_all` OS error.
- **Name validation**: rejects empty / over-64-char / non-`^[A-Za-z][A-Za-z0-9_-]*$`
  / Rust-keyword-resolving / reserved names before any write, so an invalid name
  can never produce an invalid `Cargo.toml`.

**Scaffold-rot guards (AC-8):**
- A **content-parity** unit test ties the template's `main.rs` wiring tokens to
  the CI-compiled `examples/quickstart/src/main.rs` (read at test time), so
  template drift from the shape CI actually builds fails the test.
- A Linux-only `scaffold-smoke` CI job **generates, builds, and runs** the
  scaffold to a terminal state — the literal AC-8 assertion. It builds the
  generated project against the **published** crates.io deps (no
  `[patch.crates-io]`) — exactly what a real user gets — behind a `postgres:16`
  service whose user/password/db match the scaffold's generated `autumn.toml`,
  boots the app (`AUTUMN_PROFILE=dev` auto-migrates), POSTs a workflow start,
  and polls `GET /workflows/{id}` until `state == COMPLETED`, failing the job on
  a bad terminal state or timeout. This closes the earlier "runtime execution
  deferred to a fast-follow" gap: the headline metric (scaffold → first executed
  workflow) is now asserted end to end in CI against the published API.

**Mechanism note.** The end-to-end run lives in a dedicated CI job with a
`postgres:16` service (rather than an in-crate integration test registered in
`.github/ci/integration-suites.txt`) because it must scaffold, build, and run a
*generated standalone cargo project* as a subprocess — which does not fit the
in-crate cargo-test harness and would be far heavier on disk.

**Tests (TDD red→green→refactor):** 22 integration tests in
`autumn-harvest-cli/tests/integration/new_cli.rs` (derive names, substitution,
no-placeholder-survives across every file, crates.io-deps-no-path, identifier
substitution in `main.rs`, hyphenated/uppercase/repeat-separator names → clean
`snake_case` idents in `main.rs` with no double underscore, README 3-command
path, README result-observing commands, quickstart wiring parity, writes-all-files,
fail-safe-non-empty, `--force` overwrite preserving unrelated files, existing-file
target rejection, `--path` creates missing dir, over-length + reserved-name
rejection, invalid-name-no-write) + 7 lib unit tests (`clap` parse of
`Commands::New` with flags and defaults, clean `derive_crate_ident`, keyword
rejection, valid-name acceptance, uppercase cargo-new parity).

**Local end-to-end verification.** The scaffold was built against the published
`autumn-harvest 0.4.0` / `autumn-harvest-plugin 0.4.0` / `autumn-web 0.5.0`
(registry sources, no patch) and driven to `COMPLETED` (output `hello, World!`)
against a local Postgres, confirming both the published-deps build and the
single-queue wiring.
