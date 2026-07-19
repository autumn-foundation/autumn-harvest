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
  one `#[activity]` `{ident}_activity`, `HarvestPlugin` wiring, worker polling
  **both** `["{ident}", "default"]` so the demo can't starve), `README.md` (the
  ≤3-command runbook + `AUTUMN_PROFILE=dev` auto-migration note, satisfying the
  DB-bootstrap AC without manual `diesel migration run`), `compose.yaml`
  (postgres:16), `autumn.toml`, `.gitignore`. Every emitted identifier is a pure
  function of `<name>` — no example identifiers survive (AC-5).
- **Fail-safe** (AC-6): a non-empty target directory without `--force` returns an
  error and writes **nothing** (validation + full render happen before any file
  is written); `--force` overwrites but never removes files it did not write (no
  `rm -rf`).
- **Name validation**: rejects empty / over-64-char / non-`^[A-Za-z][A-Za-z0-9_-]*$`
  / Rust-keyword-resolving / reserved names before any write, so an invalid name
  can never produce an invalid `Cargo.toml`.

**Scaffold-rot guards (AC-8):**
- A **content-parity** unit test ties the template's `main.rs` wiring tokens to
  the CI-compiled `examples/quickstart/src/main.rs` (read at test time), so
  template drift from the shape CI actually builds fails the test.
- A new Linux-only `scaffold-smoke` CI job generates a project, injects
  `[patch.crates-io]` redirecting `autumn-harvest`/`autumn-harvest-plugin` to the
  **in-workspace (unpublished)** crates, and `cargo build`s it — the honest
  guard against unpublished trunk-dev API drift (a naive build against the
  published 0.4.0 would hide it).

**Tests (TDD red→green→refactor):** 15 integration tests in
`autumn-harvest-cli/tests/integration/new_cli.rs` (derive names, substitution,
no-placeholder-survives across every file, crates.io-deps-no-path, identifier
substitution in `main.rs`, README 3-command path, quickstart wiring parity,
writes-all-files, fail-safe-non-empty, `--force` overwrite, `--path` creates
missing dir, invalid-name-no-write) + 5 lib unit tests (`clap` parse of
`Commands::New` with flags and defaults, `derive_crate_ident`, keyword
rejection, valid-name acceptance).

The literal "reaches a terminal state in CI" execution assertion is deferred to
a fast-follow (the compile guard + content parity already prevent silent rot);
the full runtime run is documented in the generated README and exercised
locally.
