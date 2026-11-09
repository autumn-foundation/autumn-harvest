# docs/audits/

Deterministic, reproducible checks over the docs corpus and adjacent
generated surfaces — no server, browser, or network access required. Each
audit is a subcommand of the workspace `xtask` crate, runnable standalone
(`cargo xtask audit <name>`) from anywhere in the repository and safe to
wire into CI as a gate. An audit with fixtures also takes `--self-test`.
The source of `cargo xtask audit <name>` lives in
`xtask/src/audit/<name with underscores>.rs`. The audits are Rust, so none
of them needs Python or PyYAML.

| Command | Checks | Wired into CI |
|---|---|---|
| `cargo xtask audit corpus-link-check` | Internal markdown links (missing file, missing anchor) and orphan pages across `docs/**/*.md` | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit config-cli-drift` | Doc-cited `[harvest]` TOML config keys, `AUTUMN_HARVEST*` env vars, and `harvest` CLI `--flags` against the real schema/CLI, extracted mechanically from `autumn-harvest-plugin/src/config.rs` and every file under `autumn-harvest-cli/src/` | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit doc-snippet-syntax` | Every ```` ```rust ```` fenced block in `docs/getting-started/*.md` and `README.md` parses as valid Rust (via `rustfmt`, not `cargo` — a syntax check, not a compile check; see the module docs). A handful of deliberate partial-chain excerpts are documented exceptions, not silently skipped | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit vantage-dashboard-contrast` | WCAG 1.4.3 contrast on the Vantage dashboard's inline stylesheet | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s `STYLE` constant |
| `cargo xtask audit vantage-flash-live-region` | WCAG 4.1.3 status-message live-region role (`role="status"`/`aria-live`) on every Vantage dashboard `div.flash` render site | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s flash-rendering sites |
| `cargo xtask audit vantage-degraded-banner-focus` | WCAG 4.1.3 live-region role *and* programmatic on-load focus (`tabindex="-1"` + `autofocus`) on every Vantage dashboard `div.degraded-banner` render site — the `div.flash` mechanism above, extended to the class a full-page-load status message also needs to reach a screen reader on initial content | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s degraded-banner-rendering sites |
| `cargo xtask audit vantage-label-association` | Form-label association on every Vantage dashboard `label` render site: the labelled control must resolve, by wrapping or by a `for="…"` matching a real `id="…"` in the file. Scoped to `label` sites — a control with no `label` at all is not separately enumerated | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s form-rendering sites |
| `cargo xtask audit openapi-response-coverage` | The API contract against the handlers. Every status a handler or its helpers return, through `StatusCode::` or an `AutumnError::` constructor, is declared. The contract marks each mandatory body field required and documents each field serde accepts. It documents each query key a hand-rolled parser matches. It marks a body required when the handler cannot run without it. Each `Query<T>` field has a param with the same type and required flag. The audit also reads bodies parsed from raw `Bytes`. A type it cannot resolve is a finding. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit cli-flag-coverage` | The inverse of `cargo xtask audit config-cli-drift`: real `harvest` CLI `--flags`, extracted the same way, that no page in the corpus mentions at all — a coverage-matrix report for manual triage, not a gate (see the module docs for why) | No — report-only; run manually as Folio triage input |
| `cargo xtask audit comment-hygiene` | Comment defects across every `*.rs`: commented-out code, unreferenced TODOs, narrative asides, blank block edges (all gated at zero), plus review-round archaeology, contractions and over-long sentences (ratcheted against the merge base, so a change may not add one to a file it touches) | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit audit-catalog-coverage` | Every `cargo xtask audit` subcommand (each variant of the `Command` enum in `xtask/src/audit/mod.rs`) has a row in this table, and every row names a real subcommand | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit tracked-artifacts` | No tracked file is binary or a patch leftover (`.orig`, `.rej`). The binary rule is git's: a NUL byte in the first 8000 bytes. The command lists each binary that the repository needs, with a reason (issue #1831). `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit tenant-header-validation-sync` | The `authz.rs`/`tenant.rs` tenant-header-validation clone class (issue #2341, linked rather than merged — 2 instances, below rule-of-three) stays byte-identical past the one allowed difference (function name, length-constant spelling). `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit required-migration-versions-sync` | The two `required_migration_versions` copies in `preflight.rs` and `shard_health.rs` stay byte-identical (issue #2358, linked rather than merged). Each copy keeps its sibling comment. The checker refuses block comments and raw strings in the body. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit libpq-keyword-sync` | The two `is_connection_keyword` copies in `autumn-harvest-cli/src/lib.rs` and `autumn-harvest-plugin/src/dev/dsn.rs` keep the same libpq `KEYWORDS` list (issue #2521, linked rather than merged — a shared home would cross a public-API boundary). Each copy keeps its sibling comment; the checker normalizes the one allowed difference (the `pub(super) ` prefix and comment wording) before comparing, and refuses block comments and raw strings in the body. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit byte-size-parse-sync` | The two byte-size-string parsers, `autumn_harvest::parse_byte_size` in `autumn-harvest/src/lib.rs` and `autumn_harvest_macros::parse_byte_size_macro` in `autumn-harvest-macros/src/lib.rs`, stay in step (issue #2625, linked rather than merged — `autumn-harvest-macros` is a proc-macro crate and cannot depend on `autumn-harvest`). Each copy keeps its sibling comment; the checker drops the signature line (name and visibility) and normalizes comment wording before comparing, and refuses block comments and raw strings in the body. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit diesel-table-width` | Every diesel `table!` has 32 columns or fewer, the default diesel tier (issue #2492). A wider table splits over a core and an extension declaration (`autumn-harvest/src/table_split.rs`). `WIDE_TABLES` is empty, and a ceiling there only goes down. No manifest outside `docs/` enables `64-column-tables` or `128-column-tables`. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit ledger-profiles` | Every function in a shipped `src/` tree that sends SQL to Postgres or SQLite has a Ledger profile in `docs/ledger/` (issue #2364). A profile names the query's driver, backend, kind, tables and the Ledger page that measures it. The scan detects the backend, kind and tables, and a profile must list at least those. A stale profile or catalog file is also a finding. `--scaffold` prints stubs; `--self-test` runs the fixtures. See [`docs/ledger/README.md`](../ledger/README.md) | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit ledger-census` | Maps each SQL statement that the DB suites send to its Ledger profile, plans it with `EXPLAIN (GENERIC_PLAN)` and `enable_seqscan = off`, and writes the access paths to `docs/ledger/plans/` (issue #2366). A remaining `Seq Scan` means that no index can serve the predicate. `--self-test` runs the fixtures in CI. See [`docs/ledger/README.md`](../ledger/README.md) | Self-test only — `.github/workflows/ci.yml`, `lint` job. The census runs by hand |
| `cargo xtask audit root-files` | Every tracked file at the repository root is in its `ALLOWED` list, with a reason. A design record goes in `docs/design/`, never the root. An `ALLOWED` entry that is not tracked is also a finding. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit doc-claim-drift` | Each doc claim matches the shipped code. The `event.rs` row in `docs/architecture.md` states no `WorkflowEvent` variant count, because a hard-coded count drifts. No two rows of one table in `docs/architecture.md` share a first cell. The newest version in `RELEASE_NOTES.md`, if any, is the workspace version. Each `STALE_CLAIMS` pin, the exact wording that a shipped change made false, stays absent from its file. No rule judges free prose (issue #1831). `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit shard-weight-drift` | Per-shard test-count weight under row-ordinal sharding, which the coverage nightly `db` job uses at the `test-db-linux` shard count (`test-db-linux` itself partitions by test hash), computed the way `cargo xtask ci run-suites`'s own `row_ordinal % SEMAPHORE_SHARD_COUNT` actually assigns rows to shards; flags any shard carrying 2+ heavy suites | Yes — `.github/workflows/ci.yml`, `lint` job, **report-only** (always exits 0; see the module docs for why it does not yet gate) |
| `cargo xtask audit workflow-yaml-parse` | Every `.github/workflows/*.yml` parses as YAML with no repeated mapping key and has a non-empty `jobs` mapping. GitHub runs an unparsable workflow as a zero-job failed run and never fires its triggers | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit action-sha-pin` | Every `uses:` in `.github/workflows/` and `.github/actions/` pins a 40-hex commit SHA with a `# <tag>` comment, or names a local `./` action or a `docker://` image by digest. A tag or branch can move to new code, and a SHA cannot (issue #1826). It finds each `uses` key in the parsed YAML tree, then reads the raw line for the comment, which a parser drops. A key in any other form is a finding. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit standalone-chapter-sync` | `docs/getting-started/standalone-axum.md` shows the files of `examples/standalone-quickstart` byte for byte and holds the Chapter 2 workflow verbatim. Every shell block in it has a parseable `chapter-run` marker for `cargo xtask dev run-standalone-chapter`, and the CI Postgres service matches its `compose.yaml`. Every Rust fence in `docs/embedding.md` is one that rustdoc compiles, and the CI steps that run these guards exist (issue #1614). `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit mixed-version-contract` | `docs/upgrading/README.md` has each section of the rolling-deploy contract and names the smoke command and the `mixed-version-smoke` CI job. The job runs `cargo xtask dev run-mixed-version-smoke`, and the smoke crate declares its own `[workspace]` (issue #1828). `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |
| `cargo xtask audit worker-image-contract` | The worker image and its Helm chart match [ADR 0006](../adr/0006-worker-container-image.md). The `Dockerfile` pins each base by digest and builds with the `rust-toolchain.toml` toolchain and `cargo auditable`. Its final stage runs as a non-root user and holds each binary. The chart `appVersion` is the workspace version, and its image is the release image. The rendered chart has the probe paths, the `preStop` sleep, a grace period that covers the drain, and a pre-install and pre-upgrade migration Job. It refuses to render with no database Secret (issue #1989). Needs `helm`. `--self-test` runs the fixtures | Yes — `.github/workflows/ci.yml`, `lint` job |

## Comment hygiene: the two tiers

`cargo xtask audit comment-hygiene` is the one audit here that scans code rather than
docs, and its tier split is deliberate.

**Tier A is absolute.** CH001–CH004 are defects under any house style,
they were driven to zero when the harness landed, and a new one fails the
build. Fix the finding; there is no baseline to absorb it.

**Tier B is a ratchet against the merge base.** CH005–CH007 have a legacy
population too large to fix in one change (CH007 alone is ~17.9k
sentences), so the rule is simply: your change may not *add* one to a file
it touches. Existing findings stay until someone chooses to fix them.

There is no checked-in baseline, deliberately. CI passes `--base`, and the
harness re-scans each changed file as it stood at the merge base, read out
of git. A stored baseline goes stale the moment the base branch moves —
one merge adding a long comment anywhere turns every open PR red for a
file its author never opened (this happened on the harness's own first CI
run) — invites being regenerated to launder a new violation, breaks on
renames, and costs half a megabyte of generated fingerprints in the tree.

Findings are matched by **fingerprint** (rule + normalized text), not
counted. A count only answers "how many", so removing one legacy violation
and adding a different one in the same file nets to zero and passes. An
identity answers "which". A renamed file is compared against its own
previous path, so a pure rename is not a regression; a file the change
adds has no merge-base version, so every Tier B finding in it is the
change's own.

When the diff cannot be computed — no `--base`, an unknown ref, a shallow
clone — Tier B reports but never fails. Gating the whole corpus at the
moment the tool cannot tell what changed is the worst option available.
(This is why the `lint` checkout uses `fetch-depth: 0`: without real
history there is no merge base, and the step would silently stop gating.)

A PR into `trunk-dev` ratchets against its merge base, and a push
against the commit before it. The release PR from `trunk-dev` into `trunk`
gets no `ci.yml` run. `.github/workflows/release-pr.yml` ratchets the
whole release there instead. It uses the merge base with `trunk`, or the
commit that added the gate if the merge base predates it. While `trunk` is
older than the gate, its merge base is the last release, so the legacy
population from before the gate would count as new. A push run on
`trunk-dev` can be cancelled, and two clean PRs can merge into a new
finding, so the release PR is the last place to catch either.

**It reads every comment, via a lexer.** Leading and trailing `//`,
`///`, `//!` and `/* */` (nested and doc forms included) all count — a
defect introduced as `let n = 1; // TODO: fix` is gated exactly like one
on its own line. Text inside a string is not a comment, which matters in
both directions: this corpus embeds Rust and SQL snippets in raw strings
(`det_check_tests.rs` fixtures carry `// harvest-suppress:` lines,
`chaos_catalogue_drift.rs` carries a literal commented-out call as test
data), and flagging one would fail CI on an innocent fixture. Run
`cargo xtask audit comment-hygiene --self-test` to check the lexer
against the forms it must get right; CI runs it before the scan.

**It does not cap comment length, and must not start.** The long rationale
blocks in this tree are load-bearing: the ABBA lock-ordering argument at
`materialize_due_child_timeout_deadlines`, the `cohort`-key argument in
`partition.rs`, the codec-rotation scope guarantee `CLAUDE.md` cites as
the proof that engine exception #3 is safe. A word budget over a comment
block would reward deleting precisely those. CH007 is measured per
*sentence*, so a thorough rationale passes cleanly once it is written as
several sentences instead of one.

Add a new audit here, as a `cargo xtask audit` subcommand with its row in
the table above, when a docs (or docs-adjacent UI/generated-artifact)
defect class is mechanical to detect — the point is to make a defect class
un-reintroducible, not to write it up once and move on. Wire it into CI's
`lint` job (ungated by the docs-only-changes filter, so it runs on every
PR including docs-only ones) once it's stable.
