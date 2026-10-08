# Gating deploys with replay-verify

`harvest replay-verify` is the CI gate that ensures code changes to `#[workflow]` functions
do not break in-flight production executions. It batch-replays exported history fixtures
against the current codebase and exits non-zero on any regression, blocking the merge.

## What it catches — and what it does not

**Catches:**
- Activity reordering (e.g. `step_a` before `step_b` became `step_b` before `step_a`)
- Missing `ctx.version()` gates around newly-inserted commands
- Renamed version gates (change-id drift that leaves orphaned `MarkerRecorded` events)
- Timer or signal command reordering
- Child-workflow name or input changes

**Does not catch:**
- Activity logic bugs or side-effect drift (the verifier never executes activities)
- Payload codec mismatches if fixtures were exported with encrypted payloads
- DAG-run replay regressions (a DAG-level verifier is a follow-up feature)

> **`replay-verify` is for *completed* histories, and replays them strictly.**
> To gate a deploy on the executions that are **in flight right now**, use the
> replay-drift gate instead — see
> [`docs/replay-drift-gate.md`](replay-drift-gate.md). It exports a stratified
> cross-shard sample of non-terminal histories (`harvest history export-sample`)
> and replays it with `WorkflowReplayer::replay_bundle`, which is
> *frontier-tolerant*: a healthy in-flight execution correctly suspends at its
> recorded frontier, which strict replay would report as a divergence. The two
> gates are complements — pin curated completed histories here, and sample live
> in-flight work there.
>
> When a gate says a history *does* diverge, step through it interactively with
> `harvest debug` to find the exact command that changed — see
> [`docs/replay-debugger.md`](replay-debugger.md).

---

## Quick start (Rust API)

Add `autumn-harvest` with the `testing` feature to your app or test binary:

```toml
[dev-dependencies]
autumn-harvest = { version = "0.3", features = ["testing"] }
```

Then write a binary or test target that registers your workflows and calls `verify_all`:

```rust
use autumn_harvest::prelude::*;
use autumn_harvest::testing::{ReplayVerifier, ReportFormat};

// Import your workflow functions.
mod workflows { /* ... */ }

#[tokio::main]
async fn main() {
    let report = ReplayVerifier::new()
        .register(workflows![
            workflows::onboarding,
            workflows::refund_saga,
            workflows::billing,
        ])
        .fixtures_dir("./fixtures/replay")
        .verify_all()
        .await;

    let ci = report.into_ci_report();
    println!("{}", ci.format_report(ReportFormat::Text));
    std::process::exit(ci.exit_code());
}
```

---

## Report formats

Pass `--report <format>` on the CLI or `ReportFormat::<Variant>` in the API:

| Format | Description |
|--------|-------------|
| `text` | Human-readable summary with per-fixture pass/fail lines (default) |
| `junit` | JUnit XML — one `<testcase>` per fixture; compatible with GitHub Actions, CircleCI, Jenkins |
| `json` | Structured `BatchReplayReport` JSON for downstream tooling |
| `github` | GitHub Actions `::error file=…` annotations surfaced inline on PRs |

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | All fixtures replayed cleanly |
| `1` | One or more replay failures (configurable via `--fail-on rate=0.95`) |
| `2` | One or more harness errors (invalid fixture JSON or unregistered workflow) — dominates over exit 1 |

---

## `--fail-on` threshold mode

For large fixture sets where occasional transient mismatches are acceptable, use the
rate threshold mode instead of the default any-failure mode:

```rust
// Rust API
let ci = report.into_ci_report_with_threshold(0.95); // fail only if < 95% pass
```

```bash
# CLI (harvest-replay binary or downstream app)
my-app replay-verify --fixtures-dir ./fixtures --fail-on rate=0.95
```

---

## `--allow-unregistered`

A single fixtures directory may hold histories from multiple binaries (e.g. a monorepo).
When `--allow-unregistered` is set, fixtures whose `workflow_name` has no registered handler
are silently skipped (counted in `fixtures_total`, not in `harness_errors`), so cross-binary
fixture stores do not produce false exit-2 harness errors:

```rust
ReplayVerifier::new()
    .register(workflows![onboarding])
    .allow_unregistered(true) // skip fixtures for other binaries
    .verify_dir(&dir)
    .await;
```

---

## Complete GitHub Actions snippet

```yaml
# .github/workflows/replay-verify.yml
name: Replay safety gate

on:
  pull_request:

jobs:
  replay-verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # Export fresh fixtures from a staging deployment (or use the fixtures
      # committed to the repo). Optional if your team commits fixtures.
      # - name: Export fixtures
      #   env:
      #     HARVEST_API_URL: ${{ secrets.HARVEST_STAGING_API_URL }}
      #     HARVEST_TOKEN:   ${{ secrets.HARVEST_STAGING_READ_TOKEN }}
      #   run: |
      #     cargo run --release -p autumn-harvest-cli -- \
      #       history export-batch \
      #       --state-group terminal \
      #       --limit 200 \
      #       --output-file ./fixtures/replay/batch.json

      - name: Run replay-verify
        run: |
          cargo run --release --bin replay-verify -- \
            --fixtures-dir ./fixtures/replay \
            --report github

      # Publish JUnit results for the PR checks tab.
      - name: Publish JUnit results
        if: always()
        uses: EnricoMi/publish-unit-test-result-action@v2
        with:
          files: target/replay-report.xml
```

To write the JUnit file (redirect `--report junit` output):

```bash
cargo run --release --bin replay-verify -- \
  --fixtures-dir ./fixtures/replay \
  --report junit > target/replay-report.xml
```

---

## Performance budget

`ReplayVerifier` runs fixtures concurrently (default = available CPUs). The performance
target from issue #251 is:

> **1,000 fixtures × ~1,000 events each in under 30 seconds on a 4-core laptop** (in-memory
> user code, no DB).

Verify against the criterion benchmark:

```bash
cargo bench -p autumn-harvest \
  --features testing --no-default-features \
  --bench replay_verifier_bench
```

---

## Limitations

- **Encrypted payloads:** If fixtures were exported with `HistoryPayloadPolicy::Redact` or a
  custom `PayloadCodec`, the verifier inherits whatever the registered workflow can decode.
  No new key-management surface is introduced by the verifier.
- **DAG runs:** The verifier covers `#[workflow]`-annotated event histories only. A DAG-level
  verifier is a planned follow-up.
- **Fixture lifecycle:** The verifier consumes a fixture directory produced by
  `harvest history export --batch` (issue #169). Fixture rotation, pruning, and
  auto-export-on-merge are deployment concerns outside the verifier's scope.
