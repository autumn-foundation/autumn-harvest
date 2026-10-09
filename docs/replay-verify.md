# Gating deploys with replay-verify

Replay-verify is the CI gate that ensures code changes to `#[workflow]` functions
do not break in-flight production executions. You run it from your own binary or test,
which calls `ReplayVerifier` (see [Quick start](#quick-start-rust-api)). The `harvest` CLI
has no `replay-verify` subcommand. The `harvest-replay` binary replays one history file
only. The verifier batch-replays exported history fixtures against the current codebase
and exits non-zero on any regression, blocking the merge.

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

### Histories from runs that FAILED

A history that ends in a terminal `WorkflowFailed` is **verified up to its
failure point**, and replays cleanly when the code fails again (issue #952).

A failing decision cycle's history is truncated by construction: the commands it
issued were never turned into events, and a sealed run can never gain another
one. So the gate checks every recorded event positionally — a reordered activity,
a renamed gate, a changed child input *before* the failure still fails the gate —
and then stops. Past the failure point there is nothing to compare against, so a
new command, an early return, or a park is not reported as drift.

Concretely, for a fixture whose last event is `WorkflowFailed`:

| The candidate build… | Verdict |
| --- | --- |
| fails the same way | `ReplaySucceeded`, with the reproduced error in `ReplayReport::failure_message()` |
| parks at the failure point, having consumed every recorded event | `ReplaySucceeded` |
| parks *early*, leaving recorded events unconsumed | `NonDeterminismDetected` — the gate still fails |
| **fixed** the failing check and now completes (consuming every recorded event) | `ReplaySucceeded` |
| diverges from a recorded event *before* the failure | `NonDeterminismDetected` — the gate still fails |
| an engine-detected non-determinism, however the workflow wraps the error | `NonDeterminismDetected` — the gate still fails |
| panics | `WorkflowFailed` — the gate still fails |

One nuance is deliberate: a build whose replay **returns an error** without
reaching every recorded event is not reported. A fail-fast fan-out is exactly
that shape — the first branch to fail aborts its siblings, so the siblings'
recorded events are legitimately never reached — and the recorded run failed for
the same reason. Positional verification still covers every event the code does
reach.

Before this, any failed-run fixture reported a divergence at its terminal event,
which is why teams allowlisted or dropped them from the gate. They can be kept
now: a failed run's history is exactly where a post-mortem needs replay to work.

The same failing cycle also records the awaited work it dispatched and then
abandoned by failing before it could suspend — each dispatched child workflow and
activity appears as its `ChildWorkflowStarted`/`ActivityScheduled` followed by a
synthetic terminal explaining that it never started — so the fixture contains
every command the code issued.

> **`replay-verify` is for *terminal* histories — completed, failed, or
> cancelled — and replays them strictly.**
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
autumn-harvest = { version = "0.7", features = ["testing"] }
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

Pass `ReportFormat::<Variant>` in the API. A binary that you write can map a `--report <format>` flag to it (see the [GitHub Actions snippet](#complete-github-actions-snippet)):

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

`CiReport::exit_code` returns `0` for an empty fixture directory. The binary in the
[GitHub Actions snippet](#complete-github-actions-snippet) exits `2` instead, so an empty
run cannot pass the gate.

---

## `--fail-on` threshold mode

For large fixture sets where occasional transient mismatches are acceptable, use the
rate threshold mode instead of the default any-failure mode:

```rust
// Rust API
let ci = report.into_ci_report_with_threshold(0.95); // fail only if < 95% pass
```

```bash
# The binary from the GitHub Actions snippet below
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

`replay-verify` below is the Quick start binary with three flags added. Replace its
`main` with this one. It also fails when the directory holds no fixtures, because an
empty run proves nothing.

```rust
#[tokio::main]
async fn main() {
    let mut fixtures = String::from("./fixtures/replay");
    let mut format = ReportFormat::Text;
    let mut min_pass_rate: Option<f64> = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().expect("each flag takes a value");
        match flag.as_str() {
            "--fixtures-dir" => fixtures = value,
            "--report" => {
                format = match value.as_str() {
                    "junit" => ReportFormat::JUnit,
                    "json" => ReportFormat::Json,
                    "github" => ReportFormat::GitHub,
                    _ => ReportFormat::Text,
                }
            }
            "--fail-on" => {
                let rate = value.strip_prefix("rate=").expect("use --fail-on rate=<0..1>");
                let rate: f64 = rate.parse().expect("rate must be a number");
                assert!((0.0..=1.0).contains(&rate), "rate must be in 0..=1");
                min_pass_rate = Some(rate);
            }
            other => panic!("unknown flag {other}"),
        }
    }

    let report = ReplayVerifier::new()
        .register(workflows![/* your workflows */])
        .fixtures_dir(&fixtures)
        .verify_all()
        .await;
    if report.fixtures_total == 0 {
        eprintln!("no fixtures in {fixtures}");
        std::process::exit(2);
    }
    let ci = match min_pass_rate {
        Some(rate) => report.into_ci_report_with_threshold(rate),
        None => report.into_ci_report(),
    };
    println!("{}", ci.format_report(format));
    std::process::exit(ci.exit_code());
}
```

The verifier reads one `HistorySnapshot` per file, so the export step splits the batch
envelope into one file per run.

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
      #       --payload-policy full \
      #       --output-file ./batch.json
      #     # A partial or empty export proves nothing, so fail the gate on it.
      #     jq -e '.status == "complete" and (.failures | length == 0) and (.exports | length > 0)' ./batch.json
      #     mkdir -p ./fixtures/replay
      #     jq -c '.exports[]' ./batch.json | while read -r doc; do
      #       id=$(jq -r '.execution_id' <<<"$doc")
      #       printf '%s\n' "$doc" > "./fixtures/replay/$id.json"
      #     done

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
- **Fixture lifecycle:** The verifier reads a directory of `HistorySnapshot` files.
  `harvest history export` writes one such file per run (issue #169).
  `harvest history export-batch` writes one envelope file, so split its `exports`
  array into one file per run first. A `partial` envelope omits histories, so treat
  it as a failed export. Fixture rotation, pruning, and
  auto-export-on-merge are deployment concerns outside the verifier's scope.
