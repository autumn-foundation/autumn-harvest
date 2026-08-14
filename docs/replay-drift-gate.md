# The in-flight replay-drift gate

> **Issue #798.** Export a stratified sample of the executions that are *in
> flight right now*, replay it against the candidate build, and block promotion
> if any of them would diverge.

A determinism regression in a `#[workflow]` body does not fail at compile time
and does not fail on a fresh run. It fails when an **already-running**
execution is resumed by the new code — minutes or hours after the deploy — and
by then the run is nd-blocked (issue #603) or dead-lettered.

This gate closes that window. It is the only check that answers the question a
deploy actually poses:

> *Will the workflows that are running **right now** still replay under the
> build I am about to ship?*

---

## The two-command loop

```bash
# 1. Export a sample of what is in flight, as a replay bundle.
#    --payload-policy full is REQUIRED: a redacted bundle is refused by the
#    gate, because redaction rewrites the activity inputs replay compares
#    against. See "Payloads and secrets" — treat the bundle as production data.
harvest history export-sample \
  --per-workflow 50 \
  --payload-policy full \
  --output-dir ./fixtures/in-flight

# 2. Replay it against the candidate build. Exit code 0 = promote.
cargo run --release --bin replay-drift-gate -- ./fixtures/in-flight
```

Step 2 is a ~15-line binary in your own crate — the CLI deliberately does not
replay, because replaying needs your `#[workflow]` functions linked in and the
`harvest` CLI is an API-only client (see *Why replay lives in your binary*).

---

## Step 1 — export the sample

```bash
harvest history export-sample --payload-policy full --output-dir ./fixtures/in-flight
```

| Flag | Default | Meaning |
|---|---|---|
| `--output-dir <DIR>` | *(required)* | Bundle directory. Created if absent. |
| `--per-workflow <N>` | `50` | At most N executions **per workflow type**, fleet-wide. Clamped to 500. |
| `--states <STATE>` | `RUNNING,PAUSED` | Non-terminal states to sample. Repeatable or comma-separated. A terminal state is rejected with `400`. |
| `--workflow-name <NAME>` | *(all)* | Narrow to one registered type. |
| `--order <oldest\|newest>` | `oldest` | Which end of each type's in-flight population to sample. |
| `--shard-id <N>` | *(all)* | Restrict to one shard. |
| `--payload-policy <full\|redacted>` | `redacted` | **Pass `full` for a gate bundle** — the default is refused by the gate. See *Payloads and secrets*. |
| `--max-bytes <N>` | server default | Per-execution export ceiling. |

The export is **read-only**: `SELECT`-only, no state transition, no event
appended, no task claimed. Running it against production is safe.

### Why per-type, not a global top-N

A plain `LIMIT 50` over an unbalanced fleet returns fifty rows of your noisiest
workflow type and **zero** of everything else — so the gate would verify one
workflow and silently ignore the rest. The export stratifies with
`ROW_NUMBER() OVER (PARTITION BY workflow_name)` so each registered type gets
its own budget. The cap is re-applied **globally** after the cross-shard union,
so `--per-workflow 50` means 50 fleet-wide, not 50 per shard.

### What lands in the bundle

```
./fixtures/in-flight/
├── billing_checkout--0f2c…json      # one fixture per sampled execution
├── billing_checkout--41ab…json
├── onboarding--9d77…json
└── harvest-sample-manifest.json     # coverage record
```

Each fixture is a `HistorySnapshot` — byte-compatible with
`WorkflowReplayer::replay_from_json`, so an individual one can be replayed by
hand. The manifest is read by `replay_bundle` as *coverage*, never replayed as a
fixture.

Re-exporting into the same directory **replaces** the bundle: the previous run's
fixtures are removed first. They would otherwise accumulate (each file name
embeds its execution id, so they never overwrite), while the manifest — which
has a fixed name — *is* overwritten. The gate walks every `*.json`, so the two
would disagree: the manifest would count this run's sample while the gate
replayed executions from every run, some of them long since completed.

Only the manifest and top-level `*.json` files are touched, and only when the
manifest is present — that is the marker for a directory this command wrote. A
directory holding JSON with **no** manifest is refused rather than cleaned, so
`--output-dir .` cannot delete your files because you mistyped a path.

---

## Step 2 — run the gate

```toml
[dev-dependencies]
autumn-harvest = { version = "0.5", features = ["testing"] }
```

```rust
use autumn_harvest::prelude::*;              // brings the `workflows![]` macro into scope
use autumn_harvest::testing::ReplayVerifier;
use my_app::{billing_checkout, onboarding};  // your own `#[workflow]` functions

#[tokio::main]
async fn main() {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./fixtures/in-flight".to_string());

    let mut verifier = ReplayVerifier::new().register(workflows![billing_checkout, onboarding]);

    // If your workers set `WorkerConfig::build_id` (build routing, issue #171),
    // pass the build you are ABOUT TO DEPLOY. See "Build-gated workflows" below.
    if let Ok(build_id) = std::env::var("CANDIDATE_BUILD_ID") {
        verifier = verifier.with_build_id(build_id);
    }

    let report = verifier.replay_bundle(&dir).await;

    println!("{report}");
    std::process::exit(report.exit_code());
}
```

Two things the compiler will hold you to:

* `workflows![…]` takes the **bare function names**, not paths — it expands to a
  call to each function's generated companion. Import the functions (as above)
  rather than qualifying them at the macro call site, and do not name a local
  module `workflows`, which would shadow the macro.
* Use [`ReplayVerifier`], not `WorkflowReplayer`, when you need any of
  `allow_unregistered`, `allow_empty_bundle`, or `require_complete_coverage` —
  those knobs live on the verifier. `WorkflowReplayer::replay_bundle` is a
  convenience shim with the defaults baked in, for a caller that already holds a
  replayer.

[`ReplayVerifier`]: https://docs.rs/autumn-harvest/latest/autumn_harvest/testing/struct.ReplayVerifier.html

`ReplayDriftReport` gives you both a one-line verdict and the detail an operator
needs to act:

| Field | Meaning |
|---|---|
| `total` / `succeeded` | Fixtures replayed, and how many were clean. |
| `diverged` | `Vec<ReplayDrift>` — one per drifting execution, each carrying `execution_id`, `workflow_name`, `kind: NonDeterminismKind`, and `first_divergence`. |
| `blocked` | Fixtures the harness could not evaluate (unparseable, an unregistered workflow type, a redacted export, offloaded payload references, or undecoded codec envelopes). |
| `skipped` | Orphan fixtures, when `allow_unregistered(true)` is set. |
| `coverage` | The parsed `SampleManifest`, when the bundle carries one. |
| `is_clean()` | `exit_code() == 0`. Always agrees with `exit_code()`. |
| `exit_code()` | See the table below. |
| `zero_coverage_types()` | Workflow types the manifest reports as having in-flight work but sampled **zero** times. |
| `fixture_count_disagrees_with_manifest()` | The bundle holds a different number of fixtures than the manifest declares. |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Clean — promote. |
| `1` | At least one execution diverged. **Do not promote.** |
| `2` | The gate could not fully run. Dominates `1`. A fixture blocked it (unparseable, an unregistered workflow type, a redacted bundle, or a fixture carrying offloaded payload references or undecoded codec envelopes); the bundle holds a different number of fixtures than its manifest declares; **or** the export itself delivered fewer fixtures than the sample selected (it hit the response byte budget, or individual selected candidates failed to fetch). |
| `3` | Nothing was verified and `allow_empty_bundle(false)` (the default) — the bundle was empty, **or** every fixture in it was skipped. |
| `4` | `require_complete_coverage(true)` and complete coverage was not proven — the manifest reports partial shard coverage, the bundle carries no readable manifest, **or** a workflow type with in-flight work was sampled zero times. |

Rungs `2`, `3` and `4` all exist for the same reason: a gate that reports green
having verified less than you think is worse than no gate. Each is a distinct way
that can happen, so each names the concrete fix in the report body rather than
collapsing into a bare non-zero.

Exit `2` covers a bundle the gate cannot *honestly evaluate*, and reports it as a
harness error rather than drift. A redacted export, a fixture carrying
large-payload claim-check envelopes ([issue #524](https://github.com/autumn-foundation/autumn-harvest/issues/524)),
and a fixture whose payloads are still codec envelopes
([issue #608](https://github.com/autumn-foundation/autumn-harvest/issues/608))
would all compare the candidate's real inputs against stand-in values and report
a determinism regression that does not exist — a false red on a clean deploy. Re-export
with `--payload-policy full`; either sample a workflow whose payloads stay under
the offload threshold or raise `payload_offload_threshold` for the exported window;
and on an encrypted deployment enable read-path decoding (see *Payloads and secrets*).
A fixture count that disagrees with the manifest means files went missing after the
export (a truncated artifact upload, a partial copy): every surviving fixture may
replay cleanly while the gate certifies a strict subset of what was sampled.

Exit code `3` is deliberate. A gate that passes because it pointed at an empty
or mistyped directory is worse than no gate at all — it reports green having
verified nothing. The same applies when every fixture was *skipped*: with
`allow_unregistered(true)` set and none of the sampled types registered on the
gate binary, the bundle is non-empty but no workflow ran. If your fleet is
legitimately idle (a fresh environment, a pre-production deploy window), opt out
explicitly:

```rust
ReplayVerifier::new()
    .register(workflows![billing_checkout, onboarding])
    .allow_empty_bundle(true)   // an idle fleet is a valid green
    .replay_bundle(&dir)
    .await
```

Exit code `4` is the opposite knob: turn it on when a partial cross-shard read
must not count as a pass. It **fails closed** — a bundle carrying no readable
manifest at all also exits `4`, because "complete coverage was not proven" is
the honest reading of missing evidence. It also catches a subtler hole: a
workflow type the manifest reports as having in-flight work but **zero** sampled
fixtures (starved by the global sample cap, say). Every fixture present replays
clean and the shard status reads `complete`, so nothing else notices that an
entire workflow type went unverified:

```rust
ReplayVerifier::new()
    .register(workflows![billing_checkout, onboarding])
    .require_complete_coverage(true)   // a partial (or unproven) read is not a pass
    .replay_bundle(&dir)
    .await
```

### Match the deployment's history policy

`continue_as_new_threshold` is a **deployment** setting, and a workflow branching
on `ctx.should_continue_as_new()` takes a different path under a different
threshold. Replaying with the default while your fleet runs another value tests
code your fleet never executes, so pass the same policy the deployment uses:

```rust
ReplayVerifier::new()
    .register(workflows![billing_checkout, onboarding])
    .with_history_policy(deployment_history_policy())   // the same value HarvestBuilder gets
    .replay_bundle(&dir)
    .await
```

`WorkflowReplayer::with_history_policy` carries through to `replay_bundle` too,
so a caller that already holds a configured replayer keeps its policy.

### Build-gated workflows

If your workflows branch on `ctx.build_id()` — the build-routing pattern from
issue #171, e.g. rolling a new code path out under `if ctx.build_id() ==
Some("v2")` — pass the **candidate** build id, meaning *the build you are about
to deploy*:

```rust
ReplayVerifier::new()
    .register(workflows![billing_checkout, onboarding])
    .with_build_id("v2")            // NOT the build that recorded the fixtures
    .replay_bundle(&dir)
    .await
```

This one is the opposite of every other value the gate threads, and getting it
backwards makes the gate say **yes** when it should say no:

|                       | `queue_name`, `workflow_id`, `context_headers`, … | `build_id` |
| --------------------- | -------------------------------------------------- | ---------- |
| Live worker sources it from | the execution's own row                       | its **own** `WorkerConfig::build_id` |
| Correct replay value  | the fixture's recorded value                        | the **candidate** build |
| Carried in the bundle | yes — the export writes it, and the fixture wins    | **no**, deliberately |

Because the live worker reports its own configured build rather than the
execution's recorded `assigned_build_id`, a build id recorded into the fixture
would be the **old** build's — so replaying under it takes the historical branch
and the candidate-only path is never executed. The gate would then report clean
for code that diverges the moment it is promoted. That is why no `build_id` is
written into the bundle and why the value must come from your CI config.

Leaving it unset makes `ctx.build_id()` report `None` during replay, which is
correct only if your deployment does not set a build id at all.

### Deadline-sensitive workflows

A workflow that calls `ctx.should_continue_as_new()` (issue #772) consults the
execution's live `deadline_at`, and that value **moves**: pausing an execution
(issue #383) suspends the SLA clock, and resuming it pushes the deadline forward
by the pause span.

The export is careful about this. Candidate discovery and the per-fixture
history fetch are separate round-trips, so an execution resumed between them
would otherwise be exported with a history from *after* the resume and a
deadline from *before* it. That pairing reports less remaining budget than the
live worker actually had, so the checkpoint fraction can trip during replay and
emit a `ContinueAsNew` the recorded history cannot contain — a divergence your
candidate build did not cause.

The exporter therefore re-reads the deadline immediately after loading each
history, so the two always describe the same moment. Nothing is required of you;
it is noted here because it explains why a paused-and-resumed execution is safe
to include in a sample rather than something you need to filter out.

The two in-process gates need no configuration here: the [replay canary] and the
replay-diagnosis endpoint both run *inside* the deployed candidate, so they
thread that process's own configured build id automatically.

[replay canary]: ./replay-verify.md

---

## Wiring it into CI

```yaml
name: replay-drift-gate
on:
  pull_request:
  workflow_dispatch:

jobs:
  drift-gate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Export the in-flight sample from staging
        env:
          HARVEST_URL:   ${{ secrets.HARVEST_STAGING_URL }}
          HARVEST_TOKEN: ${{ secrets.HARVEST_STAGING_READ_TOKEN }}
        run: |
          cargo run --release -p autumn-harvest-cli -- \
            history export-sample \
            --per-workflow 50 \
            --payload-policy full \
            --output-dir ./fixtures/in-flight

      - name: Replay the sample against this build
        run: cargo run --release --bin replay-drift-gate -- ./fixtures/in-flight
```

The bundle carries raw payloads (see *Payloads and secrets*), so it stays inside
the job: it is written to the runner's workspace, replayed in the next step, and
discarded with the runner. Do not `actions/upload-artifact` it without a short
`retention-days` and a private repository.

The export step needs a **read-only** credential. The route is classified
`RouteClass::ReadOnly`, so a scoped API token minted with `read` scope
(issue #942) or a principal holding the read-only operator role (issue #776)
reaches it — a CI job never needs a mutate-capable credential to run this gate.

---

## Reading the coverage honestly

A green gate means **the sample** replays clean. It does not mean the fleet
does. The manifest is what tells you the difference:

```json
{
  "status": "complete",
  "states": ["PAUSED", "RUNNING"],
  "sampled_total": 63,
  "in_flight_total": 4127,
  "per_workflow": [
    { "workflow_name": "billing_checkout", "sampled": 50, "in_flight_total": 4021 },
    { "workflow_name": "onboarding",       "sampled": 13, "in_flight_total": 106  }
  ],
  "inspected_shards": [0, 1],
  "unavailable_shards": []
}
```

Here the gate verified 50 of 4,021 `billing_checkout` runs. That is a
**sample**, and the CLI says so out loud:

```
sampled 63 of 4127 in-flight execution(s)

WORKFLOW                                 SAMPLED  IN FLIGHT  TRUNCATED
billing_checkout                              50       4021  yes
onboarding                                    13        106  no

NOTE: the sample is truncated; a clean gate verifies the SAMPLE, not the fleet.
```

Four failure modes the manifest exists to prevent:

* **Silent truncation.** `sampled: 50, in_flight_total: 4021` is stated, never
  implied. A workflow type whose entire sample failed the per-execution size
  ceiling still appears with `sampled: 0` and its real `in_flight_total`, so you
  can never mistake "nothing exported" for "nothing in flight".
* **Silent partial coverage.** An unreachable shard degrades `status` to
  `partial` and names the shard — the bundle is a *lower bound*. The CLI prints
  a `WARNING … LOWER BOUND` block, and `require_complete_coverage(true)` turns
  it into a hard failure.
* **A silently size-capped export.** One request holds every exported document
  in memory before responding, so the endpoint enforces an aggregate byte budget
  on top of the per-execution `max_bytes` — without it, the fixture-count cap
  alone permits ~20 GiB on a shared management API. Hitting the budget stops the
  export and sets `truncated_by_size`, which the CLI reports and the gate prints
  as `TRUNCATED BY SIZE`.

  This is **not** the same as the `NOTE: the sample is truncated` line above, and
  the difference is what you act on: that one is the *intended* truncation of
  sampling `--per-workflow` out of a larger population; this one is an
  *unplanned* resource limit. Raising `--per-workflow` makes it strictly worse —
  narrow the export instead (fewer states, a single `--shard-id`, a lower
  `--max-bytes`).
* **A silently *biased* export.** A candidate the sample *selected* can still
  fail to export — its history exceeds `--max-bytes`, or its shard becomes
  unreadable between selection and fetch. The export records the failure and
  moves on, so the bundle holds fewer fixtures than the sample chose.

  This one is invisible to every other field, which is why it gets its own:
  `sampled_total` counts only the **survivors**, so it agrees with the file
  count exactly and the bundle looks whole. `export_failures` carries the
  count, and the gate exits `2` on it. The bias is not random — it is against
  the *largest* histories, which are the longest-running and therefore the most
  likely to span the change you are gating, so the executions most worth
  replaying are exactly the ones dropped. Raise `--max-bytes`, or narrow the
  sample until every selected candidate fits.

Both shortfall causes — the byte-budget cut and dropped candidates — are read
through one predicate, `SampleManifest::is_incomplete_export()`, and both fail
the gate **unconditionally**. They do not wait for
`require_complete_coverage(true)`: that flag is about the sample you *asked for*
being a slice of the fleet, which is the gate's normal mode and only a failure
if you say so. These are about the bundle being a silent subset of *that slice*,
which nobody opted into.

### Raising confidence

* Raise `--per-workflow` (up to 500) for the types you care most about, or run
  the gate once per type with `--workflow-name`.
* `--order oldest` (the default) samples the **longest-running** executions —
  the ones most likely to span the code change, and therefore the highest-signal
  slice.
* This gate does not attempt exhaustive fleet replay or a statistical
  confidence bound. It is a sample, deliberately.

---

## Payloads and secrets

**A gate bundle must be exported with `--payload-policy full`.** The CLI default
is `redacted` — the safe default for the `history export` family generally — but
a redacted bundle is **refused** by the gate rather than replayed:

```
BLOCKED  ./fixtures/in-flight/billing_checkout--0f2c….json — invalid fixture:
  fixture was exported with payload_policy=redacted, which rewrites activity
  inputs and outputs and therefore cannot be replayed (every fixture would
  report a false divergence); re-export the bundle with `--payload-policy full`
```

That refusal (exit `2`) is deliberate, and it is the reason the recipe passes the
flag explicitly. Redaction rewrites payload-bearing fields *in place*, and replay
compares the input the candidate code computes against the input recorded in
history — so against a redacted fixture every workflow that passes a non-trivial
activity input diverges. Replaying such a bundle would not produce a weaker gate;
it would produce a **confidently wrong** one, reporting a determinism regression
in code that is fine. Refusing to answer is the honest outcome, so the gate
blocks and names the fix instead. The export step also warns at write time, so
you find out when you create the bundle rather than when you try to gate on it.

### Encrypted deployments need read-path decoding enabled

`--payload-policy full` selects the *policy*; it does not by itself decode a
payload codec ([issue #608](https://github.com/autumn-foundation/autumn-harvest/issues/608)).
The export loads history undecoded, and the read-path decoder engages only when
**both** hold:

1. the deployment opted in with `HarvestPlugin::decode_payloads_on_read()`, and
2. the export call is made by an **admin** principal.

Miss either and, on a codec-encrypting deployment, every payload-bearing field
reaches the bundle as ciphertext in a codec envelope. The offline gate has no
codec registry, so it refuses those fixtures rather than replaying them:

```
BLOCKED  ./fixtures/in-flight/billing_checkout--0f2c….json — invalid fixture:
  fixture carries 3 undecoded codec envelope(s) and 0 undecodable-payload
  marker(s) (issue #608) instead of the real payloads, so replaying it would
  compare the candidate's real inputs against ciphertext and report drift that
  does not exist. Re-export with payload decoding enabled
  (`HarvestPlugin::decode_payloads_on_read()`, and call the route as an admin),
  or run the gate against a deployment with no payload codec registered.
```

The same refusal covers the `_harvest_undecodable` marker a lossy decode leaves
behind when it *tried* and failed (unregistered codec, bad base64, codec error,
invalid JSON): the plaintext is gone either way, so replaying it would be just as
dishonest. Both are exit `2`, for the same reason redaction is — a confidently
wrong "your code drifted" is worse than an honest "I cannot evaluate this".

When decoding **is** enabled, the fixtures carry plaintext. **Treat a `full`
bundle as production data**: do not commit it, scope the CI artifact's retention
tightly (or skip the artifact entirely and replay in the same job), and mint the
export credential `read`-scoped.

If payload exposure is unacceptable in your environment, this gate is not the
right check for you — prefer the server-side replay canary (#512), which replays
in-process against live executions and never materializes a fixture.

---

## Why replay lives in your binary, not the CLI

Replaying a workflow requires the `#[workflow]` functions themselves to be
linked in. The `harvest` CLI is an API-only client with no knowledge of your
code, so it exports and stops. The gate binary is yours, roughly fifteen lines,
and it is what makes the check meaningful: it replays against *the exact build
you are about to promote*.

---

## Relationship to the other replay checks

| Check | Replays | Answers |
|---|---|---|
| **This gate** (#798) | A stratified sample of **in-flight** histories | "Will running executions survive this deploy?" |
| `replay-verify` (#251), [`docs/replay-verify.md`](replay-verify.md) | A curated fixture directory of **completed** histories, strictly | "Does this change break the histories we chose to pin?" |
| Replay canary (#512) | Live in-flight executions, server-side, post-deploy | "Is the build that is *already running* diverging?" |
| `GET /workflows/{id}/replay-diagnosis` (#614) | **One** named execution | "Why is *this* run nd-blocked?" |
| Schema contract gate (#794), [`docs/workflow-schema-contract-guide.md`](workflow-schema-contract-guide.md) | Nothing — static payload/serde diff | "Did a payload type change shape incompatibly?" |

They are complements, not substitutes. `replay-verify` uses **strict** replay
because a completed history must replay to completion. This gate uses
**frontier-tolerant** replay because a healthy in-flight execution *correctly*
suspends at its recorded frontier — replaying it strictly would report every
single one as a false divergence. That is why `replay_bundle` exists alongside
`verify_dir` rather than replacing it.

---

## Where it sits in the deploy ladder

1. Drain workers (#386)
2. Handler-coverage gate — [`docs/runbooks/safe-deploy.md`](runbooks/safe-deploy.md)
3. Schema contract gate (#794)
4. **This gate — in-flight replay drift (#798)**
5. Replay canary (#512), post-deploy
6. Non-determinism block alerting (#603)

Steps 3 and 4 are the pre-cutover pair: #794 catches a payload *shape* change
statically; this gate catches a *control-flow* change by actually replaying.

---

## Troubleshooting

**Exit 3, "bundle is empty".** The directory has no fixtures. Either the export
step did not run, the path is wrong, or the fleet is genuinely idle. Check
`harvest-sample-manifest.json` — if `sampled_total` is `0` and
`in_flight_total` is `0`, the fleet is idle and `allow_empty_bundle(true)` is
the right answer.

**Exit 2, "unregistered workflow".** The bundle contains a workflow type your
gate binary does not register. Either add it to the `workflows![…]` list, or —
if the type is genuinely being retired — set `allow_unregistered(true)` to
downgrade orphan fixtures to `skipped`. Cross-check with
[`docs/runbooks/safe-handler-removal.md`](runbooks/safe-handler-removal.md)
before removing a handler that still has in-flight work.

**Exit 1 with a divergence.** Read `first_divergence` — it names the recorded
event and what the new code emitted instead. Then feed the specific
`execution_id` to `POST /workflows/{id}/replay-diagnosis` (#614) against
staging for the full picture, and see
[`docs/workflow-determinism-guide.md`](workflow-determinism-guide.md) for the
remediation patterns (`ctx.patched`, `ctx.version`).

**Every fixture diverges after a legitimate change.** That is the gate working.
Gate the change with `ctx.patched("your-change-id")` so pre-change executions
keep the old branch, deploy, and re-run.
