# Upgrade check: a verdict for each in-flight run

The upgrade check runs before a deploy. It gives each in-flight run one
verdict against a candidate build (issue #1995):

| Verdict | Meaning | Action |
|---|---|---|
| `migrate` | The candidate build can take the run over. | Declare compat for the candidate build. |
| `review` | A person must look first. | Read the findings. Then pin the run, or accept the change. |
| `pin` | The candidate build cannot run it. | Keep workers on the run's current build until the run ends. |

The check covers three failure modes:

| Failure mode | Check | Worst verdict |
|---|---|---|
| Determinism violation | A canary replay of the recorded history under the candidate code. | `pin` |
| Rehydration failure | The replay decodes each recorded payload into the candidate types. Candidate schemas check the workflow input, signal and update payloads, and each signal that waits in `harvest_signals`. | `pin` |
| Behavioral drift | A diff of the command-emitting call graph that `harvest-verify` resolves. A changed body that the run can still execute needs review. | `review` |

The verdicts are conservative. A change that the check cannot place gives
`review`, never `migrate`. The design and its proofs are in
[`DESIGN-1995.md`](../DESIGN-1995.md).

---

## 1. Write a structure manifest for each build

Run `harvest-verify` on the source of each build, with the same toolchain:

```console
$ git checkout v1.4.0
$ cargo harvest-verify -p my-workflows --lib --emit-structure old.structure.json
$ git checkout v1.5.0
$ cargo harvest-verify -p my-workflows --lib --emit-structure new.structure.json
```

The manifest lists each body that a workflow can reach, with a digest, its
call sites and the commands it emits. A digest ignores spans, so a line shift
does not change it. See
[the `harvest-verify` guide](harvest-verify.md#structure-manifest).

## 2. Add the command to the candidate build

The check runs inside the candidate build, because only that build holds its
workflow types and codec keys. Add a small binary next to the worker binary.
Enable the `db` and `testing` features of `autumn-harvest` for it. Register
the same workflows, signals, updates, codecs and offloader as the worker:

```rust,ignore
use std::sync::Arc;
use autumn_harvest::upgrade_check::{UpgradeCheck, run_command};

#[tokio::main]
async fn main() {
    let check = UpgradeCheck::new()
        .register(workflows![place_order])
        .signals(signals![approve])
        .with_codecs(Arc::new(my_codecs()));
    std::process::exit(run_command(check, std::env::args().collect()).await);
}
```

The `upgrade_check` example in `autumn-harvest/examples/` is a complete
binary. Use `with_offloader` when the worker offloads large payloads. Use
`map_replayer` to give the replay shared state or the candidate build id.

## 3. Run the check

The check reads the in-flight runs once. A run that starts on the baseline
build after that read is not in the report. So close the baseline set first:
point the build policy of each queue at the candidate build, so every new
run starts there. See
[Build-id routing](runbooks/safe-deploy.md#runbook-build-id-routing-for-safe-rolling-deploys).
Then run the check. Without build routing, stop new starts of the checked
workflows until you act on the report.

```console
$ my-upgrade-check \
    --database-url-env SHARD0_URL --database-url-env SHARD1_URL \
    --baseline-structure old.structure.json \
    --baseline-build sha-old456 \
    --candidate-structure new.structure.json
```

| Flag | Effect |
|---|---|
| `--database-url-env NAME` | One shard, read from the environment variable `NAME`. Repeat it in shard order. |
| `--database-url URL` | One shard, given on the command line. Other local users can see it in the process list, so prefer `--database-url-env`. With neither flag, the check reads `HARVEST_DATABASE_URL`. |
| `--baseline-structure FILE` | The manifest of the build that runs now. |
| `--baseline-build ID` | The build that the baseline manifest describes. The check reads only runs assigned to it or to no build: compat from the candidate covers no other run. With no baseline build, each run with an assigned build needs review. |
| `--candidate-structure FILE` | The manifest of the candidate build. Give both manifests, or neither. With neither, each run gets `review`. |
| `--workflow-name NAME` | Check the runs of one workflow type only. |
| `--limit N` | The most runs read from one shard id, aliased shard ids included. The default is 10000. More runs make the check incomplete. |
| `--format text\|json` | The output format. The default is `text`. |

| Exit code | Meaning |
|---|---|
| `0` | Each run gets `migrate`. |
| `1` | At least one run gets `review` or `pin`. |
| `2` | The check is incomplete: a shard failed, a shard held more runs than the limit, or an input was bad. |

A flag also takes its value as `--flag=value`. The report goes to stdout,
and an error goes to stderr.

The check reads `RUNNING` and `PAUSED` runs. It reads inside read-only
transactions and writes nothing.

## 4. Read the findings

| Finding | Verdict | Meaning |
|---|---|---|
| `workflow-not-registered` | `pin` | The candidate build has no handler for the workflow type. |
| `nondeterminism` | `pin` | The candidate code emits different commands from the recorded history. |
| `replay-failed` | `pin` | The candidate code fails the run on replay. Often a recorded payload does not fit a changed type. |
| `payload-schema-violation` | `pin` | A recorded or pending payload breaks a candidate schema. |
| `history-undecodable` | `pin` | The candidate codecs cannot decode the history, a pending signal or the context headers. A codec key is missing, or a payload does not decrypt. |
| `replay-timed-out` | `review` | The replay ran longer than the replay timeout, 30 seconds by default. |
| `payload-unchecked` | `review` | A signal, recorded or pending, or an open update has no candidate schema. Replay can pass a buffered signal with no decode. Publish a schema to remove this finding. |
| `payload-offloaded` | `review` | The run holds offloaded payloads, and the check has no offloader. Pass one with `with_offloader`. |
| `structure-unavailable` | `review` | A manifest is missing, the two manifests come from different toolchains or models, two workflows share the name, or the run is assigned to another build than the baseline. |
| `unknown-boundary` | `review` | `harvest-verify` cannot see part of the workflow graph. A declarative update handler is such a part. |
| `root-changed` | `review` | The workflow body itself changed. |
| `step-not-passed` | `review` | A changed helper may still run for this run. |

A finding holds code names and event indexes only. It never holds a payload.

### When a changed helper counts as passed

A change to a helper body gives `migrate` only when the history proves that
the run finished the helper. Each rule must hold in both manifests that
have the helper, because the run ran the baseline code:

- The helper is not the workflow body.
- The helper starts at most once: one call site on each body up to the
  workflow body, and no call site in a loop.
- Only the helper calls the bodies under it, and none of their calls is in
  a loop.
- Each command the helper can emit has a known name, such as the activity
  name or the timer id. A call that can park with no command, such as
  `await_condition`, has no such name.
- No other body emits a command of the same kind with the same name or an
  unknown name.
- The run completed each command as often as the helper emits it, and none
  is still open.
- A decision ran after the last result of those commands. So a `PAUSED` run
  that holds a result with no decision after it needs review.

So factor long workflows into helper steps. A change to a step that every run
has passed then gives `migrate`.

## 5. Act on the verdicts

`declare_compat` works per build, not per run. So:

- **Every run gets `migrate`.** Declare compat from the candidate build to the
  current build, after the check and never before it. See
  [Build-id routing, Scenario A](runbooks/safe-deploy.md#scenario-a-backward-compatible-deploy-new-code-can-replay-old-history).
- **A run gets `pin`.** Do not declare compat. Keep workers on the current
  build until those runs end. `build_reachability` says when the old build is
  safe to retire.
- **A run gets `review`.** Read its findings. Then accept the change for
  that run, or treat the run like a `pin`. Declare compat only when each
  run gets `migrate`, or a person accepts each `review` finding.

## Trust boundary

The check decodes payloads in memory with the candidate codecs. No history
is exported, so no history needs scrubbing. A run verdict holds no payload
and no error text. The `incomplete` list holds shard database errors only.
Candidate workflow code runs during the replay, so keep payloads out of its
logs.

## Known limits

- The check covers activity and side-effect payloads only through replay.
  Issue #1994 adds schemas for them.
- Value-level drift is invisible to the call graph. A changed static or
  configuration value can change behavior with no digest change.
- A step name that comes from a function parameter is unknown, so that
  helper never counts as passed.
- A helper in a crate that `harvest-verify` does not analyze is a boundary,
  so the verdict is `review`.
- MIR text is not a stable API. Write both manifests with the same toolchain.
  The check refuses two manifests from different toolchains.
- Nothing ties a manifest to the binary under test. Write both from the
  commits that the two binaries come from.
- The replay timeout cannot stop candidate code that never yields.
