# Property-based tests & fuzzing

This repo has two complementary randomized-testing layers, both bootstrapped in
the property-testing workstream:

- **Property tests** ([`proptest`]) — fast, deterministic-seeded, run on every
  push. They assert *invariants* of pure functions (totality, monotonicity,
  bounds, round-trips) over structured, strategy-generated inputs.
- **Fuzz targets** ([`cargo-fuzz`] / libFuzzer) — coverage-guided, nightly
  toolchain, run every night with a persisted corpus. They hammer
  parsers/deserializers with byte-level adversarial input that property
  strategies never construct, and drive the replayer with structured histories.

Neither replaces the other: property tests are a CI regression net; fuzzing is a
soak tool for the functions that eat untrusted bytes or stored data.

---

## Property tests

### Where they live

A single external test target, `autumn-harvest/tests/property/` (mirrors the
`tests/integration/` convention — one `mod.rs` entry point), plus a few
in-crate `#[cfg(test)]` proptests for non-public targets.

- Pure, non-`db`-gated suites (compile under `--no-default-features`):
  `policy_props`, `queue_fairness_props`, `task_duration_props`,
  `completion_trigger_props`, `completion_callback_props`, `event_serde_props`.
- `db`-gated suites (only compiled with the `db` feature, because their target
  modules are `#[cfg(feature = "db")]` — but they test **pure functions** and
  need no live Postgres): `build_routing_props`, `dlq_props`.
- In-crate proptests (private / `pub(crate)` targets unreachable from an
  external test crate): `worker::nd_block_backoff` (private, `db`-gated) and
  `context::remaining_secs_until` (`pub(crate)`).

### Running them

Fast default (128 cases — well under a minute):

```bash
# Pure suites + in-crate no-db proptests:
cargo test -p autumn-harvest --no-default-features --test property
cargo test -p autumn-harvest --no-default-features            # includes the property target + lib proptests

# db-gated suites (build_routing_props, dlq_props) + in-crate db proptests.
# Pure functions only — no Docker / Postgres required, just the db feature build:
cargo test -p autumn-harvest --features db --test property
cargo test -p autumn-harvest --features db --lib             # runs worker::nd_block_backoff
```

Deep run (crank the case count via proptest's native `PROPTEST_CASES` knob):

```bash
PROPTEST_CASES=100000 cargo test -p autumn-harvest --no-default-features --test property
PROPTEST_CASES=100000 cargo test -p autumn-harvest --features db --test property
```

### Conventions (see `tests/property/prop_config.rs`)

- **Bounded default**: 128 cases, so the suite is a fast per-push net rather than
  a soak. `PROPTEST_CASES=<n>` overrides it upward (it's proptest's own env knob;
  we read it explicitly so our low hardcoded default stays overridable).
- **No on-disk regressions**: `failure_persistence = None`, so CI runners stay
  artifact-free (no `proptest-regressions/`). A discovered counterexample is
  printed in the shrunk panic message; reproduce by re-running, or pin it as an
  explicit `#[test]`. For the lifecycle model, add the sequence to `PINNED`.

### CI

The pure suites run in the `test` job, through `cargo test -p autumn-harvest
--no-default-features`. The `db`-gated suites run through the `allos` row
`property db` in `.github/ci/integration-suites.txt`.

The nightly workflow `.github/workflows/proptest-nightly.yml` runs 100000
cases of each deep pass: the `property` target, and the lifecycle model in
8 shards. A failed scheduled run opens an issue. The guard
`proptest_nightly_runs_both_deep_passes` in `ci_run_coverage.rs` keeps it
wired. The in-crate proptests in `src/` are not part of the deep pass.

### Stateful lifecycle model (issue #1829)

`tests/integration/lifecycle_model_props.rs` is a stateful, model-based
property test, in the style of ShardStore (SOSP'21). Proptest generates a
sequence of client operations: start, claim, heartbeat, park, complete,
signal, cancel, worker death, worker revival and orphan reclaim. Each
operation runs against a real Postgres and against a small reference
model. After each operation the test asserts three things:

1. The operation returns what the model predicts.
2. The rows of the case equal the model state.
3. Each run state change is in `lifecycle::TRANSITIONS`.

The model follows the documented contracts: the reuse-policy matrix, the
claim fence `(worker_id, attempt)`, the lost-wake rule of the park, and the
orphan reclaim rules. Short motifs in the strategy reach rare branches,
such as a wake that races a park. A coverage check fails the run when no
case reaches a branch.

The test also compares the lifecycle events of each run and the
dead-letter count. A claim must take the pending task with the earliest
claim-order due time. Since issue #1824, a new start sorts 30 seconds later.
The check reads `queue::CLAIM_ORDER_DUE_SQL`, so it cannot drift from the
claim.

The test needs Docker, or `HARVEST_TEST_DATABASE_URL`. Each case truncates
the engine tables, so with that variable set the test creates a throwaway
database on the server and drops it at the end. The DSN can be a URL or a
libpq keyword/value string.

```bash
cargo test -p autumn-harvest --test integration lifecycle_model_props::
PROPTEST_CASES=2000 cargo test -p autumn-harvest --test integration lifecycle_model_props::
```

CI runs the default 128 cases through the `linux` manifest row, in the
`test-db-linux` job. That job is not a required check yet. One case costs
about 150 ms, so the nightly runs 8 shards of 12500 cases, 100000 in total.
A shrink stops after 20 minutes, so a late failure still prints its
sequence.

A failure prints the shrunk operation sequence and the first step where the
database and the model disagree. To keep a counterexample, add its sequence
to `PINNED`, with the coverage labels it must reach.
`pinned_counterexamples_replay` replays each pinned sequence against the
database in the `test-db-linux` job. Put a model-only check in the model
self-tests at the end of the file.

---

## Fuzz targets

### Requirements

- The **pinned nightly** toolchain (libFuzzer needs `-Z` flags):
  `rustup toolchain install nightly-2026-08-14`
- **cargo-fuzz**: `cargo install cargo-fuzz`

The pin is `env.FUZZ_TOOLCHAIN` in `.github/workflows/fuzz-nightly.yml`.
The 1.100 nightlies that were tested (2026-09-01 and later) fail to build the
`db` feature: E0275, a recursion-limit error or an ICE in the diesel
transaction futures. Move the pin forward, in the workflow and in
`fuzz/smoke.sh`, once a newer nightly compiles the crate. The guard
`fuzz_toolchain_pins_agree` keeps the two pins equal.

The harness lives in `fuzz/` — a crate deliberately **excluded** from the root
workspace (root `Cargo.toml` `[workspace] exclude`, and its own empty
`[workspace]` table), so `cargo build`/`cargo test` at the repo root never
touches these nightly-only, libFuzzer-only binaries.

### Targets

| Target | Function under test | Why raw-bytes fuzzing (vs. proptest) |
|--------|---------------------|--------------------------------------|
| `fuzz_workflow_event_deser` | `serde_json::from_slice::<WorkflowEvent>` | Drives serde_json's parser over byte-level malformed/truncated event JSON — every history read path deserializes this append-only enum. Invariant: never panics. |
| `fuzz_det_check_source` | `det_check::check_source` | Highest-value target: a hand-rolled line/brace scanner over arbitrary Rust source, with a history of parity churn against the proc-macro lint. Runs over user code in CI/editors. Invariant: total, never panics. |
| `fuzz_validate_target_url` | `completion_callback::validate_target_url` | SSRF security boundary (issue #605): drives the `url` parser + IPv4/IPv6 literal classification with the most permissive policy so the deepest branches are reached. Invariant: never panics. |
| `fuzz_failure_signature` | `dlq::failure_signature` | Normalizes unbounded, adversarial error text (unicode, multi-byte boundaries) into a shard-stable key. `db`-gated (the `fuzz` crate enables the `db` feature). Invariants: never panics; output `<= SIGNATURE_MAX_LEN` chars. |
| `fuzz_replay` | write path, read path and `WorkflowReplayer` | Structure-aware (issue #1835). See below. |

Each target converts fuzzer bytes to the input the function wants
(`String::from_utf8_lossy` for the source/URL/error targets; the raw slice for
the deserializer) and keeps the body minimal.

### The replay target (issue #1835)

`fuzz_replay` feeds an arbitrary `Vec<WorkflowEvent>` to the replayer. The
`fuzzing` feature of `autumn-harvest` derives `arbitrary::Arbitrary` for
`WorkflowEvent` and its field types. The feature is not a stable API and is
not covered by semver. Clippy runs with `--all-features`, so these edits fail
the `lint` job until they are made:

- A new `serde_json::Value` field needs a `crate::fuzzing::value` attribute.
- A new variant needs an arm in `fuzzing::mirror`. A variant that a command
  records also needs an `Op`, so that replay can pass it.

The JSON generator builds the reserved `_harvest_*` shapes on purpose: codec
envelopes, offload references, erasure tombstones and undecodable markers.
Plain random JSON almost never has those shapes. Issues #1253 and #1758 were
replay corruption from such data.

`autumn_harvest::fuzzing::check_case` runs each case twice through the
write path (codec encode, then offload), the read path (inflate, then codec
decode) and the replayer. The replayed workflow issues the commands that
the history records, so replay goes past the first event. `fuzzing::mirror`
models join batches, races, fan-outs (fail-fast and collect-all), saga
unwinds, DAG skips, worker sessions, cancellable timers, signal and child
timeouts, deadline probes, version and patch markers, and redrives. A
history that the mirror does not model still runs.
It ends at an early mismatch, which costs coverage but no oracle. The
oracles:

1. Nothing panics.
2. A history that the write path stores reads back unchanged, through the
   strict read and through the lossy read of history export. A write error
   or a read error on such a history is a failure too.
3. Both runs give the same report.

The oracle compares after a JSON text round trip. Storage keeps text, and
serde_json parses some floats back one digit off. That loss is not a
pipeline defect. A history with a NUL character is skipped, because
Postgres `jsonb` cannot store it.

The replayer's own inflate path is not fuzzed. `replay_from_db` decodes
before it inflates, the reverse of the worker's read order.

Input that starts with `{` is a JSON case, not `arbitrary` bytes. The seeds
in `fuzz/seeds/fuzz_replay/` use this form, so they stay valid when the
generator changes. The seed corpus holds the #1253 and #1758 reproducers.
`tests/integration/replay_fuzz_seeds.rs` runs every seed, and the generator
over fixed bytes, on stable Rust in CI:

```bash
cargo test -p autumn-harvest --no-default-features --features fuzzing \
  --test integration replay_fuzz_seeds::
```

To add a reproducer, write the JSON case to `fuzz/seeds/fuzz_replay/` with
the issue number in its name. Check that the seed fails with the fix
reverted. A codec or offload setting can hide the bug.

A raw crash input decodes through `arbitrary`, so it stops reproducing when
the generator changes. Convert it to a JSON case before you commit it:

```bash
FUZZ_REPLAY_PRINT_JSON=1 cargo +nightly-2026-08-14 fuzz run fuzz_replay path/to/crash-<hash>
```

### Running them

```bash
# Build all targets (nightly + libFuzzer):
cd fuzz && cargo +nightly-2026-08-14 fuzz build

# Run one target (Ctrl-C to stop; grows a corpus under fuzz/corpus/<target>/):
cargo +nightly-2026-08-14 fuzz run fuzz_det_check_source

# Run with the committed seeds as a second, read-only corpus:
cargo +nightly-2026-08-14 fuzz run fuzz_replay corpus/fuzz_replay seeds/fuzz_replay

# Reproduce a crash input from a CI artifact:
cargo +nightly-2026-08-14 fuzz run fuzz_replay path/to/crash-<hash>

# Time-boxed run of one target:
cargo +nightly-2026-08-14 fuzz run fuzz_det_check_source -- -max_total_time=300

# Quick smoke of every target (~15s each) — the manual/local helper:
./fuzz/smoke.sh
# ...or override the per-target budget:
MAX_TOTAL_TIME=60 ./fuzz/smoke.sh
```

### CI

`.github/workflows/fuzz-nightly.yml` runs every target each night for 600
seconds, one job per target (issue #1835).

- **Corpus.** Each run uploads each target's corpus as the artifact
  `fuzz-corpus-<target>`, also after a crash. `cargo fuzz cmin` keeps it
  small. The next run downloads the newest one from a run of this
  repository. It prefers its own branch, then the default branch, and never
  takes a fork's corpus. An artifact stays 30 days, or 7 days for a pull
  request. With none left, the fuzzer starts again from the seeds.
- **Why not the actions cache.** The CI build caches fill the repository's
  cache budget. In a test on the pull request, GitHub evicted every corpus
  entry within 11 minutes, so a nightly corpus would never survive a day.
- **Crash.** The crash input is uploaded as `fuzz-artifacts-<target>`. A
  failed scheduled run opens the issue "Fuzz nightly: a scheduled run
  failed", or comments on the open one. Fix the bug and commit the input to
  `fuzz/seeds/<target>/`. For `fuzz_replay`, commit the JSON form (see above).
- **Manual run.** Run the workflow from the Actions tab. The `seconds`
  input sets the time per target.
- **Pull request.** A change under `fuzz/`, to the workflow, to
  `rust-toolchain.toml`, or to the `autumn-harvest` files that carry the
  derives runs each target for 60 seconds. A draft PR skips it.

The guard `fuzz_nightly_wiring.rs` keeps the target lists of
`fuzz/Cargo.toml`, `fuzz/smoke.sh` and the workflow equal. It also checks
the cron, the corpus cycle and the alert job.

---

## Backlog — candidates for follow-up sessions

Concrete per-subsystem targets not yet covered. Each is a pure/near-pure
function or a totality/round-trip property that fits the same bounded-runtime
harness.

### Property-test candidates

- **C12 — det_check ↔ macro-guardrail differential.** Assert that
  `det_check::check_source` and the proc-macro determinism lint
  (`autumn-harvest-macros/src/determinism_lint.rs`) agree on which
  `#[workflow]` bodies are flagged. This is the highest-value follow-up given
  the historical parity churn, but it needs a **proc-macro test harness**:
  `determinism_lint` is not an invocable `pub fn`, so the differential must
  drive it through a `trybuild`-style compile fixture or a small extracted
  entry point. (Deferred from the bootstrap slice for that reason.)
- **C13 — replay-determinism property under `--features testing`.** Use
  `WorkflowReplayer` to assert that replaying a recorded history in any event
  ordering that the engine permits yields the same commands — reusing the
  #476 "1000 randomized orderings" precedent as a proptest strategy over event
  interleavings. (Deferred: needs the `testing` feature and a generator for
  valid histories.)
- **Scheduler cron parsing** — `parse_schedule_expr_with_tz` / cron+interval
  `next_run_at` computation: total on arbitrary expr strings; a valid expr's
  next fire is always strictly in the future; timezone re-anchoring is
  idempotent.
- **`concurrency::resolve_concurrency_key` / `project_json_path`** — total over
  arbitrary dotted paths and arbitrary JSON; missing vs. present-null
  distinction is stable; never panics on deeply nested / cyclic-shaped input.
- **`throttle::parse_rate` / `ThrottlePolicy::from_rate_str`** — round-trip and
  totality over `"<count>/<unit>"` strings; burst defaulting; rejects malformed
  units without panicking.
- **Rendezvous shard routing stability** — `ShardRouter::pick_for_*`: the same
  `ExecutionId`/workflow_id maps to the same shard across process runs; a pick
  is always within the writable subset; widening `readable_shards` never
  re-routes an id that was already resolvable.
- **`validate_against_schema` vs. serde acceptance** — issue #373: a value that
  `validate_against_schema` accepts also `serde`-deserializes into the target
  type, and vice-versa, for `schemars`-derived schemas (differential property).
- **`completion_trigger` output-guard evaluation** — already partially covered;
  extend to numeric-exactness edge cases (integers above 2^53, mixed-sign)
  and deep combinator nesting at the cap boundary.

### Fuzz-target candidates

- `parse_schedule_expr_with_tz` (cron parser — total over arbitrary strings).
- `TriggerCondition` deserialization + `evaluate` over arbitrary stored JSON
  (the bounded-caps validator is a natural totality target).
- `dlq::DlqAggregateParams::from_query_pairs` / other query-string parsers
  (raw bytes → structured params).

### Production follow-ups surfaced by this workstream

Genuine product-code gaps found while writing the tests (filed separately —
out of scope for the test/tooling slices themselves):

- **SSRF guard `0.0.0.0/8` (and `198.18.0.0/15`) bypass** (issue #1005).
  `completion_callback::is_ipv4_non_routable` blocks only the exact `0.0.0.0`
  (via `is_unspecified()`), so a general `0.10.20.30` in the `0.0.0.0/8` "this
  host" range is not rejected — a documented SSRF bypass (`0.x` routes to
  localhost on Linux); `198.18.0.0/15` (RFC 2544 benchmarking) is likewise
  uncovered. Surfaced by the `validate_target_url` property test
  (`completion_callback_props.rs`). Suggested fix: add `octets()[0] == 0` and
  the `198.18.0.0/15` range to the block list.

[`proptest`]: https://docs.rs/proptest
[`cargo-fuzz`]: https://rust-fuzz.github.io/book/cargo-fuzz.html
