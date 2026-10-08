# Property-based tests & fuzzing

This repo has two complementary randomized-testing layers, both bootstrapped in
the property-testing workstream:

- **Property tests** ([`proptest`]) — fast, deterministic-seeded, run on every
  push. They assert *invariants* of pure functions (totality, monotonicity,
  bounds, round-trips) over structured, strategy-generated inputs.
- **Fuzz targets** ([`cargo-fuzz`] / libFuzzer) — coverage-guided, raw-bytes,
  nightly-only, opt-in. They hammer parsers/deserializers with byte-level
  adversarial input that property strategies never construct.

Neither replaces the other: property tests are a CI regression net; fuzzing is a
manual/soak tool for the handful of functions that eat untrusted bytes.

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
  explicit `#[test]`.

### CI

The pure suites execute via the existing `cargo test -p autumn-harvest
--no-default-features` line in the `test` job. The `db`-gated suites execute via
a dedicated **"Run db-gated property suites"** step
(`cargo test -p autumn-harvest --features db --test property`) — they were
previously only *compiled* (never executed) by the `--no-run` / `--lib` db steps.

---

## Fuzz targets

### Requirements

- A **nightly** toolchain (libFuzzer needs `-Z` flags): `rustup toolchain install nightly`
- **cargo-fuzz**: `cargo install cargo-fuzz`

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

Each target converts fuzzer bytes to the input the function wants
(`String::from_utf8_lossy` for the source/URL/error targets; the raw slice for
the deserializer) and keeps the body minimal.

### Running them

```bash
# Build all targets (nightly + libFuzzer):
cd fuzz && cargo +nightly fuzz build

# Run one target (Ctrl-C to stop; grows a corpus under fuzz/corpus/<target>/):
cargo +nightly fuzz run fuzz_det_check_source

# Time-boxed run of one target:
cargo +nightly fuzz run fuzz_det_check_source -- -max_total_time=300

# Quick smoke of every target (~15s each) — the manual/local helper:
./fuzz/smoke.sh
# ...or override the per-target budget:
MAX_TOTAL_TIME=60 ./fuzz/smoke.sh
```

### CI

There is an **opt-in `fuzz-smoke` job** in `.github/workflows/ci.yml`, gated on
`github.event_name == 'workflow_dispatch'` — it **never** runs on push or PR
(Actions minutes are tight and fuzzing needs nightly). Trigger it manually from
the Actions tab ("Run workflow") for a 30s-per-target smoke. Real campaigns run
locally with a larger `-max_total_time`.

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
- `history_export` / read-path payload-codec envelope parsing over arbitrary
  JSON (the `_harvest_*` envelope discriminators).

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
