# `ReplayVerifier::verify_dir`: opaque-payload guard fast-path (negative result)

**Outcome: negative result.** A real, mechanism-backed hypothesis was formed,
implemented, and measured — the targeted cost dropped by 76% exactly as
predicted — but the change's impact on the overall workload (4.26%
instructions) falls short of the ≥5% floor, so it was **reverted**. This note
exists so a future pass does not re-discover and re-attempt the identical
optimization. The benchmark harness that produced these numbers
(`benches/verify_profile.rs`) is committed and kept, since it exercises a
boundary (`ReplayVerifier`'s real filesystem I/O + JSON *deserialize* of
`HistorySnapshot`/`WorkflowEvent`) no prior harness in this repo touched, and
remains available for the next investigation of this code path.

Wall-clock timing is not admissible evidence on this (shared-vCPU) machine —
every number below is a deterministic instruction count
(`valgrind --tool=callgrind`) or allocation count (`valgrind --tool=dhat`),
reproducible bit-for-bit on any machine.

## Workload

`ReplayVerifier::verify_dir` is the batch fixture-verification harness behind
issue #251's budget ("verifying 1,000 fixtures averaging 1k events each
completes in under 30 seconds"). Unlike the existing `replay_profile.rs`
harness (which constructs `Vec<WorkflowEvent>` directly as Rust struct
literals and calls `WorkflowReplayer::replay_from_events`), `verify_dir`
exercises real `std::fs::read_to_string` over a directory walk and
`serde_json::from_str` deserialization of each fixture — the same boundary a
production worker crosses loading a recorded history from `harvest_events`.
No existing harness measured this path before this pass, so
`benches/verify_profile.rs` was built to mirror `replay_verifier_bench.rs`'s
exact fixture shape (`Value::Null` payloads, one `HistorySnapshot` per file)
at a reduced scale (20 fixtures × 500 activities = 20,020 events) that keeps
a single callgrind run tractable — callgrind emulation is one to two orders
of magnitude slower than native execution, and issue #251's full 1,000
fixtures is calibrated against a 30-second *native* budget. Both the targeted
cost and total cost scale linearly with fixture/event count (see
"Why this can't be fixed by re-scaling" below), so the reduced run is
representative, not merely convenient.

## Profile

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench verify_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="verify_profile") | .executable')
VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500 \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out
```

Baseline (unmodified `HEAD`):

```
130,387,162 (100.0%)  PROGRAM TOTALS

27,182,040 (20.85%)  serde_json::read::SliceRead::skip_to_escape
10,374,500 ( 7.96%)  <serde_json::de::MapAccess<R> as serde_core::de::MapAccess>::next_value_seed
 9,372,372 ( 7.19%)  __memcpy_avx_unaligned_erms
 7,695,360 ( 5.90%)  <serde_json::read::StrRead as serde_json::read::Read>::parse_str
 6,515,660 ( 5.00%)  <&str as core::str::pattern::Pattern>::is_contained_in
 5,515,260 ( 4.23%)  <&mut serde_json::de::Deserializer<R> as serde_core::de::Deserializer>::deserialize_any
 5,445,440 ( 4.18%)  <serde_json::read::SliceRead as serde_json::read::Read>::ignore_str
 4,804,800 ( 3.69%)  uuid::parser::try_parse
 4,630,880 ( 3.55%)  <serde_json::de::MapAccess<R> as serde_core::de::MapAccess>::next_key_seed::has_next_key
 3,023,020 ( 2.32%)  WorkflowEvent::deserialize::__Visitor::visit_map
 2,890,361 ( 2.22%)  _int_malloc
 2,810,000 ( 2.16%)  next_key_seed
 2,365,734 ( 1.81%)  _int_free
 2,282,140 ( 1.75%)  PhantomData::deserialize
 2,210,960 ( 1.70%)  sequential_workflow::{{closure}}
```

Everything above `is_contained_in` in this list is `serde_json`'s own
generic deserialization machinery — the harvest replay engine's stored-event
wire format (adjacently-tagged `{"type": ..., "data": ...}`, per CLAUDE.md's
explicit "never change this tagging" contract) plus third-party `serde_json`
internals, both already flagged out-of-scope by the prior
`performance-schema-validation-lazy-path.md` pass. `uuid::parser::try_parse`
(3.69%) is `uuid`-crate string parsing for the ~10,020 `ExecutionId`/
`ActivityExecId` values in each run — inherent to UUIDs-as-strings on the
wire, not a harvest-owned inefficiency. `is_contained_in` at exactly 5.00%
was the one item clearing the "≥5% of profile" gate with an identifiable,
harvest-owned mechanism.

Tracing the call graph (`is_contained_in`'s callee ID is only *named* the
first time it appears in the raw callgrind text output; every later call
site references it by bare numeric ID, so a plain `grep is_contained_in`
undercounts callers — confirmed by cross-referencing each guard function's
own `fn=` block against the numeric ID) found three independent callers, all
in `autumn_harvest::testing`:

```rust
fn offloaded_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    if !json.contains(crate::payload_store::OFFLOAD_ENVELOPE_KEY) {   // 1 scan
        return None;
    }
    // ... (only reached on a hit)
}

fn erased_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    if !json.contains(crate::erase::ERASURE_TOMBSTONE_KEY) {          // 1 scan
        return None;
    }
    // ...
}

fn codec_opaque_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    if !json.contains(CODEC_ENVELOPE_KEY) && !json.contains(UNDECODABLE_MARKER_KEY) {  // 2 scans
        return None;
    }
    // ...
}
```

These three are chained via `.or_else(...)` at the single fixture-guard
call site in `replay_fixture_file` (issue #524/#495/#608's "opaque payload"
family — refusing to certify a fixture whose payloads are offloaded,
erased, or codec-opaque rather than reporting a false replay divergence).
For a healthy fixture (the common case — no marker present), none of the
three's own `if` short-circuits to a hit, so `.or_else` never short-circuits
either: **all four** `.contains()` calls run on every fixture, each an
independent O(document length) scan of the *same* JSON text, to prove the
same kind of absence four times over. Confirmed directly in the call graph:
each of the three functions shows `calls=20` at this call site (one call per
fixture, none skipped), and `codec_opaque_fixture_reason`'s inclusive cost
(3,514,780 across its 20 calls) is roughly double
`offloaded_fixture_reason`'s (1,895,140) — matching its two-scan body exactly.

The four marker constants these functions look for:

```
ERASURE_TOMBSTONE_KEY    = "_harvest_erased"
OFFLOAD_ENVELOPE_KEY     = "_harvest_offload_envelope"
CODEC_ENVELOPE_KEY       = "_harvest_codec_envelope"
UNDECODABLE_MARKER_KEY   = "_harvest_undecodable"
```

all share the literal prefix `"_harvest_"`.

## Hypothesis

By the contrapositive, a single `json.contains("_harvest_")` check ahead of
all three functions is behavior-preserving: if the shared prefix is absent
from the document, none of the four specific markers (each of which
*contains* the prefix as a substring) can be present either, so every
specific-marker check would have missed anyway — just via three wasted
extra scans. A prefix hit still falls through to the same three checks in
the same order, so a fixture that genuinely carries a marker is detected
exactly as before.

Since `is_contained_in` was independently measured at 5.00% of the profile
and the change removes 3 of its 4 call sites, the targeted cost should drop
by roughly 75%, i.e. by close to 3.75% of the total. Combined with removing
some of the associated `Two-Way`-searcher setup overhead, the hypothesis was
that this comfortably clears the ≥5% floor.

## Change

`autumn-harvest/src/testing.rs`: introduced a private
`OPAQUE_PAYLOAD_MARKER_PREFIX: &str = "_harvest_"` constant and a combining
function

```rust
fn opaque_payload_fixture_reason(json: &str, snapshot: &HistorySnapshot) -> Option<String> {
    if !json.contains(OPAQUE_PAYLOAD_MARKER_PREFIX) {
        return None;
    }
    offloaded_fixture_reason(json, snapshot)
        .or_else(|| codec_opaque_fixture_reason(json, snapshot))
        .or_else(|| erased_fixture_reason(json, snapshot))
}
```

replacing the three separate `.or_else(...)` calls at the guard site with
one call to `opaque_payload_fixture_reason`. `unreplayable_fixture_reason`
(which does no text scanning — it only inspects two already-parsed
`Option` fields) was left in front, unchanged. A pure unit test
(`opaque_payload_marker_prefix_covers_every_marker_constant`) pinned the
invariant the fast path depends on, plus three behavioral tests covering the
fast-path-for-healthy-fixture case, the prefix-present-but-no-real-marker
case (proving the gate opening is not itself a false positive), and a
genuine erased-tombstone fixture routed through the new wrapper (proving the
fallthrough is wired correctly). All four passed; the full unit
(1,895/1,891+4) and integration (1,568) suites passed unchanged;
`cargo fmt --check` and `cargo clippy --lib --benches -- -D warnings` were
clean.

## Measurement

Both binaries built from the identical harness/`Cargo.toml` bench
declaration, differing only by the one-file `src/testing.rs` diff above,
same `valgrind --tool=callgrind --branch-sim=no --cache-sim=no` and
`valgrind --tool=dhat` invocations, same session, same
`VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500` workload.

| | Instructions (Ir) |
|---|---|
| Before | 130,387,162 |
| After  | 124,836,714 |
| **Reduction** | **5,550,448 (4.2569%)** |

**Below the ≥5% floor.** The targeted mechanism worked exactly as
predicted — `is_contained_in`'s own cost dropped from 6,515,660 (5.00% of
total) to 1,559,780 (1.25% of the smaller total), a 76.1% reduction of its
own prior cost, matching the "remove 3 of 4 scans" prediction almost
exactly — but at 4.26% of the *overall* workload, the change falls short of
the required threshold by a small but real margin.

| dhat | Before | After |
|---|---|---|
| Total blocks | 32,069 | 32,069 |
| Total bytes  | 10,563,947 | 10,563,947 |

**Identical**, as expected: `str::contains`/`Pattern::is_contained_in`
performs zero heap allocation (a pure scan over borrowed `&str` data), so
there is no independent allocation-count story that could otherwise clear
the ≥10%-allocation-reduction floor on its own.

Per the "revert if it doesn't clear the floor" rule, **the `src/testing.rs`
change was reverted.** `benches/verify_profile.rs` and its `Cargo.toml`
`[[bench]]` stanza are kept: the boundary they exercise had no prior
coverage, and they let a future attempt reproduce this exact measurement (or
try a materially different mechanism) without rebuilding the harness first.

### Why this can't be fixed by re-scaling the workload

Both the targeted cost (`.contains()`, O(document length) per call) and the
dominant cost (`serde_json` deserialization, also O(document length) times
the event count) scale with fixture size and fixture count in the same way,
so the ~4.26% ratio is not an artifact of the reduced 20-fixture/500-activity
scale chosen to keep a single callgrind run tractable — it holds at any
representative scale, including the full 1,000-fixture shape issue #251
documents. There is no free knob (more fixtures, larger fixtures, more
activities per fixture) that would shift this ratio in either direction.

### Why nothing else nearby was combined into this change

Every other line-item in the flat profile above 1% is either the frozen
adjacently-tagged event wire format, `serde_json`'s own internals (both
already ruled out by the prior schema-validation pass), or third-party
`uuid` string parsing with no identified harvest-owned inefficiency and no
independent hypothesis formed for it in this pass. Bundling an
under-evidenced change onto this one to clear the floor would violate the
"smallest behavior-preserving change" / "state a hypothesis with a specific
mechanism" rules on its own account, so none was attempted.

## Reproduce

```bash
BIN=$(cargo bench -p autumn-harvest --no-default-features --features testing \
  --bench verify_profile --no-run --message-format=json 2>/dev/null \
  | jq -r 'select(.reason=="compiler-artifact" and .target.name=="verify_profile") | .executable')

# Instruction count:
VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500 \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg.out "$BIN"
callgrind_annotate --threshold=98 cg.out | head -30

# Allocation counts/bytes:
VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500 \
  valgrind --tool=dhat --dhat-out-file=dhat.json "$BIN"
```

`VERIFY_PROFILE_FIXTURES` (default `20`) and `VERIFY_PROFILE_ACTIVITIES`
(default `500`) control the workload size; set `VERIFY_PROFILE_FIXTURES=1000`
to reproduce issue #251's exact documented shape given enough valgrind
wall-time headroom.
