# `ReplayVerifier::verify_dir`: opaque-payload guard fast-path (shipped under maintainer override)

**Outcome: shipped despite falling short of the floor, by explicit maintainer
override.** A real, mechanism-backed hypothesis was formed, implemented, and
measured — the targeted cost dropped by ~76% exactly as predicted — but the
change's impact on the overall workload fell short of this agent's
autonomous ≥5% Ir-reduction gate in **every** measurement taken, across two
independent sessions: whole-process 4.26% (session 1) and 4.16% (session 2,
re-verification); isolated to `verify_dir`'s own cost, excluding
fixture-generation setup, 4.13% (session 1) and 4.33% (session 2) — see
"Isolating `verify_dir` from fixture-generation setup cost" and
"## Maintainer override" below for both sessions' full numbers. Under this
agent's own "revert if it doesn't clear the floor" rule the change **was in
fact reverted once**, mid-investigation, for exactly that reason (see the
"Measurement" section below, which is the unmodified historical record of
that first pass). It was subsequently **re-applied and kept** on the
maintainer's own explicit, direct instruction — "Maintainer call, ship it."
— overriding the floor for this one change. See "## Maintainer override"
below for the full record: who authorized it, why, and a from-scratch
re-verification pass run specifically to confirm the override was made with
accurate, current evidence rather than numbers a prior session could not
independently attest to. The benchmark harness that produced these numbers
(`benches/verify_profile.rs`) exercises a boundary (`ReplayVerifier`'s real
filesystem I/O + JSON *deserialize* of `HistorySnapshot`/`WorkflowEvent`) no
prior harness in this repo touched.

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
"Isolating `verify_dir` from fixture-generation setup cost" below), so the
reduced run is representative, not merely convenient.

**A note on build profile.** All measurements below are against a **release**
build (`cargo bench --no-run`, which uses the `bench` profile — inheriting
from `release` by default — or an explicit `cargo build --release`). A plain
`cargo build` (debug profile) compiles in extensive `core::ub_checks`
pointer/overlap-precondition instrumentation that dominates this workload's
instruction count (measured at ~47% of total in a debug build, versus being
entirely absent from a release build's profile) and is not representative of
what ships to production. Do not use a debug build to reproduce these
numbers.

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
wire format (adjacently-tagged `{"type": ..., "data": ...}`, per `docs/architecture.md`'s
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
change was reverted at this point in the investigation.**
`benches/verify_profile.rs` and its `Cargo.toml` `[[bench]]` stanza were kept
regardless: the boundary they exercise had no prior coverage, and they let a
future attempt reproduce this exact measurement (or try a materially
different mechanism) without rebuilding the harness first.

**This is not the final disposition of the `src/testing.rs` change.** See
"## Maintainer override" at the end of this document: the maintainer
subsequently instructed this specific change be re-applied and shipped
despite the shortfall documented above, explicitly overriding the
autonomous ≥5% floor for this one case. Everything in this "## Measurement"
section and the "Isolating..." subsection below it remains an accurate,
unmodified record of the investigation as it stood *before* that override —
it is preserved rather than rewritten so the reasoning that led to the
revert-then-override sequence stays auditable.

### Isolating `verify_dir` from fixture-generation setup cost

The first cut of this note computed the 4.26% ratio against the *whole
process's* instruction count, which also includes work `verify_dir` has
nothing to do with: building the tokio runtime, constructing and
JSON-serializing the fixture data, and `N` `std::fs::write` calls to lay it
out on disk before `verify_dir` ever runs. That setup cost is roughly
*fixed* per invocation (it does not scale with the fixture count the same
way `verify_dir`'s own cost does), so a reviewer correctly flagged that
computing the ratio against a total that includes it could understate
`verify_dir`'s true share of the profile — and, since 4.26% was already
close to the 5% floor, that this could be enough to flip the verdict.

To answer this with evidence rather than algebra, `benches/verify_profile.rs`
gained a two-phase mode: `VERIFY_PROFILE_MODE=prepare` writes the fixture
files to a directory and exits, run **unprofiled**; `VERIFY_PROFILE_MODE=run`
then does *only* `tokio::runtime::Builder::build()` (~60K instructions —
0.05% of the isolated total below, genuinely negligible) followed by
`verify_dir` on the pre-populated directory — no fixture generation, no
`std::fs::write` loop, inside the profiled region at all. This is the
harness's own recommended entry point for profiling; see the module doc
comment in `benches/verify_profile.rs` for the exact commands.

Re-measuring `VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500`
through this isolated `run`-only path, before and after the same
`src/testing.rs` diff described above:

| | Instructions (Ir), `verify_dir` only |
|---|---|
| Isolated before | 127,543,706 |
| Isolated after  | 122,281,078 |
| **Reduction** | **5,262,628 (4.1261%)** |

This is the properly-bracketed answer to the reviewer's question, and it
**confirms rather than overturns** the negative result: isolating away the
~2.84M instructions (2.2% of the naive total) of fixed setup cost moves the
ratio from 4.2569% to **4.1261%** — slightly *smaller*, not larger, because
the fixture-generation setup that got excluded from the denominator
contributed essentially nothing to the delta (the `src/testing.rs` diff
touches only code `verify_dir` calls, never the setup path), so removing it
from both numerator and denominator alike leaves the ratio to standard
before/after measurement noise rather than any systematic dilution effect.
Both the naive (whole-process) and the properly-isolated (`verify_dir`-only)
ratios land comfortably below the 5% floor — 5% of the isolated
`verify_dir`-only denominator is 6,377,185 Ir, and the measured reduction
(5,262,628) falls short of that by over 1,114,000 Ir, i.e. the change would
need to be roughly 21% *more* effective than what it actually achieves to
clear the floor even under this most-favorable denominator.

`callgrind_annotate` on the isolated trace shows the same flat-cost shape as
the original whole-process listing above (same functions, same relative
order, `is_contained_in` again the sole harvest-owned entry above the
5%-of-*its own, smaller* total line) — isolating setup cost changes the
total, not the composition of what remains.

Because both `.contains()` (the targeted cost, O(document length) per call)
and `serde_json` deserialization (the dominant cost, O(document length)
times the event count) scale with fixture size and fixture count the same
way, and setup cost is now demonstrated — not merely asserted — to be a
small, roughly-fixed fraction of the isolated total, this ratio is
representative at other fixture counts too, including the full 1,000-fixture
shape issue #251 documents: at larger `N`, `verify_dir`'s own cost grows
roughly linearly while the truly-fixed portion of setup (tokio runtime
construction) shrinks as a fraction of any total that still includes it, so
the isolated ratio above is, if anything, a slight underestimate of how
close a larger run would land to 4.1%, not an overestimate. There is no free
knob (more fixtures, larger fixtures, more activities per fixture) that
would shift this ratio in either direction by anything close to the ~0.9
percentage points needed to reach the floor.

#### A second review pass: reporting correctness inside the isolated region

A follow-up review of the two-phase harness itself (not of `src/testing.rs`)
found two more issues, both in `benches/verify_profile.rs`: (1) `run` mode's
printed workload-shape label was read from the *current process's own*
`VERIFY_PROFILE_ACTIVITIES` env var rather than from what `prepare` actually
wrote to disk, so a `prepare`/`run` env-var mismatch (a real risk — the two
are separate process invocations, plausibly separate shell commands) would
silently mislabel the measured workload; and (2) an unrecognized
`VERIFY_PROFILE_MODE` value (a typo like `ru` instead of `run`) silently fell
back to `full` mode, which writes fixtures **inside** the profiled process —
exactly the setup-cost pollution the two-phase mode exists to exclude, with
no indication anything had gone wrong.

Fix (2) is a straightforward `match` tightened to an explicit `"full"` arm
plus a `panic!` on anything else — no measurement impact.

Fix (1) needed more care, because the naive fix (parse a fixture already on
disk to recover the true per-fixture activity count) would itself run
**inside** the `run`-mode profiled region — reintroducing a smaller version
of the exact problem this whole section exists to solve. Measured directly:
parsing one full ~124 KB / 1,001-event fixture to recover its activity count
costs **3,916,883 Ir (~3.07%** of the isolated total) — non-negligible, and
large enough on its own to materially shift the reported ratio. The fix that
shipped instead has `prepare` mode persist a tiny `key=value` sidecar file
(`verify_profile_meta.txt`, deliberately not `.json` — `verify_dir`'s
directory walk globs every `*.json` file as a fixture to replay) alongside
the fixtures, which `run` mode reads in preference to parsing a full
fixture. Measured cost of the sidecar read: **93,637 Ir (~0.07%**
of the isolated total) — genuinely negligible, comparable to the
already-documented ~60K-instruction tokio-runtime-build cost. The
full-fixture-parse function is kept as a fallback for a directory populated
without going through `prepare_fixtures` at all (an edge case, not the
blessed two-phase workflow), where paying that cost is the correct
trade-off for correctness over an unmeasured directory.

The isolated before/after pair above (127,543,706 / 122,281,078,
**4.1261%**) is measured against the harness with *both* of these fixes
applied — it is what `benches/verify_profile.rs` as committed actually
produces, not a superseded intermediate state. The ~93,637 Ir sidecar-read
cost appears in **both** the before and after measurement identically (it
runs after `verify_dir` returns, independent of which `src/testing.rs` is
linked in), so it shifts the total slightly but leaves the *reduction* and
the conclusion unchanged from the first isolated pass (4.1957% → 4.1261%,
both comfortably below the 5% floor).

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

# Whole-process instruction count (matches the "Profile"/"Measurement" numbers above):
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

**Reproducing the isolated `verify_dir`-only numbers** (the
"Isolating `verify_dir` from fixture-generation setup cost" section above)
requires the two-phase mode instead, so fixture generation runs unprofiled:

```bash
export VERIFY_PROFILE_DIR=/tmp/verify-fixtures
export VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500
mkdir -p "$VERIFY_PROFILE_DIR"

VERIFY_PROFILE_MODE=prepare "$BIN"          # unprofiled setup

VERIFY_PROFILE_MODE=run \
  valgrind --tool=callgrind --branch-sim=no --cache-sim=no --callgrind-out-file=cg_run.out "$BIN"
callgrind_annotate --threshold=90 cg_run.out | head -30
```

**A build-profile reminder** (see the note under "Workload" above): both of
these must use the release-profile binary `cargo bench --no-run` resolves
(or an explicit `cargo build --release ... --bench verify_profile`). A plain
`cargo build` produces a debug binary whose instruction count is dominated
by `core::ub_checks` safety instrumentation (~15x higher total, and a
completely different cost distribution) and is not a valid substitute.

## Maintainer override

Everything above this section is the unmodified record of the original
investigation, which — correctly, per this agent's own pre-committed rules —
concluded in a revert. This section records what happened next: a human
maintainer reviewed that conclusion directly and chose, explicitly and in
writing, to override it for this one change.

### What was overridden, and by whom

This agent's operating mandate treats a deterministic instruction-count
measurement as admissible evidence and sets a hard, pre-committed floor:
ship a performance change only if it clears **≥5% Ir reduction on a workload
that is itself ≥5% of realistic cost**, OR ≥10% allocation reduction, OR a
measurable syscall reduction, OR an asymptotic improvement — and *revert* if
it doesn't. That rule exists specifically so an autonomous agent does not
accumulate a long tail of small, unverified, or marginal "optimizations"
that individually might be fine but collectively erode the signal-to-noise
ratio of what "this PR improved performance" means. The measurement above —
a real mechanism, working exactly as predicted, at 4.1–4.3% depending on how
it's bracketed — is precisely the kind of result that rule exists to catch:
close to the floor, genuinely positive, but short of it.

The maintainer was shown the exact numbers above (the 4.2569%/4.1261%
whole-process/isolated split, and the reasoning for holding the floor) and
responded first with direct pushback — *"I mean, I would call a -4.26% a
win"* — which this agent did not treat as authorization on its own (a
human's informal reaction to a number is not the same as an instruction to
act, and the whole point of a pre-committed floor is that it does not bend
under in-the-moment renegotiation). This agent instead named exactly what an
explicit override would require: re-applying the diff, documenting the
override with its rationale, and re-measuring rather than trusting stale
numbers. The maintainer then gave that explicit instruction directly:

> **"Maintainer call, ship it."**

That is the authorization on record for keeping this change despite the
measured shortfall. It is scoped to this one change; it does not relax the
≥5%/≥10%/syscall/asymptotic floor as a general rule for any other change in
this repository, autonomous or otherwise. A future below-floor optimization
in this codebase still needs either a materially different (and sufficient)
measurement, or a maintainer willing to grant the same kind of explicit,
on-the-record override this one received.

### Why the diff had to be re-derived, not simply un-reverted

By the time the override instruction arrived, the functional
`src/testing.rs` diff described in "## Change" above existed only as a
prose description in this document and in the revert commit's message — it
had never itself been committed to git history (only the harness and this
document were committed; the revert happened in the same working session,
before any commit of the functional change). Restoring it therefore meant
precisely reconstructing the diff from that description against the
*current* state of `src/testing.rs`, not applying a stored patch or trusting
memory of what the code looked like. The reconstructed diff was verified,
line by line, against every detail in "## Change" above — the constant name
and value, the combining function's exact body and delegation order, and
the shape of all four new unit tests — before being treated as a faithful
restoration rather than a fresh reinvention.

### Re-verification: fresh, independent numbers, gathered specifically for this decision

Reusing the original session's numbers for a decision this consequential
would have meant trusting measurements this agent cannot itself re-derive
provenance for. Instead, both binaries were rebuilt from scratch in this
session — `git stash` isolated a clean "before" `src/testing.rs` (the
committed, un-optimized `HEAD`), a "before" binary was built and measured,
then the reconstructed diff was restored and an "after" binary was built and
measured — same machine, same session, same `Cargo.lock`, same
`VERIFY_PROFILE_FIXTURES=20 VERIFY_PROFILE_ACTIVITIES=500` workload, same
`valgrind --tool=callgrind --branch-sim=no --cache-sim=no` /
`valgrind --tool=dhat` invocations as the original pass. The "before" binary
was confirmed (via `strings`) to lack the `opaque_payload_fixture_reason`
symbol entirely and the "after" binary to contain it, before either was
profiled.

**Whole-process (`VERIFY_PROFILE_MODE=full`):**

| | Instructions (Ir) |
|---|---|
| Before | 130,242,246 |
| After  | 124,824,457 |
| **Reduction** | **5,417,789 (4.1598%)** |

**Isolated to `verify_dir` only (two-phase `prepare`/`run`, excluding
fixture-generation setup cost — see "Isolating `verify_dir` from
fixture-generation setup cost" above for why this is the more rigorous of
the two brackets):**

| | Instructions (Ir), `verify_dir` only |
|---|---|
| Isolated before | 127,510,865 |
| Isolated after  | 121,994,985 |
| **Reduction** | **5,515,880 (4.3258%)** |

Both fresh numbers land in the same range as the original session's
(4.2569% / 4.1261%) — a few tenths of a percentage point apart in either
direction, consistent with ordinary session-to-session measurement noise:
the two sessions were only about seven hours apart (original commit
`f663a316` at 2026-08-16 05:52 UTC; this re-verification the same day), so
the small drift is not attributable to dependency version movement over
time — more likely ordinary nondeterminism in exactly which code path a
generic `str::contains`/`Pattern::is_contained_in` call resolves to at
compile time from one `cargo build` invocation to the next (valgrind's
counts are fully deterministic *given* an identical binary, but two
separately-compiled binaries of the same source are not guaranteed to be
byte-identical). **Both fresh numbers still fall short of the ≥5% floor.** The targeted mechanism was
re-confirmed working exactly as designed on this fresh trace too:
`is_contained_in`'s own cost dropped from 6,510,380 (5.00% of the
whole-process total) to 1,559,780 (1.25%) — a 76.0% reduction of its own
prior cost — and from 6,515,040 (5.11% of the isolated total) to 1,559,780
(1.28%) in the isolated trace, a 76.1% reduction; both numbers match the
mechanism's prediction from the original hypothesis almost exactly, and the
post-change `is_contained_in` cost (1,559,780) is bit-for-bit identical
across the whole-process and isolated traces *and* identical to the original
session's number for the same quantity — the one figure in this whole
re-verification pass with no session-to-session drift at all, as expected
for a pure function of fixture content and call count.

**Allocation counts (`valgrind --tool=dhat`), whole-process, before/after:**

| dhat | Before | After |
|---|---|---|
| Total blocks | 32,118 | 32,118 |
| Total bytes  | 10,599,891 | 10,599,891 |

Identical, confirming — a second time, independently — that this change has
no allocation-count story of its own (`str::contains` performs no heap
allocation) and clears no floor via that path either.

### Verification gates run on the re-applied change

Before shipping, the re-applied change was independently re-verified against
every gate this agent's mandate requires for any change, not just the
performance floor:

- `cargo fmt -p autumn-harvest -- --check` — clean.
- `cargo clippy -p autumn-harvest --no-default-features --features testing -- -D warnings` — clean.
- `cargo clippy -p autumn-harvest --all-features -- -D warnings` — clean.
- `cargo test -p autumn-harvest --no-default-features --features testing --lib` — **1,895 passed, 0 failed**
  (1,891 pre-existing + the 4 new tests documented in "## Change" above,
  re-derived alongside the function they cover).

### The honest summary

This change is being shipped and kept in the repository **despite** not
clearing its own pre-committed performance-impact floor, on two independent
fresh measurements taken specifically to inform this decision, because a
human maintainer reviewed the evidence directly and explicitly instructed it
to ship anyway. That is a legitimate exception process — the floor exists to
constrain *autonomous* decisions, not to bind a maintainer who has actually
looked at the numbers — but it is exactly that: an exception, made once, on
the record, for this one change, not a precedent that lowers the bar for
anything measured after it.
