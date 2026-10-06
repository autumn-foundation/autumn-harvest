## Phase — shared deferred-admission decode for debounce/throttle/batch fire (🪞 Echo clone-class merge)

`debounce::fire_claimed_debounce_row`, `throttle::fire_claimed_throttle_row`,
and `event_batch.rs`'s two flush/fire sites each decoded the same seven
`DebounceStartOptions` fields — `reuse_policy`, `execution_timeout`, `sla`,
`max_execution_timeout_ceiling`, `chain_execution_timeout`,
`max_workflow_chain_timeout_ceiling`, `priority` — into locals, in a
byte-identical block. All three files deserialize into the literal same
type, `debounce::DebounceStartOptions`, rather than a type of their own.
Two separate feature commits, `896978eb` (issue #617) and `3fa812d2` (issue
#740), each added fields to all three files in the same commit, confirming
this was always one hand-maintained decision.

**What shipped.** Added `debounce::decode_deferred_admission_fields(&DebounceStartOptions)
-> DeferredAdmissionFields`, a pure function with no database dependency.
All four call sites now destructure its result instead of inlining the
block. `start_source`'s pre-#740-row fallback differs by carrier (a flat
default for debounce/batch vs. an `opts.origin`-derived one for throttle),
so it stays decoded separately at each call site — a real behavioral
difference, not an idiom to fold in.

**Left alone, checked not missed:** the `StartWorkflowParams` struct-literal
construction immediately after the decoded block, which also differs per
caller (`throttle.rs` sets `schedule_id`/`scheduled_for`/`origin`, which
`debounce.rs` never does).

**Removed as a forced consequence:** `event_batch.rs`'s own private copy of
`parse_reuse_policy` — a byte-for-byte duplicate of
`debounce::parse_reuse_policy` it called instead of importing the shared
one `throttle.rs` already imports — became dead code once its only call
sites were replaced, and is deleted in the same commit.

**Evidence.** `jscpd` (`--min-tokens 30 --min-lines 5 --format rust`, scoped
to `debounce.rs`/`throttle.rs`/`event_batch.rs`): 34 cross-file pairwise
matches / 2037 duplicated tokens between these three files before this
change, 0 after. Rule of three clears on instance count alone (4), so no
missed-fix defect is required as evidence.

No public API change beyond the new `pub(crate)` `DeferredAdmissionFields`/
`decode_deferred_admission_fields`. `cargo fmt --check`, `cargo check --lib
--features db`, and the full non-DB unit suite (3830 tests) are clean.
Added four characterization unit tests for `decode_deferred_admission_fields`
(all-fields-absent defaults, every field present, unparseable `reuse_policy`
string, unparseable `priority` int); all pass. No live Postgres was
available in this sandbox to re-run the `debounce_tests.rs`/
`throttle_tests.rs`/`event_batch_tests.rs` integration suites, but this is a
pure data-in/data-out extraction with no branch on caller identity, so no
new behavior exists for them to catch.
