<!-- issue-numbered per docs/changelog.d/README.md; fragment named for PR #1093 -->
## Trunk hardening — heal the batch-route gate-bypass test (Refs #1085, #1053, #1073)

**Test-code-only. No production change, no new `WorkflowEvent` variant, no
migration.** The Docker-only plugin test in
`autumn-harvest-plugin/tests/start_throttle_integration.rs` that #1073 added for
issue #1053 had a wrong runtime expectation. The plugin suite spins a
testcontainers Postgres, so CI runs it Docker-backed but #1073 could only
compile-check it — the assertion never actually executed. It is the
deterministic ubuntu **Test**-leg red blocking every open PR (surfaced in
#1085). Same class as the recent trunk heals #1076/#1077/#1078.

**Root cause — a wrong test EXPECTATION, not a code bug.** The test seeded a
`COMPLETED` prior for an explicit `workflow_id`, armed a fleet admission gate,
submitted a non-atomic batch item under the batch route's hardcoded
`AllowDuplicate` policy, and asserted `status != "rejected"`. But the batch
route reports an *attach-to-existing* (`start_or_load_workflow_execution_collect`
returning `created == false`) with `status: "rejected"` + the duplicate message
`"workflow_id '…' already has an existing execution"` + the prior's
`execution_id`. That mapping is a long-standing convention meaning "no NEW row
was inserted" — authored 2026-07-09 (PR #991), six days *before* #1073
(2026-07-15); #1073's only `api.rs` change is the single Phase-1 gate-bypass hunk
`@@ -12677,25 +12677,45 @@`. It is **not** an admission-gate block: a gate
rejection produces `error = "admission blocked by gate …"` with
`execution_id: null` in Phase 1 and never reaches the attach path. The active
uniqueness partial index `(workflow_name, workflow_id) WHERE state NOT IN
('CONTINUED_AS_NEW','TERMINATED')` does not release `COMPLETED`, so `_collect`'s
`on_conflict_do_nothing` insert is a no-op and it returns the existing run
(matrix-consistent — no rejection thrown by `_collect`), and
`start_will_create_new_execution(Some("COMPLETED"), AllowDuplicate) == false`, so
the item correctly **bypasses** the gate and attaches. The old assertion
conflated the benign duplicate-attach `"rejected"` with a gate block, and the
`#1073` code is correct. (This refutes an early hypothesis of a code bug in the
batch existence check.)

**The test's name over-promised.** It was named
`..._uses_start_will_create_predicate_not_bare_existence`, but verified against
the #1073 diff, the OLD `has_execution` boolean it replaced used the *same*
non-sealed-prior filter (`state NOT IN ('CONTINUED_AS_NEW','TERMINATED')`) the
new predicate query uses, and the batch route hardcodes `AllowDuplicate`
(`BatchStartItem` has no reuse-policy field). So the predicate and the old
bare-existence heuristic are **behaviorally identical for every prior state**
through this route (both attach for any present non-sealed prior, both create
for `None`/sealed, including `TERMINATED`). The predicate-vs-bare-existence
*implementation* distinction is therefore **not black-box observable** through
the batch route today — #1073 calls it exactly this: a structural/defensive
change that becomes observable only if a per-item reuse policy is ever added.
The genuine predicate distinction is unit-tested directly in
`autumn_harvest::execution::start_will_create_tests`.

**Fix — heal the red AND make the test genuinely meaningful.** Renamed to
`batch_gate_bypasses_an_attaching_item_but_blocks_a_would_be_fresh_create` and
rewritten to guard the property it actually *can* prove: the end-to-end bypass
DECISION OUTCOME, via a case that splits by will-create. Under one armed fleet
gate, in one non-atomic batch:

- a `COMPLETED` prior → the item **bypasses** the gate and **attaches** (error
  is the duplicate message, not a gate block; an `execution_id` is present);
- a `TERMINATED` prior (sealed → excluded from the uniqueness index →
  `_collect` would CREATE fresh) → the item does **not** bypass; it **faces the
  gate and is blocked** (`error = "admission blocked by gate …"`,
  `execution_id: null`).

The `TERMINATED`-prior gate block is the load-bearing assertion: it proves the
gate is enforced exactly for an item that would create a NEW admission — the
substance of #1053/#1073. (It does not, and cannot, distinguish the predicate
from bare-existence; both block a sealed prior. The doc comment states this
precisely.)

The rewritten test (and the full `start_throttle_integration` suite) was **run
green** against a local Postgres 16 via a temporary, uncommitted `setup_database`
adaptation (Docker is unavailable in this sandbox), which was then reverted so
only the test rewrite + this fragment remain.
