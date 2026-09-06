# `schedule_to_close_at` claim predicate: measured, confirmed cheap; the measured buffer/storage cost is index maintenance and row width

`docs/performance.md`'s "Known limitations" section flagged
`schedule_to_close_at` (issue #378), alongside worker sessions (#606) and
sticky routing (#235), as "cheap inline column tests, against columns the
seed leaves null" -- present in `queue::claim_task_query()` on every claim,
but never measured because `claim_bench_support::db::seed_backlog` never
populates the column. This page is that measurement, for `schedule_to_close_at`
only.

The result **confirms the doc's own suspicion on magnitude**: populating
`schedule_to_close_at` adds a small, real buffer cost to the claim query --
**+7.5% at 1,000 rows, +3.6% at 10,000** (100,000 does not get a clean
percentage in the committed run -- see [100,000-row plan
choice](#100000-row-plan-choice) for why) -- corroborated by two standalone
MVCC-bloat scripts. None of this comes close to the 20% impact floor; no
fix is proposed or needed.

**A late-round methodology fix changed several of this page's headline
numbers substantially, including the real-drain aggregate below (from
+15.5% to +1.9% for `claim_task_query()` alone).** Every capture on this
page seeds two data states side by side and compares them, so any
difference *other* than `schedule_to_close_at` between how the two states
were seeded is itself a source of measurement error. Through several
review rounds, the `schedule-to-close` state was re-seeded with its own
fresh, independently-random `id`/`activity_id` UUIDs rather than reusing
the `no-schedule-to-close` state's exact values -- and since every claim is
a non-HOT `UPDATE` that touches *every* index on `harvest_task_queue`, not
just `harvest_task_queue_schedule_to_close_idx` (see [Plan](#plan)),
independently-random keys in those OTHER indexes' B-trees could add
page-split/traversal noise of a similar size to the effect this page
attributes to `schedule_to_close_at`. Codex review on PR #1339 caught this,
and caught two further bugs in the first two attempts to fix it (wrong
snapshot ordering, then a snapshot that didn't survive across the
real-drain loop's per-label connections) -- see
[Workload](#workload) for the full sequence. The 1,000-/10,000-row
`EXPLAIN` deltas above reproduced identically before and after this fix;
the real-drain aggregate and the 100,000-row plan choice did not -- see
[Corroboration](#corroboration-pg_stat_statements-over-the-real-claim-drain)
and [100,000-row plan choice](#100000-row-plan-choice).

**The mechanism is not what an earlier revision of this page claimed,
though.** That revision attributed the whole delta to row width, by analogy
with `docs/performance-capability-labels.md`'s `required_capabilities`
finding, without actually reading the plan closely enough to check --
`claim_task_query()`'s candidate-side `WHERE` clause genuinely is a plain
inline column test, but the query is not read-only: its `claimed` CTE
`UPDATE`s the claimed row, and `harvest_task_queue` carries a **partial
index** on `schedule_to_close_at` (`harvest_task_queue_schedule_to_close_idx`,
migration `20260606000001`, built for the timeout scanner) that only rows
with a non-`NULL` deadline are ever members of. Codex review on PR #1339
caught this, and caught two further problems with how the finding was first
measured -- see [Plan](#plan) and [Write-side cost](#write-side-cost) for
the corrected evidence. The measured buffer delta is the **sum of two
genuinely different mechanisms**: a small, near-constant, per-claim
index-maintenance cost, plus a row-width effect on the candidate scan that
scales with backlog depth -- not row width alone.

**This page measures buffer accesses and storage growth, not CPU time --
and its findings are scoped accordingly.** Every table on this page is
buffer-based (`EXPLAIN ... BUFFERS`, `pg_stat_statements`'s block
counters) or storage-based (`pg_relation_size`). Evaluating
`schedule_to_close_at > NOW()` for every candidate row the scan visits
consumes CPU without necessarily touching an additional buffer, so nothing
here rules out a CPU-bound cost from the predicate's own evaluation, and
this page does not claim to have measured one. Per this repo's evidence
rules, wall-clock/execution time is admissible only when it clears 2x and
is corroborated by a buffer or row-count change in the same direction --
this pass did not collect `total_exec_time` or any other CPU-time metric,
so there is no such corroboration to report either way. A single
`timestamptz` comparison is among the cheapest operations a CPU can do,
which is why the row-level buffer evidence below is treated as the
practically decisive measurement -- but "index maintenance and row width"
describes the *measured buffer/storage* cost specifically, not a claim that
predicate evaluation costs exactly zero.

**One thing this page found alongside is not fully resolved and is reported
as such:** the real-drain `pg_stat_statements` aggregate and the
`pg_stat_user_tables` heap-growth/dead-tuple snapshots varied more between
runs than the `EXPLAIN` numbers did. See
[Corroboration](#corroboration-pg_stat_statements-over-the-real-claim-drain)
and [Write-side cost](#write-side-cost). This page does not have a reliable
pinned percentage for either measurement and says so rather than reporting
whichever run's number looked cleanest.

**On reproducibility and what's actually committed.** This capture was run
several times over the course of this pass as review kept finding real
problems with the harness -- see [Workload](#workload) for the full list of
fixes. Codex review on PR #1339 additionally pointed out that an earlier
revision asserted specific multi-run statistics (e.g. "2 of 3 runs show
plan X") without committing per-run artifacts to back them, so a reader
could not audit those claims from the repository -- only the most recent
run's output ever survives, because the repro script always writes to the
same canonical filenames. This revision **fixes that by narrowing scope**:
the committed artifacts back every number in this page. Where earlier,
now-uncommitted runs are mentioned, they are described as historical
context from this development session -- illustrating that variance
exists, not as independently auditable data points -- and no conclusion on
this page depends on a specific count of how many times something happened
across runs that are no longer reproducible from the repo.

## Workload

`claim_task_query()`'s candidate CTE gates every row with:

```sql
AND (
    schedule_to_close_at IS NULL
    OR schedule_to_close_at > NOW()
)
```

a plain inline test against a column already on the `harvest_task_queue` row
the scan has fetched -- no subquery, no join, no correlated cost *in this
predicate's own evaluation*. That is not the same claim as "populating this
column is free": `claim_task_query()`'s `claimed` CTE `UPDATE`s the row it
selects, and a partial index on this column exists for the timeout
scanner's benefit -- see [Plan](#plan) for the real, measured mechanism,
which an earlier revision of this page got wrong by assuming the predicate
text was the whole story. In production this column is set once at initial
enqueue (`NOW() + schedule_to_close`, issue #378) when a caller declares a
total-attempt deadline, and left `NULL` (unbounded) otherwise.

`autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_schedule_to_close_claim_evidence`
mirrors `zz_capture_capability_labels_claim_evidence` exactly in shape:
`queue::claim_task_query()` is unmodified end to end (there is no query-shape
fix to try for a plain column test), and every EXPLAIN /
`pg_stat_statements` pair is captured from the exact same query text at two
seeded states of `harvest_task_queue.schedule_to_close_at`:

- **`no-schedule-to-close`** -- every row's column is `NULL` (today's
  default, and what every other claim-path benchmark in this crate already
  measures).
- **`schedule-to-close`** -- every row seeded with `schedule_to_close_at =
  NOW() + INTERVAL '100 years' + (i::text || ' seconds')::interval` at
  `INSERT` time, where `i` is each row's `generate_series` index. Landing on
  this exact expression took four iterations, three of them caught by
  review rather than shipped silently:
  - `NOW() + INTERVAL '1 hour'` (the original value): Codex review
    correctly flagged this as unsafe, since the drain has no overall
    wall-clock bound and already took ~15-30 minutes end to end in this
    pass's own environment -- a slower machine or remote database could
    plausibly exceed an hour and start excluding later rows mid-drain.
  - `'infinity'::timestamptz` (fix attempt 1): a valid Postgres value that
    compares later than every finite timestamp, so it fixes the wall-clock
    problem above -- but it broke the real claim path outright.
    `queue::claim_task()`'s `claimed` CTE `RETURNING`s the full claimed row,
    including `schedule_to_close_at`, for Diesel to deserialize into a
    `chrono::DateTime<Utc>`; Chrono has no `infinity` sentinel, so every
    claim in the `schedule-to-close` drain panicked with "Tried to
    deserialize a timestamp that is too large for Chrono" the moment this
    was actually run.
  - `NOW() + INTERVAL '100 years'` alone, i.e. a single constant value
    shared by every row (fix attempt 2): comfortably inside
    `chrono::DateTime<Utc>`'s representable range (roughly to the year
    262,000) and far longer than any realistic drain duration, so it fixed
    both problems above -- but Codex review caught a third problem this
    introduced. `NOW()` is stable for the duration of one SQL statement, so
    every row in a single `INSERT ... SELECT` receives the byte-identical
    timestamp, unlike production (where `NOW() + schedule_to_close` is
    computed once per enqueue, at different enqueue times with different
    durations, so real deadlines are effectively distinct per row).
    Postgres's B-tree deduplication (PG13+) compresses repeated keys into
    posting lists far more efficiently than genuinely distinct keys, so a
    constant seeded value understated the partial index's real page growth
    by roughly 3x -- confirmed directly: rerunning the standalone
    corroboration scripts (see [Write-side cost](#write-side-cost)) with a
    constant value measured 10→19 index pages; with the `i`-varied
    expression below, the same 10,000-row fixture measured 30→57.
  - The final expression adds `(i::text || ' seconds')::interval`, spreading
    seeded deadlines across up to ~28 hours (100,000 seconds, covering the
    largest `BACKLOG_SWEEP` depth) while the 100-year base keeps every value
    far enough in the future to satisfy the wall-clock fix regardless of the
    spread.

  Like the capability-labels capture's matching `Exact` requirement, the
  seeded deadline excludes nothing at any point in its range: this isolates
  the predicate's *evaluation* cost from any change in which rows are
  eligible, and lets the drain loop's claimed-row count serve as a
  correctness check between labels.

Both states are captured for `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS,
TIMING OFF)` on `claim_task_query()` at each of the published `BACKLOG_SWEEP`
depths (1,000 / 10,000 / 100,000), and for a full `pg_stat_statements` drain
of the real async `queue::claim_task()` function (not literal-substituted
SQL) against the headline scenario's 10,000-row/4-queue backlog shape,
claiming every row one call at a time through a **single connection,
serially** -- not the headline scenario's 8 concurrent claimers
(`headline_scenario().claimers`, unused by this drain). Codex review on PR
#1339 (P2) correctly flagged an earlier revision's "against the ...
headline scenario" phrasing as overclaiming a match this drain doesn't
attempt: concurrent claimers could contend on the same
`harvest_task_queue_schedule_to_close_idx` leaf pages this predicate's
extra write touches, in a way a serial drain never exercises. This
capture's serial-drain shape matches the established convention the
sibling `zz_capture_capability_labels_claim_evidence` and
`zz_capture_concurrency_key_claim_evidence` captures already use for their
own `pg_stat_statements` snapshots -- reusing it here for consistency
rather than inventing a new shape -- but it means this page's aggregate
number is a serial per-claim cost accumulated over a full drain, not a
concurrent-load measurement, and should be read that way.

The drain is also paired with a `pg_stat_user_tables`/`pg_relation_size`
snapshot of `harvest_task_queue` immediately before and immediately after
that same drain, to see whether any aggregate delta is driven by MVCC bloat
accumulating over the drain itself rather than by row width alone (a
single-call EXPLAIN, seeded fresh and rolled back inside a transaction, can
never observe that: it never accumulates dead tuples). The drain also
`ANALYZE`s `harvest_workers` before either label's stat-snapshot run, since
`claim_task_query()`'s `worker_info` CTE reads that table on every claim and
stale statistics there could otherwise make the two labels' drains pick
different, unrelated access paths for that lookup -- independently of
`schedule_to_close_at` -- contaminating the very comparison this capture
exists to make. This was missing in the first several runs of this capture
and was added in response to Codex review.

**Both labels seed `id`/`activity_id` from the exact same set of values,
not independently.** Every claim is a non-HOT `UPDATE` (see [Plan](#plan))
that touches every index on `harvest_task_queue`, including the primary
key and any index on `activity_id` -- not just
`harvest_task_queue_schedule_to_close_idx`. An earlier revision of this
capture let `db::seed()`'s own `gen_random_uuid()` calls seed each label
independently, so the two labels' primary-key and `activity_id` B-trees
held genuinely different random keys -- page-split/traversal noise from
those OTHER indexes could be comparable in size to the effect this page
attributes to `schedule_to_close_at`. Codex review on PR #1339 caught
this, and caught two further problems in fixing it:

1. **Reuse the values, correctly ordered.** The first fix snapshotted the
   `no-schedule-to-close` seed's `id`/`activity_id` values and reused them
   for the `schedule-to-close` re-seed, but numbered the snapshot by
   `ROW_NUMBER() OVER (ORDER BY id)` -- sorting by the random primary key
   rather than original insertion order -- so the re-insert built its heap
   and every index in a different physical order than `db::seed()`'s
   `generate_series`-ordered bulk `INSERT` did for the baseline, which
   could reintroduce the same kind of noise. Fixed by numbering with
   `ROW_NUMBER() OVER (ORDER BY ctid)` instead (`ctid`, physical tuple
   location, preserves insertion order for a table that has only ever been
   bulk-loaded once) and explicitly re-inserting in that same order.
2. **Share the snapshot across connections, not just within one.** The
   per-depth `EXPLAIN` loop seeds once per depth and shares that seed
   between both labels on one connection (the `no-schedule-to-close`
   label's `EXPLAIN ANALYZE` runs inside a rolled-back transaction, so its
   seeded rows survive for the `schedule-to-close` re-seed that follows) --
   a `TEMP TABLE` snapshot works fine there. But the real-drain loop opens
   a **fresh** connection per label and re-seeds independently on each, so
   a `TEMP TABLE` (connection-session-scoped) snapshotted and reused only
   the `schedule-to-close` label's own already-independently-random seed
   -- a no-op for cross-label matching. Fixed by using a plain table,
   snapshotted during the `no-schedule-to-close` label's iteration and
   consumed during the `schedule-to-close` label's, on its own separate
   connection.

Both fixes are in `claim_budget_tests.rs`'s
`snapshot_seed_for_schedule_to_close`/`reseed_from_schedule_to_close_snapshot`
helpers; see their doc comments for the full detail. **The impact was
large, not cosmetic**: the real-drain aggregate dropped from +15.5% to
+1.9% for `claim_task_query()` alone once both labels shared identical
indexed values (see
[Corroboration](#corroboration-pg_stat_statements-over-the-real-claim-drain)),
and the 100,000-row `EXPLAIN` comparison, which happened to land on the
same (`Seq Scan`) plan for both labels in the pre-fix committed run, landed
on *different* plans for the two labels once re-run with this fix (see
[100,000-row plan choice](#100000-row-plan-choice)) -- direct evidence that
at least part of what this page previously reported as the
`schedule_to_close_at` effect was really this seeding confound. The
1,000-/10,000-row `EXPLAIN` deltas, by contrast, reproduced byte-identical
before and after this fix.

## Plan

`harvest_task_queue` carries a partial index built for the timeout scanner,
not touched by `claim_task_query()`'s `SELECT`-side logic at all:

```sql
CREATE INDEX harvest_task_queue_schedule_to_close_idx
    ON harvest_task_queue (schedule_to_close_at)
    WHERE schedule_to_close_at IS NOT NULL
      AND state IN ('RUNNING', 'PENDING');
```
(migration `20260606000001_harvest_activity_schedule_to_close`)

A `no-schedule-to-close` row (`schedule_to_close_at IS NULL`) never
satisfies this predicate and is never a member of this index, before or
after a claim. A `schedule-to-close` row satisfies it both before
(`state = 'PENDING'`) and after (`state = 'RUNNING'`) the claim `UPDATE` --
its logical index membership doesn't change.

The claim `UPDATE` changes `state`, and `state` is a key column of
`idx_harvest_tq_poll` and `idx_harvest_tq_running` (and appears in
`idx_harvest_tq_activity_pause`'s key too) -- so this `UPDATE` cannot use
Postgres's HOT (Heap-Only Tuple) optimization, for *every* claim, on *both*
labels, regardless of `schedule_to_close_at`. HOT eligibility is an
all-or-nothing property of the update: once any indexed column changes,
Postgres cannot skip index maintenance selectively for the indexes that
column doesn't belong to -- the new physical tuple needs a fresh entry in
*every* index on `harvest_task_queue`, not just the ones keyed on `state`.
`harvest_task_queue` carries well over a dozen indexes (the primary key,
`idx_harvest_tq_workflow`, `idx_harvest_tq_activity_id`, and others besides
`idx_harvest_tq_poll`/`idx_harvest_tq_running`), and both
`no-schedule-to-close` and `schedule-to-close` rows pay full non-HOT
maintenance across all of them on every claim -- an earlier revision of
this page incorrectly described the baseline cost as touching only
`idx_harvest_tq_poll` and `idx_harvest_tq_running` (Codex review, PR
#1339). The one-sentence version that *is* accurate:
`harvest_task_queue_schedule_to_close_idx` is the **one index in that
already-large set that only `schedule-to-close` rows are ever members
of** -- every other index gets a new entry on every claim for both
labels equally, so it cancels out of the comparison; this one does not,
which is why it is the source of the measured delta.

The `Update on public.harvest_task_queue` node's own `Buffers` line shows
this directly, and it is depth-independent -- the signature of a per-claim
index write, not a scan-side effect (artifacts: the `{no-schedule-to-close,
schedule-to-close}-claim-backlog-{depth}.explain.txt` files, the committed
run):

| backlog | no-schedule-to-close `dirtied`/`written` | schedule-to-close `dirtied`/`written` |
|---:|---:|---:|
| 1,000 | 4 / 2 | 5 / 3 |
| 10,000 | 4 / 2 | 5 / 3 |
| 100,000 | 4 / 2 | 5 / 3 |

**Exactly +1 dirtied, +1 written, every time, at every depth.** If this
delta were a row-width effect on the `UPDATE` node's own heap write, it
would not need to be constant -- a wider row still fits in the same 8KB
page here (page-crossing effects from row width show up on the *scan* side
below, where row count per page is what changes, not on a single-row
`UPDATE`'s own write). A fixed one-page write matches a B-tree leaf-page
insert into `harvest_task_queue_schedule_to_close_idx`.

**Separating the scan-side (row-width) contribution from the update-side
(index-write) contribution requires reading the child node's own buffers,
not the parent `Update` node's cumulative total.** The `Update` node's
direct child (the `Nested Loop` that selects the candidate row) reports its
own, separate cumulative `Buffers: shared hit=` line:

| backlog | child (scan-side) hit: no-stc / stc | scan-side delta | `Update`-exclusive delta (total − child) |
|---:|---:|---:|---:|
| 1,000 | 37 / 37 | **0** | +4 |
| 10,000 | 256 / 262 | +6 | +4 |
| 100,000 | 9,858 / 2,513 | not meaningful -- see below | +4 |

The `Update`-exclusive column (the index write plus whatever else the
`Update` node itself touches, beyond its child) is **exactly constant
across all three depths (+4 every time), including 100,000** --
consistent with the fixed one-page index write the `dirtied`/`written`
evidence above already established (a B-tree insert typically touches a
root and/or a leaf page as `hit`s in addition to the one page it dirties,
so a handful of total `hit` buffers for one insert is unsurprising). This
column held constant across the seeding fix in [Workload](#workload) too
(it read +3/+4/+4 under an earlier, degenerate-seed run; +4/+4/+4 with a
genuinely distinct key per row; and +4/+4/+4 again, unaffected, once both
labels' *other* indexes stopped varying independently) -- three
independent pieces of evidence for the same fixed per-claim index-write
signature, none of which moved when the seeding methodology changed
underneath them.

**The 100,000-row scan-side delta is not a row-width measurement in this
committed run, and this page does not report it as one.** At 1,000 and
10,000 rows both labels use the identical plan shape (a `Nested Loop`
feeding the same downstream `Sort`), so subtracting their child-node hits
isolates a row-width effect cleanly: zero at 1,000 rows, +6 at 10,000,
consistent with a genuine row-width effect that only becomes visible once
the table is large enough for the extra bytes per row to push the page
count itself higher (at 1,000 rows both states may simply pack into the
same number of heap pages by coincidence of alignment and fill factor).
This is consistent with the *general* row-width mechanism
`docs/performance-capability-labels.md`'s `required_capabilities` finding
describes for its own (larger) JSONB column, without claiming the same
smooth, always-positive scaling that page found for its wider effect. At
100,000 rows, though, the committed run's two labels land on **different
plans** for the candidate scan (see [100,000-row plan
choice](#100000-row-plan-choice)): `no-schedule-to-close` uses an `Index
Scan using idx_harvest_tq_poll` (9,858 child-node hits), `schedule-to-close`
a plain `Seq Scan` (2,513 hits, identical to the previously committed run).
Subtracting those two would compare the cost of two different scan
strategies, not the row-width cost of one strategy under two data states
-- so this page reports both raw numbers and explicitly declines to derive
a "scan-side delta" from them.

Neither component is a defect. The index exists because the timeout
scanner needs it (its own migration comment says so, and no alternative
was evaluated by this pass, which measures rather than redesigns); the
scan-side row-width cost is inherent to storing a wider column at all. No
schema or query change is proposed.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` **shared-buffer hits** for
`claim_task_query()`, `no-schedule-to-close` vs `schedule-to-close`
(artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-claim-backlog-{depth}.explain.txt`,
the committed run -- reproduce via the command in
[Reproduce](#reproduce)):

| backlog | no-schedule-to-close shared hit | schedule-to-close shared hit | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 57 | +4 | +7.5% |
| 10,000 | 274 | 284 | +10 | +3.6% |
| 100,000 | 9,878 | 2,537 | -7,341 | not meaningful -- see below |

**The 1,000-/10,000-row deltas are shared-buffer hits specifically, not
the query's complete I/O picture, but that distinction doesn't change
their sign or size** -- both plans at those depths report no `temp` I/O to
fold in either way. **The 100,000-row row is not a predicate-cost
comparison at all in this committed run.** The two labels land on
different plans for the candidate scan -- `no-schedule-to-close` an
`Index Scan using idx_harvest_tq_poll`, `schedule-to-close` a plain `Seq
Scan` -- so the raw totals above measure the cost of two different scan
strategies, not the marginal cost of populating `schedule_to_close_at`
under one strategy; see [100,000-row plan
choice](#100000-row-plan-choice) for the full picture, including why both
plans still pay the same identical `temp read=495 written=1914` external-merge-sort
cost regardless. An earlier revision of this page (before the seeding fix
[Workload](#workload) documents) happened to see both labels choose `Seq
Scan` at this depth and reported a clean +2.6%/+1.3% pair of percentages
(shared-hit-only and whole-query, respectively) for that run -- those
numbers were real for the run that produced them, but this page no longer
publishes a 100,000-row percentage, because the committed run backing it
changed which plan each label chose and a percentage across two different
plan shapes would misrepresent what it measures.

The scan-side and `Update`-exclusive breakdown in [Plan](#plan) above,
derived from this same committed run's shared-buffer-hit counters,
decomposes the 1,000-/10,000-row totals into their two component
mechanisms, and explains why 100,000 does not get the same treatment.

### 100,000-row plan choice

**The committed run's two labels land on different plans at this depth --
first direct, fully-auditable evidence that this instability is not tied
to `schedule_to_close_at` specifically.** `no-schedule-to-close` uses an
`Index Scan using idx_harvest_tq_poll` for the candidate-row source (9,878
total buffers on the `Update` node); `schedule-to-close` uses a plain `Seq
Scan` (2,537 total buffers, identical to the previous committed run --
see [Workload](#workload) for why that specific number reproduced exactly
across the seeding fix). Both plans still pay the identical
`temp read=495 written=1914` external-merge-sort cost regardless of which
scan feeds it (`grep`-verified against both artifacts): that index cannot
serve the query's `ORDER BY` (the non-indexable leading `CASE` expression
-- see `docs/performance.md`'s TL;DR), so choosing it does not avoid the
sort and is strictly worse here, not a genuine optimization the planner
found.

**This is the only committed run in this page's history where either
label actually hit the expensive plan.** The earlier, pre-fix committed
run had *both* labels on `Seq Scan` -- no expensive plan on either side --
so it is not a second committed data point for which label the expensive
plan lands on; only this run's `no-schedule-to-close` result is. Codex
review on PR #1339 caught an earlier revision of this paragraph
overstating that: it read the pre-fix run's absence of the expensive plan
as if it put the expensive plan on `schedule-to-close`, then cited that
alongside this run to claim two committed runs showing the phenomenon on
both labels. Still-earlier, uncommitted development runs (see below) did
show the expensive plan on `schedule-to-close` specifically, but those
artifacts no longer exist to audit. What this one committed run *does*
support directly: the expensive plan lands on the label with
`schedule_to_close_at` left `NULL`, the opposite of what a
`schedule_to_close_at`-caused theory would predict. That is still real
evidence against attributing the instability to this predicate, just not
the "flips between both labels, committed either way" claim the earlier
revision made. This page cannot say what does cause the instability: this
run and the previous one used the same query, the same backlog shape, and
(after the seeding fix) the same seeded index-key distribution between
labels, so whatever tips the planner between these two plans at 100,000
rows is sensitive to something this page hasn't isolated -- most likely
ordinary statistical noise in `ANALYZE`'s sample at this table size, but
that is not confirmed here.

This page does **not** assert how often the expensive plan recurs, or
under what conditions, for either label. Codex review caught this claim
leaking back in twice on earlier revisions: first as an explicit "2 of 3
runs" / "2 of 4 runs" framing, and then again -- after that framing was
removed -- as a spelled-out sample-of-two-against-two restating the same
statistic in prose, both sourced from uncommitted development runs (before
this pass's seeding fixes) whose artifacts no longer exist to audit. This
page continues to count none of those uncommitted runs and draws no
frequency, ratio, or before/after conclusion from them -- the only new
claim this revision adds is the one both of this pass's two *committed*
100,000-row runs directly support: the expensive plan is not confined to
one label. **This remains a risk worth being aware of at large backlog
depths, for deployments that populate `schedule_to_close_at` and those
that don't equally**, not a proposed fix target: there is no schema or
query change on offer that would pin the planner's choice without the
"planner-disabling flags... outside a diagnostic session" this repo's
rules ban, and extended statistics or a planner hint would be a
schema/config change outside this pass's scope (this repo's "ask before"
list). A future pass with the budget for many more repeated, fully-fixed
runs -- each with its own committed artifacts -- could turn this into an
actual frequency estimate; this one cannot.

### Corroboration: `pg_stat_statements` over the real claim-drain

To check whether the `EXPLAIN` deltas hold under the actual claim workload
-- repeated `claim_task()` calls draining the backlog one row at a time, as
production does -- the harness drives the real async
`queue::claim_task(...)` function 10,001 times (10,000 successful claims plus
one final empty poll) through a single connection, serially, against the
headline scenario's 10,000-row/4-queue backlog shape at each data state and
snapshots `pg_stat_statements` afterward (artifacts, the committed run:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-pg_stat_statements.txt`).
**This does not exercise the headline scenario's 8 concurrent claimers** --
see [Workload](#workload) for why, and for the same limitation in the
sibling capability-labels and concurrency-key captures this one follows.

**A successful `claim_task()` call issues more than just
`claim_task_query()`'s own SQL text.** After a claim succeeds, `claim_task()`
also runs two authoritative post-claim rechecks against the just-claimed
row -- one against `harvest_queue_pauses`, one against
`harvest_activity_pauses` -- each shaped as its own `UPDATE ... WHERE id =
$1 ... AND EXISTS (...)` statement, so each is its own row in
`pg_stat_statements`. An earlier revision of this section aggregated only
the row matching `claim_task_query()`'s own shape (found via a
`query.contains("rate_limit_debit")` match in the capture code) and called
that "the real claim-drain" cost -- Codex review on PR #1339 correctly
pointed out that this leaves out two statements the real production
operation genuinely issues on every one of its 10,000 successful claims
(the terminal empty poll issues neither, since nothing was claimed to
recheck), understating what driving `claim_task()` actually costs and
making its margin under the impact floor look larger than it is. All three
statements are already present in the committed artifacts (the capture
takes the top 10 `pg_stat_statements` rows, not just the one it asserts
on), so this is a reporting fix, not a re-run:

| statement | no-schedule-to-close total shared-buffer hits (10,000-10,001 calls) | schedule-to-close total shared-buffer hits | delta % |
|---|---:|---:|---:|
| `claim_task_query()` itself (10,001 calls) | 5,264,349 | 5,363,879 | +1.9% |
| queue-pause post-claim recheck (10,000 calls) | 55,154 | 117,755 | +113.5% |
| activity-pause post-claim recheck (10,000 calls) | 55,154 | 117,632 | +113.3% |
| **combined (all three, full drain)** | **5,374,657** | **5,599,266** | **+4.2%** |

**+4.2% is this page's one auditable figure for "the cost of driving the
real `claim_task()` function over this drain,"** with `claim_task_query()`
alone (+1.9%) kept as a separate figure since it's what the `EXPLAIN`-based
[Plan](#plan) section above is built on ( `EXPLAIN` was only run against
that one query). **Both numbers dropped sharply from an earlier revision
of this table (+17.1% combined, +15.5% for `claim_task_query()` alone)**
once the seeding fix [Workload](#workload) describes landed: that earlier
revision let the two labels seed independently-random `id`/`activity_id`
values, and once both labels shared the exact same values, most of what
had looked like a `schedule_to_close_at` effect on the main query turned
out to be that seeding confound instead. The two rechecks' own relative
increase (+113%) is markedly larger than the main query's, and *also*
changed a lot under the same fix (from +72%) -- this page cannot explain
either number with confidence: no `EXPLAIN` was captured for either recheck
statement, only the aggregate `pg_stat_statements` counters above, so there
is no plan-level evidence to confirm the mechanism. **It is not the same
non-HOT index-write mechanism [Plan](#plan) establishes for the main claim
`UPDATE`, though** -- an earlier revision of this section speculated that
it might be, but this capture's fixture never populates
`harvest_queue_pauses` or `harvest_activity_pauses` -- `db::seed()` only
seeds paused rows for `ClaimGate::PausedRows`/`AllGates` (see
`claim_bench_support.rs`'s `wants_paused_rows`), and this capture uses
`ClaimGate::Baseline` throughout -- so `EXISTS (SELECT ... FROM
harvest_queue_pauses ...)` and its activity-pause counterpart are always
false, and each recheck's `WHERE id = $1 AND state = ... AND worker_id =
... AND EXISTS (...)` therefore never matches a row to update. A statement
that never actually writes cannot perform a non-HOT update or maintain any
index -- Codex review on PR #1339 caught this. What both rechecks
genuinely do on every call is a primary-key point lookup on the
already-claimed row plus the `EXISTS` subquery scan against the (empty)
pause table, and this page has no confirmed explanation for why that
combination costs +113% more on the `schedule-to-close` label, or why that
figure itself moved so much once the seeding confound was fixed (a
plausible guess: an unrelated index the primary-key lookup touches was
itself part of the confound, though this page has not verified that); it
is left as an open question rather than attributed to a mechanism the
measured statements cannot exercise.

**Neither number reproduced to a stable value across the several runs this
capture went through over the course of this pass.** Codex review on PR
#1339 caught this same problem twice on an earlier revision, which cited
specific historical bounds from those runs (roughly +2.5%
to +22.5%), and even after that was flagged, the rewrite still asserted a
qualitative pattern across them -- "always positive" -- and predicted that
a future run would land on "a different, but still small and positive,
number." Both are the same unaudited-evidence problem this page's "On
reproducibility" note above disclaims for everything else: those runs'
artifacts are gone (the repro script always overwrites the same canonical
filenames), so nothing about them -- not a range, not a sign, not a trend
-- is something this page can support from the repository as it stands.
The only auditable data points are the committed run in the table above:
**+4.2%** combined (**+1.9%** for `claim_task_query()` alone), comfortably
under the impact floor either way. The drain loop does not capture a plan
for any of its calls, only the aggregate `pg_stat_statements` counters, so
there is no per-call plan trace available to check any hypothesis about
the cause of run-to-run variance, and this page asserts none -- including
any hypothesis about whether the aggregate stays positive, or how large it
runs, on a run other than this committed one.

## Write-side cost

Every `UPDATE` to a claimed row -- including the claim `UPDATE` itself in
`claim_task_query()`'s `claimed` CTE, which never touches
`schedule_to_close_at` -- still creates a brand-new MVCC tuple version that
carries the column's value forward, the same row-width mechanism
`docs/performance-capability-labels.md`'s "Write-side cost" section
documents for `required_capabilities` -- **plus**, specific to this column,
the partial-index write [Plan](#plan) documents: every `UPDATE` to a
`schedule-to-close` row also writes a new entry to
`harvest_task_queue_schedule_to_close_idx`, which a `no-schedule-to-close`
row never touches.

**These are measured as two separate quantities below, not combined into
one percentage** -- an earlier revision of this page reported only
`pg_relation_size('harvest_task_queue')` (the heap) and described the
result as corroborating a "combined" effect; Codex review correctly pointed
out that `pg_relation_size` on the heap relation excludes every index by
definition, so a heap-only snapshot cannot support any claim about index
growth. Both scripts snapshot `pg_relation_size('harvest_task_queue_schedule_to_close_idx')`
separately, and both seed `schedule_to_close_at` with the same per-row-varied
expression [Workload](#workload) settled on -- an earlier revision of both
scripts used a single constant value shared by every row, which understated
the index's real growth by roughly 3x (see [Workload](#workload) for the
mechanism: B-tree deduplication compresses repeated keys far more than
production's genuinely distinct ones). Two independent, standalone,
single-transaction corroborations (which is what makes these two reproduce
cleanly where the live 10,001-call drain does not -- neither leaves a
~15-30-minute window for autovacuum to run partway through, since neither
commits until the whole simulated drain finishes): artifacts
`docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.{sql,txt}`
and
`docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.{sql,txt}`:

| seeding + update shape | heap: no-stc / stc growth | heap extra growth | index: no-stc / stc growth |
|---|---:|---:|---:|
| one bulk `UPDATE ... WHERE state = 'PENDING'` (10,000 rows, one statement) | 250 / 263 pages | +5.2% | 0 / +27 pages (1→1 vs 30→57) |
| 10,000 individual `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE` pairs, PL/pgSQL loop (still one transaction end to end) | 250 / 263 pages | +5.2% | 0 / +27 pages (1→1 vs 30→57) |

The two access shapes land on **identical** results for both quantities:
within a single transaction (no commit boundaries in between), whether the
10,000 rows are touched by one bulk statement or by 10,000 individual
per-row statements changes neither the heap-page-growth nor the
index-page-growth outcome. The heap figures are close to the `EXPLAIN` band
above (2.6-7.5%) -- this is the row-width component, and it is unaffected by
whether the seeded deadline values are distinct or constant (heap-page
count depends on total row *width*, not on how compressible the *index*
built over one column happens to be). **The index figures are the cleanest
evidence on this page for the index-write mechanism**:
`harvest_task_queue_schedule_to_close_idx` never grows at all for
`no-schedule-to-close` (1 page before, 1 page after -- these rows are never
members), while for `schedule-to-close` it starts at 30 pages (the 10,000
initial `INSERT`s, one entry each, with genuinely distinct per-row values so
deduplication cannot compress them) and grows to 57 after the claim
`UPDATE` -- roughly doubling, consistent with every one of the 10,000 rows
getting a second index entry (the old entry, now dead, is not reclaimed
without a `VACUUM`, which this script deliberately does not run between the
before/after snapshots, matching the real window between a claim and
whenever autovacuum next runs).

The instrumented captures also snapshotted `pg_stat_user_tables` immediately
before and after the real 10,000-claim headline drain -- a ~15-30-minute
window in this environment, long enough for autovacuum to run
unpredictably partway through (artifacts, the committed run):

| no-schedule-to-close `n_dead_tup` | schedule-to-close `n_dead_tup` | heap-page growth (no-stc / stc) |
|---:|---:|---:|
| 4,903 | 5,052 | +49 / +52 |

**This does not support a pinned dead-tuple ratio, or even a consistent
sign.** Earlier, now-uncommitted runs of this capture measured
`no-schedule-to-close` dead-tuple counts ranging from roughly 800 to
5,000+, and `schedule-to-close` counts in a similar range, with the
relative ordering between the two labels flipping between runs -- an
earlier committed run of this capture (superseded by the seeding fix
[Workload](#workload) describes) happened to show `no-schedule-to-close`
*higher* than `schedule-to-close`, the reverse of this run's ordering and
of the "expected" direction (the extra write both this row's `UPDATE` and
its `harvest_task_queue_schedule_to_close_idx` entry perform). This run's
own ordering matches that "expected" direction, but this page treats that
as coincidence rather than confirmation, given how much the previous
committed run's ordering (and this section's own historical range) already
demonstrate the instability. Heap-page growth was comparatively closer
between the two labels in this run (+49 vs +52) than in some earlier ones,
but not by a fixed, reproducible margin either. The most plausible
explanation, consistent across every run
of this capture, is that autovacuum's exact timing relative to the
~15-30-minute drain -- entirely outside this harness's control, since
nothing in the test triggers or waits for it -- dominates whatever these two
numbers happen to read at the moment the after-drain snapshot runs, for
either label, independently of `schedule_to_close_at`. Read both numbers in
this table as "noisy and autovacuum-dominated, not a reliable measurement
of the predicate's write-side cost," and rely on the two standalone
corroboration scripts above (which control for autovacuum by never leaving
a multi-minute window open) for the write-side conclusion instead.

No schema, index, or autovacuum-configuration change is proposed by this
pass. A future pass that wants a reliable dead-tuple number for this table
from a live drain, rather than from a controlled single-transaction
simulation, should disable autovacuum for the duration of its own
measurement window explicitly (not done here, since disabling autovacuum
is itself something this repo's rules require flagging findings about
rather than doing silently inside a benchmark).

## Equivalence

All drains claim exactly 10,000 of 10,000 seeded rows
(`claimed == claimed_by_label` asserted equal between the two labels inside
the test), and `claim_row.calls == claimed + 1` is asserted for the final
empty poll in each state (this assertion is inherited from the shared
pattern; see the test source). The schedule-to-close claim path returns the
same claim behavior as the unpopulated path in every run -- the cost (and its
variance) measured here is overhead on an otherwise identical result set, not
a correctness difference.

## What shipped

- `autumn-harvest/tests/integration/claim_budget_tests.rs::zz_capture_schedule_to_close_claim_evidence`
  -- an `#[ignore]`d evidence-capture test (not a CI-gated assertion) that
  seeds both data states at all three `BACKLOG_SWEEP` depths, captures
  `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, TIMING OFF)` for each,
  `ANALYZE`s `harvest_workers` before either label's stat-snapshot drain
  (added in response to Codex review on PR #1339 -- see [Workload](#workload)
  for why the drain needs it), snapshots `pg_relation_size`/`pg_stat_user_tables`
  immediately before and after a real 10,000-row headline drain through
  `queue::claim_task()` at both states while also snapshotting
  `pg_stat_statements`, and asserts claim-count equivalence between the two
  states as a correctness check. Seeds `schedule_to_close_at` with a
  per-row-varied, far-future expression (see [Workload](#workload) for why
  three earlier choices were each wrong), and seeds `id`/`activity_id`
  identically between the two labels via
  `snapshot_seed_for_schedule_to_close`/`reseed_from_schedule_to_close_snapshot`
  (see [Workload](#workload) for why independently-random values there
  would confound the comparison, and why this took three attempts to get
  right).
- `docs/perf-artifacts/schedule-to-close-claim-predicate/` -- the committed
  `EXPLAIN` captures, `pg_stat_statements` snapshots, heap-growth snapshots,
  and the two standalone bloat-corroboration scripts (bulk `UPDATE` and
  per-row PL/pgSQL loop, both snapshotting the partial index separately
  from the heap and seeding the same per-row-varied deadline expression)
  and their output, and a `fixture-summary.txt`.
- `autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh` -- a
  reproduction script that re-runs the capture test.
- This doc.

`queue::claim_task_query()` is unmodified.

## Reproduce

```bash
HARVEST_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
  ./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
```

or, with only a reachable Docker daemon and no external Postgres:

```bash
./autumn-harvest/scripts/schedule_to_close_claim_perf_repro.sh
```

Both regenerate the `EXPLAIN` captures, `pg_stat_statements` snapshots,
heap-growth snapshots, and `fixture-summary.txt` under
`docs/perf-artifacts/schedule-to-close-claim-predicate/` from scratch,
**overwriting the previously committed files** -- there is no per-run
directory, so only the most recent invocation's output is ever present in
the repository. Expect the 100,000-row depth's plan choice and the
aggregate/heap-growth numbers to vary run to run (documented above, not a
reproduction failure); the 1,000-/10,000-row `EXPLAIN` buffer counts and the
`Plan` section's `dirtied`/`written` figures should reproduce closely, though
not necessarily to the exact byte, since the per-row-varied seed expression
does not guarantee bit-identical index layout across independently-created
Postgres instances the way a constant value would.

**They do NOT regenerate `claim_update_bloat_corroboration.txt` or
`claim_update_bloat_loop_corroboration.txt`** -- both scripts are
independent of the Rust harness and neither is invoked by the repro command
above. After any schema, index, or storage-layout change to
`harvest_task_queue`, re-run both explicitly, or the committed corroboration
output will silently go stale even though the primary `EXPLAIN`/
`pg_stat_statements` captures are fresh:

**`$DATABASE_URL` below MUST point at a disposable scratch database --
never a real development, staging, or production database.** Both SQL
scripts repeatedly run `TRUNCATE harvest_task_queue RESTART IDENTITY`, and
`psql` executes each top-level statement in its own autocommit transaction,
so if a later statement fails, the `TRUNCATE`s that already ran are **not**
rolled back. Pointed at a shared application database, this command
irreversibly deletes its queued tasks. The Rust harness above never has
this risk -- it creates and tears down its own dedicated, pid-scoped
scratch database for every run.

```bash
# 1. Create a throwaway database and apply migrations to it.
createdb -h localhost -U postgres harvest_perf_scratch
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/harvest_perf_scratch
(cd autumn-harvest && diesel migration run)

# 2. Run both corroboration scripts against the scratch database only.
psql "$DATABASE_URL" \
  -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.sql \
  > docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_corroboration.txt

psql "$DATABASE_URL" \
  -f docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.sql \
  > docs/perf-artifacts/schedule-to-close-claim-predicate/claim_update_bloat_loop_corroboration.txt

# 3. Tear the scratch database down when done.
dropdb -h localhost -U postgres harvest_perf_scratch
```
