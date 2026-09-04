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
**+7.5% at 1,000 rows, +3.6% at 10,000, +2.6% at 100,000** -- corroborated by
two standalone MVCC-bloat scripts. None of this comes close to the 20%
impact floor; no fix is proposed or needed.

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
| 100,000 | 2,453 / 2,513 | +60 | +4 |

The `Update`-exclusive column (the index write plus whatever else the
`Update` node itself touches, beyond its child) is **exactly constant across
all three depths (+4 every time)** -- consistent with the fixed one-page
index write the `dirtied`/`written` evidence above already established (a
B-tree insert typically touches a root and/or a leaf page as `hit`s in
addition to the one page it dirties, so a handful of total `hit` buffers
for one insert is unsurprising; a genuinely distinct key per row, per the
seeding fix in [Workload](#workload), needs a real tree descent rather than
landing in an already-cached duplicate-key leaf, which plausibly explains
why this settled at a clean +4 rather than the +3/+4/+4 an earlier,
degenerate-seed run measured). The scan-side column is **not** simply
proportional to backlog depth: it is exactly zero at 1,000 rows, +6 at
10,000, and +60 at 100,000. Zero at the smallest depth is consistent with a
genuine row-width effect that only becomes visible once the table is large
enough for the extra bytes per row to push the page count itself higher --
at 1,000 rows both data states may simply pack into the same number of heap
pages by coincidence of alignment and fill factor, with the effect only
crossing a whole-page threshold at larger sizes -- but this page does not
have a targeted test isolating that specific claim, so it is reported as
the most plausible explanation for the pattern, not a confirmed one. This
is consistent with the *general* row-width mechanism
`docs/performance-capability-labels.md`'s `required_capabilities` finding
describes for its own (larger) JSONB column, without claiming the same
smooth, always-positive scaling that page found for its wider effect.

Neither component is a defect. The index exists because the timeout
scanner needs it (its own migration comment says so, and no alternative
was evaluated by this pass, which measures rather than redesigns); the
scan-side row-width cost is inherent to storing a wider column at all. No
schema or query change is proposed.

## Measurement

### Buffer deltas across backlog depth

`EXPLAIN (ANALYZE, BUFFERS, ...)` total buffers for `claim_task_query()`,
`no-schedule-to-close` vs `schedule-to-close` (artifacts:
`docs/perf-artifacts/schedule-to-close-claim-predicate/{no-schedule-to-close,schedule-to-close}-claim-backlog-{depth}.explain.txt`,
the committed run -- reproduce via the command in
[Reproduce](#reproduce)):

| backlog | no-schedule-to-close buffers | schedule-to-close buffers | delta | delta % |
|---:|---:|---:|---:|---:|
| 1,000 | 53 | 57 | +4 | +7.5% |
| 10,000 | 274 | 284 | +10 | +3.6% |
| 100,000 | 2,473 | 2,537 | +64 | +2.6% |

The scan-side and `Update`-exclusive breakdown in [Plan](#plan) above,
derived from this same committed run, decomposes each of these totals into
its two component mechanisms.

### 100,000-row plan choice

The 100,000-row depth's candidate-row source used a plain `Seq Scan` on
both sides in the committed run, landing on the cheap +2.6% delta shown
above. Earlier, now-uncommitted runs of this capture (before the seeding
and `ANALYZE` fixes in [Workload](#workload)) sometimes measured a far more
expensive plan at this same depth specifically for `schedule-to-close` --
`Index Scan using idx_harvest_tq_poll` instead of `Seq Scan`, still
followed by the same external-merge sort, pushing the total well past
10,000 buffers. That index cannot serve the query's `ORDER BY` (the
non-indexable leading `CASE` expression -- see `docs/performance.md`'s
TL;DR), so the alternative plan is strictly worse here, not a genuine
optimization the planner found.

This page does **not** assert how often that expensive plan recurs, or
whether it is more or less likely now that the seeding bug (a single
byte-identical index key across all 10,000 rows, fixed in
[Workload](#workload)) is fixed -- Codex review pointed out that an earlier
revision's "2 of 3 runs" and later "2 of 4 runs" framing asserted exactly
that kind of statistic from runs whose artifacts are no longer committed to
the repository and cannot be independently audited. What can be said
honestly: the two runs that showed the expensive plan both predated *every*
fix in [Workload](#workload) (the degenerate constant-key seeding included,
which is exactly the kind of thing that can distort planner statistics into
unrepresentative territory), and it was not observed in either of the two
runs that had the seeding fix applied, including this committed one. That
is consistent with the seeding bug being part or all of the explanation,
but it is a sample of two against two, confounded with the `ANALYZE` fix
landing at the same time, and this page does not have the evidence to
distinguish "the seeding bug caused it" from "it was never that likely to
begin with." **This remains a risk worth being aware of at large backlog
depths for deployments that populate `schedule_to_close_at`**, not a
proposed fix target: there is no schema or query change on offer that
would pin the planner's choice without the "planner-disabling flags...
outside a diagnostic session" this repo's rules ban, and extended
statistics or a planner hint would be a schema/config change outside this
pass's scope (this repo's "ask before" list). A future pass with the
budget for many more repeated, fully-fixed runs -- each with its own
committed artifacts -- could turn this into an actual frequency estimate;
this one cannot.

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
sibling capability-labels and concurrency-key captures this one follows:

| no-schedule-to-close avg/call | schedule-to-close avg/call | delta % |
|---:|---:|---:|
| 484.04 | 559.18 | +15.5% |

**This did not reproduce to a stable number across the several runs this
capture went through over the course of this pass**, as earlier revisions
of this page said by citing a specific historical range (roughly +2.5% to
+22.5%, always positive -- `schedule-to-close` never came out cheaper --
but not converging on one value). Codex review on PR #1339 correctly
pointed out that those earlier runs are exactly the kind of uncommitted,
no-longer-reproducible data this page's "On reproducibility" note above
disclaims: the repro script always overwrites the same canonical
filenames, so citing specific bounds from them stated an unaudited
number as though it were evidence. The only auditable data point is the
committed run in the table above: **+15.5%**. The drain loop does not
capture a plan for every one of its 10,001 calls, only the aggregate
`pg_stat_statements` counters, so there is no per-call plan trace available
to check any hypothesis about the cause of run-to-run variance directly,
and this page does not assert one. Treat +15.5% as this pass's one
committed, auditable measurement of the aggregate delta -- positive and
real, comfortably under the impact floor either way -- not as a value this
page claims is stable: unaudited historical runs during this same pass
varied noticeably, so a different environment or run should be expected to
land on a different, but still small and positive, number rather than
exactly this one.

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
| 5,016 | 2,184 | +48 / +45 |

**This does not support a pinned dead-tuple ratio, or even a consistent
sign.** Earlier, now-uncommitted runs of this capture measured
`no-schedule-to-close` dead-tuple counts ranging from roughly 800 to
5,000+, and `schedule-to-close` counts in a similar range, with the
relative ordering between the two labels flipping between runs (this
committed run's `no-schedule-to-close` figure is actually *higher* than its
`schedule-to-close` figure, the reverse of the pattern earlier runs showed).
Heap-page growth was comparatively closer between the two labels in this
run (+48 vs +45) than in some earlier ones, but not by a fixed, reproducible
margin either. The most plausible explanation, consistent across every run
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
  three earlier choices were each wrong).
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
