### Added

- **Two pre-registered assays: cross-mode throughput, and the first
  cross-engine comparison (ledger #10, #11).** Harvest had four measurements
  of its own modes, taken at four shapes on at least three hosts against at
  least three trees, with the embedded SQLite backend appearing in none of
  them; and `docs/benchmarks.md` stated plainly that no competitor had ever
  been run on the same hardware. Both gaps are now closed by measurement, and
  both assays report a **kill**.

  **#10 (cross-mode)** drains one backlog of the canonical 3-activity workflow
  on three arms — embedded SQLite, the Postgres claim path, and Postgres with
  `RedisDispatch` — on one box, from one tree, in one sitting. Its validity
  line **failed**: the Postgres arm read 5.58 workflows/sec against a
  registered `[7.91, 71.19]` band around the published 23.73, so L2 and L3 are
  ungraded and the apparatus refuses to print them. The cause was this repo's
  own documented warning: `docs/benchmarks.md` records why #941 chose a bounded
  closed loop over a pre-loaded drain, because a drain re-publishes #786's
  claim-depth curve under an end-to-end label — and #10 registered a 2,000-deep
  drain anyway. The durable finding is its post-hoc depth diagnostic:
  `redis_pg` is **flat within 1.4%** from 250 to 2,000 rows of backlog
  (22.21–22.53 workflows/sec) while `postgres` falls **4.2x** (23.90 → 5.63).
  The channel's margin is therefore 0.93x at 500 rows and 3.94x at 2,000: it is
  **insurance against backlog depth, not a general throughput upgrade**, and a
  deployment that never builds a deep queue pays for a second stateful
  dependency and gets nothing. The embedded arm's 3.20 workflows/sec is
  deliberately *not* presented as like-for-like — `autumn-harvest-sqlite`
  hard-codes `PRAGMA synchronous = FULL` and exposes no knob, while the
  Postgres arms ran `fsync=off` per the published bench conditions, so part of
  any gap is durability rather than engine, and L3 was registered without
  noticing that.

  **#11 (harvest vs Temporal)** puts both engines on one 4-core box against the
  same PostgreSQL server at the same shape. **Harvest lost, by 7.9x** (5.58
  against 44.31 workflows/sec), and lost in the venue that disadvantages its
  competitor, with all four Temporal services co-resident. Its depth diagnostic
  is what makes the result usable rather than merely bad news: Temporal is
  faster at *every* depth tested, so the kill is not an artifact of depth;
  against harvest's *best* mode the margin is a stable ~1.7x rather than 7.9x;
  and the widening part is the known, fixable `#786`/`#1177` claim-path defect.
  That bounds the highest-value performance fix available: repairing #1177
  would move the default mode from 5.63 toward the Redis arm's flat 22 at depth
  2,000, taking the margin from 6.9x to roughly 1.8x.

  **Measurement discipline is most of the work here, and it is adversarial by
  construction.** Both pre-registrations were committed before either apparatus
  existed, and #11's fixed *in advance* what its result may not be read to mean,
  so a harvest win could not later be spun as "faster than Temporal." Six
  rounds of review found 19 defects; **three sweeps were discarded rather than
  reported**, including one whose Redis arm published no dispatch hints at all
  and was therefore timing the reconcile sweep's recovery path. Five separate
  places where #10's workload was not the port-by-value it claimed were fixed
  and then the whole workload was diffed against `e2e_bench_support.rs` field by
  field. Four parity defects in the Temporal arm, **all of which disadvantaged
  Temporal**, were fixed before the reported run. Caveats that cut against
  harvest are recorded rather than omitted: the measured window charges worker
  startup, which is far costlier for Temporal, so the shallow-depth cells
  understate it.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change; nothing under `autumn-harvest/src/` is
  touched. Both apparatus are non-production throwaways, detached from the root
  workspace. `docs/comparison.md`'s stale "no first-party benchmarks yet" bullet
  is corrected, and `docs/benchmarks.md` now carries the Temporal result
  including the part where harvest loses.
