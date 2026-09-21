### Changed

- **Assay ledger #12: re-charter of assay #10's "position of the depth
  curve's knee" pit, killed on its own pre-registered stop rule.** #10's
  post-hoc, single-repetition depth diagnostic found the `postgres` arm
  ahead of `redis_pg` at backlog depths 250 and 500, and behind it at 1000
  and 2000, suggesting a throughput crossover somewhere in `(500, 1000]`.
  `docs/operations/redis-dispatch.md`'s "When to use it" section has no
  number backing "deep"/"shallow", so this re-runs the same two bracketing
  depths at n=3 (not n=1) before spending any time narrowing a band.

  Depth 500 replicates cleanly: `postgres` mean 23.76 (range
  [23.55, 23.86]) stays above `redis_pg`'s mean 21.48 (range
  [20.06, 22.29]), no overlap. Depth 1000 does not replicate: `postgres`'s
  three repetitions (14.94, 24.04, 23.80 — mean 20.93) span a 61% swing,
  and its range fully contains `redis_pg`'s tight [21.24, 22.03]. The
  pre-registration's own committed stop rule treats a rep-range overlap at
  either bracketing depth as unsettling the crossover claim; depth 750 was
  not run.

  This does not disturb #10's own separate, tightly-clustered finding
  (`redis_pg` flat within 1.4% from 250 to 2,000 rows in that sweep) — it
  shows that `postgres`'s own repetition-to-repetition variance at depth
  1000, under this apparatus's single-worker shape, is large enough that a
  lone repetition (exactly what #10's diagnostic was) cannot reliably say
  which arm leads at that depth. The operator guide gets no depth figure
  from this; the honest outcome is that "deep" stays undefined rather than
  acquiring a false-precision number that failed to replicate.

  One environment correction made before any reported run: the apparatus's
  own printed durability line showed the freshly started Postgres server
  defaulting to `fsync = on` / `synchronous_commit = on`, not #10's
  registered `off`/`off`. Caught before treating the first attempt as
  comparable; `postgresql.conf` was corrected and the server restarted, and
  the mismatched run was discarded rather than reported.

  Named, un-chartered pits: what causes the ~60% spread in `postgres`'s
  own repeated measurements at depth 1000 (cold cache after the
  apparatus's per-repetition database recreation is one untested
  candidate); a higher-repetition (n≥6) characterization of that spread;
  the knee question itself, which needs a variance-characterized apparatus
  before any depth number is decision-grade.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change. Apparatus reused unmodified from #10;
  raw per-rep output archived under
  `docs/assays/apparatus/0010-cross-mode-throughput/results/depth-knee-0012/`.
