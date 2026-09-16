### Changed

- **Corrected the assay #10 and #11 numbers after three further review
  findings (follow-up to #1617).** #1617 merged with its apparatus already
  fixed and its reports still carrying numbers the pre-fix apparatus had
  produced. This re-measures and replaces them.

  **Assay #11 had never run its own registered workload.** Its Shape table
  registers a ~40-byte payload for both arms; both had been moved to the
  canonical empty object so assay #10's L1 could compare against a published
  figure taken that way, and #11 then reused #10's harvest arm unchanged. The
  deviation had been disclosed, which is not the same as grading registered
  lines on the registered shape. The apparatus now takes `ASSAY10_INPUT_JSON`,
  so #10 keeps the empty object its L1 needs and #11 runs both arms at its own
  payload. The same round found the Temporal arm persisting **no** activity
  input payload where harvest persists an explicit JSON null, which is 6,000
  smaller history records per repetition in Temporal's favour, and the embedded
  SQLite arm still returning JSON null where the other two return
  `{"ok": true}` — the embedded backend runs a caller-supplied callback rather
  than `ActivityInfo::handler`, so the earlier fix never reached it.

  Corrected figures: assay #11 is **43.29 against 5.47 workflows/sec**, a
  **7.91x** kill against harvest (computed from the unrounded means; a first
  revision printed 7.92x by dividing rounded display values, caught in review),
  replacing 44.31 against 5.58. Assay #10's
  embedded arm is **3.19**, replacing 3.20. The corrections moved neither
  headline measurably. Both facts are recorded: the withdrawn numbers came
  from an apparatus with known defects, *and* they happened to be right.

  **A harvest run was discarded for box contamination, and the report says so
  in detail.** Its repetitions read 5.39, 5.37 and **15.02** workflows/sec, and
  that single outlier dragged the mean to 8.60 — which *passes* the L1 validity
  band the same arm otherwise kills. The cause was `git merge` and `git push`
  running on the box mid-measurement, against the idleness precondition
  `docs/benchmarks.md` insists on. It is recorded as the most dangerous failure
  in this work: a verdict flipped from kill to pass by interference, invisible
  in the mean and visible only per repetition.

  Also newly reported rather than smoothed away: Temporal's spread across three
  repetitions (39.28-48.95, about 25%) is far wider than harvest's (about 5%),
  which three repetitions cannot characterise and this assay does not try to.

  **Every depth-diagnostic cell was re-measured** after the payload
  corrections. A first revision kept the pre-correction cells and argued from
  the single re-run cell that the curve was unaffected; review caught that as
  an overreach, since the corrections are asymmetric (one adds persisted
  payloads to Temporal only) and the shallowest cell had the narrowest margin.
  The re-measured 250-row cell reads 36.17 against 28.85 before, so the
  shallowest margin reads 1.53x where it read 1.22x. That difference is **not
  attributed to the correction**: both are single repetitions and the gap is
  about the size of this arm's own ~25% spread, so run-to-run variation alone
  explains it. The cells support a range (~1.5x-2x against harvest's best
  mode) rather than a trend, and Temporal's own cells span about 33%, which is
  too noisy to call flat. What holds is that Temporal wins at every depth
  tested and shows no counterpart to harvest's 4.2x collapse.

  **Zero engine impact.** No `WorkflowEvent` variant, no migration, no
  behaviour or public-API change. `registered-sweep.md` is kept unedited with
  the withdrawn embedded figure still in it, and the report points at the
  re-run instead, so the record shows what was measured rather than what was
  preferred.
