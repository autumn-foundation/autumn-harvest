## Comment-hygiene backlog cleared (PR #2682)

The comment-hygiene audit is at zero across the corpus. Comments that only
restated code are gone. Comments that state a false claim are corrected.
Code is unchanged, apart from one import line that rustfmt moved.

- **Backlog.** Review-round archaeology went from 1,331 findings to 0.
  Contractions went from 412 to 0. Sentences over 25 words went from
  17,065 to 0. Long rationale is split into short sentences and kept.
- **Gate change.** CH005 (review-round archaeology) and CH006
  (contractions) move to Tier A, so a new one fails the build anywhere.
  CH005 now flags any mention of the Codex reviewer. Both rules skip
  inline code spans. CH007 stays a ratchet against the merge base, now
  over a zero corpus.
- **Wrong comments.** About 70 comments contradicted the code: missing
  functions, stale counts, docs on the wrong item, and "RED" banners on
  shipped APIs. Each correction was checked against the code.
- **Evidence.** A token-stream comparison proves that each changed `.rs`
  file differs from the merge base only in comments. clippy passes on the
  workspace and on each CI feature set of the core and plugin crates. The
  xtask tests, core lib tests and every source-reading guard test pass.
