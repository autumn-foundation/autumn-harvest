## Docs — Why Harvest keeps deterministic replay (issue #1993)

Docs-only. New page `docs/why-deterministic-replay.md` answers engines that advertise "no deterministic replay" as a feature. It compares replay with checkpoint-only steps and process snapshots by what each engine checks on resume. It states what replay buys: full history, reset to a completed boundary, replay debugging and drift detection. It lists what replay costs, and pairs each problem with the tool that addresses it: HVG001–HVG011, `det_check`, the `harvest-verify` prototype, deterministic primitives, patch markers, the `ReplayVerifier` gate, the in-flight drift gate, the replay canary and ND-blocking. It also says when a checkpoint-only engine is the better choice.

`docs/comparison.md` links the page from its determinism row, its determinism narrative and its Related list. The same change fixes stale claims in the pages that the new page links:

- The comparison page cited the replay gate as "Phase 3.5". `docs/shipped-work.md` gives that phase to local activities. The page now cites the `ReplayVerifier` gate (#251).
- The determinism guide said a divergent run moves to the dead-letter queue, in two places. Since #603 it is parked.
- `docs/replay-verify.md` named a `harvest replay-verify` subcommand and an `export --batch` flag. Neither exists. Its CI snippet wrote a batch envelope into the fixture directory, which `verify_dir` rejects. The snippet now splits the envelope into one file per run.

Guard: `tests/integration/replay_positioning_docs.rs`. It checks the sections, the cited assets, the HVG range against `guardrail::catalog()`, that each cited issue has shipped, the comparison links, and that no prose sentence exceeds 25 words. A new ungated `lint` step runs it on docs-only changes. No public API, migration or `harvest_events` change.
