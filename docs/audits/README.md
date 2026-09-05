# docs/audits/

Deterministic, reproducible checks over the docs corpus and adjacent
generated surfaces — no server, browser, or network access required. Each
script is runnable standalone (`python3 docs/audits/<script>.py`) and safe
to wire into CI as a gate.

| Script | Checks | Wired into CI |
|---|---|---|
| `corpus-link-check.py` | Internal markdown links (missing file, missing anchor) and orphan pages across `docs/**/*.md` | Yes — `.github/workflows/ci.yml`, `lint` job |
| `config-cli-drift.py` | Doc-cited `[harvest]` TOML config keys, `AUTUMN_HARVEST*` env vars, and `harvest` CLI `--flags` against the real schema/CLI, extracted mechanically from `autumn-harvest-plugin/src/config.rs` and `autumn-harvest-cli/src/lib.rs` | Yes — `.github/workflows/ci.yml`, `lint` job |
| `vantage-dashboard-contrast.py` | WCAG 1.4.3 contrast on the Vantage dashboard's inline stylesheet | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s `STYLE` constant |
| `comment-hygiene.py` | Comment defects across every `*.rs`: commented-out code, unreferenced TODOs, narrative asides, blank block edges (all gated at zero), plus review-round archaeology, contractions and over-long sentences (ratcheted against the merge base, so a change may not add one to a file it touches) | Yes — `.github/workflows/ci.yml`, `lint` job |

## Comment hygiene: the two tiers

`comment-hygiene.py` is the one audit here that scans code rather than
docs, and its tier split is deliberate.

**Tier A is absolute.** CH001–CH004 are defects under any house style,
they were driven to zero when the harness landed, and a new one fails the
build. Fix the finding; there is no baseline to absorb it.

**Tier B is a ratchet against the merge base.** CH005–CH007 have a legacy
population too large to fix in one change (CH007 alone is ~17.9k
sentences), so the rule is simply: your change may not *add* one to a file
it touches. Existing findings stay until someone chooses to fix them.

There is no checked-in baseline, deliberately. CI passes `--base`, and the
harness re-scans each changed file as it stood at the merge base, read out
of git. A stored baseline goes stale the moment the base branch moves —
one merge adding a long comment anywhere turns every open PR red for a
file its author never opened (this happened on the harness's own first CI
run) — invites being regenerated to launder a new violation, breaks on
renames, and costs half a megabyte of generated fingerprints in the tree.

Findings are matched by **fingerprint** (rule + normalized text), not
counted. A count only answers "how many", so removing one legacy violation
and adding a different one in the same file nets to zero and passes. An
identity answers "which". A renamed file is compared against its own
previous path, so a pure rename is not a regression; a file the change
adds has no merge-base version, so every Tier B finding in it is the
change's own.

When the diff cannot be computed — no `--base`, an unknown ref, a shallow
clone — Tier B reports but never fails. Gating the whole corpus at the
moment the tool cannot tell what changed is the worst option available.
(This is why the `lint` checkout uses `fetch-depth: 0`: without real
history there is no merge base, and the step would silently stop gating.)

**It reads every comment, via a lexer.** Leading and trailing `//`,
`///`, `//!` and `/* */` (nested and doc forms included) all count — a
defect introduced as `let n = 1; // TODO: fix` is gated exactly like one
on its own line. Text inside a string is not a comment, which matters in
both directions: this corpus embeds Rust and SQL snippets in raw strings
(`det_check_tests.rs` fixtures carry `// harvest-suppress:` lines,
`chaos_catalogue_drift.rs` carries a literal commented-out call as test
data), and flagging one would fail CI on an innocent fixture. Run
`python3 docs/audits/comment-hygiene.py --self-test` to check the lexer
against the forms it must get right; CI runs it before the scan.

**It does not cap comment length, and must not start.** The long rationale
blocks in this tree are load-bearing: the ABBA lock-ordering argument at
`materialize_due_child_timeout_deadlines`, the `cohort`-key argument in
`partition.rs`, the codec-rotation scope guarantee `CLAUDE.md` cites as
the proof that engine exception #3 is safe. A word budget over a comment
block would reward deleting precisely those. CH007 is measured per
*sentence*, so a thorough rationale passes cleanly once it is written as
several sentences instead of one.

Add a new audit here when a docs (or docs-adjacent UI/generated-artifact)
defect class is mechanical to detect — the point is to make a defect class
un-reintroducible, not to write it up once and move on. Wire it into CI's
`lint` job (ungated by the docs-only-changes filter, so it runs on every
PR including docs-only ones) once it's stable.
