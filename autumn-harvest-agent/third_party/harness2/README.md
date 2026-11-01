# Harness²

The default proposer templates in `src/refine/prompts.rs` adapt text from
Harness².

| Field | Value |
|---|---|
| Project | [google-research/harness2](https://github.com/google-research/harness2) |
| Source file | `harness2/core/prompts.py` |
| Source commit | `1cfe254e8d25b15f37048c3730134686abe60335` |
| Copyright | Copyright 2026 Google LLC |
| License | Apache-2.0. The full text is in [`LICENSE`](LICENSE). |

## Adapted text

- `_INV_TEMPLATE`, as the base of `INV_TEMPLATE`.
- `_DOM_TEMPLATE`, as the base of `DOM_TEMPLATE`.
- The "how to decide" block of `propose`.
- `_KEEP_BASE`, from the aggregate prompts.
- `ANSWER_FREE` and `EVIDENCE_BOUNDARY`.

## Changes

- The surface is a `ProfileOverlay`, not a harness tree. The text names
  the system prompt, the guardrails and the bounds. It does not name
  skills, scripts, subagents, hooks or the workspace library.
- The opening line is new. The candidate-index and round text of
  `propose` is not used.
- The INV list drops the script and skill levers. `__LOOP_LEVER__`
  becomes "guardrail". A new lever covers the bounds. The INV text drops
  "reserve room for every artifact" and "mechanically".
- The DOM list drops the skill encoding. It keeps the rule that the system
  prompt costs context on every run.
- The `{deliverable}` and `{domain}` slots are fixed text. The `{levers}`
  slot is removed.
- `EVIDENCE_BOUNDARY` names "the evidence and the current overlay", not
  "files under `context/`". It drops the sentence about scripts in a
  candidate harness.
- `GENERALIZE`, the surface blocks and the benchmark slots are not used.
- The reply format, `OUTPUT_CONTRACT` in `src/refine/model_proposer.rs`,
  is new text. It is not adapted.
