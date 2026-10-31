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

## What we took

- `_INV_TEMPLATE`, as the base of `INV_TEMPLATE`.
- `_DOM_TEMPLATE`, as the base of `DOM_TEMPLATE`.
- The "how to decide" block of `propose`.
- `_KEEP_BASE`, `ANSWER_FREE` and `EVIDENCE_BOUNDARY`.

## What we changed

- The surface is a `ProfileOverlay`, not a harness tree. The text names
  the system prompt, the guardrails and the bounds. It does not name
  skills, scripts, subagents, hooks or the workspace library.
- The INV list drops the script and skill levers. It adds a lever for the
  bounds.
- The DOM list drops the skill encoding. It keeps the rule that the system
  prompt costs context on every run.
- The `{deliverable}`, `{domain}` and `{levers}` slots are fixed text.
- `GENERALIZE`, the surface blocks and the benchmark slots are not used.
- The reply format, `OUTPUT_CONTRACT` in `src/refine/model_proposer.rs`,
  is new text. It is not adapted.
