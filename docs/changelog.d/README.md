# Changelog fragments (`docs/changelog.d/`)

This directory holds **changelog fragments** (newsfragments), one per pull request. It exists to eliminate the merge conflicts that arise when many parallel PRs each edit a shared changelog list.

## Why

Historically every PR appended a `Phase N` entry to the phase list in `CLAUDE.md` (and to `CHANGELOG.md`). With multiple PR waves in flight at once, nearly every PR conflicted on those shared, append-only lists. Fragments move each PR's entry into its own file, so two PRs never touch the same file.

## The rule

- **Every substantive PR adds exactly one new file:** `docs/changelog.d/pr-<number>-<slug>.md`
  - `<number>` = the PR number (or the issue number if the PR number isn't known yet — rename on push if needed).
  - `<slug>` = a short kebab-case description, e.g. `pr-812-schedule-update`.
- The file contains the **full changelog entry** that previously went into the `CLAUDE.md` phase list: the issue number, what shipped, key design decisions, invariant/append-only notes (new `WorkflowEvent` variant? migration?), and test evidence. Same content, new home.
- **Do NOT edit the `### Phase Status` list in `CLAUDE.md` or `CHANGELOG.md` directly in a feature PR.** Those files are now updated only by a periodic collation sweep.

## Collation

A dedicated maintenance session periodically:

1. Folds all accumulated fragments into `CLAUDE.md`'s phase list and `CHANGELOG.md`, in one PR.
2. Deletes the collated fragment files.

Because collation is a single, isolated PR, it conflicts with nothing.

## Fragment template

```markdown
## Phase X.Y — <short title> (issue #NNN)

<What shipped: the same prose you'd have written in the CLAUDE.md phase entry —
design decisions, invariant notes (new WorkflowEvent variant? migration?),
and test evidence.>
```

Keep the heading style consistent with the existing phase entries so collation is a straight copy-paste.
