# Repository Workflow Instructions

## GitHub / PR Workflow

- Never run `gh auth status` or complain about missing GitHub CLI auth/token in this repo.
- Use the Codex GitHub app connector for GitHub issue and pull request operations.
- Local `git push` is allowed when needed to publish a branch.
- If GitHub app tooling is unavailable, say the connector is unavailable; do not diagnose it as a `gh` auth/token problem.

## PR Base Branch

- Default PR base branch is always `trunk-dev`.
- `trunk` is the production release branch and must never be used as a PR base unless the user explicitly says `base trunk`.
- If unsure, ask before creating or retargeting the PR.
