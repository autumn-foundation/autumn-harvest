## Fix — autoresearch compiles again after three parallel fixes

PR #2187 added `start_provenance` to `testing::HistorySnapshot`, and the
autoresearch replay helper did not set it. PRs #2201, #2204 and #2207 each
added the missing field on their own. Git merged all three without a
conflict, so the struct literal named the field three times, and `trunk-dev`
did not compile `autoresearch`. The helper now sets it once.

No migration. No `WorkflowEvent` change.
