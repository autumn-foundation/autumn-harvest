## Docs — stop the safety and SQLite reports from conflicting every PR

**Problem.** Two docs guards made feature PRs edit the same lines.
`sqlite_feasibility_docs` pinned live counts in the prose of
`docs/rnd/sqlite-feasibility.md`: the core module total, the coupled total,
the (a)/(b)/(c) totals, the per-mechanism counts and the migration count.
Almost every PR that added a module or a migration changed them, so each
merge conflicted the other open PRs. `safety_report_docs` paired each limit
bullet in `simulation.md` and `formal-methods.md` with a hand-written
paraphrase in `docs/safety-report.md`. When #2100 and #2101 changed those
bullets, 12 open PRs each ported a different paraphrase, and all 12
conflicted with the fix that landed in #2106.

**SQLite report.** The prose now states ratios: about half of the core
modules are coupled, class (c) holds more than a quarter of them and at
least twice class (a), most use raw SQL, and there are over 100 migrations.
`live_figures_support_the_prose` checks each ratio and prints the live
counts with `--nocapture`. `every_mechanism_has_a_row_and_no_count` rejects
a live count in the mechanism table. The per-module inventory is unchanged
and still guarded in both directions.

**Safety report.** `## Limits of this report` now holds a verbatim mirror of
each source limit list, between `<!-- mirror: ... -->` markers. Relative
links are rebased to `docs/`. `every_source_limit_is_mirrored` fails when a
mirror differs from its source. `UPDATE_DOC_MIRRORS=1` rewrites it. Two
branches that make the same source change now write identical bytes, and
`LIMIT_SOURCES` no longer lists a phrase per bullet.

**Rule.** `CLAUDE.md` now bans both shapes in docs guards.
