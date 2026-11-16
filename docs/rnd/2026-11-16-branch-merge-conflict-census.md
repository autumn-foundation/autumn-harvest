# 🏛️ Keystone findings: branch-merge conflicts (compact `openapi.json`, 71 of 485 conflicted merges)

**Status:** findings memo. No RFC, no architecture decision. Each change named
here is a two-way door for the implementing team.
**Date:** 2026-11-16. **Window:** PR-branch merges dated 2026-08-27 to 2026-11-16.
**Pinned at:** `trunk-dev` `9f2a07563`, PR head refs fetched 2026-11-16.

## Question

The 2026-09-02 memo (`2026-09-02-management-api-route-coupling.md`) ranked
files by commit touches. It inferred coordination cost from touch rate and
file size. This memo measures the coordination cost directly. It counts the
merges of `trunk-dev` into a PR branch that conflict, and it names the files
that conflict.

## Evidence (tier 2)

The repository has one human author and many agent sessions in parallel.
There are no team boundaries, so cross-team change count is zero. The cost
that remains is the manual merge of `trunk-dev` into an open branch.

Each merge commit on a PR branch was merged again with
`git merge-tree --write-tree` (git 2.43). The conflicting paths come from that
result, not from the commit message.

| Window | Merges into PR branches | Conflicted | PRs that merged | PRs with a conflict |
|---|---:|---:|---:|---:|
| 2026-08 (from 27th) | 11 | 9 | 10 | 8 |
| 2026-09 | 153 | 73 | 80 | 53 |
| 2026-10 | 446 | 327 | 157 | 122 |
| 2026-11 (to 16th) | 93 | 76 | 44 | 39 |
| Whole window | 703 | 485 (69%) | 285 | 217 |

The merge columns sum to the window totals. The PR columns do not, because
one PR can merge `trunk-dev` in more than one month.
`trunk-dev` has 851 non-merge commits in the window. A branch that rebased,
or never merged `trunk-dev`, is not visible. So the counts are a floor.

Top conflict sources in the whole window, by conflicted merges:

| Conflicted merges | PRs | Share of 485 | Path | Shape |
|---:|---:|---:|---|---|
| 120 | 51 | 24.7% | `docs/rnd/sqlite-feasibility.md` | live count in prose. 0 since 2026-11-01. |
| 71 | 30 | 14.6% | `autumn-harvest-plugin/openapi.json` | generated, one 498 KB line |
| 61 | 35 | 12.6% | `docs/upgrading/0.5.0.md` | migration table. 60 of 61 hunks add rows only. |
| 37 | 24 | 7.6% | `autumn-harvest/src/worker.rs` | source |
| 26 | 19 | 5.4% | `docs/testing/formal-methods.md` | proof table, guarded by `formal_models_coverage` |
| 14 | 12 | 2.9% | `autumn-harvest-plugin/src/api.rs` | source |

Since 2026-11-01, the top sources are `docs/testing/formal-methods.md`
(21 merges, 15 PRs), `docs/agent-adapter.md` (14, 10),
`autumn-harvest-plugin/openapi.json` (14, 8) and the `mod` list in
`formal/verus/src/lib.rs` (9, 8). The agent crate files of epic #2438 follow.
Those are parallel PRs on the same new files, which is inherent to the work.

## Finding 1: the compact `openapi.json` conflicts by construction

`cargo xtask dev regenerate-openapi` writes the same document twice from
`docs/api-contract.json`:

- `docs/openapi.json`, pretty-printed, 15,450 lines.
- `autumn-harvest-plugin/openapi.json`, compact, one line of 498,349 bytes.
  `openapi.rs` compiles it in with `include_str!`.

Both copies changed in the same 56 commits in the window. The results differ:

| File | Conflicted merges |
|---|---:|
| `autumn-harvest-plugin/openapi.json` (one line) | 71 |
| `docs/openapi.json` (pretty) | 0 |
| `docs/api-contract.json` (the source) | 1 of those 71 |

So in 70 of 71 merges the source merged cleanly. Only the one-line copy
conflicted. In 27 merges it was the only conflict. Git merges by line. Any
two branches that change the API change the one line, so they always
conflict. The content is the same as the pretty copy, which never did.

This is accidental, not inherent. It costs 30 PRs a manual resolution and a
regenerate step in 80 days.

## Finding 2: append points, not file size, predict conflicts

- `worker.rs` doubled in the window, from 31,270 to 62,180 lines. It is now
  the most-touched source file (164 of 695 `*.rs` commits, 23.6%). It caused
  7.6% of conflicted merges.
- `api.rs` grew from 54,425 to 64,419 lines. It caused 2.9% of conflicted
  merges, in 12 PRs over 80 days.
- The 2026-09-02 memo named `api.rs` a coordination risk because of its size
  and touch rate. The direct measure does not support a split for that
  reason. Record this so nobody proposes the split as a conflict fix.
- The registries that every feature appends to do conflict. A new row goes
  at the end of a table, a list or a `mod` block. Two branches add at the
  same point, so git cannot order them. Examples are the migration table in
  `docs/upgrading/0.5.0.md`, the proof table in `formal-methods.md` and the
  `mod` list in `formal/verus/src/lib.rs`.

`CLAUDE.md` bans two conflict-magnet shapes: a live count in prose and a
hand-written copy of another page. The `sqlite-feasibility.md` row shows
that the first ban worked. Its conflicts fell from 107 in October to 0 in
November. An append-only registry at a shared tail is a third shape, with
no rule yet.

## Do nothing for 12 months

Nothing breaks. No wire contract, schema or replay property depends on these
files. The compact `openapi.json` cost 54 manual resolutions in October and 14
in the first half of November. Each new proof, migration or agent section costs one
more conflict for every branch open at the same time. The cost scales with
parallel sessions, not with code size.

## Door class and reversal cost

Two-way. Each fix below changes no public API, no served bytes if done with
care, and no schema. Reversal is one revert, in hours. Under the Keystone
charter the implementing team decides these in a PR description. This memo
does not decide them.

## Options for the implementing team

For finding 1, in order of cost:

1. Stop committing the compact copy. Build it from `docs/openapi.json` at
   compile time or at first request. Constraint: `include_str!` of a path
   outside the crate breaks `cargo package`. Check that before choosing.
   `openapi_spec` compares bytes, so keep that comparison on the build output.
2. Keep the compact copy, but commit it with one line per path entry. The
   served bytes then change, so the byte comparison changes too.
3. Do nothing. A `.gitattributes` merge driver is not a fix here. A custom
   driver needs local git config, which a fresh agent session does not have.

For finding 2, the repository already has two remedies:

- `merge=union` in `.gitattributes`, as for
  `.github/ci/integration-suites.txt`. It suits a list whose order a guard
  checks.
- A fragment directory, as for `docs/changelog.d/`. It suits a table that a
  generator can build.

## Success metric

Conflicted merges on `autumn-harvest-plugin/openapi.json`: 71 in the window,
14 since 2026-11-01. A fix should take it to 0 within a month. Re-run the
census below on or before 2027-02-16 to check.

## Reproduce

Run from the repository root. The full fetch is about 1,800 refs.

```sh
git fetch --unshallow origin trunk-dev
git fetch origin '+refs/pull/*/head:refs/remotes/pr/*'
python3 -I census.py > conflicts.json
```

`census.py`:

```python
import json, subprocess

def git(*a):
    return subprocess.run(["git", *a], capture_output=True, text=True)

refs = git("for-each-ref", "--format=%(refname)", "refs/remotes/pr").stdout.split()
rows = []
for ref in refs:
    log = git("log", "--merges", "--format=%H %P %ad", "--date=short",
              f"origin/trunk-dev..{ref}").stdout
    for line in log.splitlines():
        h, p1, p2, *_, day = line.split()
        out = git("merge-tree", "--write-tree", "--name-only", "--no-messages", p1, p2)
        paths = out.stdout.splitlines()[1:] if out.returncode == 1 else []
        rows.append({"pr": int(ref.rsplit("/", 1)[1]), "merge": h,
                     "date": day, "conflicts": [p for p in paths if p]})
print(json.dumps(rows))
```

Filter `conflicts.json` by `date >= "2026-08-27"`, then count rows and
paths. To class a hunk as add-only, redo the merge with
`git -c merge.conflictStyle=diff3 merge-tree --write-tree`. A hunk is
add-only when both sides contain every base line.
