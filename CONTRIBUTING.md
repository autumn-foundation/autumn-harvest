# Contributing

Thank you for your help. This file is a summary. [`AGENTS.md`](AGENTS.md) and
[`CLAUDE.md`](CLAUDE.md) contain the rules for each change. The rules apply to
human contributors and to agents. Read them before you start.

## Before you start

- Find or open an issue for the change. Cite it as `#<issue>` in the pull
  request.
- Do not report a vulnerability in an issue. Follow
  [`SECURITY.md`](SECURITY.md).
- Read [`docs/architecture.md`](docs/architecture.md) for the workspace layout
  and the design decisions.

## Branches and pull requests

- Use `trunk-dev` as the base branch. Do not use `trunk`. It is the release
  branch.
- Open the pull request as ready for review, unless you need a draft.
- Add one changelog fragment to `docs/changelog.d/`. Do not edit
  `CHANGELOG.md` or `docs/shipped-work.md`. See
  [`docs/changelog.d/README.md`](docs/changelog.d/README.md).

## Checks to run before you push

```sh
cargo fmt --all -- --check
cargo clippy -p <crate> --all-targets -- -D warnings
cargo test -p <crate>
python3 docs/audits/comment-hygiene.py --base origin/trunk-dev
```

CI also runs each `scripts/check-*.sh` script. It runs each audit that
[`docs/audits/README.md`](docs/audits/README.md) marks as wired into CI. Run the
ones that apply to your change.

## Rules for code and docs

- Write comments and docs in ASD-STE100 style: one idea per sentence, 25 words
  or fewer, active voice, present tense, no contractions, no first person.
- Do not delete the reason for a comment to make it shorter. Split a long
  sentence into several short sentences.
- Do not change a stored `harvest_events` row. `CLAUDE.md` names the two
  sanctioned exceptions.
- Start each new migration directory name with a second-precision UTC
  timestamp (`date -u +%Y%m%d%H%M%S`). Bound each lock on a hot table. Read
  [`docs/upgrading/online-migrations.md`](docs/upgrading/online-migrations.md).

## License

The project is dual-licensed under [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
