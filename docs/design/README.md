# Design records

A design record is the planning document for one issue or one feature. It
states the facts, the options, the decision and the acceptance criteria
before the code lands. Source comments, guides and guard tests cite these
files by path.

## Where a document goes

| Document | Home | Name |
|---|---|---|
| Design record for an issue | `docs/design/` | `DESIGN-<issue>.md` |
| Design record for a feature with no single issue | `docs/design/` | `DESIGN-<slug>.md` |
| Architecture decision | [`docs/adr/`](../adr/) | `NNNN-<slug>.md` |
| Implementation plan | [`docs/plans/`](../plans/) | `YYYY-MM-DD-<slug>.md` |
| R&D note, spike or preregistration | [`docs/rnd/`](../rnd/) | `YYYY-MM-DD-<slug>.md` |
| Changelog entry | [`docs/changelog.d/`](../changelog.d/README.md) | see its README |

Never write one of these at the repository root.
[`root-files.py`](../audits/root-files.py) fails the `lint` job on a root
file that is not in its allow list.

## Links

Cite a design record by its repository path, for example
`docs/design/DESIGN-1971.md`. A Markdown link from a page in `docs/` is
relative to that page. A link from a design record to another page in
`docs/` starts with `../`.
