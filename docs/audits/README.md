# docs/audits/

Deterministic, reproducible checks over the docs corpus and adjacent
generated surfaces — no server, browser, or network access required. Each
script is runnable standalone (`python3 docs/audits/<script>.py`) and safe
to wire into CI as a gate.

| Script | Checks | Wired into CI |
|---|---|---|
| `corpus-link-check.py` | Internal markdown links (missing file, missing anchor) and orphan pages across `docs/**/*.md` | Yes — `.github/workflows/ci.yml`, `lint` job |
| `vantage-dashboard-contrast.py` | WCAG 1.4.3 contrast on the Vantage dashboard's inline stylesheet | No — run manually after touching `autumn-harvest-plugin/src/ui.rs`'s `STYLE` constant |

Add a new audit here when a docs (or docs-adjacent UI/generated-artifact)
defect class is mechanical to detect — the point is to make a defect class
un-reintroducible, not to write it up once and move on. Wire it into CI's
`lint` job (ungated by the docs-only-changes filter, so it runs on every
PR including docs-only ones) once it's stable.
