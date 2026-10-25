## Phase — performance index lists two orphan pages (Folio)

`docs/performance.md` now links `performance-analyzer-large-payload.md` and
`performance-debug-diff-redaction-probe.md`. No page linked them, so a reader
of the performance index could not reach either result. The orphan scan
(`docs/audits/corpus-link-check.py`) falls from 2 to 0.
