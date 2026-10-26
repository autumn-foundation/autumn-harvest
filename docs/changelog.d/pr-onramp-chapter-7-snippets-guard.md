## Onramp — chapter 7 reliability-knob snippets compile in CI

`scripts/check-reliability-knobs-examples.sh` extracts the 11 Rust blocks of
`docs/getting-started/07-reliability-knobs.md`, wraps them with the imports and
type stubs the chapter implies, and compiles them against the real crate. CI
runs it. A renamed builder method or attribute key in the chapter now fails the
build.

Baseline: 11 blocks, 0 errors in the chapter, 25.7 min for the first cold build
and 3 s afterwards. Docs and CI only. No public API changes.
