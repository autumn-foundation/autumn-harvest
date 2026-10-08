## License texts, security policy and contribution guide (issue #1990)

**What shipped.** The repository root holds `LICENSE-MIT`, `LICENSE-APACHE`,
`SECURITY.md` and `CONTRIBUTING.md`. `LICENSE-APACHE` is the canonical
Apache 2.0 text. `SECURITY.md` sends reports to GitHub private vulnerability
reporting. `CONTRIBUTING.md` points at the rules in `AGENTS.md` and
`CLAUDE.md`. The README License section links all four files.

**Published crates.** Cargo does not copy a root license file into a crate.
Each publishable crate directory links both files with a symlink. Cargo
follows the link, so the `.crate` archive holds the full text as a regular
file. The six crates are `autumn-harvest`, `autumn-harvest-plugin`,
`autumn-harvest-macros`, `autumn-harvest-cli`, `autumn-harvest-redis` and
`autumn-harvest-sqlite`.

**Gate.** `scripts/check-license-files.sh` runs in the CI `lint` job. It
reads the publishable packages from `cargo metadata`, so a new crate is
checked with no edit. For each package, it checks the declared license,
runs `cargo package --list`, and compares each license file with the root
bytes. It fails closed when `cargo metadata` fails.

**Test evidence.** The gate failed before the files existed. It reported
four missing root files and twelve missing crate files. It passes now. A
packaged `autumn-harvest-macros` archive holds `LICENSE-APACHE` with the
canonical SHA-256 `cfc7749b…3d30`. No engine code, migration or
`WorkflowEvent` variant changes.
