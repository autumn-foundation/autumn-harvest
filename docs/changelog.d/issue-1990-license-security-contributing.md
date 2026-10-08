## Docs — License texts, security policy and contribution guide (issue #1990)

**What shipped.** The repository root holds `LICENSE-MIT`, `LICENSE-APACHE`,
`SECURITY.md` and `CONTRIBUTING.md`. `LICENSE-APACHE` is the canonical
Apache-2.0 text. `SECURITY.md` sends reports to GitHub private vulnerability
reporting. It names the fail-closed gate and the scoped token checks as in
scope. `CONTRIBUTING.md` points at the rules in `AGENTS.md` and `CLAUDE.md`.
The README License section links all four files.

**Publishable crates.** Cargo does not copy a root license file into a crate.
Each publishable crate directory holds a symlink to each root license file.
Cargo follows the link, so the `.crate` archive holds the full text as a
regular file. The six crates are `autumn-harvest`, `autumn-harvest-plugin`,
`autumn-harvest-macros`, `autumn-harvest-cli`, `autumn-harvest-redis` and
`autumn-harvest-sqlite`.

**TypeScript client.** npm does not pack a symlink. It packs a file named
`LICENSE-MIT` only when `files` lists it. So `clients/typescript` holds copies
of both files, and `package.json` lists them. `build-typescript-client.sh`
fails when the tarball does not hold them.

**CLI release archive.** The `binaries` job in `release.yml` copies both root
license files into each `harvest` archive. The integration test
`supply_chain_ci::release_archive_ships_both_license_texts` pins that step.

**Gate.** `scripts/check-license-files.sh` runs in the CI `lint` job. It reads
the publishable crates from `cargo metadata`, so the gate checks a new crate
with no edit to the script. For each crate, it checks the declared license,
runs `cargo package --list`, and compares each license file with the root
file, byte for byte. It also compares the client copies. It fails closed when
`cargo metadata` or its parser fails, or when it finds no publishable crate.

**Test evidence.** The gate failed before the files existed. It reported
four missing root files and twelve missing crate files. It passes now. A
packaged `autumn-harvest-macros` archive holds `LICENSE-APACHE` with the
canonical SHA-256 `cfc7749b…3d30`. The change adds no engine code, migration
or `WorkflowEvent` variant.
