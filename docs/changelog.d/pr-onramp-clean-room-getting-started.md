## Onramp — clean-room harness for the getting-started path

`scripts/clean-room-getting-started.sh` starts from an empty directory and
pastes the Chapter 1 `Cargo.toml` and `src/main.rs` plus the Chapter 2
workflow, activity and registration blocks. It runs `cargo check` and logs each
step's exit code and seconds to a TSV. `--checkout` patches the crates to the
working tree, so a docs or API change is tested before release.

Baseline on 2026-10-12: 2 steps, 0 failures. Cold `cargo check` took 235 s
against crates.io 0.7 and 646 s against the checkout. The Postgres steps need
Docker and are not covered. No CI wiring yet, because of the cold build time.
