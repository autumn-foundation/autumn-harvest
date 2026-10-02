## Chore — pin the workspace toolchain to Rust 1.99.0

`rust-toolchain.toml` pins every build, format and lint run to Rust 1.99.0
with the `rustfmt` and `clippy` components. CI's `dtolnay/rust-toolchain@stable`
steps run `rustup default`, and rustup gives the file precedence over that
default, so all stable jobs now compile with the pinned release. A new stable
release moves CI only when the pin changes, so new clippy lints arrive as one
reviewed PR instead of breaking every open branch at once, as Rust 1.99 did on
2026-10-01.

The MSRV job sets `RUSTUP_TOOLCHAIN=1.88.0`, which outranks the file, so it
still compiles with the MSRV. The fuzz job already calls `cargo +nightly`. The
MSRV itself stays `rust-version = "1.88.0"` in `Cargo.toml`.

No code change. No migration. No `WorkflowEvent` change.
