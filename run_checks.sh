cargo fmt --all
cargo clippy -p autumn-harvest --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D missing_docs" cargo doc --no-deps -p autumn-harvest
