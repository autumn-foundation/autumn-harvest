## Chore — land the Dependabot bumps that needed code: Rust 1.99.1, syn 3, aes 0.9 with aes-gcm 0.11

Four Dependabot PRs could not pass CI alone. This change lands them together
with the code each one needs.

- **Rust 1.99.1** (#2233). The worker image moved to `rust:1.99.1-bookworm`.
  `docs/audits/worker-image-contract.py` requires the build image, the
  Dockerfile's `RUSTUP_TOOLCHAIN` and `rust-toolchain.toml` to name one
  release. The pin, the env var and the docs that quote it now say 1.99.1.
  `harvest-verify` matches validated `rustc` releases on major and minor, so
  its validated set does not change.
- **syn 3** (#2238). syn 3 stores raw pointer mutability as
  `PointerMutability` and drops the negative-impl slot from
  `ItemImpl::trait_`. `harvest-verify` now matches `PointerMutability::Mut`
  and reads the trait path from the two-field tuple. The existing
  `*mut`/`*const` field test covers the pointer rule.
- **aes 0.9.2 and aes-gcm 0.11.1** (#2088, #2239). They must move together.
  With only one bumped, the `zeroize` features on `aes`, `ghash` and
  `polyval` apply to copies that `aes-gcm` does not use. `cargo tree` now
  shows `aes-gcm 0.11.1` on `aes 0.9.2` and `ghash 0.6.0`, with `zeroize`
  on. The codec and the owner disk cache move from the deprecated
  `AeadInPlace` to `AeadInOut`. The key is passed as a reference to its
  array, so no unzeroized copy is made. A nonce or tag of the wrong length
  is an error, never a panic.

The stored format does not change: a nonce, the ciphertext and a 16-byte
tag, as before. The existing codec round-trip, tamper and wrong-key tests
pass unchanged.

No migration. No `WorkflowEvent` change.
