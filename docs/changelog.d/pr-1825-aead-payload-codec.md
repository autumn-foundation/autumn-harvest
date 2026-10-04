## Phase 3.x — production AES-256-GCM payload codec with a KMS key provider (issue #1825)

Harvest had a payload codec hook and key rotation, but only `IdentityCodec`.
Payloads were plaintext by default, and rotation had no production codec to
rotate. This adds one.

- **`aead_codec::AeadCodec`.** AES-256-GCM from the RustCrypto `aes-gcm`
  crate, already in the lockfile. `codec_id` is `aes-256-gcm`. Each encode
  reads a fresh 96-bit nonce from the OS RNG. Each payload starts with a
  header: format version 1, then the key id. The header is AEAD associated
  data, so a changed version, key id, nonce or ciphertext fails decode.
- **Key providers.** The async `KeyProvider` trait loads a `DataKey` once, at
  startup. `EnvKeyProvider` reads base64 from a mapped environment variable.
  `FileKeyProvider` reads `<dir>/<key_id>.key`. `KmsKeyProvider` unwraps a
  wrapped data key through the one-method `KmsDecrypt` trait, with the codec
  key id as the encryption context. The new `aws-kms` feature implements
  `KmsDecrypt` for `aws_sdk_kms::Client`. It is off by default.
- **Key hygiene.** `DataKey` is zeroized on drop. The cipher uses the
  `aes-gcm` `zeroize` feature. `Debug` output and errors never print key
  bytes or plaintext.
- **Rotation.** `AeadCodec::register_with` and
  `HarvestBuilder::aead_payload_codec_key` register a codec under its own key
  id. The issue #948 sweep then re-encrypts under the new key version with no
  change.
- **Scope.** The codec covers the `harvest_events` payload fields only.
  ADR-0003 keeps denormalized columns, such as
  `harvest_workflow_executions.input`, in clear. `docs/security-posture.md`
  lists them.

No new `WorkflowEvent` variant. No migration. The event JSON contract is
unchanged.

**Tests.** `aead_codec::tests` covers round trip, a flipped byte at every
index, truncation, the wrong key, a different key id, an unknown version,
each provider, redacted `Debug` output and a rotation re-encrypt.
`tests/property/aead_codec_props.rs` proves `decode(encode(x)) == x` for
arbitrary bytes and arbitrary JSON, and that any flipped bit fails decode.
`replay_fidelity_is_byte_identical_across_a_sweep` now runs with the XOR
fixture codec and with `AeadCodec`.
