# Plan — Production AEAD payload codec with a KMS key provider (issue #1825)

Status: implementation plan (TDD: red, green, refactor).

## 1. Brainstorming — candidate approaches

1. **AES-256-GCM from the RustCrypto `aes-gcm` crate.** The lockfile already holds
   `aes-gcm` 0.10.3, so the build gets no new crypto crate. Chosen.
2. **XChaCha20-Poly1305.** A 192-bit nonce removes the random-nonce bound. Rejected for
   now: it adds a new crate, and key rotation already bounds messages per key.
3. **Key id only in the outer harvest envelope (`kid`).** Rejected as the only binding.
   A codec installed with `set_default` writes no `kid`. Also, the outer `kid` is not
   authenticated.
4. **Key id and format version in an authenticated codec header.** Chosen. The header
   is the AEAD associated data, so a changed header fails decode.
5. **Synchronous key lookup per encode.** Rejected. A KMS call per payload adds a
   network round trip to the hot write path.
6. **Envelope encryption: load the data key once, at startup.** Chosen. The KMS
   provider unwraps a wrapped data key once. The codec then holds the key in memory.
7. **A KMS provider hard-wired to the AWS SDK.** Rejected. It cannot be tested without
   AWS. A small `KmsDecrypt` trait carries the one call. The AWS binding sits behind
   the `aws-kms` feature.
8. **A new rotation path for the AEAD codec.** Rejected. One `AeadCodec` per key id goes
   into the existing keyed registry. The issue #948 sweep then re-encrypts with no
   change.

Selected: **1 + 4 + 6 + 7 + 8.**

## 2. Reverse brainstorming — how do we make this fail?

- **R1. Nonce reuse.** A repeated nonce under one key leaks the XOR of two plaintexts
  and the GHASH key. Foreclosed: every encode reads 96 bits from the OS RNG. A test
  proves two encodes of one plaintext differ. Docs state the 2^32 messages-per-key
  bound and tell operators to rotate first.
- **R2. Silent wrong-key decode.** Foreclosed: GCM authenticates. A wrong key fails
  with a typed error. A test proves it.
- **R3. Header swap.** An attacker relabels a ciphertext with another key id or
  version. Foreclosed: the header is associated data. A test flips every byte.
- **R4. Key material in logs.** Foreclosed: `DataKey` and `AeadCodec` print a redacted
  `Debug`. No error text carries key bytes or plaintext. Tests assert both.
- **R5. Key bytes stay in freed memory.** Foreclosed: `DataKey` uses `Zeroizing`, and
  the cipher state uses the `aes-gcm` `zeroize` feature.
- **R6. Path traversal through a key id.** A key id of `..` must not leave the key
  directory. Foreclosed: the key id alphabet has no `/`, and the file name has a
  `.key` suffix. A test proves `..` stays inside the directory.
- **R7. Registry id differs from header id.** Foreclosed: `AeadCodec::register_with`
  registers the codec under its own key id.
- **R8. KMS unwraps a data key for the wrong codec key.** Foreclosed: the KMS call
  sends an encryption context that names the codec key id.
- **R9. Rotation leaves old ciphertext behind.** Foreclosed by the issue #948 sweep.
  The replay-fidelity test runs with this codec.

## 3. Six hats

- **White (facts).** `PayloadCodec` is a sync byte transform. The keyed registry and
  the sweep exist (issue #948). `aes-gcm` 0.10.3 and `zeroize` are in the lockfile.
  The plugin already uses the AWS SDK with `default-https-client`.
- **Red (instinct).** Home-made crypto is a red flag. Use the vetted crate. Write no
  primitive. Keep the format small and versioned.
- **Black (risks).** Random 96-bit nonces bound each key to about 2^32 encodes. A new
  AWS crate widens the dependency audit, so it stays optional.
- **Yellow (upside).** Payloads get encryption at rest with one builder call. Rotation
  gets a real codec to rotate. The AEAD tag adds tamper detection.
- **Green (creativity).** Bind the header as associated data. Hide KMS behind a
  one-method trait, so a fake proves the provider contract offline.
- **Blue (process).** Red tests first, then green, then refactor. Then a multi-angle
  review, then an acceptance-criteria audit.

## 4. Design

### 4.1 Wire format (inside the codec `data` field)

```
[0]          format version (1)
[1]          key id length n (1..=64)
[2..2+n]     key id (ASCII, same alphabet as the registry)
[2+n..14+n]  96-bit random nonce
[14+n..]     ciphertext followed by the 128-bit tag
```

The associated data is bytes `[0..2+n]`.

### 4.2 Types (`autumn_harvest::aead_codec`)

- `AeadCodec` — `codec_id` is `aes-256-gcm`. `new`, `load`, `key_id`, `register_with`.
- `DataKey` — 32 bytes in `Zeroizing`. `from_bytes`, `from_base64`, `generate`.
- `KeyProvider` — async `data_key(key_id)`.
- `EnvKeyProvider` — key id to environment variable, base64 value.
- `FileKeyProvider` — `<dir>/<key_id>.key`, base64 content.
- `KmsDecrypt` and `KmsKeyProvider<D>` — unwraps a wrapped data key once.
- `aws-kms` feature — `impl KmsDecrypt for aws_sdk_kms::Client`.
- `HarvestBuilder::aead_payload_codec_key(codec)`.

### 4.3 Tests mapped to acceptance criteria

| AC | Test |
|----|------|
| Round trip, tamper, wrong key | `aead_codec::tests` |
| Replay fidelity | `replay_fidelity_is_byte_identical_across_a_sweep` runs XOR and AES-256-GCM |
| Property | `tests/property/aead_codec_props.rs` |
| Docs | `docs/security-posture.md` |
