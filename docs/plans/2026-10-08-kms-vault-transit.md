# Plan — a non-AWS KMS key provider (issue #1981)

Status: implementation plan (TDD: red, green, refactor).

## 1. Brainstorming — candidate approaches

1. **GCP Cloud KMS through the official Rust SDK.** Rejected for now. It adds a
   large new crate tree and a Google auth stack to the audit.
2. **GCP Cloud KMS through its REST API.** Rejected for now. The API is simple, but
   OAuth token refresh is not. A half-done auth story is worse than none.
3. **Azure Key Vault `unwrapKey`.** Rejected. RSA-OAEP unwrap has no associated
   data, so the binding to the codec key id is lost.
4. **HashiCorp Vault Transit through its HTTP API.** Chosen. It runs on every cloud
   and on premises. The plugin already depends on `reqwest`, so the feature adds no
   new crate. `transit/datakey/wrapped` makes a wrapped key in one call.
5. **A generic HTTP provider that the operator configures.** Rejected. Each KMS has
   its own context rules. A generic shape hides them.
6. **Change `KmsDecrypt` to carry provider options.** Rejected. The issue asks to keep
   the trait unchanged. The current trait carries all that Vault needs.
7. **A shared conformance suite for every binding.** Chosen. One generic suite runs
   against a fake server for each provider. The AWS binding moves onto it.

Selected: **4 + 7**, with the trait unchanged.

## 2. Reverse brainstorming — how to make this fail

- **R1. Vault ignores the context.** Vault uses `context` only for a derived key. A
  plain key unwraps under any context, so the key-id binding fails open.
  Foreclosed: the binding reads the key and refuses a key with `derived` false.
- **R2. Two context maps encode to the same bytes.** Foreclosed: the context is
  canonical JSON of a sorted map. JSON escapes every separator.
- **R3. A key name reaches another Vault path.** `a/../b` or `%2F` could leave the
  `decrypt` path. Foreclosed: the binding accepts the Vault key-name alphabet only,
  `\w`, `-` and `.`, with a word character at each end.
- **R4. The token leaks.** Foreclosed: `Debug` redacts it. The header value is
  marked sensitive. Error text holds the status and Vault's `errors` only.
- **R5. Key material in error text.** Foreclosed: an unwrapped key of the wrong
  length fails as `InvalidKey` with its length only. A suite test checks it.
- **R6. Startup hangs on a dead Vault.** Foreclosed: each request has a timeout.
- **R7. A wrapped key in the wrong format.** Foreclosed: a non-UTF-8 wrapped key
  fails before any request. Vault rejects a malformed ciphertext.
- **R8. The AWS and Vault tests drift apart.** Foreclosed: one suite, stamped into
  both modules by one macro.
- **R9. A redirect sends the token to another host.** Foreclosed: the default
  client follows no redirect. A reply from another origin fails.
- **R10. The token and the data key cross the network in clear.** Foreclosed: the
  address must use `https`, except for a loopback host or an explicit opt-in.

## 3. Six hats

- **White (facts).** `KmsDecrypt` takes a key id, wrapped bytes and a context map.
  Vault Transit `decrypt` takes a `vault:v1:` string and a base64 `context`. The
  plugin has `reqwest` with rustls. `base64` 0.22 is in the lockfile.
- **Red (instinct).** Operators ask for Vault first when they leave AWS. A REST
  call is easier to audit than a new SDK.
- **Black (risks).** R1 is the sharp edge. The key read needs one more Vault policy
  line. Vault Enterprise needs a namespace header.
- **Yellow (upside).** One binding serves GCP, Azure and on-premises users who run
  Vault. No new crate. The suite makes the next provider cheap.
- **Green (creativity).** Read each decrypt call back into the generic triple: KMS
  key id, wrapped bytes and context. Then one suite asserts one contract.
- **Blue (process).** Red tests first, then green, then refactor. Then a
  multi-angle review, then an acceptance-criteria audit.

## 4. Design

### 4.1 Wire contract

| Step | Request |
|------|---------|
| Key check | `GET {address}/v1/{mount}/keys/{name}`. Refuse unless `data.derived` is true. |
| Unwrap | `POST {address}/v1/{mount}/decrypt/{name}` with `ciphertext` and `context`. |

`context` is base64 of the canonical JSON context, for example
`{"harvest_codec_key_id":"2026-10"}`. Each request sends `X-Vault-Token`, and
`X-Vault-Namespace` when set.

### 4.2 Types (`autumn_harvest_plugin::vault_transit`, feature `vault-transit`)

- `VaultTransit::new(address, token)`, then `with_mount`, `with_namespace` and
  `with_client`. It implements `KmsDecrypt`.
- `reqwest` is re-exported for `with_client`, for example to add a private CA.

### 4.3 Tests mapped to acceptance criteria

| AC | Evidence |
|----|----------|
| A non-AWS provider behind a feature | `vault-transit` feature, `vault_transit::VaultTransit` |
| The same suite as AWS | `kms_conformance` suite, stamped into `aws_kms` and `vault_transit` |
| Docs list the providers | `docs/security-posture.md`, "Key providers" |
