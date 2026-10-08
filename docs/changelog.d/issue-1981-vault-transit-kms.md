## Phase 3.x — HashiCorp Vault Transit KMS binding (issue #1981)

AWS KMS was the only KMS binding for the AEAD codec. This adds HashiCorp Vault
Transit, which runs on any cloud and on premises.

- **`vault_transit::VaultTransit`.** The new `autumn-harvest-plugin`
  `vault-transit` feature adds it. It implements the core `KmsDecrypt` trait
  over the Vault HTTP API. The trait is unchanged. The binding uses the
  `reqwest` client that the plugin already has, so the lockfile gets no new
  crate. It is off by default.
- **Context binding.** The binding sends the context as the Transit key
  derivation context: base64 of compact, sorted JSON. Vault ignores the
  context for a key without derivation. So the binding reads the key first
  and refuses a key with `derived` false.
- **Safety.** A key name outside the Vault alphabet, a bad mount, a bad
  address or a non-UTF-8 wrapped key fails before any request. The address
  must use `https`, except for a loopback host or after `allow_plain_http`.
  The default client follows no redirect, and it sends plain `http` direct,
  never through a proxy. A reply over 64 KiB fails. `Debug`
  never shows the token. Error text never holds key material. Each request
  times out after 30 seconds.
- **Shared suite.** The new `kms_conformance` test module holds one suite for
  every KMS binding. A macro stamps it into `aws_kms` and `vault_transit`. Each
  binding runs against a local fake server. The AWS binding moved onto it.
- **AWS error text fix.** The shared suite found a leak in `AwsKms`. Its
  error text used `DisplayErrorContext`, which also prints the raw HTTP
  response. A malformed decrypt reply then put the response body, which can
  hold the plaintext data key, into the error. The binding now joins the
  `Display` text of the error chain only.
- **Docs.** `docs/security-posture.md` lists the supported KMS bindings and
  shows the Vault setup and policy.

No new `WorkflowEvent` variant. No migration.

**Tests.** `cargo test -p autumn-harvest-plugin --features vault-transit --lib
vault_transit` runs the shared suite and the Vault cases. They check the exact
paths, headers and context bytes. They refuse a non-derived key, a redirect, an
oversized reply, a plain `http` address that is not loopback, unsafe names and
mounts, a non-UTF-8 wrapped key and a non-base64 plaintext. `Debug` redacts
the token.
