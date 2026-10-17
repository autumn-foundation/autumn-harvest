## Fix — codec failures and carried columns (issue #2065)

Follow-up to issue #1979. Three defects, one fix each.

- **Codec failures.** `HarvestError::Codec` is new. Codec encode and decode
  failures used to be `HarvestError::Config`. `HarvestError::is_codec_failure`
  is true for `Codec`, `UnknownCodecKey` and `UnknownPayloadCodec`. The outbox
  (`attempt_signal_delivery`) and inline delivery (`persist_external_signal_inline`)
  used to record `ExternalSignalFailed { reason_code: "target_terminal" }` for
  any non-database error on an `ExecutionId` target. A codec failure now leaves
  the request pending. A target that is terminal still returns `Config` and
  still fails the signal. The rotation sweep reports `Codec` as `codec_error`.
- **Continue-as-new.** The successor decodes the predecessor `memo` and encodes
  it under the active key. A clear legacy memo no longer reaches the new row in
  clear.
- **Reset.** The fork decodes the source `input` and `memo` and encodes them the
  same way. This is option (a) of the issue. With column encoding off, or when
  a registry cannot decode a value, the copy keeps the stored bytes.

No migration. `HarvestError` gains a variant, so a `match` that lists every
variant needs a new arm. Code that matched `Config` to catch a codec failure
must call `is_codec_failure()` now. An HTTP route answers a codec failure with
`503`, as it does for an unknown key, and no longer with `400`.
`upgrade_check` reports a codec failure as an undecodable run.

Tests: `payload_codec::tests` (classification and `recode_column_for_copy`) and
`codec_column_coverage_tests` (outbox, inline delivery, continue-as-new, reset).
