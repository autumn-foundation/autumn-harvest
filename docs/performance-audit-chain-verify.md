# Audit chain verification: keyed HMAC reuse and streamed encoding

`ChainVerifier::push` checks each exported audit row by recomputing its link
with `link`. This note records the profile and the change.

## Workload

`benches/audit_chain_verify_profile.rs` builds `AUDIT_CHAIN_N` chained rows
with realistic field lengths. It then verifies them through
`ChainVerifier::push` and `finish`. The cost of the verifier alone is the
full run minus the run with `AUDIT_CHAIN_MODE=build`.

## Profile

Callgrind, 20,000 rows, build plus verify, inclusive:

| Entry | Share |
| --- | --- |
| `link` | 92.8% |
| `sha2` compress (portable path) | 79.2% |
| `canonical_record` | 9.2% |
| `to_rfc3339_opts` | 4.3% |

Valgrind does not emulate SHA-NI, so SHA-256 looks larger here than on real
hardware. A saving outside SHA-256 is therefore understated by this harness.

## Change

- `link` called `Hmac::new_from_slice` on every row. That call runs two
  key-block compressions each time. The verifier now sets the key up once
  and clones the keyed state per row.
- `link` built `canonical_record` into a `Vec` and ten `String` values per
  row. It now feeds the same bytes to the MAC in pieces and formats the
  integer fields in a stack buffer.

The public `link` and `canonical_record` keep their signatures and output.
`the_encoding_and_the_link_match_a_fixed_vector` pins the bytes.

## Measurement

Verify only, 5,000 rows (`AUDIT_CHAIN_N=5000`):

| Counter | Before | After | Delta |
| --- | --- | --- | --- |
| Instructions (callgrind) | 176,842,532 | 139,213,846 | -21.3% |
| Allocation blocks (dhat) | 44,999 | 10,000 | -77.8% |
| Allocated bytes (dhat) | 4,754,961 | 380,105 | -92.0% |

## Reproduce

```sh
cargo bench -p autumn-harvest --no-default-features \
  --bench audit_chain_verify_profile --no-run
for mode in build full; do
  AUDIT_CHAIN_N=5000 AUDIT_CHAIN_MODE=$mode valgrind --tool=callgrind <binary>
  AUDIT_CHAIN_N=5000 AUDIT_CHAIN_MODE=$mode valgrind --tool=dhat <binary>
done
```
