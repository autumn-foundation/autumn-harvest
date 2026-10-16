## Phase 2141 — Exact number literals in `filter` (issue #2141)

A number in `attrs.<key> =`, `!=` or `IN` kept only `i64`, `u64` or `f64`
precision. So `12345678901234567890123` and `...124` bound the same value.

The filter now binds the number as `jsonb` text, with every digit. The text
loses its leading zeros, a bare `.5` and a trailing `.`, so it stays valid JSON.
A number outside the `f64` range still gets `400`. The legacy
`search_attr_filter` parameter uses the same conversion.

Tests: `number_literals_keep_every_digit_when_bound`,
`number_literals_bind_as_valid_json` and
`coerce_scalar_keeps_every_digit_of_a_number`.
