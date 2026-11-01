## Phase X.Y — Schema contract diff borrows canonical schemas

`diff_schema_contracts` no longer copies a schema that is already canonical.
`canonicalize_node` returns `None` when the canonical form equals its input,
and `diff_role` borrows the input then. Output is unchanged. On 200 workflows
and 400 activities the diff takes 13.5% fewer instructions and allocates 12.9%
fewer bytes. See `docs/performance-schema-contract-diff.md`.
