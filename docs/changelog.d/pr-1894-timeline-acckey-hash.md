## Phase perf — timeline AccKey hashing (PR #1894)

`derive_timeline` hashes its `AccKey` lookup keys with a manual `Hash` impl
that writes one 128-bit value, instead of three `SipHash` writes. Equality and
the keyed `RandomState` hasher are unchanged. `timeline_profile` drops from
2,550,943,985 to 2,340,195,633 instructions (-8.3%, callgrind). No
`WorkflowEvent` or migration change. The 32 `timeline::tests` cases pass
unchanged. See `docs/performance-timeline-acckey-hash.md`.
