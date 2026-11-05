## Awaitables history index uses foldhash (performance)

`project_awaitables` hashes UUID-newtype keys in its history index. It now uses
`foldhash::fast::RandomState` instead of `SipHash`. The `awaitables_profile`
harness drops from 2,727,202,632 to about 2,058M instructions (-24.5%).
Allocation counts do not change. `foldhash` is now a non-optional dependency of
`autumn-harvest`. It was already optional under the `metrics-rs` feature. No
output changes. See `docs/performance-awaitables-hasher.md`.
