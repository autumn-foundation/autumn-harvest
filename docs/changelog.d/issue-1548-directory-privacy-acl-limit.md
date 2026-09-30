## Fix — directory-privacy checks and filesystem ACLs (issue #1548)

The dev runtime checks POSIX mode bits to decide that no other user can write
to the cache root, its ancestors, and the session root. The issue asked
whether an ACL can hide a write grant from that check.

On Linux it cannot. A POSIX ACL grant raises the ACL mask, and the mask shows
as the group bits of the mode. A later `chmod` lowers the mask and the grant
with it. `harden_root` sets `0700`, which removes all effective grants.

The check is not sound for ACLs that `stat` cannot show: macOS and BSD native
ACLs, and NFSv4 ACLs. This is a deliberate, documented non-fix for those
platforms. A fix needs a per-platform ACL API and a new dependency, and this
project cannot test macOS or BSD ACLs in CI.

The write-bit test now lives in one function, `reaper::others_can_write`. Both
checks in `acquire.rs` call it, so the sites cannot drift. The limit is
recorded in the docs of `directory_is_private` and `others_can_write`.

No migration, no route change, and no `harvest_events` change.

Tests: unit tests in `reaper.rs` and `acquire.rs` use `setfacl` and skip when
it is missing. They cover a granted leaf, a granted ancestor, a masked grant,
`harden_root`, and a plain `0755` directory.
