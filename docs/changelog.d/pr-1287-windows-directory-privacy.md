## Phase — Directory-privacy enforcement no longer skips Windows (issue #1287)

Both directory-privacy checks in the dev runtime lived inside a
`#[cfg(unix)]` block: `harden_root` (`reaper.rs`, guards the session root a
planted record could point `stop_orphan`'s `pg_ctl` at) and
`directory_is_private` (`acquire.rs`, guards the managed-tier binary cache).
On Windows both functions fell through to `Ok(())` / `true` for any
non-symlink directory, so a shared session root or cache directory was
trusted with no check at all — on a machine where `--session-root`,
`HARVEST_DEV_CACHE_DIR`, or a machine-wide `TEMP` (the case for services and
some CI agents) actually points somewhere shared.

**Fix:** the two sites now share one helper, `reaper::directory_is_ours`, so
they cannot drift apart again the way the duplicated `cfg(unix)` gap let
them. On Unix it is the same uid comparison as before — unchanged. On
Windows there is no ACL check yet (issue #1287 tracks that as a follow-up),
so the fallback is a location heuristic: the directory must resolve under a
per-user root (`%LOCALAPPDATA%` or `%USERPROFILE%`). Every default this
crate ever picks on its own (`std::env::temp_dir()`, the managed-cache root)
already resolves under one of those two roots, so the heuristic only ever
refuses an explicitly configured shared location — exactly the case that
previously had no guard. Both sides are canonicalised before comparison, so
a case difference or a `\\?\` prefix does not produce a false refusal.

The heuristic proves *where* a directory is, not *who else* can write to it,
so it is weaker than the Unix check — the doc comment and the caller-facing
error text both say so rather than claiming ownership was verified.

Out of scope, and left to its own tracking issue (#1295): `process_start_token`
still returns `None` on Windows, so the pid-identity recheck in
`owner_is_the_recorded_one` / `postmaster_is_the_recorded_one` still falls
back to plain liveness there. That is a separate gap in a different check.

No new `WorkflowEvent` variant, no migration, no schema change — this is a
dev-only (`dev-runtime` / `dev-runtime-managed` feature) filesystem check.

**Tests, red → green:** inline unit tests in `reaper.rs` and `acquire.rs`.
`#[cfg(unix)]` tests pin the unchanged Unix behaviour (an owner-owned
directory is still trusted; a group-writable one is still rejected).
`#[cfg(windows)]` tests — which run for real on this repo's `windows-latest`
CI leg — pin the new Windows behaviour: a directory under `%LOCALAPPDATA%`
is trusted, and `C:\Windows\Temp` (the shared, machine-wide location the
issue names as the realistic exposure) is refused. The full workspace,
including this crate's `dev-runtime-managed` feature, was cross-compile
checked against `x86_64-pc-windows-gnu` to catch anything that would not
even build on that target.
