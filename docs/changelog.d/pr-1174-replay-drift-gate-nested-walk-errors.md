## Phase — replay-drift gate propagates nested fixture-walk errors (issue #1174)

Filed from a Codex review on #1171 (issue #798): `testing.rs::collect_json_files`
propagated a failure to read the **top-level** bundle directory, but swallowed
every error found while walking **nested** directories. Three swallow points —
`read_dir` on a subdirectory, `next_entry()` mid-iteration, and `file_type()`
on one entry — each let the walker return `Ok(files)` with a short list
instead of failing. The gate then replayed the survivors clean and exited
`0`, certifying a promotion against coverage it never actually checked.

Exposure is the manifest-less, hand-built bundle (#798): a manifest-bearing
bundle's fixture-count cross-check would normally catch a vanished
subdirectory, but a hand-built bundle has no manifest to reconcile against.

Fix: all three call sites now propagate with `?` instead of `if let Ok(...)`,
so any read failure at any depth surfaces as the same harness error (exit
`2`) the top-level probe already produced. The now-redundant top-level probe
was removed — the walk loop's first iteration performs the same check.

Regression test: `unreadable_nested_subdirectory_is_a_harness_error_not_a_silent_pass`
in `autumn-harvest/tests/integration/replay_drift_tests.rs` (`#[cfg(unix)]`,
per the issue's own test sketch — CI matrix includes `windows-latest`).
`chmod 000` on a nested subdirectory inside a manifest-less bundle that also
holds a readable top-level fixture. RED pre-fix: the report was clean (the
locked fixture silently vanished). GREEN post-fix: `exit_code() == 2`.
Verified against a real permission denial via a mapped non-root Linux user
namespace, since the sandbox this fix was authored in runs as `root` (which
ignores directory permission bits) — the test itself skips under `root` so
it never runs as a false pass on such a host; this repository's CI uses
hosted `ubuntu-latest` runners, which are not root.
