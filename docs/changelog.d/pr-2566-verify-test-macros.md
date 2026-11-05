## Phase X.Y — `harvest-verify`: test macros no longer make types writable (issue #2545)

A `proptest!` body that named `String` made `String` opaque, and so writable, for
the whole analysis. Workflows that capture a `String` then read as `unknown`.

- **Fix.** `proptest`, `prop_assert`, `prop_assert_eq`, `prop_assert_ne`,
  `prop_assume`, `prop_compose` and `prop_oneof` join `TYPE_FREE_MACROS`.
  These macros declare tests and strategies, not types.
- **Not done.** The index still reads `#[cfg(test)]` code. It does not know
  the target kind, and a `--test` target compiles that code. A skip would be
  unsound there.
- **Tests.** `a_test_macro_makes_no_type_opaque` in `resolve/impls.rs`. The
  corpus case `wf_test_macro_names_type` has a `#[cfg(test)]` `proptest!` block
  that names `String`. Without the fix, it and `wf_saga_step` read `unknown`.
