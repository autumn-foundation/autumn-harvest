1. **Identify Weak Points:**
   - Found string slicing that doesn't respect character boundaries in `autumn-harvest/src/eligibility.rs` (`parse_exact` and `parse_in`).
   - Slicing a non-ASCII character like an emoji (`🦀`) with `&token[..idx]` causes a byte-index panic.
   - Identified the weak point and wrote a failing proptest in `eligibility.rs`.

2. **Attack & Detonate:**
   - I have written a test `test_havoc_panic` in `autumn-harvest/src/eligibility.rs` using an emoji input (`🦀=value`).
   - Ran the test to confirm it crashes with `byte index 1 is not a char boundary`. (SUCCESS).

3. **Complete pre-commit steps**
   - Complete pre-commit steps to ensure proper testing, verification, review, and reflection are done.

4. **Present The Wreckage:**
   - Create a Pull Request formatted as Havoc.
