//! The `SplitMix64` mixers are bijections on `u64`.
//!
//! Three copies of the finalizer are in the engine: `policy::mix64`,
//! `dlq::mix64` and `chaos::splitmix64`. Each one maps every `u64` to a
//! distinct `u64`. This module proves that with an explicit inverse.
//!
//! The Kani jitter proofs stub `mix64` with any `u64`. A bijection makes the
//! stub lose no case. CBMC did not prove the bijection in 5 minutes. Verus
//! proves it in about one second.

use vstd::arithmetic::div_mod::{lemma_mod_multiples_vanish, lemma_mul_mod_noop_left, lemma_small_mod};
use vstd::prelude::*;

verus! {

/// The modulus of `u64` arithmetic, 2^64.
pub open spec fn modulus() -> int {
    0x1_0000_0000_0000_0000
}

/// `x.wrapping_mul(c)` as a spec.
pub open spec fn mul(x: u64, c: u64) -> u64 {
    ((x as int * c as int) % modulus()) as u64
}

/// One xor-shift step, `x ^ (x >> s)`.
pub open spec fn xs(x: u64, s: u64) -> u64 {
    x ^ (x >> s)
}

/// The finalizer that all three copies compute.
pub open spec fn spec_mix64(x: u64) -> u64 {
    xs(mul(xs(mul(xs(x, 30), 0xbf58_476d_1ce4_e5b9), 27), 0x94d0_49bb_1331_11eb), 31)
}

/// The inverse of [`spec_mix64`]. Each step undoes one step of the mixer.
pub open spec fn spec_unmix64(y: u64) -> u64 {
    let d = y ^ (y >> 31) ^ (y >> 62);
    let c = mul(d, 0x3196_42b2_d24d_8ec3);
    let b = c ^ (c >> 27) ^ (c >> 54);
    let a = mul(b, 0x96de_1b17_3f11_9089);
    a ^ (a >> 30) ^ (a >> 60)
}

/// `chaos::splitmix64`: add the golden ratio, then mix.
pub open spec fn spec_splitmix64(seed: u64) -> u64 {
    spec_mix64(((seed as int + 0x9E37_79B9_7F4A_7C15) % modulus()) as u64)
}

/// The inverse of [`spec_splitmix64`].
pub open spec fn spec_unsplitmix64(y: u64) -> u64 {
    ((spec_unmix64(y) as int - 0x9E37_79B9_7F4A_7C15 + modulus()) % modulus()) as u64
}

/// A shift of 30 or more bits is undone by two more shifts.
proof fn lemma_xs30(x: u64) by (bit_vector)
    ensures
        ({ let y = x ^ (x >> 30); y ^ (y >> 30) ^ (y >> 60) }) == x,
        ({ let y = x ^ (x >> 30) ^ (x >> 60); y ^ (y >> 30) }) == x,
{
}

/// A shift of 27 is undone by two more shifts.
proof fn lemma_xs27(x: u64) by (bit_vector)
    ensures
        ({ let y = x ^ (x >> 27); y ^ (y >> 27) ^ (y >> 54) }) == x,
        ({ let y = x ^ (x >> 27) ^ (x >> 54); y ^ (y >> 27) }) == x,
{
}

/// A shift of 31 is undone by two more shifts.
proof fn lemma_xs31(x: u64) by (bit_vector)
    ensures
        ({ let y = x ^ (x >> 31); y ^ (y >> 31) ^ (y >> 62) }) == x,
        ({ let y = x ^ (x >> 31) ^ (x >> 62); y ^ (y >> 31) }) == x,
{
}

/// A multiply by `c` is undone by a multiply by `ci` when `c * ci` is 1
/// modulo 2^64.
proof fn lemma_mul_inverse(x: u64, c: u64, ci: u64, k: int)
    requires
        c as int * ci as int == k * modulus() + 1,
    ensures
        mul(mul(x, c), ci) == x,
{
    let m = modulus();
    lemma_mul_mod_noop_left(x as int * c as int, ci as int, m);
    assert((x as int * c as int) * ci as int == (x as int * k) * m + x as int)
        by (nonlinear_arith)
        requires c as int * ci as int == k * m + 1;
    lemma_mod_multiples_vanish(x as int * k, x as int, m);
    lemma_small_mod(x as nat, m as nat);
}

/// The odd constants of the mixer and their inverses modulo 2^64.
proof fn lemma_mixer_constants()
    ensures
        0xbf58_476d_1ce4_e5b9int * 0x96de_1b17_3f11_9089int
            == 8125545580553827436int * modulus() + 1,
        0x94d0_49bb_1331_11ebint * 0x3196_42b2_d24d_8ec3int
            == 2077064247507883091int * modulus() + 1,
{
    assert(0xbf58_476d_1ce4_e5b9int * 0x96de_1b17_3f11_9089int
        == 8125545580553827436int * 0x1_0000_0000_0000_0000int + 1) by (compute_only);
    assert(0x94d0_49bb_1331_11ebint * 0x3196_42b2_d24d_8ec3int
        == 2077064247507883091int * 0x1_0000_0000_0000_0000int + 1) by (compute_only);
}

/// `spec_unmix64` undoes `spec_mix64`, so the mixer is injective.
pub proof fn lemma_unmix64_inverts_mix64(x: u64)
    ensures
        spec_unmix64(spec_mix64(x)) == x,
{
    lemma_mixer_constants();
    let a = xs(x, 30);
    let b = mul(a, 0xbf58_476d_1ce4_e5b9);
    let c = xs(b, 27);
    let d = mul(c, 0x94d0_49bb_1331_11eb);
    lemma_xs31(d);
    lemma_mul_inverse(c, 0x94d0_49bb_1331_11eb, 0x3196_42b2_d24d_8ec3, 2077064247507883091);
    lemma_xs27(b);
    lemma_mul_inverse(a, 0xbf58_476d_1ce4_e5b9, 0x96de_1b17_3f11_9089, 8125545580553827436);
    lemma_xs30(x);
}

/// `spec_mix64` undoes `spec_unmix64`, so the mixer is surjective.
pub proof fn lemma_mix64_inverts_unmix64(y: u64)
    ensures
        spec_mix64(spec_unmix64(y)) == y,
{
    lemma_mixer_constants();
    let d = y ^ (y >> 31) ^ (y >> 62);
    let c = mul(d, 0x3196_42b2_d24d_8ec3);
    let b = c ^ (c >> 27) ^ (c >> 54);
    let a = mul(b, 0x96de_1b17_3f11_9089);
    lemma_xs30(a);
    lemma_mul_inverse(b, 0x96de_1b17_3f11_9089, 0xbf58_476d_1ce4_e5b9, 8125545580553827436);
    lemma_xs27(c);
    lemma_mul_inverse(d, 0x3196_42b2_d24d_8ec3, 0x94d0_49bb_1331_11eb, 2077064247507883091);
    lemma_xs31(y);
}

/// The mixer is a bijection. No two inputs share an output, and
/// `spec_unmix64` gives each output its input.
pub proof fn theorem_mix64_is_a_bijection()
    ensures
        forall|x: u64, y: u64| #[trigger] spec_mix64(x) == #[trigger] spec_mix64(y) ==> x == y,
        forall|y: u64| spec_mix64(#[trigger] spec_unmix64(y)) == y,
{
    assert forall|x: u64, y: u64| #[trigger] spec_mix64(x) == #[trigger] spec_mix64(y) implies x == y by {
        lemma_unmix64_inverts_mix64(x);
        lemma_unmix64_inverts_mix64(y);
    }
    assert forall|y: u64| spec_mix64(#[trigger] spec_unmix64(y)) == y by {
        lemma_mix64_inverts_unmix64(y);
    }
}

/// `chaos::splitmix64` is a bijection too: the added constant is undone by
/// a subtraction.
pub proof fn theorem_splitmix64_is_a_bijection()
    ensures
        forall|x: u64| #[trigger] spec_unsplitmix64(spec_splitmix64(x)) == x,
        forall|x: u64, y: u64|
            #[trigger] spec_splitmix64(x) == #[trigger] spec_splitmix64(y) ==> x == y,
{
    assert forall|x: u64| #[trigger] spec_unsplitmix64(spec_splitmix64(x)) == x by {
        let shifted = ((x as int + 0x9E37_79B9_7F4A_7C15) % modulus()) as u64;
        lemma_unmix64_inverts_mix64(shifted);
    }
    assert forall|x: u64, y: u64|
        #[trigger] spec_splitmix64(x) == #[trigger] spec_splitmix64(y) implies x == y by {
        assert(spec_unsplitmix64(spec_splitmix64(x)) == x);
        assert(spec_unsplitmix64(spec_splitmix64(y)) == y);
    }
}

// mirror: autumn-harvest/src/policy.rs mix64
pub fn mix64(mut x: u64) -> (r: u64)
    ensures
        r == spec_mix64(x),
{
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

// mirror: autumn-harvest/src/dlq.rs mix64
pub fn dlq_mix64(seed: u64) -> (r: u64)
    ensures
        r == spec_mix64(seed),
{
    let mut z = seed;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// mirror: autumn-harvest/src/chaos.rs splitmix64
pub fn splitmix64(seed: u64) -> (r: u64)
    ensures
        r == spec_splitmix64(seed),
{
    let x = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

} // verus!
