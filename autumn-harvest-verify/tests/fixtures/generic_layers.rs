//! Fixture: generic substitution two layers deep, plus a generic impl block.
//!
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o generic_layers.mir generic_layers.rs
#![allow(dead_code)]

pub trait Score {
    fn score(&self) -> u64;
}

pub struct Leaf(pub u64);

impl Score for Leaf {
    fn score(&self) -> u64 {
        self.0
    }
}

pub struct Wrapper<T>(pub T);

impl<T: Score> Score for Wrapper<T> {
    fn score(&self) -> u64 {
        self.0.score() + 1
    }
}

/// Innermost layer: a fully-qualified `<T as Trait>` call on a type parameter.
pub fn inner<T: Score>(t: &T) -> u64 {
    <T as Score>::score(t)
}

/// Middle layer: passes its own `T` straight through, so the substitution must
/// be threaded across two frames before it becomes concrete.
pub fn outer<T: Score>(t: &T) -> u64 {
    inner(t) + inner(t)
}

/// Call site: the only place a concrete type appears.
pub fn entry() -> u64 {
    outer(&Wrapper(Leaf(3)))
}
