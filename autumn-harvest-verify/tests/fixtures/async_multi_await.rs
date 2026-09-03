//! Fixture: a coroutine with three suspend points, so the MIR uses
//! `as variant#3`, `as variant#4` and `as variant#5`, and a struct field
//! projection is held live across the awaits.
//!
//!   rustc --crate-type lib --edition 2024 --emit=mir \
//!         -o async_multi_await.mir async_multi_await.rs
#![allow(dead_code)]

pub struct Ctx;

impl Ctx {
    pub fn emit(&self, v: u64) -> u64 {
        v
    }
}

pub struct Order {
    pub id: u64,
    pub total: u64,
    pub label: String,
}

pub async fn step(v: u64) -> u64 {
    v + 1
}

/// `order.id` and `order.total` are read after suspend points, so the whole
/// `order` is moved into the coroutine's suspend variants.
pub async fn pipeline(ctx: &Ctx, order: Order) -> u64 {
    let a = step(order.id).await;
    let b = step(a).await;
    let c = step(order.total).await;
    ctx.emit(a + b + c + order.id + order.total + order.label.len() as u64)
}
