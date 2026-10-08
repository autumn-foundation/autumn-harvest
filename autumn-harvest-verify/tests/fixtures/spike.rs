use std::cell::RefCell;
use std::collections::HashMap;
pub trait Src { fn next(&self) -> u32; }
pub struct A;
impl Src for A { fn next(&self) -> u32 { 4 } }
static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
thread_local! { static TL: RefCell<u64> = RefCell::new(0); }
pub struct Ctx { pub state: RefCell<u32> }
impl Ctx { pub fn emit(&self, x: u32) -> u32 { x } }
pub fn helper<T: IntoIterator<Item=(String,u32)>>(t: T) -> Vec<(String,u32)> { t.into_iter().collect() }
pub fn deep() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() }
pub async fn sub(ctx: &Ctx) -> u32 { ctx.emit(1) }
pub async fn wf(ctx: &Ctx, m: HashMap<String,u32>, s: Box<dyn Src>) -> u32 {
    let items = helper(m);
    let c = COUNTER.load(std::sync::atomic::Ordering::SeqCst);
    let t = TL.with(|v| *v.borrow());
    let r = *ctx.state.borrow();
    let f = |x: u32| x + 1;
    let mut acc = 0;
    for (_, v) in items { acc += ctx.emit(v); }
    acc + ctx.emit(c as u32) + ctx.emit(t as u32) + r + s.next() + f(deep() as u32) + sub(ctx).await
}
