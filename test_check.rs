fn main() {
    let x: i32 = i32::MAX;
    let y: i32 = 1;
    let result = x.saturating_add(y);
    println!("result = {}", result);
}
