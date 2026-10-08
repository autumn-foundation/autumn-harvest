use std::collections::HashMap;
fn main() {
    let mut map = HashMap::new();
    map.insert("a", 1);
    println!("{:?}", map.keys());
}
