use std::fs;
fn main() {
    let source = "pub fn foo() {\n    let s = \"a💀b\"; // 💀 is a multibyte char\n}";
    // we just need to hit the det_check module. We could write a small unit test in autumn-harvest/src/det_check.rs
}
