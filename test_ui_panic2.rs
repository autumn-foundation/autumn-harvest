fn main() {
    let id_str = "1🦀🦀";
    println!("{}", &id_str[..8.min(id_str.len())]);
}
