fn main() {
    let id_str = "🦀🦀🦀🦀🦀🦀🦀🦀🦀";
    println!("{}", &id_str[..8.min(id_str.len())]);
}
