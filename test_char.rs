fn truncate_id(id_str: &str) -> &str {
    id_str.char_indices().nth(8).map_or(id_str, |(idx, _)| &id_str[..idx])
}

fn main() {
    println!("{}", truncate_id("1🦀🦀🦀🦀"));
    println!("{}", truncate_id("1234567890"));
    println!("{}", truncate_id("12"));
}
