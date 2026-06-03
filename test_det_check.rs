fn next_char(line: &str, pos: usize) -> Option<(char, usize)> {
    let ch = line[pos..].chars().next()?;
    Some((ch, pos + ch.len_utf8()))
}
fn main() {
    let line = "a💀b";
    // Let's try slicing at an invalid boundary
    // '💀' is 4 bytes long, so byte 1 is '💀', byte 2,3,4 are inside '💀'.
    println!("{:?}", next_char(line, 2));
}
