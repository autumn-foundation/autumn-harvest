use std::io::{Read,Write};
fn main(){let mut b=Vec::new();std::io::stdin().read_to_end(&mut b).unwrap();let mut o:Vec<u8>=b.into_iter().filter(|c|!matches!(c,b' '|b'\n'|b'\r'|b'\t')).collect();o.push(b'\n');std::io::stdout().write_all(&o).unwrap();}
