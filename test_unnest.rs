use diesel::sql_types::{Text, Array};

fn main() {
    println!("SELECT pg_notify(channel, $2) FROM unnest($1) AS channel");
}
