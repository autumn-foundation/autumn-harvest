fn main() {
    // Ensure Cargo recompiles embed_migrations!() whenever any migration file
    // is added or removed from the migrations/ directory.
    println!("cargo:rerun-if-changed=migrations/");
}
