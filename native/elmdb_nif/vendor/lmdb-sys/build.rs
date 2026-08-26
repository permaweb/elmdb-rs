extern crate cc;

use std::env;
use std::path::PathBuf;

fn main() {
    let mut lmdb: PathBuf = PathBuf::from(&env::var("CARGO_MANIFEST_DIR").unwrap());
    lmdb.push("lmdb");
    lmdb.push("libraries");
    lmdb.push("liblmdb");

    // Always compile the vendored LMDB 1.0 sources: linking a system liblmdb
    // would silently change the on-disk data format.
    cc::Build::new()
        .file(lmdb.join("mdb.c"))
        .file(lmdb.join("midl.c"))
        .opt_level(2)
        .compile("liblmdb.a")
}
