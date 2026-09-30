//! Locate libtachyon, or build it from source with the `vendored` feature.
//!
//! The default path uses pkg-config so a system install is enough. `vendored`
//! compiles the C sources directly, which is what most crates.io users want --
//! nobody should have to install a C library to `cargo add` a Rust crate.

fn main() {
    #[cfg(feature = "vendored")]
    {
        let root = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("..")
            .join("..")
            .join("..");

        cc::Build::new()
            .include(root.join("include"))
            .include(root.join("src"))
            .files([
                root.join("src/util.c"),
                root.join("src/ring.c"),
                root.join("src/notify.c"),
                root.join("src/pool.c"),
                root.join("src/reaper.c"),
                root.join("src/topic.c"),
                root.join("src/shm.c"),
            ])
            .flag("-std=c11")
            .flag("-O3")
            .flag("-fPIC")
            .warnings(false)
            .compile("tachyon");

        println!("cargo:rustc-link-lib=pthread");
        println!("cargo:rustc-link-lib=rt");
        return;
    }

    #[cfg(not(feature = "vendored"))]
    {
        match pkg_config::Config::new().probe("tachyon") {
            Ok(_) => {}
            Err(e) => {
                // Not fatal: the linker will say something clearer than we can.
                println!("cargo:warning=pkg-config could not find tachyon: {e}");
                println!("cargo:warning=try the `vendored` feature, or install libtachyon first");
                println!("cargo:rustc-link-lib=tachyon");
            }
        }
    }
}
