//! The generated code is committed under `src/generated/`, so building
//! this crate needs no `protoc`. Set `CHRON_PROTO_REGEN=1` to regenerate
//! it from the `.proto` (which does need `protoc`); CI checks that the
//! committed file matches.

use std::env;
use std::process::Command;

const PROTOS: [&str; 1] = ["proto/chron/v1/chron.proto"];
const OUT_DIR: &str = "src/generated";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=CHRON_PROTO_REGEN");
    println!("cargo:rerun-if-env-changed=PROTOC");
    for p in PROTOS {
        println!("cargo:rerun-if-changed={p}");
    }
    println!("cargo:rerun-if-changed={OUT_DIR}/chron.v1.rs");
    if env::var_os("CHRON_PROTO_REGEN").is_none() {
        return Ok(());
    }
    let protoc = env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
    if Command::new(&protoc).arg("--version").output().is_err() {
        return Err(format!(
            "protoc not found (looked for `{protoc}`). Install it with `brew install protobuf` \
             or `apt-get install protobuf-compiler`, or point PROTOC at the binary."
        )
        .into());
    }
    std::fs::create_dir_all(OUT_DIR)?;
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(OUT_DIR)
        .compile_protos(&PROTOS, &["proto"])?;
    Ok(())
}
