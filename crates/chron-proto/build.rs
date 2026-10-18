use std::env;
use std::process::Command;

const PROTOS: [&str; 1] = ["proto/chron/v1/chron.proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    for p in PROTOS {
        println!("cargo:rerun-if-changed={p}");
    }
    println!("cargo:rerun-if-env-changed=PROTOC");
    let protoc = env::var("PROTOC").unwrap_or_else(|_| "protoc".to_string());
    if Command::new(&protoc).arg("--version").output().is_err() {
        return Err(format!(
            "protoc not found (looked for `{protoc}`). Install it with `brew install protobuf` \
             or `apt-get install protobuf-compiler`, or point PROTOC at the binary."
        )
        .into());
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&PROTOS, &["proto"])?;
    Ok(())
}
