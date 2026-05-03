//! Build-time codegen for the raylet's proto files via `tonic-build`.
//!
//! Requires `protoc` on the build host.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = ["proto/object_transport.proto"];
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;

    Ok(())
}
