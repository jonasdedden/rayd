//! Build-time codegen for the GCS proto files via `tonic-build`.
//!
//! Requires `protoc` on the build host. The generated Rust code lands in
//! `OUT_DIR` and is included from `src/lib.rs` via `tonic::include_proto!`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "proto/node_info.proto",
        "proto/job_info.proto",
        "proto/actor_info.proto",
    ];
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &["proto"])?;

    Ok(())
}
