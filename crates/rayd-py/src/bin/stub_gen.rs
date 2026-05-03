//! Regenerates `python/rayd/_native.pyi` using `PyO3`'s native introspection.
//!
//! Workflow:
//! 1. Build the cdylib (`maturin develop` or `cargo build`).
//! 2. Run this binary, pointing at the resulting `.so` (or `.pyd` on Windows).
//! 3. The introspection metadata, embedded by `experimental-inspect`, is
//!    parsed by `pyo3-introspection` and written out as one or more
//!    `.pyi` files.
//!
//! Defaults assume the workspace's `python/rayd/` layout. Override the
//! input/output paths via `RAYD_CDYLIB_PATH` and `RAYD_STUB_OUT_DIR`.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use pyo3_introspection::{introspect_cdylib, module_stub_files};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("stub_gen failed: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cdylib = locate_cdylib()?;
    let out_dir = locate_out_dir();

    // The module name passed here must match the `#[pymodule]` declaration
    // (`pub mod _native { ... }`). The Python-side dotted name (`rayd._native`)
    // is set by `module = "rayd._native"` on each `#[pyclass]`.
    let module = introspect_cdylib(&cdylib, "_native")?;
    let files = module_stub_files(&module);

    fs::create_dir_all(&out_dir)?;
    for (name, contents) in files {
        // pyo3-introspection emits the root module as `__init__.pyi`. Our
        // root is `_native`, but we sit inside the `rayd` package, so we
        // rewrite to `_native.pyi`. Submodule stubs (none today) keep their
        // `<name>.pyi` form.
        let dest_name = if name == Path::new("__init__.pyi") {
            PathBuf::from("_native.pyi")
        } else {
            name
        };
        let dest = out_dir.join(&dest_name);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&dest, contents)?;
        println!("wrote {}", dest.display());
    }
    Ok(())
}

fn locate_cdylib() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(path) = env::var("RAYD_CDYLIB_PATH") {
        return Ok(PathBuf::from(path));
    }
    let workspace_root = workspace_root();
    let candidates = [
        workspace_root.join("target/release/librayd_native.so"),
        workspace_root.join("target/debug/librayd_native.so"),
        workspace_root.join("target/release/librayd_native.dylib"),
        workspace_root.join("target/debug/librayd_native.dylib"),
        workspace_root.join("target/release/rayd_native.dll"),
        workspace_root.join("target/debug/rayd_native.dll"),
    ];
    for cand in &candidates {
        if cand.exists() {
            return Ok(cand.clone());
        }
    }
    Err(
        "could not find rayd_native cdylib; build it with `cargo build` or set RAYD_CDYLIB_PATH"
            .into(),
    )
}

fn locate_out_dir() -> PathBuf {
    if let Ok(path) = env::var("RAYD_STUB_OUT_DIR") {
        return PathBuf::from(path);
    }
    workspace_root().join("python/rayd")
}

fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR points at this binary's crate (`crates/rayd-py/`); go up two.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(PathBuf::from)
        .expect("workspace root")
}
