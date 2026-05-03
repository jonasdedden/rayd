//! `rayd` command-line tool.
//!
//! Subcommands:
//!
//! - `rayd version` — prints the build version.
//! - `rayd plasma-server <socket>` — runs a standalone plasma store at the
//!   given UDS path until SIGINT.
//! - `rayd gcs --bind <addr>` — runs the Global Control Service (gRPC
//!   `NodeRegistry` + `JobRegistry`) until SIGINT.
//! - `rayd start --head` — composes `gcs` + `plasma-server` in one process
//!   and prints the env vars a driver needs to attach.
//! - `rayd start --address=<gcs>` — runs a `rayd-raylet` attached to an
//!   existing head's GCS, registers as a node, and serves the
//!   `ObjectTransport` gRPC. Phase 3.4a only ships the skeleton; actual
//!   cross-node Pull/Push lands with 3.4b/c.
//!
//! Future Phase 3 subcommands (planned, not yet shipped):
//! `rayd stop`, `rayd status`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use std::time::Duration;

use clap::{Parser, Subcommand};
use rayd_gcs::{GcsServer, GcsServerConfig, Resources as GcsResources};
use rayd_plasma::PlasmaServer;
use rayd_raylet::{Raylet, RayletConfig};

#[derive(Parser, Debug)]
#[command(
    name = "rayd",
    version,
    about = "rayd: a Rust+PyO3 reimplementation of Ray Core",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Print the rayd version.
    Version,
    /// Run a standalone plasma object store at the given UDS path.
    PlasmaServer {
        /// Path of the Unix domain socket to bind. Parent directories must
        /// exist; an existing file at `socket` is removed before binding.
        socket: PathBuf,
        /// Arena capacity in MiB. Defaults to 128 MiB.
        #[arg(long, default_value_t = 128)]
        capacity_mb: u64,
        /// Optional bind address for the Prometheus `/metrics` HTTP
        /// endpoint. When unset, no metrics are collected.
        #[arg(long)]
        metrics_bind: Option<SocketAddr>,
    },
    /// Run the Global Control Service (gRPC `NodeRegistry` + `JobRegistry`).
    Gcs {
        /// Socket address to bind. Pass `0.0.0.0:<port>` to accept remote
        /// nodes; default `127.0.0.1:60000` is loopback-only.
        #[arg(long, default_value = "127.0.0.1:60000")]
        bind: SocketAddr,
        /// Heartbeat timeout in milliseconds. Nodes whose last heartbeat
        /// is older than this are flipped to `Dead` by the sweeper. Pass
        /// `0` to disable expiry (useful for debugging).
        #[arg(long, default_value_t = 10_000)]
        heartbeat_timeout_ms: u64,
        /// Optional bind address for the Prometheus `/metrics` HTTP
        /// endpoint. When unset, no metrics are collected and no
        /// extra port is bound. Use `127.0.0.1:9100` (or similar)
        /// to expose to a Prometheus scraper.
        #[arg(long)]
        metrics_bind: Option<SocketAddr>,
    },
    /// Bring up a single-node head: GCS + plasma-server in one process.
    /// Prints the env vars a driver needs to attach, then waits for SIGINT.
    Start {
        /// Run as the head of a new cluster.
        #[arg(long, conflicts_with = "address")]
        head: bool,
        /// Attach this host as a worker node to an existing head's GCS,
        /// e.g. `--address=10.0.0.1:60000`. Mutually exclusive with `--head`.
        #[arg(long, conflicts_with = "head")]
        address: Option<SocketAddr>,
        /// GCS bind address (only used with `--head`).
        #[arg(long, default_value = "127.0.0.1:60000")]
        gcs_bind: SocketAddr,
        /// Plasma UDS path. Parent must exist; an existing socket file is removed.
        #[arg(long, default_value = "/tmp/rayd-head.sock")]
        plasma_socket: PathBuf,
        /// Plasma arena capacity in MiB.
        #[arg(long, default_value_t = 128)]
        plasma_capacity_mb: u64,
        /// Hostname this raylet advertises in the GCS (only used with
        /// `--address`). Peer raylets dial this name; default is the
        /// machine's hostname.
        #[arg(long)]
        advertise_host: Option<String>,
        /// gRPC bind address for the raylet's `ObjectTransport` server
        /// (only used with `--address`). `:0` lets the OS pick.
        #[arg(long, default_value = "0.0.0.0:0")]
        raylet_bind: SocketAddr,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    // The tracing subscriber is installed *inside* each subcommand
    // that builds a tokio runtime — otherwise the optional OTLP
    // span exporter (which spawns a tonic gRPC client) panics on
    // the missing runtime context. Subcommands with no runtime
    // (`version`, `plasma-server`) install the subscriber before
    // their main work so any tracing call sites still surface.
    match cli.command {
        Command::Version => {
            println!("rayd {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Command::PlasmaServer {
            socket,
            capacity_mb,
            metrics_bind,
        } => match run_plasma_server(socket, capacity_mb, metrics_bind) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("rayd plasma-server: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Gcs {
            bind,
            heartbeat_timeout_ms,
            metrics_bind,
        } => match run_gcs(bind, heartbeat_timeout_ms, metrics_bind) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("rayd gcs: {e}");
                ExitCode::FAILURE
            }
        },
        Command::Start {
            head,
            address,
            gcs_bind,
            plasma_socket,
            plasma_capacity_mb,
            advertise_host,
            raylet_bind,
        } => {
            let result = match (head, address) {
                (true, _) => run_start_head(gcs_bind, plasma_socket, plasma_capacity_mb),
                (false, Some(addr)) => run_start_worker(
                    addr,
                    plasma_socket,
                    advertise_host,
                    raylet_bind,
                    plasma_capacity_mb,
                ),
                (false, None) => {
                    eprintln!(
                        "rayd start: pass either --head (start a new cluster) \
                         or --address=<gcs-addr> (attach to an existing head)"
                    );
                    return ExitCode::FAILURE;
                }
            };
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("rayd start: {e}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

fn run_plasma_server(
    socket: PathBuf,
    capacity_mb: u64,
    metrics_bind: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    // No tokio runtime here — OTLP exporter (if requested) will fall
    // back to stderr-only with a one-line warning.
    rayd_core::init_default_subscriber();
    let capacity_bytes = capacity_mb
        .checked_mul(1024 * 1024)
        .ok_or("capacity-mb overflow")?;

    let server = PlasmaServer::start_with_metrics(socket.clone(), capacity_bytes, metrics_bind)?;
    eprintln!(
        "rayd plasma-server: listening on {} ({} MiB arena{}, ctrl-c to stop)",
        socket.display(),
        capacity_mb,
        match server.metrics_addr() {
            Some(addr) => format!(", metrics at http://{addr}/metrics"),
            None => String::new(),
        },
    );
    let _server = server;

    wait_for_sigint()?;
    eprintln!("rayd plasma-server: shutting down");
    Ok(())
}

fn run_gcs(
    bind: SocketAddr,
    heartbeat_timeout_ms: u64,
    metrics_bind: Option<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let heartbeat_timeout = Duration::from_millis(heartbeat_timeout_ms);
    let default = GcsServerConfig::default();
    let sweep_interval = if heartbeat_timeout.is_zero() {
        default.sweep_interval
    } else {
        default.sweep_interval.min(heartbeat_timeout)
    };
    let config = GcsServerConfig {
        heartbeat_timeout,
        sweep_interval,
        metrics_bind,
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        // Install subscriber inside the runtime so the OTLP layer
        // (when enabled via `OTEL_EXPORTER_OTLP_ENDPOINT`) can spawn
        // its tonic client.
        rayd_core::init_default_subscriber();
        let handle = GcsServer::start_with_config(bind, config).await?;
        eprintln!(
            "rayd gcs: NodeRegistry listening on {} (ctrl-c to stop)",
            handle.local_addr(),
        );
        wait_for_sigint_async().await?;
        eprintln!("rayd gcs: shutting down");
        handle.shutdown().await;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

fn wait_for_sigint() -> Result<(), Box<dyn std::error::Error>> {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_handler = Arc::clone(&stop);
    ctrlc::set_handler(move || {
        stop_for_handler.store(true, Ordering::SeqCst);
    })?;
    while !stop.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

async fn wait_for_sigint_async() -> Result<(), Box<dyn std::error::Error>> {
    tokio::signal::ctrl_c().await?;
    Ok(())
}

fn run_start_head(
    gcs_bind: SocketAddr,
    plasma_socket: PathBuf,
    plasma_capacity_mb: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let capacity_bytes = plasma_capacity_mb
        .checked_mul(1024 * 1024)
        .ok_or("plasma-capacity-mb overflow")?;

    let _server = PlasmaServer::start(plasma_socket.clone(), capacity_bytes)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        rayd_core::init_default_subscriber();
        let handle = GcsServer::start(gcs_bind).await?;
        let gcs_addr = handle.local_addr();
        eprintln!("rayd start --head: cluster up");
        eprintln!("  plasma socket: {}", plasma_socket.display());
        eprintln!("  gcs address:   {gcs_addr}");
        eprintln!();
        eprintln!("attach a driver in another shell:");
        eprintln!("  export RAYD_PLASMA_SOCKET={}", plasma_socket.display());
        eprintln!("  export RAYD_GCS_ADDRESS={gcs_addr}");
        eprintln!("  python -c 'import rayd; rayd.init()'");
        eprintln!();
        eprintln!("ctrl-c to stop");
        wait_for_sigint_async().await?;
        eprintln!("rayd start --head: shutting down");
        handle.shutdown().await;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

fn run_start_worker(
    gcs_address: SocketAddr,
    plasma_socket: PathBuf,
    advertise_host: Option<String>,
    raylet_bind: SocketAddr,
    plasma_capacity_mb: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let advertise_host = advertise_host
        .unwrap_or_else(|| std::env::var("HOSTNAME").unwrap_or_else(|_| "127.0.0.1".to_string()));
    let resources = GcsResources {
        num_cpus: u32::try_from(num_logical_cpus()).unwrap_or(1),
        num_gpus: 0,
        memory_bytes: 0,
    };
    let capacity_bytes = plasma_capacity_mb
        .checked_mul(1024 * 1024)
        .ok_or("plasma-capacity-mb overflow")?;

    // A worker node owns its own plasma store. (For multiple raylets
    // on one host sharing plasma, use the `Raylet` crate directly.)
    let _plasma_server = PlasmaServer::start(plasma_socket.clone(), capacity_bytes)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        rayd_core::init_default_subscriber();
        let config = RayletConfig {
            gcs_address,
            bind: raylet_bind,
            advertise_host: advertise_host.clone(),
            plasma_socket: plasma_socket.clone(),
            resources,
            ..RayletConfig::defaults()
        };
        let handle = Raylet::start(config).await?;
        eprintln!("rayd start --address: worker node up");
        eprintln!("  gcs address:        {gcs_address}");
        eprintln!("  raylet listening:   {}", handle.local_addr());
        eprintln!("  advertised host:    {advertise_host}");
        eprintln!("  plasma socket:      {}", plasma_socket.display());
        eprintln!();
        eprintln!("ctrl-c to stop");
        wait_for_sigint_async().await?;
        eprintln!("rayd start --address: draining and shutting down");
        handle.shutdown().await;
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}

fn num_logical_cpus() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZero::get)
}
