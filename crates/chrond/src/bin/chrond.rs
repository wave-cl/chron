use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Serves one chron database over gRPC.
#[derive(Parser, Debug)]
#[command(name = "chrond", version, about)]
struct Cli {
    /// The database directory; created if missing.
    #[arg(long, env = "CHRON_DATA_DIR")]
    data_dir: PathBuf,
    /// Address to listen on.
    #[arg(long, env = "CHRON_LISTEN", default_value = "127.0.0.1:4242")]
    listen: SocketAddr,
    /// Do not fsync on commit: a committed transaction survives a process
    /// crash but not power loss.
    #[arg(long)]
    no_fsync: bool,
    /// Read every segment on open, checking crcs and the index.
    #[arg(long)]
    verify_all_segments: bool,
    /// Follow the leader at this address as a read-only replica.
    #[arg(long, env = "CHRON_REPLICATE_FROM")]
    replicate_from: Option<String>,
    /// Bearer token sent to the leader.
    #[arg(long, env = "CHRON_REPLICATE_TOKEN", hide_env_values = true)]
    replicate_token: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("CHROND_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let mut opts = chrond::Options::new(cli.data_dir, cli.listen);
    opts.fsync = !cli.no_fsync;
    opts.verify_all_segments = cli.verify_all_segments;
    opts.replicate_from = cli.replicate_from;
    opts.replicate_token = cli.replicate_token;
    let running = chrond::start(opts).await?;
    signal().await;
    tracing::info!("signal received, stopping");
    running.shutdown().await
}

async fn signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
