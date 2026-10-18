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
    /// PEM certificate chain to serve TLS with.
    #[arg(long, env = "CHROND_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for --tls-cert.
    #[arg(long, env = "CHROND_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// PEM CA; clients must present a certificate it signed (mTLS).
    #[arg(long, env = "CHROND_TLS_CLIENT_CA", requires = "tls_cert")]
    tls_client_ca: Option<PathBuf>,
    /// Every call must carry this bearer token.
    #[arg(
        long,
        env = "CHRON_TOKEN",
        hide_env_values = true,
        conflicts_with = "token_file"
    )]
    token: Option<String>,
    /// Read the bearer token from this file.
    #[arg(long, env = "CHRON_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Largest request or response message, in bytes.
    #[arg(long, env = "CHROND_MAX_MESSAGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    max_message_bytes: usize,
    /// Seconds a call may take to produce its response head.
    #[arg(long, env = "CHROND_REQUEST_TIMEOUT")]
    request_timeout: Option<u64>,
    /// Concurrent calls per connection.
    #[arg(long, default_value_t = 256)]
    concurrency_per_connection: usize,
}

fn read(path: &PathBuf, what: &str) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("cannot read the {what} at {}: {e}", path.display()))
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
    if let (Some(cert), Some(key)) = (&cli.tls_cert, &cli.tls_key) {
        opts.tls = Some(chrond::Tls {
            cert_pem: read(cert, "TLS certificate")?,
            key_pem: read(key, "TLS key")?,
            client_ca_pem: cli
                .tls_client_ca
                .as_ref()
                .map(|p| read(p, "client CA"))
                .transpose()?,
        });
    }
    opts.token = match (cli.token, cli.token_file) {
        (Some(t), _) => Some(t),
        (None, Some(f)) => Some(
            String::from_utf8(read(&f, "token file")?)?
                .trim_end()
                .to_string(),
        ),
        (None, None) => None,
    };
    opts.max_message_bytes = cli.max_message_bytes;
    opts.request_timeout = cli.request_timeout.map(std::time::Duration::from_secs);
    opts.concurrency_per_connection = cli.concurrency_per_connection;
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
