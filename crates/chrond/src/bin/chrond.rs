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
    #[arg(
        long,
        env = "CHRON_REPLICATE_TOKEN",
        hide_env_values = true,
        conflicts_with = "replicate_token_file"
    )]
    replicate_token: Option<String>,
    /// Read the token sent to the leader from this file.
    #[arg(long, env = "CHRON_REPLICATE_TOKEN_FILE")]
    replicate_token_file: Option<PathBuf>,
    /// PEM CA that signed the leader's certificate (an https://
    /// --replicate-from); without it the system roots are used.
    #[arg(long, env = "CHROND_REPLICATE_CA")]
    replicate_ca: Option<PathBuf>,
    /// PEM client certificate to present to the leader (mTLS).
    #[arg(long, env = "CHROND_REPLICATE_CERT", requires = "replicate_key")]
    replicate_cert: Option<PathBuf>,
    /// PEM private key for --replicate-cert.
    #[arg(long, env = "CHROND_REPLICATE_KEY", requires = "replicate_cert")]
    replicate_key: Option<PathBuf>,
    /// Name to verify the leader's certificate against, when it is not
    /// the address's host.
    #[arg(long, env = "CHROND_REPLICATE_DOMAIN")]
    replicate_domain: Option<String>,
    /// Make a directory that was a replica the leader: bumps the epoch
    /// and writes the `promoted` marker. The old leader is not told.
    #[arg(long, conflicts_with = "replicate_from")]
    promote: bool,
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
    /// Promote, fence, retain, backup, apply and replicate need this token
    /// (it opens every other call too); without it --token covers them.
    #[arg(
        long,
        env = "CHROND_ADMIN_TOKEN",
        hide_env_values = true,
        conflicts_with = "admin_token_file"
    )]
    admin_token: Option<String>,
    /// Read the admin token from this file.
    #[arg(long, env = "CHROND_ADMIN_TOKEN_FILE")]
    admin_token_file: Option<PathBuf>,
    /// Directory for backups taken and applied over the daemon, by bare
    /// file name; without it those calls are refused.
    #[arg(long, env = "CHROND_BACKUP_DIR")]
    backup_dir: Option<PathBuf>,
    /// Largest request or response message, in bytes.
    #[arg(long, env = "CHROND_MAX_MESSAGE_BYTES", default_value_t = 64 * 1024 * 1024)]
    max_message_bytes: usize,
    /// Seconds a call may take to produce its response head.
    #[arg(long, env = "CHROND_REQUEST_TIMEOUT")]
    request_timeout: Option<u64>,
    /// Concurrent calls per connection.
    #[arg(long, default_value_t = 256)]
    concurrency_per_connection: usize,
    /// Seconds between HTTP/2 pings to idle connections.
    #[arg(long, env = "CHROND_KEEPALIVE_INTERVAL", default_value_t = 30)]
    keepalive_interval: u64,
    /// Seconds to wait for a ping's answer before closing the connection.
    #[arg(long, env = "CHROND_KEEPALIVE_TIMEOUT", default_value_t = 10)]
    keepalive_timeout: u64,
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
    opts.replicate_token = token(
        cli.replicate_token,
        cli.replicate_token_file,
        "replication token file",
    )?;
    opts.promote = cli.promote;
    if cli.replicate_ca.is_some() || cli.replicate_cert.is_some() || cli.replicate_domain.is_some()
    {
        opts.replicate_tls = Some(chron_client::TlsConfig {
            ca_pem: cli
                .replicate_ca
                .as_ref()
                .map(|p| read(p, "leader CA"))
                .transpose()?,
            identity_pem: match (&cli.replicate_cert, &cli.replicate_key) {
                (Some(c), Some(k)) => {
                    Some((read(c, "client certificate")?, read(k, "client key")?))
                }
                _ => None,
            },
            domain: cli.replicate_domain,
        });
    }
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
    opts.token = token(cli.token, cli.token_file, "token file")?;
    opts.admin_token = token(cli.admin_token, cli.admin_token_file, "admin token file")?;
    opts.backup_dir = cli.backup_dir;
    opts.keepalive_interval = std::time::Duration::from_secs(cli.keepalive_interval);
    opts.keepalive_timeout = std::time::Duration::from_secs(cli.keepalive_timeout);
    opts.max_message_bytes = cli.max_message_bytes;
    opts.request_timeout = cli.request_timeout.map(std::time::Duration::from_secs);
    opts.concurrency_per_connection = cli.concurrency_per_connection;
    let running = chrond::start(opts).await?;
    signal().await;
    tracing::info!("signal received, stopping");
    running.shutdown().await
}

/// A token given directly, or read from a file (trailing newline dropped).
fn token(
    direct: Option<String>,
    file: Option<PathBuf>,
    what: &str,
) -> anyhow::Result<Option<String>> {
    Ok(match (direct, file) {
        (Some(t), _) => Some(t),
        (None, Some(f)) => Some(String::from_utf8(read(&f, what)?)?.trim_end().to_string()),
        (None, None) => None,
    })
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
