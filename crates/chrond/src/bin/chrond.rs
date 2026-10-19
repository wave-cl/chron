use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tracing_subscriber::EnvFilter;

/// Serves the chron databases under a data directory over gRPC.
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
    /// Seconds between a replica's requests for the leader's list of
    /// databases (it also asks whenever a puller reconnects).
    #[arg(long, env = "CHROND_DISCOVER_INTERVAL", default_value_t = 10)]
    discover_interval: u64,
    /// Serve Prometheus metrics at GET /metrics on this address (plain
    /// HTTP, no token: keep it on a private interface).
    #[arg(long, env = "CHROND_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,
    /// Serve the HTTP/JSON gateway (same token and TLS as --listen) on
    /// this address; see /openapi.json there.
    #[arg(long, env = "CHROND_HTTP_LISTEN")]
    http_listen: Option<SocketAddr>,
    /// Another node of a failover group (repeat, or a comma list). With
    /// any, this node follows the leader it hears or is elected; see
    /// --advertise and --lease-ttl.
    #[arg(
        long = "peer",
        env = "CHROND_PEERS",
        value_delimiter = ',',
        conflicts_with = "replicate_from"
    )]
    peers: Vec<String>,
    /// This node's address as its peers reach it (default
    /// http(s)://<listen>; needed for 0.0.0.0 or port 0).
    #[arg(long, env = "CHROND_ADVERTISE")]
    advertise: Option<String>,
    /// Seconds a majority's acknowledgment lets the leader accept writes.
    #[arg(long, env = "CHROND_LEASE_TTL", default_value_t = 10.0)]
    lease_ttl: f64,
    /// With --peer: acknowledge a commit once a majority of the group
    /// durably holds it (majority), or once it is on this node's disk
    /// (off). Per request: CommitRequest.sync, ?sync=, chron append
    /// --sync/--async.
    #[arg(long, env = "CHROND_SYNC_COMMIT", value_enum, default_value_t = SyncArg::Majority)]
    sync_commit: SyncArg,
    /// Seconds a sync commit waits for a majority before answering
    /// "unconfirmed" (default: the lease ttl).
    #[arg(long, env = "CHROND_SYNC_TIMEOUT")]
    sync_timeout: Option<f64>,
    /// Log lines as human text or one JSON object per line.
    #[arg(long, env = "CHROND_LOG_FORMAT", value_enum, default_value_t = chrond::logging::LogFormat::Text)]
    log_format: chrond::logging::LogFormat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum SyncArg {
    Off,
    Majority,
}

fn read(path: &PathBuf, what: &str) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("cannot read the {what} at {}: {e}", path.display()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let filter = EnvFilter::try_from_env("CHROND_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    tracing::subscriber::set_global_default(chrond::logging::subscriber(
        cli.log_format,
        filter,
        std::io::stderr,
    ))
    .expect("the first subscriber");
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
            identity: match (&cli.replicate_cert, &cli.replicate_key) {
                (Some(c), Some(k)) => Some(chron_client::Identity {
                    cert_pem: read(c, "client certificate")?,
                    key_pem: read(k, "client key")?,
                }),
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
    opts.discover_interval = std::time::Duration::from_secs(cli.discover_interval.max(1));
    opts.metrics_listen = cli.metrics_listen;
    opts.http_listen = cli.http_listen;
    opts.peers = cli
        .peers
        .into_iter()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect();
    opts.advertise = cli.advertise;
    opts.lease_ttl = std::time::Duration::from_secs_f64(cli.lease_ttl.max(0.0));
    opts.sync_commit = match cli.sync_commit {
        SyncArg::Off => chrond::SyncCommit::Off,
        SyncArg::Majority => chrond::SyncCommit::Majority,
    };
    opts.sync_timeout = cli
        .sync_timeout
        .map(|s| std::time::Duration::from_secs_f64(s.max(0.0)));
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
