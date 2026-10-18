//! `chrond`: one chron database behind the `chron.v1` gRPC service, as a
//! leader or as a replica of another daemon. [`start`] serves it; the
//! `chrond` binary wraps that with flags and signal handling.

mod auth;
mod codec;
pub mod node;
mod replica;
mod service;
#[cfg(feature = "test-util")]
pub mod testing;
mod trace;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chron::{Db, Fsync};
use chron_proto::v1::chron_server::ChronServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::node::{FENCED_MARKER, Node, PROMOTED_MARKER, REPLICA_MARKER, Role, Upstream};

/// The health service name that is SERVING only while this node is the
/// leader (`chron.v1.Chron` itself is SERVING in every role).
pub const LEADER_HEALTH_SERVICE: &str = "chron.v1.Chron.leader";

/// How the daemon opens and serves its database.
#[derive(Debug, Clone)]
pub struct Options {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// `false` for [`Fsync::Never`].
    pub fsync: bool,
    pub verify_all_segments: bool,
    /// Follow the leader at this address as a read-only replica.
    pub replicate_from: Option<String>,
    /// Bearer token sent to the leader.
    pub replicate_token: Option<String>,
    /// TLS towards the leader (an `https://` address).
    pub replicate_tls: Option<chron_client::TlsConfig>,
    /// A directory that was a replica becomes the leader at start: the
    /// epoch is bumped and the `promoted` marker written. Refused when the
    /// directory is not a replica.
    pub promote: bool,
    /// The database's record limit; `None` for the library's default.
    pub max_record_bytes: Option<usize>,
    /// Serve TLS; `None` is plaintext.
    pub tls: Option<Tls>,
    /// Every `chron.v1.Chron` call must carry `authorization: Bearer
    /// <token>`. The health service stays open.
    pub token: Option<String>,
    /// When set, Promote, Fence, Retain, Backup, ApplyBackup and Replicate
    /// need this token instead (it opens every other call too). Unset,
    /// `token` covers them.
    pub admin_token: Option<String>,
    /// Where Backup writes and ApplyBackup reads, by bare file name.
    /// Unset, both are refused. Created if missing.
    pub backup_dir: Option<PathBuf>,
    /// Largest request or response message. Must be at least the
    /// database's `max_record_bytes`.
    pub max_message_bytes: usize,
    /// Bound on the time to a response head; streams keep running. Off by
    /// default: backups are long unary calls.
    pub request_timeout: Option<Duration>,
    pub concurrency_per_connection: usize,
    /// HTTP/2 ping interval and how long to wait for its answer.
    pub keepalive_interval: Duration,
    pub keepalive_timeout: Duration,
}

/// PEM bytes for the server's certificate chain and key, and optionally
/// the CA clients must present certificates from.
#[derive(Debug, Clone)]
pub struct Tls {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
    pub client_ca_pem: Option<Vec<u8>>,
}

impl Tls {
    fn server_config(&self) -> ServerTlsConfig {
        let mut cfg =
            ServerTlsConfig::new().identity(Identity::from_pem(&self.cert_pem, &self.key_pem));
        if let Some(ca) = &self.client_ca_pem {
            cfg = cfg.client_ca_root(Certificate::from_pem(ca));
        }
        cfg
    }
}

impl Options {
    pub fn new(data_dir: impl Into<PathBuf>, listen: SocketAddr) -> Self {
        Options {
            data_dir: data_dir.into(),
            listen,
            fsync: true,
            verify_all_segments: false,
            replicate_from: None,
            replicate_token: None,
            replicate_tls: None,
            promote: false,
            max_record_bytes: None,
            tls: None,
            token: None,
            admin_token: None,
            backup_dir: None,
            max_message_bytes: 64 * 1024 * 1024,
            request_timeout: None,
            concurrency_per_connection: 256,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
        }
    }
}

/// A serving daemon. Dropping it does not stop the server; call
/// [`shutdown`](Running::shutdown).
pub struct Running {
    pub local_addr: SocketAddr,
    /// The database being served; tests commit through it directly.
    pub db: Db,
    pub node: Arc<Node>,
    health: HealthReporter,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Opens (and recovers) the database, settles the node's role, binds
/// `opts.listen` (port 0 picks a free port) and serves until
/// [`Running::shutdown`]. A replica whose leader is unreachable starts
/// disconnected and keeps trying; one its leader refuses does not start.
pub async fn start(opts: Options) -> anyhow::Result<Running> {
    let mut db_opts = chron::Options::new()
        .fsync(if opts.fsync {
            Fsync::Always
        } else {
            Fsync::Never
        })
        .verify_all_segments(opts.verify_all_segments);
    if let Some(n) = opts.max_record_bytes {
        db_opts = db_opts.max_record_bytes(n);
    }
    if opts.token.as_deref() == Some("") || opts.admin_token.as_deref() == Some("") {
        anyhow::bail!("the token is empty");
    }
    if opts.request_timeout == Some(Duration::ZERO) {
        anyhow::bail!(
            "--request-timeout 0 would end every call at once; leave it unset for no bound"
        );
    }
    if !opts.listen.ip().is_loopback() && opts.token.is_none() && opts.tls.is_none() {
        tracing::warn!(
            listen = %opts.listen,
            "listening beyond loopback with no token and no TLS: anyone who can reach this address can read and write the database"
        );
    }
    let backup_dir = match &opts.backup_dir {
        Some(d) => {
            std::fs::create_dir_all(d)
                .with_context(|| format!("cannot create the backup directory {}", d.display()))?;
            Some(
                d.canonicalize()
                    .with_context(|| format!("backup directory {}", d.display()))?,
            )
        }
        None => None,
    };
    let dir = opts.data_dir.clone();
    let db = tokio::task::spawn_blocking(move || Db::open(&dir, db_opts))
        .await
        .context("open task")?
        .with_context(|| format!("cannot open the database at {}", opts.data_dir.display()))?;
    if opts.max_message_bytes < db.options().max_record_bytes {
        anyhow::bail!(
            "max_message_bytes ({}) is below the database's max_record_bytes ({})",
            opts.max_message_bytes,
            db.options().max_record_bytes
        );
    }
    if opts.replicate_tls.is_some()
        && !opts
            .replicate_from
            .as_deref()
            .unwrap_or("")
            .starts_with("https://")
    {
        anyhow::bail!("TLS towards the leader needs an https:// --replicate-from address");
    }
    let cancel = CancellationToken::new();
    let upstream = opts.replicate_from.clone().map(|addr| Upstream {
        addr,
        token: opts.replicate_token.clone(),
        tls: opts.replicate_tls.clone(),
        max_message_bytes: opts.max_message_bytes,
    });
    let node = Arc::new(Node::new(
        db.clone(),
        opts.data_dir.clone(),
        backup_dir,
        opts.admin_token.as_deref(),
        upstream,
        opts.max_message_bytes,
        cancel.clone(),
    ));

    settle_role(&node, &opts).await?;

    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    let n = opts.max_message_bytes;
    let svc = InterceptedService::new(
        ChronServer::new(service::ChronService::new(node.clone()))
            .max_decoding_message_size(n)
            .max_encoding_message_size(n),
        auth::BearerAuth::new(
            &[opts.token.as_deref(), opts.admin_token.as_deref()]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>(),
        ),
    );
    let (health, health_svc) = tonic_health::server::health_reporter();
    health
        .set_serving::<ChronServer<service::ChronService>>()
        .await;
    health.set_service_status("", ServingStatus::Serving).await;
    spawn_leader_health(health.clone(), node.watch_role(), cancel.clone());
    let mut builder = tonic::transport::Server::builder()
        .tcp_nodelay(true)
        .http2_keepalive_interval(Some(opts.keepalive_interval))
        .http2_keepalive_timeout(Some(opts.keepalive_timeout))
        .concurrency_limit_per_connection(opts.concurrency_per_connection)
        .layer(trace::layer());
    if let Some(t) = opts.request_timeout {
        builder = builder.timeout(t);
    }
    if let Some(tls) = &opts.tls {
        builder = builder.tls_config(tls.server_config()).context("tls")?;
    }
    let shutdown = cancel.clone().cancelled_owned();
    let server = tokio::spawn(
        builder
            .add_service(health_svc)
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown),
    );
    tracing::info!(%local_addr, path = %db.path().display(), head = db.head(), role = ?node.role(), epoch = db.epoch(), "listening");
    Ok(Running {
        local_addr,
        db,
        node,
        health,
        cancel,
        server,
    })
}

/// Reads the markers and settles the role: a replica (connected or not),
/// a promoted replica, a fenced leader, or a leader.
async fn settle_role(node: &Arc<Node>, opts: &Options) -> anyhow::Result<()> {
    let dir = opts.data_dir.display();
    if opts.replicate_from.is_some() {
        if opts.promote {
            anyhow::bail!("--promote and --replicate-from exclude each other");
        }
        if node.marker(PROMOTED_MARKER).exists() {
            anyhow::bail!(
                "{dir} was promoted to leader; remove {} to make it a replica again",
                node.marker(PROMOTED_MARKER).display()
            );
        }
        let stream = match replica::connect_and_check(node).await {
            Ok(s) => Some(s),
            Err(replica::CheckError::Unreachable(e)) => {
                tracing::warn!(error = %e, "starting disconnected; retrying");
                replica::record_error(node, e);
                None
            }
            Err(replica::CheckError::Refused(e)) => anyhow::bail!("cannot replicate: {e}"),
        };
        node.write_marker_text(
            REPLICA_MARKER,
            &format!("leader {}\n", node.leader_addr().unwrap_or("?")),
        )
        .context("write the replica marker")?;
        node.set_role(Role::Replica);
        node.remove_marker(FENCED_MARKER)
            .context("remove the fenced marker")?;
        *node.puller.lock().await = Some(replica::spawn(node.clone(), stream));
        return Ok(());
    }
    if let Some(text) = node.marker_text(REPLICA_MARKER) {
        let leader = text
            .trim()
            .strip_prefix("leader ")
            .unwrap_or("?")
            .to_string();
        if !opts.promote {
            anyhow::bail!(
                "{dir} is a replica of {leader}; start it with --replicate-from, or promote it with --promote"
            );
        }
        let db = node.db.clone();
        let epoch = db.epoch() + 1;
        node.write_marker(PROMOTED_MARKER, epoch)
            .context("write the promoted marker")?;
        tokio::task::spawn_blocking(move || db.set_epoch(epoch))
            .await
            .context("epoch task")?
            .context("raise the epoch")?;
        node.remove_marker(REPLICA_MARKER)
            .context("remove the replica marker")?;
        node.remove_marker(FENCED_MARKER)
            .context("remove the fenced marker")?;
        tracing::info!(epoch, %leader, "promoted at start; the old leader is not told");
        return Ok(());
    }
    if opts.promote {
        anyhow::bail!("{dir} is not a replica; nothing to promote");
    }
    if let Some(e) = node.read_marker(PROMOTED_MARKER)
        && e > node.db.epoch()
    {
        // A crash between the marker and the epoch.
        let db = node.db.clone();
        tokio::task::spawn_blocking(move || db.set_epoch(e))
            .await
            .context("epoch task")?
            .context("raise the epoch to the promoted marker's")?;
        tracing::warn!(epoch = e, "raised the epoch to the promoted marker's");
    }
    if let Some(by) = node.read_marker(FENCED_MARKER) {
        node.set_fenced_by(Some(by));
        node.set_role(Role::Fenced);
        tracing::warn!(
            by,
            "starting fenced: a newer leader exists; writes are refused"
        );
    }
    Ok(())
}

/// Keeps [`LEADER_HEALTH_SERVICE`] SERVING exactly while the role is
/// Leader.
fn spawn_leader_health(
    health: HealthReporter,
    mut role: tokio::sync::watch::Receiver<Role>,
    cancel: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            let status = if *role.borrow_and_update() == Role::Leader {
                ServingStatus::Serving
            } else {
                ServingStatus::NotServing
            };
            health
                .set_service_status(LEADER_HEALTH_SERVICE, status)
                .await;
            tokio::select! {
                _ = cancel.cancelled() => return,
                r = role.changed() => if r.is_err() { return },
            }
        }
    });
}

impl Running {
    /// Stops pulling, stops accepting, ends every stream, waits for the
    /// server, then flushes the database. The flush runs whatever came
    /// before; the first failure is returned.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.health
            .set_not_serving::<ChronServer<service::ChronService>>()
            .await;
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
        self.health
            .set_service_status(LEADER_HEALTH_SERVICE, ServingStatus::NotServing)
            .await;
        if let Some(p) = self.node.puller.lock().await.take() {
            p.stop().await;
        }
        if let Some(f) = self
            .node
            .fencer
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            f.abort();
        }
        self.cancel.cancel();
        let stopped = match tokio::time::timeout(Duration::from_secs(10), self.server).await {
            Ok(joined) => joined
                .context("server task")
                .and_then(|r| r.context("server")),
            Err(_) => Err(anyhow::anyhow!("the server did not stop within 10 s")),
        };
        let db = self.db;
        let flushed = tokio::task::spawn_blocking(move || db.flush())
            .await
            .context("flush task")
            .and_then(|r| r.context("flush"));
        stopped.and(flushed)
    }
}
