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

use crate::node::{FENCED_MARKER, Node, PROMOTED_MARKER, Role};

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
    /// The database's record limit; `None` for the library's default.
    pub max_record_bytes: Option<usize>,
    /// Serve TLS; `None` is plaintext.
    pub tls: Option<Tls>,
    /// Every `chron.v1.Chron` call must carry `authorization: Bearer
    /// <token>`. The health service stays open.
    pub token: Option<String>,
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
            max_record_bytes: None,
            tls: None,
            token: None,
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
/// [`Running::shutdown`]. A replica must reach its leader to start.
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
    if let Some(t) = &opts.token
        && t.is_empty()
    {
        anyhow::bail!("the token is empty");
    }
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
    let cancel = CancellationToken::new();
    let node = Arc::new(Node::new(
        db.clone(),
        opts.data_dir.clone(),
        opts.replicate_from.clone(),
        opts.replicate_token.clone(),
        cancel.clone(),
    ));

    if opts.replicate_from.is_some() {
        if node.marker(PROMOTED_MARKER).exists() {
            anyhow::bail!(
                "{} was promoted to leader; remove {} to make it a replica again",
                opts.data_dir.display(),
                node.marker(PROMOTED_MARKER).display()
            );
        }
        let stream = replica::connect_and_check(&node)
            .await
            .map_err(|e| anyhow::anyhow!("cannot replicate: {e}"))?;
        node.set_role(Role::Replica);
        node.remove_marker(FENCED_MARKER)
            .context("remove the fenced marker")?;
        *node.puller.lock().await = Some(replica::spawn(node.clone(), stream));
    } else if let Some(by) = node.read_marker(FENCED_MARKER) {
        node.set_fenced_by(Some(by));
        node.set_role(Role::Fenced);
        tracing::warn!(
            by,
            "starting fenced: a newer leader exists; writes are refused"
        );
    }

    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    let n = opts.max_message_bytes;
    let svc = InterceptedService::new(
        ChronServer::new(service::ChronService::new(node.clone()))
            .max_decoding_message_size(n)
            .max_encoding_message_size(n),
        auth::BearerAuth::new(opts.token.as_deref()),
    );
    let (health, health_svc) = tonic_health::server::health_reporter();
    health
        .set_serving::<ChronServer<service::ChronService>>()
        .await;
    health.set_service_status("", ServingStatus::Serving).await;
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

impl Running {
    /// Stops pulling, stops accepting, ends every stream, waits for the
    /// server, then flushes the database.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.health
            .set_not_serving::<ChronServer<service::ChronService>>()
            .await;
        self.health
            .set_service_status("", ServingStatus::NotServing)
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
        match tokio::time::timeout(Duration::from_secs(10), self.server).await {
            Ok(joined) => joined.context("server task")?.context("server")?,
            Err(_) => anyhow::bail!("the server did not stop within 10 s"),
        }
        let db = self.db;
        tokio::task::spawn_blocking(move || db.flush())
            .await
            .context("flush task")??;
        Ok(())
    }
}
