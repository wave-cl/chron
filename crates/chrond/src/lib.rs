//! `chrond`: the chron databases under one data directory behind the
//! `chron.v1` gRPC service, as a leader or as a replica of another daemon.
//! [`start`] serves them; the `chrond` binary wraps that with flags and
//! signal handling.

mod auth;
mod cluster;
mod codec;
mod http;
mod http_json;
pub mod logging;
pub mod metrics;
mod metrics_http;
pub mod node;
mod ops;
mod replica;
mod rpc_metrics;
mod service;
mod sync;
#[cfg(feature = "test-util")]
pub mod testing;
mod trace;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
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

use crate::node::{
    ClusterConfig, DbNode, FENCED_MARKER, Node, PROMOTED_MARKER, REPLICA_MARKER, Role, Upstream,
};
pub use crate::node::{ReadConsistency, SyncCommit};

/// The health service name that is SERVING only while this node is the
/// leader (`chron.v1.Chron` itself is SERVING in every role).
pub const LEADER_HEALTH_SERVICE: &str = "chron.v1.Chron.leader";

/// How the daemon opens and serves its databases.
#[derive(Debug, Clone)]
pub struct Options {
    /// Holds one directory per database (`default` always); a 0.1.0
    /// directory that is itself a database is moved into `default/` on
    /// the first start.
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
    /// Transactions one group-commit batch may land; `None` for the
    /// library's default (64).
    pub group_commit_max_txns: Option<usize>,
    /// Bytes at which a batch stops growing; `None` for the library's
    /// default (4 MiB).
    pub group_commit_max_bytes: Option<usize>,
    /// Batches between durable index commits under fsync; `None` for
    /// the library's default (256). `1` makes every batch's index
    /// commit durable: two fsyncs per batch, the pre-0.5 behaviour.
    pub index_checkpoint_batches: Option<usize>,
    /// Bytes at which a segment rolls; `None` for the library's default
    /// (256 MiB). Retention drops whole segments, so a small value lets
    /// a test raise the floor.
    pub segment_max_bytes: Option<u64>,
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
    /// How often a replica asks its leader for the list of databases
    /// (it also asks whenever a puller reconnects).
    pub discover_interval: Duration,
    /// Serve Prometheus metrics at `GET /metrics` on this address (plain
    /// HTTP/1, no token, no TLS: keep it private). Off when `None`.
    pub metrics_listen: Option<SocketAddr>,
    /// Serve the HTTP/JSON gateway on this address, with the same tokens
    /// and TLS as `listen`. Off when `None`.
    pub http_listen: Option<SocketAddr>,
    /// The other nodes of a failover group, by address. With any, this
    /// node never starts as a leader: it follows the leader it hears or
    /// is elected by a majority. `replicate_token` and `replicate_tls`
    /// are used towards them.
    pub peers: Vec<String>,
    /// This node's address as its peers reach it; defaults to
    /// `http(s)://<listen>`, which needs a concrete address and port.
    pub advertise: Option<String>,
    /// How long a majority's acknowledgment lets a leader accept writes;
    /// heartbeats go out every third of it and an election starts after
    /// 1.1 to 1.5 times it without a leader. Every node of a group should
    /// run the same value; a follower waits out the longer of its own
    /// and its leader's.
    pub lease_ttl: Duration,
    /// With peers: whether a commit is acknowledged only once a majority
    /// of the group durably holds it (the default) or once it is on this
    /// node's disk. A request's `sync` flag overrides it. Without peers
    /// every commit is local.
    pub sync_commit: SyncCommit,
    /// How long a sync commit waits for a majority before it answers
    /// DEADLINE_EXCEEDED "unconfirmed"; `None` is the lease ttl. A
    /// majority that cannot apply within a lease cannot keep the lease
    /// either, so longer waits only wait for the step-down.
    pub sync_timeout: Option<Duration>,
    /// What reads, head and subscriptions see when the request does not
    /// say: `None` (the default) is confirmed (only positions a majority
    /// of the group durably holds) when sync commits are in effect,
    /// local (this node's head) otherwise; set to force one.
    pub read_consistency: Option<ReadConsistency>,
    /// Peers this node neither calls nor answers from the start (a
    /// simulated partition for tests).
    #[cfg(feature = "test-util")]
    pub partition: Vec<String>,
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
            group_commit_max_txns: None,
            group_commit_max_bytes: None,
            index_checkpoint_batches: None,
            segment_max_bytes: None,
            tls: None,
            token: None,
            admin_token: None,
            backup_dir: None,
            max_message_bytes: 64 * 1024 * 1024,
            request_timeout: None,
            concurrency_per_connection: 256,
            keepalive_interval: Duration::from_secs(30),
            keepalive_timeout: Duration::from_secs(10),
            discover_interval: Duration::from_secs(10),
            metrics_listen: None,
            http_listen: None,
            peers: Vec::new(),
            advertise: None,
            lease_ttl: Duration::from_secs(10),
            sync_commit: SyncCommit::Majority,
            read_consistency: None,
            sync_timeout: None,
            #[cfg(feature = "test-util")]
            partition: Vec::new(),
        }
    }
}

/// A serving daemon. Dropping it does not stop the server; call
/// [`shutdown`](Running::shutdown).
pub struct Running {
    pub local_addr: SocketAddr,
    /// Where `/metrics` is served, when `Options::metrics_listen` was set.
    pub metrics_addr: Option<SocketAddr>,
    /// Where the HTTP/JSON gateway is served, when `Options::http_listen`
    /// was set.
    pub http_addr: Option<SocketAddr>,
    /// The `default` database; tests commit through it directly.
    pub db: Db,
    pub node: Arc<Node>,
    health: HealthReporter,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
    metrics_server: Option<JoinHandle<()>>,
    http_server: Option<JoinHandle<()>>,
}

/// Opens (and recovers) every database under `data_dir`, settles the
/// node's role, binds `opts.listen` (port 0 picks a free port) and serves
/// until [`Running::shutdown`]. A replica whose leader is unreachable
/// starts disconnected and keeps trying; one its leader refuses does not
/// start.
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
    if let Some(n) = opts.group_commit_max_txns {
        db_opts = db_opts.group_commit_max_txns(n);
    }
    if let Some(n) = opts.group_commit_max_bytes {
        db_opts = db_opts.group_commit_max_bytes(n);
    }
    if let Some(n) = opts.segment_max_bytes {
        db_opts = db_opts.segment_max_bytes(n);
    }
    if let Some(n) = opts.index_checkpoint_batches {
        db_opts = db_opts.index_checkpoint_batches(n);
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
    if opts.max_message_bytes < db_opts.max_record_bytes {
        anyhow::bail!(
            "max_message_bytes ({}) is below the database's max_record_bytes ({})",
            opts.max_message_bytes,
            db_opts.max_record_bytes
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
    let scheme = if opts.tls.is_some() { "https" } else { "http" };
    let advertise = match &opts.advertise {
        Some(a) => a.clone(),
        None => format!("{scheme}://{}", opts.listen),
    };
    let cluster = if opts.peers.is_empty() {
        None
    } else {
        if opts.replicate_from.is_some() {
            anyhow::bail!("--peer replaces --replicate-from: peers find their leader themselves");
        }
        if opts.advertise.is_none()
            && (opts.listen.port() == 0 || opts.listen.ip().is_unspecified())
        {
            anyhow::bail!(
                "--advertise is needed with --peer when --listen is {}: peers cannot reach that",
                opts.listen
            );
        }
        if opts.lease_ttl < Duration::from_millis(100) {
            anyhow::bail!("--lease-ttl below 100 ms cannot carry a heartbeat round trip");
        }
        if opts.peers.contains(&advertise) {
            anyhow::bail!("--peer lists this node's own address {advertise}");
        }
        if let Some(p) = opts
            .peers
            .iter()
            .find(|p| !(p.starts_with("http://") || p.starts_with("https://")))
        {
            anyhow::bail!("--peer {p:?} is not an http:// or https:// address");
        }
        let sync_timeout = opts.sync_timeout.unwrap_or(opts.lease_ttl);
        if sync_timeout.is_zero() {
            anyhow::bail!("--sync-timeout 0 would answer every sync commit as unconfirmed");
        }
        if let Some(t) = opts.request_timeout
            && t <= sync_timeout
        {
            anyhow::bail!(
                "--request-timeout ({t:?}) must exceed --sync-timeout ({sync_timeout:?}), or sync commits end as request timeouts with no answer"
            );
        }
        Some(ClusterConfig {
            seeds: opts.peers.clone(),
            advertise: advertise.clone(),
            token: opts.replicate_token.clone(),
            tls: opts.replicate_tls.clone(),
            max_message_bytes: opts.max_message_bytes,
            lease_ttl: opts.lease_ttl,
            sync_commit: opts.sync_commit,
            sync_timeout,
        })
    };
    let cancel = CancellationToken::new();
    let upstream = opts.replicate_from.clone().map(|addr| Upstream {
        addr,
        token: opts.replicate_token.clone(),
        tls: opts.replicate_tls.clone(),
        max_message_bytes: opts.max_message_bytes,
    });
    let metrics = metrics::Metrics::new();
    let node = Arc::new(Node::new(
        opts.data_dir.clone(),
        backup_dir,
        opts.admin_token.as_deref(),
        upstream,
        cluster,
        opts.max_message_bytes,
        db_opts,
        opts.read_consistency,
        opts.discover_interval,
        metrics.clone(),
        cancel.clone(),
    ));
    metrics.attach(&node);
    #[cfg(feature = "test-util")]
    {
        *node.partition.lock().unwrap_or_else(|p| p.into_inner()) =
            opts.partition.iter().cloned().collect();
    }
    let dir = opts.data_dir.clone();
    tokio::task::spawn_blocking(move || migrate_layout(&dir))
        .await
        .context("migration task")??;
    open_all(&node).await?;
    let db = node.default_db().db.clone();

    settle_role(&node, &opts).await?;

    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    node.set_advertise(match &opts.advertise {
        Some(a) => a.clone(),
        None => format!("{scheme}://{local_addr}"),
    });
    let n = opts.max_message_bytes;
    let bearer = auth::BearerAuth::new(
        &[opts.token.as_deref(), opts.admin_token.as_deref()]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>(),
    );
    let svc = InterceptedService::new(
        ChronServer::new(service::ChronService::new(node.clone()))
            .max_decoding_message_size(n)
            .max_encoding_message_size(n),
        bearer.clone(),
    );
    let (http_addr, http_server) = match opts.http_listen {
        Some(addr) => {
            if !addr.ip().is_loopback() && opts.token.is_none() && opts.tls.is_none() {
                tracing::warn!(%addr, "the HTTP gateway listens beyond loopback with no token and no TLS");
            }
            let tls = match &opts.tls {
                Some(t) => Some(http::rustls_config(t).context("http tls")?),
                None => None,
            };
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("cannot listen on {addr} for the HTTP gateway"))?;
            let local = l.local_addr()?;
            let router = http::router(
                node.clone(),
                metrics.clone(),
                bearer.clone(),
                opts.request_timeout,
            );
            let task = tokio::spawn(http::serve(l, router, tls, cancel.clone()));
            (Some(local), Some(task))
        }
        None => (None, None),
    };
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
        .layer(trace::layer())
        .layer(rpc_metrics::RpcMetricsLayer::new(
            metrics.clone(),
            node.clone(),
        ));
    let (metrics_addr, metrics_server) = match opts.metrics_listen {
        Some(addr) => {
            if !addr.ip().is_loopback() {
                tracing::warn!(%addr, "metrics listen beyond loopback with no token: head, role, lag and version are visible to anyone who can reach it");
            }
            let l = TcpListener::bind(addr)
                .await
                .with_context(|| format!("cannot listen on {addr} for metrics"))?;
            let local = l.local_addr()?;
            let task = tokio::spawn(metrics_http::serve(l, metrics.clone(), cancel.clone()));
            (Some(local), Some(task))
        }
        None => (None, None),
    };
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
    if node.cluster.is_some() {
        tracing::info!(members = %node.members().oneline(), "the group's membership");
        *node.cluster_task.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(cluster::spawn(node.clone()));
        if opts.promote {
            let n = node.clone();
            tokio::spawn(async move {
                match cluster::campaign(&n, true).await {
                    Ok((epoch, head)) => tracing::info!(epoch, head, "promoted at start"),
                    Err(s) => tracing::warn!(reason = s.message(), "--promote: not elected"),
                }
            });
        }
    }
    let names: Vec<String> = node.databases().iter().map(|d| d.name.clone()).collect();
    tracing::info!(%local_addr, metrics = ?metrics_addr, http = ?http_addr, data_dir = %opts.data_dir.display(), databases = %names.join(","), head = db.head(), role = ?node.role(), epoch = db.epoch(), peers = %opts.peers.join(","), sync_commit = ?opts.sync_commit, "listening");
    Ok(Running {
        local_addr,
        metrics_addr,
        http_addr,
        db,
        node,
        health,
        cancel,
        server,
        metrics_server,
        http_server,
    })
}

/// A 0.1.0 data directory holds one database at its root; the layout is
/// now one directory per database. The root's `index.redb` and
/// `segments` move into `default/` (each rename is idempotent, so a crash
/// midway resumes), a stale root `LOCK` goes, markers stay at the root.
fn migrate_layout(data_dir: &Path) -> anyhow::Result<()> {
    let index = data_dir.join("index.redb");
    let segments = data_dir.join("segments");
    if !index.exists() && !segments.exists() {
        return Ok(());
    }
    let target = data_dir.join(chron_proto::DEFAULT_DATABASE);
    std::fs::create_dir_all(&target)
        .with_context(|| format!("cannot create {}", target.display()))?;
    for (from, name) in [(&index, "index.redb"), (&segments, "segments")] {
        if !from.exists() {
            continue;
        }
        let to = target.join(name);
        if to.exists() {
            anyhow::bail!(
                "both {} and {} exist: the 0.1.0 layout was only half moved into default/; resolve by hand",
                from.display(),
                to.display()
            );
        }
        std::fs::rename(from, &to)
            .with_context(|| format!("cannot move {} to {}", from.display(), to.display()))?;
    }
    match std::fs::remove_file(data_dir.join("LOCK")) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("remove the root LOCK"),
    }
    for d in [data_dir, target.as_path()] {
        std::fs::File::open(d)?.sync_all()?;
    }
    tracing::info!(data_dir = %data_dir.display(), "moved the 0.1.0 layout into default/");
    Ok(())
}

/// Opens every database directory under `data_dir` (creating `default`
/// when missing) and settles their epochs on the highest one.
/// Finishes or undoes a swap a crash interrupted: `<name>.fetched`
/// without `<name>` is a restored copy whose old directory was already
/// moved aside, so it takes its place; with `<name>` present it is an
/// unfinished fetch and goes; a `.<name>.fetched.restore` staging
/// directory goes. `<name>.replaced-*` directories are never touched.
fn repair_swaps(data_dir: &Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(stem) = file_name
            .strip_suffix(".fetched.restore")
            .and_then(|s| s.strip_prefix('.'))
        {
            tracing::warn!(database = %stem, "removing a restore a crash interrupted");
            std::fs::remove_dir_all(entry.path())?;
        } else if let Some(name) = file_name.strip_suffix(".fetched") {
            let target = data_dir.join(name);
            if target.exists() {
                tracing::warn!(database = %name, "removing a fetched copy a crash left beside the database");
                std::fs::remove_dir_all(entry.path())?;
            } else {
                tracing::warn!(database = %name, "completing a swap a crash interrupted: the fetched copy takes the database's place");
                std::fs::rename(entry.path(), &target)?;
                std::fs::File::open(data_dir)?.sync_all()?;
            }
        }
    }
    Ok(())
}

async fn open_all(node: &Arc<Node>) -> anyhow::Result<()> {
    std::fs::create_dir_all(&node.data_dir)
        .with_context(|| format!("cannot create {}", node.data_dir.display()))?;
    repair_swaps(&node.data_dir).context("repair an interrupted restore")?;
    let mut names: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&node.data_dir)
        .with_context(|| format!("cannot read {}", node.data_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if chron_proto::valid_database_name(&name) {
            names.push(name);
        } else if name.contains(".replaced-") {
            tracing::info!(entry = %name, "a database copy kept aside by a restore; remove it when no longer needed");
        } else {
            tracing::warn!(entry = %name, "not a database name; ignored");
        }
    }
    if !names.iter().any(|n| n == chron_proto::DEFAULT_DATABASE) {
        names.push(chron_proto::DEFAULT_DATABASE.to_string());
    }
    names.sort();
    let tombstones = node.tombstones();
    for name in names {
        let n = node.clone();
        let name2 = name.clone();
        let db = tokio::task::spawn_blocking(move || n.open_database(&name2))
            .await
            .context("open task")?
            .with_context(|| {
                format!(
                    "cannot open the database at {}",
                    node.database_dir(&name).display()
                )
            })?;
        if name != chron_proto::DEFAULT_DATABASE && tombstones.iter().any(|t| t.db_id == db.id()) {
            // A drop a crash interrupted after the tombstone: finished
            // now.
            tracing::warn!(database = %name, "dropped before the last stop; removing its directory");
            drop(db);
            let dir = node.database_dir(&name);
            let n = node.clone();
            let name2 = name.clone();
            tokio::task::spawn_blocking(move || {
                std::fs::remove_dir_all(&dir)?;
                n.remove_marker(&format!("{name2}.options"))
            })
            .await
            .context("remove task")?
            .with_context(|| format!("cannot remove the dropped database {name}"))?;
            continue;
        }
        node.insert(DbNode::new(name, db, node));
    }
    let max = node
        .databases()
        .iter()
        .map(|d| d.db.epoch())
        .max()
        .unwrap_or(0);
    for d in node.databases() {
        if d.db.epoch() < max && node.cluster.is_some() {
            // With peers a database's epoch says what it holds; raising it
            // would let the node claim an epoch it has not caught up to.
            tracing::info!(database = %d.name, epoch = d.db.epoch(), daemon = max, "a database behind the daemon's epoch; it catches up by replication");
            continue;
        }
        if d.db.epoch() < max {
            tracing::warn!(database = %d.name, from = d.db.epoch(), to = max, "raising a lagging epoch to the daemon's");
            let db = d.db.clone();
            tokio::task::spawn_blocking(move || db.set_epoch(max))
                .await
                .context("epoch task")?
                .context("raise a lagging epoch")?;
        }
    }
    Ok(())
}

/// Reads the markers and settles the role: a replica (connected or not),
/// a promoted replica, a fenced leader, or a leader.
async fn settle_role(node: &Arc<Node>, opts: &Options) -> anyhow::Result<()> {
    let dir = opts.data_dir.display();
    // The membership file wins over the flags once it exists; the flags
    // seed it; a grouped directory refuses to start without them.
    let file = node.read_members_file().map_err(|e| {
        anyhow::anyhow!("{e}: not `epoch N / version N / member ADDR...`; fix or remove the file")
    })?;
    match (&file, &node.cluster) {
        (Some(m), Some(c)) => {
            let seeded: std::collections::BTreeSet<String> = opts
                .peers
                .iter()
                .cloned()
                .chain(std::iter::once(c.advertise.clone()))
                .collect();
            if seeded != m.addrs {
                tracing::warn!(file = %m.oneline(), flags = %seeded.iter().cloned().collect::<Vec<_>>().join(","), "ignoring --peer: the members file is the membership");
            }
            node.install_members(m.clone());
        }
        (Some(m), None) => {
            anyhow::bail!(
                "{dir} belongs to a failover group (membership version {}: {}): start it with --peer, or remove {} to run it alone",
                m.version,
                m.addrs.iter().cloned().collect::<Vec<_>>().join(","),
                node.marker(node::MEMBERS_FILE).display()
            );
        }
        (None, Some(c)) => {
            let m = node::Members {
                epoch: 0,
                version: 1,
                addrs: opts
                    .peers
                    .iter()
                    .cloned()
                    .chain(std::iter::once(c.advertise.clone()))
                    .collect(),
            };
            let n = node.clone();
            let m2 = m.clone();
            tokio::task::spawn_blocking(move || n.persist_members(&m2))
                .await
                .context("members task")?
                .context("write the members file")?;
            node.install_members(m);
        }
        (None, None) => {}
    }
    if node.cluster.is_some() {
        if let Some(text) = node.marker_text(node::VOTE_MARKER) {
            let v = node::parse_vote(&text).ok_or_else(|| {
                anyhow::anyhow!(
                    "{dir}/vote holds {text:?}, not `epoch N for ADDR`; a node that cannot read its vote could vote twice: fix or remove the file"
                )
            })?;
            node.with_lease(|l| l.voted = Some(v));
        }
        if let Some(e) = node.read_marker(PROMOTED_MARKER)
            && e > node.epoch()
        {
            let n = node.clone();
            tokio::task::spawn_blocking(move || n.raise_all_epochs(e))
                .await
                .context("epoch task")?
                .context("raise the epoch to the promoted marker's")?;
            tracing::warn!(epoch = e, "raised every epoch to the promoted marker's");
        }
        if let Some(by) = node.read_marker(FENCED_MARKER) {
            node.set_fenced_by(Some(by));
            node.set_role(Role::Fenced);
            tracing::warn!(by, "starting fenced; the next leader heard is followed");
        } else {
            node.set_role(Role::Candidate);
            tracing::info!(
                "starting as a candidate: following the first leader heard, or electing one"
            );
        }
        if !node.is_member() {
            node.set_role(Role::Removed);
            tracing::warn!(
                "not in the membership this directory holds: starting removed (reads only) until added"
            );
        }
        return Ok(());
    }
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
        let mut streams = Vec::new();
        for dbn in node.databases() {
            let stream = match replica::connect_and_check(node, &dbn).await {
                Ok(s) => Some(s),
                Err(replica::CheckError::Unreachable(e)) => {
                    tracing::warn!(database = %dbn.name, error = %e, "starting disconnected; retrying");
                    replica::record_error(node, &dbn, e);
                    None
                }
                Err(replica::CheckError::Refused(e)) => {
                    anyhow::bail!("cannot replicate {}: {e}", dbn.name)
                }
                Err(e @ replica::CheckError::Diverged { .. }) => {
                    anyhow::bail!("cannot replicate {}: {}", dbn.name, e.message())
                }
                Err(e @ replica::CheckError::BelowFloor { .. }) => {
                    anyhow::bail!(
                        "cannot replicate {}: {}; fetch a backup with `chron --addr LEADER backup --fetch FILE --db {}`, `chron restore FILE DIR/{}`, then start again",
                        dbn.name,
                        e.message(),
                        dbn.name,
                        dbn.name
                    )
                }
            };
            streams.push((dbn, stream));
        }
        node.write_marker_text(
            REPLICA_MARKER,
            &format!("leader {}\n", node.leader_addr().as_deref().unwrap_or("?")),
        )
        .context("write the replica marker")?;
        node.set_role(Role::Replica);
        node.remove_marker(FENCED_MARKER)
            .context("remove the fenced marker")?;
        for (dbn, stream) in streams {
            *dbn.puller.lock().await = Some(replica::spawn(node.clone(), dbn.clone(), stream));
        }
        *node.discoverer.lock().await = Some(replica::spawn_discoverer(node.clone()));
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
        let n = node.clone();
        let epoch = node
            .databases()
            .iter()
            .map(|d| d.db.epoch())
            .max()
            .unwrap_or(0)
            + 1;
        node.write_marker(PROMOTED_MARKER, epoch)
            .context("write the promoted marker")?;
        tokio::task::spawn_blocking(move || n.raise_all_epochs(epoch))
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
        && e > node.epoch()
    {
        // A crash between the marker and the epochs.
        let n = node.clone();
        tokio::task::spawn_blocking(move || n.raise_all_epochs(e))
            .await
            .context("epoch task")?
            .context("raise the epoch to the promoted marker's")?;
        tracing::warn!(epoch = e, "raised every epoch to the promoted marker's");
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
        // Cluster calls are refused and no transition starts from here on;
        // the cluster task is stopped and awaited before the pullers, so
        // nothing restarts them.
        self.node
            .stopping
            .store(true, std::sync::atomic::Ordering::Release);
        let cluster_task = self
            .node
            .cluster_task
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(c) = cluster_task {
            c.abort();
            let _ = c.await;
        }
        let _transitions = self.node.transitions.lock().await;
        self.health
            .set_not_serving::<ChronServer<service::ChronService>>()
            .await;
        self.health
            .set_service_status("", ServingStatus::NotServing)
            .await;
        self.health
            .set_service_status(LEADER_HEALTH_SERVICE, ServingStatus::NotServing)
            .await;
        replica::stop_all(&self.node).await;
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
        if let Some(m) = self.metrics_server {
            let _ = tokio::time::timeout(Duration::from_secs(5), m).await;
        }
        if let Some(h) = self.http_server {
            let _ = tokio::time::timeout(Duration::from_secs(5), h).await;
        }
        let mut flushed = Ok(());
        for dbn in self.node.databases() {
            let db = dbn.db.clone();
            let r = tokio::task::spawn_blocking(move || db.flush())
                .await
                .context("flush task")
                .and_then(|r| r.with_context(|| format!("flush {}", dbn.name)));
            flushed = flushed.and(r);
        }
        stopped.and(flushed)
    }
}
