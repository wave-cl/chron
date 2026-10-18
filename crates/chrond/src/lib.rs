//! `chrond`: one chron database behind the `chron.v1` gRPC service.
//! [`start`] serves it; the `chrond` binary wraps that with flags and
//! signal handling.

mod codec;
mod service;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use chron::{Db, Fsync};
use chron_proto::v1::chron_server::ChronServer;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

/// How the daemon opens and serves its database.
#[derive(Debug, Clone)]
pub struct Options {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// `false` for [`Fsync::Never`].
    pub fsync: bool,
    pub verify_all_segments: bool,
}

impl Options {
    pub fn new(data_dir: impl Into<PathBuf>, listen: SocketAddr) -> Self {
        Options {
            data_dir: data_dir.into(),
            listen,
            fsync: true,
            verify_all_segments: false,
        }
    }
}

/// A serving daemon. Dropping it does not stop the server; call
/// [`shutdown`](Running::shutdown).
pub struct Running {
    pub local_addr: SocketAddr,
    /// The database being served; tests commit through it directly.
    pub db: Db,
    cancel: CancellationToken,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

/// Opens (and recovers) the database, binds `opts.listen` (port 0 picks a
/// free port) and serves until [`Running::shutdown`].
pub async fn start(opts: Options) -> anyhow::Result<Running> {
    let db_opts = chron::Options::new()
        .fsync(if opts.fsync {
            Fsync::Always
        } else {
            Fsync::Never
        })
        .verify_all_segments(opts.verify_all_segments);
    let dir = opts.data_dir.clone();
    let db = tokio::task::spawn_blocking(move || Db::open(&dir, db_opts))
        .await
        .context("open task")?
        .with_context(|| format!("cannot open the database at {}", opts.data_dir.display()))?;
    let listener = TcpListener::bind(opts.listen)
        .await
        .with_context(|| format!("cannot listen on {}", opts.listen))?;
    let local_addr = listener.local_addr()?;
    let cancel = CancellationToken::new();
    let svc = ChronServer::new(service::ChronService::new(db.clone(), cancel.clone()));
    let shutdown = cancel.clone().cancelled_owned();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown),
    );
    tracing::info!(%local_addr, path = %db.path().display(), head = db.head(), "listening");
    Ok(Running {
        local_addr,
        db,
        cancel,
        server,
    })
}

impl Running {
    /// Stops accepting, ends every stream, waits for the server, then
    /// flushes the database.
    pub async fn shutdown(self) -> anyhow::Result<()> {
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
