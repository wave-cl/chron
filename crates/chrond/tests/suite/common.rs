#![allow(dead_code)]

use chron_proto::v1::chron_client::ChronClient;
use chron_proto::v1::{self as pb, expected};
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use uuid::Uuid;

pub struct Server {
    pub dir: tempfile::TempDir,
    pub running: Option<chrond::Running>,
    pub client: ChronClient<Channel>,
}

impl Server {
    pub async fn start() -> Server {
        let dir = tempfile::tempdir().unwrap();
        let mut opts =
            chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
        opts.fsync = false;
        let running = chrond::start(opts).await.expect("the daemon starts");
        let client = ChronClient::connect(format!("http://{}", running.local_addr))
            .await
            .expect("connect");
        Server {
            dir,
            running: Some(running),
            client,
        }
    }

    /// Starts on a fresh directory without connecting a client (TLS or
    /// token setups connect their own).
    pub async fn start_with_no_client(f: impl FnOnce(&mut chrond::Options)) -> Server {
        let dir = tempfile::tempdir().unwrap();
        let mut opts =
            chrond::Options::new(dir.path().join("data"), "127.0.0.1:0".parse().unwrap());
        opts.fsync = false;
        f(&mut opts);
        let running = chrond::start(opts).await.expect("the daemon starts");
        // A lazy channel: never used by these tests.
        let client = ChronClient::new(Channel::from_static("http://127.0.0.1:1").connect_lazy());
        Server {
            dir,
            running: Some(running),
            client,
        }
    }

    /// Starts on a fresh directory with `f` adjusting the options.
    pub async fn start_with(f: impl FnOnce(&mut chrond::Options)) -> Server {
        let dir = tempfile::tempdir().unwrap();
        Self::start_in(dir, "127.0.0.1:0".parse().unwrap(), f).await
    }

    /// Starts on an existing temp dir (its `data` subdirectory) at `addr`.
    pub async fn start_in(
        dir: tempfile::TempDir,
        addr: std::net::SocketAddr,
        f: impl FnOnce(&mut chrond::Options),
    ) -> Server {
        Self::try_start_in(dir, addr, f)
            .await
            .expect("the daemon starts")
    }

    pub async fn try_start_in(
        dir: tempfile::TempDir,
        addr: std::net::SocketAddr,
        f: impl FnOnce(&mut chrond::Options),
    ) -> anyhow::Result<Server> {
        let mut opts = chrond::Options::new(dir.path().join("data"), addr);
        opts.fsync = false;
        f(&mut opts);
        let running = chrond::start(opts).await?;
        let client = ChronClient::connect(format!("http://{}", running.local_addr))
            .await
            .expect("connect");
        Ok(Server {
            dir,
            running: Some(running),
            client,
        })
    }

    pub fn addr(&self) -> String {
        format!("http://{}", self.running.as_ref().unwrap().local_addr)
    }

    /// A fresh replica of `leader`.
    pub async fn start_replica_of(leader: &Server) -> Server {
        let addr = leader.addr();
        Self::start_with(move |o| o.replicate_from = Some(addr)).await
    }

    pub fn data_dir(&self) -> std::path::PathBuf {
        self.dir.path().join("data")
    }

    pub async fn info(&mut self) -> pb::InfoResponse {
        self.client
            .info(pb::InfoRequest {})
            .await
            .unwrap()
            .into_inner()
    }

    /// Stops the daemon and keeps the directory and client out of the way.
    pub async fn stop_keep_dir(mut self) -> tempfile::TempDir {
        self.running.take().unwrap().shutdown().await.unwrap();
        self.dir
    }

    /// Stops the daemon and hands back the directory so a test can reopen
    /// the database after the shutdown.
    pub async fn stop(mut self) -> tempfile::TempDir {
        self.running.take().unwrap().shutdown().await.unwrap();
        self.dir
    }
}

/// Backups under `<tempdir>/backups`, beside the data directory.
pub fn with_backup_dir(o: &mut chrond::Options) {
    o.backup_dir = Some(o.data_dir.parent().unwrap().join("backups"));
}

/// A free TCP port: bound then released, so a daemon can be restarted on it.
pub fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Polls `f` every 20 ms for up to 10 s.
pub async fn wait_until<F, Fut>(what: &str, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        if f().await {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

pub async fn read_all_payloads(client: &mut ChronClient<Channel>) -> Vec<String> {
    let events = collect(
        client
            .read_all(pb::ReadAllRequest {
                from: 0,
                limit: 0,
                backward: false,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await
    .unwrap();
    payloads(&events)
}

/// Adds `authorization: Bearer <token>` to every call.
#[derive(Clone)]
pub struct TokenInterceptor(pub tonic::metadata::MetadataValue<tonic::metadata::Ascii>);

impl tonic::service::Interceptor for TokenInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut().insert("authorization", self.0.clone());
        Ok(req)
    }
}

pub async fn plain_channel(addr: std::net::SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}

pub fn with_token(
    channel: Channel,
    token: &str,
) -> ChronClient<tonic::service::interceptor::InterceptedService<Channel, TokenInterceptor>> {
    ChronClient::with_interceptor(
        channel,
        TokenInterceptor(format!("Bearer {token}").parse().unwrap()),
    )
}

pub fn agg(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

pub fn new_event(payload: &str, tags: &[&str]) -> pb::NewEvent {
    pb::NewEvent {
        tags: tags.iter().map(|t| t.as_bytes().to_vec()).collect(),
        metadata: Vec::new(),
        payload: payload.as_bytes().to_vec(),
    }
}

pub fn any() -> Option<pb::Expected> {
    Some(pb::Expected {
        kind: Some(expected::Kind::Any(expected::Any {})),
    })
}

pub fn new() -> Option<pb::Expected> {
    Some(pb::Expected {
        kind: Some(expected::Kind::New(expected::New {})),
    })
}

pub fn exact(n: u64) -> Option<pb::Expected> {
    Some(pb::Expected {
        kind: Some(expected::Kind::Exact(n)),
    })
}

pub fn append(
    aggregate: Option<Uuid>,
    expected: Option<pb::Expected>,
    events: Vec<pb::NewEvent>,
) -> pb::Append {
    pb::Append {
        aggregate: aggregate.map(|a| a.as_bytes().to_vec()),
        expected,
        events,
    }
}

/// Commits one aggregate append with `Any`.
pub async fn fill(client: &mut ChronClient<Channel>, aggregate: Uuid, n: usize) {
    for i in 0..n {
        client
            .commit(pb::CommitRequest {
                appends: vec![append(
                    Some(aggregate),
                    any(),
                    vec![new_event(&format!("e{i}"), &[])],
                )],
                epoch: None,
            })
            .await
            .unwrap();
    }
}

pub async fn collect(stream: tonic::Streaming<pb::Event>) -> Result<Vec<pb::Event>, tonic::Status> {
    let mut stream = stream;
    let mut out = Vec::new();
    while let Some(item) = stream.next().await {
        out.push(item?);
    }
    Ok(out)
}

pub fn positions(events: &[pb::Event]) -> Vec<u64> {
    events.iter().map(|e| e.position).collect()
}

pub fn payloads(events: &[pb::Event]) -> Vec<String> {
    events
        .iter()
        .map(|e| String::from_utf8(e.payload.clone()).unwrap())
        .collect()
}
