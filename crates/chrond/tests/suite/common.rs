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

    pub fn data_dir(&self) -> std::path::PathBuf {
        self.dir.path().join("data")
    }

    /// Stops the daemon and hands back the directory so a test can reopen
    /// the database after the shutdown.
    pub async fn stop(mut self) -> tempfile::TempDir {
        self.running.take().unwrap().shutdown().await.unwrap();
        self.dir
    }
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
