//! The replica side: connect to the leader, check the two histories agree,
//! then pull chunks for as long as the daemon runs.

use std::sync::Arc;
use std::time::Duration;

use chron::Uuid;
use chron_proto::v1 as pb;
use chron_proto::v1::chron_client::ChronClient;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status, Streaming};

use crate::node::{Node, Role};

const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Sends `authorization: Bearer …` when a token is configured.
#[derive(Clone)]
pub struct Token(Option<MetadataValue<Ascii>>);

impl Token {
    pub fn new(token: Option<&str>) -> Result<Token, String> {
        match token {
            None => Ok(Token(None)),
            Some(t) => format!("Bearer {t}")
                .parse()
                .map(|v| Token(Some(v)))
                .map_err(|_| "the replication token is not valid header text".to_string()),
        }
    }
}

impl Interceptor for Token {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

pub type LeaderClient = ChronClient<InterceptedService<Channel, Token>>;

pub async fn connect(addr: &str, token: Option<&str>) -> Result<LeaderClient, String> {
    let endpoint = Endpoint::from_shared(addr.to_string())
        .map_err(|e| format!("{addr} is not a valid address: {e}"))?
        .connect_timeout(Duration::from_secs(3));
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("cannot reach the leader at {addr}: {e}"))?;
    Ok(ChronClient::with_interceptor(channel, Token::new(token)?))
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> chron::Result<T> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| format!("task: {e}"))?
        .map_err(|e| e.to_string())
}

/// Connects to the leader and checks this database can follow it: same
/// identity (an empty database adopts the leader's), an epoch no newer
/// than the leader's, a head no further, and the same record before it.
/// Returns the open chunk stream.
pub async fn connect_and_check(
    node: &Arc<Node>,
) -> Result<Streaming<pb::ReplicationChunk>, String> {
    let addr = node.replicate_from.clone().ok_or("no leader configured")?;
    let mut client = connect(&addr, node.replicate_token.as_deref()).await?;
    let info = client
        .info(pb::InfoRequest {})
        .await
        .map_err(|s| format!("Info on {addr}: {}: {}", s.code(), s.message()))?
        .into_inner();
    if info.role() != pb::Role::Leader {
        return Err(format!("{addr} is not a leader (role {:?})", info.role()));
    }
    let leader_id = Uuid::from_slice(&info.db_id)
        .map_err(|_| format!("{addr} reports no database identity"))?;
    let db = node.db.clone();
    let (head, floor, id, epoch) = (db.head(), db.floor(), db.id(), db.epoch());
    if id != leader_id {
        if head == 0 && floor == 0 {
            let db = db.clone();
            blocking(move || db.adopt_id(leader_id)).await?;
        } else {
            return Err(format!(
                "{} is database {id}; {addr} serves database {leader_id}; a replica must be restored from a backup of the leader",
                node.data_dir.display()
            ));
        }
    }
    if epoch > info.epoch {
        return Err(format!(
            "local epoch {epoch} is newer than the leader's {}",
            info.epoch
        ));
    }
    if epoch < info.epoch {
        let db = db.clone();
        let e = info.epoch;
        blocking(move || db.set_epoch(e)).await?;
    }
    if head > info.head {
        return Err(format!(
            "diverged: this replica is at head {head}, the leader at {}; reset it from a backup of the leader",
            info.head
        ));
    }
    let last_fingerprint = if head > 0 {
        let db = db.clone();
        blocking(move || db.fingerprint(head - 1)).await?
    } else {
        None
    };
    client
        .replicate(pb::ReplicateRequest {
            from_position: head,
            last_fingerprint,
        })
        .await
        .map(|r| r.into_inner())
        .map_err(|s| format!("Replicate on {addr}: {}: {}", s.code(), s.message()))
}

/// The pull loop's handle.
pub struct Puller {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

impl Puller {
    pub async fn stop(self) {
        self.cancel.cancel();
        let _ = self.handle.await;
    }
}

pub fn spawn(node: Arc<Node>, initial: Streaming<pb::ReplicationChunk>) -> Puller {
    let cancel = node.cancel.child_token();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        let mut stream = Some(initial);
        let mut backoff = BACKOFF_MIN;
        loop {
            let s = match stream.take() {
                Some(s) => s,
                None => loop {
                    tokio::select! {
                        _ = task_cancel.cancelled() => return,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    match connect_and_check(&node).await {
                        Ok(s) => {
                            backoff = BACKOFF_MIN;
                            break s;
                        }
                        Err(e) => record_error(&node, e),
                    }
                },
            };
            node.record_replica(|r| {
                r.connected = true;
                r.last_error = None;
            });
            match tail(&node, s, &task_cancel).await {
                Ok(()) => return,
                Err(e) => record_error(&node, e),
            }
        }
    });
    Puller { cancel, handle }
}

fn record_error(node: &Node, e: String) {
    tracing::warn!(error = %e, "replication");
    node.record_replica(|r| {
        r.connected = false;
        r.last_error = Some(e);
    });
}

/// Applies chunks until the stream ends or the puller is cancelled. A
/// chunk being applied is never abandoned midway.
async fn tail(
    node: &Arc<Node>,
    mut stream: Streaming<pb::ReplicationChunk>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            m = stream.message() => m,
        };
        let c = msg
            .map_err(|s| format!("the leader's stream failed: {}: {}", s.code(), s.message()))?
            .ok_or_else(|| "the leader closed the stream".to_string())?;
        let db = node.db.clone();
        if c.db_id != db.id().as_bytes() {
            return Err("the leader's stream carries another database's identity".into());
        }
        let ours = db.epoch();
        if c.epoch < ours {
            return Err(format!(
                "the leader's epoch {} went backwards from {ours}",
                c.epoch
            ));
        }
        if c.epoch > ours {
            let db = db.clone();
            let e = c.epoch;
            blocking(move || db.set_epoch(e)).await?;
        }
        let to = c.to;
        let chunk = chron::Chunk {
            from: c.from,
            to,
            frames: c.frames,
        };
        let new_head = blocking(move || db.apply_chunk(&chunk)).await?;
        if new_head != to {
            return Err(format!(
                "applied a chunk to {to} but the head is {new_head}"
            ));
        }
        node.record_replica(|r| r.leader_head = Some(c.head));
        if node.role() != Role::Replica {
            return Ok(());
        }
    }
}
