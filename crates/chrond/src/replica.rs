//! The replica side: connect to the leader, check the two histories agree,
//! then pull chunks for as long as the daemon runs.

use std::sync::Arc;
use std::time::Duration;

use chron_client::{ChunkStream, Client, ClientConfig, ClientError, Role as WireRole};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::node::{Node, Role, Upstream};

const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);
/// A bound on the leader's answer to `Info`, so a hung leader cannot hang
/// the puller or a start.
const INFO_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a replica could not follow its leader.
#[derive(Debug)]
pub enum CheckError {
    /// The leader did not answer; worth retrying.
    Unreachable(String),
    /// The leader answered and the two cannot be paired; retrying will
    /// not help until something changes.
    Refused(String),
}

impl CheckError {
    pub fn message(&self) -> &str {
        match self {
            CheckError::Unreachable(m) | CheckError::Refused(m) => m,
        }
    }
}

impl std::fmt::Display for CheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

fn unreachable(addr: &str, e: ClientError) -> CheckError {
    match e {
        ClientError::Transport(_) | ClientError::Unavailable(_) => {
            CheckError::Unreachable(format!("cannot reach the leader at {addr}: {e}"))
        }
        other => CheckError::Refused(format!("{addr}: {other}")),
    }
}

/// A client for the leader: the replication token on every call.
pub async fn connect(up: &Upstream) -> Result<Client, CheckError> {
    let config = ClientConfig {
        tls: up.tls.clone(),
        token: up.token.clone(),
        admin_token: up.token.clone(),
        max_message_bytes: up.max_message_bytes,
        ..ClientConfig::default()
    };
    Client::connect(&up.addr, config)
        .await
        .map_err(|e| unreachable(&up.addr, e))
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
/// Nothing is written here until the leader has accepted the stream.
/// Returns the open chunk stream.
pub async fn connect_and_check(node: &Arc<Node>) -> Result<ChunkStream, CheckError> {
    let up = node
        .upstream
        .as_ref()
        .ok_or_else(|| CheckError::Refused("no leader configured".into()))?;
    let addr = up.addr.as_str();
    let client = connect(up).await?;
    let info = match tokio::time::timeout(INFO_TIMEOUT, client.info()).await {
        Ok(r) => r.map_err(|e| unreachable(addr, e))?,
        Err(_) => {
            return Err(CheckError::Unreachable(format!(
                "{addr} did not answer Info within {INFO_TIMEOUT:?}"
            )));
        }
    };
    if info.role != WireRole::Leader {
        return Err(CheckError::Refused(format!(
            "{addr} is not a leader (role {:?})",
            info.role
        )));
    }
    let leader_id = info.db_id;
    let db = node.db.clone();
    let (head, floor, id, epoch) = (db.head(), db.floor(), db.id(), db.epoch());
    let empty = head == 0 && floor == 0;
    if id != leader_id && !empty {
        return Err(CheckError::Refused(format!(
            "{} is database {id}; {addr} serves database {leader_id}; a replica must be restored from a backup of the leader",
            node.data_dir.display()
        )));
    }
    if epoch > info.epoch {
        return Err(CheckError::Refused(format!(
            "local epoch {epoch} is newer than the leader's {}",
            info.epoch
        )));
    }
    if head > info.head {
        return Err(CheckError::Refused(format!(
            "diverged: this replica is at head {head}, the leader at {}; reset it from a backup of the leader",
            info.head
        )));
    }
    // The record before the head is gone when the head sits on the floor
    // (restored from a backup of a retained leader); the leader skips the
    // check then too.
    let last_fingerprint = if head > floor {
        let db = db.clone();
        blocking(move || db.fingerprint(head - 1))
            .await
            .map_err(CheckError::Refused)?
    } else {
        None
    };
    let stream = client
        .replicate(head, last_fingerprint)
        .await
        .map_err(|e| unreachable(addr, e))?;
    if id != leader_id {
        let db = db.clone();
        blocking(move || db.adopt_id(leader_id))
            .await
            .map_err(CheckError::Refused)?;
    }
    if epoch < info.epoch {
        let db = db.clone();
        let e = info.epoch;
        blocking(move || db.set_epoch(e))
            .await
            .map_err(CheckError::Refused)?;
    }
    Ok(stream)
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

/// Pulls from `initial` (or reconnects first, when it is `None`) until
/// stopped. On exit the node is recorded as disconnected.
pub fn spawn(node: Arc<Node>, initial: Option<ChunkStream>) -> Puller {
    let cancel = node.cancel.child_token();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        pull(&node, initial, &task_cancel).await;
        node.record_replica(|r| r.connected = false);
    });
    Puller { cancel, handle }
}

async fn pull(node: &Arc<Node>, initial: Option<ChunkStream>, cancel: &CancellationToken) {
    let mut stream = initial;
    let mut backoff = BACKOFF_MIN;
    loop {
        let s = match stream.take() {
            Some(s) => s,
            None => loop {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(BACKOFF_MAX);
                let checked = tokio::select! {
                    _ = cancel.cancelled() => return,
                    c = connect_and_check(node) => c,
                };
                match checked {
                    Ok(s) => {
                        backoff = BACKOFF_MIN;
                        break s;
                    }
                    Err(e) => record_error(node, e.message().to_string()),
                }
            },
        };
        node.record_replica(|r| {
            r.connected = true;
            r.last_error = None;
        });
        match tail(node, s, cancel).await {
            Ok(()) => return,
            Err(e) => record_error(node, e),
        }
    }
}

pub fn record_error(node: &Node, e: String) {
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
    mut stream: ChunkStream,
    cancel: &CancellationToken,
) -> Result<(), String> {
    loop {
        let msg = tokio::select! {
            _ = cancel.cancelled() => return Ok(()),
            m = stream.next() => m,
        };
        let c = msg
            .ok_or_else(|| "the leader closed the stream".to_string())?
            .map_err(|e| format!("the leader's stream failed: {e}"))?;
        let db = node.db.clone();
        if c.db_id != Some(db.id()) {
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
        let to = c.chunk.to;
        let chunk = c.chunk;
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
