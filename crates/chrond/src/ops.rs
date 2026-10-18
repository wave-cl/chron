//! Call logic shared by the gRPC service and the HTTP gateway: paging,
//! stream endings, subscriptions, database creation and removal, and the
//! info report. Both transports map errors from the same `Status`.

use std::sync::Arc;

use bytes::Bytes;
use chron::{Db, Event, Uuid};
use chron_proto::v1 as pb;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::codec;
use crate::node::{DbNode, Node, Role};

/// Events per page through the library, and the channel depth of a stream.
pub const PAGE: usize = 256;

/// Runs one library call on the blocking pool.
pub async fn blocking<T: Send + 'static>(
    db: &Db,
    op: &'static str,
    f: impl FnOnce(Db) -> chron::Result<T> + Send + 'static,
) -> Result<T, Status> {
    let db = db.clone();
    tokio::task::spawn_blocking(move || f(db))
        .await
        .map_err(|e| codec::join(op, e))?
        .map_err(codec::status)
}

/// Why a live stream ended: the daemon stopped, or stopped serving the
/// role the stream depends on.
pub fn ended(role: Option<Role>) -> Status {
    match role {
        Some(Role::Fenced) => Status::unavailable("fenced: a newer leader exists; reconnect there"),
        Some(r) => Status::unavailable(format!("the daemon's role changed to {r:?}; reconnect")),
        None => Status::unavailable("shutting down"),
    }
}

/// The database's token fired: the daemon is stopping, or the database
/// was dropped.
pub fn gone(node: &Node) -> Status {
    if node.cancel.is_cancelled() {
        ended(None)
    } else {
        Status::unavailable("the database was dropped")
    }
}

/// Waits for a role change that ends live streams: Fenced, or (for
/// replication) anything but Leader.
pub async fn role_lost(rx: &mut watch::Receiver<Role>, replication: bool) -> Role {
    loop {
        let r = *rx.borrow_and_update();
        if r == Role::Fenced || (replication && r != Role::Leader) {
            return r;
        }
        if rx.changed().await.is_err() {
            return r;
        }
    }
}

/// One finite read, paged through the library.
pub enum Read {
    Aggregate { id: Uuid, backward: bool },
    All { backward: bool },
    Tag { tag: Bytes, backward: bool },
}

impl Read {
    /// Forward pages are bounded in bytes as well as records; backward
    /// pages, read newest first, keep the record bound only.
    pub fn page(&self, db: &Db, cursor: u64, n: usize) -> chron::Result<Vec<Event>> {
        let max_bytes = db.options().page_max_bytes;
        match self {
            Read::Aggregate {
                id,
                backward: false,
            } => db.read_aggregate_bounded(*id, cursor, n, max_bytes),
            Read::Aggregate { id, backward: true } => db.read_aggregate_backward(*id, cursor, n),
            Read::All { backward: false } => db.read_all_bounded(cursor, n, max_bytes),
            Read::All { backward: true } => db.read_all_backward(cursor, n),
            Read::Tag {
                tag,
                backward: false,
            } => db.read_tag_bounded(tag, cursor, n, max_bytes),
            Read::Tag {
                tag,
                backward: true,
            } => db.read_tag_backward(tag, cursor, n),
        }
    }

    /// Where the next page starts after `last`; `None` when the read is
    /// exhausted at 0 going backward.
    pub fn next_cursor(&self, last: &Event) -> Option<u64> {
        let (key, backward) = match self {
            Read::Aggregate { backward, .. } => (last.index, *backward),
            Read::All { backward } | Read::Tag { backward, .. } => (last.position, *backward),
        };
        if backward {
            key.checked_sub(1)
        } else {
            Some(key + 1)
        }
    }
}

/// One page of a finite read on the blocking pool.
pub async fn read_page(
    dbn: &DbNode,
    read: Arc<Read>,
    cursor: u64,
    n: usize,
) -> Result<Vec<Event>, Status> {
    blocking(&dbn.db, "read", move |db| read.page(&db, cursor, n)).await
}

/// Committed events from `from` (or those carrying `tag`), then live ones,
/// until the client goes away, the database is dropped or fenced, or the
/// daemon stops; the end reason is the last item.
pub fn subscribe(
    node: Arc<Node>,
    dbn: Arc<DbNode>,
    from: u64,
    tag: Option<Vec<u8>>,
) -> ReceiverStream<Result<Event, Status>> {
    let mut sub = match tag {
        Some(tag) => dbn.db.subscribe_tag(tag, from),
        None => dbn.db.subscribe(from),
    }
    .batch(PAGE);
    let (tx, rx) = mpsc::channel(PAGE);
    let cancel = dbn.cancel.clone();
    let mut role = node.watch_role();
    let mut reset = dbn.reset.subscribe();
    reset.mark_unchanged();
    tokio::spawn(async move {
        loop {
            let next = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tx.send(Err(gone(&node))).await;
                    return;
                }
                r = role_lost(&mut role, false) => {
                    let _ = tx.send(Err(ended(Some(r)))).await;
                    return;
                }
                r = reset.changed() => {
                    // The sender goes with the database: that is a drop.
                    let status = match r {
                        Ok(()) => {
                            let to = reset.borrow_and_update().unwrap_or(0);
                            Status::unavailable(format!(
                                "the database was cut back to {to} to follow the leader; positions from there are being rewritten, subscribe again"
                            ))
                        }
                        Err(_) => gone(&node),
                    };
                    let _ = tx.send(Err(status)).await;
                    return;
                }
                _ = tx.closed() => return,
                r = sub.next_async() => r,
            };
            let item = match next {
                Ok(ev) => Ok(ev),
                Err(e) => {
                    let _ = tx.send(Err(codec::status(e))).await;
                    return;
                }
            };
            if tx.send(item).await.is_err() {
                return;
            }
        }
    });
    ReceiverStream::new(rx)
}

/// What `Info` reports about one database.
pub async fn info(node: &Node, dbn: &DbNode) -> Result<pb::InfoResponse, Status> {
    let info = blocking(&dbn.db, "info", |db| db.info()).await?;
    let role = node.role();
    let replica = dbn.replica_status();
    let is_replica = role == Role::Replica;
    let discovery_error = node
        .discovery_error
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone();
    Ok(pb::InfoResponse {
        path: dbn.db.path().display().to_string(),
        database: dbn.name.clone(),
        head: info.head,
        floor: info.floor,
        segments: info.segments as u64,
        bytes: info.bytes,
        options: Some(codec::options(dbn.db.options())),
        db_id: info.db_id.as_bytes().to_vec(),
        epoch: info.epoch,
        fsync: info.fsync == chron::Fsync::Always,
        role: role.wire() as i32,
        replicating_from: node.leader_addr().filter(|_| is_replica),
        leader_addr: crate::cluster::discover(node).leader_addr,
        lease_remaining_ms: (role == Role::Leader)
            .then(|| node.lease_remaining().map(|d| d.as_millis() as u64))
            .flatten(),
        peers: node
            .cluster
            .as_ref()
            .map_or_else(Vec::new, |c| c.peers.clone()),
        voted_epoch: node.lease().voted.map(|(e, _)| e),
        replica_connected: is_replica && replica.connected,
        leader_head: replica.leader_head.filter(|_| is_replica),
        last_replication_error: replica
            .last_error
            .or(discovery_error)
            .filter(|_| is_replica),
        fenced_by: node.fenced_by().filter(|_| role == Role::Fenced),
    })
}

/// Creates the empty database `name` on a leader. The caller checked the
/// admin token.
pub async fn create_database(node: Arc<Node>, name: String) -> Result<Arc<DbNode>, Status> {
    if !chron_proto::valid_database_name(&name) {
        return Err(Status::invalid_argument(format!(
            "{name:?} is not a database name; names are 1 to 64 of a-z, 0-9, _ and -"
        )));
    }
    if node.role() != Role::Leader {
        return Err(
            node.refuse("databases are created on the leader; a replica mirrors its leader's")
        );
    }
    let _ops = node.admin_ops.lock().await;
    if node.get(&name).is_some() {
        return Err(Status::already_exists(format!("database {name:?} exists")));
    }
    let n = node.clone();
    let epoch = node.epoch();
    let name2 = name.clone();
    let db = tokio::task::spawn_blocking(move || {
        let db = n.open_database(&name2)?;
        if db.epoch() < epoch {
            db.set_epoch(epoch)?;
        }
        Ok::<_, chron::Error>(db)
    })
    .await
    .map_err(|e| codec::join("create_database", e))?
    .map_err(codec::status)?;
    let dbn = DbNode::new(name.clone(), db, &node.cancel);
    node.insert(dbn.clone());
    tracing::info!(database = %name, "database created");
    Ok(dbn)
}

/// Drops the database `name` on a leader; `default` cannot be dropped.
/// The caller checked the admin token.
pub async fn drop_database(node: Arc<Node>, name: String) -> Result<(), Status> {
    if !chron_proto::valid_database_name(&name) {
        return Err(Status::invalid_argument(format!(
            "{name:?} is not a database name"
        )));
    }
    if node.role() != Role::Leader {
        return Err(node.refuse("databases are dropped on the leader; a replica keeps its copies"));
    }
    if name == chron_proto::DEFAULT_DATABASE {
        return Err(Status::failed_precondition(
            "the default database cannot be dropped",
        ));
    }
    let _ops = node.admin_ops.lock().await;
    let Some(dbn) = node.remove(&name) else {
        return Err(Status::not_found(format!("no database {name:?}")));
    };
    // Streams on it end; a stream still holding the Db keeps the unlinked
    // files open until it ends, the directory entry is gone at once, and
    // a recreated database gets a fresh identity.
    dbn.cancel.cancel();
    if let Some(p) = dbn.puller.lock().await.take() {
        p.stop().await;
    }
    drop(dbn);
    let dir = node.database_dir(&name);
    tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&dir))
        .await
        .map_err(|e| codec::join("drop_database", e))?
        .map_err(|e| Status::internal(format!("cannot remove the database directory: {e}")))?;
    tracing::info!(database = %name, "database dropped");
    Ok(())
}
