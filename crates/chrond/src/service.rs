//! The `Chron` service over one [`Node`].

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chron::{Db, Event, Uuid};
use chron_proto::v1 as pb;
use chron_proto::v1::chron_server::Chron;
use tokio::sync::{mpsc, watch};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::codec;
use crate::node::{Node, Role};
use crate::replica;

/// Events per page through the library, and the channel depth of a stream.
const PAGE: usize = 256;
/// Events per replication chunk.
const CHUNK: usize = 1024;

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::Event, Status>> + Send + 'static>>;
type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<pb::ReplicationChunk, Status>> + Send + 'static>>;

pub struct ChronService {
    node: Arc<Node>,
}

/// Runs one library call on the blocking pool.
async fn blocking<T: Send + 'static>(
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
fn ended(role: Option<Role>) -> Status {
    match role {
        Some(Role::Fenced) => Status::unavailable("fenced: a newer leader exists; reconnect there"),
        Some(r) => Status::unavailable(format!("the daemon's role changed to {r:?}; reconnect")),
        None => Status::unavailable("shutting down"),
    }
}

/// Waits for a role change that ends live streams: Fenced, or (for
/// replication) anything but Leader.
async fn role_lost(rx: &mut watch::Receiver<Role>, replication: bool) -> Role {
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

impl ChronService {
    pub fn new(node: Arc<Node>) -> Self {
        ChronService { node }
    }

    fn db(&self) -> &Db {
        &self.node.db
    }

    fn cancel(&self) -> CancellationToken {
        self.node.cancel.clone()
    }
}

/// One finite read, paged through the library.
enum Read {
    Aggregate { id: Uuid, backward: bool },
    All { backward: bool },
    Tag { tag: Bytes, backward: bool },
}

impl Read {
    fn page(&self, db: &Db, cursor: u64, n: usize) -> chron::Result<Vec<Event>> {
        match self {
            Read::Aggregate {
                id,
                backward: false,
            } => db.read_aggregate(*id, cursor, n),
            Read::Aggregate { id, backward: true } => db.read_aggregate_backward(*id, cursor, n),
            Read::All { backward: false } => db.read_all(cursor, n),
            Read::All { backward: true } => db.read_all_backward(cursor, n),
            Read::Tag {
                tag,
                backward: false,
            } => db.read_tag(tag, cursor, n),
            Read::Tag {
                tag,
                backward: true,
            } => db.read_tag_backward(tag, cursor, n),
        }
    }

    /// Where the next page starts after `last`; `None` when the read is
    /// exhausted at 0 going backward.
    fn next_cursor(&self, last: &Event) -> Option<u64> {
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

/// Streams a finite read: an async task pages through the library on the
/// blocking pool, one page at a time, and feeds a bounded channel. A slow
/// or departed client stalls or ends only its own task, and holds no
/// blocking thread while it waits.
fn finite(db: Db, read: Read, from: u64, limit: u64, cancel: CancellationToken) -> EventStream {
    let (tx, rx) = mpsc::channel(PAGE);
    tokio::spawn(async move {
        let read = Arc::new(read);
        let mut cursor = from;
        let mut remaining = if limit == 0 {
            usize::MAX
        } else {
            limit as usize
        };
        loop {
            let n = PAGE.min(remaining);
            let (db2, read2) = (db.clone(), read.clone());
            let page = tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tx.closed() => return,
                p = tokio::task::spawn_blocking(move || read2.page(&db2, cursor, n)) => p,
            };
            let page = match page {
                Ok(Ok(p)) => p,
                Ok(Err(e)) => {
                    let _ = tx.send(Err(codec::status(e))).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(codec::join("read", e))).await;
                    return;
                }
            };
            let got = page.len();
            let last = page.last().cloned();
            for ev in page {
                if tx.send(Ok(codec::event_to_wire(ev))).await.is_err() {
                    return;
                }
            }
            remaining -= got;
            if got < n || remaining == 0 {
                return;
            }
            match last.and_then(|l| read.next_cursor(&l)) {
                Some(c) => cursor = c,
                None => return,
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

#[tonic::async_trait]
impl Chron for ChronService {
    async fn commit(
        &self,
        req: Request<pb::CommitRequest>,
    ) -> Result<Response<pb::CommitResponse>, Status> {
        let r = req.into_inner();
        let node = self.node.clone();
        let committed =
            tokio::task::spawn_blocking(move || node.commit_guarded(r.appends, r.epoch))
                .await
                .map_err(|e| codec::join("commit", e))??;
        Ok(Response::new(codec::committed(committed)))
    }

    type ReadAggregateStream = EventStream;

    async fn read_aggregate(
        &self,
        req: Request<pb::ReadAggregateRequest>,
    ) -> Result<Response<Self::ReadAggregateStream>, Status> {
        let r = req.into_inner();
        let id = codec::aggregate(&r.aggregate)?;
        let read = Read::Aggregate {
            id,
            backward: r.backward,
        };
        Ok(Response::new(finite(
            self.db().clone(),
            read,
            r.from_index,
            r.limit,
            self.cancel(),
        )))
    }

    type ReadAllStream = EventStream;

    async fn read_all(
        &self,
        req: Request<pb::ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let r = req.into_inner();
        if !r.backward {
            // A bad starting position is the call's status, not the
            // stream's first item.
            let from = r.from;
            blocking(self.db(), "read_all", move |db| db.read_all(from, 1)).await?;
        }
        let read = Read::All {
            backward: r.backward,
        };
        Ok(Response::new(finite(
            self.db().clone(),
            read,
            r.from,
            r.limit,
            self.cancel(),
        )))
    }

    type ReadTagStream = EventStream;

    async fn read_tag(
        &self,
        req: Request<pb::ReadTagRequest>,
    ) -> Result<Response<Self::ReadTagStream>, Status> {
        let r = req.into_inner();
        let read = Read::Tag {
            tag: Bytes::from(r.tag),
            backward: r.backward,
        };
        Ok(Response::new(finite(
            self.db().clone(),
            read,
            r.from,
            r.limit,
            self.cancel(),
        )))
    }

    async fn aggregate_len(
        &self,
        req: Request<pb::AggregateLenRequest>,
    ) -> Result<Response<pb::AggregateLenResponse>, Status> {
        let id = codec::aggregate(&req.into_inner().aggregate)?;
        let len = blocking(self.db(), "aggregate_len", move |db| db.aggregate_len(id)).await?;
        Ok(Response::new(pb::AggregateLenResponse { len }))
    }

    async fn head(
        &self,
        _: Request<pb::HeadRequest>,
    ) -> Result<Response<pb::HeadResponse>, Status> {
        Ok(Response::new(pb::HeadResponse {
            head: self.db().head(),
        }))
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let r = req.into_inner();
        let mut sub = match r.tag {
            Some(tag) => self.db().subscribe_tag(tag, r.from),
            None => self.db().subscribe(r.from),
        }
        .batch(PAGE);
        let (tx, rx) = mpsc::channel(PAGE);
        let cancel = self.cancel();
        let mut role = self.node.watch_role();
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = tx.send(Err(ended(None))).await;
                        return;
                    }
                    r = role_lost(&mut role, false) => {
                        let _ = tx.send(Err(ended(Some(r)))).await;
                        return;
                    }
                    _ = tx.closed() => return,
                    r = sub.next_async() => r,
                };
                let item = match next {
                    Ok(ev) => Ok(codec::event_to_wire(ev)),
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
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn flush(
        &self,
        _: Request<pb::FlushRequest>,
    ) -> Result<Response<pb::FlushResponse>, Status> {
        blocking(self.db(), "flush", |db| db.flush()).await?;
        Ok(Response::new(pb::FlushResponse {}))
    }

    async fn info(
        &self,
        _: Request<pb::InfoRequest>,
    ) -> Result<Response<pb::InfoResponse>, Status> {
        let info = blocking(self.db(), "info", |db| db.info()).await?;
        let node = &self.node;
        let role = node.role();
        let replica = node.replica_status();
        let is_replica = role == Role::Replica;
        Ok(Response::new(pb::InfoResponse {
            path: self.db().path().display().to_string(),
            head: info.head,
            floor: info.floor,
            segments: info.segments as u64,
            bytes: info.bytes,
            options: Some(codec::options(self.db().options())),
            db_id: info.db_id.as_bytes().to_vec(),
            epoch: info.epoch,
            fsync: info.fsync == chron::Fsync::Always,
            role: role.wire() as i32,
            replicating_from: node
                .leader_addr()
                .map(str::to_string)
                .filter(|_| is_replica),
            replica_connected: is_replica && replica.connected,
            leader_head: replica.leader_head.filter(|_| is_replica),
            last_replication_error: replica.last_error.filter(|_| is_replica),
            fenced_by: node.fenced_by().filter(|_| role == Role::Fenced),
        }))
    }

    async fn backup(
        &self,
        req: Request<pb::BackupRequest>,
    ) -> Result<Response<pb::BackupMeta>, Status> {
        let r = req.into_inner();
        let meta = blocking(self.db(), "backup", move |db| match r.since {
            Some(since) => db.backup_since(&r.archive, since),
            None => db.backup(&r.archive),
        })
        .await?;
        Ok(Response::new(codec::backup_meta(meta)))
    }

    async fn apply_backup(
        &self,
        req: Request<pb::ApplyBackupRequest>,
    ) -> Result<Response<pb::BackupMeta>, Status> {
        if let Some(s) = self.node.write_refusal() {
            return Err(s);
        }
        let archive = req.into_inner().archive;
        let node = self.node.clone();
        let meta = tokio::task::spawn_blocking(move || {
            node.write_guarded(|db| db.apply_backup(&archive).map_err(codec::status))
        })
        .await
        .map_err(|e| codec::join("apply_backup", e))??;
        Ok(Response::new(codec::backup_meta(meta)))
    }

    async fn retain(
        &self,
        req: Request<pb::RetainRequest>,
    ) -> Result<Response<pb::RetainResponse>, Status> {
        let floor = req.into_inner().floor;
        let r = blocking(self.db(), "retain", move |db| db.retain_from(floor)).await?;
        Ok(Response::new(codec::retained(r)))
    }

    type ReplicateStream = ChunkStream;

    async fn replicate(
        &self,
        req: Request<pb::ReplicateRequest>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        if let Some(s) = self.node.write_refusal() {
            return Err(s);
        }
        let r = req.into_inner();
        let db = self.db().clone();
        let from = r.from_position;
        let head = db.head();
        if from > head {
            return Err(self.node.refuse(format!(
                "diverged: the replica is at {from}, this database at {head}; reset the replica from a backup of this database"
            )));
        }
        let floor = db.floor();
        if from < floor {
            return Err(Status::out_of_range(format!(
                "position {from} is below the floor {floor}; restore the replica from a full backup"
            )));
        }
        if let Some(theirs) = r.last_fingerprint
            && from > 0
        {
            if from == floor {
                tracing::warn!(
                    from,
                    "a replica starts at the floor; the record before it is gone, so its fingerprint cannot be checked"
                );
            } else {
                let ours = blocking(&db, "fingerprint", move |db| db.fingerprint(from - 1)).await?;
                if ours != Some(theirs) {
                    return Err(self.node.refuse(format!(
                        "diverged: the histories differ before position {from}; reset the replica from a backup of this database"
                    )));
                }
            }
        }
        let (tx, rx) = mpsc::channel(4);
        let node = self.node.clone();
        let cancel = self.cancel();
        let mut role = self.node.watch_role();
        let limit = node.max_message_bytes;
        let max_bytes = limit / 2;
        tokio::spawn(async move {
            let mut cursor = from;
            loop {
                let db2 = db.clone();
                let chunk = tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = tx.send(Err(ended(None))).await;
                        return;
                    }
                    r = role_lost(&mut role, true) => {
                        let _ = tx.send(Err(ended(Some(r)))).await;
                        return;
                    }
                    _ = tx.closed() => return,
                    c = tokio::task::spawn_blocking(move || db2.replication_chunk(cursor, CHUNK, max_bytes)) => c,
                };
                let chunk = match chunk {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(codec::status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(codec::join("replicate", e))).await;
                        return;
                    }
                };
                match chunk {
                    Some(c) => {
                        // 16 bytes of identity and a few varints ride along.
                        if c.frames.len() + 64 > limit {
                            let _ = tx
                                .send(Err(Status::resource_exhausted(format!(
                                    "the transaction at {}..{} is {} bytes, above the message limit of {limit}; raise --max-message-bytes on both nodes",
                                    c.from,
                                    c.to,
                                    c.frames.len()
                                ))))
                                .await;
                            return;
                        }
                        let item = pb::ReplicationChunk {
                            db_id: db.id().as_bytes().to_vec(),
                            from: c.from,
                            to: c.to,
                            frames: c.frames,
                            head: db.head(),
                            epoch: db.epoch(),
                        };
                        cursor = c.to;
                        if tx.send(Ok(item)).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                let _ = tx.send(Err(ended(None))).await;
                                return;
                            }
                            r = role_lost(&mut role, true) => {
                                let _ = tx.send(Err(ended(Some(r)))).await;
                                return;
                            }
                            _ = tx.closed() => return,
                            _ = db.wait_for_async(cursor + 1) => {}
                        }
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn promote(
        &self,
        _: Request<pb::PromoteRequest>,
    ) -> Result<Response<pb::PromoteResponse>, Status> {
        match self.node.role() {
            Role::Leader => return Err(self.node.refuse("already the leader")),
            Role::Fenced => {
                return Err(self
                    .node
                    .refuse("fenced; restart it with --replicate-from the new leader"));
            }
            Role::Replica => {}
        }
        // Its own task: a client that gives up mid-way must not leave the
        // puller stopped and the role unchanged.
        let node = self.node.clone();
        let (epoch, head) = tokio::spawn(async move { promote(node).await })
            .await
            .map_err(|e| codec::join("promote", e))??;
        Ok(Response::new(pb::PromoteResponse { epoch, head }))
    }

    async fn fence(
        &self,
        req: Request<pb::FenceRequest>,
    ) -> Result<Response<pb::FenceResponse>, Status> {
        let by = req.into_inner().epoch;
        let node = self.node.clone();
        tokio::task::spawn_blocking(move || node.fence(by))
            .await
            .map_err(|e| codec::join("fence", e))??;
        Ok(Response::new(pb::FenceResponse {
            epoch: self.db().epoch(),
        }))
    }
}

/// Stops the puller, promotes, and on failure resumes pulling.
async fn promote(node: Arc<Node>) -> Result<(u64, u64), Status> {
    let mut puller = node.puller.lock().await;
    if let Some(p) = puller.take() {
        p.stop().await;
    }
    let n = node.clone();
    let result = tokio::task::spawn_blocking(move || n.promote_locked())
        .await
        .map_err(|e| codec::join("promote", e))?;
    match result {
        Ok((epoch, head)) => {
            drop(puller);
            spawn_fencer(node, epoch);
            Ok((epoch, head))
        }
        Err(s) => {
            if node.role() == Role::Replica {
                *puller = Some(replica::spawn(node.clone(), None));
            }
            Err(s)
        }
    }
}

/// Tells the old leader, with retries, that `epoch` exists. Best effort:
/// the epoch on Commit is the guarantee, this call is the courtesy.
fn spawn_fencer(node: Arc<Node>, epoch: u64) {
    let Some(up) = node.upstream.clone() else {
        return;
    };
    let cancel = node.cancel.clone();
    let handle = tokio::spawn(async move {
        let addr = up.addr.clone();
        let mut backoff = Duration::from_millis(200);
        loop {
            let attempt = async {
                let client = replica::connect(&up).await.map_err(|e| e.to_string())?;
                client
                    .fence(epoch)
                    .await
                    .map_err(|e| (e.to_string(), e))
                    .map_err(|(m, e)| match e {
                        chron_client::ClientError::Fenced { .. }
                        | chron_client::ClientError::FailedPrecondition(_) => {
                            format!("refused: {m}")
                        }
                        _ => m,
                    })
            };
            match tokio::time::timeout(Duration::from_secs(10), attempt).await {
                Ok(Ok(_)) => {
                    tracing::info!(%addr, epoch, "old leader fenced");
                    return;
                }
                Ok(Err(e)) if e.starts_with("refused: ") => {
                    tracing::warn!(%addr, epoch, error = %e, "old leader did not take the fence");
                    return;
                }
                Ok(Err(e)) => tracing::warn!(%addr, epoch, error = %e, "fence failed; retrying"),
                Err(_) => tracing::warn!(%addr, epoch, "fence timed out; retrying"),
            }
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    });
    *node.fencer.lock().unwrap_or_else(|p| p.into_inner()) = Some(handle);
}
