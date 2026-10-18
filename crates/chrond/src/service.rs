//! The `Chron` service over one [`Node`].

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chron::{Db, Event, Uuid};
use chron_proto::v1 as pb;
use chron_proto::v1::chron_server::Chron;
use tokio::sync::mpsc;
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

    async fn blocking<T: Send + 'static>(
        &self,
        op: &'static str,
        f: impl FnOnce(Db) -> chron::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let db = self.db().clone();
        tokio::task::spawn_blocking(move || f(db))
            .await
            .map_err(|e| codec::join(op, e))?
            .map_err(codec::status)
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

/// Streams a finite read: one blocking task pages through the library and
/// feeds a bounded channel, so a slow or departed client stalls or ends
/// only its own task.
fn finite(db: Db, read: Read, from: u64, limit: u64) -> EventStream {
    let (tx, rx) = mpsc::channel(PAGE);
    tokio::task::spawn_blocking(move || {
        let mut cursor = from;
        let mut remaining = if limit == 0 {
            usize::MAX
        } else {
            limit as usize
        };
        loop {
            let n = PAGE.min(remaining);
            let page = match read.page(&db, cursor, n) {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.blocking_send(Err(codec::status(e)));
                    return;
                }
            };
            let got = page.len();
            let last = page.last().cloned();
            for ev in page {
                if tx.blocking_send(Ok(codec::event_to_wire(ev))).is_err() {
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
            self.db().read_all(r.from, 1).map_err(codec::status)?;
        }
        let read = Read::All {
            backward: r.backward,
        };
        Ok(Response::new(finite(
            self.db().clone(),
            read,
            r.from,
            r.limit,
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
        )))
    }

    async fn aggregate_len(
        &self,
        req: Request<pb::AggregateLenRequest>,
    ) -> Result<Response<pb::AggregateLenResponse>, Status> {
        let id = codec::aggregate(&req.into_inner().aggregate)?;
        let len = self
            .blocking("aggregate_len", move |db| db.aggregate_len(id))
            .await?;
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
        tokio::spawn(async move {
            loop {
                let next = tokio::select! {
                    _ = cancel.cancelled() => return,
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
        self.blocking("flush", |db| db.flush()).await?;
        Ok(Response::new(pb::FlushResponse {}))
    }

    async fn info(
        &self,
        _: Request<pb::InfoRequest>,
    ) -> Result<Response<pb::InfoResponse>, Status> {
        let info = self.blocking("info", |db| db.info()).await?;
        let node = &self.node;
        let role = node.role();
        let replica = node.replica_status();
        Ok(Response::new(pb::InfoResponse {
            path: self.db().path().display().to_string(),
            head: info.head,
            floor: info.floor,
            segments: info.segments as u64,
            bytes: info.bytes,
            options: Some(codec::options(self.db().options())),
            db_id: info.db_id.as_bytes().to_vec(),
            epoch: info.epoch,
            role: role.wire() as i32,
            replicating_from: node
                .replicate_from
                .clone()
                .filter(|_| role == Role::Replica),
            replica_connected: role == Role::Replica && replica.connected,
            leader_head: replica.leader_head.filter(|_| role == Role::Replica),
            last_replication_error: replica.last_error.filter(|_| role == Role::Replica),
            fenced_by: node.fenced_by().filter(|_| role == Role::Fenced),
        }))
    }

    async fn backup(
        &self,
        req: Request<pb::BackupRequest>,
    ) -> Result<Response<pb::BackupMeta>, Status> {
        let r = req.into_inner();
        let meta = self
            .blocking("backup", move |db| match r.since {
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
        let meta = self
            .blocking("apply_backup", move |db| db.apply_backup(&archive))
            .await?;
        Ok(Response::new(codec::backup_meta(meta)))
    }

    async fn retain(
        &self,
        req: Request<pb::RetainRequest>,
    ) -> Result<Response<pb::RetainResponse>, Status> {
        let floor = req.into_inner().floor;
        let r = self
            .blocking("retain", move |db| db.retain_from(floor))
            .await?;
        Ok(Response::new(pb::RetainResponse {
            floor: r.floor,
            segments_removed: r.segments_removed as u64,
            bytes_freed: r.bytes_freed,
        }))
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
            let ours = self
                .blocking("fingerprint", move |db| db.fingerprint(from - 1))
                .await?;
            if ours != Some(theirs) {
                return Err(self.node.refuse(format!(
                    "diverged: the histories differ before position {from}; reset the replica from a backup of this database"
                )));
            }
        }
        let (tx, rx) = mpsc::channel(4);
        let node = self.node.clone();
        let cancel = self.cancel();
        tokio::spawn(async move {
            let mut cursor = from;
            loop {
                let db2 = db.clone();
                let chunk = tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tx.closed() => return,
                    c = tokio::task::spawn_blocking(move || db2.replication_chunk(cursor, CHUNK)) => c,
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
                            _ = cancel.cancelled() => return,
                            _ = tx.closed() => return,
                            _ = db.wait_for_async(cursor + 1) => {}
                        }
                        if let Some(s) = node.write_refusal() {
                            let _ = tx.send(Err(s)).await;
                            return;
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
        if let Some(p) = self.node.puller.lock().await.take() {
            p.stop().await;
        }
        let node = self.node.clone();
        let (epoch, head) = tokio::task::spawn_blocking(move || node.promote_locked())
            .await
            .map_err(|e| codec::join("promote", e))??;
        spawn_fencer(self.node.clone(), epoch);
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

/// Tells the old leader, with retries, that `epoch` exists. Best effort:
/// the epoch on Commit is the guarantee, this call is the courtesy.
fn spawn_fencer(node: Arc<Node>, epoch: u64) {
    let Some(addr) = node.replicate_from.clone() else {
        return;
    };
    let token = node.replicate_token.clone();
    let cancel = node.cancel.clone();
    let handle = tokio::spawn(async move {
        let mut backoff = Duration::from_millis(200);
        loop {
            match replica::connect(&addr, token.as_deref()).await {
                Ok(mut client) => match client.fence(pb::FenceRequest { epoch }).await {
                    Ok(_) => {
                        tracing::info!(%addr, epoch, "old leader fenced");
                        return;
                    }
                    Err(s) if s.code() == tonic::Code::FailedPrecondition => {
                        tracing::warn!(%addr, epoch, status = %s.message(), "old leader did not take the fence");
                        return;
                    }
                    Err(s) => {
                        tracing::warn!(%addr, epoch, status = %s.message(), "fence failed; retrying")
                    }
                },
                Err(e) => tracing::warn!(%addr, epoch, error = %e, "fence failed; retrying"),
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
