//! The `Chron` service over one [`Node`]: every call is routed to the
//! database its `chron-db` metadata names (`default` without it).

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chron::Db;
use chron_proto::v1 as pb;
use chron_proto::v1::chron_server::Chron;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use crate::codec;
use crate::node::{DbNode, Node, Role};
use crate::ops::{self, PAGE, Read, blocking, ended, gone, role_lost};
use crate::replica;

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

    /// The database the call is addressed to.
    fn db<T>(&self, req: &Request<T>) -> Result<Arc<DbNode>, Status> {
        self.node.resolve(req.metadata())
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
                _ = cancel.cancelled() => {
                    let _ = tx.send(Err(Status::unavailable("the database or daemon went away"))).await;
                    return;
                }
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
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        let node = self.node.clone();
        let name = dbn.name.clone();
        let start = std::time::Instant::now();
        let result =
            tokio::task::spawn_blocking(move || node.commit_guarded(&dbn, r.appends, r.epoch))
                .await
                .map_err(|e| codec::join("commit", e))?;
        self.node.metrics.commit(&name, start, &result);
        Ok(Response::new(codec::committed(result?)))
    }

    type ReadAggregateStream = EventStream;

    async fn read_aggregate(
        &self,
        req: Request<pb::ReadAggregateRequest>,
    ) -> Result<Response<Self::ReadAggregateStream>, Status> {
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        let id = codec::aggregate(&r.aggregate)?;
        let read = Read::Aggregate {
            id,
            backward: r.backward,
        };
        Ok(Response::new(finite(
            dbn.db.clone(),
            read,
            r.from_index,
            r.limit,
            dbn.cancel.clone(),
        )))
    }

    type ReadAllStream = EventStream;

    async fn read_all(
        &self,
        req: Request<pb::ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        if !r.backward {
            // A bad starting position is the call's status, not the
            // stream's first item.
            let from = r.from;
            blocking(&dbn.db, "read_all", move |db| db.read_all(from, 1)).await?;
        }
        let read = Read::All {
            backward: r.backward,
        };
        Ok(Response::new(finite(
            dbn.db.clone(),
            read,
            r.from,
            r.limit,
            dbn.cancel.clone(),
        )))
    }

    type ReadTagStream = EventStream;

    async fn read_tag(
        &self,
        req: Request<pb::ReadTagRequest>,
    ) -> Result<Response<Self::ReadTagStream>, Status> {
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        let read = Read::Tag {
            tag: Bytes::from(r.tag),
            backward: r.backward,
        };
        Ok(Response::new(finite(
            dbn.db.clone(),
            read,
            r.from,
            r.limit,
            dbn.cancel.clone(),
        )))
    }

    async fn aggregate_len(
        &self,
        req: Request<pb::AggregateLenRequest>,
    ) -> Result<Response<pb::AggregateLenResponse>, Status> {
        let dbn = self.db(&req)?;
        let id = codec::aggregate(&req.into_inner().aggregate)?;
        let len = blocking(&dbn.db, "aggregate_len", move |db| db.aggregate_len(id)).await?;
        Ok(Response::new(pb::AggregateLenResponse { len }))
    }

    async fn head(
        &self,
        req: Request<pb::HeadRequest>,
    ) -> Result<Response<pb::HeadResponse>, Status> {
        let dbn = self.db(&req)?;
        Ok(Response::new(pb::HeadResponse {
            head: dbn.db.head(),
        }))
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        let rx = ops::subscribe(self.node.clone(), dbn, r.from, r.tag);
        Ok(Response::new(Box::pin(
            rx.map(|item| item.map(codec::event_to_wire)),
        )))
    }

    async fn flush(
        &self,
        req: Request<pb::FlushRequest>,
    ) -> Result<Response<pb::FlushResponse>, Status> {
        let dbn = self.db(&req)?;
        blocking(&dbn.db, "flush", |db| db.flush()).await?;
        Ok(Response::new(pb::FlushResponse {}))
    }

    async fn info(
        &self,
        req: Request<pb::InfoRequest>,
    ) -> Result<Response<pb::InfoResponse>, Status> {
        let dbn = self.db(&req)?;
        Ok(Response::new(ops::info(&self.node, &dbn).await?))
    }

    async fn backup(
        &self,
        req: Request<pb::BackupRequest>,
    ) -> Result<Response<pb::BackupMeta>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        let r = req.into_inner();
        let path = self.node.backup_path(&dbn.name, &r.archive)?;
        let meta = blocking(&dbn.db, "backup", move |db| match r.since {
            Some(since) => db.backup_since(&path, since),
            None => db.backup(&path),
        })
        .await?;
        Ok(Response::new(codec::backup_meta(meta)))
    }

    async fn apply_backup(
        &self,
        req: Request<pb::ApplyBackupRequest>,
    ) -> Result<Response<pb::BackupMeta>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        if let Some(s) = self.node.write_refusal() {
            return Err(s);
        }
        let archive = self
            .node
            .backup_path(&dbn.name, &req.into_inner().archive)?;
        let node = self.node.clone();
        let meta = tokio::task::spawn_blocking(move || {
            node.write_guarded(&dbn, |db| db.apply_backup(&archive).map_err(codec::status))
        })
        .await
        .map_err(|e| codec::join("apply_backup", e))??;
        Ok(Response::new(codec::backup_meta(meta)))
    }

    async fn retain(
        &self,
        req: Request<pb::RetainRequest>,
    ) -> Result<Response<pb::RetainResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        let floor = req.into_inner().floor;
        let r = blocking(&dbn.db, "retain", move |db| db.retain_from(floor)).await?;
        Ok(Response::new(codec::retained(r)))
    }

    type ReplicateStream = ChunkStream;

    async fn replicate(
        &self,
        req: Request<pb::ReplicateRequest>,
    ) -> Result<Response<Self::ReplicateStream>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        if let Some(s) = self.node.write_refusal() {
            return Err(s);
        }
        let r = req.into_inner();
        let db = dbn.db.clone();
        let from = r.from_position;
        let head = db.head();
        if from > head {
            let m = format!(
                "diverged: the replica is at {from}, this database at {head}; reset the replica from a backup of this database"
            );
            self.node.warn_diverged(&m);
            return Err(self.node.refuse(m));
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
                    let m = format!(
                        "diverged: the histories differ before position {from}; reset the replica from a backup of this database"
                    );
                    self.node.warn_diverged(&m);
                    return Err(self.node.refuse(m));
                }
            }
        }
        let (tx, rx) = mpsc::channel(4);
        let node = self.node.clone();
        let cancel = dbn.cancel.clone();
        let mut role = self.node.watch_role();
        let limit = node.max_message_bytes;
        let max_bytes = limit / 2;
        tokio::spawn(async move {
            let mut cursor = from;
            loop {
                let db2 = db.clone();
                let chunk = tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = tx.send(Err(gone(&node))).await;
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
                                let _ = tx.send(Err(gone(&node))).await;
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
        req: Request<pb::PromoteRequest>,
    ) -> Result<Response<pb::PromoteResponse>, Status> {
        self.node.require_admin(req.metadata())?;
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
        self.node.require_admin(req.metadata())?;
        let by = req.into_inner().epoch;
        let node = self.node.clone();
        tokio::task::spawn_blocking(move || node.fence(by))
            .await
            .map_err(|e| codec::join("fence", e))??;
        Ok(Response::new(pb::FenceResponse {
            epoch: self.node.epoch(),
        }))
    }

    async fn create_database(
        &self,
        req: Request<pb::CreateDatabaseRequest>,
    ) -> Result<Response<pb::CreateDatabaseResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = ops::create_database(self.node.clone(), req.into_inner().name).await?;
        Ok(Response::new(pb::CreateDatabaseResponse {
            database: Some(dbn.info()),
        }))
    }

    async fn list_databases(
        &self,
        req: Request<pb::ListDatabasesRequest>,
    ) -> Result<Response<pb::ListDatabasesResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        Ok(Response::new(pb::ListDatabasesResponse {
            databases: self.node.databases().iter().map(|d| d.info()).collect(),
        }))
    }

    async fn drop_database(
        &self,
        req: Request<pb::DropDatabaseRequest>,
    ) -> Result<Response<pb::DropDatabaseResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        ops::drop_database(self.node.clone(), req.into_inner().name).await?;
        Ok(Response::new(pb::DropDatabaseResponse {}))
    }
}

/// Stops the discoverer and every puller, promotes, and on failure
/// resumes pulling.
async fn promote(node: Arc<Node>) -> Result<(u64, u64), Status> {
    if let Some(d) = node.discoverer.lock().await.take() {
        d.stop().await;
    }
    for dbn in node.databases() {
        if let Some(p) = dbn.puller.lock().await.take() {
            p.stop().await;
        }
    }
    let n = node.clone();
    let result = tokio::task::spawn_blocking(move || n.promote_locked())
        .await
        .map_err(|e| codec::join("promote", e))?;
    match result {
        Ok((epoch, head)) => {
            spawn_fencer(node, epoch);
            Ok((epoch, head))
        }
        Err(s) => {
            if node.role() == Role::Replica {
                for dbn in node.databases() {
                    *dbn.puller.lock().await =
                        Some(replica::spawn(node.clone(), dbn.clone(), None));
                }
                *node.discoverer.lock().await = Some(replica::spawn_discoverer(node.clone()));
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
