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

use crate::cluster;
use crate::codec;
use crate::node::{DbNode, Node, Role, Upstream};
use crate::ops::{self, PAGE, Read, blocking, ended, gone, role_lost};
use crate::replica;

/// Events per replication chunk.
const CHUNK: usize = 1024;

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::Event, Status>> + Send + 'static>>;
type ChunkStream =
    Pin<Box<dyn Stream<Item = Result<pb::ReplicationChunk, Status>> + Send + 'static>>;
type ByteStream = Pin<Box<dyn Stream<Item = Result<pb::BackupChunk, Status>> + Send + 'static>>;

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

    /// Whether a replica at `from` can follow this database: not past
    /// the head, not below the floor, and with the same record before
    /// `from` when it sent a fingerprint.
    async fn admit_replica(
        &self,
        dbn: &DbNode,
        from: u64,
        last_fingerprint: Option<u32>,
    ) -> Result<(), Status> {
        let db = dbn.db.clone();
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
            return Err(codec::status(chron::Error::BelowFloor {
                position: from,
                floor,
            }));
        }
        if let Some(theirs) = last_fingerprint
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
        Ok(())
    }
}

/// Bytes per FetchBackup message.
const FETCH_CHUNK: usize = 1 << 20;

/// Bridges the library's synchronous backup writer to a response stream:
/// buffers up to one chunk, then blocks its blocking-pool thread on a
/// bounded channel, so a few chunks at most are in flight and a client
/// that stops reading stalls only that thread. A departed client makes
/// the send fail, which ends the backup and releases the maintenance
/// lock.
struct ChunkSink {
    buf: Vec<u8>,
    chunk: usize,
    tx: mpsc::Sender<Result<pb::BackupChunk, Status>>,
    cancel: CancellationToken,
}

impl ChunkSink {
    fn send_buf(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self.cancel.is_cancelled() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "the database went away",
            ));
        }
        let data = std::mem::take(&mut self.buf);
        self.tx
            .blocking_send(Ok(pb::BackupChunk { data }))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "the client went away")
            })
    }
}

impl std::io::Write for ChunkSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= self.chunk {
            self.send_buf()?;
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_buf()
    }
}

/// Streams whole-transaction chunks from `from` until the follower goes
/// away, `cancel` fires, the role is lost or the database goes. With
/// `open_chunk` the first item is an empty chunk at `from` carrying the
/// epoch and its base, so a follower that is already caught up can adopt
/// the epoch at once.
fn spawn_chunk_stream(
    node: Arc<Node>,
    dbn: Arc<DbNode>,
    from: u64,
    cancel: CancellationToken,
    open_chunk: bool,
) -> mpsc::Receiver<Result<pb::ReplicationChunk, Status>> {
    let (tx, rx) = mpsc::channel(4);
    let db = dbn.db.clone();
    let mut role = node.watch_role();
    let limit = node.max_message_bytes;
    let max_bytes = limit / 2;
    tokio::spawn(async move {
        let mut cursor = from;
        if open_chunk {
            let item = pb::ReplicationChunk {
                db_id: db.id().as_bytes().to_vec(),
                from: cursor,
                to: cursor,
                frames: Vec::new(),
                head: db.head(),
                epoch: db.epoch(),
                epoch_base: dbn.epoch_base.load(std::sync::atomic::Ordering::Acquire),
            };
            if tx.send(Ok(item)).await.is_err() {
                return;
            }
        }
        loop {
            let db2 = db.clone();
            let chunk = tokio::select! {
                _ = cancel.cancelled() => {
                    let _ = tx.send(Err(gone(&node, &dbn))).await;
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
                        epoch_base: dbn.epoch_base.load(std::sync::atomic::Ordering::Acquire),
                    };
                    cursor = c.to;
                    if tx.send(Ok(item)).await.is_err() {
                        return;
                    }
                }
                None => {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            let _ = tx.send(Err(gone(&node, &dbn))).await;
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
    rx
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
        let committed = ops::commit(&self.node, dbn, r.appends, r.epoch, r.sync).await?;
        Ok(Response::new(codec::committed(committed)))
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
        self.admit_replica(&dbn, r.from_position, r.last_fingerprint)
            .await?;
        let rx = spawn_chunk_stream(
            self.node.clone(),
            dbn.clone(),
            r.from_position,
            dbn.cancel.child_token(),
            false,
        );
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type FollowStream = ChunkStream;

    /// Replicate with acknowledgments: the first message opens the
    /// stream, every later one reports an applied head, which the
    /// leader counts toward the majority sync commits wait for.
    async fn follow(
        &self,
        req: Request<tonic::Streaming<pb::FollowRequest>>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        self.node.require_admin(req.metadata())?;
        #[cfg(feature = "test-util")]
        if self
            .node
            .refuse_follow
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Err(Status::unimplemented("Follow is not in this version"));
        }
        let dbn = self.db(&req)?;
        if let Some(s) = self.node.write_refusal() {
            return Err(s);
        }
        let mut inbound = req.into_inner();
        let first = tokio::time::timeout(Duration::from_secs(10), inbound.message())
            .await
            .map_err(|_| Status::invalid_argument("no `open` message within 10 s"))??;
        let open = match first {
            Some(pb::FollowRequest {
                msg: Some(pb::follow_request::Msg::Open(o)),
            }) => o,
            _ => {
                return Err(Status::invalid_argument(
                    "the first Follow message must be `open`",
                ));
            }
        };
        self.admit_replica(&dbn, open.from_position, open.last_fingerprint)
            .await?;
        let leader_durable = self.node.db_opts.fsync == chron::Fsync::Always;
        let is_peer = self
            .node
            .cluster
            .as_ref()
            .is_some_and(|c| c.peers.contains(&open.peer_addr));
        let counted = is_peer && (open.durable || !leader_durable);
        if is_peer && !counted {
            tracing::warn!(peer = %open.peer_addr, database = %dbn.name, "the follower applies without fsync; its acknowledgments do not count toward the majority");
        } else if !is_peer {
            tracing::info!(peer = %open.peer_addr, database = %dbn.name, "not a configured peer; its acknowledgments are recorded, not counted");
        }
        let id = dbn.sync.attach(&open.peer_addr, open.durable, counted);
        let stream_cancel = dbn.cancel.child_token();
        let rx = spawn_chunk_stream(
            self.node.clone(),
            dbn.clone(),
            open.from_position,
            stream_cancel.clone(),
            true,
        );
        let peer = open.peer_addr;
        let dbn2 = dbn.clone();
        tokio::spawn(async move {
            loop {
                let m = tokio::select! {
                    _ = stream_cancel.cancelled() => break,
                    m = inbound.message() => m,
                };
                match m {
                    Ok(Some(pb::FollowRequest {
                        msg: Some(pb::follow_request::Msg::Applied(a)),
                    })) => dbn2.sync.record(&peer, id, a.head, dbn2.db.head()),
                    // A second `open`, an empty message, an error or the
                    // end of the inbound stream all end the follow.
                    _ => break,
                }
            }
            dbn2.sync.detach(&peer, id);
            stream_cancel.cancel();
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
            Role::Fenced if self.node.cluster.is_none() => {
                return Err(self
                    .node
                    .refuse("fenced; restart it with --replicate-from the new leader"));
            }
            Role::Candidate if self.node.cluster.is_none() => {
                return Err(self.node.refuse("not a replica"));
            }
            Role::Replica | Role::Candidate | Role::Fenced => {}
        }
        // Its own task: a client that gives up mid-way must not leave the
        // puller stopped and the role unchanged.
        let node = self.node.clone();
        let (epoch, head) = tokio::spawn(async move {
            if node.cluster.is_some() {
                forced_campaign(node).await
            } else {
                promote(node).await
            }
        })
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

    async fn heartbeat(
        &self,
        req: Request<pb::HeartbeatRequest>,
    ) -> Result<Response<pb::HeartbeatResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        let node = self.node.clone();
        let r = req.into_inner();
        // Its own task: a leader that gives up on the call must not leave
        // a half-switched role behind.
        let answer = tokio::spawn(async move { cluster::handle_heartbeat(&node, r).await })
            .await
            .map_err(|e| codec::join("heartbeat", e))??;
        Ok(Response::new(answer))
    }

    async fn request_vote(
        &self,
        req: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        let node = self.node.clone();
        let r = req.into_inner();
        let answer = tokio::spawn(async move { cluster::handle_vote(&node, r).await })
            .await
            .map_err(|e| codec::join("request_vote", e))??;
        Ok(Response::new(answer))
    }

    async fn discover(
        &self,
        _req: Request<pb::DiscoverRequest>,
    ) -> Result<Response<pb::DiscoverResponse>, Status> {
        Ok(Response::new(cluster::discover(&self.node)))
    }

    type FetchBackupStream = ByteStream;

    /// A full backup as the archive's bytes, served in every role and
    /// without `--backup-dir`; the database's retention and other backups
    /// wait while it runs.
    async fn fetch_backup(
        &self,
        req: Request<pb::FetchBackupRequest>,
    ) -> Result<Response<Self::FetchBackupStream>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        #[cfg(feature = "test-util")]
        let dbn = {
            let alias = self
                .node
                .fetch_alias
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&dbn.name)
                .cloned();
            match alias {
                Some(other) => self.node.resolve_name(&other)?,
                None => dbn,
            }
        };
        let chunk = FETCH_CHUNK.min(self.node.max_message_bytes / 2).max(4096);
        let (tx, rx) = mpsc::channel(4);
        let node = self.node.clone();
        let db = dbn.db.clone();
        let name = dbn.name.clone();
        let cancel = dbn.cancel.clone();
        tokio::task::spawn_blocking(move || {
            let mut sink = ChunkSink {
                buf: Vec::with_capacity(chunk),
                chunk,
                tx: tx.clone(),
                cancel: cancel.clone(),
            };
            let result = db.backup_to(&mut sink).and_then(|m| {
                std::io::Write::flush(&mut sink)
                    .map(|()| m)
                    .map_err(|e| chron::Error::Io {
                        path: std::path::PathBuf::from("<backup stream>"),
                        op: "write",
                        source: e,
                    })
            });
            let label = match result {
                Ok(_) => "ok",
                Err(chron::Error::Io { source, .. })
                    if matches!(
                        source.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::Interrupted
                    ) =>
                {
                    if cancel.is_cancelled() {
                        let _ = tx.blocking_send(Err(gone(&node, &dbn)));
                    }
                    "cancelled"
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(codec::status(e)));
                    "error"
                }
            };
            node.metrics
                .backup_fetches
                .with_label_values(&[&name, label])
                .inc();
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn fingerprints(
        &self,
        req: Request<pb::FingerprintsRequest>,
    ) -> Result<Response<pb::FingerprintsResponse>, Status> {
        self.node.require_admin(req.metadata())?;
        let dbn = self.db(&req)?;
        let positions = req.into_inner().positions;
        if positions.len() > 4096 {
            return Err(Status::invalid_argument(
                "at most 4096 positions per Fingerprints call",
            ));
        }
        let fingerprints = blocking(&dbn.db, "fingerprints", move |db| {
            positions
                .iter()
                .map(|p| {
                    Ok(pb::FingerprintAt {
                        position: *p,
                        crc: db.fingerprint(*p)?,
                    })
                })
                .collect::<chron::Result<Vec<_>>>()
        })
        .await?;
        Ok(Response::new(pb::FingerprintsResponse { fingerprints }))
    }
}

/// Promote with peers: a forced campaign, retried a few times while the
/// leader says this node is still catching up.
async fn forced_campaign(node: Arc<Node>) -> Result<(u64, u64), Status> {
    let mut last = None;
    for _ in 0..5 {
        match cluster::campaign(&node, true).await {
            Ok(r) => return Ok(r),
            Err(s) if s.message().starts_with("not up to date") => {
                last = Some(s);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(s) => return Err(s),
        }
    }
    Err(last.expect("five attempts"))
}

/// Stops the discoverer and every puller, promotes, and on failure
/// resumes pulling.
async fn promote(node: Arc<Node>) -> Result<(u64, u64), Status> {
    let old_leader = node.upstream();
    replica::stop_all(&node).await;
    let n = node.clone();
    let result = tokio::task::spawn_blocking(move || n.promote_locked(None, None))
        .await
        .map_err(|e| codec::join("promote", e))?;
    match result {
        Ok((epoch, head)) => {
            if let Some(up) = old_leader {
                spawn_fencer(node, epoch, up);
            }
            Ok((epoch, head))
        }
        Err(s) => {
            if node.role() == Role::Replica {
                replica::start_all(&node).await;
            }
            Err(s)
        }
    }
}

/// Tells the old leader at `up`, with retries, that `epoch` exists. Best
/// effort: the epoch on Commit is the guarantee, this call is the
/// courtesy.
pub fn spawn_fencer(node: Arc<Node>, epoch: u64, up: Upstream) {
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
