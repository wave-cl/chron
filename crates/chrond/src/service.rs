//! The `Chron` service over one [`Db`].

use std::pin::Pin;

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

/// Events per page through the library, and the channel depth of a stream.
const PAGE: usize = 256;

type EventStream = Pin<Box<dyn Stream<Item = Result<pb::Event, Status>> + Send + 'static>>;

pub struct ChronService {
    db: Db,
    cancel: CancellationToken,
}

impl ChronService {
    pub fn new(db: Db, cancel: CancellationToken) -> Self {
        ChronService { db, cancel }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        op: &'static str,
        f: impl FnOnce(Db) -> chron::Result<T> + Send + 'static,
    ) -> Result<T, Status> {
        let db = self.db.clone();
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
        let appends = req.into_inner().appends;
        let db = self.db.clone();
        let committed = tokio::task::spawn_blocking(move || -> Result<chron::Committed, Status> {
            let mut txn = db.transaction();
            for a in appends {
                codec::apply_append(&mut txn, a)?;
            }
            txn.commit().map_err(codec::status)
        })
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
            self.db.clone(),
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
            self.db.read_all(r.from, 0).map_err(codec::status)?;
        }
        let read = Read::All {
            backward: r.backward,
        };
        Ok(Response::new(finite(
            self.db.clone(),
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
            self.db.clone(),
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
            head: self.db.head(),
        }))
    }

    type SubscribeStream = EventStream;

    async fn subscribe(
        &self,
        req: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let r = req.into_inner();
        let mut sub = match r.tag {
            Some(tag) => self.db.subscribe_tag(tag, r.from),
            None => self.db.subscribe(r.from),
        }
        .batch(PAGE);
        let (tx, rx) = mpsc::channel(PAGE);
        let cancel = self.cancel.clone();
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
        Ok(Response::new(pb::InfoResponse {
            path: self.db.path().display().to_string(),
            head: info.head,
            floor: info.floor,
            segments: info.segments as u64,
            bytes: info.bytes,
            options: Some(codec::options(self.db.options())),
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
}
