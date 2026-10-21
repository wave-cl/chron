//! Call logic shared by the gRPC service and the HTTP gateway: paging,
//! stream endings, subscriptions, database creation and removal, and the
//! info report. Both transports map errors from the same `Status`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chron::{Committed, Db, Event, Uuid};
use chron_proto::v1 as pb;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
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

/// The database's token fired: the daemon is stopping, the database was
/// replaced by a copy restored from the leader, or it was dropped.
pub fn gone(node: &Node, dbn: &DbNode) -> Status {
    if node.cancel.is_cancelled() {
        ended(None)
    } else if dbn.replaced.load(std::sync::atomic::Ordering::Acquire) {
        Status::unavailable(
            "the database was replaced by a copy restored from the leader; positions were rewritten, reconnect",
        )
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

/// What a read may see (see `Node::read_mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Everything the node holds.
    Local,
    /// Positions a majority durably holds.
    Confirmed,
}

impl Mode {
    pub fn wire(self) -> pb::Consistency {
        match self {
            Mode::Local => pb::Consistency::Local,
            Mode::Confirmed => pb::Consistency::Confirmed,
        }
    }
}

/// The exclusive cap a read under `mode` is served below: none for
/// local, the confirmed bound clamped to the head otherwise (a
/// follower learns the bound before it holds the positions). Without
/// peers the confirmed bound is the head: a lone node is its own
/// majority.
pub fn cap(node: &Node, dbn: &DbNode, mode: Mode) -> u64 {
    match mode {
        Mode::Local => u64::MAX,
        Mode::Confirmed if node.cluster.is_none() => u64::MAX,
        Mode::Confirmed => dbn.confirmed().min(dbn.db.head()),
    }
}

/// What `Head` answers under `mode`: the head, or the confirmed cap.
pub fn bound(node: &Node, dbn: &DbNode, mode: Mode) -> u64 {
    match mode {
        Mode::Local => dbn.db.head(),
        Mode::Confirmed => cap(node, dbn, mode).min(dbn.db.head()),
    }
}

/// Waits until the bound under `mode` reaches `at_least` (read your
/// writes: a commit's `next`), at most `timeout`; DEADLINE_EXCEEDED
/// past it with the bound in `chron-position` and the head in
/// `chron-head`.
pub async fn wait_bound(
    node: &Node,
    dbn: &DbNode,
    mode: Mode,
    at_least: u64,
    timeout: Duration,
) -> Result<u64, Status> {
    let mut conf = dbn.confirmed.subscribe();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);
    loop {
        let b = bound(node, dbn, mode);
        if b >= at_least {
            return Ok(b);
        }
        tokio::select! {
            _ = dbn.cancel.cancelled() => return Err(Status::unavailable("the database or daemon went away")),
            _ = &mut deadline => {
                let mut s = Status::deadline_exceeded(format!(
                    "the {} position is {b}, below {at_least}",
                    match mode { Mode::Local => "local", Mode::Confirmed => "confirmed" }
                ));
                for (key, v) in [
                    (chron_proto::POSITION_HEADER, b),
                    (chron_proto::HEAD_HEADER, dbn.db.head()),
                ] {
                    if let Ok(v) = v.to_string().parse() {
                        s.metadata_mut().insert(key, v);
                    }
                }
                return Err(s);
            }
            _ = dbn.db.wait_for_async(at_least.max(b + 1)) => {}
            r = conf.changed() => {
                if r.is_err() {
                    return Err(Status::unavailable("the database or daemon went away"));
                }
            }
        }
    }
}

/// The wait a request asked for: `at_least` with `wait_timeout_ms`
/// (absent: the daemon's sync timeout; 0: do not wait).
pub async fn wait_requested(
    node: &Node,
    dbn: &DbNode,
    mode: Mode,
    at_least: Option<u64>,
    wait_timeout_ms: Option<u64>,
) -> Result<(), Status> {
    let Some(p) = at_least else {
        return Ok(());
    };
    let timeout = wait_timeout_ms.map_or_else(|| node.sync_timeout(), Duration::from_millis);
    wait_bound(node, dbn, mode, p, timeout).await.map(|_| ())
}

/// The aggregate's length under `mode`: every event under local, the
/// events below the confirmed bound otherwise (the index's count is the
/// local one; the last event below the cap says how many lie under it).
pub async fn aggregate_len(node: &Node, dbn: &DbNode, id: Uuid, mode: Mode) -> Result<u64, Status> {
    let upto = cap(node, dbn, mode);
    blocking(&dbn.db, "aggregate_len", move |db| {
        if upto == u64::MAX {
            return db.aggregate_len(id);
        }
        Ok(db
            .read_aggregate_backward_upto(id, u64::MAX, 1, upto)?
            .first()
            .map_or(0, |e| e.index + 1))
    })
    .await
}

/// A durable cursor's subscription, as `subscribe` drives it.
pub struct CursorSub {
    pub name: String,
    pub session: u64,
    /// Cancelled by a takeover or a delete: the stream ends.
    pub cancel: CancellationToken,
}

/// Where the daemon keeps cursors: the leader of a group, or a node
/// without peers.
fn cursor_host(node: &Node) -> Result<(), Status> {
    if node.cluster.is_some() && node.role() != Role::Leader {
        return Err(node.refuse("cursors are kept on the leader"));
    }
    Ok(())
}

fn valid_cursor(name: &str) -> Result<(), Status> {
    if chron_proto::valid_database_name(name) {
        Ok(())
    } else {
        Err(Status::invalid_argument(format!(
            "cursor {name:?}: 1 to 64 characters of a-z, 0-9, _ and -"
        )))
    }
}

/// Registers a subscription on the cursor `name`: starts at the stored
/// position (at `from` for a new cursor), clamped to the floor, under a
/// new session that is persisted before the first event goes out; any
/// earlier subscription on the cursor is ended. Returns the start, the
/// session and the token that ends this stream.
pub async fn register_cursor(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    name: &str,
    from: u64,
) -> Result<(u64, u64, CancellationToken), Status> {
    valid_cursor(name)?;
    cursor_host(node)?;
    let n = name.to_string();
    let stored = blocking(&dbn.db, "cursor", move |db| db.cursor(&n)).await?;
    let floor = dbn.db.floor();
    let (start, session) = match stored {
        Some(c) => (c.next.max(floor), c.session + 1),
        None => (from.max(floor), 1),
    };
    let n = name.to_string();
    blocking(&dbn.db, "set_cursor", move |db| {
        db.set_cursor(&n, start, session)
    })
    .await?;
    let cancel = dbn.cancel.child_token();
    {
        let mut live = dbn
            .cursor_sessions
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(old) = live.insert(
            name.to_string(),
            crate::node::ActiveCursor {
                session,
                cancel: cancel.clone(),
            },
        ) {
            old.cancel.cancel();
        }
    }
    dbn.log_cursor(pb::CursorState {
        name: name.to_string(),
        next: start,
        session,
        deleted: false,
    });
    cursor_gauges(node, dbn, name, start).await;
    Ok((start, session, cancel))
}

/// Acknowledges a cursor's progress (see `AckRequest`).
pub async fn ack(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    name: &str,
    next: u64,
    session: u64,
) -> Result<u64, Status> {
    let result = ack_inner(node, dbn, name, next, session).await;
    let label = match &result {
        Ok((_, label)) => *label,
        Err((label, _)) => *label,
    };
    node.metrics
        .cursor_acks
        .with_label_values(&[&dbn.name, label])
        .inc();
    match result {
        Ok((next, _)) => Ok(next),
        Err((_, s)) => Err(s),
    }
}

async fn ack_inner(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    name: &str,
    next: u64,
    session: u64,
) -> Result<(u64, &'static str), (&'static str, Status)> {
    valid_cursor(name).map_err(|s| ("refused", s))?;
    cursor_host(node).map_err(|s| ("refused", s))?;
    let n = name.to_string();
    let stored = blocking(&dbn.db, "cursor", move |db| db.cursor(&n))
        .await
        .map_err(|s| ("refused", s))?;
    let Some(stored) = stored else {
        return Err((
            "not_found",
            Status::not_found(format!("cursor {name:?} does not exist")),
        ));
    };
    if session != stored.session {
        let mut s = Status::failed_precondition(format!(
            "cursor {name:?} was taken over by session {}",
            stored.session
        ));
        if let Ok(v) = stored.session.to_string().parse() {
            s.metadata_mut().insert(chron_proto::SESSION_HEADER, v);
        }
        return Err(("taken_over", s));
    }
    if next <= stored.next {
        return Ok((stored.next, "noop"));
    }
    let bound = cap(node, dbn, Mode::Confirmed).min(dbn.db.head());
    if next > bound {
        let mut s = Status::invalid_argument(format!(
            "cursor {name:?}: {next} is past the confirmed bound {bound}"
        ));
        if let Ok(v) = bound.to_string().parse() {
            s.metadata_mut().insert(chron_proto::POSITION_HEADER, v);
        }
        return Err(("ahead", s));
    }
    let n = name.to_string();
    blocking(&dbn.db, "set_cursor", move |db| {
        db.set_cursor(&n, next, session)
    })
    .await
    .map_err(|s| ("refused", s))?;
    dbn.log_cursor(pb::CursorState {
        name: name.to_string(),
        next,
        session,
        deleted: false,
    });
    cursor_gauges(node, dbn, name, next).await;
    Ok((next, "ok"))
}

/// Every cursor and the confirmed bound.
pub async fn list_cursors(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
) -> Result<pb::ListCursorsResponse, Status> {
    let cursors = blocking(&dbn.db, "cursors", |db| db.cursors()).await?;
    Ok(pb::ListCursorsResponse {
        cursors: cursors
            .into_iter()
            .map(|c| pb::CursorState {
                name: c.name,
                next: c.next,
                session: c.session,
                deleted: false,
            })
            .collect(),
        confirmed: bound(node, dbn, Mode::Confirmed),
    })
}

/// Removes a cursor (the caller checked the admin token); its live
/// subscription ends.
pub async fn delete_cursor(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    name: &str,
) -> Result<bool, Status> {
    valid_cursor(name)?;
    cursor_host(node)?;
    if let Some(live) = dbn
        .cursor_sessions
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(name)
    {
        live.cancel.cancel();
    }
    let n = name.to_string();
    let existed = blocking(&dbn.db, "remove_cursor", move |db| db.remove_cursor(&n)).await?;
    if existed {
        dbn.log_cursor(pb::CursorState {
            name: name.to_string(),
            next: 0,
            session: 0,
            deleted: true,
        });
        let _ = node
            .metrics
            .cursor_lag
            .remove_label_values(&[&dbn.name, name]);
        cursor_gauges(node, dbn, "", 0).await;
    }
    Ok(existed)
}

/// Updates the cursor gauges after a change (`name` empty: the count
/// only).
async fn cursor_gauges(node: &Node, dbn: &DbNode, name: &str, next: u64) {
    if let Ok(all) = blocking(&dbn.db, "cursors", |db| db.cursors()).await {
        node.metrics
            .cursors
            .with_label_values(&[&dbn.name])
            .set(all.len() as i64);
    }
    if !name.is_empty() {
        let lag = bound(node, dbn, Mode::Confirmed).saturating_sub(next);
        node.metrics
            .cursor_lag
            .with_label_values(&[&dbn.name, name])
            .set(lag as i64);
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
    /// pages, read newest first, keep the record bound only. Every page
    /// is served below `upto` (the read's cap, `u64::MAX` for none).
    pub fn page(&self, db: &Db, cursor: u64, n: usize, upto: u64) -> chron::Result<Vec<Event>> {
        let max_bytes = db.options().page_max_bytes;
        match self {
            Read::Aggregate {
                id,
                backward: false,
            } => db.read_aggregate_bounded_upto(*id, cursor, n, max_bytes, upto),
            Read::Aggregate { id, backward: true } => {
                db.read_aggregate_backward_upto(*id, cursor, n, upto)
            }
            Read::All { backward: false } => db.read_all_bounded_upto(cursor, n, max_bytes, upto),
            Read::All { backward: true } => db.read_all_backward_upto(cursor, n, upto),
            Read::Tag {
                tag,
                backward: false,
            } => db.read_tag_bounded_upto(tag, cursor, n, max_bytes, upto),
            Read::Tag {
                tag,
                backward: true,
            } => db.read_tag_backward_upto(tag, cursor, n, upto),
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
    upto: u64,
) -> Result<Vec<Event>, Status> {
    blocking(&dbn.db, "read", move |db| read.page(&db, cursor, n, upto)).await
}

/// Committed events from `from` (or those carrying `tag`), then live ones,
/// until the client goes away, the database is dropped or fenced, or the
/// daemon stops; the end reason is the last item.
pub fn subscribe(
    node: Arc<Node>,
    dbn: Arc<DbNode>,
    from: u64,
    tag: Option<Vec<u8>>,
    mode: Mode,
    cursor: Option<CursorSub>,
) -> ReceiverStream<Result<Event, Status>> {
    // The reset watch is taken before the subscription so a cut between
    // the two is not missed.
    let mut reset = dbn.reset.subscribe();
    let mut conf = dbn.confirmed.subscribe();
    let mut sub = match tag {
        Some(tag) => dbn.db.subscribe_tag(tag, from),
        None => dbn.db.subscribe(from),
    }
    .batch(PAGE);
    let (tx, rx) = mpsc::channel(PAGE);
    let cancel = dbn.cancel.clone();
    let cursor_cancel = cursor
        .as_ref()
        .map_or_else(CancellationToken::new, |c| c.cancel.clone());
    let mut role = node.watch_role();
    tokio::spawn(async move {
        // The registry entry goes when this stream ends, unless a later
        // session already took the cursor over.
        let _registered = cursor.as_ref().map(|c| CursorGuard {
            dbn: dbn.clone(),
            name: c.name.clone(),
            session: c.session,
        });
        loop {
            // Under the confirmed consistency the subscription is driven
            // below the cap and, once it has delivered everything below
            // it, waits for the bound to rise before asking again.
            let upto = cap(&node, &dbn, mode);
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    let _ = tx.send(Err(gone(&node, &dbn))).await;
                    return;
                }
                _ = cursor_cancel.cancelled() => {
                    let _ = tx.send(Err(cursor_ended(&dbn, cursor.as_ref()))).await;
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
                            let mut s = Status::unavailable(format!(
                                "the database was cut back to {to} to follow the leader; positions from there are being rewritten, subscribe again"
                            ));
                            if let Ok(v) = to.to_string().parse() {
                                s.metadata_mut().insert(chron_proto::POSITION_HEADER, v);
                            }
                            s
                        }
                        Err(_) => gone(&node, &dbn),
                    };
                    let _ = tx.send(Err(status)).await;
                    return;
                }
                _ = tx.closed() => return,
                r = sub.next_async_upto(upto) => r,
            };
            let item = match next {
                Ok(Some(ev)) => Ok(ev),
                Ok(None) => {
                    // Everything below the cap went out: wait for the
                    // bound to rise (the head rising alone changes nothing
                    // under the confirmed consistency).
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            let _ = tx.send(Err(gone(&node, &dbn))).await;
                            return;
                        }
                        _ = cursor_cancel.cancelled() => {
                            let _ = tx.send(Err(cursor_ended(&dbn, cursor.as_ref()))).await;
                            return;
                        }
                        r = role_lost(&mut role, false) => {
                            let _ = tx.send(Err(ended(Some(r)))).await;
                            return;
                        }
                        _ = tx.closed() => return,
                        alive = confirmed_above(&mut conf, upto) => {
                            if !alive {
                                let _ = tx.send(Err(gone(&node, &dbn))).await;
                                return;
                            }
                        }
                    }
                    continue;
                }
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

/// Waits until the confirmed watch rises above `upto`; false once the
/// sender is gone (the database was dropped).
async fn confirmed_above(rx: &mut watch::Receiver<u64>, upto: u64) -> bool {
    rx.wait_for(|c| *c > upto).await.is_ok()
}

/// Why a cursor subscription ended: the cursor was taken over by a newer
/// subscription (which now owns the session) or deleted.
fn cursor_ended(dbn: &DbNode, cursor: Option<&CursorSub>) -> Status {
    let name = cursor.map_or("", |c| c.name.as_str());
    let live = cursor.and_then(|c| {
        dbn.cursor_sessions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&c.name)
            .map(|a| a.session)
            .filter(|s| *s != c.session)
    });
    match live {
        Some(s) => {
            let mut st = Status::unavailable(format!(
                "cursor {name:?} was taken over by session {s}; this subscription ends"
            ));
            if let Ok(v) = s.to_string().parse() {
                st.metadata_mut().insert(chron_proto::SESSION_HEADER, v);
            }
            st
        }
        None => Status::unavailable(format!(
            "cursor {name:?} was deleted; this subscription ends"
        )),
    }
}

/// Removes a cursor's registry entry when its stream ends, unless a
/// newer session holds it.
struct CursorGuard {
    dbn: Arc<DbNode>,
    name: String,
    session: u64,
}

impl Drop for CursorGuard {
    fn drop(&mut self) {
        let mut live = self
            .dbn
            .cursor_sessions
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if live
            .get(&self.name)
            .is_some_and(|a| a.session == self.session)
        {
            live.remove(&self.name);
        }
    }
}

/// The whole commit: the guarded write on the blocking pool, then, when
/// sync is in effect, the wait for a majority to hold it. Observes the
/// commit metrics; `commit_duration` therefore includes the wait.
pub async fn commit(
    node: &Arc<Node>,
    dbn: Arc<DbNode>,
    appends: Vec<pb::Append>,
    epoch: Option<u64>,
    sync: Option<bool>,
    commit_id: Option<[u8; 16]>,
) -> Result<Committed, Status> {
    let start = std::time::Instant::now();
    let n = node.clone();
    let d = dbn.clone();
    let mut result =
        tokio::task::spawn_blocking(move || n.commit_guarded(&d, appends, epoch, commit_id))
            .await
            .map_err(|e| codec::join("commit", e))?;
    let landed = std::time::Instant::now();
    if let Ok(c) = &result
        && node.wants_sync(sync)
    {
        let timeout = node.sync_timeout();
        result = match crate::sync::wait_majority(node, &dbn, c.next, timeout).await {
            crate::sync::Waited::Confirmed(d) => {
                node.metrics
                    .sync_wait
                    .with_label_values(&[&dbn.name])
                    .observe(d.as_secs_f64());
                // Timing of the commit path (see docs/operations.md,
                // "Where a sync commit's time goes").
                tracing::trace!(
                    target: "chrond::timing",
                    database = %dbn.name,
                    next = c.next,
                    land_us = landed.duration_since(start).as_micros() as u64,
                    wait_us = d.as_micros() as u64,
                    "leader commit"
                );
                result
            }
            crate::sync::Waited::TimedOut => {
                node.metrics
                    .sync_timeouts
                    .with_label_values(&[&dbn.name, "timeout"])
                    .inc();
                Err(crate::sync::unconfirmed(
                    c,
                    &format!("no majority confirmed it within {timeout:?}"),
                ))
            }
            crate::sync::Waited::RoleLost(r) => {
                node.metrics
                    .sync_timeouts
                    .with_label_values(&[&dbn.name, "role_change"])
                    .inc();
                Err(crate::sync::unconfirmed(
                    c,
                    &format!("this node stopped leading ({r:?}) before a majority confirmed it"),
                ))
            }
        };
    }
    node.metrics.commit(&dbn.name, start, &result);
    result
}

/// The membership as this node holds it.
pub fn members(node: &Node) -> pb::GetMembersResponse {
    let st = node.members_state();
    pb::GetMembersResponse {
        members: Some(st.current.to_wire()),
        committed: st.committed,
        member: node.is_member(),
        leader_addr: crate::cluster::discover(node).leader_addr,
    }
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
    let members = node.members_state();
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
            .map_or_else(Vec::new, |c| members.current.others(&c.advertise)),
        voted_epoch: node.lease().voted.map(|(e, _)| e),
        majority_applied: (role == Role::Leader && node.cluster.is_some())
            .then(|| dbn.sync.majority()),
        followers: if role == Role::Leader && node.cluster.is_some() {
            dbn.sync.snapshot(&members.current)
        } else {
            Vec::new()
        },
        sync_commit: node.wants_sync(None),
        confirmed: node
            .cluster
            .is_some()
            .then(|| cap(node, dbn, Mode::Confirmed)),
        read_consistency: node.read_mode(pb::Consistency::Unspecified).wire() as i32,
        members_version: members.current.version,
        members_epoch: members.current.epoch,
        members_committed: members.committed,
        member: node.is_member(),
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
pub async fn create_database(
    node: Arc<Node>,
    name: String,
    options: crate::node::DbOptions,
) -> Result<Arc<DbNode>, Status> {
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
    if let Some(n) = options.max_record_bytes
        && n > node.max_message_bytes
    {
        return Err(Status::invalid_argument(format!(
            "max_record_bytes {n} is above the daemon's --max-message-bytes {}",
            node.max_message_bytes
        )));
    }
    let n = node.clone();
    let epoch = node.epoch();
    let name2 = name.clone();
    let db = tokio::task::spawn_blocking(move || {
        // The options file first: an orphan is harmless (the next create
        // overwrites it), a database without its file would open with
        // the daemon's defaults.
        n.write_db_options(&name2, &options)
            .map_err(|e| chron::Error::Io {
                path: n.options_file(&name2),
                op: "write",
                source: e,
            })?;
        let db = n.open_database(&name2)?;
        if db.epoch() < epoch {
            db.set_epoch(epoch)?;
        }
        Ok::<_, chron::Error>(db)
    })
    .await
    .map_err(|e| codec::join("create_database", e))?
    .map_err(codec::status)?;
    let dbn = DbNode::new(name.clone(), db, &node);
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
    let tomb = crate::node::Tombstone::now(dbn.db.id(), &name);
    drop(dbn);
    let n = node.clone();
    let dir = node.database_dir(&name);
    let name2 = name.clone();
    tokio::task::spawn_blocking(move || {
        // The tombstone before the files go: a crash in between leaves
        // a directory the next start removes (see open_all).
        n.record_tombstones(&[tomb])?;
        std::fs::remove_dir_all(&dir)?;
        n.remove_marker(&format!("{name2}.options"))
    })
    .await
    .map_err(|e| codec::join("drop_database", e))?
    .map_err(|e| Status::internal(format!("cannot remove the database: {e}")))?;
    tracing::info!(database = %name, "database dropped");
    Ok(())
}
