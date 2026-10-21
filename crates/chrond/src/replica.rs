//! The replica side: connect to the leader, check the two histories agree
//! database by database, then pull chunks for as long as the daemon runs;
//! and a discovery task that follows the leader's list of databases.

use std::sync::Arc;
use std::time::Duration;

use chron_client::{ChunkStream, Client, ClientConfig, ClientError, FollowAcks, Role as WireRole};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::node::{DbNode, Node, Upstream};

const BACKOFF_MIN: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);
/// A bound on the leader's answer to `Info` or `ListDatabases`, so a hung
/// leader cannot hang the puller or a start.
const INFO_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a replica could not follow its leader.
#[derive(Debug)]
pub enum CheckError {
    /// The leader did not answer; worth retrying.
    Unreachable(String),
    /// The leader answered and the two cannot be paired; retrying will
    /// not help until something changes.
    Refused(String),
    /// The two histories part somewhere below this replica's head: it
    /// took writes of its own. With peers the replica cuts itself back;
    /// otherwise an operator restores it.
    Diverged {
        message: String,
        leader_head: u64,
        leader_floor: u64,
    },
    /// This replica's head lies below the leader's retention floor (or
    /// it is empty and the leader retained): nothing to pull from. With
    /// peers the replica restores itself from the leader's backup;
    /// otherwise an operator does.
    BelowFloor {
        message: String,
        leader: LeaderFacts,
    },
}

/// What `Info` on the leader said, for a restore.
#[derive(Debug, Clone)]
pub struct LeaderFacts {
    pub id: chron::Uuid,
    pub head: u64,
    pub floor: u64,
    pub epoch: u64,
}

impl CheckError {
    pub fn message(&self) -> &str {
        match self {
            CheckError::Unreachable(m) | CheckError::Refused(m) => m,
            CheckError::Diverged { message, .. } | CheckError::BelowFloor { message, .. } => {
                message
            }
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

/// An accepted replication stream and, with peers, the side to report
/// applied heads on.
pub struct Following {
    pub stream: ChunkStream,
    pub acks: Option<FollowAcks>,
}

/// Connects to the leader and checks this database can follow the
/// leader's database of the same name: same identity (an empty database
/// adopts the leader's), an epoch no newer than the leader's, a head no
/// further, and the same record before it. Nothing is written here until
/// the leader has accepted the stream. With peers the stream is a
/// `Follow` (acknowledged); a leader without it gets a plain
/// `Replicate`. The epoch is not adopted here: a chunk carries the head
/// the leader had when it took its epoch, and the follower adopts the
/// epoch once it holds that much.
pub async fn connect_and_check(node: &Arc<Node>, dbn: &DbNode) -> Result<Following, CheckError> {
    let up = node
        .upstream()
        .ok_or_else(|| CheckError::Refused("no leader configured".into()))?;
    let addr = up.addr.as_str();
    let client = connect(&up).await?.database(&dbn.name);
    let info = match tokio::time::timeout(INFO_TIMEOUT, client.info()).await {
        Ok(Ok(i)) => i,
        Ok(Err(ClientError::NotFound(_))) => {
            return Err(CheckError::Refused(format!(
                "{addr} has no database {}: it was dropped there; the local copy is kept",
                dbn.name
            )));
        }
        Ok(Err(e)) => return Err(unreachable(addr, e)),
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
    let db = dbn.db.clone();
    let (head, floor, id, epoch) = (db.head(), db.floor(), db.id(), db.epoch());
    let empty = head == 0 && floor == 0;
    if id != leader_id && !empty {
        return Err(CheckError::Refused(format!(
            "{} is database {id}; {addr} serves database {leader_id} as {}; a replica must be restored from a backup of the leader",
            node.database_dir(&dbn.name).display(),
            dbn.name
        )));
    }
    if epoch > info.epoch {
        return Err(CheckError::Refused(format!(
            "local epoch {epoch} is newer than the leader's {}",
            info.epoch
        )));
    }
    if head > info.head {
        return Err(CheckError::Diverged {
            message: format!(
                "diverged: this replica is at head {head}, the leader at {}; reset it from a backup of the leader",
                info.head
            ),
            leader_head: info.head,
            leader_floor: info.floor,
        });
    }
    // The record before the head is gone when the head sits on the floor
    // (restored from a backup of a retained leader); the leader skips the
    // check then too.
    let last_fingerprint = if head > floor {
        let db = db.clone();
        let f = blocking(move || db.fingerprint(head - 1))
            .await
            .map_err(CheckError::Refused)?;
        if f.is_none() {
            // The index has no record below the head: never valid, and
            // the leader would skip its check and feed a hole.
            return Err(CheckError::Refused(format!(
                "the index holds no record at {}, below the head {head}; rebuild the index (chron rebuild-index)",
                head - 1
            )));
        }
        f
    } else {
        None
    };
    let facts = LeaderFacts {
        id: leader_id,
        head: info.head,
        floor: info.floor,
        epoch: info.epoch,
    };
    let diverged = |e: ClientError| match &e {
        ClientError::Fenced { message, .. } if message.starts_with("diverged") => {
            CheckError::Diverged {
                message: format!("{addr}: {message}"),
                leader_head: info.head,
                leader_floor: info.floor,
            }
        }
        ClientError::BelowFloor { position, floor } => CheckError::BelowFloor {
            message: format!(
                "{addr}: this replica's head {position} is below the leader's floor {floor}; a backup of the leader is needed"
            ),
            leader: facts.clone(),
        },
        _ => unreachable(addr, e),
    };
    let following = match &node.cluster {
        Some(cfg) => {
            let durable = node.db_opts.fsync == chron::Fsync::Always;
            match client
                .follow(head, last_fingerprint, cfg.advertise.clone(), durable)
                .await
            {
                Ok((stream, acks)) => Following {
                    stream,
                    acks: Some(acks),
                },
                Err(ClientError::Status(s)) if s.code() == tonic::Code::Unimplemented => {
                    tracing::warn!(leader = %addr, "the leader runs a version without Follow; replicating without acknowledgments, so this node does not count toward its sync commits");
                    Following {
                        stream: client
                            .replicate(head, last_fingerprint)
                            .await
                            .map_err(diverged)?,
                        acks: None,
                    }
                }
                Err(e) => return Err(diverged(e)),
            }
        }
        None => Following {
            stream: client
                .replicate(head, last_fingerprint)
                .await
                .map_err(diverged)?,
            acks: None,
        },
    };
    if id != leader_id {
        let db = db.clone();
        blocking(move || db.adopt_id(leader_id))
            .await
            .map_err(CheckError::Refused)?;
    }
    // A manual replica has no elections to keep the epoch honest for;
    // it adopts the leader's at connect as before.
    if epoch < info.epoch && node.cluster.is_none() {
        let db = db.clone();
        let e = info.epoch;
        blocking(move || db.set_epoch(e))
            .await
            .map_err(CheckError::Refused)?;
    }
    Ok(following)
}

/// A diverged copy cuts itself back to the history it shares with the
/// leader: the shared prefix is found by comparing stored fingerprints
/// (galloping back from the lower of the two heads, then bisecting),
/// moved back to a transaction boundary, the dropped tail written to
/// `diverged-<epoch>-<from>-<head>.chronbak` in the database directory,
/// and the records cut. The probing can be cancelled between calls; the
/// backup, the cut and the reset notice run as one task that is always
/// finished, so a transition cannot leave a half-cut database or an
/// archive overwritten by a later, empty one. Returns the new head.
/// Why a resync could not cut back.
pub enum ResyncError {
    /// The shared history lies below a retention floor: a cut cannot
    /// reach it; only a backup of the leader can.
    NoSharedHistory(String),
    /// Anything else; retried later.
    Failed(String),
}

impl From<String> for ResyncError {
    fn from(s: String) -> Self {
        ResyncError::Failed(s)
    }
}

impl From<&str> for ResyncError {
    fn from(s: &str) -> Self {
        ResyncError::Failed(s.to_string())
    }
}

async fn resync(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    leader_head: u64,
    leader_floor: u64,
    cancel: &CancellationToken,
) -> Result<u64, ResyncError> {
    let up = node
        .upstream()
        .ok_or_else(|| ResyncError::Failed("no leader configured".into()))?;
    let client = connect(&up)
        .await
        .map_err(|e| ResyncError::Failed(e.to_string()))?
        .database(&dbn.name);
    let db = dbn.db.clone();
    let head = db.head();
    let hi = head.min(leader_head);
    let lo = db.floor().max(leader_floor);
    if hi <= lo {
        return Err(ResyncError::NoSharedHistory(format!(
            "no shared history above the floors ({lo}); restore this node from a backup of the leader"
        )));
    }
    // Candidate positions, highest first: hi-1, hi-2, hi-4, ... and lo.
    let mut probes = Vec::new();
    let mut step = 1u64;
    loop {
        match hi.checked_sub(step) {
            Some(p) if p > lo => probes.push(p),
            _ => {
                probes.push(lo);
                break;
            }
        }
        step = step.saturating_mul(2);
    }
    let ask = |positions: Vec<u64>| {
        let client = client.clone();
        let addr = up.addr.clone();
        async move {
            let answer = tokio::time::timeout(INFO_TIMEOUT, client.fingerprints(&positions))
                .await
                .map_err(|_| format!("{addr} did not answer Fingerprints within {INFO_TIMEOUT:?}"))?
                .map_err(|e| format!("Fingerprints on {addr}: {e}"))?;
            if answer.len() != positions.len() {
                return Err(format!(
                    "{addr} answered {} fingerprints for {} positions",
                    answer.len(),
                    positions.len()
                ));
            }
            Ok(answer)
        }
    };
    // Compared on the blocking pool: each is an index read and a pread.
    let compare = |positions: Vec<u64>, theirs: Vec<Option<u32>>| {
        let db = db.clone();
        blocking(move || {
            positions
                .iter()
                .zip(&theirs)
                .map(|(p, t)| Ok(db.fingerprint(*p)?.is_some_and(|m| Some(m) == *t)))
                .collect::<chron::Result<Vec<bool>>>()
        })
    };
    let theirs = ask(probes.clone()).await?;
    let same = compare(probes.clone(), theirs).await?;
    // The first probe (highest) that matches, and the lowest that did not.
    let Some(k) = same.iter().position(|m| *m) else {
        return Err(ResyncError::NoSharedHistory(format!(
            "no shared history between {lo} and {hi}; restore this node from a backup of the leader"
        )));
    };
    let mut good = probes[k];
    let mut bad = if k == 0 { hi } else { probes[k - 1] };
    while bad - good > 1 {
        if cancel.is_cancelled() {
            return Err("cancelled while looking for the shared history".into());
        }
        let mid = good + (bad - good) / 2;
        let t = ask(vec![mid]).await?;
        if compare(vec![mid], t).await?[0] {
            good = mid;
        } else {
            bad = mid;
        }
    }
    if bad >= head {
        return Err(ResyncError::Failed(format!(
            "the shared history reaches this node's head {head}, yet the leader refused it; restore this node from a backup of the leader"
        )));
    }
    // Positions below `bad` are shared; the cut goes at the transaction
    // boundary at or below `bad`, found by reading backward in pages.
    let db2 = db.clone();
    let to = blocking(move || {
        let mut cursor = bad;
        loop {
            let page = db2.read_all_backward(cursor, PAGE_BACK)?;
            let Some(last) = page.last() else {
                return Ok(lo);
            };
            for ev in &page {
                if ev.position < lo {
                    return Ok(lo);
                }
                if ev.is_first_in_txn() {
                    return Ok(ev.position);
                }
            }
            if last.position <= lo {
                return Ok(lo);
            }
            cursor = last.position - 1;
        }
    })
    .await?;
    if to >= head {
        return Err(ResyncError::Failed(format!(
            "the shared history reaches this node's head {head}, yet the leader refused it; restore this node from a backup of the leader"
        )));
    }
    // The cut itself: not cancellable, awaited whatever happens to the
    // caller, so a transition during a resync finds a whole database.
    let dbn2 = dbn.clone();
    let name = dbn.name.clone();
    let cut = tokio::spawn(async move {
        let db = dbn2.db.clone();
        let head_now = db.head();
        if head_now != head {
            return Err(format!(
                "the head moved from {head} to {head_now} while the shared history was being found; retrying"
            ));
        }
        let mut archive = db
            .path()
            .join(format!("diverged-{}-{to}-{head}.chronbak", db.epoch()));
        if archive.exists() {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos());
            archive = db.path().join(format!(
                "diverged-{}-{to}-{head}-{nanos}.chronbak",
                db.epoch()
            ));
        }
        let db2 = db.clone();
        let a2 = archive.clone();
        let meta = blocking(move || db2.backup_since(&a2, to)).await?;
        if meta.head != head {
            let _ = std::fs::remove_file(&archive);
            return Err(format!(
                "the head moved to {} while the tail was being archived; retrying",
                meta.head
            ));
        }
        let db2 = db.clone();
        let cut = blocking(move || db2.truncate_to(to)).await?;
        tracing::warn!(database = %name, from = to, removed = cut.removed, archive = %archive.display(), "dropped a diverged tail");
        dbn2.reset.send_replace(Some(to));
        Ok(to)
    });
    cut.await
        .map_err(|e| ResyncError::Failed(format!("cut task: {e}")))?
        .map_err(ResyncError::Failed)
}

/// A reader over the chunks of a fetched backup, for the library's
/// synchronous restore on the blocking pool.
struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    cur: bytes::Bytes,
}

impl std::io::Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            match self.rx.blocking_recv() {
                Some(b) => self.cur = b,
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.cur.len());
        buf[..n].copy_from_slice(&self.cur[..n]);
        self.cur = self.cur.slice(n..);
        Ok(n)
    }
}

/// Replaces this database with a copy restored from the leader's backup:
/// the archive streams straight into the library's restore at
/// `<data_dir>/<name>.fetched` (no file on disk), the copy's identity is
/// checked, and then, briefly serialised with the other database
/// operations, the old directory moves to `<name>.replaced-<nanos>` (an
/// empty one is removed), the copy takes its place, a new `DbNode` with
/// its own puller replaces this one in the map (`default` is never
/// absent) and the old node's streams end saying so. A failure before the
/// swap leaves the old database running. Returns once the new node is
/// in place; the caller, the old puller, then ends.
async fn self_restore(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    leader: &LeaderFacts,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let name = dbn.name.clone();
    let fetched = node.data_dir.join(format!("{name}.fetched"));
    let staging = node.data_dir.join(format!(".{name}.fetched.restore"));
    for leftover in [&fetched, &staging] {
        if leftover.exists() {
            std::fs::remove_dir_all(leftover)
                .map_err(|e| format!("cannot remove {}: {e}", leftover.display()))?;
        }
    }
    let up = node.upstream().ok_or("no leader configured")?;
    let client = connect(&up)
        .await
        .map_err(|e| e.to_string())?
        .database(&name);
    let mut stream = client
        .fetch_backup()
        .await
        .map_err(|e| format!("FetchBackup on {}: {e}", up.addr))?;
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(4);
    let target = fetched.clone();
    let opts = node.db_opts.clone();
    let restore = tokio::task::spawn_blocking(move || {
        chron::restore_backup_from(
            ChannelReader {
                rx,
                cur: bytes::Bytes::new(),
            },
            &target,
            &opts,
        )
    });
    let forwarded: Result<(), String> = async {
        loop {
            let item = tokio::select! {
                _ = cancel.cancelled() => return Err("cancelled during the fetch".to_string()),
                i = stream.next() => i,
            };
            match item {
                Some(Ok(b)) => {
                    if tx.send(b).await.is_err() {
                        return Err("the restore stopped reading".into());
                    }
                }
                Some(Err(e)) => return Err(format!("the backup stream failed: {e}")),
                None => return Ok(()),
            }
        }
    }
    .await;
    drop(tx);
    let restored = restore.await.map_err(|e| format!("restore task: {e}"))?;
    let fail = |node: &Node, name: &str, what: String| {
        node.metrics
            .restores
            .with_label_values(&[name, "error"])
            .inc();
        let _ = std::fs::remove_dir_all(&fetched);
        Err(what)
    };
    let meta = match (forwarded, restored) {
        (Err(e), _) => return fail(node, &name, e),
        (Ok(()), Err(e)) => return fail(node, &name, format!("restore: {e}")),
        (Ok(()), Ok(m)) => m,
    };
    if meta.db_id != Some(leader.id) {
        return fail(
            node,
            &name,
            format!(
                "the fetched backup is of database {:?}; this node follows {}",
                meta.db_id, leader.id
            ),
        );
    }
    // The swap.
    let _ops = node.admin_ops.lock().await;
    let current = node.get(&name);
    if !current.as_ref().is_some_and(|d| Arc::ptr_eq(d, dbn)) || !node.role().follows() {
        return fail(
            node,
            &name,
            "the database or the role changed during the fetch; not swapped".into(),
        );
    }
    dbn.replaced
        .store(true, std::sync::atomic::Ordering::Release);
    dbn.cancel.cancel();
    let old_dir = node.database_dir(&name);
    let (old_head, old_floor, old_epoch) = (dbn.db.head(), dbn.db.floor(), dbn.db.epoch());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let replaced = node.data_dir.join(format!("{name}.replaced-{nanos}"));
    let data_dir = node.data_dir.clone();
    let n = node.clone();
    let name2 = name.clone();
    let leader_epoch = leader.epoch;
    let (fetched2, old_dir2, replaced2) = (fetched.clone(), old_dir.clone(), replaced.clone());
    let opened = tokio::task::spawn_blocking(move || -> Result<chron::Db, String> {
        if old_head == 0 && old_floor == 0 {
            std::fs::remove_dir_all(&old_dir2)
                .map_err(|e| format!("cannot remove the empty {}: {e}", old_dir2.display()))?;
        } else {
            std::fs::rename(&old_dir2, &replaced2).map_err(|e| {
                format!(
                    "cannot move {} to {}: {e}",
                    old_dir2.display(),
                    replaced2.display()
                )
            })?;
        }
        std::fs::rename(&fetched2, &old_dir2)
            .map_err(|e| format!("cannot move the restored copy into place: {e}"))?;
        std::fs::File::open(&data_dir)
            .and_then(|d| d.sync_all())
            .map_err(|e| format!("cannot sync {}: {e}", data_dir.display()))?;
        let db = n.open_database(&name2).map_err(|e| e.to_string())?;
        // The copy holds everything the leader held at its election and
        // more, so the leader's epoch is honest for it.
        let epoch = leader_epoch.max(old_epoch).max(db.epoch());
        db.set_epoch(epoch).map_err(|e| e.to_string())?;
        Ok(db)
    })
    .await
    .map_err(|e| format!("swap task: {e}"))?;
    let db = match opened {
        Ok(db) => db,
        Err(e) => {
            node.metrics
                .restores
                .with_label_values(&[&name, "error"])
                .inc();
            return Err(format!(
                "{e}; the database directory may need a hand: {}",
                old_dir.display()
            ));
        }
    };
    let fresh = DbNode::new(name.clone(), db, node);
    {
        let mut puller = fresh.puller.lock().await;
        node.insert(fresh.clone());
        *puller = Some(spawn(node.clone(), fresh.clone(), None));
    }
    node.metrics
        .restores
        .with_label_values(&[&name, "ok"])
        .inc();
    tracing::warn!(database = %name, old_head, old_floor, head = fresh.db.head(), floor = fresh.db.floor(), leader_head = leader.head, leader_floor = leader.floor, kept = %replaced.display(), "replaced by a copy restored from the leader");
    Ok(())
}

/// Records per page when walking back to a transaction boundary.
const PAGE_BACK: usize = 256;

/// What the leader says about itself, for a restore after a failed cut.
async fn leader_facts(node: &Arc<Node>, dbn: &DbNode) -> Result<LeaderFacts, String> {
    let up = node.upstream().ok_or("no leader configured")?;
    let client = connect(&up)
        .await
        .map_err(|e| e.to_string())?
        .database(&dbn.name);
    let info = tokio::time::timeout(INFO_TIMEOUT, client.info())
        .await
        .map_err(|_| format!("{} did not answer Info", up.addr))?
        .map_err(|e| format!("Info on {}: {e}", up.addr))?;
    Ok(LeaderFacts {
        id: info.db_id,
        head: info.head,
        floor: info.floor,
        epoch: info.epoch,
    })
}

/// Stops the discoverer and every database's puller.
pub async fn stop_all(node: &Arc<Node>) {
    if let Some(d) = node.discoverer.lock().await.take() {
        d.stop().await;
    }
    for dbn in node.databases() {
        if let Some(p) = dbn.puller.lock().await.take() {
            p.stop().await;
        }
    }
}

/// Starts a puller for every database (towards the current upstream)
/// and the discoverer; ones already running are left alone.
pub async fn start_all(node: &Arc<Node>) {
    for dbn in node.databases() {
        let mut puller = dbn.puller.lock().await;
        if puller.is_none() {
            *puller = Some(spawn(node.clone(), dbn.clone(), None));
        }
    }
    let mut d = node.discoverer.lock().await;
    if d.is_none() {
        *d = Some(spawn_discoverer(node.clone()));
    }
}

/// A pull or discovery loop's handle.
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

/// Pulls `dbn` from `initial` (or reconnects first, when it is `None`)
/// until stopped. On exit the database is recorded as disconnected.
pub fn spawn(node: Arc<Node>, dbn: Arc<DbNode>, initial: Option<Following>) -> Puller {
    let cancel = dbn.cancel.child_token();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        pull(&node, &dbn, initial, &task_cancel).await;
        dbn.record_replica(|r| r.connected = false);
    });
    Puller { cancel, handle }
}

async fn pull(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    initial: Option<Following>,
    cancel: &CancellationToken,
) {
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
                    c = connect_and_check(node, dbn) => c,
                };
                match checked {
                    Ok(s) => {
                        backoff = BACKOFF_MIN;
                        node.discover_now.notify_one();
                        break s;
                    }
                    Err(CheckError::Diverged {
                        message,
                        leader_head,
                        leader_floor,
                    }) if node.cluster.is_some() => {
                        record_error(node, dbn, message);
                        // Not raced against `cancel`: the cut must finish
                        // once started; resync stops early by itself.
                        let r = resync(node, dbn, leader_head, leader_floor, cancel).await;
                        if cancel.is_cancelled() && r.is_err() {
                            return;
                        }
                        match r {
                            Ok(to) => {
                                tracing::warn!(database = %dbn.name, head = to, "cut back to the history shared with the leader; the dropped tail is kept as a backup");
                                backoff = BACKOFF_MIN;
                            }
                            Err(ResyncError::NoSharedHistory(e)) => {
                                record_error(node, dbn, e);
                                match leader_facts(node, dbn).await {
                                    Ok(facts) => {
                                        match self_restore(node, dbn, &facts, cancel).await {
                                            Ok(()) => return,
                                            Err(e) => record_error(node, dbn, e),
                                        }
                                    }
                                    Err(e) => record_error(node, dbn, e),
                                }
                            }
                            Err(ResyncError::Failed(e)) => record_error(node, dbn, e),
                        }
                    }
                    Err(CheckError::BelowFloor { message, leader }) if node.cluster.is_some() => {
                        record_error(node, dbn, message);
                        match self_restore(node, dbn, &leader, cancel).await {
                            // The new node has its own puller; this one ends.
                            Ok(()) => return,
                            Err(e) => record_error(node, dbn, e),
                        }
                    }
                    Err(CheckError::BelowFloor { message, .. }) => record_error(
                        node,
                        dbn,
                        format!(
                            "{message}; without peers: chron --addr LEADER backup --fetch FILE --db {}, chron restore FILE DIR/{}, then restart",
                            dbn.name, dbn.name
                        ),
                    ),
                    Err(e) => record_error(node, dbn, e.message().to_string()),
                }
            },
        };
        dbn.record_replica(|r| {
            r.connected = true;
            r.last_error = None;
        });
        match tail(node, dbn, s, cancel).await {
            Ok(()) => return,
            Err(e) => record_error(node, dbn, e),
        }
    }
}

pub fn record_error(node: &Node, dbn: &DbNode, e: String) {
    tracing::warn!(database = %dbn.name, error = %e, "replication");
    node.metrics
        .replication_errors
        .with_label_values(&[&dbn.name])
        .inc();
    dbn.record_replica(|r| {
        r.connected = false;
        r.last_error = Some(e);
    });
}

/// Applies chunks until the stream ends or the puller is cancelled. A
/// chunk being applied is never abandoned midway.
async fn tail(
    node: &Arc<Node>,
    dbn: &Arc<DbNode>,
    following: Following,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let Following { mut stream, acks } = following;
    #[cfg(feature = "test-util")]
    let mut hold = node.hold_apply.subscribe();
    // A chunk taken from the stream but not merged into the one being
    // applied: it opens the next round.
    let mut pending: Option<Result<chron_client::ReplicationChunk, ClientError>> = None;
    let mut last_round_end = std::time::Instant::now();
    loop {
        let msg = match pending.take() {
            Some(m) => Some(m),
            None => tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                m = stream.next() => m,
            },
        };
        let received = std::time::Instant::now();
        let idle_us = received.duration_since(last_round_end).as_micros() as u64;
        let mut c = msg
            .ok_or_else(|| "the leader closed the stream".to_string())?
            .map_err(|e| format!("the leader's stream failed: {e}"))?;
        let db = dbn.db.clone();
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
        // A manual replica adopts the epoch before applying, as before;
        // a group member only once it holds what the leader held at its
        // election (below), so its epoch says what it has.
        if c.epoch > ours && node.cluster.is_none() {
            let db = db.clone();
            let e = c.epoch;
            blocking(move || db.set_epoch(e)).await?;
        }
        #[cfg(feature = "test-util")]
        {
            let _ = tokio::select! {
                _ = cancel.cancelled() => return Ok(()),
                r = hold.wait_for(|h| !*h) => r.map(|_| ()),
            };
        }
        // Chunks that arrived while this one waited (for the previous
        // apply, or the hold) are landed with it as one batch: one
        // segment sync and one index commit per round of what the leader
        // sent, not one per leader batch. Only contiguous chunks under
        // the same identity and epoch merge; anything else, or an error,
        // opens the next round.
        let mut merged = 0u64;
        while c.chunk.from != c.chunk.to {
            let Some(next) = ready_chunk(&mut stream) else {
                break;
            };
            match next {
                Ok(n)
                    if n.db_id == c.db_id
                        && n.epoch == c.epoch
                        && n.epoch_base == c.epoch_base
                        && n.chunk.from == c.chunk.to
                        && n.chunk.from != n.chunk.to
                        && c.chunk.frames.len() + n.chunk.frames.len()
                            <= node.max_message_bytes =>
                {
                    c.chunk.to = n.chunk.to;
                    c.chunk.frames.extend_from_slice(&n.chunk.frames);
                    c.head = c.head.max(n.head);
                    c.confirmed = c.confirmed.max(n.confirmed);
                    merged += 1;
                }
                other => {
                    pending = Some(other);
                    break;
                }
            }
        }
        let from = c.chunk.from;
        let to = c.chunk.to;
        let empty = from == to;
        let new_head = if empty {
            // The empty chunk that opens a Follow: nothing to apply.
            db.head()
        } else {
            let chunk = c.chunk;
            let db = db.clone();
            blocking(move || db.apply_chunk(&chunk)).await?
        };
        if !empty && new_head != to {
            return Err(format!(
                "applied a chunk to {to} but the head is {new_head}"
            ));
        }
        if c.epoch > db.epoch() && node.cluster.is_some() && new_head >= c.epoch_base {
            let db = db.clone();
            let e = c.epoch;
            blocking(move || db.set_epoch(e)).await?;
        }
        let applied = std::time::Instant::now();
        if let Some(acks) = &acks {
            acks.applied(new_head)
                .await
                .map_err(|_| "the leader stopped reading acknowledgments".to_string())?;
        }
        last_round_end = std::time::Instant::now();
        // What the leader said a majority held when it cut the chunk; it
        // may exceed this copy's head until the next chunk, readers clamp.
        dbn.raise_confirmed(c.confirmed);
        // Timing of the replication path (see docs/operations.md, "Where
        // a sync commit's time goes"): how long this follower waited for
        // the chunk, applied it (segment write, fsync, index commit) and
        // reported it.
        tracing::trace!(
            target: "chrond::timing",
            database = %dbn.name,
            from,
            to,
            merged,
            idle_us,
            apply_us = applied.duration_since(received).as_micros() as u64,
            ack_us = last_round_end.duration_since(applied).as_micros() as u64,
            "follower round"
        );
        dbn.record_replica(|r| r.leader_head = Some(c.head));
        node.metrics
            .chunks_applied
            .with_label_values(&[&dbn.name])
            .inc_by(1 + merged);
        if !node.role().follows() {
            return Ok(());
        }
    }
}

/// The next chunk if the stream already holds one; `None` when it would
/// have to wait. A single poll with a waker that wakes nothing: the next
/// real await registers its own.
fn ready_chunk(
    stream: &mut ChunkStream,
) -> Option<Result<chron_client::ReplicationChunk, ClientError>> {
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    match stream.as_mut().poll_next(&mut cx) {
        Poll::Ready(item) => item,
        Poll::Pending => None,
    }
}

/// Follows the leader's list of databases: at spawn, every
/// `discover_interval`, and whenever a puller reconnects. A database the
/// leader has and this daemon does not is created locally and pulled; one
/// the leader no longer lists stops being pulled and is kept, read-only.
pub fn spawn_discoverer(node: Arc<Node>) -> Puller {
    let cancel = node.cancel.child_token();
    let task_cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        loop {
            let outcome = tokio::select! {
                _ = task_cancel.cancelled() => return,
                r = discover(&node) => r,
            };
            match outcome {
                Ok(()) => {
                    *node
                        .discovery_error
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = None
                }
                Err(e) => {
                    tracing::warn!(error = %e, "database discovery");
                    *node
                        .discovery_error
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(e);
                }
            }
            tokio::select! {
                _ = task_cancel.cancelled() => return,
                _ = tokio::time::sleep(node.discover_interval) => {}
                _ = node.discover_now.notified() => {}
            }
        }
    });
    Puller { cancel, handle }
}

async fn discover(node: &Arc<Node>) -> Result<(), String> {
    let up = node.upstream().ok_or("no leader configured")?;
    let client = connect(&up).await.map_err(|e| e.to_string())?;
    let listed = match tokio::time::timeout(INFO_TIMEOUT, client.list_databases()).await {
        Ok(r) => r.map_err(|e| format!("ListDatabases on {}: {e}", up.addr))?,
        Err(_) => return Err(format!("{} did not answer ListDatabases", up.addr)),
    };
    let names: std::collections::HashSet<String> = listed.into_iter().map(|d| d.name).collect();
    if !node.role().follows() {
        return Ok(());
    }
    for name in &names {
        if !chron_proto::valid_database_name(name) {
            tracing::warn!(database = %name, "the leader lists a database with an invalid name; skipped");
            continue;
        }
        let dbn = match node.get(name) {
            Some(d) => d,
            None => {
                let n = node.clone();
                let name2 = name.clone();
                let db = tokio::task::spawn_blocking(move || n.open_database(&name2))
                    .await
                    .map_err(|e| format!("open task: {e}"))?
                    .map_err(|e| format!("cannot create the database {name}: {e}"))?;
                let dbn = DbNode::new(name.clone(), db, node);
                node.insert(dbn.clone());
                tracing::info!(database = %name, "created to mirror the leader");
                dbn
            }
        };
        let mut puller = dbn.puller.lock().await;
        if puller.is_none() {
            *puller = Some(spawn(node.clone(), dbn.clone(), None));
        }
    }
    for dbn in node.databases() {
        if names.contains(&dbn.name) {
            continue;
        }
        let mut puller = dbn.puller.lock().await;
        if let Some(p) = puller.take() {
            p.stop().await;
            let msg = "dropped on the leader; the local copy is kept".to_string();
            tracing::warn!(database = %dbn.name, "{msg}");
            dbn.record_replica(|r| {
                r.connected = false;
                r.last_error = Some(msg);
            });
        }
    }
    Ok(())
}
