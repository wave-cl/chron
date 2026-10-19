//! What a daemon is, beyond its databases: its role, the markers that
//! survive restarts, the write gate that keeps the role check and a write
//! one critical section, and the named databases it serves.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chron::{Committed, Db};
use chron_proto::v1 as pb;
use chron_proto::{DEFAULT_DATABASE, valid_database_name};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tonic::metadata::MetadataMap;

use crate::auth;
use crate::codec;
use crate::metrics::Metrics;
use crate::replica::Puller;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader,
    Replica,
    Fenced,
    /// With peers: no leader is known and no lease is held. Writes are
    /// refused; a heartbeat makes it a replica, an election a leader.
    Candidate,
}

impl Role {
    pub fn wire(self) -> pb::Role {
        match self {
            Role::Leader => pb::Role::Leader,
            Role::Replica => pb::Role::Replica,
            Role::Fenced => pb::Role::Fenced,
            Role::Candidate => pb::Role::Candidate,
        }
    }
}

/// What a database's puller last saw.
#[derive(Debug, Clone, Default)]
pub struct ReplicaStatus {
    pub connected: bool,
    pub leader_head: Option<u64>,
    pub last_error: Option<String>,
}

/// `epoch N`: this directory promoted itself to leader under epoch N.
pub const PROMOTED_MARKER: &str = "promoted";
/// `epoch N`: a leader under epoch N exists; writes are refused.
pub const FENCED_MARKER: &str = "fenced";
/// `leader ADDR`: this directory follows a leader; it must be started
/// with `--replicate-from` or promoted.
pub const REPLICA_MARKER: &str = "replica";
/// `epoch N for ADDR`: the vote this node gave; it survives restarts so a
/// node never votes twice in one epoch.
pub const VOTE_MARKER: &str = "vote";

/// A rendezvous for tests: the next guarded write signals `entered`
/// after its role check, then waits for `proceed` before writing. Both
/// are two-party barriers; the test is the other party.
#[cfg(feature = "test-util")]
pub struct WritePause {
    pub entered: std::sync::Barrier,
    pub proceed: std::sync::Barrier,
}

#[cfg(feature = "test-util")]
impl WritePause {
    pub fn new() -> Arc<WritePause> {
        Arc::new(WritePause {
            entered: std::sync::Barrier::new(2),
            proceed: std::sync::Barrier::new(2),
        })
    }
}

/// How a replica reaches its leader.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub addr: String,
    pub token: Option<String>,
    pub tls: Option<chron_client::TlsConfig>,
    pub max_message_bytes: usize,
}

/// The peers of a node started with `--peer`, and how to reach them.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// The other nodes' addresses, as advertised.
    pub peers: Vec<String>,
    /// This node's own address as the peers reach it.
    pub advertise: String,
    pub token: Option<String>,
    pub tls: Option<chron_client::TlsConfig>,
    pub max_message_bytes: usize,
    /// How long a majority acknowledgment lets a leader accept writes.
    pub lease_ttl: Duration,
}

impl ClusterConfig {
    /// Votes or acknowledgments that make a majority, this node included.
    pub fn quorum(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    pub fn upstream(&self, addr: &str) -> Upstream {
        Upstream {
            addr: addr.to_string(),
            token: self.token.clone(),
            tls: self.tls.clone(),
            max_message_bytes: self.max_message_bytes,
        }
    }
}

/// The last leader (or candidate) a follower accepted: when, who, at
/// which epoch, and the lease ttl the sender runs with, so a peer with a
/// longer lease is waited for that long.
#[derive(Debug, Clone)]
pub struct Heard {
    pub at: Instant,
    pub addr: String,
    pub epoch: u64,
    pub ttl: Duration,
}

/// What a node knows about leadership in time: its own lease as a leader,
/// the last leader it heard as a follower, and the vote it gave.
#[derive(Debug, Clone, Default)]
pub struct Lease {
    /// A leader: until when a majority acknowledged it. Writes are refused
    /// past it.
    pub held_until: Option<Instant>,
    /// A follower: the last heartbeat or vote it accepted.
    pub heard: Option<Heard>,
    /// The vote given: epoch and candidate. Persisted in [`VOTE_MARKER`].
    pub voted: Option<(u64, String)>,
    /// The highest epoch a peer has answered with: a campaign starts
    /// above it.
    pub seen: u64,
}

/// One named database the daemon serves.
pub struct DbNode {
    pub name: String,
    pub db: Db,
    pub replica: Mutex<ReplicaStatus>,
    pub puller: tokio::sync::Mutex<Option<Puller>>,
    /// Cancelled when the database is dropped (or the daemon stops: it is
    /// a child of the daemon's token); its streams end then.
    pub cancel: CancellationToken,
    /// The position the database was last cut back to (a diverged copy
    /// resetting itself); every live stream on the database ends when it
    /// changes, since positions from there on are rewritten.
    pub reset: watch::Sender<Option<u64>>,
}

impl DbNode {
    pub fn new(name: String, db: Db, parent: &CancellationToken) -> Arc<DbNode> {
        Arc::new(DbNode {
            name,
            db,
            replica: Mutex::new(ReplicaStatus::default()),
            puller: tokio::sync::Mutex::new(None),
            cancel: parent.child_token(),
            reset: watch::Sender::new(None),
        })
    }

    pub fn replica_status(&self) -> ReplicaStatus {
        self.replica
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn record_replica(&self, f: impl FnOnce(&mut ReplicaStatus)) {
        f(&mut self.replica.lock().unwrap_or_else(|p| p.into_inner()));
    }

    pub fn info(&self) -> pb::DatabaseInfo {
        pb::DatabaseInfo {
            name: self.name.clone(),
            head: self.db.head(),
            floor: self.db.floor(),
            epoch: self.db.epoch(),
            db_id: self.db.id().as_bytes().to_vec(),
        }
    }
}

pub struct Node {
    pub data_dir: PathBuf,
    /// Where Backup writes and ApplyBackup reads; both refused without it.
    pub backup_dir: Option<PathBuf>,
    /// Required on the administrative calls when set.
    pub admin_token: Option<Arc<[u8]>>,
    diverged_warned: Mutex<Option<Instant>>,
    /// A test's pause inside the next guarded write (feature `test-util`).
    #[cfg(feature = "test-util")]
    pub write_pause: Mutex<Option<Arc<WritePause>>>,
    role: watch::Sender<Role>,
    /// Readers check the role and epoch and write; writers flip the role.
    write_gate: RwLock<()>,
    /// The leader this node follows; changes when a new leader is heard.
    upstream: RwLock<Option<Upstream>>,
    pub max_message_bytes: usize,
    /// Set when started with peers.
    pub cluster: Option<ClusterConfig>,
    lease: Mutex<Lease>,
    /// This node's address as clients and peers reach it; set once the
    /// listener is bound.
    advertise: std::sync::OnceLock<String>,
    /// Serialises role transitions driven by heartbeats and elections.
    pub transitions: tokio::sync::Mutex<()>,
    pub cluster_task: Mutex<Option<JoinHandle<()>>>,
    /// Set first thing in shutdown: cluster calls are refused and no
    /// transition starts after it.
    pub stopping: std::sync::atomic::AtomicBool,
    /// Peers this node neither calls nor answers (a simulated partition).
    #[cfg(feature = "test-util")]
    pub partition: Mutex<HashSet<String>>,
    /// Grant votes without the recency check (the control for the test
    /// that a follower cut off from the leader cannot unseat it).
    #[cfg(feature = "test-util")]
    pub skip_recency_check: std::sync::atomic::AtomicBool,
    /// How every database is opened.
    pub db_opts: chron::Options,
    databases: RwLock<HashMap<String, Arc<DbNode>>>,
    /// Serialises CreateDatabase and DropDatabase.
    pub admin_ops: tokio::sync::Mutex<()>,
    fenced_by: Mutex<Option<u64>>,
    pub fencer: Mutex<Option<JoinHandle<()>>>,
    /// The replica's discovery task: follows the leader's database list.
    pub discoverer: tokio::sync::Mutex<Option<Puller>>,
    pub discovery_error: Mutex<Option<String>>,
    /// Pokes the discoverer (a puller reconnected).
    pub discover_now: tokio::sync::Notify,
    pub discover_interval: Duration,
    pub metrics: Arc<Metrics>,
    pub cancel: CancellationToken,
}

impl Node {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        data_dir: PathBuf,
        backup_dir: Option<PathBuf>,
        admin_token: Option<&str>,
        upstream: Option<Upstream>,
        cluster: Option<ClusterConfig>,
        max_message_bytes: usize,
        db_opts: chron::Options,
        discover_interval: Duration,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    ) -> Node {
        Node {
            data_dir,
            backup_dir,
            admin_token: admin_token.map(|t| Arc::from(t.as_bytes())),
            diverged_warned: Mutex::new(None),
            #[cfg(feature = "test-util")]
            write_pause: Mutex::new(None),
            role: watch::Sender::new(Role::Leader),
            write_gate: RwLock::new(()),
            upstream: RwLock::new(upstream),
            max_message_bytes,
            cluster,
            lease: Mutex::new(Lease::default()),
            advertise: std::sync::OnceLock::new(),
            transitions: tokio::sync::Mutex::new(()),
            cluster_task: Mutex::new(None),
            stopping: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            partition: Mutex::new(HashSet::new()),
            #[cfg(feature = "test-util")]
            skip_recency_check: std::sync::atomic::AtomicBool::new(false),
            db_opts,
            databases: RwLock::new(HashMap::new()),
            admin_ops: tokio::sync::Mutex::new(()),
            fenced_by: Mutex::new(None),
            fencer: Mutex::new(None),
            discoverer: tokio::sync::Mutex::new(None),
            discovery_error: Mutex::new(None),
            discover_now: tokio::sync::Notify::new(),
            discover_interval,
            metrics,
            cancel,
        }
    }

    pub fn role(&self) -> Role {
        *self.role.borrow()
    }

    pub fn set_role(&self, role: Role) {
        self.role.send_replace(role);
    }

    /// Changes of role, for streams that must end on one.
    pub fn watch_role(&self) -> watch::Receiver<Role> {
        self.role.subscribe()
    }

    pub fn set_advertise(&self, addr: String) {
        let _ = self.advertise.set(addr);
    }

    /// This node's own address (`?` before the listener is bound).
    pub fn advertise(&self) -> String {
        self.advertise.get().cloned().unwrap_or_else(|| "?".into())
    }

    /// The leader this node follows, if any.
    pub fn upstream(&self) -> Option<Upstream> {
        self.upstream
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn set_upstream(&self, up: Option<Upstream>) {
        *self.upstream.write().unwrap_or_else(|p| p.into_inner()) = up;
    }

    /// The address of the leader this node follows.
    pub fn leader_addr(&self) -> Option<String> {
        self.upstream().map(|u| u.addr)
    }

    // ---- the lease

    pub fn lease(&self) -> Lease {
        self.lease.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    pub fn with_lease<T>(&self, f: impl FnOnce(&mut Lease) -> T) -> T {
        f(&mut self.lease.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// How much of the lease is left: `None` without peers (a lone leader
    /// needs none), zero once it expired.
    pub fn lease_remaining(&self) -> Option<Duration> {
        self.cluster.as_ref()?;
        let now = Instant::now();
        Some(
            self.lease()
                .held_until
                .map(|u| u.saturating_duration_since(now))
                .unwrap_or(Duration::ZERO),
        )
    }

    /// Whether a leader holds its lease right now (always, without peers).
    pub fn lease_held(&self) -> bool {
        match self.lease_remaining() {
            None => true,
            Some(d) => d > Duration::ZERO,
        }
    }

    /// The ttl this node waits out before it treats a leader as gone:
    /// its own, or the longer one the last leader heard runs with.
    pub fn silence_ttl(&self) -> Duration {
        let own = self
            .cluster
            .as_ref()
            .map_or(Duration::ZERO, |c| c.lease_ttl);
        self.lease().heard.map_or(own, |h| h.ttl.max(own))
    }

    /// When the last leader (or candidate) was heard, for the silence
    /// timer.
    pub fn heard_at(&self) -> Option<Instant> {
        self.lease().heard.map(|h| h.at)
    }

    /// The leader last heard, if it was heard within the lease ttl (its
    /// own ttl when longer than ours).
    pub fn heard_leader(&self) -> Option<(String, u64)> {
        self.cluster.as_ref()?;
        let ttl = self.silence_ttl();
        let h = self.lease().heard?;
        (h.at.elapsed() < ttl).then_some((h.addr, h.epoch))
    }

    /// Whether this daemon is shutting down: cluster calls and
    /// transitions are refused.
    pub fn is_stopping(&self) -> bool {
        self.stopping.load(std::sync::atomic::Ordering::Acquire)
    }

    /// The epoch this node has promised itself to: the highest of its
    /// data epoch, the vote it gave, the leader it heard, the epoch it was
    /// fenced by and any epoch a peer answered with. Heartbeats below it
    /// are refused, vote requests at or below it are stale, and a
    /// campaign starts above it.
    pub fn floor_epoch(&self) -> u64 {
        let l = self.lease();
        self.data_epoch()
            .max(l.voted.map_or(0, |(e, _)| e))
            .max(l.heard.map_or(0, |h| h.epoch))
            .max(self.fenced_by().unwrap_or(0))
            .max(l.seen)
    }

    /// Notes an epoch a peer answered with.
    pub fn note_seen(&self, epoch: u64) {
        self.with_lease(|l| l.seen = l.seen.max(epoch));
    }

    /// Records a heartbeat (or a vote given) from `addr` at `epoch`, whose
    /// sender runs a lease of `ttl`: the follower's recency clock starts
    /// now.
    pub fn record_heard(&self, addr: &str, epoch: u64, ttl: Duration) {
        self.with_lease(|l| {
            l.heard = Some(Heard {
                at: Instant::now(),
                addr: addr.to_string(),
                epoch,
                ttl,
            })
        });
    }

    /// Extends the lease to `until` (never shortens it); only a leader
    /// has one, so a round that ends after a step-down extends nothing.
    pub fn extend_lease(&self, until: Instant) {
        if self.role() != Role::Leader {
            return;
        }
        self.with_lease(|l| {
            if l.held_until.is_none_or(|u| u < until) {
                l.held_until = Some(until);
            }
        });
    }

    /// Reads `epoch N for ADDR` from the vote marker.
    pub fn read_vote_marker(&self) -> Option<(u64, String)> {
        parse_vote(&self.marker_text(VOTE_MARKER)?)
    }

    /// Persists the vote, then records it. Blocking.
    pub fn record_vote(&self, epoch: u64, candidate: &str) -> Result<(), Status> {
        self.write_marker_text(VOTE_MARKER, &format!("epoch {epoch} for {candidate}\n"))
            .map_err(|e| Status::internal(format!("cannot write the vote marker: {e}")))?;
        self.with_lease(|l| l.voted = Some((epoch, candidate.to_string())));
        Ok(())
    }

    /// Whether this node refuses to talk to `addr` (feature `test-util`).
    pub fn partitioned_from(&self, addr: &str) -> bool {
        #[cfg(feature = "test-util")]
        {
            self.partition
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains(addr)
        }
        #[cfg(not(feature = "test-util"))]
        {
            let _ = addr;
            false
        }
    }

    pub fn fenced_by(&self) -> Option<u64> {
        *self.fenced_by.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn set_fenced_by(&self, by: Option<u64>) {
        *self.fenced_by.lock().unwrap_or_else(|p| p.into_inner()) = by;
    }

    // ---- databases

    /// The database `name`, if the daemon has it.
    pub fn get(&self, name: &str) -> Option<Arc<DbNode>> {
        self.databases
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(name)
            .cloned()
    }

    /// The `default` database, which always exists once the daemon runs.
    pub fn default_db(&self) -> Arc<DbNode> {
        self.get(DEFAULT_DATABASE)
            .expect("the default database exists")
    }

    /// Every database, by name.
    pub fn databases(&self) -> Vec<Arc<DbNode>> {
        let mut all: Vec<Arc<DbNode>> = self
            .databases
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        all
    }

    /// The database a call names with the `chron-db` metadata, `default`
    /// without it.
    pub fn resolve(&self, meta: &MetadataMap) -> Result<Arc<DbNode>, Status> {
        let name = match meta.get(chron_proto::DATABASE_HEADER) {
            None => DEFAULT_DATABASE,
            Some(v) => v
                .to_str()
                .map_err(|_| Status::invalid_argument("the chron-db metadata is not valid text"))?,
        };
        self.resolve_name(name)
    }

    /// The database called `name`: INVALID_ARGUMENT for a bad name,
    /// NOT_FOUND for one the daemon does not have.
    pub fn resolve_name(&self, name: &str) -> Result<Arc<DbNode>, Status> {
        if !valid_database_name(name) {
            return Err(Status::invalid_argument(format!(
                "{name:?} is not a database name; names are 1 to 64 of a-z, 0-9, _ and -"
            )));
        }
        self.get(name).ok_or_else(|| {
            Status::not_found(format!(
                "no database {name:?}; create it with CreateDatabase (chron db create {name})"
            ))
        })
    }

    pub fn database_dir(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    /// Opens (creating if missing) the database directory `name`. Blocking.
    pub fn open_database(&self, name: &str) -> chron::Result<Db> {
        Db::open(self.database_dir(name), self.db_opts.clone())
    }

    pub fn insert(&self, dbn: Arc<DbNode>) {
        self.databases
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(dbn.name.clone(), dbn);
    }

    pub fn remove(&self, name: &str) -> Option<Arc<DbNode>> {
        self.databases
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(name)
    }

    /// The daemon's epoch: every database carries the same one; `default`
    /// is the anchor.
    pub fn epoch(&self) -> u64 {
        self.default_db().db.epoch()
    }

    /// Raises every database below `epoch` to it. Blocking.
    pub fn raise_all_epochs(&self, epoch: u64) -> chron::Result<()> {
        for d in self.databases() {
            if d.db.epoch() < epoch {
                d.db.set_epoch(epoch)?;
            }
        }
        Ok(())
    }

    // ---- markers

    pub fn marker(&self, name: &str) -> PathBuf {
        self.data_dir.join(name)
    }

    /// The marker's text, if present.
    pub fn marker_text(&self, name: &str) -> Option<String> {
        fs::read_to_string(self.marker(name)).ok()
    }

    /// Reads `epoch N` from a marker, if present.
    pub fn read_marker(&self, name: &str) -> Option<u64> {
        let text = self.marker_text(name)?;
        text.trim().strip_prefix("epoch ")?.parse().ok()
    }

    pub fn write_marker(&self, name: &str, epoch: u64) -> std::io::Result<()> {
        self.write_marker_text(name, &format!("epoch {epoch}\n"))
    }

    pub fn write_marker_text(&self, name: &str, text: &str) -> std::io::Result<()> {
        let tmp = self.marker(&format!("{name}.tmp"));
        fs::write(&tmp, text)?;
        fs::File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, self.marker(name))?;
        fs::File::open(&self.data_dir)?.sync_all()
    }

    pub fn remove_marker(&self, name: &str) -> std::io::Result<()> {
        match fs::remove_file(self.marker(name)) {
            Ok(()) => fs::File::open(&self.data_dir)?.sync_all(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    // ---- admin and backups

    /// PERMISSION_DENIED unless the call carries the admin token (when one
    /// is configured; otherwise the interceptor's check is enough).
    pub fn require_admin(&self, meta: &MetadataMap) -> Result<(), Status> {
        self.require_admin_token(auth::bearer(meta))
    }

    /// [`require_admin`](Node::require_admin) given the bearer token the
    /// call carried, whatever the transport.
    pub fn require_admin_token(&self, bearer: Option<&str>) -> Result<(), Status> {
        let Some(want) = &self.admin_token else {
            return Ok(());
        };
        match bearer {
            Some(t) if auth::matches(t, want) => Ok(()),
            _ => Err(Status::permission_denied(
                "this call needs the admin token (--admin-token on the daemon)",
            )),
        }
    }

    /// `archive` under the backup directory's subdirectory for `database`:
    /// a bare file name, nothing that could leave the directory. The
    /// subdirectory is created.
    pub fn backup_path(&self, database: &str, archive: &str) -> Result<PathBuf, Status> {
        let Some(dir) = &self.backup_dir else {
            return Err(Status::failed_precondition(
                "backups over the daemon are disabled: start it with --backup-dir",
            ));
        };
        if archive.is_empty()
            || archive == "."
            || archive == ".."
            || archive.contains(['/', '\\', '\0'])
            || Path::new(archive).components().count() != 1
            || Path::new(archive).has_root()
        {
            return Err(Status::invalid_argument(format!(
                "{archive:?} is not a bare file name; backups live under the daemon's --backup-dir"
            )));
        }
        let sub = dir.join(database);
        fs::create_dir_all(&sub).map_err(|e| {
            Status::internal(format!(
                "cannot create the backup directory {}: {e}",
                sub.display()
            ))
        })?;
        Ok(sub.join(archive))
    }

    /// A diverged replica keeps knocking; log it at most once a minute.
    pub fn warn_diverged(&self, message: &str) {
        let mut last = self
            .diverged_warned
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let now = Instant::now();
        if last.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(60)) {
            *last = Some(now);
            tracing::warn!(message, "a replica was refused as diverged");
        }
    }

    // ---- writes and the role

    /// FAILED_PRECONDITION carrying `epoch` (a database's, or the daemon's).
    pub fn refuse_at(&self, epoch: u64, message: impl Into<String>) -> Status {
        let mut s = Status::failed_precondition(message.into());
        if let Ok(v) = epoch.to_string().parse() {
            s.metadata_mut().insert(chron_proto::EPOCH_HEADER, v);
        }
        s
    }

    /// FAILED_PRECONDITION carrying the daemon's epoch.
    pub fn refuse(&self, message: impl Into<String>) -> Status {
        self.refuse_at(self.epoch(), message)
    }

    /// Why this node refuses writes right now, if it does. A leader with
    /// peers refuses once its majority lease has expired, whether or not
    /// the cluster task has noticed.
    pub fn write_refusal(&self) -> Option<Status> {
        match self.role() {
            Role::Leader if self.lease_held() => None,
            Role::Leader => Some(self.refuse(
                "the leader's majority lease has expired; writes resume when a majority answers again or another node is elected",
            )),
            Role::Replica => Some(self.refuse(format!(
                "read-only replica of {}; send writes to the leader",
                self.leader_addr().as_deref().unwrap_or("?")
            ))),
            Role::Fenced => Some(self.refuse(format!(
                "fenced: a newer leader (epoch {}) exists; send writes there",
                self.fenced_by().unwrap_or(0)
            ))),
            Role::Candidate => Some(self.refuse(
                "no leader: this node holds no lease and has heard none; retry on the node Discover names",
            )),
        }
    }

    /// Runs `f` on `dbn` while the role is Leader and no role change can
    /// land. Blocking: `f` is the write itself.
    pub fn write_guarded<T>(
        &self,
        dbn: &DbNode,
        f: impl FnOnce(&Db) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let _gate = self.write_gate.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = self.write_refusal() {
            return Err(s);
        }
        #[cfg(feature = "test-util")]
        if let Some(p) = self
            .write_pause
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
        {
            p.entered.wait();
            p.proceed.wait();
        }
        f(&dbn.db)
    }

    /// Checks the role and the writer's epoch against the database's, then
    /// commits. Blocking. An epoch exactly one above this leader's fences
    /// it; a larger jump is refused without taking effect, since no leader
    /// can be that far ahead of a node it was promoted from.
    pub fn commit_guarded(
        &self,
        dbn: &DbNode,
        appends: Vec<pb::Append>,
        epoch: Option<u64>,
    ) -> Result<Committed, Status> {
        let gate = self.write_gate.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = self.write_refusal() {
            return Err(s);
        }
        let ours = dbn.db.epoch();
        if let Some(e) = epoch {
            if e < ours {
                return Err(self.refuse_at(
                    ours,
                    format!("stale epoch {e}; the leader is at epoch {ours}; refresh it from Info"),
                ));
            }
            // Without peers a writer's epoch token is how a promotion
            // reaches an old leader; with peers elections do that, and a
            // client must not be able to unseat the leader.
            if e == ours + 1 && self.cluster.is_none() {
                drop(gate);
                self.fence(e)?;
                return Err(self.refuse_at(
                    ours,
                    format!("fenced: a newer leader (epoch {e}) exists; send writes there"),
                ));
            }
            if e > ours {
                return Err(self.refuse_at(
                    ours,
                    format!(
                        "epoch {e} is more than one above this leader's {ours}; refresh it from Info"
                    ),
                ));
            }
        }
        let mut txn = dbn.db.transaction();
        for a in appends {
            codec::apply_append(&mut txn, a)?;
        }
        txn.commit().map_err(codec::status)
    }

    /// Records that a leader at `by` exists; this node refuses writes from
    /// now on and across restarts. Blocking.
    pub fn fence(&self, by: u64) -> Result<(), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        let ours = self.epoch();
        match self.role() {
            Role::Replica => {
                return Err(self.refuse(format!(
                    "this node is a replica of {}",
                    self.leader_addr().as_deref().unwrap_or("?")
                )));
            }
            Role::Leader | Role::Candidate if by <= ours => {
                return Err(
                    self.refuse(format!("epoch {by} is not newer than this leader's {ours}"))
                );
            }
            Role::Fenced if by <= self.fenced_by().unwrap_or(ours) => {
                return Err(self.refuse(format!(
                    "epoch {by} is not newer than the epoch this node was fenced by"
                )));
            }
            _ => {}
        }
        self.write_marker(FENCED_MARKER, by)
            .map_err(|e| Status::internal(format!("cannot write the fenced marker: {e}")))?;
        self.set_fenced_by(Some(by));
        self.set_role(Role::Fenced);
        self.with_lease(|l| l.held_until = None);
        tracing::warn!(by, "fenced: a newer leader exists");
        Ok(())
    }

    /// A leader whose lease expired becomes a candidate: writes were
    /// already refused by the lease check; this makes it visible and lets
    /// the node follow whoever is elected. Blocking.
    pub fn step_down(&self) {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Leader {
            return;
        }
        self.with_lease(|l| l.held_until = None);
        self.set_role(Role::Candidate);
        tracing::warn!("stepped down: the majority lease expired");
    }

    /// Follows the leader at `addr` from now on: the role becomes Replica,
    /// the replica marker names it, the fenced marker goes. Blocking; the
    /// caller starts the pullers. A leader must be fenced or stepped down
    /// first.
    pub fn become_replica_locked(&self, addr: &str) -> Result<(), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() == Role::Leader {
            return Err(self.refuse("still the leader"));
        }
        let up = self
            .cluster
            .as_ref()
            .map(|c| c.upstream(addr))
            .ok_or_else(|| Status::failed_precondition("no peers configured"))?;
        self.write_marker_text(REPLICA_MARKER, &format!("leader {addr}\n"))
            .map_err(|e| Status::internal(format!("cannot write the replica marker: {e}")))?;
        if let Err(e) = self.remove_marker(FENCED_MARKER) {
            tracing::warn!(error = %e, "cannot remove the fenced marker");
        }
        self.set_upstream(Some(up));
        self.set_fenced_by(None);
        self.with_lease(|l| l.held_until = None);
        self.set_role(Role::Replica);
        tracing::info!(leader = %addr, "following the leader");
        Ok(())
    }

    /// The highest epoch among the databases: the daemon's data epoch.
    pub fn data_epoch(&self) -> u64 {
        self.databases()
            .iter()
            .map(|d| d.db.epoch())
            .max()
            .unwrap_or(0)
    }

    /// The replica (or candidate) becomes the leader under `elected` (an
    /// election's epoch, which must be above the data epoch; the election
    /// is stale otherwise) or, without one, the next epoch; every
    /// database is raised to it. `lease_until` is the lease the
    /// election's votes grant. Blocking; the caller stopped the pullers
    /// first. The marker goes first: a crash between it and the epochs is
    /// repaired on the next start, which raises every database to the
    /// marker's.
    pub fn promote_locked(
        &self,
        elected: Option<u64>,
        lease_until: Option<Instant>,
    ) -> Result<(u64, u64), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if !matches!(self.role(), Role::Replica | Role::Candidate | Role::Fenced) {
            return Err(self.refuse("already the leader"));
        }
        let data = self.data_epoch();
        let epoch = match elected {
            Some(e) if e > data => e,
            Some(e) => {
                return Err(self.refuse(format!(
                    "the election was for epoch {e} but this node is already at {data}"
                )));
            }
            None => data + 1,
        };
        self.write_marker(PROMOTED_MARKER, epoch)
            .map_err(|e| Status::internal(format!("cannot write the promoted marker: {e}")))?;
        if let Err(e) = self.raise_all_epochs(epoch) {
            let _ = self.remove_marker(PROMOTED_MARKER);
            return Err(Status::internal(format!(
                "cannot raise the epoch on every database: {e}; databases already raised refuse to follow the old leader until the promotion is retried"
            )));
        }
        if let Err(e) = self.remove_marker(REPLICA_MARKER) {
            tracing::warn!(error = %e, "cannot remove the replica marker");
        }
        // A fenced node elected above the epoch that fenced it leads
        // again; the marker must not bring the fence back at a restart.
        if let Err(e) = self.remove_marker(FENCED_MARKER) {
            tracing::warn!(error = %e, "cannot remove the fenced marker");
        }
        self.with_lease(|l| {
            l.held_until = lease_until;
            l.heard = None;
        });
        self.set_fenced_by(None);
        self.set_upstream(None);
        self.set_role(Role::Leader);
        for d in self.databases() {
            d.record_replica(|r| r.connected = false);
        }
        tracing::info!(epoch, "promoted to leader");
        Ok((epoch, self.default_db().db.head()))
    }
}

/// `epoch N for ADDR`.
pub fn parse_vote(text: &str) -> Option<(u64, String)> {
    let rest = text.trim().strip_prefix("epoch ")?;
    let (epoch, addr) = rest.split_once(" for ")?;
    let addr = addr.trim();
    if addr.is_empty() {
        return None;
    }
    Some((epoch.parse().ok()?, addr.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_vote_marker_parses_and_rejects_junk() {
        assert_eq!(
            parse_vote("epoch 7 for http://a:1\n"),
            Some((7, "http://a:1".to_string()))
        );
        assert_eq!(parse_vote("epoch 7\n"), None);
        assert_eq!(parse_vote("epoch x for a"), None);
        assert_eq!(parse_vote("epoch 7 for "), None);
    }

    /// The lease check inside `write_refusal` refuses on its own, before
    /// the cluster task has noticed anything.
    #[test]
    fn a_leader_refuses_writes_the_instant_its_lease_has_expired() {
        let dir = tempfile::tempdir().unwrap();
        let cluster = ClusterConfig {
            peers: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            advertise: "http://127.0.0.1:3".into(),
            token: None,
            tls: None,
            max_message_bytes: 1 << 20,
            lease_ttl: Duration::from_secs(1),
        };
        let make = |cluster: Option<ClusterConfig>| {
            let cancel = CancellationToken::new();
            let node = Node::new(
                dir.path().to_path_buf(),
                None,
                None,
                None,
                cluster,
                1 << 20,
                chron::Options::new().fsync(chron::Fsync::Never),
                Duration::from_secs(1),
                Metrics::new(),
                cancel.clone(),
            );
            let db = node.open_database("default").unwrap();
            node.insert(DbNode::new("default".into(), db, &cancel));
            node.set_role(Role::Leader);
            node
        };
        let node = make(Some(cluster.clone()));
        assert!(node.write_refusal().is_some(), "no lease yet");
        node.with_lease(|l| l.held_until = Some(Instant::now() + Duration::from_secs(5)));
        assert!(node.write_refusal().is_none(), "a live lease");
        node.with_lease(|l| l.held_until = Some(Instant::now() - Duration::from_millis(1)));
        let r = node.write_refusal().expect("an expired lease refuses");
        assert!(r.message().contains("lease has expired"), "{r}");
        assert_eq!(node.role(), Role::Leader, "the role alone did not change");
        drop(node);
        let lone = make(None);
        assert!(lone.write_refusal().is_none(), "no peers, no lease needed");
    }

    #[test]
    fn quorum_is_a_majority_of_peers_and_self() {
        let c = |n: usize| ClusterConfig {
            peers: (0..n).map(|i| format!("p{i}")).collect(),
            advertise: "me".into(),
            token: None,
            tls: None,
            max_message_bytes: 1,
            lease_ttl: Duration::from_secs(1),
        };
        assert_eq!(c(0).quorum(), 1);
        assert_eq!(c(1).quorum(), 2);
        assert_eq!(c(2).quorum(), 2);
        assert_eq!(c(3).quorum(), 3);
        assert_eq!(c(4).quorum(), 3);
    }
}
