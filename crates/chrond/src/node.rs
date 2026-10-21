//! What a daemon is, beyond its databases: its role, the markers that
//! survive restarts, the write gate that keeps the role check and a write
//! one critical section, and the named databases it serves.

use std::collections::{BTreeMap, BTreeSet, HashMap};
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
    /// With peers: this node is not in the group's membership (removed,
    /// or not yet added). It serves reads, follows the leader it hears
    /// without voting or counting, never campaigns, refuses writes, and
    /// becomes a replica the moment a membership holding it arrives.
    Removed,
}

impl Role {
    pub fn wire(self) -> pb::Role {
        match self {
            Role::Leader => pb::Role::Leader,
            Role::Replica => pb::Role::Replica,
            Role::Fenced => pb::Role::Fenced,
            Role::Candidate => pb::Role::Candidate,
            Role::Removed => pb::Role::Removed,
        }
    }

    /// Whether the node follows a leader in this role (pullers run).
    pub fn follows(self) -> bool {
        matches!(self, Role::Replica | Role::Removed)
    }
}

/// `epoch N / version N / member ADDR...`: the group's membership this
/// node holds; replaces the `--peer` flags once written.
pub const MEMBERS_FILE: &str = "members";

/// The failover group's membership: every member's advertised address,
/// this node's included, stamped with the epoch of the leader that
/// published it and a version that rises by one per change. Generations
/// order by `(epoch, version)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Members {
    pub epoch: u64,
    pub version: u64,
    pub addrs: BTreeSet<String>,
}

impl Members {
    pub fn generation(&self) -> (u64, u64) {
        (self.epoch, self.version)
    }

    /// Votes or acknowledgments that make a majority of the membership.
    pub fn quorum(&self) -> usize {
        self.addrs.len() / 2 + 1
    }

    pub fn contains(&self, addr: &str) -> bool {
        self.addrs.contains(addr)
    }

    /// Every member but `me`.
    pub fn others(&self, me: &str) -> Vec<String> {
        self.addrs.iter().filter(|a| *a != me).cloned().collect()
    }

    pub fn to_wire(&self) -> pb::Members {
        pb::Members {
            epoch: self.epoch,
            version: self.version,
            addrs: self.addrs.iter().cloned().collect(),
        }
    }

    /// `None` for an empty or malformed set (an address with whitespace,
    /// or none at all).
    pub fn from_wire(m: &pb::Members) -> Option<Members> {
        let addrs: BTreeSet<String> = m.addrs.iter().map(|a| a.trim().to_string()).collect();
        if addrs.is_empty()
            || addrs
                .iter()
                .any(|a| a.is_empty() || a.chars().any(char::is_whitespace))
        {
            return None;
        }
        Some(Members {
            epoch: m.epoch,
            version: m.version,
            addrs,
        })
    }

    /// The file's text.
    pub fn text(&self) -> String {
        let mut s = format!("epoch {}\nversion {}\n", self.epoch, self.version);
        for a in &self.addrs {
            s.push_str("member ");
            s.push_str(a);
            s.push('\n');
        }
        s
    }

    /// Parses [`text`](Members::text); `None` for anything else.
    pub fn parse(text: &str) -> Option<Members> {
        let mut lines = text.lines().map(str::trim).filter(|l| !l.is_empty());
        let epoch = lines.next()?.strip_prefix("epoch ")?.parse().ok()?;
        let version = lines.next()?.strip_prefix("version ")?.parse().ok()?;
        let mut addrs = BTreeSet::new();
        for l in lines {
            let a = l.strip_prefix("member ")?.trim();
            if a.is_empty() || a.chars().any(char::is_whitespace) || !addrs.insert(a.to_string()) {
                return None;
            }
        }
        if addrs.is_empty() {
            return None;
        }
        Some(Members {
            epoch,
            version,
            addrs,
        })
    }

    pub fn oneline(&self) -> String {
        format!(
            "({},{}) {}",
            self.epoch,
            self.version,
            self.addrs.iter().cloned().collect::<Vec<_>>().join(",")
        )
    }
}

/// The membership as a node holds it, and on a leader whether the
/// current generation is committed: a majority of it acknowledged at
/// that generation and durably holds everything this leader had when
/// the generation was made.
#[derive(Debug, Clone)]
pub struct MembersState {
    pub current: Members,
    pub committed: bool,
    /// The last generation known committed; heartbeats fan out to its
    /// members and the current ones.
    pub last_committed: Members,
    /// Per database, the leader's head when `current` was made.
    pub created_heads: BTreeMap<String, u64>,
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

/// When a leader with peers acknowledges a commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncCommit {
    /// Once it is on the leader's disk.
    Off,
    /// Once a majority of the group durably holds it.
    Majority,
}

/// What a read, head or subscription sees when the request does not say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Everything this node holds.
    Local,
    /// Only positions a majority of the group durably holds.
    Confirmed,
}

/// How a node started with `--peer` reaches the group, and the group's
/// timing; the membership itself lives in [`Node::members`].
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// The `--peer` addresses this node was started with: the seed of a
    /// new group's membership, and where a node outside the membership
    /// looks for the leader.
    pub seeds: Vec<String>,
    /// This node's own address as the peers reach it.
    pub advertise: String,
    pub token: Option<String>,
    pub tls: Option<chron_client::TlsConfig>,
    pub max_message_bytes: usize,
    /// How long a majority acknowledgment lets a leader accept writes.
    pub lease_ttl: Duration,
    /// The daemon's default for commits without a `sync` flag.
    pub sync_commit: SyncCommit,
    /// How long a sync commit waits for a majority before it answers
    /// "unconfirmed".
    pub sync_timeout: Duration,
}

impl ClusterConfig {
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
    /// Set when a copy restored from the leader took this database's
    /// place: its streams end saying so.
    pub replaced: std::sync::atomic::AtomicBool,
    /// On a leader: the followers' applied positions and the majority.
    pub sync: crate::sync::SyncState,
    /// The head this database had when the node took its epoch, sent
    /// with every chunk so a follower adopts the epoch only once it
    /// holds that much.
    pub epoch_base: std::sync::atomic::AtomicU64,
    /// The confirmed bound: the highest position a majority of the group
    /// durably holds, as this node knows it. Only rises. On a leader the
    /// followers' reports raise it (through `sync`); on a follower the
    /// leader's chunks and heartbeats do. Reads under the confirmed
    /// consistency are capped at `min(confirmed, head)`.
    pub confirmed: Arc<watch::Sender<u64>>,
}

impl DbNode {
    pub fn new(name: String, db: Db, node: &Node) -> Arc<DbNode> {
        let _ = node;
        let confirmed = Arc::new(watch::Sender::new(0));
        Arc::new(DbNode {
            name,
            db,
            replica: Mutex::new(ReplicaStatus::default()),
            puller: tokio::sync::Mutex::new(None),
            cancel: node.cancel.child_token(),
            reset: watch::Sender::new(None),
            replaced: std::sync::atomic::AtomicBool::new(false),
            sync: crate::sync::SyncState::with_confirmed(confirmed.clone()),
            epoch_base: std::sync::atomic::AtomicU64::new(0),
            confirmed,
        })
    }

    /// Raises the confirmed bound to `p` if that is higher.
    pub fn raise_confirmed(&self, p: u64) {
        self.confirmed.send_if_modified(|c| {
            if p > *c {
                *c = p;
                true
            } else {
                false
            }
        });
    }

    /// The confirmed bound as this node knows it (may exceed the head on
    /// a follower still applying; readers clamp).
    pub fn confirmed(&self) -> u64 {
        *self.confirmed.borrow()
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
    /// The group's membership (see [`Members`]); a one-address set
    /// without peers.
    members: RwLock<MembersState>,
    /// Serialises `ChangeMembers`.
    pub member_changes: tokio::sync::Mutex<()>,
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
    pub partition: Mutex<std::collections::HashSet<String>>,
    /// Grant votes without the recency check (the control for the test
    /// that a follower cut off from the leader cannot unseat it).
    #[cfg(feature = "test-util")]
    pub skip_recency_check: std::sync::atomic::AtomicBool,
    /// While true, this follower applies no chunk (its acknowledgments
    /// stop) without its pullers stopping.
    #[cfg(feature = "test-util")]
    pub hold_apply: watch::Sender<bool>,
    /// Answer Follow with UNIMPLEMENTED, as a daemon before 0.4.0 does.
    #[cfg(feature = "test-util")]
    pub refuse_follow: std::sync::atomic::AtomicBool,
    /// Accept heartbeats and votes from any address (the control for the
    /// non-member refusal).
    #[cfg(feature = "test-util")]
    pub skip_member_check: std::sync::atomic::AtomicBool,
    /// Adopt a membership without persisting it (the control for
    /// persist-before-acknowledge).
    #[cfg(feature = "test-util")]
    pub adopt_in_memory_only: std::sync::atomic::AtomicBool,
    /// Commit a membership change on acknowledgments alone, without the
    /// new majority holding the heads (the control for that rule).
    #[cfg(feature = "test-util")]
    pub skip_catchup_rule: std::sync::atomic::AtomicBool,
    /// Lead without republishing the membership under the new epoch
    /// (the control for the republish rule).
    #[cfg(feature = "test-util")]
    pub skip_restamp: std::sync::atomic::AtomicBool,
    /// FetchBackup for a database serves another one instead (the test
    /// of a fetched backup that is not the database it was asked for).
    #[cfg(feature = "test-util")]
    pub fetch_alias: Mutex<HashMap<String, String>>,
    /// How every database is opened.
    pub db_opts: chron::Options,
    /// The consistency reads get when they do not ask (`--read-
    /// consistency`); `None` is confirmed when sync commits are in
    /// effect, local otherwise.
    pub read_consistency: Option<ReadConsistency>,
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
        read_consistency: Option<ReadConsistency>,
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
            members: RwLock::new(MembersState {
                current: Members {
                    epoch: 0,
                    version: 0,
                    addrs: cluster
                        .as_ref()
                        .map(|c| c.advertise.clone())
                        .into_iter()
                        .collect(),
                },
                committed: true,
                last_committed: Members {
                    epoch: 0,
                    version: 0,
                    addrs: cluster
                        .as_ref()
                        .map(|c| c.advertise.clone())
                        .into_iter()
                        .collect(),
                },
                created_heads: BTreeMap::new(),
            }),
            member_changes: tokio::sync::Mutex::new(()),
            cluster,
            lease: Mutex::new(Lease::default()),
            advertise: std::sync::OnceLock::new(),
            transitions: tokio::sync::Mutex::new(()),
            cluster_task: Mutex::new(None),
            stopping: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            partition: Mutex::new(std::collections::HashSet::new()),
            #[cfg(feature = "test-util")]
            skip_recency_check: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            hold_apply: watch::Sender::new(false),
            #[cfg(feature = "test-util")]
            refuse_follow: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            skip_member_check: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            adopt_in_memory_only: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            skip_catchup_rule: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            skip_restamp: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "test-util")]
            fetch_alias: Mutex::new(HashMap::new()),
            db_opts,
            read_consistency,
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

    // ---- membership

    /// The membership this node holds.
    pub fn members(&self) -> Members {
        self.members
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .current
            .clone()
    }

    pub fn members_state(&self) -> MembersState {
        self.members
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Whether this node is in the membership it holds.
    pub fn is_member(&self) -> bool {
        self.cluster
            .as_ref()
            .is_none_or(|c| self.members().contains(&c.advertise))
    }

    /// Votes or acknowledgments that make a majority of the current
    /// membership (1 without peers).
    pub fn quorum(&self) -> usize {
        self.members().quorum()
    }

    /// Every address a leader's heartbeats go to: the last committed
    /// membership and the current one, less itself.
    pub fn fanout(&self) -> Vec<String> {
        let me = self.cluster.as_ref().map(|c| c.advertise.clone());
        let st = self.members_state();
        let mut all: BTreeSet<String> = st.last_committed.addrs.clone();
        all.extend(st.current.addrs.iter().cloned());
        all.into_iter().filter(|a| Some(a) != me.as_ref()).collect()
    }

    /// Writes the membership file. Blocking.
    pub fn persist_members(&self, m: &Members) -> std::io::Result<()> {
        self.write_marker_text(MEMBERS_FILE, &m.text())
    }

    /// Reads the membership file, if present; `Err` when it does not
    /// parse.
    pub fn read_members_file(&self) -> Result<Option<Members>, String> {
        match self.marker_text(MEMBERS_FILE) {
            None => Ok(None),
            Some(text) => Members::parse(&text)
                .map(Some)
                .ok_or_else(|| format!("{} holds {text:?}", self.marker(MEMBERS_FILE).display())),
        }
    }

    /// Installs `m` as the membership this node holds, committed as far
    /// as this node is concerned (only a leader tracks a pending change),
    /// and settles the Replica/Removed role on it. The caller persisted
    /// it first.
    pub fn install_members(&self, m: Members) {
        {
            let mut st = self.members.write().unwrap_or_else(|p| p.into_inner());
            st.current = m.clone();
            st.last_committed = m;
            st.committed = true;
            st.created_heads.clear();
        }
        self.reconcile_membership_role();
    }

    /// Persists and installs `m`: what a node behind a sender does
    /// before acknowledging or voting. Blocking.
    pub fn adopt_members(&self, m: &Members) -> Result<(), Status> {
        self.persist_members(m)
            .map_err(|e| Status::internal(format!("cannot write the members file: {e}")))?;
        tracing::info!(members = %m.oneline(), "adopted the membership");
        self.install_members(m.clone());
        Ok(())
    }

    /// A leader's pending generation `m`: persisted, then current but
    /// not committed, with every database's head noted for the commit
    /// rule. Blocking.
    pub fn propose_members(&self, m: &Members) -> Result<(), Status> {
        self.persist_members(m)
            .map_err(|e| Status::internal(format!("cannot write the members file: {e}")))?;
        let heads: BTreeMap<String, u64> = self
            .databases()
            .iter()
            .map(|d| (d.name.clone(), d.db.head()))
            .collect();
        let mut st = self.members.write().unwrap_or_else(|p| p.into_inner());
        st.current = m.clone();
        st.committed = false;
        st.created_heads = heads;
        Ok(())
    }

    /// On election: the membership republished under this leader's
    /// epoch, to be committed before any change. Blocking.
    pub fn restamp_members(&self, epoch: u64) -> Result<(), Status> {
        let mut m = self.members();
        m.epoch = epoch;
        self.propose_members(&m)
    }

    /// The current generation, if it is `gen` and pending, is committed.
    pub fn mark_members_committed(&self, generation: (u64, u64)) -> bool {
        let mut st = self.members.write().unwrap_or_else(|p| p.into_inner());
        if st.committed || st.current.generation() != generation {
            return false;
        }
        st.committed = true;
        st.last_committed = st.current.clone();
        st.created_heads.clear();
        true
    }

    /// Whether a majority of the current membership durably holds, on
    /// every database, the head this leader had when the generation was
    /// made (the second commit condition: the new majority holds every
    /// write confirmed before the change).
    pub fn new_majority_holds_created_heads(&self) -> bool {
        let st = self.members_state();
        let me = self.cluster.as_ref().map(|c| c.advertise.clone());
        let self_counted = me.as_ref().is_some_and(|a| st.current.contains(a));
        self.databases().iter().all(|d| {
            let need = st.created_heads.get(&d.name).copied().unwrap_or(0);
            d.sync.majority_for(&st.current, self_counted) >= need
        })
    }

    /// Replica or Removed, by whether this node is in its membership;
    /// a leader, candidate or fenced node that is not a member becomes
    /// Removed (a leader after it stepped down).
    pub fn reconcile_membership_role(&self) {
        if self.cluster.is_none() {
            return;
        }
        let member = self.is_member();
        match (self.role(), member) {
            (Role::Replica, false) => {
                self.set_role(Role::Removed);
                tracing::warn!("not in the group's membership: running removed (reads only)");
            }
            (Role::Removed, true) => {
                self.set_role(Role::Replica);
                tracing::info!("in the group's membership again: a replica");
            }
            (Role::Candidate | Role::Fenced, false) => {
                self.set_role(Role::Removed);
                tracing::warn!("not in the group's membership: running removed (reads only)");
            }
            _ => {}
        }
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
            Role::Removed => Some(self.refuse(format!(
                "not a member of the group (membership version {}); send writes to {}",
                self.members().version,
                self.leader_addr().as_deref().unwrap_or("the leader")
            ))),
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
        commit_id: Option<[u8; 16]>,
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
        if let Some(id) = commit_id {
            txn.commit_id(id);
        }
        txn.commit().map_err(codec::status)
    }

    /// Records that a leader at `by` exists; this node refuses writes from
    /// now on and across restarts. Blocking.
    pub fn fence(&self, by: u64) -> Result<(), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        let ours = self.epoch();
        match self.role() {
            Role::Replica | Role::Removed => {
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
    /// the node follow whoever is elected. A leader no longer in the
    /// membership becomes Removed instead. Blocking.
    pub fn step_down(&self) {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Leader {
            return;
        }
        self.with_lease(|l| l.held_until = None);
        if self.is_member() {
            self.set_role(Role::Candidate);
            tracing::warn!("stepped down: the majority lease expired");
        } else {
            self.set_role(Role::Removed);
            tracing::warn!("stepped down: removed from the membership");
        }
    }

    /// A leader that committed its own removal stops leading at once.
    /// Blocking.
    pub fn step_down_removed(&self) {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Leader {
            return;
        }
        self.with_lease(|l| l.held_until = None);
        self.set_role(Role::Removed);
        tracing::warn!("stepped down: this node removed itself from the membership");
    }

    /// Follows the leader at `addr` from now on: the role becomes
    /// Replica (Removed when this node is not in the membership), the
    /// replica marker names it, the fenced marker goes. Blocking; the
    /// caller starts the pullers. A leader must be fenced or stepped
    /// down first.
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
        if self.is_member() {
            self.set_role(Role::Replica);
            tracing::info!(leader = %addr, "following the leader");
        } else {
            self.set_role(Role::Removed);
            tracing::info!(leader = %addr, "following the leader as a non-member (removed)");
        }
        Ok(())
    }

    /// Whether a commit waits for a majority: never without peers (a
    /// lone node is its own majority), else the request's choice or the
    /// daemon's default.
    pub fn wants_sync(&self, request: Option<bool>) -> bool {
        self.cluster
            .as_ref()
            .is_some_and(|c| request.unwrap_or(c.sync_commit == SyncCommit::Majority))
    }

    /// The consistency a read gets: the request's when it says, else the
    /// daemon's `--read-consistency`, else confirmed when sync commits
    /// are in effect. Without peers every read is local whatever is
    /// asked (a lone node is its own majority).
    pub fn read_mode(&self, requested: pb::Consistency) -> crate::ops::Mode {
        use crate::ops::Mode;
        if self.cluster.is_none() {
            return Mode::Local;
        }
        match requested {
            pb::Consistency::Local => Mode::Local,
            pb::Consistency::Confirmed => Mode::Confirmed,
            pb::Consistency::Unspecified => match self.read_consistency {
                Some(ReadConsistency::Local) => Mode::Local,
                Some(ReadConsistency::Confirmed) => Mode::Confirmed,
                None if self.wants_sync(None) => Mode::Confirmed,
                None => Mode::Local,
            },
        }
    }

    /// How long a sync commit waits before answering "unconfirmed".
    pub fn sync_timeout(&self) -> Duration {
        self.cluster
            .as_ref()
            .map_or(Duration::ZERO, |c| c.sync_timeout)
    }

    /// The epoch a candidate claims: the lowest among its databases, so
    /// it claims an epoch only when every database has caught up to
    /// what that epoch's leader held at its election.
    pub fn vote_epoch(&self) -> u64 {
        self.databases()
            .iter()
            .map(|d| d.db.epoch())
            .min()
            .unwrap_or(0)
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
        if self.role() == Role::Removed {
            return Err(self.refuse("not a member of the group"));
        }
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
        // The membership republished under this epoch, committed by the
        // first heartbeat rounds before any change is taken (a stale
        // leader's uncommitted change cannot outrank it afterwards);
        // before the role flips, so a leader never shows the old stamp.
        #[cfg(feature = "test-util")]
        let restamp = !self.skip_restamp.load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(not(feature = "test-util"))]
        let restamp = true;
        if self.cluster.is_some() && restamp {
            self.restamp_members(epoch)?;
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
            d.epoch_base
                .store(d.db.head(), std::sync::atomic::Ordering::Release);
            d.sync.reset();
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
            seeds: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
            advertise: "http://127.0.0.1:3".into(),
            token: None,
            tls: None,
            max_message_bytes: 1 << 20,
            lease_ttl: Duration::from_secs(1),
            sync_commit: SyncCommit::Majority,
            sync_timeout: Duration::from_secs(1),
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
                None,
                Duration::from_secs(1),
                Metrics::new(),
                cancel.clone(),
            );
            let db = node.open_database("default").unwrap();
            node.insert(DbNode::new("default".into(), db, &node));
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
    fn quorum_is_a_majority_of_the_membership() {
        let m = |n: usize| Members {
            epoch: 0,
            version: 1,
            addrs: (0..n).map(|i| format!("p{i}")).collect(),
        };
        assert_eq!(m(1).quorum(), 1);
        assert_eq!(m(2).quorum(), 2);
        assert_eq!(m(3).quorum(), 2);
        assert_eq!(m(4).quorum(), 3);
        assert_eq!(m(5).quorum(), 3);
    }

    #[test]
    fn the_members_file_round_trips_and_rejects_junk() {
        let m = Members {
            epoch: 7,
            version: 3,
            addrs: ["http://b:1", "http://a:1"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        };
        let text = m.text();
        assert_eq!(
            text,
            "epoch 7\nversion 3\nmember http://a:1\nmember http://b:1\n"
        );
        assert_eq!(Members::parse(&text), Some(m.clone()));
        assert_eq!(Members::from_wire(&m.to_wire()), Some(m));
        for junk in [
            "",
            "epoch 7\n",
            "epoch 7\nversion 3\n",
            "version 3\nepoch 7\nmember a\n",
            "epoch x\nversion 3\nmember a\n",
            "epoch 7\nversion 3\nmember a\nmember a\n",
            "epoch 7\nversion 3\nmember a b\n",
            "epoch 7\nversion 3\npeer a\n",
        ] {
            assert_eq!(Members::parse(junk), None, "{junk:?}");
        }
        assert_eq!(
            Members::from_wire(&pb::Members {
                epoch: 1,
                version: 1,
                addrs: vec![]
            }),
            None
        );
        assert!((1, 2) < (2, 0), "generations order by epoch first");
    }
}
