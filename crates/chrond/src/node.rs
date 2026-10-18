//! What a daemon is, beyond its databases: its role, the markers that
//! survive restarts, the write gate that keeps the role check and a write
//! one critical section, and the named databases it serves.

use std::collections::HashMap;
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
}

impl Role {
    pub fn wire(self) -> pb::Role {
        match self {
            Role::Leader => pb::Role::Leader,
            Role::Replica => pb::Role::Replica,
            Role::Fenced => pb::Role::Fenced,
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

/// One named database the daemon serves.
pub struct DbNode {
    pub name: String,
    pub db: Db,
    pub replica: Mutex<ReplicaStatus>,
    pub puller: tokio::sync::Mutex<Option<Puller>>,
    /// Cancelled when the database is dropped (or the daemon stops: it is
    /// a child of the daemon's token); its streams end then.
    pub cancel: CancellationToken,
}

impl DbNode {
    pub fn new(name: String, db: Db, parent: &CancellationToken) -> Arc<DbNode> {
        Arc::new(DbNode {
            name,
            db,
            replica: Mutex::new(ReplicaStatus::default()),
            puller: tokio::sync::Mutex::new(None),
            cancel: parent.child_token(),
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
    pub upstream: Option<Upstream>,
    pub max_message_bytes: usize,
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
            upstream,
            max_message_bytes,
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

    pub fn leader_addr(&self) -> Option<&str> {
        self.upstream.as_ref().map(|u| u.addr.as_str())
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

    /// Why this node refuses writes right now, if it does.
    pub fn write_refusal(&self) -> Option<Status> {
        match self.role() {
            Role::Leader => None,
            Role::Replica => Some(self.refuse(format!(
                "read-only replica of {}; send writes to the leader",
                self.leader_addr().unwrap_or("?")
            ))),
            Role::Fenced => Some(self.refuse(format!(
                "fenced: a newer leader (epoch {}) exists; send writes there",
                self.fenced_by().unwrap_or(0)
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
            if e == ours + 1 {
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
                    self.leader_addr().unwrap_or("?")
                )));
            }
            Role::Leader if by <= ours => {
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
        tracing::warn!(by, "fenced: a newer leader exists");
        Ok(())
    }

    /// The replica becomes the leader under the next epoch, raised on
    /// every database. Blocking; the caller stopped the pullers first. The
    /// marker goes first: a crash between it and the epochs is repaired on
    /// the next start, which raises every database to the marker's.
    pub fn promote_locked(&self) -> Result<(u64, u64), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Replica {
            return Err(self.refuse("not a replica"));
        }
        let epoch = self
            .databases()
            .iter()
            .map(|d| d.db.epoch())
            .max()
            .unwrap_or(0)
            + 1;
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
        self.set_role(Role::Leader);
        for d in self.databases() {
            d.record_replica(|r| r.connected = false);
        }
        tracing::info!(epoch, "promoted to leader");
        Ok((epoch, self.default_db().db.head()))
    }
}
