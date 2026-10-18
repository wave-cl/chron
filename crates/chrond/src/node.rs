//! What a daemon is, beyond its database: its role, its epoch, the
//! markers that survive restarts, and the write gate that keeps the role
//! check and the commit one critical section.

use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::RwLock;

use chron::{Committed, Db};
use chron_proto::v1 as pb;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::codec;
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

/// What the puller last saw.
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

/// How a replica reaches its leader.
#[derive(Debug, Clone)]
pub struct Upstream {
    pub addr: String,
    pub token: Option<String>,
    pub tls: Option<chron_client::TlsConfig>,
    pub max_message_bytes: usize,
}

pub struct Node {
    pub db: Db,
    pub data_dir: PathBuf,
    role: watch::Sender<Role>,
    /// Readers check the role and epoch and commit; writers flip the role.
    write_gate: RwLock<()>,
    pub upstream: Option<Upstream>,
    pub max_message_bytes: usize,
    pub replica: Mutex<ReplicaStatus>,
    fenced_by: Mutex<Option<u64>>,
    pub puller: tokio::sync::Mutex<Option<Puller>>,
    pub fencer: Mutex<Option<JoinHandle<()>>>,
    pub cancel: CancellationToken,
}

impl Node {
    pub fn new(
        db: Db,
        data_dir: PathBuf,
        upstream: Option<Upstream>,
        max_message_bytes: usize,
        cancel: CancellationToken,
    ) -> Node {
        Node {
            db,
            data_dir,
            role: watch::Sender::new(Role::Leader),
            write_gate: RwLock::new(()),
            upstream,
            max_message_bytes,
            replica: Mutex::new(ReplicaStatus::default()),
            fenced_by: Mutex::new(None),
            puller: tokio::sync::Mutex::new(None),
            fencer: Mutex::new(None),
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

    pub fn replica_status(&self) -> ReplicaStatus {
        self.replica
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn record_replica(&self, f: impl FnOnce(&mut ReplicaStatus)) {
        f(&mut self.replica.lock().unwrap_or_else(|p| p.into_inner()));
    }

    /// FAILED_PRECONDITION carrying this node's epoch.
    pub fn refuse(&self, message: impl Into<String>) -> Status {
        let mut s = Status::failed_precondition(message.into());
        if let Ok(v) = self.db.epoch().to_string().parse() {
            s.metadata_mut().insert(chron_proto::EPOCH_HEADER, v);
        }
        s
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

    /// Runs `f` while the role is Leader and no role change can land.
    /// Blocking: `f` is the write itself.
    pub fn write_guarded<T>(&self, f: impl FnOnce(&Db) -> Result<T, Status>) -> Result<T, Status> {
        let _gate = self.write_gate.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = self.write_refusal() {
            return Err(s);
        }
        f(&self.db)
    }

    /// Checks the role and the writer's epoch, then commits. Blocking. An
    /// epoch exactly one above this leader's fences it; a larger jump is
    /// refused without taking effect, since no leader can be that far
    /// ahead of a node it was promoted from.
    pub fn commit_guarded(
        &self,
        appends: Vec<pb::Append>,
        epoch: Option<u64>,
    ) -> Result<Committed, Status> {
        let gate = self.write_gate.read().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = self.write_refusal() {
            return Err(s);
        }
        let ours = self.db.epoch();
        if let Some(e) = epoch {
            if e < ours {
                return Err(self.refuse(format!(
                    "stale epoch {e}; the leader is at epoch {ours}; refresh it from Info"
                )));
            }
            if e == ours + 1 {
                drop(gate);
                self.fence(e)?;
                return Err(self.refuse(format!(
                    "fenced: a newer leader (epoch {e}) exists; send writes there"
                )));
            }
            if e > ours {
                return Err(self.refuse(format!(
                    "epoch {e} is more than one above this leader's {ours}; refresh it from Info"
                )));
            }
        }
        let mut txn = self.db.transaction();
        for a in appends {
            codec::apply_append(&mut txn, a)?;
        }
        txn.commit().map_err(codec::status)
    }

    /// Records that a leader at `by` exists; this node refuses writes from
    /// now on and across restarts. Blocking.
    pub fn fence(&self, by: u64) -> Result<(), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        let ours = self.db.epoch();
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

    /// The replica becomes the leader under the next epoch. Blocking; the
    /// caller stopped the puller first. The marker goes first: a crash
    /// between it and the epoch is repaired on the next start, which
    /// raises the epoch to the marker's.
    pub fn promote_locked(&self) -> Result<(u64, u64), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Replica {
            return Err(self.refuse("not a replica"));
        }
        let epoch = self.db.epoch() + 1;
        self.write_marker(PROMOTED_MARKER, epoch)
            .map_err(|e| Status::internal(format!("cannot write the promoted marker: {e}")))?;
        if let Err(e) = self.db.set_epoch(epoch) {
            let _ = self.remove_marker(PROMOTED_MARKER);
            return Err(codec::status(e));
        }
        if let Err(e) = self.remove_marker(REPLICA_MARKER) {
            tracing::warn!(error = %e, "cannot remove the replica marker");
        }
        self.set_role(Role::Leader);
        self.record_replica(|r| r.connected = false);
        tracing::info!(epoch, "promoted to leader");
        Ok((epoch, self.db.head()))
    }
}
