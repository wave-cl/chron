//! What a daemon is, beyond its database: its role, its epoch, the
//! markers that survive restarts, and the write gate that keeps the role
//! check and the commit one critical section.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Mutex, RwLock};

use chron::{Committed, Db};
use chron_proto::v1 as pb;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::codec;
use crate::replica::Puller;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Leader = 0,
    Replica = 1,
    Fenced = 2,
}

impl Role {
    fn from_u8(v: u8) -> Role {
        match v {
            1 => Role::Replica,
            2 => Role::Fenced,
            _ => Role::Leader,
        }
    }

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

pub const PROMOTED_MARKER: &str = "promoted";
pub const FENCED_MARKER: &str = "fenced";

pub struct Node {
    pub db: Db,
    pub data_dir: PathBuf,
    role: AtomicU8,
    /// Readers check the role and epoch and commit; writers flip the role.
    write_gate: RwLock<()>,
    pub replicate_from: Option<String>,
    pub replicate_token: Option<String>,
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
        replicate_from: Option<String>,
        replicate_token: Option<String>,
        cancel: CancellationToken,
    ) -> Node {
        Node {
            db,
            data_dir,
            role: AtomicU8::new(Role::Leader as u8),
            write_gate: RwLock::new(()),
            replicate_from,
            replicate_token,
            replica: Mutex::new(ReplicaStatus::default()),
            fenced_by: Mutex::new(None),
            puller: tokio::sync::Mutex::new(None),
            fencer: Mutex::new(None),
            cancel,
        }
    }

    pub fn role(&self) -> Role {
        Role::from_u8(self.role.load(Ordering::Acquire))
    }

    pub fn set_role(&self, role: Role) {
        self.role.store(role as u8, Ordering::Release);
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

    /// Reads `epoch N` from a marker, if present.
    pub fn read_marker(&self, name: &str) -> Option<u64> {
        let text = fs::read_to_string(self.marker(name)).ok()?;
        text.trim().strip_prefix("epoch ")?.parse().ok()
    }

    pub fn write_marker(&self, name: &str, epoch: u64) -> std::io::Result<()> {
        fs::write(self.marker(name), format!("epoch {epoch}\n"))?;
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
                self.replicate_from.as_deref().unwrap_or("?")
            ))),
            Role::Fenced => Some(self.refuse(format!(
                "fenced: a newer leader (epoch {}) exists; send writes there",
                self.fenced_by().unwrap_or(0)
            ))),
        }
    }

    /// Checks the role and the writer's epoch, then commits. Blocking.
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
            if e > ours {
                drop(gate);
                self.fence(e)?;
                return Err(self.refuse(format!(
                    "fenced: a newer leader (epoch {e}) exists; send writes there"
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
                    self.replicate_from.as_deref().unwrap_or("?")
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
    /// caller stopped the puller first.
    pub fn promote_locked(&self) -> Result<(u64, u64), Status> {
        let _gate = self.write_gate.write().unwrap_or_else(|p| p.into_inner());
        if self.role() != Role::Replica {
            return Err(self.refuse("not a replica"));
        }
        let epoch = self.db.epoch() + 1;
        self.db.set_epoch(epoch).map_err(codec::status)?;
        self.write_marker(PROMOTED_MARKER, epoch)
            .map_err(|e| Status::internal(format!("cannot write the promoted marker: {e}")))?;
        self.set_role(Role::Leader);
        self.record_replica(|r| r.connected = false);
        tracing::info!(epoch, "promoted to leader");
        Ok((epoch, self.db.head()))
    }
}
