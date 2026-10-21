use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::txn::Expected;

/// Every fallible operation in this crate returns this.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// A second copy of a failure, for the other waiters of a batch that
    /// failed as a whole: the same variant where one can be rebuilt (the
    /// `op`, path and kind of an I/O error; the text of an index error),
    /// otherwise an I/O error carrying this error's text. `Error` is not
    /// `Clone` because its sources are not.
    pub(crate) fn redescribe(&self) -> Error {
        match self {
            Error::Io { path, op, source } => Error::Io {
                path: path.clone(),
                op,
                source: std::io::Error::new(source.kind(), source.to_string()),
            },
            Error::Index(e) => {
                Error::Index(redb::Error::from(std::io::Error::other(e.to_string())))
            }
            Error::Corrupt {
                path,
                offset,
                reason,
            } => Error::Corrupt {
                path: path.clone(),
                offset: *offset,
                reason: reason.clone(),
            },
            Error::Inconsistent { position, reason } => Error::Inconsistent {
                position: *position,
                reason: reason.clone(),
            },
            other => Error::Io {
                path: PathBuf::new(),
                op: "commit",
                source: std::io::Error::other(other.to_string()),
            },
        }
    }
}

/// Errors raised by the database.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A filesystem call failed. `op` names the call (`open`, `write`,
    /// `fdatasync`, ...) so the message says what the database was doing.
    #[error("io error during {op} on {path}: {source}")]
    Io {
        /// The file or directory involved.
        path: PathBuf,
        /// The call that failed (`open`, `write`, `fdatasync`, ...).
        op: &'static str,
        #[source]
        /// The underlying error.
        source: std::io::Error,
    },

    /// A segment file or the index disagrees with itself in a way recovery
    /// cannot repair by truncation (for example the index knows positions the
    /// segment files do not contain).
    #[error("corrupt database: {path} at offset {offset}: {reason}")]
    Corrupt {
        /// The file or directory involved.
        path: PathBuf,
        /// Byte offset in `path` where the problem was found.
        offset: u64,
        /// What was found.
        reason: String,
    },

    /// Another process (or another `Db` in this process) holds `LOCK`.
    #[error("database at {path} is locked by another writer")]
    Locked {
        /// The locked directory's `LOCK` file.
        path: PathBuf,
    },

    /// An append's expectation about the aggregate's length did not hold.
    /// The whole transaction was refused; nothing was written.
    #[error("aggregate {aggregate} has {actual} events, append expected {expected:?}")]
    ExpectedLength {
        /// The aggregate the append named.
        aggregate: Uuid,
        /// What the append expected.
        expected: Expected,
        /// How many events the aggregate has.
        actual: u64,
    },

    /// `commit` was called on a transaction with no appends.
    #[error("transaction has no appends")]
    EmptyTransaction,

    /// The transaction's guard (see `Transaction::guard`) refused it
    /// right before the batch landed; nothing of it was written.
    #[error("refused: {0}")]
    Refused(String),

    /// A commit id seen before, on a transaction of another shape (other
    /// aggregates, or other event counts per append). A resend of the
    /// same transaction is answered with its first landing instead.
    #[error("commit id {} was used by transaction {first}..{next} with a different shape: {reason}", Uuid::from_bytes(*id))]
    CommitIdReused {
        /// The id.
        id: [u8; 16],
        /// Where the earlier transaction landed (0 when it is in the same
        /// batch and not landed yet).
        first: u64,
        /// Its next position.
        next: u64,
        /// What differed.
        reason: String,
    },

    /// An append in the transaction carried no events. `aggregate` is
    /// `None` for a free append.
    #[error("append to {} has no events", aggregate.map_or("no aggregate".to_string(), |a| format!("aggregate {a}")))]
    EmptyAppend {
        /// The aggregate, or `None` for a free append.
        aggregate: Option<Uuid>,
    },

    /// One event's frame and body exceed `Options::max_record_bytes`, or
    /// the 4 GiB a frame can describe.
    #[error("record of {size} bytes exceeds the maximum of {max}")]
    RecordTooLarge {
        /// The record's frame plus body, in bytes.
        size: usize,
        /// The configured limit.
        max: usize,
    },

    /// A tag longer than 255 bytes, or more than 255 tags on one event
    /// (counted before duplicates are dropped).
    #[error("invalid tag: {0}")]
    InvalidTag(String),

    /// A global position past the head: from a read, a subscription's
    /// catch-up, `retain_from`, `backup_since` or `replication_chunk`.
    #[error("position {position} is past the head {head}")]
    PositionOutOfRange {
        /// The position asked for.
        position: u64,
        /// The head at the time.
        head: u64,
    },

    /// A position below the retention floor, whose segment was dropped:
    /// from a read, a subscription, `backup_since` or `replication_chunk`.
    #[error("position {position} is below the floor {floor}")]
    BelowFloor {
        /// The position asked for.
        position: u64,
        /// The floor at the time.
        floor: u64,
    },

    /// `restore_backup` was given a directory that already exists.
    #[error("{path} already exists")]
    AlreadyExists {
        /// The directory that exists.
        path: PathBuf,
    },

    /// An incremental backup applies onto `base_head`, but the database is
    /// at `head`.
    #[error("{archive} applies onto head {base_head} but the database is at head {head}")]
    WrongBaseHead {
        /// The archive.
        archive: PathBuf,
        /// The head the archive applies onto.
        base_head: u64,
        /// The head at the time.
        head: u64,
    },

    /// `adopt_id` on a database that already holds events.
    #[error("the database is not empty (head {head}); only an empty one can adopt an identity")]
    NotEmpty {
        /// The database's head.
        head: u64,
    },

    /// A replication chunk that does not start at the head.
    #[error("replication chunk starts at {from} but the head is {head}")]
    ReplicationGap {
        /// Where the chunk starts.
        from: u64,
        /// The head it would have had to start at.
        head: u64,
    },

    /// Data from another database: a backup or chunk whose identity is
    /// not this database's.
    #[error("{what} is from database {theirs}; this is database {ours}")]
    WrongDatabase {
        /// The archive or stream that carried the other identity.
        what: String,
        /// The identity it carried.
        theirs: Uuid,
        /// This database's identity.
        ours: Uuid,
    },

    /// `restore_backup` given an incremental archive, or `apply_backup`
    /// given a full one.
    #[error("{archive} is a {kind:?} backup; a {} one was needed", match kind { crate::BackupKind::Full => "incremental", crate::BackupKind::Incremental => "full" })]
    WrongBackupKind {
        /// The archive.
        archive: PathBuf,
        /// What the archive is.
        kind: crate::BackupKind,
    },

    /// The index and the data disagree about a position: the index lists
    /// it but the record at the place it names says otherwise.
    #[error("position {position}: {reason}")]
    Inconsistent {
        /// The position the index and the data disagree about.
        position: u64,
        /// How they disagree.
        reason: String,
    },

    /// `set_epoch` below the current epoch.
    #[error("epoch {epoch} is below the current epoch {current}")]
    EpochNotNewer {
        /// The epoch asked for.
        epoch: u64,
        /// The epoch in force.
        current: u64,
    },

    /// The redb index failed.
    #[error("index: {0}")]
    Index(#[from] redb::Error),
}

impl Error {
    pub(crate) fn io(path: impl AsRef<Path>, op: &'static str, source: std::io::Error) -> Self {
        Error::Io {
            path: path.as_ref().to_path_buf(),
            op,
            source,
        }
    }

    pub(crate) fn corrupt(path: impl AsRef<Path>, offset: u64, reason: impl Into<String>) -> Self {
        Error::Corrupt {
            path: path.as_ref().to_path_buf(),
            offset,
            reason: reason.into(),
        }
    }
}

macro_rules! from_redb {
    ($($t:ty),* $(,)?) => {
        $(
            impl From<$t> for Error {
                fn from(e: $t) -> Self {
                    Error::Index(redb::Error::from(e))
                }
            }
        )*
    };
}

from_redb!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
    redb::SetDurabilityError,
);
