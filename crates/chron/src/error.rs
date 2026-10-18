use std::path::{Path, PathBuf};

use uuid::Uuid;

use crate::txn::Expected;

/// Every fallible operation in this crate returns this.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors raised by the database.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A filesystem call failed. `op` names the call (`open`, `write`,
    /// `fdatasync`, ...) so the message says what the database was doing.
    #[error("io error during {op} on {path}: {source}")]
    Io {
        path: PathBuf,
        op: &'static str,
        #[source]
        source: std::io::Error,
    },

    /// A segment file or the index disagrees with itself in a way recovery
    /// cannot repair by truncation (for example the index knows positions the
    /// segment files do not contain).
    #[error("corrupt database: {path} at offset {offset}: {reason}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        reason: String,
    },

    /// Another process (or another `Db` in this process) holds `LOCK`.
    #[error("database at {path} is locked by another writer")]
    Locked { path: PathBuf },

    /// An append's expectation about the aggregate's length did not hold.
    /// The whole transaction was refused; nothing was written.
    #[error("aggregate {aggregate} has {actual} events, append expected {expected:?}")]
    ExpectedLength {
        aggregate: Uuid,
        expected: Expected,
        actual: u64,
    },

    /// `commit` was called on a transaction with no appends.
    #[error("transaction has no appends")]
    EmptyTransaction,

    /// An append in the transaction carried no events. `aggregate` is
    /// `None` for a free append.
    #[error("append to {} has no events", aggregate.map_or("no aggregate".to_string(), |a| format!("aggregate {a}")))]
    EmptyAppend { aggregate: Option<Uuid> },

    /// One event's frame and body exceed `Options::max_record_bytes`.
    #[error("record of {size} bytes exceeds the maximum of {max}")]
    RecordTooLarge { size: usize, max: usize },

    /// A tag longer than 255 bytes, or more than 255 tags on one event.
    #[error("invalid tag: {0}")]
    InvalidTag(String),

    /// A read from a global position past the head.
    #[error("position {position} is past the head {head}")]
    PositionOutOfRange { position: u64, head: u64 },

    /// A read, backup or subscription from a position below the retention
    /// floor: its segment was dropped.
    #[error("position {position} is below the floor {floor}")]
    BelowFloor { position: u64, floor: u64 },

    /// `restore_backup` was given a directory that already exists.
    #[error("{path} already exists")]
    AlreadyExists { path: PathBuf },

    /// An incremental backup applies onto `base_head`, but the database is
    /// at `head`.
    #[error("{archive} applies onto head {base_head} but the database is at head {head}")]
    WrongBaseHead {
        archive: PathBuf,
        base_head: u64,
        head: u64,
    },

    /// `adopt_id` on a database that already holds events.
    #[error("the database is not empty (head {head}); only an empty one can adopt an identity")]
    NotEmpty { head: u64 },

    /// A replication chunk that does not start at the head.
    #[error("replication chunk starts at {from} but the head is {head}")]
    ReplicationGap { from: u64, head: u64 },

    /// Data from another database: a backup or chunk whose identity is
    /// not this database's.
    #[error("{what} is from database {theirs}; this is database {ours}")]
    WrongDatabase {
        what: String,
        theirs: Uuid,
        ours: Uuid,
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
