use std::error::Error as _;

use chron::{Expected, Uuid};
use tonic::{Code, Status};

/// What a call can fail with. The daemon's status codes become variants;
/// the rest is carried as [`ClientError::Status`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// An append's expectation did not hold; nothing was written.
    #[error("aggregate {aggregate} has {actual} events: {message}")]
    Conflict {
        /// The aggregate whose length did not match.
        aggregate: Uuid,
        /// Its actual length.
        actual: u64,
        /// What the append expected, when the client sent it (a commit);
        /// `None` when the conflict came from elsewhere.
        expected: Option<Expected>,
        /// The daemon's message.
        message: String,
    },
    /// A write refused because of the daemon's role or epoch: a replica,
    /// a fenced leader, or a stale epoch on the call. `epoch` is the
    /// daemon's; refresh from [`Info`](crate::Info) and retry against
    /// the leader.
    #[error("{message} (daemon epoch {epoch})")]
    Fenced {
        /// The daemon's epoch.
        epoch: u64,
        /// The daemon's message.
        message: String,
    },
    /// A read from a position past the head.
    #[error("position {position} is past the head {head}")]
    PositionOutOfRange {
        /// The position asked for.
        position: u64,
        /// The daemon's head.
        head: u64,
    },
    /// A read from a position retention has removed.
    #[error("position {position} is below the floor {floor}")]
    BelowFloor {
        /// The position asked for.
        position: u64,
        /// The first readable position.
        floor: u64,
    },
    /// A malformed request (INVALID_ARGUMENT).
    #[error("{0}")]
    InvalidArgument(String),
    /// OUT_OF_RANGE without the floor or head metadata.
    #[error("{0}")]
    OutOfRange(String),
    /// FAILED_PRECONDITION other than a conflict or a fence: a locked
    /// directory, a wrong backup kind or base, backups disabled.
    #[error("{0}")]
    FailedPrecondition(String),
    /// A missing or wrong bearer token.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// The call needs the daemon's admin token.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    /// No such database.
    #[error("not found: {0}")]
    NotFound(String),
    /// A database of that name exists.
    #[error("already exists: {0}")]
    AlreadyExists(String),
    /// The daemon is fenced, stopping, or the connection failed.
    #[error("unavailable: {0}")]
    Unavailable(String),
    /// A message or transaction above the daemon's limit.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    /// The connection could not be established.
    #[error("cannot connect: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// Any other status.
    #[error("{0}")]
    Status(Status),
    /// The daemon sent a message the client cannot read as a library
    /// value.
    #[error("malformed response: {0}")]
    Decode(String),
}

fn header<T: std::str::FromStr>(s: &Status, key: &str) -> Option<T> {
    s.metadata().get(key)?.to_str().ok()?.parse().ok()
}

impl From<Status> for ClientError {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::FailedPrecondition => {
                if let (Some(aggregate), Some(actual)) = (
                    header::<Uuid>(&s, chron_proto::CONFLICT_AGGREGATE_HEADER),
                    header::<u64>(&s, chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER),
                ) {
                    return ClientError::Conflict {
                        aggregate,
                        actual,
                        expected: None,
                        message: s.message().to_string(),
                    };
                }
                if let Some(epoch) = header::<u64>(&s, chron_proto::EPOCH_HEADER) {
                    return ClientError::Fenced {
                        epoch,
                        message: s.message().to_string(),
                    };
                }
                ClientError::FailedPrecondition(s.message().to_string())
            }
            Code::OutOfRange => {
                let position = header::<u64>(&s, chron_proto::POSITION_HEADER);
                match (
                    position,
                    header::<u64>(&s, chron_proto::FLOOR_HEADER),
                    header::<u64>(&s, chron_proto::HEAD_HEADER),
                ) {
                    (Some(position), Some(floor), _) => ClientError::BelowFloor { position, floor },
                    (Some(position), _, Some(head)) => {
                        ClientError::PositionOutOfRange { position, head }
                    }
                    _ => ClientError::OutOfRange(s.message().to_string()),
                }
            }
            Code::InvalidArgument => ClientError::InvalidArgument(s.message().to_string()),
            Code::PermissionDenied => ClientError::PermissionDenied(s.message().to_string()),
            Code::NotFound => ClientError::NotFound(s.message().to_string()),
            Code::AlreadyExists => ClientError::AlreadyExists(s.message().to_string()),
            Code::Unauthenticated => ClientError::Unauthenticated(s.message().to_string()),
            Code::Unavailable => ClientError::Unavailable(s.message().to_string()),
            Code::ResourceExhausted => ClientError::ResourceExhausted(s.message().to_string()),
            // A connection-level failure (a TLS handshake refused after the
            // connect, a reset) reaches the caller as this status.
            Code::Unknown if s.message() == "transport error" => {
                let detail = s
                    .source()
                    .map(|e| format!("transport error: {e}"))
                    .unwrap_or_else(|| "transport error".to_string());
                ClientError::Unavailable(detail)
            }
            _ => ClientError::Status(s),
        }
    }
}

impl From<chron_proto::convert::ConvertError> for ClientError {
    fn from(e: chron_proto::convert::ConvertError) -> Self {
        ClientError::Decode(e.to_string())
    }
}
