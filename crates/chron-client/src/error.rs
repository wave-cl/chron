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
    /// A live stream ended because the database was cut back to
    /// `position` to follow its leader: what was delivered from there on
    /// is being rewritten; subscribe again from `position`.
    #[error("cut back to {position}: {message}")]
    CutBack {
        /// The position the database was cut back to.
        position: u64,
        /// The daemon's message.
        message: String,
    },
    /// No seed of [`Client::connect_any`](crate::Client::connect_any)
    /// named a reachable leader.
    #[error("no leader: {0}")]
    NoLeader(String),
    /// An acknowledgment from a cursor subscription that a newer one took
    /// over: `session` is the live one. Stop; the other consumer owns the
    /// cursor now.
    #[error("the cursor was taken over by session {session}: {message}")]
    TakenOver {
        /// The session that owns the cursor.
        session: u64,
        /// The daemon's message.
        message: String,
    },
    /// A read with `at_least` waited its timeout out: the bound under the
    /// read's consistency stayed at `position`, below what was asked
    /// (`head` is the node's head). Nothing was read.
    #[error("the bound is {position} (head {head}): {message}")]
    BoundTimeout {
        /// The bound (local or confirmed) when the wait ended.
        position: u64,
        /// The node's head then.
        head: u64,
        /// The daemon's message.
        message: String,
    },
    /// A sync commit landed on the leader's disk but a majority did not
    /// confirm it in time (or the node stopped leading first). It is at
    /// `first..next` there and replicates unless the node dies. With a
    /// commit id the client resends it to the leader it finds next
    /// (see [`Client::commit`](crate::Client::commit)), so a caller sees
    /// it only once the failover timeout has passed; without one it is
    /// never retried, since a resend could write it twice: resolve with
    /// an exact expectation on the aggregate, or a read.
    #[error("unconfirmed: committed locally at {first} (next {next}) but {message}")]
    Unconfirmed {
        /// The transaction's first position on the leader.
        first: u64,
        /// The head after it.
        next: u64,
        /// The daemon's message.
        message: String,
    },
    /// A message or transaction above the daemon's limit.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
    /// The connection could not be established.
    #[error("cannot connect: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A local file could not be written (a fetched backup).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
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
            Code::FailedPrecondition
                if let Some(session) = header::<u64>(&s, chron_proto::SESSION_HEADER) =>
            {
                ClientError::TakenOver {
                    session,
                    message: s.message().to_string(),
                }
            }
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
            Code::DeadlineExceeded
                if let (Some(first), Some(next)) = (
                    header::<u64>(&s, chron_proto::COMMITTED_FIRST_HEADER),
                    header::<u64>(&s, chron_proto::COMMITTED_NEXT_HEADER),
                ) =>
            {
                ClientError::Unconfirmed {
                    first,
                    next,
                    message: s.message().to_string(),
                }
            }
            Code::DeadlineExceeded
                if let (Some(position), Some(head)) = (
                    header::<u64>(&s, chron_proto::POSITION_HEADER),
                    header::<u64>(&s, chron_proto::HEAD_HEADER),
                ) =>
            {
                ClientError::BoundTimeout {
                    position,
                    head,
                    message: s.message().to_string(),
                }
            }
            Code::Unavailable
                if let Some(position) = header::<u64>(&s, chron_proto::POSITION_HEADER) =>
            {
                ClientError::CutBack {
                    position,
                    message: s.message().to_string(),
                }
            }
            Code::Unavailable => ClientError::Unavailable(s.message().to_string()),
            Code::ResourceExhausted => ClientError::ResourceExhausted(s.message().to_string()),
            // The daemon going away mid-call: the HTTP/2 GOAWAY of a
            // shutdown arrives as an Internal "h2 protocol error", a
            // process killed under an open call (the body read failing on
            // a reset) as the same message under Unknown.
            Code::Internal | Code::Unknown if s.message().starts_with("h2 protocol error") => {
                ClientError::Unavailable(s.message().to_string())
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A daemon killed under an open call: tonic reports the body read
    /// failing as `Unknown` with the h2 message (a shutdown's GOAWAY is
    /// the same message under `Internal`). Both are the connection
    /// going, so both are `Unavailable`.
    #[test]
    fn a_connection_lost_under_a_call_is_unavailable_whatever_the_code() {
        for code in [Code::Unknown, Code::Internal] {
            let s = Status::new(
                code,
                "h2 protocol error: error reading a body from connection",
            );
            let e = ClientError::from(s);
            assert!(matches!(e, ClientError::Unavailable(_)), "{code:?}: {e}");
        }
        let plain = ClientError::from(Status::new(Code::Unknown, "something else"));
        assert!(matches!(plain, ClientError::Status(_)), "{plain}");
    }
}
