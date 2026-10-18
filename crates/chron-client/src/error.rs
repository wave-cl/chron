use chron::Uuid;
use tonic::{Code, Status};

/// What a call can fail with. The daemon's status codes become variants;
/// the rest is carried as [`ClientError::Status`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// An append's expectation did not hold; nothing was written.
    #[error("aggregate {aggregate} has {actual} events: {message}")]
    Conflict {
        aggregate: Uuid,
        actual: u64,
        message: String,
    },
    /// A write refused because of the daemon's role or epoch (`epoch` is
    /// the daemon's): a replica, a fenced leader, or a stale token.
    #[error("{message} (daemon epoch {epoch})")]
    Fenced { epoch: u64, message: String },
    #[error("{0}")]
    InvalidArgument(String),
    #[error("{0}")]
    OutOfRange(String),
    #[error("{0}")]
    FailedPrecondition(String),
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("cannot connect: {0}")]
    Transport(#[from] tonic::transport::Error),
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
            Code::InvalidArgument => ClientError::InvalidArgument(s.message().to_string()),
            Code::OutOfRange => ClientError::OutOfRange(s.message().to_string()),
            Code::Unauthenticated => ClientError::Unauthenticated(s.message().to_string()),
            Code::Unavailable => ClientError::Unavailable(s.message().to_string()),
            _ => ClientError::Status(s),
        }
    }
}

impl From<chron_proto::convert::ConvertError> for ClientError {
    fn from(e: chron_proto::convert::ConvertError) -> Self {
        ClientError::Decode(e.to_string())
    }
}
