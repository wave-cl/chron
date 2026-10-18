//! Between the wire and the library: the pieces the daemon owns. The pure
//! type conversions live in `chron_proto::convert`.

use chron::{Error, Expected, NewEvent, Transaction};
use chron_proto::convert;
use chron_proto::v1 as pb;
use tonic::Status;

pub use chron_proto::convert::{
    aggregate, backup_meta_to_wire as backup_meta, committed_to_wire as committed, event_to_wire,
    options_to_wire as options,
};

/// Adds one wire append to a transaction. A free append may not carry an
/// expectation other than `any`.
pub fn apply_append(txn: &mut Transaction, a: pb::Append) -> Result<(), Status> {
    let events: Vec<NewEvent> = a
        .events
        .into_iter()
        .map(convert::new_event_from_wire)
        .collect();
    let expected = convert::expected_from_wire(a.expected.as_ref());
    match a.aggregate {
        Some(bytes) => {
            txn.append(aggregate(&bytes)?, expected, events);
        }
        None => {
            if expected != Expected::Any {
                return Err(Status::invalid_argument(
                    "a free append (no aggregate) cannot expect a length",
                ));
            }
            txn.append_free(events);
        }
    }
    Ok(())
}

/// The status a library error becomes.
pub fn status(e: Error) -> Status {
    match &e {
        Error::ExpectedLength {
            aggregate, actual, ..
        } => {
            let mut s = Status::failed_precondition(e.to_string());
            if let Ok(v) = actual.to_string().parse() {
                s.metadata_mut()
                    .insert(chron_proto::CONFLICT_ACTUAL_LENGTH_HEADER, v);
            }
            if let Ok(v) = aggregate.to_string().parse() {
                s.metadata_mut()
                    .insert(chron_proto::CONFLICT_AGGREGATE_HEADER, v);
            }
            s
        }
        Error::EmptyTransaction
        | Error::EmptyAppend { .. }
        | Error::RecordTooLarge { .. }
        | Error::InvalidTag(_) => Status::invalid_argument(e.to_string()),
        Error::PositionOutOfRange { .. } | Error::BelowFloor { .. } => {
            Status::out_of_range(e.to_string())
        }
        Error::Locked { .. }
        | Error::WrongBaseHead { .. }
        | Error::AlreadyExists { .. }
        | Error::NotEmpty { .. }
        | Error::ReplicationGap { .. }
        | Error::WrongDatabase { .. } => Status::failed_precondition(e.to_string()),
        _ => {
            tracing::error!(error = %e, "database failure");
            Status::internal(e.to_string())
        }
    }
}

/// A `spawn_blocking` join failure.
pub fn join(op: &str, e: tokio::task::JoinError) -> Status {
    Status::internal(format!("{op} task: {e}"))
}
