//! Between the wire and the library: the pieces the daemon owns. The pure
//! type conversions live in `chron_proto::convert`.

use chron::{Error, Expected, NewEvent, Retained, Transaction};
use chron_proto::convert;
use chron_proto::v1 as pb;
use tonic::Status;

pub use chron_proto::convert::{
    aggregate, backup_meta_to_wire as backup_meta, committed_to_wire as committed, event_to_wire,
    options_to_wire as options,
};

/// A wire commit id: empty is none; anything but 16 bytes is
/// INVALID_ARGUMENT.
pub fn commit_id(bytes: &[u8]) -> Result<Option<[u8; 16]>, Status> {
    convert::commit_id_from_wire(bytes).map_err(Status::from)
}

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

pub fn retained(r: Retained) -> pb::RetainResponse {
    pb::RetainResponse {
        floor: r.floor,
        segments_removed: r.segments_removed as u64,
        bytes_freed: r.bytes_freed,
    }
}

fn insert(s: &mut Status, key: &'static str, value: u64) {
    if let Ok(v) = value.to_string().parse() {
        s.metadata_mut().insert(key, v);
    }
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
        | Error::CommitIdReused { .. }
        | Error::InvalidTag(_) => Status::invalid_argument(e.to_string()),
        Error::PositionOutOfRange { position, head } => {
            let mut s = Status::out_of_range(e.to_string());
            insert(&mut s, chron_proto::POSITION_HEADER, *position);
            insert(&mut s, chron_proto::HEAD_HEADER, *head);
            s
        }
        Error::BelowFloor { position, floor } => {
            let mut s = Status::out_of_range(e.to_string());
            insert(&mut s, chron_proto::POSITION_HEADER, *position);
            insert(&mut s, chron_proto::FLOOR_HEADER, *floor);
            s
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

/// The `Head` answer under `mode`: the bound asked for, and both bounds
/// (without peers the confirmed bound is the head: a lone node is its
/// own majority).
pub fn head(
    node: &crate::node::Node,
    dbn: &crate::node::DbNode,
    mode: crate::ops::Mode,
) -> pb::HeadResponse {
    use crate::ops::{Mode, bound};
    let local = dbn.db.head();
    pb::HeadResponse {
        head: bound(node, dbn, mode),
        confirmed: bound(node, dbn, Mode::Confirmed),
        local,
    }
}
