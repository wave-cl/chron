//! Between the wire and the library: types, expectations, status codes.

use bytes::Bytes;
use chron::{Error, Expected, NewEvent, Transaction, Uuid};
use chron_proto::v1 as pb;
use tonic::Status;

pub fn event_to_wire(ev: chron::Event) -> pb::Event {
    pb::Event {
        position: ev.position,
        aggregate: ev.aggregate.map(|a| a.as_bytes().to_vec()),
        index: ev.index,
        recorded_at_unix_nanos: ev.recorded_at,
        first_in_txn: ev.is_first_in_txn(),
        last_in_txn: ev.is_last_in_txn(),
        tags: ev.tags.into_iter().map(|t| t.to_vec()).collect(),
        metadata: ev.metadata.to_vec(),
        payload: ev.payload.to_vec(),
    }
}

/// Sixteen bytes, or INVALID_ARGUMENT.
pub fn aggregate(bytes: &[u8]) -> Result<Uuid, Status> {
    let arr: [u8; 16] = bytes.try_into().map_err(|_| {
        Status::invalid_argument(format!("aggregate must be 16 bytes, got {}", bytes.len()))
    })?;
    Ok(Uuid::from_bytes(arr))
}

/// Unset or `any` reads as `Any`.
pub fn expected(e: Option<&pb::Expected>) -> Expected {
    match e.and_then(|e| e.kind.as_ref()) {
        None | Some(pb::expected::Kind::Any(_)) => Expected::Any,
        Some(pb::expected::Kind::New(_)) => Expected::New,
        Some(pb::expected::Kind::Exact(n)) => Expected::Exact(*n),
    }
}

pub fn new_event(e: pb::NewEvent) -> NewEvent {
    NewEvent {
        tags: e.tags.into_iter().map(Bytes::from).collect(),
        metadata: Bytes::from(e.metadata),
        payload: Bytes::from(e.payload),
    }
}

/// Adds one wire append to a transaction. A free append may not carry an
/// expectation other than `any`.
pub fn apply_append(txn: &mut Transaction, a: pb::Append) -> Result<(), Status> {
    let events: Vec<NewEvent> = a.events.into_iter().map(new_event).collect();
    match a.aggregate {
        Some(bytes) => {
            txn.append(aggregate(&bytes)?, expected(a.expected.as_ref()), events);
        }
        None => {
            if expected(a.expected.as_ref()) != Expected::Any {
                return Err(Status::invalid_argument(
                    "a free append (no aggregate) cannot expect a length",
                ));
            }
            txn.append_free(events);
        }
    }
    Ok(())
}

pub fn append_result(r: &chron::AppendResult) -> pb::AppendResult {
    pb::AppendResult {
        aggregate: r.aggregate.map(|a| a.as_bytes().to_vec()),
        first_position: r.first_position,
        first_index: r.first_index,
        count: r.count,
        new_len: r.new_len,
    }
}

pub fn committed(c: chron::Committed) -> pb::CommitResponse {
    pb::CommitResponse {
        first: c.first,
        last: c.last,
        next: c.next,
        appends: c.appends.iter().map(append_result).collect(),
    }
}

pub fn options(o: &chron::Options) -> pb::Options {
    pb::Options {
        segment_max_bytes: o.segment_max_bytes,
        max_record_bytes: o.max_record_bytes as u64,
        fsync: o.fsync == chron::Fsync::Always,
        verify_all_segments: o.verify_all_segments,
    }
}

pub fn backup_meta(m: chron::BackupMeta) -> pb::BackupMeta {
    pb::BackupMeta {
        incremental: m.kind == chron::BackupKind::Incremental,
        base_head: m.base_head,
        head: m.head,
        floor: m.floor,
        created_at_unix_nanos: m.created_at_unix_nanos,
        files: m.files,
        bytes: m.bytes,
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
        | Error::InvalidTag(_) => Status::invalid_argument(e.to_string()),
        Error::PositionOutOfRange { .. } | Error::BelowFloor { .. } => {
            Status::out_of_range(e.to_string())
        }
        Error::Locked { .. } | Error::WrongBaseHead { .. } | Error::AlreadyExists { .. } => {
            Status::failed_precondition(e.to_string())
        }
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
