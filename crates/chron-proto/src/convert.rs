//! Between the wire types and the chron library's types. Pure functions,
//! shared by the daemon and the client.

use bytes::Bytes;
use chron::{
    AppendResult, BackupKind, BackupMeta, Committed, Event, Expected, FLAG_COMMIT_ID,
    FLAG_FIRST_IN_TXN, FLAG_FREE, FLAG_LAST_IN_TXN, NewEvent, Retained, Uuid,
};

use crate::v1 as pb;

/// A wire message that does not describe a valid library value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvertError {
    /// An aggregate field of this many bytes, where 16 were needed.
    Aggregate(usize),
    /// A commit id of this many bytes, where 16 (or none) were needed.
    CommitId(usize),
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConvertError::Aggregate(n) => write!(f, "aggregate must be 16 bytes, got {n}"),
            ConvertError::CommitId(n) => write!(f, "commit_id must be 16 bytes, got {n}"),
        }
    }
}

impl std::error::Error for ConvertError {}

impl From<ConvertError> for tonic::Status {
    fn from(e: ConvertError) -> Self {
        tonic::Status::invalid_argument(e.to_string())
    }
}

/// A wire commit id: empty means none, sixteen bytes an id, anything
/// else [`ConvertError::CommitId`].
pub fn commit_id_from_wire(bytes: &[u8]) -> Result<Option<[u8; 16]>, ConvertError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    <[u8; 16]>::try_from(bytes)
        .map(Some)
        .map_err(|_| ConvertError::CommitId(bytes.len()))
}

/// Sixteen bytes, or [`ConvertError::Aggregate`].
pub fn aggregate(bytes: &[u8]) -> Result<Uuid, ConvertError> {
    let arr: [u8; 16] = bytes
        .try_into()
        .map_err(|_| ConvertError::Aggregate(bytes.len()))?;
    Ok(Uuid::from_bytes(arr))
}

pub fn event_to_wire(ev: Event) -> pb::Event {
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
        commit_id: ev.commit_id.map(|i| i.to_vec()),
    }
}

pub fn event_from_wire(ev: pb::Event) -> Result<Event, ConvertError> {
    let aggregate = ev.aggregate.as_deref().map(aggregate).transpose()?;
    let mut flags = 0;
    if ev.first_in_txn {
        flags |= FLAG_FIRST_IN_TXN;
    }
    if ev.last_in_txn {
        flags |= FLAG_LAST_IN_TXN;
    }
    if aggregate.is_none() {
        flags |= FLAG_FREE;
    }
    let commit_id = match ev.commit_id.as_deref() {
        None => None,
        Some(bytes) => commit_id_from_wire(bytes)?,
    };
    if commit_id.is_some() {
        flags |= FLAG_COMMIT_ID;
    }
    Ok(Event {
        position: ev.position,
        aggregate,
        index: if aggregate.is_some() { ev.index } else { 0 },
        recorded_at: ev.recorded_at_unix_nanos,
        flags,
        commit_id,
        tags: ev.tags.into_iter().map(Bytes::from).collect(),
        metadata: Bytes::from(ev.metadata),
        payload: Bytes::from(ev.payload),
    })
}

/// Unset or `any` reads as `Any`.
pub fn expected_from_wire(e: Option<&pb::Expected>) -> Expected {
    match e.and_then(|e| e.kind.as_ref()) {
        None | Some(pb::expected::Kind::Any(_)) => Expected::Any,
        Some(pb::expected::Kind::New(_)) => Expected::New,
        Some(pb::expected::Kind::Exact(n)) => Expected::Exact(*n),
    }
}

pub fn expected_to_wire(e: Expected) -> pb::Expected {
    let kind = match e {
        Expected::Any => pb::expected::Kind::Any(pb::expected::Any {}),
        Expected::New => pb::expected::Kind::New(pb::expected::New {}),
        Expected::Exact(n) => pb::expected::Kind::Exact(n),
    };
    pb::Expected { kind: Some(kind) }
}

pub fn new_event_from_wire(e: pb::NewEvent) -> NewEvent {
    NewEvent {
        tags: e.tags.into_iter().map(Bytes::from).collect(),
        metadata: Bytes::from(e.metadata),
        payload: Bytes::from(e.payload),
    }
}

pub fn new_event_to_wire(e: NewEvent) -> pb::NewEvent {
    pb::NewEvent {
        tags: e.tags.into_iter().map(|t| t.to_vec()).collect(),
        metadata: e.metadata.to_vec(),
        payload: e.payload.to_vec(),
    }
}

/// `aggregate` `None` is a free append; its expectation is sent as `any`.
pub fn append_to_wire(
    aggregate: Option<Uuid>,
    expected: Expected,
    events: Vec<NewEvent>,
) -> pb::Append {
    pb::Append {
        aggregate: aggregate.map(|a| a.as_bytes().to_vec()),
        expected: Some(expected_to_wire(if aggregate.is_some() {
            expected
        } else {
            Expected::Any
        })),
        events: events.into_iter().map(new_event_to_wire).collect(),
    }
}

pub fn append_result_to_wire(r: &AppendResult) -> pb::AppendResult {
    pb::AppendResult {
        aggregate: r.aggregate.map(|a| a.as_bytes().to_vec()),
        first_position: r.first_position,
        first_index: r.first_index,
        count: r.count,
        new_len: r.new_len,
    }
}

pub fn append_result_from_wire(r: pb::AppendResult) -> Result<AppendResult, ConvertError> {
    Ok(AppendResult {
        aggregate: r.aggregate.as_deref().map(aggregate).transpose()?,
        first_position: r.first_position,
        first_index: r.first_index,
        count: r.count,
        new_len: r.new_len,
    })
}

pub fn committed_to_wire(c: Committed) -> pb::CommitResponse {
    pb::CommitResponse {
        first: c.first,
        last: c.last,
        next: c.next,
        appends: c.appends.iter().map(append_result_to_wire).collect(),
        duplicate: c.duplicate,
    }
}

pub fn committed_from_wire(c: pb::CommitResponse) -> Result<Committed, ConvertError> {
    Ok(Committed {
        first: c.first,
        last: c.last,
        next: c.next,
        appends: c
            .appends
            .into_iter()
            .map(append_result_from_wire)
            .collect::<Result<_, _>>()?,
        duplicate: c.duplicate,
    })
}

pub fn options_to_wire(o: &chron::Options) -> pb::Options {
    pb::Options {
        segment_max_bytes: o.segment_max_bytes,
        max_record_bytes: o.max_record_bytes as u64,
        fsync: o.fsync == chron::Fsync::Always,
        verify_all_segments: o.verify_all_segments,
    }
}

pub fn options_from_wire(o: pb::Options) -> chron::Options {
    chron::Options::new()
        .segment_max_bytes(o.segment_max_bytes)
        .max_record_bytes(o.max_record_bytes as usize)
        .fsync(if o.fsync {
            chron::Fsync::Always
        } else {
            chron::Fsync::Never
        })
        .verify_all_segments(o.verify_all_segments)
}

pub fn backup_meta_to_wire(m: BackupMeta) -> pb::BackupMeta {
    pb::BackupMeta {
        incremental: m.kind == BackupKind::Incremental,
        base_head: m.base_head,
        head: m.head,
        floor: m.floor,
        created_at_unix_nanos: m.created_at_unix_nanos,
        files: m.files,
        bytes: m.bytes,
        format: m.format,
        db_id: m.db_id.map(|u| u.as_bytes().to_vec()),
    }
}

pub fn backup_meta_from_wire(m: pb::BackupMeta) -> BackupMeta {
    BackupMeta {
        format: m.format,
        kind: if m.incremental {
            BackupKind::Incremental
        } else {
            BackupKind::Full
        },
        base_head: m.base_head,
        head: m.head,
        floor: m.floor,
        created_at_unix_nanos: m.created_at_unix_nanos,
        files: m.files,
        bytes: m.bytes,
        db_id: m.db_id.as_deref().and_then(|b| aggregate(b).ok()),
    }
}

pub fn retained_to_wire(r: Retained) -> pb::RetainResponse {
    pb::RetainResponse {
        floor: r.floor,
        segments_removed: r.segments_removed as u64,
        bytes_freed: r.bytes_freed,
    }
}

pub fn retained_from_wire(r: pb::RetainResponse) -> Retained {
    Retained {
        floor: r.floor,
        segments_removed: r.segments_removed as usize,
        bytes_freed: r.bytes_freed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_round_trip() {
        let owned = Event {
            position: 7,
            aggregate: Some(Uuid::from_u128(9)),
            index: 3,
            recorded_at: -1,
            flags: FLAG_FIRST_IN_TXN,
            commit_id: None,
            tags: vec![Bytes::from_static(b"t")],
            metadata: Bytes::from_static(b"m"),
            payload: Bytes::from_static(b"p"),
        };
        assert_eq!(
            event_from_wire(event_to_wire(owned.clone())).unwrap(),
            owned
        );
        let free = Event {
            aggregate: None,
            index: 0,
            flags: FLAG_FREE | FLAG_LAST_IN_TXN,
            ..owned.clone()
        };
        assert_eq!(event_from_wire(event_to_wire(free.clone())).unwrap(), free);
        let both = Event {
            flags: FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN,
            ..owned
        };
        assert_eq!(event_from_wire(event_to_wire(both.clone())).unwrap(), both);
        let mut bad = event_to_wire(both.clone());
        bad.aggregate = Some(vec![1; 17]);
        assert_eq!(event_from_wire(bad), Err(ConvertError::Aggregate(17)));
        let with_id = Event {
            flags: FLAG_FIRST_IN_TXN | FLAG_COMMIT_ID,
            commit_id: Some([3; 16]),
            ..both
        };
        let wire = event_to_wire(with_id.clone());
        assert_eq!(wire.commit_id.as_deref(), Some(&[3u8; 16][..]));
        assert_eq!(event_from_wire(wire.clone()).unwrap(), with_id);
        let mut short = wire;
        short.commit_id = Some(vec![3; 15]);
        assert_eq!(event_from_wire(short), Err(ConvertError::CommitId(15)));
        assert_eq!(commit_id_from_wire(&[]), Ok(None));
        assert_eq!(commit_id_from_wire(&[1; 16]), Ok(Some([1; 16])));
        assert_eq!(commit_id_from_wire(&[1; 3]), Err(ConvertError::CommitId(3)));
    }

    #[test]
    fn committed_and_expected_round_trip() {
        let c = Committed {
            first: 1,
            last: 3,
            next: 4,
            appends: vec![
                AppendResult {
                    aggregate: Some(Uuid::from_u128(1)),
                    first_position: 1,
                    first_index: 5,
                    count: 2,
                    new_len: 7,
                },
                AppendResult {
                    aggregate: None,
                    first_position: 3,
                    first_index: 0,
                    count: 1,
                    new_len: 0,
                },
            ],
            duplicate: true,
        };
        assert_eq!(
            committed_from_wire(committed_to_wire(c.clone())).unwrap(),
            c
        );
        for e in [Expected::Any, Expected::New, Expected::Exact(4)] {
            assert_eq!(expected_from_wire(Some(&expected_to_wire(e))), e);
        }
        assert_eq!(expected_from_wire(None), Expected::Any);
        let free = append_to_wire(None, Expected::Exact(3), vec![NewEvent::new("x")]);
        assert_eq!(expected_from_wire(free.expected.as_ref()), Expected::Any);
    }
}
