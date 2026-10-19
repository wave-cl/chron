//! Events as the caller hands them in and as the database hands them back,
//! and the record body encoding.
//!
//! ```text
//! body (all big-endian)
//!   u8   format (1)
//!   u8   flags            bit0 FIRST_IN_TXN, bit1 LAST_IN_TXN, bit2 FREE, bit3 COMMIT_ID;
//!                         other bits must be 0
//!   u64  position
//!   [16] aggregate uuid   zero when FREE
//!   u64  aggregate index  zero when FREE
//!   i64  recorded_at      unix nanoseconds, one value per transaction
//!   [16] commit id        only when COMMIT_ID is set, which needs FIRST_IN_TXN
//!   u8   tag_count, then tag_count × (u8 len, bytes)
//!   u32  metadata_len, bytes
//!   u32  payload_len, bytes
//! ```

use bytes::Bytes;
use uuid::Uuid;

/// The event is the first of its transaction.
pub const FLAG_FIRST_IN_TXN: u8 = 0b01;
/// The event is the last of its transaction. A one-event transaction sets
/// both flags.
pub const FLAG_LAST_IN_TXN: u8 = 0b10;
/// The event belongs to no aggregate.
pub const FLAG_FREE: u8 = 0b100;
/// The event is the first of its transaction and carries the 16-byte
/// commit id the committer gave (since 0.5.0; a 0.4.0 reader refuses it).
pub const FLAG_COMMIT_ID: u8 = 0b1000;
const KNOWN_FLAGS: u8 = FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN | FLAG_FREE | FLAG_COMMIT_ID;
/// The flags a 0.4.0 reader knows; what it refuses is a record with a
/// commit id.
#[cfg(test)]
const KNOWN_FLAGS_0_4: u8 = FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN | FLAG_FREE;

/// Longest tag, in bytes.
pub const MAX_TAG_BYTES: usize = 255;
/// Most tags on one event.
pub const MAX_TAGS_PER_EVENT: usize = 255;

pub(crate) const BODY_FORMAT: u8 = 1;
/// Body bytes before the tags.
const FIXED_LEN: usize = 1 + 1 + 8 + 16 + 8 + 8 + 1;

/// An event to append.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NewEvent {
    /// Tags the event is indexed under. Stored sorted and without
    /// duplicates, whatever order they were given in.
    pub tags: Vec<Bytes>,
    /// Opaque bytes beside the payload; empty when absent.
    pub metadata: Bytes,
    /// The event itself, opaque to the database.
    pub payload: Bytes,
}

impl NewEvent {
    /// An event with this payload, no tags and no metadata.
    pub fn new(payload: impl Into<Bytes>) -> Self {
        NewEvent {
            tags: Vec::new(),
            metadata: Bytes::new(),
            payload: payload.into(),
        }
    }

    /// Adds one tag. Tags are stored sorted and deduplicated, so the order
    /// given here is not kept and a repeated tag counts once.
    pub fn tag(mut self, tag: impl Into<Bytes>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Adds several tags.
    pub fn tags<T: Into<Bytes>>(mut self, tags: impl IntoIterator<Item = T>) -> Self {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Sets the metadata.
    pub fn metadata(mut self, metadata: impl Into<Bytes>) -> Self {
        self.metadata = metadata.into();
        self
    }
}

/// An event as recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// Dense 0-based position in the whole database.
    pub position: u64,
    /// `None` for a free event: one appended to no aggregate.
    pub aggregate: Option<Uuid>,
    /// Dense 0-based position within the aggregate; 0 for a free event.
    pub index: u64,
    /// Unix nanoseconds when the transaction was committed; the same for
    /// every event of one transaction.
    pub recorded_at: i64,
    /// Transaction-boundary and free-event flags: [`FLAG_FIRST_IN_TXN`],
    /// [`FLAG_LAST_IN_TXN`], [`FLAG_FREE`], [`FLAG_COMMIT_ID`].
    pub flags: u8,
    /// The id the committer gave the transaction, on its first event
    /// only; `None` on the others and on transactions committed without
    /// one.
    pub commit_id: Option<[u8; 16]>,
    /// The tags the event carries, sorted, without duplicates.
    pub tags: Vec<Bytes>,
    /// Opaque bytes beside the payload; empty when none were given.
    pub metadata: Bytes,
    /// The event itself.
    pub payload: Bytes,
}

impl Event {
    /// Whether this event began its transaction.
    pub fn is_first_in_txn(&self) -> bool {
        self.flags & FLAG_FIRST_IN_TXN != 0
    }

    /// Whether this event ended its transaction.
    pub fn is_last_in_txn(&self) -> bool {
        self.flags & FLAG_LAST_IN_TXN != 0
    }

    /// True when the event belongs to no aggregate.
    pub fn is_free(&self) -> bool {
        self.aggregate.is_none()
    }

    /// Whether the event carries `tag`.
    pub fn has_tag(&self, tag: &[u8]) -> bool {
        self.tags.iter().any(|t| t.as_ref() == tag)
    }
}

/// Encoded size of a body with these variable parts.
pub(crate) fn body_len(
    tags: &[Bytes],
    metadata_len: usize,
    payload_len: usize,
    has_id: bool,
) -> usize {
    FIXED_LEN
        + if has_id { 16 } else { 0 }
        + tags.iter().map(|t| 1 + t.len()).sum::<usize>()
        + 4
        + metadata_len
        + 4
        + payload_len
}

/// Appends the body of `ev` to `out`. The caller has checked the tag limits.
pub(crate) fn encode_body(ev: &Event, out: &mut Vec<u8>) {
    debug_assert!(ev.tags.len() <= MAX_TAGS_PER_EVENT);
    debug_assert_eq!(ev.aggregate.is_none(), ev.flags & FLAG_FREE != 0);
    debug_assert_eq!(ev.commit_id.is_some(), ev.flags & FLAG_COMMIT_ID != 0);
    out.push(BODY_FORMAT);
    out.push(ev.flags);
    out.extend_from_slice(&ev.position.to_be_bytes());
    out.extend_from_slice(ev.aggregate.unwrap_or(Uuid::nil()).as_bytes());
    out.extend_from_slice(&ev.index.to_be_bytes());
    out.extend_from_slice(&ev.recorded_at.to_be_bytes());
    if let Some(id) = &ev.commit_id {
        out.extend_from_slice(id);
    }
    out.push(ev.tags.len() as u8);
    for t in &ev.tags {
        debug_assert!(t.len() <= MAX_TAG_BYTES);
        out.push(t.len() as u8);
        out.extend_from_slice(t);
    }
    out.extend_from_slice(&(ev.metadata.len() as u32).to_be_bytes());
    out.extend_from_slice(&ev.metadata);
    out.extend_from_slice(&(ev.payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&ev.payload);
}

/// Why a body did not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DecodeError {
    Short,
    Trailing(usize),
    Format(u8),
    Flags(u8),
    /// A free event carrying an aggregate or an index.
    FreeWithOwner,
    /// A commit id on a record that does not start a transaction.
    IdNotFirst,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Short => write!(f, "body truncated"),
            DecodeError::Trailing(n) => write!(f, "{n} trailing bytes after the body"),
            DecodeError::Format(v) => write!(f, "unknown body format {v}"),
            DecodeError::Flags(v) => write!(f, "unknown flag bits {v:#04x}"),
            DecodeError::FreeWithOwner => write!(f, "free event carries an aggregate or index"),
            DecodeError::IdNotFirst => {
                write!(f, "commit id on a record that does not start a transaction")
            }
        }
    }
}

struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.at.checked_add(n).ok_or(DecodeError::Short)?;
        let s = self.buf.get(self.at..end).ok_or(DecodeError::Short)?;
        self.at = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
}

/// Decodes a body. Copies the variable parts out of `body`.
pub(crate) fn decode_body(body: &[u8]) -> Result<Event, DecodeError> {
    decode_body_with(body, KNOWN_FLAGS)
}

/// `decode_body` for a reader that knows only `known_flags`: how an older
/// binary sees a record.
fn decode_body_with(body: &[u8], known_flags: u8) -> Result<Event, DecodeError> {
    let mut c = Cursor { buf: body, at: 0 };
    let format = c.u8()?;
    if format != BODY_FORMAT {
        return Err(DecodeError::Format(format));
    }
    let flags = c.u8()?;
    if flags & !known_flags != 0 {
        return Err(DecodeError::Flags(flags));
    }
    let position = c.u64()?;
    let id = Uuid::from_bytes(c.take(16)?.try_into().unwrap());
    let index = c.u64()?;
    let aggregate = if flags & FLAG_FREE != 0 {
        if !id.is_nil() || index != 0 {
            return Err(DecodeError::FreeWithOwner);
        }
        None
    } else {
        Some(id)
    };
    let recorded_at = c.i64()?;
    let commit_id = if flags & FLAG_COMMIT_ID != 0 {
        if flags & FLAG_FIRST_IN_TXN == 0 {
            return Err(DecodeError::IdNotFirst);
        }
        Some(<[u8; 16]>::try_from(c.take(16)?).unwrap())
    } else {
        None
    };
    let tag_count = c.u8()? as usize;
    let mut tags = Vec::with_capacity(tag_count);
    for _ in 0..tag_count {
        let len = c.u8()? as usize;
        tags.push(Bytes::copy_from_slice(c.take(len)?));
    }
    let metadata_len = c.u32()? as usize;
    let metadata = Bytes::copy_from_slice(c.take(metadata_len)?);
    let payload_len = c.u32()? as usize;
    let payload = Bytes::copy_from_slice(c.take(payload_len)?);
    if c.at != body.len() {
        return Err(DecodeError::Trailing(body.len() - c.at));
    }
    Ok(Event {
        position,
        aggregate,
        index,
        recorded_at,
        flags,
        commit_id,
        tags,
        metadata,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(flags: u8, tags: Vec<Bytes>, metadata: &[u8], payload: &[u8]) -> Event {
        Event {
            position: 0x0102030405060708,
            aggregate: (flags & FLAG_FREE == 0).then_some(Uuid::from_bytes([7; 16])),
            index: if flags & FLAG_FREE == 0 { 42 } else { 0 },
            recorded_at: -5,
            flags,
            commit_id: (flags & FLAG_COMMIT_ID != 0).then_some([9; 16]),
            tags,
            metadata: Bytes::copy_from_slice(metadata),
            payload: Bytes::copy_from_slice(payload),
        }
    }

    fn encode(ev: &Event) -> Vec<u8> {
        let mut out = Vec::new();
        encode_body(ev, &mut out);
        out
    }

    #[test]
    fn round_trip_with_tags_metadata_and_both_flags() {
        let ev = sample(
            FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"longer tag")],
            b"meta",
            b"payload bytes",
        );
        let body = encode(&ev);
        assert_eq!(body.len(), body_len(&ev.tags, 4, 13, false));
        assert_eq!(decode_body(&body).unwrap(), ev);
    }

    #[test]
    fn round_trip_empty_parts() {
        let ev = sample(0, vec![], b"", b"");
        let body = encode(&ev);
        assert_eq!(body.len(), FIXED_LEN + 8);
        assert_eq!(decode_body(&body).unwrap(), ev);
    }

    #[test]
    fn free_event_round_trips_and_rejects_an_owner() {
        let ev = sample(
            FLAG_FREE | FLAG_FIRST_IN_TXN,
            vec![Bytes::from_static(b"t")],
            b"m",
            b"p",
        );
        assert!(ev.is_free());
        let body = encode(&ev);
        assert_eq!(body.len(), body_len(&ev.tags, 1, 1, false));
        assert_eq!(decode_body(&body).unwrap(), ev);
        let mut owned = body.clone();
        owned[10 + 15] = 1;
        assert_eq!(decode_body(&owned), Err(DecodeError::FreeWithOwner));
        let mut indexed = body;
        indexed[10 + 16 + 7] = 1;
        assert_eq!(decode_body(&indexed), Err(DecodeError::FreeWithOwner));
    }

    #[test]
    fn empty_tag_round_trips() {
        let ev = sample(0, vec![Bytes::new()], b"", b"x");
        assert_eq!(decode_body(&encode(&ev)).unwrap(), ev);
    }

    #[test]
    fn maximum_tags_round_trip() {
        let tags: Vec<Bytes> = (0..MAX_TAGS_PER_EVENT)
            .map(|i| Bytes::from(vec![i as u8; MAX_TAG_BYTES]))
            .collect();
        let ev = sample(FLAG_LAST_IN_TXN, tags, b"", b"");
        let body = encode(&ev);
        assert_eq!(body.len(), body_len(&ev.tags, 0, 0, false));
        assert_eq!(decode_body(&body).unwrap(), ev);
    }

    #[test]
    fn every_short_cut_is_detected() {
        let ev = sample(
            FLAG_FIRST_IN_TXN,
            vec![Bytes::from_static(b"tag")],
            b"md",
            b"payload",
        );
        let body = encode(&ev);
        for cut in 0..body.len() {
            assert_eq!(
                decode_body(&body[..cut]),
                Err(DecodeError::Short),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn trailing_bytes_rejected() {
        let ev = sample(0, vec![], b"", b"p");
        let mut body = encode(&ev);
        body.push(0);
        assert_eq!(decode_body(&body), Err(DecodeError::Trailing(1)));
    }

    #[test]
    fn bad_format_rejected() {
        let mut body = encode(&sample(0, vec![], b"", b""));
        body[0] = 2;
        assert_eq!(decode_body(&body), Err(DecodeError::Format(2)));
    }

    #[test]
    fn unknown_flag_bits_rejected() {
        let mut body = encode(&sample(0, vec![], b"", b""));
        body[1] = 0b1_0000;
        assert_eq!(decode_body(&body), Err(DecodeError::Flags(0b1_0000)));
    }

    #[test]
    fn a_commit_id_round_trips_on_a_first_record() {
        let ev = sample(
            FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN | FLAG_COMMIT_ID,
            vec![Bytes::from_static(b"t")],
            b"m",
            b"p",
        );
        assert_eq!(ev.commit_id, Some([9; 16]));
        let body = encode(&ev);
        assert_eq!(body.len(), body_len(&ev.tags, 1, 1, true));
        assert_eq!(decode_body(&body).unwrap(), ev);
        // The id sits right after recorded_at.
        assert_eq!(&body[FIXED_LEN - 1..FIXED_LEN - 1 + 16], &[9u8; 16]);
    }

    #[test]
    fn a_commit_id_off_a_first_record_is_rejected() {
        let ev = sample(FLAG_FIRST_IN_TXN | FLAG_COMMIT_ID, vec![], b"", b"");
        let mut body = encode(&ev);
        body[1] = FLAG_COMMIT_ID | FLAG_LAST_IN_TXN;
        assert_eq!(decode_body(&body), Err(DecodeError::IdNotFirst));
    }

    /// The proof that a 0.4.0 binary refuses a record with a commit id
    /// rather than misreading it: its flag mask does not know the bit.
    #[test]
    fn an_old_reader_rejects_a_record_with_a_commit_id() {
        let with_id = sample(FLAG_FIRST_IN_TXN | FLAG_COMMIT_ID, vec![], b"", b"p");
        let body = encode(&with_id);
        assert_eq!(
            decode_body_with(&body, KNOWN_FLAGS_0_4),
            Err(DecodeError::Flags(FLAG_FIRST_IN_TXN | FLAG_COMMIT_ID))
        );
        let without = sample(FLAG_FIRST_IN_TXN, vec![], b"", b"p");
        assert_eq!(
            decode_body_with(&encode(&without), KNOWN_FLAGS_0_4).unwrap(),
            without
        );
    }

    #[test]
    fn has_tag_and_flag_accessors() {
        let ev = sample(FLAG_LAST_IN_TXN, vec![Bytes::from_static(b"t")], b"", b"");
        assert!(ev.has_tag(b"t"));
        assert!(!ev.has_tag(b"u"));
        assert!(!ev.is_first_in_txn());
        assert!(ev.is_last_in_txn());
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        fn arb_event() -> impl Strategy<Value = Event> {
            let flags = prop::sample::select(vec![
                0u8,
                FLAG_FIRST_IN_TXN,
                FLAG_LAST_IN_TXN,
                FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN,
                FLAG_FIRST_IN_TXN | FLAG_COMMIT_ID,
                FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN | FLAG_COMMIT_ID,
            ]);
            let tags = prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..=MAX_TAG_BYTES).prop_map(Bytes::from),
                0..=8,
            );
            let blob = || prop::collection::vec(any::<u8>(), 0..=1024).prop_map(Bytes::from);
            (
                flags,
                any::<bool>(),
                any::<u128>(),
                any::<u64>(),
                any::<u64>(),
                any::<i64>(),
                any::<u128>(),
                tags,
                blob(),
                blob(),
            )
                .prop_map(
                    |(
                        flags,
                        free,
                        id,
                        index,
                        position,
                        recorded_at,
                        cid,
                        tags,
                        metadata,
                        payload,
                    )| {
                        Event {
                            position,
                            aggregate: (!free).then_some(Uuid::from_u128(id)),
                            index: if free { 0 } else { index },
                            recorded_at,
                            flags: flags | if free { FLAG_FREE } else { 0 },
                            commit_id: (flags & FLAG_COMMIT_ID != 0).then_some(cid.to_be_bytes()),
                            tags,
                            metadata,
                            payload,
                        }
                    },
                )
        }

        proptest! {
            #[test]
            fn any_event_round_trips(ev in arb_event()) {
                let body = encode(&ev);
                prop_assert_eq!(body.len(), body_len(&ev.tags, ev.metadata.len(), ev.payload.len(), ev.commit_id.is_some()));
                prop_assert_eq!(decode_body(&body).unwrap(), ev);
                prop_assert_eq!(decode_body(&body[..body.len() - 1]), Err(DecodeError::Short));
                let mut longer = body.clone();
                longer.push(0);
                prop_assert_eq!(decode_body(&longer), Err(DecodeError::Trailing(1)));
            }
        }
    }

    #[test]
    fn new_event_builder() {
        let ev = NewEvent::new("p").tag("a").tags(["b", "c"]).metadata("m");
        assert_eq!(ev.payload, Bytes::from_static(b"p"));
        assert_eq!(ev.metadata, Bytes::from_static(b"m"));
        assert_eq!(ev.tags.len(), 3);
    }
}
