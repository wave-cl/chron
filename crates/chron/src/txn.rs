//! Transactions: several appends, one commit, one position range.

use std::collections::HashMap;
use std::path::Path;
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use redb::ReadableTable;
use uuid::Uuid;

use crate::db::{Db, Inner};
use crate::error::{Error, Result};
use crate::event::{
    self, Event, FLAG_FIRST_IN_TXN, FLAG_FREE, FLAG_LAST_IN_TXN, MAX_TAG_BYTES, MAX_TAGS_PER_EVENT,
    NewEvent,
};
use crate::index::{AGG_INDEX, AGG_LEN, HEAD_KEY, META, POS, TAG};
use crate::options::Fsync;
use crate::segment::{self, FRAME_LEN, SegmentWriter};

/// What an append asserts about the aggregate's current length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Expected {
    /// No check.
    Any,
    /// The aggregate must have no events yet. Same check as `Exact(0)`.
    New,
    /// The aggregate must have exactly this many events.
    Exact(u64),
}

impl Expected {
    fn holds(self, actual: u64) -> bool {
        match self {
            Expected::Any => true,
            Expected::New => actual == 0,
            Expected::Exact(n) => actual == n,
        }
    }
}

pub(crate) struct PendingAppend {
    /// `None` for free events.
    aggregate: Option<Uuid>,
    expected: Expected,
    events: Vec<NewEvent>,
}

/// Appends to any number of aggregates, and free events belonging to none,
/// committed atomically.
///
/// Appends are checked at [`commit`](Transaction::commit), in call order.
/// The same aggregate may be appended to more than once; a later append's
/// expectation is checked against the length the earlier ones leave.
pub struct Transaction {
    db: Db,
    appends: Vec<PendingAppend>,
}

impl Transaction {
    pub(crate) fn new(db: Db) -> Self {
        Transaction {
            db,
            appends: Vec::new(),
        }
    }

    pub fn append(
        &mut self,
        aggregate: Uuid,
        expected: Expected,
        events: Vec<NewEvent>,
    ) -> &mut Self {
        self.appends.push(PendingAppend {
            aggregate: Some(aggregate),
            expected,
            events,
        });
        self
    }

    /// Appends events that belong to no aggregate. They take global
    /// positions and tags like any other and commit with the rest of the
    /// transaction; there is no length to expect.
    pub fn append_free(&mut self, events: Vec<NewEvent>) -> &mut Self {
        self.appends.push(PendingAppend {
            aggregate: None,
            expected: Expected::Any,
            events,
        });
        self
    }

    pub fn append_one(
        &mut self,
        aggregate: Uuid,
        expected: Expected,
        event: NewEvent,
    ) -> &mut Self {
        self.append(aggregate, expected, vec![event])
    }

    /// True when no `append` was called.
    pub fn is_empty(&self) -> bool {
        self.appends.is_empty()
    }

    /// Checks every expectation, writes every event, and commits. On any
    /// error nothing is written.
    pub fn commit(self) -> Result<Committed> {
        commit(&self.db.inner, self.appends, Mode::Commit)
    }

    pub(crate) fn into_parts(self) -> (Db, Vec<PendingAppend>) {
        (self.db, self.appends)
    }
}

/// What a committed transaction received.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Committed {
    /// Position of the transaction's first event.
    pub first: u64,
    /// Position of its last event.
    pub last: u64,
    /// The head after the commit: `last + 1`.
    pub next: u64,
    /// One per `append` call, in call order.
    pub appends: Vec<AppendResult>,
}

/// What one `append` or `append_free` call received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// `None` for a free append.
    pub aggregate: Option<Uuid>,
    /// Position of the append's first event.
    pub first_position: u64,
    /// Index within the aggregate of the append's first event; 0 for a
    /// free append.
    pub first_index: u64,
    pub count: u64,
    /// The aggregate's length after this append; 0 for a free append.
    pub new_len: u64,
}

#[derive(PartialEq, Eq)]
pub(crate) enum Mode {
    Commit,
    /// Write and fsync the segment bytes, then stop: the state a crash
    /// between the segment write and the index commit leaves.
    WriteWithoutCommit,
}

struct Entry {
    position: u64,
    rel_offset: u64,
    frame_len: u32,
    aggregate: Option<Uuid>,
    index: u64,
    tags: Vec<Bytes>,
}

fn now_nanos() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos() as i64,
        Err(e) => -(e.duration().as_nanos() as i64),
    }
}

fn check_shape(appends: &[PendingAppend], max_record: usize) -> Result<()> {
    if appends.is_empty() {
        return Err(Error::EmptyTransaction);
    }
    for a in appends {
        if a.events.is_empty() {
            return Err(Error::EmptyAppend {
                aggregate: a.aggregate,
            });
        }
        for ev in &a.events {
            if ev.tags.len() > MAX_TAGS_PER_EVENT {
                return Err(Error::InvalidTag(format!(
                    "{} tags on one event, at most {MAX_TAGS_PER_EVENT} allowed",
                    ev.tags.len()
                )));
            }
            if let Some(t) = ev.tags.iter().find(|t| t.len() > MAX_TAG_BYTES) {
                return Err(Error::InvalidTag(format!(
                    "tag of {} bytes, at most {MAX_TAG_BYTES} allowed",
                    t.len()
                )));
            }
            let size = FRAME_LEN + event::body_len(&ev.tags, ev.metadata.len(), ev.payload.len());
            if size > max_record || size > u32::MAX as usize {
                return Err(Error::RecordTooLarge {
                    size,
                    max: max_record,
                });
            }
        }
    }
    Ok(())
}

/// Lands one transaction whose frames are already encoded in `buf`: writes
/// them at the segment's end, syncs per the options, records every entry
/// and the new head in `wtxn`, commits (the commit point), publishes the
/// head and rolls the segment when it is full. Called under the writer
/// lock.
fn land(
    inner: &Inner,
    w: &mut MutexGuard<'_, SegmentWriter>,
    wtxn: redb::WriteTransaction,
    buf: &[u8],
    entries: &[Entry],
    lens: &HashMap<Uuid, u64>,
    new_head: u64,
) -> Result<()> {
    // Bytes first, synced before the index commit vouches for them.
    let base = w.base();
    let at = w.write_at_end(buf)?;
    let landed = land_index(inner, w, wtxn, base, at, buf, entries, lens, new_head);
    if landed.is_err() {
        // Leftover bytes past the logical end would survive a later,
        // shorter commit and a roll; cut them now.
        let _ = w.truncate_to_len();
    }
    landed
}

#[allow(clippy::too_many_arguments)]
fn land_index(
    inner: &Inner,
    w: &mut MutexGuard<'_, SegmentWriter>,
    wtxn: redb::WriteTransaction,
    base: u64,
    at: u64,
    buf: &[u8],
    entries: &[Entry],
    lens: &HashMap<Uuid, u64>,
    new_head: u64,
) -> Result<()> {
    if inner.opts.fsync == Fsync::Always {
        w.sync_data()?;
    }
    if inner.fail_next_commit.swap(false, Ordering::AcqRel) {
        return Err(Error::io(
            inner.layout.index_file(),
            "commit",
            std::io::Error::other("injected failure before the index commit"),
        ));
    }
    {
        let mut pos = wtxn.open_table(POS)?;
        let mut agg_index = wtxn.open_table(AGG_INDEX)?;
        let mut tag = wtxn.open_table(TAG)?;
        let mut agg_len = wtxn.open_table(AGG_LEN)?;
        let mut meta = wtxn.open_table(META)?;
        for e in entries {
            pos.insert(e.position, (base, at + e.rel_offset, e.frame_len))?;
            if let Some(id) = &e.aggregate {
                agg_index.insert((id.as_bytes(), e.index), e.position)?;
            }
            for t in &e.tags {
                tag.insert((t.as_ref(), e.position), ())?;
            }
        }
        for (id, len) in lens {
            agg_len.insert(id.as_bytes(), *len)?;
        }
        meta.insert(HEAD_KEY, new_head)?;
    }
    // Commit point.
    wtxn.commit()?;

    w.advance(buf.len() as u64);
    inner.publish_head(new_head);
    if w.len() >= inner.opts.segment_max_bytes {
        inner.roll(w, new_head);
    }
    Ok(())
}

/// One transaction's worth of imported frames.
struct Group {
    start: usize,
    end: usize,
    entries: Vec<Entry>,
    lens: HashMap<Uuid, u64>,
    new_head: u64,
}

/// Appends `frames` (back-to-back `len, crc, body` records, as an
/// incremental backup carries them) through the normal landing path, one
/// transaction at a time. Every frame is checked before anything is
/// written: it must decode, positions must continue from the head,
/// transactions must be whole and well flagged, and aggregate indices must
/// be dense. The head must be `base_head` first and `expect_head` after.
pub(crate) fn import_frames(
    inner: &Inner,
    archive: &Path,
    frames: &[u8],
    base_head: u64,
    expect_head: u64,
) -> Result<u64> {
    let mut w = inner.writer.lock().unwrap_or_else(|p| p.into_inner());
    let head = inner.head.load(Ordering::Acquire);
    if head != base_head {
        return Err(Error::WrongBaseHead {
            archive: archive.to_path_buf(),
            base_head,
            head,
        });
    }
    let corrupt = |offset: u64, reason: String| Error::corrupt(archive, offset, reason);

    let rtxn = inner.index.begin_read()?;
    let agg_len = rtxn.open_table(AGG_LEN)?;
    let mut lens: HashMap<Uuid, u64> = HashMap::new();
    let mut groups: Vec<Group> = Vec::new();
    let mut open: Option<Group> = None;
    let mut position = head;
    let mut walker = segment::Frames::new(frames);
    let mut last_offset = 0;
    while let Some((offset, frame)) = walker.next(archive, inner.opts.max_record_bytes)? {
        last_offset = offset;
        let body = segment::check_frame(archive, offset, frame, inner.opts.max_record_bytes)?;
        let ev = event::decode_body(body).map_err(|e| corrupt(offset, e.to_string()))?;
        if ev.position != position {
            return Err(corrupt(
                offset,
                format!(
                    "record carries position {} where {position} was expected",
                    ev.position
                ),
            ));
        }
        if ev.is_first_in_txn() != open.is_none() {
            return Err(corrupt(offset, "transaction flags out of order".into()));
        }
        let group = open.get_or_insert_with(|| Group {
            start: offset as usize,
            end: offset as usize,
            entries: Vec::new(),
            lens: HashMap::new(),
            new_head: position,
        });
        if let Some(id) = ev.aggregate {
            let cur = match group.lens.get(&id).or_else(|| lens.get(&id)) {
                Some(l) => *l,
                None => agg_len.get(id.as_bytes())?.map_or(0, |v| v.value()),
            };
            if ev.index != cur {
                return Err(corrupt(
                    offset,
                    format!("aggregate {id} index {} where {cur} was expected", ev.index),
                ));
            }
            group.lens.insert(id, cur + 1);
        }
        let last = ev.is_last_in_txn();
        group.entries.push(Entry {
            position,
            rel_offset: offset - group.start as u64,
            frame_len: frame.len() as u32,
            aggregate: ev.aggregate,
            index: ev.index,
            tags: ev.tags,
        });
        position += 1;
        if last {
            let mut g = open.take().unwrap();
            g.end = offset as usize + frame.len();
            g.new_head = position;
            lens.extend(g.lens.iter().map(|(k, v)| (*k, *v)));
            groups.push(g);
        }
    }
    if open.is_some() {
        return Err(corrupt(
            last_offset,
            "frames end inside a transaction".into(),
        ));
    }
    if position != expect_head {
        return Err(corrupt(
            frames.len() as u64,
            format!("frames end at position {position}, the archive says {expect_head}"),
        ));
    }
    drop(agg_len);
    drop(rtxn);

    for g in groups {
        let wtxn = inner.index.begin_write()?;
        land(
            inner,
            &mut w,
            wtxn,
            &frames[g.start..g.end],
            &g.entries,
            &g.lens,
            g.new_head,
        )?;
    }
    Ok(position)
}

pub(crate) fn commit(inner: &Inner, appends: Vec<PendingAppend>, mode: Mode) -> Result<Committed> {
    check_shape(&appends, inner.opts.max_record_bytes)?;

    // The single writer lock, held to the end.
    let mut w = inner.writer.lock().unwrap_or_else(|p| p.into_inner());
    let head = inner.head.load(Ordering::Acquire);

    // One redb write transaction: it checks the expectations and, after the
    // bytes are on disk, records them. Dropping it without commit aborts.
    let wtxn = inner.index.begin_write()?;
    let mut lens: HashMap<Uuid, u64> = HashMap::new();
    let mut first_indices = Vec::with_capacity(appends.len());
    {
        let agg_len = wtxn.open_table(AGG_LEN)?;
        for a in &appends {
            let Some(id) = a.aggregate else {
                first_indices.push(0);
                continue;
            };
            let cur = match lens.get(&id) {
                Some(l) => *l,
                None => agg_len.get(id.as_bytes())?.map_or(0, |v| v.value()),
            };
            if mode == Mode::Commit && !a.expected.holds(cur) {
                return Err(Error::ExpectedLength {
                    aggregate: id,
                    expected: a.expected,
                    actual: cur,
                });
            }
            first_indices.push(cur);
            lens.insert(id, cur + a.events.len() as u64);
        }
    }

    // Encode every record into one buffer.
    let total: usize = appends.iter().map(|a| a.events.len()).sum();
    let recorded_at = now_nanos();
    let mut buf = Vec::new();
    let mut body = Vec::new();
    let mut entries = Vec::with_capacity(total);
    let mut results = Vec::with_capacity(appends.len());
    let mut position = head;
    let mut seq = 0usize;
    for (a, first_index) in appends.into_iter().zip(first_indices) {
        let count = a.events.len() as u64;
        let free = a.aggregate.is_none();
        results.push(AppendResult {
            aggregate: a.aggregate,
            first_position: position,
            first_index,
            count,
            new_len: if free { 0 } else { first_index + count },
        });
        for (j, ev) in a.events.into_iter().enumerate() {
            let mut flags = if free { FLAG_FREE } else { 0 };
            if seq == 0 {
                flags |= FLAG_FIRST_IN_TXN;
            }
            if seq + 1 == total {
                flags |= FLAG_LAST_IN_TXN;
            }
            let mut tags = ev.tags;
            tags.sort();
            tags.dedup();
            let rec = Event {
                position,
                aggregate: a.aggregate,
                index: if free { 0 } else { first_index + j as u64 },
                recorded_at,
                flags,
                tags,
                metadata: ev.metadata,
                payload: ev.payload,
            };
            body.clear();
            event::encode_body(&rec, &mut body);
            let rel_offset = buf.len() as u64;
            segment::frame_record(&body, &mut buf);
            entries.push(Entry {
                position,
                rel_offset,
                frame_len: (FRAME_LEN + body.len()) as u32,
                aggregate: rec.aggregate,
                index: rec.index,
                tags: rec.tags,
            });
            position += 1;
            seq += 1;
        }
    }
    let new_head = position;

    if mode == Mode::WriteWithoutCommit {
        w.write_at_end(&buf)?;
        w.sync_data()?;
        return Ok(Committed {
            first: head,
            last: new_head - 1,
            next: new_head,
            appends: results,
        });
    }
    land(inner, &mut w, wtxn, &buf, &entries, &lens, new_head)?;

    Ok(Committed {
        first: head,
        last: new_head - 1,
        next: new_head,
        appends: results,
    })
}
