//! Transactions: several appends, one commit, one position range.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use redb::ReadableTable;
use uuid::Uuid;

use crate::db::{Db, Inner};
use crate::error::{Error, Result};
use crate::event::{
    self, Event, FLAG_COMMIT_ID, FLAG_FIRST_IN_TXN, FLAG_FREE, FLAG_LAST_IN_TXN, MAX_TAG_BYTES,
    MAX_TAGS_PER_EVENT, NewEvent,
};
use crate::index::{AGG_INDEX, AGG_LEN, HEAD_KEY, Location, META, POS, TAG, TXN_ID};
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

#[derive(Debug)]
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
///
/// A transaction given a [`commit_id`](Transaction::commit_id) can be
/// resent: a later commit with the same id and the same shape (the same
/// sequence of events' aggregates, with each append's events on one
/// aggregate) is answered with the first landing's positions and
/// [`Committed::duplicate`] set, whatever its expectations and payloads,
/// and writes nothing. The same id on a transaction of another shape is
/// [`Error::CommitIdReused`].
/// Ids live in the records, so they survive restarts, rebuilds, restores
/// and replication; truncation and retention forget the ids of the
/// records they drop, after which a resend lands again.
#[derive(Debug)]
pub struct Transaction {
    db: Db,
    appends: Vec<PendingAppend>,
    commit_id: Option<[u8; 16]>,
}

impl Transaction {
    pub(crate) fn new(db: Db) -> Self {
        Transaction {
            db,
            appends: Vec::new(),
            commit_id: None,
        }
    }

    /// Gives the transaction an id, under which a resend is deduplicated
    /// (see the type's documentation). Stored in the first record, so a
    /// 0.4.0 binary cannot read a database holding one.
    pub fn commit_id(&mut self, id: [u8; 16]) -> &mut Self {
        self.commit_id = Some(id);
        self
    }

    /// Appends `events` to `aggregate`, checked against `expected` at commit.
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

    /// Appends one event to `aggregate`.
    pub fn append_one(
        &mut self,
        aggregate: Uuid,
        expected: Expected,
        event: NewEvent,
    ) -> &mut Self {
        self.append(aggregate, expected, vec![event])
    }

    /// True when neither `append` nor `append_free` was called.
    pub fn is_empty(&self) -> bool {
        self.appends.is_empty()
    }

    /// Checks every expectation, writes every event, and commits. On any
    /// error nothing is written.
    pub fn commit(self) -> Result<Committed> {
        commit(&self.db.inner, self.appends, self.commit_id, Mode::Commit)
    }

    pub(crate) fn into_parts(self) -> (Db, Vec<PendingAppend>, Option<[u8; 16]>) {
        (self.db, self.appends, self.commit_id)
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
    /// The commit id was seen before: these are the positions of the
    /// earlier landing and nothing was written now.
    pub duplicate: bool,
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
    /// Events appended.
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

/// Checks the shape of a transaction before any lock is taken and returns
/// an upper bound on its encoded size (tags are deduplicated later).
fn check_shape(appends: &[PendingAppend], has_id: bool, max_record: usize) -> Result<usize> {
    if appends.is_empty() {
        return Err(Error::EmptyTransaction);
    }
    let mut bytes = 0usize;
    let mut first = has_id;
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
            let size =
                FRAME_LEN + event::body_len(&ev.tags, ev.metadata.len(), ev.payload.len(), first);
            first = false;
            if size > max_record || size > u32::MAX as usize {
                return Err(Error::RecordTooLarge {
                    size,
                    max: max_record,
                });
            }
            bytes = bytes.saturating_add(size);
        }
    }
    Ok(bytes)
}

/// Lands one transaction whose frames are already encoded in `buf`: writes
/// them at the segment's end, syncs per the options, records every entry
/// and the new head in `wtxn`, commits (the commit point), publishes the
/// head and rolls the segment when it is full. Called under the writer
/// lock.
/// Lands one batch of whole transactions, encoded back to back in `buf`:
/// one segment write, one sync, one index commit. `txns` is how many
/// transactions the batch holds, for the counters; `txn_ids` the commit
/// ids the batch carries with each one's `(first, next)`.
#[allow(clippy::too_many_arguments)]
fn land(
    inner: &Inner,
    w: &mut SegmentWriter,
    wtxn: redb::WriteTransaction,
    buf: &[u8],
    entries: &[Entry],
    lens: &HashMap<Uuid, u64>,
    txn_ids: &[([u8; 16], u64, u64)],
    new_head: u64,
    txns: u64,
) -> Result<()> {
    // Bytes first, synced before the index commit vouches for them.
    let base = w.base();
    let at = w.write_at_end(buf)?;
    let landed = land_index(
        inner, w, wtxn, base, at, buf, entries, lens, txn_ids, new_head, txns,
    );
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
    w: &mut SegmentWriter,
    wtxn: redb::WriteTransaction,
    base: u64,
    at: u64,
    buf: &[u8],
    entries: &[Entry],
    lens: &HashMap<Uuid, u64>,
    txn_ids: &[([u8; 16], u64, u64)],
    new_head: u64,
    txns: u64,
) -> Result<()> {
    if inner.opts.fsync == Fsync::Always {
        let t = Instant::now();
        w.sync_data()?;
        inner.counters.fsyncs.fetch_add(1, Ordering::Relaxed);
        inner
            .counters
            .fsync_nanos
            .fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
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
        if !txn_ids.is_empty() {
            let mut ids = wtxn.open_table(TXN_ID)?;
            for (id, first, next) in txn_ids {
                ids.insert(id, (*first, *next))?;
            }
        }
        meta.insert(HEAD_KEY, new_head)?;
    }
    // Commit point.
    wtxn.commit()?;
    inner.counters.batches.fetch_add(1, Ordering::Relaxed);
    inner
        .counters
        .transactions
        .fetch_add(txns, Ordering::Relaxed);
    inner
        .counters
        .events
        .fetch_add(entries.len() as u64, Ordering::Relaxed);

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
    /// The id on the transaction's first record, if any.
    commit_id: Option<[u8; 16]>,
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
    let mut w = inner.lock_writer();
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
            commit_id: ev.commit_id,
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
        let first = g.entries[0].position;
        let ids: Vec<([u8; 16], u64, u64)> = g
            .commit_id
            .map(|id| (id, first, g.new_head))
            .into_iter()
            .collect();
        land(
            inner,
            &mut w,
            wtxn,
            &frames[g.start..g.end],
            &g.entries,
            &g.lens,
            &ids,
            g.new_head,
            1,
        )?;
    }
    Ok(position)
}

/// A transaction waiting to be landed by whoever holds the writer lock.
pub(crate) struct Pending {
    appends: Vec<PendingAppend>,
    commit_id: Option<[u8; 16]>,
    /// Upper bound on the encoded size, from `check_shape`.
    bytes: usize,
    slot: Arc<Slot>,
}

impl Pending {
    pub(crate) fn nominate(&self) {
        self.slot.nominate();
    }
}

enum SlotState {
    Waiting,
    /// The lock's last holder left and this waiter is first in line: it
    /// must take the lock and lead.
    Nominated,
    Done(Result<Committed>),
}

/// One committer's result cell.
struct Slot {
    state: Mutex<SlotState>,
    cv: Condvar,
}

enum Waited {
    Done(Result<Committed>),
    Nominated,
}

impl Slot {
    fn new() -> Arc<Slot> {
        Arc::new(Slot {
            state: Mutex::new(SlotState::Waiting),
            cv: Condvar::new(),
        })
    }

    fn wait(&self) -> Waited {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            match std::mem::replace(&mut *st, SlotState::Waiting) {
                SlotState::Done(r) => return Waited::Done(r),
                SlotState::Nominated => return Waited::Nominated,
                SlotState::Waiting => {
                    st = self.cv.wait(st).unwrap_or_else(|p| p.into_inner());
                }
            }
        }
    }

    fn nominate(&self) {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if matches!(*st, SlotState::Waiting) {
            *st = SlotState::Nominated;
            self.cv.notify_one();
        }
    }

    fn set(&self, r: Result<Committed>) {
        *self.state.lock().unwrap_or_else(|p| p.into_inner()) = SlotState::Done(r);
        self.cv.notify_one();
    }

    fn take_done(&self) -> Option<Result<Committed>> {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match std::mem::replace(&mut *st, SlotState::Waiting) {
            SlotState::Done(r) => Some(r),
            other => {
                *st = other;
                None
            }
        }
    }
}

/// Commits one transaction. The holder of the writer lock lands every
/// queued transaction in batches; a committer that finds the lock taken
/// joins the queue and waits for its result, or for the lock to be
/// handed to it.
pub(crate) fn commit(
    inner: &Inner,
    appends: Vec<PendingAppend>,
    commit_id: Option<[u8; 16]>,
    mode: Mode,
) -> Result<Committed> {
    let bytes = check_shape(&appends, commit_id.is_some(), inner.opts.max_record_bytes)?;

    if mode == Mode::WriteWithoutCommit {
        let mut w = inner.lock_writer();
        let head = inner.head.load(Ordering::Acquire);
        let wtxn = inner.index.begin_write()?;
        let plan = {
            let agg_len = wtxn.open_table(AGG_LEN)?;
            match plan_txn(&agg_len, &HashMap::new(), &appends, false)? {
                Ok(p) => p,
                Err((e, _)) => return Err(e),
            }
        };
        drop(wtxn);
        let mut buf = Vec::new();
        let mut body = Vec::new();
        let mut entries = Vec::new();
        let mut position = head;
        let committed = encode_txn(
            appends,
            commit_id,
            &plan.first_indices,
            now_nanos(),
            &mut position,
            &mut buf,
            &mut body,
            &mut entries,
        );
        w.write_at_end(&buf)?;
        w.sync_data()?;
        return Ok(committed);
    }

    let slot = Slot::new();
    inner
        .commits
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .push_back(Pending {
            appends,
            commit_id,
            bytes,
            slot: slot.clone(),
        });
    if let Some(w) = inner.try_lock_writer() {
        return lead(inner, w, &slot);
    }
    match slot.wait() {
        Waited::Done(r) => r,
        Waited::Nominated => {
            let w = inner.lock_writer();
            lead(inner, w, &slot)
        }
    }
}

/// Lands batches until this committer's own transaction is done, then
/// releases the lock (handing it to the next queued committer).
fn lead(inner: &Inner, mut w: crate::db::WriterGuard<'_>, slot: &Slot) -> Result<Committed> {
    loop {
        if let Some(r) = slot.take_done() {
            return r;
        }
        let batch = drain(inner, w.len());
        if batch.is_empty() {
            // Cannot happen: this committer's entry is queued until it
            // is done. Guard against a spin all the same.
            if let Some(r) = slot.take_done() {
                return r;
            }
            continue;
        }
        run_batch(inner, &mut w, batch);
    }
}

/// Takes the next batch from the queue: in arrival order, up to the
/// configured number of transactions and bytes, and not past the point
/// where the segment is full (so a segment overshoots by at most one
/// transaction, as before).
fn drain(inner: &Inner, segment_len: u64) -> Vec<Pending> {
    let mut queue = inner.commits.lock().unwrap_or_else(|p| p.into_inner());
    let max_txns = inner.opts.group_commit_max_txns.max(1);
    let max_bytes = inner.opts.group_commit_max_bytes;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    while let Some(next) = queue.front() {
        if !out.is_empty() {
            if out.len() >= max_txns {
                break;
            }
            if bytes.saturating_add(next.bytes) > max_bytes {
                break;
            }
            if segment_len.saturating_add(bytes as u64) >= inner.opts.segment_max_bytes {
                break;
            }
        }
        let p = queue.pop_front().expect("front was Some");
        bytes = bytes.saturating_add(p.bytes);
        out.push(p);
    }
    out
}

/// Delivers every result of a batch when dropped, so a panic inside the
/// batch leaves no waiter parked.
struct SlotFiller {
    slots: Vec<Arc<Slot>>,
    results: Vec<Option<Result<Committed>>>,
}

impl SlotFiller {
    fn set(&mut self, i: usize, r: Result<Committed>) {
        self.results[i] = Some(r);
    }
}

impl Drop for SlotFiller {
    fn drop(&mut self) {
        for (slot, r) in self.slots.iter().zip(self.results.drain(..)) {
            slot.set(r.unwrap_or_else(|| {
                Err(Error::Io {
                    path: std::path::PathBuf::new(),
                    op: "commit",
                    source: std::io::Error::other("the committing thread panicked"),
                })
            }));
        }
    }
}

/// What a transaction's expectation check decided.
struct Plan {
    first_indices: Vec<u64>,
    /// The lengths this transaction leaves its aggregates at.
    lens: HashMap<Uuid, u64>,
}

/// Checks a transaction's expectations against the committed lengths
/// as the batch so far leaves them (`overlay`). `Err((error, against_
/// committed))` is a refused expectation; `against_committed` says
/// whether the length it failed against is the committed one (so the
/// refusal stands whatever happens to the batch) or one another
/// transaction of this batch produced.
fn plan_txn(
    agg_len: &impl ReadableTable<&'static [u8; 16], u64>,
    overlay: &HashMap<Uuid, u64>,
    appends: &[PendingAppend],
    check: bool,
) -> Result<std::result::Result<Plan, (Error, bool)>> {
    let mut lens: HashMap<Uuid, u64> = HashMap::new();
    let mut first_indices = Vec::with_capacity(appends.len());
    for a in appends {
        let Some(id) = a.aggregate else {
            first_indices.push(0);
            continue;
        };
        let (cur, from_overlay) = match lens.get(&id).or_else(|| overlay.get(&id)) {
            Some(l) => (*l, overlay.contains_key(&id)),
            None => (agg_len.get(id.as_bytes())?.map_or(0, |v| v.value()), false),
        };
        if check && !a.expected.holds(cur) {
            return Ok(Err((
                Error::ExpectedLength {
                    aggregate: id,
                    expected: a.expected,
                    actual: cur,
                },
                !from_overlay,
            )));
        }
        first_indices.push(cur);
        lens.insert(id, cur + a.events.len() as u64);
    }
    Ok(Ok(Plan {
        first_indices,
        lens,
    }))
}

/// Encodes one transaction's records after the ones already in `buf`,
/// advancing `position`, and returns what the committer is told.
#[allow(clippy::too_many_arguments)]
fn encode_txn(
    appends: Vec<PendingAppend>,
    commit_id: Option<[u8; 16]>,
    first_indices: &[u64],
    recorded_at: i64,
    position: &mut u64,
    buf: &mut Vec<u8>,
    body: &mut Vec<u8>,
    entries: &mut Vec<Entry>,
) -> Committed {
    let total: usize = appends.iter().map(|a| a.events.len()).sum();
    let first = *position;
    let mut results = Vec::with_capacity(appends.len());
    let mut seq = 0usize;
    for (a, &first_index) in appends.into_iter().zip(first_indices) {
        let count = a.events.len() as u64;
        let free = a.aggregate.is_none();
        results.push(AppendResult {
            aggregate: a.aggregate,
            first_position: *position,
            first_index,
            count,
            new_len: if free { 0 } else { first_index + count },
        });
        for (j, ev) in a.events.into_iter().enumerate() {
            let mut flags = if free { FLAG_FREE } else { 0 };
            if seq == 0 {
                flags |= FLAG_FIRST_IN_TXN;
                if commit_id.is_some() {
                    flags |= FLAG_COMMIT_ID;
                }
            }
            if seq + 1 == total {
                flags |= FLAG_LAST_IN_TXN;
            }
            let mut tags = ev.tags;
            tags.sort();
            tags.dedup();
            let rec = Event {
                position: *position,
                aggregate: a.aggregate,
                index: if free { 0 } else { first_index + j as u64 },
                recorded_at,
                flags,
                commit_id: if seq == 0 { commit_id } else { None },
                tags,
                metadata: ev.metadata,
                payload: ev.payload,
            };
            body.clear();
            event::encode_body(&rec, body);
            let rel_offset = buf.len() as u64;
            segment::frame_record(body, buf);
            entries.push(Entry {
                position: *position,
                rel_offset,
                frame_len: (FRAME_LEN + body.len()) as u32,
                aggregate: rec.aggregate,
                index: rec.index,
                tags: rec.tags,
            });
            *position += 1;
            seq += 1;
        }
    }
    Committed {
        first,
        last: *position - 1,
        next: *position,
        appends: results,
        duplicate: false,
    }
}

/// The shape of a transaction: per append, the aggregate (`None` for a
/// free append) and the event count. Two commits with one id must agree
/// on it for the second to be a duplicate of the first.
type Shape = Vec<(Option<Uuid>, u64)>;

fn shape(appends: &[PendingAppend]) -> Shape {
    appends
        .iter()
        .map(|a| (a.aggregate, a.events.len() as u64))
        .collect()
}

/// Reads the records of the committed transaction at `first..next`, by
/// the index's locations, checking each carries its position.
fn read_committed(
    inner: &Inner,
    pos: &impl ReadableTable<u64, (u64, u64, u32)>,
    first: u64,
    next: u64,
) -> Result<Vec<Event>> {
    let mut out = Vec::with_capacity((next - first) as usize);
    for p in first..next {
        let loc: Location =
            pos.get(p)?
                .map(|v| v.value().into())
                .ok_or_else(|| Error::Inconsistent {
                    position: p,
                    reason: "a commit id names it but the position table lacks it".into(),
                })?;
        let path = inner.layout.segment(loc.base);
        let file = inner.file(loc.base)?;
        let body = segment::read_record_at(
            &file,
            &path,
            loc.offset,
            loc.frame_len,
            inner.opts.max_record_bytes,
        )?;
        let ev = event::decode_body(&body)
            .map_err(|e| Error::corrupt(&path, loc.offset, e.to_string()))?;
        if ev.position != p {
            return Err(Error::corrupt(
                &path,
                loc.offset,
                format!(
                    "index points position {p} at a record carrying {}",
                    ev.position
                ),
            ));
        }
        out.push(ev);
    }
    Ok(out)
}

/// What a resend with a known id is told: the first landing's result,
/// rebuilt from its records and the resend's append shape, which must
/// match the records (same aggregates, same counts, in order).
fn reconstruct(
    id: [u8; 16],
    first: u64,
    next: u64,
    recs: &[Event],
    appends: &[PendingAppend],
) -> Result<Committed> {
    let reused = |reason: String| Error::CommitIdReused {
        id,
        first,
        next,
        reason,
    };
    let total: u64 = appends.iter().map(|a| a.events.len() as u64).sum();
    if total != next - first {
        return Err(reused(format!(
            "{total} events where the landed transaction has {}",
            next - first
        )));
    }
    let mut results = Vec::with_capacity(appends.len());
    let mut cursor = 0usize;
    for (k, a) in appends.iter().enumerate() {
        let count = a.events.len();
        let slice = &recs[cursor..cursor + count];
        if slice.iter().any(|r| r.aggregate != a.aggregate) {
            return Err(reused(format!(
                "append {k} names {} where the landed records belong to {}",
                describe(a.aggregate),
                describe(slice[0].aggregate)
            )));
        }
        let free = a.aggregate.is_none();
        let first_index = if free { 0 } else { slice[0].index };
        if !free
            && slice
                .iter()
                .enumerate()
                .any(|(j, r)| r.index != first_index + j as u64)
        {
            return Err(reused(format!(
                "append {k} spans records whose aggregate indices are not consecutive"
            )));
        }
        results.push(AppendResult {
            aggregate: a.aggregate,
            first_position: first + cursor as u64,
            first_index,
            count: count as u64,
            new_len: if free { 0 } else { first_index + count as u64 },
        });
        cursor += count;
    }
    Ok(Committed {
        first,
        last: next - 1,
        next,
        appends: results,
        duplicate: true,
    })
}

fn describe(aggregate: Option<Uuid>) -> String {
    aggregate.map_or("no aggregate".to_string(), |a| format!("aggregate {a}"))
}

/// Lands one batch: every transaction is checked in order against the
/// committed state as the batch so far leaves it; a refused one gets its
/// error and consumes nothing; a resend of a landed commit id gets the
/// first landing's result and consumes nothing; the rest are written,
/// synced and indexed together and each acknowledged with its own
/// result. A failure of the write or the index commit fails every
/// accepted transaction.
fn run_batch(inner: &Inner, w: &mut SegmentWriter, batch: Vec<Pending>) {
    let n = batch.len();
    let mut filler = SlotFiller {
        slots: batch.iter().map(|p| p.slot.clone()).collect(),
        results: (0..n).map(|_| None).collect(),
    };
    let fail_all = |filler: &mut SlotFiller, e: Error, which: &[usize]| {
        let mut first = Some(e);
        for &i in which {
            let err = match first.take() {
                Some(e) => e,
                None => filler.results[i]
                    .as_ref()
                    .and_then(|r| r.as_ref().err())
                    .map(Error::redescribe)
                    .unwrap_or_else(|| Error::Io {
                        path: std::path::PathBuf::new(),
                        op: "commit",
                        source: std::io::Error::other("the batch failed"),
                    }),
            };
            filler.set(i, Err(err));
        }
    };

    let head = inner.head.load(Ordering::Acquire);
    let wtxn = match inner.index.begin_write() {
        Ok(t) => t,
        Err(e) => {
            let all: Vec<usize> = (0..n).collect();
            fail_all(&mut filler, e, &all);
            return;
        }
    };
    let mut overlay: HashMap<Uuid, u64> = HashMap::new();
    let mut buf = Vec::new();
    let mut body = Vec::new();
    let mut entries = Vec::new();
    let mut position = head;
    let mut accepted: Vec<usize> = Vec::new();
    let mut overlay_rejected: Vec<usize> = Vec::new();
    let mut committed: Vec<Option<Committed>> = (0..n).map(|_| None).collect();
    // Commit ids this batch lands, with the accepted transaction's index.
    let mut batch_ids: HashMap<[u8; 16], usize> = HashMap::new();
    let mut shapes: Vec<Option<Shape>> = (0..n).map(|_| None).collect();
    // (i, j): i resends j's transaction, which lands in this batch.
    let mut echoes: Vec<(usize, usize)> = Vec::new();
    let mut txn_ids: Vec<([u8; 16], u64, u64)> = Vec::new();
    {
        let tables = (|| -> Result<_> {
            Ok((
                wtxn.open_table(AGG_LEN)?,
                wtxn.open_table(TXN_ID)?,
                wtxn.open_table(POS)?,
            ))
        })();
        let (agg_len, ids, pos) = match tables {
            Ok(t) => t,
            Err(e) => {
                let all: Vec<usize> = (0..n).collect();
                fail_all(&mut filler, e, &all);
                return;
            }
        };
        for (i, p) in batch.into_iter().enumerate() {
            if let Some(id) = p.commit_id {
                if let Some(&j) = batch_ids.get(&id) {
                    if shapes[j].as_deref() == Some(shape(&p.appends).as_slice()) {
                        echoes.push((i, j));
                    } else {
                        filler.set(
                            i,
                            Err(Error::CommitIdReused {
                                id,
                                first: 0,
                                next: 0,
                                reason: "another transaction with this id is in the same batch"
                                    .into(),
                            }),
                        );
                        overlay_rejected.push(i);
                    }
                    continue;
                }
                let landed = match ids.get(&id) {
                    Ok(v) => v.map(|v| v.value()),
                    Err(e) => {
                        let rest: Vec<usize> = (i..n).collect();
                        fail_all(&mut filler, Error::from(e), &rest);
                        return;
                    }
                };
                if let Some((first, next)) = landed {
                    let answer = read_committed(inner, &pos, first, next)
                        .and_then(|recs| reconstruct(id, first, next, &recs, &p.appends));
                    if answer.is_ok() {
                        inner.counters.duplicates.fetch_add(1, Ordering::Relaxed);
                    }
                    // Judged against committed state: it stands whatever
                    // happens to the batch.
                    filler.set(i, answer);
                    continue;
                }
            }
            let plan = match plan_txn(&agg_len, &overlay, &p.appends, true) {
                Ok(Ok(plan)) => plan,
                Ok(Err((e, against_committed))) => {
                    filler.set(i, Err(e));
                    if !against_committed {
                        overlay_rejected.push(i);
                    }
                    continue;
                }
                Err(e) => {
                    let rest: Vec<usize> = (i..n).collect();
                    fail_all(&mut filler, e, &rest);
                    return;
                }
            };
            overlay.extend(plan.lens.iter().map(|(k, v)| (*k, *v)));
            if let Some(id) = p.commit_id {
                batch_ids.insert(id, i);
                shapes[i] = Some(shape(&p.appends));
            }
            let c = encode_txn(
                p.appends,
                p.commit_id,
                &plan.first_indices,
                now_nanos(),
                &mut position,
                &mut buf,
                &mut body,
                &mut entries,
            );
            if let Some(id) = p.commit_id {
                txn_ids.push((id, c.first, c.next));
            }
            committed[i] = Some(c);
            accepted.push(i);
        }
    }
    if accepted.is_empty() {
        drop(wtxn);
        return;
    }
    match land(
        inner,
        w,
        wtxn,
        &buf,
        &entries,
        &overlay,
        &txn_ids,
        position,
        accepted.len() as u64,
    ) {
        Ok(()) => {
            for &(i, j) in &echoes {
                let mut c = committed[j].clone().expect("planned");
                c.duplicate = true;
                filler.set(i, Ok(c));
            }
            inner
                .counters
                .duplicates
                .fetch_add(echoes.len() as u64, Ordering::Relaxed);
            for i in accepted {
                filler.set(i, Ok(committed[i].take().expect("planned")));
            }
        }
        Err(e) => {
            // The first accepted transaction keeps the original error; the
            // others, any refusal that was judged against lengths this
            // batch did not land, and any resend of a transaction this
            // batch did not land, get a copy.
            let mut which = accepted;
            which.extend(overlay_rejected);
            which.extend(echoes.iter().map(|&(i, _)| i));
            which.sort_unstable();
            let first = which[0];
            let copies: Vec<Error> = which.iter().skip(1).map(|_| e.redescribe()).collect();
            filler.set(first, Err(e));
            for (i, err) in which.into_iter().skip(1).zip(copies) {
                filler.set(i, Err(err));
            }
        }
    }
}
