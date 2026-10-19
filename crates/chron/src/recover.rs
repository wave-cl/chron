//! Open-time recovery: validate segment headers, finish an interrupted
//! truncation, check the index against the data it vouches for, replay
//! the complete transactions the segments hold past the durable index,
//! and cut a torn tail.
//!
//! The segment files are the source of truth once their bytes are
//! synced; the index commit is the commit point in memory and reaches
//! the disk at checkpoints (every `index_checkpoint_batches` batches or
//! `index_checkpoint_interval`, on close, and on every maintenance
//! write). So on open the index may lag the data, never lead it. What
//! `recover` checks and does, in order:
//!
//! - I1 (durable prefix): the segment holding the index head `H` reaches
//!   `H`, and the index places `H - 1` where the data has it.
//! - I2 (replay): records from `H` on are consecutive, crc-valid,
//!   well flagged (FIRST/LAST in order), and dense per aggregate against
//!   the durable lengths; a well-formed record that contradicts the index
//!   is damage, not a torn tail.
//! - I3 (rolls): every segment but the last ends clean with no open
//!   transaction, and the next segment starts where it ends; a last
//!   segment nothing was ever written into is removed.
//! - I4 (tail): the last segment is cut after its last complete
//!   transaction; a fault followed by a later valid record is damage.
//! - I5 (checkpoint): the replay is committed durably, so after open the
//!   index is at the data's head and the file ends at the cut.
//! - I6 (floor): no segment below the floor survives, and `floor <= H`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use redb::Durability;
use uuid::Uuid;

use crate::db::Layout;
use crate::error::{Error, Result};
use crate::event::{self, Event};
use crate::index::{AGG_INDEX, AGG_LEN, HEAD_KEY, Index, META, POS, TAG, TXN_ID};
use crate::options::Options;
use crate::segment::{self, HEADER_LEN, ScanItem, Scanner, SegmentWriter};

pub(crate) struct Recovered {
    pub index: Index,
    pub head: u64,
    pub floor: u64,
    pub tail_base: u64,
    pub tail_path: PathBuf,
    /// Byte length of the tail after truncation; the writer continues here.
    pub tail_len: u64,
}

/// One good record found by a scan.
pub(crate) struct Record {
    pub position: u64,
    pub offset: u64,
    pub frame_len: u32,
    pub event: Event,
}

pub(crate) enum Item {
    Record(Record),
    /// The scan stopped at `offset`; `clean` when the file ends exactly on
    /// a record boundary.
    Tail {
        offset: u64,
        clean: bool,
        reason: String,
    },
}

/// Walks one segment's records in order: whole, crc-valid, decodable, at
/// consecutive positions from the header's base. Stops at the first fault
/// and reports it as a [`Item::Tail`].
pub(crate) struct Records {
    scanner: Scanner,
    expected: u64,
}

impl Records {
    pub(crate) fn open(path: &Path, base: u64, max_record: usize) -> Result<Self> {
        Ok(Records {
            scanner: Scanner::open(path, HEADER_LEN, max_record)?,
            expected: base,
        })
    }

    /// The position after the last good record so far.
    pub(crate) fn reached(&self) -> u64 {
        self.expected
    }

    pub(crate) fn next(&mut self) -> Result<Item> {
        match self.scanner.next()? {
            ScanItem::Record { offset, body } => {
                let frame_len = (segment::FRAME_LEN + body.len()) as u32;
                let event = match event::decode_body(&body) {
                    Ok(ev) => ev,
                    Err(e) => {
                        return Ok(Item::Tail {
                            offset,
                            clean: false,
                            reason: e.to_string(),
                        });
                    }
                };
                if event.position != self.expected {
                    return Ok(Item::Tail {
                        offset,
                        clean: false,
                        reason: format!(
                            "record carries position {} where {} was expected",
                            event.position, self.expected
                        ),
                    });
                }
                self.expected += 1;
                Ok(Item::Record(Record {
                    position: event.position,
                    offset,
                    frame_len,
                    event,
                }))
            }
            ScanItem::Tail { offset, reason } => Ok(Item::Tail {
                offset,
                clean: reason == segment::TailReason::Clean,
                reason: reason.to_string(),
            }),
        }
    }
}

/// A record bound for the index.
pub(crate) struct Entry {
    pub position: u64,
    pub base: u64,
    pub offset: u64,
    pub frame_len: u32,
    pub aggregate: Option<Uuid>,
    pub index: u64,
    pub tags: Vec<Bytes>,
}

/// Entries per index write while replaying or rebuilding.
pub(crate) const FLUSH_EVERY: usize = 50_000;

/// Groups scanned records into transactions and checks them as the
/// commit path would have: flags in order, aggregate indices dense
/// against the lengths it knows. Shared by open-time replay (over the
/// live index) and `rebuild_index` (into a fresh one).
pub(crate) struct Replayer {
    /// Lengths as committed before the replay plus what it landed.
    pub lens: HashMap<Uuid, u64>,
    /// Lengths the open transaction will leave.
    pending: HashMap<Uuid, u64>,
    open: Vec<Entry>,
    open_id: Option<[u8; 16]>,
    pub batch: Vec<Entry>,
    pub txn_ids: Vec<([u8; 16], u64, u64)>,
    pub touched: HashSet<Uuid>,
    /// One past the last complete transaction.
    pub head: u64,
    /// Offset after the last record of the current segment that is
    /// committed: in the durable prefix or ending a replayed transaction.
    pub last_complete_end: u64,
    /// An aggregate the index does not know may start at any index: the
    /// rebuild of a database above a floor, whose earlier events are gone.
    unknown_starts_anywhere: bool,
    pub decoded: u64,
}

impl Replayer {
    pub(crate) fn new(head: u64, unknown_starts_anywhere: bool) -> Self {
        Replayer {
            lens: HashMap::new(),
            pending: HashMap::new(),
            open: Vec::new(),
            open_id: None,
            batch: Vec::new(),
            txn_ids: Vec::new(),
            touched: HashSet::new(),
            head,
            last_complete_end: HEADER_LEN,
            unknown_starts_anywhere,
            decoded: 0,
        }
    }

    pub(crate) fn begin_segment(&mut self) {
        self.last_complete_end = HEADER_LEN;
    }

    pub(crate) fn open_txn(&self) -> bool {
        !self.open.is_empty()
    }

    /// Takes one record. `known` answers the committed length of an
    /// aggregate the replay has not seen yet.
    pub(crate) fn record(
        &mut self,
        r: Record,
        base: u64,
        path: &Path,
        known: &mut impl FnMut(&Uuid) -> Result<Option<u64>>,
    ) -> Result<()> {
        self.decoded += 1;
        let ev = &r.event;
        if ev.is_first_in_txn() != self.open.is_empty() {
            let what = if self.open.is_empty() {
                "record without FIRST_IN_TXN starts a transaction"
            } else {
                "record with FIRST_IN_TXN inside an open transaction"
            };
            return Err(Error::corrupt(path, r.offset, what));
        }
        let agg_index = if let Some(id) = ev.aggregate {
            let cur = match self.pending.get(&id).or_else(|| self.lens.get(&id)) {
                Some(l) => *l,
                None => match known(&id)? {
                    Some(l) => l,
                    None if self.unknown_starts_anywhere => ev.index,
                    None => 0,
                },
            };
            if ev.index != cur {
                return Err(Error::corrupt(
                    path,
                    r.offset,
                    format!("aggregate {id} index {} where {cur} was expected", ev.index),
                ));
            }
            self.pending.insert(id, cur + 1);
            cur
        } else {
            0
        };
        if ev.is_first_in_txn() {
            self.open_id = ev.commit_id;
        }
        let last = ev.is_last_in_txn();
        self.open.push(Entry {
            position: r.position,
            base,
            offset: r.offset,
            frame_len: r.frame_len,
            aggregate: ev.aggregate,
            index: agg_index,
            tags: r.event.tags,
        });
        if last {
            self.head = r.position + 1;
            self.last_complete_end = r.offset + u64::from(r.frame_len);
            if let Some(id) = self.open_id.take() {
                self.txn_ids.push((id, self.open[0].position, self.head));
            }
            self.batch.append(&mut self.open);
            for (id, len) in self.pending.drain() {
                self.lens.insert(id, len);
                self.touched.insert(id);
            }
        }
        Ok(())
    }

    /// Writes the batched entries, ids and touched lengths into `txn`.
    pub(crate) fn flush_into(&mut self, txn: &redb::WriteTransaction) -> Result<()> {
        let mut pos = txn.open_table(POS)?;
        let mut agg_index = txn.open_table(AGG_INDEX)?;
        let mut tag = txn.open_table(TAG)?;
        let mut agg_len = txn.open_table(AGG_LEN)?;
        let mut ids = txn.open_table(TXN_ID)?;
        for (id, first, next) in self.txn_ids.drain(..) {
            ids.insert(&id, (first, next))?;
        }
        for e in self.batch.drain(..) {
            pos.insert(e.position, (e.base, e.offset, e.frame_len))?;
            if let Some(id) = &e.aggregate {
                agg_index.insert((id.as_bytes(), e.index), e.position)?;
            }
            for t in &e.tags {
                tag.insert((t.as_ref(), e.position), ())?;
            }
        }
        for id in self.touched.drain() {
            agg_len.insert(id.as_bytes(), self.lens[&id])?;
        }
        Ok(())
    }

    /// Flushes into one index write at `durability`.
    pub(crate) fn flush(&mut self, index: &Index, durability: Durability) -> Result<()> {
        let txn = index.begin_write_with(durability)?;
        self.flush_into(&txn)?;
        txn.commit()?;
        Ok(())
    }
}

/// Whether a crc-valid record carrying a position at or past `after`
/// exists anywhere in the file from `offset` on: damage inside committed
/// data rather than a torn tail.
pub(crate) fn later_record_exists(path: &Path, offset: u64, after: u64) -> Result<bool> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::File::open(path).map_err(|e| Error::io(path, "open", e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io(path, "seek", e))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| Error::io(path, "read", e))?;
    let frame = segment::FRAME_LEN;
    for i in 0..buf.len().saturating_sub(frame) {
        let body_len = u32::from_be_bytes(buf[i..i + 4].try_into().unwrap()) as usize;
        if body_len == 0 || i + frame + body_len > buf.len() {
            continue;
        }
        let crc = u32::from_be_bytes(buf[i + 4..i + 8].try_into().unwrap());
        let body = &buf[i + frame..i + frame + body_len];
        if crc32fast::hash(body) != crc {
            continue;
        }
        if let Ok(ev) = event::decode_body(body)
            && ev.position >= after
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remove(path: &Path, dir: &Path) -> Result<()> {
    fs::remove_file(path).map_err(|e| Error::io(path, "remove", e))?;
    segment::sync_dir(dir)
}

pub(crate) fn recover(layout: &Layout, opts: &Options) -> Result<Recovered> {
    let dir = layout.segments_dir();
    let index = Index::open(&layout.index_file(), opts.fsync)?;
    // A truncation that committed its index change but was interrupted
    // before the files were cut: finish it, or the cut records would be
    // replayed below as if committed.
    if let Some(to) = index.truncate_intent()? {
        crate::truncate::finish(&index, layout, to)?;
    }
    let head_d = index.head()?;
    let floor = index.floor()?;

    // Segments a retention dropped from the index before a crash stopped
    // it removing the files.
    let mut listed = Vec::new();
    for (base, path) in segment::list(&dir)? {
        if base < floor {
            remove(&path, &dir)?;
        } else {
            listed.push((base, path));
        }
    }
    let n = listed.len();
    let mut segments = Vec::with_capacity(n);
    for (k, (base, path)) in listed.into_iter().enumerate() {
        match segment::read_header(&path)? {
            Ok(Some(h)) if h.base == base => segments.push((base, path)),
            Ok(Some(h)) => {
                return Err(Error::corrupt(
                    &path,
                    0,
                    format!("header says base {} but the file name says {base}", h.base),
                ));
            }
            // A roll that crashed before its header was complete, or
            // whose header never reached the disk whole.
            Ok(None) if k + 1 == n => remove(&path, &dir)?,
            Err(_) if k + 1 == n && base >= head_d => remove(&path, &dir)?,
            Ok(None) => {
                return Err(Error::corrupt(&path, 0, "segment shorter than a header"));
            }
            Err(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
    }

    // The segment holding the durable head: the last one starting at or
    // before it. Later segments may hold replayable data.
    let Some(k) = segments.iter().rposition(|(base, _)| *base <= head_d) else {
        if segments.is_empty() && head_d == floor {
            let w = SegmentWriter::create(&dir, floor, true)?;
            drop(w);
            return Ok(Recovered {
                index,
                head: head_d,
                floor,
                tail_base: floor,
                tail_path: layout.segment(floor),
                tail_len: HEADER_LEN,
            });
        }
        return Err(Error::corrupt(
            layout.index_file(),
            0,
            format!("head is {head_d} but no segment starts at or before it"),
        ));
    };

    let max_record = opts.max_record_bytes;
    let mut replayer = Replayer::new(head_d, false);
    let mut known = |id: &Uuid| index.aggregate_len(id).map(Some);
    let mut tail: Option<(usize, u64)> = None;
    let mut j = k;
    while j < segments.len() {
        let (base, path) = &segments[j];
        let is_last = j + 1 == segments.len();
        let mut records = Records::open(path, *base, max_record)?;
        replayer.begin_segment();
        // Where the durable prefix ends, for I1.
        let mut durable_last: Option<(u64, u32)> = None;
        loop {
            match records.next()? {
                Item::Record(r) => {
                    if r.position < head_d {
                        if r.position + 1 == head_d {
                            durable_last = Some((r.offset, r.frame_len));
                        }
                        replayer.last_complete_end = r.offset + u64::from(r.frame_len);
                        continue;
                    }
                    replayer.record(r, *base, path, &mut known)?;
                    if replayer.batch.len() >= FLUSH_EVERY {
                        replayer.flush(&index, Durability::None)?;
                    }
                }
                Item::Tail {
                    offset,
                    clean,
                    reason,
                } => {
                    let reached = records.reached();
                    if reached < head_d {
                        return Err(Error::corrupt(
                            path,
                            offset,
                            format!(
                                "index head is {head_d} but the data ends at {reached} ({reason})"
                            ),
                        ));
                    }
                    if j == k && head_d > *base {
                        let (off, len) = durable_last.expect("the prefix reached the head");
                        let want = (*base, off, len);
                        let got = index
                            .locate(head_d - 1)?
                            .map(|l| (l.base, l.offset, l.frame_len));
                        if got != Some(want) {
                            return Err(Error::corrupt(
                                path,
                                off,
                                format!(
                                    "index places position {} at {got:?}, the data has it at {want:?}",
                                    head_d - 1
                                ),
                            ));
                        }
                    }
                    if !is_last {
                        if !clean {
                            return Err(Error::corrupt(path, offset, reason));
                        }
                        if replayer.open_txn() {
                            return Err(Error::corrupt(
                                path,
                                offset,
                                "transaction still open at the end of a segment that is not the last",
                            ));
                        }
                        let (next_base, next_path) = &segments[j + 1];
                        if *next_base != reached {
                            // A roll that crashed before anything was
                            // written into the new segment leaves one
                            // starting past the data; it is the last one.
                            if j + 2 == segments.len() && *next_base > reached {
                                remove(next_path, &dir)?;
                                segments.pop();
                                tail = Some((j, offset));
                                break;
                            }
                            return Err(Error::corrupt(
                                next_path,
                                0,
                                format!(
                                    "segment starts at position {next_base}, {reached} expected"
                                ),
                            ));
                        }
                    } else if !clean || replayer.open_txn() {
                        // A torn tail is cut; damage with committed
                        // records after it is not.
                        if later_record_exists(path, offset, reached)? {
                            return Err(Error::corrupt(
                                path,
                                offset,
                                format!("{reason}; committed records follow the damage"),
                            ));
                        }
                        tail = Some((j, replayer.last_complete_end));
                    } else {
                        tail = Some((j, offset));
                    }
                    break;
                }
            }
        }
        if tail.is_some() {
            break;
        }
        j += 1;
    }
    let (tail_k, tail_cut) = tail.expect("the last segment sets the tail");
    let (tail_base, tail_path) = segments[tail_k].clone();

    // I5: the replay reaches the disk before the writer continues past it.
    let head = replayer.head;
    if head > head_d || !replayer.batch.is_empty() {
        let txn = index.begin_write_with(Durability::Immediate)?;
        replayer.flush_into(&txn)?;
        txn.open_table(META)?.insert(HEAD_KEY, head)?;
        txn.commit()?;
    }
    let file_len = fs::metadata(&tail_path)
        .map_err(|e| Error::io(&tail_path, "metadata", e))?
        .len();
    if tail_cut < file_len {
        segment::truncate(&tail_path, tail_cut)?;
    }

    if opts.verify_all_segments {
        verify_all(&segments, head, &index, max_record)?;
    }

    Ok(Recovered {
        index,
        head,
        floor,
        tail_base,
        tail_path,
        tail_len: tail_cut,
    })
}

/// Reads every record of every segment, checking crc, consecutive
/// positions reaching exactly the next segment's base (or the head), and
/// that the index places each record where it is.
fn verify_all(
    segments: &[(u64, PathBuf)],
    head: u64,
    index: &Index,
    max_record: usize,
) -> Result<()> {
    let txn = index.begin_read()?;
    let pos = txn.open_table(POS)?;
    for (k, (base, path)) in segments.iter().enumerate() {
        let end = segments.get(k + 1).map_or(head, |(next, _)| *next);
        let mut records = Records::open(path, *base, max_record)?;
        loop {
            if records.reached() == end && *base < end {
                // Everything the index knows is checked; bytes after the
                // last committed record are a failed commit's leftovers.
                break;
            }
            match records.next()? {
                Item::Record(r) => {
                    if r.position >= end {
                        return Err(Error::corrupt(
                            path,
                            r.offset,
                            format!(
                                "record at position {} past the segment's end {end}",
                                r.position
                            ),
                        ));
                    }
                    let want = (*base, r.offset, r.frame_len);
                    let got = pos.get(r.position)?.map(|v| v.value());
                    if got != Some(want) {
                        return Err(Error::corrupt(
                            path,
                            r.offset,
                            format!(
                                "index places position {} at {got:?}, the data has it at {want:?}",
                                r.position
                            ),
                        ));
                    }
                }
                Item::Tail {
                    offset,
                    clean,
                    reason,
                } => {
                    if !clean {
                        return Err(Error::corrupt(path, offset, reason));
                    }
                    let reached = records.reached();
                    if reached != end {
                        return Err(Error::corrupt(
                            path,
                            offset,
                            format!("segment ends at position {reached}, {end} expected"),
                        ));
                    }
                    break;
                }
            }
        }
    }
    Ok(())
}
