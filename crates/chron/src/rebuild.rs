//! Rebuilding the index from the segment files alone.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use bytes::Bytes;
use uuid::Uuid;

use crate::db::{Layout, Lock};
use crate::error::{Error, Result};
use crate::index::{AGG_INDEX, AGG_LEN, FLOOR_KEY, HEAD_KEY, Index, META, POS, TAG};
use crate::options::{Fsync, Options};
use crate::recover::{Item, Records};
use crate::segment;

/// What [`Db::rebuild_index`](crate::Db::rebuild_index) found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rebuilt {
    /// The new head: one past the last event of the last complete
    /// transaction.
    pub head: u64,
    /// Base of the first segment: the retention floor the data carries.
    pub floor: u64,
    /// Segment files read.
    pub segments: usize,
    /// Decodable events of an incomplete trailing transaction, left out of
    /// the index. The next open truncates their bytes.
    pub dropped: u64,
}

/// Records committed under the current flush, bound for the index.
struct Entry {
    position: u64,
    base: u64,
    offset: u64,
    frame_len: u32,
    aggregate: Option<Uuid>,
    index: u64,
    tags: Vec<Bytes>,
}

const FLUSH_EVERY: usize = 50_000;

fn flush(
    index: &Index,
    batch: &mut Vec<Entry>,
    lens: &HashMap<Uuid, u64>,
    touched: &mut HashSet<Uuid>,
) -> Result<()> {
    let txn = index.begin_write()?;
    {
        let mut pos = txn.open_table(POS)?;
        let mut agg_index = txn.open_table(AGG_INDEX)?;
        let mut tag = txn.open_table(TAG)?;
        let mut agg_len = txn.open_table(AGG_LEN)?;
        for e in batch.drain(..) {
            pos.insert(e.position, (e.base, e.offset, e.frame_len))?;
            if let Some(id) = &e.aggregate {
                agg_index.insert((id.as_bytes(), e.index), e.position)?;
            }
            for t in &e.tags {
                tag.insert((t.as_ref(), e.position), ())?;
            }
        }
        for id in touched.drain() {
            agg_len.insert(id.as_bytes(), lens[&id])?;
        }
    }
    txn.commit()?;
    Ok(())
}

fn validated_segments(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let listed = segment::list(dir)?;
    let n = listed.len();
    let mut out = Vec::with_capacity(n);
    for (k, (base, path)) in listed.into_iter().enumerate() {
        match segment::read_header(&path)? {
            Ok(Some(h)) if h.base == base => out.push((base, path)),
            Ok(Some(h)) => {
                return Err(Error::corrupt(
                    &path,
                    0,
                    format!("header says base {} but the file name says {base}", h.base),
                ));
            }
            // A roll that crashed before its header was complete; the next
            // open removes it.
            Ok(None) if k + 1 == n => {}
            Ok(None) => return Err(Error::corrupt(&path, 0, "segment shorter than a header")),
            Err(reason) => return Err(Error::corrupt(&path, 0, reason)),
        }
    }
    Ok(out)
}

pub(crate) fn rebuild(path: &Path, opts: &Options) -> Result<Rebuilt> {
    let layout = Layout::new(path);
    let dir = layout.segments_dir();
    fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, "mkdir", e))?;
    let _lock = Lock::acquire(&layout.lock_file())?;

    let tmp = layout.root().join("index.redb.rebuild");
    if tmp.exists() {
        fs::remove_file(&tmp).map_err(|e| Error::io(&tmp, "remove", e))?;
    }
    let segments = validated_segments(&dir)?;

    let index = Index::create(&tmp, Fsync::Never)?;
    let mut lens: HashMap<Uuid, u64> = HashMap::new();
    let mut touched: HashSet<Uuid> = HashSet::new();
    let mut batch: Vec<Entry> = Vec::new();
    let mut open_txn: Vec<Entry> = Vec::new();
    let mut pending: HashMap<Uuid, u64> = HashMap::new();
    let floor = segments.first().map_or(0, |(b, _)| *b);
    let mut next_base = floor;
    let mut head = floor;
    let mut decoded = 0u64;
    let last = segments.len().saturating_sub(1);

    for (k, (base, seg_path)) in segments.iter().enumerate() {
        if *base != next_base {
            return Err(Error::corrupt(
                seg_path,
                0,
                format!("segment starts at position {base}, {next_base} expected"),
            ));
        }
        let mut records = Records::open(seg_path, *base, opts.max_record_bytes)?;
        loop {
            match records.next()? {
                Item::Record(r) => {
                    decoded += 1;
                    let ev = &r.event;
                    if ev.is_first_in_txn() != open_txn.is_empty() {
                        let what = if open_txn.is_empty() {
                            "record without FIRST_IN_TXN starts a transaction"
                        } else {
                            "record with FIRST_IN_TXN inside an open transaction"
                        };
                        return Err(Error::corrupt(seg_path, r.offset, what));
                    }
                    let agg_index = if let Some(id) = ev.aggregate {
                        // An aggregate first seen above a floor may start at
                        // any index: its earlier events were dropped.
                        let cur = pending
                            .get(&id)
                            .copied()
                            .or_else(|| lens.get(&id).copied())
                            .unwrap_or(if floor > 0 { ev.index } else { 0 });
                        if ev.index != cur {
                            return Err(Error::corrupt(
                                seg_path,
                                r.offset,
                                format!(
                                    "aggregate {id} index {} where {cur} was expected",
                                    ev.index
                                ),
                            ));
                        }
                        pending.insert(id, cur + 1);
                        cur
                    } else {
                        0
                    };
                    let last_in_txn = ev.is_last_in_txn();
                    open_txn.push(Entry {
                        position: r.position,
                        base: *base,
                        offset: r.offset,
                        frame_len: r.frame_len,
                        aggregate: ev.aggregate,
                        index: agg_index,
                        tags: r.event.tags,
                    });
                    if last_in_txn {
                        head = r.position + 1;
                        batch.append(&mut open_txn);
                        for (id, len) in pending.drain() {
                            lens.insert(id, len);
                            touched.insert(id);
                        }
                        if batch.len() >= FLUSH_EVERY {
                            flush(&index, &mut batch, &lens, &mut touched)?;
                        }
                    }
                }
                Item::Tail {
                    offset,
                    clean,
                    reason,
                } => {
                    if k != last {
                        if !clean {
                            return Err(Error::corrupt(seg_path, offset, reason));
                        }
                        if !open_txn.is_empty() {
                            return Err(Error::corrupt(
                                seg_path,
                                offset,
                                "transaction still open at the end of a segment that is not the last",
                            ));
                        }
                    }
                    next_base = records.reached();
                    break;
                }
            }
        }
    }
    let dropped = decoded - (head - floor);
    drop(open_txn);
    drop(pending);

    flush(&index, &mut batch, &lens, &mut touched)?;
    let txn = index.begin_write_durable()?;
    {
        let mut meta = txn.open_table(META)?;
        meta.insert(HEAD_KEY, head)?;
        meta.insert(FLOOR_KEY, floor)?;
    }
    txn.commit()?;
    drop(index);

    let target = layout.index_file();
    fs::rename(&tmp, &target).map_err(|e| Error::io(&tmp, "rename", e))?;
    segment::sync_dir(layout.root())?;
    Ok(Rebuilt {
        head,
        floor,
        segments: segments.len(),
        dropped,
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::event::{self, Event, FLAG_FIRST_IN_TXN, FLAG_LAST_IN_TXN};
    use crate::segment::SegmentWriter;

    /// Writes one segment of hand-made records: `(position, aggregate,
    /// index, flags)`.
    fn write_segment(dir: &Path, base: u64, records: &[(u64, Option<Uuid>, u64, u8)]) {
        let mut w = SegmentWriter::create(dir, base, false).unwrap();
        for (position, aggregate, index, flags) in records {
            let ev = Event {
                position: *position,
                aggregate: *aggregate,
                index: *index,
                recorded_at: 1,
                flags: *flags
                    | if aggregate.is_none() {
                        event::FLAG_FREE
                    } else {
                        0
                    },
                tags: vec![],
                metadata: Bytes::new(),
                payload: Bytes::from_static(b"p"),
            };
            let mut body = Vec::new();
            event::encode_body(&ev, &mut body);
            let mut buf = Vec::new();
            segment::frame_record(&body, &mut buf);
            w.write_at_end(&buf).unwrap();
            w.advance(buf.len() as u64);
        }
    }

    const BOTH: u8 = FLAG_FIRST_IN_TXN | FLAG_LAST_IN_TXN;

    fn setup() -> (tempfile::TempDir, PathBuf) {
        let d = tempfile::tempdir().unwrap();
        let dir = d.path().join("segments");
        fs::create_dir_all(&dir).unwrap();
        (d, dir)
    }

    #[test]
    fn hand_made_segments_rebuild() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(
            &dir,
            0,
            &[
                (0, a, 0, BOTH),
                (1, a, 1, FLAG_FIRST_IN_TXN),
                (2, None, 0, FLAG_LAST_IN_TXN),
            ],
        );
        write_segment(&dir, 3, &[(3, a, 2, BOTH)]);
        let r = rebuild(d.path(), &Options::new()).unwrap();
        assert_eq!(
            r,
            Rebuilt {
                head: 4,
                floor: 0,
                segments: 2,
                dropped: 0
            }
        );
        let index = Index::open(&d.path().join("index.redb"), Fsync::Never).unwrap();
        assert_eq!(index.head().unwrap(), 4);
        assert_eq!(index.aggregate_len(&Uuid::from_u128(1)).unwrap(), 3);
        assert_eq!(index.locate(3).unwrap().unwrap().base, 3);
    }

    #[test]
    fn non_dense_aggregate_index_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, BOTH), (1, a, 2, BOTH)]);
        assert!(
            matches!(rebuild(d.path(), &Options::new()), Err(Error::Corrupt { offset, .. }) if offset > 64)
        );
        assert!(!d.path().join("index.redb").exists());
    }

    #[test]
    fn open_transaction_in_a_middle_segment_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, FLAG_FIRST_IN_TXN)]);
        write_segment(&dir, 1, &[(1, a, 1, FLAG_LAST_IN_TXN)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn a_gap_between_segments_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, BOTH)]);
        write_segment(&dir, 5, &[(5, a, 1, BOTH)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { offset: 0, .. })
        ));
    }

    #[test]
    fn a_first_segment_above_zero_is_a_floor() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 7, &[(7, a, 5, BOTH), (8, a, 6, BOTH)]);
        let r = rebuild(d.path(), &Options::new()).unwrap();
        assert_eq!(
            r,
            Rebuilt {
                head: 9,
                floor: 7,
                segments: 1,
                dropped: 0
            }
        );
        let index = Index::open(&d.path().join("index.redb"), Fsync::Never).unwrap();
        assert_eq!(index.floor().unwrap(), 7);
        assert_eq!(index.aggregate_len(&Uuid::from_u128(1)).unwrap(), 7);
    }

    #[test]
    fn first_inside_an_open_transaction_is_corrupt() {
        let (d, dir) = setup();
        let a = Some(Uuid::from_u128(1));
        write_segment(&dir, 0, &[(0, a, 0, FLAG_FIRST_IN_TXN), (1, a, 1, BOTH)]);
        assert!(matches!(
            rebuild(d.path(), &Options::new()),
            Err(Error::Corrupt { .. })
        ));
    }
}
